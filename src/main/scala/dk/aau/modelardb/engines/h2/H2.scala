/* Copyright 2021 The ModelarDB Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dk.aau.modelardb.engines.h2

import dk.aau.modelardb.core.Dimensions.Types
import dk.aau.modelardb.core._
import dk.aau.modelardb.core.utility.{Logger, SegmentFunction, Static}
import dk.aau.modelardb.engines.{EngineUtilities, QueryEngine}
import dk.aau.modelardb.remote.{ArrowResultSet, QueryInterface, RemoteStorageFlightProducer, RemoteUtilities}

import org.h2.expression.condition.{Comparison, ConditionAndOr, ConditionInConstantSet}
import org.h2.expression.{Expression, ExpressionColumn, ValueExpression}
import org.h2.table.TableFilter
import org.h2.value.{ValueInt, ValueTimestamp}

import org.apache.arrow.flight.{FlightServer, Location}
import org.codehaus.jackson.map.util.ISO8601Utils

import java.sql.{DriverManager, Timestamp}
import java.util.concurrent.{CountDownLatch, Executors}
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.function.BooleanSupplier
import java.util.{Base64, TimeZone, UUID}

import scala.collection.mutable
import scala.collection.JavaConverters._

class H2(configuration: Configuration, h2storage: H2Storage) extends QueryEngine {

  /** Public Methods **/
  def start(): Unit = {
    //Initialize
    val connection = DriverManager.getConnection(this.h2ConnectionString)
    val stmt = connection.createStatement()
    stmt.execute(H2.getCreateDataPointViewSQL(configuration.getDimensions))
    stmt.execute(H2.getCreateSegmentViewSQL(configuration.getDimensions))
    H2UDAF.initialize(stmt)
    stmt.close()
    val dimensions = configuration.getDimensions
    EngineUtilities.initialize(dimensions)

    //Ingestion
    H2.initialize(this, h2storage)
    startIngestion(dimensions)

    //Interface
    QueryInterface.start(configuration, this)

    //Shutdown
    waitUntilIngestionIsDone()
    if (this.flightServer != null) { // null unless modelardb.transfer server
      this.flightServer.close()
    }
    connection.close()
  }

  override def listTables(): Array[String] = {
    Array("DATAPOINT", "SEGMENT")
  }

  override def executeToJSON(query: String): Array[String] = {
    //Execute Query
    val connection = DriverManager.getConnection(this.h2ConnectionString)
    val stmt = connection.createStatement()
    stmt.execute(query)
    val rs = stmt.getResultSet
    val md = rs.getMetaData

    //Format Result
    val result = mutable.ArrayBuffer[String]()
    val line = new StringBuilder()
    val columnSeparators = md.getColumnCount
    while (rs.next()) {
      var columnIndex = 1
      line.append('{')
      while (columnIndex < columnSeparators) {
        addColumnToOutput(md.getColumnName(columnIndex), rs.getObject(columnIndex), ',', line)
        columnIndex += 1
      }
      addColumnToOutput(md.getColumnName(columnIndex), rs.getObject(columnIndex), '}', line)
      result.append(line.mkString)
      line.clear()
    }

    //Close and Return
    rs.close()
    stmt.close()
    connection.close()
    result.toArray
  }

  override def executeToArrow(query: String): ArrowResultSet = {
    new H2ResultSet(this.h2ConnectionString, query, this.rootAllocator)
  }

  def getSegmentGroups(filter: TableFilter): Iterator[SegmentGroup] = {
    this.cacheLock.readLock().lock()
    val cachedTemporarySegments = this.temporarySegments.values.flatten.toArray
    val cachedFinalizedSegments = this.finalizedSegments.take(this.finalizedSegmentsIndex)
    val persistedFinalizedSegments = this.h2storage.getSegmentGroups(filter)
    this.cacheLock.readLock().unlock()
    cachedTemporarySegments.iterator ++ cachedFinalizedSegments.iterator ++ persistedFinalizedSegments
  }

  /** Private Methods **/
  //Ingestion
  private def startIngestion(dimensions: Dimensions): Unit = {
    h2storage.open(dimensions)
    val ingestors = configuration.getIngestors
    if (ingestors == 0) {
      if ( ! configuration.getDerivedTimeSeries.isEmpty) { //Initializes derived time series
        Partitioner.initializeTimeSeries(configuration, h2storage.getMaxTid)
      }
      h2storage.storeMetadataAndInitializeCaches(configuration, Array[TimeSeriesGroup]())
    } else {
      val transfer = configuration.getString("modelardb.transfer", "None")
      val (mode, port) = RemoteUtilities.getInterfaceAndPort(transfer, 10000)

      mode match {
        case "server" =>
          //Initialize Data Transfer Server
          h2storage.storeMetadataAndInitializeCaches(configuration, Array[TimeSeriesGroup]())
          val location = new Location("grpc://0.0.0.0:" + port)
          val producer = new RemoteStorageFlightProducer(configuration, h2storage, port)
          val executor = Executors.newFixedThreadPool(ingestors + 1) //Plus one so ingestors threads perform ingestion
          this.flightServer = FlightServer.builder(this.rootAllocator, location, producer).executor(executor).build()
          this.flightServer.start()
          Static.info(f"ModelarDB: Arrow Flight transfer end-point is ready (Port: $port)")
        case _ => //If data transfer is enabled to StorageFactory have wrapped H2Storage with a RemoteStorage instance
          //Initialize Local Ingestion to H2Storage (Possible RemoteStorage)
          val timeSeries = Partitioner.initializeTimeSeries(configuration, h2storage.getMaxTid)
          val timeSeriesGroups = Partitioner.groupTimeSeries(configuration, timeSeries, h2storage.getMaxGid)
          h2storage.storeMetadataAndInitializeCaches(configuration, timeSeriesGroups)

          val mtidCache = h2storage.mtidCache.asJava
          this.numberOfRunningIngestors = new CountDownLatch(ingestors)
          this.workingSets = Partitioner.partitionTimeSeries(configuration, timeSeriesGroups, mtidCache, ingestors)

          //Start Ingestion
          Static.info("ModelarDB: waiting for all ingestors to finnish")
          println(WorkingSet.toString(this.workingSets))
          val executor = configuration.getExecutorService
          for (workingSet <- this.workingSets) {
            executor.execute(() => ingest(workingSet))
          }
      }
    }
  }

  private def ingest(workingSet: WorkingSet): Unit = {
    //Creates a method that stores temporary segments in memory and finalized segments in batches to be written to disk
    val consumeTemporary = new SegmentFunction {
      override def emit(gid: Int, startTime: Long, endTime: Long, mtid: Int, model: Array[Byte], gaps: Array[Byte]): Unit = {
        cacheLock.writeLock().lock()
        val newTemporarySegment = new SegmentGroup(gid, startTime, endTime, mtid, model, gaps)
        val currentTemporarySegments = temporarySegments.getOrElse(gid, Array())
        temporarySegments(gid) = updateTemporarySegment(currentTemporarySegments, newTemporarySegment, isTemporary = true)
        cacheLock.writeLock().unlock()
      }
    }

    val consumeFinalized = new SegmentFunction {
      override def emit(gid: Int, startTime: Long, endTime: Long, mtid: Int, model: Array[Byte], gaps: Array[Byte]): Unit = {
        cacheLock.writeLock().lock()
        //Update the current temporary segments
        val newFinalizedSegment = new SegmentGroup(gid, startTime, endTime, mtid, model, gaps)
        val currentTemporarySegments = temporarySegments.getOrElse(gid, Array())
        temporarySegments(gid) = updateTemporarySegment(currentTemporarySegments, newFinalizedSegment, isTemporary = false)

        //Batch the finalized segment
        finalizedSegments(finalizedSegmentsIndex) = newFinalizedSegment
        finalizedSegmentsIndex += 1
        if (finalizedSegmentsIndex == configuration.getBatchSize) {
          h2storage.storeSegmentGroups(finalizedSegments)
          finalizedSegmentsIndex = 0
        }
        cacheLock.writeLock().unlock()
      }
    }

    val isTerminated = new BooleanSupplier {
      override def getAsBoolean: Boolean = false
    }

    //Start Ingestion
    workingSet.process(consumeTemporary, consumeFinalized, isTerminated)

    //Write remaining finalized segments
    this.cacheLock.writeLock().lock()
    h2storage.storeSegmentGroups(this.finalizedSegments.take(this.finalizedSegmentsIndex))
    this.finalizedSegmentsIndex = 0

    //The CountDownLatch is decremented in the lock to ensure countDown and getCount is atomic
    this.numberOfRunningIngestors.countDown()
    if (this.numberOfRunningIngestors.getCount == 0) {
      val logger = new Logger()
      this.workingSets.foreach(ws => logger.add(ws.logger))
      logger.printWorkingSetResult()
      if (this.numberOfRunningIngestors != null) {
      }
    }
    this.cacheLock.writeLock().unlock()
  }

  private def updateTemporarySegment(cache: Array[SegmentGroup], inputSegmentGroup: SegmentGroup,
                                     isTemporary: Boolean): Array[SegmentGroup]= {
    //The gaps are extracted from the new finalized or temporary segment
    val inputGaps = Static.bytesToInts(inputSegmentGroup.offsets)

    //Extracts the metadata for the group of time series being updated
    val groupMetadataCache = h2storage.groupMetadataCache
    val group = groupMetadataCache(inputSegmentGroup.gid).drop(1)
    val samplingInterval = groupMetadataCache(inputSegmentGroup.gid)(0)
    val inputIngested = group.toSet.diff(inputGaps.toSet)
    var updatedExistingSegment = false

    for (i <- cache.indices) {
      //The gaps are extracted for each existing temporary row
      val cachedSegmentGroup = cache(i)
      val cachedGap = Static.bytesToInts(cachedSegmentGroup.offsets)
      val cachedIngested = group.toSet.diff(cachedGap.toSet)

      //Each existing temporary segment that contains values for the same time series as the new segment is updated
      if (cachedIngested.intersect(inputIngested).nonEmpty) {
        if (isTemporary) {
          //A new temporary segment always represent newer data points than the previous temporary segment
          cache(i) = inputSegmentGroup
        } else {
          //Moves the start time of the temporary segment to the data point right after the finalized segment, if
          // the new start time is after the end time of the temporary segment it can be dropped from the cache
          cache(i) = null //The current temporary segment is deleted if it overlaps completely with the finalized segment
          val startTime = inputSegmentGroup.endTime + samplingInterval
          if (startTime <= cachedSegmentGroup.endTime) {
            val newGaps = Static.intToBytes(cachedGap :+ -((startTime - cachedSegmentGroup.startTime) / samplingInterval).toInt)
            cache(i) = new SegmentGroup(cachedSegmentGroup.gid, startTime, cachedSegmentGroup.endTime,
              cachedSegmentGroup.mtid, cachedSegmentGroup.model, newGaps)
          }
        }
        updatedExistingSegment = true
      }
    }

    if (isTemporary && ! updatedExistingSegment) {
      //A split has occurred and multiple segments now represent what one did before, so the new ones are appended
      cache.filter(_ != null) :+ inputSegmentGroup
    } else {
      //If temporary segment have been deleted, e.g., because a join have occurred and one segment now represents
      // what two did before, null values and possible duplicate temporary segments must be removed from the cache
      cache.filter(_ != null).distinct
    }
  }

  private def waitUntilIngestionIsDone(): Unit = {
    if (this.numberOfRunningIngestors != null) {
      this.numberOfRunningIngestors.await()
    }
  }

  //Query Processing
  private def addColumnToOutput(columnName: String, value: AnyRef, end: Char, output: StringBuilder): Unit = {
    output.append('"')
    output.append(columnName)
    output.append('"')
    output.append(':')

    //Numbers should not be quoted
    if (value.isInstanceOf[Int] || value.isInstanceOf[Long]
      || value.isInstanceOf[Float] || value.isInstanceOf[Double]) {
      output.append(value)
    } else if (value.isInstanceOf[Timestamp]) {
      output.append('"')
      output.append(ISO8601Utils.format(value.asInstanceOf[Timestamp], true, TimeZone.getDefault))
      output.append('"')
    } else if (value.isInstanceOf[Array[Byte]]) {
      output.append('"')
      output.append(this.base64Encoder.encodeToString(value.asInstanceOf[Array[Byte]]))
      output.append('"')
    } else {
      output.append('"')
      output.append(value)
      output.append('"')
    }
    output.append(end)
  }

  /** Instance Variables **/
  private var finalizedSegmentsIndex = 0
  private val finalizedSegments: Array[SegmentGroup] = new Array[SegmentGroup](configuration.getBatchSize)
  private var workingSets: Array[WorkingSet] = _
  private var numberOfRunningIngestors: CountDownLatch = _
  private val cacheLock = new ReentrantReadWriteLock()
  private val temporarySegments = mutable.HashMap[Int, Array[SegmentGroup]]()
  private val base64Encoder = Base64.getEncoder
  private val rootAllocator = RemoteUtilities.getRootAllocator(this.configuration)
  private var flightServer: FlightServer = _

  //The H2 in-memory databases is named by an UUID so multiple can be constructed in parallel by the tests
  private val h2ConnectionString: String = "jdbc:h2:mem:" + UUID.randomUUID()
}

object H2 {

  /** Public Methods **/
  def initialize(h2: H2, h2Storage: H2Storage): Unit = {
    this.h2 = h2
    this.h2storage = h2Storage
  }

  //Data Point View
  def getCreateDataPointViewSQL(dimensions: Dimensions): String = {
    s"""CREATE TABLE DataPoint(tid INT, timestamp TIMESTAMP, value REAL${H2.getDimensionColumns(dimensions)})
       |ENGINE "dk.aau.modelardb.engines.h2.ViewDataPoint";
       |""".stripMargin
  }

  //Segment View
  def getCreateSegmentViewSQL(dimensions: Dimensions): String = {
    s"""CREATE TABLE Segment
       |(tid INT, start_time TIMESTAMP, end_time TIMESTAMP, mtid INT, model BINARY, offsets BINARY${H2.getDimensionColumns(dimensions)})
       |ENGINE "dk.aau.modelardb.engines.h2.ViewSegment";
       |""".stripMargin
  }

  def expressionToSQLLikePredicates(expression: Expression, tsgc: Array[Int],
                                    idc: mutable.HashMap[String, mutable.HashMap[Object, Array[Integer]]],
                                    sql: Boolean): String = { //CQL is SQL-like but does not support OR and () in WHERE
    expression match {
      //NO PREDICATES
      case null => ""
      //COLUMN OPERATOR VALUE
      case c: Comparison =>
        //HACK: Extracts the operator from the sub-tree using reflection as compareType seems to be completely inaccessible
        val operator = this.compareTypeMethod.invoke(c, this.compareTypeField.get(c)).asInstanceOf[String]
        val ec = c.getSubexpression(0).asInstanceOf[ExpressionColumn]
        val ve = c.getSubexpression(1).asInstanceOf[ValueExpression]
        (ec.getColumnName, operator) match {
          //TID
          case ("TID", "=") => val tid = ve.getValue(null).asInstanceOf[ValueInt].getInt
            "GID = " + EngineUtilities.tidPointToGidPoint(tid, tsgc)
          //TIMESTAMP
          case ("TIMESTAMP", ">") => "END_TIME > " + ve.getValue(null).asInstanceOf[ValueTimestamp].getTimestamp(TimeZone.getDefault).getTime
          case ("TIMESTAMP", ">=") => "END_TIME >= " + ve.getValue(null).asInstanceOf[ValueTimestamp].getTimestamp(TimeZone.getDefault).getTime
          case ("TIMESTAMP", "<") => "START_TIME < " + ve.getValue(null).asInstanceOf[ValueTimestamp].getTimestamp(TimeZone.getDefault).getTime
          case ("TIMESTAMP", "<=") => "START_TIME <= " + ve.getValue(null).asInstanceOf[ValueTimestamp].getTimestamp(TimeZone.getDefault).getTime
          case ("TIMESTAMP", "=") =>
            val predicate = "START_TIME <= " + ve.getValue(null).asInstanceOf[ValueTimestamp].getTimestamp(TimeZone.getDefault).getTime +
              " AND END_TIME >= " + ve.getValue(null).asInstanceOf[ValueTimestamp].getTimestamp(TimeZone.getDefault).getTime
            if (sql) "(" + predicate + ")" else predicate
          //DIMENSIONS
          case (columnName, "=") if idc.contains(columnName) =>
            EngineUtilities.dimensionEqualToGidIn(columnName, ve.getValue(null).getObject, idc).mkString("GID IN (", ",", ")")
          case p => Static.warn("ModelarDB: predicate push-down is not supported for " + p, 120); ""
        }
      //IN
      case cin: ConditionInConstantSet =>
        cin.getSubexpression(0).getColumnName match {
          case "TID" =>
            val tids = Array.fill[Any](cin.getSubexpressionCount - 1)(0) //The first value is the column name
            for (i <- Range(1, cin.getSubexpressionCount)) {
              tids(i - 1) = cin.getSubexpression(i).getValue(null).asInstanceOf[ValueInt].getInt
            }
            EngineUtilities.tidInToGidIn(tids, tsgc).mkString("GID IN (", ",", ")")
          case p => Static.warn("ModelarDB: predicate push-down is not supported for " + p, 120); ""
        }
      //AND
      case cao: ConditionAndOr if this.andOrTypeField.getInt(cao) == ConditionAndOr.AND =>
        val left = expressionToSQLLikePredicates(cao.getSubexpression(0), tsgc, idc, sql)
        val right = expressionToSQLLikePredicates(cao.getSubexpression(1), tsgc, idc, sql)
        if (left == "" || right == "") "" else sql match {
          case true => "(" + left + " AND " + right + ")" //SQL
          case false => left + " AND " + right            //CQL
        }
      //OR
      case cao: ConditionAndOr if this.andOrTypeField.getInt(cao) == ConditionAndOr.OR && sql =>
        val left = expressionToSQLLikePredicates(cao.getSubexpression(0), tsgc, idc, sql)
        val right = expressionToSQLLikePredicates(cao.getSubexpression(1), tsgc, idc, sql)
        if (left == "" || right == "") "" else "(" + left + " OR " + right + ")"
      case p => Static.warn("ModelarDB: predicate push-down is not supported for " + p, 120); ""
    }
  }

  /** Private Methods **/
  private def getDimensionColumns(dimensions: Dimensions): String = {
    if (dimensions.getColumns.isEmpty) {
      ""
    } else {
      dimensions.getColumns.zip(dimensions.getTypes).map {
        case (name, Types.INT) => name + " INT"
        case (name, Types.LONG) => name + " BIGINT"
        case (name, Types.FLOAT) => name + " REAL"
        case (name, Types.DOUBLE) => name + " DOUBLE"
        case (name, Types.TEXT) => name + " VARCHAR"
      }.mkString(", ", ", ", "")
    }
  }

  /** Instance Variables **/
  var h2: H2 = _ //Provides access to the h2 and h2storage instances from the views
  var h2storage: H2Storage =  _
  private val compareTypeField = classOf[Comparison].getDeclaredField("compareType")
  this.compareTypeField.setAccessible(true)
  private val compareTypeMethod = classOf[Comparison].getDeclaredMethod("getCompareOperator", classOf[Int])
  this.compareTypeMethod.setAccessible(true)
  private val andOrTypeField = classOf[ConditionAndOr].getDeclaredField("andOrType")
  this.andOrTypeField.setAccessible(true)
}
