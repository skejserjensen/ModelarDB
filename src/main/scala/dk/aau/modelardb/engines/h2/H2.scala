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

import dk.aau.modelardb.Interface
import dk.aau.modelardb.core.Dimensions.Types
import dk.aau.modelardb.core._
import dk.aau.modelardb.core.utility.{Logger, SegmentFunction, Static}
import dk.aau.modelardb.engines.EngineUtilities
import org.h2.expression.condition.{Comparison, ConditionAndOr, ConditionInConstantSet}
import org.h2.expression.{Expression, ExpressionColumn, ValueExpression}
import org.h2.table.TableFilter
import org.h2.value.{ValueInt, ValueTimestamp}

import java.sql.DriverManager
import java.util
import java.util.concurrent.CountDownLatch
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.function.BooleanSupplier
import java.util.{Base64, TimeZone}
import scala.collection.mutable

class H2(configuration: Configuration, h2storage: H2Storage) {
  /** Instance Variables **/
  private var finalizedSegmentsIndex = 0
  private val finalizedSegments: Array[SegmentGroup] = new Array[SegmentGroup](configuration.getBatchSize)
  private var workingSets: Array[WorkingSet] = _
  private var numberOfRunningIngestors: CountDownLatch = _
  private val cacheLock = new ReentrantReadWriteLock()
  private val temporarySegments = mutable.HashMap[Int, Array[SegmentGroup]]()
  private val base64Encoder = Base64.getEncoder

  /** Public Methods **/
  def start(): Unit = {
    //Initialize
    val connection = DriverManager.getConnection(H2.h2ConnectionString)
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
    Interface.start(configuration, q => this.executeQuery(q))

    //Shutdown
    connection.close()
    waitUntilIngestionIsDone()
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
    //Initialize Storage
    h2storage.open(dimensions)
    if (configuration.getIngestors == 0) {
      if ( ! configuration.getDerivedTimeSeries.isEmpty) { //Initializes derived time series
        Partitioner.initializeTimeSeries(configuration, h2storage.getMaxTid)
      }
      h2storage.initialize(Array(), configuration.getDerivedTimeSeries, dimensions, configuration.getModelTypeNames)
      return
    }

    //Initialize Ingestion
    val timeSeries = Partitioner.initializeTimeSeries(configuration, h2storage.getMaxTid)
    val timeSeriesGroups = Partitioner.groupTimeSeries(configuration, timeSeries, h2storage.getMaxGid)
    h2storage.initialize(timeSeriesGroups, configuration.getDerivedTimeSeries, dimensions, configuration.getModelTypeNames)

    val mtidCache = h2storage.mtidCache
    val ingestors = configuration.getIngestors
    val executor = configuration.getExecutorService
    this.numberOfRunningIngestors = new CountDownLatch(ingestors)
    this.workingSets = Partitioner.partitionTimeSeries(configuration, timeSeriesGroups, mtidCache, ingestors)

    //Start Ingestion
    Static.info("ModelarDB: waiting for all ingestors to finnish")
    println(WorkingSet.toString(workingSets))
    for (workingSet <- workingSets) {
      executor.execute(() => ingest(workingSet))
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
          h2storage.storeSegmentGroups(finalizedSegments, finalizedSegmentsIndex)
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
    cacheLock.writeLock().lock()
    h2storage.storeSegmentGroups(finalizedSegments, finalizedSegmentsIndex)
    finalizedSegmentsIndex = 0

    //The CountDownLatch is decremented in the lock to ensure countDown and getCount is atomic
    this.numberOfRunningIngestors.countDown()
    if (this.numberOfRunningIngestors.getCount == 0) {
      val logger = new Logger()
      this.workingSets.foreach(ws => logger.add(ws.logger))
      logger.printWorkingSetResult()
      if (this.numberOfRunningIngestors != null) {
      }
    }
    cacheLock.writeLock().unlock()
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
  private def executeQuery(query: String): Array[String] = {
    //Execute Query
    val connection = DriverManager.getConnection(H2.h2ConnectionString)
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
    result.toArray
  }

  private def addColumnToOutput(columnName: String, value: AnyRef, end: Char, output: StringBuilder): Unit = {
    output.append('"')
    output.append(columnName)
    output.append('"')
    output.append(':')

    //Numbers should not be quoted
    if (value.isInstanceOf[Int] || value.isInstanceOf[Float]) {
      output.append(value)
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
}

object H2 {
  /** Instance Variables * */
  var h2: H2 = _ //Provides access to the h2 and h2storage instances from the views
  var h2storage: H2Storage =  _
  private val h2ConnectionString: String = "jdbc:h2:mem:modelardb"
  private val compareTypeField = classOf[Comparison].getDeclaredField("compareType")
  this.compareTypeField.setAccessible(true)
  private val compareTypeMethod = classOf[Comparison].getDeclaredMethod("getCompareOperator", classOf[Int])
  this.compareTypeMethod.setAccessible(true)
  private val andOrTypeField = classOf[ConditionAndOr].getDeclaredField("andOrType")
  this.andOrTypeField.setAccessible(true)

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
       |(tid INT, start_time TIMESTAMP, end_time TIMESTAMP, mtid INT, model BINARY, gaps BINARY${H2.getDimensionColumns(dimensions)})
       |ENGINE "dk.aau.modelardb.engines.h2.ViewSegment";
       |""".stripMargin
  }

  def expressionToSQLPredicates(expression: Expression, tsgc: Array[Int], idc: util.HashMap[String, util.HashMap[Object, Array[Integer]]],
                                supportsOr: Boolean): String = { //HACK: supportsOR ensures Cassandra does not receive an OR operator
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
            "(START_TIME <= " + ve.getValue(null).asInstanceOf[ValueTimestamp].getTimestamp(TimeZone.getDefault).getTime +
              " AND END_TIME >= " + ve.getValue(null).asInstanceOf[ValueTimestamp].getTimestamp(TimeZone.getDefault).getTime + ")"
          //DIMENSIONS
          case (columnName, "=") if idc.containsKey(columnName) =>
            EngineUtilities.dimensionEqualToGidIn(columnName, ve.getValue(null).getObject, idc).mkString("GID IN (", ",", ")")
          case p => Static.warn("ModelarDB: unsupported predicate " + p, 120); ""
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
          case p => Static.warn("ModelarDB: unsupported predicate " + p, 120); ""
        }
      //AND
      case cao: ConditionAndOr if this.andOrTypeField.getInt(cao) == ConditionAndOr.AND =>
        val left = expressionToSQLPredicates(cao.getSubexpression(0), tsgc, idc, supportsOr)
        val right = expressionToSQLPredicates(cao.getSubexpression(1), tsgc, idc, supportsOr)
        if (left == "" || right == "") "" else "(" + left + " AND " + right + ")"
      //OR
      case cao: ConditionAndOr if this.andOrTypeField.getInt(cao) == ConditionAndOr.OR && supportsOr =>
        val left = expressionToSQLPredicates(cao.getSubexpression(0), tsgc, idc, supportsOr)
        val right = expressionToSQLPredicates(cao.getSubexpression(1), tsgc, idc, supportsOr)
        if (left == "" || right == "") "" else "(" + left + " OR " + right + ")"
      case p => Static.warn("ModelarDB: unsupported predicate " + p, 120); ""
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
}
