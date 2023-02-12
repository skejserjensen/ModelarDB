/* Copyright 2018 The ModelarDB Contributors
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
package dk.aau.modelardb.storage

import dk.aau.modelardb.core._
import dk.aau.modelardb.core.utility.Static
import dk.aau.modelardb.engines.h2.{H2, H2Storage}
import dk.aau.modelardb.engines.spark.{Spark, SparkStorage}

import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.h2.table.TableFilter

import java.sql.{Array => _, _}
import java.lang

import scala.collection.mutable

class JDBCStorage(connectionStringAndTypes: String) extends Storage with H2Storage with SparkStorage {

  /** Public Methods **/
  //Storage
  override def open(dimensions: Dimensions): Unit = {
    //Initializes the RDBMS connection
    this.connection = DriverManager.getConnection(connectionString)
    this.connection.setAutoCommit(false)

    //Checks if the tables and indexes exist and create them if necessary
    val metadata = this.connection.getMetaData
    val tableType = Array("TABLE")
    val tables = metadata.getTables(null, null, "SEGMENT", tableType)

    if ( ! tables.next()) {
      val stmt = this.connection.createStatement()
      stmt.executeUpdate(s"CREATE TABLE model_type(mtid INTEGER, name ${this.textType})")
      stmt.executeUpdate(s"CREATE TABLE segment(gid INTEGER, start_time BIGINT, end_time BIGINT, mtid INTEGER, model ${this.blobType}, gaps ${this.blobType})")
      stmt.executeUpdate(s"CREATE TABLE time_series(tid INTEGER, scaling_factor REAL, sampling_interval INTEGER, gid INTEGER${getDimensionsSQL(dimensions, this.textType)})")

      stmt.executeUpdate("CREATE INDEX segment_gid ON segment(gid)")
      stmt.executeUpdate("CREATE INDEX segment_start_time ON segment(start_time)")
      stmt.executeUpdate("CREATE INDEX segment_end_time ON segment(end_time)")
    }

    //Prepares the necessary statements
    this.insertStmt = this.connection.prepareStatement("INSERT INTO segment VALUES(?, ?, ?, ?, ?, ?)")
    this.getMaxTidStmt = this.connection.prepareStatement("SELECT MAX(tid) FROM time_series")
    this.getMaxGidStmt = this.connection.prepareStatement("SELECT MAX(gid) FROM time_series")
  }

  def storeTimeSeries(timeSeriesGroupRows: Array[Array[Object]]): Unit = {
    val columnsInNormalizedDimensions = this.dimensions.getColumns.length
    val columns = "?, " * (columnsInNormalizedDimensions + 3) + "?"
    val insertSourceStmt = this.connection.prepareStatement("INSERT INTO time_series VALUES(" + columns + ")")
    for (row <- timeSeriesGroupRows) {
      insertSourceStmt.clearParameters()
      for (elementAndIndex <- row.zipWithIndex) {
        val jdbcIndex = elementAndIndex._2 + 1
        elementAndIndex._1 match {
          case elem: lang.String => insertSourceStmt.setString(jdbcIndex, elem)
          case elem: lang.Integer => insertSourceStmt.setInt(jdbcIndex, elem)
          case elem: lang.Long => insertSourceStmt.setLong(jdbcIndex, elem)
          case elem: lang.Float => insertSourceStmt.setFloat(jdbcIndex, elem)
          case elem: lang.Double => insertSourceStmt.setDouble(jdbcIndex, elem)
        }
      }
      insertSourceStmt.executeUpdate()
    }
  }

  def getTimeSeries: mutable.HashMap[Integer, Array[Object]] = {
    val columnsInNormalizedDimensions = dimensions.getColumns.length
    val stmt = this.connection.createStatement()
    val results = stmt.executeQuery("SELECT * FROM time_series")
    val timeSeriesInStorage = mutable.HashMap[Integer, Array[Object]]()
    while (results.next) {
      //The metadata is stored as (Tid => Scaling Factor, Sampling Interval, Gid, Dimensions)
      val tid = results.getInt(1) //Tid
      val metadata = mutable.ArrayBuffer[Object]()
      metadata += results.getFloat(2).asInstanceOf[Object] //Scaling Factor
      metadata += results.getInt(3).asInstanceOf[Object] //Sampling Interval
      metadata += results.getInt(4).asInstanceOf[Object] //Gid

      //Dimensions
      var column = 5
      val dimensionTypes = dimensions.getTypes
      while(column <= columnsInNormalizedDimensions + 4) {
        dimensionTypes(column - 5) match {
          case Dimensions.Types.TEXT => metadata += results.getString(column).asInstanceOf[Object]
          case Dimensions.Types.INT => metadata += results.getInt(column).asInstanceOf[Object]
          case Dimensions.Types.LONG => metadata += results.getLong(column).asInstanceOf[Object]
          case Dimensions.Types.FLOAT => metadata += results.getFloat(column).asInstanceOf[Object]
          case Dimensions.Types.DOUBLE => metadata += results.getDouble(column).asInstanceOf[Object]
        }
        column += 1
      }
      timeSeriesInStorage.put(tid, metadata.toArray)
    }
    timeSeriesInStorage
  }

  def storeModelTypes(modelsToInsert: mutable.HashMap[String, Integer]): Unit = {
    val insertModelStmt = connection.prepareStatement("INSERT INTO model_type VALUES(?, ?)")
    for ((k, v) <- modelsToInsert) {
      insertModelStmt.clearParameters()
      insertModelStmt.setInt(1, v)
      insertModelStmt.setString(2, k)
      insertModelStmt.executeUpdate()
    }
  }

  def getModelTypes: mutable.HashMap[String, Integer] = {
    val stmt = this.connection.createStatement()
    val results = stmt.executeQuery("SELECT * FROM model_type")
    val modelsInStorage = mutable.HashMap[String, Integer]()
    while (results.next) {
      modelsInStorage.put(results.getString(2), results.getInt(1))
    }
    modelsInStorage
  }

  override def getMaxTid: Int = {
    getFirstInteger(this.getMaxTidStmt)
  }

  override def getMaxGid: Int = {
    getFirstInteger(this.getMaxGidStmt)
  }

  override def close(): Unit = {
    //Connection cannot be closed while a transaction is running
    this.connection.commit()
    this.connection.close()
  }

  //H2Storage
  override def storeSegmentGroups(segmentGroups: Array[SegmentGroup]): Unit = {
    this.storageSegmentGroupLock.writeLock().lock()
    try {
      for (segmentGroup <- segmentGroups) {
        this.insertStmt.setInt(1, segmentGroup.gid)
        this.insertStmt.setLong(2, segmentGroup.startTime)
        this.insertStmt.setLong(3, segmentGroup.endTime)
        this.insertStmt.setInt(4, segmentGroup.mtid)
        this.insertStmt.setBytes(5, segmentGroup.model)
        this.insertStmt.setBytes(6, segmentGroup.offsets)
        this.insertStmt.addBatch()
      }
      this.insertStmt.executeBatch()
      this.connection.commit()
    } catch {
      case se: java.sql.SQLException =>
        close()
        throw new java.lang.RuntimeException(se)
    }
    this.storageSegmentGroupLock.writeLock().unlock()
  }

  override def getSegmentGroups(filter: TableFilter): Iterator[SegmentGroup] = {
    this.storageSegmentGroupLock.readLock().lock()
    val rs = this.getSegmentGroups(H2.expressionToSQLLikePredicates(filter.getSelect.getCondition,
      this.timeSeriesGroupCache, this.memberTimeSeriesCache, sql = true))
    this.storageSegmentGroupLock.readLock().unlock()
    rs
  }

  //SparkStorage
  override def open(ssb: SparkSession.Builder, dimensions: Dimensions): SparkSession = {
    this.open(dimensions)
    ssb.getOrCreate()
  }

  override def storeSegmentGroups(sparkSession: SparkSession, df: DataFrame): Unit = {
    this.storageSegmentGroupLock.writeLock().lock()
    val segmentGroups = df.collect().map(row => new SegmentGroup(row.getInt(0), row.getTimestamp(1).getTime,
      row.getTimestamp(2).getTime, row.getInt(3), row.getAs[Array[Byte]](4), row.getAs[Array[Byte]](5)))
    this.storeSegmentGroups(segmentGroups)
    this.storageSegmentGroupLock.writeLock().unlock()
  }

  override def getSegmentGroups(sparkSession: SparkSession, filters: Array[Filter]): DataFrame = {
    this.storageSegmentGroupLock.readLock().lock()
    Static.warn("ModelarDB: projection and predicate push-down is not yet implemented")
    val rows = getSegmentGroups("").map(sg => {
      Row(sg.gid, new Timestamp(sg.startTime), new Timestamp(sg.endTime), sg.mtid, sg.model, sg.offsets)
    })
    val df = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(rows.toSeq), Spark.getStorageSegmentGroupsSchema)
    this.storageSegmentGroupLock.readLock().unlock()
    df
  }

  /** Private Methods **/
  private def splitConnectionStringAndTypes(connectionStringWithArguments: String): (String, String, String) = {
    val split = connectionStringWithArguments.split(" ")
    if (split.length == 3) {
      (split(0), split(1), split(2))
    } else {
      val rdbms = connectionStringWithArguments.split(":")(1)
      val defaults = Map(
        "sqlite" -> Tuple3(connectionStringWithArguments, "TEXT", "BYTEA"),
        "postgresql" -> Tuple3(connectionStringWithArguments, "TEXT", "BYTEA"),
        "derby" -> Tuple3(connectionStringWithArguments, "LONG VARCHAR", "LONG VARCHAR FOR BIT DATA"),
        "h2" -> Tuple3(connectionStringWithArguments, "VARCHAR", "BINARY"),
        "hsqldb" -> Tuple3(connectionStringWithArguments, "LONGVARCHAR", "LONGVARBINARY"))
      if ( ! defaults.contains(rdbms)) {
        throw new IllegalArgumentException("ModelarDB: the string and binary type must also be specified for " + rdbms)
      }
      defaults(rdbms)
    }
  }

  private def getSegmentGroups(predicates: String): Iterator[SegmentGroup] = {
    val stmt = this.connection.createStatement()
    Static.info(s"ModelarDB: constructed predicates ($predicates)")
    val results = if (predicates.isEmpty) {
      stmt.executeQuery("SELECT * FROM segment")
    } else {
      stmt.executeQuery("SELECT * FROM segment WHERE " + predicates)
    }
    new Iterator[SegmentGroup] {
      override def hasNext: Boolean = {
        if (results.next()) {
          true
        } else {
          results.close()
          stmt.close()
          false
        }
      }
      override def next(): SegmentGroup = resultSetToSegmentGroup(results)
    }
  }

  private def resultSetToSegmentGroup(resultSet: ResultSet): SegmentGroup = {
    val gid = resultSet.getInt(1)
    val startTime = resultSet.getLong(2)
    val endTime = resultSet.getLong(3)
    val mtid = resultSet.getInt(4)
    val model = resultSet.getBytes(5)
    val gaps = resultSet.getBytes(6)
    new SegmentGroup(gid, startTime, endTime, mtid, model, gaps)
  }

  def getFirstInteger(query: PreparedStatement): Int = {
    try {
      val results = query.executeQuery()
      results.next
      results.getInt(1)
    } catch {
      case se: java.sql.SQLException =>
        close()
        throw new java.lang.RuntimeException(se)
    }
  }

  /** Instance Variables **/
  private var connection: Connection = _
  private var insertStmt: PreparedStatement = _
  private var getMaxTidStmt: PreparedStatement = _
  private var getMaxGidStmt: PreparedStatement = _
  private val (connectionString, textType, blobType) = splitConnectionStringAndTypes(connectionStringAndTypes)
}