/* Copyright 2018-2020 Aalborg University
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

import java.sql.{Array => _, _}
import java.util
import java.util.stream.Stream

import dk.aau.modelardb.core._

import scala.collection.JavaConverters._

class RDBMSStorage(connectionString: String) extends Storage {

  /** Public Methods **/
  override def open(dimensions: Dimensions): Unit = {
    //Initializes the RDBMS connection
    this.connection = DriverManager.getConnection(connectionString)
    this.connection.setAutoCommit(false)

    //Checks if the tables exist and create them if necessary
    val metadata = this.connection.getMetaData
    val tableType = Array("TABLE")
    val tables = metadata.getTables(null, null, "segment", tableType)

    if ( ! tables.next()) {
      val stmt = this.connection.createStatement()
      stmt.executeUpdate("CREATE TABLE model(mid INTEGER, name TEXT)")
      stmt.executeUpdate("CREATE TABLE segment(gid INTEGER, start_time BIGINT, end_time BIGINT, mid INTEGER, params BYTEA, gaps BYTEA)")
      stmt.executeUpdate("CREATE TABLE source(sid INTEGER, scaling FLOAT, resolution INTEGER, gid INTEGER" + dimensions.getSchema + ")")
    }

    //Prepares the necessary statements
    this.insertStmt = this.connection.prepareStatement("INSERT INTO segment VALUES(?, ?, ?, ?, ?, ?)")
    this.getSegmentsStmt = this.connection.prepareStatement("SELECT * FROM segment")
    this.getMaxSidStmt = this.connection.prepareStatement("SELECT MAX(sid) FROM source")
    this.getMaxGidStmt = this.connection.prepareStatement("SELECT MAX(gid) FROM source")
  }

  override def getMaxSID: Int = {
    getFirstInteger(this.getMaxSidStmt)
  }

  override def getMaxGID: Int = {
    getFirstInteger(this.getMaxGidStmt)
  }

  override def initialize(timeSeriesGroups: Array[TimeSeriesGroup], dimensions: Dimensions, modelNames: Array[String]): Unit = {
    //Inserts the metadata for the sources defined in the configuration file (Sid, Resolution, Gid, Dimensions)
    val sourceDimensions = dimensions.getColumns.length
    val columns = "?, " * (sourceDimensions + 3) + "?"
    val insertSourceStmt = connection.prepareStatement("INSERT INTO source VALUES(" + columns + ")")
    for (tsg <- timeSeriesGroups) {
      for (ts <- tsg.getTimeSeries) {
        insertSourceStmt.clearParameters()
        insertSourceStmt.setInt(1, ts.sid)
        insertSourceStmt.setFloat(2, ts.getScalingFactor)
        insertSourceStmt.setInt(3, ts.resolution)
        insertSourceStmt.setInt(4, tsg.gid)

        var column = 5
        for (dim <- dimensions.get(ts.source)) {
          insertSourceStmt.setObject(column, dim.toString)
          column += 1
        }
        insertSourceStmt.executeUpdate()
      }
    }

    //Extracts the scaling factor, resolution, gid, and dimensions for the sources in storage
    var stmt = this.connection.createStatement()
    var results = stmt.executeQuery("SELECT * FROM source")
    val sourcesInStorage = new util.HashMap[Integer, Array[Object]]()
    while (results.next) {
      //The metadata is stored as (Sid => Scaling, Resolution, Gid, Dimensions)
      val sid = results.getInt(1)
      val metadata = new util.ArrayList[Object]()
      metadata.add(results.getFloat(2).asInstanceOf[Object]) //Scaling
      metadata.add(results.getInt(3).asInstanceOf[Object]) //Resolution
      metadata.add(results.getInt(4).asInstanceOf[Object]) //Gid

      //Dimensions
      var column = 5
      while(column <= sourceDimensions + 4) {
        metadata.add(results.getObject(column))
        column += 1
      }
      sourcesInStorage.put(sid, metadata.toArray)
    }


    //Extracts the name of all models in storage
    stmt = this.connection.createStatement()
    results = stmt.executeQuery("SELECT * FROM model")
    val modelsInStorage = new util.HashMap[String, Integer]()
    while (results.next) {
      modelsInStorage.put(results.getString(2), results.getInt(1))
    }

    //Initializes the caches managed by Storage
    val modelsToInsert = super.initializeCaches(modelNames, dimensions, modelsInStorage, sourcesInStorage)

    //Inserts the name of each model in the configuration file but not in the model table
    val insertModelStmt = connection.prepareStatement("INSERT INTO model VALUES(?, ?)")
    for ((k, v) <- modelsToInsert.asScala) {
      insertModelStmt.clearParameters()
      insertModelStmt.setInt(1, v)
      insertModelStmt.setString(2, k)
      insertModelStmt.executeUpdate()
    }
  }

  override def insert(segments: Array[SegmentGroup], size: Int): Unit = {
    try {
      for (segmentGroup <- segments.take(size)) {
        this.insertStmt.setInt(1, segmentGroup.gid)
        this.insertStmt.setLong(2, segmentGroup.startTime)
        this.insertStmt.setLong(3, segmentGroup.endTime)
        this.insertStmt.setInt(4, segmentGroup.mid)
        this.insertStmt.setBytes(5, segmentGroup.parameters)
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
  }

  override def close(): Unit = {
    this.connection.close()
  }

  override def getSegments: Stream[SegmentGroup] = {
    try {
      val results = this.getSegmentsStmt.executeQuery()
      val segments = new util.ArrayList[SegmentGroup]()
      while (results.next()) {
        segments.add(resultToSegmentData(results))
      }
      segments.stream()
    } catch {
      case se: SQLException =>
        close()
        throw new java.lang.RuntimeException(se)
    }
  }

  /** Private Methods **/
  private def resultToSegmentData(result: ResultSet): SegmentGroup = {
    val gid = result.getInt(1)
    val startTime = result.getLong(2)
    val endTime = result.getLong(3)
    val mid = result.getInt(4)
    val params = result.getBytes(5)
    val gaps = result.getBytes(6)
    new SegmentGroup(gid, startTime, endTime, mid, params, gaps)
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
  private var getSegmentsStmt: PreparedStatement = _
  private var getMaxSidStmt: PreparedStatement = _
  private var getMaxGidStmt: PreparedStatement = _
}
