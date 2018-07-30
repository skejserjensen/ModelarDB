/* Copyright 2018 Aalborg University
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
import java.util.stream.Stream
import java.util.{ArrayList, HashMap}

import dk.aau.modelardb.core.{Storage, TimeSeries}
import dk.aau.modelardb.core.models.Segment

import scala.collection.JavaConverters._

class RDBMSStorage(connectionString: String) extends Storage {

  /** Instance Variables **/
  private var connection: Connection = _
  private var insertStmt: PreparedStatement = _
  private var getSegmentsStmt: PreparedStatement = _
  private var getMaxSidStmt: PreparedStatement = _

  /** Public Methods **/
  def open(): Unit = {
    //Initializes the RDBMS connection
    this.connection = DriverManager.getConnection(connectionString)
    this.connection.setAutoCommit(false)

    //Check if the table exists and create them if necessary
    val metadata = this.connection.getMetaData
    val tableType = Array("TABLE")
    val tables = metadata.getTables(null, null, "segment", tableType)

    if ( ! tables.next()) {
      val stmt = this.connection.createStatement()
      stmt.executeUpdate("CREATE TABLE model(mid INTEGER, name TEXT)")
      stmt.executeUpdate("CREATE TABLE segment(sid INTEGER, start_time BIGINT, end_time BIGINT, mid INTEGER, params BYTEA, gaps BYTEA)")
      stmt.executeUpdate("CREATE TABLE source(sid INTEGER, resolution INTEGER)")
    }

    //Prepare the necessary prepared statements
    this.insertStmt = this.connection.prepareStatement("INSERT INTO segment VALUES(?, ?, ?, ?, ?, ?)")
    this.getSegmentsStmt = this.connection.prepareStatement("SELECT * FROM segment")
    this.getMaxSidStmt = this.connection.prepareStatement("SELECT max(sid) FROM segment")
  }

  def getMaxSID: Int = {
    try {
      val results = getMaxSidStmt.executeQuery()
      results.next
      results.getInt(1)
    } catch {
      case se: java.sql.SQLException =>
        close()
        throw new java.lang.RuntimeException(se)
    }
  }

  def init(timeSeries: Array[TimeSeries], modelNames: Array[String]): Unit = {
    //Extract the model names for models in storage
    var stmt = this.connection.createStatement()
    var results = stmt.executeQuery("SELECT * FROM model")
    val modelsInStorage = new HashMap[String, Integer]()
    while (results.next) {
      modelsInStorage.put(results.getString(2), results.getInt(1))
    }

    //Extract the metadata for the sources in storage
    stmt = this.connection.createStatement()
    results = stmt.executeQuery("SELECT * FROM source")
    val sourcesInStorage = new HashMap[Integer, Integer]()
    while (results.next) {
      sourcesInStorage.put(results.getInt(1), results.getInt(2))
    }


    //Initialize the storage caches
    val toInsert = super.initCaches(timeSeries, modelNames, modelsInStorage, sourcesInStorage)


    //Insert the model names for all the models in the config but not in storage
    val insertModelStmt = connection.prepareStatement("INSERT INTO model VALUES(?, ?)")
    for ((k, v) <- toInsert._1.asScala) {
      insertModelStmt.clearParameters()
      insertModelStmt.setInt(1, v)
      insertModelStmt.setString(2, k)
      insertModelStmt.executeUpdate()
    }

    //Insert the resolution for all the sources in the config but not in storage
    val insertSourceStmt = connection.prepareStatement("INSERT INTO source VALUES(?, ?)")
    for ((k, v) <- toInsert._2.asScala) {
      insertSourceStmt.clearParameters()
      insertSourceStmt.setInt(1, k)
      insertSourceStmt.setInt(2, v)
      insertSourceStmt.executeUpdate()
    }
  }

  def insert(segments: Array[Segment], size: Int): Unit = {
    try {
      for (segment <- segments.take(size)) {
        val segmentName: String = segment.getClass.getName
        this.insertStmt.setInt(1, segment.sid)
        this.insertStmt.setLong(2, segment.startTime)
        this.insertStmt.setLong(3, segment.endTime)
        this.insertStmt.setInt(4, this.midCache.get(segmentName))
        this.insertStmt.setBytes(5, segment.parameters())
        this.insertStmt.setBytes(6, segment.gaps())
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

  def close(): Unit = {
    this.connection.close()
  }

  def getSegments: Stream[Segment] = {
    try {
      val results = this.getSegmentsStmt.executeQuery()
      val segments = new ArrayList[Segment]()
      while (results.next()) {
        segments.add(resultToSegment(results))
      }
      segments.stream()
    } catch {
      case se: SQLException =>
        close()
        throw new java.lang.RuntimeException(se)
    }
  }

  /** Private Methods **/
  private def resultToSegment(result: ResultSet): Segment = {
    //Retrieve the data from the segment table ResultSet
    val sid = result.getInt(1)
    val startTime = result.getLong(2)
    val endTime = result.getLong(3)
    val mid = result.getInt(4)
    val params = result.getBytes(5)
    val gaps = result.getBytes(6)

    //Retrieve the cached constructor for the segment and build it
    super.constructSegment(sid, startTime, endTime, mid, params, gaps)
  }
}