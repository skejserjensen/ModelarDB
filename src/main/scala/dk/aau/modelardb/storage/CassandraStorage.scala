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

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector

import dk.aau.modelardb.core.utility.Static
import dk.aau.modelardb.core.{Dimensions, SegmentGroup}
import dk.aau.modelardb.engines.h2.{H2, H2Storage}
import dk.aau.modelardb.engines.spark.{Spark, SparkStorage}

import org.apache.spark.SparkConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.h2.table.TableFilter

import java.nio.ByteBuffer
import java.time.Instant
import java.util

import scala.collection.mutable

class CassandraStorage(connectionString: String) extends Storage with H2Storage with SparkStorage {

  /** Public Methods **/
  //Storage
  override def open(dimensions: Dimensions): Unit = {
    val (host, user, pass) = parseConnectionString(connectionString)
    this.connector = CassandraConnector(new SparkConf()
      .set("spark.cassandra.connection.host", host)
      .set("spark.cassandra.auth.username", user)
      .set("spark.cassandra.auth.password", pass))
    createTables(dimensions)
  }

  def storeTimeSeries(timeSeriesGroupRows: Array[Array[Object]]): Unit = {
    val session = this.connector.openSession()
    val columnsInNormalizedDimensions = dimensions.getColumns.length
    val columns = if (columnsInNormalizedDimensions  == 0) "" else dimensions.getColumns.mkString(", ", ", ", "")
    val placeholders = "?, " * (columnsInNormalizedDimensions + 3) + "?"
    val insertString = s"INSERT INTO ${this.keyspace}.time_series(tid, scaling_factor, sampling_interval, gid $columns) VALUES($placeholders)"
    for (row <- timeSeriesGroupRows) {
      session.execute(SimpleStatement.builder(insertString).addPositionalValues(row:_*).build())
    }
    session.close()
  }

  def getTimeSeries: mutable.HashMap[Integer, Array[Object]] = {
    val session = this.connector.openSession()
    val stmt = SimpleStatement.newInstance(s"SELECT * FROM ${this.keyspace}.time_series")
    val results = session.execute(stmt)
    val timeSeriesInStorage = mutable.HashMap[Integer, Array[Object]]()
    val rows = results.iterator()
    while (rows.hasNext) {
      //The metadata is stored as (Tid => Scaling, Sampling Interval, Gid, Dimensions)
      val row = rows.next
      val tid = row.getInt("tid")
      val metadata = mutable.ArrayBuffer[Object]()
      metadata += row.getFloat("scaling_factor").asInstanceOf[Object]
      metadata += row.getInt("sampling_interval").asInstanceOf[Object]
      metadata += row.getInt("gid").asInstanceOf[Object]

      //Dimensions
      for (column <- dimensions.getColumns) {
        metadata += row.getObject(column)
      }
      timeSeriesInStorage.put(tid, metadata.toArray)
    }
    session.close()
    timeSeriesInStorage
  }

  def storeModelTypes(modelsToInsert: mutable.HashMap[String, Integer]): Unit = {
    val session = this.connector.openSession()
    val insertStmt = session.prepare(s"INSERT INTO ${this.keyspace}.model_type(mtid, name) VALUES(?, ?)")
    for ((k, v) <- modelsToInsert) {
      session.execute(insertStmt.bind().setInt(0, v).setString(1, k))
    }
    session.close()
  }

  def getModelTypes: mutable.HashMap[String, Integer] = {
    val session = this.connector.openSession()
    val stmt = SimpleStatement.newInstance(s"SELECT * FROM ${this.keyspace}.model_type")
    val results = session.execute(stmt)
    val modelsInStorage = mutable.HashMap[String, Integer]()

    val rows = results.iterator()
    while (rows.hasNext) {
      val row = rows.next
      val mtid = row.getInt("mtid")
      modelsInStorage.put(row.getString("name"), mtid)
    }
    session.close()
    modelsInStorage
  }

  override def getMaxTid: Int = {
    getMaxID(s"SELECT DISTINCT tid FROM ${this.keyspace}.time_series")
  }

  override def getMaxGid: Int = {
    getMaxID(s"SELECT gid FROM ${this.keyspace}.time_series")
  }

  override def close(): Unit = {
    //CassandraConnector will close the underlying Cluster object automatically whenever it is not used i.e.
    // no Session or Cluster is open for longer than spark.cassandra.connection.keepAliveMS property value.
  }

  //H2Storage
  override def storeSegmentGroups(segmentGroups: Array[SegmentGroup]): Unit = {
    this.storageSegmentGroupLock.writeLock().lock()
    val session = this.connector.openSession()
    val batch = new util.ArrayList[BoundStatement](Math.min(segmentGroups.length, 65535))
    for (segmentGroup <- segmentGroups) {
      batch.add(insertStmt.bind()
        .setInt(0, segmentGroup.gid)
        .setInstant(1, Instant.ofEpochMilli(segmentGroup.startTime))
        .setInstant(2, Instant.ofEpochMilli(segmentGroup.endTime))
        .setInt(3, segmentGroup.mtid)
        .setByteBuffer(4, ByteBuffer.wrap(segmentGroup.model))
        .setByteBuffer(5, ByteBuffer.wrap(segmentGroup.offsets)))

      //The maximum batch size supported by Cassandra is 65535
      if (batch.size() == 65535) {
        storeSegmentGroups(session, batch)
      }
    }
    storeSegmentGroups(session, batch)
    session.close()
    this.storageSegmentGroupLock.writeLock().unlock()
  }

  def getSegmentGroups(filter: TableFilter): Iterator[SegmentGroup] = {
    this.storageSegmentGroupLock.readLock().lock()
    val predicates = H2.expressionToSQLLikePredicates(filter.getSelect.getCondition,
      this.timeSeriesGroupCache, this.memberTimeSeriesCache, sql = false)
    Static.info(s"ModelarDB: constructed predicates ($predicates)")
    val session = this.connector.openSession()
    val results = if (predicates.isEmpty) {
      session.execute(s"SELECT gid, start_time, end_time, mtid, model, gaps FROM ${this.keyspace}.segment").iterator()
    } else {
      session.execute(s"SELECT gid, start_time, end_time, mtid, model, gaps FROM ${this.keyspace}.segment WHERE $predicates ALLOW FILTERING").iterator()
    }
    session.close()

    val rs = new Iterator[SegmentGroup] {
      override def hasNext: Boolean = results.hasNext

      override def next(): SegmentGroup = {
        val row = results.next()
        val gid = row.getInt(0).intValue()
        val startTime= row.getInstant(1).toEpochMilli
        val endTime = row.getInstant(2).toEpochMilli
        val mtid = row.getInt(3)
        val model = row.getByteBuffer(4)
        val gaps = row.getByteBuffer(5)
        new SegmentGroup(gid, startTime, endTime, mtid, model.array, gaps.array)
      }
    }
    this.storageSegmentGroupLock.readLock().unlock()
    rs
  }

  //SparkStorage
  override def open(ssb: SparkSession.Builder, dimensions: Dimensions): SparkSession = {
    val (host, user, pass) = parseConnectionString(connectionString)
    val sparkSession = ssb
      .config("spark.sql.catalog.cassandra", "com.datastax.spark.connector.datasource.CassandraCatalog")
      .config("spark.sql.catalog.cassandra.spark.cassandra.connection.host", host)
      .config("spark.sql.catalog.cassandra.spark.cassandra.auth.username", user)
      .config("spark.sql.catalog.cassandra.spark.cassandra.auth.password", pass)
      .withExtensions(new CassandraSparkExtensions)
      .getOrCreate()
    this.connector = CassandraConnector(sparkSession.sparkContext)
    createTables(dimensions)
    sparkSession
  }

  override def storeSegmentGroups(sparkSession: SparkSession, df: DataFrame): Unit = {
    this.storageSegmentGroupLock.writeLock().lock()
    df.write.format("org.apache.spark.sql.cassandra")
      .option("keyspace", this.keyspace).option("table", "segment")
      .mode(SaveMode.Append).save()
    this.storageSegmentGroupLock.writeLock().unlock()
  }

  override def getSegmentGroups(sparkSession: SparkSession, filters: Array[Filter]): DataFrame = {
    this.storageSegmentGroupLock.readLock().lock()
    val df = Spark.applyFiltersToDataFrame(sparkSession.read.table(s"cassandra.${this.keyspace}.segment")
      .select("gid", "start_time", "end_time", "mtid", "model", "gaps"), filters)
    this.storageSegmentGroupLock.readLock().unlock()
    df
  }

  /** Private Methods **/
  private def parseConnectionString(connectionString: String): (String, String, String) = {
    val elems: Array[String] = connectionString.split('?')
    if (elems.length != 1 && elems.length != 2) {
      throw new IllegalArgumentException("ModelarDB: unable to parse connection string \"" + connectionString + "\"")
    }

    //Parses the parameters defined by as key-value pairs after a ? char
    val parsed = new util.HashMap[String, String]()
    if (elems.length == 2) {
      val parameters = elems(1).split('&')
      for (parameter <- parameters) {
        val na = parameter.split('=')
        parsed.put(na(0), na(1))
      }
    }
    this.keyspace = parsed.getOrDefault("keyspace", "modelardb")
    (elems(0), parsed.getOrDefault("username", "cassandra"), parsed.getOrDefault("password", "cassandra"))
  }

  private def createTables(dimensions: Dimensions): Unit = {
    val session = this.connector.openSession()
    var createTable: SimpleStatement = null
    createTable = SimpleStatement.newInstance(s"CREATE KEYSPACE IF NOT EXISTS ${this.keyspace} WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
    session.execute(createTable)

    createTable = SimpleStatement.newInstance(s"CREATE TABLE IF NOT EXISTS ${this.keyspace}.segment(gid INT, start_time TIMESTAMP, end_time TIMESTAMP, mtid INT, model BLOB, gaps BLOB, PRIMARY KEY (gid, start_time, gaps));")
    session.execute(createTable)

    createTable = SimpleStatement.newInstance(s"CREATE TABLE IF NOT EXISTS ${this.keyspace}.model_type(mtid INT, name TEXT, PRIMARY KEY (mtid));")
    session.execute(createTable)

    createTable = SimpleStatement.newInstance(s"CREATE TABLE IF NOT EXISTS ${this.keyspace}.time_series(tid INT, scaling_factor FLOAT, sampling_interval INT, gid INT${getDimensionsSQL(dimensions, "TEXT")}, PRIMARY KEY (tid));")
    session.execute(createTable)

    //The insert statement will be used for every batch of segment groups
    this.insertStmt = session.prepare(s"INSERT INTO ${this.keyspace}.segment(gid, start_time, end_time, mtid, model, gaps) VALUES(?, ?, ?, ?, ?, ?)")
    session.close()
  }

  private def getMaxID(query: String): Int = {
    val rows = this.connector.openSession().execute(query)

    //Extracts the maximum id manually as Cassandra does not like aggregate queries
    var maxID = 0
    val it = rows.iterator()
    while (it.hasNext) {
      val currentID = it.next.getInt(0)
      if (currentID > maxID) {
        maxID = currentID
      }
    }
    maxID
  }

  private def storeSegmentGroups(session: CqlSession, batch: util.ArrayList[BoundStatement]): Unit = {
    val batchStatement = BatchStatement.newInstance(BatchType.LOGGED)
    batchStatement.setIdempotent(true)
    session.execute(batchStatement.addAll(batch))
    batch.clear()
  }

  /** Instance Variables **/
  private var keyspace: String = _
  private var connector: CassandraConnector = _
  private var insertStmt: PreparedStatement = _
}