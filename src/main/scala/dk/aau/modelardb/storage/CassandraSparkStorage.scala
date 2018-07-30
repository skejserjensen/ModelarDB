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

import java.math.BigInteger
import java.nio.ByteBuffer
import java.sql.Timestamp
import java.util
import java.util.stream.Stream

import com.datastax.driver.core._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.rdd.CassandraTableScanRDD

import dk.aau.modelardb.core.{Storage, TimeSeries}
import dk.aau.modelardb.core.models.Segment
import dk.aau.modelardb.engines.spark.SparkStorage

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.{Row, SparkSession, sources}

import scala.collection.JavaConverters._

class CassandraSparkStorage(val host: String) extends Storage with SparkStorage {

  /* Public Methods */
  def open(): Unit = {
    //DEBUG: username and password hardcoded for development
    this.connector = CassandraConnector(new SparkConf()
      .set("spark.cassandra.connection.host", host)
      .set("spark.cassandra.auth.username", "cassandra")
      .set("spark.cassandra.auth.password", "cassandra"))
    initialize()
  }

  def getMaxSID: Int = {
    val rows = this.connector.openSession().execute("SELECT DISTINCT sid FROM modelardb.source")

    //Extracts the maximum sid manually as Cassandra does not like aggregate queries
    var maxSID = 0
    val sids = rows.iterator()
    while (sids.hasNext) {
      val currentSID = sids.next.getVarint(0).intValueExact()
      if (currentSID > maxSID) {
        maxSID = currentSID
      }
    }
    maxSID
  }

  def init(timeSeries: Array[TimeSeries], modelNames: Array[String]): Unit = {
    //Uses a manual Cassandra session through the shared session proxy
    val session = this.connector.openSession()

    //Extract all model names from storage
    var stmt = new SimpleStatement("SELECT * FROM modelardb.model")
    var results = session.execute(stmt)
    val modelsInStorage = new util.HashMap[String, Integer]()

    var rows = results.iterator()
    while (rows.hasNext) {
      val row = rows.next
      val value = row.getVarint(0).intValueExact()
      modelsInStorage.put(row.getString(1), value)
    }

    //Extract all the metadata for the sources in storage
    stmt = new SimpleStatement("SELECT * FROM modelardb.source")
    results = session.execute(stmt)
    val sourcesInStorage = new util.HashMap[Integer, Integer]()

    rows = results.iterator()
    while (rows.hasNext) {
      val row = rows.next
      val k = row.getVarint(0).intValueExact()
      val v = row.getVarint(1).intValueExact()
      sourcesInStorage.put(k, v)
    }

    //Initialize the storage caches
    val toInsert = super.initCaches(timeSeries, modelNames, modelsInStorage, sourcesInStorage)

    //Insert the model name for all the models in the config but not in storage
    var insertStmt = session.prepare("INSERT INTO modelardb.model(mid, name) VALUES(?, ?)")
    for ((k, v) <- toInsert._1.asScala) {
      session.execute(
        insertStmt
          .bind()
          .setVarint(0, BigInteger.valueOf(v.longValue()))
          .setString(1, k))
    }

    //Insert the resolution for all the sources in the config but not in storage
    insertStmt = session.prepare("INSERT INTO modelardb.source(sid, resolution) VALUES(?, ?)")
    for ((k, v) <- toInsert._2.asScala) {
      session.execute(
        insertStmt
          .bind()
          .setVarint(0, BigInteger.valueOf(k.longValue()))
          .setVarint(1, BigInteger.valueOf(v.longValue())))
    }
    session.close()

    //Stores the current max sid for later as it should not be increased outside ModelarDB
    this.currentMaxSID = getMaxSID
  }

  def close(): Unit = {
    //NOTE: The Cassandra connector need not be closed explicitly as it lives with the Spark Session
  }

  def insert(segments: Array[Segment], size: Int): Unit = {
    val batch = new BatchStatement()
    for (segment <- segments.take(size)) {
      val segmentName = segment.getClass.getName
      val size = BigInteger.valueOf((segment.endTime - segment.startTime) / segment.resolution)
      val mid = BigInteger.valueOf(this.midCache.get(segmentName).toLong)

      val boundStatement = insertStmt.bind()
        .setVarint(0, BigInteger.valueOf(segment.sid))
        .setVarint(1, size)
        .setTimestamp(2, new Timestamp(segment.endTime))
        .setVarint(3, mid)
        .setBytes(4, ByteBuffer.wrap(segment.parameters()))
        .setBytes(5, ByteBuffer.wrap(segment.gaps()))
      batch.add(boundStatement)
    }

    //Uses a manual Cassandra session through the shared session proxy
    val session = this.connector.openSession()
    session.execute(batch)
    session.close()
  }

  def getSegments: Stream[Segment] = {
    val results = this.connector.openSession().execute("SELECT * FROM modelardb.segment")
    val rc = this.resolutionCache

    java.util.stream.StreamSupport.stream(results.spliterator(), false).map(
      new java.util.function.Function[com.datastax.driver.core.Row, Segment] {
        override def apply(row: com.datastax.driver.core.Row): Segment = {
          val sid = row.getVarint("sid").intValue()
          val size: Long = row.getVarint("size").longValue()
          val endTime = row.getTimestamp("end_time").getTime
          val mid = row.getVarint("mid").intValue()
          val params = row.getBytes("parameters")
          val gaps = row.getBytes("gaps")

          //Retrieve the cached constructor for the segment and build it
          val resolution = rc(sid)
          val startTime = endTime - (size * resolution)
          constructSegment(sid, startTime, endTime, mid, params.array, gaps.array)
        }
      })
  }

  def open(ssb: SparkSession.Builder): SparkSession = {
    //DEBUG: username and password hardcoded for development
    this.sparkSession = ssb
      .config("spark.cassandra.connection.host", host)
      .config("spark.cassandra.auth.username", "cassandra")
      .config("spark.cassandra.auth.password", "cassandra")
      .getOrCreate()
    this.connector = CassandraConnector(this.sparkSession.sparkContext)
    initialize()
    this.sparkSession
  }

  def writeRDD(rdd: RDD[Row]): Unit = {
    rdd.map(row => {
      val ts = row.getTimestamp(1).getTime
      val te = row.getTimestamp(2).getTime
      val res = row.getInt(3)
      val size = (te - ts) / res
      (row.getInt(0), size, te, row.getInt(4), row.getAs[Array[Byte]](5), row.getAs[Array[Byte]](6))
    }).saveToCassandra("modelardb", "segment", SomeColumns("sid", "size", "end_time", "mid", "parameters", "gaps"))
  }

  def getRDD(filters: Array[Filter]): RDD[Row] = {
    //Setup the mappings from Apache Cassandra rows to Apache Spark rows and the rdd representing the full table
    val rowsToRows = getRowsToRows
    val rdd = this.sparkSession.sparkContext.cassandraTable("modelardb", "segment")

    //Constructs a CQL WHERE clause and maximum start time for Apache Spark to filter rows by from the filters
    constructPredicate(filters) match {
      case (null, null) =>
        rdd.map(rowsToRows)
      case (null, maxStartTime) =>
        takeWhile(rdd, rowsToRows, maxStartTime)
      case (predicate, null) =>
        rdd.where(predicate).map(rowsToRows)
      case (predicate, maxStartTime) =>
        takeWhile(rdd.where(predicate), rowsToRows, maxStartTime)
    }
  }

  /** Private Methods **/
  private def initialize() = {
    //Creates the required tables using a manual Cassandra session through the shared session proxy
    val session = this.connector.openSession()
    var createTable: SimpleStatement = null
    createTable = new SimpleStatement("CREATE KEYSPACE IF NOT EXISTS modelardb WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
    session.execute(createTable)

    createTable = new SimpleStatement("CREATE TABLE IF NOT EXISTS modelardb.segment(sid varint, size varint, end_time timestamp, mid varint, parameters blob, gaps blob, PRIMARY KEY (sid, end_time));")
    session.execute(createTable)

    createTable = new SimpleStatement("CREATE TABLE IF NOT EXISTS modelardb.model(mid varint, name ascii, PRIMARY KEY (mid));")
    session.execute(createTable)

    createTable = new SimpleStatement("CREATE TABLE IF NOT EXISTS modelardb.source(sid varint, resolution varint, PRIMARY KEY (sid));")
    session.execute(createTable)

    //Prepare the statement necessary for insertion
    this.insertStmt = session.prepare("INSERT INTO modelardb.segment(sid, size, end_time, mid, parameters, gaps) VALUES(?, ?, ?, ?, ?, ?)")
    session.close()
  }

  private def constructPredicate(filters: Array[Filter]): (String, Timestamp) = {
    val predicates: scala.collection.mutable.ArrayBuffer[String] = scala.collection.mutable.ArrayBuffer()
    var minStartTime: Timestamp = new Timestamp(Long.MaxValue)

    //Spark Documentation: All filters should be parsed as a set of conjunctions, OR is represented as a nested case class
    //NOTE: This method expects segments to be sorted by end_time as fetching until a maximum start_time is done in Spark
    for (filter: Filter <- filters) {
      filter match {
        //Predicate push-down for sid using SELECT * FROM segment WHERE sid IN (..)
        case sources.GreaterThan("sid", value: Int) => predicates.append(rangeToInQuery(value + 1, this.currentMaxSID))
        case sources.GreaterThanOrEqual("sid", value: Int) => predicates.append(rangeToInQuery(value, this.currentMaxSID))
        case sources.LessThan("sid", value: Int) => predicates.append(rangeToInQuery(1, value - 1))
        case sources.LessThanOrEqual("sid", value: Int) => predicates.append(rangeToInQuery(1, value))
        case sources.EqualTo("sid", value: Int) => predicates.append(s"sid = $value")
        case sources.In("sid", values: Array[Any]) => predicates.append(s"sid IN ( ${values.mkString(", ")} )")

        //Predicate half push-down with rows filtered by start time by Apache Spark
        case sources.LessThan("st", value: Timestamp) => if (value.before(minStartTime)) minStartTime = value; null
        case sources.LessThanOrEqual("st", value: Timestamp) => if (value.before(minStartTime)) minStartTime = value; null

        //Predicate push-down for et using SELECT * FROM segment WHERE et <=> ?
        case sources.GreaterThan("et", value: Timestamp) => predicates.append(s"end_time > '$value'")
        case sources.GreaterThanOrEqual("et", value: Timestamp) => predicates.append(s"end_time >= '$value'")
        case sources.LessThan("et", value: Timestamp) => predicates.append(s"end_time < '$value'")
        case sources.LessThanOrEqual("et", value: Timestamp) => predicates.append(s"end_time <= '$value'")
        case sources.EqualTo("et", value: Timestamp) => predicates.append(s"end_time = '$value'")

        //The predicate cannot be supported when using Apache Cassandra for storage so all we can do is inform the user
        case p => println("ModelarDB: unsupported predicate for CassandraSparkStorage predicate push-down " + p); null
      }
    }

    //The full predicate have been constructed and max start time have been extracted
    val pr = if (predicates.isEmpty) null else predicates.mkString(" AND ")
    val tr = if (minStartTime.getTime == Long.MaxValue) null else minStartTime
    println(s"ModelarDB: constructed predicates ($pr, ResultSet.takeWhile(st <= $tr)")
    (pr, tr)
  }

  private def rangeToInQuery(st: Int, et: Int): String = {
    //Using in queries for a large number of partition key becomes slower then scans around 1500 keys
    val range = et - st
    if (range > 1500) {
      return null
    }
    s"sid IN ( ${(st to et).mkString(", ")} )"
  }

  private def takeWhile(rdd: CassandraTableScanRDD[CassandraRow],
                        rowsToRows: (CassandraRow => Row),
                        maxStartTime: Timestamp): RDD[Row] = {
    rdd
      .keyBy(_.getInt(0))
      .spanByKey
      .flatMap(_._2.map(rowsToRows).takeWhile((row: Row) => ! row.getTimestamp(1).after(maxStartTime)))
  }

  private def getRowsToRows: (CassandraRow => Row) = {
    val rc = this.resolutionCache

    //Converts the cassandra rows to spark rows and reconstruct start time as a long value
    //Schema: Int, java.sql.Timestamp, java.sql.Timestamp, Int, Int, Array[Byte], Array[Byte]
    row => {
      val sid = row.getInt("sid")
      val size: Long = row.getInt("size")
      val endTime = row.getLong("end_time")
      val mid = row.getInt("mid")
      val params = row.getBytes("parameters")
      val gaps = row.getBytes("gaps")

      //Retrieve the resolution cache so the actual rows can be reconstructed
      val resolution = rc(sid)
      val startTime = endTime - (size * resolution)
      Row(sid, new Timestamp(startTime), new Timestamp(endTime), resolution, mid, params.array(), gaps.array())
    }
  }

  /** Instance Variable **/
  private var currentMaxSID = 0
  private var connector: CassandraConnector = _
  private var insertStmt: PreparedStatement = _
  private var sparkSession: SparkSession = _
}