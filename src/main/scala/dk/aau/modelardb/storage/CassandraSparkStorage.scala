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

import java.math.BigInteger
import java.nio.ByteBuffer
import java.sql.Timestamp
import java.util
import java.util.stream.Stream

import com.datastax.driver.core._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import dk.aau.modelardb.core.utility.Static
import dk.aau.modelardb.core.{Dimensions, SegmentGroup, Storage, TimeSeriesGroup}
import dk.aau.modelardb.engines.spark.{Spark, SparkStorage}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.{Row, SparkSession, sources}

import scala.collection.JavaConverters._

class CassandraSparkStorage(connectionString: String) extends Storage with SparkStorage {

  /** Public Methods **/
  override def open(dimensions: Dimensions): Unit = {
    val (host, user, pass) = parseConnectionString(connectionString)
    this.connector = CassandraConnector(new SparkConf()
      .set("spark.cassandra.connection.host", host)
      .set("spark.cassandra.auth.username", user)
      .set("spark.cassandra.auth.password", pass))
    createTables(dimensions)
  }

  override def getMaxSID: Int = {
    getMaxID(s"SELECT DISTINCT sid FROM ${this.keyspace}.source")
  }

  override def getMaxGID: Int = {
    getMaxID(s"SELECT gid FROM ${this.keyspace}.source")
  }

  override def initialize(timeSeriesGroups: Array[TimeSeriesGroup], dimensions: Dimensions, modelNames: Array[String]): Unit = {
    val session = this.connector.openSession()

    //Gaps are encoded using 64 bits integers so groups cannot consist of more than 64 time series
    if (timeSeriesGroups.nonEmpty && timeSeriesGroups.map(tsg => tsg.size()).max > 64) {
      throw new IllegalArgumentException("ModelarDB: CassandraSparkStorage groups must be less than 64 time series")
    }

    //Inserts the metadata for the sources defined in the configuration file (Sid, Scaling, Resolution, Gid, Dimensions)
    val sourceDimensions = dimensions.getColumns.length
    val columns = if (sourceDimensions == 0) "" else dimensions.getColumns.mkString(", ", ", ", "")
    val placeholders = "?, " * (sourceDimensions + 3) + "?"
    val insertString = s"INSERT INTO ${this.keyspace}.source(sid, scaling, resolution, gid $columns) VALUES($placeholders)"
    for (tsg <- timeSeriesGroups) {
      for (ts <- tsg.getTimeSeries) {
        val metadata = new util.HashMap[String, Object]()
        metadata.put("sid", BigInteger.valueOf(ts.sid.toLong))
        metadata.put("scaling", ts.getScalingFactor.asInstanceOf[Object])
        metadata.put("resolution", BigInteger.valueOf(ts.resolution))
        metadata.put("gid", BigInteger.valueOf(tsg.gid))

        for (dim <- dimensions.getColumns.zip(dimensions.get(ts.source))) {
          metadata.put(dim._1, dim._2)
        }
        session.execute(insertString, metadata)
      }
    }

    //Extracts all metadata for the sources in storage
    var stmt = new SimpleStatement(s"SELECT * FROM ${this.keyspace}.source")
    var results = session.execute(stmt)
    val sourcesInStorage = new util.HashMap[Integer, Array[Object]]()
    var rows = results.iterator()
    while (rows.hasNext) {
      //The metadata is stored as (Sid => Scaling, Resolution, Gid, Dimensions)
      val row = rows.next
      val sid = row.getVarint(0).intValueExact()
      val metadata = new util.ArrayList[Object]()
      metadata.add(row.getFloat("scaling").asInstanceOf[Object])
      metadata.add(row.getVarint("resolution").intValueExact().asInstanceOf[Object])
      metadata.add(row.getVarint("gid").intValueExact().asInstanceOf[Object])

      //Dimensions
      for (column <- dimensions.getColumns) {
        metadata.add(row.getObject(column))
      }
      sourcesInStorage.put(sid, metadata.toArray)
    }

    //Extracts the name of all models in storage
    stmt = new SimpleStatement(s"SELECT * FROM ${this.keyspace}.model")
    results = session.execute(stmt)
    val modelsInStorage = new util.HashMap[String, Integer]()

    rows = results.iterator()
    while (rows.hasNext) {
      val row = rows.next
      val value = row.getVarint(0).intValueExact()
      modelsInStorage.put(row.getString(1), value)
    }

    //Initializes the caches managed by Storage
    val modelsToInsert = super.initializeCaches(modelNames, dimensions, modelsInStorage, sourcesInStorage)

    //Inserts the name of each model in the configuration file but not in the model table
    val insertStmt = session.prepare(s"INSERT INTO ${this.keyspace}.model(mid, name) VALUES(?, ?)")
    for ((k, v) <- modelsToInsert.asScala) {
      session.execute(
        insertStmt
          .bind()
          .setVarint(0, BigInteger.valueOf(v.toLong))
          .setString(1, k))
    }
    session.close()

    //Stores the current max sid for later as it is assumed to not be increased outside ModelarDB
    this.currentMaxSID = getMaxSID
  }

  override def close(): Unit = {
    //NOTE: the Cassandra connector need not be closed explicitly as it is managed by the Spark Session
  }

  override def insert(segments: Array[SegmentGroup], size: Int): Unit = {
    val session = this.connector.openSession()

    val batch = new BatchStatement()
    batch.setIdempotent(true)
    for (segment <- segments.take(size)) {
      val gmdc = this.groupMetadataCache(segment.gid)
      val resolution = gmdc(0)
      val gaps = Static.gapsToBits(segment.offsets, gmdc)
      val size = BigInteger.valueOf((segment.endTime - segment.startTime) / resolution)
      val mid = BigInteger.valueOf(segment.mid.toLong)

      val boundStatement = insertStmt.bind()
        .setVarint(0, BigInteger.valueOf(segment.gid))
        .setVarint(1, BigInteger.valueOf(gaps))
        .setVarint(2, size)
        .setTimestamp(3, new Timestamp(segment.endTime))
        .setVarint(4, mid)
        .setBytes(5, ByteBuffer.wrap(segment.parameters))
      batch.add(boundStatement)

      //The maximum batch size supported by Cassandra
      if (batch.size() == 65535) {
        session.execute(batch)
        batch.clear()
      }
    }
    session.execute(batch)
    session.close()
  }

  override def getSegments: Stream[SegmentGroup] = {
    val results = this.connector.openSession().execute(s"SELECT * FROM ${this.keyspace}.segment")
    val gmdc = this.groupMetadataCache

    java.util.stream.StreamSupport.stream(results.spliterator(), false).map(
      new java.util.function.Function[com.datastax.driver.core.Row, SegmentGroup] {
        override def apply(row: com.datastax.driver.core.Row): SegmentGroup = {
          val gid = row.getVarint("gid").intValue()
          val gaps = row.getVarint("gaps").longValue()
          val size: Long = row.getVarint("size").longValue()
          val endTime = row.getTimestamp("end_time").getTime
          val mid = row.getVarint("mid").intValue()
          val params = row.getBytes("parameters")

          //Reconstructs the gaps array from the bit flag
          val gapsArray = Static.bitsToGaps(gaps, gmdc(gid))

          //Reconstructs the start time from the end time and length
          val startTime = endTime - (size * gmdc(gid)(0))
          new SegmentGroup(gid, startTime, endTime, mid, params.array, gapsArray)
        }
      })
  }

  override def open(ssb: SparkSession.Builder, dimensions: Dimensions): SparkSession = {
    val (host, user, pass) = parseConnectionString(connectionString)
    this.sparkSession = ssb
      .config("spark.cassandra.connection.host", host)
      .config("spark.cassandra.auth.username", user)
      .config("spark.cassandra.auth.password", pass)
      .getOrCreate()
    this.connector = CassandraConnector(this.sparkSession.sparkContext)
    createTables(dimensions)
    this.sparkSession
  }

  override def writeRDD(rdd: RDD[Row]): Unit = {
    val gmdc = this.groupMetadataCache
    rdd.map(row => {
      val gid = row.getInt(0)
      val gaps = Static.gapsToBits(row.getAs[Array[Byte]](5), gmdc(gid))
      val ts = row.getTimestamp(1).getTime
      val te = row.getTimestamp(2).getTime
      val res = gmdc(gid)(0)
      val size = (te - ts) / res

      (gid, gaps, size, te, row.getInt(3), row.getAs[Array[Byte]](4))

    }).saveToCassandra(this.keyspace, "segment", SomeColumns("gid", "gaps", "size", "end_time", "mid", "parameters"))
  }

  override def getRDD(filters: Array[Filter]): RDD[Row] = {
    //The function mapping from Cassandra to Spark rows must be stored in a local variable to not serialize the object
    val rowsToRows = getRowsToRows
    val rdd = this.sparkSession.sparkContext.cassandraTable(this.keyspace, "segment")

    //Constructs a CQL WHERE clause and the maximum start time Apache Spark should read rows until
    constructPredicate(filters) match {
      case (null, null, null) =>
        rdd.map(rowsToRows)
      case (null, maxStartTime, null) =>
        takeWhile(rdd, rowsToRows, maxStartTime)
      case (predicate, null, null) =>
        rdd.where(predicate).map(rowsToRows)
      case (predicate, maxStartTime, null) =>
        takeWhile(rdd.where(predicate), rowsToRows, maxStartTime)
      case (null, null, gids) =>
        val rdds = gids.map(gid => rdd.where("gid = ?", gid))
        this.sparkSession.sparkContext.union(rdds).map(rowsToRows)
    }
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
      val params = elems(1).split('&')
      for (param <- params) {
        val na = param.split('=')
        parsed.put(na(0), na(1))
      }
    }
    this.keyspace = parsed.getOrDefault("keyspace", "modelardb")
    (elems(0), parsed.getOrDefault("username", "cassandra"), parsed.getOrDefault("password", "cassandra"))
  }

  private def createTables(dimensions: Dimensions): Unit = {
    val session = this.connector.openSession()
    var createTable: SimpleStatement = null
    createTable = new SimpleStatement(s"CREATE KEYSPACE IF NOT EXISTS ${this.keyspace} WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
    session.execute(createTable)

    createTable = new SimpleStatement(s"CREATE TABLE IF NOT EXISTS ${this.keyspace}.segment(gid VARINT, gaps VARINT, size VARINT, end_time TIMESTAMP, mid VARINT, parameters BLOB, PRIMARY KEY (gid, end_time, gaps));")
    session.execute(createTable)

    createTable = new SimpleStatement(s"CREATE TABLE IF NOT EXISTS ${this.keyspace}.model(mid VARINT, name TEXT, PRIMARY KEY (mid));")
    session.execute(createTable)

    createTable = new SimpleStatement(s"CREATE TABLE IF NOT EXISTS ${this.keyspace}.source(sid VARINT, scaling FLOAT, resolution VARINT, gid VARINT ${dimensions.getSchema}, PRIMARY KEY (sid));")
    session.execute(createTable)

    //The insert statement will be used for every batch of segments
    this.insertStmt = session.prepare(s"INSERT INTO ${this.keyspace}.segment(gid, gaps, size, end_time, mid, parameters) VALUES(?, ?, ?, ?, ?, ?)")
    session.close()
  }

  private def constructPredicate(filters: Array[Filter]): (String, Timestamp, Array[Int]) = {
    val predicates: scala.collection.mutable.ArrayBuffer[String] = scala.collection.mutable.ArrayBuffer()
    var minStartTime: Timestamp = new Timestamp(Long.MaxValue)
    val gid = scala.collection.mutable.Set[Int]()
    val gidPushDownLimit = 1500

    //All filters should be parsed as a set of conjunctions as Spark SQL represents OR as a separate case class
    //NOTE: the segments retrieved must be sorted by end_time as Spark fetches segments until a maximum start_time
    for (filter: Filter <- filters) {
      filter match {
        //Predicate push-down for gid using SELECT * FROM segment with GID = ? and gid IN (..)
        case sources.EqualTo("gid", value: Int) => predicates.append(s"gid = $value")
        case sources.EqualNullSafe("gid", value: Int) => predicates.append(s"gid = $value")
        case sources.GreaterThan("gid", value: Int) if this.currentMaxSID - value + 1 <= gidPushDownLimit => gid ++= value + 1 to this.currentMaxSID
        case sources.GreaterThanOrEqual("gid", value: Int) if this.currentMaxSID - value <= gidPushDownLimit => gid ++= value to this.currentMaxSID
        case sources.LessThan("gid", value: Int) if value - 1 <= gidPushDownLimit => gid ++= 1 to value - 1
        case sources.LessThanOrEqual("gid", value: Int) if value <= gidPushDownLimit => gid ++= 1 to value
        case sources.In("gid", values: Array[Any]) if values.length <= gidPushDownLimit => gid ++= values.map(_.asInstanceOf[Int])

        //Predicate push-down for "start_time" with rows ingested by Apache Spark until the requested start_time
        case sources.LessThan("st", value: Timestamp) => if (value.before(minStartTime)) minStartTime = value; null
        case sources.LessThanOrEqual("st", value: Timestamp) => if (value.before(minStartTime)) minStartTime = value; null

        //Predicate push-down for end_time using SELECT * FROM segment WHERE et <=> ?
        case sources.GreaterThan("et", value: Timestamp) => predicates.append(s"end_time > '$value'")
        case sources.GreaterThanOrEqual("et", value: Timestamp) => predicates.append(s"end_time >= '$value'")
        case sources.LessThan("et", value: Timestamp) => predicates.append(s"end_time < '$value'")
        case sources.LessThanOrEqual("et", value: Timestamp) => predicates.append(s"end_time <= '$value'")
        case sources.EqualTo("et", value: Timestamp) => predicates.append(s"end_time = '$value'")

        //If a predicate is not supported when using Apache Cassandra for storage all we can do is inform the user
        case p => Static.warn("ModelarDB: unsupported predicate for CassandraSparkStorage " + p, 120); null
      }
    }

    //The full predicate have been constructed and the latest start_time have been extracted
    val pr = if (predicates.isEmpty) null else predicates.mkString(" AND ")
    val tr = if (minStartTime.getTime == Long.MaxValue) null else minStartTime
    val gr = if (gid.isEmpty) null else gid.toArray
    Static.info(s"ModelarDB: constructed predicates ($pr, takeWhile(st <= $tr, Gid IN ${gid.mkString(", ")})", 120)
    (pr, tr, gr)
  }

  private def takeWhile(rdd: CassandraTableScanRDD[CassandraRow],
                        rowsToRows: CassandraRow => Row,
                        maxStartTime: Timestamp): RDD[Row] = {

    //For large data sets a scan is more efficient
    if ( ! Spark.isDataSetSmall(rdd)) {
      return rdd.map(rowsToRows)
    }
    Static.info("ModelarDB: limiting segments read using takeWhile")

    //Read segments until the requested start time is reached for all time series
    val gmdc = this.groupMetadataCache
    val maxStartTimeRaw = maxStartTime.getTime
    rdd
      .spanBy(_.getInt(0))
      .flatMap(pair => {
        //Dynamic splitting creates multiple segment that match the predicate but with different gaps
        var sids = Static.gapsToBits(Static.intToBytes(Array(gmdc(pair._1).drop(1):_*)), gmdc(pair._1))

        //Segments are retrieved until a segment with a timestamp after maxStartTime is observed for all sids
        pair._2.takeWhile((row: CassandraRow) => {
          //Reconstructs the start time of the time series
          val gid = row.getInt("gid")
          val size = row.getVarInt("size").toLong
          val endTime = row.getLong("end_time")
          val startTime = endTime - (size * gmdc(gid)(0))

          if (startTime > maxStartTimeRaw) {
            val gaps = row.getVarInt("gaps").longValue()
            if (gaps == 0) {
              //Zero means that the segment contains data from all time series in the group
              sids = 0
            } else {
              //A one bit means that this segment does not contain data for the corresponding time series in the group
              sids &= ~gaps
            }
          }
          sids != 0
        }).map(rowsToRows)
      })
  }

  private def getRowsToRows: CassandraRow => Row = {
    val gmdc = this.groupMetadataCache

    //Converts the Cassandra rows to Spark rows and reconstructs start_time from length
    //Schema: Int, java.sql.Timestamp, java.sql.Timestamp, Int, Int, Array[Byte], Array[Byte]
    row => {
      val gid = row.getInt("gid")
      val gaps = row.getVarInt("gaps")
      val size: Long = row.getInt("size")
      val endTime = row.getLong("end_time")
      val mid = row.getInt("mid")
      val params = row.getBytes("parameters")

      //Reconstructs the gaps array from the bit flag
      val gapsArray = Static.bitsToGaps(gaps.longValue(), gmdc(gid))

      //Retrieves the resolution from the metadata cache so start_time can be reconstructed
      val startTime = endTime - (size * gmdc(gid)(0))
      Row(gid, new Timestamp(startTime), new Timestamp(endTime), mid, params.array(), gapsArray)
    }
  }

  private def getMaxID(query: String): Int = {
    val rows = this.connector.openSession().execute(query)

    //Extracts the maximum id manually as Cassandra does not like aggregate queries
    var maxID = 0
    val it = rows.iterator()
    while (it.hasNext) {
      val currentID = it.next.getVarint(0).intValueExact()
      if (currentID > maxID) {
        maxID = currentID
      }
    }
    maxID
  }

  /** Instance Variables **/
  private var keyspace: String = _
  private var currentMaxSID = 0
  private var connector: CassandraConnector = _
  private var insertStmt: PreparedStatement = _
  private var sparkSession: SparkSession = _
}
