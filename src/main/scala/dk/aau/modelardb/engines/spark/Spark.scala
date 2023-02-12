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
package dk.aau.modelardb.engines.spark

import dk.aau.modelardb.core._
import dk.aau.modelardb.core.utility.{Static, ValueFunction}
import dk.aau.modelardb.engines.h2.H2Storage
import dk.aau.modelardb.engines.{EngineUtilities, QueryEngine}
import dk.aau.modelardb.remote.{ArrowResultSet, QueryInterface, RemoteStorageFlightProducer, RemoteUtilities}

import org.apache.arrow.flight.{FlightServer, Location}

import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, DataFrameReader, Row, SparkSession, sources}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.net.InetAddress
import java.sql.Timestamp

import scala.collection.mutable
import scala.collection.JavaConverters._

class Spark(configuration: Configuration, sparkStorage: SparkStorage) extends QueryEngine {

  /** Public Methods **/
  def start(): Unit = {
    //Creates the Spark Session, Spark Streaming Context, and initializes the companion object
    val (ss, ssc) = initialize()
    this.sparkSession = ss

    //Starts listening for and executes queries using the user-configured interface
    QueryInterface.start(configuration, this)

    //Ensures that Spark does not terminate until ingestion is safely stopped
    if (ssc != null) {
      Static.info("ModelarDB: awaiting termination")
      ssc.stop(false, true)
      ssc.awaitTermination()
      Spark.getCache.flush()
    }
    if (this.flightServer != null) { // null unless modelardb.transfer server
      this.flightServer.close()
    }
    ss.stop()
  }

  override def listTables(): Array[String] = {
    Spark.sparkSession.catalog.listTables().collect().map(_.name.toUpperCase)
  }

  override def executeToJSON(query: String): Array[String] = {
    this.sparkSession.sql(query).toJSON.collect()
  }

  override def executeToArrow(query: String): ArrowResultSet = {
    new SparkResultSet(this.sparkSession.sql(query), this.rootAllocator)
  }

  /** Private Methods **/
  private def initialize(): (SparkSession, StreamingContext) = {
    //Constructs the necessary Spark Conf and Spark Session Builder
    val engine = configuration.getString("modelardb.engine")
    val master = if (engine == "spark") "local[*]" else engine
    val conf = new SparkConf()
      .set("spark.streaming.unpersist", "false")
      .set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
    val ssb = SparkSession.builder.master(master).config(conf)

    //Checks if the Storage instance provided has native Apache Spark integration
    val dimensions = configuration.getDimensions
    val spark = sparkStorage.open(ssb, dimensions)

    //Initializes storage and Spark with any new time series that the system must ingest
    configuration.containsOrThrow("modelardb.batch_size")
    val ingestors = configuration.getIngestors
    val ssc = if (ingestors == 0) {
      if ( ! configuration.getDerivedTimeSeries.isEmpty) { //Initializes derived time series
        Partitioner.initializeTimeSeries(configuration, sparkStorage.getMaxTid)
      }
      sparkStorage.storeMetadataAndInitializeCaches(configuration, Array[TimeSeriesGroup]())
      Spark.initialize(spark, configuration, sparkStorage, Range(0,0))
      null
    } else {
      configuration.containsOrThrow("modelardb.spark.streaming")
      val transfer = configuration.getString("modelardb.transfer", "None")
      val (mode, port) = RemoteUtilities.getInterfaceAndPort(transfer, 10000)

      mode match {
        case "server" =>
          //Initialize Data Transfer Server (Master)
          sparkStorage.storeMetadataAndInitializeCaches(configuration, Array[TimeSeriesGroup]())
          val location = new Location("grpc://0.0.0.0:" + port)
          val producer = new RemoteStorageFlightProducer(configuration, sparkStorage.asInstanceOf[H2Storage], port)
          val executor = configuration.getExecutorService
          this.flightServer = FlightServer.builder(this.rootAllocator, location, producer).executor(executor).build()
          this.flightServer.start()
          Static.info(f"ModelarDB: Arrow Flight transfer end-point is ready (Port: $port)")

          //Initialize Data Transfer Server (Workers)
          Spark.initialize(spark, configuration, sparkStorage, Range(0, 0)) //Temporary segments are not transferred
          val master = InetAddress.getLocalHost().getHostAddress() + ":" + port
          setupStream(spark, Range(0, ingestors).map(_=> new RemoteStorageReceiver(master, port)).toArray, true)
        case _ => //If data transfer is enabled to StorageFactory have wrapped SparkStorage with a RemoteStorage instance
          //Initialize Local Ingestion to SparkStorage (Possible RemoteStorage)
          val firstGid = sparkStorage.getMaxGid + 1
          val timeSeries = Partitioner.initializeTimeSeries(configuration, sparkStorage.getMaxTid)
          val timeSeriesGroups = Partitioner.groupTimeSeries(configuration, timeSeries, sparkStorage.getMaxGid)
          sparkStorage.storeMetadataAndInitializeCaches(configuration, timeSeriesGroups)
          Spark.initialize(spark, configuration, sparkStorage, Range(firstGid, firstGid + timeSeriesGroups.size))

          val mtidCache = Spark.getSparkStorage.mtidCache.asJava
          val workingSets = Partitioner.partitionTimeSeries(configuration, timeSeriesGroups, mtidCache, ingestors)
          if (workingSets.length != ingestors) {
            throw new java.lang.RuntimeException("ModelarDB: the Spark engine did not receive a workings sets for each receiver")
          }
          setupStream(spark, workingSets.map(ws => new WorkingSetReceiver(ws)), false)
      }
    }

    //Creates Spark SQL tables that can be queried using SQL
    val vp = Spark.getViewProvider
    val segmentView = vp.option("type", "Segment").load()
    segmentView.createOrReplaceTempView("Segment")
    val dataPointView = vp.option("type", "DataPoint").load()
    dataPointView.createOrReplaceTempView("DataPoint")
    SparkUDAF.initialize(spark)
    EngineUtilities.initialize(dimensions)

    configuration.get("modelardb.spark.external").foreach(obj => {
      val nameFormatAndOptions = obj.asInstanceOf[Array[String]]
      val options = nameFormatAndOptions.drop(2).sliding(2).map(kva => (kva(0), kva(1))).toMap
      spark.read.format(nameFormatAndOptions(1)).options(options).load().createOrReplaceTempView(nameFormatAndOptions(0))
    })

    //Last, return the Spark interfaces so they can be controlled
    (spark, ssc)
  }

  private def setupStream(spark: SparkSession, receivers: Array[Receiver[Row]], dataTransferServer: Boolean): StreamingContext = {
    //Creates a receiver per ingestor with each receiving a working set or clients to ingest from
    val ssc = new StreamingContext(spark.sparkContext, Seconds(configuration.getInteger("modelardb.spark.streaming")))
    val streams = receivers.map(ssc.receiverStream(_))
    val stream = ssc.union(streams.toSeq)


    if ( ! configuration.contains("modelardb.interface")) {
      //If querying is disabled, segments can be written directly to disk without being cached
      stream.foreachRDD(Spark.getCache.write(_))
      Static.info("ModelarDB: Spark Streaming initialized in bulk-loading mode")
    } else if (dataTransferServer) {
      //Temporary segments are never transferred so segments can be written directly to disk if the cache is invalidated
      stream.foreachRDD(rdd => {
        val cache = Spark.getCache
        if ( ! rdd.isEmpty()) {
          cache.invalidate()
        }
        cache.write(rdd)
      })
      Static.info("ModelarDB: Spark Streaming initialized in flushing bulk-loading mode")
    } else {
      stream.foreachRDD(Spark.getCache.update(_))
      Static.info("ModelarDB: Spark Streaming initialized in online-analytics mode")
    }

    //The streaming context is started and the context is returned
    ssc.start()
    ssc
  }

  /** Instance Variable **/
  private var sparkSession: SparkSession = _
  private val rootAllocator = RemoteUtilities.getRootAllocator(this.configuration)
  private var flightServer: FlightServer = _
}

object Spark {

  /** Constructors **/
  def initialize(spark: SparkSession, configuration: Configuration, sparkStorage: SparkStorage, newGids: Range): Unit = {
    this.sparkSession = spark
    this.parallelism = spark.sparkContext.defaultParallelism
    this.viewProvider = spark.read.format("dk.aau.modelardb.engines.spark.ViewProvider")
    this.timeSeriesTransformationCache = sparkStorage.timeSeriesTransformationCache
    this.broadcastedTimeSeriesTransformationCache = spark.sparkContext.broadcast(sparkStorage.timeSeriesTransformationCache)
    this.sparkStorage = sparkStorage
    this.cache = new SparkCache(spark, configuration.getInteger("modelardb.batch_size"), newGids)
  }

  /** Public Methods **/
  def getCache: SparkCache = Spark.cache
  def getViewProvider: DataFrameReader = Spark.viewProvider
  def getSparkStorage: SparkStorage = Spark.sparkStorage
  def getBroadcastedTimeSeriesTransformationCache: Broadcast[Array[ValueFunction]] = {
    if (this.timeSeriesTransformationCache != this.sparkStorage.timeSeriesTransformationCache) {
      //The TimeSeriesTransformationCache was updated due to a new client registering
      this.broadcastedTimeSeriesTransformationCache =
        this.sparkSession.sparkContext.broadcast(this.sparkStorage.timeSeriesTransformationCache)
      this.timeSeriesTransformationCache = this.sparkStorage.timeSeriesTransformationCache
    }
    this.broadcastedTimeSeriesTransformationCache
  }
  def getStorageSegmentGroupsSchema: StructType = this.storageSegmentGroupsSchema
  def isDataSetSmall(rows: RDD[_]): Boolean = rows.partitions.length <= parallelism

  def applyFiltersToDataFrame(df: DataFrame, filters: Array[Filter]): DataFrame = {
    //All filters must be parsed as a set of conjunctions as Apache Spark SQL represents OR as a separate case class
    val predicates = mutable.ArrayBuffer[String]()
    for (filter: Filter <- filters) {
      filter match {
        //Predicate push-down for gid using SELECT * FROM segment with GID = ? and gid IN (..)
        case sources.EqualTo("gid", value: Int) => predicates.append(s"gid = $value")
        case sources.EqualNullSafe("gid", value: Int) => predicates.append(s"gid = $value")
        case sources.In("gid", values: Array[Any]) => values.map(_.asInstanceOf[Int]).mkString("GID IN (", ",", ")")

        //Predicate push-down for start_time using SELECT * FROM segment WHERE start_time <=> ?
        case sources.GreaterThan("start_time", value: Timestamp) => predicates.append(s"start_time > '$value'")
        case sources.GreaterThanOrEqual("start_time", value: Timestamp) => predicates.append(s"start_time >= '$value'")
        case sources.LessThan("start_time", value: Timestamp) => predicates.append(s"start_time < '$value'")
        case sources.LessThanOrEqual("start_time", value: Timestamp) => predicates.append(s"start_time <= '$value'")
        case sources.EqualTo("start_time", value: Timestamp) => predicates.append(s"start_time = '$value'")

        //Predicate push-down for end_time using SELECT * FROM segment WHERE end_time <=> ?
        case sources.GreaterThan("end_time", value: Timestamp) => predicates.append(s"end_time > '$value'")
        case sources.GreaterThanOrEqual("end_time", value: Timestamp) => predicates.append(s"end_time >= '$value'")
        case sources.LessThan("end_time", value: Timestamp) => predicates.append(s"end_time < '$value'")
        case sources.LessThanOrEqual("end_time", value: Timestamp) => predicates.append(s"end_time <= '$value'")
        case sources.EqualTo("end_time", value: Timestamp) => predicates.append(s"end_time = '$value'")

        //If a predicate is not supported the information is simply logged to inform the user
        case p => Static.warn("ModelarDB: predicate push-down is not supported for " + p, 120)
      }
    }
    val predicate = predicates.mkString(" AND ")
    Static.info(s"ModelarDB: constructed predicates ($predicate)", 120)
    if (predicate.isEmpty) {
      df
    } else {
      df.where(predicate)
    }
  }

  /** Instance Variables **/
  private var sparkSession: SparkSession = _
  private var parallelism: Int = _
  private var cache: SparkCache = _
  private var viewProvider: DataFrameReader = _
  private var sparkStorage: SparkStorage = _
  private var timeSeriesTransformationCache: Array[ValueFunction] = _
  private var broadcastedTimeSeriesTransformationCache: Broadcast[Array[ValueFunction]] = _
  private val storageSegmentGroupsSchema: StructType = StructType(Seq(
    StructField("gid", IntegerType, nullable = false),
    StructField("start_time", TimestampType, nullable = false),
    StructField("end_time", TimestampType, nullable = false),
    StructField("mtid", IntegerType, nullable = false),
    StructField("model", BinaryType, nullable = false),
    StructField("gaps", BinaryType, nullable = false)))
}
