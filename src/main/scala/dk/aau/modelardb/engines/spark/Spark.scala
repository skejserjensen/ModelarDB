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

import dk.aau.modelardb.Interface
import dk.aau.modelardb.core._
import dk.aau.modelardb.core.utility.{Static, ValueFunction}
import dk.aau.modelardb.engines.EngineUtilities
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrameReader, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}

class Spark(configuration: Configuration, sparkStorage: SparkStorage) {

  /** Public Methods **/
  def start(): Unit = {
    //Creates the Spark Session, Spark Streaming Context, and initializes the companion object
    val (ss, ssc) = initialize()

    //Starts listening for and executes queries using the user-configured interface
    Interface.start(configuration, q => ss.sql(q).toJSON.collect())

    //Ensures that Spark does not terminate until ingestion is safely stopped
    if (ssc != null) {
      Static.info("ModelarDB: awaiting termination")
      ssc.awaitTermination()
      Spark.getCache.flush()
    }
    ss.stop()
  }

  /** Private Methods **/
  private def initialize(): (SparkSession, StreamingContext) = {

    //Constructs the necessary Spark Conf and Spark Session Builder
    val engine = configuration.getString("modelardb.engine")
    val master = if (engine == "spark") "local[*]" else engine
    val conf = new SparkConf()
      .set("spark.streaming.unpersist", "false")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
    val ssb = SparkSession.builder.master(master).config(conf)

    //Checks if the Storage instance provided has native Apache Spark integration
    val dimensions = configuration.getDimensions
    val spark = sparkStorage.open(ssb, dimensions)

    //Initializes storage and Spark with any new time series that the system must ingest
    configuration.containsOrThrow("modelardb.batch_size")
    val ssc = if (configuration.getIngestors == 0) {
      if ( ! configuration.getDerivedTimeSeries.isEmpty) { //Initializes derived time series
        Partitioner.initializeTimeSeries(configuration, sparkStorage.getMaxTid)
      }
      sparkStorage.initialize(Array(), configuration.getDerivedTimeSeries, dimensions, configuration.getModelTypeNames)
      Spark.initialize(spark, configuration, sparkStorage, Range(0,0))
      null
    } else {
      configuration.containsOrThrow("modelardb.spark.streaming")
      val newGid = sparkStorage.getMaxGid + 1
      val timeSeries = Partitioner.initializeTimeSeries(configuration, sparkStorage.getMaxTid)
      val timeSeriesGroups = Partitioner.groupTimeSeries(configuration, timeSeries, sparkStorage.getMaxGid)
      sparkStorage.initialize(timeSeriesGroups, configuration.getDerivedTimeSeries, dimensions, configuration.getModelTypeNames)
      Spark.initialize(spark, configuration, sparkStorage, Range(newGid, newGid + timeSeriesGroups.size))
      setupStream(spark, timeSeriesGroups)
    }

    //Creates Spark SQL tables that can be queried using SQL
    val vp = Spark.getViewProvider
    val segmentView = vp.option("type", "Segment").load()
    segmentView.createOrReplaceTempView("Segment")
    val dataPointView = vp.option("type", "DataPoint").load()
    dataPointView.createOrReplaceTempView("DataPoint")
    SparkUDAF.initialize(spark)
    EngineUtilities.initialize(dimensions)

    //Last, return the Spark interfaces so they can be controlled
    (spark, ssc)
  }

  private def setupStream(spark: SparkSession, timeSeriesGroups: Array[TimeSeriesGroup]): StreamingContext = {
    //Creates a receiver per ingestor with each receiving a working set created by Partitioner.partitionTimeSeries
    val ssc = new StreamingContext(spark.sparkContext, Seconds(configuration.getInteger("modelardb.spark.streaming")))
    val mtidCache = Spark.getSparkStorage.mtidCache
    val workingSets = Partitioner.partitionTimeSeries(configuration, timeSeriesGroups, mtidCache,
      configuration.getInteger("modelardb.ingestors"))
    if (workingSets.length != configuration.getInteger("modelardb.ingestors")) {
      throw new java.lang.RuntimeException("ModelarDB: the Spark engine did not receive a workings sets for each receiver")
    }

    val modelReceivers = workingSets.map(ws => new WorkingSetReceiver(ws))
    val streams = modelReceivers.map(ssc.receiverStream(_))
    val stream = ssc.union(streams.toSeq)

    //If querying and temporary segments are disabled, segments can be written directly to disk without being cached
    if ( ! configuration.contains("modelardb.interface") && ! configuration.contains("modelardb.latency")) {
      stream.foreachRDD(Spark.getCache.write(_))
      Static.info("ModelarDB: Spark Streaming initialized in bulk-loading mode")
    } else {
      stream.foreachRDD(Spark.getCache.update(_))
      Static.info("ModelarDB: Spark Streaming initialized in online-analytics mode")
    }

    //The streaming context is started and the context is returned
    ssc.start()
    ssc
  }
}

object Spark {
  /** Constructors **/
  def initialize(spark: SparkSession, configuration: Configuration, sparkStorage: SparkStorage, newGids: Range): Unit = {
    this.parallelism = spark.sparkContext.defaultParallelism
    this.relations = spark.read.format("dk.aau.modelardb.engines.spark.ViewProvider")
    this.sparkStorage = null
    this.broadcastedTimeSeriesTransformationCache = spark.sparkContext.broadcast(sparkStorage.timeSeriesTransformationCache)
    this.sparkStorage = sparkStorage
    this.cache = new SparkCache(spark, configuration.getInteger("modelardb.batch_size"), newGids)
  }

  /** Public Methods **/
  def getCache: SparkCache = Spark.cache
  def getViewProvider: DataFrameReader = Spark.relations
  def getSparkStorage: SparkStorage = Spark.sparkStorage
  def getBroadcastedTimeSeriesTransformationCache: Broadcast[Array[ValueFunction]] = Spark.broadcastedTimeSeriesTransformationCache
  def isDataSetSmall(rows: RDD[_]): Boolean = rows.partitions.length <= parallelism

  /** Instance Variables **/
  private var parallelism: Int = _
  private var cache: SparkCache = _
  private var relations: DataFrameReader = _
  private var sparkStorage: SparkStorage = _
  private var broadcastedTimeSeriesTransformationCache: Broadcast[Array[ValueFunction]] = _
}
