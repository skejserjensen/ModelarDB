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
package dk.aau.modelardb.engines.spark

import java.sql.Timestamp

import dk.aau.modelardb.core.{Storage, TimeSeries}
import dk.aau.modelardb.core.models.Segment

import dk.aau.modelardb.{Interface, Main}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrameReader, Row, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}

class Spark(interface: String, engine: String, storage: Storage, models: Array[String],
            receiverCount: Int, microBatchSize: Int, maxSegmentsCached: Int) {

  /** Public Methods **/
  def start(): Unit = {
    //Setup the Spark Session, Spark Streaming Context and Companion Object
    val (ss, ssc) = init()

    //Starts listening for queries on the interface
    Interface.start(
      interface,
      q => ss.sql(q).toJSON.collect()
    )

    //Cleanup
    if (ssc != null) {
      ssc.awaitTermination()
      Spark.getCache.flush()
      ssc.stop(stopSparkContext = false, stopGracefully = true)
    }
    ss.stop()
    storage.close()
  }

  /** Private Methods **/
  private def init(): (SparkSession, StreamingContext) = {
    //Construct the necessary Spark Conf and Spark Session Builder
    val master = if (engine == "spark") "local[*]" else engine
    val conf = new SparkConf().set("spark.streaming.unpersist", "false")
    val ssb = SparkSession.builder.master(master).config(conf)

    //Checks if the storage provided have native Spark capabilities
    val spark = storage match {
      case storage: SparkStorage =>
        storage.open(ssb)
      case storage: Storage =>
        storage.open()
        ssb.getOrCreate()
    }

    //Now that the storage connection is ready we can initialize it with the new time series
    val timeSeries = Main.initTimeSeries(storage.getMaxSID)
    storage.init(timeSeries, models)

    //Setup shared references to the Spark Session and Spark Cache
    Spark.init(spark, storage, maxSegmentsCached)

    //Setup Spark Streaming
    val ssc = if (receiverCount == 0) null else setupStream(spark, timeSeries)

    //Setup Spark SQL Tables
    val vp = Spark.getViewProvider
    vp.option("type", "Segment").load().createOrReplaceTempView("Segment")
    vp.option("type", "DataPoint").load().createOrReplaceTempView("DataPoint")
    UDAF.init(spark)

    //Last, return the Spark interfaces so processing can start
    (spark, ssc)
  }

  private def setupStream(spark: SparkSession, timeSeries: Array[TimeSeries]): StreamingContext = {
    val ssc = new StreamingContext(spark.sparkContext, Seconds(microBatchSize))
    val workingSets = Main.buildWorkingSets(timeSeries, receiverCount).toArray
    if (workingSets.length != receiverCount) {
      throw new java.lang.RuntimeException("spark engine did not receive a workings sets for each receiver")
    }

    //Creates a receiverCount number of receivers with the time series distributed as per modelardb.partitionby
    val modelReceivers = workingSets.map(ws => new WorkingSetReceiver(ws, Spark.getStorage.getMidCache))
    val streams = modelReceivers.map(ssc.receiverStream(_))
    val stream = ssc.union(streams.toSeq)

    //Setup in-memory caching of temporary and finalized segments received from the input stream
    stream.foreachRDD(Spark.getCache.update(_))

    //The streaming context is started and the context is returned
    ssc.start()
    ssc
  }
}

object Spark {

  /** Constructors **/
  def init(spark: SparkSession, storage: Storage, maxSegmentsCached: Int): Unit = {
    this.storage = storage
    this.sparkStorage = null
    this.relations = spark.read.format("dk.aau.modelardb.engines.spark.ViewProvider")

    //If the storage implements the SparkStorage interface we use the interface methods
    if (storage.isInstanceOf[SparkStorage]) {
      this.sparkStorage = storage.asInstanceOf[SparkStorage]
    }
    this.cache = new SparkCache(spark, maxSegmentsCached)
  }

  /** Public Methods **/
  def getCache: SparkCache = Spark.cache
  def getStorage: Storage = Spark.storage
  def getViewProvider: DataFrameReader = Spark.relations
  def getSparkStorage: SparkStorage = Spark.sparkStorage

  //Schema expected of all rows used throughout the Apache Spark engine
  //  sid: Int, start_time: java.sql.Timestamp, end_time: java.sql.Timestamp,
  //  resolution: Int, mid: Int, parameters: Array[Byte], gaps: Array[Byte]
  def getRowToSegment: Row => Segment = {
    val mc = storage.getModelCache
    row => {
      val mid = row.getInt(4)
      val model = mc(mid)
      model.get(row.getInt(0), row.getTimestamp(1).getTime, row.getTimestamp(2).getTime,
        row.getInt(3), row.getAs[Array[Byte]](5), row.getAs[Array[Byte]](6))
    }
  }

  def getSegmentToRow: Segment => Row = {
    val mc = storage.getMidCache
    seg => {
      val name = seg.getClass.getName
      val mid = mc.get(name)
      Row(seg.sid, new Timestamp(seg.startTime), new Timestamp(seg.endTime),
        seg.resolution, mid, seg.parameters(), seg.gaps())
    }
  }

  /** Instance Variables **/
  private var cache: SparkCache = _
  private var relations: DataFrameReader = _
  private var storage: Storage = _
  private var sparkStorage: SparkStorage = _
}