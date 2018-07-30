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
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.JavaConverters._
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD.intSet
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.{Row, SparkSession}

class SparkCache(spark: SparkSession, maxSegmentsCached: Int) extends Serializable {

  /** Public Methods **/
  def update(microBatch: RDD[Row]): Unit = {
    this.cacheLock.writeLock().lock()

    //Update the cache of temporary segments which are marked as false in column seven
    this.temporaryRDD = this.temporaryRDD.multiputRDD(
      microBatch.map(row => (row.getInt(0), row)),
      (_, r1: Row, r2: Row) => if (r2.getBoolean(7)) updateTemporarySegment(r1, r2) else r2)

    //Prunes the finalized segments added if no temporary segments exists for a tid, and
    // prunes any temporary segments representing no data points as they are invalidated
    this.temporaryRDD = this.temporaryRDD.filter((kv: (Int, Row)) =>
      ! kv._2.getBoolean(7) && kv._2.getTimestamp(1).getTime <= kv._2.getTimestamp(2).getTime)

    //Flush the ingested finalized segments to disk if the batch size is reached and clear the cache
    this.finalizedRDD = spark.sparkContext.union(this.finalizedRDD, microBatch.filter(_.getBoolean(7)))
    if (this.finalizedRDD.count() >= maxSegmentsCached) {
      flush()
    }

    //All data in the write-cache are persisted so the linage is only traversed once
    this.temporaryRDD = checkpointOrPersist(this.temporaryRDD)
    this.finalizedRDD.persist()
    this.ingestedRDD.unpersist()
    this.ingestedRDD = spark.sparkContext.union(finalizedRDD, temporaryRDD.values)
    this.ingestedRDD.persist()

    this.cacheLock.writeLock().unlock()
  }

  def flush(): Unit = {
    this.cacheLock.writeLock().lock()

    //The flush method must be atomic to prevent duplicate segments being read from
    // the disk and finalizedRDD when a query is issued in parallel to ingestion
    val ss = Spark.getSparkStorage
    if (ss == null) {
      val rowToSegment = Spark.getRowToSegment
      this.finalizedRDD
        .map(rowToSegment).glom
        .foreach(segments => Spark.getStorage.insert(segments, segments.length))
    } else {
      ss.writeRDD(this.finalizedRDD)
    }

    //The cache is now invalid and must be cleared to ensure queries are correct
    this.finalizedRDD.unpersist()
    this.finalizedRDD = this.emptyRDD
    this.storageCacheRDD.unpersist()
    this.storageCacheKey = Array()

    this.cacheLock.writeLock().unlock()
  }

  def getSegmentRDD(filters: Array[Filter]): RDD[Row] = {
    //If the rows required matches the contents of the cache we return
    this.cacheLock.readLock().lock()
    if (filters.nonEmpty && filters.sameElements(this.storageCacheKey)) {
      println("ModelarDB: cache hit")
      val segmentRDD = spark.sparkContext.union(this.storageCacheRDD, this.ingestedRDD)
      this.cacheLock.readLock().unlock()
      return segmentRDD
    }

    //The segment RDD cannot be constructed while the cache is being flushed,
    // as some segments might be read from both the disk and finalizedRDD
    println("ModelarDB: cache miss")
    val storageRDD = getStorageRDDFromDisk(filters)
    val segmentRDD = spark.sparkContext.union(storageRDD, this.ingestedRDD)
    this.cacheLock.readLock().unlock()

    //A data set retrieved using a filter is presumed to fit in-memory to reduce
    // latency compared to computing or estimating the size of the read data set
    if (filters.nonEmpty) {
      println("ModelarDB: caching RDD")
      this.cacheLock.writeLock().lock()
      this.storageCacheRDD.unpersist()
      this.storageCacheKey = filters
      this.storageCacheRDD = storageRDD
      this.storageCacheRDD.persist()
      this.cacheLock.writeLock().unlock()
    }
    segmentRDD
  }

  /** Private Methods **/
  private def getStorageRDDFromDisk(filters: Array[Filter]): RDD[Row] = {
    val ss = Spark.getSparkStorage
    if (ss == null) {
      val rows = Spark.getStorage.getSegments.iterator().asScala.map(Spark.getSegmentToRow)
      spark.sparkContext.parallelize(rows.toSeq)
    } else {
      ss.getRDD(filters)
    }
  }

  private def getIndexedRDD = {
    //IndexedRDD must be built from another RDD to prevent null pointer exceptions
    val initialData: Array[(Int, Row)] = Array()
    val rdd = spark.sparkContext.parallelize(initialData)
    IndexedRDD(rdd)
  }

  private def checkpointOrPersist(indexedRDD: IndexedRDD[Int, Row]) = {
    if (checkpointCounter == 0) {
      //HACK: gets around that IndexedRDD cannot be checkpointed to clear linage
      val checkpointableRDD = indexedRDD.mapPartitions(x => x)
      checkpointableRDD.localCheckpoint()
      checkpointCounter = 10
      getIndexedRDD.multiputRDD(checkpointableRDD)
    } else {
      checkpointCounter = checkpointCounter - 1
      indexedRDD.persist()
    }
  }

  private def updateTemporarySegment(r1: Row, r2: Row): Row = {
    //Verify that the temporary segments does not contain gaps as that is not supported
    if (r1.getAs[Array[Byte]](6).nonEmpty) {
      throw new RuntimeException("ModelarDB: cannot update the starting time of a temporary segment with gaps")
    }

    //Move the start time of the temporary segment to the data point after the finalized segment
    val startTime = new Timestamp(r2.getTimestamp(2).getTime + this.resolutionCache(r1.getInt(0)))
    Row(r1.getInt(0), startTime, r1.getTimestamp(2), r1.getInt(3), r1.getInt(4),
      r1.getAs[Array[Byte]](5), r1.getAs[Array[Byte]](6), r1.getBoolean(7))
  }

  /** Instance Variable **/
  private var checkpointCounter = 10
  private val emptyRDD = spark.sparkContext.emptyRDD[Row]
  private val resolutionCache = Spark.getStorage.getResolutionCache
  private val cacheLock = new ReentrantReadWriteLock()

  private var storageCacheKey: Array[Filter] = Array(null)
  private var storageCacheRDD = this.emptyRDD

  private var temporaryRDD = getIndexedRDD
  private var finalizedRDD = this.emptyRDD
  private var ingestedRDD = this.emptyRDD
}