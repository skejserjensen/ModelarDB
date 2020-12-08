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
package dk.aau.modelardb.engines.spark

import java.sql.Timestamp
import java.util.concurrent.locks.ReentrantReadWriteLock

import dk.aau.modelardb.core.SegmentGroup
import dk.aau.modelardb.core.utility.Static
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD.intSet
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.JavaConverters._

class SparkCache(spark: SparkSession, newGids: Range, maxSegmentsCached: Int) extends Serializable {

  /** Public Methods **/
  def update(microBatch: RDD[Row]): Unit = {
    this.cacheLock.writeLock().lock()

    //Updates the cache of temporary segments (they are marked as false in column seven)
    this.temporaryRDD = this.temporaryRDD.multiputRDD(
      microBatch.map(row => (row.getInt(0), Array(row))),
      (_, r1: Array[Row], r2: Array[Row]) => updateTemporarySegment(r1, r2))

    //Flushes the ingested finalized segments to disk if the user-configurable batch size is reached
    this.finalizedRDD = spark.sparkContext.union(this.finalizedRDD, microBatch.filter(_.getBoolean(6)))
    if (this.finalizedRDD.count() >= maxSegmentsCached) {
      flush()
    }

    //All data in the ingestion cache are persisted so the linage is only traversed one more time
    this.temporaryRDD = checkpointOrPersist(this.temporaryRDD)
    this.finalizedRDD.persist()
    this.ingestedRDD.unpersist()
    this.ingestedRDD = spark.sparkContext.union(this.finalizedRDD, this.temporaryRDD.values.flatMap(rows => rows))
    this.ingestedRDD.persist()
    this.cacheLock.writeLock().unlock()
  }

  def flush(): Unit = {
    this.cacheLock.writeLock().lock()
    Static.info("ModelarDB: flushing in-memory cache")

    //The flush method must be atomic to prevent duplicate segments being read from both
    // the disk and the finalizedRDD if a query is issued while the system is ingesting
    write(this.finalizedRDD)

    //The cache is now invalid and must be cleared to ensure queries are correct
    this.finalizedRDD.unpersist()
    this.finalizedRDD = this.emptyRDD
    this.storageCacheRDD.unpersist()
    this.storageCacheKey = Array(null)

    //A ReentrantReadWriteLock cannot be upgraded, so a reader must be informed that a flush occurred
    // between it releasing its read lock and getting a write lock. A counter is used instead of a flag
    // so two writes cannot reset the flag for a reader: Unlock Read -> Writer -> Writer -> Lock Write
    this.lastFlush += 1
    this.cacheLock.writeLock().unlock()
  }

  def getSegmentGroupRDD(filters: Array[Filter]): RDD[Row] = {
    //Multiple readers must be allowed to support execution of multiple queries in parallel
    this.cacheLock.readLock().lock()

    //If the rows required from storage matches the contents of the cache, we return the cached rows
    if (filters.sameElements(this.storageCacheKey)) {
      Static.info("ModelarDB: cache hit")
      val segmentRDD = spark.sparkContext.union(this.storageCacheRDD, this.ingestedRDD)
      this.cacheLock.readLock().unlock()
      return segmentRDD
    }

    //The segment RDD cannot be constructed while update() or flush() is running
    // as some segments then might be read from both the disk and finalizedRDD
    Static.info("ModelarDB: cache miss")
    val storageRDD = getStorageRDDFromDisk(filters)
    val segmentRDD = spark.sparkContext.union(storageRDD, this.ingestedRDD)
    val lastFlush = this.lastFlush
    this.cacheLock.readLock().unlock()

    //Large data sets are not cached in memory to prevent spilling to disk, and as
    // ReentrantReadWriteLock cannot be upgraded we check if a flush have occurred
    if (Spark.isDataSetSmall(storageRDD)) {
      this.cacheLock.writeLock().lock()
      if (lastFlush == this.lastFlush) {
        Static.info("ModelarDB: caching RDD")
        this.storageCacheRDD.unpersist()
        this.storageCacheKey = filters
        this.storageCacheRDD = storageRDD
        this.storageCacheRDD.persist()
      }
      this.cacheLock.writeLock().unlock()
    }
    segmentRDD
  }

  /** Private[spark] Methods **/
  private[spark] def write(microBatch: RDD[Row]): Unit = {
    //This method is not completely private so Spark can write RDDs directly to storage when bulk-loading
    val ss = Spark.getSparkStorage
    if (ss == null) {
      val groups = microBatch.collect.map(row => new SegmentGroup(row.getInt(0), row.getTimestamp(1).getTime,
        row.getTimestamp(2).getTime, row.getInt(3), row.getAs[Array[Byte]](4), row.getAs[Array[Byte]](5)))
      Spark.getStorage.insert(groups, groups.length)
    } else {
      ss.writeRDD(microBatch)
    }
  }

  /** Private Methods **/
  private def getStorageRDDFromDisk(filters: Array[Filter]): RDD[Row] = {
    val ss = Spark.getSparkStorage
    if (ss == null) {
      val rows = Spark.getStorage.getSegments.iterator().asScala.map(sg =>
        Row(sg.gid, new Timestamp(sg.startTime), new Timestamp(sg.endTime), sg.mid, sg.parameters, sg.offsets))
      spark.sparkContext.parallelize(rows.toSeq)
    } else {
      ss.getRDD(filters)
    }
  }

  private def getIndexedRDD = {
    //The IndexedRDD is populated with empty arrays for each new gid so that the merge function is always executed
    val initialData: Array[(Int, Array[Row])] = if (newGids.isEmpty) {
      Array()
    } else {
      newGids.map(gid => (gid, Array[Row]())).toArray
    }
    val rdd = spark.sparkContext.parallelize(initialData)
    IndexedRDD(rdd)
  }

  private def checkpointOrPersist(indexedRDD: IndexedRDD[Int, Array[Row]]) = {
    if (checkpointCounter == 0) {
      //HACK: allows IndexedRDDs to be checkpointed so its linage can be cleared
      val checkpointableRDD = indexedRDD.mapPartitions(x => x)
      checkpointableRDD.localCheckpoint()
      checkpointCounter = 10
      getIndexedRDD.multiputRDD(checkpointableRDD)
    } else {
      checkpointCounter = checkpointCounter - 1
      indexedRDD.persist()
    }
  }

  private def updateTemporarySegment(buffer: Array[Row], input: Array[Row]): Array[Row] = {
    //The gaps are extracted from the new finalized or temporary segment
    val inputRow = input(0)
    val isTemporary = ! inputRow.getBoolean(6)
    val inputGaps = Static.bytesToInts(inputRow.getAs[Array[Byte]](5))

    //Extracts the metadata for the group of time series being updated
    val group = this.groupMetadataCache(inputRow.getInt(0)).drop(1)
    val resolution = this.groupMetadataCache(inputRow.getInt(0))(0)
    val inputIngested = group.toSet.diff(inputGaps.toSet)
    var updatedExistingSegment = false

    for (i <- buffer.indices) {
      //The gaps are extracted for each existing temporary row
      val row = buffer(i)
      val gap = Static.bytesToInts(row.getAs[Array[Byte]](5))
      val ingested = group.toSet.diff(gap.toSet)

      //Each existing temporary segment that contains values for the same time series as the new segment is updated
      if (ingested.intersect(inputIngested).nonEmpty) {
        if (isTemporary) {
          //A new temporary segment always represent newer data points than the previous temporary segment
          buffer(i) = inputRow
        } else {
          //Moves the start time of the temporary segment to the data point right after the finalized segment, if
          // the new start time is after the end time of the temporary segment it can be dropped from the cache
          buffer(i) = null //The current temporary segment is deleted if it overlaps completely with the finalized segment
          val startTime = inputRow.getTimestamp(2).getTime + resolution
          if (startTime <= row.getTimestamp(2).getTime) {
            val newGaps = Static.intToBytes(gap :+ -((startTime - row.getTimestamp(1).getTime) / resolution).toInt)
            buffer(i) = Row(row.getInt(0), new Timestamp(startTime), row.getTimestamp(2),
              row.getInt(3), row.getAs[Array[Byte]](4), newGaps, row.getBoolean(6))
          }
        }
        updatedExistingSegment = true
      }
    }

    if (isTemporary && ! updatedExistingSegment) {
      //A split has occurred and multiple segments now represent what one did before, so the new ones are appended
      buffer.filter(_ != null) ++ input
    } else {
      //A join have occurred and one segment now represent what two did before, so duplicates must be removed
      buffer.filter(_ != null).distinct
    }
  }

  /** Instance Variables **/
  private var checkpointCounter = 10
  private val emptyRDD = spark.sparkContext.emptyRDD[Row]
  private val groupMetadataCache = Spark.getStorage.getGroupMetadataCache
  private val cacheLock = new ReentrantReadWriteLock()
  private var lastFlush = 0

  private var storageCacheKey: Array[Filter] = Array(null)
  private var storageCacheRDD = this.emptyRDD

  private var temporaryRDD = getIndexedRDD
  private var finalizedRDD = this.emptyRDD
  private var ingestedRDD = this.emptyRDD
}
