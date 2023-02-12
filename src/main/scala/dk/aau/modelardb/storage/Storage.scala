/* Copyright 2021 The ModelarDB Contributors
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

import dk.aau.modelardb.core.{Configuration, Dimensions, TimeSeriesGroup}
import dk.aau.modelardb.core.models.ModelType
import dk.aau.modelardb.core.models.ModelTypeFactory
import dk.aau.modelardb.core.utility.{Pair, Static, ValueFunction}

import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.math.Ordering.Implicits.infixOrderingOps

abstract class Storage {

  /** Public Methods * */
  def open(dimensions: Dimensions): Unit
  def getMaxTid: Int
  def getMaxGid: Int
  def close(): Unit

  def allocateRemoteIDs(timeSeriesCount: Int, timeSeriesGroupCount: Int): (Int, Int) = {
    this.storageMetadataLock.writeLock().lock()
    //The stored ids are only used once to initialize this.tid and this.gid to prevent a race-condition where
    // client A requests ids, client B requests ids, and then client A transmits information about its time series
    if (this.tid == -1) {
      this.tid = this.getMaxTid
      this.tid += 1
    }
    val tid = this.tid
    this.tid += timeSeriesCount

    if (this.gid == -1) {
      this.gid = this.getMaxGid
      this.gid += 1
    }
    val gid = this.gid
    this.gid += timeSeriesGroupCount
    this.storageMetadataLock.writeLock().unlock()

    Static.info(f"ModelarDB: assigned Tid $tid-${tid + timeSeriesCount - 1} and Gid $gid-${gid + timeSeriesGroupCount - 1} to client")
    (tid, gid)
  }

  def storeMetadataAndInitializeCaches(configuration: Configuration, timeSeriesGroups: Array[TimeSeriesGroup]): mutable.HashMap[Integer, Array[AnyRef]] = {
    //The Dimensions object is stored so the schema can be retrieved later
    this.dimensions = configuration.getDimensions

    //Inserts the metadata for the sources defined in the configuration file (Tid, Scaling Factor,
    // Sampling Interval, Gid, Members) into the persistent storage defined by modelardb.storage
    val timeSeriesGroupRows = mutable.ArrayBuffer[Array[AnyRef]]()
    for (tsg <- timeSeriesGroups) {
      for (ts <- tsg.getTimeSeries) {
        val row = new ArrayBuffer[AnyRef]()
        row.append(ts.tid.asInstanceOf[AnyRef], ts.scalingFactor.asInstanceOf[AnyRef],
          ts.samplingInterval.asInstanceOf[AnyRef], tsg.gid.asInstanceOf[AnyRef])
        row.appendAll(this.dimensions.get(ts.source))
        timeSeriesGroupRows.append(row.toArray)
      }
    }
    this.storeTimeSeries(timeSeriesGroupRows.toArray)

    //Inserts all model types that are not currently stored per definition the mtid of the fallback model type is one
    val modelTypesToBeInserted = mutable.HashMap[String, Integer]()
    val modelsWithFallback = mutable.ArrayBuffer[String](configuration.getModelTypeNames: _*)
    modelsWithFallback.prepend("dk.aau.modelardb.core.models.UncompressedModelType")
    val modelTypesInStorage = this.getModelTypes
    var mtid: Integer = modelTypesInStorage.values.reduceOption(_ max _).getOrElse(0)

    for (model <- modelsWithFallback) {
      if ( ! modelTypesInStorage.contains(model)) {
        mtid += 1
        modelTypesInStorage.put(model, mtid)
        modelTypesToBeInserted.put(model, mtid)
      }
    }
    this.storeModelTypes(modelTypesToBeInserted)
    initializeCaches(configuration)

    //The time series is returned so RemoteStorage can transfer them without exposing getTimeSeries
    this.getTimeSeries
  }

  def storeMetadataAndInitializeCaches(configuration: Configuration, timeSeriesGroupRows: Array[Array[AnyRef]]): Unit = {
    this.storageMetadataLock.writeLock().lock()
    this.storeTimeSeries(timeSeriesGroupRows)
    this.initializeCaches(configuration)
    this.storageMetadataLock.writeLock().unlock()
  }

  /** Protected Methods * */
  protected def storeTimeSeries(timeSeriesGroupRows: Array[Array[AnyRef]]): Unit
  protected def getTimeSeries: mutable.HashMap[Integer, Array[AnyRef]]
  protected def storeModelTypes(modelsToInsert: mutable.HashMap[String, Integer]): Unit
  protected def getModelTypes: mutable.HashMap[String, Integer]

  protected def getDimensionsSQL(dimensions: Dimensions, textType: String): String = {
    val columns = dimensions.getColumns
    val types = dimensions.getTypes
    if (types.isEmpty) {
      return ""
    }

    //The schema is build with a starting comma so it is easy to embed into a CREATE table statement
    val sb = new StringBuilder
    sb.append(", ")
    val withPunctuation = columns.length - 1
    for (i <- 0 until withPunctuation) {
      sb.append(columns(i))
      sb.append(' ')
      if (types(i) eq Dimensions.Types.TEXT) sb.append(textType)
      else sb.append(types(i).toString)
      sb.append(", ")
    }
    sb.append(columns(withPunctuation))
    sb.append(' ')
    if (types(withPunctuation) eq Dimensions.Types.TEXT) sb.append(textType)
    else sb.append(types(withPunctuation).toString)
    sb.toString
  }

  /** Private Methods **/
  private def initializeCaches(configuration: Configuration): Unit = {

    //Verifies that the implementation of all model types are loaded and creates the modelTypeCache and mtidCache
    val modelTypesInStorage = this.getModelTypes
    val modelTypeCache = Array.fill[ModelType](modelTypesInStorage.size + 1)(null)
    val mtidCache = mutable.HashMap[String, Integer]()

    for ((modelTypeName, mtid) <- modelTypesInStorage) {
      val modelType = ModelTypeFactory.getModel(modelTypeName, 0, 0.0F, 0)
      modelTypeCache(mtid) = modelType
      mtidCache.put(modelType.getClass.getName, mtid)
    }

    //Creates the timeSeriesGroupCache, timeSeriesScalingFactorCache, and timeSeriesMembersCache
    var nextTid = getMaxTid + 1
    val derivedTimeSeries = configuration.getDerivedTimeSeries
    val totalNumberOfSources = nextTid + derivedTimeSeries.values.stream.mapToInt((v: Array[Pair[String, ValueFunction]]) => v.length).sum
    val timeSeriesGroupCache = Array.fill[Int](totalNumberOfSources)(0)
    val timeSeriesSamplingIntervalCache = Array.fill[Int](totalNumberOfSources)(0)
    val timeSeriesScalingFactorCache = Array.fill(totalNumberOfSources)(0.0F)
    val timeSeriesMembersCache = Array.fill[Array[AnyRef]](totalNumberOfSources)(null)
    val gsc = mutable.HashMap[Integer, mutable.ArrayBuffer[Integer]]()
    val scalingTransformation = new ValueFunction()
    val timeSeriesTransformationCache = Array.fill[ValueFunction](totalNumberOfSources)(null)
    val groupDerivedCacheBuilder = mutable.HashMap[Integer, mutable.ArrayBuffer[Integer]]()
    val timeSeriesInStorage = this.getTimeSeries
    for ((tid, metadata) <- timeSeriesInStorage) {
      //Metadata is a mapping from Tid to Scaling, Sampling Interval, Gid, and Dimensions
      //Creates mappings from tid -> gid, tid -> sampling interval, tid -> scaling factor, and tid -> dimensions
      val gid = metadata(2).asInstanceOf[Int]
      timeSeriesGroupCache(tid) = gid
      timeSeriesSamplingIntervalCache(tid) = metadata(1).asInstanceOf[Int]
      timeSeriesScalingFactorCache(tid) = metadata(0).asInstanceOf[Float]
      if ( ! gsc.contains(gid)) {
        //A group consist of time series with equivalent SI
        val metadataArray = mutable.ArrayBuffer[Integer]()
        metadataArray.append(metadata(1).asInstanceOf[Int])
        gsc.put(gid, metadataArray)
      }
      gsc(gid).append(tid)

      var dim = 0
      val columns = Array.fill[AnyRef](metadata.length - 3)(null)
      for (i <- 3 until metadata.length) {
        columns(dim) = metadata(i)
        dim += 1
      }
      timeSeriesMembersCache(tid) = columns
      timeSeriesTransformationCache(tid) = scalingTransformation

      //Creates mappings from gid -> pair of tids for original and derived (gdc), and from tid -> to transformation (tc)
      if (derivedTimeSeries.containsKey(tid)) {
        //All derived time series perform all of their transformations in their user-defined function
        val sourcesAndTransformations = derivedTimeSeries.get(tid)
        val gdcb: mutable.ArrayBuffer[Integer] = groupDerivedCacheBuilder.getOrElse(gid, mutable.ArrayBuffer[Integer]())
        for (sat <- sourcesAndTransformations) {
          val dtid = { nextTid += 1; nextTid - 1 }  //nextTid++
          timeSeriesGroupCache(dtid) = gid
          timeSeriesSamplingIntervalCache(dtid) = metadata(1).asInstanceOf[Int]
          timeSeriesScalingFactorCache(dtid) = 1.0F //HACK: scaling is assumed to be part of the transformation
          timeSeriesMembersCache(dtid) = dimensions.get(sat._1)
          timeSeriesTransformationCache(dtid) = sat._2
          gdcb.append(tid)
          gdcb.append(dtid)
        }
      }
    }
    val groupDerivedCache = mutable.HashMap[Integer, Array[Int]]()
    groupDerivedCacheBuilder.foreach(kv => groupDerivedCache.put(kv._1, kv._2.map(i => i.intValue()).toArray))

    //The inverseDimensionsCache is constructed from the dimensions cache
    val columns = this.dimensions.getColumns
    val outer = new mutable.HashMap[String, mutable.HashMap[AnyRef, mutable.HashSet[Integer]]]
    for (i <- 1 until timeSeriesMembersCache.length) {
      //If segments are transferred to a remote instance missing tids can occur and must be handled
      if (timeSeriesMembersCache(i) != null) {
        for (j <- 0 until columns.length) {
          val value = timeSeriesMembersCache(i)(j)
          val inner = outer.getOrElse(columns(j), mutable.HashMap[AnyRef, mutable.HashSet[Integer]]())
          val tids = inner.getOrElse(value, mutable.HashSet[Integer]())
          tids.add(timeSeriesGroupCache(i))
          inner.put(value, tids)
          outer.put(columns(j), inner)
        }
      }
    }
    val memberTimeSeriesCache = mutable.HashMap[String, mutable.HashMap[AnyRef, Array[Integer]]]()
    for (oes <- outer) {
      val innerAsArray = mutable.HashMap[AnyRef, Array[Integer]]()
      for (ies <- oes._2) {
        innerAsArray.put(ies._1, ies._2.toArray.sorted) //Sorted to make it simpler to read when debugging
      }
      //Some engines converts all columns to uppercase so the caches key must also be so
      memberTimeSeriesCache.put(oes._1.toUpperCase, innerAsArray)
    }

    //Finally the sorted groupMetadataCache is created and consists of sampling interval and tids
    val groupMetadataCache = new Array[Array[Int]](getMaxGid + 1)
    gsc.foreach(kv => {
      groupMetadataCache(kv._1) = kv._2.map((i: Integer) => i.intValue()).toArray
      java.util.Arrays.sort(groupMetadataCache(kv._1), 1, groupMetadataCache(kv._1).length)
    })

    //Updates the reference to all volatile caches so threads never see a half-constructed cache
    this.modelTypeCache = modelTypeCache
    this.mtidCache = mtidCache
    this.mtidCache = mtidCache
    this.timeSeriesGroupCache = timeSeriesGroupCache
    this.timeSeriesSamplingIntervalCache = timeSeriesSamplingIntervalCache
    this.timeSeriesScalingFactorCache = timeSeriesScalingFactorCache
    this.timeSeriesMembersCache = timeSeriesMembersCache
    this.timeSeriesTransformationCache = timeSeriesTransformationCache
    this.groupDerivedCache = groupDerivedCache
    this.memberTimeSeriesCache = memberTimeSeriesCache
    this.groupMetadataCache = groupMetadataCache
  }

  /** Instance Variables * */
  private var tid = -1
  private var gid = -1
  var dimensions: Dimensions = _

  protected val storageSegmentGroupLock = new ReentrantReadWriteLock()
  protected val storageMetadataLock = new ReentrantReadWriteLock()


  //Write Cache: Maps the name of a model type to the corresponding mtid used by the storage layer
  @volatile var mtidCache: mutable.HashMap[String, Integer] = _

  //Read Cache: Maps the mtid of a model type to an instance of the model type so segments can be constructed from it
  @volatile var modelTypeCache: Array[ModelType] = _


  //Read Cache: Maps the tid of a time series to the gid of the group that the time series is a member of
  @volatile var timeSeriesGroupCache: Array[Int] = _

  //Read Cache: Maps the tid of a time series to the sampling interval specified for that time series
  @volatile var timeSeriesSamplingIntervalCache: Array[Int] = _

  //Read Cache: Maps the tid of a time series to the scaling factor specified for for that time series
  @volatile var timeSeriesScalingFactorCache: Array[Float] = _

  //Read Cache: Maps the tid of a time series to the transformation specified for that time series
  @volatile var timeSeriesTransformationCache: Array[ValueFunction] = _

  //Read Cache: Maps the tid of a time series to the members specified for that time series
  @volatile var timeSeriesMembersCache: Array[Array[AnyRef]] = _


  //Read Cache: Maps the value of a column for a dimension to the tids with that member
  @volatile var memberTimeSeriesCache: mutable.HashMap[String, mutable.HashMap[AnyRef, Array[Integer]]] = _


  //Read Cache: Maps the gid of a group to the groups sampling interval and the tids that are part of that group
  @volatile var groupMetadataCache: Array[Array[Int]] = _

  //Read Cache: Maps the gid of a group to pairs of tids for time series with derived time series
  @volatile var groupDerivedCache: mutable.HashMap[Integer, Array[Int]] = _
}