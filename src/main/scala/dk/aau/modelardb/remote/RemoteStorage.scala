/* Copyright 2022 The ModelarDB Contributors
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
package dk.aau.modelardb.remote

import dk.aau.modelardb.core.utility.Static
import dk.aau.modelardb.core.{Configuration, Dimensions, SegmentGroup, TimeSeriesGroup}
import dk.aau.modelardb.engines.h2.H2Storage
import dk.aau.modelardb.engines.spark.SparkStorage
import dk.aau.modelardb.storage.Storage

import org.apache.arrow.flight.{Action, FlightClient, FlightDescriptor, Location, SyncPutListener}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.{BigIntVector, FloatingPointVector, IntVector, VarBinaryVector, VarCharVector, VectorSchemaRoot}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.sources.Filter
import org.h2.table.TableFilter

import java.util
import java.net.InetAddress
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import scala.collection.mutable

class RemoteStorage(ip: String, port: Int, storage: Storage, configuration: Configuration) extends H2Storage with SparkStorage {

  /** Public Methods **/
  //Storage
  override def open(dimensions: Dimensions): Unit = {
    this.storage.open(dimensions)
  }

  override def storeMetadataAndInitializeCaches(configuration: Configuration, timeSeriesGroups: Array[TimeSeriesGroup]): mutable.HashMap[Integer, Array[AnyRef]] = {
    if ( ! configuration.getDerivedTimeSeries.isEmpty) {
      throw new IllegalArgumentException("ModelarDB: a data transfer client currently cannot have derived time series")
    }

    //Update the time series and groups ids to match remote
    var (nextTid, nextGid) = this.getIDsFromRemote(timeSeriesGroups)
    for (tsg <- timeSeriesGroups) {
      this.expectedLoadOnRemote += Static.dataPointsPerMinute(tsg.samplingInterval, tsg.size())
      for (ts <- tsg.getTimeSeries) {
        //The time series in a group must be sorted by tid, otherwise, optimizations in SegmentGenerator fail
        ts.tid = nextTid
        nextTid += 1
      }
      tsg.gid = nextGid
      nextGid += 1
    }

    //Update local metadata and caches
    val timeSeriesInStorage = this.storage.storeMetadataAndInitializeCaches(configuration, timeSeriesGroups)

    //Retrieve the Apache Arrow Flight end-point to transfer segment groups to
    this.segmentFlightClient = this.getEndPointFromRemote

    //Ensures that model types locally and remotely match
    this.assertModelTypes()

    //Copy references to the read-only caches from this.storage to this
    this.dimensions = this.storage.dimensions
    this.mtidCache = this.storage.mtidCache
    this.modelTypeCache = this.storage.modelTypeCache
    this.timeSeriesGroupCache = this.storage.timeSeriesGroupCache
    this.timeSeriesSamplingIntervalCache = this.storage.timeSeriesSamplingIntervalCache
    this.timeSeriesScalingFactorCache = this.storage.timeSeriesScalingFactorCache
    this.timeSeriesTransformationCache = this.storage.timeSeriesTransformationCache
    this.timeSeriesMembersCache = this.storage.timeSeriesMembersCache
    this.memberTimeSeriesCache = this.storage.memberTimeSeriesCache
    this.groupMetadataCache = this.storage.groupMetadataCache
    this.groupDerivedCache = this.storage.groupDerivedCache

    //Define schema for the time series table
    val dimensions = configuration.getDimensions()
    val timeSeriesSchema = {
      val fields = new util.ArrayList[Field]()
      val children = new util.ArrayList[Field]()
      fields.add(new Field("TID", new FieldType(false, new ArrowType.Int(32, true), null), children))
      fields.add(new Field("SCALING_FACTOR", new FieldType(false, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), null), children))
      fields.add(new Field("SAMPLING_INTERVAL", new FieldType(false, new ArrowType.Int(32, true), null), children))
      fields.add(new Field("GID", new FieldType(false, new ArrowType.Int(32, true), null), children))
      for (nameAndType <- dimensions.getColumns.zip(dimensions.getTypes)) {
        nameAndType match {
          case (name, Dimensions.Types.TEXT) => fields.add(new Field(name, new FieldType(false, new ArrowType.Utf8(), null), children))
          case (name, Dimensions.Types.INT) => fields.add(new Field(name, new FieldType(false, new ArrowType.Int(32, true), null), children))
          case (name, Dimensions.Types.LONG) => fields.add(new Field(name, new FieldType(false, new ArrowType.Int(64, true), null), children))
          case (name, Dimensions.Types.FLOAT) => fields.add(new Field(name, new FieldType(false, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), null), children))
          case (name, Dimensions.Types.DOUBLE) => fields.add(new Field(name, new FieldType(false, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), null), children))
        }
      }
      val metadata = new util.HashMap[String, String]()
      metadata.put("name", "time_series")
      new Schema(fields, metadata)
    }

    //Package information about all the time series
    var index = 0
    val vsr = VectorSchemaRoot.create(timeSeriesSchema, this.rootAllocator)
    vsr.setRowCount(timeSeriesInStorage.size) //Size is set before values are added to preallocate memory
    val streamListener = this.masterFlightClient.startPut(this.flightDescriptor, vsr, new SyncPutListener())
    for ((tid, metadata) <- timeSeriesInStorage) { //The vectors must be written from zero to row count as string otherwise are overwritten
      //Add static columns to data frame
      vsr.getVector("TID").asInstanceOf[IntVector].setSafe(index, tid)
      vsr.getVector("SCALING_FACTOR").asInstanceOf[FloatingPointVector].setSafeWithPossibleTruncate(index, metadata(0).asInstanceOf[Float])
      vsr.getVector("SAMPLING_INTERVAL").asInstanceOf[IntVector].setSafe(index, metadata(1).asInstanceOf[Integer])
      vsr.getVector("GID").asInstanceOf[IntVector].setSafe(index, metadata(2).asInstanceOf[Integer])

      //Add dimensions to data frame
      var column = 3 //Start of members
      for (nameAndType <- dimensions.getColumns.zip(dimensions.getTypes)) {
        nameAndType match {
          case (name, Dimensions.Types.INT) =>
            vsr.getVector(name).asInstanceOf[IntVector].setSafe(index, metadata(column).asInstanceOf[Integer])
          case (name, Dimensions.Types.LONG) =>
            vsr.getVector(name).asInstanceOf[BigIntVector].setSafe(index, metadata(column).asInstanceOf[Long])
          case (name, Dimensions.Types.FLOAT) => vsr.getVector(name).asInstanceOf[FloatingPointVector]
            .setSafeWithPossibleTruncate(index, metadata(column).asInstanceOf[Float])
          case (name, Dimensions.Types.DOUBLE) => vsr.getVector(name).asInstanceOf[FloatingPointVector]
            .setSafeWithPossibleTruncate(index, metadata(column).asInstanceOf[Double])
          case (name, Dimensions.Types.TEXT) =>
            vsr.getVector(name).asInstanceOf[VarCharVector].setSafe(index,
              metadata(column).asInstanceOf[String].getBytes(StandardCharsets.UTF_8))
        }
        column += 1
      }
      index += 1
    }

    //Update remote metadata and caches
    streamListener.putNext()
    while ( ! streamListener.isReady) {}
    streamListener.completed()
    streamListener.getResult()
    vsr.close()
    timeSeriesInStorage
  }

  override def getMaxTid: Int = this.storage.getMaxTid
  override def getMaxGid: Int = this.storage.getMaxGid

  def close(): Unit = {
    this.masterFlightClient.close()
    this.segmentFlightClient.close()
    this.storage.close()
  }

  //H2Storage
  override def storeSegmentGroups(segmentGroups: Array[SegmentGroup]): Unit = {
    //Transfers the segment groups to the storage on a remote instance
    val vsr = VectorSchemaRoot.create(this.segmentGroupSchema, this.rootAllocator)
    val streamListener = this.segmentFlightClient.startPut(this.flightDescriptor, vsr, new SyncPutListener())
    vsr.setRowCount(segmentGroups.length) //Size is set before values are added to preallocate memory
    segmentGroups.zipWithIndex.foreach({ case (sg, index) => this.addSegmentGroupToRoot(index, sg, vsr) })
    streamListener.putNext()
    while ( ! streamListener.isReady) {}
    streamListener.completed()
    streamListener.getResult()
    vsr.close()
  }

  override def getSegmentGroups(filter: TableFilter): Iterator[SegmentGroup] = ???

  //SparkStorage
  override def open(ssb: SparkSession.Builder, dimensions: Dimensions): SparkSession = {
    this.storage.asInstanceOf[SparkStorage].open(ssb, dimensions)
  }

  override def storeSegmentGroups(sparkSession: SparkSession, df: DataFrame): Unit = ???
  override def getSegmentGroups(sparkSession: SparkSession, filters: Array[Filter]): DataFrame = ???

  /** Protected Methods **/
  override protected def storeTimeSeries(timeSeriesGroupRows: Array[Array[AnyRef]]): Unit = ???
  override protected def getTimeSeries: mutable.HashMap[Integer, Array[AnyRef]] = ???
  override protected def storeModelTypes(modelsToInsert: mutable.HashMap[String, Integer]): Unit = ???
  override protected def getModelTypes: mutable.HashMap[String, Integer] = ???

  /** Private Methods **/
  private def getIDsFromRemote(timeSeriesGroups: Array[TimeSeriesGroup]): (Int, Int) = {
    val requestBodyBuffer = ByteBuffer.allocate(8)
    val timeSeriesCount = timeSeriesGroups.map(tsg => tsg.getTimeSeries.length).sum
    requestBodyBuffer.putInt(timeSeriesCount).putInt(timeSeriesGroups.length)
    val body = requestBodyBuffer.flip().array()
    val action = new Action("offsets", body)
    val results = this.masterFlightClient.doAction(action)
    if (results.hasNext) {
      val resultByteBuffer = ByteBuffer.wrap(results.next().getBody)
      (resultByteBuffer.getInt(), resultByteBuffer.getInt())
    } else {
      throw new Exception("ModelarDB: unable to obtain the offsets from the server")
    }
  }

  private def getEndPointFromRemote: FlightClient = {
    val requestBodyBuffer = ByteBuffer.allocate(4)
    requestBodyBuffer.putInt(this.expectedLoadOnRemote)
    val body = requestBodyBuffer.flip().array()
    val action = new Action("retrieve", body)
    val results = this.masterFlightClient.doAction(action)
    val remote = if (results.hasNext) {
      new String(results.next().getBody, StandardCharsets.UTF_8)
    } else {
      throw new Exception("ModelarDB: unable to obtain the data transfer end-point from the server")
    }

    //Create a connection to the remote instance to transfer segment groups to
    FlightClient.builder().location(new Location("grpc://" + remote)).allocator(this.rootAllocator).build()
  }

  private def assertModelTypes(): Unit = {
    val action = new Action("model_types")
    val results = this.masterFlightClient.doAction(action)
    if (results.hasNext) {
      //The encoding is kept simple as it is assumed that only a small number of model types are used
      val local_model_types = storage.modelTypeCache.drop(1).map(_.getClass.getName)
      val remote_model_types = new String(results.next().getBody, StandardCharsets.UTF_8).trim.split(' ')
      if (local_model_types.length > remote_model_types.length) {
        throw new IllegalArgumentException(
          f"ModelarDB: local (${storage.modelTypeCache.length}) is configured to use more model types then remote (${remote_model_types.length })")
      }

      //Validates all mtids and model types match but assumes the same version is used
      local_model_types.zipWithIndex.foreach(lmti => {
        val lmtid = lmti._2
        val lmtn = lmti._1
        val rmtn = remote_model_types(lmtid)
        if (lmtn != rmtn) {
          val rmtid = remote_model_types.indexOf(lmtn)
          val rmtids = if (rmtid == -1) "None" else (rmtid + 1).toString
          throw new IllegalArgumentException(
            f"ModelarDB: local (${lmtid + 1}) and remote ($rmtids) is configured to use different mtids for $lmtn")
        }
      })
    } else {
      throw new Exception("ModelarDB: unable to check the model types used by the server")
    }
  }

  private def addSegmentGroupToRoot(index: Int, sg: SegmentGroup, vsr: VectorSchemaRoot): Unit = {
    vsr.getVector("GID").asInstanceOf[IntVector].setSafe(index, sg.gid)
    vsr.getVector("START_TIME").asInstanceOf[BigIntVector].setSafe(index, sg.startTime)
    vsr.getVector("END_TIME").asInstanceOf[BigIntVector].setSafe(index, sg.endTime)
    vsr.getVector("MTID").asInstanceOf[IntVector].setSafe(index, sg.mtid)
    vsr.getVector("MODEL").asInstanceOf[VarBinaryVector].setSafe(index, sg.model)
    vsr.getVector("OFFSETS").asInstanceOf[VarBinaryVector].setSafe(index, sg.offsets)
  }

  /** Instance Variables **/
  private val rootAllocator: BufferAllocator = RemoteUtilities.getRootAllocator(configuration)
  private val flightDescriptor = FlightDescriptor.path(InetAddress.getLocalHost().getHostName())
  private val masterFlightClient = {
    val location = new Location("grpc://" + ip + ":" + port)
    FlightClient.builder().location(location).allocator(this.rootAllocator).build()
  }
  private val segmentGroupSchema = {
    val fields = new util.ArrayList[Field]();
    val children = new util.ArrayList[Field]();
    fields.add(new Field("GID", new FieldType(false, new ArrowType.Int(32, true), null), children))
    fields.add(new Field("START_TIME", new FieldType(false, new ArrowType.Int(64, true), null), children))
    fields.add(new Field("END_TIME", new FieldType(false, new ArrowType.Int(64, true), null), children))
    fields.add(new Field("MTID", new FieldType(false, new ArrowType.Int(32, true), null), children))
    fields.add(new Field("MODEL", new FieldType(false, new ArrowType.Binary(), null), children))
    fields.add(new Field("OFFSETS", new FieldType(false, new ArrowType.Binary(), null), children))
    val metadata = new util.HashMap[String, String]()
    metadata.put("name", "segment")
    new Schema(fields, metadata)
  }
  private var expectedLoadOnRemote: Int = _
  private var segmentFlightClient: FlightClient = _
}