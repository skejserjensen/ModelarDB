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
package dk.aau.modelardb.engines.spark

import dk.aau.modelardb.core.{Dimensions, SegmentGroup}
import dk.aau.modelardb.engines.h2.H2Storage
import dk.aau.modelardb.remote.RemoteStorageFlightProducer

import org.apache.arrow.flight.{Action, FlightClient, FlightServer, Location}
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.spark.sql.Row
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import org.h2.table.TableFilter

import java.net.InetAddress
import java.nio.charset.StandardCharsets
import java.sql.Timestamp
import java.util.concurrent.Executors

import scala.collection.mutable

class RemoteStorageReceiver(masterIPWithPort: String, port: Int) extends Receiver[Row](StorageLevel.MEMORY_AND_DISK) {

  /** Public Methods **/
  override def onStart(): Unit = {
    //H2Storage
    this.h2storage = new H2Storage {
      override def storeSegmentGroups(segmentGroups: Array[SegmentGroup]): Unit = {
        segmentGroups.foreach(sg =>
          store(Row(sg.gid, new Timestamp(sg.startTime), new Timestamp(sg.endTime), sg.mtid, sg.model, sg.offsets, true)))
      }

      //Not needed as all received segment groups are written directly to storage
      override def getSegmentGroups(filter: TableFilter): Iterator[SegmentGroup] = ???
      override def open(dimensions: Dimensions): Unit = ???
      override def getMaxTid: Int = ???
      override def getMaxGid: Int = ???
      override def close(): Unit = ???
      override protected def storeTimeSeries(timeSeriesGroupRows: Array[Array[AnyRef]]): Unit = ???
      override protected def getTimeSeries: mutable.HashMap[Integer, Array[AnyRef]] = ???
      override protected def storeModelTypes(modelsToInsert: mutable.HashMap[String, Integer]): Unit = ???
      override protected def getModelTypes: mutable.HashMap[String, Integer] = ???
    }

    //Allocator
    this.rootAllocator = new RootAllocator() //A new RootAllocator is created as this runs on workers and not the master

    //Flight Server
    this.flightServer = {
      val location = new Location("grpc://0.0.0.0:" + port) //Configuration and port are never used
      val producer = new RemoteStorageFlightProducer(null, this.h2storage, -1)
      FlightServer.builder(this.rootAllocator, location, producer).executor(Executors.newCachedThreadPool()).build()
    }
    this.flightServer.start()

    //Register this with the master
    val location = new Location("grpc://" + masterIPWithPort)
    val flightClient = FlightClient.builder().location(location).allocator(this.rootAllocator).build()
    val receiverLocation = InetAddress.getLocalHost.getHostAddress + ":" + port
    val action = new Action("register", receiverLocation.getBytes(StandardCharsets.UTF_8))
    flightClient.doAction(action).hasNext //Ensures action is performed
    flightClient.close()
  }

  override def onStop(): Unit = {
    this.flightServer.close()
  }

  /** Instance Variables **/
  //Not val to prevent NotSerializableException
  private var rootAllocator: BufferAllocator = _
  private var flightServer: FlightServer = _
  private var h2storage: H2Storage = _
}