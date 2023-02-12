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
import dk.aau.modelardb.core.{Configuration, SegmentGroup}
import dk.aau.modelardb.engines.h2.H2Storage

import org.apache.arrow.flight.{Action, ActionType, Criteria, FlightDescriptor, FlightInfo, FlightProducer, FlightStream, PutResult, Result, Ticket}
import org.apache.arrow.vector.{BigIntVector, Float4Vector, Float8Vector, IntVector, VarBinaryVector, VarCharVector, VectorSchemaRoot}

import java.net.InetAddress
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.mutable

class RemoteStorageFlightProducer(configuration: Configuration, h2Storage: H2Storage, port: Int) extends FlightProducer {

  /** Public Methods **/
  override def getStream(context: FlightProducer.CallContext, ticket: Ticket, listener: FlightProducer.ServerStreamListener): Unit = ???

  override def listFlights(context: FlightProducer.CallContext, criteria: Criteria, listener: FlightProducer.StreamListener[FlightInfo]): Unit = ???

  override def getFlightInfo(context: FlightProducer.CallContext, descriptor: FlightDescriptor): FlightInfo = ???

  override def acceptPut(context: FlightProducer.CallContext, flightStream: FlightStream, ackStream: FlightProducer.StreamListener[PutResult]): Runnable = {
    () => {
      val vsr = flightStream.getRoot
      val schema = vsr.getSchema
      schema.getCustomMetadata.get("name") match {
        case "segment" =>
          flightStream.getDescriptor
          while (flightStream.next()) {
            val rowCount = vsr.getRowCount
            val sgs = (0 until rowCount).map(row => toSegmentGroup(row, vsr)).toArray
            h2Storage.storeSegmentGroups(sgs) //Also used for Apache Spark for simplicity despite the overhead of sgs
          }
          vsr.close()
          flightStream.close()
        case "time_series" =>
          //Updates the time series table and caches on the remote instance with the local time series
          val rowsBuilder = mutable.ArrayBuffer[Array[Object]]()
          val columns = flightStream.getSchema.getFields.size()
          while (flightStream.next()) {
            for (row <- Range(0, vsr.getRowCount)) {
              rowsBuilder.append(toTimeSeriesRow(row, columns, vsr))
            }
          }
          vsr.close()
          flightStream.close()
          //HACK: it is assumed that new instances are rarely added so storeMetadataAndInitializeCaches is used
          h2Storage.storeMetadataAndInitializeCaches(configuration, rowsBuilder.toArray)
      }
    }
  }

  override def doAction(context: FlightProducer.CallContext, action: Action, listener: FlightProducer.StreamListener[Result]): Unit = {
    action.getType match {
      case "model_types" =>
        //Returns the model types used by the remote instance so the local instance can check its configuration matches
        val requestBody = new mutable.StringBuilder()
        h2Storage.modelTypeCache.drop(1).foreach(mt => {
          //The encoding is kept simple as it is assumed that only a small number of model types are used
          requestBody.append(mt.getClass.getName)
          requestBody.append(" ")
        })
        val body = requestBody.mkString.getBytes(StandardCharsets.UTF_8)
        listener.onNext(new Result(body))
        listener.onCompleted()
      case "offsets" =>
        //Returns the tid and gid that the local instance must use for its time series groups
        // so the remote instance need not change the ids of the data it receives from the client
        val bodyByteBuffer = ByteBuffer.wrap(action.getBody)
        val (tid, gid) = h2Storage.allocateRemoteIDs(bodyByteBuffer.getInt, bodyByteBuffer.getInt)
        val resultByteBuffer = ByteBuffer.allocate(8)
        resultByteBuffer.putInt(tid).putInt(gid)
        listener.onNext(new Result(resultByteBuffer.flip().array()))
        listener.onCompleted()
      case "register" =>
        //Allows distributed RemoteStorageReceivers to register with the master so it can assign clients to it
        this.remoteLock.writeLock().lock()
        val uri = new String(action.getBody, StandardCharsets.UTF_8)
        this.remoteStorageReceivers.put(uri, 0)
        listener.onCompleted()
        Static.info("ModelarDB: registered " + uri + " as an Apache Arrow Flight transfer end-point")
        this.remoteLock.writeLock().unlock()
      case "retrieve" =>
        //Returns the end-point that the client should start transmitting segments to
        this.remoteLock.writeLock().lock()
        val remote = if (this.remoteStorageReceivers.isEmpty) {
          //Only the master is available for receiving segments
          InetAddress.getLocalHost.getHostAddress + ":" + port
        } else {
          //Distributed receivers are available for receiving segments
          val expectedLoadFromClient = ByteBuffer.wrap(action.getBody).getInt
          val (uri, currentLoad) = this.remoteStorageReceivers.minBy(_._2)
          this.remoteStorageReceivers.put(uri, currentLoad + expectedLoadFromClient)
          uri
        }
        listener.onNext(new Result(remote.getBytes(StandardCharsets.UTF_8)))
        listener.onCompleted()
        Static.info("ModelarDB: assigned " + remote + " as an Apache Arrow Flight transfer end-point to a client")
        this.remoteLock.writeLock().unlock()
      case actionType => listener.onError(new IllegalArgumentException("ModelarDB: unknown action type: " + actionType))
    }
  }

  override def listActions(context: FlightProducer.CallContext, listener: FlightProducer.StreamListener[ActionType]): Unit = ???

  /** Private Methods **/
  private def toTimeSeriesRow(row: Int, columns: Int, vsr: VectorSchemaRoot): Array[Object] = {
    val rowBuilder = mutable.ArrayBuffer[Object]()
    for (column <- Range(0, columns)) {
      rowBuilder.append(vsr.getVector(column) match {
        case fvi: IntVector => fvi.get(row).asInstanceOf[Object]
        case fvbi: BigIntVector => fvbi.get(row).asInstanceOf[Object]
        case fv4f: Float4Vector => fv4f.get(row).asInstanceOf[Object]
        case fv8f: Float8Vector => fv8f.get(row).asInstanceOf[Object]
        case fvcv: VarCharVector => new String(fvcv.get(row), StandardCharsets.UTF_8)
      })
    }
    rowBuilder.toArray
  }

  private def toSegmentGroup(row: Int, vsr: VectorSchemaRoot): SegmentGroup = {
    val gid = vsr.getVector("GID").asInstanceOf[IntVector].get(row)
    val start = vsr.getVector("START_TIME").asInstanceOf[BigIntVector].get(row)
    val end = vsr.getVector("END_TIME").asInstanceOf[BigIntVector].get(row)
    val mtid = vsr.getVector("MTID").asInstanceOf[IntVector].get(row)
    val model = vsr.getVector("MODEL").asInstanceOf[VarBinaryVector].get(row)
    val gaps = vsr.getVector("OFFSETS").asInstanceOf[VarBinaryVector].get(row)
    new SegmentGroup(gid, start, end, mtid, model, gaps)
  }

  /** Instance Variables **/  //Tracks the currently operating RemoteStorageReceivers and their workload
  private val remoteLock = new ReentrantReadWriteLock()
  private val remoteStorageReceivers = mutable.HashMap[String, Int]()
}