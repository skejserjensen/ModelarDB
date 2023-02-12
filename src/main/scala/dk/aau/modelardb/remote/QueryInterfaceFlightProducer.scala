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
import dk.aau.modelardb.engines.QueryEngine

import org.apache.arrow.flight.{Action, ActionType, Criteria, FlightDescriptor, FlightEndpoint,
  FlightInfo, FlightProducer, FlightStream, Location, PutResult, Result, Ticket}
import org.apache.arrow.vector.ipc.message.IpcOption
import org.apache.arrow.vector.types.pojo.{Field, Schema}

import java.net.InetAddress
import java.nio.charset.StandardCharsets.UTF_8
import java.util

class QueryInterfaceFlightProducer(queryEngine: QueryEngine) extends FlightProducer {

  /** Public Methods **/
  override def getStream(context: FlightProducer.CallContext, ticket: Ticket, listener: FlightProducer.ServerStreamListener): Unit = {
    try {
      //Assumes that the ticket contains a SQL query
      val query = new String(ticket.getBytes, UTF_8)
      if (query.isEmpty || query.startsWith("--")) {
        listener.completed()
        return
      }

      //Executes the query and transmits the result
      val query_rewritten = RemoteUtilities.rewriteQuery(query)
      val ars = queryEngine.executeToArrow(query_rewritten)
      listener.start(ars.get(), null, this.defaultIpcOption)
      while (ars.hasNext) {
        ars.fillNext()
        listener.putNext()
        this.spinUntilReadyOrTimeout(context, listener)
      }
      listener.completed()
      ars.close()
    } catch {
      case t: Throwable =>
        Static.warn("ModelarDB: query failed due to " + t)
        listener.error(t)
    }
  }

  override def listFlights(context: FlightProducer.CallContext, criteria: Criteria, listener: FlightProducer.StreamListener[FlightInfo]): Unit = {
    try {
      val schema = new Schema(new util.ArrayList[Field]())
      val flightDescriptor = FlightDescriptor.path(queryEngine.listTables():_*)
      val ticket = new Ticket(Array())
      val ip = InetAddress.getLocalHost().getHostAddress()
      val flightEndPoint = new FlightEndpoint(ticket, new Location(ip))
      val endPoints = new util.ArrayList[FlightEndpoint]()
      endPoints.add(flightEndPoint)

      val flightInfo = new FlightInfo(schema, flightDescriptor, endPoints, -1, -1, this.defaultIpcOption)
      listener.onNext(flightInfo)
      listener.onCompleted()
    } catch {
      case t: Throwable =>
        Static.warn("ModelarDB: list flights due to " + t)
        listener.onError(t)
    }
  }

  override def getFlightInfo(context: FlightProducer.CallContext, descriptor: FlightDescriptor): FlightInfo = ???

  override def acceptPut(context: FlightProducer.CallContext, flightStream: FlightStream, ackStream: FlightProducer.StreamListener[PutResult]): Runnable = ???

  override def doAction(context: FlightProducer.CallContext, action: Action, listener: FlightProducer.StreamListener[Result]): Unit = ???

  override def listActions(context: FlightProducer.CallContext, listener: FlightProducer.StreamListener[ActionType]): Unit = ???

  /** Private Methods **/
  private def spinUntilReadyOrTimeout(context: FlightProducer.CallContext, listener: FlightProducer.ServerStreamListener): Unit = {
    val start = System.currentTimeMillis()
    while ( ! listener.isReady && ! context.isCancelled && ! listener.isCancelled &&
      System.currentTimeMillis() - start < 5000) {}

    if (context.isCancelled || listener.isCancelled) {
      throw new InterruptedException("query was canceled")
    }

    if ( ! listener.isReady) {
      throw new util.concurrent.TimeoutException("timeout before connection became ready")
    }
  }

  /** Instance Variable **/
  //Replacement for org.apache.arrow.vector.ipc.message.IpcOption.DEFAULT to not conflict with Apache Spark
  private val defaultIpcOption = new IpcOption()
}