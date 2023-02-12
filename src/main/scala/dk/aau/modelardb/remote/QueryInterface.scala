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
package dk.aau.modelardb.remote

import dk.aau.modelardb.core.Configuration
import dk.aau.modelardb.engines.QueryEngine
import dk.aau.modelardb.core.utility.Static

import org.apache.arrow.flight.{FlightServer, Location}
import org.apache.arrow.memory.BufferAllocator
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}

import java.nio.charset.StandardCharsets
import java.util.concurrent.ExecutorService
import java.nio.file.{Files, Paths}

import scala.io.StdIn.readLine
import scala.io.Source

object QueryInterface {

  /** Public Methods * */
  def start(configuration: Configuration, queryEngine: QueryEngine): Unit = {
    if ( ! configuration.contains("modelardb.interface")) {
      return
    }

    this.queryEngine = queryEngine
    val (interface, port) = RemoteUtilities.getInterfaceAndPort(
      configuration.getString("modelardb.interface"), 9999)
    interface match {
      case "arrow" => arrow(configuration.getExecutorService, port, RemoteUtilities.getRootAllocator(configuration))
      case "socket" => socket(configuration.getExecutorService, port)
      case "http" => http(configuration.getExecutorService, port)
      case "repl" => repl(configuration.getStorage)
      case path if Files.exists(Paths.get(path)) => file(path)
      case _ => throw new java.lang.UnsupportedOperationException("unknown value for modelardb.interface in the config file")
    }
  }

  /** Private Methods * */
  private def arrow(executor: ExecutorService, port: Int, rootAllocator: BufferAllocator): Unit = {
    val location = new Location("grpc://0.0.0.0:" + port)
    val producer = new QueryInterfaceFlightProducer(this.queryEngine)
    val flightServer = FlightServer.builder(rootAllocator, location, producer).executor(executor).build()
    flightServer.start()
    Static.info(f"ModelarDB: Arrow Flight query end-point is ready (Port: $port)")
    readLine() //Prevents the method from returning to keep the Arrow Flight query end-point running
    flightServer.close()
    Static.info("ModelarDB: connection is closed")
  }

  private def socket(executor: ExecutorService, port: Int): Unit = {
    //Setup
    val serverSocket = new java.net.ServerSocket(port)

    while (true) {
      Static.info(f"ModelarDB: socket query end-point is ready (Port: $port)")
      val clientSocket = serverSocket.accept()
      executor.execute(() => {
        val in = new java.io.BufferedReader(new java.io.InputStreamReader(clientSocket.getInputStream))
        val out = new java.io.PrintWriter(clientSocket.getOutputStream, true)

        //Query
        Static.info("ModelarDB: connection is ready")

        try {
          var stop = true
          while (stop) {
            val query = in.readLine().trim()
            if ( ! query.startsWith("--") && query.contains("SELECT")) {
              execute(query, out.write)
              out.flush()
            } else if (query.nonEmpty) {
              in.close()
              out.close()
              clientSocket.close()
              stop = false //The empty string terminates the connection
              Static.info("ModelarDB: connection is closed")
            } else {
              out.write("only SELECT is supported")
              out.flush()
            }
          }
        } catch {
          case _: NullPointerException =>
          //Thrown if the client closes in while ModelarDB waits for input
        }
      })
    }

    //Cleanup
    serverSocket.close()
  }

  private def http(executor: ExecutorService, port: Int): Unit = {
    //Setup
    val server = HttpServer.create(new java.net.InetSocketAddress(port), 0)

    //Query
    class QueryHandler extends HttpHandler {
      override def handle(httpExchange: HttpExchange): Unit = {
        val request = httpExchange.getRequestBody
        val reader = new java.io.BufferedReader(new java.io.InputStreamReader(request))

        //The query is executed with the result returned as an HTTP response
        val results = scala.collection.mutable.ArrayBuffer[String]()
        execute(reader.readLine.trim(), line => results.append(line))
        val out = results.mkString("")
        httpExchange.sendResponseHeaders(200, out.length)
        val response = httpExchange.getResponseBody
        response.write(out.getBytes(StandardCharsets.UTF_8))
        response.close()
      }
    }

    //Configures the HTTP server to executes QueryHandler on a separate thread for each incoming request on /
    server.createContext("/", new QueryHandler())
    server.setExecutor(executor)
    server.start()
    Static.info(f"ModelarDB: HTTP query end-point is ready (Port: $port)")
    readLine() //Prevents the method from returning to keep the HTTP server and query end-point running

    //Cleanup
    server.stop(0)
    Static.info("ModelarDB: connection is closed")
  }

  private def repl(storage: String): Unit = {
    val prompt = storage.substring(storage.lastIndexOf('/') + 1) + "> "
    do {
      print(prompt)
      execute(readLine, print)
    } while (true)
  }

  private def file(path: String): Unit = {
    //This method is only called if the file exist
    val st = System.currentTimeMillis()
    Static.info("ModelarDB: executing queries from " + path)
    val source = Source.fromFile(path)
    for (line: String <- source.getLines()) {
      val q = line.trim()
      if ( ! (q.isEmpty || q.startsWith("--"))) {
        execute(q.stripMargin, print)
      }
    }
    source.close()
    val et = System.currentTimeMillis() - st
    val jst = java.time.Duration.ofMillis(et)
    Static.info("ModelarDB: finished all queries after " + jst)
  }

  private def execute(query: String, out: String => Unit): Unit = {
    val st = System.currentTimeMillis()
    var result: Array[String] = null
    try {
      val query_rewritten = RemoteUtilities.rewriteQuery(query)
      result = this.queryEngine.executeToJSON(query_rewritten)
    } catch {
      case e: Exception =>
        Static.warn("ModelarDB: query failed due to " + e)
        result = Array(e.toString)
    }
    val et = System.currentTimeMillis() - st
    val jst = java.time.Duration.ofMillis(et)

    //Outputs the query result using the method provided as the arguments out
    out(s"""{\n  "time": "$jst",\n  "query": "$query",\n  "result":  [\n    """)
    if (result.nonEmpty) {
      var index = 0
      val end = result.length - 1
      while (index < end) {
        out(result(index))
        out(",\n    ")
        index += 1
      }
      out(result(index))
      out(s"""\n  ]\n}\n""")
    } else {
      out(s"""  ]\n}\n""")
    }
  }

  /** Instance Variables **/
  var queryEngine: QueryEngine = _
}