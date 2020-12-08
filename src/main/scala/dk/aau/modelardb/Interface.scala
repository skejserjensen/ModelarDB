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
package dk.aau.modelardb

import java.nio.file.{Files, Paths}

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import dk.aau.modelardb.core.utility.Static

import scala.io.Source

object Interface {

  /** Public Methods **/
  def start(interface: String, sql: String => Array[String]): Unit = {
    this.sql = sql
    interface match {
      case "none" =>
      case "socket" => socket()
      case "http" => http()
      case path if Files.exists(Paths.get(path)) => file(path)
      case _ => throw new java.lang.UnsupportedOperationException("unknown value for modelardb.interface in the config file")
    }
  }

  /** Private Methods **/
  private def socket(): Unit = {
    //Setup
    Static.info("ModelarDB: preparing socket end-point (Port: 9999)")
    val serverSocket = new java.net.ServerSocket(9999)
    val clientSocket = serverSocket.accept()
    val in = new java.io.BufferedReader(new java.io.InputStreamReader(clientSocket.getInputStream))
    val out = new java.io.PrintWriter(clientSocket.getOutputStream, true)

    //Query
    Static.info("ModelarDB: the socket is ready to receive queries (Port: 9999)")
    while (true) {
      val query = in.readLine().trim
      if (query == ":quit") {
        Static.info("ModelarDB: received termination command, shutdown imminent")
        return
      } else if ( ! query.isEmpty) {
        execute(query, out.write)
        out.flush()
      }
    }

    //Cleanup
    in.close()
    out.close()
    clientSocket.close()
    serverSocket.close()
  }

  private def http(): Unit = {
    //Setup
    Static.info("ModelarDB: preparing HTTP end-point (Port: 9999)")
    val server = HttpServer.create(new java.net.InetSocketAddress(9999), 0)

    //Query
    class QueryHandler extends HttpHandler {
      override def handle(httpExchange: HttpExchange): Unit = {
        val request = httpExchange.getRequestBody
        val reader = new java.io.BufferedReader(new java.io.InputStreamReader(request))

        //The query is executed with the result returned as an HTTP response
        val results = scala.collection.mutable.ArrayBuffer[String]()
        execute(reader.readLine.trim, line => results.append(line))
        val out = results.mkString("\n")
        httpExchange.sendResponseHeaders(200, out.length)
        val response = httpExchange.getResponseBody
        response.write(out.getBytes)
        response.close()
      }
    }

    //Starts a HTTP server that executes QueryHandler for each incoming request on /
    server.createContext("/", new QueryHandler())
    server.start()
    Static.info("ModelarDB: the HTTP server is ready to receive queries (Port: 9999)")
    scala.io.StdIn.readLine() //Prevents the method from returning to keep the server running

    //Cleanup
    server.stop(0)
  }

  private def file(path: String): Unit = {
    //This method is only called if the file exist
    val st = System.currentTimeMillis()
    Static.info("ModelarDB: executing queries from " + path)
    val lines = Source.fromFile(path).getLines()

    for (line: String <- lines) {
      val q = line.trim
      if ( ! (q.isEmpty || q.startsWith("--"))) {
        execute(q.stripMargin, print)
      }
    }
    val et = System.currentTimeMillis() - st
    val jst = java.time.Duration.ofMillis(et)
    Static.info("ModelarDB: finished all queries after " + jst)
  }

  private def execute(query: String, out: String => Unit): Unit = {
    val st = System.currentTimeMillis()
    var result: Array[String] = null
    try {
      val query_rewritten = query.replace("#", "sid, st, et, res, mid, param, gaps")
      result = this.sql(query_rewritten)
    } catch {
      case e: Exception =>
        e.printStackTrace()
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
  var sql: String => Array[String] = _
}
