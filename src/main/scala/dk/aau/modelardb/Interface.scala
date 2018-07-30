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
package dk.aau.modelardb

import java.nio.file.{Files, Paths}

import scala.io.Source

object Interface {

  /** Public Methods **/
  def start(interface: String, sql: String => Array[String]): Unit = {
    this.sql = sql
    interface match {
      case "none" =>
      case "socket" => socket()
      case path if Files.exists(Paths.get(path)) => file(path)
      //NOTE: The version below below of file below exist exclusively to benchmark ingestion with automated queries
      //case path if Files.exists(Paths.get(path)) => benchFile(path)
      case _ => throw new java.lang.UnsupportedOperationException("unknown value for modelardb.interface in the config file")
    }
  }

  /** Private Methods **/
  private def socket(): Unit = {
    //Setup
    println("ModelarDB: socketSQL is ready for connection (Port: 9999)")
    val serverSocket = new java.net.ServerSocket(9999)
    val clientSocket = serverSocket.accept()
    val in = new java.io.BufferedReader(new java.io.InputStreamReader(clientSocket.getInputStream))
    val out = new java.io.PrintWriter(clientSocket.getOutputStream, true)

    //Query
    println("ModelarDB: client socket is ready for query")
    while (true) {
      val query = in.readLine()
      if (query.isEmpty) {
        return
      }
      execute(query, out.write)
      out.flush()
    }

    //Cleanup
    in.close()
    out.close()
    clientSocket.close()
    serverSocket.close()
  }

  private def file(path: String): Unit = {
    //This method is only called if the file exist
    val st = System.currentTimeMillis()
    println("ModelarDB: executing queries from " + path)
    val lines = Source.fromFile(path).getLines()

    for (line: String <- lines) {
      val q = line.trim
      if ( ! (q.isEmpty || q.startsWith("--"))) {
        execute(q.stripMargin.toString, print)
      }
    }
    val et = System.currentTimeMillis() - st
    val jst = java.time.Duration.ofMillis(et)
    println("ModelarDB: finished all queries after " + jst)
  }

  private def benchFile(path: String): Unit = {
    val rand = scala.util.Random
    val maxSid = 2500

    //Ingest all queries into a list
    val queries = new java.util.ArrayList[String]()
    val lines = Source.fromFile(path).getLines()
    for (line: String <- lines) {
      val q = line.trim
      if ( ! (q.isEmpty || q.startsWith("--"))) {
        queries.add(q.stripMargin.toString)
      }
    }
    val queriesCount = queries.size()

    //This method is only called if the file exist
    val st = System.currentTimeMillis()
    println("ModelarDB: continuously executing queries from " + path)
    while(true) {
      val ri = rand.nextInt(queriesCount)
      val sid = rand.nextInt(maxSid)
      val query = queries.get(ri)
      execute(query.replace("{}", sid.toString), print)
    }

    val et = System.currentTimeMillis() - st
    val jst = java.time.Duration.ofMillis(et)
    println("ModelarDB: ingestion finished after: " + jst)
  }

  private def execute(query: String, out: String => Unit): Unit = {
    //Time and process query
    val st = System.currentTimeMillis()
    var result: Array[String] = null
    try {
      result = this.sql(query)
    } catch {
      case e: Exception =>
        result = Array(e.toString)
    }
    val et = System.currentTimeMillis() - st
    val jst = java.time.Duration.ofMillis(et)

    //Outputs the result using what ever method is passed as the arguments out
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