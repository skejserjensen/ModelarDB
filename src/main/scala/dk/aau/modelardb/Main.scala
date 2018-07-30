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

import java.io.File
import java.nio.file.{FileSystems, Paths}

import dk.aau.modelardb.core.{Storage, TimeSeries, WorkingSet}
import dk.aau.modelardb.core.utility.Static

import scala.collection.JavaConverters._
import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

import dk.aau.modelardb.engines.EngineFactory
import dk.aau.modelardb.storage.StorageFactory

object Main {

  /** Public Methods **/
  def main(args: Array[String]): Unit = {

    //ModelarDB checks args[0] for a config and use $HOME/Programs/modelardb.conf as a fallback
    val fallback = System.getProperty("user.home") + "/Programs/modelardb.conf"
    val configPath: String = if (args.length == 1) {
      args(0)
    } else if (new java.io.File(fallback).exists) {
      fallback
    } else {
      println("usage: modelardb path/to/modelardb.conf")
      System.exit(-1)
      //HACK: Required to satisfied that the type of all branches in the match
      ""
    }

    /* Settings */
    this.settings = readConfigFile(configPath)

    this.error = settings("modelardb.error").head.toFloat
    if (this.error < 0.0 || 100.0 < this.error) {
      throw new UnsupportedOperationException("modelardb.error is a percentage written from 0.0 to 100.0")
    }

    this.latency = settings("modelardb.latency").head.toInt
    if (this.latency < 0) {
      throw new UnsupportedOperationException("modelardb.latency must be a positive number of seconds or zero to disable")
    }

    this.limit = settings("modelardb.limit").head.toInt
    if (this.limit < 0) {
      throw new UnsupportedOperationException("modelardb.limit must be a positive number of seconds or zero to disable")
    }

    //Java in general works with timestamps in millisecond so we convert the resolution
    this.resolution = (settings("modelardb.resolution").head.toDouble * 1000).toInt
    if (this.resolution <= 0) {
      throw new UnsupportedOperationException("modelardb.resolution must be a positive number of seconds")
    }

    val batchSize = settings("modelardb.batch").head.toInt
    if (batchSize <= 0) {
      throw new java.lang.UnsupportedOperationException("modelardb.batch must be a positive number of segments not-yet flushed to storage")
    }

    /* Models */
    this.models = settings("modelardb.models").toArray

    /* Storage */
    this.storage = StorageFactory.getStorage(settings("modelardb.storage").head)

    /* Engine */
    EngineFactory.startEngine(settings("modelardb.interface").head, settings("modelardb.engine").head, storage, models, batchSize, settings)

    /* Debug */
    if (settings.contains("modelardb.debug")) {
      Static.writeDebugFile(settings("modelardb.debug")(0), this.storage, initTimeSeries(0)(0), this.error)
    }

    /* Cleanup */
    this.storage.close()

    //Hack to force Spark and associated connections to exit after we are done
    Static.SIGTERM()
  }

  def buildWorkingSets(timeSeries: Array[TimeSeries], partitions: Int): Iterator[WorkingSet] = {
    val partitionBy = settings("modelardb.partitionby").head

    val pts = partitionBy match {
      case "rate" => Static.partitionTimeSeriesByRate(timeSeries, partitions)
      case "length" => Static.partitionTimeSeriesByLength(timeSeries, partitions)
      case _ => throw new UnsupportedOperationException("unknown setting \"" + partitionBy + "\" in config file")
    }

    pts.asScala.map(new WorkingSet(_, this.models, this.error, this.latency, this.limit))
  }

  def initTimeSeries(currentMaximumSID: Int): Array[TimeSeries] = {
    var cms: Int = currentMaximumSID
    val sources = settings("modelardb.source")
    val tss = ArrayBuffer[TimeSeries]()

    val separator = this.settings("modelardb.separator").head
    val header = this.settings("modelardb.header").head.toBoolean
    val timestamps = this.settings("modelardb.timestamps").head.toInt
    val dateFormat = this.settings("modelardb.dateformat").head
    val timezone = this.settings("modelardb.timezone").head
    val values = this.settings("modelardb.values").head.toInt
    val locale = this.settings("modelardb.locale").head

    //HACK: The resolution is given as one argument as each data set only contains time series with the same sampling interval
    val resolution = this.resolution

    for (source: String <- sources) {
      cms += 1
      val ts = if (source.contains(":")) {
        //The locations is an address with port
        val ipSplitPort = source.split(":")
        new TimeSeries(ipSplitPort(0), ipSplitPort(1).toInt, cms, resolution, separator, header, timestamps, dateFormat, timezone, values, locale)
      } else {
        //The locations is an csv file without a .csv suffix
        new TimeSeries(source, cms, resolution, separator, header, timestamps, dateFormat, timezone, values, locale)
      }
      tss.append(ts)
    }
    tss.toArray
  }

  /** Private Methods **/
  private def readConfigFile(configPath: String): HashMap[String, List[String]] = {
    val settings = collection.mutable.HashMap[String, List[String]]()
    val models = ArrayBuffer[String]()
    val sources = ArrayBuffer[String]()

    for (line <- Source.fromFile(configPath).getLines().filter(_.nonEmpty)) {
      //Parsing is performed naively and will terminate if the config is malformed.
      val lineSplit = line.trim().split(" ", 2)
      lineSplit(0) match {
        case "modelardb.model" => models.append(lineSplit(1))
        case "modelardb.source" => appendSources(lineSplit(1), sources)
        case "modelardb.engine" | "modelardb.interface" | "modelardb.latency" | "modelardb.limit" |  "modelardb.partitionby" |
             "modelardb.spark.receivers" | "modelardb.spark.streaming" | "modelardb.batch" | "modelardb.storage" | "modelardb.separator" |
             "modelardb.header" | "modelardb.timestamps" | "modelardb.dateformat" | "modelardb.timezone" | "modelardb.values" | "modelardb.locale" |
             "modelardb.error" | "modelardb.resolution" | "modelardb.debug" =>
          settings.put(lineSplit(0), List(lineSplit(1).stripPrefix("'").stripSuffix("'")))
        case _ =>
          if (lineSplit(0).charAt(0) != '#') {
            throw new UnsupportedOperationException("unknown setting \"" + lineSplit(0) + "\" in config file")
          }
      }
    }
    settings.put("modelardb.models", models.toList)
    settings.put("modelardb.source", sources.toList)
    collection.immutable.HashMap(settings.toSeq: _*)
  }

  private def appendSources(pathName: String, sources: ArrayBuffer[String]): Unit = {
    val file = new File(pathName)
    file match {
      case _ if file.isFile => sources.append(pathName)
      case _ if file.isDirectory => sources.appendAll(file.list)
      case _ if file.getName.contains("*") =>
        //NOTE: Simple glob parser that only works if it is a full path globbing files with a suffix
        val folder = file.getParentFile
        val matcher = FileSystems.getDefault.getPathMatcher("glob:" + file.getName)
        val files = folder.list.filter(str => matcher.matches(Paths.get(str)))
        sources.appendAll(files.sorted.map(folder.getPath + '/' + _))
      //The path is not a file but contains a semicolon so it must be a socket we can read from
      case _ if pathName.contains(":") => sources.append(pathName)
      case _ =>
        throw new UnsupportedOperationException("source \"" + pathName + "\" in config file does not exist")
    }
  }

  /** Instance Variables **/
  private var storage: Storage = _
  private var models: Array[String] = _
  private var error: Float = 0.0F
  private var latency: Int = 0
  private var limit: Int = 0
  private var resolution: Int = 0
  private var settings: HashMap[String, List[String]] = _
}