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

import java.io.File
import java.nio.file.{FileSystems, Paths}

import dk.aau.modelardb.core._
import dk.aau.modelardb.core.utility.Static
import dk.aau.modelardb.engines.EngineFactory
import dk.aau.modelardb.storage.StorageFactory

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object Main {

  /** Public Methods **/
  def main(args: Array[String]): Unit = {

    //ModelarDB checks args(0) for a config and uses $HOME/Programs/modelardb.conf as a fallback
    val fallback = System.getProperty("user.home") + "/Programs/modelardb.conf"
    val configPath: String = if (args.length == 1) {
      args(0)
    } else if (new java.io.File(fallback).exists) {
      fallback
    } else {
      println("usage: modelardb path/to/modelardb.conf")
      System.exit(-1)
      //HACK: necessary to have the same type in all branches of the match expression
      ""
    }

    /* Configuration */
    val configuration = readConfigurationFile(configPath)

    /* Storage */
    val storage = StorageFactory.getStorage(configuration.getString("modelardb.storage"))

    /* Engine */
    EngineFactory.startEngine(
      configuration.getString("modelardb.interface"), configuration.getString("modelardb.engine"),
      storage, configuration.getModels, configuration.getInteger("modelardb.batch"))

    /* Cleanup */
    storage.close()

    //HACK: some engine and storage providers makes the main process hang despite being terminated beforehand
    Static.SIGTERM()
  }

  /** Private Methods **/
  private def readConfigurationFile(configPath: String): Configuration = {
    Static.info(s"ModelarDB: $configPath")
    val configuration = new Configuration()
    val models = ArrayBuffer[String]()
    val sources = ArrayBuffer[String]()
    val correlations = ArrayBuffer[Correlation]()


    //The information about dimensions are extracted first so correlation objects can depends on it being available
    val dimensionLines = Source.fromFile(configPath).getLines().map(_.trim).filter(_.startsWith("modelardb.dimensions"))
    val dimensions: Dimensions = if (dimensionLines.nonEmpty) {
      val lineSplit = dimensionLines.toStream.last.trim().split(" ", 2)
      configuration.add("modelardb.dimensions", readDimensionsFile(lineSplit(1)))
      configuration.getDimensions
    } else {
      null
    }

    //Parses everything but modelardb.dimensions as dimensions are already initialized
    for (line <- Source.fromFile(configPath).getLines().filter(_.nonEmpty)) {
      //Parsing is performed naively and will terminate if the config is malformed
      val lineSplit = line.trim().split(" ", 2)
      lineSplit(0) match {
        case "modelardb.model" => models.append(lineSplit(1))
        case "modelardb.source" => appendSources(lineSplit(1), sources)
        case "modelardb.dimensions" => //Purposely empty as modelardb.dimensions have already been parsed
        case "modelardb.correlation" =>
          //If the value is a file each line is considered a clause
          val tls = lineSplit(1).trim
          if (new File(tls).exists()) {
            for (line <- Source.fromFile(tls).getLines()) {
              correlations.append(parseCorrelation(line, dimensions))
            }
          } else {
            correlations.append(parseCorrelation(tls, dimensions))
          }
        case "modelardb.resolution" =>
          //Java, in general, works with timestamps in milliseconds, but modelardb.resolution is in seconds
          configuration.add("modelardb.resolution", (lineSplit(1).toDouble * 1000).toInt)
        case "modelardb.engine" | "modelardb.interface" | "modelardb.latency" | "modelardb.limit" | "modelardb.batch" |
             "modelardb.dynamicsplitfraction"  | "modelardb.storage" | "modelardb.separator" | "modelardb.header" |
             "modelardb.timestamps" | "modelardb.dateformat" | "modelardb.timezone" | "modelardb.values" |
             "modelardb.locale" | "modelardb.error" | "modelardb.spark.streaming" | "modelardb.spark.receivers" =>
          configuration.add(lineSplit(0), lineSplit(1).stripPrefix("'").stripSuffix("'"))
        case _ =>
          if (lineSplit(0).charAt(0) != '#') {
            throw new UnsupportedOperationException("ModelarDB: unknown setting \"" + lineSplit(0) + "\" in config file")
          }
      }
    }

    configuration.add("modelardb.model", models.toArray)
    configuration.add("modelardb.source", sources.toArray)
    configuration.add("modelardb.correlation", correlations.toArray)
    configuration
  }

  private def appendSources(pathName: String, sources: ArrayBuffer[String]): Unit = {
    val file = new File(pathName)
    if ((pathName.contains("*") && ! file.getParentFile.exists()) && ! file.exists()) {
      throw new IllegalArgumentException("ModelarDB: file/folder \"" + pathName + "\" do no exist")
    }

    file match {
      case _ if file.isFile => sources.append(pathName)
      case _ if file.isDirectory =>
        val files = file.listFiles.filterNot(_.isHidden).map(file => file.getAbsolutePath)
        sources.appendAll(files)
      case _ if file.getName.contains("*") =>
        //This is a simple glob-based filter that allows users to only ingest specific files, e.g., based on their suffix
        val folder = file.getParentFile
        val matcher = FileSystems.getDefault.getPathMatcher("glob:" + file.getName)
        val files = folder.listFiles.filterNot(_.isHidden).filter(file => matcher.matches(Paths.get(file.getName)))
        sources.appendAll(files.sorted.map(file => file.toString))
      //The path is not a file but contains a semicolon, so it is assumed to be an IP and port number
      case _ if pathName.contains(":") => sources.append(pathName)
      case _ =>
        throw new IllegalArgumentException("ModelarDB: source \"" + pathName + "\" in config file does not exist")
    }
  }

  private def readDimensionsFile(dimensionPath: String): Dimensions = {
    Static.info(s"ModelarDB: $dimensionPath")
    //The user explicitly specified that no dimensions exist
    if (dimensionPath.equalsIgnoreCase("none")) {
      return new Dimensions(Array())
    }

    //Checks if the user has specified a schema inline, and if not, ensures that the dimensions file exists
    if ( ! new File(dimensionPath).exists()) {
      val dimensions = dimensionPath.split(';').map(_.trim)
      if (dimensions(0).split(',').length < 2) {
        //A schema with a dimension that has no levels is invalid, so this must be a missing file
        throw new IllegalArgumentException("ModelarDB: file \"" + dimensionPath + "\" does no exist")
      }
      return new Dimensions(dimensions)
    }

    //Parses a dimensions file with the format (Dimension Definition+, Empty Line, Row+)
    val lines = Source.fromFile(dimensionPath).getLines()
    var line: String = " "

    //Parses each dimension definition in the dimensions file
    val tables = mutable.Buffer[String]()
    while (lines.hasNext && line.nonEmpty) {
      line = lines.next().trim
      if (line.nonEmpty) {
        tables.append(line)
      }
    }
    val dimensions = new Dimensions(tables.toArray)

    //Skips the empty lines separating dimension definitions and rows
    lines.dropWhile(_.isEmpty)

    //Parses each row in the dimensions file
    line = " "
    while (lines.hasNext && line.nonEmpty) {
      line = lines.next().trim
      if (line.nonEmpty) {
        dimensions.add(line)
      }
    }
    dimensions
  }

  private def parseCorrelation(line: String, dimensions: Dimensions): Correlation = {
    //The line is split into correlations and scaling factors
    var split = line.split('*')
    val correlations = split(0).split(',').map(_.trim)
    val scaling = if (split.length > 1) split(1).split(',').map(_.trim) else Array[String]()
    val correlation = new Correlation()

    //Correlation is either specified as a set of sources, a set of LCA levels, a set of members, or a distance
    for (elem <- correlations) {
      split = elem.split(' ').map(_.trim)
      if (split.length == 1 && split(0).toLowerCase.equals("auto")) {
        //Automatic
        correlation.setDistance(dimensions.getLowestNoneZeroDistance)
      } else if (split.length == 1 && Static.isFloat(split(0))) {
        //Distance
        correlation.setDistance(split(0).toFloat)
      } else if (split.length == 2 && Static.isInteger(split(1).trim)) {
        //Dimensions and LCA level
        correlation.addDimensionAndLCA(split(0).trim, split(1).trim.toInt, dimensions)
      } else if (split.length >= 3 && Static.isInteger(split(1).trim)) {
        //Dimension, level, and members
        correlation.addDimensionAndMembers(split(0).trim, split(1).trim.toInt, split.drop(2), dimensions)
      } else {
        //Sources
        correlation.addSources(split)
      }
    }

    //Scaling factors are set for time series from specific sources, or for time series with specific members
    for (elem <- scaling) {
      split = elem.split(' ')
      if (split.length == 2) {
        //Sets the scaling factor for time series from specific sources
        correlation.addScalingFactorForSource(split(0).trim, split(1).trim.toInt)
      } else if (split.length == 4) {
        //Sets the scaling factor for time series with a specific member
        correlation.addScalingFactorForMember(split(0).trim, split(1).trim.toInt, split(2).trim, split(3).trim.toFloat, dimensions)
      } else {
        throw new IllegalArgumentException("ModelarDB: unable to parse scaling factors \"" + elem + "\"")
      }
    }
    correlation
  }
}
