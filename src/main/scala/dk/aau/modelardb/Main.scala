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
package dk.aau.modelardb

import dk.aau.modelardb.core._
import dk.aau.modelardb.core.models.ModelTypeFactory
import dk.aau.modelardb.core.utility.{Pair, Static, ValueFunction}
import dk.aau.modelardb.engines.{CodeGenerator, EngineFactory}
import dk.aau.modelardb.storage.StorageFactory

import java.io.File
import java.nio.file.{FileSystems, Paths}
import java.util
import java.util.TimeZone
import java.util.concurrent.Executors
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
    TimeZone.setDefault(configuration.getTimeZone) //Ensures all components use the same time zone

    /* Storage */
    val storage = StorageFactory.getStorage(configuration.getString("modelardb.storage"))

    /* Engine */
    EngineFactory.startEngine(configuration, storage)

    /* Cleanup */
    storage.close()
  }

  /** Private Methods **/
  private def readConfigurationFile(configPath: String): Configuration = {
    Static.info(s"ModelarDB: $configPath")
    val configuration = new Configuration()
    val models = ArrayBuffer[String]()
    val sources = ArrayBuffer[String]()
    val derivedSources = new util.HashMap[String, ArrayBuffer[Pair[String, ValueFunction]]]()
    val correlations = ArrayBuffer[Correlation]()


    //The information about dimensions are extracted first so correlation objects can depends on it being available
    val configDimensionsSource = Source.fromFile(configPath)
    val dimensionLine = configDimensionsSource.getLines().map(_.trim).filter(_.startsWith("modelardb.dimensions"))
    val dimensions: Dimensions = if (dimensionLine.nonEmpty) {
      val lineSplit = dimensionLine.toStream.last.trim().split(" ", 2)
      configuration.add("modelardb.dimensions", readDimensionsFile(lineSplit(1))).asInstanceOf[Dimensions]
    } else {
      //The user have not specified any dimensions
      configuration.add("modelardb.dimensions", new Dimensions(Array())).asInstanceOf[Dimensions]
    }
    configDimensionsSource.close()

    //Parses everything but modelardb.dimensions as dimensions are already initialized
    val configFullSource = Source.fromFile(configPath)
    for (line <- configFullSource.getLines().filter(_.nonEmpty)) {
      //Parsing is performed naively and will terminate if the config is malformed
      val lineSplit = line.trim().split(" ", 2)
      lineSplit(0) match {
        case "modelardb.model_type" => models.append(lineSplit(1))
        case "modelardb.source" => appendSources(lineSplit(1), sources)
        case "modelardb.source.derived" =>
          //Store a mapping from the original source to the derived source and the function to map over its values
          val derived = lineSplit(1).split(' ').map(_.trim)
          val transformation = CodeGenerator.getValueFunction(derived(2))
          if ( ! derivedSources.containsKey(derived(0))) {
            derivedSources.put(derived(0), ArrayBuffer[Pair[String, ValueFunction]]())
          }
          derivedSources.get(derived(0)).append(new Pair(derived(1), transformation))
        case "modelardb.dimensions" => //Purposely empty as modelardb.dimensions have already been parsed
        case "modelardb.correlation" =>
          //If the value is a file each line is considered a clause
          val tls = lineSplit(1).trim
          if (new File(tls).exists()) {
            val correlationSource = Source.fromFile(tls)
            for (line <- correlationSource.getLines()) {
              correlations.append(parseCorrelation(line, dimensions))
            }
            correlationSource.close()
          } else {
            correlations.append(parseCorrelation(tls, dimensions))
          }
        case "modelardb.engine" | "modelardb.storage" | "modelardb.interface" | "modelardb.time_zone" |
             "modelardb.ingestors" | "modelardb.timestamp_column" | "modelardb.value_column" |
             "modelardb.error_bound" | "modelardb.length_bound" | "modelardb.maximum_latency" |
             "modelardb.sampling_interval" | "modelardb.batch_size" | "modelardb.dynamic_split_fraction"  |
             "modelardb.csv.separator" | "modelardb.csv.header" | "modelardb.csv.date_format" | "modelardb.csv.locale" |
             "modelardb.spark.streaming" =>
          configuration.add(lineSplit(0), lineSplit(1).stripPrefix("'").stripSuffix("'"))
        case _ =>
          if (lineSplit(0).charAt(0) != '#') {
            throw new UnsupportedOperationException("ModelarDB: unknown setting \"" + lineSplit(0) + "\" in config file")
          }
      }
    }
    configFullSource.close()

    configuration.add("modelardb.model_types", models.toArray)
    configuration.add("modelardb.sources", sources.toArray)
    val finalDerivedSources = new util.HashMap[String, Array[Pair[String, ValueFunction]]]()
    val dsIter = derivedSources.entrySet().iterator()
    while (dsIter.hasNext) {
      val entry = dsIter.next()
      finalDerivedSources.put(entry.getKey, entry.getValue.toArray)
    }
    configuration.add("modelardb.sources.derived", finalDerivedSources)
    configuration.add("modelardb.correlations", correlations.toArray)
    configuration.add("modelardb.executor_service", Executors.newCachedThreadPool())
    validate(configuration)
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
    val dimensionSource = Source.fromFile(dimensionPath)
    val lines = dimensionSource.getLines()
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
    dimensionSource.close()
    dimensions
  }

  private def parseCorrelation(line: String, dimensions: Dimensions): Correlation = {
    //The line is split into correlations and scaling factors
    var split = line.split('*')
    val correlations = split(0).split(',').map(_.trim)
    val scaling_factor = if (split.length > 1) split(1).split(',').map(_.trim) else Array[String]()
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
    for (elem <- scaling_factor) {
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

  private def validate(configuration: Configuration): Configuration = {
    //Settings used outside core are validated to ensure their values are within the expected range
    if (configuration.getInteger("modelardb.spark.streaming", 0) <= 0) {
      throw new UnsupportedOperationException ("ModelarDB: modelardb.spark.streaming must be a positive number of seconds between micro-batches")
    }

    //Ensure both included and user-defined model types all can be constructed without errors
    val mtn = configuration.getModelTypeNames
    val mtids = Range(1, mtn.length + 1).toArray
    ModelTypeFactory.getModelTypes(mtn, mtids, configuration.getErrorBound, configuration.getLengthBound)
    configuration
  }
}
