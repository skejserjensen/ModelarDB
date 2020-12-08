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
package dk.aau.modelardb.engines

import java.util.function.Consumer

import dk.aau.modelardb.core.models.Segment
import dk.aau.modelardb.core.utility.SegmentFunction
import dk.aau.modelardb.core.{Configuration, Partitioner, SegmentGroup, Storage}

object EngineFactory {

  /** Public Methods **/
  def startEngine(interface: String, engine: String, storage: Storage, models: Array[String], batchSize: Int): Unit = {

    //Extracts the name of the system from the engine connection string
    engine.takeWhile(_ != ':') match {
      case "local" => startLocal(storage, models, batchSize)
      case "spark" => startSpark(interface, engine, storage, models, batchSize)
      case _ =>
        throw new java.lang.UnsupportedOperationException("ModelarDB: unknown value for modelardb.engine in the config file")
    }
  }

  /** Private Methods **/
  private def startLocal(storage: Storage, models: Array[String], batchSize: Int): Unit = {
    //Creates a method that drops temporary segments and one that store finalized segments in batches
    val consumeTemporary = new SegmentFunction {
      override def emit(gid: Int, startTime: Long, endTime: Long, mid: Int, parameters: Array[Byte], gaps: Array[Byte]): Unit = ()
    }

    var batchIndex = 0
    val batch = new Array[SegmentGroup](batchSize)
    val consumeFinalized = new SegmentFunction {
      override def emit(gid: Int, startTime: Long, endTime: Long, mid: Int, parameters: Array[Byte], gaps: Array[Byte]): Unit = {
        batch(batchIndex) = new SegmentGroup(gid, startTime, endTime, mid, parameters, gaps)
        batchIndex += 1
        if (batchIndex == batchSize) {
          storage.insert(batch, batchIndex)
          batchIndex = 0
        }
      }
    }

    val isTerminated = new java.util.function.BooleanSupplier {
      override def getAsBoolean: Boolean = false
    }

    val configuration = Configuration.get()
    val dimensions = configuration.getDimensions
    storage.open(dimensions)

    val timeSeries = Partitioner.initializeTimeSeries(configuration, storage.getMaxSID)
    val timeSeriesGroups = Partitioner.groupTimeSeries(configuration, timeSeries, storage.getMaxGID)
    storage.initialize(timeSeriesGroups, dimensions, models)

    val midCache = storage.getMidCache
    val workingSets = Partitioner.partitionTimeSeries(configuration, timeSeriesGroups, midCache, 1)

    if (workingSets.length != 1) {
      throw new java.lang.RuntimeException("ModelarDB: the local engine did no receive exactly one working set")
    }
    val workingSet = workingSets(0)
    println(workingSet)
    workingSet.process(consumeTemporary, consumeFinalized, isTerminated)
    storage.insert(batch, batchIndex)
    workingSet.logger.printWorkingSetResult()

    //DEBUG: for debugging we print the number of data points returned from storage
    var segmentDebugCount = 0L
    storage.getSegments.forEach(new Consumer[SegmentGroup] {
      override def accept(sg: SegmentGroup): Unit = {
        val segments = sg.toSegments(storage)
        for (segment: Segment <- segments) {
          segmentDebugCount += segment.grid().count()
        }
      }
    })
    println(
      s"Gridded: $segmentDebugCount\n=========================================================")
  }

  private def startSpark(interface: String, engine: String, storage: Storage,
                         models: Array[String], segmentBatchSize: Int): Unit = {

    //Checks the Apache Spark specific configuration parameters
    val configuration = Configuration.get()
    val receiverCount = configuration.getInteger("modelardb.spark.receivers")
    if (receiverCount < 0) {
      throw new java.lang.UnsupportedOperationException("ModelarDB: modelardb.spark.receiver must be a positive number of receivers or zero to disable")
    }

    val microBatchSize = configuration.getInteger("modelardb.spark.streaming")
    if (microBatchSize <= 0) {
      throw new java.lang.UnsupportedOperationException("ModelarDB: modelardb.spark.streaming must be a positive number of seconds between micro-batches")
    }

    val dimensions = configuration.getDimensions
    new dk.aau.modelardb.engines.spark.Spark(
      interface, engine, storage, dimensions, models,
      receiverCount, microBatchSize, segmentBatchSize).start()
  }
}
