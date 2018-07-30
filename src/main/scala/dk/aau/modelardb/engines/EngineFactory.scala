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
package dk.aau.modelardb.engines

import java.util.function.Consumer

import dk.aau.modelardb.core.Storage
import dk.aau.modelardb.core.models.Segment
import dk.aau.modelardb.Main

import scala.collection.immutable.HashMap

object EngineFactory {

  /** Public Methods **/
  def startEngine(interface: String, engine: String, storage: Storage, models: Array[String],
                  batchSize: Int, settings: HashMap[String, List[String]]): Unit = {


    //Grabs the name of the system from the engine connection string
    engine.takeWhile(_ != ':') match {
      case "local" => startLocal(storage, models, batchSize)
      case "spark" => startSpark(interface, engine, storage, models, batchSize, settings)
      case _ =>
        throw new java.lang.UnsupportedOperationException("unknown value for modelardb.engine in the config file")
    }
  }

  /** Private Methods **/
  private def startLocal(storage: Storage, models: Array[String], batchSize: Int): Unit = {
    //Wraps the insert method for emitting both segment types to the stream
    val consumeTemporary = new java.util.function.Consumer[Segment] {
      override def accept(segment: Segment): Unit = ()
    }

    var batchIndex = 0
    val batch = new Array[Segment](batchSize)
    val consumeFinalized = new java.util.function.Consumer[Segment] {
      override def accept(segment: Segment): Unit = {
        batch(batchIndex) = segment
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

    storage.open()
    val timeSeries = Main.initTimeSeries(storage.getMaxSID)
    storage.init(timeSeries, models)
    val workingSets = Main.buildWorkingSets(timeSeries, 1).toArray

    //There should only be one working set so we verify and throw if we due to an error get more
    if (workingSets.length != 1) {
      throw new java.lang.RuntimeException("local engine received multiple workings sets instead of one")
    }
    val workingSet = workingSets(0)
    println(workingSet)
    workingSet.process(consumeTemporary, consumeFinalized, isTerminated)
    storage.insert(batch, batchIndex)
    workingSet.logger.printWorkingSetResult()

    //DEBUG: For debugging we print the number of data points returned from storage
    var segmentDebugCount = 0L
    storage.getSegments.forEach(new Consumer[Segment] {
      override def accept(segment: Segment): Unit = segmentDebugCount += segment.grid().count()
    })
    println(
      s"Gridded: $segmentDebugCount\n=========================================================")
  }

  private def startSpark(interface: String, engine: String, storage: Storage, models: Array[String],
                         batchSize: Int, settings: HashMap[String, List[String]]): Unit = {
    //Read spark specific configuration variables
    val receiverCount = settings("modelardb.spark.receivers").head.toInt
    if (receiverCount < 0) {
      throw new java.lang.UnsupportedOperationException("modelardb.spark.receiver must be a positive number of receivers or zero to disable")
    }

    val microBatchSize = settings("modelardb.spark.streaming").head.toInt
    if (microBatchSize <= 0) {
      throw new java.lang.UnsupportedOperationException("modelardb.spark.streaming must be a positive number of seconds between micro-batches")
    }

    new dk.aau.modelardb.engines.spark.Spark(interface, engine, storage, models, receiverCount, microBatchSize, batchSize).start()
  }
}