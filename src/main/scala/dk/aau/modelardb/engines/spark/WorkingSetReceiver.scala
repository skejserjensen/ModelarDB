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
package dk.aau.modelardb.engines.spark

import java.sql.Timestamp
import java.util

import dk.aau.modelardb.core.WorkingSet
import dk.aau.modelardb.core.models.Segment

import org.apache.spark.sql.Row
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

class WorkingSetReceiver(workingSet: WorkingSet, midCache: util.HashMap[String, Integer])
  extends Receiver[Row](StorageLevel.MEMORY_AND_DISK) {

  /** Public Methods **/
  def onStart(): Unit = {
    thread.start()
  }

  def onStop(): Unit = {
    thread.interrupt()
  }

  /** Private Methods **/
  private def receive(): Unit = {
    //Wraps the insert method for emitting both segment types to the stream
    val consumeTemporary = new java.util.function.Consumer[Segment] {
      override def accept(segment: Segment): Unit = store(segmentToRow(segment, isFinalized = false))
    }

    val consumeFinalized = new java.util.function.Consumer[Segment] {
      override def accept(segment: Segment): Unit = store(segmentToRow(segment, isFinalized = true))
    }

    val isTerminated = new java.util.function.BooleanSupplier {
      override def getAsBoolean: Boolean = isStopped()
    }

    println(workingSet)
    workingSet.process(consumeTemporary, consumeFinalized, isTerminated)
    workingSet.logger.printWorkingSetResult()
  }

  private def segmentToRow(seg: Segment, isFinalized: Boolean): Row = {
    val name = seg.getClass.getName
    val mid = midCache.get(name)
    Row(seg.sid, new Timestamp(seg.startTime), new Timestamp(seg.endTime),
      seg.resolution, mid, seg.parameters(), seg.gaps(), isFinalized)
  }

  /** Instance Variable **/
  lazy val thread = new Thread(workingSet.toString) {
    override def run(): Unit = receive()
  }
}