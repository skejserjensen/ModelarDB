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
package dk.aau.modelardb.engines.spark

import java.sql.Timestamp

import dk.aau.modelardb.core.WorkingSet
import dk.aau.modelardb.core.utility.SegmentFunction
import org.apache.spark.sql.Row
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

class WorkingSetReceiver(workingSet: WorkingSet)
  extends Receiver[Row](StorageLevel.MEMORY_AND_DISK) {

  /** Public Methods **/
  override def onStart(): Unit = {
    thread.start()
  }

  override def onStop(): Unit = {
    thread.interrupt()
  }

  /** Private Methods **/
  private def receive(): Unit = {
    //Creates methods that emit both segment types to Spark Streaming
    val consumeTemporary = new SegmentFunction {
      override def emit(gid: Int, startTime: Long, endTime: Long, mid: Int, parameters: Array[Byte], gaps: Array[Byte]): Unit = {
        store(Row(gid, new Timestamp(startTime), new Timestamp(endTime), mid, parameters, gaps, false))
      }
    }

    val consumeFinalized = new SegmentFunction {
      override def emit(gid: Int, startTime: Long, endTime: Long, mid: Int, parameters: Array[Byte], gaps: Array[Byte]): Unit = {
        store(Row(gid, new Timestamp(startTime), new Timestamp(endTime), mid, parameters, gaps, true))
      }
    }

    val isTerminated = new java.util.function.BooleanSupplier {
      override def getAsBoolean: Boolean = isStopped()
    }

    println(workingSet)
    workingSet.process(consumeTemporary, consumeFinalized, isTerminated)
    workingSet.logger.printWorkingSetResult()
  }

  /** Instance Variables **/
  lazy val thread: Thread = new Thread(workingSet.toString) {
    override def run(): Unit = receive()
  }
}
