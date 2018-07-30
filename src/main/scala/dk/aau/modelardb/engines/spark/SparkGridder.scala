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

import dk.aau.modelardb.core.DataPoint
import java.sql.Timestamp

import org.apache.spark.rdd.RDD

import scala.annotation.switch
import scala.collection.immutable.HashMap
import org.apache.spark.sql.Row

import scala.collection.JavaConverters._

object SparkGridder {

  /* Public Functions */
  def segmentProjection(rows: RDD[Row], requiredColumns: Array[String]): RDD[Row] = {

    //NOTE: Optimized projection methods are not generated for all combinations
    // of columns in the segment view due to technical limitations, the Scala
    // compiler cannot compile the large amount of generated code. Instead an
    // appropriate subset is selected to match the requirement of queries from
    // the Data Point View and queries using the optimized UDAFs for aggregate
    // queries. This ensures all queries intended for the Segment View use an
    // optimized projection method generated using static code generation,
    // despite the limitations of Scalac, while keeping the compile time low.
    if (requiredColumns.isEmpty) {
      //Projections are not necessary if only empty rows or all columns are requested in the query
      rows
    } else {
      val target = computeJumpTarget(requiredColumns, SparkGridder.segmentView)
      (target: @switch) match {
        case 234 =>
          rows.map((row: Row) => Row(row.getTimestamp(1), row.getTimestamp(2), row.getInt(3)))
        case 1234 =>
          rows.map((row: Row) => Row(row.getInt(0), row.getTimestamp(1), row.getTimestamp(2), row.getInt(3)))
        case 2341 =>
          rows.map((row: Row) => Row(row.getTimestamp(1), row.getTimestamp(2), row.getInt(3), row.getInt(0)))
        case 1234567 =>
          rows
        case _ => rows.map(SparkGridder.getSegmentGridFunctionFallback(requiredColumns))
      }
    }
  }

  def dataPointProjection(rows: RDD[Row], requiredColumns: Array[String]): RDD[Row] = {
    val segments = rows.map(Spark.getRowToSegment)

    //If a query only need the rows but not the content we skip data point creation
    if (requiredColumns.isEmpty) {
      segments.flatMap(segment => Range.inclusive(1, segment.length()).map(_ => Row()))
    } else {
      segments.flatMap(segment => segment.grid().iterator().asScala)
        .map(SparkGridder.getDataPointGridFunctionSwitch(requiredColumns))
    }
  }

  /** Private Functions **/
  private def getSegmentGridFunctionFallback(requiredColumns: Array[String]): (Row => Row) = {
      val columns = requiredColumns.map(this.segmentView)
      row => Row.fromSeq(columns.map({
        case 1 => row.getInt(0)
        case 2 => row.getTimestamp(1)
        case 3 => row.getTimestamp(2)
        case 4 => row.getInt(3)
        case 5 => row.getInt(4)
        case 6 => row.getAs[Array[Byte]](5)
        case 7 => row.getAs[Array[Long]](6)
      }))
    }

  private def getDataPointGridFunctionMatch(requiredColumns: Array[String]): (DataPoint => Row) = {
    requiredColumns match {
      //Permutations with (sid, ts, val)
      case Array("sid", "ts", "val") =>
        (dp: DataPoint) => Row(dp.sid, new Timestamp(dp.timestamp), dp.value)
      case Array("sid", "val", "ts") =>
        (dp: DataPoint) => Row(dp.sid, dp.value, new Timestamp(dp.timestamp))
      case Array("ts", "sid", "val") =>
        (dp: DataPoint) => Row(new Timestamp(dp.timestamp), dp.sid, dp.value)
      case Array("ts", "val", "sid") =>
        (dp: DataPoint) => Row(new Timestamp(dp.timestamp), dp.value, dp.sid)
      case Array("val", "sid", "ts") =>
        (dp: DataPoint) => Row(dp.value, dp.sid, new Timestamp(dp.timestamp))
      case Array("val", "ts", "sid") =>
        (dp: DataPoint) => Row(dp.value, new Timestamp(dp.timestamp), dp.sid)
      //Permutations with (sid, ts)
      case Array("sid", "ts") =>
        (dp: DataPoint) => Row(dp.sid, new Timestamp(dp.timestamp))
      case Array("ts", "sid") =>
        (dp: DataPoint) => Row(new Timestamp(dp.timestamp), dp.sid)
      //Permutations with (ts, val)
      case Array("ts", "val") =>
        (dp: DataPoint) => Row(new Timestamp(dp.timestamp), dp.value)
      case Array("val", "ts") =>
        (dp: DataPoint) => Row(dp.value, new Timestamp(dp.timestamp))
      //Permutations with (sid, val)
      case Array("sid", "val") =>
        (dp: DataPoint) => Row(dp.sid, dp.value)
      case Array("val", "sid") =>
        (dp: DataPoint) => Row(dp.value, dp.sid)
      //Permutations with (sid)
      case Array("sid") =>
        (dp: DataPoint) => Row(dp.sid)
      //Permutations with (ts)
      case Array("ts") =>
        (dp: DataPoint) => Row(new Timestamp(dp.timestamp))
      //Permutations with (val)
      case Array("val") =>
        (dp: DataPoint) => Row(dp.value)
      //Projection with dimensions must be dynamic
      case _ =>
        throw new RuntimeException("ModelarDB: unknown permutation of required columns received")
    }
  }

  private def getDataPointGridFunctionSwitch(requiredColumns: Array[String]): (DataPoint => Row) = {
    val target = computeJumpTarget(requiredColumns, SparkGridder.dataPointView)
      (target: @switch) match {
      //Permutations of ('sid')
      case 1 => (dp: DataPoint) => Row(dp.sid)
      //Permutations of ('ts')
      case 2 => (dp: DataPoint) => Row(new Timestamp(dp.timestamp))
      //Permutations of ('val')
      case 3 => (dp: DataPoint) => Row(dp.value)
      //Permutations of ('sid', 'ts')
      case 12 => (dp: DataPoint) => Row(dp.sid, new Timestamp(dp.timestamp))
      case 21 => (dp: DataPoint) => Row(new Timestamp(dp.timestamp), dp.sid)
      //Permutations of ('sid', 'val')
      case 13 => (dp: DataPoint) => Row(dp.sid, dp.value)
      case 31 => (dp: DataPoint) => Row(dp.value, dp.sid)
      //Permutations of ('ts', 'val')
      case 23 => (dp: DataPoint) => Row(new Timestamp(dp.timestamp), dp.value)
      case 32 => (dp: DataPoint) => Row(dp.value, new Timestamp(dp.timestamp))
      //Permutations of ('sid', 'ts', 'val')
      case 123 => (dp: DataPoint) => Row(dp.sid, new Timestamp(dp.timestamp), dp.value)
      case 132 => (dp: DataPoint) => Row(dp.sid, dp.value, new Timestamp(dp.timestamp))
      case 213 => (dp: DataPoint) => Row(new Timestamp(dp.timestamp), dp.sid, dp.value)
      case 231 => (dp: DataPoint) => Row(new Timestamp(dp.timestamp), dp.value, dp.sid)
      case 312 => (dp: DataPoint) => Row(dp.value, dp.sid, new Timestamp(dp.timestamp))
      case 321 => (dp: DataPoint) => Row(dp.value, new Timestamp(dp.timestamp), dp.sid)
      //Projection with dimensions must be dynamic
      case _ =>
        throw new RuntimeException("ModelarDB: unknown permutation of required columns received")
    }
  }

  private def getDataPointGridFunctionFallback(requiredColumns: Array[String]): (DataPoint => Row) = {
    val columns = requiredColumns.map(this.dataPointView)
    dp => Row.fromSeq(columns.map({
      case 1 => dp.sid
      case 2 => new Timestamp(dp.timestamp)
      case 3 => dp.value
    }))
  }

  private def computeJumpTarget(requiredColumns: Array[String], map: HashMap[String, Int]): Int = {
    var factor: Int = 1
    var target: Int = 0
    for (col: String <- requiredColumns.reverseIterator) {
      target = target + factor * map(col)
      factor = factor * 10
    }
    target
  }

  /** Instance Variable **/
  private val segmentView = HashMap[String, Int]("sid" -> 1, "st" -> 2, "et" -> 3, "res" -> 4, "mid" -> 5, "param" -> 6, "gaps" -> 7)
  private val dataPointView = HashMap[String, Int]("sid" -> 1, "ts" -> 2, "val" -> 3)
}