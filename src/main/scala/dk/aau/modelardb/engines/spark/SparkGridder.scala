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

import dk.aau.modelardb.core.DataPoint
import dk.aau.modelardb.core.models.Segment
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import scala.annotation.switch
import scala.collection.JavaConverters._

object SparkGridder {

  /** Public Functions **/
  def initialize(segmentViewMappings: Map[String, Int], dataPointViewMappings: Map[String, Int]): Unit = {
    this.segmentViewNameToIndex = segmentViewMappings
    this.dataPointViewNameToIndex = dataPointViewMappings
  }

  def segmentProjection(rows: RDD[Row], requiredColumns: Array[String]): RDD[Row] = {

    //NOTE: optimized projection methods are not generated for all combinations
    // of columns in the segment view due to technical limitations, the Scala
    // compiler cannot compile the large amount of generated code. Instead, an
    // appropriate subset is selected to match the requirement of queries from
    // the Data Point View and queries using the optimized UDAFs for aggregate
    // queries. This ensures most queries sent to the Segment View can use an
    // optimized projection method generated using static code generation,
    // despite the limitations of Scalac, while keeping the compile time low
    val target = computeJumpTarget(requiredColumns, SparkGridder.segmentViewNameToIndex, 7)
    (target: @switch) match {
      case 0 =>
        rows
      case 234 =>
        rows.map((row: Row) => Row(row.getTimestamp(1), row.getTimestamp(2), row.getInt(3)))
      case 1234 =>
        rows.map((row: Row) => Row(row.getInt(0), row.getTimestamp(1), row.getTimestamp(2), row.getInt(3)))
      case 2341 =>
        rows.map((row: Row) => Row(row.getTimestamp(1), row.getTimestamp(2), row.getInt(3), row.getInt(0)))
      case 1234567 =>
        rows.map((row: Row) => Row(row.getInt(0), row.getTimestamp(1), row.getTimestamp(2),
          row.getInt(3), row.getInt(4), row.getAs[Array[Byte]](5), row.getAs[Array[Byte]](6)))
      case _ =>
        //Selects an appropriate dynamic projection method based on the approximate size of the data set
        if (Spark.isDataSetSmall(rows)) {
          rows.map(SparkGridder.getSegmentGridFunctionFallback(requiredColumns))
        } else {
          val code = SparkProjection.getSegmentProjection(requiredColumns, this.segmentViewNameToIndex)
          val dmc = dk.aau.modelardb.engines.spark.Spark.getStorage.getDimensionsCache
          rows.mapPartitions(it => {
            val projector = SparkProjection.compileSegmentProjection(code)
            it.map(row => projector.project(row, dmc))
          })
        }
    }
  }

  def dataPointProjection(rows: RDD[Row], requiredColumns: Array[String]): RDD[Row] = {
    val segments = rows.map(getRowToSegment)

    //If a query only needs the rows but not the content data point creation is skipped
    if (requiredColumns.isEmpty) {
      segments.flatMap(segment => Range.inclusive(1, segment.length()).map(_ => Row()))
    } else {
      val dataPoints = segments.flatMap(segment => segment.grid().iterator().asScala)

      //Projections that do not include dimensions can used statically generated projection methods
      val projection = getDataPointGridFunction(requiredColumns)
      if (projection != null) {
        return dataPoints.map(projection)
      }

      //Selects an appropriate dynamic projection method based on the approximate size of the data set
      if (Spark.isDataSetSmall(rows)) {
        dataPoints.map(SparkGridder.getDataPointGridFunctionFallback(requiredColumns))
      } else {
        val code = SparkProjection.getDataPointProjection(requiredColumns, this.dataPointViewNameToIndex)
        val dmc = dk.aau.modelardb.engines.spark.Spark.getStorage.getDimensionsCache
        val sc = dk.aau.modelardb.engines.spark.Spark.getStorage.getSourceScalingFactorCache
        dataPoints.mapPartitions(it => {
          val projector = SparkProjection.compileDataPointProjection(code)
          it.map(row => projector.project(row, dmc, sc))
        })
      }
    }
  }

  def getRowToSegment: Row => Segment = {
    val mc = Spark.getStorage.getModelCache
    row => {
      val model = mc(row.getInt(4))
      model.get(row.getInt(0), row.getTimestamp(1).getTime, row.getTimestamp(2).getTime,
        row.getInt(3), row.getAs[Array[Byte]](5), row.getAs[Array[Byte]](6))
    }
  }

  /** Private Functions **/
  private def getSegmentGridFunctionFallback(requiredColumns: Array[String]): Row => Row = {
    val dmc = Spark.getStorage.getDimensionsCache
    val columns = requiredColumns.map(this.segmentViewNameToIndex)
    row => Row.fromSeq(columns.map({
      case 1 => row.getInt(0)
      case 2 => row.getTimestamp(1)
      case 3 => row.getTimestamp(2)
      case 4 => row.getInt(3)
      case 5 => row.getInt(4)
      case 6 => row.getAs[Array[Byte]](5)
      case 7 => row.getAs[Array[Long]](6)
      case index => dmc(row.getInt(0))(index - 8)
    }))
  }

  def getDataPointGridFunction(requiredColumns: Array[String]): DataPoint => Row = {
    val sc = Spark.getStorage.getSourceScalingFactorCache
    val target = computeJumpTarget(requiredColumns, SparkGridder.dataPointViewNameToIndex, 3)
    (target: @switch) match {
      //Permutations of ('sid')
      case 1 => (dp: DataPoint) => Row(dp.sid)
      //Permutations of ('ts')
      case 2 => (dp: DataPoint) => Row(new Timestamp(dp.timestamp))
      //Permutations of ('val')
      case 3 => (dp: DataPoint) => Row(dp.value / sc(dp.sid))
      //Permutations of ('sid', 'ts')
      case 12 => (dp: DataPoint) => Row(dp.sid, new Timestamp(dp.timestamp))
      case 21 => (dp: DataPoint) => Row(new Timestamp(dp.timestamp), dp.sid)
      //Permutations of ('sid', 'val')
      case 13 => (dp: DataPoint) => Row(dp.sid, dp.value / sc(dp.sid))
      case 31 => (dp: DataPoint) => Row(dp.value / sc(dp.sid), dp.sid)
      //Permutations of ('ts', 'val')
      case 23 => (dp: DataPoint) => Row(new Timestamp(dp.timestamp), dp.value / sc(dp.sid))
      case 32 => (dp: DataPoint) => Row(dp.value / sc(dp.sid), new Timestamp(dp.timestamp))
      //Permutations of ('sid', 'ts', 'val')
      case 123 => (dp: DataPoint) => Row(dp.sid, new Timestamp(dp.timestamp), dp.value / sc(dp.sid))
      case 132 => (dp: DataPoint) => Row(dp.sid, dp.value / sc(dp.sid), new Timestamp(dp.timestamp))
      case 213 => (dp: DataPoint) => Row(new Timestamp(dp.timestamp), dp.sid, dp.value / sc(dp.sid))
      case 231 => (dp: DataPoint) => Row(new Timestamp(dp.timestamp), dp.value / sc(dp.sid), dp.sid)
      case 312 => (dp: DataPoint) => Row(dp.value / sc(dp.sid), dp.sid, new Timestamp(dp.timestamp))
      case 321 => (dp: DataPoint) => Row(dp.value / sc(dp.sid), new Timestamp(dp.timestamp), dp.sid)
      //Static projections cannot be used for rows with dimensions
      case _ => null
    }
  }

  def getDataPointGridFunctionFallback(requiredColumns: Array[String]): DataPoint => Row = {
    val dmc = Spark.getStorage.getDimensionsCache
    val sc = Spark.getStorage.getSourceScalingFactorCache
    val columns = requiredColumns.map(this.dataPointViewNameToIndex)
    dp => Row.fromSeq(columns.map({
      case 1 => dp.sid
      case 2 => new Timestamp(dp.timestamp)
      case 3 => dp.value / sc(dp.sid) //Applies the scaling factor
      case index => dmc(dp.sid)(index - 4)
    }))
  }

  private def computeJumpTarget(requiredColumns: Array[String], map: Map[String, Int], staticColumns: Int): Int = {
    var factor: Int = 1
    var target: Int = 0
    //Computes a key for a statically generated projection function as the query only require static columns
    for (col: String <- requiredColumns.reverseIterator) {
      val index = map(col)
      if (index > staticColumns) {
        return Integer.MAX_VALUE
      }
      target = target + factor * index
      factor = factor * 10
    }
    target
  }

  /** Instance Variables **/
  private var segmentViewNameToIndex: Map[String, Int] = _
  private var dataPointViewNameToIndex: Map[String, Int] = _
}
