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
package dk.aau.modelardb.engines.spark

import dk.aau.modelardb.core.DataPoint
import dk.aau.modelardb.core.models.Segment
import dk.aau.modelardb.engines.{CodeGenerator, EngineUtilities}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import java.sql.Timestamp
import scala.annotation.switch
import scala.collection.JavaConverters._

object SparkProjector {

  /** Public Methods **/
  def segmentProjection(rows: RDD[Row], requiredColumns: Array[String]): RDD[Row] = {
    //NOTE: optimized projection methods are not generated for all combinations
    // of columns in the segment view due to technical limitations, the Scala
    // compiler cannot compile the large amount of generated code. Instead, an
    // appropriate subset is selected to match the requirement of queries from
    // the Data Point View and queries using the optimized UDAFs for aggregate
    // queries. This ensures most queries sent to the Segment View can use an
    // optimized projection method generated using static code generation,
    // despite the limitations of Scalac, while keeping the compile time low
    val target = computeJumpTarget(requiredColumns, EngineUtilities.segmentViewNameToIndex, 6)
    (target: @switch) match {
      case 0 => //COUNT(*)
        rows
      case 123 => //COUNT_S(#)
        rows.map((row: Row) => Row(row.getInt(0), row.getTimestamp(1), row.getTimestamp(2)))
      case 123456 => //Data Point View and UDAFs
        rows.map((row: Row) => Row(row.getInt(0), row.getTimestamp(1), row.getTimestamp(2),
          row.getInt(3), row.getAs[Array[Byte]](4), row.getAs[Array[Byte]](5)))
      case _ =>
        //Selects an appropriate dynamic projection method based on the approximate size of the data set
        if (Spark.isDataSetSmall(rows)) {
          rows.map(SparkProjector.getSegmentGridFunctionFallback(requiredColumns))
        } else {
          val code = CodeGenerator.getSparkSegmentProjection(requiredColumns, EngineUtilities.segmentViewNameToIndex)
          val tsmc = Spark.getSparkStorage.timeSeriesMembersCache
          rows.mapPartitions(it => {
            val projector = CodeGenerator.compileSparkSegmentProjection(code)
            it.map(row => projector.project(row, tsmc))
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
        dataPoints.map(SparkProjector.getDataPointGridFunctionFallback(requiredColumns))
      } else {
        val code = CodeGenerator.getSparkDataPointProjection(requiredColumns, EngineUtilities.dataPointViewNameToIndex)
        val storage = Spark.getSparkStorage
        val tsmc = storage.timeSeriesMembersCache
        val tssfc = storage.timeSeriesScalingFactorCache
        val btstc = Spark.getBroadcastedTimeSeriesTransformationCache
        dataPoints.mapPartitions(it => {
          val projector = CodeGenerator.compileSparkDataPointProjection(code)
          it.map(row => projector.project(row, tsmc, tssfc, btstc))
        })
      }
    }
  }

  def getRowToSegment: Row => Segment = {
    val mtc = Spark.getSparkStorage.modelTypeCache
    val tssic = Spark.getSparkStorage.timeSeriesSamplingIntervalCache
    row => {
      val tid = row.getInt(0)
      mtc(row.getInt(3)).get(tid, row.getTimestamp(1).getTime, row.getTimestamp(2).getTime,
        tssic(tid), row.getAs[Array[Byte]](4), row.getAs[Array[Byte]](5))
    }
  }

  /** Private Methods **/
  private def getSegmentGridFunctionFallback(requiredColumns: Array[String]): Row => Row = {
    val tsmc = Spark.getSparkStorage.timeSeriesMembersCache
    val columns = requiredColumns.map(EngineUtilities.segmentViewNameToIndex)
    row => Row.fromSeq(columns.map({
      case 1 => row.getInt(0)
      case 2 => row.getTimestamp(1)
      case 3 => row.getTimestamp(2)
      case 4 => row.getInt(3)
      case 5 => row.getAs[Array[Byte]](4)
      case 6 => row.getAs[Array[Long]](5)
      case index => tsmc(row.getInt(0))(index - 7)
    }))
  }

  def getDataPointGridFunction(requiredColumns: Array[String]): DataPoint => Row = {
    val tssfc = Spark.getSparkStorage.timeSeriesScalingFactorCache
    val btstc = Spark.getBroadcastedTimeSeriesTransformationCache
    val target = computeJumpTarget(requiredColumns, EngineUtilities.dataPointViewNameToIndex, 3)
    (target: @switch) match {
      //Permutations of ('tid')
      case 1 => (dp: DataPoint) => Row(dp.tid)
      //Permutations of ('ts')
      case 2 => (dp: DataPoint) => Row(new Timestamp(dp.timestamp))
      //Permutations of ('val')
      case 3 => (dp: DataPoint) => Row(btstc.value(dp.tid).transform(dp.value, tssfc(dp.tid)))
      //Permutations of ('tid', 'ts')
      case 12 => (dp: DataPoint) => Row(dp.tid, new Timestamp(dp.timestamp))
      case 21 => (dp: DataPoint) => Row(new Timestamp(dp.timestamp), dp.tid)
      //Permutations of ('tid', 'val')
      case 13 => (dp: DataPoint) => Row(dp.tid, btstc.value(dp.tid).transform(dp.value, tssfc(dp.tid)))
      case 31 => (dp: DataPoint) => Row(btstc.value(dp.tid).transform(dp.value, tssfc(dp.tid)), dp.tid)
      //Permutations of ('ts', 'val')
      case 23 => (dp: DataPoint) => Row(new Timestamp(dp.timestamp), btstc.value(dp.tid).transform(dp.value, tssfc(dp.tid)))
      case 32 => (dp: DataPoint) => Row(btstc.value(dp.tid).transform(dp.value, tssfc(dp.tid)), new Timestamp(dp.timestamp))
      //Permutations of ('tid', 'ts', 'val')
      case 123 => (dp: DataPoint) => Row(dp.tid, new Timestamp(dp.timestamp), btstc.value(dp.tid).transform(dp.value, tssfc(dp.tid)))
      case 132 => (dp: DataPoint) => Row(dp.tid, btstc.value(dp.tid).transform(dp.value, tssfc(dp.tid)), new Timestamp(dp.timestamp))
      case 213 => (dp: DataPoint) => Row(new Timestamp(dp.timestamp), dp.tid, btstc.value(dp.tid).transform(dp.value, tssfc(dp.tid)))
      case 231 => (dp: DataPoint) => Row(new Timestamp(dp.timestamp), btstc.value(dp.tid).transform(dp.value, tssfc(dp.tid)), dp.tid)
      case 312 => (dp: DataPoint) => Row(btstc.value(dp.tid).transform(dp.value, tssfc(dp.tid)), dp.tid, new Timestamp(dp.timestamp))
      case 321 => (dp: DataPoint) => Row(btstc.value(dp.tid).transform(dp.value, tssfc(dp.tid)), new Timestamp(dp.timestamp), dp.tid)
      //Static projections cannot be used for rows with dimensions
      case _ => null
    }
  }

  def getDataPointGridFunctionFallback(requiredColumns: Array[String]): DataPoint => Row = {
    val tsmc = Spark.getSparkStorage.timeSeriesMembersCache
    val tssfc = Spark.getSparkStorage.timeSeriesScalingFactorCache
    val btstc = Spark.getBroadcastedTimeSeriesTransformationCache
    val columns = requiredColumns.map(EngineUtilities.dataPointViewNameToIndex)
    dp => Row.fromSeq(columns.map({
      case 1 => dp.tid
      case 2 => new Timestamp(dp.timestamp)
      case 3 => btstc.value(dp.tid).transform(dp.value, tssfc(dp.tid))
      case index => tsmc(dp.tid)(index - 4)
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
}
