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

import dk.aau.modelardb.core.models.Segment
import dk.aau.modelardb.core.utility.{CubeFunction, Static}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoders, SparkSession, functions}

import java.sql.Timestamp
import java.util.Calendar
import scala.collection.mutable

//Implementation of simple user-defined aggregate functions on top of the Segment View
//The name of the variables in the case classes should match the schema of the segment view to make sub-queries intuitive
case class UDAFCountInput(tid: Int, start_time: Timestamp, end_time: Timestamp) //CountS only require the tid and timestamps
case class UDAFInput(tid: Int, start_time: Timestamp, end_time: Timestamp, mtid: Integer, model: Array[Byte], gaps: Array[Byte])

//UDAFs for Simple Aggregates
//Count
class CountS extends Aggregator[UDAFCountInput, Long, Long] {

  /** Public Methods **/
  override def zero: Long = 0L

  override def reduce(total: Long, input: UDAFCountInput): Long = {
    total + ((input.end_time.getTime - input.start_time.getTime) / this.timeSeriesSamplingIntervalCache(input.tid)) + 1
  }

  override def merge(total1: Long, total2: Long): Long = {
    total1 + total2
  }

  override def finish(result: Long): Long = result

  override def bufferEncoder: org.apache.spark.sql.Encoder[Long] = Encoders.scalaLong

  override def outputEncoder: org.apache.spark.sql.Encoder[Long] = Encoders.scalaLong

  /** Instance Variables **/
  private val timeSeriesSamplingIntervalCache: Array[Int] = Spark.getSparkStorage.timeSeriesSamplingIntervalCache
}

//Min
class MinS extends Aggregator[UDAFInput, Float, Option[Float]] {

  /** Public Methods **/
  override def zero: Float = Float.PositiveInfinity

  override def reduce(currentMin: Float, input: UDAFInput): Float = {
    Math.min(currentMin, this.inputToSegment(input).min() / this.timeSeriesScalingFactorCache(input.tid))
  }

  override def merge(total1: Float, total2: Float): Float = {
    Math.min(total1, total2)
  }

  override def finish(result: Float): Option[Float] = {
    if (result == Float.PositiveInfinity) {
      Option.empty
    } else {
      Option.apply(result)
    }
  }

  override def bufferEncoder: org.apache.spark.sql.Encoder[Float] = Encoders.scalaFloat

  override def outputEncoder: org.apache.spark.sql.Encoder[Option[Float]] = ExpressionEncoder()

  /** Instance Variables **/
  private val inputToSegment: UDAFInput => Segment = SparkUDAF.getInputToSegment
  private val timeSeriesScalingFactorCache: Array[Float] = Spark.getSparkStorage.timeSeriesScalingFactorCache
}

//Max
class MaxS extends Aggregator[UDAFInput, Float, Option[Float]] {

  /** Public Methods **/
  override def zero: Float = Float.NegativeInfinity

  override def reduce(currentMax: Float, input: UDAFInput): Float = {
    Math.max(currentMax, this.inputToSegment(input).max() / this.scalingCache(input.tid))
  }

  override def merge(total1: Float, total2: Float): Float = {
    Math.max(total1, total2)
  }

  override def finish(result: Float): Option[Float] = {
    if (result == Float.NegativeInfinity) {
      Option.empty
    } else {
      Option.apply(result)
    }
  }

  override def bufferEncoder: org.apache.spark.sql.Encoder[Float] = Encoders.scalaFloat

  override def outputEncoder: org.apache.spark.sql.Encoder[Option[Float]] = ExpressionEncoder()

  /** Instance Variables **/
  private val inputToSegment: UDAFInput => Segment = SparkUDAF.getInputToSegment
  private val scalingCache: Array[Float] = Spark.getSparkStorage.timeSeriesScalingFactorCache
}

//Sum
class SumS extends Aggregator[UDAFInput, Option[Double], Option[Double]] {

  /** Public Methods **/
  override def zero: Option[Double] = None

  override def reduce(currentSum: Option[Double], input: UDAFInput): Option[Double] = {
    Option.apply(currentSum.getOrElse(0.0) + (this.inputToSegment(input).sum() / this.timeSeriesScalingFactorCache(input.tid)))
  }

  override def merge(total1: Option[Double], total2: Option[Double]): Option[Double] = {
    if (total1.isEmpty && total2.isEmpty) {
      Option.empty
    } else {
      Option.apply(total1.getOrElse(0.0) + total2.getOrElse(0.0))
    }
  }

  override def finish(result: Option[Double]): Option[Double] = {
    if (result.isEmpty) {
      Option.empty
    } else {
      result
    }
  }

  override def bufferEncoder: org.apache.spark.sql.Encoder[Option[Double]] =  ExpressionEncoder()

  override def outputEncoder: org.apache.spark.sql.Encoder[Option[Double]] = ExpressionEncoder()

  /** Instance Variables **/
  private val inputToSegment: UDAFInput => Segment = SparkUDAF.getInputToSegment
  private val timeSeriesScalingFactorCache: Array[Float] = Spark.getSparkStorage.timeSeriesScalingFactorCache
}

//Avg
class AvgS extends Aggregator[UDAFInput, (Double, Long), Option[Double]] {

  /** Public Methods **/
  override def zero: (Double, Long) = (0.0, 0L)

  override def reduce(currentAvg: (Double, Long), input: UDAFInput): (Double, Long) = {
    val segment = this.inputToSegment(input)
    (currentAvg._1 + (segment.sum() / this.timeSeriesScalingFactorCache(input.tid)), currentAvg._2 + segment.length())
  }

  override def merge(total1: (Double, Long), total2: (Double, Long)): (Double, Long) = {
    (total1._1 + total2._1, total1._2 + total2._2)
  }

  override def finish(result: (Double, Long)): Option[Double] = {
    if (result._2 == 0) {
      Option.empty
    } else {
      Option.apply(result._1 / result._2)
    }
  }

  override def bufferEncoder: org.apache.spark.sql.Encoder[(Double, Long)] =  ExpressionEncoder()

  override def outputEncoder: org.apache.spark.sql.Encoder[Option[Double]] = ExpressionEncoder()

  /** Instance Variables **/
  private val inputToSegment: UDAFInput => Segment = SparkUDAF.getInputToSegment
  private val timeSeriesScalingFactorCache: Array[Float] = Spark.getSparkStorage.timeSeriesScalingFactorCache
}

//UDAFs for Time-based Aggregates
//TimeUDAF
abstract class TimeUDAF[OUT](val size: Int) extends Aggregator[UDAFInput, Array[Double], OUT] {

  /** Public Methods **/
  override def zero: Array[Double] = Array.fill(size){this.default}

  override def reduce(currentCount: Array[Double], input: UDAFInput): Array[Double] = {
    this.inputToSegment(input).cube(calendar, this.level, this.aggregate, currentCount)
  }

  override def merge(total1: Array[Double], total2: Array[Double]): Array[Double] = {
    for (i <- total1.indices){
      total1(i) += total2(i)
    }
    total1
  }

  override def bufferEncoder: org.apache.spark.sql.Encoder[Array[Double]] =  ExpressionEncoder()

  /** Instance Variables **/
  protected val inputToSegment: UDAFInput => Segment = SparkUDAF.getInputToSegment
  protected val calendar: Calendar = Calendar.getInstance()

  //The hierarchy level, the default value, and the aggregation function should be overwritten by each subclass
  protected val level: Int
  protected val default: Double = 0.0
  protected val aggregate: CubeFunction
  protected val timeSeriesScalingFactorCache: Array[Float] = Spark.getSparkStorage.timeSeriesScalingFactorCache
}

class TimeCount(override val level: Int, override val size: Int) extends TimeUDAF[Map[Int, Long]](size) {

  /** Public Methods **/
  override def outputEncoder: org.apache.spark.sql.Encoder[Map[Int, Long]] = ExpressionEncoder()

  override def finish(total: Array[Double]): Map[Int, Long] = {
    val result = mutable.HashMap[Int, Long]()
    total.zipWithIndex.filter(_._1 != this.default).foreach(t => {
      result(t._2) = t._1.longValue()
    })
    scala.collection.immutable.SortedMap[Int, Long]() ++ result
  }

  /** Instance Variables **/
  override protected val aggregate: CubeFunction = (segment: Segment, _: Int, field: Int, total: Array[Double]) => {
    total(field) = total(field) + segment.length.toDouble
  }
}

class TimeMin(override val level: Int, override val size: Int) extends TimeUDAF[Map[Int, Float]](size) {

  /** Public Methods **/
  override def outputEncoder: org.apache.spark.sql.Encoder[Map[Int, Float]] = ExpressionEncoder()

  override def merge(total1: Array[Double], total2: Array[Double]): Array[Double] = {
    for (i <- total1.indices){
      total1(i) = Math.min(total1(i), total2(i))
    }
    total1
  }

  override def finish(total: Array[Double]): Map[Int, Float] = {
    val result = mutable.HashMap[Int, Float]()
    total.zipWithIndex.filter(_._1 != this.default).foreach(t => {
      result(t._2) = t._1.toFloat
    })
    scala.collection.immutable.SortedMap[Int, Float]() ++ result
  }

  /** Instance Variables **/
  override protected val aggregate: CubeFunction = (segment: Segment, tid: Int, field: Int, total: Array[Double]) => {
    total(field) = Math.min(total(field).toFloat, segment.min / this.timeSeriesScalingFactorCache(tid))
  }
  override protected val default: Double = Double.PositiveInfinity
}

class TimeMax(override val level: Int, override val size: Int) extends TimeUDAF[Map[Int, Float]](size) {

  /** Public Methods **/
  override def outputEncoder: org.apache.spark.sql.Encoder[Map[Int, Float]] = ExpressionEncoder()

  override def merge(total1: Array[Double], total2: Array[Double]): Array[Double] = {
    for (i <- total1.indices){
      total1(i) = Math.max(total1(i), total2(i))
    }
    total1
  }

  override def finish(total: Array[Double]): Map[Int, Float] = {
    val result = mutable.HashMap[Int, Float]()
    total.zipWithIndex.filter(_._1 != this.default).foreach(t => {
      result(t._2) = t._1.toFloat
    })
    scala.collection.immutable.SortedMap[Int, Float]() ++ result
  }

  /** Instance Variables **/
  override protected val aggregate: CubeFunction = (segment: Segment, tid: Int, field: Int, total: Array[Double]) => {
    total(field) = Math.max(total(field).toFloat, segment.max / this.timeSeriesScalingFactorCache(tid))
  }
  override protected val default: Double = Double.NegativeInfinity
}

class TimeSum(override val level: Int, override val size: Int) extends TimeUDAF[Map[Int, Double]](size) {

  /** Public Methods **/
  override def outputEncoder: org.apache.spark.sql.Encoder[Map[Int, Double]] = ExpressionEncoder()

  override def finish(total: Array[Double]): Map[Int, Double] = {
    val sums = total.length / 2
    val result = mutable.HashMap[Int, Double]()
    for (i <- 0 until sums) {
      val hasSum = sums + i - 1
      if (total(hasSum) >= 1.0) { //Merge sums the indicators
        result(i) = total(i)
      }
    }
    scala.collection.immutable.SortedMap[Int, Double]() ++ result
  }

  /** Instance Variables **/
  override protected val aggregate: CubeFunction = (segment: Segment, tid: Int, field: Int, total: Array[Double]) => {
    //HACK: as field is continuous all indicators that values were added are stored after the sum
    val hasSum = (size / 2) + field - 1
    total(field) = total(field) + (segment.sum() / this.timeSeriesScalingFactorCache(tid))
    total(hasSum) = 1.0
  }
}

class TimeAvg(override val level: Int, override val size: Int) extends TimeUDAF[Map[Int, Double]](size) {

  /** Public Methods **/
  override def outputEncoder: org.apache.spark.sql.Encoder[Map[Int, Double]] = ExpressionEncoder()

  override def finish(total: Array[Double]): Map[Int, Double] = {
    val sums = total.length / 2
    val result = mutable.HashMap[Int, Double]()
    for (i <- 0 until sums) {
      val count = sums + i - 1
      if (total(count) != 0.0) {
        result(i) = total(i) / total(count)
      }
    }
    scala.collection.immutable.SortedMap[Int, Double]() ++ result
  }

  /** Instance Variables **/
  override protected val aggregate: CubeFunction = (segment: Segment, tid: Int, field: Int, total: Array[Double]) => {
    //HACK: as field is continuous all of the counts are stored after the sum
    val count = (size / 2) + field - 1
    total(field) = total(field) + (segment.sum / this.timeSeriesScalingFactorCache(tid))
    total(count) = total(count) + segment.length
  }
}

//Connecting UDFs and UDAFs to Spark
object SparkUDAF {

  /** Public Methods **/
  def initialize(spark: SparkSession): Unit = {
    spark.udf.register("COUNT_S", functions.udaf(new CountS))
    spark.udf.register("MIN_S", functions.udaf(new MinS))
    spark.udf.register("MAX_S", functions.udaf(new MaxS))
    spark.udf.register("SUM_S", functions.udaf(new SumS))
    spark.udf.register("AVG_S", functions.udaf(new AvgS))

    //Some useful aggregates cannot be performed as DateUtils3 cannot round some fields
    //A somewhat realistic upper bound of the year 2500 is set for *_YEAR to preserve memory
    spark.udf.register("COUNT_YEAR", functions.udaf(new TimeCount(1, 2501)))
    spark.udf.register("COUNT_MONTH", functions.udaf(new TimeCount(2, 13)))
    spark.udf.register("COUNT_DAY_OF_MONTH", functions.udaf(new TimeCount(5, 32)))
    spark.udf.register("COUNT_AM_PM", functions.udaf(new TimeCount(9, 3)))
    spark.udf.register("COUNT_HOUR", functions.udaf(new TimeCount(10, 25)))
    spark.udf.register("COUNT_HOUR_OF_DAY", functions.udaf(new TimeCount(11, 25)))
    spark.udf.register("COUNT_MINUTE", functions.udaf(new TimeCount(12, 61)))
    spark.udf.register("COUNT_SECOND", functions.udaf(new TimeCount(13, 61)))
    spark.udf.register("MIN_YEAR", functions.udaf(new TimeMin(1, 2501)))
    spark.udf.register("MIN_MONTH", functions.udaf(new TimeMin(2, 13)))
    spark.udf.register("MIN_DAY_OF_MONTH", functions.udaf(new TimeMin(5, 32)))
    spark.udf.register("MIN_AM_PM", functions.udaf(new TimeMin(9, 3)))
    spark.udf.register("MIN_HOUR", functions.udaf(new TimeMin(10, 25)))
    spark.udf.register("MIN_HOUR_OF_DAY", functions.udaf(new TimeMin(11, 25)))
    spark.udf.register("MIN_MINUTE", functions.udaf(new TimeMin(12, 61)))
    spark.udf.register("MIN_SECOND", functions.udaf(new TimeMin(13, 61)))
    spark.udf.register("MAX_YEAR", functions.udaf(new TimeMax(1, 2501)))
    spark.udf.register("MAX_MONTH", functions.udaf(new TimeMax(2, 13)))
    spark.udf.register("MAX_DAY_OF_MONTH", functions.udaf(new TimeMax(5, 32)))
    spark.udf.register("MAX_AM_PM", functions.udaf(new TimeMax(9, 3)))
    spark.udf.register("MAX_HOUR", functions.udaf(new TimeMax(10, 25)))
    spark.udf.register("MAX_HOUR_OF_DAY", functions.udaf(new TimeMax(11, 25)))
    spark.udf.register("MAX_MINUTE", functions.udaf(new TimeMax(12, 61)))
    spark.udf.register("MAX_SECOND", functions.udaf(new TimeMax(13, 61)))
    spark.udf.register("SUM_YEAR", functions.udaf(new TimeSum(1, 5002)))
    spark.udf.register("SUM_MONTH", functions.udaf(new TimeSum(2, 26)))
    spark.udf.register("SUM_DAY_OF_MONTH", functions.udaf(new TimeSum(5, 64)))
    spark.udf.register("SUM_AM_PM", functions.udaf(new TimeSum(9, 6)))
    spark.udf.register("SUM_HOUR", functions.udaf(new TimeSum(10, 50)))
    spark.udf.register("SUM_HOUR_OF_DAY", functions.udaf(new TimeSum(11, 50)))
    spark.udf.register("SUM_MINUTE", functions.udaf(new TimeSum(12, 122)))
    spark.udf.register("SUM_SECOND", functions.udaf(new TimeSum(13, 122)))
    spark.udf.register("AVG_YEAR", functions.udaf(new TimeAvg(1, 5002)))
    spark.udf.register("AVG_MONTH", functions.udaf(new TimeAvg(2, 26)))
    spark.udf.register("AVG_DAY_OF_MONTH", functions.udaf(new TimeAvg(5, 64)))
    spark.udf.register("AVG_AM_PM", functions.udaf(new TimeAvg(9, 6)))
    spark.udf.register("AVG_HOUR", functions.udaf(new TimeAvg(10, 50)))
    spark.udf.register("AVG_HOUR_OF_DAY", functions.udaf(new TimeAvg(11, 50)))
    spark.udf.register("AVG_MINUTE", functions.udaf(new TimeAvg(12, 122)))
    spark.udf.register("AVG_SECOND", functions.udaf(new TimeAvg(13, 122)))

    spark.udf.register("START", functions.udf(start _))
    spark.udf.register("END", functions.udf(end _))
    spark.udf.register("START_END", functions.udf(interval _))
  }

  //User-defined Functions
  def start(tid: Int, st: Timestamp, endTime: Timestamp, mtid: Int,
            model: Array[Byte], gaps: Array[Byte], newStartTime: Timestamp): Array[UDAFInput] = {
    val samplingInterval = Spark.getSparkStorage.timeSeriesSamplingIntervalCache(tid)
    val offsets = Static.bytesToInts(gaps)
    val fromTime = Segment.start(newStartTime.getTime, st.getTime, endTime.getTime, samplingInterval, offsets)
    val updatedGaps = Static.intToBytes(offsets)
    Array(UDAFInput(tid, new Timestamp(fromTime), endTime, mtid, model, updatedGaps))
  }

  def end(tid: Int, startTime: Timestamp, endTime: Timestamp, mtid: Int,
          model: Array[Byte], gaps: Array[Byte], newEndTime: Timestamp):  Array[UDAFInput]  = {
    val samplingInterval = Spark.getSparkStorage.timeSeriesSamplingIntervalCache(tid)
    val toTime = Segment.end(newEndTime.getTime, startTime.getTime, endTime.getTime, samplingInterval)
    Array(UDAFInput(tid, startTime, new Timestamp(toTime), mtid, model, gaps))
  }

  def interval(tid: Int, startTime: Timestamp, endTime: Timestamp, mtid: Int,
               model: Array[Byte], gaps: Array[Byte], newStartTime: Timestamp, newEndTime: Timestamp): Array[UDAFInput] = {
    val samplingInterval = Spark.getSparkStorage.timeSeriesSamplingIntervalCache(tid)
    val offsets = Static.bytesToInts(gaps)
    val fromTime = Segment.start(newStartTime.getTime, startTime.getTime, endTime.getTime, samplingInterval, offsets)
    val toTime = Segment.end(newEndTime.getTime, startTime.getTime, endTime.getTime, samplingInterval)
    val updatedGaps = Static.intToBytes(offsets)
    Array(UDAFInput(tid, new Timestamp(fromTime), new Timestamp(toTime), mtid, model, updatedGaps))
  }

  def getInputToSegment: UDAFInput => Segment = {
    val mtc = Spark.getSparkStorage.modelTypeCache
    val tssic = Spark.getSparkStorage.timeSeriesSamplingIntervalCache
    input => {
      mtc(input.mtid).get(input.tid, input.start_time.getTime, input.end_time.getTime, tssic(input.tid), input.model, input.gaps)
    }
  }
}
