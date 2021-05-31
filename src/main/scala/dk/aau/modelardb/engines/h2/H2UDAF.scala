/* Copyright 2021 The ModelarDB Contributors
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
package dk.aau.modelardb.engines.h2

import dk.aau.modelardb.core.models.{ModelType, Segment}
import dk.aau.modelardb.core.utility.{CubeFunction, Static}
import org.h2.api.AggregateFunction

import java.sql.{Connection, Statement, Timestamp}
import java.util.Calendar
import scala.collection.mutable

//UDAFs for Simple Aggregates
//Count
class CountS extends AggregateFunction {

  /** Public Methods **/
  override def init(conn: Connection): Unit = {
    this.tssic = H2.h2storage.timeSeriesSamplingIntervalCache
  }

  override def getType(inputTypes: Array[Int]): Int = {
    java.sql.Types.BIGINT
  }

  override def add(row: Any): Unit = {
    val values = row.asInstanceOf[Array[Object]]
    val si = this.tssic(values(0).asInstanceOf[java.lang.Integer])
    val st = values(1).asInstanceOf[java.sql.Timestamp]
    val et = values(2).asInstanceOf[java.sql.Timestamp]
    this.count = this.count + ((et.getTime - st.getTime) / si) + 1
  }

  override def getResult: AnyRef = {
    this.count.asInstanceOf[AnyRef]
  }

  /** Instance Variables **/
  private var count: Long = 0
  private var tssic: Array[Int] = _
}

//Min
class MinS extends AggregateFunction {

  /** Public Methods **/
  override def init(conn: Connection): Unit = {
    this.mtc = H2.h2storage.modelTypeCache
    this.tssfc = H2.h2storage.timeSeriesScalingFactorCache
    this.tssic = H2.h2storage.timeSeriesSamplingIntervalCache
  }

  override def getType(inputTypes: Array[Int]): Int = {
    java.sql.Types.FLOAT
  }

  override def add(row: Any): Unit = {
    val segment = rowToSegment(row)
    this.min = Math.min(this.min, segment.min() / this.tssfc(segment.tid))
  }

  override def getResult: AnyRef = {
    if (this.min == Float.PositiveInfinity) {
      null
    } else {
      this.min.asInstanceOf[AnyRef]
    }
  }

  def rowToSegment(row: Any): Segment = {
    val values = row.asInstanceOf[Array[Object]]
    this.mtc(values(3).asInstanceOf[Int]).get(
      values(0).asInstanceOf[Int], values(1).asInstanceOf[Timestamp].getTime, values(2).asInstanceOf[Timestamp].getTime,
      this.tssic(values(0).asInstanceOf[Int]), values(4).asInstanceOf[Array[Byte]], values(5).asInstanceOf[Array[Byte]])
  }

  /** Instance Variables **/
  private var min: Float = Float.PositiveInfinity
  private var mtc: Array[ModelType] = _
  private var tssfc: Array[Float] = _
  private var tssic: Array[Int] = _
}

//Max
class MaxS extends AggregateFunction {

  /** Public Methods **/
  override def init(conn: Connection): Unit = {
    this.mtc = H2.h2storage.modelTypeCache
    this.tssfc = H2.h2storage.timeSeriesScalingFactorCache
    this.tssic = H2.h2storage.timeSeriesSamplingIntervalCache
  }

  override def getType(inputTypes: Array[Int]): Int = {
    java.sql.Types.FLOAT
  }

  override def add(row: Any): Unit = {
    val segment = rowToSegment(row)
    this.max = Math.max(this.max, segment.max() / this.tssfc(segment.tid))
  }

  override def getResult: AnyRef = {
    if (this.max == Float.NegativeInfinity) {
      null
    } else {
      this.max.asInstanceOf[AnyRef]
    }
  }

  def rowToSegment(row: Any): Segment = {
    val values = row.asInstanceOf[Array[Object]]
    this.mtc(values(3).asInstanceOf[Int]).get(
      values(0).asInstanceOf[Int], values(1).asInstanceOf[Timestamp].getTime, values(2).asInstanceOf[Timestamp].getTime,
      this.tssic(values(0).asInstanceOf[Int]), values(4).asInstanceOf[Array[Byte]], values(5).asInstanceOf[Array[Byte]])
  }

  /** Instance Variables **/
  private var max: Float = Float.NegativeInfinity
  private var mtc: Array[ModelType] = _
  private var tssfc: Array[Float] = _
  private var tssic: Array[Int] = _
}

//Sum
class SumS extends AggregateFunction {

  /** Public Methods **/
  override def init(conn: Connection): Unit = {
    this.mtc = H2.h2storage.modelTypeCache
    this.tssfc = H2.h2storage.timeSeriesScalingFactorCache
    this.tssic = H2.h2storage.timeSeriesSamplingIntervalCache
  }

  override def getType(inputTypes: Array[Int]): Int = {
    java.sql.Types.FLOAT
  }

  override def add(row: Any): Unit = {
    val segment = rowToSegment(row)
    this.sum += rowToSegment(row).sum() / this.tssfc(segment.tid)
    this.added = true
  }

  override def getResult: AnyRef = {
    if (this.added) {
      this.sum.toFloat.asInstanceOf[AnyRef]
    } else {
      null
    }
  }

  def rowToSegment(row: Any): Segment = {
    val values = row.asInstanceOf[Array[Object]]
    this.mtc(values(3).asInstanceOf[Int]).get(
      values(0).asInstanceOf[Int], values(1).asInstanceOf[Timestamp].getTime, values(2).asInstanceOf[Timestamp].getTime,
      this.tssic(values(0).asInstanceOf[Int]), values(4).asInstanceOf[Array[Byte]], values(5).asInstanceOf[Array[Byte]])
  }

  /** Instance Variables **/
  private var sum: Double = 0.0
  private var added = false
  private var mtc: Array[ModelType] = _
  private var tssfc: Array[Float] = _
  private var tssic: Array[Int] = _
}

//Avg
class AvgS extends AggregateFunction {

  /** Public Methods **/
  override def init(conn: Connection): Unit = {
    this.mtc = H2.h2storage.modelTypeCache
    this.tssfc = H2.h2storage.timeSeriesScalingFactorCache
    this.tssic = H2.h2storage.timeSeriesSamplingIntervalCache
  }

  override def getType(inputTypes: Array[Int]): Int = {
    java.sql.Types.FLOAT
  }

  override def add(row: Any): Unit = {
    val segment = rowToSegment(row)
    this.sum += segment.sum() / this.tssfc(segment.tid)
    this.count += segment.length()
  }

  override def getResult: AnyRef = {
    if (this.count == 0) {
      null
    } else {
      (this.sum / this.count).toFloat.asInstanceOf[AnyRef]
    }
  }

  def rowToSegment(row: Any): Segment = {
    val values = row.asInstanceOf[Array[Object]]
    this.mtc(values(3).asInstanceOf[Int]).get(
      values(0).asInstanceOf[Int], values(1).asInstanceOf[Timestamp].getTime, values(2).asInstanceOf[Timestamp].getTime,
      this.tssic(values(0).asInstanceOf[Int]), values(4).asInstanceOf[Array[Byte]], values(5).asInstanceOf[Array[Byte]])
  }

  /** Instance Variables **/
  private var sum: Double = 0.0
  private var count: Long = 0
  private var mtc: Array[ModelType] = _
  private var tssfc: Array[Float] = _
  private var tssic: Array[Int] = _
}

//UDAFs for Time-based Aggregates
abstract class TimeAggregate(level: Int, bufferSize: Int, initialValue: Double) extends AggregateFunction {

  /** Public Methods **/
  override def init(conn: Connection): Unit = {
    this.mtc = H2.h2storage.modelTypeCache
    this.tssfc = H2.h2storage.timeSeriesScalingFactorCache
    this.tssic = H2.h2storage.timeSeriesSamplingIntervalCache
  }

  override def getType(inputTypes: Array[Int]): Int = {
    java.sql.Types.JAVA_OBJECT
  }

  override def add(row: Any): Unit = {
    rowToSegment(row).cube(this.calendar, level, this.aggregate, this.current)
  }

  override def getResult: AnyRef = {
    val result = mutable.HashMap[Int, Double]()
    this.current.zipWithIndex.filter(_._1 != initialValue).foreach(t => {
      result(t._2) = t._1
    })

    if (result.isEmpty) {
      null
    } else {
      scala.collection.immutable.SortedMap[Int, Double]() ++ result
    }
  }

  def rowToSegment(row: Any): Segment = {
    val values = row.asInstanceOf[Array[Object]]
    this.mtc(values(3).asInstanceOf[Int]).get(
      values(0).asInstanceOf[Int], values(1).asInstanceOf[Timestamp].getTime, values(2).asInstanceOf[Timestamp].getTime,
      this.tssic(values(0).asInstanceOf[Int]), values(4).asInstanceOf[Array[Byte]], values(5).asInstanceOf[Array[Byte]])
  }

  /** Instance Variables **/
  private val calendar = Calendar.getInstance()
  private var mtc: Array[ModelType] = _
  protected var tssfc: Array[Float] = _
  protected var tssic: Array[Int] = _
  protected val current: Array[Double] = Array.fill(bufferSize){initialValue}
  protected val aggregate: CubeFunction
}

//CountTime
class CountTime(level: Int, bufferSize: Int) extends TimeAggregate(level, bufferSize, 0.0) {

  /** Public Methods **/
  override def getResult: AnyRef = {
    val result = mutable.HashMap[Int, Long]()
    this.current.zipWithIndex.filter(_._1 != 0).foreach(t => {
      result(t._2) = t._1.longValue()
    })

    if (result.isEmpty) {
      null
    } else {
      scala.collection.immutable.SortedMap[Int, Long]() ++ result
    }
  }

  /** Instance Variables **/
  override protected val aggregate: CubeFunction = (segment: Segment, _: Int, field: Int, total: Array[Double]) => {
    total(field) = total(field) + segment.length.toDouble
  }
}
//Some useful aggregates cannot be performed as DateUtils3 cannot round some fields
//A somewhat realistic upper bound of the year 2500 is set for *_YEAR to preserve memory
class CountYear extends CountTime(2, 2501)
class CountMonth extends CountTime(2, 13)
class CountDayOfMonth extends CountTime(2, 32)
class CountAmPm extends CountTime(2, 3)
class CountHour extends CountTime(2, 25)
class CountHourOfDay extends CountTime(2, 25)
class CountMinute extends CountTime(2, 61)
class CountSecond extends CountTime(2, 61)

//MinTime
class MinTime(level: Int, bufferSize: Int) extends TimeAggregate(level, bufferSize, Double.MaxValue) {
  override protected val aggregate: CubeFunction = (segment: Segment, _: Int, field: Int, total: Array[Double]) => {
    total(field) = Math.min(total(field), segment.min() / this.tssfc(segment.tid))
  }
}
class MinYear extends MinTime(2, 2501)
class MinMonth extends MinTime(2, 13)
class MinDayOfMonth extends MinTime(2, 32)
class MinAmPm extends MinTime(2, 3)
class MinHour extends MinTime(2, 25)
class MinHourOfDay extends MinTime(2, 25)
class MinMinute extends MinTime(2, 61)
class MinSecond extends MinTime(2, 61)

//MaxTime
class MaxTime(level: Int, bufferSize: Int) extends TimeAggregate(level, bufferSize, Double.MinValue) {
  override protected val aggregate: CubeFunction = (segment: Segment, _: Int, field: Int, total: Array[Double]) => {
    total(field) = Math.max(total(field), segment.max() / this.tssfc(segment.tid))
  }
}
class MaxYear extends MaxTime(2, 2501)
class MaxMonth extends MaxTime(2, 13)
class MaxDayOfMonth extends MaxTime(2, 32)
class MaxAmPm extends MaxTime(2, 3)
class MaxHour extends MaxTime(2, 25)
class MaxHourOfDay extends MaxTime(2, 25)
class MaxMinute extends MaxTime(2, 61)
class MaxSecond extends MaxTime(2, 61)

//SumTime
class SumTime(level: Int, bufferSize: Int) extends TimeAggregate(level, bufferSize, 0.0) {
  override protected val aggregate: CubeFunction = (segment: Segment, _: Int, field: Int, total: Array[Double]) => {
    total(field) = total(field) + (segment.sum() / this.tssfc(segment.tid))
  }
}
class SumYear extends SumTime(2, 2501)
class SumMonth extends SumTime(2, 13)
class SumDayOfMonth extends SumTime(2, 32)
class SumAmPm extends SumTime(2, 3)
class SumHour extends SumTime(2, 25)
class SumHourOfDay extends SumTime(2, 25)
class SumMinute extends SumTime(2, 61)
class SumSecond extends SumTime(2, 61)

//AvgTime
class AvgTime(level: Int, bufferSize: Int) extends TimeAggregate(level, 2 * bufferSize, 0.0) {

  /** Public Methods **/
  override def getResult: AnyRef = {
    val sums = this.current.length / 2
    val result = mutable.HashMap[Int, Double]()
    for (i <- 0 until sums) {
      val count = sums + i - 1
      if (this.current(count) != 0.0) {
        result(i) = this.current(i) / this.current(count)
      }
    }

    if (result.isEmpty) {
      null
    } else {
      scala.collection.immutable.SortedMap[Int, Double]() ++ result
    }
  }

  /** Instance Variables **/
  override protected val aggregate: CubeFunction = (segment: Segment, _: Int, field: Int, total: Array[Double]) => {
    //HACK: as field is continuous all of the counts are stored after the sum
    val count = bufferSize + field - 1
    total(field) = total(field) + (segment.sum / this.tssfc(segment.tid))
    total(count) = total(count) + segment.length
  }
}
class AvgYear extends AvgTime(2, 2501)
class AvgMonth extends AvgTime(2, 13)
class AvgDayOfMonth extends AvgTime(2, 32)
class AvgAmPm extends AvgTime(2, 3)
class AvgHour extends AvgTime(2, 25)
class AvgHourOfDay extends AvgTime(2, 25)
class AvgMinute extends AvgTime(2, 61)
class AvgSecond extends AvgTime(2, 61)

//Connecting UDFs and UDAFs to H2
object H2UDAF {

  /** Public Methods **/
  def initialize(stmt: Statement): Unit = {
    stmt.execute(getCreateUDAFSQL("COUNT_S"))
    stmt.execute(getCreateUDAFSQL("MIN_S"))
    stmt.execute(getCreateUDAFSQL("MAX_S"))
    stmt.execute(getCreateUDAFSQL("SUM_S"))
    stmt.execute(getCreateUDAFSQL("AVG_S"))

    //Some useful aggregates cannot be performed as DateUtils3 cannot round some fields
    //A somewhat realistic upper bound of the year 2500 is set for *_YEAR to preserve memory
    stmt.execute(getCreateUDAFSQL("COUNT_YEAR"))
    stmt.execute(getCreateUDAFSQL("COUNT_MONTH"))
    stmt.execute(getCreateUDAFSQL("COUNT_DAY_OF_MONTH"))
    stmt.execute(getCreateUDAFSQL("COUNT_AM_PM"))
    stmt.execute(getCreateUDAFSQL("COUNT_HOUR"))
    stmt.execute(getCreateUDAFSQL("COUNT_HOUR_OF_DAY"))
    stmt.execute(getCreateUDAFSQL("COUNT_MINUTE"))
    stmt.execute(getCreateUDAFSQL("COUNT_SECOND"))
    stmt.execute(getCreateUDAFSQL("MIN_YEAR"))
    stmt.execute(getCreateUDAFSQL("MIN_MONTH"))
    stmt.execute(getCreateUDAFSQL("MIN_DAY_OF_MONTH"))
    stmt.execute(getCreateUDAFSQL("MIN_AM_PM"))
    stmt.execute(getCreateUDAFSQL("MIN_HOUR"))
    stmt.execute(getCreateUDAFSQL("MIN_HOUR_OF_DAY"))
    stmt.execute(getCreateUDAFSQL("MIN_MINUTE"))
    stmt.execute(getCreateUDAFSQL("MIN_SECOND"))
    stmt.execute(getCreateUDAFSQL("MAX_YEAR"))
    stmt.execute(getCreateUDAFSQL("MAX_MONTH"))
    stmt.execute(getCreateUDAFSQL("MAX_DAY_OF_MONTH"))
    stmt.execute(getCreateUDAFSQL("MAX_AM_PM"))
    stmt.execute(getCreateUDAFSQL("MAX_HOUR"))
    stmt.execute(getCreateUDAFSQL("MAX_HOUR_OF_DAY"))
    stmt.execute(getCreateUDAFSQL("MAX_MINUTE"))
    stmt.execute(getCreateUDAFSQL("MAX_SECOND"))
    stmt.execute(getCreateUDAFSQL("SUM_YEAR"))
    stmt.execute(getCreateUDAFSQL("SUM_MONTH"))
    stmt.execute(getCreateUDAFSQL("SUM_DAY_OF_MONTH"))
    stmt.execute(getCreateUDAFSQL("SUM_AM_PM"))
    stmt.execute(getCreateUDAFSQL("SUM_HOUR"))
    stmt.execute(getCreateUDAFSQL("SUM_HOUR_OF_DAY"))
    stmt.execute(getCreateUDAFSQL("SUM_MINUTE"))
    stmt.execute(getCreateUDAFSQL("SUM_SECOND"))
    stmt.execute(getCreateUDAFSQL("AVG_YEAR"))
    stmt.execute(getCreateUDAFSQL("AVG_MONTH"))
    stmt.execute(getCreateUDAFSQL("AVG_DAY_OF_MONTH"))
    stmt.execute(getCreateUDAFSQL("AVG_AM_PM"))
    stmt.execute(getCreateUDAFSQL("AVG_HOUR"))
    stmt.execute(getCreateUDAFSQL("AVG_HOUR_OF_DAY"))
    stmt.execute(getCreateUDAFSQL("AVG_MINUTE"))
    stmt.execute(getCreateUDAFSQL("AVG_SECOND"))

    stmt.execute("""CREATE ALIAS START FOR "dk.aau.modelardb.engines.h2.H2UDAF.start"""")
    stmt.execute("""CREATE ALIAS END FOR "dk.aau.modelardb.engines.h2.H2UDAF.end"""")
    stmt.execute("""CREATE ALIAS START_END FOR "dk.aau.modelardb.engines.h2.H2UDAF.interval"""")
  }

  def getCreateUDAFSQL(sqlName: String): String = {
    val splitSQLName = sqlName.split("_")
    val className = splitSQLName.map(_.toLowerCase.capitalize).mkString("")
    s"""CREATE AGGREGATE $sqlName FOR "dk.aau.modelardb.engines.h2.$className";"""
  }

  //User-defined Functions
  def start(tid: Int, st: Timestamp, endTime: Timestamp, mtid: Int,
            model: Array[Byte], gaps: Array[Byte], newStartTime: Timestamp): Array[Object] = {
    val samplingInterval = H2.h2storage.timeSeriesSamplingIntervalCache(tid)
    val offsets = Static.bytesToInts(gaps)
    val fromTime = Segment.start(newStartTime.getTime, st.getTime, endTime.getTime, samplingInterval, offsets)
    val updatedGaps = Static.intToBytes(offsets)
    Array(tid.asInstanceOf[Object], new Timestamp(fromTime), endTime,
      samplingInterval.asInstanceOf[Object], mtid.asInstanceOf[Object], model, updatedGaps)
  }

  def end(tid: Int, startTime: Timestamp, endTime: Timestamp, mtid: Int,
          model: Array[Byte], gaps: Array[Byte], newEndTime: Timestamp):  Array[Object] = {
    val samplingInterval = H2.h2storage.timeSeriesSamplingIntervalCache(tid)
    val toTime = Segment.end(newEndTime.getTime, startTime.getTime, endTime.getTime, samplingInterval)
    Array(tid.asInstanceOf[Object], startTime, new Timestamp(toTime),
      samplingInterval.asInstanceOf[Object], mtid.asInstanceOf[Object], model, gaps)
  }

  def interval(tid: Int, startTime: Timestamp, endTime: Timestamp, mtid: Int,
               model: Array[Byte], gaps: Array[Byte], newStartTime: Timestamp, newEndTime: Timestamp): Array[Object] = {
    val samplingInterval = H2.h2storage.timeSeriesSamplingIntervalCache(tid)
    val offsets = Static.bytesToInts(gaps)
    val fromTime = Segment.start(newStartTime.getTime, startTime.getTime, endTime.getTime, samplingInterval, offsets)
    val toTime = Segment.end(newEndTime.getTime, startTime.getTime, endTime.getTime, samplingInterval)
    val updatedGaps = Static.intToBytes(offsets)
    Array(tid.asInstanceOf[Object], new Timestamp(fromTime), new Timestamp(toTime),
      samplingInterval.asInstanceOf[Object], mtid.asInstanceOf[Object], model, updatedGaps)
  }
}
