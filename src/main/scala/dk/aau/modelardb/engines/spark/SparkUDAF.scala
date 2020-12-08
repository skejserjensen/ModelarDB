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
import java.util.Calendar

import dk.aau.modelardb.core.models.Segment
import dk.aau.modelardb.core.utility.{CubeFunction, Static}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable

//Implementation of simple user-defined aggregate functions on top of the segment view
//Count
class CountS extends UserDefinedAggregateFunction {

  /** Public Methods **/
  override def inputSchema: StructType = StructType(Seq(
    StructField("st", TimestampType, nullable = false),
    StructField("et", TimestampType, nullable = false),
    StructField("res", IntegerType, nullable = false)))

  override def bufferSchema: StructType = StructType(StructField("count", LongType) :: Nil)

  override def dataType: DataType = LongType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = buffer(0) = 0L

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + ((input.getTimestamp(1).getTime - input.getTimestamp(0).getTime) / input.getInt(2)) + 1
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
  }

  override def evaluate(buffer: Row): Any = buffer.getLong(0)
}

class CountSS extends CountS {

  /** Public Methods **/
  override def inputSchema: StructType = SparkUDAF.udfType

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val row = input.getStruct(0)
    buffer(0) = buffer.getLong(0) + ((row.getTimestamp(2).getTime - row.getTimestamp(1).getTime) / row.getInt(3)) + 1
  }
}

//Min
class MinS extends UserDefinedAggregateFunction {

  /** Public Methods **/
  override def inputSchema: StructType = SparkUDAF.segmentSchema

  override def bufferSchema: StructType = StructType(StructField("min", FloatType) :: Nil)

  override def dataType: DataType = FloatType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = buffer(0) = Float.PositiveInfinity

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = Math.min(buffer.getFloat(0), this.rowToSegment(input).min() / this.scalingCache(input.getInt(0)))
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = Math.min(buffer1.getFloat(0), buffer2.getFloat(0))
  }

  override def evaluate(buffer: Row): Any = {
    val result = buffer.getFloat(0)
    if (result == Float.PositiveInfinity) {
      null
    } else {
      result
    }
  }

  /** Instance Variables **/
  protected val rowToSegment: Row => Segment = SparkGridder.getRowToSegment
  protected val scalingCache: Array[Float] = Spark.getStorage.getSourceScalingFactorCache
}

class MinSS extends MinS {

  /** Public Methods **/
  override def inputSchema: StructType = SparkUDAF.udfType

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = Math.min(buffer.getFloat(0),
      this.rowToSegment(input.getStruct(0)).min() / this.scalingCache(input.getStruct(0).getInt(0)))
  }
}

//Max
class MaxS extends UserDefinedAggregateFunction {

  /** Public Methods **/
  override def inputSchema: StructType = SparkUDAF.segmentSchema

  override def bufferSchema: StructType = StructType(StructField("max", FloatType) :: Nil)

  override def dataType: DataType = FloatType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = buffer(0) = Float.NegativeInfinity

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = Math.max(buffer.getFloat(0), this.rowToSegment(input).max() / this.scalingCache(input.getInt(0)))
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = Math.max(buffer1.getFloat(0), buffer2.getFloat(0))
  }

  override def evaluate(buffer: Row): Any = {
    val result = buffer.getFloat(0)
    if (result == Float.NegativeInfinity) {
      null
    } else {
      result
    }
  }

  /** Instance Variables **/
  protected val rowToSegment: Row => Segment = SparkGridder.getRowToSegment
  protected val scalingCache: Array[Float] = Spark.getStorage.getSourceScalingFactorCache
}

class MaxSS extends MaxS {

  /** Public Methods **/
  override def inputSchema: StructType = SparkUDAF.udfType

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = Math.max(buffer.getFloat(0),
      this.rowToSegment(input.getStruct(0)).max() / this.scalingCache(input.getStruct(0).getInt(0)))
  }
}

//Sum
class SumS extends UserDefinedAggregateFunction {

  /** Public Methods **/
  override def inputSchema: StructType = SparkUDAF.segmentSchema

  override def bufferSchema: StructType = StructType(StructField("sum", DoubleType) :: StructField("check", BooleanType) :: Nil)

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0.0
    buffer(1) = false
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getDouble(0) + (this.rowToSegment(input).sum() / this.scalingCache(input.getInt(0)))
    buffer(1) = true
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
    buffer1(1) = buffer1.getBoolean(1) || buffer2.getBoolean(1)
  }

  override def evaluate(buffer: Row): Any = {
    if (buffer.getBoolean(1)) {
      buffer.getDouble(0)
    } else {
      null
    }
  }

  /** Instance Variables **/
  protected val rowToSegment: Row => Segment = SparkGridder.getRowToSegment
  protected val scalingCache: Array[Float] = Spark.getStorage.getSourceScalingFactorCache
}

class SumSS extends SumS {

  /** Public Methods **/
  override def inputSchema: StructType = SparkUDAF.udfType

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getDouble(0) +
      (this.rowToSegment(input.getStruct(0)).sum() / this.scalingCache(input.getStruct(0).getInt(0)))
  }
}

//Avg
class AvgS extends UserDefinedAggregateFunction {

  /** Public Methods **/
  override def inputSchema: StructType = SparkUDAF.segmentSchema

  override def bufferSchema: StructType = StructType(
    StructField("sum", DoubleType) :: StructField("count", LongType) :: Nil)

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0.0
    buffer(1) = 0L
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val segment = this.rowToSegment(input)
    buffer(0) = buffer.getDouble(0) + segment.sum() / this.scalingCache(input.getInt(0))
    buffer(1) = buffer.getLong(1) + segment.length()
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  override def evaluate(buffer: Row): Any = {
    val result = buffer.getDouble(0) / buffer.getLong(1).toDouble
    if (result.isNaN) {
      null
    } else {
      result
    }
  }

  /** Instance Variables **/
  protected val rowToSegment: Row => Segment = SparkGridder.getRowToSegment
  protected val scalingCache: Array[Float] = Spark.getStorage.getSourceScalingFactorCache
}

class AvgSS extends AvgS {

  /** Public Methods **/
  override def inputSchema: StructType = SparkUDAF.udfType

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val segment = this.rowToSegment(input.getStruct(0))
    buffer(0) = buffer.getDouble(0) + segment.sum() / this.scalingCache(input.getStruct(0).getInt(0))
    buffer(1) = buffer.getLong(1) + segment.length()
  }
}

//Implementation of user-defined aggregate functions in the time dimension on top of the segment view
//TimeUDAF
abstract class TimeUDAF(val size: Int) extends UserDefinedAggregateFunction {

  /** Public Methods **/
  override def initialize(buffer: MutableAggregationBuffer): Unit = buffer(0) = Array.fill(size){this.default}

  override def inputSchema: StructType = SparkUDAF.segmentSchema

  override def bufferSchema: StructType =
    StructType(StructField("Aggregate", ArrayType(DoubleType, containsNull = false)) :: Nil)

  override def dataType: DataType = MapType(IntegerType, DoubleType, valueContainsNull = false)

  override def deterministic: Boolean = true

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val result = buffer.getAs[mutable.WrappedArray[Double]](0)
    val segment = this.rowToSegment(input)
    buffer(0) = segment.cube(calendar, this.level, this.aggregate, result.toArray)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val result1 = buffer1.getAs[mutable.WrappedArray[Double]](0)
    val result2 = buffer2.getAs[mutable.WrappedArray[Double]](0)
    for (i <- result1.indices){
      result1(i) += result2(i)
    }
    buffer1(0) = result1
  }

  /** Instance Variables **/
  protected val rowToSegment: Row => Segment = SparkGridder.getRowToSegment
  protected val calendar: Calendar = Calendar.getInstance()

  //The hierarchy level, the default value, and the aggregation function should be overwritten by each subclass
  protected val level: Int
  protected val default: Double
  protected val aggregate: CubeFunction
  protected val scalingCache: Array[Float] = Spark.getStorage.getSourceScalingFactorCache
}

class TimeCount(override val level: Int, override val size: Int) extends TimeUDAF(size) {

  /** Public Methods **/
  override def dataType: DataType = MapType(IntegerType, LongType, valueContainsNull = false)

  override protected val aggregate: CubeFunction = new CubeFunction {
    override def aggregate(segment: Segment, sid: Int, field: Int, total: Array[Double]): Unit = {
      total(field) = total(field) + segment.length.toDouble
    }
  }

  override def evaluate(buffer: Row): Any = {
    val result = mutable.HashMap[Int, Long]()
    val total = buffer.getAs[mutable.WrappedArray[Double]](0)
    total.zipWithIndex.filter(_._1 != 0.0).foreach(t => {
      result(t._2) = t._1.longValue()
    })
    scala.collection.immutable.SortedMap[Int, Long]() ++ result
  }

  /** Instance Variables **/
  protected val default: Double = 0.0
}

class TimeMin(override val level: Int, override val size: Int) extends TimeUDAF(size) {

  /** Public Methods **/
  override def dataType: DataType = MapType(IntegerType, FloatType, valueContainsNull = false)

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val result1 = buffer1.getAs[mutable.WrappedArray[Double]](0)
    val result2 = buffer2.getAs[mutable.WrappedArray[Double]](0)
    for (i <- result1.indices){
      result1(i) = Math.min(result1(i), result2(i))
    }
    buffer1(0) = result1
  }

  override protected val aggregate: CubeFunction = new CubeFunction {
    override def aggregate(segment: Segment, sid: Int, field: Int, total: Array[Double]): Unit = {
      total(field) = Math.min(total(field).toFloat, segment.min / scalingCache(sid))
    }
  }

  override def evaluate(buffer: Row): Any = {
    val result = mutable.HashMap[Int, Float]()
    val total = buffer.getAs[mutable.WrappedArray[Double]](0)
    total.zipWithIndex.filter(_._1 != this.default).foreach(t => {
      result(t._2) = t._1.toFloat
    })
    scala.collection.immutable.SortedMap[Int, Float]() ++ result
  }

  /** Instance Variables **/
  protected val default: Double = Double.PositiveInfinity
}

class TimeMax(override val level: Int, override val size: Int) extends TimeUDAF(size) {

  /** Public Methods **/
  override def dataType: DataType = MapType(IntegerType, FloatType, valueContainsNull = false)

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val result1 = buffer1.getAs[mutable.WrappedArray[Double]](0)
    val result2 = buffer2.getAs[mutable.WrappedArray[Double]](0)
    for (i <- result1.indices){
      result1(i) = Math.max(result1(i), result2(i))
    }
    buffer1(0) = result1
  }

  override protected val aggregate: CubeFunction = new CubeFunction {
    override def aggregate(segment: Segment, sid: Int, field: Int, total: Array[Double]): Unit = {
      total(field) = Math.max(total(field).toFloat, segment.max / scalingCache(sid))
    }
  }

  override def evaluate(buffer: Row): Any = {
    val result = mutable.HashMap[Int, Float]()
    val total = buffer.getAs[mutable.WrappedArray[Double]](0)
    total.zipWithIndex.filter(_._1 != this.default).foreach(t => {
      result(t._2) = t._1.toFloat
    })
    scala.collection.immutable.SortedMap[Int, Float]() ++ result
  }

  /** Instance Variables **/
  protected val default: Double = Double.NegativeInfinity
}

class TimeSum(override val level: Int, override val size: Int) extends TimeUDAF(size) {

  /** Public Methods **/
  override protected val aggregate: CubeFunction = new CubeFunction {
    override def aggregate(segment: Segment, sid: Int, field: Int, total: Array[Double]): Unit = {
      //HACK: as field is continuous all indicators that values were added are stored after the sum
      val hasSum = (size / 2) + field - 1
      total(field) = total(field) + (segment.sum() / scalingCache(sid))
      total(hasSum) = 1.0
    }
  }

  override def evaluate(buffer: Row): Any = {
    val total = buffer.getAs[mutable.WrappedArray[Double]](0)
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
  protected val default: Double = 0.0
}

class TimeAvg(override val level: Int, override val size: Int) extends TimeUDAF(size) {

  /** Public Methods **/
  override protected val aggregate: CubeFunction = new CubeFunction {
    override def aggregate(segment: Segment, sid: Int, field: Int, total: Array[Double]): Unit = {
      //HACK: as field is continuous all of the counts are stored after the sum
      val count = (size / 2) + field - 1
      total(field) = total(field) + segment.sum  / scalingCache(sid)
      total(count) = total(count) + segment.length
    }
  }

  override def evaluate(buffer: Row): Any = {
    val total = buffer.getAs[mutable.WrappedArray[Double]](0)
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
  protected val default: Double = 0.0
}

//Helper functions shared between the various UDF and UDAF
object SparkUDAF {

  /** Public Methods **/
  def initialize(spark: SparkSession): Unit = {
    spark.sqlContext.udf.register("COUNT_S", new CountS)
    spark.sqlContext.udf.register("COUNT_SS", new CountSS)
    spark.sqlContext.udf.register("MIN_S", new MinS)
    spark.sqlContext.udf.register("MIN_SS", new MinSS)
    spark.sqlContext.udf.register("MAX_S", new MaxS)
    spark.sqlContext.udf.register("MAX_SS", new MaxSS)
    spark.sqlContext.udf.register("SUM_S", new SumS)
    spark.sqlContext.udf.register("SUM_SS", new SumSS)
    spark.sqlContext.udf.register("AVG_S", new AvgS)
    spark.sqlContext.udf.register("AVG_SS", new AvgSS)

    //Some useful aggregates cannot be performed as DateUtils3 cannot round some fields
    //A somewhat realistic upper bound of the year 2500 is set for *_YEAR to preserve memory
    spark.sqlContext.udf.register("COUNT_YEAR", new TimeCount(1, 2501))
    spark.sqlContext.udf.register("COUNT_MONTH", new TimeCount(2, 13))
    spark.sqlContext.udf.register("COUNT_DAY_OF_MONTH", new TimeCount(5, 32))
    spark.sqlContext.udf.register("COUNT_AM_PM", new TimeCount(9, 3))
    spark.sqlContext.udf.register("COUNT_HOUR", new TimeCount(10, 25))
    spark.sqlContext.udf.register("COUNT_HOUR_OF_DAY", new TimeCount(11, 25))
    spark.sqlContext.udf.register("COUNT_MINUTE", new TimeCount(12, 61))
    spark.sqlContext.udf.register("COUNT_SECOND", new TimeCount(13, 61))
    spark.sqlContext.udf.register("MIN_YEAR", new TimeMin(1, 2501))
    spark.sqlContext.udf.register("MIN_MONTH", new TimeMin(2, 13))
    spark.sqlContext.udf.register("MIN_DAY_OF_MONTH", new TimeMin(5, 32))
    spark.sqlContext.udf.register("MIN_AM_PM", new TimeMin(9, 3))
    spark.sqlContext.udf.register("MIN_HOUR", new TimeMin(10, 25))
    spark.sqlContext.udf.register("MIN_HOUR_OF_DAY", new TimeMin(11, 25))
    spark.sqlContext.udf.register("MIN_MINUTE", new TimeMin(12, 61))
    spark.sqlContext.udf.register("MIN_SECOND", new TimeMin(13, 61))
    spark.sqlContext.udf.register("MAX_YEAR", new TimeMax(1, 2501))
    spark.sqlContext.udf.register("MAX_MONTH", new TimeMax(2, 13))
    spark.sqlContext.udf.register("MAX_DAY_OF_MONTH", new TimeMax(5, 32))
    spark.sqlContext.udf.register("MAX_AM_PM", new TimeMax(9, 3))
    spark.sqlContext.udf.register("MAX_HOUR", new TimeMax(10, 25))
    spark.sqlContext.udf.register("MAX_HOUR_OF_DAY", new TimeMax(11, 25))
    spark.sqlContext.udf.register("MAX_MINUTE", new TimeMax(12, 61))
    spark.sqlContext.udf.register("MAX_SECOND", new TimeMax(13, 61))
    spark.sqlContext.udf.register("SUM_YEAR", new TimeSum(1, 5002))
    spark.sqlContext.udf.register("SUM_MONTH", new TimeSum(2, 26))
    spark.sqlContext.udf.register("SUM_DAY_OF_MONTH", new TimeSum(5, 64))
    spark.sqlContext.udf.register("SUM_AM_PM", new TimeSum(9, 6))
    spark.sqlContext.udf.register("SUM_HOUR", new TimeSum(10, 50))
    spark.sqlContext.udf.register("SUM_HOUR_OF_DAY", new TimeSum(11, 50))
    spark.sqlContext.udf.register("SUM_MINUTE", new TimeSum(12, 122))
    spark.sqlContext.udf.register("SUM_SECOND", new TimeSum(13, 122))
    spark.sqlContext.udf.register("AVG_YEAR", new TimeAvg(1, 5002))
    spark.sqlContext.udf.register("AVG_MONTH", new TimeAvg(2, 26))
    spark.sqlContext.udf.register("AVG_DAY_OF_MONTH", new TimeAvg(5, 64))
    spark.sqlContext.udf.register("AVG_AM_PM", new TimeAvg(9, 6))
    spark.sqlContext.udf.register("AVG_HOUR", new TimeAvg(10, 50))
    spark.sqlContext.udf.register("AVG_HOUR_OF_DAY", new TimeAvg(11, 50))
    spark.sqlContext.udf.register("AVG_MINUTE", new TimeAvg(12, 122))
    spark.sqlContext.udf.register("AVG_SECOND", new TimeAvg(13, 122))

    spark.sqlContext.udf.register("START", start _)
    spark.sqlContext.udf.register("END", end _)
    spark.sqlContext.udf.register("INTERVAL", interval _)
  }

  def start(sid: Int, st: Timestamp, et: Timestamp, res: Int, mid: Int, param: Array[Byte], gaps: Array[Byte], nst: Timestamp) = {
    val offsets = Static.bytesToInts(gaps)
    val fromTime = Segment.start(nst.getTime, st.getTime, et.getTime, res, offsets)
    val updatedGaps = Static.intToBytes(offsets)
    (sid, new Timestamp(fromTime), et, res, mid, param, updatedGaps)
  }

  def end(sid: Int, st: Timestamp, et: Timestamp, res: Int, mid: Int, param: Array[Byte], gaps: Array[Byte], net: Timestamp) = {
    (sid, st, new Timestamp(Segment.end(net.getTime, st.getTime, et.getTime, res)), res, mid, param, gaps)
  }

  def interval(sid: Int, st: Timestamp, et: Timestamp, res: Int, mid: Int, param: Array[Byte], gaps: Array[Byte], nst: Timestamp, net: Timestamp) = {
    val offsets = Static.bytesToInts(gaps)
    val fromTime = Segment.start(nst.getTime, st.getTime, et.getTime, res, offsets)
    val endTime = Segment.end(net.getTime, st.getTime, et.getTime, res)
    val updatedGaps = Static.intToBytes(offsets)
    (sid, new Timestamp(fromTime), new Timestamp(endTime), res, mid, param, updatedGaps)
  }

  /** Instance Variables **/
  val segmentSchema = StructType(Seq(
    StructField("sid", IntegerType, nullable = false),
    StructField("st", TimestampType, nullable = false),
    StructField("et", TimestampType, nullable = false),
    StructField("res", IntegerType, nullable = false),
    StructField("mid", IntegerType, nullable = false),
    StructField("param", BinaryType, nullable = false),
    StructField("gaps", BinaryType, nullable = false)))

  val udfType = StructType(Seq(
    StructField("udf",
      StructType(Seq(
        StructField("_1", IntegerType, nullable = false),
        StructField("_2", TimestampType, nullable = false),
        StructField("_3", TimestampType, nullable = false),
        StructField("_4", IntegerType, nullable = false),
        StructField("_5", IntegerType, nullable = false),
        StructField("_6", BinaryType, nullable = false),
        StructField("_7", BinaryType, nullable = false))),
      nullable = true)))
}
