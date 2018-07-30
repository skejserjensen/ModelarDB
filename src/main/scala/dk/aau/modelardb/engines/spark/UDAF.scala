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

import dk.aau.modelardb.core.models.Segment
import dk.aau.modelardb.core.utility.Static

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

//Implementation of user defined aggregate functions on top of the segment view
//Count
class CountR extends UserDefinedAggregateFunction {

  //NOTE: CountR computes the count directly from rows and only work when gaps are disabled
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

class CountRS extends CountR {

  //NOTE: CountR computes the count directly from rows and only work when gaps are disabled
  override def inputSchema: StructType = UDAF.udfType

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val row = input.getStruct(0)
    buffer(0) = buffer.getLong(0) + ((row.getTimestamp(2).getTime - row.getTimestamp(1).getTime) / row.getInt(3)) + 1
  }
}

class CountS extends UserDefinedAggregateFunction {

  /** Public Methods **/
  override def inputSchema: StructType = UDAF.segmentSchema

  override def bufferSchema: StructType = StructType(StructField("count", LongType) :: Nil)

  override def dataType: DataType = LongType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = buffer(0) = 0L

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + this.rowToSegment(input).length()
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
  }

  override def evaluate(buffer: Row): Any = buffer.getLong(0)

  /* Instance Variables */
  protected val rowToSegment = Spark.getRowToSegment
}

class CountSS extends CountS {

  /** Public Methods **/
  override def inputSchema: StructType = UDAF.udfType

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + this.rowToSegment(input.getStruct(0)).length()
  }
}

//Min
class MinS extends UserDefinedAggregateFunction {

  /** Public Methods **/
  override def inputSchema: StructType = UDAF.segmentSchema

  override def bufferSchema: StructType = StructType(StructField("min", FloatType) :: Nil)

  override def dataType: DataType = FloatType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = buffer(0) = Float.PositiveInfinity

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = Math.min(buffer.getFloat(0), this.rowToSegment(input).min())
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

  /* Instance Variables */
  protected val rowToSegment = Spark.getRowToSegment
}

class MinSS extends MinS {

  /** Public Methods **/
  override def inputSchema: StructType = UDAF.udfType

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = Math.min(buffer.getFloat(0), this.rowToSegment(input.getStruct(0)).min())
  }
}

//Max
class MaxS extends UserDefinedAggregateFunction {

  /** Public Methods **/
  override def inputSchema: StructType = UDAF.segmentSchema

  override def bufferSchema: StructType = StructType(StructField("max", FloatType) :: Nil)

  override def dataType: DataType = FloatType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = buffer(0) = Float.NegativeInfinity

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = Math.max(buffer.getFloat(0), this.rowToSegment(input).max())
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

  /* Instance Variables */
  protected val rowToSegment = Spark.getRowToSegment
}

class MaxSS extends MaxS {

  /** Public Methods **/
  override def inputSchema: StructType = UDAF.udfType

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = Math.max(buffer.getFloat(0), this.rowToSegment(input.getStruct(0)).max())
  }
}

//Sum
class SumS extends UserDefinedAggregateFunction {

  /** Public Methods **/
  override def inputSchema: StructType = UDAF.segmentSchema

  override def bufferSchema: StructType = StructType(StructField("sum", DoubleType) :: StructField("check", BooleanType) :: Nil)

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0.0
    buffer(1) = false
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getDouble(0) + this.rowToSegment(input).sum()
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

  /* Instance Variables */
  protected val rowToSegment = Spark.getRowToSegment
}

class SumSS extends SumS {

  /** Public Methods **/
  override def inputSchema: StructType = UDAF.udfType

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getDouble(0) + this.rowToSegment(input.getStruct(0)).sum()
  }
}

//Avg
class AvgS extends UserDefinedAggregateFunction {

  /** Public Methods **/
  override def inputSchema: StructType = UDAF.segmentSchema

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
    buffer(0) = buffer.getDouble(0) + segment.sum()
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

  /* Instance Variables */
  protected val rowToSegment = Spark.getRowToSegment
}

class AvgSS extends AvgS {

  /** Public Methods **/
  override def inputSchema: StructType = UDAF.udfType

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val segment = this.rowToSegment(input.getStruct(0))
    buffer(0) = buffer.getDouble(0) + segment.sum()
    buffer(1) = buffer.getLong(1) + segment.length()
  }
}

//Helper functions shared between the various UDF and UDAF
object UDAF {

  /** Public Methods **/
  def init(spark: SparkSession): Unit = {
    spark.sqlContext.udf.register("count_r", new CountR)
    spark.sqlContext.udf.register("count_rs", new CountRS)
    spark.sqlContext.udf.register("count_s", new CountS)
    spark.sqlContext.udf.register("count_ss", new CountSS)
    spark.sqlContext.udf.register("min_s", new MinS)
    spark.sqlContext.udf.register("min_ss", new MinSS)
    spark.sqlContext.udf.register("max_s", new MaxS)
    spark.sqlContext.udf.register("max_ss", new MaxSS)
    spark.sqlContext.udf.register("sum_s", new SumS)
    spark.sqlContext.udf.register("sum_ss", new SumSS)
    spark.sqlContext.udf.register("avg_s", new AvgS)
    spark.sqlContext.udf.register("avg_ss", new AvgSS)

    spark.sqlContext.udf.register("start", start _)
    spark.sqlContext.udf.register("end", end _)
    spark.sqlContext.udf.register("interval", interval _)
  }

  def start(sid: Int, st: Timestamp, et: Timestamp, res: Int, mid: Int, param: Array[Byte], gaps: Array[Byte], nst: Timestamp) = {
    val fromTime = Segment.start(nst.getTime, st.getTime, et.getTime, res)
    val newGaps = gaps ++ Static.longsToBytes(Array[Long]((fromTime - st.getTime) / res))
    (sid, new Timestamp(fromTime), et, res, mid, param, newGaps)
  }

  def end(sid: Int, st: Timestamp, et: Timestamp, res: Int, mid: Int, param: Array[Byte], gaps: Array[Byte], net: Timestamp) = {
      (sid, st, new Timestamp(Segment.end(net.getTime, st.getTime, et.getTime, res)), res, mid, param, gaps)
  }

  def interval(sid: Int, st: Timestamp, et: Timestamp, res: Int, mid: Int, param: Array[Byte], gaps: Array[Byte], nst: Timestamp, net: Timestamp) = {
    val fromTime = Segment.start(nst.getTime, st.getTime, et.getTime, res)
    val newGaps = gaps ++ Static.longsToBytes(Array[Long]((fromTime - st.getTime) / res))
    val toTime = Segment.end(net.getTime, st.getTime, et.getTime, res)
    (sid, new Timestamp(fromTime), new Timestamp(toTime), res, mid, param, newGaps)
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

  val udfType =  StructType(Seq(
    StructField("udf",
      StructType(Seq(
        StructField("_1", IntegerType, nullable = false),
        StructField("_2", TimestampType, nullable = true),
        StructField("_3", TimestampType, nullable = true),
        StructField("_4", IntegerType, nullable = false),
        StructField("_5", IntegerType, nullable = false),
        StructField("_6", BinaryType, nullable = true),
        StructField("_7", BinaryType, nullable = true))),
    nullable = true)))
}