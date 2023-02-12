/* Copyright 2022 The ModelarDB Contributors
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

import dk.aau.modelardb.engines.{CodeGenerator, SparkDataFrameToArrow}
import dk.aau.modelardb.remote.ArrowResultSet

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.{FloatingPointPrecision, TimeUnit}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{BinaryType, DoubleType, FloatType, IntegerType, LongType, StringType, StructType, TimestampType}

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._

class SparkResultSet(df: DataFrame, rootAllocator: BufferAllocator) extends ArrowResultSet {

  /** Public Methods **/
  def get(): VectorSchemaRoot = {
    this.vsr
  }

  def hasNext(): Boolean = {
    this.rows.hasNext
  }

  def fillNext(): Unit = {
    this.sparkDataFrameToArrow.fillVectors(this.rows, this.vsr)
  }

  def close(): Unit = {
    this.vsr.close()
    this.df.unpersist()
  }

  /** Private Methods **/
  def convertSchema(schema: StructType): Schema = {
    val fields = schema.map(sf => sf.dataType match {
      case IntegerType => this.getField(sf.name, new ArrowType.Int(32, true))
      case LongType => this.getField(sf.name, new ArrowType.Int(64, true))
      case TimestampType => this.getField(sf.name, new ArrowType.Timestamp(TimeUnit.MILLISECOND, null))
      case FloatType => this.getField(sf.name, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE))
      case DoubleType => this.getField(sf.name, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))
      case StringType => this.getField(sf.name, new ArrowType.Utf8())
      case BinaryType => this.getField(sf.name, new ArrowType.Binary())
    })
    new Schema(fields.asJava)
  }

  def getField(name: String, at: ArrowType): Field = {
    new Field(name, new FieldType(false, at, null), null)
  }

  /** Instance Variables **/
  private val rows = {
    this.df.persist()
    this.df.toLocalIterator()
  }
  private val vsr = {
    val schema = this.convertSchema(df.schema)
    val vsr = VectorSchemaRoot.create(schema, this.rootAllocator)
    vsr.setRowCount(this.DEFAULT_TARGET_BATCH_SIZE)
    vsr
  }
  private val sparkDataFrameToArrow = {
    val cacheKey = this.df.schema
    if ( ! SparkResultSet.sparkDataFrameToArrow.containsKey(cacheKey)) {
      //ContainsKey and put is not synchronized as the generated code for a specific key is always the same
      SparkResultSet.sparkDataFrameToArrow.put(cacheKey,
        CodeGenerator.getSparkDataFrameToArrow(this.df.schema, this.DEFAULT_TARGET_BATCH_SIZE))
    }
    SparkResultSet.sparkDataFrameToArrow.get(cacheKey)
  }
}

object SparkResultSet {
  private val sparkDataFrameToArrow = new ConcurrentHashMap[StructType, SparkDataFrameToArrow]()
}