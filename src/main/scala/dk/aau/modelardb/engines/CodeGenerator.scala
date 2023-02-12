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
package dk.aau.modelardb.engines

import dk.aau.modelardb.core.utility.ValueFunction
import dk.aau.modelardb.core.{DataPoint, SegmentGroup}

import org.apache.arrow.vector.VectorSchemaRoot

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{BinaryType, DataType, DoubleType, FloatType, IntegerType, LongType, StringType, StructType, TimestampType}
import java.util

import org.h2.table.Column
import org.h2.value.{TypeInfo, Value}
import java.sql.{ResultSet, Types}

//Abstract classes that the CodeGenerator can generated instances for at run-time using the ToolBox APIs
abstract class SparkSegmentProjector {
  def project(row: Row, tsmc: Array[Array[Object]]): Row
}

abstract class SparkDataPointProjector {
  def project(dp: DataPoint, tsmc: Array[Array[Object]], sc: Array[Float], btc: Broadcast[Array[ValueFunction]]): Row
}

abstract class H2SegmentProjector {
  def project(segment: SegmentGroup, currentRow: Array[Value], tsmc: Array[Array[Object]]): Array[Value]
}

abstract class H2DataPointProjector {
  def project(dp: DataPoint, currentRow: Array[Value], tsmc: Array[Array[Object]], sc: Array[Float], tc: Array[ValueFunction]): Array[Value]
}

abstract class SparkDataFrameToArrow {
  def fillVectors(rows: util.Iterator[Row], vsr: VectorSchemaRoot): Unit
}

abstract class H2JDBCToArrow {
  def fillVectors(rs: ResultSet, vsr: VectorSchemaRoot): Unit
}

//CodeGenerator
object CodeGenerator {

  /** Public Methods **/
  //Apache Spark
  def getSparkSegmentProjection(requiredColumns: Array[String], segmentViewNameToIndex: Map[String, Int]): String = {
    val columns = requiredColumns.map(column => {
      val index = segmentViewNameToIndex(column)
      if (index <= 6) "segmentRow.get(" + (index - 1) + ")" else "tsmc(tid)(" + (index - 7) + ")"
    })

    val code = s"""
        import dk.aau.modelardb.engines.SparkSegmentProjector
        import java.sql.Timestamp
        import org.apache.spark.sql.Row

        new SparkSegmentProjector {
            override def project(segmentRow: Row, tsmc: Array[Array[Object]]): Row = {
              val tid = segmentRow.getInt(0)
              ${columns.mkString("Row(", ",", ")")}
            }
        }
    """.stripMargin
    code
  }

  def compileSparkSegmentProjection(code: String): SparkSegmentProjector = {
    //For Apache Spark code-generation is done once on the master while compilation is done on each worker
    compileCode(code).asInstanceOf[SparkSegmentProjector]
  }

  def getSparkDataPointProjection(requiredColumns: Array[String], dataPointViewNameToIndex: Map[String, Int]): String = {
    val columns = requiredColumns.map(column => {
      val index = dataPointViewNameToIndex(column)
      if (index <= 3) sparkDataPointProjectionFragments(index - 1) else "tsmc(dp.tid)(" + (index - 4) + ")"
    })

    val code = s"""
        import dk.aau.modelardb.engines.SparkDataPointProjector
        import dk.aau.modelardb.core.DataPoint
        import java.sql.Timestamp
        import dk.aau.modelardb.core.utility.ValueFunction
        import org.apache.spark.broadcast.Broadcast
        import org.apache.spark.sql.Row

        new SparkDataPointProjector {
            override def project(dp: DataPoint, tsmc: Array[Array[Object]], sc: Array[Float], btc: Broadcast[Array[ValueFunction]]): Row = {
                ${columns.mkString("Row(", ",", ")")}
            }
        }
    """.stripMargin
    code
  }

  def compileSparkDataPointProjection(code: String): SparkDataPointProjector = {
    //For Apache Spark code-generation is done once on the master while compilation is done on each worker
    compileCode(code).asInstanceOf[SparkDataPointProjector]
  }

  def getSparkDataFrameToArrow(schema: StructType, batchSize: Int): SparkDataFrameToArrow = {
    val vectors = schema.zipWithIndex.map({ case (sf, columnIndex) => getDataFrameColumn(sf.dataType, columnIndex) })
    val code = s"""import dk.aau.modelardb.engines.SparkDataFrameToArrow
                   import java.util
                   import java.nio.charset.StandardCharsets
                   import org.apache.spark.sql.Row
                   import org.apache.arrow.vector._
                   new SparkDataFrameToArrow {
                     override def fillVectors(rows: util.Iterator[Row], vsr: VectorSchemaRoot): Unit = {
                       vsr.setRowCount($batchSize)
                       var currentIndex = 0
                       while (currentIndex < $batchSize && rows.hasNext) {
                         val row = rows.next
                         ${vectors.mkString}
                         currentIndex += 1
                       }
                       vsr.setRowCount(currentIndex)
                     }
                   }""".stripMargin
    compileCode(code).asInstanceOf[SparkDataFrameToArrow]
  }

  //H2
  def getH2SegmentProjection(columns: Array[Column]): H2SegmentProjector = {
    val codeToConstructColumns: Array[String] = columns.map(column => {
      val constructor = h2ValueConstructor(column.getType.getValueType)
      val variableName = column.getName match {
        case "TID" => "segment.gid" //Exploded so .gid is the tid
        case "START_TIME" => "segment.startTime"
        case "END_TIME" => "segment.endTime"
        case "MTID" => "segment.mtid"
        case "MODEL" => "segment.model"
        case "OFFSETS" => "segment.offsets"
        case _ => "tsmc(segment.gid)(%d)".format(column.getColumnId - 6)
      }
      "currentValues(%d)".format(column.getColumnId) + " = " + constructor.format(variableName)
    })

    val code = s"""
        import dk.aau.modelardb.engines.H2SegmentProjector
        import dk.aau.modelardb.core.SegmentGroup
        import org.h2.value._

        new H2SegmentProjector {
            override def project(segment: SegmentGroup, currentValues: Array[Value], tsmc: Array[Array[Object]]): Array[Value] = {
              ${codeToConstructColumns.mkString("\n")}
              currentValues
            }
        }
    """.stripMargin
    compileCode(code).asInstanceOf[H2SegmentProjector]
  }

  def getH2DataPointProjection(columns: Array[Column]): H2DataPointProjector = {
    val codeToConstructColumns: Array[String] = columns.map(column => {
      val constructor = h2ValueConstructor(column.getType.getValueType)
      val variableName = column.getName match {
        case "TID" => "dataPoint.tid"
        case "TIMESTAMP" => "dataPoint.timestamp"
        case "VALUE" => "tc(dataPoint.tid).transform(dataPoint.value, sc(dataPoint.tid))"
        case _ => "tsmc(dataPoint.tid)(%d)".format(column.getColumnId - 3)
      }
      "currentValues(%d)".format(column.getColumnId) + " = " + constructor.format(variableName)
    })

    val code = s"""
        import dk.aau.modelardb.engines.H2DataPointProjector
        import dk.aau.modelardb.core.DataPoint
        import dk.aau.modelardb.core.utility.ValueFunction
        import org.h2.value._

        new H2DataPointProjector {
            override def project(dataPoint: DataPoint, currentValues: Array[Value], tsmc: Array[Array[Object]], sc: Array[Float], tc: Array[ValueFunction]): Array[Value] = {
              ${codeToConstructColumns.mkString("\n")}
              currentValues
            }
        }
    """.stripMargin
    compileCode(code).asInstanceOf[H2DataPointProjector]
  }

  def getH2JDBCToArrow(rs: ResultSet, batchSize: Int): H2JDBCToArrow = {
    val vectors = StringBuilder.newBuilder
    val metadata = rs.getMetaData
    for (jdbcIndex <- Range.inclusive(1, metadata.getColumnCount)) {
      val columnType = metadata.getColumnType(jdbcIndex)
      vectors.append(getJDBCColumn(columnType, jdbcIndex))
    }

    val code = s"""import dk.aau.modelardb.engines.H2JDBCToArrow
                   import java.sql.ResultSet
                   import java.nio.charset.StandardCharsets
                   import org.apache.arrow.vector._
                   new H2JDBCToArrow {
                     def fillVectors(rs: ResultSet, vsr: VectorSchemaRoot): Unit = {
                       vsr.setRowCount($batchSize)
                       var currentIndex = 0
                       //H2ResultSet.hasNext() has already called rs.next() once
                       do {
                         ${vectors.mkString}
                         currentIndex += 1
                       } while (currentIndex < $batchSize && rs.next())
                       vsr.setRowCount(currentIndex)
                     }
                   }""".stripMargin
    compileCode(code).asInstanceOf[H2JDBCToArrow]
  }

  //Shared
  def getValueFunction(transformation: String): ValueFunction = {
    val code = s"""
    import dk.aau.modelardb.core.utility.ValueFunction
    import scala.math._
    new ValueFunction() {
      override def transform(value: Float, scalingFactor: Float): Float = {
        return ($transformation).asInstanceOf[Float]
      }
    }"""
    compileCode(code).asInstanceOf[ValueFunction]
  }

  /** Private Methods **/
  private def getDataFrameColumn(columnType: DataType, index: Int): String = {
    columnType match {
      case IntegerType => f"vsr.getVector($index).asInstanceOf[IntVector].set(currentIndex, row.getInt($index))\n"
      case LongType => f"vsr.getVector($index).asInstanceOf[BigIntVector].set(currentIndex, row.getLong($index))\n"
      case TimestampType => f"vsr.getVector($index).asInstanceOf[TimeStampMilliVector].set(currentIndex, row.getTimestamp($index).getTime)\n"
      case FloatType => f"vsr.getVector($index).asInstanceOf[Float4Vector].set(currentIndex, row.getFloat($index))\n"
      case DoubleType => f"vsr.getVector($index).asInstanceOf[Float8Vector].set(currentIndex, row.getDouble($index))\n"
      case StringType => f"vsr.getVector($index).asInstanceOf[VarCharVector].setSafe(currentIndex, row.getString($index).getBytes(StandardCharsets.UTF_8))\n"
      case BinaryType => f"vsr.getVector($index).asInstanceOf[VarBinaryVector].setSafe(currentIndex, row.getAs[Array[Byte]]($index))\n"
    }
  }

  private def getJDBCColumn(columnType: Int, jdbcIndex: Int): String = {
    val vectorIndex = jdbcIndex - 1
    columnType match {
      case Types.INTEGER => f"vsr.getVector($vectorIndex).asInstanceOf[IntVector].set(currentIndex, rs.getInt($jdbcIndex))\n"
      case Types.BIGINT => f"vsr.getVector($vectorIndex).asInstanceOf[BigIntVector].set(currentIndex, rs.getLong($jdbcIndex))\n"
      case Types.TIMESTAMP => f"vsr.getVector($vectorIndex).asInstanceOf[TimeStampMilliVector].set(currentIndex, rs.getTimestamp($jdbcIndex).getTime)\n"
      case Types.REAL => f"vsr.getVector($vectorIndex).asInstanceOf[Float4Vector].set(currentIndex, rs.getFloat($jdbcIndex))\n"
      case Types.FLOAT => f"vsr.getVector($vectorIndex).asInstanceOf[Float4Vector].set(currentIndex, rs.getFloat($jdbcIndex))\n"
      case Types.DOUBLE => f"vsr.getVector($vectorIndex).asInstanceOf[Float8Vector].set(currentIndex, rs.getDouble($jdbcIndex))\n"
      case Types.VARCHAR => f"vsr.getVector($vectorIndex).asInstanceOf[VarCharVector].setSafe(currentIndex, rs.getString($jdbcIndex).getBytes(StandardCharsets.UTF_8))\n"
      case Types.VARBINARY => f"vsr.getVector($vectorIndex).asInstanceOf[VarBinaryVector].setSafe(currentIndex, rs.getBytes($jdbcIndex))\n"
    }
  }

  private def compileCode(code: String): Any = {
    //Imports the packages required to construct the toolbox
    import scala.reflect.runtime.currentMirror
    import scala.tools.reflect.ToolBox
    val toolBox = currentMirror.mkToolBox()

    //Parses and compiles the code before constructing an object
    val ast = toolBox.parse(code)
    val compiled = toolBox.compile(ast)
    compiled()
  }

  /** Instance Variables **/
  private val sparkDataPointProjectionFragments = Array("dp.tid", "new Timestamp(dp.timestamp)",
    "btc.value(dp.tid).transform(dp.value, sc(dp.tid))")
  private val h2ValueConstructor = Map(
    TypeInfo.TYPE_INT.getValueType -> "ValueInt.get(%s.asInstanceOf[Int])",
    TypeInfo.TYPE_LONG.getValueType -> "ValueLong.get(%s.asInstanceOf[Long])",
    TypeInfo.TYPE_FLOAT.getValueType -> "ValueFloat.get(%s.asInstanceOf[Float])",
    TypeInfo.TYPE_DOUBLE.getValueType -> "ValueDouble.get(%s.asInstanceOf[Double])",
    TypeInfo.TYPE_TIMESTAMP.getValueType -> "ValueTimestamp.fromMillis(%s, 0)",
    TypeInfo.TYPE_STRING.getValueType -> "ValueString.get(%s.asInstanceOf[String])",
    TypeInfo.TYPE_BYTES.getValueType -> "ValueBytes.get(%s.asInstanceOf[Array[Byte]])")
}