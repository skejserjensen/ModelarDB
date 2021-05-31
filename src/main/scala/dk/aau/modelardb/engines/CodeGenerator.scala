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
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row
import org.h2.table.Column
import org.h2.value.{TypeInfo, Value}

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
    compileCode(code).asInstanceOf[SparkDataPointProjector]
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
        case "GAPS" => "segment.offsets"
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
