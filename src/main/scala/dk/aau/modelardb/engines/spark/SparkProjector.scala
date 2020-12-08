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

import dk.aau.modelardb.core.DataPoint
import org.apache.spark.sql.Row

//Abstract classes that projections generated at run-time using the ToolBox APIs can derive from
abstract class SparkSegmentProjector extends Serializable {
  def project(row: Row, dmc: Array[Array[Object]]): Row
}

abstract class SparkDataPointProjector extends Serializable {
  def project(dp: DataPoint, dmc: Array[Array[Object]], sc: Array[Float]): Row
}

object SparkProjection {

  def getSegmentProjection(requiredColumns: Array[String], segmentViewNameToIndex: Map[String, Int]): String = {
    val columns = requiredColumns.map(column => {
      val index = segmentViewNameToIndex(column)
      if (index <= 7) "segmentRow.get(" + (index - 1) + ")" else "dmc(sid)(" + (index - 8) + ")"
    })

    val code = s"""
        import dk.aau.modelardb.engines.spark.SparkSegmentProjector
        import java.sql.Timestamp
        import org.apache.spark.sql.Row

        new SparkSegmentProjector {
            override def project(segmentRow: Row, dmc: Array[Array[Object]]): Row = {
              val sid = segmentRow.getInt(0)
              ${columns.mkString("Row(", ",", ")")}
            }
        }
    """.stripMargin
    code
  }

  def compileSegmentProjection(code: String): SparkSegmentProjector = {
    compileProjection(code).asInstanceOf[SparkSegmentProjector]
  }

  def getDataPointProjection(requiredColumns: Array[String], dataPointViewNameToIndex: Map[String, Int]): String = {

    val columns = requiredColumns.map(column => {
      val index = dataPointViewNameToIndex(column)
      if (index <= 3) dataPointProjectionFragments(index - 1) else "dmc(dp.sid)(" + (index - 4) + ")"
    })

    val code = s"""
        import dk.aau.modelardb.engines.spark.SparkDataPointProjector
        import dk.aau.modelardb.core.DataPoint
        import java.sql.Timestamp
        import org.apache.spark.sql.Row

        new SparkDataPointProjector {
            override def project(dp: DataPoint, dmc: Array[Array[Object]], sc: Array[Float]): Row = {
                ${columns.mkString("Row(", ",", ")")}
            }
        }
    """.stripMargin
    code
  }

  def compileDataPointProjection(code: String): SparkDataPointProjector = {
    compileProjection(code).asInstanceOf[SparkDataPointProjector]
  }

  /** Private Methods **/
  private def compileProjection(code: String): Any = {
    //Imports the packages required to construct the toolbox
    import scala.reflect.runtime.currentMirror
    import scala.tools.reflect.ToolBox
    val toolBox = currentMirror.mkToolBox()

    //Parses and compiles the code before constructing a projector object
    val ast = toolBox.parse(code)
    val compiled = toolBox.compile(ast)
    compiled()
  }

  /** Instance Variables **/
  private val dataPointProjectionFragments = Array("dp.sid", "new Timestamp(dp.timestamp)", "dp.value / sc(dp.sid)")
}
