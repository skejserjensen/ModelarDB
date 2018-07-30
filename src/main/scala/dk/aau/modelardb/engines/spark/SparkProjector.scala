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

import java.util.concurrent.ConcurrentHashMap

import dk.aau.modelardb.core.DataPoint
import org.apache.spark.sql.Row

//Abstract class that projections generated on run-time using the ToolBox can derive from.
abstract class SparkSegmentProjector extends Serializable {
  def project(row: Row): Row
}

abstract class SparkDataPointProjector extends Serializable {
  def project(dp: DataPoint): Row
}

object SparkProjection {
  def getSegmentProjection(requiredColumns: Array[String]): String= {
    //We allow a race condition that overwrite a key with the same code to not lock
    val key = requiredColumns.mkString("")
    if (codeCache.containsKey(key)) {
      return codeCache.get(key)
    }

    val columns = requiredColumns.map ({
      case "sid" => "row.getInt(0)"
      case "st" => "row.getTimestamp(1)"
      case "et" => "row.getTimestamp(2)"
      case "res" => "row.getInt(3)"
      case "mid" => "row.getInt(4)"
      case "param" => "row.getAs[Array[Byte]](5)"
      case "gaps" => "row.getAs[Array[Long]](6)"
    })

    val code = s"""
        import dk.aau.modelardb.engines.spark.SparkSegmentProjector
        import java.sql.Timestamp
        import org.apache.spark.sql.Row
          new SparkSegmentProjector {
            override def project(row: Row): Row = {
              ${columns.mkString ("Row(", ",", ")")}
           }
        }
    """.stripMargin
    codeCache.put(key, code)
    code
  }

  def compileSegmentProjection(code: String): SparkSegmentProjector = {
    //Imports the packages with the run-time information necessary to construct the toolbox
    import scala.tools.reflect.ToolBox
    import scala.reflect.runtime.currentMirror
    val toolBox = currentMirror.mkToolBox()

    //Parse and compile the code before constructing a projector object
    val ast = toolBox.parse (code)
    val compiled = toolBox.compile (ast)
    compiled ().asInstanceOf[SparkSegmentProjector]
  }

  def getDataPointProjection(requiredColumns: Array[String]): String = {
    //We allow a race condition that overwrite a key with the same code to not lock
    val key = requiredColumns.mkString("")
    if (codeCache.containsKey(key)) {
      return codeCache.get(key)
    }

    val columns = requiredColumns.map({
      case "sid" => "dp.sid"
      case "ts" => "new Timestamp(dp.timestamp)"
      case "val" => "dp.value"
    })

    //Parse and compile the code before constructing a projector object
    val code = s"""
        import dk.aau.modelardb.engines.spark.SparkDataPointProjector
        import dk.aau.modelardb.core.DataPoint
        import java.sql.Timestamp
        import org.apache.spark.sql.Row
          new SparkDataPointProjector {
            override def project(dp: DataPoint): Row = {
              ${columns.mkString("Row(", ",", ")")}
           }
        }
    """.stripMargin
    codeCache.put(key, code)
    code
  }

  def compileDataPointProjection(code: String): SparkDataPointProjector = {
    //Imports the packages with the run-time information necessary to construct the toolbox
    import scala.tools.reflect.ToolBox
    import scala.reflect.runtime.currentMirror
    val toolBox = currentMirror.mkToolBox()

    //Parse and compile the code before constructing a projector object
    val ast = toolBox.parse (code)
    val compiled = toolBox.compile (ast)
    compiled().asInstanceOf[SparkDataPointProjector]
  }

  /** Instance Variable **/
  val codeCache: ConcurrentHashMap[String, String] = new ConcurrentHashMap[String, String]()
}