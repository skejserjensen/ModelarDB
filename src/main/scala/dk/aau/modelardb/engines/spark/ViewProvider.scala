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

import dk.aau.modelardb.core.Dimensions.Types
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider}
import org.apache.spark.sql.types._

class ViewProvider extends RelationProvider {

  /** Public Methods **/
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    //Retrieves the names and types of the denormalized dimensions columns
    val columns = Spark.getStorage.getDimensions.getColumns
    val types = Spark.getStorage.getDimensions.getTypes

    //Converts the types to the types used by Spark SQL
    val dimensions: Array[StructField] = columns.zip(types).map {
      case (name, Types.INT) => StructField(name, IntegerType, nullable = false)
      case (name, Types.LONG) => StructField(name, LongType, nullable = false)
      case (name, Types.FLOAT) => StructField(name, FloatType, nullable = false)
      case (name, Types.DOUBLE) => StructField(name, DoubleType, nullable = false)
      case (name, Types.TEXT) => StructField(name, StringType, nullable = false)
    }

    //Constructs the requested view
    parameters("type") match {
      case "Segment" => new ViewSegment(dimensions)(sqlContext)
      case "DataPoint" => new ViewDataPoint(dimensions)(sqlContext)
      case _ => throw new java.lang.UnsupportedOperationException("ModelarDB: unknown relation type")
    }
  }
}
