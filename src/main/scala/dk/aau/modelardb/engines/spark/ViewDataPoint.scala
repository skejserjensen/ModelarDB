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

import dk.aau.modelardb.core.utility.Static
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext, sources}

import java.sql.Timestamp

class ViewDataPoint(dimensions: Array[StructField]) (@transient override val sqlContext: SQLContext)
  extends BaseRelation with PrunedFilteredScan {

  /** Public Methods **/
  override def schema: StructType = StructType(Seq(
    StructField("tid", IntegerType, nullable = false),
    StructField("timestamp", TimestampType, nullable = false),
    StructField("value", FloatType, nullable = false))
    ++ dimensions)

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = filters

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    //DEBUG: prints the columns and predicates Spark has pushed to the view
    Static.info("ModelarDB: data point required columns { " + requiredColumns.mkString(" ") + " }")
    Static.info("ModelarDB: data point filters { " + filters.mkString(" ") + " }")

    val rows = getDataPointRDD(filters)
    SparkProjector.dataPointProjection(rows, requiredColumns)
  }

  /** Private Methods **/
  private def getDataPointRDD(filters: Array[Filter]): RDD[Row] = {
    var df = Spark.getViewProvider.option("type", "Segment").load()
    for (filter: Filter <- filters) {
      filter match {
        //Cases are only added for tid and timestamp as the segment view have no understanding of min/max values
        case sources.GreaterThan("tid", value: Int) => df = df.filter(s"tid > $value")
        case sources.GreaterThanOrEqual("tid", value: Int) => df = df.filter(s"tid >= $value")
        case sources.LessThan("tid", value: Int) => df = df.filter(s"tid < $value")
        case sources.LessThanOrEqual("tid", value: Int) => df = df.filter(s"tid <= $value")
        case sources.EqualTo("tid", value: Int) => df = df.filter(s"tid = $value")
        case sources.In("tid", value: Array[Any]) => df = df.filter(value.mkString("tid IN (", ",", ")"))

        case sources.GreaterThan("timestamp", value: Timestamp) => df = df.filter(s"end_time > CAST('$value' AS TIMESTAMP)")
        case sources.GreaterThanOrEqual("timestamp", value: Timestamp) => df = df.filter(s"end_time >= CAST('$value' AS TIMESTAMP)")
        case sources.LessThan("timestamp", value: Timestamp) => df = df.filter(s"start_time < CAST('$value' AS TIMESTAMP)")
        case sources.LessThanOrEqual("timestamp", value: Timestamp) => df = df.filter(s"start_time <= CAST('$value' AS TIMESTAMP)")
        case sources.EqualTo("timestamp", value: Timestamp) => df =
          df.filter(s"start_time <= CAST('$value' AS TIMESTAMP) AND end_time >= CAST('$value' AS TIMESTAMP)")

        //All predicates requesting specific members can be pushed directly to the segment view for conversion to Gids
        case sources.EqualTo(column: String, value: Any) if column != "val" => df = if (value.isInstanceOf[String])
          df.filter(s"$column = '$value'") else df.filter(s"$column = $value")

        //If a predicate is not supported by the segment view all we can do is inform the user
        case p => Static.warn("ModelarDB: predicate push-down is not supported for " + p, 120)
      }
    }

    //Dimensions are appended to each data point if necessary, so they are not requested from the segment view
    df = df.select("tid", "start_time", "end_time", "mtid", "model", "offsets")
    df.rdd
  }
}
