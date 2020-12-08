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

import dk.aau.modelardb.core.utility.Static
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext, sources}

class ViewDataPoint(dimensions: Array[StructField]) (@transient override val sqlContext: SQLContext)
  extends BaseRelation with PrunedFilteredScan {

  /** Public Methods **/
  override def schema = StructType(Seq(
    StructField("sid", IntegerType, nullable = false),
    StructField("ts", TimestampType, nullable = false),
    StructField("val", FloatType, nullable = false))
    ++ dimensions)

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = filters

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    //DEBUG: prints the columns and predicates Spark has pushed to the view
    Static.info("ModelarDB: data point required columns { " + requiredColumns.mkString(" ") + " }")
    Static.info("ModelarDB: data point filters { " + filters.mkString(" ") + " }")

    val rows = getDataPointRDD(filters)
    SparkGridder.dataPointProjection(rows, requiredColumns)
  }

  /** Private Methods **/
  private def getDataPointRDD(filters: Array[Filter]): RDD[Row] = {
    var df = Spark.getViewProvider.option("type", "Segment").load()
    for (filter: Filter <- filters) {
      filter match {
        //Cases are only added for sid and ts as the segment view have no understanding of min/max values
        case sources.GreaterThan("sid", value: Int) => df = df.filter(s"sid > $value")
        case sources.GreaterThanOrEqual("sid", value: Int) => df = df.filter(s"sid >= $value")
        case sources.LessThan("sid", value: Int) => df = df.filter(s"sid < $value")
        case sources.LessThanOrEqual("sid", value: Int) => df = df.filter(s"sid <= $value")
        case sources.EqualTo("sid", value: Int) => df = df.filter(s"sid = $value")
        case sources.In("sid", value: Array[Any]) => df = df.filter(value.mkString("sid IN (", ",", ")"))

        case sources.GreaterThan("ts", value: Timestamp) => df = df.filter(s"et > CAST('$value' AS TIMESTAMP)")
        case sources.GreaterThanOrEqual("ts", value: Timestamp) => df = df.filter(s"et >= CAST('$value' AS TIMESTAMP)")
        case sources.LessThan("ts", value: Timestamp) => df = df.filter(s"st < CAST('$value' AS TIMESTAMP)")
        case sources.LessThanOrEqual("ts", value: Timestamp) => df = df.filter(s"st <= CAST('$value' AS TIMESTAMP)")
        case sources.EqualTo("ts", value: Timestamp) => df =
          df.filter(s"st <= CAST('$value' AS TIMESTAMP) AND et >= CAST('$value' AS TIMESTAMP)")

        //All predicates requesting specific members can be pushed directly to the segment view for conversion to Gids
        case sources.EqualTo(column: String, value: Any) if column != "val" => df = if (value.isInstanceOf[String])
          df.filter(s"$column = '$value'") else df.filter(s"$column = $value")

        //If a predicate is not supported by the segment view all we can do is inform the user
        case p => Static.warn("ModelarDB: unsupported predicate for DataPointView predicate push-down " + p)
      }
    }

    //Dimensions are appended to each data point if necessary, so they are not requested from the segment view
    df = df.select("sid", "st", "et", "res", "mid", "param", "gaps")
    df.rdd
  }
}
