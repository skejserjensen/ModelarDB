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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

class ViewSegment()(@transient val sqlContext: SQLContext)
  extends BaseRelation with PrunedFilteredScan {

  /** Public Methods **/
  override def schema = StructType(Seq(
    StructField("sid", IntegerType, nullable = false),
    StructField("st", TimestampType, nullable = false),
    StructField("et", TimestampType, nullable = false),
    StructField("res", IntegerType, nullable = false),
    StructField("mid", IntegerType, nullable = false),
    StructField("param", BinaryType, nullable = false),
    StructField("gaps", BinaryType, nullable = false)))

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = filters

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    //DEBUG: prints the columns and predicates Spark have pushed to the view
    println("ModelarDB: segment required columns { " + requiredColumns.mkString(" ") + " }")
    println("ModelarDB: segment filters { " + filters.mkString(" ") + " }")

    //Creates an RDD representing the segments extracted from storage and currently in-memory
    val segmentRDD = cache.getSegmentRDD(filters)
    SparkGridder.segmentProjection(segmentRDD, requiredColumns)
  }
  /** Instance Variable **/
  private val cache = Spark.getCache
}