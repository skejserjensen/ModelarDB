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
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.{Row, SparkSession}

trait SparkStorage {
  //Open the connection to this Spark Storage
  def open(ssb: SparkSession.Builder): SparkSession

  //Write an RDD of segments to the underlying data source
  def writeRDD(rdd: RDD[Row]): Unit

  //Retrieve an RDD representing the underlying data source
  def getRDD(filters: Array[Filter]): RDD[Row]
}