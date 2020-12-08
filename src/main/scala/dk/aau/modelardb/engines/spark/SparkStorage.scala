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

import dk.aau.modelardb.core.Dimensions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.{Row, SparkSession}

trait SparkStorage {
  //Opens a connection to a segment group store with Spark integration
  def open(ssb: SparkSession.Builder, dimensions: Dimensions): SparkSession

  //Writes an RDD of segments to the segment group store
  def writeRDD(rdd: RDD[Row]): Unit

  //Reads an RDD of segments from the segment group store
  def getRDD(filters: Array[Filter]): RDD[Row]
}
