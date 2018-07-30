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

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider}

class ViewProvider extends RelationProvider {

  /** Public Methods **/
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    parameters("type") match {
      case "Segment" => new ViewSegment()(sqlContext)
      case "DataPoint" => new ViewDataPoint()(sqlContext)
      case _ => throw new java.lang.UnsupportedOperationException("ModelarDB: unknown relation type")
    }
  }
}