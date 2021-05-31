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
package dk.aau.modelardb.engines

import dk.aau.modelardb.core.{Configuration, Storage}
import dk.aau.modelardb.engines.h2.H2Storage
import dk.aau.modelardb.engines.spark.SparkStorage

object EngineFactory {

  /** Public Methods **/
  def startEngine(configuration: Configuration, storage: Storage): Unit = {
    //Extracts the name of the system from the engine connection string
    configuration.getString("modelardb.engine").takeWhile(_ != ':') match {
      case "h2" => new dk.aau.modelardb.engines.h2.H2(configuration, storage.asInstanceOf[H2Storage]).start()
      case "spark" => new dk.aau.modelardb.engines.spark.Spark(configuration, storage.asInstanceOf[SparkStorage]).start()
      case _ =>
        throw new java.lang.UnsupportedOperationException("ModelarDB: unknown value for modelardb.engine in the config file")
    }
  }
}