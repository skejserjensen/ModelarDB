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
package dk.aau.modelardb.storage

import dk.aau.modelardb.core.Storage

object StorageFactory {

  /** Public Methods **/
  def getStorage(connectionString: String): Storage = {
    //Selects the correct storage backend based on the connection string provided
    try {
      if (connectionString.startsWith("jdbc:")) {
        new JDBCStorage(connectionString)
      } else if (connectionString.startsWith("cassandra:")) {
        new CassandraStorage(connectionString.split("://")(1))
      } else {
        throw new java.lang.IllegalArgumentException("ModelarDB: unknown value for modelardb.storage in the config file")
      }
    } catch {
      case e: Exception =>
        throw new java.lang.IllegalArgumentException("ModelarDB: failed to initialize modelardb.storage from the config file", e)
    }
  }
}