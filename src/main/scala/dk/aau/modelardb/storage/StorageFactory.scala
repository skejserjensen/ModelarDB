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
package dk.aau.modelardb.storage

import dk.aau.modelardb.core.Storage

object StorageFactory {

  /** Public Methods **/
  def getStorage(connectionString: String): Storage = {
    //Selects the correct storage backend based on the connection string provided
    try {
      if (connectionString.startsWith("sqlite:")) {
        Class.forName("org.sqlite.JDBC")
        new RDBMSStorage("jdbc:" + connectionString)
      } else if (connectionString.startsWith("postgresql:")) {
        Class.forName("org.postgresql.Driver")
        new dk.aau.modelardb.storage.RDBMSStorage("jdbc:" + connectionString)
      } else if (connectionString.startsWith("cassandra:")) {
        new CassandraSparkStorage(connectionString.split("://")(1))
      } else {
        throw new java.lang.IllegalArgumentException("ModelarDB: unknown value for modelardb.storage in the config file")
      }
    } catch {
      case _: Exception =>
        throw new java.lang.IllegalArgumentException("ModelarDB: failed to initialize modelardb.storage from the config file")
    }
  }
}
