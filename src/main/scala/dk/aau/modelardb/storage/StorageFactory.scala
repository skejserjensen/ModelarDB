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

import dk.aau.modelardb.core.Configuration
import dk.aau.modelardb.remote.{RemoteStorage, RemoteUtilities}

object StorageFactory {

  /** Public Methods **/
  def getStorage(configuration: Configuration): Storage = {
    //Selects the correct storage backend based on the connection string provided
    val connectionString = configuration.getString("modelardb.storage")
    val storage = try {
      if (connectionString.startsWith("cassandra://")) {
        new CassandraStorage(connectionString.substring(12))
      } else if (connectionString.startsWith("jdbc:")) {
        new JDBCStorage(connectionString)
      } else if (connectionString.startsWith("orc:")) {
        new ORCStorage(connectionString.substring(4) + '/')
      } else if (connectionString.startsWith("parquet:")) {
        new ParquetStorage(connectionString.substring(8) + '/')
      } else {
        throw new java.lang.IllegalArgumentException("ModelarDB: unknown value for modelardb.storage in the config file")
      }
    } catch {
      case e: Exception =>
        throw new java.lang.IllegalArgumentException("ModelarDB: failed to initialize modelardb.storage from the config file", e)
    }

    //Local storage is only used as a temporary buffer if transfer of segmentGroups to a remote instance is enabled
    val transfer = configuration.getString("modelardb.transfer", "server")
    val (mode, port) = RemoteUtilities.getInterfaceAndPort(transfer, 10000)
    mode match {
      case "server" => storage
      case ip => new RemoteStorage(ip, port, storage, configuration)
    }
  }
}