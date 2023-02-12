/* Copyright 2022 The ModelarDB Contributors
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
package dk.aau.modelardb.engines.h2

import dk.aau.modelardb.engines.{CodeGenerator, H2JDBCToArrow}
import dk.aau.modelardb.remote.ArrowResultSet

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.{FloatingPointPrecision, TimeUnit}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}

import java.sql.{DriverManager, ResultSetMetaData, Types}
import java.util
import java.util.concurrent.ConcurrentHashMap

class H2ResultSet(connectionString: String, query: String, rootAllocator: BufferAllocator) extends ArrowResultSet {

  /** Public Methods **/
  override def get(): VectorSchemaRoot = {
    this.vsr
  }

  override def hasNext(): Boolean = {
    this.rs.next()
  }

  override def fillNext(): Unit = {
    this.h2JDBCToArrow.fillVectors(this.rs, this.vsr)
  }

  override def close(): Unit = {
    this.vsr.close()
    this.rs.close()
    this.stmt.close()
    this.connection.close()
  }

  /** Private Methods **/
  def convertSchema(rsmd: ResultSetMetaData): Schema = {
    val fields = new util.ArrayList[Field]()
    for (columnIndex <- Range.inclusive(1, rsmd.getColumnCount)) {
      fields.add(rsmd.getColumnType(columnIndex) match {
        case Types.INTEGER => this.getField(rsmd, columnIndex, new ArrowType.Int(32, true))
        case Types.BIGINT => this.getField(rsmd, columnIndex, new ArrowType.Int(64, true))
        case Types.TIMESTAMP => this.getField(rsmd, columnIndex, new ArrowType.Timestamp(TimeUnit.MILLISECOND, null))
        case Types.REAL => this.getField(rsmd, columnIndex, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE))
        case Types.FLOAT => this.getField(rsmd, columnIndex, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE))
        case Types.DOUBLE => this.getField(rsmd, columnIndex, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))
        case Types.VARCHAR => this.getField(rsmd, columnIndex, new ArrowType.Utf8())
        case Types.VARBINARY => this.getField(rsmd, columnIndex, new ArrowType.Binary())
      })
    }
    new Schema(fields)
  }

  def getField(rsmd: ResultSetMetaData, columnIndex: Int, at: ArrowType): Field = {
    new Field(rsmd.getColumnName(columnIndex), new FieldType(false, at, null), null)
  }

  /** Instance Variables **/
  private val connection = DriverManager.getConnection(connectionString)
  private val stmt = this.connection.createStatement()
  private val rs = {
    this.stmt.execute(query)
    this.stmt.getResultSet
  }
  private val vsr = {
    val schema = this.convertSchema(this.rs.getMetaData)
    val vsr = VectorSchemaRoot.create(schema, this.rootAllocator)
    vsr.setRowCount(this.DEFAULT_TARGET_BATCH_SIZE)
    vsr
  }
  private val h2JDBCToArrow = {
    val columnTypes = StringBuilder.newBuilder
    val metadata = rs.getMetaData
    for (jdbcIndex <- Range.inclusive(1, metadata.getColumnCount)) {
      columnTypes.append(metadata.getColumnType(jdbcIndex))
    }
    val cacheKey: String = columnTypes.mkString(" ")
    if ( ! H2ResultSet.h2JDBCToArrowCache.containsKey(cacheKey)) {
      //ContainsKey and put is not synchronized as the generated code for a specific key is always the same
      H2ResultSet.h2JDBCToArrowCache.put(cacheKey,
        CodeGenerator.getH2JDBCToArrow(this.rs, this.DEFAULT_TARGET_BATCH_SIZE))
    }
    H2ResultSet.h2JDBCToArrowCache.get(cacheKey)
  }
}

object H2ResultSet {
  private val h2JDBCToArrowCache = new ConcurrentHashMap[String, H2JDBCToArrow]()
}