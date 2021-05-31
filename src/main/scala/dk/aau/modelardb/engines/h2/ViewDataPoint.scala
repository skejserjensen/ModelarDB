/* Copyright 2021 The ModelarDB Contributors
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

import dk.aau.modelardb.core.DataPoint
import org.h2.api.TableEngine
import org.h2.command.ddl.CreateTableData
import org.h2.command.dml.AllColumnsForPlan
import org.h2.engine.{Database, DbObject, Session}
import org.h2.index.{Cursor, Index, IndexLookupBatch, IndexType}
import org.h2.result.{Row, SearchRow, SortOrder}
import org.h2.schema.Schema
import org.h2.table._

import java.{lang, util}
import scala.collection.JavaConverters._

class ViewDataPoint extends TableEngine {
  override def createTable(data: CreateTableData): TableBase = {
    new ViewDataPointTable(data)
  }
}

class ViewDataPointTable(data: CreateTableData) extends TableBase(data) {
  override def lock(session: Session, exclusive: Boolean, forceLockEvenInMvcc: Boolean): Boolean = false

  override def close(session: Session): Unit = () //Nothing to do

  override def unlock(s: Session): Unit = ???

  override def addIndex(session: Session, indexName: String, indexId: Int, cols: Array[IndexColumn], indexType: IndexType, create: Boolean, indexComment: String): Index = ???

  override def removeRow(session: Session, row: Row): Unit = ???

  override def truncate(session: Session): Unit = ???

  override def addRow(session: Session, row: Row): Unit = ???

  override def checkSupportAlter(): Unit = ???

  override def getTableType: TableType = ???

  override def getScanIndex(session: Session): Index = new ViewDataPointIndex(this)

  override def getUniqueIndex: Index = ???

  override def getIndexes: util.ArrayList[Index] = new util.ArrayList[Index]()

  override def isLockedExclusively: Boolean = ???

  override def getMaxDataModificationId: Long = ???

  override def isDeterministic: Boolean = false

  override def canGetRowCount: Boolean = false

  override def canDrop: Boolean = ???

  override def getRowCount(session: Session): Long = ???

  override def getRowCountApproximation: Long = ???

  override def getDiskSpaceUsed: Long = ???

  override def checkRename(): Unit = ???
}

class ViewDataPointIndex(table: Table) extends Index {
  override def getPlanSQL: String = ???

  override def close(session: Session): Unit = ???

  override def add(session: Session, row: Row): Unit = ???

  override def remove(session: Session, row: Row): Unit = ???

  override def update(session: Session, oldRow: Row, newRow: Row): Unit = ???

  override def isFindUsingFullTableScan: Boolean = ???

  override def find(session: Session, first: SearchRow, last: SearchRow): Cursor = ???

  override def find(filter: TableFilter, first: SearchRow, last: SearchRow): Cursor = new ViewDataPointCursor(filter)

  override def getCost(session: Session, masks: Array[Int], filters: Array[TableFilter], filter: Int,  sortOrder: SortOrder, allColumnsSet: AllColumnsForPlan): Double = 1.0  //HACK: unclear what we have to return...

  override def remove(session: Session): Unit = ???

  override def truncate(session: Session): Unit = ???

  override def canGetFirstOrLast: Boolean = ???

  override def canFindNext: Boolean = ???

  override def findNext(session: Session, higherThan: SearchRow, last: SearchRow): Cursor = ???

  override def findFirstOrLast(session: Session, first: Boolean): Cursor = ???

  override def needRebuild(): Boolean = ???

  override def getRowCount(session: Session): Long = ???

  override def getRowCountApproximation: Long = ???

  override def getDiskSpaceUsed: Long = ???

  override def compareRows(rowData: SearchRow, compare: SearchRow): Int = ???

  override def getColumnIndex(col: Column): Int = -1 //HACK: -1 seems to indicate that no index exists for that column

  override def isFirstColumn(column: Column): Boolean = ???

  override def getIndexColumns: Array[IndexColumn] = Array[IndexColumn]()

  override def getColumns: Array[Column] = ???

  override def getIndexType: IndexType = ???

  override def getTable: Table = table

  override def getRow(session: Session, key: Long): Row = ???

  override def isRowIdIndex: Boolean = ???

  override def canScan: Boolean = ???

  override def setSortedInsertMode(sortedInsertMode: Boolean): Unit = ???

  override def createLookupBatch(filters: Array[TableFilter], filter: Int): IndexLookupBatch = ???

  override def getSchema: Schema = ???

  override def isHidden: Boolean = ???

  override def getSQL(alwaysQuote: Boolean): String = ???

  override def getSQL(builder: lang.StringBuilder, alwaysQuote: Boolean): lang.StringBuilder = ???

  override def getChildren: util.ArrayList[DbObject] = ???

  override def getDatabase: Database = ???

  override def getId: Int = ???

  override def getName: String = ???

  override def getCreateSQLForCopy(table: Table, quotedName: String): String = ???

  override def getCreateSQL: String = ???

  override def getDropSQL: String = ???

  override def getType: Int = ???

  override def removeChildrenAndResources(session: Session): Unit = ???

  override def checkRename(): Unit = ???

  override def rename(newName: String): Unit = ???

  override def isTemporary: Boolean = ???

  override def setTemporary(temporary: Boolean): Unit = ???

  override def setComment(comment: String): Unit = ???

  override def getComment: String = ???
}

class ViewDataPointCursor(filter: TableFilter) extends Cursor {

  /** Instance Variables **/
  private val dataPoints: Iterator[DataPoint] = H2.h2.getSegmentGroups(filter)
    .flatMap(_.toSegments(H2.h2storage)).flatMap(segment => segment.grid().iterator().asScala)
  private val values = H2Projector.dataPointProjection(dataPoints, filter)
  private val currentViewRow = new ViewRow()

  /** Public Methods **/
  override def get(): Row = ???

  override def getSearchRow: SearchRow = {
    this.currentViewRow.setValueList(values.next())
    this.currentViewRow
  }

  override def next(): Boolean = this.dataPoints.hasNext

  override def previous(): Boolean = false
}