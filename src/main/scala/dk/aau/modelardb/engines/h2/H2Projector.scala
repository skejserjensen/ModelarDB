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

import dk.aau.modelardb.core.utility.Static
import dk.aau.modelardb.core.{DataPoint, SegmentGroup}
import dk.aau.modelardb.engines.{CodeGenerator, EngineUtilities, H2DataPointProjector, H2SegmentProjector}
import org.h2.expression.aggregate.AbstractAggregate
import org.h2.expression.function.FunctionCall
import org.h2.expression.{Expression, ExpressionColumn, ValueExpression}
import org.h2.table.{Column, TableFilter}
import org.h2.value._

import scala.annotation.switch
import scala.collection.mutable

object H2Projector {
  /** Instance Variable **/
  private val segmentProjectorCache = mutable.HashMap[Integer, H2SegmentProjector]()
  private val dataPointProjectorCache = mutable.HashMap[Integer, H2DataPointProjector]()
  private val argsField = classOf[AbstractAggregate].getDeclaredField("args")
  this.argsField.setAccessible(true)

  /** Public Methods * */
  def segmentProjection(segments: Iterator[SegmentGroup], filter: TableFilter): Iterator[Array[Value]] = {
    //NOTE: optimized projection methods are not generated for all combinations
    // of columns in the segment view due to technical limitations, the Scala
    // compiler cannot compile the large amount of generated code. Instead, an
    // appropriate subset is selected to match the requirement of queries from
    // the Data Point View and queries using the optimized UDAFs for aggregate
    // queries. This ensures most queries sent to the Segment View can use an
    // optimized projection method generated using static code generation,
    // despite the limitations of Scalac, while keeping the compile time low
    val currentValues = new Array[Value](EngineUtilities.segmentViewNameToIndex.size)
    val requiredColumns = tableFilterToColumns(filter)
    val columnNames =  requiredColumns.map(_.getName.toLowerCase)
    val target = EngineUtilities.computeJumpTarget(columnNames, EngineUtilities.segmentViewNameToIndex, 6)
    (target: @switch) match {
      case 0 => //COUNT(*)
        segments.map(_ => null)
      case 123 => //COUNT_S(#)
        segments.map(segment => {
          currentValues(0) = ValueInt.get(segment.gid) //Exploded so .gid is the tid
          currentValues(1) = ValueTimestamp.fromMillis(segment.startTime, 0)
          currentValues(2) = ValueTimestamp.fromMillis(segment.endTime, 0)
          currentValues
        })
      case 123456 => //Data Point View and UDAFs
        segments.map(segment  => {
          currentValues(0) = ValueInt.get(segment.gid) //Exploded so .gid is the tid
          currentValues(1) = ValueTimestamp.fromMillis(segment.startTime, 0)
          currentValues(2) = ValueTimestamp.fromMillis(segment.endTime, 0)
          currentValues(3) = ValueInt.get(segment.mtid)
          currentValues(4) = ValueBytes.get(segment.model)
          currentValues(5) = ValueBytes.get(segment.offsets)
          currentValues
        })
      //Static projections cannot be used for rows with dimensions
      case _ =>
        val projector = if (segmentProjectorCache.contains(target)) {
          segmentProjectorCache(target)
        } else {
          val projector = CodeGenerator.getH2SegmentProjection(requiredColumns)
          segmentProjectorCache(target) = projector
          projector
        }
        segments.map(segment => projector.project(segment, currentValues, H2.h2storage.timeSeriesMembersCache))
    }
  }

  def dataPointProjection(dataPoints: Iterator[DataPoint], filter: TableFilter): Iterator[Array[Value]] = {
    val currentValues = new Array[Value](EngineUtilities.dataPointViewNameToIndex.size)
    val requiredColumns = tableFilterToColumns(filter)
    val columnNames =  requiredColumns.map(_.getName.toLowerCase)
    val target = EngineUtilities.computeJumpTarget(columnNames, EngineUtilities.dataPointViewNameToIndex, 3)
    (target: @switch) match {
      case 0 => dataPoints.map(_ => null)
      case 1 => dataPoints.map(dataPoint => {
        currentValues(0) = ValueInt.get(dataPoint.tid)
        currentValues
      })
      case 2 => dataPoints.map(dataPoint => {
        currentValues(1) = ValueTimestamp.fromMillis(dataPoint.timestamp, 0)
        currentValues
      })
      case 3 => dataPoints.map(dataPoint => {
        currentValues(2) = ValueFloat.get(H2.h2storage.timeSeriesTransformationCache(dataPoint.tid)
          .transform(dataPoint.value, H2.h2storage.timeSeriesScalingFactorCache(dataPoint.tid)))
        currentValues
      })
      case 12 => dataPoints.map(dataPoint => {
        currentValues(0) = ValueInt.get(dataPoint.tid)
        currentValues(1) = ValueTimestamp.fromMillis(dataPoint.timestamp, 0)
        currentValues
      })
      case 13 => dataPoints.map(dataPoint => {
        currentValues(0) = ValueInt.get(dataPoint.tid)
        currentValues(2) = ValueFloat.get(H2.h2storage.timeSeriesTransformationCache(dataPoint.tid)
          .transform(dataPoint.value, H2.h2storage.timeSeriesScalingFactorCache(dataPoint.tid)))
        currentValues
      })
      case 23 => dataPoints.map(dataPoint => {
        currentValues(1) = ValueTimestamp.fromMillis(dataPoint.timestamp, 0)
        currentValues(2) = ValueFloat.get(H2.h2storage.timeSeriesTransformationCache(dataPoint.tid)
          .transform(dataPoint.value, H2.h2storage.timeSeriesScalingFactorCache(dataPoint.tid)))
        currentValues
      })
      case 123 => dataPoints.map(dataPoint => {
        currentValues(0) = ValueInt.get(dataPoint.tid)
        currentValues(1) = ValueTimestamp.fromMillis(dataPoint.timestamp, 0)
        currentValues(2) = ValueFloat.get(H2.h2storage.timeSeriesTransformationCache(dataPoint.tid)
          .transform(dataPoint.value, H2.h2storage.timeSeriesScalingFactorCache(dataPoint.tid)))
        currentValues
      })
      //Static projections cannot be used for rows with dimensions
      case _ =>
        val projector = if (dataPointProjectorCache.contains(target)) {
          dataPointProjectorCache(target)
        } else {
          val projector = CodeGenerator.getH2DataPointProjection(requiredColumns)
          dataPointProjectorCache(target) = projector
          projector
        }
        dataPoints.map(dataPoint => projector.project(dataPoint, currentValues, H2.h2storage.timeSeriesMembersCache,
          H2.h2storage.timeSeriesScalingFactorCache, H2.h2storage.timeSeriesTransformationCache))
    }
  }

  /** Private Methods **/
  private def tableFilterToColumns(filter: TableFilter): Array[Column] = {
    try {
      val columnNames = mutable.HashSet[String]()
      filter.getSelect.getExpressions.forEach(expression => {
        expresionToColumns(expression, columnNames)
      })
      columnNames.toArray.map(columnName => filter.findColumn(columnName)).sortBy(_.getColumnId)
    } catch { //An unsupported type of expression was found so all of the columns are returned
      case _: IllegalArgumentException => filter.getColumns
    }
  }

  private def expresionToColumns(expression: Expression, columnNames: mutable.HashSet[String]): Unit = {
    expression match {
      case abstractAggregate: AbstractAggregate => this.argsField.get(abstractAggregate)
        .asInstanceOf[Array[Expression]].foreach(expresionToColumns(_, columnNames))
      case functionCall: FunctionCall => functionCall.getArgs.foreach(expresionToColumns(_, columnNames))
      case expressionColumn: ExpressionColumn => columnNames.add(expressionColumn.getColumnName)
      case _: ValueExpression => //Ignore expressions that only contain values
      case p => Static.warn("ModelarDB: unsupported expression " + p.getClass, 120)
        throw new IllegalArgumentException() //Uses to unwind recursive stack as all columns must be returned
    }
  }
}