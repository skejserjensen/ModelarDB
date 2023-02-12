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
package dk.aau.modelardb.engines.spark

import dk.aau.modelardb.core.SegmentGroup
import dk.aau.modelardb.core.utility.Static
import dk.aau.modelardb.engines.EngineUtilities
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext, sources}

import java.sql.Timestamp

class ViewSegment(dimensions: Array[StructField]) (@transient val sqlContext: SQLContext)
  extends BaseRelation with PrunedFilteredScan {

  /** Public Methods **/
  override def schema: StructType = StructType(Seq(
    StructField("tid", IntegerType, nullable = false),
    StructField("start_time", TimestampType, nullable = false),
    StructField("end_time", TimestampType, nullable = false),
    StructField("mtid", IntegerType, nullable = false),
    StructField("model", BinaryType, nullable = false),
    StructField("offsets", BinaryType, nullable = false))
    ++ dimensions)

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = filters

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    //DEBUG: prints the columns and predicates Spark has pushed to the view
    Static.info("ModelarDB: segment required columns { " + requiredColumns.mkString(" ") + " }")
    Static.info("ModelarDB: segment provided filters { " + filters.mkString(" ") + " }")

    //Extracts segment groups from the segment group store and expand each into a set of segments
    val segmentGroupRows = getSegmentGroupRDD(filters)
    val segmentGroupRowToSegmentRows = getSegmentGroupRowToSegmentRows
    val segmentRows = segmentGroupRows.flatMap(segmentGroupRowToSegmentRows(_))
    SparkProjector.segmentProjection(segmentRows, requiredColumns)
  }

  /** Private Methods **/
  private def getSegmentGroupRDD(filters: Array[Filter]): RDD[Row] = {
    //Tids and members are mapped to Gids so only segments from the necessary groups are retrieved
    val tsgc = Spark.getSparkStorage.timeSeriesGroupCache
    val mtsc = Spark.getSparkStorage.memberTimeSeriesCache
    val gidFilters: Array[Filter] = filters.map {
      case sources.EqualTo("tid", tid: Int) => sources.EqualTo("gid", EngineUtilities.tidPointToGidPoint(tid, tsgc))
      case sources.EqualNullSafe("tid", tid: Int) => sources.EqualNullSafe("gid", EngineUtilities.tidPointToGidPoint(tid, tsgc))
      case sources.GreaterThan("tid", tid: Int) => sources.In("gid", EngineUtilities.tidRangeToGidIn(tid + 1, tsgc.length, tsgc))
      case sources.GreaterThanOrEqual("tid", tid: Int) => sources.In("gid", EngineUtilities.tidRangeToGidIn(tid, tsgc.length, tsgc))
      case sources.LessThan("tid", tid: Int) => sources.In("gid", EngineUtilities.tidRangeToGidIn(0, tid - 1, tsgc))
      case sources.LessThanOrEqual("tid", tid: Int) => sources.In("gid", EngineUtilities.tidRangeToGidIn(0, tid, tsgc))
      case sources.In("tid", values: Array[Any]) => sources.In("gid", EngineUtilities.tidInToGidIn(values, tsgc))
      case sources.EqualTo(column: String, value: Any) if mtsc.contains(column.toUpperCase) => //mtsc's keys are uppercase for consistency
        sources.In("gid", EngineUtilities.dimensionEqualToGidIn(column.toUpperCase, value, mtsc))
      case f => f
    }

    //DEBUG: prints the predicates spark provides the segment group store after query rewriting
    Static.info("ModelarDB: segment rewritten filters { " + gidFilters.mkString(" ") + " }", 120)
    this.cache.getSegmentGroupRDD(gidFilters)
  }

  private def getSegmentGroupRowToSegmentRows: Row => Array[Row] = {
    val storage = Spark.getSparkStorage
    val gmdc = storage.groupMetadataCache
    val gdc = storage.groupDerivedCache
    row =>
      val sg = new SegmentGroup(row.getInt(0), row.getTimestamp(1).getTime, row.getTimestamp(2).getTime,
        row.getInt(3), row.getAs[Array[Byte]](4), row.getAs[Array[Byte]](5))
      sg.explode(gmdc, gdc).map(e =>
        Row(e.gid, new Timestamp(e.startTime), new Timestamp(e.endTime), e.mtid, e.model, e.offsets))
  }

  /** Instance Variables **/
  private val cache = Spark.getCache
}
