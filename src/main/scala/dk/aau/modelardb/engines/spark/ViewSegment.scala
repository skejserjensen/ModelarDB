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

import java.sql.Timestamp

import dk.aau.modelardb.core.SegmentGroup
import dk.aau.modelardb.core.utility.Static
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext, sources}

class ViewSegment(dimensions: Array[StructField]) (@transient val sqlContext: SQLContext)
  extends BaseRelation with PrunedFilteredScan {

  /** Public Methods **/
  override def schema = StructType(Seq(
    StructField("sid", IntegerType, nullable = false),
    StructField("st", TimestampType, nullable = false),
    StructField("et", TimestampType, nullable = false),
    StructField("res", IntegerType, nullable = false),
    StructField("mid", IntegerType, nullable = false),
    StructField("param", BinaryType, nullable = false),
    StructField("gaps", BinaryType, nullable = false))
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
    SparkGridder.segmentProjection(segmentRows, requiredColumns)
  }

  /** Private Methods **/
  private def getSegmentGroupRDD(filters: Array[Filter]): RDD[Row] = {
    //Sids and members are mapped to Gids so only segments from the necessary groups are retrieved
    val sgc = Spark.getStorage.getSourceGroupCache
    val idc = Spark.getStorage.getInverseDimensionsCache

    val maxSid = sgc.length
    val gidFilters: Array[Filter] = filters.map {
      case sources.EqualTo("sid", sid: Int) => sidPointToGidPoint(sid, sgc)
      case sources.EqualNullSafe("sid", sid: Int) => sidPointToGidPoint(sid, sgc)
      case sources.GreaterThan("sid", sid: Int) => sidRangeToGidIn(sid + 1, maxSid, sgc, maxSid)
      case sources.GreaterThanOrEqual("sid", sid: Int) => sidRangeToGidIn(sid, maxSid, sgc, maxSid)
      case sources.LessThan("sid", sid: Int) => sidRangeToGidIn(0, sid - 1, sgc, maxSid)
      case sources.LessThanOrEqual("sid", sid: Int) => sidRangeToGidIn(0, sid, sgc, maxSid)
      case sources.In("sid", values: Array[Any]) => sidInToGidIn(values, sgc, maxSid)
      case sources.IsNull("sid") => sources.IsNull("gid")
      case sources.IsNotNull("sid") => sources.IsNotNull("gid")
      case sources.EqualTo(column: String, value: Any) if idc.containsKey(column) =>
        sources.In("gid", idc.get(column).getOrDefault(value, Array(new Integer(-1))).asInstanceOf[Array[Any]])
      case f => f
    }

    //DEBUG: prints the predicates spark provides the segment group store after query rewriting
    Static.info("ModelarDB: segment rewritten filters { " + gidFilters.mkString(" ") + " }", 120)
    this.cache.getSegmentGroupRDD(gidFilters)
  }

  private def sidPointToGidPoint(sid: Int, sgc: Array[Int]): Filter = {
    if (sid < sgc.length) {
      sources.EqualTo("gid", sgc(sid))
    } else {
      sources.EqualTo("gid", -1)
    }
  }

  private def sidRangeToGidIn(startSid: Int, endSid: Int, sgc: Array[Int], maxSid: Int): Filter = {
    if (endSid <= 0 || startSid >= maxSid) {
      //All sids are outside the range of assigned sids, so a sentinel is used to ensure no gids match
      return sources.EqualTo("gid", -1)
    }

    //All sids within the range of assigned sids are translated with the set removing duplicates
    val sids = scala.collection.mutable.Set[Int]()
    for (sid <- Math.max(startSid, 1) to Math.min(endSid, maxSid)) {
      sids.add(sgc(sid))
    }
    sources.In("gid", sids.toArray)
  }

  private def sidInToGidIn(sids: Array[Any], sgc: Array[Int], maxSid: Int): Filter = {
    sources.In("gid",
      sids.map(obj => {
        val sid = obj.asInstanceOf[Int]
        if (sid <= 0 || maxSid < sid) -1 else sgc(sid)
      }))
  }

  private def getSegmentGroupRowToSegmentRows: Row => Array[Row] = {
    val storage = Spark.getStorage
    val gmdc = storage.getGroupMetadataCache
    row =>
      val sg = new SegmentGroup(row.getInt(0), row.getTimestamp(1).getTime, row.getTimestamp(2).getTime,
        row.getInt(3), row.getAs[Array[Byte]](4), row.getAs[Array[Byte]](5))
      val exploded = sg.explode(gmdc)
      val resolution = gmdc(sg.gid)(0)
      exploded.map(e =>
        Row(e.gid, new Timestamp(e.startTime), new Timestamp(e.endTime), resolution, e.mid, e.parameters, e.offsets))
  }

  /** Instance Variables **/
  private val cache = Spark.getCache
}
