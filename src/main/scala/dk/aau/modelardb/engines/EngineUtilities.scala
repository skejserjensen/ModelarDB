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
package dk.aau.modelardb.engines

import dk.aau.modelardb.core.Dimensions

import java.sql.{SQLException, Timestamp}

import scala.collection.mutable

object EngineUtilities {

  /** Public Methods **/
  //Projection
  def initialize(dimensions: Dimensions): Unit = {
    //Segment View
    this.segmentViewNameToIndex = Map(
      "tid" -> 1,
      "start_time" -> 2,
      "end_time" -> 3,
      "mtid" -> 4,
      "model" -> 5,
      "offsets" -> 6) ++
      dimensions.getColumns.zipWithIndex.map(p => (p._1, p._2 + 7)).toMap

    //Data Point View
    this.dataPointViewNameToIndex = Map(
      "tid" -> 1,
      "timestamp" -> 2,
      "value" -> 3) ++
      dimensions.getColumns.zipWithIndex.map(p => (p._1, p._2 + 4)).toMap
  }

  def computeJumpTarget(requiredColumns: Array[String], map: Map[String, Int], staticColumns: Int): Int = {
    var factor: Int = 1
    var target: Int = 0
    //Computes a key for a statically generated projection function as the query only require static columns
    for (col: String <- requiredColumns.reverseIterator) {
      val index = map(col)
      if (index > staticColumns) {
        return Integer.MAX_VALUE
      }
      target = target + factor * index
      factor = factor * 10
    }
    target
  }

  //Predicate Push-down
  //Tid => Gid
  def tidPointToGidPoint(tid: Int, tsgc: Array[Int]): Int = {
    if (0 < tid && tid < tsgc.length) {
      tsgc(tid)
    } else {
      -1
    }
  }

  def tidRangeToGidIn(startTid: Int, endTid: Int, tsgc: Array[Int]): Array[Any] = {
    val maxTid = tsgc.length
    if (endTid <= 0 || startTid >= maxTid) {
      //All tids are outside the range of assigned tids, so a sentinel is used to ensure no gids match
      return Array(-1)
    }

    //All tids within the range of assigned tids are translated with the set removing duplicates
    val gids = scala.collection.mutable.Set[Int]()
    for (tid <- Math.max(startTid, 1) to Math.min(endTid, maxTid)) {
      gids.add(tsgc(tid))
    }
    gids.toArray
  }

  def tidInToGidIn(tids: Array[Any], tsgc: Array[Int]): Array[Any] = {
    val maxTid = tsgc.length

    //All tids in the IN clause are translated with the set removing duplicates
    val gids = scala.collection.mutable.Set[Int]()
    tids.foreach(obj => {
      val tid = obj.asInstanceOf[Int]
      gids.add(if (tid <= 0 || maxTid < tid) -1 else tsgc(tid))
    })
    gids.toArray
  }

  //Dimensions => Gid
  def dimensionEqualToGidIn(column: String, value: Any,
                            idc: mutable.HashMap[String, mutable.HashMap[Object, Array[Integer]]]): Array[Any] = {
    idc(column).getOrElse(value.asInstanceOf[Object], Array(Integer.valueOf(-1))).asInstanceOf[Array[Any]]
  }

  //Timestamp => BigInt
  def toLongThroughTimestamp(columnValue: AnyRef): Long = {
    columnValue match {
      case ts: Timestamp => ts.getTime
      case str: String => Timestamp.valueOf(str).getTime
      case cl => throw new SQLException(s"ModelarDB: a ${cl.getClass} cannot be converted to a Timestamp")
    }
  }

  /** Instance Variables **/
  var segmentViewNameToIndex: Map[String, Int] = _
  var dataPointViewNameToIndex: Map[String, Int] = _
}
