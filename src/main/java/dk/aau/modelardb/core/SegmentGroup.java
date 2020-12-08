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
package dk.aau.modelardb.core;

import dk.aau.modelardb.core.models.Model;
import dk.aau.modelardb.core.models.Segment;
import dk.aau.modelardb.core.utility.Static;

import java.nio.ByteBuffer;

public class SegmentGroup {

    /** Constructors **/
    public SegmentGroup(int gid, long startTime, long endTime, int mid, byte[] parameters, byte[] offsets) {
        this.gid = gid;
        this.startTime = startTime;
        this.endTime = endTime;
        this.mid = mid;
        this.parameters = parameters;
        this.offsets = offsets;
    }

    /** Public Methods **/
    public String toString() {
        //The segments might not represent all time series in the time series group
        int[] os = Static.bytesToInts(this.offsets);
        StringBuilder sb = new StringBuilder();
        sb.append("Segment: [").append(this.gid).append(" | ").append(this.startTime).append(" | ")
                .append(this.endTime).append(" | ").append(this.mid);
        for (int o : os) {
            sb.append(" | ").append(o);
        }
        sb.append("]");
        return sb.toString();
    }

    public SegmentGroup[] explode(int[][] groupMetadataCache) {
        int[] gmc = groupMetadataCache[this.gid];
        int[] missingTimeSeries = Static.bytesToInts(this.offsets);
        int temporalOffset = 0;
        if (missingTimeSeries.length > 0 && missingTimeSeries[missingTimeSeries.length - 1] < 0) {
            temporalOffset = -1 * missingTimeSeries[missingTimeSeries.length - 1];
        }
        int groupSize = gmc.length - 1 - missingTimeSeries.length;
        SegmentGroup[] sg = new SegmentGroup[groupSize];

        //If no gaps exist we can simply reconstruct all segments
        if (missingTimeSeries.length == 0) {
            int segmentGroup = 0;
            for (int index = 1; index < gmc.length; index++) {
                int sid = gmc[index];
                //Offsets store the following: [0] Group Offset, [1] Group Size, [2] Temporal Offset
                byte[] offset = ByteBuffer.allocate(12).putInt(segmentGroup + 1).putInt(groupSize).putInt(temporalOffset).array();
                sg[segmentGroup] = new SegmentGroup(sid, this.startTime, this.endTime, this.mid, this.parameters, offset);
                segmentGroup++;
            }
        } else {
            int segmentGroup = 0;
            for (int index = 1; index < gmc.length; index++) {
                int sid = gmc[index];
                if ( ! Static.contains(sid, missingTimeSeries)) {
                    //Offsets store the following: [0] Group Offset, [1] Group Size, [2] Temporal Offset
                    byte[] offset = ByteBuffer.allocate(12).putInt(segmentGroup + 1).putInt(groupSize).putInt(temporalOffset).array();
                    sg[segmentGroup] = new SegmentGroup(sid, this.startTime, this.endTime, this.mid, this.parameters, offset);
                    segmentGroup++;
                }
            }
        }
        return sg;
    }

    public Segment[] toSegments(Storage storage) {
        int[][] groupMetadataCache = storage.getGroupMetadataCache();
        SegmentGroup[] sgs = this.explode(groupMetadataCache);
        Segment[] segments = new Segment[sgs.length];

        Model m = storage.getModelCache()[mid];
        int[] gmc = groupMetadataCache[this.gid];
        for (int i = 0; i < sgs.length; i++) {
            segments[i] = m.get(sgs[i].gid, this.startTime, this.endTime, gmc[0], this.parameters, sgs[i].offsets);
        }
        return segments;
    }

    /** Instance Variables **/
    public final int gid;
    public final long startTime;
    public final long endTime;
    public final int mid;
    public final byte[] parameters;
    public final byte[] offsets;
}
