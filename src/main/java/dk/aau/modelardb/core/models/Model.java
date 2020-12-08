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
package dk.aau.modelardb.core.models;

import dk.aau.modelardb.core.DataPoint;
import dk.aau.modelardb.core.utility.Static;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

public abstract class Model implements Serializable {

    /** Constructors **/
    public Model(int mid, float error, int limit) {
        this.mid = mid;
        this.error = error;
        this.limit = limit;
    }

    /** Public Methods **/
    abstract public boolean append(DataPoint[] currentDataPoints);
    abstract public void initialize(List<DataPoint[]> currentSegment);
    abstract public byte[] parameters(long startTime, long endTime, int resolution, List<DataPoint[]> dps);
    abstract public Segment get(int sid, long startTime, long endTime, int resolution, byte[] parameters, byte[] offsets);
    abstract public int length();
    abstract public float size(long startTime, long endTime, int resolution, List<DataPoint[]> dps);

    public boolean withinErrorBound(float errorBound, Iterator<DataPoint> tsA, Iterator<DataPoint> tsB) {
        boolean allWithinErrorBound = true;
        while (allWithinErrorBound && tsA.hasNext() && tsB.hasNext()){
            allWithinErrorBound &= Static.percentageError(tsA.next().value, tsB.next().value) < errorBound;
        }
        return allWithinErrorBound;
    }

    final public float compressionRatio(long startTime, long endTime, int resolution, List<DataPoint[]> dps, int gaps) {
        //   DPs sid: int, ts: long, v: float
        // model sid: int, start_time: long, end_time: long, mid: int, parameters: bytes[], gaps: bytes[]
        //4 + 8 + 4 = 16 * data points is reduced to 4 + 8 + 8 + 4 + sizeof parameters + sizeof gaps
        return (16.0F * this.length()) / (24.0F + this.size(startTime, endTime, resolution, dps) + (4.0F * gaps));
    }

    final public float unsafeSize() {
        //Computes the size without providing the model with the information necessary for it to verify its precision
        return this.size(0L, 0L, 0, new java.util.ArrayList<>());
    }

    /** Instance Variables **/
    public final int mid;
    public final float error;
    public final int limit;
}
