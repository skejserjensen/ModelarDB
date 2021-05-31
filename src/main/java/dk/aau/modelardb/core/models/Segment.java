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
package dk.aau.modelardb.core.models;

import dk.aau.modelardb.core.DataPoint;
import dk.aau.modelardb.core.utility.CubeFunction;
import dk.aau.modelardb.core.utility.Static;
import org.apache.commons.lang.time.DateUtils;

import java.util.Calendar;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public abstract class Segment {

    /** Constructors **/
    public Segment(int tid, long startTime, long endTime, int samplingInterval, int[] offsets) {
        this.tid = tid;
        this.startTime = startTime;
        this.endTime = endTime;
        this.samplingInterval = samplingInterval;
        this.offsets = offsets;
    }

    public Segment(int tid, long startTime, long endTime, int samplingInterval, byte[] offsets) {
        this.tid = tid;
        this.startTime = startTime;
        this.endTime = endTime;
        this.samplingInterval = samplingInterval;
        this.offsets = Static.bytesToInts(offsets);
    }

    /** Public Methods **/
    public long getStartTime() {
        return this.startTime;
    }

    public long getEndTime() {
        return this.endTime;
    }

    public String toString() {
        return "Segment: [" + this.tid + " | " + new java.sql.Timestamp(this.startTime) + " | " + new java.sql.Timestamp(this.endTime) + " | " + this.samplingInterval + " | " + this.getClass() + "]";
    }

    public int length() {
        //Computes the number of data points represented by this segment
        return (int) ((this.endTime - this.startTime) / this.samplingInterval) + 1;
    }

    public int capacity() {
        //Computes the full length of the entire segment even if it is restricted by a new start time
        //Offsets is: [0] Group Offset, [1] Group Size, [2] Temporal Offset computed by Start()
        int currentLength = this.offsets[1] * this.length();
        if (this.offsets.length == 3) {
            currentLength = this.offsets[2] + currentLength;
        }
        return currentLength;
    }

    public static long start(long newStartTime, long startTime, long endTime, int samplingInterval, int[] offsets) {
        //The new start time is before the current start time, so no changes are performed
        if (newStartTime <= startTime || endTime < newStartTime) {
            return startTime;
        }

        //The new start time is rounded up to match the sampling interval
        long diff = (newStartTime - startTime) % samplingInterval;
        newStartTime = newStartTime + (samplingInterval - diff) - samplingInterval;
        offsets[2] = offsets[2] + (int) ((newStartTime - startTime) / samplingInterval);
        return newStartTime;
    }

    public static long end(long newEndTime, long startTime, long endTime, int samplingInterval) {
        //The new end time is later than the current end time, so no changes are performed
        if (newEndTime < startTime || endTime <= newEndTime) {
            return endTime;
        }

        //The new end time is rounded down to match the sampling interval
        long diff = (endTime - newEndTime) % samplingInterval;
        return newEndTime - (samplingInterval - diff) + samplingInterval;
    }

    public Stream<DataPoint> grid() {
        //If the segment have been restricted by start time the data points should be returned from an offset,
        // offsets store the following offsets: [0] Group Offset, [1] Group Size, [2] Temporal Offset computed by Start()
        int groupOffset = this.offsets[0] - 1;
        int groupSize = this.offsets[1];
        int temporalOffset = this.offsets[2];

        return IntStream.range(0, this.length()).mapToObj(index -> {
            long ts = this.startTime + (this.samplingInterval * (long) index);
            return new DataPoint(tid, ts, get(ts, (index + temporalOffset) * groupSize + groupOffset));
        });
    }

    public float min() {
        return this.grid().min((s1, s2) -> Float.compare(s1.value, s2.value)).get().value;
    }

    public float max() {
        return this.grid().max((s1, s2) -> Float.compare(s1.value, s2.value)).get().value;
    }

    public double sum() {
        return this.grid().mapToDouble(s -> s.value).sum();
    }

    public double[] cube(Calendar calendar, int type, CubeFunction aggregator, double[] result) {
        //The original start time and end time is stored so they can be restored at the end of the method
        long originalStartTime = this.startTime;
        long originalEndTime = this.endTime;
        int originalOffset = this.offsets[2];

        //Computes a new end time that is the end of the first interval of size type in the time dimension
        calendar.setTimeInMillis(this.startTime);
        calendar = DateUtils.ceiling(calendar, type);
        this.endTime = calendar.getTimeInMillis() - this.samplingInterval;
        calendar.setTimeInMillis(this.startTime);
        int field = calendar.get(type);

        //For each time interval until the original end time the specified aggregate is computed and stored in result
        calendar = DateUtils.ceiling(calendar, type);
        while (this.endTime < originalEndTime) {
            aggregator.aggregate(this, this.tid, field, result);

            //Moves the start time and end time to delimit the next interval to compute the aggregate for
            field = calendar.get(type);
            long previousStartTime = this.startTime;
            this.startTime = this.endTime + this.samplingInterval;
            this.offsets[2] = this.offsets[2] + (int) ((this.startTime - previousStartTime) / this.samplingInterval);
            calendar = DateUtils.ceiling(calendar, type);
            this.endTime = calendar.getTimeInMillis() - this.samplingInterval;
        }

        //The last time interval ends with the original end time
        this.endTime = originalEndTime;
        aggregator.aggregate(this, this.tid, field, result);
        this.startTime = originalStartTime;
        this.offsets[2] = originalOffset;
        return result;
    }

    /** Protected Methods **/
    protected abstract float get(long timestamp, int index);

    protected int getGroupOffset() {
        return this.offsets[0] - 1;
    }

    protected int getGroupSize() {
        return this.offsets[1];
    }

    protected int getTemporalOffset() {
        return this.offsets[2];
    }

    /** Instance Variables **/
    public final int tid;
    public final int samplingInterval;
    private long startTime;
    private long endTime;
    private final int[] offsets;
}
