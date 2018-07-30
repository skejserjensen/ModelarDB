/* Copyright 2018 Aalborg University
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

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import dk.aau.modelardb.core.DataPoint;
import dk.aau.modelardb.core.utility.Static;

public abstract class Segment implements Serializable {

    /** Constructors **/
    public Segment(int sid, long startTime, long endTime, int resolution, long[] gaps) {
        this.sid = sid;
        this.startTime = startTime;
        this.endTime = endTime;
        this.resolution = resolution;
        this.gaps = gaps;
    }

    public Segment(int sid, long startTime, long endTime, int resolution, byte[] gaps) {
        this.sid = sid;
        this.startTime = startTime;
        this.endTime = endTime;
        this.resolution = resolution;
        this.gaps = Static.bytesToLongs(gaps);
    }

    /** Public Methods **/
    public abstract byte[] parameters();

    public byte[] gaps() {
        ByteBuffer gapBuffer = ByteBuffer.allocate(8 * this.gaps.length);
        for (Long ts : gaps) {
            gapBuffer.putLong(ts);
        }
        gapBuffer.flip();
        return gapBuffer.array();
    }

    public int length() {
        //The current system only support time series with static resolution and plus one makes length inclusive
        int theoreticalSize = (int) ((this.endTime - this.startTime) / resolution) + 1;
        int gapsSize = 0;
        for (int i = 0; i < this.gaps.length - 1; i += 2) {
            //One element is subtracted from the gap length to not include the last element
            gapsSize += ((this.gaps[i + 1] - this.gaps[i]) / this.resolution) - 1;
        }
        return theoreticalSize - gapsSize;
    }

    public static long start(long fromTime, long startTime, long endTime, int resolution) {
        //The new start time is before the current start time, so no changes are performed
        if (fromTime <= startTime || endTime < fromTime ) {
            return startTime;
        }

        //The new start time is rounded up to match the sampling interval
        long diff = (fromTime - startTime) % resolution;
        return fromTime + (resolution - diff) - resolution;
    }

    public static long end(long toTime, long startTime, long endTime, int resolution) {
        //The new end time is after the current end time, so no changes are performed
        if (toTime < startTime || endTime <= toTime) {
            return endTime;
        }

        //The new end time is rounded down to match the sampling interval
        long diff = (endTime - toTime) % resolution;
        return toTime - (resolution - diff) + resolution;
    }

    public Stream<DataPoint> grid() {
        if (this.gaps.length == 0) {
            return gridNoGaps();
        } else if (this.gaps.length == 1) {
            return gridWithOffset();
        } else {
            return gridWithGaps();
        }
    }

    public String toString() {
        return "Segment: [" + sid + " | " + startTime + " | " + endTime + " | " + resolution + "]";
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

    /** Protected Methods **/
    protected abstract float get(long timestamp, int index);

    protected int getIndexWithOffset() {
        if (gaps.length % 2 != 0) {
            return (int) gaps[gaps.length - 1];
        } else {
            return 0;
        }
    }

    protected int capacity() {
        if (this.gaps.length == 0) {
            return (int) ((this.endTime - this.startTime) / resolution) + 1;
        }

        //Computes the total length of the time series even when it is restricted by a new start time or end time
        int theoreticalLength = (int) ((this.endTime - this.startTime) / resolution) + 1;
        int gapsLength = 0;
        for (int i = 0; i < this.gaps.length - 1; i += 2) {
            //One element is subtracted from the gap length to not include the last element
            gapsLength += ((this.gaps[i + 1] - this.gaps[i]) / this.resolution) - 1;
        }

        //Extracts the offset if one is set to offset a possible new start time
        int offset = 0;
        if (this.gaps.length % 2 != 0) {
            offset = (int) this.gaps[this.gaps.length - 1];
        }
        return theoreticalLength - gapsLength + offset;
    }

    /** Private Methods **/
    private Stream<DataPoint> gridNoGaps() {
        //NOTE: This methods is optimized using the assumption that no gaps exist
        int length = (int) ((this.endTime - this.startTime) / resolution) + 1;
        return IntStream.range(0, length).mapToObj(index -> {
            long ts = this.startTime + (this.resolution * (long) index);
            return new DataPoint(sid, ts, get(ts, index));
        });
    }

    private Stream<DataPoint> gridWithOffset() {
        //NOTE: This methods is optimized using the assumption that only an offset exist
        int offset = (int) this.gaps[this.gaps.length - 1];
        int length = (int) ((this.endTime - this.startTime) / resolution) + 1;
        return IntStream.range(offset, length + offset).mapToObj(index -> {
            long ts = this.startTime + (this.resolution * (long) index);
            return new DataPoint(sid, ts, get(ts, index));
        });
    }

    private Stream<DataPoint> gridWithGaps() {
        //NOTE: This methods is optimized using the assumption that only gaps exist
        int currentGap = 0;
        long gapsSize = 0;
        long ts = this.startTime;
        long runEnd = this.gaps[currentGap];
        long runStart = this.gaps[currentGap + 1];
        ArrayList<DataPoint> res = new ArrayList<>();

        //Reconstruct all data points represented by the segment
        while (ts <= this.endTime) {

            //The loop creates new data points as runs between gaps
            for (; ts <= runEnd; ts += this.resolution) {
                int index = (int) ((ts - gapsSize - this.startTime) / this.resolution);
                res.add(new DataPoint(sid, ts, get(ts, index)));
            }
            gapsSize += runStart - runEnd - this.resolution;
            ts = runStart;
            currentGap += 2;

            //Runs must be executed until no more gaps exist and we can run until end time
            if (currentGap < this.gaps.length) {
                runEnd = this.gaps[currentGap];
                runStart = this.gaps[currentGap + 1];
            } else {
                runEnd = this.endTime;
                runStart = this.endTime + this.resolution;
            }
        }
        return res.stream();
    }

    /** Instance Variables **/
    public final int sid;
    public final long startTime;
    public final long endTime;
    public final int resolution;
    private final long[] gaps;
}