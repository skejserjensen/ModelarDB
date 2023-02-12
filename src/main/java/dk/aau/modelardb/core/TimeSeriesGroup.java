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
package dk.aau.modelardb.core;

import dk.aau.modelardb.core.timeseries.AsyncTimeSeries;
import dk.aau.modelardb.core.timeseries.TimeSeries;

import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.Selector;
import java.util.Arrays;
import java.util.Set;
import java.util.StringJoiner;

public class TimeSeriesGroup implements Serializable {

    /** Constructors **/
    public TimeSeriesGroup(int gid, TimeSeries[] timeSeries) {
        if (timeSeries.length == 0) {
            throw new UnsupportedOperationException("CORE: a group must consist of at least one time series");
        }

        //Each time series is assumed to have the same sampling interval, alignment, and boundness
        this.isAsync = timeSeries[0] instanceof AsyncTimeSeries;
        this.samplingInterval = timeSeries[0].samplingInterval;
        for (TimeSeries ts : timeSeries) {
            if (this.isAsync != ts instanceof AsyncTimeSeries) {
                throw new UnsupportedOperationException("CORE: All time series in a group must be bounded or unbounded");
            }

            if (this.samplingInterval != ts.samplingInterval) {
                throw new UnsupportedOperationException("CORE: All time series in a group must share the same sampling interval");
            }
        }

        //Initializes variables for holding the latest data point for each time series
        this.gid = gid;
        this.timeSeries = timeSeries;
        this.nextDataPoints = new DataPoint[timeSeries.length];
        this.currentDataPoints = new DataPoint[timeSeries.length];
        this.next = Long.MAX_VALUE;
        this.timeSeriesHasNext = timeSeries.length;
    }

    TimeSeriesGroup(TimeSeriesGroup tsg, int[] splitIndex) {
        this.gid = tsg.gid;
        this.nextDataPoints = new DataPoint[splitIndex.length];
        this.currentDataPoints = new DataPoint[splitIndex.length];
        this.timeSeries = new TimeSeries[splitIndex.length];
        this.next = tsg.next;

        int j = 0;
        for(int i : splitIndex) {
            this.timeSeriesHasNext += tsg.getTimeSeries()[i].hasNext() ? 1 : 0;
            this.nextDataPoints[j] = tsg.nextDataPoints[i];
            this.timeSeries[j] = tsg.timeSeries[i];
            j++;
        }
        this.isAsync = this.timeSeries[0] instanceof AsyncTimeSeries;
        this.samplingInterval = this.timeSeries[0].samplingInterval;
        tsg.timeSeriesHasNext = 0;
    }

    TimeSeriesGroup(Set<TimeSeriesGroup> tsgs, int[] joinIndex) {
        this.gid = tsgs.iterator().next().gid;
        this.nextDataPoints = new DataPoint[joinIndex.length];
        this.currentDataPoints = new DataPoint[joinIndex.length];
        this.timeSeries = new TimeSeries[joinIndex.length];
        this.next = Long.MAX_VALUE;

        for (TimeSeriesGroup tsg : tsgs) {
            this.timeSeriesHasNext += tsg.timeSeriesHasNext;
            this.next = Math.min(this.next, tsg.next);

            for (int i = 0; i < tsg.timeSeries.length; i++) {
                TimeSeries ts = tsg.timeSeries[i];
                int index = Arrays.binarySearch(joinIndex, ts.tid);
                this.nextDataPoints[index] = tsg.nextDataPoints[i];
                this.currentDataPoints[index] = tsg.currentDataPoints[i];
                this.timeSeries[index] = tsg.timeSeries[i];
            }
            tsg.timeSeriesHasNext = 0;
        }
        this.isAsync = this.timeSeries[0] instanceof AsyncTimeSeries;
        this.samplingInterval = this.timeSeries[0].samplingInterval;
    }

    /** Public Methods **/
    public void initialize() {
        for (int i = 0; i < this.timeSeries.length; i++) {
            TimeSeries ts = this.timeSeries[i];
            ts.open();

            //Stores the first data point from each time series to track when a gap occurs
            if (ts.hasNext()) {
                this.nextDataPoints[i] = ts.next();
                if (this.nextDataPoints[i] == null) {
                    throw new IllegalArgumentException("CORE: unable to initialize " + this.timeSeries[i].source);
                }
                this.next = Math.min(this.next, this.nextDataPoints[i].timestamp);
            }
        }
    }

    public void attachToSelector(Selector s, SegmentGenerator mg) throws IOException {
        for(TimeSeries ts : this.timeSeries) {
            if (ts instanceof AsyncTimeSeries) {
                ((AsyncTimeSeries) ts).attachToSelector(s, mg);
            }
        }
    }

    public TimeSeries[] getTimeSeries() {
        return this.timeSeries;
    }

    public String getTids() {
        StringJoiner sj = new StringJoiner(",", "{", "}");
        for (TimeSeries ts : this.timeSeries) {
            sj.add(Integer.toString(ts.tid));
        }
        return sj.toString();
    }

    public String getSources() {
        StringJoiner sj = new StringJoiner(",", "{", "}");
        for (TimeSeries ts : this.timeSeries) {
            sj.add(ts.source);
        }
        return sj.toString();
    }

    public int size() {
        return this.timeSeries.length;
    }

    public boolean hasNext() {
        return this.timeSeriesHasNext != 0;
    }

    public DataPoint[] next() {
        //Prepares the data points for the next SI
        this.timeSeriesActive = 0;
        this.timeSeriesActive = this.timeSeries.length;
        for (int i = 0; i < this.timeSeries.length; i++) {
            TimeSeries ts = this.timeSeries[i];

            if (this.nextDataPoints[i].timestamp == this.next) {
                //No gap have occurred so this data point can be emitted in this iteration
                currentDataPoints[i] = this.nextDataPoints[i];
                if (ts.hasNext()) {
                    this.nextDataPoints[i] = ts.next();
                } else {
                    this.timeSeriesHasNext--;
                }
            } else {
                //A gap have occurred so this data point cannot be not emitted in this iteration
                currentDataPoints[i] = new DataPoint(ts.tid, this.next, Float.NaN);
                this.timeSeriesActive--;
            }
        }
        this.next += this.samplingInterval;
        return this.currentDataPoints;
    }

    public int getActiveTimeSeries() {
        return this.timeSeriesActive;
    }

    public void close() {
        for (TimeSeries ts : this.timeSeries) {
            ts.close();
        }
    }

    /** Instance Variables **/
    //Updated by local instances after tids have been received by a remote instance
    public int gid;

    public final boolean isAsync;
    public final int samplingInterval;

    private int timeSeriesActive;
    private int timeSeriesHasNext;
    private final TimeSeries[] timeSeries;

    private long next;
    private final DataPoint[] currentDataPoints;
    private final DataPoint[] nextDataPoints;
}
