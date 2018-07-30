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
import java.util.List;

import dk.aau.modelardb.core.DataPoint;

public abstract class Model implements Serializable {

    /** Constructors **/
    public Model(Class<? extends Segment> segmentType, float error, int limit) {
        this.error = error;
        this.limit = limit;
        this.segment = segmentType;
    }

    /** Public Methods **/
    abstract public boolean append(DataPoint currentDataPoint);
    abstract public void initialize(List<DataPoint> currentSegment);
    abstract public Segment get(int sid, long startTime, long endTime, int resolution,
                                List<DataPoint> currentSegment, long[] gaps);
    abstract public Segment get(int sid, long startTime, long endTime, int resolution, byte[] parameters, byte[] gaps);
    abstract public int length();
    abstract public float size();

    final public float compressionRatio() {
        //   DPs sid: int, ts: long, v: float
        // model sid: int, start_time: long, end_time: long, mid: int, arguments: bytes[]
        //4 + 8 + 4 = 16 * data points is reduced to 4 + 8 + 8 + 4 + sizeof parameters
        return (16.0F * this.length()) / (24.0F + this.size());
    }

    /** Protected Methods **/
    final protected boolean outsideErrorBound(double approximation, double real) {
        return percentageError(approximation, real) > this.error;
    }

    final protected double percentageError(double approximation, double real) {
        //Necessary if approximate and real are 0.0 as the division would return NaN instead of Infinity
        if (approximation == real) {
            return 0.0;
        }

        double difference = real - approximation;
        double result = Math.abs(difference / real);
        return result * 100.0;
    }

    final protected double percentageDifference(double a, double b) {
        //Necessary if approximate and difference are 0.0 as the division would return NaN instead of Infinity
        if (a == b) {
            return 0.0;
        }

        //Computes the difference between the two values in percentage, as both the
        //average or difference can be negative the absolute value is computed last
        double average = (a + b) / 2.0;
        double difference = a - b;
        double result = Math.abs(difference / average);
        return result * 100.0;
    }

    /** Instance Variables **/
    public final float error;
    public final int limit;
    public final Class<? extends Segment> segment;
}
