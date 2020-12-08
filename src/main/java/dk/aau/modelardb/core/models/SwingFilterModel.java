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
import dk.aau.modelardb.core.utility.LinearFunction;
import dk.aau.modelardb.core.utility.Static;

import java.nio.ByteBuffer;
import java.util.List;

class SwingFilterModel extends Model {

    /** Constructors **/
    SwingFilterModel(int mid, float error, int limit) {
        super(mid, error, limit);
        this.withinErrorBound = true;
    }

    /** Public Methods **/
    @Override
    public boolean append(DataPoint[] currentDataPoints) {
        if ( ! this.withinErrorBound) {
            return false;
        }

        //Allows the size to be be updated after adding the second data point without the need of branches
        int currentSize = this.currentSize;
        int nextSize = this.currentSize + 1;

        if (this.currentSize == 0) {
            //An average data point must be constructed so all data points in the group are within the error bound
            float min = Static.min(currentDataPoints);
            float max = Static.max(currentDataPoints);
            float avg = Static.avg(currentDataPoints);
            if (Static.outsidePercentageErrorBound(this.error, avg, min) ||
                    Static.outsidePercentageErrorBound(this.error, avg, max)) {
                this.withinErrorBound = false;
                return false;
            }

            // Line 1 - 2
            this.initialDataPoint = new DataPoint(currentDataPoints[0].sid, currentDataPoints[0].timestamp, avg);
        } else {
            //Expect for the first set of data point, all data points can be appended one at a time
            for (DataPoint currentDataPoint : currentDataPoints) {
                //Calculates the absolute allowed deviation before the error bound is exceeded. In theory the deviation
                // should be calculated as the Math.abs(currentDataPoint.value * (this.error / 100.0)). However, due to
                // the calculation not being perfectly accurate, 100.0 allows data points slightly above the error bound
                double deviation = Math.abs(currentDataPoint.value * (this.error / 100.1));

                if (this.currentSize == 1) {
                    // Line 3
                    this.upperBound = new LinearFunction(
                            initialDataPoint.timestamp, initialDataPoint.value,
                            currentDataPoint.timestamp, currentDataPoint.value + deviation);
                    this.lowerBound = new LinearFunction(
                            initialDataPoint.timestamp, initialDataPoint.value,
                            currentDataPoint.timestamp, currentDataPoint.value - deviation);
                    this.currentSize = nextSize;
                } else {
                    //Line 6
                    double uba = upperBound.get(currentDataPoint.timestamp);
                    double lba = lowerBound.get(currentDataPoint.timestamp);

                    if (uba + deviation < currentDataPoint.value || lba - deviation > currentDataPoint.value) {
                        this.withinErrorBound = false;
                        this.currentSize = currentSize;
                        return false;
                    } else {
                        //Line 16
                        if (uba - deviation > currentDataPoint.value) {
                            this.upperBound = new LinearFunction(
                                    initialDataPoint.timestamp, initialDataPoint.value,
                                    currentDataPoint.timestamp, currentDataPoint.value + deviation);
                        }
                        //Line 15
                        if (lba + deviation < currentDataPoint.value) {
                            this.lowerBound = new LinearFunction(
                                    initialDataPoint.timestamp, initialDataPoint.value,
                                    currentDataPoint.timestamp, currentDataPoint.value - deviation);
                        }
                    }
                }
            }
        }
        this.currentSize = nextSize;
        return true;
    }

    @Override
    public void initialize(List<DataPoint[]> currentSegment) {
        this.currentSize = 0;
        this.withinErrorBound = true;

        for (DataPoint[] dataPoints : currentSegment) {
            if ( ! append(dataPoints)) {
                return;
            }
        }
    }

    @Override
    public byte[] parameters(long startTime, long endTime, int resolution, List<DataPoint[]> dps) {
        //All lines within the two bounds are valid but always selecting one of the bounds add unnecessary error to sums
        double a = (this.lowerBound.a + this.upperBound.a) / 2.0;
        double b = (this.lowerBound.b + this.upperBound.b) / 2.0;

        if ((double) (float) a == a && (double) (float) b == b) {
            return ByteBuffer.allocate(8).putFloat((float) a).putFloat((float) b).array();
        } else if ((double) (float) a == a) {
            return ByteBuffer.allocate(12).putFloat((float) a).putDouble(b).array();
        } else {
            return ByteBuffer.allocate(16).putDouble(a).putDouble(b).array();
        }
    }


    @Override
    public Segment get(int sid, long startTime, long endTime, int resolution, byte[] parameters, byte[] offsets) {
        return new SwingFilterSegment(sid, startTime, endTime, resolution, parameters, offsets);
    }

    @Override
    public int length() {
        return this.currentSize;
    }

    @Override
    public float size(long startTime, long endTime, int resolution, List<DataPoint[]> dps) {
        //A linear function cannot be computed without at least two data points so we return NaN
        if (this.currentSize < 2) {
            return Float.NaN;
        }

        //All lines within the two bounds are valid but always selecting one of the bounds add unnecessary error to sums
        double a = (this.lowerBound.a + this.upperBound.a) / 2.0;
        double b = (this.lowerBound.b + this.upperBound.b) / 2.0;

        //Verifies that the model has the necessary precession to be utilized, while the function computed in theory
        // should not exceed the error bound it can do so (especially with 0% error) due to floating-point imprecision
        for (int i = 0; startTime < endTime + resolution; i++, startTime += resolution) {
            DataPoint[] dpa = dps.get(i);
            float approximation = (float) (a * dpa[0].timestamp + b);
            for (DataPoint dp : dpa) {
                if (Static.outsidePercentageErrorBound(this.error, approximation, dp.value)) {
                    return Float.NaN;
                }
            }
        }

        //Determines if we need to use doubles or if floats are precise enough for the parameters
        if ((double) (float) a == a && (double) (float) b == b) {
            return 8.0F;
        } else if ((double) (float) a == a) {
            return 12.0F;
        } else {
            return 16.0F;
        }
    }

    /** Instance Variables **/
    private int currentSize;
    private LinearFunction upperBound;
    private LinearFunction lowerBound;
    private DataPoint initialDataPoint;
    private boolean withinErrorBound;
}

class SwingFilterSegment extends Segment {

    /** Constructors **/
    SwingFilterSegment(int sid, long startTime, long endTime, int resolution, byte[] parameters, byte[] offsets) {
        super(sid, startTime, endTime, resolution, offsets);
        ByteBuffer arguments = ByteBuffer.wrap(parameters);

        //Depending on the data being encoded, the linear function might have required double precision floating-point
        if (parameters.length == 16) {
            this.a = arguments.getDouble();
            this.b = arguments.getDouble();
        } else if (parameters.length == 12) {
            this.a = arguments.getFloat();
            this.b = arguments.getDouble();
        } else {
            this.a = arguments.getFloat();
            this.b = arguments.getFloat();
        }
    }

    /** Public Methods **/
    @Override
    public float min() {
        if (this.a == 0) {
            return (float) this.b;
        } else if (this.a > 0) {
            return this.get(this.getStartTime(), 0);
        } else {
            return this.get(this.getEndTime(), 0);
        }
    }

    @Override
    public float max() {
        if (this.a == 0) {
            return (float) this.b;
        } else if (this.a < 0) {
            return this.get(this.getStartTime(), 0);
        } else {
            return this.get(this.getEndTime(), 0);
        }
    }

    @Override
    public double sum() {
        double first = this.a * this.getStartTime() + this.b;
        double last = this.a * this.getEndTime() + this.b;
        double average = (first + last) / 2;
        return average * this.length();
    }

    /** Protected Methods **/
    @Override
    protected float get(long timestamp, int index) {
        return (float) (this.a * timestamp + this.b);
    }

    /** Instance Variables **/
    private final double a;
    private final double b;
}
