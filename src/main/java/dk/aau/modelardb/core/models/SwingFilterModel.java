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

import java.nio.ByteBuffer;
import java.util.List;

import dk.aau.modelardb.core.DataPoint;
import dk.aau.modelardb.core.utility.LinearFunction;

class SwingFilterModel extends Model {

    /** Constructors **/
    SwingFilterModel(float error, int limit) {
        super(SwingFilterSegment.class, error, limit);
        this.withinErrorBound = true;
    }

    /** Public Methods **/
    @Override
    public boolean append(DataPoint currentDataPoint) {
        if ( ! this.withinErrorBound) {
            return false;
        }

        //Calculates the absolute allowed deviation before the error bound is broken. While in theory the deviation
        // should be a calculated as the currentDataPoint.value * (this.error / 100.0)), however, due to the calculation
        // not being completely accurate, 100.0 would allow data points above the error at the 5 or above decimal.
        double deviation = Math.abs(currentDataPoint.value * (this.error / 100.1));

        if (this.currentSize == 0) {
            // Line 1 - 2
            this.initialDataPoint = currentDataPoint;
            this.currentSize += 1;
            return true;
        } else if (this.currentSize == 1) {
            // Line 3
            this.upperBound = new LinearFunction(
                    initialDataPoint.timestamp, initialDataPoint.value,
                    currentDataPoint.timestamp, currentDataPoint.value + deviation);
            this.lowerBound = new LinearFunction(
                    initialDataPoint.timestamp, initialDataPoint.value,
                    currentDataPoint.timestamp, currentDataPoint.value - deviation);
            this.currentSize += 1;
            return true;
        }

        //Line 6
        double uba = upperBound.get(currentDataPoint.timestamp);
        double lba = lowerBound.get(currentDataPoint.timestamp);

        if (uba + deviation < currentDataPoint.value || lba - deviation > currentDataPoint.value) {
            this.withinErrorBound = false;
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
            this.currentSize += 1;
            return true;
        }
    }

    @Override
    public void initialize(List<DataPoint> currentSegment) {
        this.currentSize = 0;
        this.withinErrorBound = true;

        for (DataPoint DataPoint : currentSegment) {
            if (!append(DataPoint)) {
                return;
            }
        }
    }

    @Override
    public Segment get(int sid, long startTime, long endTime, int resolution, List<DataPoint> currentSegment, long[] gaps) {
        //NOTE: As we do not need to minimize the average error we forgo calculating the line as specified in the paper
        return new SwingFilterSegment(sid, startTime, endTime, resolution, this.upperBound.a, this.upperBound.b, gaps);
    }

    @Override
    public Segment get(int sid, long startTime, long endTime, int resolution, byte[] parameters, byte[] gaps) {
        return new SwingFilterSegment(sid, startTime, endTime, resolution, parameters, gaps);
    }

    @Override
    public int length() {
        return this.currentSize;
    }

    @Override
    public float size() {
        //A linear function cannot be computed without two data points so we return NaN
        if (this.currentSize < 2) {
            return Float.NaN;
        }

        //The parameters might either be encoded as two floats, two doubles or a float and a double
        double a = this.upperBound.a;
        double b = this.upperBound.b;

        if ((double) (float) a == a && (double) (float) b == b) {
            return 8.0F;
        } else if ((double) (float) a == a) {
            return 12.0F;
        } else {
            return 16.0F;
        }
    }

    /** Instance Variable **/
    private int currentSize;
    private LinearFunction upperBound;
    private LinearFunction lowerBound;
    private DataPoint initialDataPoint;
    private boolean withinErrorBound;
}

class SwingFilterSegment extends Segment {

    /** Constructors **/
    SwingFilterSegment(int sid, long startTime, long endTime, int resolution, double a, double b, long[] gaps) {
        super(sid, startTime, endTime, resolution, gaps);
        this.a = a;
        this.b = b;
    }

    SwingFilterSegment(int sid, long startTime, long endTime, int resolution, byte[] parameters, byte[] gaps) {
        super(sid, startTime, endTime, resolution, gaps);
        ByteBuffer arguments = ByteBuffer.wrap(parameters);

        //Depending on the data being encoded the linear function might have required double precision decimals
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
    public byte[] parameters() {
        if ((double) (float) a == a && (double) (float) b == b) {
            return ByteBuffer.allocate(8).putFloat((float) this.a).putFloat((float) this.b).array();
        } else if ((double) (float) a == a) {
            return ByteBuffer.allocate(12).putFloat((float) this.a).putDouble(this.b).array();
        } else {
            return ByteBuffer.allocate(16).putDouble(this.a).putDouble(this.b).array();
        }
    }

    @Override
    public float min() {
        if (this.a == 0) {
            return (float) this.b;
        } else if (this.a > 0) {
            return this.get(this.startTime, 0);
        } else {
            return this.get(this.endTime, 0);
        }
    }

    @Override
    public float max() {
        if (this.a == 0) {
            return (float) this.b;
        } else if (this.a < 0) {
            return this.get(this.startTime, 0);
        } else {
            return this.get(this.endTime, 0);
        }
    }

    @Override
    public double sum() {
        double first = this.a * this.startTime + this.b;
        double last = this.a * this.endTime + this.b;
        double average = (first + last) / 2;
        return average * this.length();
    }

    /** Protected Methods **/
    @Override
    protected float get(long timestamp, int index) {
        return (float) (this.a * timestamp + this.b);
    }

    /** Instance Variable **/
    private final double a;
    private final double b;
}