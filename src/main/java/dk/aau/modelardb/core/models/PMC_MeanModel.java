/* Copyright 2018-2019 Aalborg University
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

import java.nio.ByteBuffer;
import java.util.List;

class PMC_MeanModel extends Model {

    /** Constructors **/
    PMC_MeanModel(int mid, float error, int limit) {
        super(mid, error, limit);
        this.currentSize = 0;
        this.min = Float.MAX_VALUE;
        this.max = -Float.MAX_VALUE;
        this.sum = 0.0F;
        this.withinErrorBound = true;
    }

    /** Public Methods **/
    @Override
    public boolean append(DataPoint[] currentDataPoints) {
        if ( ! this.withinErrorBound) {
            return false;
        }

        //The model can represent all data points if the average value is within error bound of the min and max values
        float nextMin = this.min;
        float nextMax = this.max;
        float nextSum = this.sum;
        for (DataPoint cdp : currentDataPoints) {
            float value = cdp.value;
            nextSum += value;
            nextMin = Math.min(nextMin, value);
            nextMax = Math.max(nextMax, value);
        }

        float average = nextSum / ((this.currentSize + 1) * currentDataPoints.length);
        if (outsidePercentageErrorBound(average, nextMin) || outsidePercentageErrorBound(average, nextMax)) {
            this.withinErrorBound = false;
            return false;
        }
        this.min = nextMin;
        this.max = nextMax;
        this.sum = nextSum;
        this.currentSize += 1;
        return true;
    }

    @Override
    public void initialize(List<DataPoint[]> currentSegment) {
        this.sum = 0.0F;
        this.currentSize = 0;
        this.min = Float.MAX_VALUE;
        this.max = -Float.MAX_VALUE;
        this.withinErrorBound = true;

        for (DataPoint[] dataPoints : currentSegment) {
            if ( ! append(dataPoints)) {
                return;
            }
        }
    }

    @Override
    public byte[] parameters(long startTime, long endTime, int resolution, List<DataPoint[]> dps) {
        return ByteBuffer.allocate(4).putFloat(sum / (currentSize * dps.get(0).length)).array();
    }

    @Override
    public Segment get(int sid, long startTime, long endTime, int resolution, byte[] parameters, byte[] offsets) {
        return new PMC_MeanSegment(sid, startTime, endTime, resolution, parameters, offsets);
    }

    @Override
    public int length() {
        return this.currentSize;
    }

    @Override
    public float size(long startTime, long endTime, int resolution, List<DataPoint[]> dps) {
        if (this.currentSize == 0) {
            return Float.NaN;
        } else {
            //The segment is represented as a single float which is 4 bytes
            return 4.0F;
        }
    }

    /** Instance Variables **/
    private int currentSize;
    private float min;
    private float max;
    private float sum;
    private boolean withinErrorBound;
}


class PMC_MeanSegment extends Segment {

    /** Constructors **/
    PMC_MeanSegment(int sid, long startTime, long endTime, int resolution, byte[] parameters, byte[] offsets) {
        super(sid, startTime, endTime, resolution, offsets);
        this.value = ByteBuffer.wrap(parameters).getFloat();
    }

    /** Public Methods **/
    @Override
    public float min() {
        return this.value;
    }

    @Override
    public float max() {
        return this.value;
    }

    @Override
    public double sum() {
        return this.length() * this.value;
    }

    /** Protected Methods **/
    protected float get(long timestamp, int index) {
        return this.value;
    }

    /** Instance Variables **/
    private final float value;
}