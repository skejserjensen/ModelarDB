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

class PMC_MRModel extends Model {

    /** Constructors **/
    PMC_MRModel(float error, int limit) {
        super(PMC_MRSegment.class, error, limit);
        this.currentSize = 0;
        this.min = Float.MAX_VALUE;
        this.max = -Float.MAX_VALUE;
        this.withinErrorBound = true;
    }

    /** Public Methods **/
    @Override
    public boolean append(DataPoint currentDataPoint) {
        if ( ! this.withinErrorBound) {
            return false;
        }

        float value = currentDataPoint.value;
        float nextMin = (this.min > value) ? value : this.min;
        float nextMax = (this.max < value) ? value : this.max;

        double approximation = (nextMax + nextMin) / 2;
        if (outsideErrorBound(approximation, nextMin) || outsideErrorBound(approximation, nextMax)) {
            this.withinErrorBound = false;
            return false;
        }

        this.min = nextMin;
        this.max = nextMax;
        this.currentSize += 1;
        return true;
    }

    @Override
    public void initialize(List<DataPoint> currentSegment) {
        this.currentSize = 0;
        this.min = Float.MAX_VALUE;
        this.max = -Float.MAX_VALUE;
        this.withinErrorBound = true;

        for (DataPoint DataPoint : currentSegment) {
            if ( ! append(DataPoint)) {
                return;
            }
        }
    }

    @Override
    public Segment get(int sid, long startTime, long endTime, int resolution,
                       List<DataPoint> currentSegment, long[] gaps) {
        return new PMC_MRSegment(sid, startTime, endTime, resolution, (min + max) / 2.0F, gaps);
    }

    @Override
    public Segment get(int sid, long startTime, long endTime, int resolution, byte[] parameters, byte[] gaps) {
        return new PMC_MRSegment(sid, startTime, endTime, resolution, parameters, gaps);
    }

    @Override
    public int length() {
        return this.currentSize;
    }

    @Override
    public float size() {
        //The segment is represented as a single float which is 4 bytes
        return 4.0F;
    }

    /** Instance Variables **/
    private int currentSize;
    private float min;
    private float max;
    private boolean withinErrorBound;
}


class PMC_MRSegment extends Segment {

    /** Constructors **/
    PMC_MRSegment(int sid, long startTime, long endTime, int resolution, float value, long[] gaps) {
        super(sid, startTime, endTime, resolution, gaps);
        this.value = value;
    }

    PMC_MRSegment(int sid, long startTime, long endTime, int resolution, byte[] parameters, byte[] gaps) {
        super(sid, startTime, endTime, resolution, gaps);
        this.value = ByteBuffer.wrap(parameters).getFloat();
    }

    /** Public Methods **/
    @Override
    public byte[] parameters() {
        return ByteBuffer.allocate(4).putFloat(this.value).array();
    }

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