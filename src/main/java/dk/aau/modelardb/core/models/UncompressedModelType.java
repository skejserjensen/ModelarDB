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

import java.nio.ByteBuffer;
import java.nio.FloatBuffer;
import java.util.List;

class UncompressedModelType extends ModelType {

    /** Constructors **/
    UncompressedModelType(int mtid, float errorBound, int lengthBound) {
        super(mtid, errorBound, lengthBound);
    }

    /** Public Methods **/
    @Override
    public boolean append(DataPoint[] currentDataPoints) {
        //UncompressedModelType is a last resort fallback so it simply stores the current buffer in an array
        this.currentSize++;
        return true;
    }

    @Override
    public void initialize(List<DataPoint[]> currentSegment) {
        this.currentSize = Integer.min(this.lengthBound, currentSegment.size());
    }

    @Override
    public byte[] getModel(long startTime, long endTime, int samplingInterval, List<DataPoint[]> dps) {
        ByteBuffer values = ByteBuffer.allocate(4 * dps.get(0).length * dps.size());
        for(DataPoint[] dpss : dps) {
            for (DataPoint dp : dpss) {
                values.putFloat(dp.value);
            }
        }
        return values.array();
    }

    @Override
    public Segment get(int tid, long startTime, long endTime, int samplingInterval, byte[] model, byte[] offsets) {
        return new UncompressedSegment(tid, startTime, endTime, samplingInterval, model, offsets);
    }

    @Override
    public int length() {
        return this.currentSize;
    }

    @Override
    public float size(long startTime, long endTime, int samplingInterval, List<DataPoint[]> dps) {
        if (this.currentSize == 0) {
            return Float.NaN;
        } else {
            //The model type stores each value as a float, which requires four bytes for each
            return 4.0F * this.currentSize;
        }
    }

    /** Instance Variables **/
    private int currentSize;
}


class UncompressedSegment extends Segment {

    /** Constructors **/
    UncompressedSegment(int tid, long startTime, long endTime, int samplingInterval, byte[] model, byte[] offsets) {
        super(tid, startTime, endTime, samplingInterval, offsets);

        FloatBuffer floatBuffer = ByteBuffer.wrap(model).asFloatBuffer();
        float[] values = new float[model.length / 4];
        floatBuffer.get(values);
        this.values = values;
    }

    /** Protected Methods **/
    @Override
    protected float get(long timestamp, int index) {
        return this.values[index];
    }

    /** Instance Variables **/
    private final float[] values;
}
