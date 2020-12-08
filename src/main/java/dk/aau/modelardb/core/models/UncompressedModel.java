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

import java.nio.ByteBuffer;
import java.nio.FloatBuffer;
import java.util.List;

class UncompressedModel extends Model {

    /** Constructors **/
    UncompressedModel(int mid, float error, int limit) {
        super(mid, error, limit);
        this.currentSize = 0;
    }

    /** Public Methods **/
    @Override
    public boolean append(DataPoint[] currentDataPoints) {
        //UncompressedModel is a last resort fallback so it simply stores the current buffer in an array
        this.currentSize++;
        return true;
    }

    @Override
    public void initialize(List<DataPoint[]> currentSegment) {
        this.currentSize = Integer.min(this.limit, currentSegment.size());
    }

    @Override
    public byte[] parameters(long startTime, long endTime, int resolution, List<DataPoint[]> dps) {
        ByteBuffer values = ByteBuffer.allocate(4 * dps.get(0).length * dps.size());
        for(DataPoint[] dpss : dps) {
            for (DataPoint dp : dpss) {
                values.putFloat(dp.value);
            }
        }
        return values.array();
    }

    @Override
    public Segment get(int sid, long startTime, long endTime, int resolution, byte[] parameters, byte[] offsets) {
        return new UncompressedSegment(sid, startTime, endTime, resolution, parameters, offsets);
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
            //The model stores each value as a float, which requires four bytes for each
            return 4.0F * this.currentSize;
        }
    }

    /** Instance Variables **/
    private int currentSize;
}


class UncompressedSegment extends Segment {

    /** Constructors **/
    UncompressedSegment(int sid, long startTime, long endTime, int resolution, byte[] parameters, byte[] offsets) {
        super(sid, startTime, endTime, resolution, offsets);

        FloatBuffer floatBuffer = ByteBuffer.wrap(parameters).asFloatBuffer();
        float[] values = new float[parameters.length / 4];
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
