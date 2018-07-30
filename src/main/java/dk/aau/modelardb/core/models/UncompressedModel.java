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
import java.nio.FloatBuffer;
import java.util.List;

import dk.aau.modelardb.core.DataPoint;

class UncompressedModel extends Model {

    /** Constructors **/
    UncompressedModel(float error, int limit) {
        super(UncompressedSegment.class, error, limit);
        this.currentSize = 0;
    }

    /** Public Methods **/
    @Override
    public boolean append(DataPoint currentDataPoint) {
        if (this.currentSize == this.limit) {
            return false;
        }

        currentSize++;
        return true;
    }

    @Override
    public void initialize(List<DataPoint> currentSegment) {
        this.currentSize = currentSegment.size();
    }

    @Override
    public Segment get(int sid, long startTime, long endTime, int resolution, List<DataPoint> currentSegment, long[] gaps) {
        int currentSegmentSize = currentSegment.size();
        float[] values = new float[currentSegmentSize];

        int i = 0;
        for (DataPoint s : currentSegment) {
            values[i] = s.value;
            i++;
        }
        return new UncompressedSegment(sid, startTime, endTime, resolution, values, gaps);
    }

    @Override
    public Segment get(int sid, long startTime, long endTime, int resolution, byte[] parameters, byte[] gaps) {
        return new UncompressedSegment(sid, startTime, endTime, resolution, parameters, gaps);
    }

    @Override
    public int length() {
        return this.currentSize;
    }

    @Override
    public float size() {
        //The model store each value as floats which require 4 bytes for each
        return 4.0F * this.currentSize;
    }

    /** Instance Variables **/
    private int currentSize;
}


class UncompressedSegment extends Segment {


    /** Constructors **/
    UncompressedSegment(int sid, long startTime, long endTime, int resolution, float[] values, long[] gaps) {
        super(sid, startTime, endTime, resolution, gaps);
        this.values = values;
    }

    UncompressedSegment(int sid, long startTime, long endTime, int resolution, byte[] parameters, byte[] gaps) {
        super(sid, startTime, endTime, resolution, gaps);

        FloatBuffer floatBuffer = ByteBuffer.wrap(parameters).asFloatBuffer();
        float[] values = new float[parameters.length / 4];
        floatBuffer.get(values);
        this.values = values;
    }

    /** Public Methods **/
    @Override
    public byte[] parameters() {
        FloatBuffer floatBuffer = FloatBuffer.wrap(this.values);
        ByteBuffer byteBuffer = ByteBuffer.allocate(4 * this.values.length);
        byteBuffer.asFloatBuffer().put(floatBuffer);
        return byteBuffer.array();
    }

    /** Protected Methods **/
    @Override
    protected float get(long timestamp, int index) {
        return this.values[index];
    }

    /** Instance Variables **/
    private final float[] values;
}