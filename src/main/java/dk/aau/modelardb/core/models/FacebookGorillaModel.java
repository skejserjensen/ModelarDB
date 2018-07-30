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

import java.util.List;

import dk.aau.modelardb.core.DataPoint;
import dk.aau.modelardb.core.utility.BitBuffer;

// The implementation of this model is based on code published by Michael Burman
// under the Apache2 license. LINK: https://github.com/burmanm/gorilla-tsc
class FacebookGorillaModel extends Model {

    /** Constructor **/
    FacebookGorillaModel(float error, int limit) {
        super(FacebookGorillaSegment.class, error, limit);
        this.currentSize = 0;
        this.compressed = null;
        this.storedLeadingZeros = Integer.MAX_VALUE;
        this.storedTrailingZeros = 0;
    }

    /** Public Methods **/
    @Override
    public boolean append(DataPoint currentDataPoint) {
        if (this.currentSize == this.limit) {
            return false;
        }

        if (this.currentSize == 0) {
            this.lastVal = Float.floatToIntBits(currentDataPoint.value);
            this.compressed.writeBits(lastVal, java.lang.Integer.SIZE);
        } else {
            compress(currentDataPoint.value);
        }
        this.currentSize += 1;
        return true;
    }

    @Override
    public void initialize(List<DataPoint> currentSegment) {
        this.currentSize = 0;
        this.compressed = new BitBuffer();
        this.storedLeadingZeros = Integer.MAX_VALUE;
        this.storedTrailingZeros = 0;

        for(DataPoint dataPoint : currentSegment) {
            this.append(dataPoint);
        }
    }

    @Override
    public Segment get(int sid, long startTime, long endTime, int resolution, List<DataPoint> currentSegment, long[] gaps) {
        return new FacebookGorillaSegment(sid, startTime, endTime, resolution,  this.compressed.array(), gaps);

    }

    @Override
    public Segment get(int sid, long startTime, long endTime, int resolution, byte[] parameters, byte[] gaps) {
        return new FacebookGorillaSegment(sid, startTime, endTime, resolution, parameters, gaps);
    }


    @Override
    public int length() {
        return this.currentSize;
    }

    @Override
    public float size() {
        return this.compressed.size();
    }

    /** Private Methods **/
    private void compress(float value) {
        int curVal = Float.floatToIntBits(value);
        int xor = curVal ^ lastVal;

        if (xor == 0) {
            compressed.writeBit(false);
        } else {
            //Compute the number of leading and trailing zeros that it might be necessary to store
            int leadingZeros = Integer.numberOfLeadingZeros(xor);
            int trailingZeros = Integer.numberOfTrailingZeros(xor);

            if(leadingZeros >= 32) {
                leadingZeros = 31;
            }
            this.compressed.writeBit(true);

            if (leadingZeros >= this.storedLeadingZeros && trailingZeros >= this.storedTrailingZeros) {
                //Write only the significant bits
                this.compressed.writeBit(false);
                int significantBits = 32 - storedLeadingZeros - storedTrailingZeros;
                this.compressed.writeBits(xor >>> storedTrailingZeros, significantBits);
            } else {
                //Store a new number of leading zero bits before the significant bits
                this.compressed.writeBit(true);
                this.compressed.writeBits(leadingZeros, 5); // Number of leading zeros in the next 5 bits

                int significantBits = 32 - leadingZeros - trailingZeros;
                this.compressed.writeBits(significantBits, 6); // Length of meaningful bits in the next 6 bits
                this.compressed.writeBits(xor >>> trailingZeros, significantBits); // Store the meaningful bits of XOR

                this.storedLeadingZeros = leadingZeros;
                this.storedTrailingZeros = trailingZeros;
            }
        }
        lastVal = curVal;
    }

    /** Instance Variable **/
    private BitBuffer compressed;
    private int lastVal;
    private int currentSize;
    private int storedLeadingZeros;
    private int storedTrailingZeros;
}


class FacebookGorillaSegment extends Segment {

    /** Constructor **/
    FacebookGorillaSegment(int sid, long startTime, long endTime, int resolution, byte[] parameters, long[] gaps) {
        super(sid, startTime, endTime, resolution, gaps);
        this.compressedValues = parameters;
        this.values = decompress(parameters, super.length());
    }

    FacebookGorillaSegment(int sid, long startTime, long endTime, int resolution, byte[] parameters, byte[] gaps) {
        //Total size is used as the segment must be guaranteed to get the total number of data points it represents
        super(sid, startTime, endTime, resolution, gaps);
        this.compressedValues = parameters;
        this.values = decompress(parameters, super.capacity());
    }

    /** Public Methods **/
    @Override
    public byte[] parameters() {
        if (this.compressedValues == null){
            throw new UnsupportedOperationException("parameters are not currently stored");
        }
        return compressedValues;
    }

    @Override
    public float min() {
        float min = Float.MAX_VALUE;
        for (int index = getIndexWithOffset(); index < this.values.length; index++) {
            min = Float.min(min, this.values[index]);
        }
        return min;
    }

    @Override
    public float max() {
        float max = -Float.MAX_VALUE;
        for (int index = getIndexWithOffset(); index < this.values.length; index++) {
            max = Float.max(max, this.values[index]);
        }
        return max;
    }

    @Override
    public double sum() {
        double acc = 0;
        for (int index = getIndexWithOffset(); index < this.values.length; index++) {
            acc += this.values[index];
        }
        return acc;
    }

    /** Protected Methods **/
    @Override
    protected float get(long timestamp, int index) {
        return this.values[index];
    }

    /** Private Methods **/
    private float[] decompress(byte[] values, int size) {
        float[] result = new float[size];
        BitBuffer bitBuffer = new BitBuffer(values);

        int storedLeadingZeros = Integer.MAX_VALUE;
        int storedTrailingZeros = 0;
        int lastVal = bitBuffer.getInt(java.lang.Integer.SIZE);
        result[0] = Float.intBitsToFloat(lastVal);

        for (int i = 1; i < size; i++) {
            if (bitBuffer.readBit()) {
                if (bitBuffer.readBit()) {
                    // New leading and trailing zeros
                    storedLeadingZeros = bitBuffer.getInt(5);
                    byte significantBits = (byte) bitBuffer.getInt(6);
                    if (significantBits == 0) {
                        significantBits = 32;
                    }
                    storedTrailingZeros = 32 - significantBits - storedLeadingZeros;
                }

                int value = bitBuffer.getInt(32 - storedLeadingZeros - storedTrailingZeros);
                value <<= storedTrailingZeros;
                value = lastVal ^ value;
                lastVal = value;
                result[i] = Float.intBitsToFloat(lastVal);
            } else {
                result[i] = Float.intBitsToFloat(lastVal);
            }
        }
        return result;
    }

    /** Instance Variables **/
    private float[] values;
    private byte[] compressedValues;
}