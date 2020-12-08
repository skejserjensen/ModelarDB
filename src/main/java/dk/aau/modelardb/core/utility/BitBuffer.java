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

//This class is intended to store single bits and must be used only for reading
// or writing, not both concurrently. The implementation of this buffer is based
// on code published by Michael Burman under the Apache2 license.
// LINK: https://github.com/burmanm/gorilla-tsc
package dk.aau.modelardb.core.utility;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class BitBuffer {

    /** Constructors **/
    public BitBuffer(int size) {
        this.byteBuffer = ByteBuffer.allocate(size);
        this.currentByte = this.byteBuffer.get(this.byteBuffer.position());
        this.bitsLeft = Byte.SIZE;
    }

    public BitBuffer(byte[] in) {
        this.byteBuffer = ByteBuffer.wrap(in);
        readNextByte();
    }

    /** Public Methods **/
    public void writeBit(boolean bit) {
        if (bit) {
            this.currentByte |= (1 << (this.bitsLeft - 1));
        }
        this.bitsLeft--;

        //If we reached the last bit a new byte is allocated
        if (this.bitsLeft == 0) {
            allocateNewByte();
        }
    }

    public void writeBits(int value, int bits) {
        while (bits > 0) {
            int bitsToWrite = 0;
            if (bits > this.bitsLeft) {
                bitsToWrite = this.bitsLeft;
                int shift = bits - this.bitsLeft;
                this.currentByte |= (byte) ((value >> shift) & ((1 << this.bitsLeft) - 1));
            } else {
                bitsToWrite = bits;
                int shift = this.bitsLeft - bits;
                this.currentByte |= (byte) (value << shift);
            }
            bits -= bitsToWrite;
            this.bitsLeft -= bitsToWrite;

            //If we reached the last bit a new byte is allocated
            if (this.bitsLeft == 0) {
                allocateNewByte();
            }
        }
    }

    public boolean readBit() {
        boolean bit = ((this.currentByte >> (this.bitsLeft - 1)) & 1) == 1;
        this.bitsLeft--;

        //If we reached the last bit the next byte is read
        if (this.bitsLeft == 0 && this.byteBuffer.hasRemaining()) {
            readNextByte();
        }
        return bit;
    }

    public int getInt(int bits) {
        int value = 0;
        while (bits > 0) {
            if (bits > this.bitsLeft || bits == Byte.SIZE) {
                //Reads only the bitsLeft least significant bits
                byte d = (byte) (this.currentByte & ((1 << this.bitsLeft) - 1));
                value = (value << this.bitsLeft) + (d & 0xFF);
                bits -= this.bitsLeft;
                this.bitsLeft = 0;
            } else {
                //Shifts to correct position and read only least significant bits
                byte d = (byte) ((this.currentByte >>> (this.bitsLeft - bits)) & ((1 << bits) - 1));
                value = (value << bits) + (d & 0xFF);
                this.bitsLeft -= bits;
                bits = 0;
            }

            //The current byte has been exhausted and we move to the next
            if (this.bitsLeft == 0 && this.byteBuffer.hasRemaining()) {
                readNextByte();
            }
        }
        return value;
    }

    public byte[] array() {
        byte[] result;

        if (Byte.SIZE != this.bitsLeft) {
            int resultSize = this.byteBuffer.position() + 1;
            result = Arrays.copyOf(this.byteBuffer.array(), resultSize);
            result[resultSize - 1] = this.currentByte;
        } else {
            result = Arrays.copyOf(this.byteBuffer.array(), this.byteBuffer.position());
        }
        return result;
    }

    public int size() {
        if (Byte.SIZE != this.bitsLeft) {
            return this.byteBuffer.position() + 1;
        } else {
            return this.byteBuffer.position();
        }
    }

    /** Private Methods **/
    private void allocateNewByte() {
        this.byteBuffer.put(this.currentByte);
        if ( ! this.byteBuffer.hasRemaining()) {
            expandAllocation();
        }
        this.currentByte = this.byteBuffer.get(this.byteBuffer.position());
        bitsLeft = Byte.SIZE;
    }

    private void readNextByte() {
        this.currentByte = this.byteBuffer.get();
        this.bitsLeft = Byte.SIZE;
    }

    private void expandAllocation() {
        ByteBuffer expandedByteBuffer =
                ByteBuffer.allocate(this.byteBuffer.capacity() * 2);

        this.byteBuffer.flip();
        expandedByteBuffer.put(this.byteBuffer);
        expandedByteBuffer.position(this.byteBuffer.capacity());
        this.byteBuffer = expandedByteBuffer;
    }

    /** Instance Variables **/
    private int bitsLeft;
    private byte currentByte;
    private ByteBuffer byteBuffer;
}
