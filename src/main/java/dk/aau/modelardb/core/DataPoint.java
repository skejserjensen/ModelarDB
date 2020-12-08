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
package dk.aau.modelardb.core;

public class DataPoint {

    /** Constructors **/
    public DataPoint(int sid, long timestamp, float value) {
        this.sid = sid;
        this.timestamp = timestamp;
        this.value = value;
    }

    /** Public Methods **/
    public String toString() {
        return "DataPoint: [" + this.sid + " | " + new java.sql.Timestamp(this.timestamp) + " | " + this.value + "]";
    }

    /** Instance Variables **/
    public final int sid;
    public final long timestamp;
    public final float value;
}
