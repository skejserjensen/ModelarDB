/* Copyright 2021 The ModelarDB Contributors
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
package dk.aau.modelardb.core.timeseries;

import dk.aau.modelardb.core.DataPoint;

import java.io.Serializable;
import java.util.Iterator;

public abstract class TimeSeries implements  Serializable, Iterator<DataPoint> {
    /** Public Methods **/
    public TimeSeries(String source, int tid, int samplingInterval) {
        this.source = source;
        this.tid = tid;
        this.samplingInterval = samplingInterval;
        this.scalingFactor = 1.0F;
    }
    abstract public void open();
    abstract public void close();

    /** Instance Variables **/
    //Updated by local instances after tids have been received by a remote instance
    public int tid;

    public final String source;
    public final int samplingInterval;
    public float scalingFactor;

}