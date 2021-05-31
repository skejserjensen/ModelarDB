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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;

import java.io.IOException;

public class TimeSeriesORC extends TimeSeries {
    /** Public Methods **/
    public TimeSeriesORC(String stringPath, int tid, int samplingInterval, int timestampColumnIndex, int valueColumnIndex) {
        super(stringPath.substring(stringPath.lastIndexOf('/') + 1), tid, samplingInterval);
        this.stringPath = stringPath;
        this.timestampColumnIndex = timestampColumnIndex;
        this.valueColumnIndex = valueColumnIndex;
    }

    public void open() {
        try {
            Path path = new Path(this.stringPath);
            OrcFile.ReaderOptions ro = OrcFile.readerOptions(new Configuration());
            this.reader = OrcFile.createReader(path, ro);

            //Include only the required columns so unnecessary columns are not read
            int columns = this.reader.getSchema().getMaximumId() + 1;
            boolean[] include = new boolean[columns];
            java.util.Arrays.fill(include, false);
            include[this.timestampColumnIndex + 1] = true;
            include[this.valueColumnIndex + 1] = true;
            this.recordReader = this.reader.rows(this.reader.options().include(include));
            this.rowBatch = this.reader.getSchema().createRowBatch();
        } catch (IOException ioe) {
            close();
            throw new java.lang.RuntimeException(ioe);
        }
    }

    public DataPoint next() {
        long timestamp = this.timestampColumn.asScratchTimestamp(this.rowIndex).getTime();
        float value = (float) this.valueColumn.vector[this.rowIndex];
        this.rowIndex++;
        return new DataPoint(this.tid, timestamp, this.scalingFactor * value);
    }

    public boolean hasNext() {
        try {
            if (this.rowIndex != this.rowBatch.size && this.rowBatch.size != 0) {
                return true;
            }

            if (this.recordReader.nextBatch(this.rowBatch)) {
                this.timestampColumn = (TimestampColumnVector) this.rowBatch.cols[this.timestampColumnIndex];
                this.valueColumn = (DoubleColumnVector) this.rowBatch.cols[this.valueColumnIndex];
                this.rowIndex = 0;
                return true;
            } else {
                return false;
            }
        } catch (IOException ioe) {
            close();
            throw new java.lang.RuntimeException(ioe);
        }
    }

    public String toString() {
        return "Time Series: [" + this.tid + " | " + this.source + " | " + this.samplingInterval + "]";
    }

    public void close() {
        try {
            this.recordReader.close();
            this.reader.close();
        } catch (IOException ioe) {
            throw new java.lang.RuntimeException(ioe);
        }
    }

    /** Instance Variables **/
    private final String stringPath;
    private final int timestampColumnIndex;
    private final int valueColumnIndex;

    private int rowIndex;
    private Reader reader;
    private RecordReader recordReader;
    private VectorizedRowBatch rowBatch;
    private TimestampColumnVector timestampColumn;
    private DoubleColumnVector valueColumn;
}