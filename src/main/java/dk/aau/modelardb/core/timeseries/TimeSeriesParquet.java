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
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;

public class TimeSeriesParquet extends TimeSeries {
    /** Public Methods **/
    public TimeSeriesParquet(String stringPath, int tid, int samplingInterval, int timestampColumnIndex, int valueColumnIndex) {
        super(stringPath.substring(stringPath.lastIndexOf('/') + 1), tid, samplingInterval);
        this.stringPath = stringPath;
        this.timestampColumnIndex = timestampColumnIndex;
        this.valueColumnIndex = valueColumnIndex;
    }

    public void open() {
        try {
            Path path = new Path(this.stringPath);
            InputFile iff = HadoopInputFile.fromPath(path, new Configuration());
            ParquetReadOptions pro = ParquetReadOptions.builder().build();
            this.fileReader = new ParquetFileReader(iff, pro);

            //Constructs a schema that only contains the required columns so unnecessary columns are not read
            MessageType schema = this.fileReader.getFooter().getFileMetaData().getSchema();
            this.schema = new MessageType("schema",
                    schema.getFields().get(this.timestampColumnIndex), schema.getFields().get(this.valueColumnIndex));

            //next() assumes timestamps are stored as int64 with the TIMESTAMP_MICROS logical type annotation
            String[] typeComponents = schema.getColumns().get(this.timestampColumnIndex).getPrimitiveType().toString().split(" ");
            if ( ! typeComponents[1].equals("int64") || ! typeComponents[3].equals("(TIMESTAMP_MICROS)")) {
              throw new UnsupportedOperationException("CORE: Parquet files must store timestamps as int64 (TIMESTAMP_MICROS)");
            }
        } catch (IOException ioe) {
            close();
            throw new RuntimeException(ioe);
        }
    }

    public DataPoint next() {
        Group rowGroup = this.recordReader.read();
        long timestamp = rowGroup.getLong(0, 0) / 1000;
        float value = rowGroup.getFloat(1, 0);
        this.rowIndex++;
        return new DataPoint(this.tid, timestamp, this.scalingFactor * value);
    }

    public boolean hasNext() {
        try {
            if (this.rowIndex != this.rowCount && this.rowCount != 0) {
                return true;
            }

            PageReadStore readStore = this.fileReader.readNextRowGroup();
            if (readStore != null) {
                GroupRecordConverter grc = new GroupRecordConverter(this.schema);
                MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(this.schema);
                this.recordReader = columnIO.getRecordReader(readStore, grc);
                this.rowIndex = 0;
                this.rowCount = readStore.getRowCount();
                return true;
            } else {
                return false;
            }
        } catch (IOException ioe) {
            close();
            throw new RuntimeException(ioe);
        }
    }

    public String toString() {
        return "Time Series: [" + this.tid + " | " + this.source + " | " + this.samplingInterval + "]";
    }

    public void close() {
        try {
            this.fileReader.close();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    /** Instance Variables **/
    private final String stringPath;
    private final int timestampColumnIndex;
    private final int valueColumnIndex;

    private int rowIndex;
    private long rowCount;
    private ParquetFileReader fileReader;
    private MessageType schema;
    private RecordReader<Group> recordReader;
}
