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
package dk.aau.modelardb.core.timeseries;

import dk.aau.modelardb.core.DataPoint;
import dk.aau.modelardb.core.SegmentGenerator;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class AsyncTimeSeriesSocket extends TimeSeries implements AsyncTimeSeries {

    /** Constructors **/
    public AsyncTimeSeriesSocket(String source, int tid, int samplingInterval, String splitString, int timestampColumnIndex,
                                 String dateFormat, String timeZone, int valueColumnIndex, String localeString) {
        super(source, tid, samplingInterval);
        String[] hostAndPort = source.split(":");
        this.host = hostAndPort[0];
        this.port = Integer.parseInt(hostAndPort[1]);

        //A small buffer is used so more time series can be ingested in parallel
        this.bufferSize = 1024;
        this.splitString = splitString;

        this.timestampColumn = timestampColumnIndex;
        switch (dateFormat) {
            case "unix":
                this.dateParserType = 1;
                break;
            case "java":
                this.dateParserType = 2;
                break;
            default:
                this.dateParserType = 3;
                this.dateParser = new SimpleDateFormat(dateFormat);
                this.dateParser.setTimeZone(java.util.TimeZone.getTimeZone(timeZone));
                this.dateParser.setLenient(false);
                break;
        }

        this.valueColumn = valueColumnIndex;
        Locale locale = new Locale(localeString);
        this.valueParser = NumberFormat.getInstance(locale);
        this.decodeBuffer = new StringBuffer();
        this.nextBuffer = new StringBuffer();
    }

    /** Public Methods **/
    public void open() throws RuntimeException {
        try {
            this.channel = SocketChannel.open(new InetSocketAddress(this.host, this.port));
            this.host = null;
            this.byteBuffer = ByteBuffer.allocate(this.bufferSize);
        } catch (IOException ioe) {
            //An unchecked exception is used so the function can be called in a lambda function
            throw new RuntimeException(ioe);
        }
    }

    public DataPoint next() {
        try {
            if (this.nextBuffer.length() == 0) {
                readLines();
            }
            return nextDataPoint();
        } catch (IOException ioe) {
            close();
            throw new RuntimeException(ioe);
        }
    }

    public boolean hasNext() {
        try {
            if (this.nextBuffer.length() == 0) {
                readLines();
            }
            return this.nextBuffer.length() != 0;
        } catch (IOException ioe) {
            close();
            throw new RuntimeException(ioe);
        }
    }

    public void attachToSelector(Selector s, SegmentGenerator mg) throws IOException {
        SelectableChannel sc = (SelectableChannel) this.channel;
        sc.configureBlocking(false);
        sc.register(s, SelectionKey.OP_READ, mg);
    }

    public String toString() {
        return "Time Series: [" + this.tid + " | " + this.source + " | " + this.samplingInterval + "]";
    }

    public void close() {
        //If the channel was never initialized there is nothing to close
        if (this.channel == null) {
            return;
        }

        try {
            this.channel.close();
            //Clears all references to channels and buffers to enable garbage collection
            this.byteBuffer = null;
            this.nextBuffer = null;
            this.channel = null;
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    /** Private Methods **/
    private void readLines() throws IOException {
        //Reads until at least one full data point have been read as read() does not always add bytes
        // to this.byteBuffer for some time after the Selector in WorkingSet have returned rom Select()
        int bytesRead;
        do {
            this.byteBuffer.clear();
            bytesRead = this.channel.read(this.byteBuffer);
            this.byteBuffer.flip();
            this.decodeBuffer.append(StandardCharsets.UTF_8.decode(this.byteBuffer));
        } while (bytesRead != -1 && this.decodeBuffer.indexOf("\n") == -1); //-1 means the channel has reached end-of-stream

        //Transfer all fully read data points into a new buffer to simplify the remaining implementation
        int lastFullyParsedDataPoint = this.decodeBuffer.lastIndexOf("\n") + 1;
        this.nextBuffer.append(this.decodeBuffer, 0, lastFullyParsedDataPoint);
        this.decodeBuffer.delete(0, lastFullyParsedDataPoint);
    }

    private DataPoint nextDataPoint() throws IOException {
        try {
            int nextDataPointIndex = this.nextBuffer.indexOf("\n") + 1;
            String[] split;

            if (nextDataPointIndex == 0) {
                split = this.nextBuffer.toString().split(splitString);
            } else {
                split = this.nextBuffer.substring(0, nextDataPointIndex).split(splitString);
                this.nextBuffer.delete(0, nextDataPointIndex);
            }

            //Parses the timestamp column as either Unix time, Java time, or a human readable timestamp
            long timestamp = 0;
            switch (this.dateParserType) {
                case 1:
                    //Unix time
                    timestamp = new Date(Long.parseLong(split[timestampColumn]) * 1000).getTime();
                    break;
                case 2:
                    //Java time
                    timestamp = new Date(Long.parseLong(split[timestampColumn])).getTime();
                    break;
                case 3:
                    //Human readable timestamp
                    timestamp = dateParser.parse(split[timestampColumn]).getTime();
                    break;
            }
            float value = valueParser.parse(split[valueColumn]).floatValue();
            return new DataPoint(this.tid, timestamp, this.scalingFactor * value);
        } catch (ParseException pe) {
            //If the input cannot be parsed the stream is considered empty
            this.channel.close();
            throw new RuntimeException(pe);
        }
    }

    /** Instance Variables **/
    private String host;
    private final int port;
    private final int bufferSize;
    private ByteBuffer byteBuffer;
    private final StringBuffer decodeBuffer;
    private StringBuffer nextBuffer;
    private ReadableByteChannel channel;
    private final String splitString;
    private final int timestampColumn;
    private SimpleDateFormat dateParser;
    private final int dateParserType;
    private final int valueColumn;
    private final NumberFormat valueParser;
}
