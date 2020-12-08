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

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Locale;
import java.util.zip.GZIPInputStream;

public class TimeSeries implements Serializable, Iterator<DataPoint> {

    /** Constructors **/
    //Comma Separated Values
    public TimeSeries(String stringPath, int sid, int resolution,
                      String splitString, boolean hasHeader,
                      int timestampColumn, String dateFormat, String timeZone,
                      int valueColumn, String locale) {

        //Values are set in the constructor to allow the fields to be final and exported as public
        this.source = stringPath.substring(stringPath.lastIndexOf('/') + 1);
        this.sid = sid;
        this.resolution = resolution;
        this.isBounded = true;
        this.initialize = (Runnable & Serializable) () -> initFileChannel(stringPath);
        setSerializable(splitString, hasHeader, timestampColumn, dateFormat, timeZone, valueColumn, locale);
    }

    //Socket
    public TimeSeries(String host, int port, int sid, int resolution, String splitString, int timestampColumn,
                      String dateFormat, String timeZone, int valueColumn, String locale) {

        //Values are set in the constructor to allow the fields to be final and exported as public
        this.source = host + " : " + port;
        this.sid = sid;
        this.resolution = resolution;
        this.isBounded = false;
        this.initialize = (Runnable & Serializable) () -> initSocketChannel(host, port);
        setSerializable(splitString, false, timestampColumn, dateFormat, timeZone, valueColumn, locale);
    }

    /** Public Methods **/
    public DataPoint next() {
        try {
            if (this.nextBuffer.length() == 0) {
                readLines();
            }
            return nextDataPoint();
        } catch (IOException ioe) {
            close();
            throw new java.lang.RuntimeException(ioe);
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
            throw new java.lang.RuntimeException(ioe);
        }
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
            throw new java.lang.RuntimeException(ioe);
        }
    }

    public void setScalingFactor(float scalingFactor) {
        this.scalingFactor = scalingFactor;
    }

    public float getScalingFactor() {
        return this.scalingFactor;
    }

    public void attachToSelector(Selector s, SegmentGenerator mg) throws IOException {
        SelectableChannel sc = (SelectableChannel) this.channel;
        sc.configureBlocking(false);
        sc.register(s, SelectionKey.OP_READ, mg);
    }

    public String toString() {
        return "Time Series: [" + this.sid + " | " + this.source + " | " + this.resolution + "]";
    }

    /** Private Methods **/
    private void initFileChannel(String stringPath) throws RuntimeException {
        try {
            FileChannel fc = FileChannel.open(Paths.get(stringPath));

            //Wraps the channel in a stream if the data is compressed
            String suffix = "";
            int lastIndexOfDot = stringPath.lastIndexOf('.');
            if (lastIndexOfDot > -1) {
                suffix = stringPath.substring(lastIndexOfDot);
            }

            if (".gz".equals(suffix)) {
                InputStream is = Channels.newInputStream(fc);
                GZIPInputStream gis = new GZIPInputStream(is);
                this.channel = Channels.newChannel(gis);
            } else {
                this.channel = fc;
            }
            this.byteBuffer = ByteBuffer.allocate(this.bufferSize);
            if (this.hasHeader) {
                readLines();
            }
        } catch (IOException ioe) {
            //An unchecked exception is used so the function can be called in a lambda function
            throw new RuntimeException(ioe);
        }
    }

    private void initSocketChannel(String host, int port) throws RuntimeException {
        try {
            this.channel = SocketChannel.open(new InetSocketAddress(host, port));
            this.byteBuffer = ByteBuffer.allocate(this.bufferSize);
        } catch (IOException ioe) {
            //An unchecked exception is used so the function can be called in a lambda function
            throw new RuntimeException(ioe);
        }
    }

    private void setSerializable(String splitString, boolean hasHeader,
                                 int timestampColumn, String dateFormat, String timeZone,
                                 int valueColumn, String localeString) {
        //A small buffer is used so more time series can be ingested in parallel
        this.bufferSize = 1024;
        this.hasHeader = hasHeader;
        this.splitString = splitString;
        this.scalingFactor = 1.0F;

        this.timestampColumn = timestampColumn;
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

        this.valueColumn = valueColumn;
        Locale locale = new Locale(localeString);
        this.valueParser = NumberFormat.getInstance(locale);
        this.decodeBuffer = new StringBuffer();
        this.nextBuffer = new StringBuffer();
    }

    private void readLines() throws IOException {
        int lastChar = 0;

        //Reads a whole line from the channel by looking for either a new line or if no additional bytes are returned
        do {
            this.byteBuffer.clear();
            this.channel.read(this.byteBuffer);
            lastChar = this.byteBuffer.position();
            this.byteBuffer.flip();
            this.decodeBuffer.append(Charset.forName("UTF-8").decode(this.byteBuffer));
        } while (lastChar != 0 && this.decodeBuffer.indexOf("\n") == -1);

        //Transfer all fully read data points into a new buffer to simplify the remaining implementation
        int lastFullyParsedDataPoint = this.decodeBuffer.lastIndexOf("\n") + 1;
        this.nextBuffer.append(this.decodeBuffer, 0, lastFullyParsedDataPoint);
        this.decodeBuffer.delete(0, lastFullyParsedDataPoint);
    }

    private DataPoint nextDataPoint() throws IOException {
        try {
            int nextDataPointIndex = this.nextBuffer.indexOf("\n") + 1;
            String[] split = null;

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
                    timestamp = new Date(Long.valueOf(split[timestampColumn]) * 1000).getTime();
                    break;
                case 2:
                    //Java time
                    timestamp = new Date(Long.valueOf(split[timestampColumn])).getTime();
                    break;
                case 3:
                    //Human readable timestamp
                    timestamp = dateParser.parse(split[timestampColumn]).getTime();
                    break;
            }
            float value = valueParser.parse(split[valueColumn]).floatValue();
            return new DataPoint(this.sid, timestamp, this.scalingFactor * value);
        } catch (ParseException pe) {
            //If the input cannot be parsed the stream is considered empty
            this.channel.close();
            throw new java.lang.RuntimeException(pe);
        }
    }

    /** Instance Variables **/
    public final String source;
    public final int sid;
    public final int resolution;
    public final boolean isBounded;
    public final Runnable initialize;

    private boolean hasHeader;
    private float scalingFactor;
    private int bufferSize;
    private ByteBuffer byteBuffer;
    private StringBuffer decodeBuffer;
    private StringBuffer nextBuffer;
    private ReadableByteChannel channel;
    private String splitString;
    private int timestampColumn;
    private SimpleDateFormat dateParser;
    private int dateParserType;
    private int valueColumn;
    private NumberFormat valueParser;
}
