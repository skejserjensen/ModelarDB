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
                      int valueColumn, String locale) throws IOException {

        //Values set in the constructor to allow the fields to be final and exported as public
        this.location = stringPath.substring(stringPath.lastIndexOf('/') + 1);
        this.sid = sid;
        this.resolution = resolution;
        this.isBounded = true;
        this.init = (Runnable & Serializable) () -> initFileChannel(stringPath);
        setSerializable(splitString, hasHeader, timestampColumn, dateFormat, timeZone, valueColumn, locale);
    }

    //Socket
    public TimeSeries(String host, int port, int sid, int resolution,
                      String splitString, boolean hasHeader,
                      int timestampColumn, String dateFormat, String timeZone,
                      int valueColumn, String locale) throws IOException {

        //Values are set in the constructor to allow the fields to be final and exported as public
        this.location = host + " : " + port;
        this.sid = sid;
        this.resolution = resolution;
        this.isBounded = false;
        this.init = (Runnable & Serializable) () -> initSocketChannel(host, port);
        setSerializable(splitString, hasHeader, timestampColumn, dateFormat, timeZone, valueColumn, locale);
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
            //Clear all reference to channels and buffers to enable garbage collection
            this.byteBuffer = null;
            this.nextBuffer = null;
            this.channel = null;
        } catch (IOException ioe) {
            throw new java.lang.RuntimeException(ioe);
        }
    }

    public void attachToSelector(Selector s, SegmentGenerator mg) throws IOException {
        SelectableChannel sc = (SelectableChannel) this.channel;
        sc.configureBlocking(false);
        sc.register(s, SelectionKey.OP_READ, mg);
    }

    public String toString() {
        return "Time Series: [" + sid + " | " + location + " | " + resolution + "]";
    }

    /** Private Methods **/
    private void initFileChannel(String stringPath) throws RuntimeException {
        try {
            FileChannel fc = FileChannel.open(Paths.get(stringPath));

            //Adding an additional layer of stream if data must be uncompressed
            String suffix = "";
            int lastIndexOfDot = stringPath.lastIndexOf('.');
            if (lastIndexOfDot > -1) {
                suffix = stringPath.substring(lastIndexOfDot);
            }
            switch (suffix) {
                case ".gz":
                    InputStream is = Channels.newInputStream(fc);
                    GZIPInputStream gis = new GZIPInputStream(is);
                    this.channel = Channels.newChannel(gis);
                    break;
                default:
                    this.channel = fc;
                    break;
            }
            this.byteBuffer = ByteBuffer.allocate(this.bufferSize);
        } catch (IOException ioe) {
            //An unchecked exception is used here to allow for the function to be used in a lambda
            throw new RuntimeException(ioe);
        }
    }

    private void initSocketChannel(String host, int port) throws RuntimeException {
        try {
            this.channel = SocketChannel.open(new InetSocketAddress(host, port));
            this.byteBuffer = ByteBuffer.allocate(this.bufferSize);
        } catch (IOException ioe) {
            //An unchecked exception is used here to allow for the function to be used in a lambda
            throw new RuntimeException(ioe);
        }
    }

    private void setSerializable(String splitString, boolean hasHeader,
                                 int timestampColumn, String dateFormat, String timeZone,
                                 int valueColumn, String localeString) throws java.io.IOException {
        //Initializes a line buffer with an arbitrarily chosen size as BufferedReaders 8192 is unnecessary for (ts, v) pairs
        this.bufferSize = 1024;

        this.splitString = splitString;
        if (hasHeader) {
            readLines();
        }

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
        this.nextBuffer = new StringBuffer();
    }

    private void readLines() throws IOException {
        int lastChar = 0;

        //Read a whole line from the channel by looking for either a new line or if no additional bytes are returned
        do {
            this.byteBuffer.clear();
            this.channel.read(this.byteBuffer);
            lastChar = this.byteBuffer.position();
            this.byteBuffer.flip();
            this.nextBuffer.append(Charset.forName("UTF-8").decode(this.byteBuffer));
        } while (lastChar != 0 && this.byteBuffer.get(lastChar - 1) != '\n');
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

            //Parse the timestamp column as either Unix time, java time or a human readable timestamp
            long timestamp = 0;
            switch (this.dateParserType) {
            case 1:
                //Unix Time
                timestamp = new Date(Long.valueOf(split[timestampColumn]) * 1000).getTime();
                break;
            case 2:
                //Java Time
                timestamp = new Date(Long.valueOf(split[timestampColumn])).getTime();
                break;
            case 3:
                //String Time
                timestamp = dateParser.parse(split[timestampColumn]).getTime();
                break;
            }
            float value = valueParser.parse(split[valueColumn]).floatValue();
            return new DataPoint(this.sid, timestamp, value);
        } catch (ParseException pe) {
            //If the input cannot be parsed we interpret it as the stream of data points ending
            this.channel.close();
            return null;
        }
    }

    /** Instance Variables **/
    public final String location;
    public final int sid;
    public final int resolution;
    public final boolean isBounded;
    public final Runnable init;

    private int bufferSize;
    private ByteBuffer byteBuffer;
    private StringBuffer nextBuffer;
    private ReadableByteChannel channel;
    private String splitString;
    private int timestampColumn;
    private SimpleDateFormat dateParser;
    private int dateParserType;
    private int valueColumn;
    private NumberFormat valueParser;
}