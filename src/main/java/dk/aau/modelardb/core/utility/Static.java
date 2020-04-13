/* Copyright 2018-2019 Aalborg University
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
package dk.aau.modelardb.core.utility;

import dk.aau.modelardb.core.*;
import dk.aau.modelardb.core.models.Segment;

import java.io.FileWriter;
import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.time.LocalTime;
import java.util.*;

public class Static {

    /** Public Methods **/
    public static int[] bytesToInts(byte[] bytes) {
        int intsLength = bytes.length / 4;
        int[] ints = new int[intsLength];

        IntBuffer lb = ByteBuffer.wrap(bytes).asIntBuffer();
        for (int i = 0; i < intsLength; i++) {
            ints[i] = lb.get();
        }
        return ints;
    }

    public static byte[] intToBytes(int[] ints) {
        int byteLength = ints.length * 4;
        ByteBuffer bb = ByteBuffer.allocate(byteLength);

        for (int i : ints) {
            bb.putInt(i);
        }
        bb.flip();
        return bb.array();
    }

    public static int indexOf(Object o, Object[] objs) {
        for (int i = 0; i < objs.length; i++) {
            if (objs[i].equals(o)) {
                return i;
            }
        }
        return -1;
    }

    public static boolean contains(int i, int[] xs) {
        for (int x : xs) {
            if (x == i) {
                return true;
            }
        }
        return false;
    }

    public static TimeSeries[] merge(TimeSeries[] tsA, TimeSeries[] tsB) {
        TimeSeries[] tss = new TimeSeries[tsA.length + tsB.length];
        //The join and split algorithms depend on each group being numerically ordered by sid
        if (tsA[0].sid < tsB[0].sid) {
            System.arraycopy(tsA, 0, tss, 0, tsA.length);
            System.arraycopy(tsB, 0, tss, tsA.length, tsB.length);
        } else {
            System.arraycopy(tsB, 0, tss, 0, tsB.length);
            System.arraycopy(tsA, 0, tss, tsB.length, tsA.length);
        }
        return tss;
    }

    public static float min(DataPoint[] dps) {
        float min = dps[0].value;
        for (DataPoint dp : dps) {
            if (dp.value < min) {
                min = dp.value;
            }
        }
        return min;
    }

    public static float max(DataPoint[] dps) {
        float max = dps[0].value;
        for (DataPoint dp : dps) {
            if (max < dp.value) {
                max = dp.value;
            }
        }
        return max;
    }

    public static float avg(DataPoint[] dps) {
        double sum = 0.0;
        for (DataPoint dp : dps) {
            sum += dp.value;
        }
        return (float) (sum / dps.length);
    }

    public static double percentageError(double approximation, double real) {
        //Necessary as the method would return NaN if approximation and real are both 0.0 otherwise
        if (approximation == real) {
            return 0.0;
        }

        double difference = real - approximation;
        double result = Math.abs(difference / real);
        return result * 100.0;
    }

    public static long gapsToBits(byte[] gaps, int[] sources) {
        if (gaps.length == 0) {
            return 0;
        }

        BitSet bs = new BitSet();
        int[] values = bytesToInts(gaps);
        for (int g : values) {
            //The metadata cache contains the resolution as the first element
            int sid = Arrays.binarySearch(sources, 1, sources.length, g);
            bs.set(sid - 1); //Sids start at one but there is not reason to waste a bit
        }
        return bs.toLongArray()[0];
    }

    public static byte[] bitsToGaps(long value, int[] sources) {
        int index = 0;
        ArrayList<Integer> gaps = new ArrayList<>();
        while (value != 0L) {
            if (value % 2L != 0) {
                //Sids start at one but there is not reason to waste a bit so each sid are stored as sid - 1
                gaps.add(sources[index + 1]);
            }
            ++index;
            value = value >>> 1;
        }
        return intToBytes(gaps.stream().mapToInt(i -> i).toArray());
    }

    public static boolean isInteger(String s) {
        try {
            Integer.parseInt(s);
            return true;
        } catch (NumberFormatException nfe){
            return false;
        }
    }

    public static boolean isFloat(String s) {
        try {
            Float.parseFloat(s);
            return true;
        } catch (NumberFormatException nfe){
            return false;
        }
    }

    public static void SIGTERM() throws IOException {
        String name = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
        String pid = name.split("@")[0];
        Runtime.getRuntime().exec("kill " + pid);
    }

    public static void info(String line) {
        log(line, "INFO");
    }

    public static void info(String line, int limit) {
        log(line, "INFO", limit);
    }

    public static void warn(String line) {
        log(line, "WARN");
    }

    public static void warn(String line, int limit) {
        log(line, "WARN", limit);
    }

    public static void log(String line, String level) {
        System.out.printf("[%s] (%s) %s\n", LocalTime.now(), level, line);
    }

    public static void log(String line, String level, int limit) {
        if (line.length() <= limit) {
            log(line, level);
        } else {
            log(line.substring(0, limit) + " ...", level);
        }
    }

    public static String getIPs() {
        StringBuilder result = new StringBuilder();
        try {
            //Iterates through all non-loopback interfaces and extracts the IP addresses
            Enumeration<NetworkInterface> nss = NetworkInterface.getNetworkInterfaces();
            while (nss.hasMoreElements()) {
                java.net.NetworkInterface ns = nss.nextElement();
                if (ns.isLoopback() || !ns.isUp())
                    continue;

                //Iterates through all IP addresses and extracts all IPv4 addresses
                Enumeration<InetAddress> ips = ns.getInetAddresses();
                while (ips.hasMoreElements()) {
                    java.net.InetAddress ip = ips.nextElement();
                    if (ip instanceof Inet6Address)
                        continue;

                    if (result.length() == 0) {
                        result.append(ip.getHostAddress());
                    } else {
                        result.append(",");
                        result.append(ip.getHostAddress());
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return result.toString();
    }

    public static void writeModelDebugFile(String debugFilePath, Storage storage, TimeSeries realTimeSeries, float error) {
        ArrayList<String> debugOutputBuffer = new ArrayList<>();

        //Reinitializes all time series to reset the iterators
        Iterator<Pair<DataPoint, Segment>> modelledTimeSeries =  storage.getSegments().flatMap(segmentData -> {
            Segment[] segments = segmentData.toSegments(storage);
            return Arrays.stream(segments).flatMap(segment -> segment.grid().map(dp -> new Pair<>(dp, segment)));
        }).iterator();
        realTimeSeries.initialize.run();

        while (realTimeSeries.hasNext() && modelledTimeSeries.hasNext()) {
            DataPoint os = realTimeSeries.next();
            Pair<DataPoint, Segment> ms = modelledTimeSeries.next();

            String sb = String.valueOf(ms._1.timestamp / 1000) + ';' +  os.value + ';' + ms._1.value + ';' +
                    error + ';' + System.identityHashCode(ms._2) + ';' + ms._2.getClass().getName() + '\n';
            debugOutputBuffer.add(sb);
        }

        //Verifies that the time series are of equal length before writing the buffer to disk
        if ((realTimeSeries.hasNext() && realTimeSeries.next() != null) || modelledTimeSeries.hasNext()) {
            throw new java.lang.IllegalArgumentException("CORE: modelardb.resolution must match the original sampling rate for debug");
        }

        String header = "timestamp;real_value;approx_value;error;segment_id;segment_type\n";
        Static.writeDebugFile(debugFilePath, header, debugOutputBuffer);
    }

    public static void writeDimensionDebugFile(String debugFilePath, Dimensions dimensions) {
        //Constructs fake time series for each line in the dimensions file
        String[] sources = dimensions.getSources();
        TimeSeries[] tss = new TimeSeries[sources.length];
        for (int i = 0; i < sources.length; i++) {
            tss[i] = new TimeSeries(sources[i], 0, 0, "", false, 0, "", "", 0, "");
        }

        TimeSeriesGroup[] tsgs = Partitioner.groupTimeSeries(Configuration.get(), tss, 0);
        Arrays.sort(tsgs, (a, b) -> -Integer.compare(a.size(), b.size()));

        ArrayList<String> debugOutputBuffer = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        for (TimeSeriesGroup tsg : tsgs) {
            for (TimeSeries ts : tsg.getTimeSeries()) {
                Object[] columns = dimensions.get(ts.source);
                sb.append(ts.source);
                for (Object col : columns) {
                    sb.append(",");
                    sb.append(col.toString());
                }
                sb.append("\n");
            }
            sb.append("\n");
            debugOutputBuffer.add(sb.toString());
            sb.setLength(0);
        }

        Static.writeDebugFile(debugFilePath, null, debugOutputBuffer);
    }

    /** Private Methods **/
    private static void writeDebugFile(String debugFilePath, String header, ArrayList<String> debugOutputBuffer) {
        try {
            FileWriter debugOutputFile = new FileWriter(debugFilePath);
            if (header != null) {
                debugOutputFile.write(header);
            }
            for (String line : debugOutputBuffer) {
                debugOutputFile.write(line);
            }
            debugOutputFile.close();
        } catch (IOException ioe) {
            throw new java.lang.RuntimeException("CORE: unable to write debug output file due to " + ioe);
        }
    }
}