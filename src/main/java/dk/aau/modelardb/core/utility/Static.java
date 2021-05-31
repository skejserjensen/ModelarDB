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
package dk.aau.modelardb.core.utility;

import dk.aau.modelardb.core.DataPoint;
import dk.aau.modelardb.core.timeseries.TimeSeries;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Enumeration;

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
        //The split and join algorithms depend on each group being ordered by tid
        if (tsA[0].tid < tsB[0].tid) {
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

    public static boolean outsidePercentageErrorBound(float error, double approximation, double real) {
        return Static.percentageError(approximation, real) > error;
    }

    public static double percentageError(double approximation, double real) {
        //Necessary as the method would return NaN if approximation and real are both zero otherwise
        if (approximation == real) {
            return 0.0;
        }

        double difference = real - approximation;
        double result = Math.abs(difference / real);
        return result * 100.0;
    }

    public static long gapsToBits(byte[] gaps, int[] timeSeries) {
        if (gaps.length == 0) {
            return 0;
        }

        BitSet bs = new BitSet();
        int[] values = bytesToInts(gaps);
        for (int g : values) {
            //The metadata cache contains the sampling interval as the first element
            int tid = Arrays.binarySearch(timeSeries, 1, timeSeries.length, g);
            bs.set(tid - 1); //Tids start at one, but there is no reason to waste a bit
        }
        return bs.toLongArray()[0];
    }

    public static byte[] bitsToGaps(long value, int[] timeSeries) {
        int index = 0;
        ArrayList<Integer> gaps = new ArrayList<>();
        while (value != 0L) {
            if (value % 2L != 0) {
                //Tids start at one, but gaps are stored as tid - 1 to not waste a bit
                gaps.add(timeSeries[index + 1]);
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
        System.out.printf("[%s] (%s) %s\n", LocalDateTime.now(), level, line);
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

                    if (result.length() != 0) {
                        result.append(",");
                    }
                    result.append(ip.getHostAddress());
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return result.toString();
    }
}
