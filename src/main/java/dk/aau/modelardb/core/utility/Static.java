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
package dk.aau.modelardb.core.utility;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.*;

import dk.aau.modelardb.core.DataPoint;
import dk.aau.modelardb.core.Storage;
import dk.aau.modelardb.core.TimeSeries;
import dk.aau.modelardb.core.models.Segment;

public class Static {

    /** Public Methods **/
    public static long[] bytesToLongs(byte[] bytes) {
        int longsLength = bytes.length / 8;
        long[] longs = new long[longsLength];

        LongBuffer lb = ByteBuffer.wrap(bytes).asLongBuffer();
        for (int i = 0; i < longsLength; i++) {
            longs[i] = lb.get();
        }
        return longs;
    }

    public static byte[] longsToBytes(long[] la) {
        int byteLength = la.length * 8;
        ByteBuffer bb = ByteBuffer.allocate(byteLength);

        for (long l : la) {
            bb.putLong(l);
        }
        bb.flip();
        return bb.array();
    }

    public static Iterator<TimeSeries[]> partitionTimeSeriesByRate(TimeSeries[] timeSeries, int partitions) {
        if (timeSeries.length == 0 && partitions == 0) {
            return Collections.emptyIterator();
        }

        if (timeSeries.length > 0 && partitions == 0) {
            throw new RuntimeException("cannot split more then one time series into zero partitions");
        }

        if (timeSeries.length < partitions) {
            throw new RuntimeException("at least one time series must be available per partition");
        }

        //Multi-Way time series partitioning loosely based on the Complete Greedy Algorithm (CGA)
        PriorityQueue<Pair<Long, ArrayList<TimeSeries>>> sets = new PriorityQueue<>(Comparator.comparingLong(qe -> qe._1));
        for (int i = 0; i < partitions; i++) {
            sets.add(new Pair<>(0L, new ArrayList<>()));
        }

        Arrays.sort(timeSeries, (t1, t2) -> Long.compare(t2.resolution, t1.resolution));
        for (TimeSeries ts : timeSeries) {
            Pair<Long, ArrayList<TimeSeries>> min = sets.poll();
            min._1 = min._1 + (60000 / ts.resolution); //Data Points per Minute
            min._2.add(ts);
            sets.add(min);
        }
        return sets.stream().map(ts -> ts._2.toArray(new TimeSeries[0])).iterator();
    }

    public static Iterator<TimeSeries[]> partitionTimeSeriesByLength(TimeSeries[] timeSeries, int partitions) {
        if (timeSeries.length == 0 && partitions == 0) {
            return Collections.emptyIterator();
        }

        if (timeSeries.length > 0 && partitions == 0) {
            throw new RuntimeException("cannot split more then one time series into zero partitions");
        }

        if (timeSeries.length < partitions) {
            throw new RuntimeException("at least one time series must be available per partition");
        }

        //Partitioning of time series into equally size chunks to preserve the ordering
        ArrayList<List<TimeSeries>> sets = new ArrayList<>();
        List<TimeSeries> alts = Arrays.asList(timeSeries);
        if (partitions == 1) {
            sets.add(alts);
        } else {
            int start = 0;
            int chunkSize = timeSeries.length / partitions;
            int leftovers = timeSeries.length - (partitions * chunkSize);
            while (start < timeSeries.length) {
                int end = start + chunkSize;;
                if (leftovers > 0) {
                    end += 1;
                    leftovers -= 1;
                }
                sets.add(alts.subList(start, end));
                start = end;
            }
        }
        return sets.stream().map(ts -> ts.toArray(new TimeSeries[0])).iterator();
    }

    public static void writeDebugFile(String debugFilePath, Storage storage, TimeSeries realTimeSeries, float error) {
        ArrayList<String> debugOutputBuffer = new ArrayList<>();

        //Reinitialize all time series to reset the iterators
        Iterator<Pair<DataPoint, Segment>> modelledTimeSeries =
                storage.getSegments().flatMap(segment -> segment.grid().map(dp -> new Pair<>(dp, segment))).iterator();
        realTimeSeries.init.run();

        while (realTimeSeries.hasNext() && modelledTimeSeries.hasNext()) {
            DataPoint os = realTimeSeries.next();
            Pair<DataPoint, Segment> ms = modelledTimeSeries.next();
            StringBuilder sb = new StringBuilder();

            sb.append(ms._1.timestamp / 1000);
            sb.append(';');
            sb.append(os.value);
            sb.append(';');
            sb.append(ms._1.value);
            sb.append(';');
            sb.append(error);
            sb.append(';');
            sb.append(java.lang.System.identityHashCode(ms._2));
            sb.append(';');
            sb.append(ms._2.getClass().getName());
            sb.append('\n');
            debugOutputBuffer.add(sb.toString());
        }

        //Verify that both iterators are finished, indicating matching resolution, before writing the buffer to disk
        if ((realTimeSeries.hasNext() && realTimeSeries.next() != null) || modelledTimeSeries.hasNext()) {
            throw new java.lang.IllegalArgumentException("modelardb.resolution must match the original sampling rate for debug");
        }

        try {
            FileWriter debugOutputFile = new FileWriter(debugFilePath);
            debugOutputFile.write("timestamp;real_value;approx_value;error;segment_id;segment_type\n");
            for (String line : debugOutputBuffer) {
                debugOutputFile.write(line);
            }
            debugOutputFile.close();
        } catch (IOException ioe) {
            throw new java.lang.RuntimeException("unable to write debug output file due to " + ioe);
        }
    }

    public static void SIGTERM() throws IOException {
        String name = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
        String pid = name.split("@")[0];
        Runtime.getRuntime().exec("kill " + pid);
    }
}