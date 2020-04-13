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
package dk.aau.modelardb.core;

import dk.aau.modelardb.core.utility.Pair;
import dk.aau.modelardb.core.utility.Static;

import java.util.*;
import java.util.stream.IntStream;

public class Partitioner {

    /** Public Methods **/
    public static TimeSeries[] initializeTimeSeries(Configuration configuration, int currentMaximumSID) {
        int cms = currentMaximumSID;
        String[] sources = configuration.getDataSources();
        ArrayList<TimeSeries> tss = new ArrayList<>();

        String separator = configuration.getString("modelardb.separator");
        boolean header = configuration.getBoolean("modelardb.header");
        int timestamps = configuration.getInteger("modelardb.timestamps");
        String dateFormat = configuration.getString("modelardb.dateformat");
        String timezone = configuration.getString("modelardb.timezone");
        int values = configuration.getInteger("modelardb.values");
        String locale = configuration.getString("modelardb.locale");

        //HACK: Resolution is one argument as all time series used for evaluation exhibit the same sampling interval
        int resolution = configuration.getResolution();

        //Checks if the users-defined configuration matches the actual data for all bounded time series (files)
        for (String source : sources) {
            if (source.contains(":")) {
                continue;
            }
            TimeSeries ts = new TimeSeries(source, cms, resolution, separator, header,
                    timestamps, dateFormat, timezone, values, locale);
            ts.initialize.run();
            if (ts.hasNext()) {
                DataPoint dp1 = ts.next();
                if (ts.hasNext()) {
                    long actualResolution = ts.next().timestamp - dp1.timestamp;
                    if (actualResolution != resolution) {
                        throw new IllegalArgumentException("CORE: resolution was " + actualResolution / 1000.0 + "s");
                    } else {
                        //The resolution matches what was expected so ingestion can continue
                        break;
                    }
                }
            }
        }

        //Initialize all time series, both bounded (files) and unbounded (sockets)
        for (String source : sources) {
            cms += 1;
            TimeSeries ts = null;
            if (source.contains(":")) {
                //The source is an address with port
                String[] ipSplitPort = source.split(":");
                int port = Integer.parseInt(ipSplitPort[1]);
                ts = new TimeSeries(ipSplitPort[0], port, cms, resolution, separator,
                        timestamps, dateFormat, timezone, values, locale);
            } else {
                //The source is an csv file without a .csv suffix
                ts = new TimeSeries(source, cms, resolution, separator, header,
                        timestamps, dateFormat, timezone, values, locale);
            }
            tss.add(ts);
        }
        Static.info(String.format("CORE: initialized %d time series", tss.size()));
        return tss.toArray(new TimeSeries[0]);
    }

    public static TimeSeriesGroup[] groupTimeSeries(Configuration configuration, TimeSeries[] timeSeries, int currentMaximumGid) {
        if (timeSeries.length == 0) {
            return new TimeSeriesGroup[0];
        }

        Correlation[] correlations = (Correlation[]) configuration.get("modelardb.correlation");
        Iterator<Integer> gids = IntStream.range(currentMaximumGid + 1, Integer.MAX_VALUE).iterator();
        TimeSeriesGroup[] groups = null;
        if (correlations.length == 0) {
            groups = Arrays.stream(timeSeries).map(ts -> new TimeSeriesGroup(gids.next(), new TimeSeries[]{ts}))
                    .toArray(TimeSeriesGroup[]::new);
        } else {
            //The time series in a group must be sorted by sid otherwise optimizations in SegmentGenerator fail
            Dimensions dimensions = configuration.getDimensions();
            TimeSeries[][] tss = Partitioner.groupTimeSeriesByCorrelation(timeSeries, dimensions, correlations);
            groups = Arrays.stream(tss).map(ts -> {
                Arrays.sort(ts, Comparator.comparingInt(ts2 -> ts2.sid));
                return new TimeSeriesGroup(gids.next(), ts);
            }).toArray(TimeSeriesGroup[]::new);
        }
        Static.info(String.format("CORE: created %d correlated groups", groups.length));
        return groups;
    }

    public static WorkingSet[] partitionTimeSeries(Configuration configuration, TimeSeriesGroup[] timeSeriesGroups,
                                                   HashMap<String, Integer> midCache, int partitions) {
        String partitionBy = configuration.getString("modelardb.partitionby");
        TimeSeriesGroup[][] pts = null;
        switch (partitionBy) {
            case "rate":
                pts = Partitioner.partitionTimeSeriesByRate(timeSeriesGroups, partitions);
                break;
            case "length":
                pts = Partitioner.partitionTimeSeriesByLength(timeSeriesGroups, partitions);
                break;
            default:
                throw new UnsupportedOperationException("CORE: unknown partitionby value \"" + partitionBy + "\"");
        }

        int[] mids = Arrays.stream(configuration.getModels()).mapToInt(midCache::get).toArray();
        return Arrays.stream(pts).map(tss -> new WorkingSet(tss, configuration.getFloat("modelardb.dynamicsplitfraction"),
                configuration.getModels(), mids, configuration.getError(), configuration.getLatency(), configuration.getLimit())).toArray(WorkingSet[]::new);
    }

    /** Private Methods **/
    //Grouping methods
    private static TimeSeries[][] groupTimeSeriesByCorrelation(TimeSeries[] timeSeries, Dimensions dimensions, Correlation[] correlations) {
        //Construct the initial set of groups
        ArrayList<TimeSeries[]> tsgs = new ArrayList<>();
        for (TimeSeries ts : timeSeries) {
            tsgs.add(new TimeSeries[]{ ts });
        }

        //Combine groups until a fixed point
        for (Correlation correlation : correlations) {
            boolean modified = true;
            while (modified) {
                modified = false;
                for (int i = 0; i < tsgs.size(); i++) {
                    for (int j = i + 1; j < tsgs.size(); j++) {
                        TimeSeries[] groupOne = tsgs.get(i);
                        TimeSeries[] groupTwo = tsgs.get(j);

                        //Combines the two groups if any of the correlations specified are applicable
                        if (correlation.test(groupOne, groupTwo, dimensions)) {
                            tsgs.set(i, Static.merge(groupOne, groupTwo));
                            correlation.updateScalingFactors(tsgs.get(i), dimensions);
                            tsgs.set(j, null); //Used instead of remove to silence the linter and allow batch removal
                            modified = true;
                        }
                    }
                    //Use of remove inside the loop annoys the linter and shifts all elements for each call of remove
                    tsgs.removeAll(Collections.singleton(null));
                }
            }
        }
        return tsgs.toArray(new TimeSeries[tsgs.size()][]);
    }

    //Partitioning methods
    private static TimeSeriesGroup[][] partitionTimeSeriesByRate(TimeSeriesGroup[] timeSeriesGroups, int partitions) {
        if (timeSeriesGroups.length == 0 && partitions == 0) {
            return new TimeSeriesGroup[0][0];
        }

        if (timeSeriesGroups.length > 0 && partitions == 0) {
            throw new RuntimeException("CORE: cannot split more then one time series group into zero partitions");
        }

        if (timeSeriesGroups.length < partitions) {
            throw new RuntimeException("CORE: at least one time series group must be available per partition");
        }

        //Multi-Way time series partitioning loosely based on the Complete Greedy Algorithm (CGA)
        PriorityQueue<Pair<Long, ArrayList<TimeSeriesGroup>>> sets = new PriorityQueue<>(Comparator.comparingLong(qe -> qe._1));
        for (int i = 0; i < partitions; i++) {
            sets.add(new Pair<>(0L, new ArrayList<>()));
        }

        //The groups are sorted by the rate of data points produced so the most resource intensive groups are placed first
        Arrays.sort(timeSeriesGroups, Comparator.comparingLong(tsg -> tsg.resolution / tsg.size()));
        for (TimeSeriesGroup tsg : timeSeriesGroups) {
            Pair<Long, ArrayList<TimeSeriesGroup>> min = sets.poll();
            min._1 = min._1 + (60000 / (tsg.resolution / tsg.size())); //Data Points per Minute
            min._2.add(tsg);
            sets.add(min);
        }
        return sets.stream().map(ts -> ts._2.toArray(new TimeSeriesGroup[0])).toArray(TimeSeriesGroup[][]::new);
    }

    private static TimeSeriesGroup[][] partitionTimeSeriesByLength(TimeSeriesGroup[] timeSeriesGroups, int partitions) {
        if (timeSeriesGroups.length == 0) {
            throw new RuntimeException("CORE: cannot split zero time series into partitions");
        }

        if (partitions == 0) {
            throw new RuntimeException("CORE: cannot split time series into zero partitions");
        }

        if (timeSeriesGroups.length < partitions) {
            throw new RuntimeException("CORE: at least one time series must be available per partition");
        }

        //Attempts to partition the time series groups into equal sized chunks while preserving the ordering for the groups
        ArrayList<List<TimeSeriesGroup>> sets = new ArrayList<>();
        List<TimeSeriesGroup> alts = Arrays.asList(timeSeriesGroups);
        if (partitions == 1) {
            sets.add(alts);
        } else {
            int timeSeries = Arrays.stream(timeSeriesGroups).map(TimeSeriesGroup::size).reduce((x,y) -> x + y).get();
            int chunkSize = timeSeries / partitions;
            int leftovers = timeSeries - (partitions * chunkSize);

            int toInsert = chunkSize;
            ArrayList<TimeSeriesGroup> partition = new ArrayList<>();
            for (TimeSeriesGroup tsg : timeSeriesGroups) {
                partition.add(tsg);
                toInsert -= tsg.size();
                if (toInsert <= 0) {
                    partitions -= 1;
                    toInsert = chunkSize;
                    //As partitions always contain at least 'toInsert' elements the last partition is often too small,
                    // to somewhat alleviate this problem in practise all leftover elements are added to this partition.
                    if (partitions == 1) {
                        toInsert += leftovers;
                    }
                    sets.add(partition);
                    partition = new ArrayList<>();
                }
            }
            sets.add(partition);
        }
        return sets.stream().map(ts -> ts.toArray(new TimeSeriesGroup[0])).toArray(TimeSeriesGroup[][]::new);
    }
}