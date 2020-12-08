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

        //HACK: Resolution is one argument as all time series used for evaluation has exhibits the same sampling interval
        int resolution = configuration.getResolution();

        //Initializes all time series, both bounded (files) and unbounded (sockets)
        for (String source : sources) {
            cms += 1;
            TimeSeries ts = null;
            if (source.contains(":")) {
                //The source is a socket
                String[] ipSplitPort = source.split(":");
                int port = Integer.parseInt(ipSplitPort[1]);
                ts = new TimeSeries(ipSplitPort[0], port, cms, resolution, separator,
                        timestamps, dateFormat, timezone, values, locale);
            } else {
                //The source is a csv file
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
            //If groups are specified as disjoint sets of time series, they can be created directly
            TimeSeries[][] tss = null;
            if (areAllDisjoint(correlations)) {
                tss = Partitioner.groupTimeSeriesOnlyBySource(timeSeries, correlations);
            } else {
                Dimensions dimensions = configuration.getDimensions();
                tss = Partitioner.groupTimeSeriesByCorrelation(timeSeries, dimensions, correlations);
            }

            //The time series in a group must be sorted by sid, otherwise, optimizations in SegmentGenerator fail
            groups = Arrays.stream(tss).map(ts -> {
                Arrays.sort(ts, Comparator.comparingInt(ts2 -> ts2.sid));
                return new TimeSeriesGroup(gids.next(), ts);
            }).toArray(TimeSeriesGroup[]::new);
        }
        Static.info(String.format("CORE: created %d time series group(s)", groups.length));
        return groups;
    }

    public static WorkingSet[] partitionTimeSeries(Configuration configuration, TimeSeriesGroup[] timeSeriesGroups,
                                                   HashMap<String, Integer> midCache, int partitions) {
        TimeSeriesGroup[][] pts = Partitioner.partitionTimeSeriesByRate(timeSeriesGroups, partitions);
        int[] mids = Arrays.stream(configuration.getModels()).mapToInt(midCache::get).toArray();
        WorkingSet[] workingSets = Arrays.stream(pts).map(tss -> new WorkingSet(tss, configuration.getFloat(
                "modelardb.dynamicsplitfraction"), configuration.getModels(), mids, configuration.getError(),
                configuration.getLatency(), configuration.getLimit())).toArray(WorkingSet[]::new);
        Static.info(String.format("CORE: created %d working set(s)", workingSets.length));
        return workingSets;
    }

    /** Private Methods **/
    public static boolean areAllDisjoint(Correlation[] corr) {
        HashSet<String> all = new HashSet<>();
        for (Correlation clause : corr) {
            if ( ! clause.hasOnlyCorrelatedSources()) {
                return false;
            }

            HashSet<String> sources = clause.getCorrelatedSources();
            int orgSize = all.size();
            for (String source : sources) {
                all.add(source);
            }

            int newSize = all.size();
            if (newSize - orgSize != sources.size()) {
                return false;
            }
        }
        return true;
    }

    //Grouping Methods
    private static TimeSeries[][] groupTimeSeriesByCorrelation(TimeSeries[] timeSeries, Dimensions dimensions, Correlation[] correlations) {
        //Constructs the initial set of groups
        ArrayList<TimeSeries[]> tsgs = new ArrayList<>();
        for (TimeSeries ts : timeSeries) {
            tsgs.add(new TimeSeries[]{ ts });
        }

        //Combines groups until a fixed point is reached
        for (Correlation correlation : correlations) {
            boolean modified = true;
            while (modified) {
                modified = false;
                for (int i = 0; i < tsgs.size(); i++) {
                    for (int j = i + 1; j < tsgs.size(); j++) {
                        TimeSeries[] groupOne = tsgs.get(i);
                        TimeSeries[] groupTwo = tsgs.get(j);

                        //Combines the two groups if they are correlated according to the user-specified primitives
                        if (correlation.test(groupOne, groupTwo, dimensions)) {
                            tsgs.set(i, Static.merge(groupOne, groupTwo));
                            correlation.updateScalingFactors(tsgs.get(i), dimensions);
                            tsgs.set(j, null);
                            modified = true;
                        }
                    }
                    //Use of remove inside the loop shifts all elements for each call of remove
                    tsgs.removeAll(Collections.singleton(null));
                }
            }
        }
        return tsgs.toArray(new TimeSeries[tsgs.size()][]);
    }

    private static TimeSeries[][] groupTimeSeriesOnlyBySource(TimeSeries[] timeSeries, Correlation[] correlations) {
        //Allows iterating over the correlations and time series only once
        HashMap<String, ArrayList<TimeSeries>> sourceToGroup = new HashMap<>();
        for (Correlation corr : correlations) {
            int expectedGroupSize = corr.getCorrelatedSources().size();
            ArrayList<TimeSeries> groupMembers = new ArrayList<>(expectedGroupSize);
            for (String source : corr.getCorrelatedSources()) {
                sourceToGroup.put(source, groupMembers);
            }
        }

        //Each time series can now be assigned directly to its group
        for (TimeSeries ts : timeSeries) {
            //Time series that are not assigned to a group are added to their own group
            if (sourceToGroup.containsKey(ts.source)) {
                sourceToGroup.get(ts.source).add(ts);
            } else {
                ArrayList<TimeSeries> group = new ArrayList<>();
                group.add(ts);
                sourceToGroup.put(ts.source, group);
            }
        }
        return sourceToGroup.values().stream().distinct()
                .map(al -> al.toArray(new TimeSeries[al.size()])).toArray(TimeSeries[][]::new);
    }

    //Partitioning Methods
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

        //The groups are sorted by gid to make the order they are ingested in deterministic
        return sets.stream().map(ts -> {
            ts._2.sort(Comparator.comparingInt(tsg -> tsg.gid));
            return ts._2.toArray(new TimeSeriesGroup[ts._2.size()]);
        }).toArray(TimeSeriesGroup[][]::new);
    }
}
