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
package dk.aau.modelardb.core;

import dk.aau.modelardb.core.timeseries.*;
import dk.aau.modelardb.core.utility.Pair;
import dk.aau.modelardb.core.utility.Static;
import dk.aau.modelardb.core.utility.ValueFunction;

import java.util.*;
import java.util.stream.IntStream;

public class Partitioner {

    /** Public Methods **/
    public static TimeSeries[] initializeTimeSeries(Configuration configuration, int currentMaximumTid) {
        int cms = currentMaximumTid;
        String[] sources = configuration.getSources();
        ArrayList<TimeSeries> tss = new ArrayList<>();

        String separator = configuration.getString("modelardb.csv.separator");
        boolean header = configuration.getBoolean("modelardb.csv.header");
        int timestampColumnIndex = configuration.getInteger("modelardb.timestamp_column");
        String dateFormat = configuration.getString("modelardb.csv.date_format");
        String timeZone = configuration.getString("modelardb.time_zone");
        int valueColumnIndex = configuration.getInteger("modelardb.value_column");
        String locale = configuration.getString("modelardb.csv.locale");

        //HACK: Sampling interval is one argument as all time series used for evaluation used the same sampling interval
        int samplingInterval = configuration.getSamplingInterval();

        //Derived data sources are normalized so all use tids to simply the processing in Storage
        String derivedKey = "modelardb.sources.derived";
        HashMap<String, Pair<String, ValueFunction>[]> derivedDataSources =
                (HashMap<String, Pair<String, ValueFunction>[]>) configuration.remove(derivedKey)[0];
        HashMap<Integer, Pair<String, ValueFunction>[]> derivedTimeSeries = new HashMap<>();

        //Initializes all time series, both bounded (files) and unbounded (sockets)
        for (String source : sources) {
            cms += 1;
            TimeSeries ts;
            if (source.contains(":")) {
                ts = new AsyncTimeSeriesSocket(source, cms, samplingInterval, separator,
                        timestampColumnIndex, dateFormat, timeZone, valueColumnIndex, locale);
            }  else if (source.endsWith(".orc")) {
                ts = new TimeSeriesORC(source, cms, samplingInterval, timestampColumnIndex, valueColumnIndex);
            }  else if (source.endsWith(".parquet")) {
                ts = new TimeSeriesParquet(source, cms, samplingInterval, timestampColumnIndex, valueColumnIndex);
            } else {
                ts = new TimeSeriesCSV(source, cms, samplingInterval, separator, header,
                        timestampColumnIndex, dateFormat, timeZone, valueColumnIndex, locale);
            }
            tss.add(ts);

            //If any derived time series are defined for the source they must be mapped to its tid
            if (derivedDataSources.containsKey(ts.source)) {
                derivedTimeSeries.put(cms, derivedDataSources.get(ts.source));
                derivedDataSources.remove(ts.source);
            }
        }

        //All derived data sources that do not map to a new data source must map to a tid
        try {
            final int finalCMS = cms;
            derivedDataSources.forEach((key, value) -> {
                int tid = Integer.parseInt(key);
                if (tid < 1 || tid > finalCMS) {
                    throw new IllegalArgumentException("CORE: tid " + tid + " in " + derivedKey + " is out of range");
                }
                derivedTimeSeries.put(tid, value);
            });
        } catch (NumberFormatException nfe) {
            String valueBeingParsed = nfe.getMessage().substring(18);
            throw new IllegalArgumentException("CORE: error parsing " + valueBeingParsed  + " specified in " + derivedKey, nfe);
        }
        configuration.add(derivedKey, derivedTimeSeries);

        int dtsc = derivedTimeSeries.values().stream().mapToInt(pairs -> pairs.length).sum();
        Static.info(String.format("CORE: initialized %d time series and %d derived time series", tss.size(), dtsc));
        return tss.toArray(new TimeSeries[0]);
    }

    public static TimeSeriesGroup[] groupTimeSeries(Configuration configuration, TimeSeries[] timeSeries, int currentMaximumGid) {
        if (timeSeries.length == 0) {
            return new TimeSeriesGroup[0];
        }

        Correlation[] correlations = configuration.getCorrelations();
        Iterator<Integer> gids = IntStream.range(currentMaximumGid + 1, Integer.MAX_VALUE).iterator();
        TimeSeriesGroup[] groups;
        if (correlations.length == 0) {
            groups = Arrays.stream(timeSeries).map(ts -> new TimeSeriesGroup(gids.next(), new TimeSeries[]{ts}))
                    .toArray(TimeSeriesGroup[]::new);
        } else {
            //If groups are specified as disjoint sets of time series, they can be created directly
            TimeSeries[][] tss;
            if (areAllDisjoint(correlations)) {
                tss = Partitioner.groupTimeSeriesOnlyBySource(timeSeries, correlations);
            } else {
                Dimensions dimensions = configuration.getDimensions();
                tss = Partitioner.groupTimeSeriesByCorrelation(timeSeries, dimensions, correlations);
            }

            //The time series in a group must be sorted by tid, otherwise, optimizations in SegmentGenerator fail
            groups = Arrays.stream(tss).map(ts -> {
                Arrays.sort(ts, Comparator.comparingInt(ts2 -> ts2.tid));
                return new TimeSeriesGroup(gids.next(), ts);
            }).toArray(TimeSeriesGroup[]::new);
        }
        Static.info(String.format("CORE: created %d time series group(s)", groups.length));
        return groups;
    }

    public static WorkingSet[] partitionTimeSeries(Configuration configuration, TimeSeriesGroup[] timeSeriesGroups,
                                                   HashMap<String, Integer> mtidCache, int partitions) {
        TimeSeriesGroup[][] pts = Partitioner.partitionTimeSeriesByRate(timeSeriesGroups, partitions);
        int[] mtids = Arrays.stream(configuration.getModelTypeNames()).mapToInt(mtidCache::get).toArray();
        WorkingSet[] workingSets = Arrays.stream(pts).map(tss -> new WorkingSet(tss, configuration.getFloat(
                "modelardb.dynamic_split_fraction"), configuration.getModelTypeNames(), mtids,
                configuration.getErrorBound(), configuration.getLengthBound(), configuration.getMaximumLatency()))
                .toArray(WorkingSet[]::new);
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

            int orgSize = all.size();
            HashSet<String> sources = clause.getCorrelatedSources();
            all.addAll(sources);

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
                .map(al -> al.toArray(new TimeSeries[0])).toArray(TimeSeries[][]::new);
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
        Arrays.sort(timeSeriesGroups, Comparator.comparingLong(tsg -> tsg.samplingInterval / tsg.size()));
        for (TimeSeriesGroup tsg : timeSeriesGroups) {
            Pair<Long, ArrayList<TimeSeriesGroup>> min = sets.poll();
            min._1 = min._1 + (60000 / (tsg.samplingInterval / tsg.size())); //Data Points per Minute
            min._2.add(tsg);
            sets.add(min);
        }

        //The groups are sorted by gid to make the order they are ingested in deterministic
        return sets.stream().map(ts -> {
            ts._2.sort(Comparator.comparingInt(tsg -> tsg.gid));
            return ts._2.toArray(new TimeSeriesGroup[0]);
        }).toArray(TimeSeriesGroup[][]::new);
    }
}
