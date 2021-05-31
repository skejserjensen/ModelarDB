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

import dk.aau.modelardb.core.models.ModelType;
import dk.aau.modelardb.core.timeseries.TimeSeries;
import dk.aau.modelardb.core.utility.Logger;
import dk.aau.modelardb.core.utility.ReverseBufferIterator;
import dk.aau.modelardb.core.utility.SegmentFunction;
import dk.aau.modelardb.core.utility.Static;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SegmentGenerator {

    /** Constructors **/
    SegmentGenerator(TimeSeriesGroup timeSeriesGroup, Supplier<ModelType[]> modelTypeInitializer,
                     ModelType fallbackModelType, List<Integer> tids, int maximumLatency, float dynamicSplitFraction,
                     SegmentFunction temporarySegmentStream, SegmentFunction finalizedSegmentStream) {

        //Variables from the constructor
        this.gid = timeSeriesGroup.gid;
        this.timeSeriesGroup = timeSeriesGroup;
        this.modelTypes = modelTypeInitializer.get();
        this.fallbackModelType = fallbackModelType;
        this.maximumLatency = maximumLatency;
        this.tids = tids;
        this.samplingInterval = timeSeriesGroup.samplingInterval;

        this.modelTypeInitializer = modelTypeInitializer;
        this.finalizedSegmentStream = finalizedSegmentStream;
        this.temporarySegmentStream = temporarySegmentStream;

        //State variables for controlling split generators
        this.emittedFinalizedSegments = 0;
        this.compressionRatioAverage = 0.0;
        this.finalizedSegmentsBeforeNextJoinCheck = 1;
        this.dynamicSplitFraction = dynamicSplitFraction;
        this.splitSegmentGenerators = new ArrayList<>();
        this.splitsToJoinIfCorrelated = new HashSet<>();

        //State variables for buffering data points
        this.gaps = new HashSet<>();
        this.buffer = new ArrayList<>();
        this.previousTimeStamps = new long[timeSeriesGroup.size()];

        //State variables for fitting the current model
        this.modelTypeIndex = 0;
        this.dataPointsYetEmitted = 0;
        this.currentModelType = this.modelTypes[0];
        this.currentModelType.initialize(this.buffer);

        //DEBUG: logger instance for counting segments used for this generator
        this.logger = new Logger(this.timeSeriesGroup.size());
    }

    /** Package Methods **/
    boolean consumeAllDataPoints() {
        boolean consumedDataPoints = false;
        while (this.timeSeriesGroup.hasNext()) {
            //Ingests data points until a split occurs or no more data points are available
            while (this.splitSegmentGenerators.isEmpty() && this.timeSeriesGroup.hasNext()) {
                consumeDataPoints(this.timeSeriesGroup.next(), this.timeSeriesGroup.getActiveTimeSeries());
                consumedDataPoints = true;
            }

            //Ingests data points for all splits until they are all joined or no more data points are available
            boolean splitSegmentGeneratorHasNext = true;
            while ( ! this.splitSegmentGenerators.isEmpty() && splitSegmentGeneratorHasNext) {
                splitSegmentGeneratorHasNext = false;
                int splitSegmentGeneratorSize = this.splitSegmentGenerators.size();
                for (int i = 0; i < splitSegmentGeneratorSize; i++) {
                    SegmentGenerator sg = this.splitSegmentGenerators.get(i);
                    if (sg.timeSeriesGroup.hasNext()) {
                        splitSegmentGeneratorHasNext = true;
                        sg.consumeDataPoints(sg.timeSeriesGroup.next(), sg.timeSeriesGroup.getActiveTimeSeries());
                        consumedDataPoints = true;
                    }
                }
                this.splitSegmentGenerators.removeAll(Collections.singleton(null));
                joinGroupsIfTheirTimeSeriesAreCorrelated();
            }
        }
        return consumedDataPoints;
    }

    TimeSeriesGroup getTimeSeriesGroup() {
        return this.timeSeriesGroup;
    }

    void close() {
        for (SegmentGenerator sg : this.splitSegmentGenerators) {
            sg.flushBuffer();
            sg.timeSeriesGroup.close();
        }
        flushBuffer();
        this.timeSeriesGroup.close();
    }

    //** Private Methods **/
    private void consumeDataPoints(DataPoint[] curDataPointsAndGaps, int activeTimeSeries) {
        //DEBUG: adds either a key our five seconds delay to continue
        //this.logger.pauseAndPrint(curDataPointsAndGaps);
        //this.logger.sleepAndPrint(curDataPointsAndGaps, 5000);

        //If no time series provided any values for this time stamp all computations can be skipped
        if (activeTimeSeries == 0) {
            return;
        }

        //If any of the time series are missing values, a gap is stored for that time series
        int nextDataPoint = 0;
        DataPoint[] currentDataPoints = new DataPoint[activeTimeSeries];
        for (int i = 0; i < curDataPointsAndGaps.length; i++) {
            DataPoint cdpg = curDataPointsAndGaps[i];
            if (Float.isNaN(cdpg.value)) {
                //A NaN value indicates the start of a gap, so we flush and store its tid in gaps
                if ( ! this.gaps.contains(cdpg.tid)) {
                    flushBuffer();
                    this.gaps.add(cdpg.tid);
                }
            } else {
                //A floating-point value indicates the end of a gap if more then the sampling interval have passed
                long pts = this.previousTimeStamps[i];
                if ((cdpg.timestamp - pts) > this.samplingInterval) {
                    //A gap have ended so we flush the buffer and remove the tid from gaps
                    flushBuffer();
                    this.gaps.remove(cdpg.tid);
                }
                currentDataPoints[nextDataPoint] = cdpg;
                this.previousTimeStamps[i] = cdpg.timestamp;
                nextDataPoint++;
            }
        }
        //A new data point has been ingested but not yet emitted
        this.buffer.add(currentDataPoints);
        this.dataPointsYetEmitted++;

        //The current model type is given the data points and it verifies that the model can represent them and all prior,
        // it is assumed that append will fail if it failed in the past, so append(t,V) must fail if append(t-1,V) failed
        if ( ! this.currentModelType.append(currentDataPoints)) {
            this.modelTypeIndex += 1;
            if (this.modelTypeIndex == this.modelTypes.length) {
                //If none of the model types can represent all of the buffered data points, the model type that provides
                // the best compression is selected and a segment that store the values a model of the type is construct
                emitFinalSegment();
                resetModelTypeIndex();
            } else {
                this.currentModelType = this.modelTypes[this.modelTypeIndex];
                this.currentModelType.initialize(this.buffer);
            }
        }

        //Emits a temporary segment if latency data points have been added to the buffer without a finalized segment being
        // emitted, if the current model does not represent all of the data points in the buffer the fallback model is used
        if (this.maximumLatency > 0 && this.dataPointsYetEmitted == this.maximumLatency) {
            emitTemporarySegment();
            this.dataPointsYetEmitted = 0;
        }
    }

    private void flushBuffer() {
        //If no data points are currently stored in the buffer it cannot be flushed
        if (this.buffer.isEmpty()) {
            return;
        }

        //Any uninitialized model types must be initialized before the buffer is flushed
        for (this.modelTypeIndex += 1; this.modelTypeIndex < this.modelTypes.length; this.modelTypeIndex++) {
            modelTypes[this.modelTypeIndex].initialize(this.buffer);
        }

        //Finalized segments are emitted until the buffer is empty, dynamic splitting is disabled as flushing can
        // create models with a poor compression ratio despite the time series in the group still being correlated
        float previousDynamicSplitFraction = this.dynamicSplitFraction;
        this.dynamicSplitFraction = 0;
        while ( ! buffer.isEmpty()) {
            emitFinalSegment();
            for (ModelType m : this.modelTypes) {
                m.initialize(this.buffer);
            }
        }
        this.dynamicSplitFraction = previousDynamicSplitFraction;
        resetModelTypeIndex();
    }

    private void resetModelTypeIndex() {
        //Restarts ingestion using the first model type and the currently buffered data points
        this.modelTypeIndex = 0;
        this.currentModelType = modelTypes[modelTypeIndex];
        this.currentModelType.initialize(this.buffer);
    }

    private void emitTemporarySegment() {
        //The fallback model type is used if the current model type cannot represent the data points in the buffer
        ModelType modelTypeToBeEmitted = this.currentModelType;
        if (modelTypeToBeEmitted.length() < this.buffer.size() ||
                Float.isNaN(compressionRatio(modelTypeToBeEmitted))) {
            modelTypeToBeEmitted = this.fallbackModelType;
            modelTypeToBeEmitted.initialize(this.buffer);
        }

        //The list of gaps are copied to ensure they do not change
        ArrayList<Integer> gaps = new ArrayList<>(this.gaps);

        //A segment containing the current model type is constructed and emitted
        emitSegment(this.temporarySegmentStream, modelTypeToBeEmitted, gaps);

        //DEBUG: all the debug counters can be updated as we have emitted a temporary segment
        this.logger.updateTemporarySegmentCounters(modelTypeToBeEmitted, gaps.size());
    }

    private void emitFinalSegment() {
        //The model type providing the model with best compression ratio is selected as mostEfficientModelType
        ModelType mostEfficientModelType = this.modelTypes[0];
        for (ModelType modelType : this.modelTypes) {
            mostEfficientModelType = (compressionRatio(modelType) < compressionRatio(mostEfficientModelType)) ? mostEfficientModelType : modelType;
        }

        //If none of the model types has received enough data points to fit a model to them, the fallback model type is used
        int mostEfficientModelTypeLength = mostEfficientModelType.length();
        float highestCompressionRatio = compressionRatio(mostEfficientModelType);
        if (Float.isNaN(highestCompressionRatio) || mostEfficientModelTypeLength == 0) {
            mostEfficientModelType = this.fallbackModelType;
            mostEfficientModelType.initialize(this.buffer);
            mostEfficientModelTypeLength = mostEfficientModelType.length();
            highestCompressionRatio = compressionRatio(mostEfficientModelType);
        }

        //A segment containing the model with the best compression ratio is constructed and emitted
        emitSegment(this.finalizedSegmentStream, mostEfficientModelType, new ArrayList<>(this.gaps));
        this.buffer.subList(0, mostEfficientModelTypeLength).clear();

        //If the number of data points in the buffer is less then the number of data points that has yet to be
        // emitted, then some of these data points have already been emitted as part of the finalized segment
        this.dataPointsYetEmitted = Math.min(this.dataPointsYetEmitted, buffer.size());

        //The best model is stored as it's error function is used when computing the split/join heuristics
        this.lastEmittedModelType = mostEfficientModelType;

        //DEBUG: all the debug counters are updated based on the emitted finalized segment
        this.logger.updateFinalizedSegmentCounters(mostEfficientModelType, this.gaps.size());

        //If the time series have changed it might beneficial to split or join their groups
        boolean compressionRatioIsBelowAverage = checkIfCompressionRatioIsBelowAverageAndUpdateTheAverage(highestCompressionRatio);
        if ( ! this.buffer.isEmpty() && this.buffer.get(0).length > 1 && compressionRatioIsBelowAverage ) {
            splitGroupIfItsTimeSeriesAreNoLongerCorrelated();
        } else if ( ! this.splitSegmentGenerators.isEmpty() && this.emittedFinalizedSegments == this.finalizedSegmentsBeforeNextJoinCheck) {
            this.splitsToJoinIfCorrelated.add(this);
            this.emittedFinalizedSegments = 0;
            this.finalizedSegmentsBeforeNextJoinCheck *= 2;
        }
    }

    private float compressionRatio(ModelType modelType) {
        int modelTypeLength = modelType.length();
        if (modelTypeLength == 0) {
            return Float.NaN;
        }
        long startTime = this.buffer.get(0)[0].timestamp;
        long endTime = this.buffer.get(modelTypeLength - 1)[0].timestamp;
        return modelType.compressionRatio(startTime, endTime, samplingInterval, this.buffer, this.gaps.size());
    }

    private void emitSegment(SegmentFunction stream, ModelType modelType, List<Integer> segmentGaps) {
        int modelTypeLength = modelType.length();
        long startTime = this.buffer.get(0)[0].timestamp;
        long endTime = this.buffer.get(modelTypeLength - 1)[0].timestamp;
        int[] gaps = segmentGaps.stream().mapToInt(l -> l).toArray();
        byte[] model = modelType.getModel(startTime, endTime, samplingInterval, this.buffer);
        stream.emit(this.gid, startTime, endTime, modelType.mtid, model, Static.intToBytes(gaps));
    }

    private boolean checkIfCompressionRatioIsBelowAverageAndUpdateTheAverage(double compressionRatio) {
        boolean isBelowAverage = compressionRatio < this.dynamicSplitFraction * this.compressionRatioAverage;
        this.compressionRatioAverage = (this.compressionRatioAverage * this.emittedFinalizedSegments + compressionRatio) / (this.emittedFinalizedSegments + 1);
        this.emittedFinalizedSegments += 1;
        return isBelowAverage;
    }

    private void splitGroupIfItsTimeSeriesAreNoLongerCorrelated() {
        //If only a subset of the time series in it are currently correlated the group is temporarily split into multiple groups
        DataPoint[] bufferHead = this.buffer.get(0);
        float doubleErrorBound = 2 * this.fallbackModelType.errorBound;
        int lengthOfDataPointsInBuffer = bufferHead.length;
        int[] tsTids = Arrays.stream(this.timeSeriesGroup.getTimeSeries()).mapToInt(ts -> ts.tid).toArray();
        Set<Integer> timeSeriesWithoutGaps = IntStream.range(0, lengthOfDataPointsInBuffer).boxed().collect(Collectors.toSet());

        while ( ! timeSeriesWithoutGaps.isEmpty()) {
            int i = timeSeriesWithoutGaps.iterator().next();
            ArrayList<Integer> bufferSplitIndexes = new ArrayList<>();
            ArrayList<Integer> timeSeriesSplitIndexes = new ArrayList<>();

            for (Integer j : timeSeriesWithoutGaps) {
                //Comparing a time series to itself should always return true
                if (i == j) {
                    bufferSplitIndexes.add(i);
                    timeSeriesSplitIndexes.add(Arrays.binarySearch(tsTids, bufferHead[i].tid));
                    continue;
                }

                //The splitIfNotCorrelated method is only executed if the buffer contains data points
                boolean allDataPointsWithinDoubleErrorBound = lastEmittedModelType.withinErrorBound(doubleErrorBound,
                        this.buffer.stream().map(dps -> dps[i]).iterator(),
                        this.buffer.stream().map(dps -> dps[j]).iterator());

                //Time series should be ingested together if all of their data point are within the double error bound
                if (allDataPointsWithinDoubleErrorBound) {
                    bufferSplitIndexes.add(j);
                    timeSeriesSplitIndexes.add(Arrays.binarySearch(tsTids, bufferHead[j].tid));
                }
            }
            //If the size of the split is the number of the time series not currently in a gap, no split is required
            if (bufferSplitIndexes.size() == lengthOfDataPointsInBuffer) {
                return;
            }

            //Only the time series that currently are not in a gap can be grouped together as they have data points buffered
            bufferSplitIndexes.forEach(timeSeriesWithoutGaps::remove);
            HashSet<Integer> gaps = new HashSet<>(this.tids);
            bufferSplitIndexes.forEach(index -> gaps.remove(this.buffer.get(0)[index].tid));
            int[] bufferSplitIndex = bufferSplitIndexes.stream().mapToInt(k -> k).toArray();
            int[] timeSeriesSplitIndex = timeSeriesSplitIndexes.stream().mapToInt(k -> k).toArray();
            splitSegmentGenerator(bufferSplitIndex, timeSeriesSplitIndex, gaps);
        }

        //If the number of time series with data points in the buffer is smaller than the size of the group, than some
        // of the time series in the group are in a gap and are grouped together as we have no knowledge about them
        if (lengthOfDataPointsInBuffer != this.timeSeriesGroup.getTimeSeries().length) {
            int[] timeSeriesSplitIndex = //If a gap's tid is not in this group it is part of another split
                    this.gaps.stream().mapToInt(tid -> Arrays.binarySearch(tsTids, tid)).filter(k -> k >= 0).toArray();
            Arrays.sort(timeSeriesSplitIndex); //This.gaps is a set so sorting is required
            splitSegmentGenerator(new int[0], timeSeriesSplitIndex, new HashSet<>(this.tids));
        }
        this.buffer.clear();
    }

    private void splitSegmentGenerator(int[] bufferSplitIndex, int[] timeSeriesSplitIndex, Set<Integer> gaps) {
        TimeSeriesGroup tsg = new TimeSeriesGroup(this.timeSeriesGroup, timeSeriesSplitIndex);
        SegmentGenerator sg = new SegmentGenerator(tsg, this.modelTypeInitializer, this.fallbackModelType,
                this.tids, this.maximumLatency, this.dynamicSplitFraction, this.temporarySegmentStream, this.finalizedSegmentStream);
        sg.buffer = copyBuffer(this.buffer, bufferSplitIndex);
        sg.gaps = gaps;
        sg.logger = this.logger;
        int i = 0;
        sg.previousTimeStamps = new long[timeSeriesSplitIndex.length];
        for (int j : timeSeriesSplitIndex) {
            sg.previousTimeStamps[i] = this.previousTimeStamps[j];
            i++;
        }
        sg.resetModelTypeIndex();
        sg.splitSegmentGenerators = this.splitSegmentGenerators;
        sg.splitsToJoinIfCorrelated = this.splitsToJoinIfCorrelated;
        int index = this.splitSegmentGenerators.indexOf(this);
        if (index != -1) {
            this.splitSegmentGenerators.set(index, null);
        }
        this.splitSegmentGenerators.add(sg);

        //As the current temporary segment is shared with the parent SegmentGenerator, a new temporary segment is
        // emitted for each split generator so the temporary segment can be updated separately for each generator
        if (this.maximumLatency > 0) {
            this.dataPointsYetEmitted = 0;
            sg.emitTemporarySegment();
        }
    }

    private ArrayList<DataPoint[]> copyBuffer(ArrayList<DataPoint[]> buffer, int[] bufferSplitIndex) {
        //No data points are buffered for time series currently in a gap
        if (bufferSplitIndex.length == 0) {
            return new ArrayList<>();
        }

        //Copies all data points for the split time series to the new buffer
        ArrayList<DataPoint[]> newBuffer = new ArrayList<>(buffer.size());
        for (DataPoint[] dps : buffer) {
            DataPoint[] newDps = new DataPoint[bufferSplitIndex.length];
            int j = 0;
            for (int i : bufferSplitIndex) {
                newDps[j] = new DataPoint(dps[i].tid, dps[i].timestamp, dps[i].value);
                j++;
            }
            newBuffer.add(newDps);
        }
        return newBuffer;
    }

    private void joinGroupsIfTheirTimeSeriesAreCorrelated() {
        //Assumes that time series which are not correlated would have been split of from the group, so only [0] is checked
        float doubleErrorBound = 2 * this.fallbackModelType.errorBound;
        HashSet<SegmentGenerator> markedForJoining = new HashSet<>();
        ArrayList<SegmentGenerator> joined = new ArrayList<>();
        while ( ! this.splitsToJoinIfCorrelated.isEmpty()) {
            SegmentGenerator sgi = this.splitsToJoinIfCorrelated.iterator().next();
            HashSet<SegmentGenerator> toBeJoined = new HashSet<>();

            //If all data points with a shared time stamp is within the double error bound the groups are joined
            int shortestSharedBufferLength = Integer.MAX_VALUE;
            for (SegmentGenerator sgj : this.splitSegmentGenerators) {
                //Comparing the time series group to itself always return true
                if (sgi == sgj) {
                    toBeJoined.add(sgi);
                    markedForJoining.add(sgi);
                    this.splitsToJoinIfCorrelated.remove(sgi);
                    continue;
                }

                //A time series group cannot be joined with another group more than once
                if (markedForJoining.contains(sgj)) {
                    continue;
                }

                //If no data points are buffered it is not possible to check if the time series should be joined
                int is = sgi.buffer.size();
                int js = sgj.buffer.size();
                boolean canBeJoined = is > 0 && js > 0 &&
                        sgi.buffer.get(is - 1)[0].timestamp == sgj.buffer.get(js - 1)[0].timestamp;

                //The time series are joined if their data points with equal time stamps are within twice the error bound
                canBeJoined &= lastEmittedModelType.withinErrorBound(doubleErrorBound,
                        new ReverseBufferIterator(sgi.buffer, 0), new ReverseBufferIterator(sgj.buffer, 0));

                if (canBeJoined) {
                    int shortestBufferLength = Math.min(sgi.buffer.size(), sgj.buffer.size());
                    shortestSharedBufferLength = Math.min(shortestSharedBufferLength, shortestBufferLength);
                    toBeJoined.add(sgj);
                    markedForJoining.add(sgj);
                    this.splitsToJoinIfCorrelated.remove(sgj);
                }
            }

            //If the join set contains more than one SegmentGenerator they are joined together
            if (toBeJoined.size() > 1) {
                joinSegmentGenerators(toBeJoined, shortestSharedBufferLength, joined);
                //HACK: a SegmentGenerator might add itself to the splitsToJoinIfCorrelated list while being joined
                this.splitsToJoinIfCorrelated.removeAll(toBeJoined);
            }
        }
        this.splitSegmentGenerators.addAll(joined);
    }

    private void joinSegmentGenerators(Set<SegmentGenerator> sgs, int shortestSharedBufferLength,
                                       ArrayList<SegmentGenerator> joined) {
        //The join index is build with the assumption that groups are numerically ordered by tid
        ArrayList<Integer> totalJoinIndexList = new ArrayList<>();
        ArrayList<Integer> activeJoinIndexList = new ArrayList<>();
        for (SegmentGenerator sg : sgs) {
            for (TimeSeries ts : sg.timeSeriesGroup.getTimeSeries()) {
                totalJoinIndexList.add(ts.tid);

                //Segment generators store the tid for all time series it controls currently in a gap
                if ( ! sg.gaps.contains(ts.tid)) {
                    activeJoinIndexList.add(ts.tid);
                }
            }
        }
        Collections.sort(totalJoinIndexList);
        Collections.sort(activeJoinIndexList);
        int[] totalJoinIndex = totalJoinIndexList.stream().mapToInt(i -> i).toArray();
        int[] activeJoinIndex = activeJoinIndexList.stream().mapToInt(i -> i).toArray();

        //Construct a new time series group with all of the time series
        Set<TimeSeriesGroup> tsgs = sgs.stream().map(sg -> sg.timeSeriesGroup).collect(Collectors.toSet());
        TimeSeriesGroup tsg = new TimeSeriesGroup(tsgs, totalJoinIndex);

        //If the original group is recreated the master SegmentGenerator is used, otherwise a new one is created
        SegmentGenerator nsg;
        if (this.tids.size() == tsg.getTimeSeries().length) {
            nsg = this;
            this.timeSeriesGroup = tsg;
        } else {
            nsg = new SegmentGenerator(tsg, this.modelTypeInitializer, this.fallbackModelType, this.tids,
                    this.maximumLatency, this.dynamicSplitFraction, this.temporarySegmentStream, this.finalizedSegmentStream);
            nsg.logger = this.logger;
            nsg.splitSegmentGenerators = this.splitSegmentGenerators;
            nsg.splitsToJoinIfCorrelated = this.splitsToJoinIfCorrelated;
            joined.add(nsg);
        }
        this.splitSegmentGenerators.removeAll(sgs);

        //The overlapping data points are moved to nsg before the old SegmentGenerators are flushed
        for (int next = 1; next <= shortestSharedBufferLength; next++) {
            DataPoint[] result = new DataPoint[activeJoinIndex.length];
            for (SegmentGenerator sg : sgs) {
                DataPoint[] dps = sg.buffer.get(sg.buffer.size() - next);
                for (DataPoint dp : dps) {
                    int write = Arrays.binarySearch(activeJoinIndex, dp.tid);
                    result[write] = dp;
                }
            }
            nsg.buffer.add(result);
        }
        Collections.reverse(nsg.buffer);

        //The remaining data points stored by each SegmentGenerator are flushed
        for (SegmentGenerator sg : sgs) {
            int size = sg.buffer.size();
            sg.buffer.subList(size - nsg.buffer.size(), size).clear();
            sg.modelTypeIndex = 0;
            sg.currentModelType = sg.modelTypes[0];
            sg.currentModelType.initialize(sg.buffer);
            sg.flushBuffer();
            TimeSeries[] tss = sg.timeSeriesGroup.getTimeSeries();
            for (int i = 0; i < tss.length; i++) {
                TimeSeries ts = tss[i];
                int index = Arrays.binarySearch(totalJoinIndex, ts.tid);
                nsg.previousTimeStamps[index] = sg.previousTimeStamps[i];
            }
        }

        //Finally the set of time series currently in a gap and controlled by nsg is computed
        Set<Integer> gaps = new HashSet<>(this.tids);
        Arrays.stream(nsg.buffer.get(0)).forEach(dp -> gaps.remove(dp.tid));
        nsg.gaps = gaps;

        //Initializes the first model with the content in the new combined buffer
        nsg.resetModelTypeIndex();

        //As multiple temporary segments currently represent values for the new combined group, a new temporary segment
        // is emitted so the existing temporary segments can be overwritten by one temporary segment from nsg
        if (this.maximumLatency > 0) {
            nsg.emitTemporarySegment();
        }
    }

    /** Instance Variables **/
    //Variables from the constructor
    private final int gid;
    private final int maximumLatency;
    private final int samplingInterval;
    private final ModelType[] modelTypes;
    private final ModelType fallbackModelType;
    private TimeSeriesGroup timeSeriesGroup;
    private final Supplier<ModelType[]> modelTypeInitializer;
    private final SegmentFunction finalizedSegmentStream;
    private final SegmentFunction temporarySegmentStream;

    //State variables for buffering data points
    private Set<Integer> gaps;
    private ArrayList<DataPoint[]> buffer;
    private long[] previousTimeStamps;

    //State variables for controlling split generators
    private final List<Integer> tids;
    private float dynamicSplitFraction;
    private long emittedFinalizedSegments;
    private double compressionRatioAverage;
    private long finalizedSegmentsBeforeNextJoinCheck;
    private Set<SegmentGenerator> splitsToJoinIfCorrelated;
    private ArrayList<SegmentGenerator> splitSegmentGenerators;

    //State variables for fitting the current model
    private int modelTypeIndex;
    private int dataPointsYetEmitted;
    private ModelType currentModelType;
    private ModelType lastEmittedModelType;

    //DEBUG: logger instance, for counting segments, used for this generator
    Logger logger;
}
