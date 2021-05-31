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
import dk.aau.modelardb.core.models.ModelTypeFactory;
import dk.aau.modelardb.core.utility.Logger;
import dk.aau.modelardb.core.utility.SegmentFunction;

import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class WorkingSet implements Serializable {

    /** Constructors **/
    public WorkingSet(TimeSeriesGroup[] timeSeriesGroups, float dynamicSplitFraction, String[] models,
                      int[] mtids, float errorBound, int lengthBound, int latency) {
        this.timeSeriesGroups = timeSeriesGroups;
        this.dynamicSplitFraction = (dynamicSplitFraction > 0.0F) ? 1.0F / dynamicSplitFraction : 0.0F;
        this.currentTimeSeriesGroup = 0;
        this.modelTypeNames = models;
        this.mtids = mtids;
        this.errorBound = errorBound;
        this.maximumLatency = latency;
        this.lengthBound = lengthBound;
    }

    /** Public Methods **/
    public void process(SegmentFunction consumeTemporarySegment, SegmentFunction consumeFinalizedSegment,
                        BooleanSupplier haveExecutionBeenTerminated) throws IOException {
        //DEBUG: initializes the timer stored in the logger
        this.logger.getTimeSpan();
        this.consumeTemporarySegment = consumeTemporarySegment;
        this.consumeFinalizedSegment = consumeFinalizedSegment;
        this.haveExecutionBeenTerminated = haveExecutionBeenTerminated;

        processBounded();
        processUnbounded();

        //Ensures all resources are closed
        for (TimeSeriesGroup tsg : this.timeSeriesGroups) {
            tsg.close();
        }
    }

    public String toString() {
        String body = "Working Set [Current Gid: " + this.timeSeriesGroups[this.currentTimeSeriesGroup].gid +
                " | Total TSGs: " + this.timeSeriesGroups.length +
                " | Current TSG: " + (this.currentTimeSeriesGroup + 1) + //Parentheses makes the + add instead of concat
                " | Error Bound: " + this.errorBound +
                " | Length Bound: " + this.lengthBound +
                " | Maximum Latency: " + this.maximumLatency;
        String headerFooter = "=".repeat(body.length());
        return headerFooter + "\n" + body + "\n" + headerFooter;
    }

    public static String toString(WorkingSet[] workingSets) {
        String[] bodies = Arrays.stream(workingSets).map(ws ->
                "Working Set [Current Gid: " + ws.timeSeriesGroups[ws.currentTimeSeriesGroup].gid +
                        " | Total TSGs: " + ws.timeSeriesGroups.length +
                        " | Current TSG: " + (ws.currentTimeSeriesGroup + 1) + //Parentheses makes the + add instead of concat
                        " | Error Bound: " + ws.errorBound +
                        " | Length Bound: " + ws.lengthBound +
                        " | Maximum Latency: " + ws.maximumLatency).toArray(String[]::new);
        int longestLine = Arrays.stream(bodies).mapToInt(String::length).max().getAsInt(); //workingSets is never empty
        String headerFooter = "=".repeat(longestLine);
        StringBuilder body = new StringBuilder();
        for (String line : bodies) {
            body.append(line);
            body.append('\n');
        }
        return headerFooter + "\n" + body + headerFooter;
    }

    /** Private Methods **/
    private void processBounded() {
        while (this.currentTimeSeriesGroup < this.timeSeriesGroups.length &&
                ! this.timeSeriesGroups[this.currentTimeSeriesGroup].isAsync) {
            //Checks if the engine currently ingesting from this working set has been terminated
            if (this.haveExecutionBeenTerminated.getAsBoolean()) {
                return;
            }
            SegmentGenerator sg = getNextSegmentGenerator();
            sg.consumeAllDataPoints();
            sg.close();
            sg.logger.printGeneratorResult(sg.getTimeSeriesGroup());
            this.logger.add(sg.logger);
        }
    }

    private void processUnbounded() throws IOException {
        //There is no work to do if no unbounded time series were in the configuration file or if the engine is terminated
        if (this.currentTimeSeriesGroup == this.timeSeriesGroups.length || this.haveExecutionBeenTerminated.getAsBoolean()) {
            return;
        }
        Selector selector = Selector.open();
        int unboundedChannelsRegistered = 0;

        //The SegmentGenerators are attached to the Selector to make them easy to access when a data point is received
        while (this.currentTimeSeriesGroup < this.timeSeriesGroups.length) {
            TimeSeriesGroup tsg = this.timeSeriesGroups[this.currentTimeSeriesGroup];
            SegmentGenerator sg = getNextSegmentGenerator();
            tsg.attachToSelector(selector, sg);
            unboundedChannelsRegistered++;
        }

        while (true) {
            //Checks if the engine currently ingesting from this working set have been terminated
            if (this.haveExecutionBeenTerminated.getAsBoolean()) {
                selector.close();
                return;
            }

            //Blocks until a channel is ready and verifies that a channel is actually ready
            int readyChannels = selector.select();
            if (readyChannels == 0) {
                continue;
            }

            Iterator<SelectionKey> keyIterator = selector.selectedKeys().iterator();
            while (keyIterator.hasNext()) {
                SelectionKey key = keyIterator.next();
                if ( ! key.isReadable()) {
                    throw new IOException("CORE: non-readable channel selected");
                }

                //Ingests the data points using the segment generator and closes the channels if it no longer provides data
                SegmentGenerator sg = (SegmentGenerator) key.attachment();
                if ( ! sg.consumeAllDataPoints()) {
                    sg.close();
                    sg.logger.printGeneratorResult(sg.getTimeSeriesGroup());
                    this.logger.add(sg.logger);
                    key.cancel();
                    unboundedChannelsRegistered--;
                }

                //The Selector does not remove the SelectionKey instances from the selected key set itself
                keyIterator.remove();

                //If all channels have been closed no more data points will be received
                if (unboundedChannelsRegistered == 0) {
                    selector.close();
                    return;
                }
            }
        }
    }

    private SegmentGenerator getNextSegmentGenerator() {
        int index = this.currentTimeSeriesGroup++;
        TimeSeriesGroup tsg = this.timeSeriesGroups[index];
        tsg.initialize();
        Supplier<ModelType[]> modelTypeInitializer = () -> ModelTypeFactory.getModelTypes(
                this.modelTypeNames, this.mtids,this.errorBound,this.lengthBound);
        ModelType fallbackModelType = ModelTypeFactory.getFallbackModelType(this.errorBound, this.lengthBound);
        List<Integer> tids = null;
        if (this.dynamicSplitFraction != 0.0F) {
            tids = Arrays.stream(tsg.getTimeSeries()).map(ts -> ts.tid).collect(Collectors.toList());
        }
        return new SegmentGenerator(tsg, modelTypeInitializer, fallbackModelType, tids, this.maximumLatency,
                this.dynamicSplitFraction, this.consumeTemporarySegment, this.consumeFinalizedSegment);
    }

    /** Instance Variables **/
    private int currentTimeSeriesGroup;
    private SegmentFunction consumeTemporarySegment;
    private SegmentFunction consumeFinalizedSegment;
    private BooleanSupplier haveExecutionBeenTerminated;

    private final TimeSeriesGroup[] timeSeriesGroups;
    private final float dynamicSplitFraction;
    private final String[] modelTypeNames;
    private final int[] mtids;
    private final float errorBound;
    private final int lengthBound;
    private final int maximumLatency;

    //DEBUG: the logger provides various counters and methods for debugging
    public final Logger logger = new Logger();
}
