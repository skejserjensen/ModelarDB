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

import dk.aau.modelardb.core.models.Model;
import dk.aau.modelardb.core.models.ModelFactory;
import dk.aau.modelardb.core.utility.Logger;
import dk.aau.modelardb.core.utility.SegmentFunction;
import dk.aau.modelardb.core.utility.Static;

import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.StringJoiner;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class WorkingSet implements Serializable {

    /** Constructors **/
    public WorkingSet(TimeSeriesGroup[] timeSeriesGroups, float dynamicSplitFraction,
                      String[] models, int[] mids, float error, int latency, int limit) {
        this.timeSeriesGroups = timeSeriesGroups;
        this.dynamicSplitFraction = (dynamicSplitFraction > 0.0F) ? 1.0F / dynamicSplitFraction : 0.0F;
        this.currentTimeSeries = 0;
        this.models = models;
        this.mids = mids;
        this.error = error;
        this.latency = latency;
        this.limit = limit;
    }

    /** Public Methods **/
    public void process(SegmentFunction consumeTemporary, SegmentFunction consumeFinalized,
                        BooleanSupplier haveBeenTerminated) throws IOException {
        //DEBUG: initializes the timer stored in the logger
        this.logger.getTimeSpan();
        this.consumeTemporary = consumeTemporary;
        this.consumeFinalized = consumeFinalized;
        this.haveBeenTerminated = haveBeenTerminated;

        processBounded();
        processUnbounded();

        //Ensures all resources are closed
        for (TimeSeriesGroup tsg : this.timeSeriesGroups) {
            tsg.close();
        }
    }

    public String toString() {
        return "====================================================================================================\n" +
                "Working Set [Current GID: " +
                this.timeSeriesGroups[this.currentTimeSeries].gid +
                " | Total TSGs: " +
                this.timeSeriesGroups.length +
                " | Current TSG: " +
                (this.currentTimeSeries + 1) +
                " | Error: " +
                this.error +
                " | Latency: " +
                this.latency +
                " | Limit: " +
                this.limit +
                "\n====================================================================================================";
    }

    /** Private Methods **/
    private void processBounded() {
        while (this.currentTimeSeries < this.timeSeriesGroups.length &&
                this.timeSeriesGroups[this.currentTimeSeries].isBounded) {
            //Checks if the engine currently ingesting from this working set has been terminated
            if (this.haveBeenTerminated.getAsBoolean()) {
                return;
            }

            SegmentGenerator sg = getNextSegmentGenerator();
            sg.consumeAllDataPoints();
            sg.close();
            sg.logger.printGeneratorResult();
            this.logger.add(sg.logger);
        }
    }

    private void processUnbounded() throws IOException {
        //There is no work to do if no unbounded time series were in the configuration file or if the engine is terminated
        if (this.currentTimeSeries == this.timeSeriesGroups.length || this.haveBeenTerminated.getAsBoolean()) {
            return;
        }
        Selector selector = Selector.open();
        int unboundedChannelsRegistered = 0;

        //The SegmentGenerators are attached to the Selector to make them easy to access when a data point is received
        while (this.currentTimeSeries < this.timeSeriesGroups.length) {
            TimeSeriesGroup tsg = this.timeSeriesGroups[this.currentTimeSeries];
            SegmentGenerator sg = getNextSegmentGenerator();
            tsg.attachToSelector(selector, sg);
            unboundedChannelsRegistered++;
        }

        while (true) {
            //Checks if the engine currently ingesting from this working set have been terminated
            if (this.haveBeenTerminated.getAsBoolean()) {
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

                //Processes the data points using the segment generator and closes the channel if an error occur
                SegmentGenerator sg = (SegmentGenerator) key.attachment();
                try {
                    sg.consumeAllDataPoints();
                } catch (RuntimeException re) {
                    sg.close();
                    sg.logger.printGeneratorResult();
                    this.logger.add(sg.logger);
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
        int index = this.currentTimeSeries++;
        TimeSeriesGroup tsg = this.timeSeriesGroups[index];
        tsg.initialize();
        Supplier<Model[]> modelsInitializer = () -> ModelFactory.getModels(models, mids, error, limit);
        Model fallbackModel = ModelFactory.getFallbackModel(this.error, this.limit);
        List<Integer> sids = null;
        if (this.dynamicSplitFraction != 0.0F) {
            sids = Arrays.stream(tsg.getTimeSeries()).map(ts -> ts.sid).collect(Collectors.toList());
        }

        //The source the data is ingested from is printed before ingestion is done to simplify debugging deadlocks
        if (this.currentTimeSeries > 1) {
            System.out.println("---------------------------------------------------------");
        }
        System.out.println("GID: " + tsg.gid);
        System.out.println("SIDs: " + getSids(tsg));
        System.out.println("Source: " + tsg.getSource());
        System.out.println("Ingested: " + Static.getIPs());
        return new SegmentGenerator(tsg, modelsInitializer, fallbackModel, sids, this.latency,
                this.dynamicSplitFraction, this.consumeTemporary, this.consumeFinalized);
    }

    private String getSids(TimeSeriesGroup timeSeriesGroup) {
        StringJoiner sj = new StringJoiner(",", "{", "}");
        for (TimeSeries ts : timeSeriesGroup.getTimeSeries()) {
            sj.add(Integer.toString(ts.sid));
        }
        return sj.toString();
    }

    /** Instance Variables **/
    private int currentTimeSeries;
    private SegmentFunction consumeTemporary;
    private SegmentFunction consumeFinalized;
    private BooleanSupplier haveBeenTerminated;

    private final TimeSeriesGroup[] timeSeriesGroups;
    private final float dynamicSplitFraction;
    private final String[] models;
    private final int[] mids;
    private final float error;
    private final int latency;
    private final int limit;

    //DEBUG: the logger provides various counters and methods for debugging
    public final Logger logger = new Logger();
}
