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

import dk.aau.modelardb.core.models.ModelFactory;
import dk.aau.modelardb.core.models.Segment;
import dk.aau.modelardb.core.models.Model;
import dk.aau.modelardb.core.utility.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

public class WorkingSet implements Serializable {

    /** Constructors **/
    public WorkingSet(TimeSeries[] timeSeries, String[] models, float error, int latency, int limit) {
        this.timeSeries = timeSeries;
        this.currentTimeSeries = 0;
        this.models = models;
        this.error = error;
        this.latency = latency;
        this.limit = limit;
    }

    /** Public Methods **/
    public void process(Consumer<Segment> consumeTemporary, Consumer<Segment> consumeFinalized,
                        BooleanSupplier haveBeenTerminated) throws IOException {
        //DEBUG: Initializes the timer stored in the logger
        this.logger.getTimeSpan();
        this.consumeTemporary = consumeTemporary;
        this.consumeFinalized = consumeFinalized;
        this.haveBeenTerminated = haveBeenTerminated;

        processBounded();
        processUnbounded();

        //Ensure all channels are closed if something occurred while processing
        for (TimeSeries ts : this.timeSeries) {
            ts.close();
        }
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("==================================================================================================\n");
        sb.append("Working Set [Current SID: ");
        sb.append(this.timeSeries[this.currentTimeSeries].sid);
        sb.append(" | Total TS: ");
        sb.append(this.timeSeries.length);
        sb.append(" | Current TS: ");
        sb.append(this.currentTimeSeries + 1);
        sb.append(" | Error: ");
        sb.append(this.error);
        sb.append(" | Latency: ");
        sb.append(this.latency);
        sb.append(" | Limit: ");
        sb.append(this.limit);
        sb.append("\n==================================================================================================");
        return sb.toString();
    }

    /** Private Methods **/
    private void processBounded() {
        while (this.currentTimeSeries < this.timeSeries.length &&
                this.timeSeries[this.currentTimeSeries].isBounded) {
            //Check if the engine currently using this working set have been terminated
            if (this.haveBeenTerminated.getAsBoolean()) {
                return;
            }

            SegmentGenerator sg = getNextSegmentGenerator();
            sg.consumeDataPoints();
            sg.close();
            sg.logger.printGeneratorResult();
            this.logger.add(sg.logger);
        }
    }

    private void processUnbounded() throws IOException {
        //No unbounded time series sources were included in the configuration file or the engine is terminated
        if (this.currentTimeSeries == this.timeSeries.length || this.haveBeenTerminated.getAsBoolean()) {
            return;
        }
        Selector selector = Selector.open();
        int unboundedChannelsRegistered = 0;

        //The time series are all attached to the selector to allow multiple iterations over the segment generators
        while (this.currentTimeSeries < this.timeSeries.length) {
            TimeSeries ts = this.timeSeries[this.currentTimeSeries];
            SegmentGenerator sg = getNextSegmentGenerator();
            ts.attachToSelector(selector, sg);
            unboundedChannelsRegistered++;
        }

        while (true) {
            //Check if the engine currently using this working set have been terminated
            if (this.haveBeenTerminated.getAsBoolean()) {
                selector.close();
                return;
            }

            //Block until a channel is ready and verify that channels actually were ready
            int readyChannels = selector.select();
            if (readyChannels == 0) {
                continue;
            }

            Iterator<SelectionKey> keyIterator = selector.selectedKeys().iterator();
            while (keyIterator.hasNext()) {
                SelectionKey key = keyIterator.next();
                if ( ! key.isReadable()) {
                    throw new IOException("non-readable channel selected");
                }

                //Process the data points with the segment generator and closes the channel if a punctuation is seen
                SegmentGenerator sg = (SegmentGenerator) key.attachment();
                if ( ! sg.consumeDataPoints()) {
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
        TimeSeries ts = this.timeSeries[index];
        ts.init.run();
        Model[] mgModels = ModelFactory.getModels(this.models, this.error, this.limit);
        Model fallbackModel = ModelFactory.getFallbackModel(this.error, this.limit);
        return new SegmentGenerator(ts, mgModels, fallbackModel,
                this.latency, this.consumeTemporary, this.consumeFinalized);
    }

    /** Instance Variable **/
    private int currentTimeSeries;
    private Consumer<Segment> consumeTemporary;
    private Consumer<Segment> consumeFinalized;
    private BooleanSupplier haveBeenTerminated;

    private final TimeSeries[] timeSeries;
    private final String[] models;
    private final float error;
    private final int latency;
    private final int limit;

    //DEBUG: logger storing various debug counters and methods
    public Logger logger = new Logger();
}