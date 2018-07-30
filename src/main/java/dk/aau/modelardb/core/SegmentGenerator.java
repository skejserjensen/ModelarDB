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

import dk.aau.modelardb.core.models.Segment;
import dk.aau.modelardb.core.models.Model;
import dk.aau.modelardb.core.utility.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

class SegmentGenerator {

    /** Constructors **/
    SegmentGenerator(TimeSeries timeSeries, Model[] models, Model fallbackModel, int latency,
                     Consumer<Segment> temporaryStream, Consumer<Segment> finalizedStream) {

        //Variables from object constructor
        this.sid = timeSeries.sid;
        this.timeSeries = timeSeries;
        this.models = models;
        this.fallbackModel = fallbackModel;
        this.latency = latency;
        this.resolution = timeSeries.resolution;

        this.finalizedStream = finalizedStream;
        this.temporaryStream = temporaryStream;

        //State variables for currentModel generation
        this.modelIndex = 0;
        this.yetEmitted = 0;
        this.currentModel = this.models[0];
        this.buffer = new ArrayList<>();
        this.gaps = new ArrayList<>();

        this.prevTimeStamp = 0;
        this.currentModel.initialize(this.buffer);

        //Debugging
        this.logger = new Logger(sid, timeSeries.location);
    }

    /** Package Methods **/
    boolean consumeDataPoints() {
        while (this.timeSeries.hasNext()) {
            DataPoint curDataPoint = timeSeries.next();
            //DEBUG: adds either a key our five seconds delay to continue
            //this.logger.pauseAndPrint(curDataPoint);
            //this.logger.sleepAndPrint(curDataPoint);

            //A new value have been received, however as it is not a data point the stream must be finished
            if (curDataPoint == null) {
                flushBuffer();
                return false;
            }

            //If the time series are missing values the buffer is flushed or the gap is stored, if needed
            if ((curDataPoint.timestamp - this.prevTimeStamp) > this.resolution && !this.buffer.isEmpty()) {
                flushBuffer();
                //this.gaps.add(this.prevTimeStamp);
                //this.gaps.add(curDataPoint.timestamp);
            }
            this.buffer.add(curDataPoint);
            this.prevTimeStamp = curDataPoint.timestamp;

            //The current model is provided the data point and we verify that the model can represent the current segment,
            //we assume that append will still fail if it failed in the past, so (t,v) must fail if (t-1, v) failed.
            if (this.currentModel.append(curDataPoint)) {

                //Emit a temporary based on data points added since the last emit
                this.yetEmitted++;
                if (this.yetEmitted == this.latency) {
                    emitTemporarySegment();
                }
            } else {
                this.modelIndex += 1;
                if (this.modelIndex == this.models.length) {
                    //If none of the models can represent the current segment, we select the model
                    //that provide the best compression ratio and construct a segment using that model
                    emitFinalSegment();
                    resetModelIndex();
                } else {
                    this.currentModel = models[this.modelIndex];
                    this.currentModel.initialize(this.buffer);
                }
            }
        }
        return true;
    }

    void close() {
        flushBuffer();
        this.timeSeries.close();
    }

    /** Private Methods **/
    private void resetModelIndex() {
        //Restarts the modelling process with the first model attempting to represent the new current segment
        this.modelIndex = 0;
        this.currentModel = models[modelIndex];
        this.currentModel.initialize(this.buffer);
    }

    private void flushBuffer() {
        //First, as we cannot guarantee that all models are initialized we do so now
        for (this.modelIndex += 1; this.modelIndex < this.models.length; this.modelIndex++) {
            models[this.modelIndex].initialize(this.buffer);
        }

        //Until the buffer no longer contain data points we greedily construct segments
        while ( ! buffer.isEmpty()) {
            emitFinalSegment();
            for (Model m : this.models) {
                m.initialize(this.buffer);
            }
        }
        resetModelIndex();
    }

    private void emitTemporarySegment() {
        this.yetEmitted = 0;
        if (this.buffer.isEmpty()) {
            return;
        }

        //If the current model have not received enough data points to represent the segment we fallback to an array
        if (Float.isNaN(this.currentModel.compressionRatio())) {
            this.currentModel = this.fallbackModel;
            this.currentModel.initialize(this.buffer);
        }

        //The list of gaps are cloned to ensure that the values are kept alive
        ArrayList<Long> gaps = new ArrayList<>(this.gaps);

        //A segment represented using the current model is constructed and emitted
        emitSegment(this.temporaryStream, this.currentModel, this.buffer, gaps);

        //DEBUG: all the debug counters can be updated as we have emitted a temporary segment
        this.logger.updateTemporarySegmentCounters(currentModel);
    }

    private void emitFinalSegment() {
        //From the entire list of models the model providing the best compression is selected as mcModel
        Model mcModel = this.models[0];
        for (Model model : this.models) {
            mcModel = (model.compressionRatio() < mcModel.compressionRatio()) ? mcModel : model;
        }

        //If no model have received enough data points to represent the segment we fallback to an array
        if (Float.isNaN(mcModel.compressionRatio())) {
            mcModel = this.fallbackModel;
            mcModel.initialize(this.buffer);
        }

        //The necessary gaps are extracted as a sublist and then cloned to ensure the values are kept alive
        int extractGapIndex = 0;
        long endTime = this.buffer.get(mcModel.length() - 1).timestamp;
        int gapsSize = this.gaps.size();

        for (int i = 0; i < gapsSize; i += 2) {
            if (this.gaps.get(i) > endTime) {
                break;
            }
            extractGapIndex += 2;
        }
        List<Long> segmentGaps = this.gaps.subList(0, extractGapIndex);

        //A segment represented using the model with the highest compression ratio is constructed and emitted
        emitSegment(this.finalizedStream, mcModel, this.buffer, new ArrayList<>(segmentGaps));
        this.buffer.subList(0, mcModel.length()).clear();
        int segmentGapsSize = segmentGaps.size();
        segmentGaps.clear();

        //If the number of data points in the buffer is less then the current number of data points yet to emitted, some of the data
        // points have already been emitted as part of the finalized segment, and we wait until new unseen data points have arrived.
        int crs = buffer.size();
        this.yetEmitted = (this.yetEmitted < crs) ? this.yetEmitted : crs;

        //DEBUG:, all the debug counters can be updated as we have emitted a finalized segment
        this.logger.updateFinalizedSegmentCounters(mcModel, segmentGapsSize);
    }

    private void emitSegment(Consumer<Segment> stream, Model model, List<DataPoint> curSegment, List<Long> localGaps) {
        int modelSegmentLength = model.length();
        long startTime = curSegment.get(0).timestamp;
        long endTime = curSegment.get(modelSegmentLength - 1).timestamp;

        //Checks if either start time or end time can be corrected instead of storing a gap
        if ( ! localGaps.isEmpty()) {
            int startIndex = 0;
            int lastIndex = localGaps.size();
            long gapFirstEnd = localGaps.get(1);
            long gapLastStart = localGaps.get(lastIndex - 2);

            //The first part of the segment is a gap so we throw the gap away
            if (gapFirstEnd == startTime) {
                startTime = gapFirstEnd;
                startIndex += 2;
            }

            //The last part of the segment is a gap so we throw the gap away
            if (gapLastStart == endTime) {
                endTime = gapLastStart;
                lastIndex -= 2;
            }

            localGaps = new ArrayList<>(localGaps.subList(startIndex, lastIndex));
        }

        Segment segment = model.get(this.sid, startTime, endTime, this.resolution, curSegment,
                localGaps.stream().mapToLong(l -> l).toArray());
        stream.accept(segment);
    }

    /** Instance Variables **/
    //Variables from object constructor
    private final int sid;
    private final int latency;
    private final int resolution;
    private final Model[] models;
    private final TimeSeries timeSeries;
    private final Consumer<Segment> finalizedStream;
    private final Consumer<Segment> temporaryStream;
    private final Model fallbackModel;
    private final ArrayList<DataPoint> buffer;
    private final ArrayList<Long> gaps;

    //State variables for fitting the current model
    private int modelIndex;
    private long prevTimeStamp;
    private int yetEmitted;
    private Model currentModel;

    //DEBUG: logger instance, for counting segments, used for this generator
    final Logger logger;
}
