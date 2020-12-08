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
package dk.aau.modelardb.core.utility;

import dk.aau.modelardb.core.DataPoint;
import dk.aau.modelardb.core.models.Model;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

public class Logger implements Serializable {

    /** Constructors **/
    public Logger() {
        //An empty Logger object can be used to aggregate data from multiple Logger objects
    }

    public Logger(int groupSize) {
        this.groupSize = groupSize;
    }

    /** Public Methods **/
    public void add(Logger logger) {
        this.temporaryDataPointCounter += logger.temporaryDataPointCounter;
        this.temporarySegmentCounter += logger.temporarySegmentCounter;

        this.finalizedMetadataSize += logger.finalizedMetadataSize;
        this.finalizedParameterSize += logger.finalizedParameterSize;
        this.finalizedGapSize += logger.finalizedGapSize;

        logger.finalizedSegmentCounter.forEach((k, v) -> this.finalizedSegmentCounter.merge(k, v, Long::sum));
        logger.finalizedDataPointCounter.forEach((k, v) -> this.finalizedDataPointCounter.merge(k, v, Long::sum));
    }

    public String getTimeSpan() {
        long oldTime = this.processingTime;
        this.processingTime = System.currentTimeMillis();
        return java.time.Duration.ofMillis(this.processingTime - oldTime).toString();
    }

    public void pauseAndPrint(DataPoint[] dataPoints) {
        try {
            print(dataPoints);
            System.in.read();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public void sleepAndPrint(DataPoint[] dataPoints, long sleepTime) {
        try {
            print(dataPoints);
            Thread.sleep(sleepTime);
        } catch (InterruptedException ie) {
            throw new RuntimeException(ie);
        }
    }

    public void updateTemporarySegmentCounters(Model temporaryModel, int segmentGapsSize) {
        this.temporaryDataPointCounter += (this.groupSize - segmentGapsSize) * temporaryModel.length();
        this.temporarySegmentCounter += 1;
    }

    public void updateFinalizedSegmentCounters(Model finalizedModel, int segmentGapsSize) {
        //   DPs sid: int, ts: long, v: float
        // model gid: int, start_time: long, end_time: long, mid: int, parameters: bytes[], gaps: byte[]
        //4 + 8 + 4 = 16 * data points is reduced to 4 + 8 + 8 + 4 + sizeof parameters + sizeof gaps
        this.finalizedMetadataSize += 24.0F;
        this.finalizedParameterSize += finalizedModel.unsafeSize();

        String modelType = finalizedModel.getClass().getName();
        long count = this.finalizedSegmentCounter.getOrDefault(modelType, 0L);
        this.finalizedSegmentCounter.put(modelType, count + 1);

        count = this.finalizedDataPointCounter.getOrDefault(modelType, 0L);
        long dataPoints = (this.groupSize  - segmentGapsSize) * finalizedModel.length();
        this.finalizedDataPointCounter.put(modelType, count + dataPoints);

        this.finalizedGapSize += segmentGapsSize * 4;
    }

    public void printGeneratorResult() {
        //Prints the number of points that have been stored as each type of segment for debugging
        System.out.println("\nTemporary Segment Counter - Total: " + this.temporarySegmentCounter);
        System.out.println("Temporary DataPoint Counter - Total: " + this.temporaryDataPointCounter);

        long finalizedCounter = this.finalizedSegmentCounter.values().stream().mapToLong(Long::longValue).sum();
        System.out.println("\nFinalized Segment Counter - Total: " + finalizedCounter);
        for (Map.Entry<String, Long> e : this.finalizedSegmentCounter.entrySet()) {
            System.out.println("-- " + e.getKey() + " | Count: " + e.getValue());
        }

        finalizedCounter = this.finalizedDataPointCounter.values().stream().mapToLong(Long::longValue).sum();
        System.out.println("\nFinalized Segment DataPoint Counter - Total: " + finalizedCounter);
        for (Map.Entry<String, Long> e : this.finalizedDataPointCounter.entrySet()) {
            System.out.println("-- " + e.getKey() + " | DataPoint: " + e.getValue());
        }
        //   DPs sid: int, ts: long, v: float
        // model gid: int, start_time: long, end_time: long, mid: int, parameters: bytes[], gaps: bytes[]
        //4 + 8 + 4 = 16 * data points is reduced to 4 + 8 + 8 + 4 + sizeof parameters + sizeof gaps
        double finalizedTotalSize = this.finalizedMetadataSize + this.finalizedParameterSize + this.finalizedGapSize;

        System.out.println("\nCompression Ratio: " + (16.0 * finalizedCounter) / finalizedTotalSize);
    }

    public void printWorkingSetResult() {
        long dataPointCounter = this.finalizedDataPointCounter.values().stream().mapToLong(Long::longValue).sum();
        long segmentCounter = this.finalizedSegmentCounter.values().stream().mapToLong(Long::longValue).sum();
        int cs = Float.toString(dataPointCounter).length();

        System.out.println("=========================================================");
        System.out.println("Time: " + getTimeSpan());
        System.out.println("Segments: " + segmentCounter);
        System.out.println("Data Points: " + dataPointCounter);
        System.out.println("---------------------------------------------------------");
        printAlignedDebugVariables("Data Points Size", dataPointCounter * 16.0F, cs);
        printAlignedDebugVariables("Metadata Size", this.finalizedMetadataSize, cs);
        printAlignedDebugVariables("Parameters Size", this.finalizedParameterSize, cs);
        printAlignedDebugVariables("Gaps Size", this.finalizedGapSize, cs);
        System.out.println("---------------------------------------------------------");
        printAlignedDebugVariables("Total Size", getTotalSize(), cs);
        System.out.println("=========================================================");
    }

    /** Private Methods **/
    private void printAlignedDebugVariables(String variableName, double sizeInBytes, int cs) {
        System.out.format("%16s: %" + cs + ".0f B | %" + cs + ".3f KB | %" + cs + ".3f MB\n",
                variableName,
                sizeInBytes,
                sizeInBytes / 1024.0F,
                sizeInBytes / 1024.0F / 1024.0F);
    }

    private void print(DataPoint[] dataPoints) {
        for (DataPoint dp : dataPoints) {
            System.out.println(dp);
        }
        System.out.println("------------------------------------------");
    }

    private double getTotalSize() {
        return this.finalizedMetadataSize + this.finalizedParameterSize + this.finalizedGapSize;
    }

    /** Instance Variables **/
    private long processingTime = 0L;
    private int groupSize = 0;
    private long temporaryDataPointCounter = 0L;
    private long temporarySegmentCounter = 0L;
    private float finalizedMetadataSize = 0.0F;
    private float finalizedParameterSize = 0.0F;
    private float finalizedGapSize = 0.0F;

    private final java.util.HashMap<String, Long> finalizedSegmentCounter = new java.util.HashMap<>();
    private final java.util.HashMap<String, Long> finalizedDataPointCounter = new java.util.HashMap<>();
}
