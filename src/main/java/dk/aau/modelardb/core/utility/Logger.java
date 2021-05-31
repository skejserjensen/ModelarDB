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
package dk.aau.modelardb.core.utility;

import dk.aau.modelardb.core.DataPoint;
import dk.aau.modelardb.core.TimeSeriesGroup;
import dk.aau.modelardb.core.models.ModelType;

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
        this.processingTime = Long.max(this.processingTime, logger.processingTime);
        this.temporarySegmentCounter += logger.temporarySegmentCounter;
        this.temporaryDataPointCounter += logger.temporaryDataPointCounter;

        this.finalizedMetadataSize += logger.finalizedMetadataSize;
        this.finalizedModelsSize += logger.finalizedModelsSize;
        this.finalizedGapsSize += logger.finalizedGapsSize;

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

    public void updateTemporarySegmentCounters(ModelType temporaryModelType, int segmentGapsSize) {
        this.temporarySegmentCounter += 1;
        this.temporaryDataPointCounter += (long) (this.groupSize - segmentGapsSize) * temporaryModelType.length();
    }

    public void updateFinalizedSegmentCounters(ModelType finalizedModelType, int segmentGapsSize) {
        //     DPs tid: int, ts: long, v: float
        // Segment gid: int, start_time: long, end_time: long, mtid: int, model: bytes[], gaps: byte[]
        //4 + 8 + 4 = 16 * data points is reduced to 4 + 8 + 8 + 4 + sizeof model + sizeof gaps
        this.finalizedMetadataSize += 24.0F;
        this.finalizedModelsSize += finalizedModelType.unsafeSize();

        String modelType = finalizedModelType.getClass().getName();
        long count = this.finalizedSegmentCounter.getOrDefault(modelType, 0L);
        this.finalizedSegmentCounter.put(modelType, count + 1);

        count = this.finalizedDataPointCounter.getOrDefault(modelType, 0L);
        long dataPoints = (long) (this.groupSize - segmentGapsSize) * finalizedModelType.length();
        this.finalizedDataPointCounter.put(modelType, count + dataPoints);

        this.finalizedGapsSize += segmentGapsSize * 4;
    }

    public void printGeneratorResult(TimeSeriesGroup tsg) {
        StringBuilder sb = new StringBuilder(); //Used by multiple threads so a single print must be used
        //Prints information about the time series
        sb.append("Gid: ").append(tsg.gid).append('\n')
                .append("Tids: ").append(tsg.getTids()).append('\n')
                .append("Sources: ").append(tsg.getSources()).append('\n')
                .append("Ingested: ").append(Static.getIPs()).append('\n');

        //Prints the number of points that have been stored as each type of segment for debugging
        sb.append("\nTemporary Segment Counter - Total: ").append(this.temporarySegmentCounter).append('\n');
        sb.append("Temporary DataPoint Counter - Total: ").append(this.temporaryDataPointCounter).append('\n');

        long finalizedCounter = this.finalizedSegmentCounter.values().stream().mapToLong(Long::longValue).sum();
        sb.append("\nFinalized Segment Counter - Total: ").append(finalizedCounter).append('\n');
        for (Map.Entry<String, Long> e : this.finalizedSegmentCounter.entrySet()) {
            sb.append("-- ").append(e.getKey()).append(" | Count: ").append(e.getValue()).append('\n');
        }

        finalizedCounter = this.finalizedDataPointCounter.values().stream().mapToLong(Long::longValue).sum();
        sb.append("\nFinalized Segment DataPoint Counter - Total: ").append(finalizedCounter).append('\n');
        for (Map.Entry<String, Long> e : this.finalizedDataPointCounter.entrySet()) {
            sb.append("-- ").append(e.getKey()).append(" | DataPoint: ").append(e.getValue()).append('\n');
        }
        //     DPs tid: int, ts: long, v: float
        // Segment gid: int, start_time: long, end_time: long, mtid: int, model: bytes[], gaps: bytes[]
        //4 + 8 + 4 = 16 * data points is reduced to 4 + 8 + 8 + 4 + sizeof model + sizeof gaps
        double finalizedTotalSize = this.finalizedMetadataSize + this.finalizedModelsSize + this.finalizedGapsSize;
        sb.append("\nCompression Ratio: ").append((16.0 * finalizedCounter) / finalizedTotalSize).append('\n');
        sb.append("---------------------------------------------------------");
        System.out.println(sb);
    }

    public void printWorkingSetResult() {
        long finalizedSegmentCounter = this.finalizedSegmentCounter.values().stream().mapToLong(Long::longValue).sum();
        long finalizedPointCounter = this.finalizedDataPointCounter.values().stream().mapToLong(Long::longValue).sum();
        int cs = Float.toString(finalizedPointCounter).length();

        //The header is already printed by printGeneratorResult
        System.out.println("Time: " + getTimeSpan());
        System.out.println("Segments: " + finalizedSegmentCounter);
        System.out.println("Data Points: " + finalizedPointCounter);
        System.out.println("---------------------------------------------------------");
        printAlignedDebugVariables("Data Points Size", finalizedPointCounter * 16.0F, cs);
        printAlignedDebugVariables("Metadata Size", this.finalizedMetadataSize, cs);
        printAlignedDebugVariables("Models Size", this.finalizedModelsSize, cs);
        printAlignedDebugVariables("Gaps Size", this.finalizedGapsSize, cs);
        System.out.println("---------------------------------------------------------");
        printAlignedDebugVariables("Total Size", getTotalSize(), cs);
        System.out.println("---------------------------------------------------------");
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
        return this.finalizedMetadataSize + this.finalizedModelsSize + this.finalizedGapsSize;
    }

    /** Instance Variables **/
    private long processingTime = 0L;
    private int groupSize = 0;
    private long temporarySegmentCounter = 0L;
    private long temporaryDataPointCounter = 0L;
    private float finalizedMetadataSize = 0.0F;
    private float finalizedModelsSize = 0.0F;
    private float finalizedGapsSize = 0.0F;

    private final java.util.HashMap<String, Long> finalizedSegmentCounter = new java.util.HashMap<>();
    private final java.util.HashMap<String, Long> finalizedDataPointCounter = new java.util.HashMap<>();
}
