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
package dk.aau.modelardb.core.utility;

import java.io.IOException;
import java.io.Serializable;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.Map;

import dk.aau.modelardb.core.DataPoint;
import dk.aau.modelardb.core.models.Model;

public class Logger implements Serializable {


    /** Constructors **/
    public Logger() {
        //An empty Logger object can be used to aggregate data from multiple Logger objects
    }

    public Logger(int sid, String location) {
        this.sourceID = sid;
        this.sourceLocation = location;
    }

    /** Public Methods **/
    public void add(Logger logger) {
        this.temporaryDataPointCounter += logger.temporaryDataPointCounter;
        this.temporarySegmentCounter += logger.temporarySegmentCounter;

        this.finalizedMetadataSize += logger.finalizedMetadataSize;
        this.finalizedParameterSize += logger.finalizedParameterSize;
        this.finalizedGapSize += logger.finalizedGapSize;

        logger.finalizedSegmentCounter.forEach((k, v) -> {
            this.finalizedSegmentCounter.merge(k, v, (v1, v2) -> v1 + v2);
        });

        logger.finalizedDataPointCounter.forEach((k, v) -> {
            this.finalizedDataPointCounter.merge(k, v, (v1, v2) -> v1 + v2);
        });
    }

    public String getTimeSpan() {
        long oldTime = this.processingTime;
        this.processingTime = System.currentTimeMillis();
        return java.time.Duration.ofMillis(this.processingTime - oldTime).toString();
    }

    public void pauseAndPrint(DataPoint curDataPoint) {
        try {
            System.in.read();
            System.out.println(curDataPoint);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public void sleepAndPrint(DataPoint curDataPoint) {
        try {
            Thread.sleep(5000);
            System.out.println(curDataPoint);
        } catch (InterruptedException ie) {
            throw new RuntimeException(ie);
        }
    }

    public void updateTemporarySegmentCounters(Model temporaryModel) {
        this.temporaryDataPointCounter += temporaryModel.length();
        this.temporarySegmentCounter += 1;
    }

    public void updateFinalizedSegmentCounters(Model finalizedModel, int segmentGapsSize) {
        //   DPs sid: int, ts: long, v: float
        // model sid: int, start_time: long, end_time: long, mid: int, parameters: bytes[], gaps: byte[]
        //4 + 8 + 4 = 16 * data points is reduced to 4 + 8 + 8 + 4 + sizeof parameters + sizeof gaps
        this.finalizedMetadataSize += 24.0F;
        this.finalizedParameterSize += finalizedModel.size();

        String modelType = finalizedModel.getClass().getName();
        int count = this.finalizedSegmentCounter.getOrDefault(modelType, 0);
        this.finalizedSegmentCounter.put(modelType, count + 1);

        count = this.finalizedDataPointCounter.getOrDefault(modelType, 0);
        this.finalizedDataPointCounter.put(modelType, count + finalizedModel.length());

        this.finalizedGapSize += segmentGapsSize * 16;
    }

    public void printGeneratorResult() {
        //Prints the number of points that have been stored as each type of segment for debugging
        System.out.println("SID: " + this.sourceID);
        System.out.println("Ingested: " + getIPs());
        System.out.println("Location: " + this.sourceLocation);

        int finalizedCounter = 0;
        System.out.println("\nTemporary Segment Counter - Total: " + this.temporarySegmentCounter);
        System.out.println("Temporary DataPoint Counter - Total: " + this.temporaryDataPointCounter);

        finalizedCounter = this.finalizedSegmentCounter.values().stream().mapToInt(Integer::intValue).sum();
        System.out.println("\nFinalized Segment Counter - Total: " + finalizedCounter);
        for (Map.Entry<String, Integer> e : this.finalizedSegmentCounter.entrySet()) {
            System.out.println("-- " + e.getKey() + " | Count: " + e.getValue());
        }

        finalizedCounter = this.finalizedDataPointCounter.values().stream().mapToInt(Integer::intValue).sum();
        System.out.println("\nFinalized Segment DataPoint Counter - Total: " + finalizedCounter);
        for (Map.Entry<String, Integer> e : this.finalizedDataPointCounter.entrySet()) {
            System.out.println("-- " + e.getKey() + " | DataPoint: " + e.getValue());
        }
        //   DPs sid: int, ts: long, v: float
        // model sid: int, start_time: long, end_time: long, mid: int, parameters: bytes[], gaps: bytes[]
        //4 + 8 + 4 = 16 * data points is reduced to 4 + 8 + 8 + 4 + sizeof parameters + sizeof gaps
        double finalizedTotalSize = this.finalizedMetadataSize + this.finalizedParameterSize + this.finalizedGapSize;

        System.out.println("\nCompression Ratio: " + (16.0 * finalizedCounter) / finalizedTotalSize);
        System.out.println("---------------------------------------------------------");
    }

    public void printWorkingSetResult() {
        int dataPointCounter = this.finalizedDataPointCounter.values().stream().mapToInt(Integer::intValue).sum();
        int segmentCounter = this.finalizedSegmentCounter.values().stream().mapToInt(Integer::intValue).sum();
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

    private void printAlignedDebugVariables(String variableName, double sizeInBytes, int cs) {
        System.out.format("%16s: %" + cs + ".0f B | %" + cs + ".3f KB | %" + cs + ".3f MB\n",
                variableName,
                sizeInBytes,
                sizeInBytes / 1024.0F,
                sizeInBytes / 1024.0F / 1024.0F);
    }

    /** Private Methods **/
    private String getIPs() {
        StringBuilder result = new StringBuilder();
        try {
            //Iterates through all non loopback interfaces and extracts the IP addresses
            Enumeration<NetworkInterface> nss = NetworkInterface.getNetworkInterfaces();
            while (nss.hasMoreElements()) {
                java.net.NetworkInterface ns = nss.nextElement();
                if (ns.isLoopback() || !ns.isUp())
                    continue;

                //Iterate through all IP addresses and extracts all IPv4 addresses
                Enumeration<InetAddress> ips = ns.getInetAddresses();
                while (ips.hasMoreElements()) {
                    java.net.InetAddress ip = ips.nextElement();
                    if (ip instanceof Inet6Address)
                        continue;

                    if (result.length() == 0) {
                        result.append(ip.getHostAddress());
                    } else {
                        result.append(",");
                        result.append(ip.getHostAddress());
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return result.toString();
    }

    private double getTotalSize() {
        return this.finalizedMetadataSize + this.finalizedParameterSize + this.finalizedGapSize;
    }

    /** Instance Variables **/
    private long processingTime = 0L;
    private int sourceID = -1;
    private String sourceLocation = "";
    private int temporaryDataPointCounter = 0;
    private int temporarySegmentCounter = 0;
    private float finalizedMetadataSize = 0.0F;
    private float finalizedParameterSize = 0.0F;
    private float finalizedGapSize = 0.0F;

    private final java.util.HashMap<String, Integer> finalizedSegmentCounter = new java.util.HashMap<>();
    private final java.util.HashMap<String, Integer> finalizedDataPointCounter = new java.util.HashMap<>();
}
