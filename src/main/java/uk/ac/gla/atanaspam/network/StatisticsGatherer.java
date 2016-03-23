package uk.ac.gla.atanaspam.network;

import backtype.storm.task.OutputCollector;

import java.net.InetAddress;

/**
 * Defines the minimum capabilities expected out of each module that performs statistical checks
 * @author atanaspam
 * @version 0.1
 * @created 15/02/2016
 */
public interface StatisticsGatherer {
    int NUM_WINDOW_CHUNKS = 2;
    int DEFAULT_SLIDING_WINDOW_IN_SECONDS = NUM_WINDOW_CHUNKS * 30;
    int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = DEFAULT_SLIDING_WINDOW_IN_SECONDS / NUM_WINDOW_CHUNKS;


    public void addSrcIpHit(InetAddress addr, int value);
    public void addDstIpHit(InetAddress addr, int value);
    public void addSrcPortHit(int port, int value);
    public void addDstPortHit(int port, int value);
    public void addFlagCount(int flagNum, int value);

    public void emitCurrentWindowCounts(OutputCollector collector);

    public void setDetectionRatio(double detectionRatio);
    public void setTaskId(int taskId);


}
