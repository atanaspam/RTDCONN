package uk.ac.gla.atanaspam.network.utils;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.gla.atanaspam.network.StatisticsGatherer;

import java.net.InetAddress;
import java.util.Map;

/**
 * @author atanaspam
 * @version 0.1
 * @created 16/02/2016
 */
public class SlidingWindowCMAStatistics implements StatisticsGatherer {

    private static final Logger LOG = LoggerFactory.getLogger(SlidingWindowCMAStatistics.class);
    private static final int NUM_WINDOW_CHUNKS = 2;
    private static final int DEFAULT_SLIDING_WINDOW_IN_SECONDS = NUM_WINDOW_CHUNKS * 30;
    private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = DEFAULT_SLIDING_WINDOW_IN_SECONDS / NUM_WINDOW_CHUNKS;
    private static final String WINDOW_LENGTH_WARNING_TEMPLATE =
            "Actual window length is %d seconds when it should be %d seconds"
                    + " (you can safely ignore this warning during the startup phase)";

    private SlidingWindowCounter<InetAddress> srcIpHitCount;
    private SlidingWindowCounter<InetAddress> dstIpHitCount;
    private SlidingWindowCounter<Integer> srcPortHitCount;
    private SlidingWindowCounter<Integer> dstPortHitCount;
    private int[] flagCount;
    private HitCountKeeper hitCount;
    private NthLastModifiedTimeTracker lastModifiedTracker;
    private final int windowLengthInSeconds;
    private int taskId;
    private double detectionRatio;
    private final int detectionFloor;


    public SlidingWindowCMAStatistics() {
        srcIpHitCount = new SlidingWindowCounter<>(deriveNumWindowChunksFrom(DEFAULT_SLIDING_WINDOW_IN_SECONDS,
                DEFAULT_EMIT_FREQUENCY_IN_SECONDS));
        dstIpHitCount = new SlidingWindowCounter<>(deriveNumWindowChunksFrom(DEFAULT_SLIDING_WINDOW_IN_SECONDS,
                DEFAULT_EMIT_FREQUENCY_IN_SECONDS));
        srcPortHitCount = new SlidingWindowCounter<>(deriveNumWindowChunksFrom(DEFAULT_SLIDING_WINDOW_IN_SECONDS,
                DEFAULT_EMIT_FREQUENCY_IN_SECONDS));
        dstPortHitCount = new SlidingWindowCounter<>(deriveNumWindowChunksFrom(DEFAULT_SLIDING_WINDOW_IN_SECONDS,
                DEFAULT_EMIT_FREQUENCY_IN_SECONDS));
        flagCount = new int[9];
        hitCount = new HitCountKeeper();
        this.taskId = 0;
        this.detectionRatio = 1.5;
        this.detectionFloor = 2500;
        this.windowLengthInSeconds = DEFAULT_SLIDING_WINDOW_IN_SECONDS;
        lastModifiedTracker = new NthLastModifiedTimeTracker(deriveNumWindowChunksFrom(DEFAULT_SLIDING_WINDOW_IN_SECONDS,
                DEFAULT_EMIT_FREQUENCY_IN_SECONDS));
    }

    @Override
    public void addSrcIpHit(InetAddress addr, int value) {
        srcIpHitCount.incrementCount(addr);
    }

    @Override
    public void addDstIpHit(InetAddress addr, int value) {
        dstIpHitCount.incrementCount(addr);
    }

    @Override
    public void addSrcPortHit(int port, int value) {
        srcPortHitCount.incrementCount(port);
    }

    @Override
    public void addDstPortHit(int port, int value) {
        dstPortHitCount.incrementCount(port);
    }

    @Override
    public void addFlagCount(int flagNum, int value) {
        flagCount[flagNum]+=value;
    }

    @Override
    public void emitCurrentWindowCounts(OutputCollector collector) {
        int actualWindowLengthInSeconds = lastModifiedTracker.secondsSinceOldestModification();
        lastModifiedTracker.markAsModified();
        if (actualWindowLengthInSeconds != windowLengthInSeconds) {
            LOG.warn(String.format(WINDOW_LENGTH_WARNING_TEMPLATE, actualWindowLengthInSeconds, windowLengthInSeconds));
            return;
        }
        for(Map.Entry<InetAddress, Long> a : srcIpHitCount.getCountsThenAdvanceWindow().entrySet()){
            //LOG.info("SRC IP "+ a.getKey()+" " + a.getValue().toString() + " || "+  hitCount.getSrcIpHitCount().get(a.getKey()));
            //LOG.info("SRC IP "+ a.getKey()+" " + a.getValue().toString());
            if (hitCount.addSrcIpHitCount(a.getKey(), a.getValue().intValue())) {
                if (a.getValue() > detectionFloor) {
                    report(3, a.getKey(), collector);
                    LOG.info("Reported " + a.getKey() + " for " + a.getValue() + " hits");
                }
            }
        }

        for(Map.Entry<InetAddress, Long> a : dstIpHitCount.getCountsThenAdvanceWindow().entrySet()){
            //LOG.info("DST IP "+ a.getKey()+" " + a.getValue().toString() + " || "+  hitCount.getSrcIpHitCount().get(a.getKey()));
            //LOG.info("DST IP "+ a.getKey()+" " + a.getValue().toString());
            if (hitCount.addDesIpHitCount(a.getKey(), a.getValue().intValue())) {
                if (a.getValue() > detectionFloor) {
                    report(3, a.getKey(), collector);
                    LOG.info("Reported " + a.getKey() + " for " + a.getValue() + " hits");
                }
            }
        }
//TODO finish ports
//        for(Map.Entry<Integer, Long> a : new HashMap<>(srcPortHitCount.getCountsThenAdvanceWindow()).entrySet()){
//            //LOG.info("DST IP "+ a.getKey()+" " + a.getValue().toString() + " || "+  hitCount.getSrcIpHitCount().get(a.getKey()));
//            LOG.info("DST IP "+ a.getKey()+" " + a.getValue().toString());
//            if (hitCount.addSrcIpHitCount(a.getKey(), a.getValue().intValue())) {
//                if (a.getValue() > detectionRatio) {
//                    report(3, a.getKey(), collector);
//                    LOG.info("Reported " + a.getKey() + " for " + a.getValue() + " hits");
//                }
//            }
//        }




    }

    private int deriveNumWindowChunksFrom(int windowLengthInSeconds, int windowUpdateFrequencyInSeconds) {
        return windowLengthInSeconds / windowUpdateFrequencyInSeconds;
    }

    /**
     * Report some type of error or event to the Configurator bolt.
     * @param type the code representing the event type
     * @param descr the value for the event if applicable
     */
    private void report(int type, Object descr, OutputCollector collector){
        collector.emit("Reporting", new Values(taskId, type, descr));
    }

    public void setTaskId(int taskId) {
        this.taskId = taskId;
    }

    public void setDetectionRatio(double detectionRatio) {
        this.detectionRatio = detectionRatio;
    }
}
