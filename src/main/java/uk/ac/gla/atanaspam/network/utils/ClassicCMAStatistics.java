package uk.ac.gla.atanaspam.network.utils;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.gla.atanaspam.network.StatisticsGatherer;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents a module that implements statistics gathering capability
 * @see StatisticsGatherer
 * @author atanaspam
 * @version 0.1
 * @created 16/02/2016
 */
public class ClassicCMAStatistics implements StatisticsGatherer, Serializable {

    private HashMap<InetAddress, Integer> srcIpHitCount;
    private HashMap<InetAddress, Integer> dstIpHitCount;
    private HashMap<Integer, Integer> srcPortHitCount;
    private HashMap<Integer, Integer> dstPortHitCount;
    private int[] flagCount;
    private HitCountKeeper hitCount;
    private double detectionRatio;
    private int taskId;
    private final int detectionFloor;

    private static final Logger LOG = LoggerFactory.getLogger(ClassicCMAStatistics.class);

    /**
     * Default constructor to initialize datastructures
     */
    public ClassicCMAStatistics() {
        this.taskId = 0;
        this.detectionRatio = 1.5;
        this.detectionFloor = 2500;
        srcIpHitCount = new HashMap<>();
        dstIpHitCount = new HashMap<>();
        srcPortHitCount = new HashMap<>();
        dstPortHitCount = new HashMap<>();
        flagCount = new int[9];
        hitCount = new HitCountKeeper();

    }

    /**
     * Register a Source IP address hit in the packet processed
     * @param addr the source IP address in the packet processed
     * @param value always 1 since only one IP ip address can be targeted by the packet
     */
    @Override
    public void addSrcIpHit(InetAddress addr, int value) {
        if(srcIpHitCount.get(addr) != null){
            srcIpHitCount.put(addr, srcIpHitCount.get(addr)+value);
        } else{
            srcIpHitCount.put(addr, value);
        }
    }

    /**
     * Register a Destination IP address hit in the packet processed
     * @param addr the destination IP address in the packet processed
     * @param value always 1 since only one IP ip address can be targeted by the packet
     */
    @Override
    public void addDstIpHit(InetAddress addr, int value) {
        if(dstIpHitCount.get(addr) != null){
            dstIpHitCount.put(addr, dstIpHitCount.get(addr)+value);
        } else{
            dstIpHitCount.put(addr, value);
        }
    }

    /**
     * Register a Source Port hit in the packet processed
     * @param port the source port for the packet processed
     * @param value always 1 since only one port hit per packet is possible
     */
    @Override
    public void addSrcPortHit(int port, int value) {
        if(srcPortHitCount.get(port) != null){
            srcPortHitCount.put(port, srcPortHitCount.get(port)+value);
        } else{
            srcPortHitCount.put(port, value);
        }
    }

    /**
     * Register a Destination Port hit in the packet processed
     * @param port the destination port for the packet processed
     * @param value always 1 since only one port hit per packet is possible
     */
    @Override
    public void addDstPortHit(int port, int value) {
        if(dstPortHitCount.get(port) != null){
            dstPortHitCount.put(port, dstPortHitCount.get(port)+value);
        } else{
            dstPortHitCount.put(port, value);
        }
    }

    /**
     * Register a flag set within a packet
     * @param flagNum the flag number within the array of TCP flags
     * @param value always 1 since only one flagHit per packet processed is possible
     */
    @Override
    public void addFlagCount(int flagNum, int value) {
        flagCount[flagNum]+=value;
    }

    /**
     * Emits all the relevant data collected during the execution of an iteration
     * @param collector the outputCollector of the bolt that has deployed this module
     */
    @Override
    public void emitCurrentWindowCounts(OutputCollector collector) {

        //TODO optimize this
        for(Map.Entry<InetAddress, Integer> a : srcIpHitCount.entrySet()){
            if (hitCount.addSrcIpHitCount(a.getKey(), a.getValue())) {
                if (a.getValue() > detectionFloor) {
                    report(3, a.getKey(), collector);
//                    LOG.info("Reported " + a.getKey() + " for " + a.getValue() + " hits");
                }
            }
        }
        for(Map.Entry<InetAddress, Integer> a : dstIpHitCount.entrySet()){
            if (hitCount.addDesIpHitCount(a.getKey(), a.getValue())) {
                if (a.getValue() > detectionFloor) {
                    report(3, a.getKey(), collector);
//                        LOG.info("Reported " + a.getKey() + " for " + a.getValue() + " hits");
                }
            }
        }
        clear();
    }

    /**
     * Report some type of error or event to the Configurator bolt.
     * @param type the code representing the event type
     * @param descr the value for the event if applicable
     */
    private void report(int type, Object descr, OutputCollector collector){
        collector.emit("Reporting", new Values(taskId, type, descr));
    }

    private void clear(){
        srcIpHitCount.clear();
        dstIpHitCount.clear();
        srcPortHitCount.clear();
        dstPortHitCount.clear();
        for (int i=0; i<flagCount.length; i++){
            flagCount[i] = 0;
        }
    }

    public void setDetectionRatio(double detectionRatio) {
        this.detectionRatio = detectionRatio;
        hitCount.setDetectionRatio(detectionRatio);
    }

    public void setTaskId(int taskId) {
        this.taskId = taskId;
    }

    public HashMap<InetAddress, Integer> getSrcIpHitCount() {
        return srcIpHitCount;
    }

    public HashMap<InetAddress, Integer> getDstIpHitCount() {
        return dstIpHitCount;
    }

    public HashMap<Integer, Integer> getSrcPortHitCount() {
        return srcPortHitCount;
    }

    public HashMap<Integer, Integer> getDstPortHitCount() {
        return dstPortHitCount;
    }

    public int[] getFlagCount() {
        return flagCount;
    }
}
