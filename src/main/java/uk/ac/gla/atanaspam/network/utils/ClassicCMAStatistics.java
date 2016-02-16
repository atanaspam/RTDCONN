package uk.ac.gla.atanaspam.network.utils;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.gla.atanaspam.network.StatisticsGatherer;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

/**
 * @author atanaspam
 * @version 0.1
 * @created 16/02/2016
 */
public class ClassicCMAStatistics implements StatisticsGatherer {

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

    @Override
    public void addSrcIpHit(InetAddress addr, int value) {
        if(srcIpHitCount.get(addr) != null){
            srcIpHitCount.put(addr, srcIpHitCount.get(addr)+value);
        } else{
            srcIpHitCount.put(addr, value);
        }
    }

    @Override
    public void addDstIpHit(InetAddress addr, int value) {
        if(dstIpHitCount.get(addr) != null){
            dstIpHitCount.put(addr, dstIpHitCount.get(addr)+value);
        } else{
            dstIpHitCount.put(addr, value);
        }
    }

    @Override
    public void addSrcPortHit(int port, int value) {
        if(srcPortHitCount.get(port) != null){
            srcPortHitCount.put(port, srcPortHitCount.get(port)+value);
        } else{
            srcPortHitCount.put(port, value);
        }
    }

    @Override
    public void addDstPortHit(int port, int value) {
        if(dstPortHitCount.get(port) != null){
            dstPortHitCount.put(port, dstPortHitCount.get(port)+value);
        } else{
            dstPortHitCount.put(port, value);
        }
    }

    @Override
    public void addFlagCount(int flagNum, int value) {
        flagCount[flagNum]+=value;
    }

    @Override
    public void emitCurrentWindowCounts(OutputCollector collector) {

        //TODO optimize this
        for(Map.Entry<InetAddress, Integer> a : srcIpHitCount.entrySet()){
            //LOG.info("SRC IP "+ a.getKey()+" " + a.getValue().toString() + " || "+  hitCount.getSrcIpHitCount().get(a.getKey()));
            //LOG.info("SRC IP "+ a.getKey()+" " + a.getValue().toString());
            if (hitCount.addSrcIpHitCount(a.getKey(), a.getValue())) {
                if (a.getValue() > detectionFloor) {
                    report(3, a.getKey(), collector);
                    LOG.info("Reported " + a.getKey() + " for " + a.getValue() + " hits");
                }
            }
        }
        for(Map.Entry<InetAddress, Integer> a : dstIpHitCount.entrySet()){
            //LOG.info("DST IP "+ a.getKey() +" "+ a.getValue().toString() + " || "+  hitCount.getSrcIpHitCount().get(a.getKey()).toString());
            if (hitCount.addDesIpHitCount(a.getKey(), a.getValue())) {
                if (a.getValue() > detectionFloor) {
                    report(3, a.getKey(), collector);
                        LOG.info("Reported " + a.getKey() + " for " + a.getValue() + " hits");
                }
            }
        }
//
//        for(Map.Entry<Integer, Integer> a : srcPortHitCount.entrySet()) {
//            //LOG.info("DST IP "+ a.getKey() +" "+ a.getValue().toString() + " || "+  hitCount.getSrcIpHitCount().get(a.getKey()).toString());
//            if (hitCount.add(a.getKey(), a.getValue())) {
//                if (a.getValue() > detectionRatio) {
//                    report(3, a.getKey(), collector);
//                    LOG.info("Reported " + a.getKey() + " for " + a.getValue() + " port hits");
//                }
//
//            }
//


//
//        if (state.getFlagCount()[4] > hitCount.getFlagCount()[4]) {
//            report(7, 4);
//            LOG.info("Reported flag 4 for" + state.getFlagCount()[4] + " hits");
//        }
//        if (state.getFlagCount()[5] > hitCount.getFlagCount()[5]) {
//            report(7, 5);
//            LOG.info("Reported flag 5 for " + state.getFlagCount()[4] + " hits");
//        }
        //TODO implement SYN flood detection using StateKeeper flagCount metric

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
