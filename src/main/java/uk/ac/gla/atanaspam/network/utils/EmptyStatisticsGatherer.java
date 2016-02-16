package uk.ac.gla.atanaspam.network.utils;

import backtype.storm.task.OutputCollector;
import uk.ac.gla.atanaspam.network.StatisticsGatherer;

import java.io.Serializable;
import java.net.InetAddress;

/**
 * @author atanaspam
 * @version 0.1
 * @created 16/02/2016
 */
public class EmptyStatisticsGatherer implements StatisticsGatherer, Serializable {


    @Override
    public void addSrcIpHit(InetAddress addr, int value) {

    }

    @Override
    public void addDstIpHit(InetAddress addr, int value) {

    }

    @Override
    public void addSrcPortHit(int port, int value) {

    }

    @Override
    public void addDstPortHit(int port, int value) {

    }

    @Override
    public void addFlagCount(int flagNum, int value) {

    }

    @Override
    public void emitCurrentWindowCounts(OutputCollector collector) {

    }

    @Override
    public void setDetectionRatio(double detectionRatio) {

    }

    @Override
    public void setTaskId(int taskId) {

    }
}
