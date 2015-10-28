package uk.ac.gla.atanaspam.network;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Map;

/**
 * Created by atanaspam on 06/10/2015.
 */
public class NetworkAggregatorBolt extends BaseRichBolt {

    private OutputCollector collector;

    int componentId;
    long timestamp;
    String srcMAC;
    String destMAC;
    InetAddress srcIP;
    InetAddress destIP;
    int srcPort;
    int destPort;
    boolean[] flags;

    public void prepare( Map conf, TopologyContext context, OutputCollector collector )
    {
        this.collector = collector;
        componentId = context.getThisTaskId();
    }

    public void execute( Tuple tuple )
    {
        int sourceComponentId = (Integer) tuple.getValueByField("componentId");
        timestamp = (Long) tuple.getValueByField("timestamp");
        srcMAC = (String) tuple.getValueByField("srcMAC");
        destMAC = (String) tuple.getValueByField("destMAC");
        srcIP = (InetAddress) tuple.getValueByField("srcIP");
        destIP = (InetAddress) tuple.getValueByField("destIP");
        srcPort = (Integer) tuple.getValueByField("srcPort");
        destPort = (Integer) tuple.getValueByField("destPort");
        flags = (boolean[]) tuple.getValueByField("Flags");
        //System.out.println(sourceComponentId + "  "+ timestamp + " "+ srcMAC + " "+ destMAC + " "+ srcIP + " "+destIP+ " "+srcPort + " "+destPort+ " "+ Arrays.toString(flags) + " IS SAFE");
        collector.ack(tuple);
    }

    public void declareOutputFields( OutputFieldsDeclarer declarer )
    {

        //declarer.declare( new Fields( "Array" ) );
        declarer.declareStream("Reporting", new Fields("componentID", "anomalyType", "anomalyData"));
    }
}
