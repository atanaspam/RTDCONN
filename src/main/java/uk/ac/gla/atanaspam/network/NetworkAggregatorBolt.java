package uk.ac.gla.atanaspam.network;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * This bolt currently represents all the clients within a datacenter topology and
 * therefore accepts all the packets that have managed to pass through the topology
 * currently it only counts all the packets it recieves.
 * @see NetworkNodeBolt - to see where packets are comming from
 * @see NetworkConfiguratorBolt - to see where this bolt sends events (IT DOES NOT DO IT YET)
 * @author atanaspam
 * @created 06/10/2015
 * @version 0.1
 */
public class NetworkAggregatorBolt extends BaseRichBolt {

    private OutputCollector collector;
    int componentId;
    long packetCount;

    public void prepare( Map conf, TopologyContext context, OutputCollector collector )
    {
        this.collector = collector;
        componentId = context.getThisTaskId();
        packetCount = 0;
    }

    public void execute( Tuple tuple )
    {
        packetCount++;
        collector.ack(tuple);
    }

    public void declareOutputFields( OutputFieldsDeclarer declarer )
    {
        declarer.declareStream("Reporting", new Fields("componentID", "anomalyType", "anomalyData"));
    }
    @Override
    public Map<String,Object> getComponentConfiguration(){
        Map<String, Object> m = new HashMap<String, Object>();
        m.put("ID", componentId);
        m.put("packetCount", packetCount);
        return m;
    }
}
