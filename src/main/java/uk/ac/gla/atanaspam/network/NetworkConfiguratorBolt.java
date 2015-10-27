package uk.ac.gla.atanaspam.network;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import java.util.Map;

/**
 * Created by atanaspam on 06/10/2015.
 */
public class NetworkConfiguratorBolt extends BaseRichBolt {

    private OutputCollector collector;

    int componentId;

    public void prepare( Map conf, TopologyContext context, OutputCollector collector )
    {
        this.collector = collector;
        componentId = context.getThisTaskId();
    }

    public void execute( Tuple tuple )
    {
        int srcComponentId = (Integer) tuple.getValueByField("componentId");
        String anomalyType = (String) tuple.getValueByField("anomalyType");
        String description = (String) tuple.getValueByField("anomalyData");
        System.out.println("Problem with " + anomalyType);
    }

    public void declareOutputFields( OutputFieldsDeclarer declarer )
    {
        declarer.declare( new Fields( "Array" ) );
    }
}
