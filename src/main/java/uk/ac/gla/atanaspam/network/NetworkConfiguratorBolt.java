package uk.ac.gla.atanaspam.network;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Created by atanaspam on 06/10/2015.
 */
public class NetworkConfiguratorBolt extends BaseRichBolt {

    private OutputCollector collector;

    int componentId;
    boolean changed;

    public void prepare( Map conf, TopologyContext context, OutputCollector collector )
    {
        this.collector = collector;
        componentId = context.getThisTaskId();
        changed = false;
    }

    public void execute( Tuple tuple )
    {
        int srcComponentId = (Integer) tuple.getValueByField("componentId");
        String anomalyType = (String) tuple.getValueByField("anomalyType");
        String description = (String) tuple.getValueByField("anomalyData");
        System.out.println("Problem with " + anomalyType +" " + description);
        if ((anomalyType == "port")&& changed == false){
            // 1 is the code to add port to list of blocked ports
            int code = 1;
            collector.emit("Configure", new Values(srcComponentId, code, new Integer(description)));
            changed = true;
        }
        collector.ack(tuple);
    }

    public void declareOutputFields( OutputFieldsDeclarer declarer )
    {

        declarer.declareStream("Configure", new Fields("componentId", "code", "setting"));
    }
}
