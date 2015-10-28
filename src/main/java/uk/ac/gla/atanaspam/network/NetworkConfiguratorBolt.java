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
 * This bolt listens for events from all the other bolts and pushes new configurations to them accordingly
 * @author atanaspam
 * @created 06/10/2015
 * @version 0.1
 */
public class NetworkConfiguratorBolt extends BaseRichBolt {

    private OutputCollector collector;
    int componentId;
    boolean changed; // this is a temp field to restrict the software from blocking all ports (block only the first port)

    public void prepare( Map conf, TopologyContext context, OutputCollector collector )
    {
        this.collector = collector;
        componentId = context.getThisTaskId();
        changed = false;
    }

    public void execute( Tuple tuple )
    {
        /** obtain the message from a bolt */
        int srcComponentId = (Integer) tuple.getValueByField("componentId");
        int anomalyType = (Integer) tuple.getValueByField("anomalyType");
        String description = (String) tuple.getValueByField("anomalyData");
        /** we currently have only one rule set up check for 'abnormal' traffic through a port */
        switch (anomalyType){
            case 1:{
                if (changed == false) {
                    // 1 is the code to add port to list of blocked ports
                    int code = 1;
                    collector.emit("Configure", new Values(srcComponentId, code, new Integer(description)));
                    changed = true;
                }
                System.out.println("Problem with port " + description); // for testing
                break;
            }
            case 2:{
                /**
                 * TODO More rules
                 */
                break;
            }
            case 5:{
                /** 5 means a dropped packet */
                System.out.println("Dropped: " + description); // for testing
                break;
            }
        }
        collector.ack(tuple);
    }

    public void declareOutputFields( OutputFieldsDeclarer declarer )
    {
        declarer.declareStream("Configure", new Fields("componentId", "code", "setting"));
    }
}
