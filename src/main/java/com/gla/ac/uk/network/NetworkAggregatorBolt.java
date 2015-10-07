package com.gla.ac.uk.network;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.ArrayList;
import java.util.Map;

/**
 * Created by atanaspam on 06/10/2015.
 */
public class NetworkAggregatorBolt extends BaseRichBolt {

    private OutputCollector collector;

    public void prepare( Map conf, TopologyContext context, OutputCollector collector )
    {
        this.collector = collector;
    }

    public void execute( Tuple tuple )
    {
        int sourceComponentId = (Integer) tuple.getValueByField("componentId");
        ArrayList <Packet> packetCache = (ArrayList <Packet>) tuple.getValueByField("Array");
        //System.out.println(packetCache + "AAA");
        System.out.println("Got a list of packets form " + sourceComponentId + " || " + packetCache.get(0).getDestination());


    }

    public void declareOutputFields( OutputFieldsDeclarer declarer )
    {
        declarer.declare( new Fields( "Array" ) );
    }
}
