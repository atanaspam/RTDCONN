package com.gla.ac.uk.network;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by atanaspam on 05/10/2015.
 */
public class NetworkNodeBolt extends BaseRichBolt {

    private OutputCollector collector;
    protected HashMap<String, ArrayList<Packet> > packetCache;
    int componentId;

    public void prepare( Map conf, TopologyContext context, OutputCollector collector )
    {
        this.collector = collector;
        packetCache = new HashMap<String, ArrayList<Packet> >();
        componentId = context.getThisTaskId();
        System.out.println("Initialized component " + componentId);

    }

    public void execute( Tuple tuple )
    {
        Packet packet = null;
        try {
            int id = (Integer) tuple.getValueByField("id");
            String source = (String) tuple.getValueByField("source");
            String destination = (String) tuple.getValueByField("destination");
            int size = (Integer) tuple.getValueByField("size");

            packet = new Packet(id, source, destination, size);
            // do your bolt processing with the bean
        } catch (Exception e) {
            //LOG.error("NetworkNodeBolt error", e);

            collector.reportError(e);
        }
        if (packet != null){
            ArrayList<Packet> tempArray  = packetCache.get(packet.getDestination());
            if (tempArray == null) {
                tempArray = new ArrayList<Packet>();
                tempArray.add(packet);
                packetCache.put(packet.getDestination(), tempArray);
            }
            else{
                tempArray.add(packet);
                packetCache.put(packet.getDestination(), tempArray);
            }
            collector.ack( tuple );

            if (tempArray.size() == 90){
                collector.emit(new Values(componentId, tempArray));
                packetCache.put(packet.getDestination(), null);
            }
        }

    }

    public void declareOutputFields( OutputFieldsDeclarer declarer )
    {
        Fields f = new Fields("componentId", "Array" );
        declarer.declare( f );
        //declarer.declareStream("dest1", f); Not used as Streams are not aplicable
    }


}
