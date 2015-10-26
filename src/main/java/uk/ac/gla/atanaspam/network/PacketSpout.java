package uk.ac.gla.atanaspam.network;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

import java.util.Map;

/**
 * Created by atanaspam on 04/10/2015.
 */
public class PacketSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;

    @Override
    public void open( Map conf, TopologyContext context, SpoutOutputCollector collector )
    {
        this.collector = collector;

        //idGenerator gen = (idGenerator) conf.get("my.object");
        //setGen(gen);
    }

    @Override
    public void nextTuple()
    {
        // Emit the next packet using the packet Generator
        collector.emit( PacketGenerator.getPacket() );
    }

    @Override
    public void ack(Object id)
    {
    }

    @Override
    public void fail(Object id)
    {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare( new Fields( "id", "source", "destination", "size" ) );
    }

}
