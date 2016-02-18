package uk.ac.gla.atanaspam.network;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;
import java.util.UUID;

/**
 * This spout requests packets from the packet generator and passes them to the bolts in the topology
 * Packets are actually generated by an external library - pcapj
 * @see NetworkNodeBolt - to see where packets are sent to
 * @author atanaspam
 * @created 04/10/2015
 * @version 0.1
 */
public class PacketSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private UUID msgId;
    private long packets;

    @Override
    public void open( Map conf, TopologyContext context, SpoutOutputCollector collector ) {
        this.collector = collector;
        packets = 0;
    }

    @Override
    public void nextTuple() {
        if (packets < 2250000){
            msgId = UUID.randomUUID();
            collector.emit("trigger", new Values(), msgId);
        }else {
            backtype.storm.utils.Utils.sleep(500);
        }
        packets++;
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("trigger", new Fields());
    }


}
