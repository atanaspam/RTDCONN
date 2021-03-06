package uk.ac.gla.atanaspam.network;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;
import java.util.UUID;

/**
 * This spout sets the pace for the topology. initially it was emitting packets, but due to the need
 * to control the anomalous traffic levels during runtime this capability was moved to the packetSpoutBolt
 * Packets are actually generated by an external library - pcapj
 * @see PacketSpoutBolt to see where packets are actually emitted into the topology
 * @author atanaspam
 * @created 04/10/2015
 * @version 0.1
 */
public class PacketSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private UUID msgId;
    private long packets;
    private boolean emitLimit;
    private final int emitLimitNum = 22500000;

    @Override
    public void open( Map conf, TopologyContext context, SpoutOutputCollector collector ) {
        this.collector = collector;
        packets = 0;
        emitLimit = (boolean) conf.get("emitLimit");
    }

    @Override
    public void nextTuple() {
        if (packets < emitLimitNum){
            msgId = UUID.randomUUID();
            collector.emit("trigger", new Values(), msgId);
        }else {
            if (!emitLimit){
               packets = 0;
            }else {
                backtype.storm.utils.Utils.sleep(500);
            }
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
