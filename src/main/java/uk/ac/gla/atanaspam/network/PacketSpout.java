package uk.ac.gla.atanaspam.network;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import uk.ac.gla.atanaspam.pcapj.*;

import java.util.Map;

/**
 * This spout requests packets from the packet generator and passes them to the bolts in the ropology
 * @see NetworkNodeBolt - to see where packets are sent to
 * @see PacketGenerator - to see where packets are actually generated
 * @author atanaspam
 * @created 04/10/2015
 * @version 0.1
 */
public class PacketSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    PacketGenerator p;

    @Override
    public void open( Map conf, TopologyContext context, SpoutOutputCollector collector )
    {
        this.collector = collector;
        p = new PacketGenerator();
    }

    @Override
    public void nextTuple()
    {
        BasicPacket packet = p.getPacket();
        if(packet instanceof TCPPacket){
            TCPPacket packet1 = (TCPPacket) packet;
            /** If the packet is a TCPPacket then emit it to the TCPPacket stream */
            collector.emit("TCPPackets", new Values(packet1.getTimestamp(), packet1.getSourceMacAddress(),
                    packet1.getDestMacAddress(), packet1.getSrc_ip(),
                    packet1.getDst_ip(), packet1.getSrc_port(), packet1.getDst_port(),
                    packet1.getFlags()));
        }
        else if(packet instanceof UDPPacket){
            UDPPacket packet1 = (UDPPacket) packet;
            /** If the packet is a UDPPacket then emit it to the UDPPacket stream */
            collector.emit("UDPPackets", new Values(packet1.getTimestamp(), packet1.getSourceMacAddress(),
                    packet1.getDestMacAddress(), packet1.getSrc_ip(),
                    packet1.getDst_ip(), packet1.getSrc_port(), packet1.getDst_port()));
        }
        else if(packet instanceof IPPacket){
            IPPacket packet1 = (IPPacket) packet;
            /** If the packet is a IPPacket then emit it to the IPPacket stream */
            collector.emit("IPPackets", new Values(packet1.getTimestamp(), packet1.getSourceMacAddress(),
                    packet1.getDestMacAddress(), packet1.getSrc_ip(), packet1.getDst_ip()));
        }
        else
            /** If it is not recognised, dont emit anything */
            return;
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
        declarer.declareStream("IPPackets", new Fields("timestamp", "srcMAC", "destMAC", "srcIP", "destIP" ));
        declarer.declareStream("UDPPackets", new Fields("timestamp", "srcMAC", "destMAC", "srcIP", "destIP", "srcPort", "destPort"));
        declarer.declareStream("TCPPackets", new Fields("timestamp", "srcMAC", "destMAC", "srcIP", "destIP", "srcPort", "destPort", "Flags"));
    }

}
