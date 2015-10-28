package uk.ac.gla.atanaspam.network;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import uk.ac.gla.atanaspam.*;

import java.util.Map;

/**
 * Created by atanaspam on 04/10/2015.
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
            //System.out.println("TCP");
            collector.emit("TCPPackets", new Values(((TCPPacket) packet).getTimestamp(), ((TCPPacket) packet).getSourceMacAddress(),
                    ((TCPPacket) packet).getDestMacAddress(), ((TCPPacket) packet).getSrc_ip(),
                    ((TCPPacket) packet).getDst_ip(), ((TCPPacket) packet).getSrc_port(), ((TCPPacket) packet).getDst_port(),
                    ((TCPPacket) packet).getFlags()));
        }
        else if(packet instanceof UDPPacket){
            //System.out.println("UDP");
            collector.emit("UDPPackets", new Values(((UDPPacket) packet).getTimestamp(), ((UDPPacket) packet).getSourceMacAddress(),
                    ((UDPPacket) packet).getDestMacAddress(), ((UDPPacket) packet).getSrc_ip(),
                    ((UDPPacket) packet).getDst_ip(), ((UDPPacket) packet).getSrc_port(), ((UDPPacket) packet).getDst_port()));
        }
        else if(packet instanceof IPPacket){
            //System.out.println("IPP");
            collector.emit("IPPackets", new Values(((IPPacket) packet).getTimestamp(), ((IPPacket) packet).getSourceMacAddress(),
                    ((IPPacket) packet).getDestMacAddress(), ((IPPacket) packet).getSrc_ip(),
                    ((IPPacket) packet).getDst_ip()));
        }
        else
            return;
        //collector.emit(p.getPacket() );
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
