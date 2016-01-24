package uk.ac.gla.atanaspam.network;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.gla.atanaspam.pcapj.*;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Map;

/**
 * @author atanaspam
 * @version 0.1
 * @created 12/01/2016
 */
public class PacketSpoutBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(PacketSpoutBolt.class);
    private OutputCollector collector;
    int taskId;
    TopologyContext context;
    PacketGenerator p;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.context = context;
        taskId = context.getThisTaskId();
        String filePath = (String) conf.get("filePath");
        p = new PacketGenerator(filePath, true, false);
        p.configure(new ArrayList<InetAddress>(), new ArrayList<InetAddress>(), new ArrayList<Integer>(),
                new ArrayList<Integer>(), new ArrayList<>(),0);
        p.setAnomalousTrafficPercentage(5);
    }

    @Override
    public void execute(Tuple tuple) {
        if ("Configure".equals(tuple.getSourceStreamId())) {
            int dest = (Integer) tuple.getValueByField("taskId");
            /** obtain the address and check if you are the intended recipient of the message */
            if (dest != taskId) {
                collector.ack(tuple);
                return;
            }
            int code = (Integer) tuple.getValueByField("code");
            switch (code) {
                case 30:{
                    int anomaly = (Integer) tuple.getValueByField("setting");
                    LOG.info("Changing anomaly to "+ anomaly);
//                    p.configure(new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new ArrayList<>(),anomaly);
                }
            }
            //TODO configure packetGenerator
            collector.ack(tuple);
        }else {
            //backtype.storm.utils.Utils.sleep(5);
            BasicPacket packet = p.getPacket();
            emitPacket(packet);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("IPPackets", new Fields("timestamp", "srcMAC", "destMAC", "srcIP", "destIP" ));
        declarer.declareStream("UDPPackets", new Fields("timestamp", "srcMAC", "destMAC", "srcIP", "destIP", "srcPort", "destPort", "data"));
        declarer.declareStream("TCPPackets", new Fields("timestamp", "srcMAC", "destMAC", "srcIP", "destIP", "srcPort", "destPort", "flags", "data"));
    }

    /**
     * Emits a packet on a stream depending on its type
     * @param packet the Generic Packet instance to be emitted
     */
    private void emitPacket(BasicPacket packet){
        if(packet instanceof TCPPacket){
            TCPPacket packet1 = (TCPPacket) packet;
            /** If the packet is a TCPPacket then emit it to the TCPPacket stream */
            collector.emit("TCPPackets", new Values(packet1.getTimestamp(), packet1.getSourceMacAddress(),
                    packet1.getDestMacAddress(), packet1.getSrc_ip(),
                    packet1.getDst_ip(), packet1.getSrc_port(), packet1.getDst_port(),
                    packet1.getFlags().toArray(), packet1.getData().getData()));
        }
        else if(packet instanceof UDPPacket){
            UDPPacket packet1 = (UDPPacket) packet;
            /** If the packet is a UDPPacket then emit it to the UDPPacket stream */
            collector.emit("UDPPackets", new Values(packet1.getTimestamp(), packet1.getSourceMacAddress(),
                    packet1.getDestMacAddress(), packet1.getSrc_ip(),
                    packet1.getDst_ip(), packet1.getSrc_port(), packet1.getDst_port(), packet1.getData().getData()));
        }
        else if(packet instanceof IPPacket){
            IPPacket packet1 = (IPPacket) packet;
            /** If the packet is a IPPacket then emit it to the IPPacket stream */
            collector.emit("IPPackets", new Values(packet1.getTimestamp(), packet1.getSourceMacAddress(),
                    packet1.getDestMacAddress(), packet1.getSrc_ip(), packet1.getDst_ip()));
        }
        else {
            /** If it is not recognised, dont emit anything */
            LOG.warn("Encountered an unknown packet type");
            return;
        }
    }
}
