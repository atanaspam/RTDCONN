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
import java.net.UnknownHostException;
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
    TCPPacket tcpPacket;
    UDPPacket udpPacket;
    IPPacket ipPacket;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.context = context;
        taskId = context.getThisTaskId();
        String filePath = (String) conf.get("filePath");
        p = new PacketGenerator(filePath, true, false);
        ArrayList<InetAddress> a = new ArrayList<>();
        try {
            a.add(InetAddress.getByName("10.10.1.1"));
        } catch (UnknownHostException e) {
        }
        p.configure(a, new ArrayList<InetAddress>(), new ArrayList<Integer>(),
                new ArrayList<Integer>(), new ArrayList<>(), 5, new ArrayList<>());
        p.setAnomalousTrafficPercentage(20);
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            if ("Configure".equals(tuple.getSourceStreamId())) {
                int dest = (Integer) tuple.getValueByField("taskId");
                /** obtain the address and check if you are the intended recipient of the message */
                if (dest != taskId) {
                    collector.ack(tuple);
                    return;
                }
                int code = (Integer) tuple.getValueByField("code");
                switch (code) {
                    case 30: {
                        int anomaly = (Integer) tuple.getValueByField("setting");
                        LOG.info("Changing anomaly to " + anomaly);
                        p.configure(new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), anomaly, new ArrayList<>());
                        break;
                    }
                    case 31: {
                        int anomaly = (Integer) tuple.getValueByField("setting");
                        LOG.info("Changing anomaly percentage to " + anomaly);
                        p.setAnomalousTrafficPercentage(anomaly);
                        break;
                    }
                    case 32: {
                        LOG.info("NUMBER OF ANOMALOUS PACKETS: " + p.getAnomalousPacketsEmitted());
                        report(8, p.getAnomalousPacketsEmitted());
                        break;
                    }
                }
                collector.ack(tuple);
            } else {
                BasicPacket packet = p.getPacket();
                emitPacket(packet);
                packet = null;
                collector.ack(tuple);
            }
        } catch (Exception e) {
            collector.reportError(e);
        }
    }

    /**
     * Report an event to the Configurator bolt.
     *
     * @param type  the code representing the event type
     * @param descr the value for the event if applicable
     */
    private void report(int type, Object descr) {
        collector.emit("Reporting", new Values(taskId, type, descr));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("Reporting", new Fields("taskId", "anomalyType", "anomalyData"));
        declarer.declareStream("IPPackets", new Fields("timestamp", "srcMAC", "destMAC", "srcIP", "destIP"));
        declarer.declareStream("UDPPackets", new Fields("timestamp", "srcMAC", "destMAC", "srcIP", "destIP", "srcPort", "destPort", "data"));
        declarer.declareStream("TCPPackets", new Fields("timestamp", "srcMAC", "destMAC", "srcIP", "destIP", "srcPort", "destPort", "flags", "data"));
    }

    /**
     * Emits a packet on a stream depending on its type
     *
     * @param packet the Generic Packet instance to be emitted
     */
    private void emitPacket(BasicPacket packet) {
        if (packet instanceof TCPPacket) {
            tcpPacket = (TCPPacket) packet;
            /** If the packet is a TCPPacket then emit it to the TCPPacket stream */
            collector.emit("TCPPackets", new Values(tcpPacket.getTimestamp(), tcpPacket.getSourceMacAddress(),
                    tcpPacket.getDestMacAddress(), tcpPacket.getSrc_ip(),
                    tcpPacket.getDst_ip(), tcpPacket.getSrc_port(), tcpPacket.getDst_port(),
                    tcpPacket.getFlags().toArray(), tcpPacket.getData().getData()));
            tcpPacket = null;
        } else if (packet instanceof UDPPacket) {
            udpPacket = (UDPPacket) packet;
            /** If the packet is a UDPPacket then emit it to the UDPPacket stream */
            collector.emit("UDPPackets", new Values(udpPacket.getTimestamp(), udpPacket.getSourceMacAddress(),
                    udpPacket.getDestMacAddress(), udpPacket.getSrc_ip(),
                    udpPacket.getDst_ip(), udpPacket.getSrc_port(), udpPacket.getDst_port(), udpPacket.getData().getData()));
            udpPacket = null;
        } else if (packet instanceof IPPacket) {
            ipPacket = (IPPacket) packet;
            /** If the packet is a IPPacket then emit it to the IPPacket stream */
            collector.emit("IPPackets", new Values(ipPacket.getTimestamp(), ipPacket.getSourceMacAddress(),
                    ipPacket.getDestMacAddress(), ipPacket.getSrc_ip(), ipPacket.getDst_ip()));
            ipPacket = null;
        } else {
            /** If it is not recognised, dont emit anything */
            LOG.warn("Encountered an unknown packet type");
            return;
        }
    }
}
