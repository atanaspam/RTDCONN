package uk.ac.gla.atanaspam.network;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import uk.ac.gla.atanaspam.IPacket;
import uk.ac.gla.atanaspam.TCPPacket;

import java.net.InetAddress;
import java.util.Map;

/**
 * Created by atanaspam on 05/10/2015.
 */
public class NetworkNodeBolt extends BaseRichBolt {

    private OutputCollector collector;
    //protected HashMap<String, ArrayList<IPPacket> > packetCache;
    int componentId;
    long timestamp;
    String srcMAC;
    String destMAC;
    InetAddress srcIP;
    InetAddress destIP;
    int srcPort;
    int destPort;
    boolean[] flags;

    public void prepare( Map conf, TopologyContext context, OutputCollector collector )
    {
        this.collector = collector;
        //packetCache = new HashMap<String, ArrayList<IPPacket> >();
        componentId = context.getThisTaskId();
        System.out.println("Initialized component " + componentId);

    }

    public void execute( Tuple tuple )
    {
        //IPacket packet = null;
        try {
            if ("TCPPackets".equals(tuple.getSourceStreamId())) {
                //System.out.println("TCP1");
                timestamp = (Long) tuple.getValueByField("timestamp");
                srcMAC = (String) tuple.getValueByField("srcMAC");
                destMAC = (String) tuple.getValueByField("destMAC");
                srcIP = (InetAddress) tuple.getValueByField("srcIP");
                destIP = (InetAddress) tuple.getValueByField("destIP");
                srcPort = (Integer) tuple.getValueByField("srcPort");
                destPort = (Integer) tuple.getValueByField("destPort");
                flags = (boolean[]) tuple.getValueByField("Flags");
                //System.out.println(timestamp + " "+ srcMAC + " "+ destMAC + " "+ srcIP + " "+destIP+ " "+srcPort + " "+destPort+ " "+flags);
                //packet = new TCPPacket(timestamp, srcMAC, destMAC, srcIP, destIP, srcPort, destPort, flags);
                //System.out.println(packet);
            } else if ("UDPPackets".equals(tuple.getSourceStreamId())) {
                //System.out.println("UDP1");
                timestamp = (Long) tuple.getValueByField("timestamp");
                srcMAC = (String) tuple.getValueByField("srcMAC");
                destMAC = (String) tuple.getValueByField("destMAC");
                srcIP = (InetAddress) tuple.getValueByField("srcIP");
                destIP = (InetAddress) tuple.getValueByField("destIP");
                srcPort = (Integer) tuple.getValueByField("srcPort");
                destPort = (Integer) tuple.getValueByField("destPort");
                //System.out.println(srcMAC);
                //packet = new UDPPacket(timestamp, srcMAC, destMAC, srcIP, destIP, srcPort, destPort);
                //System.out.println(packet);
            }
            else {
                //System.out.println("IPP1");
                timestamp = (Long) tuple.getValueByField("timestamp");
                srcMAC = (String) tuple.getValueByField("srcMAC");
                destMAC = (String) tuple.getValueByField("destMAC");
                srcIP = (InetAddress )tuple.getValueByField("srcIP");
                destIP = (InetAddress )tuple.getValueByField("destIP");
                //System.out.println(srcMAC);
                //packet = new IPPacket(timestamp, srcMAC, destMAC, srcIP, destIP);
                //System.out.println(packet);
            }

            /*
            int id = (Integer) tuple.getValueByField("id");
            String source = (String) tuple.getValueByField("source");
            String destination = (String) tuple.getValueByField("destination");
            int size = (Integer) tuple.getValueByField("size");

            packet = new SamplePacket(id, source, destination, size);
            // do your bolt processing with the bean
            */
        } catch (Exception e) {
            //LOG.error("NetworkNodeBolt error", e);

            collector.reportError(e);
        }
        if (isValid(srcIP)){
            collector.emit(new Values(componentId,timestamp,srcMAC, destMAC,srcIP,destIP,srcPort,destPort, flags));

            /*
            ArrayList<IPPacket> tempArray  = packetCache.get(packet.getDestMacAddress());
            if (tempArray == null) {
                tempArray = new ArrayList<IPPacket>();
                tempArray.add(packet);
                packetCache.put(packet.getDestMacAddress(), tempArray);
            }
            else{
                tempArray.add(packet);
                packetCache.put(packet.getDestMacAddress(), tempArray);
            }
            collector.ack( tuple );

            if (tempArray.size() == 90){
                collector.emit(new Values(componentId, tempArray));
                packetCache.put(packet.getDestMacAddress(), null);
            }
            */
        }
    }

    public void declareOutputFields( OutputFieldsDeclarer declarer )
    {
        //Fields f = new Fields("componentId", "timestamp", "srcMAC", "destMAC", "srcIP", "destIP", "srcPort", "destPort", "Flags");
        //declarer.declare( f );
        //declarer.declareStream("IPPackets", new Fields("timestamp", "srcMAC", "destMAC", "srcIP", "destIP" ));
        //declarer.declareStream("UDPPackets", new Fields("timestamp", "srcMAC", "destMAC", "srcIP", "destIP", "srcPort", "destPort"));
        declarer.declare( new Fields("componentId", "timestamp", "srcMAC", "destMAC", "srcIP", "destIP", "srcPort", "destPort", "Flags"));
    }

    private boolean isValid(InetAddress n){
        if (n.getHostAddress().equals("74.125.136.109")){
            return false;
        }else{
            return true;
        }

    }


}
