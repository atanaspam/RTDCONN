package uk.ac.gla.atanaspam.network;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Map;

/**
 * Created by atanaspam on 05/10/2015.
 */
public class NetworkNodeBolt extends BaseRichBolt {

    private OutputCollector collector;

    int componentId;
    long timestamp;
    String srcMAC;
    String destMAC;
    InetAddress srcIP;
    InetAddress destIP;
    int srcPort = -1;
    int destPort = -1;
    boolean[] flags;


    int[] ports;
    int[] portCount;
    ArrayList<InetAddress> blocked;
    boolean[][] badFlags;


    public void prepare( Map conf, TopologyContext context, OutputCollector collector )
    {
        ports = new int[65535];
        portCount = new int[65535];
        blocked = new ArrayList<InetAddress>();
        try {
            blocked.add(InetAddress.getByName("74.125.136.109"));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        badFlags = new boolean[2][2];
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

            } else if ("UDPPackets".equals(tuple.getSourceStreamId())) {
                //System.out.println("UDP1");
                timestamp = (Long) tuple.getValueByField("timestamp");
                srcMAC = (String) tuple.getValueByField("srcMAC");
                destMAC = (String) tuple.getValueByField("destMAC");
                srcIP = (InetAddress) tuple.getValueByField("srcIP");
                destIP = (InetAddress) tuple.getValueByField("destIP");
                srcPort = (Integer) tuple.getValueByField("srcPort");
                destPort = (Integer) tuple.getValueByField("destPort");
            }
            else if ("IPPackets".equals(tuple.getSourceStreamId())) {
                //System.out.println("IPP1");
                timestamp = (Long) tuple.getValueByField("timestamp");
                srcMAC = (String) tuple.getValueByField("srcMAC");
                destMAC = (String) tuple.getValueByField("destMAC");
                srcIP = (InetAddress) tuple.getValueByField("srcIP");
                destIP = (InetAddress) tuple.getValueByField("destIP");

            }
            else if ("Configure".equals(tuple.getSourceStreamId())) {
                int dest = (Integer) tuple.getValueByField("componentId");
                /*
                if (dest !=componentId){
                    collector.ack(tuple);
                    return;
                }*/
                int code = (Integer) tuple.getValueByField("code");
                switch(code){
                    case 1:{
                        int newPort = (Integer) tuple.getValueByField("setting");
                        ports[newPort] = -1;
                        portCount[newPort] = 0;
                        System.out.println("Added port "+ newPort + " to blacklist");
                        System.out.println(ports[newPort] + " " + ports[newPort+1]);
                    }
                }
                collector.ack(tuple);
                return;

            }

        } catch (Exception e) {
            //LOG.error("NetworkNodeBolt error", e);

            collector.reportError(e);
        }

        if (performChecks()){
            collector.emit("Packets", new Values(componentId,timestamp,srcMAC, destMAC,srcIP,destIP,srcPort,destPort, flags));
        }else{
            report("Dropped", srcIP.getHostAddress());
        }
        collector.ack(tuple);
    }

    public void declareOutputFields( OutputFieldsDeclarer declarer )
    {
        declarer.declareStream("Reporting", new Fields("componentId", "anomalyType", "anomalyData"));
        declarer.declareStream("Packets",new Fields("componentId", "timestamp", "srcMAC", "destMAC", "srcIP", "destIP", "srcPort", "destPort", "Flags"));
        //declarer.declare( new Fields("componentId", "timestamp", "srcMAC", "destMAC", "srcIP", "destIP", "srcPort", "destPort", "Flags"));
    }


    private boolean performChecks(){
        boolean status = true;
        /**
         * Perform port checks
         */
        status = status & checkPort(destPort);
        /**
         * perform IP checks
         */
        status = status & checkIP(srcIP);

        /**
         * check flags
         */
        status = status&checkFlags(flags);

        return status;

    }

    private boolean checkPort(int port){
        int x = ports[port];
        if (x != -1){
            portCount[port]++;
            int y = portCount[port];
            if (y == 100){
                report("port", Integer.toString(port));
                portCount[port] = 0;
            }
        } else if (x == -1){
            return false;
        }
        return true;
    }
    private void report(String type, String descr){
        collector.emit("Reporting", new Values(componentId, type, descr));
    }

    private boolean checkIP(InetAddress addr){
        if (blocked.contains(srcIP))
            return false;
        else
            return true;

    }
    private boolean checkFlags(boolean[] flags){
        for (boolean[] sig : badFlags){
            if (flags.equals(sig))
                return false;
        }
        return true;
    }





}
