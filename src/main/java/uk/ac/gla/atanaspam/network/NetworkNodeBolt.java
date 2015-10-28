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
 * This bolt receives packets, performs basic checks on them and either forwards or drops them
 * @see PacketSpout - to see where packets come from
 * @see NetworkConfiguratorBolt - to see where configuration comes from and events are sent
 * @see NetworkAggregatorBolt - to see where non-dropped packets are sent
 * @author atanaspam
 * @created 05/10/2015
 * @version 0.1
 */
public class NetworkNodeBolt extends BaseRichBolt {

    private OutputCollector collector;

    /** store a packet  in order to process it*/
    int componentId;
    long timestamp;
    String srcMAC;
    String destMAC;
    InetAddress srcIP;
    InetAddress destIP;
    int srcPort = -1;
    int destPort = -1;
    boolean[] flags;
    /** End of packet representation */

    /** those fields store the rules under which the bolt currently operates */
    /** ports is an array of 65535 ints and if ports[portNum] is -1 then traffic through this port is dropped **/
    int[] ports;
    /** portCount is used to store the number of packets for a specific port so that portCount[portNum] will
     * store the number of packets that have passed through this port */
    int[] portCount;
    /** an arraylist of blocked IP addresses */
    ArrayList<InetAddress> blocked;
    /** each boolean[] within badFlags represents a TCP packet'sflags */
    boolean[][] badFlags;


    public void prepare( Map conf, TopologyContext context, OutputCollector collector )
    {
        /** initialize rules to default values
         *  TODO this should later be configured to some initial parameters using the Configurator bolt */
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
        componentId = context.getThisTaskId();
        System.out.println("Initialized component " + componentId);

    }

    public void execute( Tuple tuple )
    {
        /** Obtain the packet from the spout */
        try {
            /** if the packet originates from the TCPPackets Stream its a TCPPacket
             * when its a TCP packet we extract the appropriate fields */
            if ("TCPPackets".equals(tuple.getSourceStreamId())) {
                timestamp = (Long) tuple.getValueByField("timestamp");
                srcMAC = (String) tuple.getValueByField("srcMAC");
                destMAC = (String) tuple.getValueByField("destMAC");
                srcIP = (InetAddress) tuple.getValueByField("srcIP");
                destIP = (InetAddress) tuple.getValueByField("destIP");
                srcPort = (Integer) tuple.getValueByField("srcPort");
                destPort = (Integer) tuple.getValueByField("destPort");
                flags = (boolean[]) tuple.getValueByField("Flags");

            /** if the packet originates from the UDPPackets Stream its a UDPPacket */
            } else if ("UDPPackets".equals(tuple.getSourceStreamId())) {
                timestamp = (Long) tuple.getValueByField("timestamp");
                srcMAC = (String) tuple.getValueByField("srcMAC");
                destMAC = (String) tuple.getValueByField("destMAC");
                srcIP = (InetAddress) tuple.getValueByField("srcIP");
                destIP = (InetAddress) tuple.getValueByField("destIP");
                srcPort = (Integer) tuple.getValueByField("srcPort");
                destPort = (Integer) tuple.getValueByField("destPort");

            /** if the packet originates from the IPPackets Stream its a IPPacket */
            /** an IP packet is a packet that is not UDP or TCP and therefore we only extract basic data */
            } else if ("IPPackets".equals(tuple.getSourceStreamId())) {
                timestamp = (Long) tuple.getValueByField("timestamp");
                srcMAC = (String) tuple.getValueByField("srcMAC");
                destMAC = (String) tuple.getValueByField("destMAC");
                srcIP = (InetAddress) tuple.getValueByField("srcIP");
                destIP = (InetAddress) tuple.getValueByField("destIP");

                /** If data originates from theConfigure stream then it is some sort of new configuration */
            } else if ("Configure".equals(tuple.getSourceStreamId())) {
                /**
                 * Each Configuration message consists of three fields:
                 * 1. Destination: ID of the bolt that should receive it
                 * 2. Code: the code for the operation that has to be performed
                 * 3. The new setting that has to be applied (this can be any type and depends on the code)
                 */
                int dest = (Integer) tuple.getValueByField("componentId");
                /** obtain the address and check if you are the intended recipient of the message */
                /*
                if (dest !=componentId){
                    collector.ack(tuple);
                    return;
                }*/
                /** get the code for the operation to be performed */
                int code = (Integer) tuple.getValueByField("code");
                /** I have currently only implemented code 1 which means add this port to the list of blocked ports */
                switch(code){
                    case 1:{
                        int newPort = (Integer) tuple.getValueByField("setting");
                        ports[newPort] = -1;
                        portCount[newPort] = 0;
                        System.out.println("Added port "+ newPort + " to blacklist"); // for debugging
                    }
                }
                /** we return here because otherwise the performChecks() method would be invoked on the config data => crash */
                collector.ack(tuple);
                return;
            }

        } catch (Exception e) {
            //LOG.error("NetworkNodeBolt error", e);
            collector.reportError(e);
        }
        /** Invoke performChecks() on each packet that is received
         * if performChecks returns true the packet is allowed to pass, else it is just dropped
         * */
        if (performChecks()){
            collector.emit("Packets", new Values(componentId,timestamp,srcMAC, destMAC,srcIP,destIP,srcPort,destPort, flags));
        }else{
            report(5, srcIP.getHostAddress());
        }
        collector.ack(tuple);
    }

    public void declareOutputFields( OutputFieldsDeclarer declarer )
    {
        declarer.declareStream("Reporting", new Fields("componentId", "anomalyType", "anomalyData"));
        declarer.declareStream("Packets",new Fields("componentId", "timestamp", "srcMAC", "destMAC", "srcIP", "destIP", "srcPort", "destPort", "Flags"));
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
        status = status & checkFlags(flags);

        return status;

    }


    private boolean checkPort(int port){
        int x = ports[port];
        /** if the entry in ports for this port is -1, then this port is blocked => drop */
        if (x != -1){
            portCount[port]++;
            int y = portCount[port];
            /** This is a very basic rule to simulate the detection of an abnormal amount of traffic through a port
             * if 100 oackets have passed through that port the Configurator bolt is informed of the 'Unusual' traffic
             * @see NetworkConfiguratorBolt to see how it handles that
             */
            if (y == 100){
                report(1, Integer.toString(port));
                portCount[port] = 0;
            }
        } else if (x == -1){
            return false;
        }
        return true;
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

    /**
     * This method is invoked in order to report some type of error or event to the Configurator bolt.
     * @param type
     * @param descr
     */
    private void report(int type, String descr){
        collector.emit("Reporting", new Values(componentId, type, descr));
    }




}
