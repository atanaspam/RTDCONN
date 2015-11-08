package uk.ac.gla.atanaspam.network;

import backtype.storm.metric.api.MultiCountMetric;
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
import java.util.HashMap;
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
    int componentId;


    /** those fields store the rules under which the bolt currently operates */
    /** ports is an array of 65535 ints and if ports[portNum] is -1 then traffic through this port is dropped **/
    int[] ports;

    /** portCount is used to store the number of packets for a specific port so that portCount[portNum] will
     * store the number of packets that have passed through this port */
    int[] portCount;

    /** an arraylist of blocked IP addresses */
    ArrayList<InetAddress> blocked;
    ArrayList<InetAddress> monitored;
    /** each boolean[] within badFlags represents a TCP packet'sflags */
    ArrayList<boolean[]> badFlags;

    GenericPacket packet;

    /** stores the current verbosity level of checks  This can be changed by a Configure message*/
    int verbosity;
    /** Used to collect packet count metric */
    //transient MultiCountMetric _genMetric;


    public void prepare( Map conf, TopologyContext context, OutputCollector collector )
    {
        /** initialize rules to default values
         *  TODO this should later be configured to some initial parameters using the Configurator bolt */
        ports = new int[65535];
        portCount = new int[65535];
        blocked = new ArrayList<>();
        monitored = new ArrayList<>();
        try {
            blocked.add(InetAddress.getByName("74.125.136.109"));
            monitored.add(InetAddress.getByName("192.168.1.1"));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        badFlags = new ArrayList<>();

        this.collector = collector;
        componentId = context.getThisTaskId();
        //initMetrics(context);
        if(context.getThisComponentId().equals("node_0_lvl_0")){
            verbosity = 1;
        } else if (context.getThisComponentId().equals("node_0_lvl_1")){
            verbosity= 0;
        }
        System.out.println("Initialized component " + componentId + " from " + context.getThisComponentId() + " with verbosity " + verbosity);

    }

    public void execute( Tuple tuple )
    {
        try {
                /** If data originates from theConfigure stream then it is some sort of new configuration */
            if ("Configure".equals(tuple.getSourceStreamId())) {
                /**
                 * Each Configuration message consists of three fields:
                 * 1. Destination: ID of the bolt that should receive it
                 * 2. Code: the code for the operation that has to be performed
                 * 3. The new setting that has to be applied (this can be any type and depends on the code)
                 */
                int dest = (Integer) tuple.getValueByField("componentId");
                /** obtain the address and check if you are the intended recipient of the message */

                if (dest !=componentId){
                    collector.ack(tuple);
                    return;
                }
                /** get the code for the operation to be performed */
                int code = (Integer) tuple.getValueByField("code");
                /** I have currently only implemented code 1 which means add this port to the list of blocked ports */
                switch(code){
                    case 10:{ // 10 means push new check verbosity
                        int newVerb = (Integer) tuple.getValueByField("setting");
                        verbosity = newVerb;
                        System.out.println("Chnaged check-verbosity for "+ componentId + " to " + newVerb); // for debugging
                    }
                    case 11:{
                        int newPort = (Integer) tuple.getValueByField("setting");
                        ports[newPort] = -1;
                        portCount[newPort] = 0;
                        System.out.println(componentId + " Added port "+ newPort + " to blacklist"); // for debugging
                    }
                }
                /** we return here because otherwise the performChecks() method would be invoked on the config data => crash */
                collector.ack(tuple);
                return;
            }else{
                /** Obtain the packet from the spout */
                packet = obtainPacket(tuple);
                if (packet == null){
                    System.out.println("Null packet");
                    return;
                }
            }

        } catch (Exception e) {
            //LOG.error("NetworkNodeBolt error", e);
            collector.reportError(e);
        }
        /** Invoke performChecks() on each packet that is received
         * if performChecks returns true the packet is allowed to pass, else it is just dropped
         * */
        if (performChecks(verbosity, packet)){
            send(packet);
        }else{
            report(5, packet.getSrc_ip());
        }
        collector.ack(tuple);
        //_genMetric.scope("count").incr();
    }

    public void declareOutputFields( OutputFieldsDeclarer declarer )
    {
        declarer.declareStream("Reporting", new Fields("componentId", "anomalyType", "anomalyData"));

        declarer.declareStream("IPPackets", new Fields("timestamp", "srcMAC", "destMAC", "srcIP", "destIP" ));
        declarer.declareStream("UDPPackets", new Fields("timestamp", "srcMAC", "destMAC", "srcIP", "destIP", "srcPort", "destPort"));
        declarer.declareStream("TCPPackets", new Fields("timestamp", "srcMAC", "destMAC", "srcIP", "destIP", "srcPort", "destPort", "Flags"));
    }


    private boolean performChecks(int code,GenericPacket packet){
        /**
         * TODO the number(severity) of checks preformed should also be configurable by the Configurator
         */
        if (code == 0)
            return true;
        boolean status = true;

        if (packet.getType().equals("TCP")){
            /**
             * Perform port checks
             */
            status = status & checkPort(packet.getDst_port());
            /**
             * perform IP checks
             */
            status = status & checkIP(packet.getSrc_ip());

            /**
             * check flags
             */
            status = status & checkFlags(packet.getFlags());
        }
        else if (packet.getType().equals("UDP")){
            /**
             * Perform port checks
             */
            status = status & checkPort(packet.getDst_port());
            /**
             * perform IP checks
             */
            status = status & checkIP(packet.getSrc_ip());
        }
        else if (packet.getType().equals("IPP")){
            /**
             * perform IP checks
             */
            status = status & checkIP(packet.getSrc_ip());
        }
        else return false;

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
                report(1, port);
                portCount[port] = 0;
            }
        } else if (x == -1){
            return false;
        }
        return true;
    }

    private boolean checkIP(InetAddress addr){
        if (blocked.contains(addr))
            return false;
        else if(monitored.contains(addr)){
            report(4, addr);
        }
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
    private void report(int type, Object descr){
        collector.emit("Reporting", new Values(componentId, type, descr));
    }

    /**
     *
     * @param tuple
     */
    private GenericPacket obtainPacket(Tuple tuple) {
        /** if the packet originates from the TCPPackets Stream its a TCPPacket
         * when its a TCP packet we extract the appropriate fields */
        if ("TCPPackets".equals(tuple.getSourceStreamId())) {
            long timestamp = (Long) tuple.getValueByField("timestamp");
            String srcMAC = (String) tuple.getValueByField("srcMAC");
            String destMAC = (String) tuple.getValueByField("destMAC");
            InetAddress srcIP = (InetAddress) tuple.getValueByField("srcIP");
            InetAddress destIP = (InetAddress) tuple.getValueByField("destIP");
            int srcPort = (Integer) tuple.getValueByField("srcPort");
            int destPort = (Integer) tuple.getValueByField("destPort");
            boolean[] flags = (boolean[]) tuple.getValueByField("Flags");
            return new GenericPacket(timestamp, srcMAC, destMAC, srcIP, destIP, srcPort, destPort, flags);

            /** if the packet originates from the UDPPackets Stream its a UDPPacket */
        } else if ("UDPPackets".equals(tuple.getSourceStreamId())) {
            long timestamp = (Long) tuple.getValueByField("timestamp");
            String srcMAC = (String) tuple.getValueByField("srcMAC");
            String destMAC = (String) tuple.getValueByField("destMAC");
            InetAddress srcIP = (InetAddress) tuple.getValueByField("srcIP");
            InetAddress destIP = (InetAddress) tuple.getValueByField("destIP");
            int srcPort = (Integer) tuple.getValueByField("srcPort");
            int destPort = (Integer) tuple.getValueByField("destPort");
            return new GenericPacket(timestamp, srcMAC, destMAC, srcIP, destIP, srcPort, destPort);

            /** if the packet originates from the IPPackets Stream its a IPPacket */
            /** an IP packet is a packet that is not UDP or TCP and therefore we only extract basic data */
        } else if ("IPPackets".equals(tuple.getSourceStreamId())) {
            long timestamp = (Long) tuple.getValueByField("timestamp");
            String srcMAC = (String) tuple.getValueByField("srcMAC");
            String destMAC = (String) tuple.getValueByField("destMAC");
            InetAddress srcIP = (InetAddress) tuple.getValueByField("srcIP");
            InetAddress destIP = (InetAddress) tuple.getValueByField("destIP");
            return new GenericPacket(timestamp, srcMAC, destMAC, srcIP, destIP);
        }
        System.out.println(tuple.getSourceStreamId());
        return null;
    }
    @Override
    public Map<String,Object> getComponentConfiguration(){
        Map<String, Object> m = new HashMap<String, Object>();
        m.put("ID", componentId);
        m.put("BlockedPorts", ports);
        m.put("PortHits", portCount);
        m.put("BlockedIP", blocked);
        m.put("BadFlags", badFlags);
        m.put("Verbosity", verbosity);
        return m;
    }

    private void send(GenericPacket packet){
        if(packet.getType().equals("TCP")){
            collector.emit("TCPPackets", new Values(packet.getTimestamp(), packet.getSourceMacAddress(),
                    packet.getDestMacAddress(), packet.getSrc_ip(), packet.getDst_ip(), packet.getSrc_port(),
                    packet.getDst_port(), packet.getFlags()));
        }
        else if(packet.getType().equals("UDP")){
            collector.emit("UDPPackets", new Values(packet.getTimestamp(), packet.getSourceMacAddress(),
                    packet.getDestMacAddress(), packet.getSrc_ip(), packet.getDst_ip(), packet.getSrc_port(),
                    packet.getDst_port()));
        }
        else if(packet.getType().equals("IPP")){
            collector.emit("IPPackets", new Values(packet.getTimestamp(), packet.getSourceMacAddress(),
                    packet.getDestMacAddress(), packet.getSrc_ip(), packet.getDst_ip()));
        }
    }
    /*
    void initMetrics(TopologyContext context)
    {
        _genMetric = new MultiCountMetric();

        context.registerMetric("execute_count", _genMetric, 2);
    }
    */
}
