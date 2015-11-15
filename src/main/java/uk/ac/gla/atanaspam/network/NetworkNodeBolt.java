package uk.ac.gla.atanaspam.network;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.TupleUtils;
import uk.ac.gla.atanaspam.network.utils.NthLastModifiedTimeTracker;
import uk.ac.gla.atanaspam.network.utils.SlidingWindowCounter;
import uk.ac.gla.atanaspam.network.utils.StateKeeper;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

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

    private static final Logger LOG = Logger.getLogger(NetworkNodeBolt.class.getName());
    private OutputCollector collector;
    int componentId;

    private final SlidingWindowCounter<InetAddress> ipCounter;
    private final SlidingWindowCounter<Integer> portCounter;
    private static final int NUM_WINDOW_CHUNKS = 3;
    private static final int DEFAULT_SLIDING_WINDOW_IN_SECONDS = NUM_WINDOW_CHUNKS * 60;
    private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = DEFAULT_SLIDING_WINDOW_IN_SECONDS / NUM_WINDOW_CHUNKS;
    private static final String WINDOW_LENGTH_WARNING_TEMPLATE =
            "Actual window length is %d seconds when it should be %d seconds"
                    + " (you can safely ignore this warning during the startup phase)";
    private final int windowLengthInSeconds;
    private final int emitFrequencyInSeconds;
    private NthLastModifiedTimeTracker lastModifiedTracker;
    private StateKeeper state;

    GenericPacket packet;
    /** stores the current verbosity level of checks  This can be changed by a Configure message*/
    int verbosity;
    /** Sets wether time based checking is enabled */
    boolean timeChecks;
    int packetsProcessed;


    public NetworkNodeBolt(){
        this(DEFAULT_SLIDING_WINDOW_IN_SECONDS, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
    }

    public NetworkNodeBolt(int windowLengthInSeconds, int emitFrequencyInSeconds){
        packetsProcessed = 0;
        state = new StateKeeper();

        this.windowLengthInSeconds = windowLengthInSeconds;
        this.emitFrequencyInSeconds = emitFrequencyInSeconds;
        ipCounter = new SlidingWindowCounter<InetAddress>(deriveNumWindowChunksFrom(this.windowLengthInSeconds,
                this.emitFrequencyInSeconds));
        portCounter = new SlidingWindowCounter<Integer>(deriveNumWindowChunksFrom(this.windowLengthInSeconds,
                this.emitFrequencyInSeconds));
    }

    //@SuppressWarnings("UsafeCast")
    public void prepare( Map conf, TopologyContext context, OutputCollector collector )
    {
        this.collector = collector;
        componentId = context.getThisTaskId();
        verbosity = 1;
        timeChecks = false;
        lastModifiedTracker = new NthLastModifiedTimeTracker(deriveNumWindowChunksFrom(this.windowLengthInSeconds,
                this.emitFrequencyInSeconds));
        System.out.println("Initialized component " + componentId + " from " + context.getThisComponentId() + " with verbosity " + verbosity);

    }

    private void emitCurrentWindowCounts() {
        Map<InetAddress, Long> ipCounts = ipCounter.getCountsThenAdvanceWindow();
        Map<Integer, Long> portCounts = portCounter.getCountsThenAdvanceWindow();

        Map<InetAddress, Long> tempIpCount = new HashMap<InetAddress, Long>();
        int actualWindowLengthInSeconds = lastModifiedTracker.secondsSinceOldestModification();
        lastModifiedTracker.markAsModified();
        if (actualWindowLengthInSeconds != windowLengthInSeconds) {
            LOG.log(Level.WARNING, String.format(WINDOW_LENGTH_WARNING_TEMPLATE, actualWindowLengthInSeconds, windowLengthInSeconds));
            tempIpCount = ipCounts;
            //System.out.println(tempIpCount);
        }else {
            for (Map.Entry<InetAddress, Long> a : ipCounts.entrySet()) {
                try {
                    if (state.getSrcIpHitCount().get(a.getKey()) < a.getValue()) {
                        tempIpCount.put(a.getKey(), a.getValue());
                        System.out.println(componentId + ": " + a.getKey() + " " + state.getSrcIpHitCount().get(a.getKey()) + "-" + a.getValue());
                    }
                } catch (NullPointerException e) {
                    continue;
                }
            }
            //TODO emit to configurator
            //emit(counts, actualWindowLengthInSeconds);
        }
        state.setSrcIpHitCount(tempIpCount);
        state.setPortHitCount(portCounts);
    }
    /*
    private void emit(Map<Object, Long> counts, int actualWindowLengthInSeconds) {
        for (Map.Entry<Object, Long> entry : counts.entrySet()) {
            Object obj = entry.getKey();
            Long count = entry.getValue();
            collector.emit(new Values(obj, count, actualWindowLengthInSeconds));
        }
    }*/

    public void execute( Tuple tuple )
    {
        if (TupleUtils.isTick(tuple) && timeChecks) {
            //LOG.log(Level.INFO, "Received tick tuple, triggering emit of current window counts");
            emitCurrentWindowCounts();

        }else {
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
                    if (dest != componentId) {
                        collector.ack(tuple);
                        return;
                    }
                    /** get the code for the operation to be performed */
                    int code = (Integer) tuple.getValueByField("code");
                    /** I have currently only implemented code 1 which means add this port to the list of blocked ports */
                    switch (code) {
                        case 10: { // means push new check verbosity
                            int newVerb = (Integer) tuple.getValueByField("setting");
                            verbosity = newVerb;
                            System.out.println(componentId + " Chnaged check-verbosity for " + componentId + " to " + newVerb); // for debugging
                            break;
                        }
                        case 11: { // means push new port to blacklist
                            int newPort = (Integer) tuple.getValueByField("setting");
                            state.setBlockedPort(newPort, true);
                            //TODO clear current Port HitCount if necesary
                            System.out.println(componentId + " Added port " + newPort + " to blacklist"); // for debugging
                            break;
                        }
                        case 12: { // means remove a port from blacklist
                            int newPort = (Integer) tuple.getValueByField("setting");
                            state.setBlockedPort(newPort, false);
                            //TODO clear current Port HitCount if necesary
                            System.out.println(componentId + " Removed port " + newPort + " from blacklist"); // for debugging
                            break;
                        }
                        case 13:{ // means add new IP to blacklist
                            InetAddress newAddr = (InetAddress) tuple.getValueByField("setting");
                            state.addBlockedIpAddr(newAddr);
                            System.out.println(componentId + " Added " + newAddr.getHostAddress() + " to blacklist"); // for debugging
                            break;
                        }
                        case 14: { // means remove IP from blacklist
                            InetAddress newAddr = (InetAddress) tuple.getValueByField("setting");
                            state.removeBlockedIpAddr(newAddr);
                            System.out.println(componentId + " Removed " + newAddr.getHostAddress() + " from blacklist"); // for debugging
                            break;
                        }
                        case 15:{ // means add new IP to monitored
                            InetAddress newAddr = (InetAddress) tuple.getValueByField("setting");
                            state.addMonitoredIpAddr(newAddr);
                            System.out.println(componentId + " Added " + newAddr.getHostAddress() + " to monitored"); // for debugging
                            break;
                        }
                        case 16: { // means remove IP from monitored
                            InetAddress newAddr = (InetAddress) tuple.getValueByField("setting");
                            state.removeMonitoredIpAddr(newAddr);
                            System.out.println(componentId + " Removed " + newAddr.getHostAddress() + " from monitored"); // for debugging
                            break;
                        }
                        case 17: { // add new flag to blocked
                            boolean[] newFlags = (boolean[]) tuple.getValueByField("setting");
                            state.addBlockedFlag(newFlags);
                            System.out.println(componentId + " Added new flags to blocked"); // for debugging
                            break;
                        }
                        case 18: { // means remove flag from flags
                            boolean[] newFlags = (boolean[]) tuple.getValueByField("setting");
                            state.removeBlockedFlag(newFlags);
                            System.out.println(componentId + " Removed flags from blocked"); // for debugging
                            break;
                        }
                        case 19: { // set timecheck value
                            boolean timecheck = (boolean) tuple.getValueByField("setting");
                            timeChecks = timecheck;
                            System.out.println(componentId + " Set timeChecks to " + timeChecks); // for debugging
                            break;
                        }

                    }
                    /** we return here because otherwise the performChecks() method would be invoked on the config data => crash */
                    collector.ack(tuple);
                    return;
                } else {
                    /** Obtain the packet from the spout */
                    packet = obtainPacket(tuple);
                    if (packet == null) {
                        System.out.println("Null packet");
                        return;
                    }
                    packetsProcessed++;
                    if (packetsProcessed >= 6000 && !timeChecks){
                        System.out.println(state);
                        //TODO derive statistics from state
                        packetsProcessed = 0;
                    }
                }

            } catch (Exception e) {
                //LOG.error("NetworkNodeBolt error", e);
                collector.reportError(e);
            }
            /** Invoke performChecks() on each packet that is received
             * if performChecks returns true the packet is allowed to pass, else it is just dropped
             * */
            if (performChecks(verbosity, packet)) {
                send(packet);
            } else {
                report(5, packet.getSrc_ip());
            }
            collector.ack(tuple);
            ipCounter.incrementCount(packet.getSrc_ip());
            portCounter.incrementCount(packet.getDst_port());
        }
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
         * TODO each check should depend on the "code"
         */
        if (code == 0)
            return true;
        boolean status = true;

        if (packet.getType().equals("TCP")){
            /** Perform port checks */
            status = status & checkPort(packet.getDst_port());
            /** perform IP checks */
            status = status & checkSrcIP(packet.getSrc_ip());
            /** check flags */
            status = status & checkFlags(packet.getFlags());
        }
        else if (packet.getType().equals("UDP")){
            /** Perform port checks */
            status = status & checkPort(packet.getDst_port());
            /** perform IP checks */
            status = status & checkSrcIP(packet.getSrc_ip());
        }
        else if (packet.getType().equals("IPP")){
            /** perform IP checks */
            status = status & checkSrcIP(packet.getSrc_ip());
        }
        else return false;

        return status;

    }

    private boolean checkPort(int port){
        boolean blocked = state.getBlockedPort(port);
        /** if the entry in ports for this port is -1, then this port is blocked => drop */

        if (!blocked && !timeChecks){
            state.incrementPortHitCount(port);
            /** This is a very basic rule to simulate the detection of an abnormal amount of traffic through a port
             * if 100 oackets have passed through that port the Configurator bolt is informed of the 'Unusual' traffic
             * @see NetworkConfiguratorBolt to see how it handles that
             *//*
             * int y = portCount[port];
            if (y == 100){
                report(1, port);
                portCount[port] = 0;
            }*/
        } else if (blocked){
            return false;
        }
        return true;
    }

    private boolean checkSrcIP(InetAddress addr){
        if (state.isBlockedIpAddr(addr))
            return false;
        else if(state.isMonitoredIpAddr(addr)){
            report(4, addr);
        }
        state.incrementSrcIpHitCount(addr);
        return true;

    }
    private boolean checkFlags(boolean[] flags){
        if (state.isBadFlag(flags))
            return false;
        else {
            for(int i=0; i<flags.length; i++){
                if (flags[i])
                    state.incrementFlagCount(i);
            }
            return true;
        }
        // TODO add flag to flagcounts
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

    private int deriveNumWindowChunksFrom(int windowLengthInSeconds, int windowUpdateFrequencyInSeconds) {
        return windowLengthInSeconds / windowUpdateFrequencyInSeconds;
    }

    @Override
    public Map<String,Object> getComponentConfiguration(){
        Map<String, Object> m = new HashMap<String, Object>();
        m.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
        m.put("ID", componentId);
        m.put("Verbosity", verbosity);
        m.put("TimeChecks", timeChecks);
        return m;
    }
}
