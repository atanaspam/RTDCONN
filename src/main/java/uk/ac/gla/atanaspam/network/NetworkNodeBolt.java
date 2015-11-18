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
    int taskId;

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

    int verbosity;
    boolean timeChecks;
    int packetsProcessed;


    public NetworkNodeBolt(){
        this(DEFAULT_SLIDING_WINDOW_IN_SECONDS, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
    }

    public NetworkNodeBolt(int windowLengthInSeconds, int emitFrequencyInSeconds){
        packetsProcessed = 0;
        verbosity = 3;
        timeChecks = false;
        state = new StateKeeper();

        this.windowLengthInSeconds = windowLengthInSeconds;
        this.emitFrequencyInSeconds = emitFrequencyInSeconds;
        ipCounter = new SlidingWindowCounter<InetAddress>(deriveNumWindowChunksFrom(this.windowLengthInSeconds,
                this.emitFrequencyInSeconds));
        portCounter = new SlidingWindowCounter<Integer>(deriveNumWindowChunksFrom(this.windowLengthInSeconds,
                this.emitFrequencyInSeconds));
    }

    public void prepare( Map conf, TopologyContext context, OutputCollector collector ) {
        this.collector = collector;
        taskId = context.getThisTaskId();
        lastModifiedTracker = new NthLastModifiedTimeTracker(deriveNumWindowChunksFrom(this.windowLengthInSeconds,
                this.emitFrequencyInSeconds));
        System.out.println("Initialized task " + taskId + " from " + taskId + " with verbosity " + verbosity);
    }

    private void emitCurrentWindowCounts() {
        Map<InetAddress, Long> ipCounts = ipCounter.getCountsThenAdvanceWindow();
        Map<Integer, Long> portCounts = portCounter.getCountsThenAdvanceWindow();

        Map<InetAddress, Long> tempIpCount = new HashMap<InetAddress, Long>();
        int actualWindowLengthInSeconds = lastModifiedTracker.secondsSinceOldestModification();
        lastModifiedTracker.markAsModified();
        if (actualWindowLengthInSeconds != windowLengthInSeconds) {
            LOG.log(Level.FINE, String.format(WINDOW_LENGTH_WARNING_TEMPLATE, actualWindowLengthInSeconds, windowLengthInSeconds));
            tempIpCount = ipCounts;
        }else {
            for (Map.Entry<InetAddress, Long> a : ipCounts.entrySet()) {
                try {
                    if (state.getSrcIpHitCount().get(a.getKey()) < a.getValue()) {
                        tempIpCount.put(a.getKey(), a.getValue());
                        System.out.println(taskId + ": " + a.getKey() + " " + state.getSrcIpHitCount().get(a.getKey()) + "-" + a.getValue());
                    }
                } catch (NullPointerException e) {
                    continue;
                }
            }
        }
        state.setSrcIpHitCount(tempIpCount);
        state.setPortHitCount(portCounts);
        for(Map.Entry<InetAddress, Long> a : state.getSrcIpHitCount().entrySet())
            report(3, a.getKey());
        for(Map.Entry<Integer, Long> a : state.getPortHitCount().entrySet())
            report(1, a.getKey());
    }

    public void execute( Tuple tuple ) {
        if (TupleUtils.isTick(tuple)) {
            //LOG.log(Level.INFO, "Received tick tuple, triggering emit of current window counts");
            if (timeChecks)
                emitCurrentWindowCounts();
            collector.ack(tuple);
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
                    int dest = (Integer) tuple.getValueByField("taskId");
                    /** obtain the address and check if you are the intended recipient of the message */
                    if (dest != taskId) {
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
                            System.out.println(taskId + " Chnaged check-verbosity for " + taskId + " to " + newVerb); // for debugging
                            break;
                        }
                        case 11: { // means push new port to blacklist
                            int newPort = (Integer) tuple.getValueByField("setting");
                            state.setBlockedPort(newPort, true);
                            //TODO clear current Port HitCount if necesary
                            System.out.println(taskId + " Added port " + newPort + " to blacklist"); // for debugging
                            break;
                        }
                        case 12: { // means remove a port from blacklist
                            int newPort = (Integer) tuple.getValueByField("setting");
                            state.setBlockedPort(newPort, false);
                            //TODO clear current Port HitCount if necesary
                            System.out.println(taskId + " Removed port " + newPort + " from blacklist"); // for debugging
                            break;
                        }
                        case 13:{ // means add new IP to blacklist
                            InetAddress newAddr = (InetAddress) tuple.getValueByField("setting");
                            state.addBlockedIpAddr(newAddr);
                            System.out.println(taskId + " Added " + newAddr.getHostAddress() + " to blacklist"); // for debugging
                            break;
                        }
                        case 14: { // means remove IP from blacklist
                            InetAddress newAddr = (InetAddress) tuple.getValueByField("setting");
                            state.removeBlockedIpAddr(newAddr);
                            System.out.println(taskId + " Removed " + newAddr.getHostAddress() + " from blacklist"); // for debugging
                            break;
                        }
                        case 15:{ // means add new IP to monitored
                            InetAddress newAddr = (InetAddress) tuple.getValueByField("setting");
                            state.addMonitoredIpAddr(newAddr);
                            System.out.println(taskId + " Added " + newAddr.getHostAddress() + " to monitored"); // for debugging
                            break;
                        }
                        case 16: { // means remove IP from monitored
                            InetAddress newAddr = (InetAddress) tuple.getValueByField("setting");
                            state.removeMonitoredIpAddr(newAddr);
                            System.out.println(taskId + " Removed " + newAddr.getHostAddress() + " from monitored"); // for debugging
                            break;
                        }
                        case 17: { // add new flag to blocked
                            boolean[] newFlags = (boolean[]) tuple.getValueByField("setting");
                            state.addBlockedFlag(newFlags);
                            System.out.println(taskId + " Added new flags to blocked"); // for debugging
                            break;
                        }
                        case 18: { // means remove flag from flags
                            boolean[] newFlags = (boolean[]) tuple.getValueByField("setting");
                            state.removeBlockedFlag(newFlags);
                            System.out.println(taskId + " Removed flags from blocked"); // for debugging
                            break;
                        }
                        case 19: { // set timecheck value
                            boolean timecheck = (boolean) tuple.getValueByField("setting");
                            timeChecks = timecheck;
                            System.out.println(taskId + " Set timeChecks to " + timeChecks); // for debugging
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
                        return;
                    }
                    packetsProcessed++;
                    if (packetsProcessed >= 6000 && !timeChecks){
                        //System.out.println(state);
                        for(Map.Entry<InetAddress, Long> a : state.getSrcIpHitCount().entrySet())
                        report(3, a.getKey());
                        for(Map.Entry<Integer, Long> a : state.getPortHitCount().entrySet())
                            report(1, a.getKey());
                        if (state.getFlagCount()[4] > 1000)
                            report(6,4);
                        if (state.getFlagCount()[5] > 1000)
                            report(6,5);
                        //TODO derive statistics from state
                        packetsProcessed = 0;
                    }
                }

            } catch (Exception e) {
                LOG.log(Level.SEVERE, "An exception occured during bolt "+ taskId + "execution");
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

    public void declareOutputFields( OutputFieldsDeclarer declarer ) {
        declarer.declareStream("Reporting", new Fields("taskId", "anomalyType", "anomalyData"));
        declarer.declareStream("IPPackets", new Fields("timestamp", "srcMAC", "destMAC", "srcIP", "destIP" ));
        declarer.declareStream("UDPPackets", new Fields("timestamp", "srcMAC", "destMAC", "srcIP", "destIP", "srcPort", "destPort"));
        declarer.declareStream("TCPPackets", new Fields("timestamp", "srcMAC", "destMAC", "srcIP", "destIP", "srcPort", "destPort", "Flags"));
    }


    private boolean performChecks(int code,GenericPacket packet){
        if (code == 0)
            return true;
        boolean status = true;

        if (packet.getType().equals("TCP")){
            if (code >0)status = status & checkPort(packet.getDst_port());
            if (code >1)status = status & checkSrcIP(packet.getSrc_ip());
            if (code >2)status = status & checkFlags(packet.getFlags());
        }
        else if (packet.getType().equals("UDP")){
            if (code >0)status = status & checkPort(packet.getDst_port());
            if (code >1)status = status & checkSrcIP(packet.getSrc_ip());
        }
        else if (packet.getType().equals("IPP")){
            if (code >0)status = status & checkSrcIP(packet.getSrc_ip());
        }
        else return false;

        return status;

    }

    private boolean checkPort(int port){
        boolean blocked = state.getBlockedPort(port);
        /** if the entry in ports for this port is true, then this port is blocked => drop */

        if (!blocked && !timeChecks){
            state.incrementPortHitCount(port);
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
    }

    /**
     * This method is invoked in order to report some type of error or event to the Configurator bolt.
     * @param type
     * @param descr
     */
    private void report(int type, Object descr){
        collector.emit("Reporting", new Values(taskId, type, descr));
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
        LOG.log(Level.WARNING, "Recieved a message from "+ tuple.getSourceStreamId());
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
        m.put("ID", taskId);
        m.put("Verbosity", verbosity);
        m.put("TimeChecks", timeChecks);
        return m;
    }
}
