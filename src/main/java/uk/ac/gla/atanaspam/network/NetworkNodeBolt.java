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
import org.apache.storm.shade.org.apache.zookeeper.data.Stat;
import uk.ac.gla.atanaspam.network.utils.HitCountKeeper;
import uk.ac.gla.atanaspam.network.utils.NthLastModifiedTimeTracker;
import uk.ac.gla.atanaspam.network.utils.SlidingWindowCounter;
import uk.ac.gla.atanaspam.network.utils.StateKeeper;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.gla.atanaspam.pcapj.PacketContents;
import uk.ac.gla.atanaspam.pcapj.TCPFlags;


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

    private static final Logger LOG = LoggerFactory.getLogger(NetworkNodeBolt.class);

    private OutputCollector collector;
    int taskId;

    private final SlidingWindowCounter<InetAddress> srcIpCounter;
    private final SlidingWindowCounter<InetAddress> destIpCounter;
    private final SlidingWindowCounter<Integer> destPortCounter;
    private static final int NUM_WINDOW_CHUNKS = 2;
    private static final int DEFAULT_SLIDING_WINDOW_IN_SECONDS = NUM_WINDOW_CHUNKS * 30;
    private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = DEFAULT_SLIDING_WINDOW_IN_SECONDS / NUM_WINDOW_CHUNKS;
    private static final String WINDOW_LENGTH_WARNING_TEMPLATE =
            "Actual window length is %d seconds when it should be %d seconds"
                    + " (you can safely ignore this warning during the startup phase)";
    private final int windowLengthInSeconds;
    private final int emitFrequencyInSeconds;
    private NthLastModifiedTimeTracker lastModifiedTracker;
    private StateKeeper state;
    private HitCountKeeper hitCount;
    GenericPacket packet;
    /**
     * Check verbosity indicates the level of checks to be performed
     * 0 - do nothing, 1 - check ports, 2 - check IP's, 3 - check Flags
     */
    private int verbosity;
    private boolean gatherStatistics;
    private boolean timeChecks;
    private int packetsProcessed;
    private int detectionRatio;

    /**
     * Initializes a NetworkNodeBolt with a specific state
     * Used for testing.
     */
    public NetworkNodeBolt(StateKeeper state, boolean gatherStatistics, int verbosity, int packetsProcessed){
        this(DEFAULT_SLIDING_WINDOW_IN_SECONDS, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
        this.state = state;
        this.gatherStatistics = gatherStatistics;
        this.verbosity = verbosity;
        this.packetsProcessed = packetsProcessed;
    }

    /**
     * Initializes a NetworkNodeBolt with default slidingWindow settings
     */
    public NetworkNodeBolt(){
        this(DEFAULT_SLIDING_WINDOW_IN_SECONDS, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
    }

    /**
     * Initializes a NetworkNodeBolt with the specified slidingWindow settings
     * @param windowLengthInSeconds length of the sliding window
     * @param emitFrequencyInSeconds the frequency in which results are emitted
     */
    public NetworkNodeBolt(int windowLengthInSeconds, int emitFrequencyInSeconds){
        packetsProcessed = 0;
        verbosity = 3;
        timeChecks = false;
        gatherStatistics = false;
        detectionRatio = 2000;
        state = new StateKeeper();
        hitCount = new HitCountKeeper();

        this.windowLengthInSeconds = windowLengthInSeconds;
        this.emitFrequencyInSeconds = emitFrequencyInSeconds;
        srcIpCounter = new SlidingWindowCounter<InetAddress>(deriveNumWindowChunksFrom(this.windowLengthInSeconds,
                this.emitFrequencyInSeconds));
        destIpCounter = new SlidingWindowCounter<InetAddress>(deriveNumWindowChunksFrom(this.windowLengthInSeconds,
                this.emitFrequencyInSeconds));
        destPortCounter = new SlidingWindowCounter<Integer>(deriveNumWindowChunksFrom(this.windowLengthInSeconds,
                this.emitFrequencyInSeconds));
    }

    public void prepare( Map conf, TopologyContext context, OutputCollector collector ) {
        this.collector = collector;
        timeChecks = (boolean) conf.get("timeCheck");
        taskId = context.getThisTaskId();
        lastModifiedTracker = new NthLastModifiedTimeTracker(deriveNumWindowChunksFrom(this.windowLengthInSeconds,
                this.emitFrequencyInSeconds));
        LOG.debug("Initialized task " + taskId + " from " + taskId + " with verbosity " + verbosity);
    }

    /**
     * Updates the data stored in the StateKeeper dataStructure and emits the data
     * to the NetworkConfuguratorBolt
     */
    private void emitCurrentWindowCounts() {
        if(timeChecks){
            hitCount.set(state.getSrcIpHitCount(), state.getDestIpHitCount(), state.getPortHitCount(), state.getFlagCount());
            state.setSrcIpHitCount(new HashMap<>(srcIpCounter.getCountsThenAdvanceWindow()));
            state.setDestIpHitCount(new HashMap<>(destIpCounter.getCountsThenAdvanceWindow()));
            state.setPortHitCount(new HashMap<>(destPortCounter.getCountsThenAdvanceWindow()));

            int actualWindowLengthInSeconds = lastModifiedTracker.secondsSinceOldestModification();
            lastModifiedTracker.markAsModified();
            if (actualWindowLengthInSeconds != windowLengthInSeconds) {
                LOG.warn(String.format(WINDOW_LENGTH_WARNING_TEMPLATE, actualWindowLengthInSeconds, windowLengthInSeconds));
                return;
            }
        }
        for(Map.Entry<InetAddress, Long> a : state.getSrcIpHitCount().entrySet()){
            try {
                //LOG.info("SRC IP "+ a.getKey()+" " + a.getValue().toString() + " || "+  hitCount.getSrcIpHitCount().get(a.getKey()).toString());
                if ((a.getValue() > (hitCount.getSrcIpHitCount().get(a.getKey()) * 2) && a.getValue() > detectionRatio) /*|| (a.getValue() > detectionRatio*2)*/) {
                    report(3, a.getKey());
                    LOG.info("Reported " + a.getKey() + " for " + a.getValue() + " hits");
                }
            }catch (NullPointerException e){
//                if (a.getValue() > detectionRatio){
//                    report(3, a.getKey());
//                    LOG.info("Reported " + a.getKey() + " for " + a.getValue() + " hits");
//                }
            }
        }
        for(Map.Entry<InetAddress, Long> a : state.getDestIpHitCount().entrySet()){
            try {
                //LOG.info("DST IP "+ a.getKey() +" "+ a.getValue().toString() + " || "+  hitCount.getSrcIpHitCount().get(a.getKey()).toString());
                if ((a.getValue() > (hitCount.getDestIpHitCount().get(a.getKey()) * 2) && a.getValue() > detectionRatio) /*|| (a.getValue() > detectionRatio*2)*/) {
                    report(4, a.getKey());
                    LOG.info("Reported " + a.getKey() + " for " + a.getValue() + " hits");
                }
            }catch (NullPointerException e){
//                if (a.getValue() > detectionRatio){
//                    report(4, a.getKey());
//                    LOG.info("Reported " + a.getKey() + " for " + a.getValue() + " hits");
//                }
            }
        }
        for(Map.Entry<Integer, Long> a : state.getPortHitCount().entrySet()) {
            try {
                //LOG.info("PORT "+ a.getKey()+" " + a.getValue().toString() + " || "+  hitCount.getSrcIpHitCount().get(a.getKey()).toString());
                if ((a.getValue() > (hitCount.getPortHitCount().get(a.getKey()) * 2) && a.getValue() > detectionRatio) /*|| (a.getValue() > detectionRatio*2)*/){
                    report(1, a.getKey());
                    LOG.info("Reported " + a.getKey() + " for " + a.getValue() + " port hits");
                }
            }catch (NullPointerException e){
                if (a.getValue() > detectionRatio){
                    report(1, a.getKey());
                    LOG.info("Reported " + a.getKey() + " for " + a.getValue() + " port hits");
                }
            }
        }
//
//        if (state.getFlagCount()[4] > hitCount.getFlagCount()[4]) {
//            report(7, 4);
//            LOG.info("Reported flag 4 for" + state.getFlagCount()[4] + " hits");
//        }
//        if (state.getFlagCount()[5] > hitCount.getFlagCount()[5]) {
//            report(7, 5);
//            LOG.info("Reported flag 5 for " + state.getFlagCount()[4] + " hits");
//        }

        if (!timeChecks) {
            //TODO derive statistics from state
            //LOG.info("Before: " + hitCount.toString());
            hitCount.set(state.getSrcIpHitCount(), state.getDestIpHitCount(), state.getPortHitCount(), state.getFlagCount());
            //LOG.info("After: " + hitCount.toString());
            state.resetCounts();
        }
    }

    public void execute( Tuple tuple ) {
        if (TupleUtils.isTick(tuple)) {
            if (timeChecks) {
                LOG.trace("Received tick tuple");
                emitCurrentWindowCounts();
            }
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
                    switch (code) {
                        case 10: { // means push new check verbosity
                            int newVerb = (Integer) tuple.getValueByField("setting");
                            verbosity = newVerb;
                            LOG.debug(taskId + " Changed check-verbosity to " + newVerb); // for debugging
                            break;
                        }
                        case 11: { // means push new port to blacklist
                            int newPort = (Integer) tuple.getValueByField("setting");
                            state.setBlockedPort(newPort, true);
                            LOG.debug(taskId + " Added port " + newPort + " to blacklist"); // for debugging
                            break;
                        }
                        case 12: { // means remove a port from blacklist
                            int newPort = (Integer) tuple.getValueByField("setting");
                            state.setBlockedPort(newPort, false);
                            LOG.debug(taskId + " Removed port " + newPort + " from blacklist"); // for debugging
                            break;
                        }
                        case 13:{ // means add new IP to blacklist
                            InetAddress newAddr = (InetAddress) tuple.getValueByField("setting");
                            state.addBlockedIpAddr(newAddr);
                            LOG.debug(taskId + " Added " + newAddr.getHostAddress() + " to blacklist"); // for debugging
                            break;
                        }
                        case 14: { // means remove IP from blacklist
                            InetAddress newAddr = (InetAddress) tuple.getValueByField("setting");
                            state.removeBlockedIpAddr(newAddr);
                            LOG.debug(taskId + " Removed " + newAddr.getHostAddress() + " from blacklist"); // for debugging
                            break;
                        }
                        case 15:{ // means add new IP to monitored
                            InetAddress newAddr = (InetAddress) tuple.getValueByField("setting");
                            state.addMonitoredIpAddr(newAddr);
                            LOG.debug(taskId + " Added " + newAddr.getHostAddress() + " to monitored"); // for debugging
                            break;
                        }
                        case 16: { // means remove IP from monitored
                            InetAddress newAddr = (InetAddress) tuple.getValueByField("setting");
                            state.removeMonitoredIpAddr(newAddr);
                            LOG.debug(taskId + " Removed " + newAddr.getHostAddress() + " from monitored"); // for debugging
                            break;
                        }
                        case 17: { // add new flag to blocked
                            TCPFlags newFlags = new TCPFlags((boolean[]) tuple.getValueByField("setting"));
                            state.addBlockedFlag(newFlags);
                            LOG.debug(taskId + " Added new flags to blocked"); // for debugging
                            break;
                        }
                        case 18: { // means remove flag from flags
                            TCPFlags newFlags = new TCPFlags((boolean[]) tuple.getValueByField("setting"));
                            state.removeBlockedFlag(newFlags);
                            LOG.debug(taskId + " Removed flags from blocked"); // for debugging
                            break;
                        }
                        case 19: { // set timecheck value
                            timeChecks = (boolean) tuple.getValueByField("setting");
                            LOG.debug(taskId + " Set timeChecks to " + timeChecks); // for debugging
                            break;
                        }
                        case 20: { // set gatherStaistics value
                            gatherStatistics = (boolean) tuple.getValueByField("setting");
                            LOG.debug(taskId + " Set gatherStatistics to " + gatherStatistics); // for debugging
                            break;
                        }
                        case 21: { //add packetContents to list of blocked packetContents
                            state.addBlockedData(Pattern.compile((String) tuple.getValueByField("setting")));
                            LOG.debug(taskId + " Added a new ApplicationLayer signature to blocked.");
                            break;
                        }
                        //TODO add detectionRatio change command

                    }
                    /** we return here because otherwise the performChecks() method would be invoked on the config data => crash */
                    collector.ack(tuple);
                    return;
                } else {
                    /** Obtain the packet from the spout */
                    packet = obtainPacket(tuple);
                    if (packet == null) {
                        collector.ack(tuple);
                        return;
                    }

                }

            } catch (Exception e) {
                LOG.error("An exception occurred during bolt "+ taskId + " execution", e);
                collector.reportError(e);
            }
            /** Invoke performChecks() on each packet that is received
             * if performChecks returns true the packet is allowed to pass, else it is just dropped
             * */
            if (performChecks(verbosity)) {
                emitPacket(packet);
            } else {
                report(8, packet.getSrc_ip());
            }
            /** If gatherStatistics is true gather data about the traffic characteristics*/
            if (gatherStatistics) {
                /** Increment respective counts */
                if (timeChecks) {
                    srcIpCounter.incrementCount(packet.getSrc_ip());
                    destIpCounter.incrementCount(packet.getDst_ip());
                    destPortCounter.incrementCount(packet.getDst_port());
                } else{
                    state.incrementSrcIpHitCount(packet.getSrc_ip());
                    state.incrementDestIpHitCount(packet.getDst_ip());
                    state.incrementPortHitCount(packet.getDst_port());
                    packetsProcessed++;
                    /** report as soon as 6000 packets processed */
                    if (packetsProcessed >= 10000){
                        LOG.info(taskId + " processed 10000 packets.");
                        emitCurrentWindowCounts();
                        packetsProcessed = 0;
                    }
                }
            }
            collector.ack(tuple);
            packet = null;
        }
    }

    /**
     * Performs checks upon a packet instance depending on the verbosity specified
     * @param code an integer representing the verbosity value (0 - do nothing, 1 - check ports, 2 - check IP's, 3 - check Flags)
     * @return true if all the checks succeed, false otherwise
     */
    private boolean performChecks(int code){
        if (code == 0)
            return true;
        boolean status = true;

        if (packet.getType().equals("TCP")){
            if (code == 5) return checkApplicationLayer(packet.getData());
            else {
                if (code > 0) status = status & checkPort(packet.getDst_port());
                //if (code >0)status = status & checkPort(packet.getSrc_port());
                if (code > 1) status = status & checkSrcIP(packet.getSrc_ip());
                if (code > 2) status = status & checkFlags(packet.getFlags());
                if (code > 3) status = status & checkApplicationLayer(packet.getData());
            }
        }
        else if (packet.getType().equals("UDP")){
            if (code == 5) return checkApplicationLayer(packet.getData());
            else {
                if (code > 0) status = status & checkPort(packet.getDst_port());
                //if (code >0)status = status & checkPort(packet.getDst_port());
                if (code > 1) status = status & checkSrcIP(packet.getSrc_ip());
                if (code > 3) status = status & checkApplicationLayer(packet.getData());
            }
        }
        else if (packet.getType().equals("IPP")){
            if (code >0)status = status & checkSrcIP(packet.getSrc_ip());
        }
        else return false;

        return status;
    }

    /**
     * Check a port number against the rules specified for the bolt
     * @param port the port to be checked
     * @return true if no problem is detected, false otherwise
     */
    private boolean checkPort(int port){
        return !state.isBlockedPort(port);
    }

    /**
     * Check an IP address against the rules specified for the bolt
     * @param addr the IP address to be checked
     * @return true if no problem is detected, false otherwise
     */
    private boolean checkSrcIP(InetAddress addr){
        if (state.isBlockedIpAddr(addr))
            return false;
//        else if(state.isMonitoredIpAddr(addr)){
//            report(4, addr);
//        }
        return true;

    }

    /**
     * Check an IP address against the rules specified for the bolt
     * @param addr the IP address to be checked
     * @return true if no problem is detected, false otherwise
     */
    private boolean checkDstIP(InetAddress addr){
        if (state.isBlockedIpAddr(addr))
            return false;
        return true;

    }

    /**
     * Check a set of flags against the rules specified for the bolt
     * @param flags the flags to be checked
     * @return true if no problem is detected, false otherwise
     */
    private boolean checkFlags(TCPFlags flags){
        if (state.isBadFlag(flags)) {
            return false;
        }
        else {
            if (gatherStatistics){
                boolean[] a = flags.toArray();
                for(int i=0; i<a.length; i++){
                    if (a[i]) {
                        state.incrementFlagCount(i);
                    }
                }
            }
            return true;
        }
    }

    /**
     * Check the actual contents of a packet for any anomalies
     * The check is based on searching for signatures within the data field
     * @return true if no problem is detected, false otherwise
     */
    private boolean checkApplicationLayer(PacketContents data){
        if (packet.data == null)
            return true;
        else{
            return !state.dataIsBlocked(data);
        }
    }

    /**
     * Report some type of error or event to the Configurator bolt.
     * @param type the code representing the event type
     * @param descr the value for the event if applicable
     */
    private void report(int type, Object descr){
        collector.emit("Reporting", new Values(taskId, type, descr));
    }

    /**
     * Reads the contents of a tuple and creates a Generic Packet instance depending on the packet type
     * @param tuple the tuple received by the bolt
     */
    private GenericPacket obtainPacket(Tuple tuple) {
        /** if the packet originates from the TCPPackets Stream its a TCPPacket
         * when its a TCP packet we extract the appropriate fields */
        if ("TCPPackets".equals(tuple.getSourceStreamId())) {
            long timestamp = (long) tuple.getValueByField("timestamp");
            String srcMAC = (String) tuple.getValueByField("srcMAC");
            String destMAC = (String) tuple.getValueByField("destMAC");
            InetAddress srcIP = (InetAddress) tuple.getValueByField("srcIP");
            InetAddress destIP = (InetAddress) tuple.getValueByField("destIP");
            int srcPort = (int) tuple.getValueByField("srcPort");
            int destPort = (int) tuple.getValueByField("destPort");
            boolean[] flags = (boolean[]) tuple.getValueByField("flags");
            byte[] data = (byte[]) tuple.getValueByField("data");
            return new GenericPacket(timestamp, srcMAC, destMAC, srcIP, destIP, srcPort, destPort, new TCPFlags(flags), data);

            /** if the packet originates from the UDPPackets Stream its a UDPPacket */
        } else if ("UDPPackets".equals(tuple.getSourceStreamId())) {
            long timestamp = (long) tuple.getValueByField("timestamp");
            String srcMAC = (String) tuple.getValueByField("srcMAC");
            String destMAC = (String) tuple.getValueByField("destMAC");
            InetAddress srcIP = (InetAddress) tuple.getValueByField("srcIP");
            InetAddress destIP = (InetAddress) tuple.getValueByField("destIP");
            int srcPort = (int) tuple.getValueByField("srcPort");
            int destPort = (int) tuple.getValueByField("destPort");
            byte[] data = (byte[]) tuple.getValueByField("data");
            return new GenericPacket(timestamp, srcMAC, destMAC, srcIP, destIP, srcPort, destPort, data);

            /** if the packet originates from the IPPackets Stream its a IPPacket */
            /** an IP packet is a packet that is not UDP or TCP and therefore we only extract basic data */
        } else if ("IPPackets".equals(tuple.getSourceStreamId())) {
            long timestamp = (long) tuple.getValueByField("timestamp");
            String srcMAC = (String) tuple.getValueByField("srcMAC");
            String destMAC = (String) tuple.getValueByField("destMAC");
            InetAddress srcIP = (InetAddress) tuple.getValueByField("srcIP");
            InetAddress destIP = (InetAddress) tuple.getValueByField("destIP");
            return new GenericPacket(timestamp, srcMAC, destMAC, srcIP, destIP);
        }
        LOG.trace("Received a message from "+ tuple.getSourceStreamId());
        return null;
    }

    public void declareOutputFields( OutputFieldsDeclarer declarer ) {
        declarer.declareStream("Reporting", new Fields("taskId", "anomalyType", "anomalyData"));
        declarer.declareStream("IPPackets", new Fields("timestamp", "srcMAC", "destMAC", "srcIP", "destIP" ));
        declarer.declareStream("UDPPackets", new Fields("timestamp", "srcMAC", "destMAC", "srcIP", "destIP", "srcPort", "destPort", "data"));
        declarer.declareStream("TCPPackets", new Fields("timestamp", "srcMAC", "destMAC", "srcIP", "destIP", "srcPort", "destPort", "flags", "data"));
    }

    /**
     * Emits a packet on a stream depending on its type
     * @param packet the Generic Packet instance to be emitted
     */
    private void emitPacket(GenericPacket packet){
        if(packet.getType().equals("TCP")){
            collector.emit("TCPPackets", new Values(packet.getTimestamp(), packet.getSourceMacAddress(),
                    packet.getDestMacAddress(), packet.getSrc_ip(), packet.getDst_ip(), packet.getSrc_port(),
                    packet.getDst_port(), packet.getFlags().toArray(), packet.getData().getData()));
        }
        else if(packet.getType().equals("UDP")){
            collector.emit("UDPPackets", new Values(packet.getTimestamp(), packet.getSourceMacAddress(),
                    packet.getDestMacAddress(), packet.getSrc_ip(), packet.getDst_ip(), packet.getSrc_port(),
                    packet.getDst_port(), packet.getData().getData()));
        }
        else if(packet.getType().equals("IPP")){
            collector.emit("IPPackets", new Values(packet.getTimestamp(), packet.getSourceMacAddress(),
                    packet.getDestMacAddress(), packet.getSrc_ip(), packet.getDst_ip()));
        }
        else {
            LOG.warn("Encountered an unknown packet type");
            return;
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
        m.put("Statistics", gatherStatistics);
        m.put("TimeChecks", timeChecks);
        return m;
    }
}
