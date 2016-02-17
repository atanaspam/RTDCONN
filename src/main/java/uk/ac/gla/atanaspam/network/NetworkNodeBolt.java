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
import uk.ac.gla.atanaspam.network.utils.*;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    private int taskId;
    private GenericPacket packet;
    private ChecksPerformer checks;
    private StatisticsGatherer statistics;
    /**
     * Check verbosity indicates the level of checks to be performed
     * 0 - do nothing, 1 - check ports, 2 - check IP's, 3 - check Flags
     */
    private int checkVerbosity;
    private int statisticsVerbosity;
    private boolean timeChecks;
    private int packetsProcessed;
    private int detectionRatio;

    /**
     * Initializes a NetworkNodeBolt with a specific state
     * Used for testing.
     */
    public NetworkNodeBolt(StatisticsGatherer statistics, ChecksPerformer checks, int checkVerbosity,
                           int statisticsVerbosity, int packetsProcessed){
        this(checkVerbosity, statisticsVerbosity);
        this.statistics = statistics;
        this.checks = checks;
    }

    /**
     * Initializes a NetworkNodeBolt with default slidingWindow settings
     */
    public NetworkNodeBolt(){
        this(1, 0);
    }


    /**
     * Initializes a NetworkNodeBolt with the specified verbosity settings
     * @param checksVerbosity The verbosity of Firewall checks
     * @param statisticsVerbosity The verbosity of Statistics gathering
     */
    public NetworkNodeBolt(int checksVerbosity, int statisticsVerbosity){
        handleCheckVerbosityChange(checksVerbosity);
        handleStatisticsVerbosityChange(statisticsVerbosity);

        packetsProcessed = 0;
        this.checkVerbosity = checksVerbosity;
        this.statisticsVerbosity = statisticsVerbosity;
        detectionRatio = 2000;
    }

    @Override
    public void prepare( Map conf, TopologyContext context, OutputCollector collector ) {
        this.collector = collector;
//        timeChecks = (boolean) conf.get("timeCheck");
        taskId = context.getThisTaskId();
        statistics.setTaskId(taskId);
        statistics.setDetectionRatio(detectionRatio);
        LOG.info("Initialized task " + taskId + " from " + taskId + " with check verb " +
                checkVerbosity + " and stat verb " + statisticsVerbosity);
    }

    @Override
    public void execute( Tuple tuple ) {
        if (TupleUtils.isTick(tuple)) {
            if (statisticsVerbosity == 2) {
                statistics.emitCurrentWindowCounts(collector);
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
                            this.checkVerbosity = newVerb;
                            handleCheckVerbosityChange(checkVerbosity);
                            LOG.debug(taskId + " Changed check-verbosity to " + newVerb); // for debugging
                            break;
                        }
                        case 11: { // means push new port to blacklist
                            int newPort = (Integer) tuple.getValueByField("setting");
                            //TODO this only works with SrcPorts
                            checks.addPort(newPort, 1);
                            LOG.debug(taskId + " Added port " + newPort + " to blacklist"); // for debugging
                            break;
                        }
                        case 12: { // means remove a port from blacklist
                            int newPort = (Integer) tuple.getValueByField("setting");
                            //TODO this only works with SrcPorts
                            checks.removePort(newPort, 1);
                            LOG.debug(taskId + " Removed port " + newPort + " from blacklist"); // for debugging
                            break;
                        }
                        case 13:{ // means add new IP to blacklist
                            InetAddress newAddr = (InetAddress) tuple.getValueByField("setting");
                            //TODO this only works with SrcIP
                            checks.addIpAddress(newAddr, 1);
                            LOG.debug(taskId + " Added " + newAddr.getHostAddress() + " to blacklist"); // for debugging
                            break;
                        }
                        case 14: { // means remove IP from blacklist
                            InetAddress newAddr = (InetAddress) tuple.getValueByField("setting");
                            //TODO this only works with SrcIP
                            checks.removeIpAddress(newAddr, 1);
                            LOG.debug(taskId + " Removed " + newAddr.getHostAddress() + " from blacklist"); // for debugging
                            break;
                        }
                        case 15:{ // means add new IP to monitored
//                            InetAddress newAddr = (InetAddress) tuple.getValueByField("setting");
//                            state.addMonitoredIpAddr(newAddr);
//                            LOG.debug(taskId + " Added " + newAddr.getHostAddress() + " to monitored"); // for debugging
//                            break;
                        }
                        case 16: { // means remove IP from monitored
//                            InetAddress newAddr = (InetAddress) tuple.getValueByField("setting");
//                            state.removeMonitoredIpAddr(newAddr);
//                            LOG.debug(taskId + " Removed " + newAddr.getHostAddress() + " from monitored"); // for debugging
//                            break;
                        }
                        case 17: { // add new flag to blocked
                            TCPFlags newFlags = new TCPFlags((boolean[]) tuple.getValueByField("setting"));
                            checks.addFlag(newFlags, 1);
                            LOG.debug(taskId + " Added new flags to blocked"); // for debugging
                            break;
                        }
                        case 18: { // means remove flag from flags
                            TCPFlags newFlags = new TCPFlags((boolean[]) tuple.getValueByField("setting"));
                            checks.removeFlag(newFlags, 1);
                            LOG.debug(taskId + " Removed flags from blocked"); // for debugging
                            break;
                        }
                        case 19: { // set timecheck value
                            timeChecks = (boolean) tuple.getValueByField("setting");
                            LOG.debug(taskId + " Set timeChecks to " + timeChecks); // for debugging
                            break;
                        }
                        case 20: { // set statisticsVerbosity value
                            this.statisticsVerbosity = (int) tuple.getValueByField("setting");
                            handleStatisticsVerbosityChange(statisticsVerbosity);
                            LOG.debug(taskId + " Set statisticsVerbosity to " + statisticsVerbosity); // for debugging
                            break;
                        }
                        case 21: { //add packetContents to list of blocked packetContents
                            checks.addPattern(Pattern.compile((String) tuple.getValueByField("setting")), 1);
                            LOG.debug(taskId + " Added a new ApplicationLayer signature to blocked.");
                            break;
                        }
                        case 22: {
                            statistics.setDetectionRatio((int) tuple.getValueByField("setting"));
                            LOG.debug(taskId + " Changed detection ratio to :" + (int) tuple.getValueByField("setting"));
                        }

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
            if (checks.performChecks(packet)) {
                emitPacket(packet);
            } else {
                //LOG.info(packet.getSrc_ip() + "");
                report(6, packet.getSrc_ip());
            }
            statistics.addSrcIpHit(packet.getSrc_ip(),1);
            statistics.addDstIpHit(packet.getDst_ip(),1);
            if( packet.getType() == 3 || packet.getType() == 2) {
                statistics.addSrcPortHit(packet.getSrc_port(), 1);
                statistics.addDstPortHit(packet.getDst_port(), 1);
                if (packet.getType() == 2){
                    for(int i=0;i<packet.getFlags().toArray().length;i++) {
                        if (packet.getFlags().toArray()[i]) {
                            statistics.addFlagCount(i, 1);
                        }
                    }
                }
            }
            if (statisticsVerbosity == 1){
                packetsProcessed++;
                if (packetsProcessed == 10000){
                    LOG.info("Processed 10000 packets");
                    statistics.emitCurrentWindowCounts(collector);
                    packetsProcessed = 0;
                }
            }
            collector.ack(tuple);
            packet = null;
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

    @Override
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
        if(packet.getType() == 2){
            collector.emit("TCPPackets", new Values(packet.getTimestamp(), packet.getSourceMacAddress(),
                    packet.getDestMacAddress(), packet.getSrc_ip(), packet.getDst_ip(), packet.getSrc_port(),
                    packet.getDst_port(), packet.getFlags().toArray(), packet.getData().getData()));
        }
        else if(packet.getType() == 3){
            collector.emit("UDPPackets", new Values(packet.getTimestamp(), packet.getSourceMacAddress(),
                    packet.getDestMacAddress(), packet.getSrc_ip(), packet.getDst_ip(), packet.getSrc_port(),
                    packet.getDst_port(), packet.getData().getData()));
        }
        else if(packet.getType() == 1){
            collector.emit("IPPackets", new Values(packet.getTimestamp(), packet.getSourceMacAddress(),
                    packet.getDestMacAddress(), packet.getSrc_ip(), packet.getDst_ip()));
        }
        else {
            LOG.warn("Encountered an unknown packet type");
            return;
        }
    }

    private void handleCheckVerbosityChange(int checkVerbosity){
        switch (checkVerbosity){
            case 0: {
                this.checks = new EmptyFirewallChecker();
                break;
            }
            case 1: {
                this.checks = new BasicFirewallChecker();
                break;
            }
            case 2: {
                this.checks = new DPIFirewallChecker();
                break;
            }
            case 3: {
                this.checks = new FullFirewallChecker();
                break;
            }
        }
    }

    private void handleStatisticsVerbosityChange(int statisticsVerbosity){
        switch (statisticsVerbosity){
            case 0: {
                this.statistics = new EmptyStatisticsGatherer();
                break;
            }
            case 1: {
                this.statistics = new ClassicCMAStatistics();
                break;
            }
            case 2: {
                this.statistics = new SlidingWindowCMAStatistics();
                break;
            }
        }
    }

    @Override
    public Map<String,Object> getComponentConfiguration(){
        Map<String, Object> m = new HashMap<String, Object>();
        m.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, StatisticsGatherer.DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
        m.put("ID", taskId);
        m.put("CheckVerbosity", checkVerbosity);
        m.put("StatisticVerbosity", statisticsVerbosity);
        m.put("TimeChecks", timeChecks);
        return m;
    }
}
