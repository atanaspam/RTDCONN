package uk.ac.gla.atanaspam.network;

import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.TupleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.gla.atanaspam.network.utils.ConfiguratorStateKeeper;
import uk.ac.gla.atanaspam.pcapj.TCPFlags;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Listens for events reported from other bolts and pushes new configurations to them accordingly
 * @author atanaspam
 * @created 06/10/2015
 * @version 0.1
 */
public class NetworkConfiguratorBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(NetworkConfiguratorBolt.class);
    private OutputCollector collector;

    private int[] blockedPorts;
    private ArrayList<InetAddress> blockedIP;
    private ArrayList<InetAddress> monitoredIP;
    private ArrayList<String> blockedData;
    private TCPFlags[] badflags;


    private int taskId;
    private Long numBolts;
    private int[] commandHistory;
    private TopologyContext context;
    protected ConfiguratorStateKeeper state;
    private int round = 0;
    private ArrayList<Integer> aggregators;
    private ArrayList<Integer> spouts;
    private ArrayList<Integer> lvl0;
    private ArrayList<Integer> lvl1;
    private ArrayList<Integer> lvl2;
    private ArrayList<Integer> all;
    private boolean isRemote;
    private int n;
    private String topologyName;

    /**
     * Prepares the configurator bolt by initializing all the appropriate fields and obtaining
     * awareness over the rest of the topology.
     */
    public void prepare( Map conf, TopologyContext context, OutputCollector collector )
    {
        this.collector = collector;
        this.context = context;
        taskId = context.getThisTaskId();
        commandHistory = new int[10];
        // numBolts is cast to Long due to a bug in Storm
        numBolts = (Long) conf.get("boltNum");
        spouts = new ArrayList<>();
        aggregators = new ArrayList<>();
        lvl0 = new ArrayList<>();
        lvl1 = new ArrayList<>();
        lvl2 = new ArrayList<>();
        n = 0;

        blockedPorts = new int[0];
        blockedIP = new ArrayList<>();
        monitoredIP = new ArrayList<>();
        blockedData = new ArrayList<>();
        badflags = new TCPFlags[0];

        Map<Integer, String> map = context.getTaskToComponent();
        for (Map.Entry<Integer, String> entry : map.entrySet()) {
            if (entry.getValue().equals("node_0_lvl_0")) {
                lvl0.add(entry.getKey());
            }else if (entry.getValue().equals("node_0_lvl_1")) {
                lvl1.add(entry.getKey());
            }else if (entry.getValue().equals("node_0_lvl_2")) {
                lvl2.add(entry.getKey());
            }else if (entry.getValue().equals("emitter_bolt")) {
                spouts.add(entry.getKey());
            }else if (entry.getValue().equals("Aggregator")) {
                aggregators.add(entry.getKey());
            }
        }
        if (numBolts.intValue() > map.size()){
            state = new ConfiguratorStateKeeper(numBolts.intValue());
            LOG.trace("USING NUM_BOLTS FOR STATEKEEPER - " + numBolts.intValue());
        } else{
            state = new ConfiguratorStateKeeper(map.size());
            LOG.trace("USING MAP SIZE FOR STATEKEEPER - " + map.size());
        }

        topologyName = context.getStormId();

        all = new ArrayList<>();
        all.addAll(lvl0);
        all.addAll(lvl1);
        all.addAll(lvl2);
        initialConfig();
        String mode = (String) conf.get("mode");
        if (mode != null) {
            if (conf.get("mode").equals("remote")) {
                isRemote = true;
                //writeToFile("------," + topologyName + ",------");
            }
        }
    }

    public void execute( Tuple tuple ) {
        try {
            if (TupleUtils.isTick(tuple)) {
//                LOG.info(state.toString());
                round++;
                LOG.info("Round: " + round);
                if (round == 10) {
                    LOG.info("NUMBER OF PACKETS Detected : " + state.getNumberOfPacketsDropped());
                    emitBulkConfig(spouts, 32, 1);
                    emitBulkConfig(aggregators, 32, 1);
                    emitBulkConfig(all, 32, 1);
                    writeToFile(topologyName+",OVERALL,"+state.getNumberOfPacketsDropped()+", DROPPED");
                    round++;
                }
                //TODO emit config according to current stats
                collector.ack(tuple);
            } else {
                /** obtain the message from a bolt */
                int srcTaskId = (int) tuple.getValueByField("taskId");
                int anomalyType = (int) tuple.getValueByField("anomalyType");
                /** we check for all known error codes */
                switch (anomalyType) {
                    case 1: {   // 1 means lots of hits to a single port
                        int port = (int) tuple.getValueByField("anomalyData");
                        if (state.addPortHit(port, srcTaskId, round)) {
                            //TODO here we handle blocking or restructuring
                            LOG.info("Lots of hits to " + port);
                        }
                        break;
                    }
                    case 2: {    /* 2 means hits to an unexpected port */
                        int port = (int) tuple.getValueByField("anomalyData");
                        if (state.addUnexpPortHit(port, srcTaskId, round)) {
                            //TODO here we handle blocking or restructuring
                            LOG.info("Unexpected hit to" + port);
                        }
                        break;
                    }
                    case 3: {    /* 3 means lots of hits to the same src IP */
                        InetAddress ip = (InetAddress) tuple.getValueByField("anomalyData");
                        if (state.addIpHit(ip, srcTaskId, round)) {
                            //TODO here we handle blocking or restructuring
                            LOG.info("Lots of hits to srcIP:  " + ip);
                        }
                        break;
                    }
                    case 4: {    /* 4 means lots of hits to the same dest IP */
                        InetAddress ip = (InetAddress) tuple.getValueByField("anomalyData");
                        if (state.addIpHit(ip, srcTaskId, round)) {
                            //TODO here we handle blocking or restructuring
                            LOG.info("Lots of hits to dstIP:  " + ip);
                        }
                        break;
                    }
                    case 5: {    /* 5 means ask for reconfiguration */
                        reconfigure(srcTaskId);
                        break;
                    }
                    case 6: {    /* 6 means a dropped packet */
                        InetAddress ip = (InetAddress) tuple.getValueByField("anomalyData");
//                        if (state.addDroppedPacket(ip, srcTaskId, round)) {
//                            state.incrementDroppedPacket();
//                            //LOG.info("Dropped: " + ip);
//                        }
                        state.incrementDroppedPacket();
                        break;
                    }
                    case 7: { /* anomalious flag trafic */
                        int flagNum = (int) tuple.getValueByField("anomalyData");
                        if (state.addBadFlagHit(flagNum, srcTaskId, round)) {
                            //TODO here we handle blocking or restructuring
                            LOG.info("Anomalous flag detected...");
                            //TODO this seems to be unused....
                        }
                    }
                    case 8: { /* Receive numberOfAnomalousPackets */
                        long number = (long) tuple.getValueByField("anomalyData");
                        writeToFile(topologyName+",Emitter," + number + ",EMITTED");
                        LOG.warn("Emitter Emitted " + number + " ANOMALOUS PACKETS...");
                        break;
                    }
                    case 9: { /* Receive numberOfAnomalousPackets */
                        long number = (long) tuple.getValueByField("anomalyData");
                        writeToFile(topologyName+",Aggregator," + number + ",UNWANTED");
                        LOG.warn("Aggregator REPORTED " + number + " ANOMALOUS PACKETS...");
                        break;
                    }
                    case 0: { /* Receive numberOfAnomalousPackets */
                        long number = (long) tuple.getValueByField("anomalyData");
                        if (lvl0.contains(srcTaskId)){
                            writeToFile(topologyName+",LVL0," + number + ",PROCESSED");
                            LOG.warn("LVL0 bolt processed " + number);
                        }else if (lvl1.contains(srcTaskId)){
                            writeToFile(topologyName+",LVL1," + number + ",PROCESSED");
                            LOG.warn("LVL1 bolt processed " + number);
                        }else{
                            writeToFile(topologyName+",LVL2," + number + ",PROCESSED");
                            LOG.warn("LVL2 bolt processed " + number);
                        }
                        break;
                    }
                }
                collector.ack(tuple);
            }
        }catch (Exception e){
            collector.reportError(e);
        }
    }

    public void declareOutputFields( OutputFieldsDeclarer declarer ) {
        declarer.declareStream("Configure", new Fields("taskId", "code", "setting"));
    }

    /**
     * Obtains the current configuration and state of the bolt.
     * @return a String|Object map of all the configs
     */
    @Override
    public Map<String,Object> getComponentConfiguration(){
        Map<String, Object> m = new HashMap<String, Object>();
        m.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 60);
        m.put("History", commandHistory);
        return m;
    }

    /**
     * Emits a command to a specific bolt only.
     * @param dest the taskID that needs to change its config
     * @param code code for the command to be executed
     * @param setting the new value (command specific)
     */
    public void emitConfig(int dest, int code, Object setting){
        collector.emit("Configure", new Values(dest, code, setting));
        // command codes start from 10 so the first will be 10-10=0
        commandHistory[code-10]++;
    }

    /**
     * Emits a command to each of the bolts within a specific group.
     * @param target an arraylist storing the taskID's of each bolt to be addressed
     * @param code code for the command to be executed
     * @param setting the new value (command specific)
     */
    private void emitBulkConfig(ArrayList<Integer> target, int code, Object setting){
        for (int n: target){
            collector.emit("Configure", new Values(n, code, setting));
        }
    }

    /**
     * Performs the initial configuration of the bolts in the topology
     * and can be used to restore all the configuration to its default state.
     */
    public void initialConfig(){
        try {
            blockedIP .add(InetAddress.getByName("192.168.1.1"));
            //monitoredIP.add(InetAddress.getByName("192.168.1.2"));
            blockedData.add("aaa");
        }catch (Exception e){}

        //emitBulkConfig(lvl0, 20, false); (this is default)   // top level does not gather statistics
        emitBulkConfig(lvl0, 10, 1);        // top level performs no checks
        emitBulkConfig(lvl1, 10, 2);
        emitBulkConfig(lvl2, 20, 1);
        emitBulkConfig(lvl2, 10, 0);
        for(int port : blockedPorts)
            emitBulkConfig(all, 11, port);
        for(InetAddress ip : blockedIP)
            emitBulkConfig(all, 13, ip);
        for(InetAddress ip : monitoredIP)
            emitBulkConfig(all, 15, ip);
        for(TCPFlags flags : badflags)
            emitBulkConfig(all, 17, flags);
        for(String blockedContent : blockedData){
            emitBulkConfig(all, 21, blockedContent);
        }
        //emitBulkConfig(lvl2, 19, true);
    }

    /**
     * Sends all the available configuration to a node
     * Used in case of a node failure, in order to bring the failed node up to speed with teh system-wide configuration
     * @param dest the taskId of the node to be addressed.
     */
    private void reconfigure (int dest){
        if (lvl0.contains(dest)){
            emitConfig(dest, 10, 1);
        }else if (lvl1.contains(dest)){
            emitConfig(dest, 10, 2);
        }else{
            emitConfig(dest, 20, 1);
        }
        emitConfig(dest, 10, 1);
        emitConfig(dest, 10, 2);
        for(int port : blockedPorts)
            emitBulkConfig(all, 11, port);
        for(InetAddress ip : blockedIP)
            emitBulkConfig(all, 13, ip);
        for(InetAddress ip : monitoredIP)
            emitBulkConfig(all, 15, ip);
        for(TCPFlags flags : badflags)
            emitBulkConfig(all, 17, flags);
        for(String blockedContent : blockedData){
            emitBulkConfig(all, 21, blockedContent);
        }
    }

    /**
     * Dumps the string given to a file on the filesystem
     * Allows the configurator to dump its state before shutdown.
     * @param textToWrite a string representation of the file to write
     */
    public void writeToFile(String textToWrite){
        try {
            BufferedWriter out = new BufferedWriter
                    (new FileWriter("/users/level4/2031647p/Desktop/eval-results.csv",true));
                    //(new FileWriter("/Users/atanaspam/eval-results.csv",true));
            out.write(textToWrite + "\n");
            out.close();
        }
        catch (IOException e) {
            LOG.error("UNABLE TO WRITE TO FILE " + e);
        }
    }


}
