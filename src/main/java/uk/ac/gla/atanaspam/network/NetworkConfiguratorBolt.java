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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.gla.atanaspam.network.utils.ConfiguratorStateKeeper;
import uk.ac.gla.atanaspam.pcapj.TCPFlags;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
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
    int taskId;
    Long numBolts;
    int[] commandHistory;
    TopologyContext context;
    ConfiguratorStateKeeper state;
    int round = 0;
    ArrayList<Integer> spouts;
    ArrayList<Integer> lvl0;
    ArrayList<Integer> lvl1;
    ArrayList<Integer> lvl2;
    ArrayList<Integer> all;
    boolean isRemote;
    int n;

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
        lvl0 = new ArrayList<>();
        lvl1 = new ArrayList<>();
        lvl2 = new ArrayList<>();

        //TODO This is temporary
        n = 0;

        Map<Integer, String> map = context.getTaskToComponent();
        for (Map.Entry<Integer, String> entry : map.entrySet()) {
            if (entry.getValue().equals("node_0_lvl_0")) {
                lvl0.add(entry.getKey());
            }else if (entry.getValue().equals("node_0_lvl_1")) {
                lvl1.add(entry.getKey());
            }else if (entry.getValue().equals("node_0_lvl_2")) {
                lvl2.add(entry.getKey());
            } else if (entry.getValue().equals("emitter_bolt")) {
                spouts.add(entry.getKey());
            }
        }
        if (numBolts.intValue() > map.size()){
            state = new ConfiguratorStateKeeper(numBolts.intValue());
            LOG.trace("USING NUM_BOLTS FOR STATEKEEPER - " + numBolts.intValue());
        } else{
            state = new ConfiguratorStateKeeper(map.size());
            LOG.trace("USING MAP SIZE FOR STATEKEEPER - " + map.size());
        }

        all = new ArrayList<>();
        all.addAll(lvl0);
        all.addAll(lvl1);
        all.addAll(lvl2);
        initialConfig();
        String mode = (String) conf.get("mode");
        if (mode != null) {
            if (conf.get("mode").equals("remote")) {
                isRemote = true;
                writeToFile("------  " + context.getStormId() + " ------");
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
//                LOG.info("Changing anomaly...");
//                emitBulkConfig(spouts,30, 1);
//                emitBulkConfig(spouts,31, 40);
                    LOG.error("NUMBER OF PACKETS Detected : " + state.getNumberOfPacketsDropped());
                    emitBulkConfig(spouts, 32, 1);
                    round++;
                }
                //TODO emit config according to current stats
                //TODO change Integer to int
                collector.ack(tuple);
            } else {
                /** obtain the message from a bolt */
                int srcTaskId = (int) tuple.getValueByField("taskId");
                int anomalyType = (int) tuple.getValueByField("anomalyType");
                /** we check for all known error codes */
                //LOG.info(srcTaskId + " " +  anomalyType + "");
                switch (anomalyType) {
                    case 1: {   // 1 means lots of hits to a single port
                        Integer port = (Integer) tuple.getValueByField("anomalyData");
                        if (state.addPortHit(port, srcTaskId, round)) {
                            //TODO here we handle blocking or restructuring
                            LOG.info("Lots of hits to " + port);
                        }
                        break;
                    }
                    case 2: {    /* 2 means hits to an unexpected port */
                        int port = (Integer) tuple.getValueByField("anomalyData");
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
                    case 5: {    /* 5 means hits to an unexpected IP */
                        InetAddress ip = (InetAddress) tuple.getValueByField("anomalyData");
                        if (state.addUnexpIpHit(ip, srcTaskId, round)) {
                            //TODO here we handle blocking or restructuring
                            LOG.info("Unexpected hit to:  " + ip);
                        }
                        break;
                    }
                    case 6: {    /* 6 means a dropped packet */
                        InetAddress ip = (InetAddress) tuple.getValueByField("anomalyData");
//                        if (state.addDroppedPacket(ip, srcTaskId, round)) {
//                            //TODO here we handle blocking or restructuring
//                            state.incrementDroppedPacket();
//                            //LOG.info("Dropped: " + ip);
//                        }
                        state.incrementDroppedPacket();
                        break;
                    }
                    case 7: { /* anomalious flag trafic */
                        int flagNum = (Integer) tuple.getValueByField("anomalyData");
                        if (state.addBadFlagHit(flagNum, srcTaskId, round)) {
                            //TODO here we handle blocking or restructuring
                            LOG.info("Anomalous flag detected...");
                            //TODO this seems to be unused....
                        }
                    }
                    case 8: { /* Receive numberOfAnomalousPackets */
                        double number = (double) tuple.getValueByField("anomalyData");
                        if (n == 4){
                            writeToFile("OVERALL PACKETS DROPPED: " + state.getNumberOfPacketsDropped());
                        }
                        writeToFile(srcTaskId + "REPORTED " + number + "ANOMALOUS PACKETS...");
                        n++;
                        LOG.warn(srcTaskId + "REPORTED " + number + "ANOMALOUS PACKETS...");
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
        int[] blockedPorts = {};
        ArrayList<InetAddress> blockedIP = new ArrayList<>();
        ArrayList<InetAddress> monitoredIP = new ArrayList<>();
        ArrayList<Pattern> blockedData = new ArrayList<>();
        try {
            blockedIP .add(InetAddress.getByName("192.168.1.1"));
            //monitoredIP.add(InetAddress.getByName("192.168.1.2"));
        }catch (Exception e){}
        TCPFlags[] badflags = {};

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
        //emitBulkConfig(lvl2, 19, true);
    }
    public void writeToFile(String textToWrite){
        try {
            BufferedWriter out = new BufferedWriter
                    (new FileWriter("/users/level4/2031647p/Desktop/eval-results.txt",true));
            out.write(textToWrite + "\n");
            out.close();
        }
        catch (IOException e) {
            LOG.error("UNABLE TO WRITE TO FILE " + e);
        }
    }


}
