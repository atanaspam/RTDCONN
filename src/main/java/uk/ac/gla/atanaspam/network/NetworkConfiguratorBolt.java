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

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

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

    ArrayList<Integer> lvl0;
    ArrayList<Integer> lvl1;
    ArrayList<Integer> lvl2;
    ArrayList<Integer> all;

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
        lvl0 = new ArrayList<>();
        lvl1 = new ArrayList<>();
        lvl2 = new ArrayList<>();
        state = new ConfiguratorStateKeeper(numBolts.intValue());

        Map<Integer, String> map = context.getTaskToComponent();
        for (Map.Entry<Integer, String> entry : map.entrySet()) {
            if (entry.getValue().equals("node_0_lvl_0")) {
                lvl0.add(entry.getKey());
            }
            if (entry.getValue().equals("node_0_lvl_1")) {
                lvl1.add(entry.getKey());
            }
            if (entry.getValue().equals("node_0_lvl_2")) {
                lvl2.add(entry.getKey());
            }
        }
        all = new ArrayList<>();
        all.addAll(lvl0);
        all.addAll(lvl1);
        initialConfig();
    }

    public void execute( Tuple tuple )
    {
        if(TupleUtils.isTick(tuple)){
            LOG.info(state.toString());
            //TODO emit config according to current stats
            return;
        }
        /** obtain the message from a bolt */
        int srcTasktId = (Integer) tuple.getValueByField("taskId");
        int anomalyType = (Integer) tuple.getValueByField("anomalyType");
        /** we check for all known error codes */
        switch (anomalyType){
            case 1:{   // 1 means lots of hits to a single port
                Integer port = (Integer) tuple.getValueByField("anomalyData");
                state.addPortHit(port, srcTasktId);
                //System.out.println("Problem with port " + port); // for testing
                break;
            }
            case 2:{    /* 2 means hits to an unexpected port */
                int port = (Integer)tuple.getValueByField("anomalyData");
                //System.out.println("got "+ count + " form " + srcTasktId);
                state.addUnexpPortHit(port,srcTasktId);
                break;
            }
            case 3:{    /* 3 means lots of hits to the same src IP */
                InetAddress ip = (InetAddress) tuple.getValueByField("anomalyData");
                state.addIpHit(ip,srcTasktId);
                break;
            }
            case 4:{    /* 4 means lots of hits to the same dest IP */
                InetAddress ip = (InetAddress) tuple.getValueByField("anomalyData");
                state.addIpHit(ip,srcTasktId);
                break;
            }
            case 5:{    /* 5 means hits to an unexpected IP */
                InetAddress ip = (InetAddress) tuple.getValueByField("anomalyData");
                state.addUnexpIpHit(ip,srcTasktId);
                break;
            }
            case 6:{    /* 6 means a dropped packet */
                InetAddress ip = (InetAddress) tuple.getValueByField("anomalyData");
                state.addDropPacket(ip, srcTasktId);
                //System.out.println("Dropped: " + ip.getHostAddress()); // for testing
                break;
            }
            case 7: { /* anomalious flag trafic */
                //InetAddress ip = (InetAddress) tuple.getValueByField("anomalyData");
                int port = (Integer) tuple.getValueByField("anomalyData");
                state.addBadFlag(port, srcTasktId);
            }
            case 8: { /* Dropped Packet */
                //TODO handle dropped packets
            }
        }
        collector.ack(tuple);
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
     * Emits a a command to a specific bolt only.
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
        try {
            //blockedIP .add(InetAddress.getByName("192.168.1.2"));
            //monitoredIP.add(InetAddress.getByName("192.168.1.2"));
        }catch (Exception e){}
        boolean[][] badflags = {};

        //emitBulkConfig(lvl0, 20, false); (this is default)   // top level does not gather statistics
        emitBulkConfig(lvl0, 10, 0);        // top level perfroms no checks
        emitBulkConfig(lvl1, 10,3);
        emitBulkConfig(lvl2, 20, true);
        emitBulkConfig(lvl2, 10, 0);
        for(int port : blockedPorts)
            emitBulkConfig(all, 11, port);
        for(InetAddress ip : blockedIP)
            emitBulkConfig(all, 13, ip);
        for(InetAddress ip : monitoredIP)
            emitBulkConfig(all, 15, ip);
        for(boolean[] flags :badflags)
            emitBulkConfig(all, 15, flags);
        emitBulkConfig(all, 19, false);
    }


}
