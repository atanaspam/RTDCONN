package uk.ac.gla.atanaspam.network;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * This bolt listens for events from all the other bolts and pushes new configurations to them accordingly
 * @author atanaspam
 * @created 06/10/2015
 * @version 0.1
 */
public class NetworkConfiguratorBolt extends BaseRichBolt {

    private OutputCollector collector;
    int componentId;
    boolean changed, changed1; // this is a temp field to restrict the software from blocking all ports (block only the first port)
    int[] commandHistory;
    TopologyContext context;

    ArrayList<Integer> lvl0;
    ArrayList<Integer> lvl1;
    ArrayList<Integer> all;


    /**
     * This method prepares the configurator bolt by initializing all the appropriate fields and obtaining
     * awareness over the rest of the topology.
     */
    public void prepare( Map conf, TopologyContext context, OutputCollector collector )
    {
        this.collector = collector;
        this.context = context;
        componentId = context.getThisTaskId();
        changed = changed1 = false;
        commandHistory = new int[10];
        lvl0 = new ArrayList<>();
        lvl1 = new ArrayList<>();
        Map<Integer, String> map = context.getTaskToComponent();
        for (Map.Entry<Integer, String> entry : map.entrySet()) {
            if (entry.getValue().equals("node_0_lvl_0")) {
                lvl0.add(entry.getKey());
            }
            if (entry.getValue().equals("node_0_lvl_1")) {
                lvl1.add(entry.getKey());
            }
        }
        all = new ArrayList<>();
        all.addAll(lvl0);
        all.addAll(lvl1);
        intialConfig();
    }

    public void execute( Tuple tuple )
    {
        /** obtain the message from a bolt */
        int srcComponentId = (Integer) tuple.getValueByField("componentId");
        int anomalyType = (Integer) tuple.getValueByField("anomalyType");
        /** we check for all known error codes */
        /* TODO Refactor so that codes are specified in a different class */
        switch (anomalyType){
            case 1:{   // 1 means lots of hits to a single port
                Integer port = (Integer) tuple.getValueByField("anomalyData");
                if (changed == false) {
                    // 11 is the code to add port to list of blocked ports
                    int code = 11;
                    if (lvl0.contains(srcComponentId)) {
                        emitBulkConfig(lvl1, code, port);
                    } else{
                        emitBulkConfig(lvl0, code, port);
                    }
                    changed = true;
                }

                //System.out.println("Problem with port " + port); // for testing
                break;
            }
            case 2:{    /* 2 means hits to an unexpected port */
                int count = (Integer)tuple.getValueByField("anomalyData");
                System.out.println("got "+ count + " form " + srcComponentId);
                /* TODO implement */
                break;
            }
            case 3:{    /* 3 means lots of hits to the same dest IP */
                /* TODO implement */
                break;
            }
            case 4:{    /* 4 means hits to an unexpected IP */
                InetAddress ip = (InetAddress) tuple.getValueByField("anomalyData");
                if (changed1 == false) {
                    int code = 10; // 10 means change the general scanning pattern for that bolt
                    if (lvl0.contains(srcComponentId)) {
                        emitBulkConfig(lvl1, code, 1);
                        emitBulkConfig(lvl0, code, 0);
                    } else{
                        emitBulkConfig(lvl1, code, 0);
                        emitBulkConfig(lvl0, code, 1);
                    }
                    changed1 = true;
                }
            }
            case 5:{    /* 5 means a dropped packet */
                InetAddress ip = (InetAddress) tuple.getValueByField("anomalyData");
                //System.out.println("Dropped: " + ip.getHostAddress()); // for testing
                break;
            }
            /* TODO implement more rules */
        }
        collector.ack(tuple);
    }

    public void declareOutputFields( OutputFieldsDeclarer declarer )
    {
        declarer.declareStream("Configure", new Fields("componentId", "code", "setting"));
    }

    /**
     * This method is used to obtain the current configuration and state of this bolt
     * @return a String|Object map of all the configs
     */
    @Override
    public Map<String,Object> getComponentConfiguration(){
        Map<String, Object> m = new HashMap<String, Object>();
        m.put("History", commandHistory);
        return m;
    }

    /**
     * This method emits a a command to a specific bolt only
     * @param dest the taskID that needs to change its config
     * @param code the command to be executed
     * @param setting the new value
     */
    public void emitConfig(int dest, int code, Object setting){
        collector.emit("Configure", new Values(dest, code, setting));
        // command codes start from 10 so the first will be 10-10=0
        commandHistory[code-10]++;
    }

    /**
     * This method emits a command to each of the bolts within a specific group
     * @param target an arraylist storing the taskID's of each bolt to be addressed
     * @param code the command to be executed
     * @param setting the new value
     */
    private void emitBulkConfig(ArrayList<Integer> target, int code, Object setting){
        for (int n: target){
            collector.emit("Configure", new Values(n, code, setting));
        }
    }

    public void intialConfig(){
        int[] blockedPorts = {};
        ArrayList<InetAddress> blockedIP = new ArrayList<>();
        ArrayList<InetAddress> monitoredIP = new ArrayList<>();
        try {
            //blockedIP .add(InetAddress.getByName("192.168.1.2"));
            //monitoredIP.add(InetAddress.getByName("192.168.1.2"));
        }catch (Exception e){}
        boolean[][] badflags = {};

        emitBulkConfig(lvl0, 10,1);
        emitBulkConfig(lvl1, 10,0);
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
