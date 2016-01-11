package uk.ac.gla.atanaspam.network;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.gla.atanaspam.network.utils.StateKeeper;

import java.net.InetAddress;
import java.util.ArrayList;


/**
 * This class specifies the Network topology and runs the topology for 10 seconds
 * @author atanaspam
 * @created 05/10/2015
 * @version 0.3
 */
public class NetworkTopology {

    private static final Logger LOG = LoggerFactory.getLogger(NetworkTopology.class);

    private static final int DEFAULT_RUNTIME_IN_SECONDS = 60;
    private static final int TOP_N = 5;
    private static final int NUM_SPOUTS = 2;
    private static final int NUM_CONTROLLERS = 1;
    private static final int NUM_LVL0_BOLTS = 2;
    private static final int NUM_LVL1_BOLTS = 4;
    private static final int NUM_LVL2_BOLTS = 8;
    private static final int NUM_BOLTS = NUM_LVL0_BOLTS + NUM_LVL1_BOLTS + NUM_LVL2_BOLTS;

    public static void main(String[] args) {

        if (args[0] == null){
            LOG.error("Invalid path to pcap file. Terminating");
            System.exit(1);
        }
        String filePath = args[0];
        TopologyBuilder builder = new TopologyBuilder();



        /***                        Topology Configuration                  ***/

        builder.setSpout("spout", new PacketSpout(), NUM_SPOUTS );//TODO fix to 4                               // we have 4 packet emitters


        builder.setBolt("node_0_lvl_0", new NetworkNodeBolt(), NUM_LVL0_BOLTS )                      // we have 2 low level Bolts
                .allGrouping("Controller", "Configure")                                 // each one receives everything from the configurator
                .fieldsGrouping("spout", "IPPackets", new Fields("destIP"))             // packets from the emitter are grouped by the destIP
                .fieldsGrouping("spout", "TCPPackets", new Fields("destIP"))
                .fieldsGrouping("spout", "UDPPackets", new Fields("destIP"));

        builder.setBolt("node_0_lvl_1", new NetworkNodeBolt(), NUM_LVL1_BOLTS)                       // we have 3 mid level bolts
                .allGrouping("Controller", "Configure")
                .fieldsGrouping("node_0_lvl_0", "IPPackets", new Fields("destIP"))      // packets from the emitter are grouped by the destIP
                .fieldsGrouping("node_0_lvl_0", "TCPPackets", new Fields("destIP"))
                .fieldsGrouping("node_0_lvl_0", "UDPPackets", new Fields("destIP"));    // they receive approved packets from the low level bolts

        builder.setBolt("node_0_lvl_2", new NetworkNodeBolt(), NUM_LVL2_BOLTS)                       // we have 8 high level bolts
                .allGrouping("Controller", "Configure")
                .fieldsGrouping("node_0_lvl_0", "IPPackets", new Fields("destIP"))      // packets from the emitter are grouped by the destIP
                .fieldsGrouping("node_0_lvl_0", "TCPPackets", new Fields("destIP"))
                .fieldsGrouping("node_0_lvl_0", "UDPPackets", new Fields("destIP"));    // they receive approved packets from the mid level bolts

        builder.setBolt("Controller", new NetworkConfiguratorBolt(), NUM_CONTROLLERS)                 // we have 1 controller bolt
                .allGrouping("node_0_lvl_0", "Reporting")                               // it receives all the reporting streams from
                .allGrouping("node_0_lvl_1", "Reporting")
                .allGrouping("node_0_lvl_2", "Reporting");                              // the high and low level bolts

//        builder.setBolt("Aggregator", new NetworkAggregatorBolt(),1)
//                .allGrouping("node_0_lvl_2", "IPPackets")
//                .allGrouping("node_0_lvl_2", "TCPPackets")
//                .allGrouping("node_0_lvl_2", "UDPPackets");

        /***                End of Topology Configuration                   ***/


        Config conf = new Config();
        conf.put("timecheck", false);
        conf.put("boltNum", (int) NUM_BOLTS);
        conf.put("filePath", filePath);
        conf.registerSerialization(StateKeeper.class);
        LocalCluster cluster = new LocalCluster();

        /** submit the topology and run it for 10 seconds */
        cluster.submitTopology("test", conf, builder.createTopology());
        Utils.sleep(5000*200);
        cluster.killTopology("test");
        cluster.shutdown();
    }
    @Deprecated
    private static Config initializeConfig(Config conf){
        ArrayList<InetAddress> blocked = new ArrayList<>();
        ArrayList<InetAddress> monitored = new ArrayList<>();
        ArrayList<boolean[]> badFlags = new ArrayList<>();
        ArrayList<Integer> blockedPorts = new ArrayList<>();
        /*
        try {
            blocked.add(InetAddress.getByName("74.125.136.109"));
            monitored.add(InetAddress.getByName("192.168.1.1"));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }*/ //This is commented as we dont want to block anything right now

        conf.put("timeChecks", false);
        conf.put("node_0_lvl_0", 1);
        conf.put("node_0_lvl_1", 0);
        conf.put("blockedIp", blocked);
        conf.put("monitoredIp", monitored);
        conf.put("badFlags", badFlags);
        conf.put("blockedPorts", blockedPorts);
        return conf;
    }
}