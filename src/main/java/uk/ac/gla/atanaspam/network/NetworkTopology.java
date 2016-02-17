package uk.ac.gla.atanaspam.network;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.gla.atanaspam.network.utils.*;

import java.net.InetAddress;
import java.net.Inet4Address;
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
    private static final int NUM_SPOUTS = 4;
    private static final int NUM_CONTROLLERS = 1;
    private static final int NUM_LVL0_BOLTS = 2;
    private static final int NUM_LVL1_BOLTS = 4;
    private static final int NUM_LVL2_BOLTS = 8;
    private static final int NUM_AGGREGATORS = 2;
    private static final int NUM_SPOUT_TASKS = 8;

    private static final int NUM_BOLTS = NUM_LVL0_BOLTS + NUM_LVL1_BOLTS + NUM_LVL2_BOLTS +
            NUM_AGGREGATORS + (NUM_SPOUTS*2);
    //NUM_SPOUTS = Number of PacketSpoutBolts

    public static void main(String[] args) {

        if (args.length != 3){
            System.out.println("Invalid arguments provided. Please specify:");
            System.out.println("1. Path to pcap file. 2. Operation mode: 'local' or 'remote'. 3. Topology name.");
            System.exit(1);
        }
        String filePath = args[0];
        String mode = args[1];
        String topologyName = args[2];

        TopologyBuilder builder = new TopologyBuilder();

        /***                        Topology Configuration                  ***/

        builder.setSpout("spout", new PacketSpout(), NUM_SPOUTS ) // we have 4 packet emitters
                ;//.setNumTasks(8);

        builder.setBolt("emitter_bolt", new PacketSpoutBolt(), NUM_SPOUTS )
                .allGrouping("Controller", "Configure")
                .shuffleGrouping("spout", "trigger")
                ;//.setNumTasks(8);

        builder.setBolt("node_0_lvl_0", new NetworkNodeBolt(1,0), NUM_LVL0_BOLTS )                      // we have 2 low level Bolts
                .allGrouping("Controller", "Configure")                                 // each one receives everything from the configurator
                .fieldsGrouping("emitter_bolt", "IPPackets", new Fields("destIP"))             // packets from the emitter are grouped by the destIP
                .fieldsGrouping("emitter_bolt", "TCPPackets", new Fields("destIP"))
                .fieldsGrouping("emitter_bolt", "UDPPackets", new Fields("destIP"));

        builder.setBolt("node_0_lvl_1", new NetworkNodeBolt(2,0), NUM_LVL1_BOLTS)                       // we have 3 mid level bolts
                .allGrouping("Controller", "Configure")
                .fieldsGrouping("node_0_lvl_0", "IPPackets", new Fields("destIP"))      // packets from the emitter are grouped by the destIP
                .fieldsGrouping("node_0_lvl_0", "TCPPackets", new Fields("destIP"))
                .fieldsGrouping("node_0_lvl_0", "UDPPackets", new Fields("destIP"));    // they receive approved packets from the low level bolts

        builder.setBolt("node_0_lvl_2", new NetworkNodeBolt(0,1), NUM_LVL2_BOLTS)                       // we have 8 high level bolts
                .allGrouping("Controller", "Configure")
                .fieldsGrouping("node_0_lvl_1", "IPPackets", new Fields("destIP"))      // packets from the emitter are grouped by the destIP
                .fieldsGrouping("node_0_lvl_1", "TCPPackets", new Fields("destIP"))
                .fieldsGrouping("node_0_lvl_1", "UDPPackets", new Fields("destIP"));    // they receive approved packets from the mid level bolts

        builder.setBolt("Controller", new NetworkConfiguratorBolt(), NUM_CONTROLLERS)                 // we have 1 controller bolt
                .allGrouping("node_0_lvl_0", "Reporting")                               // it receives all the reporting streams from
                .allGrouping("node_0_lvl_1", "Reporting")
                .allGrouping("node_0_lvl_2", "Reporting");                              // the high and low level bolts

        builder.setBolt("Aggregator", new NetworkAggregatorBolt(), NUM_AGGREGATORS)
                .shuffleGrouping("node_0_lvl_2", "IPPackets")
                .shuffleGrouping("node_0_lvl_2", "TCPPackets")
                .shuffleGrouping("node_0_lvl_2", "UDPPackets");

        /***                End of Topology Configuration                   ***/


        Config conf = new Config();
        conf.registerSerialization(InetAddress.class, InetAddressSerializer.class);
        conf.registerSerialization(Inet4Address.class);
        conf.registerSerialization(HitCountPair.class);
        conf.registerSerialization(HitCountKeeper.class);
//        conf.registerSerialization(BasicFirewallChecker.class);
//        conf.registerSerialization(ClassicCMAStatistics.class);
//        conf.registerSerialization(DPIFirewallChecker.class);
//        conf.registerSerialization(EmptyFirewallChecker.class);
//        conf.registerSerialization(EmptyStatisticsGatherer.class);
//        conf.registerSerialization(FullFirewallChecker.class);
//        conf.registerSerialization(SlidingWindowCMAStatistics.class);
        conf.registerSerialization(boolean[].class);
        conf.setFallBackOnJavaSerialization(false);
        conf.put("timeCheck", false);
        conf.put("boltNum", NUM_BOLTS);
        conf.put("filePath", filePath);

        if (mode.equals("remote")) {
            conf.setNumWorkers(NUM_BOLTS + 1);
            conf.setNumAckers(NUM_BOLTS+ 1);
            conf.setMaxSpoutPending(5000);
            try {
                StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            } catch (AuthorizationException e) {
                e.printStackTrace();
            }
        } else if (mode.equals("local")){
            LocalCluster cluster = new LocalCluster();
            conf.setMaxSpoutPending(5000);
            cluster.submitTopology(topologyName, conf, builder.createTopology());
            Utils.sleep(5000*1000);
            cluster.killTopology(topologyName);
            cluster.shutdown();
        } else {
            LOG.error("Invalid mode specified. Terminating.");
            System.exit(1);
        }
    }
}
