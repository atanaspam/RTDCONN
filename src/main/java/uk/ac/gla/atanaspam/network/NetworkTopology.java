package uk.ac.gla.atanaspam.network;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;


/**
 * This class specifies the Network topology and runs the topology for 10 seconds
 * @author atanaspam
 * @created 05/10/2015
 * @version 0.3
 */
public class NetworkTopology {
    //private static org.apache.log4j.Logger log = Logger.getLogger(MyLogger.class);
    public static void main(String[] args)
    {
        TopologyBuilder builder = new TopologyBuilder();

        /***                        Topology Configuration                  ***/

        builder.setSpout("spout", new PacketSpout(), 4 );                           // we have 4 packet emitters


        builder.setBolt("node_0_lvl_0", new NetworkNodeBolt(), 4 )                  // we have 4 low level Bolts
                .allGrouping("Controller", "Configure")                             // each one receives everything from the configurator
                .fieldsGrouping("spout", "IPPackets", new Fields("destIP"))         // packets from the emitter are grouped by the destIP
                .fieldsGrouping("spout", "TCPPackets", new Fields("destIP"))
                .fieldsGrouping("spout", "UDPPackets", new Fields("destIP"));

        builder.setBolt("node_0_lvl_1", new NetworkAggregatorBolt(), 2)             // we have 2 high level bolts
                .shuffleGrouping("node_0_lvl_0", "Packets");                        // they receive approved packets from the low level bolts

        builder.setBolt("Controller", new NetworkConfiguratorBolt(), 1)             // we have 1 controller bolt
                .shuffleGrouping("node_0_lvl_0", "Reporting")                       // it receives all the reporting streams from
                .shuffleGrouping("node_0_lvl_1", "Reporting");                      // the high and low level bolts


        /***                End of Topology Configuration                   ***/

        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();

        /** submit the topology and run it for 10 seconds */
        cluster.submitTopology("test", conf, builder.createTopology());
        Utils.sleep(10000);
        cluster.killTopology("test");
        cluster.shutdown();
    }
}