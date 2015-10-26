package uk.ac.gla.atanaspam.network;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;


/**
 * Created by atanaspam on 05/10/2015.
 */
public class NetworkTopology {
    //private static org.apache.log4j.Logger log = Logger.getLogger(MyLogger.class);
    public static void main(String[] args)
    {
        //BasicConfigurator.configure();
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout( "spout", new PacketSpout(), 4 );
        builder.setBolt("node_0_lvl_0", new NetworkNodeBolt(), 4 )
                .fieldsGrouping("spout", "IPPackets", new Fields("destIP"))
                .fieldsGrouping("spout", "TCPPackets", new Fields("destIP"))
                .fieldsGrouping("spout", "UDPPackets", new Fields("destIP"));
        builder.setBolt( "node_0_lvl_1", new NetworkAggregatorBolt(), 2)
                //.fieldsGrouping("node_0_lvl_0", "TCPPackets", new Fields("destIP"));
                .shuffleGrouping("node_0_lvl_0");


        Config conf = new Config();
        /**
         * TODO remove registration
         */
        //conf.registerSerialization(Packet.class);
        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("test", conf, builder.createTopology());
        Utils.sleep(10000);
        cluster.killTopology("test");
        cluster.shutdown();
    }
}