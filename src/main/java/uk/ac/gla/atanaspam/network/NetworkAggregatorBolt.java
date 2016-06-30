package uk.ac.gla.atanaspam.network;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * This bolt currently represents all the clients within a datacenter topology and
 * therefore accepts all the packets that have managed to pass through the topology
 * currently it monitors the number of packets that reach it and meet a specific criteria.
 * @see NetworkNodeBolt - to see where packets are comming from
 * @see NetworkConfiguratorBolt - to see where this bolt sends events
 * @author atanaspam
 * @created 06/10/2015
 * @version 0.1
 */
public class NetworkAggregatorBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(NetworkAggregatorBolt.class);
    private OutputCollector collector;
    int taskId;
    long packetCount;
    HashSet<InetAddress> blockedIp;

    /**
     * Prepares the bolt for execution
     * Additionally it initializes the data structure that monitors for anomalous traffic that might have been undetected.
     * @param conf
     * @param context
     * @param collector
     */
    public void prepare( Map conf, TopologyContext context, OutputCollector collector ) {
        this.collector = collector;
        taskId = context.getThisTaskId();
        packetCount = 0;
        blockedIp = new HashSet<>();
        try {
            blockedIp.add(InetAddress.getByName("192.168.1.1"));
        }catch (Exception e){}
    }

    public void execute( Tuple tuple ) {
        try {
            if ("Configure".equals(tuple.getSourceStreamId())) {
                int dest = (Integer) tuple.getValueByField("taskId");
                /** obtain the address and check if you are the intended recipient of the message */
                if (dest != taskId) {
                    collector.ack(tuple);
                    return;
                }
                int code = (Integer) tuple.getValueByField("code");
                if (code == 32) {
                    LOG.info("NUMBER OF ANOMALOUS PACKETS: " + packetCount);
                    report(9, packetCount);
                }
                collector.ack(tuple);
            } else {
                InetAddress srcIP = (InetAddress) tuple.getValueByField("srcIP");
                if (blockedIp.contains(srcIP)){
                    packetCount++;
                }
            collector.ack(tuple);
            }
        } catch (Exception e) {
            collector.reportError(e);
        }
    }

    /**
     * Report an event to the Configurator bolt.
     * @param type  the code representing the event type
     * @param descr the value for the event if applicable
     */
    private void report(int type, Object descr) {
        collector.emit("Reporting", new Values(taskId, type, descr));
    }

    public void declareOutputFields( OutputFieldsDeclarer declarer ) {
        declarer.declareStream("Reporting", new Fields("taskId", "anomalyType", "anomalyData"));
    }
    @Override
    public Map<String,Object> getComponentConfiguration(){
        Map<String, Object> m = new HashMap<String, Object>();
        m.put("ID", taskId);
        m.put("packetCount", packetCount);
        return m;
    }
}
