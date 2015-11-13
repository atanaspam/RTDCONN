package uk.ac.gla.atanaspam.network;

import backtype.storm.task.OutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import backtype.storm.metric.api.IMetricsConsumer;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;
import backtype.storm.utils.Utils;

/**
 * @author atanaspam
 * @version 0.1
 * @created 08/11/2015
 */

public class MetricsConsumer implements IMetricsConsumer{
    public static final Logger LOG = LoggerFactory.getLogger(MetricsConsumer.class);

    private OutputCollector collector;
    int componentId;

    @Override
    public void prepare(Map stormConf, Object registrationArgument, TopologyContext context, IErrorReporter errorReporter) {
    }

    static private String padding = "                       ";

    @Override
    public void handleDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {
        StringBuilder sb = new StringBuilder();
        String header = String.format("%d\t%15s:%-4d\t%3d:%-11s\t",
                taskInfo.timestamp,
                taskInfo.srcWorkerHost, taskInfo.srcWorkerPort,
                taskInfo.srcTaskId,
                taskInfo.srcComponentId);
        sb.append(header);
        for (DataPoint p : dataPoints) {
            sb.delete(header.length(), sb.length());
            /*
            sb.append(p.name)
                    .append(padding).delete(header.length()+23,sb.length()).append("\t")
                    .append(p.value);
            LOG.info(sb.toString());
            */
            HashMap<String, Integer> k = (HashMap<String, Integer>) p.value;
            //System.out.println("AAAAAAAAAAAAA" + taskInfo.srcTaskId + " "+  k.get("count"));

        }
    }

    /*
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        System.out.println("AAAAAAAAAAA " + this.hashCode());
        this.collector = collector;
        this.componentId = context.getThisTaskId();
    }

    @Override
    public void execute(Tuple tuple) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) { //TODO componentID should be taskID
        declarer.declareStream("Reporting", new Fields("componentId", "anomalyType", "anomalyData"));
    }
    */

    @Override
    public void cleanup() { }


    /**
     * This method is invoked in order to report some type of error or event to the Configurator bolt.
     * @param type
     * @param descr
     */
    private void report(int componentId1, int type, Object descr){
        System.out.println(type + " " + descr);
        collector.emit("Reporting", new Values(componentId1, type, descr));
    }
}
