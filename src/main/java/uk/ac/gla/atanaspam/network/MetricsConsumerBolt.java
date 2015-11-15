package uk.ac.gla.atanaspam.network;

import backtype.storm.Config;
import backtype.storm.metric.api.IMetricsConsumer;
import backtype.storm.task.IBolt;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import java.util.Collection;
import java.util.Map;

/**
 * @deprecated
 * @author atanaspam
 * @version 0.1
 * @created 08/11/2015
 */

public class MetricsConsumerBolt implements IBolt {
    IMetricsConsumer _metricsConsumer;
    OutputCollector _collector;
    Object _registrationArgument;

    public MetricsConsumerBolt(Object registrationArgument) {
        _registrationArgument = registrationArgument;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        try {
            _metricsConsumer = new MetricsConsumer();
        } catch (Exception e) {
            throw new RuntimeException("Could not instantiate a class listed in config under section " +
                    Config.TOPOLOGY_METRICS_CONSUMER_REGISTER + " with fully qualified name MetricsConsumer",  e);
        }
        _metricsConsumer.prepare(stormConf, _registrationArgument, context, collector);
        _collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        _metricsConsumer.handleDataPoints((IMetricsConsumer.TaskInfo)input.getValue(0), (Collection)input.getValue(1));
        _collector.ack(input);
    }

    @Override
    public void cleanup() {
        _metricsConsumer.cleanup();
    }

}
