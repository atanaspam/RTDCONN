package uk.ac.gla.atanaspam.network;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import junit.framework.TestCase;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;
import static uk.ac.gla.atanaspam.network.utils.MockTupleHelpers.mockReportingTuple;

/**
 * @author atanaspam
 * @version 0.1
 * @created 21/01/2016
 */
public class NetworkConfiguratorBoltTest {

    public static Map mockConf(){
        Config conf = mock(Config.class);
        when(conf.get("timeCheck")).thenReturn(false);
        when(conf.get("boltNum")).thenReturn(new Long(2));
        return conf;
    }

    public static TopologyContext mockContext(){
        TopologyContext context = mock(TopologyContext.class);
        when(context.getThisTaskId()).thenReturn(1);
        HashMap taskToComponent = new HashMap<Integer, String>();
        taskToComponent.put(2,"node_0_lvl_0");
        when(context.getTaskToComponent()).thenReturn(taskToComponent);
        return context;
    }

    /**
     * Show that the system routes normal traffic and drops anomalous
     */
    @Test
    public void shouldRecordPortHits() {
        // given a report for Many hits to port 1000 from task 2
        Tuple reportingTuple = mockReportingTuple(2,1,1000);
        OutputCollector collector = mock(OutputCollector.class);
        NetworkConfiguratorBolt bolt = new NetworkConfiguratorBolt();
        bolt.prepare(mockConf(), mockContext(), collector);
        // when the report is processed
        bolt.execute(reportingTuple);
        // then the report should be stored in the state under task 2
        assertEquals(1, bolt.state.getPortHit(1000)[2]);
    }

    @Test
    public void shouldRecordUnexpectedPortHits() {
        // given a report for an unexpected hit to port 1000 from task 2
        Tuple reportingTuple = mockReportingTuple(2,2,1000);
        OutputCollector collector = mock(OutputCollector.class);
        NetworkConfiguratorBolt bolt = new NetworkConfiguratorBolt();
        bolt.prepare(mockConf(), mockContext(), collector);
        // when the report is processed
        bolt.execute(reportingTuple);
        // then the report should be stored in the state under task 2
        assertEquals(1, bolt.state.getUnexpPortHit(1000)[2]);
    }

    @Test
    public void shouldRecordIpHits() {
        // given a report for many hits to 192.168.1.1 form task 2
        InetAddress ip = null;
        try {
           ip = InetAddress.getByName("192.168.1.1");
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        Tuple reportingTuple = mockReportingTuple(2,3,ip);
        OutputCollector collector = mock(OutputCollector.class);
        NetworkConfiguratorBolt bolt = new NetworkConfiguratorBolt();
        bolt.prepare(mockConf(), mockContext(), collector);
        // when the report is processed
        bolt.execute(reportingTuple);
        // then the report should be stored in the state under task 2
        assertEquals(1, bolt.state.getIpHit(ip)[2]);
    }

    @Test
    public void shouldRecordUnexpectedIpHits() {
        // given a report for unexpected hits to 192.168.1.1 from task 2
        InetAddress ip = null;
        try {
            ip = InetAddress.getByName("192.168.1.1");
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        Tuple reportingTuple = mockReportingTuple(2,5,ip);
        OutputCollector collector = mock(OutputCollector.class);
        NetworkConfiguratorBolt bolt = new NetworkConfiguratorBolt();
        bolt.prepare(mockConf(), mockContext(), collector);
        // when the report is processed
        bolt.execute(reportingTuple);
        // then the report should be stored in the state under task 2
        assertEquals(1, bolt.state.getUnexpIpHit(ip)[2]);
    }

    @Test
    public void shouldRecordDroppedPacket() {
        // given a report for a dropped packet from task 2
        InetAddress ip = null;
        try {
            ip = InetAddress.getByName("192.168.1.1");
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        Tuple reportingTuple = mockReportingTuple(2,6,ip);
        OutputCollector collector = mock(OutputCollector.class);
        NetworkConfiguratorBolt bolt = new NetworkConfiguratorBolt();
        bolt.prepare(mockConf(), mockContext(), collector);
        // when the report is processed
        bolt.execute(reportingTuple);
        // then the report should be stored in the state under task 2
        assertEquals(1, bolt.state.getDroppedPacket(ip)[2]);
    }

    @Test
    public void shouldRecordBadFlags() {
        // given a report for hits with bad flags from taks 2
        Tuple reportingTuple = mockReportingTuple(2,7,1);
        OutputCollector collector = mock(OutputCollector.class);
        NetworkConfiguratorBolt bolt = new NetworkConfiguratorBolt();
        bolt.prepare(mockConf(), mockContext(), collector);
        // when the report is processed
        bolt.execute(reportingTuple);
        // then the report should be stored in the state under task 2
        assertEquals(1, bolt.state.getBadFlag(1)[2]);
    }

}