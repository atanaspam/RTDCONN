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
import static uk.ac.gla.atanaspam.network.utils.MockTupleHelpers.mockReportingTuple;

/**
 * @author atanaspam
 * @version 0.1
 * @created 21/01/2016
 */
public class NetworkConfiguratorBoltTest extends TestCase {

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
        // given
        Tuple reportingTuple = mockReportingTuple(2,1,1000);
        OutputCollector collector = mock(OutputCollector.class);
        NetworkConfiguratorBolt bolt = new NetworkConfiguratorBolt();
        bolt.prepare(mockConf(), mockContext(), collector);
        // when
        bolt.execute(reportingTuple);
        // then
        assertEquals(1, bolt.state.getPortHit(1000)[2]);
    }

    @Test
    public void shouldRecordUnexpectedPortHits() {
        // given
        Tuple reportingTuple = mockReportingTuple(2,2,1000);
        OutputCollector collector = mock(OutputCollector.class);
        NetworkConfiguratorBolt bolt = new NetworkConfiguratorBolt();
        bolt.prepare(mockConf(), mockContext(), collector);
        // when
        bolt.execute(reportingTuple);
        // then
        assertEquals(1, bolt.state.getUnexpPortHit(1000)[2]);
    }

    @Test
    public void shouldRecordIpHits() {
        // given
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
        // when
        bolt.execute(reportingTuple);
        // then
        assertEquals(1, bolt.state.getIpHit(ip)[2]);
    }

    @Test
    public void shouldRecordUnexpectedIpHits() {
        // given
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
        // when
        bolt.execute(reportingTuple);
        // then
        assertEquals(1, bolt.state.getUnexpIpHit(ip)[2]);
    }

    @Test
    public void shouldRecordDroppedPacket() {
        // given
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
        // when
        bolt.execute(reportingTuple);
        // then
        assertEquals(1, bolt.state.getDroppedPacket(ip)[2]);
    }

    /** To be enabled when bad Flags reporting is implemented... */
//    @Test
//    public void shouldRecordBadFlags() {
//        // given
//        InetAddress ip = null;
//        try {
//            ip = InetAddress.getByName("192.168.1.1");
//        } catch (UnknownHostException e) {
//            e.printStackTrace();
//        }
//        Tuple reportingTuple = mockReportingTuple(2,6,ip);
//        OutputCollector collector = mock(OutputCollector.class);
//        NetworkConfiguratorBolt bolt = new NetworkConfiguratorBolt();
//        bolt.prepare(mockConf(), mockContext(), collector);
//        // when
//        bolt.execute(reportingTuple);
//        // then
//        assertEquals(1, bolt.state.getDroppedPacket(ip)[2]);
//    }

}