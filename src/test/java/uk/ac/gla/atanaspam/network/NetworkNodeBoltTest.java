package uk.ac.gla.atanaspam.network;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.Constants;
import static org.mockito.Mockito.*;
import org.testng.annotations.Test;
import uk.ac.gla.atanaspam.network.utils.StateKeeper;
import uk.ac.gla.atanaspam.pcapj.TCPFlags;
import java.net.InetAddress;
import java.util.Map;

/**
 * @author atanaspam
 * @version 0.1
 * @created 19/01/2016
 */
public class NetworkNodeBoltTest {
    private static final String ANY_NON_SYSTEM_COMPONENT_ID = "irrelevant_component_id";

    private Tuple mockTCPPacketTuple() {
        Tuple tuple = mockTuple(ANY_NON_SYSTEM_COMPONENT_ID, "TCPPackets");
        when(tuple.getValueByField("timestamp")).thenReturn(new Long(123456789));
        when(tuple.getValueByField("srcMAC")).thenReturn("FF:FF:FF:FF:FF:FF");
        when(tuple.getValueByField("destMAC")).thenReturn("FF:FF:FF:FF:FF:FF");
        try{
            when(tuple.getValueByField("srcIP")).thenReturn(InetAddress.getByName("192.168.1.1"));
            when(tuple.getValueByField("destIP")).thenReturn(InetAddress.getByName("192.168.1.1"));
        }catch(Exception e){
           // LOG.error("An error occurred while parsing the src_ip address.");
        }
        when(tuple.getValueByField("srcPort")).thenReturn(1000);
        when(tuple.getValueByField("destPort")).thenReturn(1000);
        when(tuple.getValueByField("flags")).thenReturn(new boolean[]{false,false,false,false,false,false,false,false});
        return tuple;
    }

    public static Tuple mockTuple(String componentId, String streamId) {
        Tuple tuple = mock(Tuple.class);
        when(tuple.getSourceComponent()).thenReturn(componentId);
        when(tuple.getSourceStreamId()).thenReturn(streamId);
        return tuple;
    }

    public static Map mockConf(){
        Config conf = mock(Config.class);
        when(conf.get("timeCheck")).thenReturn(false);
        return conf;
    }

    public static TopologyContext mockContext(){
        TopologyContext context = mock(TopologyContext.class);
        when(context.getThisTaskId()).thenReturn(1);
        return context;
    }

    public static Tuple mockTickTuple() {
        return mockTuple(Constants.SYSTEM_COMPONENT_ID, Constants.SYSTEM_TICK_STREAM_ID);
    }

    @Test
    public void shouldReportIfPacketIsDropped() {
        // given
        Tuple tcpTuple = mockTCPPacketTuple();
        OutputCollector collector = mock(OutputCollector.class);
        StateKeeper s = new StateKeeper();
        s.addBlockedFlag(new TCPFlags(false,false,false,false,false,false,false,false));
        NetworkNodeBolt bolt = new NetworkNodeBolt(s);
        bolt.prepare(mockConf(), mockContext(), collector);

        // when
        bolt.execute(tcpTuple);

        // then
        verify(collector).emit(eq("Reporting"), any(Values.class));
    }

    @Test
    public void shouldEmitPacketIfChecksPassed() {
        // given
        Tuple tcpTuple = mockTCPPacketTuple();
        OutputCollector collector = mock(OutputCollector.class);
        NetworkNodeBolt bolt = new NetworkNodeBolt();
        bolt.prepare(mockConf(), mockContext(), collector);

        // when
        bolt.execute(tcpTuple);

        // then
        verify(collector).emit(eq("TCPPackets"), any(Values.class));
    }


}