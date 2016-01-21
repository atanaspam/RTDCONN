package uk.ac.gla.atanaspam.network;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.Constants;
import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.assertEquals;
import static uk.ac.gla.atanaspam.network.utils.MockTupleHelpers.*;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import uk.ac.gla.atanaspam.network.utils.StateKeeper;
import uk.ac.gla.atanaspam.pcapj.IPPacket;
import uk.ac.gla.atanaspam.pcapj.TCPFlags;
import uk.ac.gla.atanaspam.pcapj.TCPPacket;
import uk.ac.gla.atanaspam.pcapj.UDPPacket;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Map;

/**
 * @author atanaspam
 * @version 0.1
 * @created 19/01/2016
 */
public class NetworkNodeBoltTest {
    TCPPacket tcpPacket;
    UDPPacket udpPacket;
    IPPacket ipPacket;


    @BeforeClass
    public void init() {
        try {
            tcpPacket = new TCPPacket(new Long(123456789), "FF:FF:FF:FF:FF:FF", "FF:FF:FF:FF:FF:FF",
                    InetAddress.getByName("192.168.1.1"), InetAddress.getByName("192.168.1.1"), 1000, 1000,
                    new TCPFlags(false, false, false, false, false, false, false, false), null);
            udpPacket = new UDPPacket(new Long(123456789), "FF:FF:FF:FF:FF:FF", "FF:FF:FF:FF:FF:FF",
                    InetAddress.getByName("192.168.1.1"), InetAddress.getByName("192.168.1.1"), 1000, 1000,
                    null);
            ipPacket = new IPPacket(new Long(123456789), "FF:FF:FF:FF:FF:FF", "FF:FF:FF:FF:FF:FF",
                    InetAddress.getByName("192.168.1.1"), InetAddress.getByName("192.168.1.1"));
        } catch (Exception e) {

        }
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

    /**
     * Show that the system routes normal traffic and drops anomalous
     */
    @Test
    public void shouldReportIfPacketIsDropped() {
        // given
        Tuple tcpTuple = mockTCPPacketTuple(tcpPacket);
        OutputCollector collector = mock(OutputCollector.class);
        StateKeeper s = new StateKeeper();
        s.addBlockedFlag(new TCPFlags(false,false,false,false,false,false,false,false));
        NetworkNodeBolt bolt = new NetworkNodeBolt(s,false,3,0);
        bolt.prepare(mockConf(), mockContext(), collector);
        // when
        bolt.execute(tcpTuple);
        // then
        verify(collector).emit(eq("Reporting"), any(Values.class));
    }
    @Test
    public void shouldNotCheckPacketIfVerbosityIs0() {
        // given an anomalous TCP packet and disabled
        Tuple tcpTuple = mockTCPPacketTuple(tcpPacket);
        OutputCollector collector = mock(OutputCollector.class);
        StateKeeper s = new StateKeeper();
        s.addBlockedFlag(new TCPFlags(false,false,false,false,false,false,false,false));
        NetworkNodeBolt bolt = new NetworkNodeBolt(s,false,0,0);
        bolt.prepare(mockConf(), mockContext(), collector);
        // when the packet is processed
        bolt.execute(tcpTuple);
        // then it should be routed as usual
        verify(collector).emit(eq("TCPPackets"), any(Values.class));
    }
    @Test
    public void shouldEmitPacketIfChecksPassed() {
        // given a valid TCP packet
        Tuple tcpTuple = mockTCPPacketTuple(tcpPacket);
        OutputCollector collector = mock(OutputCollector.class);
        NetworkNodeBolt bolt = new NetworkNodeBolt();
        bolt.prepare(mockConf(), mockContext(), collector);
        // when it is processed
        bolt.execute(tcpTuple);
        // then it should be routed as usual
        verify(collector).emit(eq("TCPPackets"), any(Values.class));
    }

    /**
     * Show that given a packet with a specific anomaly the system reacts accordingly - Drop
     */
    @Test
    public void shouldDropIfBlockedPort() {
        // given a TCP packet with port 1000 and a rule that blocks port 1000
        Tuple tcpTuple = mockTCPPacketTuple(tcpPacket);
        OutputCollector collector = mock(OutputCollector.class);
        StateKeeper s = new StateKeeper();
        s.setBlockedPort(1000, true);
        NetworkNodeBolt bolt = new NetworkNodeBolt(s,false,3,0);
        bolt.prepare(mockConf(), mockContext(), collector);
        // when the packet is processed
        bolt.execute(tcpTuple);
        // then packet is dropped
        verify(collector).emit(eq("Reporting"), any(Values.class));
    }
    @Test
    public void shouldDropIfBlockedIp() {
        // given a TCP packet with an IP address 192.168.1.1 and a rule that blocks 192.168.1.1
        Tuple tcpTuple = mockTCPPacketTuple(tcpPacket);
        OutputCollector collector = mock(OutputCollector.class);
        StateKeeper s = new StateKeeper();
        try {
            s.addBlockedIpAddr(InetAddress.getByName("192.168.1.1"));
        } catch (UnknownHostException e) {
        }
        NetworkNodeBolt bolt = new NetworkNodeBolt(s, false, 3, 0);
        bolt.prepare(mockConf(), mockContext(), collector);
        // when the packet is processed
        bolt.execute(tcpTuple);
        // then the packet is dropped
        verify(collector).emit(eq("Reporting"), any(Values.class));
    }
    @Test
    public void shouldDropIfBlockedFlags() {
        // given a TCP packet with all flags to false and a rule to block those flags
        Tuple tcpTuple = mockTCPPacketTuple(tcpPacket);
        OutputCollector collector = mock(OutputCollector.class);
        StateKeeper s = new StateKeeper();
        s.addBlockedFlag(new TCPFlags(false,false,false,false,false,false,false,false));
        NetworkNodeBolt bolt = new NetworkNodeBolt(s,false,3,1);
        bolt.prepare(mockConf(), mockContext(), collector);
        // when the packet is processed
        bolt.execute(tcpTuple);
        // then it is dropped
        verify(collector).emit(eq("Reporting"), any(Values.class));
    }

//    @Test
//    public void shouldDropIfApplicationLayerCheckFails() {
//        // given a TCP packet with specific application layerdata signature
//        Tuple tcpTuple = mockTCPPacketTuple(tcpPacket);
//        OutputCollector collector = mock(OutputCollector.class);
//        StateKeeper s = new StateKeeper();
//        s.addApplicationLayerSignature(signature);
//        NetworkNodeBolt bolt = new NetworkNodeBolt(s,false,3,0);
//        bolt.prepare(mockConf(), mockContext(), collector);
//        // when the packet is processed
//        bolt.execute(tcpTuple);
//        // then packet is dropped
//        verify(collector).emit(eq("Reporting"), any(Values.class));
//    }

    @Test
    public void shouldIncrementPortHitCount() {
        // given a TCP packet with port 1000
        Tuple tcpTuple = mockTCPPacketTuple(tcpPacket);
        OutputCollector collector = mock(OutputCollector.class);
        StateKeeper s = new StateKeeper();
        NetworkNodeBolt bolt = new NetworkNodeBolt(s,true,0,0);
        bolt.prepare(mockConf(), mockContext(), collector);
        // when it is processed
        bolt.execute(tcpTuple);
        // then portHitCount is incremented
        assertEquals(new Long(1), s.getPortHitCount().get(1000));
    }
    @SuppressWarnings("SuspiciousMethodCalls")
    @Test
    public void shouldIncrementIpHitCount() {
        // given a TCP packet with an IP address 192.168.1.1 and a rule that blocks 192.168.1.1
        Tuple tcpTuple = mockTCPPacketTuple(tcpPacket);
        OutputCollector collector = mock(OutputCollector.class);
        StateKeeper s = new StateKeeper();
        NetworkNodeBolt bolt = new NetworkNodeBolt(s,true,0,0);
        bolt.prepare(mockConf(), mockContext(), collector);
        // when the packet is processed
        bolt.execute(tcpTuple);
        // then srcIpHitCount and dstIpHitCount are incremented
        assertEquals(new Long(1), s.getSrcIpHitCount().get(tcpTuple.getValueByField("srcIP")));
        assertEquals(new Long(1), s.getDestIpHitCount().get(tcpTuple.getValueByField("destIP")));
    }
    @Test
    public void shouldIncrementFlagCounts() {
        // given a TCP packet with specific flags
        boolean[] flags = new boolean[]{true,true,true,false,false,false,false,true};
        TCPPacket p = null;
        try {
            p = new TCPPacket(new Long(123456789), "FF:FF:FF:FF:FF:FF", "FF:FF:FF:FF:FF:FF",
                    InetAddress.getByName("192.168.1.1"), InetAddress.getByName("192.168.1.1"), 1000, 1000,
                    new TCPFlags(flags), null);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        Tuple tcpTuple = mockTCPPacketTuple(p);
        OutputCollector collector = mock(OutputCollector.class);
        StateKeeper s = new StateKeeper();
        NetworkNodeBolt bolt = new NetworkNodeBolt(s,true,0,0);
        bolt.prepare(mockConf(), mockContext(), collector);
        // when the packet is processed
        bolt.execute(tcpTuple);
        // then the respective flagCount is incremented
        for(int i=0; i<flags.length; i++){
            if (flags[i]) {
                assertEquals(new Long(1), s.getFlagCount()[i]);
            }
        }
    }


}