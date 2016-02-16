package uk.ac.gla.atanaspam.network;

import backtype.storm.Config;
import backtype.storm.scheduler.Cluster;
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
import sun.nio.ch.Net;
import uk.ac.gla.atanaspam.network.utils.*;
import uk.ac.gla.atanaspam.pcapj.*;

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
                    InetAddress.getByName("192.168.1.1"), InetAddress.getByName("192.168.1.2"), 1000, 2000,
                    new TCPFlags(false, false, false, false, false, false, false, false), new PacketContents(new byte[0]));
            udpPacket = new UDPPacket(new Long(123456789), "FF:FF:FF:FF:FF:FF", "FF:FF:FF:FF:FF:FF",
                    InetAddress.getByName("192.168.1.1"), InetAddress.getByName("192.168.1.2"), 1000, 2000,
                    new PacketContents(new byte[0]));
            ipPacket = new IPPacket(new Long(123456789), "FF:FF:FF:FF:FF:FF", "FF:FF:FF:FF:FF:FF",
                    InetAddress.getByName("192.168.1.1"), InetAddress.getByName("192.168.1.2"));
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
        ChecksPerformer c = new BasicFirewallChecker();
        c.addFlag(new TCPFlags(false,false,false,false,false,false,false,false),1);
        NetworkNodeBolt bolt = new NetworkNodeBolt(new EmptyStatisticsGatherer(), c, 1, 0, 0);
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
        ChecksPerformer c = new BasicFirewallChecker();
        c.addFlag(new TCPFlags(false,false,false,false,false,false,false,false), 1);
        NetworkNodeBolt bolt = new NetworkNodeBolt(new EmptyStatisticsGatherer(), new EmptyFirewallChecker(), 0, 0, 0);
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
    public void shouldDropIfBlockedSrcTCPPort() {
        // given a TCP packet with port 1000 and a rule that blocks port 1000
        Tuple tcpTuple = mockTCPPacketTuple(tcpPacket);
        OutputCollector collector = mock(OutputCollector.class);
        ChecksPerformer c = new BasicFirewallChecker();
        c.addPort(1000, 1);
        NetworkNodeBolt bolt = new NetworkNodeBolt(new EmptyStatisticsGatherer(), c, 1, 0, 0);
        bolt.prepare(mockConf(), mockContext(), collector);
        // when the packet is processed
        bolt.execute(tcpTuple);
        // then packet is dropped
        verify(collector).emit(eq("Reporting"), any(Values.class));
    }
    @Test
    public void shouldDropIfBlockedDstTCPPort() {
        // given a TCP packet with port 1000 and a rule that blocks port 1000
        Tuple tcpTuple = mockTCPPacketTuple(tcpPacket);
        OutputCollector collector = mock(OutputCollector.class);
        ChecksPerformer c = new BasicFirewallChecker();
        c.addPort(2000, 0);
        NetworkNodeBolt bolt = new NetworkNodeBolt(new EmptyStatisticsGatherer(), c, 1, 0, 0);
        bolt.prepare(mockConf(), mockContext(), collector);
        // when the packet is processed
        bolt.execute(tcpTuple);
        // then packet is dropped
        verify(collector).emit(eq("Reporting"), any(Values.class));
    }
    @Test
    public void shouldDropIfBlockedSrcUDPPort() {
        // given a UDP packet with port 1000 and a rule that blocks port 1000
        Tuple udpTuple = mockUDPPacketTuple(udpPacket);
        OutputCollector collector = mock(OutputCollector.class);
        ChecksPerformer c = new BasicFirewallChecker();
        c.addPort(1000, 1);
        NetworkNodeBolt bolt = new NetworkNodeBolt(new EmptyStatisticsGatherer(), c, 1, 0, 0);
        bolt.prepare(mockConf(), mockContext(), collector);
        // when the packet is processed
        bolt.execute(udpTuple);
        // then packet is dropped
        verify(collector).emit(eq("Reporting"), any(Values.class));
    }
    @Test
    public void shouldDropIfBlockedDstUDPPort() {
        // given a UDP packet with port 1000 and a rule that blocks port 1000
        Tuple udpTuple = mockUDPPacketTuple(udpPacket);
        OutputCollector collector = mock(OutputCollector.class);
        ChecksPerformer c = new BasicFirewallChecker();
        c.addPort(2000, 0);
        NetworkNodeBolt bolt = new NetworkNodeBolt(new EmptyStatisticsGatherer(), c, 1, 0, 0);
        bolt.prepare(mockConf(), mockContext(), collector);
        // when the packet is processed
        bolt.execute(udpTuple);
        // then packet is dropped
        verify(collector).emit(eq("Reporting"), any(Values.class));
    }
    @Test
    public void shouldDropIfBlockedTCPSrcIp() {
        // given a TCP packet with an IP address 192.168.1.1 and a rule that blocks 192.168.1.1
        Tuple tcpTuple = mockTCPPacketTuple(tcpPacket);
        OutputCollector collector = mock(OutputCollector.class);
        ChecksPerformer c = new BasicFirewallChecker();
        try {
            c.addIpAddress(InetAddress.getByName("192.168.1.1"), 1);
        } catch (UnknownHostException e) {
        }
        NetworkNodeBolt bolt = new NetworkNodeBolt(new EmptyStatisticsGatherer(), c, 1, 0, 0);
        bolt.prepare(mockConf(), mockContext(), collector);
        // when the packet is processed
        bolt.execute(tcpTuple);
        // then the packet is dropped
        verify(collector).emit(eq("Reporting"), any(Values.class));
    }
    @Test
    public void shouldDropIfBlockedTCPDstIp() {
        // given a TCP packet with an IP address 192.168.1.1 and a rule that blocks 192.168.1.1
        Tuple tcpTuple = mockTCPPacketTuple(tcpPacket);
        OutputCollector collector = mock(OutputCollector.class);
        ChecksPerformer c = new BasicFirewallChecker();
        try {
            c.addIpAddress(InetAddress.getByName("192.168.1.2"), 0);
        } catch (UnknownHostException e) {
        }
        NetworkNodeBolt bolt = new NetworkNodeBolt(new EmptyStatisticsGatherer(), c, 1, 0, 0);
        bolt.prepare(mockConf(), mockContext(), collector);
        // when the packet is processed
        bolt.execute(tcpTuple);
        // then the packet is dropped
        verify(collector).emit(eq("Reporting"), any(Values.class));
    }
    @Test
    public void shouldDropIfBlockedUDPSrcIp() {
        // given a UDP packet with an IP address 192.168.1.1 and a rule that blocks 192.168.1.1
        Tuple udpTuple = mockUDPPacketTuple(udpPacket);
        OutputCollector collector = mock(OutputCollector.class);
        ChecksPerformer c = new BasicFirewallChecker();
        try {
            c.addIpAddress(InetAddress.getByName("192.168.1.1"), 1);
        } catch (UnknownHostException e) {
        }
        NetworkNodeBolt bolt = new NetworkNodeBolt(new EmptyStatisticsGatherer(), c, 1, 0, 0);
        bolt.prepare(mockConf(), mockContext(), collector);
        // when the packet is processed
        bolt.execute(udpTuple);
        // then the packet is dropped
        verify(collector).emit(eq("Reporting"), any(Values.class));
    }
    @Test
    public void shouldDropIfBlockedUDPDstIp() {
        // given a UDP packet with an IP address 192.168.1.1 and a rule that blocks 192.168.1.1
        Tuple udpTuple = mockUDPPacketTuple(udpPacket);
        OutputCollector collector = mock(OutputCollector.class);
        ChecksPerformer c = new BasicFirewallChecker();
        try {
            c.addIpAddress(InetAddress.getByName("192.168.1.2"), 0);
        } catch (UnknownHostException e) {
        }
        NetworkNodeBolt bolt = new NetworkNodeBolt(new EmptyStatisticsGatherer(), c, 1, 0, 0);
        bolt.prepare(mockConf(), mockContext(), collector);
        // when the packet is processed
        bolt.execute(udpTuple);
        // then the packet is dropped
        verify(collector).emit(eq("Reporting"), any(Values.class));
    }
    @Test
    public void shouldDropIfBlockedSrcIPIp() {
        // given an IP packet with an IP address 192.168.1.1 and a rule that blocks 192.168.1.1
        Tuple ipTuple = mockIPPacketTuple(ipPacket);
        OutputCollector collector = mock(OutputCollector.class);
        ChecksPerformer c = new BasicFirewallChecker();
        try {
            c.addIpAddress(InetAddress.getByName("192.168.1.1"), 1);
        } catch (UnknownHostException e) {
        }
        NetworkNodeBolt bolt = new NetworkNodeBolt(new EmptyStatisticsGatherer(), c, 1, 0, 0);
        bolt.prepare(mockConf(), mockContext(), collector);
        // when the packet is processed
        bolt.execute(ipTuple);
        // then the packet is dropped
        verify(collector).emit(eq("Reporting"), any(Values.class));
    }
    @Test
    public void shouldDropIfBlockedDstIPIp() {
        // given an IP packet with an IP address 192.168.1.1 and a rule that blocks 192.168.1.1
        Tuple ipTuple = mockIPPacketTuple(ipPacket);
        OutputCollector collector = mock(OutputCollector.class);
        ChecksPerformer c = new BasicFirewallChecker();
        try {
            c.addIpAddress(InetAddress.getByName("192.168.1.2"), 0);
        } catch (UnknownHostException e) {
        }
        NetworkNodeBolt bolt = new NetworkNodeBolt(new EmptyStatisticsGatherer(), c, 1, 0, 0);
        bolt.prepare(mockConf(), mockContext(), collector);
        // when the packet is processed
        bolt.execute(ipTuple);
        // then the packet is dropped
        verify(collector).emit(eq("Reporting"), any(Values.class));
    }
    @Test
    public void shouldDropIfBlockedFlags() {
        // given a TCP packet with all flags to false and a rule to block those flags
        Tuple tcpTuple = mockTCPPacketTuple(tcpPacket);
        OutputCollector collector = mock(OutputCollector.class);
        ChecksPerformer c = new BasicFirewallChecker();
        c.addFlag(new TCPFlags(false,false,false,false,false,false,false,false), 1);
        NetworkNodeBolt bolt = new NetworkNodeBolt(new EmptyStatisticsGatherer(), c, 1, 0, 0);
        bolt.prepare(mockConf(), mockContext(), collector);
        // when the packet is processed
        bolt.execute(tcpTuple);
        // then it is dropped
        verify(collector).emit(eq("Reporting"), any(Values.class));
    }

    @Test
    public void shouldDropIfTCPApplicationLayerCheckFails() {
        // given a TCP packet with specific application layer data signature
        TCPPacket p = null;
        try {
            p = new TCPPacket(new Long(123456789), "FF:FF:FF:FF:FF:FF", "FF:FF:FF:FF:FF:FF",
                    InetAddress.getByName("192.168.1.1"), InetAddress.getByName("192.168.1.1"), 1000, 1000,
                    new TCPFlags(false,false,false,false,false,false,false,false),
                    new PacketContents("This is not permitted".getBytes()));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        Tuple tcpTuple = mockTCPPacketTuple(p);
        OutputCollector collector = mock(OutputCollector.class);
        ChecksPerformer c = new DPIFirewallChecker();
        c.addPattern(java.util.regex.Pattern.compile("This is not permitted"),1);
        NetworkNodeBolt bolt = new NetworkNodeBolt(new EmptyStatisticsGatherer(), c, 2, 0, 0);
        bolt.prepare(mockConf(), mockContext(), collector);
        // when the packet is processed
        bolt.execute(tcpTuple);
        // then packet is dropped
        verify(collector).emit(eq("Reporting"), any(Values.class));
    }

    @Test
    public void shouldDropIfUDPApplicationLayerCheckFails() {
        // given a TCP packet with specific application layer data signature
        UDPPacket p = null;
        try {
            p = new UDPPacket(new Long(123456789), "FF:FF:FF:FF:FF:FF", "FF:FF:FF:FF:FF:FF",
                    InetAddress.getByName("192.168.1.1"), InetAddress.getByName("192.168.1.1"), 1000, 1000,
                    new PacketContents("This is not permitted".getBytes()));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        Tuple udpTuple = mockUDPPacketTuple(p);
        OutputCollector collector = mock(OutputCollector.class);
        ChecksPerformer c = new DPIFirewallChecker();
        c.addPattern(java.util.regex.Pattern.compile("This is not permitted"),1);
        NetworkNodeBolt bolt = new NetworkNodeBolt(new EmptyStatisticsGatherer(), c, 2, 0, 0);
        bolt.prepare(mockConf(), mockContext(), collector);
        // when the packet is processed
        bolt.execute(udpTuple);
        // then packet is dropped
        verify(collector).emit(eq("Reporting"), any(Values.class));
    }

    @Test
    public void shouldIncrementPortHitCountWithClassicCMA() {
        // given a TCP packet with port 1000
        StatisticsGatherer s;
        Tuple tcpTuple = mockTCPPacketTuple(tcpPacket);
        OutputCollector collector = mock(OutputCollector.class);
        s = new ClassicCMAStatistics();
        NetworkNodeBolt bolt = new NetworkNodeBolt(s, new EmptyFirewallChecker(), 0, 1, 0);
        bolt.prepare(mockConf(), mockContext(), collector);
        // when it is processed
        bolt.execute(tcpTuple);
        // then portHitCount is incremented
        ClassicCMAStatistics s1 = (ClassicCMAStatistics) s;
        assertEquals(1, (int) s1.getSrcPortHitCount().get(1000));
        assertEquals(1, (int) s1.getDstPortHitCount().get(2000));
    }


    @SuppressWarnings("SuspiciousMethodCalls")
    @Test
    public void shouldIncrementIpHitCount() {
        // given a TCP packet with an IP address 192.168.1.1 and a rule that blocks 192.168.1.1
        StatisticsGatherer s;
        Tuple tcpTuple = mockTCPPacketTuple(tcpPacket);
        OutputCollector collector = mock(OutputCollector.class);
        s = new ClassicCMAStatistics();
        NetworkNodeBolt bolt = new NetworkNodeBolt(s, new EmptyFirewallChecker(), 0, 1, 0);
        bolt.prepare(mockConf(), mockContext(), collector);
        // when it is processed
        bolt.execute(tcpTuple);
        // then portHitCount is incremented
        ClassicCMAStatistics s1 = (ClassicCMAStatistics) s;
        assertEquals(1, (int) s1.getSrcIpHitCount().get(tcpTuple.getValueByField("srcIP")));
        assertEquals(1, (int) s1.getDstIpHitCount().get(tcpTuple.getValueByField("destIP")));
    }
    @Test
    public void shouldIncrementFlagCounts() {
        // given a TCP packet with specific flags
        boolean[] flags = new boolean[]{true,true,true,false,false,false,false,true};
        TCPPacket p = null;
        try {
            p = new TCPPacket(new Long(123456789), "FF:FF:FF:FF:FF:FF", "FF:FF:FF:FF:FF:FF",
                    InetAddress.getByName("192.168.1.1"), InetAddress.getByName("192.168.1.1"), 1000, 1000,
                    new TCPFlags(flags), new PacketContents(new byte[0]));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        Tuple tcpTuple = mockTCPPacketTuple(p);
        OutputCollector collector = mock(OutputCollector.class);
        StatisticsGatherer s = new ClassicCMAStatistics();
        NetworkNodeBolt bolt = new NetworkNodeBolt(s, new EmptyFirewallChecker(), 0, 1, 0);
        bolt.prepare(mockConf(), mockContext(), collector);
        // when the packet is processed
        bolt.execute(tcpTuple);
        // then the respective flagCount is incremented
        ClassicCMAStatistics s1 = (ClassicCMAStatistics) s;
        for(int i=0; i<flags.length; i++){
            if (flags[i]) {
                assertEquals(1, s1.getFlagCount()[i]);
            }
        }
    }


}