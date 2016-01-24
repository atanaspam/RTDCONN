package uk.ac.gla.atanaspam.network.utils;

/**
 * This file is copied from @link https://github.com/nathanmarz/storm-starter/blob/master/test/jvm/storm/starter/tools/MockTupleHelpers.java
 * TODO quote properly
 * @version 0.1
 * @created 20/01/2016
 */
import backtype.storm.Constants;
import backtype.storm.tuple.Tuple;
import uk.ac.gla.atanaspam.pcapj.IPPacket;
import uk.ac.gla.atanaspam.pcapj.TCPPacket;
import uk.ac.gla.atanaspam.pcapj.UDPPacket;

import static org.mockito.Mockito.*;

public final class MockTupleHelpers {
    private static final String ANY_NON_SYSTEM_COMPONENT_ID = "irrelevant_component_id";

    private MockTupleHelpers() {
    }

    public static Tuple mockTickTuple() {
        return mockTuple(Constants.SYSTEM_COMPONENT_ID, Constants.SYSTEM_TICK_STREAM_ID);
    }

    public static Tuple mockTuple(String componentId, String streamId) {
        Tuple tuple = mock(Tuple.class);
        when(tuple.getSourceComponent()).thenReturn(componentId);
        when(tuple.getSourceStreamId()).thenReturn(streamId);
        return tuple;
    }
    public static Tuple mockTCPPacketTuple(TCPPacket p) {
        Tuple tuple = mockTuple(ANY_NON_SYSTEM_COMPONENT_ID, "TCPPackets");
        when(tuple.getValueByField("timestamp")).thenReturn(p.getTimestamp());
        when(tuple.getValueByField("srcMAC")).thenReturn(p.getSourceMacAddress());
        when(tuple.getValueByField("destMAC")).thenReturn(p.getDestMacAddress());
        when(tuple.getValueByField("srcIP")).thenReturn(p.getSrc_ip());
        when(tuple.getValueByField("destIP")).thenReturn(p.getDst_ip());
        when(tuple.getValueByField("srcPort")).thenReturn(p.getSrc_port());
        when(tuple.getValueByField("destPort")).thenReturn(p.getDst_port());
        when(tuple.getValueByField("flags")).thenReturn(p.getFlags().toArray());
        when(tuple.getValueByField("data")).thenReturn(p.getData().getData());
        //TODO enable Application Layer
        return tuple;
    }

    public static Tuple mockUDPPacketTuple(UDPPacket p) {
        Tuple tuple = mockTuple(ANY_NON_SYSTEM_COMPONENT_ID, "UDPPackets");
        when(tuple.getValueByField("timestamp")).thenReturn(p.getTimestamp());
        when(tuple.getValueByField("srcMAC")).thenReturn(p.getSourceMacAddress());
        when(tuple.getValueByField("destMAC")).thenReturn(p.getDestMacAddress());
        when(tuple.getValueByField("srcIP")).thenReturn(p.getSrc_ip());
        when(tuple.getValueByField("destIP")).thenReturn(p.getDst_ip());
        when(tuple.getValueByField("srcPort")).thenReturn(p.getSrc_port());
        when(tuple.getValueByField("destPort")).thenReturn(p.getDst_port());
        when(tuple.getValueByField("data")).thenReturn(p.getData().getData());
        return tuple;
    }

    public static Tuple mockIPPacketTuple(IPPacket p) {
        Tuple tuple = mockTuple(ANY_NON_SYSTEM_COMPONENT_ID, "IPPackets");
        when(tuple.getValueByField("timestamp")).thenReturn(p.getTimestamp());
        when(tuple.getValueByField("srcMAC")).thenReturn(p.getSourceMacAddress());
        when(tuple.getValueByField("destMAC")).thenReturn(p.getDestMacAddress());
        when(tuple.getValueByField("srcIP")).thenReturn(p.getSrc_ip());
        when(tuple.getValueByField("destIP")).thenReturn(p.getDst_ip());
        return tuple;
    }

    public static Tuple mockReportingTuple(int taskId, int code, Object data) {
        Tuple tuple = mockTuple(ANY_NON_SYSTEM_COMPONENT_ID, "Reporting");
        when(tuple.getValueByField("taskId")).thenReturn(taskId);
        when(tuple.getValueByField("anomalyType")).thenReturn(code);
        when(tuple.getValueByField("anomalyData")).thenReturn(data);
        return tuple;
    }

    public static Tuple mockConfigTuple(int dest, int code, Object setting) {
        Tuple tuple = mockTuple(ANY_NON_SYSTEM_COMPONENT_ID, "Configure");
        when(tuple.getValueByField("taskId")).thenReturn(dest);
        when(tuple.getValueByField("code")).thenReturn(code);
        when(tuple.getValueByField("setting")).thenReturn(setting);
        return tuple;
    }
}
