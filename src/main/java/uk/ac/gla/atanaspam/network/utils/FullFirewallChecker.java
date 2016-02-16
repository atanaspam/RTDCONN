package uk.ac.gla.atanaspam.network.utils;

import uk.ac.gla.atanaspam.network.ChecksPerformer;
import uk.ac.gla.atanaspam.network.GenericPacket;
import uk.ac.gla.atanaspam.pcapj.PacketContents;
import uk.ac.gla.atanaspam.pcapj.TCPFlags;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author atanaspam
 * @version 0.1
 * @created 15/02/2016
 */
public class FullFirewallChecker implements ChecksPerformer, Serializable{


    private BitSet blockedSrcPorts;
    private BitSet blockedDstPorts;
    private HashSet<InetAddress> blockedSrcIpAddr;
    private HashSet<InetAddress> blockedDstIpAddr;
    private HashSet<TCPFlags> blockedFlags;
    private ArrayList<Pattern> blockedData;

    public FullFirewallChecker() {
        blockedSrcPorts = new BitSet(65536);
        blockedDstPorts = new BitSet(65536);
        blockedSrcIpAddr = new HashSet<>();
        blockedDstIpAddr = new HashSet<>();
        blockedFlags = new HashSet<>();
        blockedData = new ArrayList<>();
    }

    /**
     * Performs checks upon a packet instance depending on the verbosity specified
     * @param code an integer representing the verbosity value (0 - do nothing, 1 - check ports, 2 - check IP's, 3 - check Flags)
     * @return true if all the checks succeed, false otherwise
     */
    @Override
    public boolean performChecks(GenericPacket packet) {
        boolean status = true;
        if (packet.getType().equals(GenericPacket.PacketType.TCP)){
            status = status & checkSrcPort(packet.getSrc_port());
            status = status & checkDstPort(packet.getDst_port());
            status = status & checkSrcIP(packet.getSrc_ip());
            status = status & checkDstIP(packet.getDst_ip());
            status = status & checkFlags(packet.getFlags());
            status = status & checkApplicationLayer(packet.getData());

        }
        else if (packet.getType().equals(GenericPacket.PacketType.UDP)){
            status = status & checkSrcPort(packet.getSrc_port());
            status = status & checkDstPort(packet.getDst_port());
            status = status & checkSrcIP(packet.getSrc_ip());
            status = status & checkDstIP(packet.getDst_ip());
            status = status & checkApplicationLayer(packet.getData());

        }
        else if (packet.getType().equals(GenericPacket.PacketType.IP)){
            status = status & checkSrcIP(packet.getSrc_ip());
            status = status & checkDstIP(packet.getDst_ip());
        }
        else return false;

        return status;
    }

    @Override
    public void addIpAddress(InetAddress addr, int code) {
        if (code == 1){
            blockedSrcIpAddr.add(addr);
        }else{
            blockedDstIpAddr.add(addr);
        }
    }

    @Override
    public void removeIpAddress(InetAddress addr, int code) {
        if (code == 1){
            blockedSrcIpAddr.remove(addr);
        }else{
            blockedDstIpAddr.remove(addr);
        }
    }

    @Override
    public void addPort(int port, int code) {
        if (code == 1){
            blockedSrcPorts.set(port, true);
        }else{
            blockedDstPorts.set(port, true);
        }
    }

    @Override
    public void removePort(int port, int code) {
        if (code == 1){
            blockedSrcPorts.set(port, false);
        }else{
            blockedDstPorts.set(port, false);
        }
    }

    @Override
    public void addFlag(TCPFlags flag, int code) {
        blockedFlags.add(flag);
    }

    @Override
    public void removeFlag(TCPFlags flag, int code) {
        blockedFlags.remove(flag);
    }

    @Override
    public void addPattern(Pattern pattern, int code) {
        blockedData.add(pattern);
    }

    @Override
    public void removePattern(Pattern pattern, int code) {
        blockedData.remove(pattern);
    }


    /**
     * Check a src port number against the rules specified for the bolt
     * @param port the port to be checked
     * @return true if no problem is detected, false otherwise
     */
    private boolean checkSrcPort(int port){
        return !blockedSrcPorts.get(port);
    }

    /**
     * Check a dst port number against the rules specified for the bolt
     * @param port the port to be checked
     * @return true if no problem is detected, false otherwise
     */
    private boolean checkDstPort(int port){
        return !blockedDstPorts.get(port);
    }

    /**
     * Check an IP address against the rules specified for the bolt
     * @param addr the IP address to be checked
     * @return true if no problem is detected, false otherwise
     */
    private boolean checkSrcIP(InetAddress addr){
        return !blockedSrcIpAddr.contains(addr);

    }

    /**
     * Check an IP address against the rules specified for the bolt
     * @param addr the IP address to be checked
     * @return true if no problem is detected, false otherwise
     */
    private boolean checkDstIP(InetAddress addr){
        return !blockedDstIpAddr.contains(addr);

    }

    /**
     * Check a set of flags against the rules specified for the bolt
     * @param flags the flags to be checked
     * @return true if no problem is detected, false otherwise
     */
    private boolean checkFlags(TCPFlags flags){
        return !blockedFlags.contains(flags);
    }

    /**
     * Check the actual contents of a packet for any anomalies
     * The check is based on searching for signatures within the data field
     * @return true if no problem is detected, false otherwise
     */
    private boolean checkApplicationLayer(PacketContents data){
        if (data == null)
            return true;
        else{
            for (Pattern p : blockedData){
                Matcher m = p.matcher(new String(data.getData()));
                if (m.find()) {return false;}
            }
            return true;
        }
    }
}
