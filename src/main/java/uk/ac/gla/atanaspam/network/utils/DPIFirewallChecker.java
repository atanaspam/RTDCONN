package uk.ac.gla.atanaspam.network.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.gla.atanaspam.network.ChecksPerformer;
import uk.ac.gla.atanaspam.network.GenericPacket;
import uk.ac.gla.atanaspam.pcapj.PacketContents;
import uk.ac.gla.atanaspam.pcapj.TCPFlags;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Represents a module capable fo performing a set of IDS detections
 * @author atanaspam
 * @version 0.1
 * @created 16/02/2016
 */
public class DPIFirewallChecker implements ChecksPerformer, Serializable{

    private static final Logger LOG = LoggerFactory.getLogger(DPIFirewallChecker.class);
    private ArrayList<Pattern> blockedData;

    /**
     * Basic constructor to initialize the data structures
     */
    public DPIFirewallChecker() {
        blockedData = new ArrayList<>();
    }

    /**
     * Performs checks upon a packet instance depending on the verbosity specified
     * @param packet an integer representing the verbosity value (0 - do nothing, 1 - check ports, 2 - check IP's, 3 - check Flags)
     * @return true if all the checks succeed, false otherwise
     */
    @Override
    public boolean performChecks(GenericPacket packet) {
        if (packet.getType() == 2){
            return checkApplicationLayer(packet.getData());
        }
        else if (packet.getType() == 3){
            return checkApplicationLayer(packet.getData());
        }
        else return true;
    }

    @Override
    public void addIpAddress(InetAddress addr, int code) {
       return;
    }

    @Override
    public void removeIpAddress(InetAddress addr, int code) {
        return;
    }

    @Override
    public void addPort(int port, int code) {
        return;
    }

    @Override
    public void removePort(int port, int code) {
        return;
    }

    @Override
    public void addFlag(TCPFlags flag, int code) {
        return;
    }

    @Override
    public void removeFlag(TCPFlags flag, int code) {
        return;
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
     * Check the actual contents of a packet for any anomalies
     * The check is based on searching for signatures within the data field
     * @return true if no problem is detected, false otherwise
     */
    private boolean checkApplicationLayer(PacketContents data){
        if (data == null)
            return true;
        else{
            for (Pattern p : blockedData){
                Matcher m = null;
                try {
                    m = p.matcher(new String(data.getData(),"UTF-8"));
                    if (m.matches()) {
                        return false;}
                } catch (UnsupportedEncodingException e) {
                    return false;
                }
            }
            return true;
        }
    }
}
