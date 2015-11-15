package uk.ac.gla.atanaspam.network.utils;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.*;

/**
 * @author atanaspam
 * @version 0.1
 * @created 14/11/2015
 */
public class StateKeeper implements Serializable{

    private boolean[] blockedPorts;
    private HashSet<InetAddress> blockedIpAddr;
    private HashSet<InetAddress> monitoredIpAddr;
    private HashSet<boolean[]> blockedFlags;
    private Map<InetAddress, Long> srcIpHitCount;
    private Map<InetAddress, Long> destIpHitCount;
    private Map<Integer, Long> portHitCount;
    private Long[] flagCount;

    public StateKeeper(){
        blockedPorts = new boolean[65535];
        blockedIpAddr = new HashSet<>();
        monitoredIpAddr = new HashSet<>();
        blockedFlags = new HashSet<>();
        srcIpHitCount = new HashMap<>();
        destIpHitCount = new HashMap<>();
        portHitCount = new HashMap<>();
        flagCount = new Long[9];
        for(int i=0;i<9;i++)
            flagCount[i] = new Long(0);
    }

    public void setBlockedPort(int port, boolean value){
                                    blockedPorts[port] = value;
    }

    public boolean getBlockedPort(int port){
                return blockedPorts[port];
    }

    public boolean isBlockedIpAddr(InetAddress addr){return blockedIpAddr.contains(addr);}

    public void addBlockedIpAddr(InetAddress addr){
        blockedIpAddr.add(addr);
    }

    public boolean removeBlockedIpAddr(InetAddress addr){
        return blockedIpAddr.remove(addr);
    }

    public boolean isMonitoredIpAddr(InetAddress addr){return monitoredIpAddr.contains(addr);}

    public void addMonitoredIpAddr(InetAddress addr){
        monitoredIpAddr.add(addr);
    }

    public boolean removeMonitoredIpAddr(InetAddress addr){
        return monitoredIpAddr.remove(addr);
    }

    public boolean isBadFlag(boolean[] flag){return blockedFlags.contains(flag); }

    public void addBlockedFlag(boolean[] flag){
        blockedFlags.add(flag);
    }

    public boolean removeBlockedFlag(boolean[] flag){
        return blockedFlags.remove(flag);
    }

    public void incrementSrcIpHitCount(InetAddress addr){
        if(srcIpHitCount.get(addr) != null){
            srcIpHitCount.put(addr, srcIpHitCount.get(addr)+1);
        } else{
            srcIpHitCount.put(addr, new Long(1));
        }
    }

    public Map<InetAddress, Long> getSrcIpHitCount() {
        return srcIpHitCount;
    }

    public void setSrcIpHitCount(Map<InetAddress, Long> srcIpHitCount) {
        this.srcIpHitCount = srcIpHitCount;
    }

    public void incrDesrIpHitCount(InetAddress addr){
        if(destIpHitCount.get(addr) != null){
            destIpHitCount.put(addr, destIpHitCount.get(addr)+1);
        } else{
            destIpHitCount.put(addr, new Long(1));
        }
    }

    public Map<InetAddress, Long> getDestIpHitCount() {
        return destIpHitCount;
    }

    public void setDestIpHitCount(Map<InetAddress, Long> destIpHitCount) {
        this.destIpHitCount = destIpHitCount;
    }

    public void incrementPortHitCount(int port) {
        if (portHitCount.get(port) != null) {
            portHitCount.put(port, (portHitCount.get(port) + 1));
        } else {
            portHitCount.put(port, new Long(1));
        }
    }

    public Map<Integer, Long> getPortHitCount() {
        return portHitCount;
    }

    public void setPortHitCount(Map<Integer, Long> portHitCount) {
        this.portHitCount = portHitCount;
    }

    public void incrementFlagCount(int flag){
        flagCount[flag] = flagCount[flag] +1;
    }

    public void clearFlagCount(int flag){
        flagCount[flag] = new Long(0);
    }

    public Long[] getFlagCount() {
        return flagCount;
    }

    public void setFlagCount(Long[] flagCount) {
        this.flagCount = flagCount;
    }

    public boolean flush(){
        for(int i=0; i<65535; i++){
            blockedPorts[i] = false;
        }
        blockedIpAddr.clear();
        monitoredIpAddr.clear();
        blockedFlags.clear();
        srcIpHitCount.clear();
        destIpHitCount.clear();
        portHitCount.clear();
        for(int i=0; i<9;i++)
            flagCount[i] = new Long(0);
        return true;
    }

    public boolean resetCounts(){
        srcIpHitCount.clear();
        destIpHitCount.clear();
        portHitCount.clear();
        for(int i=0; i<9;i++)
            flagCount[i] = new Long(0);
        return true;
    }

    public void obtainStatistics(){
        //TODO generate statistics
    }

    @Override
    public String toString() {
        return "StateKeeper{" +
                //"blockedPorts=" + Arrays.toString(blockedPorts) +
                ", blockedIpAddr=" + blockedIpAddr +
                ", monitoredIpAddr=" + monitoredIpAddr +
                ", blockedFlags=" + blockedFlags +
                ", srcIpHitCount=" + srcIpHitCount +
                ", destIpHitCount=" + destIpHitCount +
                ", portHitCount=" + portHitCount +
                ", flagCount=" + Arrays.toString(flagCount) +
                '}';
    }
}
