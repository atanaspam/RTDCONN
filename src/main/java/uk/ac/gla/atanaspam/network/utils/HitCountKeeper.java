package uk.ac.gla.atanaspam.network.utils;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author atanaspam
 * @version 0.1
 * @created 10/01/2016
 */
public class HitCountKeeper implements Serializable{

    //TODO ad version UID

    private HashMap<InetAddress, Long> srcIpHitCount;
    private HashMap<InetAddress, Long> destIpHitCount;
    private HashMap<Integer, Long> portHitCount;
    private Long[] flagCount;

    public HitCountKeeper(){
        srcIpHitCount = new HashMap<>();
        destIpHitCount = new HashMap<>();
        portHitCount = new HashMap<>();
        flagCount = new Long[9];
        for(int i=0;i<9;i++)
            flagCount[i] = new Long(0);
    }

    public void set(HashMap<InetAddress, Long> newSrcIpHitCount, HashMap<InetAddress, Long> newDestIpHitCount,
                    HashMap<Integer, Long> newPortHitCount, Long[] newFlagCount){
        srcIpHitCount = new HashMap<>(newSrcIpHitCount);
        destIpHitCount = new HashMap<>(newDestIpHitCount);
        portHitCount = new HashMap<>(newPortHitCount);
        flagCount = newFlagCount;
    }


    public Long[] getFlagCount() {
        return flagCount;
    }

    public void setFlagCount(Long[] flagCount) {
        this.flagCount = flagCount;
    }

    public void clearHitCounts(){
        srcIpHitCount.clear();
        destIpHitCount.clear();
        portHitCount.clear();
        for(int i=0;i<9;i++)
            flagCount[i] = new Long(0);
    }

    public void incrementSrcIpHitCount(InetAddress addr){
        if(srcIpHitCount.get(addr) != null){
            srcIpHitCount.put(addr, srcIpHitCount.get(addr)+1);
        } else{
            srcIpHitCount.put(addr, new Long(1));
        }
    }

    public HashMap<InetAddress, Long> getSrcIpHitCount() {
        return srcIpHitCount;
    }

    public void setSrcIpHitCount(HashMap<InetAddress, Long> srcIpHitCount) {
        this.srcIpHitCount = srcIpHitCount;
    }

    public void incrDesrIpHitCount(InetAddress addr){
        if(destIpHitCount.get(addr) != null){
            destIpHitCount.put(addr, destIpHitCount.get(addr)+1);
        } else{
            destIpHitCount.put(addr, new Long(1));
        }
    }

    public HashMap<InetAddress, Long> getDestIpHitCount() {
        return destIpHitCount;
    }

    public void setDestIpHitCount(HashMap<InetAddress, Long> destIpHitCount) {
        this.destIpHitCount = destIpHitCount;
    }

    public void incrementPortHitCount(int port) {
        if (portHitCount.get(port) != null) {
            portHitCount.put(port, (portHitCount.get(port) + 1));
        } else {
            portHitCount.put(port, new Long(1));
        }
    }

    public HashMap<Integer, Long> getPortHitCount() {
        return portHitCount;
    }

    public void setPortHitCount(HashMap<Integer, Long> portHitCount) {
        this.portHitCount = portHitCount;
    }

    public void incrementFlagCount(int flag){
        flagCount[flag] = flagCount[flag] +1;
    }

    public String toString(){
        return String.format("src: %s%ndest: %s%nport: %s%nflag:%s%n",
                srcIpHitCount.toString(), destIpHitCount.toString(), portHitCount.toString(), Arrays.toString(flagCount));
    }

}
