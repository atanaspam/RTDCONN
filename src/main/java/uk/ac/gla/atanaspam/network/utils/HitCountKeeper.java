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

    public HashMap<InetAddress, Long> getSrcIpHitCount() {
        return srcIpHitCount;
    }

    public void setSrcIpHitCount(HashMap<InetAddress, Long> srcIpHitCount) {
        this.srcIpHitCount = srcIpHitCount;
    }

    public HashMap<InetAddress, Long> getDestIpHitCount() {
        return destIpHitCount;
    }

    public void setDestIpHitCount(HashMap<InetAddress, Long> destIpHitCount) {
        this.destIpHitCount = destIpHitCount;
    }

    public HashMap<Integer, Long> getPortHitCount() {
        return portHitCount;
    }

    public void setPortHitCount(HashMap<Integer, Long> portHitCount) {
        this.portHitCount = portHitCount;
    }

    public Long[] getFlagCount() {
        return flagCount;
    }

    public void setFlagCount(Long[] flagCount) {
        this.flagCount = flagCount;
    }

    public String toString(){
        return String.format("src: %s%ndest: %s%nport: %s%nflag:%s%n",
                srcIpHitCount.toString(), destIpHitCount.toString(), portHitCount.toString(), Arrays.toString(flagCount));
    }

}
