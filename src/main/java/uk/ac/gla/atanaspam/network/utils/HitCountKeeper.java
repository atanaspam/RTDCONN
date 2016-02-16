package uk.ac.gla.atanaspam.network.utils;


import java.io.Serializable;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.HashMap;

/**
 * @author atanaspam
 * @version 0.1
 * @created 10/01/2016
 */
public class HitCountKeeper implements Serializable{

    //TODO add version UID
    //TODO change serializer
    public double detectionRatio = 1.5;

    private HashMap<InetAddress, CMAPair> srcIpHitCount;
    private HashMap<InetAddress, CMAPair> destIpHitCount;
    private HashMap<Integer, CMAPair> portHitCount;
    private Long[] flagCount;

    public HitCountKeeper(){
        srcIpHitCount = new HashMap<>();
        destIpHitCount = new HashMap<>();
        portHitCount = new HashMap<>();
        flagCount = new Long[9];
        for(int i=0;i<9;i++)
            flagCount[i] = new Long(0);
    }

    public void set(HashMap<InetAddress, CMAPair> newSrcIpHitCount, HashMap<InetAddress, CMAPair> newDestIpHitCount,
                    HashMap<Integer, CMAPair> newPortHitCount, Long[] newFlagCount){
        srcIpHitCount = new HashMap<>(newSrcIpHitCount);
        destIpHitCount = new HashMap<>(newDestIpHitCount);
        portHitCount = new HashMap<>(newPortHitCount);
        flagCount = newFlagCount;
    }

    public void setDetectionRatio(double detectionRatio) {
        this.detectionRatio = detectionRatio;
    }


    public void clearHitCounts(){
        srcIpHitCount.clear();
        destIpHitCount.clear();
        portHitCount.clear();
        for(int i=0;i<9;i++)
            flagCount[i] = new Long(0);
    }

    /**
     * Methods related to Source Ip Addresses
     */

    /**
     * Add a new value to the CMA entry for this address. If it does not exist create it.
     * @see CMAPair
     * @param addr The address data is added for
     * @param value The new value to be added.
     * @return true if the value to add is larger than the current CMA before the update
     */
    public boolean addSrcIpHitCount(InetAddress addr, int value){
        CMAPair a = srcIpHitCount.get(addr);
        boolean result = false;
        if(a != null){
            result = value > (a.getCumulativeMovingAverage() * detectionRatio);
            a.addValue(value);
            srcIpHitCount.put(addr, a);
        } else{
            srcIpHitCount.put(addr, new CMAPair(value, 1));
        }
        return result;
    }

    /**
     * Get the CMA for a specific address
     * @param addr the address we care fore
     * @return the CMA or null
     */
    public int getSrcIpCMA(InetAddress addr){
        return srcIpHitCount.get(addr).getCumulativeMovingAverage();
    }

    public HashMap<InetAddress, CMAPair> getSrcIpHitCount() {
        return srcIpHitCount;
    }

    /**
     * Methods related to Destination Ip addresses
     */

    /**
     * Add a new value to the CMA entry for this address. If it does not exist create it.
     * @see CMAPair
     * @param addr The address data is added for
     * @param value The new value to be added.
     * @return true if the value to add is larger than the current CMA before the update
     */
    public boolean addDesIpHitCount(InetAddress addr, int value){
        CMAPair a = destIpHitCount.get(addr);
        boolean result = false;
        if(a != null){
            result = value > (a.getCumulativeMovingAverage() * detectionRatio);
            a.addValue(value);
            destIpHitCount.put(addr, a);
        } else{
            destIpHitCount.put(addr, new CMAPair(value, 1));
        }
        return result;
    }

    /**
     * Get the CMA for a specific address
     * @param addr the address we care fore
     * @return the CMA or null
     */
    public int getDstIpCMA(InetAddress addr){
        return destIpHitCount.get(addr).cumulativeMovingAverage;
    }

    public HashMap<InetAddress, CMAPair> getDestIpHitCount() {
        return destIpHitCount;
    }


    /**
     * Methods related to Ports
     */

    /**
     * Add a new value to the CMA entry for this port. If it does not exist create it.
     * @see CMAPair
     * @param port The port data is added for.
     * @param value The new value to be added.
     * @return true if the value to add is larger than the current CMA before the update
     */
    public boolean addPortHitCount(int port, int value) {
        CMAPair a = portHitCount.get(port);
        boolean result = false;
        if (a != null) {
            result = value > (a.getCumulativeMovingAverage() * detectionRatio);
            a.addValue(value);
            portHitCount.put(port, a);
        } else {
            portHitCount.put(port, new CMAPair());
        }
        return result;
    }

    public HashMap<Integer, CMAPair> getPortHitCount() {
        return portHitCount;
    }

    /**
     * Methods relatedto Flags
     */

    public void incrementFlagCount(int flag){
        flagCount[flag] = flagCount[flag] +1;
    }

    public Long[] getFlagCount() {
        return flagCount;
    }

    public void setFlagCount(Long[] flagCount) {
        this.flagCount = flagCount;
    }

    @Override
    public String toString() {
        return String.format("src: %s%ndest: %s%nport: %s%nflag:%s%n",
                srcIpHitCount.toString(), destIpHitCount.toString(), portHitCount.toString(), Arrays.toString(flagCount));
    }
}
