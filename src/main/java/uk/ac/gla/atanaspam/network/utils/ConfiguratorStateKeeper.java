package uk.ac.gla.atanaspam.network.utils;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.*;

/**
 * This data structure keeps track of the data reported by each of the nodes in the network.
 * Top level data is organised in an arrayList of HashMaps. Each HashMap represents a specific event.
 *  1: Report Port Hits
 *  2: Report Unexpected Port Hit
 *  3: Report Ip Hit
 *  4: Report Unexpected Ip Hit
 *  5: Report Dropped Packet
 *  6: Report Bad Flag combination
 *  7: Future Work
 *  8: Future Work
 *  9: Future Work
 * Within each of those HashMaps the key is an Object. This object depends on the representation of the specific rules.
 * For Port Hits the key would be the integer representing the port. For an IP hit the key would be an InetAddress object
 * For Each Key in the hashmaps the value is an integer array with size the number of taskIds in the topology. This way
 * if you want to know how many times port 1000 has been unexpectedly hit through bolt 3 you would:
 *          data.get(2).get(1000)[3]
 *              ^          ^      ^
 *              |          |      |
 *       2 represents      |      |
 *     Unexpected port     |      |
 *     hit statistics      |      |
 *                   port we are  |
 *                  interested in |
 *                                |
 *                         taskId we are
 *                         interested in
 *
 * @author atanaspam
 * @version 0.1
 * @created 15/11/2015
 */
public class ConfiguratorStateKeeper implements Serializable{

    private static final long serialVersionUID = 0;
    //TODO see if you can numOfBolts correlates to taskId's

    private ArrayList<HashMap<Object, int[]>> data;
    private int numOfBolts;

    public ConfiguratorStateKeeper(int numOfBolts){
        this.numOfBolts = numOfBolts+1;
        this.data = new ArrayList<>(10);
        for (int i=0; i<10;i++){
            this.data.add(i, new HashMap<Object, int[]>());
        }
    }

    public void addPortHit(int port, int taskId){
        int[] a = data.get(1).get(port);
        if (a == null){
            a = new int[numOfBolts];
        }
        a[taskId]++; // TODO if taskId's dont correspond to numOfBolts this should be changed
        data.get(1).put(port,a);
    }

    public int[] getPortHit(int port){
        return data.get(1).get(port);
    }

    public void addUnexpPortHit(int port, int taskId){
        int[] a = data.get(2).get(port);
        if (a == null){
            a = new int[numOfBolts];
        }
        a[taskId]++;
        data.get(2).put(port,a);
    }

    public int[] getUnexpPortHit(int port){
        return data.get(2).get(port);
    }

    public void addIpHit(InetAddress addr, int taskId){
        int[] a = data.get(3).get(addr);
        if (a == null){
            a = new int[numOfBolts];
        }
        a[taskId]++;
        data.get(3).put(addr,a);
    }

    public int[] getIpHit(InetAddress addr){
        return data.get(3).get(addr);
    }

    public void addUnexpIpHit(InetAddress addr, int taskId){
        int[] a = data.get(4).get(addr);
        if (a == null){
            a = new int[numOfBolts];
        }
        a[taskId]++;
        data.get(4).put(addr,a);
    }

    public int[] getUnexpIpHit(InetAddress addr){
        return data.get(4).get(addr);
    }

    public void addDroppedPacket(InetAddress addr, int taskId){
        int[] a = data.get(5).get(addr);
        if (a == null){
            a = new int[numOfBolts];
        }
        a[taskId]++;
        data.get(5).put(addr,a);
    }

    public int[] getDroppedPacket(InetAddress addr){
        return data.get(5).get(addr);
    }

    public void addBadFlagHit(int flagNum, int taskId){
        int[] a = data.get(6).get(flagNum);
        if (a == null){
            a = new int[numOfBolts];
        }
        a[taskId]++;
        data.get(6).put(flagNum,a);
    }

    public int[] getBadFlag(int flagNum){
        return data.get(6).get(flagNum);
    }

    @Override
    public String toString() {
        StringBuilder c = new StringBuilder();
        c.append("[");
        for(HashMap<Object, int[]> a: data){
            for(Map.Entry<Object, int[]> b : a.entrySet()){
                c.append(" ");
                c.append(b.getKey());
                c.append("-");
                c.append(Arrays.toString(b.getValue()));
            }
        }
        c.append("]");
        return c.toString();
    }
}
