package uk.ac.gla.atanaspam.network.utils;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.*;

/**
 * @author atanaspam
 * @version 0.1
 * @created 15/11/2015
 */
public class ConfiguratorStateKeeper implements Serializable{

    private ArrayList<HashMap<Object, int[]>> data;
    private int numOfBolts;

    public ConfiguratorStateKeeper(int numOfBolts){
        this.numOfBolts = numOfBolts;
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
        a[taskId-4]++;
        data.get(1).put(port,a);
    }

    public void addUnexpPortHit(int port, int taskId){
        int[] a = data.get(2).get(port);
        if (a == null){
            a = new int[numOfBolts];
        }
        a[taskId-4]++;
        data.get(2).put(port,a);
    }

    public void addIpHit(InetAddress ip, int taskId){
        int[] a = data.get(3).get(ip);
        if (a == null){
            a = new int[numOfBolts];
        }
        a[taskId-4]++;
        data.get(3).put(ip,a);
    }

    public void addUnexpIpHit(InetAddress ip, int taskId){
        int[] a = data.get(4).get(ip);
        if (a == null){
            a = new int[numOfBolts];
        }
        a[taskId-4]++;
        data.get(4).put(ip,a);
    }

    public void addDropPacket(InetAddress ip, int taskId){
        int[] a = data.get(5).get(ip);
        if (a == null){
            a = new int[numOfBolts];
        }
        a[taskId-4]++;
        data.get(5).put(ip,a);
    }

    public void addBadFlag(Integer n, int taskId){
        int[] a = data.get(6).get(n);
        if (a == null){
            a = new int[numOfBolts];
        }
        a[taskId-4]++;
        data.get(6).put(n,a);
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
