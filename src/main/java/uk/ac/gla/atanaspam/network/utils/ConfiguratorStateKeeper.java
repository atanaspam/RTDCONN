package uk.ac.gla.atanaspam.network.utils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.NotNull;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.CollectionSerializer;
import com.esotericsoftware.kryo.serializers.MapSerializer;

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
 * For Each Key in the hashMap the value is an {@link uk.ac.gla.atanaspam.network.utils.HitCountPair} array with size
 * the number of taskIds in the topology. This way if you want to know how many times port 1000 has been unexpectedly
 * hit through bolt 3 you would:
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

public class ConfiguratorStateKeeper implements Serializable {


    //TODO change serializer
    private static final long serialVersionUID = 1;


    private ArrayList<HashMap<Object, HitCountPair[]>> data;
    private int numOfBolts;
    private double numberOfPacketsDropped;

    public ConfiguratorStateKeeper(int numOfBolts){
        numberOfPacketsDropped = 0;
        this.numOfBolts = numOfBolts+1;
        this.data = new ArrayList<>(10);
        for (int i=0; i<10;i++){
            this.data.add(i, new HashMap<Object, HitCountPair[]>());
        }
    }

    public boolean addPortHit(int port, int taskId, int iterationNumber){
        HitCountPair[] a = data.get(1).get(port);
        if (a == null){
            a = new HitCountPair[numOfBolts];
            for (int i=0;i<numOfBolts;i++){
                a[i] = new HitCountPair();
            }
        }
        boolean result = a[taskId].increment(iterationNumber);
        data.get(1).put(port,a);
        return result;
    }

    public HitCountPair[] getPortHit(int port){
        return data.get(1).get(port);
    }

    public boolean addUnexpPortHit(int port, int taskId, int iterationNumber){
        HitCountPair[] a = data.get(2).get(port);
        if (a == null){
            a = new HitCountPair[numOfBolts];
            for (int i=0;i<numOfBolts;i++){
                a[i] = new HitCountPair();
            }
        }
        boolean result = a[taskId].increment(iterationNumber);
        data.get(2).put(port,a);
        return result;
    }

    public HitCountPair[] getUnexpPortHit(int port){
        return data.get(2).get(port);
    }

    public boolean addIpHit(InetAddress addr, int taskId, int iterationNumber){
        HitCountPair[] a = data.get(3).get(addr);
        if (a == null){
            a = new HitCountPair[numOfBolts];
            for (int i=0;i<numOfBolts;i++){
                a[i] = new HitCountPair();
            }
        }
        boolean result = a[taskId].increment(iterationNumber);
        data.get(3).put(addr,a);
        return result;
    }

    public HitCountPair[] getIpHit(InetAddress addr){return data.get(3).get(addr);}

    public boolean addUnexpIpHit(InetAddress addr, int taskId, int iterationNumber){
        HitCountPair[] a = data.get(4).get(addr);
        if (a == null){
            a = new HitCountPair[numOfBolts];
            for (int i=0;i<numOfBolts;i++){
                a[i] = new HitCountPair();
            }
        }
        boolean result = a[taskId].increment(iterationNumber);
        data.get(4).put(addr,a);
        return result;
    }

    public HitCountPair[] getUnexpIpHit(InetAddress addr){return data.get(4).get(addr);}

    public boolean addDroppedPacket(InetAddress addr, int taskId, int iterationNumber){
        HitCountPair[] a = data.get(5).get(addr);
        if (a == null){
            a = new HitCountPair[numOfBolts];
            for (int i=0;i<numOfBolts;i++){
                a[i] = new HitCountPair();
            }
        }
        boolean result = a[taskId].increment(iterationNumber);
        data.get(5).put(addr,a);
        return result;
    }

    public HitCountPair[] getDroppedPacket(InetAddress addr){
        return data.get(5).get(addr);
    }

    public boolean addBadFlagHit(int flagNum, int taskId, int iterationNumber){
        HitCountPair[] a = data.get(6).get(flagNum);
        if (a == null){
            a = new HitCountPair[numOfBolts];
            for (int i=0;i<numOfBolts;i++){
                a[i] = new HitCountPair();
            }
        }
        boolean result = a[taskId].increment(iterationNumber);
        data.get(6).put(flagNum,a);
        return result;
    }

    public HitCountPair[] getBadFlag(int flagNum){
        return data.get(6).get(flagNum);
    }

    public void incrementDroppedPacket(){
        this.numberOfPacketsDropped++;
    }

    public double getNumberOfPacketsDropped() {
        return numberOfPacketsDropped;
    }

    @Override
    public String toString() {
        StringBuilder c = new StringBuilder();
        c.append("[");
        for(HashMap<Object, HitCountPair[]> a: data){
            for(Map.Entry<Object, HitCountPair[]> b : a.entrySet()){
                c.append(" ");
                c.append(b.getKey());
                c.append("-");
                c.append(Arrays.toString(b.getValue()));
            }
        }
        c.append("]");
        return c.toString();
    }

//    @Override
//    public void write(Kryo kryo, Output output) {
//        MapSerializer mapSerializer = new MapSerializer();
//        CollectionSerializer collSerializer = new CollectionSerializer();
//        kryo.register(HitCountPair[].class, new HitCountPairArraySerializer());
//        kryo.register(ArrayList.class, collSerializer);
//        kryo.register(HashMap.class, mapSerializer);
//        kryo.register(LinkedHashMap.class, mapSerializer);
//        collSerializer.setElementClass(HashMap.class, mapSerializer);
//        mapSerializer.setKeyClass(Object.class, kryo.getSerializer(Object.class));
//        mapSerializer.setKeysCanBeNull(false);
//        mapSerializer.setKeyClass(Arrays.class, kryo.getSerializer(String.class));
//        kryo.writeClassAndObject(output, data);
//        output.writeInt(numOfBolts);
//    }
//
//    @Override
//    public void read(Kryo kryo, Input input) {
//        MapSerializer mapSerializer = new MapSerializer();
//        CollectionSerializer collSerializer = new CollectionSerializer();
//        kryo.register(HitCountPair[].class, new HitCountPairArraySerializer());
//        kryo.register(ArrayList.class, collSerializer);
//        kryo.register(HashMap.class, mapSerializer);
//        kryo.register(LinkedHashMap.class, mapSerializer);
//        collSerializer.setElementClass(HashMap.class, mapSerializer);
//        mapSerializer.setKeyClass(Object.class, kryo.getSerializer(Object.class));
//        mapSerializer.setKeysCanBeNull(false);
//        mapSerializer.setKeyClass(Arrays.class, kryo.getSerializer(String.class));
//        data = (ArrayList<HashMap<Object,HitCountPair[]>>) kryo.readClassAndObject(input);
//        numOfBolts = input.readInt();
//    }
}
