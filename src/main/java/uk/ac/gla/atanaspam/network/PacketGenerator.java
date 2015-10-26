package uk.ac.gla.atanaspam.network;


import uk.ac.gla.atanaspam.*;

import java.util.ArrayList;
import java.util.Random;


/**
 * Created by atanaspam on 04/10/2015.
 */
public class PacketGenerator {

    PcapParser pcapParser;
    String filePath;
    ArrayList<BasicPacket> list;
    Random generator;

    public PacketGenerator() {
        pcapParser = new PcapParser();
        list = new ArrayList<BasicPacket>();
        generator = new Random();
        filePath = "/Users/atanaspam/Desktop/DumpFile03.pcap";
        if(pcapParser.openFile(filePath) < 0){
            System.err.println("Failed to open " + filePath + ", exiting.");
            return;
        }
        BasicPacket packet = pcapParser.getPacket();
        while(packet != BasicPacket.EOF){
            if(!(packet instanceof IPPacket)){
                packet = pcapParser.getPacket();
                continue;
            }
            packet = pcapParser.getPacket();
            list.add(packet);
            //System.out.println(packet);
        }
        pcapParser.closeFile();
    }
    public BasicPacket getPacket(){

        return list.get(generator.nextInt(list.size()));
    }

    public void cleanUp(){
        pcapParser.closeFile();
    }
    //private static final long serialVersionUID = 1L;

    /*
    public static Values getPacket1() {

        String[] sourceOptions = {"10.10.10.10", "10.10.10.12", "10.10.10.13", "10.10.10.13", "10.10.10.13", "10.10.10.14", "10.10.10.15"};
        String[] destOptions = {"192.168.1.100", "192.168.1.101", "192.168.1.102", "192.168.1.103"};

        Random generator = new Random();
        int id = idGenerator.getNextID();
        String source = sourceOptions[generator.nextInt(sourceOptions.length)];
        String destination = destOptions[generator.nextInt(destOptions.length)];
        int size = 16;

        return new Values(id, source, destination, size );
    }
    */

   }

