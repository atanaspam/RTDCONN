package uk.ac.gla.atanaspam.network;


import uk.ac.gla.atanaspam.pcapj.*;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Random;


/**
 * This class uses the pcapj library to read a static TCPDump output and generate the corresponding packet objects
 * Each parsed packet is stored into an arraylist so that the storm topology has an unlimited source of packets
 * @author atanaspam
 * @created 04/10/2015
 * @version 0.1
 */
public class PacketGenerator {

    PcapParser pcapParser;
    String filePath;
    ArrayList<BasicPacket> list;
    Random generator;

    /**
     * This constructor initializes the pcapj library, reads the static dump of packets and saves them to an arraylist
     */
    public PacketGenerator() {
        pcapParser = new PcapParser();
        list = new ArrayList<BasicPacket>();
        generator = new Random();
        filePath = "/Users/atanaspam/Desktop/DumpFile03.pcap"; /**CHANGEME path to your static pcap export */
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
        try {
            list.add(new GenericPacket(1111111111,"FF:FF:FF:FF:FF", "FF:FF:FF:FF:FF", InetAddress.getByName("192.168.1.1"),
                    InetAddress.getByName("203.164.234.111")));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    /**
     * This method picks up a random packet from the list of packets parsed and returns it
     * @return the packet selected BasicPacket is used as it is a superclass of any of the packets
     */
    public BasicPacket getPacket(){
        return list.get(generator.nextInt(list.size()));
    }

    /**
     *
     */
    public void cleanUp(){
        pcapParser.closeFile();
    }
    //private static final long serialVersionUID = 1L;

   }

