package uk.ac.gla.atanaspam.network;

import uk.ac.gla.atanaspam.pcapj.*;


/**
 * This class is used to test the module that is responsible for
 * @author atanaspam
 * @created 20/10/2015
 * @version 0.1
 */

public class pcapjPacketGenerator {
    public static void main(String[] args) {
        PcapParser pcapParser = new PcapParser();
        if(pcapParser.openFile("/Users/atanaspam/Desktop/partial.pcap") < 0){
            System.err.println("Failed to open " + args[0] + ", exiting.");
            return;
        }
        long startTime = System.currentTimeMillis();
        BasicPacket packet = pcapParser.getPacket();
        while(packet != BasicPacket.EOF){
            if(!(packet instanceof IPPacket)){
                packet = pcapParser.getPacket();
                continue;
            }

            System.out.println(packet);
            packet = pcapParser.getPacket();
        }
        long endTime = System.currentTimeMillis();
        pcapParser.closeFile();
        System.out.println("Execution time: " + (endTime - startTime));
    }

}

