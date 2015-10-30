package uk.ac.gla.atanaspam.network;

import uk.ac.gla.atanaspam.pcapj.*;


/**
 * This class is used to test the module that is responsible for
 * @author atanaspam
 * @created 20/10/2015
 * @version 0.1
 */

public class NewPacketGenerator {
    public static void main(String[] args) {
        PcapParser pcapParser = new PcapParser();
        if(pcapParser.openFile("/Users/atanaspam/Desktop/DumpFile03.pcap") < 0){
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

/*
public class NewPacketGenerator extends Thread{
    private Thread t;
    private int threadId;
    private PcapParser pcapParser;

    public NewPacketGenerator(PcapParser pcapParser, int threadId){
        this.pcapParser = pcapParser;
        this.threadId = threadId;
    }

    public static void main(String[] args) {
        PcapParser pcapParser = new PcapParser();
        if(pcapParser.openFile("/Users/atanaspam/Desktop/DumpFile03.pcap") < 0){
            System.err.println("Failed to open " + args[0] + ", exiting.");
            return;
        }

        Thread[] threads = new Thread[4];
        for(int i=0; i<threads.length; i++){
            threads[i] = new NewPacketGenerator(pcapParser, i);
        }
        for(int i=0; i<threads.length; i++){
            threads[i].start();
        }


    }

    @Override
    public void run() {
        long startTime = System.currentTimeMillis();
        BasicPacket packet = pcapParser.getPacket();
        //System.out.println(packet);
        while (packet != BasicPacket.EOF) {
            if (!(packet instanceof IPPacket)) {
                packet = pcapParser.getPacket();
                continue;
            }

            //System.out.println(packet);

            packet = pcapParser.getPacket();
        }
        long endTime = System.currentTimeMillis();
        //System.out.println("Execution time: " + (endTime - startTime));
        System.out.println("Thread " + threadId + " exiting after: " + (endTime - startTime));
    }

    public void start ()
    {
        System.out.println("Starting " +  "Thread " + threadId);
        if (t == null)
        {
            t = new Thread (this, String.valueOf(threadId));
            t.start ();
        }
    }

}
*/