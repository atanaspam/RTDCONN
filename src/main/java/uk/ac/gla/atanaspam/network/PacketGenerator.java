package uk.ac.gla.atanaspam.network;

import backtype.storm.tuple.Values;
import java.util.Random;

/**
 * Created by atanaspam on 04/10/2015.
 */
public class PacketGenerator {

    //private static final long serialVersionUID = 1L;


    public static Values getPacket() {

        String[] sourceOptions = {"10.10.10.10", "10.10.10.12", "10.10.10.13", "10.10.10.13", "10.10.10.13", "10.10.10.14", "10.10.10.15"};
        String[] destOptions = {"192.168.1.100", "192.168.1.101", "192.168.1.102", "192.168.1.103"};

        Random generator = new Random();
        int id = idGenerator.getNextID();
        String source = sourceOptions[generator.nextInt(sourceOptions.length)];
        String destination = destOptions[generator.nextInt(destOptions.length)];
        int size = 16;

        return new Values(id, source, destination, size );
    }

   }
