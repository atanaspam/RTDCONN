package uk.ac.gla.atanaspam.network;

import java.io.Serializable;
import java.util.Random;

/**
 * This class is no longer used and is pending removal...
 * @deprecated
 * @author atanaspam
 * @created 04/10/2015
 * @version 0.1
 */
public class SamplePacket implements Serializable {
    /**
     * TODO remove serialization
     */
    private static final long serialVersionUID = 1L;
    private int id;
    private String source;
    private String destination;
    private int size;

    public SamplePacket(int id, String source, String destination, int size) throws Exception{

        this.id = id;
        this.source = source;
        this.destination = destination;
        this.size = size;
    }

    public String getDestination() {
        return destination;
    }

    public String getSource() {
        return source;
    }

    public int getSize() {
        return size;
    }

    @Override
    public String toString() {
        return "Packet " + id +  " | Source: " + source +  " | Destnation: " + destination;

    }
}
