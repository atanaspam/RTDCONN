package com.gla.ac.uk.network;

import java.io.Serializable;
import java.util.Random;

/**
 * Created by atanaspam on 04/10/2015.
 */
public class Packet implements Serializable {

    private static final long serialVersionUID = 1L;
    private int id;
    private String source;
    private String destination;
    private int size;

    public Packet(int id, String source, String destination, int size) throws Exception{

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
