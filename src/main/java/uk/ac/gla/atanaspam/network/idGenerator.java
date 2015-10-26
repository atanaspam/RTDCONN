package uk.ac.gla.atanaspam.network;

import java.io.Serializable;

/**
 * Created by atanaspam on 05/10/2015.
 */
public class idGenerator implements Serializable {

    private static int id = 0;

    public static int getNextID(){
        return id++;
    }
}
