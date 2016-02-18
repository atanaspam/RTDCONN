package uk.ac.gla.atanaspam.network.utils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author atanaspam
 * @version 0.1
 * @created 18/02/2016
 */
public class Inet4AddressSerializer extends Serializer<Inet4Address> {

    @Override
    public void write(Kryo kryo, Output output, Inet4Address object) {
        Inet4Address i4a = object;
        kryo.writeObject(output, i4a.getAddress());
    }

    @Override
    public Inet4Address read(Kryo kryo, Input input, Class<Inet4Address> type) {
        byte[] address = kryo.readObject(input, byte[].class);
        try {
            return (Inet4Address) InetAddress.getByAddress(address);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }
}
