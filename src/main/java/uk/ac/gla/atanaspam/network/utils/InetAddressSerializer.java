package uk.ac.gla.atanaspam.network.utils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author atanaspam
 * @version 0.1
 * @created 08/02/2016
 */
public class InetAddressSerializer extends Serializer<InetAddress> {

    @Override
    public void write(Kryo kryo, Output output, InetAddress object) {
        output.writeInt(object.getAddress().length);
        output.writeBytes(object.getAddress());
    }

    @Override
    public InetAddress read(Kryo kryo, Input input, Class<InetAddress> type) {
        int length = input.readInt();
        byte[] address = input.readBytes(length);
        try {
            return InetAddress.getByAddress(address);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

}
