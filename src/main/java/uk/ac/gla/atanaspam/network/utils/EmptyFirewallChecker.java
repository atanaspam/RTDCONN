package uk.ac.gla.atanaspam.network.utils;

import uk.ac.gla.atanaspam.network.ChecksPerformer;
import uk.ac.gla.atanaspam.network.GenericPacket;
import uk.ac.gla.atanaspam.pcapj.TCPFlags;

import java.net.InetAddress;
import java.util.regex.Pattern;

/**
 * @author atanaspam
 * @version 0.1
 * @created 16/02/2016
 */
public class EmptyFirewallChecker implements ChecksPerformer {


    @Override
    public boolean performChecks(GenericPacket packet) {
        return true;
    }

    @Override
    public void addIpAddress(InetAddress addr, int code) {

    }

    @Override
    public void removeIpAddress(InetAddress addr, int code) {

    }

    @Override
    public void addPort(int port, int code) {

    }

    @Override
    public void removePort(int port, int code) {

    }

    @Override
    public void addFlag(TCPFlags flag, int code) {

    }

    @Override
    public void removeFlag(TCPFlags flag, int code) {

    }

    @Override
    public void addPattern(Pattern pattern, int code) {

    }

    @Override
    public void removePattern(Pattern pattern, int code) {

    }
}
