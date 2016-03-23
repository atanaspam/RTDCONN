package uk.ac.gla.atanaspam.network;

import uk.ac.gla.atanaspam.pcapj.TCPFlags;

import java.net.InetAddress;
import java.util.regex.Pattern;

/**
 * Defines the minimum capabilities expected out of each module that performs signature checks
 * @author atanaspam
 * @version 0.1
 * @created 14/02/2016
 */
public interface ChecksPerformer {

    public boolean performChecks(GenericPacket packet);

    public void addIpAddress(InetAddress addr, int code);

    public void removeIpAddress(InetAddress addr, int code);

    public void addPort(int port, int code);

    public void removePort(int port, int code);

    public void addFlag(TCPFlags flag, int code);

    public void removeFlag(TCPFlags flag, int code);

    public void addPattern(Pattern pattern, int code);

    public void removePattern(Pattern pattern, int code);


}

