package net.czqu.rmtt.protocol;

/**
 * CONNACK variable header: return-code byte + server keepalive (Uint16).
 * ServerKeepalive is the server-decided heartbeat interval in seconds; it is only meaningful when
 * the return code is 0x00.
 */
public final class ConnAckVariableHeader {
    private final ConnectReturnCode connectReturnCode;
    private final int serverKeepaliveSeconds;

    /**
     * Build a rejected CONNACK with no keepalive.
     *
     * @param connectReturnCode the return code to send
     */
    public ConnAckVariableHeader(ConnectReturnCode connectReturnCode) {
        this(connectReturnCode, 0);
    }

    /**
     * Build a CONNACK header with a return code and keepalive.
     *
     * @param connectReturnCode      the return code to send
     * @param serverKeepaliveSeconds the server-decided heartbeat interval in seconds
     */
    public ConnAckVariableHeader(ConnectReturnCode connectReturnCode, int serverKeepaliveSeconds) {
        this.connectReturnCode = connectReturnCode;
        this.serverKeepaliveSeconds = serverKeepaliveSeconds;
    }

    /**
     * The CONNACK return code.
     *
     * @return the CONNACK return code
     */
    public ConnectReturnCode connectReturnCode() {
        return connectReturnCode;
    }

    /**
     * The server-decided heartbeat interval.
     *
     * @return the server-decided heartbeat interval in seconds (meaningful only when accepted)
     */
    public int serverKeepaliveSeconds() {
        return serverKeepaliveSeconds;
    }

    @Override
    public String toString() {
        return "ConnAckVariableHeader{" + connectReturnCode + ",serverKeepalive=" + serverKeepaliveSeconds + '}';
    }
}