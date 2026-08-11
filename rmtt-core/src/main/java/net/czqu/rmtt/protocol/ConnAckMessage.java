package net.czqu.rmtt.protocol;

/** CONNACK packet: return code ({@link ConnectReturnCode}) plus server-decided keepalive. */
public final class ConnAckMessage extends RmttMessage {

    /**
     * Create a CONNACK packet.
     *
     * @param fixedHeader    the parsed fixed header
     * @param variableHeader the CONNACK variable header
     * @param payload        unused for CONNACK, may be null
     */
    public ConnAckMessage(FixedHeader fixedHeader, Object variableHeader, byte[] payload) {
        super(fixedHeader, variableHeader, payload);
    }

    @Override
    public ConnAckVariableHeader variableHeader() {
        return (ConnAckVariableHeader) super.variableHeader();
    }
}