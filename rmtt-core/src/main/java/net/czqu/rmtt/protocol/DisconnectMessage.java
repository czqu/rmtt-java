package net.czqu.rmtt.protocol;

/** DISCONNECT packet: variable header {@link DisconnectVariableHeader}, no payload. */
public final class DisconnectMessage extends RmttMessage {

    /**
     * Create a DISCONNECT packet.
     *
     * @param fixedHeader    the parsed fixed header
     * @param variableHeader the DISCONNECT variable header
     * @param payload        unused for DISCONNECT, may be null
     */
    public DisconnectMessage(FixedHeader fixedHeader, Object variableHeader, byte[] payload) {
        super(fixedHeader, variableHeader, payload);
    }

    @Override
    public DisconnectVariableHeader variableHeader() {
        return (DisconnectVariableHeader) super.variableHeader();
    }
}