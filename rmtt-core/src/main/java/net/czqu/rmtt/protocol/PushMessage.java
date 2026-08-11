package net.czqu.rmtt.protocol;

/** PUSH packet carrying server-initiated payload bytes. */
public final class PushMessage extends RmttMessage {

    /**
     * Create a PUSH packet.
     *
     * @param fixedHeader    the parsed fixed header
     * @param variableHeader the PUSH variable header
     * @param payload        the push payload bytes
     */
    public PushMessage(FixedHeader fixedHeader, Object variableHeader, byte[] payload) {
        super(fixedHeader, variableHeader, payload);
    }

    @Override
    public PushVariableHeader variableHeader() {
        return (PushVariableHeader) super.variableHeader();
    }

    @Override
    public byte[] payload() {
        return super.payload();
    }
}