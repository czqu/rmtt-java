package net.czqu.rmtt.protocol;

import java.nio.charset.StandardCharsets;

/** CONNECT packet: variable header ({@link ConnectVariableHeader}) plus a UTF-8 credential payload. */
public final class ConnectMessage extends RmttMessage {

    /**
     * Create a CONNECT packet.
     *
     * @param fixedHeader    the parsed fixed header
     * @param variableHeader the CONNECT variable header
     * @param payload        the raw credential bytes (UTF-8)
     */
    public ConnectMessage(FixedHeader fixedHeader, Object variableHeader, byte[] payload) {
        super(fixedHeader, variableHeader, payload);
    }

    /**
     * @return the variable header typed as {@link ConnectVariableHeader}
     */
    @Override
    public ConnectVariableHeader variableHeader() {
        return (ConnectVariableHeader) super.variableHeader();
    }

    /**
     * Decode the credential from the wire payload (UTF-8 string).
     *
     * @return the decoded payload, or a payload with a null credential when absent
     */
    public ConnectPayload connectPayload() {
        byte[] p = super.payload();
        return new ConnectPayload(p == null ? null : new String(p, StandardCharsets.UTF_8));
    }
}