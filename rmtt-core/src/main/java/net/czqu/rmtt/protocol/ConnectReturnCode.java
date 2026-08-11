package net.czqu.rmtt.protocol;

/** CONNACK return codes. */
public enum ConnectReturnCode {
    /** CONNECT accepted. */
    CONNECT_ACCEPTED(0x00),
    /** The offered protocol version is not supported. */
    CONNECT_BAD_PROTOCOL_VERSION(0x01),
    /** The server cannot accept the connection right now. */
    CONNECT_SERVER_UNAVAILABLE(0x02),
    /** The credential was rejected. */
    CONNECT_UNAUTHORIZED(0x03),
    /** Any other failure. */
    CONNECT_UNKNOWN_ERROR(0xFE),
    /** Reserved value, never sent. */
    CONNECT_RESERVED(0xFF);

    private final byte byteValue;

    ConnectReturnCode(int byteValue) {
        this.byteValue = (byte) byteValue;
    }

    /**
     * The wire byte of this code.
     *
     * @return the one-byte wire value of this code
     */
    public byte byteValue() {
        return byteValue;
    }

    /**
     * Look up the code matching a wire byte.
     *
     * @param b the wire byte
     * @return the matching code, or {@link #CONNECT_UNKNOWN_ERROR} for unknown values
     */
    public static ConnectReturnCode valueOf(byte b) {
        for (ConnectReturnCode c : values()) {
            if (c.byteValue == b) {
                return c;
            }
        }
        return CONNECT_UNKNOWN_ERROR;
    }
}