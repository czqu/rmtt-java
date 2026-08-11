package net.czqu.rmtt.protocol;

/** DISCONNECT return/reason codes. */
public enum DisconnectReturnCode {
    /** Clean client-initiated disconnect. */
    NORMAL_DISCONNECT(0x00),
    /** The credential expired while the session was open. */
    CREDENTIAL_EXPIRED(0x01),
    /** Another connection took over the same device id. */
    SESSION_TAKEN_OVER(0x02),
    /** The server is shutting down. */
    SERVER_SHUTDOWN(0x03),
    /** The peer violated the wire format. */
    PROTOCOL_VIOLATION(0x04),
    /** No heartbeat received within the keepalive window. */
    KEEPALIVE_TIMEOUT(0x05),
    /** An administrator kicked the device. */
    KICKED_BY_ADMIN(0x06),
    /** The client exceeded its send rate budget. */
    RATE_LIMITED(0x07),
    /** The credential was rejected after the session closed. */
    CREDENTIAL_REJECTED(0x08),
    /** Any other failure. */
    UNKNOWN_ERROR(0xFE),
    /** Reserved value. */
    RESERVED(0xFF);

    /**
     * The wire byte of this code.
     */
    public final byte binaryCode;

    DisconnectReturnCode(int binaryCode) {
        this.binaryCode = (byte) binaryCode;
    }

    /**
     * Look up the code matching a wire byte.
     *
     * @param b the wire byte
     * @return the matching code, or {@link #RESERVED} for unknown values
     */
    public static DisconnectReturnCode valueOf(byte b) {
        for (DisconnectReturnCode c : values()) {
            if (c.binaryCode == b) {
                return c;
            }
        }
        return RESERVED;
    }
}