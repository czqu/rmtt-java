package net.czqu.rmtt.protocol;

/** RMTT control-packet message types (fixed-header high nibble). */
public enum RmttMessageType {
    /** CONNECT packet. */
    CONNECT(1),
    /** CONNACK packet. */
    CONNACK(2),
    /** PUSH packet. */
    PUSH(3),
    /** PINGREQ packet. */
    PINGREQ(5),
    /** PINGRESP packet. */
    PINGRESP(6),
    /** DISCONNECT packet. */
    DISCONNECT(14),
    /** Unknown / reserved type. */
    RESERVED(0);

    private final byte value;

    RmttMessageType(int value) {
        this.value = (byte) value;
    }

    /**
     * The fixed-header high nibble.
     *
     * @return the wire value of this type
     */
    public byte value() {
        return value;
    }

    /**
     * Resolve a wire value to a type.
     *
     * @param v the wire byte
     * @return the matching type, or {@link #RESERVED} for unknown values
     */
    public static RmttMessageType valueOf(byte v) {
        switch (v) {
            case 1: return CONNECT;
            case 2: return CONNACK;
            case 3: return PUSH;
            case 5: return PINGREQ;
            case 6: return PINGRESP;
            case 14: return DISCONNECT;
            default: return RESERVED;
        }
    }
}