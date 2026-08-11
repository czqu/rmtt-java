package net.czqu.rmtt.protocol;

/** Wire-level sizes/constants of the RMTT protocol. */
public final class RmttProtocol {
    /** CONNECT magic number 0x637a7175 = "czqu". */
    public static final int CONNECT_MAGIC_NUMBER = 0x637a7175;
    /** Current protocol version offered by this implementation. */
    public static final int PROTOCOL_VERSION = 1;
    /** Earliest protocol version a server accepts. */
    public static final int PROTOCOL_MIN_VERSION = 1;
    /** Default cap on the total size of a single message. */
    public static final int DEFAULT_MAX_BYTES_IN_MESSAGE = 8092;
    /** Default cap on the CONNECT credential length. */
    public static final int DEFAULT_MAX_CREDENTIAL_LENGTH = 23;

    private RmttProtocol() {
    }
}