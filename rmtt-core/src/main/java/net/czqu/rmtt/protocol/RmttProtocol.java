package net.czqu.rmtt.protocol;

/** Wire-level sizes/constants of the RMTT protocol. */
public final class RmttProtocol {
    /** CONNECT magic number 0x637a7175 = "czqu". */
    public static final int CONNECT_MAGIC_NUMBER = 0x637a7175;
    /** Current protocol version offered by this implementation. */
    public static final int PROTOCOL_VERSION = 1;
    /** Earliest protocol version a server accepts. */
    public static final int PROTOCOL_MIN_VERSION = 1;
    /** Default cap on the total size of a single message (16 MiB, aligned with rmtt-go / rmtt-rust). */
    public static final int DEFAULT_MAX_BYTES_IN_MESSAGE = 16 * 1024 * 1024;
    /** Maximum credential length representable on the wire (2-byte length prefix). */
    public static final int MAX_CREDENTIAL_LENGTH = 0xFFFF;
    /** Default cap on the CONNECT credential length (the wire maximum, aligned with rmtt-go / rmtt-rust). */
    public static final int DEFAULT_MAX_CREDENTIAL_LENGTH = MAX_CREDENTIAL_LENGTH;

    private RmttProtocol() {
    }
}