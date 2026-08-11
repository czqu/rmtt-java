package net.czqu.rmtt.protocol;

/**
 * CONNECT variable header: magic number (0x637a7175), protocol version, reserved flag,
 * keep-alive seconds. On the wire it is: magic(4) | version(1) | reserve(1) | keepalive(2).
 */
public final class ConnectVariableHeader {
    private final int magicNumber;
    private final int version;
    private final byte reserve;
    private final int keepAliveTimeSeconds;

    /**
     * Build a CONNECT header with the protocol magic number.
     *
     * @param version              the protocol version offered by the client
     * @param reserve              reserved flag byte (must be 0)
     * @param keepAliveTimeSeconds the client-proposed heartbeat interval in seconds
     */
    public ConnectVariableHeader(int version, byte reserve, int keepAliveTimeSeconds) {
        this(RmttProtocol.CONNECT_MAGIC_NUMBER, version, reserve, keepAliveTimeSeconds);
    }

    /**
     * Build a CONNECT header with an explicit magic number.
     *
     * @param magicNumber          the magic number written on the wire
     * @param version              the protocol version offered by the client
     * @param reserve              reserved flag byte (must be 0)
     * @param keepAliveTimeSeconds the client-proposed heartbeat interval in seconds
     */
    public ConnectVariableHeader(int magicNumber, int version, byte reserve, int keepAliveTimeSeconds) {
        this.magicNumber = magicNumber;
        this.version = version;
        this.reserve = reserve;
        this.keepAliveTimeSeconds = keepAliveTimeSeconds;
    }

    /**
     * The magic number.
     *
     * @return the magic number
     */
    public int magicNumber() { return magicNumber; }

    /**
     * The protocol version.
     *
     * @return the protocol version
     */
    public int version() { return version; }

    /**
     * The reserved flag byte.
     *
     * @return the reserved flag byte
     */
    public byte reserve() { return reserve; }

    /**
     * The client-proposed keepalive.
     *
     * @return the client-proposed keepalive in seconds (0 = unspecified)
     */
    public int keepAliveTimeSeconds() { return keepAliveTimeSeconds; }

    @Override
    public String toString() {
        return "ConnectVariableHeader{version=" + version + ",keepalive=" + keepAliveTimeSeconds + '}';
    }
}