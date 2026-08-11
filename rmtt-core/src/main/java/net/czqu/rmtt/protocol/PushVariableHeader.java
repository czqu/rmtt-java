package net.czqu.rmtt.protocol;

/** PUSH variable header: a single reserved byte. */
public final class PushVariableHeader {
    private final byte reserve;

    /**
     * Create a header with a reserved byte.
     *
     * @param reserve the reserved byte (must be 0)
     */
    public PushVariableHeader(byte reserve) {
        this.reserve = reserve;
    }

    /**
     * The reserved byte.
     *
     * @return the reserved byte
     */
    public byte reserve() {
        return reserve;
    }

    @Override
    public String toString() {
        return "PushVariableHeader{}";
    }
}