package net.czqu.rmtt.protocol;

/** CONNECT protocol version. The wire always carries 1 for v1. */
public enum RmttVersion {
    /** Protocol version 1. */
    VERSION_1(1);

    private final int value;

    RmttVersion(int value) {
        this.value = value;
    }

    /**
     * The wire value of this version.
     *
     * @return the wire value
     */
    public int value() {
        return value;
    }

    /**
     * Resolve a wire value to a version.
     *
     * @param v the wire value
     * @return the matching version, or null for unknown values
     */
    public static RmttVersion valueOf(int v) {
        return v == 1 ? VERSION_1 : null;
    }
}