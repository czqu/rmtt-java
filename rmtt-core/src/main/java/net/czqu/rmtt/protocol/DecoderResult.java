package net.czqu.rmtt.protocol;

/** Decoding status of a decoded packet. */
public enum DecoderResult {
    /** Decoded cleanly. */
    SUCCESS,
    /** Decoding failed for an unspecified reason. */
    FAILURE,
    /** The frame violated the wire format. */
    PROTOCOL_VIOLATION,
    /** The offered protocol version is not supported. */
    BAD_PROTOCOL_VERSION,
    /** The server is not available. */
    SERVER_UNAVAILABLE,
    /** The credential was rejected. */
    UNAUTHORIZED;

    /**
     * Whether the packet decoded cleanly.
     *
     * @return true when the packet decoded with {@link #SUCCESS}
     */
    public boolean isSuccess() {
        return this == SUCCESS;
    }

    /**
     * Whether the packet failed to decode.
     *
     * @return true when the packet did not decode successfully
     */
    public boolean isFailure() {
        return !isSuccess();
    }
}