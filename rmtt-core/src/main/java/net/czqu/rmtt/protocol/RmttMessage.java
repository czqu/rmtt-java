package net.czqu.rmtt.protocol;

/** Base class for all decoded/encoded RMTT packets. Payload is a reference-count-free byte[]. */
public class RmttMessage {

    /** The parsed fixed header of the packet. */
    public final FixedHeader fixedHeader;
    private final Object variableHeader;
    private final byte[] payload;
    private final DecoderResult decoderResult;

    /**
     * Create a message without variable header or payload.
     *
     * @param fixedHeader the fixed header
     */
    public RmttMessage(FixedHeader fixedHeader) {
        this(fixedHeader, null, null, DecoderResult.SUCCESS);
    }

    /**
     * Create a message with a variable header and no payload.
     *
     * @param fixedHeader    the fixed header
     * @param variableHeader the transport-typed variable header
     */
    public RmttMessage(FixedHeader fixedHeader, Object variableHeader) {
        this(fixedHeader, variableHeader, null, DecoderResult.SUCCESS);
    }

    /**
     * Create a message with a variable header and payload.
     *
     * @param fixedHeader    the fixed header
     * @param variableHeader the transport-typed variable header
     * @param payload        the payload bytes, may be null
     */
    public RmttMessage(FixedHeader fixedHeader, Object variableHeader, byte[] payload) {
        this(fixedHeader, variableHeader, payload, DecoderResult.SUCCESS);
    }

    /**
     * Create a message with an explicit decoding status.
     *
     * @param fixedHeader    the fixed header
     * @param variableHeader the transport-typed variable header
     * @param payload        the payload bytes, may be null
     * @param decoderResult  the decoding status
     */
    public RmttMessage(FixedHeader fixedHeader, Object variableHeader, byte[] payload, DecoderResult decoderResult) {
        this.fixedHeader = fixedHeader;
        this.variableHeader = variableHeader;
        this.payload = payload;
        this.decoderResult = decoderResult;
    }

    /**
     * The fixed header.
     *
     * @return the fixed header
     */
    public FixedHeader fixedHeader() {
        return fixedHeader;
    }

    /**
     * The variable header, typed per concrete message subclass.
     *
     * @return the variable header, typed per concrete message subclass
     */
    public Object variableHeader() {
        return variableHeader;
    }

    /**
     * The payload bytes.
     *
     * @return the payload bytes, may be null
     */
    public byte[] payload() {
        return payload;
    }

    /**
     * The decoding status.
     *
     * @return the decoding status of this packet
     */
    public DecoderResult decoderResult() {
        return decoderResult;
    }

    @Override
    public String toString() {
        return "RmttMessage{" + fixedHeader + ",payload=" + (payload == null ? 0 : payload.length) + "b}";
    }
}