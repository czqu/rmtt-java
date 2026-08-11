package net.czqu.rmtt.protocol;

/** Factory + shared singletons for stateless RMTT messages. */
public final class RmttMessageFactory {

    /**
     * Shared stateless PINGREQ message.
     */
    public static final RmttMessage PINGREQ =
            new RmttMessage(new FixedHeader(RmttMessageType.PINGREQ, false, false, false, false, 0));
    /**
     * Shared stateless PINGRESP message.
     */
    public static final RmttMessage PINGRESP =
            new RmttMessage(new FixedHeader(RmttMessageType.PINGRESP, false, false, false, false, 0));
    /**
     * Alias of {@link #PINGREQ} kept for compatibility.
     */
    public static final RmttMessage PINGREQ_MESSAGE = PINGREQ;

    private RmttMessageFactory() {
    }

    /**
     * Build the typed message matching the fixed-header type.
     *
     * @param fixedHeader    the fixed header
     * @param variableHeader the transport-typed variable header
     * @param payload        the payload bytes, may be null
     * @return the concrete message subclass for the packet type
     */
    public static RmttMessage newMessage(FixedHeader fixedHeader, Object variableHeader, byte[] payload) {
        RmttMessageType t = fixedHeader.messageType();
        switch (t) {
            case CONNECT:
                return new ConnectMessage(fixedHeader, variableHeader, payload);
            case CONNACK:
                return new ConnAckMessage(fixedHeader, variableHeader, payload);
            case PUSH:
                return new PushMessage(fixedHeader, variableHeader, payload);
            case DISCONNECT:
                return new DisconnectMessage(fixedHeader, variableHeader, payload);
            default:
                return new RmttMessage(fixedHeader, variableHeader, payload);
        }
    }

    /**
     * Build a DISCONNECT packet carrying the given return code.
     *
     * @param code the return code to send
     * @return a new DISCONNECT message
     */
    public static DisconnectMessage disconnect(DisconnectReturnCode code) {
        return new DisconnectMessage(
                new FixedHeader(RmttMessageType.DISCONNECT, false, false, false, false, 1),
                new DisconnectVariableHeader(code.binaryCode), null);
    }
}