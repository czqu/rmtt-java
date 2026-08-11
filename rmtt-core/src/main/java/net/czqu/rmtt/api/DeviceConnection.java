package net.czqu.rmtt.api;

import net.czqu.rmtt.protocol.DisconnectReturnCode;

/**
 * Minimal transport-agnostic handle on an established device connection, used by the shared
 * route table ({@link ConnectionStore}) and the downstream push path. Each stack (netty / aio)
 * provides its own implementation over its native channel.
 */
public interface DeviceConnection {

    /**
     * Whether the underlying connection is still open and writable.
     *
     * @return true if the connection can still carry frames
     */
    boolean isActive();

    /**
     * Write an already-encoded RMTT frame.
     *
     * @param frame the encoded frame bytes
     * @return true if the write was accepted into the write path
     */
    boolean write(byte[] frame);

    /**
     * Best-effort: send a DISCONNECT carrying {@code code}, then close.
     *
     * @param code the DISCONNECT return code to send
     */
    void sendDisconnect(DisconnectReturnCode code);

    /** Close the connection immediately. */
    void close();
}