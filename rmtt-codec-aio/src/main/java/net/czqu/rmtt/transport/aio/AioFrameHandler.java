package net.czqu.rmtt.transport.aio;

import net.czqu.rmtt.protocol.RmttMessage;

/** Callback surface for a fully-multiplexed AIO connection. */
public interface AioFrameHandler {

    /**
     * A decoded RMTT message body arrived.
     *
     * @param msg the decoded message
     */
    void onMessage(RmttMessage msg);

    /** All handshakes (TLS and/or WebSocket) completed; the connection is ready for RMTT traffic. */
    void onReady();

    /**
     * The connection was closed / failed.
     *
     * @param cause the reason, or null for a clean close
     */
    void onClosed(Throwable cause);
}