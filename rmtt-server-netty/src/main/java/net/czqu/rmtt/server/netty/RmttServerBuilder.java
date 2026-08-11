package net.czqu.rmtt.server.netty;

import io.netty.handler.ssl.SslContext;
import net.czqu.rmtt.api.Authenticator;
import net.czqu.rmtt.api.ConnectionListener;
import net.czqu.rmtt.api.ConnectionStore;
import net.czqu.rmtt.api.RmttMessageHandler;
import net.czqu.rmtt.protocol.ServerKeepalivePolicy;

/**
 * Fluent builder for {@link RmttServer}.
 *
 * <p>This is a general library: it never generates or guesses TLS material for the caller. If a
 * secure WebSocket (wss) listener is requested, an {@link SslContext} MUST be supplied via
 * {@link #wssSslContext(SslContext)}; otherwise {@link #build()} throws. Applications that want a
 * self-signed certificate are expected to build one themselves (see the examples).</p>
 */
public final class RmttServerBuilder {

    /** Create a builder with the default configuration. */
    public RmttServerBuilder() {
    }

    private final ConnectionStore connectionStore = new ConnectionStore();
    private Authenticator authenticator;
    private RmttMessageHandler messageHandler;
    private ConnectionListener connectionListener = ConnectionListener.NOOP;
    private int port = 18883;
    private int wsPort = 0;
    private int wssPort = 0;
    private int kcpPort = 0;
    private int quicPort = 0;
    private io.netty.handler.codec.quic.QuicSslContext quicSslContext;
    private String wsPath = "/rmtt";
    private SslContext wssSslContext;
    private ServerKeepalivePolicy keepalivePolicy = ServerKeepalivePolicy.defaults();

    /**
     * Required. The application-injected authentication policy (JWT / plain id / user-pass / ...).
     *
     * @param authenticator the authenticator
     * @return this builder
     */
    public RmttServerBuilder authenticator(Authenticator authenticator) {
        this.authenticator = authenticator;
        return this;
    }

    /**
     * Called for every upstream PUSH received from a device (required).
     *
     * @param messageHandler the message handler
     * @return this builder
     */
    public RmttServerBuilder messageHandler(RmttMessageHandler messageHandler) {
        this.messageHandler = messageHandler;
        return this;
    }

    /**
     * Optional lifecycle listener; defaults to a no-op.
     *
     * @param connectionListener the listener, or null for none
     * @return this builder
     */
    public RmttServerBuilder connectionListener(ConnectionListener connectionListener) {
        this.connectionListener = connectionListener == null ? ConnectionListener.NOOP : connectionListener;
        return this;
    }

    /**
     * TCP listener port.
     *
     * @param port the TCP port
     * @return this builder
     */
    public RmttServerBuilder port(int port) {
        this.port = port;
        return this;
    }

    /**
     * Bind a plain WebSocket (ws://) listener on this port in addition to the TCP listener.
     *
     * @param wsPort the WebSocket port, 0 to disable
     * @return this builder
     */
    public RmttServerBuilder wsPort(int wsPort) {
        this.wsPort = wsPort;
        return this;
    }

    /**
     * Bind a KCP (UDP) listener on this port in addition to the TCP listener.
     *
     * @param kcpPort the KCP port, 0 to disable
     * @return this builder
     */
    public RmttServerBuilder kcpPort(int kcpPort) {
        this.kcpPort = kcpPort;
        return this;
    }

    /**
     * Bind a QUIC (UDP) listener on this port. Requires {@link #quicSslContext(io.netty.handler.codec.quic.QuicSslContext)}.
     *
     * @param quicPort the QUIC port, 0 to disable
     * @return this builder
     */
    public RmttServerBuilder quicPort(int quicPort) {
        this.quicPort = quicPort;
        return this;
    }

    /**
     * QUIC TLS context served on the {@link #quicPort(int)} listener. REQUIRED when that port is used.
     *
     * @param quicSslContext the QUIC TLS context
     * @return this builder
     */
    public RmttServerBuilder quicSslContext(io.netty.handler.codec.quic.QuicSslContext quicSslContext) {
        this.quicSslContext = quicSslContext;
        return this;
    }

    /**
     * WebSocket endpoint path (default "/rmtt"). Must match what clients connect to.
     *
     * @param wsPath the endpoint path
     * @return this builder
     */
    public RmttServerBuilder wsPath(String wsPath) {
        this.wsPath = wsPath == null || wsPath.isEmpty() ? "/rmtt" : wsPath;
        return this;
    }

    /**
     * Bind a secure WebSocket (wss://) listener on this port. Requires {@link #wssSslContext(SslContext)}.
     *
     * @param wssPort the WSS port, 0 to disable
     * @return this builder
     */
    public RmttServerBuilder wssPort(int wssPort) {
        this.wssPort = wssPort;
        return this;
    }

    /**
     * TLS context served on the wss listener. REQUIRED when {@link #wssPort(int)} is used.
     *
     * @param wssSslContext the TLS context
     * @return this builder
     */
    public RmttServerBuilder wssSslContext(SslContext wssSslContext) {
        this.wssSslContext = wssSslContext;
        return this;
    }

    /**
     * Default keepalive when the client does not specify one. Rebased onto {@code keepalivePolicy}.
     *
     * @param heartbeatSeconds the default interval in seconds
     * @return this builder
     */
    public RmttServerBuilder heartbeatSeconds(long heartbeatSeconds) {
        this.keepalivePolicy = new ServerKeepalivePolicy(
                (int) Math.min(heartbeatSeconds, keepalivePolicy.minSeconds()),
                keepalivePolicy.maxSeconds(),
                (int) heartbeatSeconds,
                keepalivePolicy.allowDisable());
        return this;
    }

    /**
     * Set the keepalive negotiation policy explicitly (overrides {@link #heartbeatSeconds}).
     *
     * @param keepalivePolicy the policy, null to restore the defaults
     * @return this builder
     */
    public RmttServerBuilder keepalivePolicy(ServerKeepalivePolicy keepalivePolicy) {
        this.keepalivePolicy = keepalivePolicy == null ? ServerKeepalivePolicy.defaults() : keepalivePolicy;
        return this;
    }

    /**
     * Validate the configuration and build the server.
     *
     * @return the configured server
     * @throws IllegalStateException when a required dependency is missing
     */
    public RmttServer build() {
        if (authenticator == null) {
            throw new IllegalStateException("authenticator is required");
        }
        if (messageHandler == null) {
            throw new IllegalStateException("messageHandler is required");
        }
        if (wssPort > 0 && wssSslContext == null) {
            throw new IllegalStateException(
                    "wssPort(" + wssPort + ") requires a caller-provided SslContext via wssSslContext(...)");
        }
        if (quicPort > 0 && quicSslContext == null) {
            throw new IllegalStateException(
                    "quicPort(" + quicPort + ") requires a caller-provided QuicSslContext via quicSslContext(...)");
        }
        return new RmttServer(connectionStore, authenticator, messageHandler,
                connectionListener, port, wsPort, wssPort, wsPath, wssSslContext, keepalivePolicy, kcpPort,
                quicPort, quicSslContext);
    }
}