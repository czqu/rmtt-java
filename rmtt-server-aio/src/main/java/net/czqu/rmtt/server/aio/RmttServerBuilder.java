package net.czqu.rmtt.server.aio;

import net.czqu.rmtt.api.Authenticator;
import net.czqu.rmtt.api.ConnectionListener;
import net.czqu.rmtt.api.ConnectionStore;
import net.czqu.rmtt.api.RmttMessageHandler;

import javax.net.ssl.SSLContext;

import net.czqu.rmtt.protocol.RmttProtocol;
import net.czqu.rmtt.protocol.ServerKeepalivePolicy;

/**
 * Fluent builder for the AIO {@link RmttServer}.
 *
 * <p>General library rule: TLS material is never generated or guessed here. If a TLS listener
 * ({@link #tlsPort(int)} or {@link #wssPort(int)}) is requested, an {@link SSLContext} MUST be
 * supplied via {@link #sslContext(SSLContext)}; otherwise {@link #build()} throws.</p>
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
    private int tlsPort = 0;
    private int wsPort = 0;
    private int wssPort = 0;
    private SSLContext sslContext;
    private ServerKeepalivePolicy keepalivePolicy = ServerKeepalivePolicy.defaults();
    private int maxConnections = 10_000;
    private int maxBytesInMessage = RmttProtocol.DEFAULT_MAX_BYTES_IN_MESSAGE;
    private int maxCredentialLength = RmttProtocol.DEFAULT_MAX_CREDENTIAL_LENGTH;

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
     * Raw TCP listener port.
     *
     * @param port the TCP port
     * @return this builder
     */
    public RmttServerBuilder port(int port) {
        this.port = port;
        return this;
    }

    /**
     * TLS (tls://) listener port. Requires {@link #sslContext(SSLContext)}.
     *
     * @param tlsPort the TLS port, 0 to disable
     * @return this builder
     */
    public RmttServerBuilder tlsPort(int tlsPort) {
        this.tlsPort = tlsPort;
        return this;
    }

    /**
     * Plain WebSocket (ws://) listener port.
     *
     * @param wsPort the WebSocket port, 0 to disable
     * @return this builder
     */
    public RmttServerBuilder wsPort(int wsPort) {
        this.wsPort = wsPort;
        return this;
    }

    /**
     * Secure WebSocket (wss://) listener port. Requires {@link #sslContext(SSLContext)}.
     *
     * @param wssPort the WSS port, 0 to disable
     * @return this builder
     */
    public RmttServerBuilder wssPort(int wssPort) {
        this.wssPort = wssPort;
        return this;
    }

    /**
     * TLS material for tls:// and wss:// listeners. REQUIRED when those are enabled.
     *
     * @param sslContext the TLS context
     * @return this builder
     */
    public RmttServerBuilder sslContext(SSLContext sslContext) {
        this.sslContext = sslContext;
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
     * Maximum concurrent connections accepted. Connections beyond the limit are closed
     * immediately. Defaults to 10000; pass 0 for unlimited.
     *
     * @param maxConnections the connection cap (0 = unlimited)
     * @return this builder
     */
    public RmttServerBuilder maxConnections(int maxConnections) {
        this.maxConnections = maxConnections;
        return this;
    }

    /**
     * Maximum total size of a single RMTT message. Frames declaring more are rejected. Defaults to
     * 16 MiB, matching rmtt-go / rmtt-rust.
     *
     * @param maxBytesInMessage the frame size cap in bytes (&gt; 0)
     * @return this builder
     */
    public RmttServerBuilder maxBytesInMessage(int maxBytesInMessage) {
        if (maxBytesInMessage <= 0) {
            throw new IllegalArgumentException("maxBytesInMessage must be > 0");
        }
        this.maxBytesInMessage = maxBytesInMessage;
        return this;
    }

    /**
     * Maximum accepted CONNECT credential length. Longer credentials are rejected with CONNACK
     * 0x03. Defaults to 65535 (the wire maximum), matching rmtt-go / rmtt-rust.
     *
     * @param maxCredentialLength the credential size cap in bytes (&gt; 0)
     * @return this builder
     */
    public RmttServerBuilder maxCredentialLength(int maxCredentialLength) {
        if (maxCredentialLength <= 0 || maxCredentialLength > RmttProtocol.MAX_CREDENTIAL_LENGTH) {
            throw new IllegalArgumentException("maxCredentialLength must be in [1, "
                    + RmttProtocol.MAX_CREDENTIAL_LENGTH + "]");
        }
        this.maxCredentialLength = maxCredentialLength;
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
        if ((tlsPort > 0 || wssPort > 0) && sslContext == null) {
            throw new IllegalStateException(
                    "tlsPort/wssPort require a caller-provided SSLContext via sslContext(...)");
        }
        return new RmttServer(connectionStore, authenticator, messageHandler,
                connectionListener, port, tlsPort, wsPort, wssPort, sslContext, keepalivePolicy,
                maxConnections, maxBytesInMessage, maxCredentialLength);
    }
}