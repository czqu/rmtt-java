package net.czqu.rmtt.client.netty;

import io.netty.handler.ssl.SslContext;

import java.util.concurrent.TimeUnit;

/**
 * Fluent builder for {@link RmttClient}.
 *
 * <p>This is a general library: it never trusts a server implicitly. When {@link #secure(boolean)}
 * is enabled an {@link SslContext} MUST be supplied via {@link #sslContext(SslContext)}; otherwise
 * {@link #build()} throws. Applications that want to trust-all (e.g. against a self-signed demo
 * server) must construct the trust-all context themselves. When {@link #quic(boolean)} is enabled a
 * {@link io.netty.handler.codec.quic.QuicSslContext} MUST be supplied via
 * {@link #quicSslContext(io.netty.handler.codec.quic.QuicSslContext)}.</p>
 */
public final class RmttClientBuilder {

    /** Create a builder with the default configuration. */
    public RmttClientBuilder() {
    }

    private String host = "127.0.0.1";
    private int port = 18883;
    private String credential;
    private int keepAliveSeconds = 30;
    private boolean adaptiveHeartbeat = false;
    private int adaptiveShortSeconds = 10;
    private int adaptiveMaxSeconds = 60;
    private int probeCount = 3;
    private long responseWindowMillis = 2000;
    private int fineStepSeconds = 5;
    private long connectTimeoutMillis = 5000;
    private long writeTimeoutMillis = 5000;
    private RmttPushHandler pushHandler;
    private boolean webSocket = false;
    private boolean secure = false;
    private String wsPath = "/rmtt";
    private SslContext sslContext;
    private boolean autoReconnect = true;
    private boolean connectRetry = true;
    private long reconnectBaseMillis = 1000;
    private long maxReconnectIntervalMillis = TimeUnit.MINUTES.toMillis(10);
    private float reconnectJitter = 0.25f;
    private boolean kcp = false;
    private boolean quic = false;
    private io.netty.handler.codec.quic.QuicSslContext quicSslContext;

    /**
     * Server host.
     *
     * @param host the hostname or IP
     * @return this builder
     */
    public RmttClientBuilder host(String host) {
        this.host = host;
        return this;
    }

    /**
     * Server port.
     *
     * @param port the port
     * @return this builder
     */
    public RmttClientBuilder port(int port) {
        this.port = port;
        return this;
    }

    /**
     * Required. Arbitrary credential carried by CONNECT (token / device id / user-pass / ...).
     *
     * @param credential the credential
     * @return this builder
     */
    public RmttClientBuilder credential(String credential) {
        this.credential = credential;
        return this;
    }

    /**
     * Proposed keepalive interval in seconds.
     *
     * @param keepAliveSeconds the proposal
     * @return this builder
     */
    public RmttClientBuilder keepAliveSeconds(int keepAliveSeconds) {
        this.keepAliveSeconds = keepAliveSeconds;
        return this;
    }

    /**
     * Enable adaptive heartbeat. The client probes the maximum sustainable heartbeat
     * interval within {@code [shortSeconds, maxSeconds]} and settles at ~90% of the found maximum.
     * Incompatible with {@link #keepAliveSeconds(int)}. The negotiated server_kp from CONNACK caps
     * the probing ceiling.
     *
     * @param shortSeconds preset short heartbeat period used for initial liveness probes (&gt;=1)
     * @param maxSeconds   upper bound of the adaptive range (&gt;= shortSeconds)
     * @return this builder
     */
    public RmttClientBuilder adaptiveHeartbeat(int shortSeconds, int maxSeconds) {
        if (shortSeconds < 1) {
            throw new IllegalArgumentException("shortSeconds must be >= 1");
        }
        if (maxSeconds < shortSeconds) {
            throw new IllegalArgumentException("maxSeconds must be >= shortSeconds");
        }
        this.adaptiveHeartbeat = true;
        this.adaptiveShortSeconds = shortSeconds;
        this.adaptiveMaxSeconds = maxSeconds;
        return this;
    }

    /**
     * Consecutive successful short heartbeats before entering the probing phase. Default 3.
     *
     * @param probeCount the probe count
     * @return this builder
     */
    public RmttClientBuilder probeCount(int probeCount) {
        this.probeCount = probeCount;
        return this;
    }

    /**
     * Max wait for a PINGRESP before counting a probe as failed. Default 2000ms.
     *
     * @param responseWindowMillis the response window
     * @return this builder
     */
    public RmttClientBuilder responseWindowMillis(long responseWindowMillis) {
        this.responseWindowMillis = responseWindowMillis;
        return this;
    }

    /**
     * Nudge step used in the fine-tuning probing phase (seconds). Default 5.
     *
     * @param fineStepSeconds the nudge step
     * @return this builder
     */
    public RmttClientBuilder fineStepSeconds(int fineStepSeconds) {
        this.fineStepSeconds = fineStepSeconds;
        return this;
    }

    /**
     * Connect timeout for the transport handshake.
     *
     * @param connectTimeoutMillis the timeout in millis
     * @return this builder
     */
    public RmttClientBuilder connectTimeoutMillis(long connectTimeoutMillis) {
        this.connectTimeoutMillis = connectTimeoutMillis;
        return this;
    }

    /**
     * Write timeout for synchronous writes.
     *
     * @param writeTimeoutMillis the timeout in millis
     * @return this builder
     */
    public RmttClientBuilder writeTimeoutMillis(long writeTimeoutMillis) {
        this.writeTimeoutMillis = writeTimeoutMillis;
        return this;
    }

    /**
     * Downstream PUSH callback.
     *
     * @param pushHandler the handler
     * @return this builder
     */
    public RmttClientBuilder pushHandler(RmttPushHandler pushHandler) {
        this.pushHandler = pushHandler;
        return this;
    }

    /**
     * Connect over WebSocket (ws://) instead of raw TCP.
     *
     * @param webSocket true to use WebSocket
     * @return this builder
     */
    public RmttClientBuilder webSocket(boolean webSocket) {
        this.webSocket = webSocket;
        return this;
    }

    /**
     * Secure transport (wss://). REQUIRES {@link #sslContext(SslContext)}.
     *
     * @param secure true to use TLS
     * @return this builder
     */
    public RmttClientBuilder secure(boolean secure) {
        this.secure = secure;
        return this;
    }

    /**
     * WebSocket endpoint path (default "/rmtt"). Must match the server's {@code wsPath}.
     *
     * @param wsPath the endpoint path
     * @return this builder
     */
    public RmttClientBuilder wsPath(String wsPath) {
        this.wsPath = wsPath == null || wsPath.isEmpty() ? "/rmtt" : wsPath;
        return this;
    }

    /**
     * TLS context used for TLS/wss. REQUIRED when {@link #secure(boolean)} is enabled.
     *
     * @param sslContext the TLS context
     * @return this builder
     */
    public RmttClientBuilder sslContext(SslContext sslContext) {
        this.sslContext = sslContext;
        return this;
    }

    /**
     * Reconnect automatically after an abnormal disconnect. Default true.
     *
     * @param autoReconnect true to enable
     * @return this builder
     */
    public RmttClientBuilder autoReconnect(boolean autoReconnect) {
        this.autoReconnect = autoReconnect;
        return this;
    }

    /**
     * Keep retrying if a connection/initial attempt fails. Default true.
     *
     * @param connectRetry true to enable
     * @return this builder
     */
    public RmttClientBuilder connectRetry(boolean connectRetry) {
        this.connectRetry = connectRetry;
        return this;
    }

    /**
     * Reconnect backoff base interval. Default 1s.
     *
     * @param reconnectBaseMillis the base interval in millis
     * @return this builder
     */
    public RmttClientBuilder reconnectBaseMillis(long reconnectBaseMillis) {
        this.reconnectBaseMillis = reconnectBaseMillis;
        return this;
    }

    /**
     * Reconnect backoff upper bound. Default 10min.
     *
     * @param maxReconnectIntervalMillis the upper bound in millis
     * @return this builder
     */
    public RmttClientBuilder maxReconnectIntervalMillis(long maxReconnectIntervalMillis) {
        this.maxReconnectIntervalMillis = maxReconnectIntervalMillis;
        return this;
    }

    /**
     * Reconnect backoff jitter (default ±25%, i.e. 0.25).
     *
     * @param reconnectJitter the jitter fraction
     * @return this builder
     */
    public RmttClientBuilder reconnectJitter(float reconnectJitter) {
        this.reconnectJitter = reconnectJitter;
        return this;
    }

    /**
     * Connect over KCP (UDP) instead of TCP. Incompatible with {@link #webSocket(boolean)}.
     *
     * @param kcp true to use KCP
     * @return this builder
     */
    public RmttClientBuilder kcp(boolean kcp) {
        this.kcp = kcp;
        return this;
    }

    /**
     * Connect over QUIC instead of TCP. Incompatible with {@link #webSocket(boolean)} and {@link #kcp(boolean)}.
     *
     * @param quic true to use QUIC
     * @return this builder
     */
    public RmttClientBuilder quic(boolean quic) {
        this.quic = quic;
        return this;
    }

    /**
     * QUIC TLS context. REQUIRED when {@link #quic(boolean)} is enabled (e.g. self-signed).
     *
     * @param quicSslContext the QUIC TLS context
     * @return this builder
     */
    public RmttClientBuilder quicSslContext(io.netty.handler.codec.quic.QuicSslContext quicSslContext) {
        this.quicSslContext = quicSslContext;
        return this;
    }

    /**
     * Validate the configuration and build the client.
     *
     * @return the configured client
     * @throws IllegalStateException when a required dependency is missing
     */
    public RmttClient build() {
        if (credential == null) {
            throw new IllegalStateException("credential is required");
        }
        if (secure && sslContext == null) {
            throw new IllegalStateException(
                    "secure(true) requires a caller-provided SslContext via sslContext(...)");
        }
        if (kcp && webSocket) {
            throw new IllegalStateException("kcp(true) is incompatible with webSocket(true)");
        }
        if (quic && (webSocket || kcp)) {
            throw new IllegalStateException("quic(true) is incompatible with webSocket(true)/kcp(true)");
        }
        if (quic && quicSslContext == null) {
            throw new IllegalStateException(
                    "quic(true) requires a caller-provided QuicSslContext via quicSslContext(...)");
        }
        if (adaptiveHeartbeat && kcp) {
            throw new IllegalStateException("adaptiveHeartbeat is incompatible with kcp(true)");
        }
        return new RmttClient(host, port, credential, keepAliveSeconds, adaptiveHeartbeat,
                adaptiveShortSeconds, adaptiveMaxSeconds, probeCount, responseWindowMillis, fineStepSeconds,
                connectTimeoutMillis, writeTimeoutMillis, pushHandler,
                webSocket, secure, wsPath, sslContext,
                autoReconnect, connectRetry, reconnectBaseMillis, maxReconnectIntervalMillis, reconnectJitter,
                kcp, quic, quicSslContext);
    }
}