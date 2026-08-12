package net.czqu.rmtt.client.aio;

import net.czqu.rmtt.protocol.ConnectReturnCode;
import net.czqu.rmtt.protocol.DisconnectReturnCode;
import net.czqu.rmtt.protocol.FixedHeader;
import net.czqu.rmtt.protocol.PushMessage;
import net.czqu.rmtt.protocol.PushVariableHeader;
import net.czqu.rmtt.protocol.ReconnectBackoff;
import net.czqu.rmtt.protocol.RmttMessage;
import net.czqu.rmtt.protocol.RmttMessageFactory;
import net.czqu.rmtt.protocol.RmttMessageType;
import net.czqu.rmtt.protocol.RmttWireCodec;
import net.czqu.rmtt.protocol.AdaptiveHeartbeat;
import net.czqu.rmtt.logging.InternalLogger;
import net.czqu.rmtt.logging.InternalLoggerFactory;
import net.czqu.rmtt.transport.aio.AioConnection;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/** AIO-backed RMTT client supporting tcp / tls / ws / wss transports. */
public final class RmttClient {

    private static final InternalLogger LOG = InternalLoggerFactory.getLogger(RmttClient.class);

    private final String host;
    private final int port;
    private final String credential;
    private final int keepAliveSeconds;
    private final boolean adaptiveHeartbeat;
    private final int adaptiveShortSeconds;
    private final int adaptiveMaxSeconds;
    private final int probeCount;
    private final long responseWindowMillis;
    private final int fineStepSeconds;
    private final long connectTimeoutMillis;
    private final RmttPushHandler pushHandler;
    private final boolean tls;
    private final boolean webSocket;
    private final String wsPath;
    private final SSLContext sslContext;
    private final boolean autoReconnect;
    private final boolean connectRetry;
    private final long reconnectBaseMillis;
    private final long maxReconnectIntervalMillis;
    private final float reconnectJitter;

    private final CompletableFuture<ConnectReturnCode> connectFuture = new CompletableFuture<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile AioConnection conn;
    private volatile AioClientSession session;
    private volatile AsynchronousSocketChannel channel;
    private volatile boolean connected;
    private volatile int serverKp;
    private volatile boolean manuallyDisconnecting;
    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> adaptiveTick;
    private ReconnectBackoff reconnectBackoff;

    RmttClient(String host,
               int port,
               String credential,
               int keepAliveSeconds,
               boolean adaptiveHeartbeat,
               int adaptiveShortSeconds,
               int adaptiveMaxSeconds,
               int probeCount,
               long responseWindowMillis,
               int fineStepSeconds,
               long connectTimeoutMillis,
               RmttPushHandler pushHandler,
               boolean tls,
               boolean webSocket,
               String wsPath,
               SSLContext sslContext,
               boolean autoReconnect,
               boolean connectRetry,
               long reconnectBaseMillis,
               long maxReconnectIntervalMillis,
               float reconnectJitter) {
        this.host = host;
        this.port = port;
        this.credential = credential;
        this.keepAliveSeconds = keepAliveSeconds;
        this.adaptiveHeartbeat = adaptiveHeartbeat;
        this.adaptiveShortSeconds = adaptiveShortSeconds;
        this.adaptiveMaxSeconds = adaptiveMaxSeconds;
        this.probeCount = probeCount;
        this.responseWindowMillis = responseWindowMillis;
        this.fineStepSeconds = fineStepSeconds;
        this.connectTimeoutMillis = connectTimeoutMillis;
        this.pushHandler = pushHandler;
        this.tls = tls;
        this.webSocket = webSocket;
        this.wsPath = wsPath == null || wsPath.isEmpty() ? "/rmtt" : wsPath;
        this.sslContext = sslContext;
        this.autoReconnect = autoReconnect;
        this.connectRetry = connectRetry;
        this.reconnectBaseMillis = reconnectBaseMillis;
        this.maxReconnectIntervalMillis = maxReconnectIntervalMillis;
        this.reconnectJitter = reconnectJitter;
    }

    /**
     * Connect and wait for the CONNACK handshake.
     *
     * @return the server's CONNACK return code
     * @throws IOException          when the socket cannot be opened or connected
     * @throws TimeoutException     when the handshake does not complete in time
     * @throws InterruptedException when the wait is interrupted
     */
    public ConnectReturnCode connect() throws IOException, TimeoutException, InterruptedException {
        if (closed.get()) {
            throw new IOException("client is closed");
        }
        scheduler = Executors.newSingleThreadScheduledExecutor();
        LOG.info("connecting {}://{}:{} credential={} keepalive={}s",
                webSocket ? "ws" : (tls ? "tls" : "tcp"), host, port, credential,
                connectKeepaliveProposal());
        return connectOnce();
    }

    private ConnectReturnCode connectOnce() throws IOException, TimeoutException, InterruptedException {
        AsynchronousSocketChannel ch = AsynchronousSocketChannel.open();
        try {
            ch.connect(new InetSocketAddress(host, port)).get(connectTimeoutMillis, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw new IOException(e.getCause() == null ? e : e.getCause());
        }
        this.channel = ch;

        AioClientSession s = new AioClientSession(credential, connectKeepaliveProposal(), pushHandler, connectFuture,
                new AioClientSession.SessionEvents() {
                    @Override
                    public void onConnAck(ConnectReturnCode code, int kp) {
                        serverKp = kp;
                        connected = code == ConnectReturnCode.CONNECT_ACCEPTED;
                        if (connected) {
                            LOG.info("CONNACK code={} serverKp={}s -> {} heartbeat",
                                    code, kp, adaptiveHeartbeat
                                            ? "adaptive (short=" + adaptiveShortSeconds + "s max=" + adaptiveMaxSeconds + "s)"
                                            : (kp > 0 ? "fixed " + kp + "s" : "disabled"));
                            if (adaptiveHeartbeat) {
                                startAdaptiveHeartbeat();
                            } else {
                                scheduleHeartbeat();
                            }
                        } else {
                            LOG.warn("CONNACK rejected code={}", code);
                        }
                    }

                    @Override
                    public void onDisconnect(DisconnectReturnCode code) {
                        onConnectionLost(code == DisconnectReturnCode.NORMAL_DISCONNECT
                                ? "normal disconnect" : "disconnect " + code);
                    }
                });
        this.session = s;
        AioConnection connection = new AioConnection(ch, true, webSocket,
                tls ? () -> clientEngine() : null, s);
        s.bind(connection);
        connection.wsHost(host, port);
        this.conn = connection;
        connection.wsPath(wsPath);
        connection.start();

        ConnectReturnCode code;
        try {
            code = connectFuture.get(connectTimeoutMillis, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw new IOException(e.getCause() == null ? e : e.getCause());
        }
        this.connected = code == ConnectReturnCode.CONNECT_ACCEPTED;
        return code;
    }

    private void scheduleHeartbeat() {
        if (serverKp <= 0) {
            LOG.debug("heartbeat disabled: serverKp={}", serverKp);
            return; // server_kp==0: no PING, no keepalive-based liveness
        }
        long checkMillis = Math.max(500, serverKp * 1000L / 4);
        LOG.info("scheduling fixed heartbeat check every {}ms (serverKp={}s)", checkMillis, serverKp);
        scheduler.scheduleAtFixedRate(this::heartbeatCheck, checkMillis, checkMillis, TimeUnit.MILLISECONDS);
    }

    /** CONNECT.Keepalive proposal: the max of the adaptive range in adaptive mode. */
    private int connectKeepaliveProposal() {
        return adaptiveHeartbeat ? adaptiveMaxSeconds : keepAliveSeconds;
    }

    /** Start the adaptive heartbeat state machine driven by the shared scheduler. */
    private void startAdaptiveHeartbeat() {
        stopAdaptiveHeartbeat();
        if (serverKp <= 0) {
            return; // server_kp==0: keepalive disabled by the server
        }
        final AdaptiveHeartbeat adaptive = new AdaptiveHeartbeat(
                adaptiveShortSeconds, adaptiveMaxSeconds, serverKp,
                probeCount, responseWindowMillis, fineStepSeconds, new AdaptiveHeartbeat.Transport() {
                    @Override
                    public void sendPing() {
                        AioClientSession s = session;
                        if (s != null) {
                            s.sendPing();
                        }
                    }

                    @Override
                    public long lastSentMs() {
                        AioClientSession s = session;
                        return s == null ? 0 : s.lastSentMs();
                    }

                    @Override
                    public long lastReceivedMs() {
                        AioClientSession s = session;
                        return s == null ? 0 : s.lastReceivedMs();
                    }

                    @Override
/**
     * Whether the CONNECT/CONNACK handshake completed with ACCEPTED.
     *
     * @return true when connected and the channel is open
     */
    public boolean isConnected() {
                        return RmttClient.this.isConnected();
                    }

                    @Override
                    public long nowMs() {
                        return System.currentTimeMillis();
                    }
                });
        adaptiveTick = scheduler.scheduleAtFixedRate(
                () -> {
                    if (!closed.get() && connected && adaptive.tick()) {
                        onConnectionLost("adaptive heartbeat failed");
                    }
                }, 250, 250, TimeUnit.MILLISECONDS);
    }

    private void stopAdaptiveHeartbeat() {
        if (adaptiveTick != null) {
            adaptiveTick.cancel(false);
            adaptiveTick = null;
        }
    }

    private void heartbeatCheck() {
        AioClientSession s = session;
        if (!connected || s == null || closed.get()) {
            return;
        }
        long now = System.currentTimeMillis();
        if (now - s.lastSentMs() >= serverKp * 1000L) {
            LOG.debug("PINGREQ sent (no packet for {}ms, serverKp={}s)", now - s.lastSentMs(), serverKp);
            s.sendPing();
        }
        if (now - s.lastReceivedMs() >= (long) (serverKp * 1500)) {
            // no PINGRESP within ~1.5×server_kp -> connection dead -> reconnect
            LOG.warn("keepalive timeout: no PINGRESP for {}ms (serverKp={}s)", now - s.lastReceivedMs(), serverKp);
            onConnectionLost("keepalive timeout");
        }
    }

    private void onConnectionLost(String reason) {
        if (closed.get() || manuallyDisconnecting) {
            return;
        }
        connected = false;
        stopAdaptiveHeartbeat();
        LOG.warn("connection lost ({})", reason);
        if (conn != null) {
            conn.close();
        }
        if (autoReconnect && !closed.get()) {
            scheduleReconnect();
        }
    }

    private void scheduleReconnect() {
        if (reconnectBackoff == null) {
            reconnectBackoff = new ReconnectBackoff(reconnectBaseMillis, maxReconnectIntervalMillis, reconnectJitter);
        }
        scheduler.execute(() -> {
            if (closed.get() || manuallyDisconnecting) {
                return;
            }
            long delay = reconnectBackoff.nextDelayMillis();
            scheduler.schedule(this::reconnectAttempt, delay, TimeUnit.MILLISECONDS);
        });
    }

    private void reconnectAttempt() {
        if (closed.get() || manuallyDisconnecting) {
            return;
        }
        try {
            connectOnce();
            if (reconnectBackoff != null) {
                reconnectBackoff.reset();
            }
        } catch (Exception e) {
            if (connectRetry && !closed.get()) {
                scheduleReconnect();
            }
        }
    }

    private javax.net.ssl.SSLEngine clientEngine() {
        javax.net.ssl.SSLEngine engine = sslContext.createSSLEngine(host, port);
        engine.setUseClientMode(true);
        return engine;
    }

    /**
     * Whether the CONNECT/CONNACK handshake completed with ACCEPTED.
     *
     * @return true when connected and the channel is open
     */
    public boolean isConnected() {
        return connected && channel != null && channel.isOpen();
    }

    /**
     * Push application data to the server (must be connected).
     *
     * @param payload the raw payload bytes
     * @return true when the frame was written
     */
    public boolean push(byte[] payload) {
        if (conn == null || !isConnected()) {
            return false;
        }
        RmttMessage push = RmttMessageFactory.newMessage(
                new FixedHeader(RmttMessageType.PUSH, false, false, false, false, 1 + payload.length),
                new PushVariableHeader((byte) 0), payload);
        conn.writeFrame(RmttWireCodec.encodeToBytes(push));
        return true;
    }

    /**
     * Push a UTF-8 payload to the server.
     *
     * @param payload the UTF-8 payload
     * @return true when the frame was written
     */
    public boolean push(String payload) {
        return push(payload.getBytes(StandardCharsets.UTF_8));
    }

    /** Graceful disconnect: send DISCONNECT then close. */
    public void disconnect() {
        manuallyDisconnecting = true;
        if (conn != null) {
            conn.writeFrame(RmttWireCodec.encodeToBytes(RmttMessageFactory.disconnect(DisconnectReturnCode.NORMAL_DISCONNECT)));
        }
        shutdown();
    }

    /**
     * Tear down the transport and release all resources. Safe to call multiple times.
     */
    public void shutdown() {
        closed.set(true);
        stopAdaptiveHeartbeat();
        if (conn != null) {
            conn.close();
        }
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
        connected = false;
    }
}
