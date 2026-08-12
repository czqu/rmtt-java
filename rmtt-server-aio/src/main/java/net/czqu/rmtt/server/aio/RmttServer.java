package net.czqu.rmtt.server.aio;

import net.czqu.rmtt.api.Authenticator;
import net.czqu.rmtt.api.ConnectionListener;
import net.czqu.rmtt.api.ConnectionStore;
import net.czqu.rmtt.api.PushResult;
import net.czqu.rmtt.api.RmttMessageHandler;
import net.czqu.rmtt.protocol.DisconnectReturnCode;
import net.czqu.rmtt.protocol.FixedHeader;
import net.czqu.rmtt.protocol.PushMessage;
import net.czqu.rmtt.protocol.PushVariableHeader;
import net.czqu.rmtt.protocol.RmttMessageType;
import net.czqu.rmtt.protocol.RmttWireCodec;
import net.czqu.rmtt.protocol.ServerKeepalivePolicy;
import net.czqu.rmtt.logging.InternalLogger;
import net.czqu.rmtt.logging.InternalLoggerFactory;
import net.czqu.rmtt.transport.aio.AioConnection;
import net.czqu.rmtt.transport.aio.AioFrameHandler;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static net.czqu.rmtt.api.PushResult.DEVICE_OFFLINE;
import static net.czqu.rmtt.api.PushResult.REJECTED;
import static net.czqu.rmtt.api.PushResult.SUCCESS;

/** AIO-backed RMTT server: raw TCP + optional TLS/ws/wss. TLS material is always caller-supplied. */
public final class RmttServer {

    private static final InternalLogger LOG = InternalLoggerFactory.getLogger(RmttServer.class);

    private final ConnectionStore connectionStore;
    private final Authenticator authenticator;
    private final RmttMessageHandler messageHandler;
    private final ConnectionListener connectionListener;
    private final int port;
    private final int tlsPort;
    private final int wsPort;
    private final int wssPort;
    private final SSLContext sslContext;
    private final ServerKeepalivePolicy keepalivePolicy;
    private final ConcurrentHashMap<AioServerSession, Long> sessions = new ConcurrentHashMap<>();

    private AsynchronousChannelGroup group;
    private final ExecutorService acceptExecutor = Executors.newFixedThreadPool(
            Math.max(4, Runtime.getRuntime().availableProcessors()));
    private ScheduledExecutorService scheduler;
    private volatile boolean started;

    RmttServer(ConnectionStore connectionStore,
               Authenticator authenticator,
               RmttMessageHandler messageHandler,
               ConnectionListener connectionListener,
               int port,
               int tlsPort,
               int wsPort,
               int wssPort,
               SSLContext sslContext,
               ServerKeepalivePolicy keepalivePolicy) {
        this.connectionStore = connectionStore;
        this.authenticator = authenticator;
        this.messageHandler = messageHandler;
        this.connectionListener = connectionListener;
        this.port = port;
        this.tlsPort = tlsPort;
        this.wsPort = wsPort;
        this.wssPort = wssPort;
        this.sslContext = sslContext;
        this.keepalivePolicy = keepalivePolicy;
    }

    /**
     * Bind all configured listeners and start the accept loops (non-blocking).
     *
     * @throws IOException when a listener cannot be bound
     */
    public void start() throws IOException {
        if (started) {
            return;
        }
        started = true;
        group = AsynchronousChannelGroup.withThreadPool(acceptExecutor);
        bind(port, false, false);
        bind(tlsPort, true, false);
        bind(wsPort, false, true);
        bind(wssPort, true, true);
        LOG.info("RMTT server (aio) listening on tcp://0.0.0.0:{} tls://0.0.0.0:{} ws://0.0.0.0:{} wss://0.0.0.0:{}",
                port, tlsPort, wsPort, wssPort);
        scheduler = Executors.newSingleThreadScheduledExecutor();
        long checkMillis = Math.max(1000, keepalivePolicy.defaultSeconds() * 1000 / 2);
        scheduler.scheduleWithFixedDelay(this::checkHeartbeats, checkMillis, checkMillis, TimeUnit.MILLISECONDS);
    }

    private void bind(int p, boolean tls, boolean ws) throws IOException {
        if (p <= 0) {
            return;
        }
        final boolean useTls = tls;
        final boolean useWs = ws;
        AsynchronousServerSocketChannel server = AsynchronousServerSocketChannel.open(group)
                .bind(new InetSocketAddress(p), 1024);
        server.accept(null, new CompletionHandler<AsynchronousSocketChannel, Void>() {
            @Override
            public void completed(AsynchronousSocketChannel ch, Void attachment) {
                server.accept(null, this);
                setupConnection(ch, useTls, useWs);
            }

            @Override
            public void failed(Throwable exc, Void attachment) {
                // if the server socket is still open, keep accepting
                if (server.isOpen()) {
                    server.accept(null, this);
                }
            }
        });
    }

    private void setupConnection(AsynchronousSocketChannel ch, boolean tls, boolean ws) {
        AioServerSession session = new AioServerSession(
                connectionStore, authenticator, messageHandler, connectionListener, keepalivePolicy);
        session.onSessionClosed(() -> sessions.remove(session));
        AioConnection conn = new AioConnection(ch, false, ws,
                tls ? () -> serverEngine() : null, session);
        session.bind(conn);
        sessions.put(session, System.currentTimeMillis());
        conn.start();
    }

    private javax.net.ssl.SSLEngine serverEngine() {
        javax.net.ssl.SSLEngine engine = sslContext.createSSLEngine();
        engine.setUseClientMode(false);
        return engine;
    }

    private void checkHeartbeats() {
        long now = System.currentTimeMillis();
        for (AioServerSession session : sessions.keySet()) {
            long kp = session.serverKp();
            if (kp <= 0) {
                continue; // server_kp==0: keepalive-based liveness is disabled
            }
            if (now - session.lastReadTime() > kp * 1500) {
                session.kick(DisconnectReturnCode.KEEPALIVE_TIMEOUT);
                sessions.remove(session);
            }
        }
    }

    /** Close all listeners and connections. */
    public void closeAll() {
        for (AioServerSession session : sessions.keySet()) {
            session.closed();
        }
        sessions.clear();
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
        if (group != null) {
            group.shutdown();
        }
        acceptExecutor.shutdown();
    }

    /**
     * Synchronous downstream push.
     *
     * @param deviceId the target device id
     * @param payload  the raw payload bytes
     * @return the push outcome
     */
    public PushResult push(String deviceId, byte[] payload) {
        if (payload == null) {
            LOG.warn("push rejected: null payload for device={}", deviceId);
            return REJECTED;
        }
        return connectionStore.get(deviceId)
                .map(conn -> {
                    if (!conn.isActive()) {
                        return DEVICE_OFFLINE;
                    }
                    byte[] frame = RmttWireCodec.encodeToBytes(new PushMessage(
                            new FixedHeader(RmttMessageType.PUSH, false, false, false, false, 1 + payload.length),
                            new PushVariableHeader((byte) 0), payload));
                    return conn.write(frame) ? SUCCESS : REJECTED;
                })
                .orElse(DEVICE_OFFLINE);
    }

    /**
     * Synchronous downstream push of a UTF-8 payload.
     *
     * @param deviceId the target device id
     * @param payload  the UTF-8 payload
     * @return the push outcome
     */
    public PushResult push(String deviceId, String payload) {
        return push(deviceId, payload.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Whether a device is currently connected.
     *
     * @param deviceId the device id
     * @return true when the device is online
     */
    public boolean isOnline(String deviceId) {
        return connectionStore.isOnline(deviceId);
    }

    /**
     * Number of currently connected devices.
     *
     * @return the connection count
     */
    public int onlineCount() {
        return connectionStore.size();
    }

    /**
     * Disconnect a device with the given return code.
     *
     * @param deviceId the target device id
     * @param reason   the DISCONNECT return code to send
     */
    public void kick(String deviceId, DisconnectReturnCode reason) {
        connectionStore.get(deviceId).ifPresent(conn -> {
            conn.sendDisconnect(reason);
            connectionStore.remove(deviceId, conn);
        });
    }
}