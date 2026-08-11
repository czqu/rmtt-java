package net.czqu.rmtt.client.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketClientProtocolHandler;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicClientCodecBuilder;
import io.netty.handler.codec.quic.QuicSslContext;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamType;
import io.netty.handler.ssl.SslContext;
import net.czqu.rmtt.codec.netty.RmttDecoder;
import net.czqu.rmtt.codec.netty.RmttEncoder;
import net.czqu.rmtt.codec.netty.ws.ByteBufToWsFrame;
import net.czqu.rmtt.codec.netty.ws.WsFrameToByteBuf;
import net.czqu.rmtt.protocol.ConnectReturnCode;
import net.czqu.rmtt.protocol.DisconnectReturnCode;
import net.czqu.rmtt.protocol.FixedHeader;
import net.czqu.rmtt.protocol.PushMessage;
import net.czqu.rmtt.protocol.PushVariableHeader;
import net.czqu.rmtt.protocol.ReconnectBackoff;
import net.czqu.rmtt.protocol.RmttMessage;
import net.czqu.rmtt.protocol.RmttMessageFactory;
import net.czqu.rmtt.protocol.RmttMessageType;
import net.czqu.rmtt.protocol.AdaptiveHeartbeat;

import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import net.czqu.rmtt.logging.InternalLogger;
import net.czqu.rmtt.logging.InternalLoggerFactory;

/**
 * Netty-backed RMTT client: connects, performs the CONNECT/CONNACK handshake, pushes messages
 * upstream and receives downstream PUSH via {@link RmttPushHandler}.
 */
public class RmttClient {

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
    private final long writeTimeoutMillis;
    private final RmttPushHandler pushHandler;
    private final boolean webSocket;
    private final boolean secure;
    private final String wsPath;
    private final SslContext sslContext;
    private final boolean autoReconnect;
    private final boolean connectRetry;
    private final long reconnectBaseMillis;
    private final long maxReconnectIntervalMillis;
    private final float reconnectJitter;
    private final boolean kcp;
    private final boolean quic;
    private final QuicSslContext quicSslContext;

    private EventLoopGroup group;
    private Channel channel;
    private QuicChannel quicChannel;
    private QuicStreamChannel quicStreamChannel;
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile ClientSession session;
    private volatile boolean connected;
    private volatile int serverKp;
    private volatile boolean manuallyDisconnecting;
    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> adaptiveTick;
    private kcp.KcpClient kcpClient;
    private Channel kcpDummy;
    private final AtomicBoolean reconnectScheduled = new AtomicBoolean();

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
               long writeTimeoutMillis,
               RmttPushHandler pushHandler,
               boolean webSocket,
               boolean secure,
               String wsPath,
               SslContext sslContext,
               boolean autoReconnect,
               boolean connectRetry,
               long reconnectBaseMillis,
               long maxReconnectIntervalMillis,
               float reconnectJitter,
               boolean kcp,
               boolean quic,
               QuicSslContext quicSslContext) {
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
        this.writeTimeoutMillis = writeTimeoutMillis;
        this.pushHandler = pushHandler;
        this.webSocket = webSocket;
        this.secure = secure;
        this.wsPath = wsPath == null || wsPath.isEmpty() ? "/rmtt" : wsPath;
        this.sslContext = sslContext;
        this.autoReconnect = autoReconnect;
        this.connectRetry = connectRetry;
        this.reconnectBaseMillis = reconnectBaseMillis;
        this.maxReconnectIntervalMillis = maxReconnectIntervalMillis;
        this.reconnectJitter = reconnectJitter;
        this.kcp = kcp;
        this.quic = quic;
        this.quicSslContext = quicSslContext;
    }

    /**
     * Connect and wait for the CONNACK handshake.
     *
     * @return the server's CONNACK return code
     * @throws InterruptedException when the wait is interrupted
     * @throws TimeoutException     when the handshake does not complete in time
     */
    public ConnectReturnCode connect() throws InterruptedException, TimeoutException {
        if (closed.get()) {
            throw new RuntimeException("client is closed");
        }
        scheduler = Executors.newSingleThreadScheduledExecutor();
        LOG.info("connecting {}://{}:{} credential={} keepalive={}s",
                transportName(), host, port, credential, connectKeepaliveProposal());
        return connectOnce();
    }

    private String transportName() {
        if (kcp) {
            return "kcp";
        }
        if (quic) {
            return "quic";
        }
        return webSocket ? (secure ? "wss" : "ws") : "tcp";
    }

    private ConnectReturnCode connectOnce() throws InterruptedException, TimeoutException {
        if (kcp) {
            return connectKcp();
        }
        if (quic) {
            return connectQuic();
        }
        group = new NioEventLoopGroup();
        Bootstrap bootstrap = new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) connectTimeoutMillis)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        if (webSocket) {
                            initWebSocketPipeline(ch);
                        } else {
                            initTcpPipeline(ch);
                        }
                    }
                });

        ChannelFuture ready = bootstrap.connect(host, port).awaitUninterruptibly();
        if (!ready.isSuccess()) {
            group.shutdownGracefully();
            Throwable cause = ready.cause() == null ? new RuntimeException("connect failure") : ready.cause();
            LOG.error("connect to {}:{} failed", host, port, cause);
            throw new RuntimeException(cause);
        }
        this.channel = ready.channel();
        channel.closeFuture().addListener(f -> onConnectionLost("channel closed"));
        ConnectReturnCode code;
        try {
            code = handshakeFuture.get(connectTimeoutMillis, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause() == null ? e : e.getCause());
        }
        this.connected = code == ConnectReturnCode.CONNECT_ACCEPTED;
        return code;
    }

    private CompletableFuture<ConnectReturnCode> handshakeFuture = new CompletableFuture<>();

    private ConnectReturnCode connectKcp() throws InterruptedException, TimeoutException {
        if (kcpClient != null) {
            try {
                kcpClient.stop();
            } catch (Exception ignored) {
                // best-effort stop of the previous attempt
            }
        }
        kcp.ChannelConfig config = new kcp.ChannelConfig();
        config.setConv(ThreadLocalRandom.current().nextInt());
        config.setNettyBootstrapGroup(new NioEventLoopGroup(), NioDatagramChannel.class);
        kcpClient = new kcp.KcpClient(config);
        KcpClientSession s = new KcpClientSession(credential, keepAliveSeconds,
                pushHandler, handshakeFuture, events());
        this.session = s;
        kcpClient.connect(new InetSocketAddress(host, port), config, s);
        if (kcpDummy == null) {
            kcpDummy = new EmbeddedChannel();
        }
        ConnectReturnCode code;
        try {
            code = handshakeFuture.get(connectTimeoutMillis, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause() == null ? e : e.getCause());
        }
        this.connected = code == ConnectReturnCode.CONNECT_ACCEPTED;
        return code;
    }

    private ConnectReturnCode connectQuic() throws InterruptedException, TimeoutException {
        group = new NioEventLoopGroup();
        Bootstrap bootstrap = new Bootstrap()
                .group(group)
                .channel(NioDatagramChannel.class)
                .option(ChannelOption.SO_REUSEADDR, true)
                .handler(new ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(Channel ch) {
                        ch.pipeline().addLast(qos(new QuicClientCodecBuilder())
                                .sslContext(quicSslContext).build());
                    }
                });
        ChannelFuture ready = bootstrap.connect(new InetSocketAddress(host, port)).awaitUninterruptibly();
        if (!ready.isSuccess()) {
            group.shutdownGracefully();
            Throwable cause = ready.cause() == null ? new RuntimeException("connect failure") : ready.cause();
            LOG.error("quic connect to {}:{} failed", host, port, cause);
            throw new RuntimeException(cause);
        }
        this.channel = ready.channel();
        try {
            io.netty.util.concurrent.Future<QuicChannel> qf =
                    QuicChannel.newBootstrap(ready.channel())
                    .streamHandler(new ChannelInitializer<QuicStreamChannel>() {
                        @Override
                        protected void initChannel(QuicStreamChannel ch) {
                            ch.pipeline().addLast(new RmttDecoder());
                            ch.pipeline().addLast(RmttEncoder.INSTANCE);
                            ClientRmttHandler h = new ClientRmttHandler(credential, connectKeepaliveProposal(),
                                    pushHandler, handshakeFuture, false, events());
                            session = h;
                            ch.pipeline().addLast(h);
                        }
                    })
                    .connect();
            qf.awaitUninterruptibly();
            if (!qf.isSuccess()) {
                Throwable c = qf.cause();
                LOG.error("quic handshake to {}:{} failed", host, port, c);
                if (c instanceof RuntimeException) {
                    throw (RuntimeException) c;
                }
                throw new RuntimeException("quic handshake failed", c);
            }
            quicChannel = qf.getNow();
            if (quicChannel == null) {
                throw new RuntimeException("quic channel is null");
            }
            quicStreamChannel = quicChannel.createStream(QuicStreamType.BIDIRECTIONAL,
                    new ChannelInitializer<QuicStreamChannel>() {
                        @Override
                        protected void initChannel(QuicStreamChannel ch) {
                            ch.pipeline().addLast(new RmttDecoder());
                            ch.pipeline().addLast(RmttEncoder.INSTANCE);
                            ClientRmttHandler h = new ClientRmttHandler(credential, connectKeepaliveProposal(),
                                    pushHandler, handshakeFuture, false, events());
                            session = h;
                            ch.pipeline().addLast(h);
                        }
                    }).awaitUninterruptibly().getNow();
            if (quicStreamChannel == null) {
                throw new RuntimeException("quic stream channel is null");
            }
        } catch (RuntimeException e) {
            if (quicChannel != null) {
                quicChannel.close().awaitUninterruptibly();
            }
            if (ready.channel() != null) {
                ready.channel().close();
            }
            group.shutdownGracefully();
            LOG.error("quic open to {}:{} failed", host, port, e);
            throw e;
        }
        quicStreamChannel.closeFuture().addListener(f -> onConnectionLost("quic stream closed"));
        ConnectReturnCode code;
        try {
            code = handshakeFuture.get(connectTimeoutMillis, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause() == null ? e : e.getCause());
        }
        this.connected = code == ConnectReturnCode.CONNECT_ACCEPTED;
        return code;
    }

    private <B extends io.netty.handler.codec.quic.QuicCodecBuilder<B>> B qos(B builder) {
        return builder.maxIdleTimeout(15, TimeUnit.MINUTES)
                .maxRecvUdpPayloadSize(2048)
                .maxSendUdpPayloadSize(2048)
                .initialMaxData(10_000_000)
                .initialMaxStreamDataBidirectionalLocal(1_000_000)
                .initialMaxStreamDataBidirectionalRemote(1_000_000)
                .initialMaxStreamDataUnidirectional(1_000_000)
                .initialMaxStreamsBidirectional(100)
                .initialMaxStreamsUnidirectional(100);
    }

    private void initTcpPipeline(SocketChannel ch) {
        ch.pipeline().addLast(new RmttDecoder());
        ch.pipeline().addLast(RmttEncoder.INSTANCE);
        ClientRmttHandler h = new ClientRmttHandler(credential, connectKeepaliveProposal(),
                pushHandler, handshakeFuture, webSocket, events());
        session = h;
        ch.pipeline().addLast(h);
    }

    private void initWebSocketPipeline(SocketChannel ch) {
        if (secure && sslContext != null) {
            ch.pipeline().addLast(sslContext.newHandler(ch.alloc(), host, port));
        }
        String scheme = secure ? "wss" : "ws";
        WebSocketClientHandshaker handshaker = WebSocketClientHandshakerFactory.newHandshaker(
                URI.create(scheme + "://" + host + ":" + port + wsPath),
                WebSocketVersion.V13, "rmtt", false, HttpHeaders.EMPTY_HEADERS, 65536);
        ch.pipeline().addLast(new HttpClientCodec());
        ch.pipeline().addLast(new HttpObjectAggregator(65536));
        ch.pipeline().addLast(new WebSocketClientProtocolHandler(handshaker));
        ch.pipeline().addLast(new WsFrameToByteBuf());
        ch.pipeline().addLast(new RmttDecoder());
        ch.pipeline().addLast(new ByteBufToWsFrame());
        ch.pipeline().addLast(RmttEncoder.INSTANCE);
        ClientRmttHandler h = new ClientRmttHandler(credential, connectKeepaliveProposal(),
                pushHandler, handshakeFuture, true, events());
        session = h;
        ch.pipeline().addLast(h);
    }

    /** CONNECT.Keepalive proposal: the max of the adaptive range in adaptive mode. */
    private int connectKeepaliveProposal() {
        return adaptiveHeartbeat ? adaptiveMaxSeconds : keepAliveSeconds;
    }

    private ClientRmttHandler.SessionEvents events() {
        return new ClientRmttHandler.SessionEvents() {
            @Override
            public void onConnAck(ConnectReturnCode code, int kp) {
                serverKp = kp;
                connected = code == ConnectReturnCode.CONNECT_ACCEPTED;
                LOG.info("CONNACK {} serverKp={}s credential={}", code, kp, credential);
                if (connected) {
                    if (adaptiveHeartbeat) {
                        LOG.info("adaptive heartbeat enabled short={}s max={}s probeCount={} responseWindow={}ms fineStep={}s ceiling={}s",
                                adaptiveShortSeconds, adaptiveMaxSeconds, probeCount,
                                responseWindowMillis, fineStepSeconds, kp);
                        startAdaptiveHeartbeat();
                    } else {
                        LOG.info("fixed heartbeat enabled keepalive={}s serverKp={}s", keepAliveSeconds, kp);
                        scheduleHeartbeat();
                    }
                }
            }

            @Override
            public void onDisconnect(DisconnectReturnCode code) {
                onConnectionLost("disconnect " + code);
            }

            @Override
            public void onClosed() {
                onConnectionLost("connection closed");
            }
        };
    }

    private void scheduleHeartbeat() {
        if (serverKp <= 0) {
            LOG.debug("keepalive disabled (serverKp=0)");
            return;
        }
        long checkMillis = Math.max(500, serverKp * 1000L / 4);
        LOG.debug("fixed keepalive scheduler start interval={}ms serverKp={}s", checkMillis, serverKp);
        scheduler.scheduleAtFixedRate(this::heartbeatCheck, checkMillis, checkMillis, TimeUnit.MILLISECONDS);
    }

    private void heartbeatCheck() {
        ClientSession s = session;
        if (!connected || s == null || closed.get()) {
            return;
        }
        long now = System.currentTimeMillis();
        if (now - s.lastSentMs() >= serverKp * 1000L) {
            LOG.debug("PINGREQ send (serverKp={}s, lastSent={}ms ago)", serverKp, now - s.lastSentMs());
            s.sendPing();
        }
        if (now - s.lastReceivedMs() >= (long) (serverKp * 1500)) {
            LOG.warn("keepalive timeout: no packet from server for {}ms (serverKp={}s)",
                    now - s.lastReceivedMs(), serverKp);
            onConnectionLost("keepalive timeout");
        }
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
                        ClientSession s = session;
                        if (s != null) {
                            s.sendPing();
                        }
                    }

                    @Override
                    public long lastSentMs() {
                        ClientSession s = session;
                        return s == null ? 0 : s.lastSentMs();
                    }

                    @Override
                    public long lastReceivedMs() {
                        ClientSession s = session;
                        return s == null ? 0 : s.lastReceivedMs();
                    }

                    @Override
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

    private void onConnectionLost(String reason) {
        if (closed.get() || manuallyDisconnecting) {
            return;
        }
        LOG.warn("connection lost: {} (credential={})", reason, credential);
        connected = false;
        stopAdaptiveHeartbeat();
        if (kcp) {
            if (kcpClient != null) {
                try {
                    kcpClient.stop();
                } catch (Exception ignored) {
                    // best-effort stop
                }
                kcpClient = null;
            }
        } else if (quic) {
            if (quicChannel != null) {
                quicChannel.close();
            }
            if (channel != null) {
                channel.close();
            }
            quicChannel = null;
            quicStreamChannel = null;
        } else if (channel != null) {
            channel.close();
        }
        if (autoReconnect && reconnectScheduled.compareAndSet(false, true)) {
            scheduleReconnect();
        }
    }

    private void scheduleReconnect() {
        ReconnectBackoff backoff = new ReconnectBackoff(reconnectBaseMillis, maxReconnectIntervalMillis, reconnectJitter);
        scheduler.execute(() -> {
            if (closed.get() || manuallyDisconnecting) {
                return;
            }
            long delay = backoff.nextDelayMillis();
            scheduler.schedule(this::reconnectAttempt, delay, TimeUnit.MILLISECONDS);
        });
    }

    private void reconnectAttempt() {
        if (closed.get() || manuallyDisconnecting) {
            return;
        }
        try {
            handshakeFuture = new CompletableFuture<>();
            connectOnce();
        } catch (Exception e) {
            if (connectRetry) {
                scheduleReconnect();
            }
        } finally {
            reconnectScheduled.set(false);
        }
    }

    /**
     * Whether the CONNECT/CONNACK handshake completed with ACCEPTED.
     *
     * @return true when connected and the transport is active
     */
    public boolean isConnected() {
        if (!connected) {
            return false;
        }
        if (kcp) {
            return session instanceof KcpClientSession && ((KcpClientSession) session).isSessionActive();
        }
        if (quic) {
            return quicChannel != null && quicChannel.isActive() && quicStreamChannel != null && quicStreamChannel.isActive();
        }
        return channel != null && channel.isActive();
    }

    /**
     * Push application data to the server (must be connected).
     *
     * @param payload the raw payload bytes
     * @return the write future
     */
    public ChannelFuture push(byte[] payload) {
        RmttMessage push = RmttMessageFactory.newMessage(
                new FixedHeader(RmttMessageType.PUSH, false, false, false, false, 1 + payload.length),
                new PushVariableHeader((byte) 0), payload);
        ClientSession s = session;
        if (kcp) {
            if (s != null) {
                s.push(push);
            }
            return kcpDummy == null ? new EmbeddedChannel().newSucceededFuture()
                    : kcpDummy.newSucceededFuture();
        }
        if (quic) {
            if (quicStreamChannel != null) {
                return quicStreamChannel.writeAndFlush(push);
            }
            return new EmbeddedChannel().newSucceededFuture();
        }
        return channel.writeAndFlush(push);
    }

    /**
     * Push a UTF-8 payload to the server.
     *
     * @param payload the UTF-8 payload
     * @return the write future
     */
    public ChannelFuture push(String payload) {
        return push(payload.getBytes(StandardCharsets.UTF_8));
    }

    /** Graceful disconnect: send DISCONNECT then close the transport. */
    public void disconnect() {
        LOG.debug("disconnect (credential={})", credential);
        manuallyDisconnecting = true;
        if (kcp) {
            ClientSession s = session;
            if (s != null) {
                s.push(RmttMessageFactory.disconnect(DisconnectReturnCode.NORMAL_DISCONNECT));
            }
        } else if (quic) {
            if (quicStreamChannel != null && quicStreamChannel.isActive()) {
                quicStreamChannel.writeAndFlush(
                        RmttMessageFactory.disconnect(DisconnectReturnCode.NORMAL_DISCONNECT))
                        .awaitUninterruptibly(writeTimeoutMillis);
            }
        } else if (channel != null && channel.isActive()) {
            channel.writeAndFlush(RmttMessageFactory.disconnect(DisconnectReturnCode.NORMAL_DISCONNECT))
                    .awaitUninterruptibly(writeTimeoutMillis);
        }
        shutdown();
    }

    /**
     * Tear down the transport and release all resources. Safe to call multiple times.
     */
    public void shutdown() {
        closed.set(true);
        stopAdaptiveHeartbeat();
        if (kcp) {
            if (kcpClient != null) {
                try {
                    kcpClient.stop();
                } catch (Exception ignored) {
                    // best-effort stop
                }
            }
        } else if (quic) {
            if (quicChannel != null) {
                quicChannel.close().awaitUninterruptibly();
            } else if (channel != null) {
                channel.close().awaitUninterruptibly();
            }
            quicChannel = null;
            quicStreamChannel = null;
        } else if (channel != null) {
            channel.close().awaitUninterruptibly();
        }
        if (group != null) {
            group.shutdownGracefully();
        }
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
        connected = false;
    }
}
