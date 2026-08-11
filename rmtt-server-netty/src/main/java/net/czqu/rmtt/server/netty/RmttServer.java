package net.czqu.rmtt.server.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.ssl.SslContext;
import net.czqu.rmtt.api.Authenticator;
import net.czqu.rmtt.api.ConnectionListener;
import net.czqu.rmtt.api.ConnectionStore;
import net.czqu.rmtt.api.PushResult;
import net.czqu.rmtt.api.RmttMessageHandler;
import net.czqu.rmtt.codec.netty.RmttDecoder;
import net.czqu.rmtt.codec.netty.RmttEncoder;
import net.czqu.rmtt.codec.netty.ws.ByteBufToWsFrame;
import net.czqu.rmtt.codec.netty.ws.WsFrameToByteBuf;
import net.czqu.rmtt.protocol.DisconnectReturnCode;
import net.czqu.rmtt.protocol.PushMessage;
import net.czqu.rmtt.protocol.PushVariableHeader;
import net.czqu.rmtt.protocol.FixedHeader;
import net.czqu.rmtt.protocol.RmttMessageFactory;
import net.czqu.rmtt.protocol.RmttMessageType;
import net.czqu.rmtt.protocol.RmttWireCodec;
import net.czqu.rmtt.protocol.ServerKeepalivePolicy;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import net.czqu.rmtt.logging.InternalLogger;
import net.czqu.rmtt.logging.InternalLoggerFactory;

import static net.czqu.rmtt.api.PushResult.DEVICE_OFFLINE;
import static net.czqu.rmtt.api.PushResult.REJECTED;
import static net.czqu.rmtt.api.PushResult.SUCCESS;

/**
 * Netty-backed RMTT server: accepts long-lived device connections, authenticates them via the
 * injected {@link Authenticator}, registers them in the route table and provides a downstream
 * push API. TLS material (for wss) is always supplied by the caller.
 */
public class RmttServer {

    private static final InternalLogger LOG = InternalLoggerFactory.getLogger(RmttServer.class);


    private final ConnectionStore connectionStore;
    private final Authenticator authenticator;
    private final RmttMessageHandler messageHandler;
    private final ConnectionListener connectionListener;
    private final int port;
    private final int wsPort;
    private final int wssPort;
    private final String wsPath;
    private final SslContext wssSslContext;
    private final ServerKeepalivePolicy keepalivePolicy;
    private final KcpServer kcpServer;
    private final QuicServer quicServer;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private final List<Channel> boundChannels = new ArrayList<>();

    RmttServer(ConnectionStore connectionStore,
               Authenticator authenticator,
               RmttMessageHandler messageHandler,
               ConnectionListener connectionListener,
               int port,
               int wsPort,
               int wssPort,
               String wsPath,
               SslContext wssSslContext,
               ServerKeepalivePolicy keepalivePolicy,
               int kcpPort,
               int quicPort,
               io.netty.handler.codec.quic.QuicSslContext quicSslContext) {
        this.connectionStore = connectionStore;
        this.authenticator = authenticator;
        this.messageHandler = messageHandler;
        this.connectionListener = connectionListener;
        this.port = port;
        this.wsPort = wsPort;
        this.wssPort = wssPort;
        this.wsPath = wsPath == null || wsPath.isEmpty() ? "/rmtt" : wsPath;
        this.wssSslContext = wssSslContext;
        this.keepalivePolicy = keepalivePolicy;
        this.kcpServer = kcpPort > 0 ? new KcpServer(connectionStore, authenticator, messageHandler,
                connectionListener, keepalivePolicy, kcpPort) : null;
        this.quicServer = quicPort > 0 && quicSslContext != null
                ? new QuicServer(connectionStore, authenticator, messageHandler,
                connectionListener, keepalivePolicy, quicPort, quicSslContext) : null;
    }

    /**
     * Bind the listening channels (non-blocking). Call {@link #awaitStartup()} to wait for bind.
     *
     * @return the TCP bind future
     */
    public ChannelFuture start() {
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();

        ServerBootstrap base = new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 1024)
                .childOption(ChannelOption.TCP_NODELAY, true);

        ChannelFuture future = base.clone()
                .childHandler(tcpInitializer())
                .bind(port);
        future.addListener((ChannelFutureListener) f -> {
            if (f.isSuccess()) {
                boundChannels.add(f.channel());
                LOG.info("RMTT server (netty) listening on tcp://0.0.0.0:{}", port);
            } else {
                LOG.error("bind tcp://0.0.0.0:{} failed", port, f.cause());
            }
        });

        if (wsPort > 0) {
            bindWs(base.clone(), wsPort, null, false);
            LOG.info("WebSocket (ws://) listening on 0.0.0.0:{}", wsPort);
        }
        if (wssPort > 0) {
            bindWs(base.clone(), wssPort, wssSslContext, true);
            LOG.info("Secure WebSocket (wss://) listening on 0.0.0.0:{}", wssPort);
        }
        if (kcpServer != null) {
            kcpServer.start();
        }
        if (quicServer != null) {
            quicServer.start();
        }
        return future;
    }

    private ChannelInitializer<SocketChannel> tcpInitializer() {
        return new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) {
                ch.pipeline().addLast(new RmttDecoder());
                ch.pipeline().addLast(RmttEncoder.INSTANCE);
                ch.pipeline().addLast(new ServerRmttHandler(connectionStore, authenticator,
                        messageHandler, connectionListener, keepalivePolicy));
            }
        };
    }

    private void bindWs(ServerBootstrap bootstrap, int p,
                        SslContext sslContext, boolean secure) {
        bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) {
                if (secure && sslContext != null) {
                    ch.pipeline().addLast(sslContext.newHandler(ch.alloc()));
                }
                ch.pipeline().addLast(new HttpServerCodec());
                ch.pipeline().addLast(new HttpObjectAggregator(65536));
                ch.pipeline().addLast(new WebSocketServerProtocolHandler(wsPath, "rmtt", false, 65536));
                ch.pipeline().addLast(new WsFrameToByteBuf());
                ch.pipeline().addLast(new RmttDecoder());
                ch.pipeline().addLast(new ByteBufToWsFrame());
                ch.pipeline().addLast(RmttEncoder.INSTANCE);
                ch.pipeline().addLast(new ServerRmttHandler(connectionStore, authenticator,
                        messageHandler, connectionListener, keepalivePolicy));
            }
        });
        bootstrap.bind(p).addListener((ChannelFutureListener) f -> {
            if (f.isSuccess()) {
                boundChannels.add(f.channel());
            }
        });
    }

    /**
     * Block until the server has bound successfully.
     *
     * @return the completed TCP bind future
     * @throws InterruptedException when the wait is interrupted
     */
    public ChannelFuture awaitStartup() throws InterruptedException {
        return start().sync();
    }

    /**
     * Close all bound channels (TCP, WS, KCP, QUIC) and shut down the event loops.
     */
    public void closeAll() {
        for (Channel c : boundChannels) {
            c.close().awaitUninterruptibly();
        }
        if (kcpServer != null) {
            kcpServer.closeAll();
        }
        if (quicServer != null) {
            quicServer.closeAll();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
    }

    /**
     * Synchronous downstream push. The frame is encoded once into a byte[] then written.
     *
     * @param deviceId the target device id
     * @param payload  the raw payload bytes
     * @return the push outcome
     */
    public PushResult push(String deviceId, byte[] payload) {
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
     * Kick a device offline, sending it a DISCONNECT with the given reason.
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