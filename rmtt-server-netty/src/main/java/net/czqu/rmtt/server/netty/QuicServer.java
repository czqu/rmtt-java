package net.czqu.rmtt.server.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.quic.InsecureQuicTokenHandler;
import io.netty.handler.codec.quic.QuicServerCodecBuilder;
import io.netty.handler.codec.quic.QuicSslContext;
import io.netty.handler.codec.quic.QuicStreamChannel;
import net.czqu.rmtt.api.Authenticator;
import net.czqu.rmtt.api.ConnectionListener;
import net.czqu.rmtt.api.ConnectionStore;
import net.czqu.rmtt.api.RmttMessageHandler;
import net.czqu.rmtt.codec.netty.RmttDecoder;
import net.czqu.rmtt.codec.netty.RmttEncoder;
import net.czqu.rmtt.logging.InternalLogger;
import net.czqu.rmtt.logging.InternalLoggerFactory;
import net.czqu.rmtt.protocol.ServerKeepalivePolicy;

import java.util.concurrent.TimeUnit;

/**
 * QUIC transport for the RMTT server, backed by netty 4.2 {@code netty-codec-quic} (quiche).
 *
 * <p>One QUIC connection carries RMTT bytes on a single bidirectional stream; each stream is
 * treated as one device connection exactly like a TCP socket, reusing the existing
 * {@link RmttDecoder}/{@link RmttEncoder}/{@link ServerRmttHandler} pipeline. The transport idle
 * timeout is set to 15 minutes (mirroring the Go / C++ QUIC transports) so the RMTT-layer
 * heartbeat fully owns liveness; quiche has no transport-level keepalive API.</p>
 */
public final class QuicServer {

    private static final InternalLogger LOG = InternalLoggerFactory.getLogger(QuicServer.class);

    private final ConnectionStore connectionStore;
    private final Authenticator authenticator;
    private final RmttMessageHandler messageHandler;
    private final ConnectionListener connectionListener;
    private final ServerKeepalivePolicy keepalivePolicy;
    private final int port;
    private final QuicSslContext sslContext;

    private EventLoopGroup group;
    private Channel channel;

    QuicServer(ConnectionStore connectionStore,
               Authenticator authenticator,
               RmttMessageHandler messageHandler,
               ConnectionListener connectionListener,
               ServerKeepalivePolicy keepalivePolicy,
               int port,
               QuicSslContext sslContext) {
        this.connectionStore = connectionStore;
        this.authenticator = authenticator;
        this.messageHandler = messageHandler;
        this.connectionListener = connectionListener;
        this.keepalivePolicy = keepalivePolicy;
        this.port = port;
        this.sslContext = sslContext;
    }

    /** Bind the QUIC (UDP) listener. Call after the boss/worker groups are up. */
    public void start() {
        group = new NioEventLoopGroup(1);
        io.netty.channel.ChannelHandler codec = new QuicServerCodecBuilder()
                .sslContext(sslContext)
                // 15min transport idle: liveness is owned by the RMTT heartbeat (mirror Go/C++).
                .maxIdleTimeout(15, TimeUnit.MINUTES)
                .initialMaxData(1_000_000)
                .initialMaxStreamDataBidirectionalLocal(256_000)
                .initialMaxStreamDataBidirectionalRemote(256_000)
                .initialMaxStreamsBidirectional(1024)
                .initialMaxStreamsUnidirectional(16)
                .tokenHandler(InsecureQuicTokenHandler.INSTANCE)
                .streamHandler(new ChannelInitializer<QuicStreamChannel>() {
                    @Override
                    protected void initChannel(QuicStreamChannel ch) {
                        ch.pipeline().addLast(new RmttDecoder());
                        ch.pipeline().addLast(RmttEncoder.INSTANCE);
                        ch.pipeline().addLast(new ServerRmttHandler(connectionStore, authenticator,
                                messageHandler, connectionListener, keepalivePolicy));
                    }
                })
                .build();
        Bootstrap bootstrap = new Bootstrap()
                .group(group)
                .channel(NioDatagramChannel.class)
                .option(ChannelOption.SO_REUSEADDR, true)
                .handler(new ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(Channel ch) {
                        ch.pipeline().addLast("dbg", new io.netty.channel.ChannelInboundHandlerAdapter() {
                            @Override
                            public void channelRead(io.netty.channel.ChannelHandlerContext ctx, Object msg) {
                                int sz = msg instanceof io.netty.channel.socket.DatagramPacket
                                        ? ((io.netty.channel.socket.DatagramPacket) msg).content().readableBytes()
                                        : (msg instanceof io.netty.buffer.ByteBuf
                                            ? ((io.netty.buffer.ByteBuf) msg).readableBytes() : -1);
                                LOG.warn("QUIC srv datagram read: {} bytes", sz);
                                ctx.fireChannelRead(msg);
                            }

                            @Override
                            public void exceptionCaught(io.netty.channel.ChannelHandlerContext ctx, Throwable cause) {
                                LOG.error("QUIC srv datagram exception", cause);
                            }
                        });
                        ch.pipeline().addLast(codec);
                    }
                });
        channel = bootstrap.bind(port).syncUninterruptibly().channel();
        LOG.info("RMTT server (netty) QUIC listening on quic://0.0.0.0:{}", port);
    }

    /**
     * Whether the QUIC listener is bound and active.
     *
     * @return true when the datagram channel is open
     */
    public boolean isStarted() {
        return channel != null && channel.isActive();
    }

    /**
     * Stop the listener and shut down the event loop.
     */
    public void closeAll() {
        if (channel != null) {
            channel.close().awaitUninterruptibly();
        }
        if (group != null) {
            group.shutdownGracefully();
        }
    }
}
