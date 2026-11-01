package net.czqu.rmtt.server.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.AttributeKey;
import net.czqu.rmtt.api.AuthResult;
import net.czqu.rmtt.api.Authenticator;
import net.czqu.rmtt.api.ConnectionListener;
import net.czqu.rmtt.api.ConnectionLimiter;
import net.czqu.rmtt.api.ConnectionStore;
import net.czqu.rmtt.api.Credentials;
import net.czqu.rmtt.api.DeviceConnection;
import net.czqu.rmtt.api.RmttMessageHandler;
import net.czqu.rmtt.protocol.ConnAckVariableHeader;
import net.czqu.rmtt.protocol.ConnectMessage;
import net.czqu.rmtt.protocol.ConnectReturnCode;
import net.czqu.rmtt.protocol.DisconnectReturnCode;
import net.czqu.rmtt.protocol.FixedHeader;
import net.czqu.rmtt.protocol.RmttMessage;
import net.czqu.rmtt.protocol.RmttMessageFactory;
import net.czqu.rmtt.protocol.RmttMessageType;
import net.czqu.rmtt.protocol.RmttProtocol;
import net.czqu.rmtt.protocol.RmttWireCodec.MagicNumberViolation;
import net.czqu.rmtt.protocol.RmttWireCodec.BadProtocolVersionViolation;
import net.czqu.rmtt.protocol.RmttWireCodec.ProtocolViolation;
import net.czqu.rmtt.protocol.ServerKeepalivePolicy;

import java.util.Optional;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import net.czqu.rmtt.logging.InternalLogger;
import net.czqu.rmtt.logging.InternalLoggerFactory;

/**
 * Netty server handler: CONNECT handshake + auth, PUSH dispatch, heartbeat and lifecycle callbacks.
 * The {@link ConnectionStore} holds netty channels wrapped as {@link NettyDeviceConnection}.
 */
public class ServerRmttHandler extends SimpleChannelInboundHandler<RmttMessage> {

    private static final InternalLogger LOG = InternalLoggerFactory.getLogger(ServerRmttHandler.class);

    static final AttributeKey<String> DEVICE_ID_KEY = AttributeKey.valueOf("rmtt.deviceId");
    static final AttributeKey<NettyDeviceConnection> CONNECTION_KEY = AttributeKey.valueOf("rmtt.connection");
    static final AttributeKey<Long> LAST_READ_KEY = AttributeKey.valueOf("rmtt.lastRead");
    static final AttributeKey<Long> SERVER_KP_KEY = AttributeKey.valueOf("rmtt.serverKp");
    static final AttributeKey<ScheduledFuture<?>> REAPER_KEY = AttributeKey.valueOf("rmtt.reaper");
    static final AttributeKey<ScheduledFuture<?>> PRE_AUTH_KEY = AttributeKey.valueOf("rmtt.preAuth");

    /** A connection that has not completed CONNECT within this window is closed. */
    static final long UNAUTHENTICATED_TIMEOUT_MILLIS = 30_000;

    private final ConnectionStore connectionStore;
    private final Authenticator authenticator;
    private final RmttMessageHandler messageHandler;
    private final ConnectionListener connectionListener;
    private final ServerKeepalivePolicy keepalivePolicy;
    private final ConnectionLimiter limiter;
    private final int maxCredentialLength;
    private boolean limitAcquired;

    /**
     * Create the handler wiring the shared protocol services, with no connection limit.
     *
     * @param connectionStore     shared route table
     * @param authenticator       CONNECT authentication policy
     * @param messageHandler      upstream PUSH dispatch
     * @param connectionListener  lifecycle callbacks
     * @param keepalivePolicy     keepalive negotiation policy
     */
    public ServerRmttHandler(ConnectionStore connectionStore,
                             Authenticator authenticator,
                             RmttMessageHandler messageHandler,
                             ConnectionListener connectionListener,
                             ServerKeepalivePolicy keepalivePolicy) {
        this(connectionStore, authenticator, messageHandler, connectionListener, keepalivePolicy,
                new ConnectionLimiter(0));
    }

    /**
     * Create the handler wiring the shared protocol services and a shared admission limiter.
     *
     * @param connectionStore     shared route table
     * @param authenticator       CONNECT authentication policy
     * @param messageHandler      upstream PUSH dispatch
     * @param connectionListener  lifecycle callbacks
     * @param keepalivePolicy     keepalive negotiation policy
     * @param limiter             shared connection admission limiter
     */
    public ServerRmttHandler(ConnectionStore connectionStore,
                             Authenticator authenticator,
                             RmttMessageHandler messageHandler,
                             ConnectionListener connectionListener,
                             ServerKeepalivePolicy keepalivePolicy,
                             ConnectionLimiter limiter) {
        this(connectionStore, authenticator, messageHandler, connectionListener, keepalivePolicy, limiter,
                RmttProtocol.DEFAULT_MAX_CREDENTIAL_LENGTH);
    }

    /**
     * Create the handler with an explicit CONNECT credential length cap.
     *
     * @param connectionStore     shared route table
     * @param authenticator       CONNECT authentication policy
     * @param messageHandler      upstream PUSH dispatch
     * @param connectionListener  lifecycle callbacks
     * @param keepalivePolicy     keepalive negotiation policy
     * @param limiter             shared connection admission limiter
     * @param maxCredentialLength maximum accepted credential length in bytes
     */
    public ServerRmttHandler(ConnectionStore connectionStore,
                             Authenticator authenticator,
                             RmttMessageHandler messageHandler,
                             ConnectionListener connectionListener,
                             ServerKeepalivePolicy keepalivePolicy,
                             ConnectionLimiter limiter,
                             int maxCredentialLength) {
        this.connectionStore = connectionStore;
        this.authenticator = authenticator;
        this.messageHandler = messageHandler;
        this.connectionListener = connectionListener;
        this.keepalivePolicy = keepalivePolicy;
        this.limiter = limiter;
        this.maxCredentialLength = maxCredentialLength;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        if (!limiter.tryAcquire()) {
            LOG.warn("connection limit {} reached, rejecting {}", limiter.maxConnections(),
                    ctx.channel().remoteAddress());
            ctx.close();
            return;
        }
        limitAcquired = true;
        // pre-auth handshake deadline: unauthenticated connections must not accumulate
        ScheduledFuture<?> preAuth = ctx.executor().schedule(() -> {
            if (ctx.channel().attr(DEVICE_ID_KEY).get() == null && ctx.channel().isActive()) {
                LOG.warn("unauthenticated connection timed out, closing {}", ctx.channel().remoteAddress());
                ctx.close();
            }
        }, UNAUTHENTICATED_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        ctx.channel().attr(PRE_AUTH_KEY).set(preAuth);
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        if (limitAcquired) {
            limitAcquired = false;
            limiter.release();
        }
        ScheduledFuture<?> preAuth = ctx.channel().attr(PRE_AUTH_KEY).getAndSet(null);
        if (preAuth != null) {
            preAuth.cancel(false);
        }
        ScheduledFuture<?> reaper = ctx.channel().attr(REAPER_KEY).getAndSet(null);
        if (reaper != null) {
            reaper.cancel(false);
        }
        unregisterAndNotify(ctx, "connection closed");
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RmttMessage msg) {
        ctx.channel().attr(LAST_READ_KEY).set(System.nanoTime());
        switch (msg.fixedHeader().messageType()) {
            case CONNECT:
                handleConnect(ctx, msg);
                break;
            case PUSH:
                handlePush(ctx, msg);
                break;
            case PINGREQ:
                LOG.debug("PINGREQ from device={} -> PINGRESP", ctx.channel().attr(DEVICE_ID_KEY).get());
                ctx.writeAndFlush(RmttMessageFactory.PINGRESP, ctx.voidPromise());
                break;
            case DISCONNECT:
                ctx.close();
                break;
            default:
                break;
        }
    }

    private void handleConnect(ChannelHandlerContext ctx, RmttMessage msg) {
        if (ctx.channel().attr(DEVICE_ID_KEY).get() != null) {
            // one CONNECT per session; a repeat would re-register and self-kick the connection
            LOG.warn("duplicate CONNECT on established session, closing {}", ctx.channel().remoteAddress());
            ctx.writeAndFlush(RmttMessageFactory.disconnect(DisconnectReturnCode.PROTOCOL_VIOLATION))
                    .addListener(ChannelFutureListener.CLOSE);
            return;
        }
        byte[] rawCredential = msg instanceof ConnectMessage ? ((ConnectMessage) msg).payload() : null;
        if (rawCredential != null && rawCredential.length > maxCredentialLength) {
            LOG.warn("CONNECT rejected: credential too long ({} > max {})", rawCredential.length,
                    maxCredentialLength);
            ctx.writeAndFlush(connAck(ConnectReturnCode.CONNECT_UNAUTHORIZED, 0))
                    .addListener(ChannelFutureListener.CLOSE);
            return;
        }
        String credential = msg instanceof ConnectMessage
                ? ((ConnectMessage) msg).connectPayload().credential() : null;
        AuthResult authResult;
        try {
            authResult = authenticator.authenticate(credential);
        } catch (Exception e) {
            LOG.error("auth error for credential={}", Credentials.mask(credential), e);
            ctx.writeAndFlush(connAck(ConnectReturnCode.CONNECT_SERVER_UNAVAILABLE, 0));
            ctx.close();
            return;
        }
        if (authResult == null || !authResult.allowed()) {
            ConnectReturnCode code = authResult != null ? authResult.returnCode()
                    : ConnectReturnCode.CONNECT_UNAUTHORIZED;
            LOG.warn("CONNECT rejected credential={} code={}", Credentials.mask(credential), code);
            ctx.writeAndFlush(connAck(code, 0)).addListener(ChannelFutureListener.CLOSE);
            return;
        }

        long kp = keepalivePolicy.decide(
                msg instanceof ConnectMessage
                        ? ((ConnectMessage) msg).variableHeader().keepAliveTimeSeconds() : 0);
        String deviceId = authResult.deviceId();
        NettyDeviceConnection conn = new NettyDeviceConnection(ctx.channel());
        ctx.channel().attr(CONNECTION_KEY).set(conn);
        ctx.channel().attr(SERVER_KP_KEY).set(kp);
        // Send CONNACK BEFORE making the connection visible to the store, so a
        // downstream push cannot target the client before it has finished the
        // handshake (matches the rmtt-go server ordering).
        ctx.writeAndFlush(connAck(ConnectReturnCode.CONNECT_ACCEPTED, (int) kp));
        Optional<DeviceConnection> previous = connectionStore.register(deviceId, conn);
        previous.ifPresent(old -> {
            if (old.isActive()) {
                old.sendDisconnect(DisconnectReturnCode.SESSION_TAKEN_OVER);
            }
        });
        ctx.channel().attr(DEVICE_ID_KEY).set(deviceId);
        ScheduledFuture<?> preAuth = ctx.channel().attr(PRE_AUTH_KEY).getAndSet(null);
        if (preAuth != null) {
            preAuth.cancel(false); // handshake done; the deadline is no longer needed
        }
        scheduleIdleReaper(ctx, kp);
        connectionListener.onConnectionEstablished(deviceId);
        LOG.info("device {} connected (proposal={}s serverKp={}s)", deviceId,
                msg instanceof ConnectMessage
                        ? ((ConnectMessage) msg).variableHeader().keepAliveTimeSeconds() : 0, kp);
    }

    /** Reap connection if no message arrived within 1.5×server_kp. */
    private void scheduleIdleReaper(ChannelHandlerContext ctx, long serverKp) {
        if (serverKp <= 0) {
            return; // server_kp==0: keepalive-based liveness disabled
        }
        ScheduledFuture<?> reaper = ctx.executor().scheduleAtFixedRate(() -> {
            if (!ctx.channel().isActive()) {
                return;
            }
            Long lastRead = ctx.channel().attr(LAST_READ_KEY).get();
            long now = System.nanoTime();
            if (lastRead != null && now - lastRead > serverKp * 1_500_000_000L) {
                String deviceId = ctx.channel().attr(DEVICE_ID_KEY).get();
                LOG.warn("keepalive timeout reaping device={} (no packet for {}ms, serverKp={}s)",
                        deviceId, (now - lastRead) / 1_000_000L, serverKp);
                NettyDeviceConnection conn = ctx.channel().attr(CONNECTION_KEY).get();
                if (conn != null) {
                    conn.sendDisconnect(DisconnectReturnCode.KEEPALIVE_TIMEOUT);
                } else {
                    ctx.close();
                }
                unregisterAndNotify(ctx, "keepalive timeout");
            }
        }, serverKp * 1500, serverKp * 1500, TimeUnit.MILLISECONDS);
        ctx.channel().attr(REAPER_KEY).set(reaper);
    }

    private void handlePush(ChannelHandlerContext ctx, RmttMessage msg) {
        String deviceId = ctx.channel().attr(DEVICE_ID_KEY).get();
        if (deviceId == null || messageHandler == null) {
            return;
        }
        messageHandler.onMessage(deviceId, msg.payload());
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        ctx.fireUserEventTriggered(evt);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        Throwable root = cause;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        if (root instanceof MagicNumberViolation) {
            // magic wrong -> close without any RMTT message
            LOG.warn("bad magic from {}", ctx.channel().remoteAddress());
            unregisterAndNotify(ctx, "bad magic");
            ctx.close();
            return;
        }
        if (root instanceof BadProtocolVersionViolation) {
            // bad version -> CONNACK(0x01) then close
            LOG.warn("bad protocol version from {}", ctx.channel().remoteAddress());
            ctx.writeAndFlush(connAck(ConnectReturnCode.CONNECT_BAD_PROTOCOL_VERSION, 0))
                    .addListener(ChannelFutureListener.CLOSE);
            unregisterAndNotify(ctx, "bad protocol version");
            return;
        }
        if (root instanceof ProtocolViolation) {
            // other violations -> DISCONNECT(0x04) then close
            LOG.warn("protocol violation from {}: {}", ctx.channel().remoteAddress(), root.getMessage());
            ctx.writeAndFlush(RmttMessageFactory.disconnect(DisconnectReturnCode.PROTOCOL_VIOLATION))
                    .addListener(ChannelFutureListener.CLOSE);
            unregisterAndNotify(ctx, "protocol violation");
            return;
        }
        LOG.error("exception for {}: {}", ctx.channel().remoteAddress(), cause.toString());
        unregisterAndNotify(ctx, cause.getMessage());
        ctx.close();
    }

    private void unregisterAndNotify(ChannelHandlerContext ctx, String reason) {
        String deviceId = ctx.channel().attr(DEVICE_ID_KEY).getAndSet(null);
        NettyDeviceConnection conn = ctx.channel().attr(CONNECTION_KEY).getAndSet(null);
        if (deviceId != null && conn != null) {
            connectionStore.remove(deviceId, conn);
            connectionListener.onConnectionClosed(deviceId, reason);
            LOG.info("device {} disconnected ({})", deviceId, reason);
        }
    }

    private static net.czqu.rmtt.protocol.RmttMessage connAck(ConnectReturnCode code, int serverKeepalive) {
        return RmttMessageFactory.newMessage(
                new FixedHeader(RmttMessageType.CONNACK, false, false, false, false, 3),
                new ConnAckVariableHeader(code, serverKeepalive), null);
    }
}