package net.czqu.rmtt.client.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.WebSocketClientProtocolHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import net.czqu.rmtt.protocol.ConnAckMessage;
import net.czqu.rmtt.protocol.ConnectPayload;
import net.czqu.rmtt.protocol.ConnectReturnCode;
import net.czqu.rmtt.protocol.ConnectVariableHeader;
import net.czqu.rmtt.protocol.DisconnectMessage;
import net.czqu.rmtt.protocol.DisconnectReturnCode;
import net.czqu.rmtt.protocol.FixedHeader;
import net.czqu.rmtt.protocol.PushMessage;
import net.czqu.rmtt.protocol.RmttMessage;
import net.czqu.rmtt.protocol.RmttMessageFactory;
import net.czqu.rmtt.protocol.RmttMessageType;

import java.util.concurrent.CompletableFuture;

import net.czqu.rmtt.logging.InternalLogger;
import net.czqu.rmtt.logging.InternalLoggerFactory;

/** Client-side netty handler: completes the CONNECT/CONNACK handshake and dispatches PUSH. */
class ClientRmttHandler extends SimpleChannelInboundHandler<RmttMessage> implements ClientSession {

    private static final InternalLogger LOG = InternalLoggerFactory.getLogger(ClientRmttHandler.class);

    private final String credential;
    private final int keepAliveSeconds;
    private final RmttPushHandler pushHandler;
    private final CompletableFuture<ConnectReturnCode> connectFuture;
    private final SessionEvents events;
    private final boolean webSocket;
    private volatile ChannelHandlerContext ctx;
    private volatile boolean connectSent;
    private volatile boolean connected;
    private volatile int serverKp;
    private volatile long lastReceivedMs;
    private volatile long lastSentMs;

    interface SessionEvents {
        void onConnAck(ConnectReturnCode code, int serverKp);
        void onDisconnect(DisconnectReturnCode code);

        default void onClosed() {
        }
    }

    ClientRmttHandler(String credential,
                      int keepAliveSeconds,
                      RmttPushHandler pushHandler,
                      CompletableFuture<ConnectReturnCode> connectFuture,
                      boolean webSocket,
                      SessionEvents events) {
        this.credential = credential;
        this.keepAliveSeconds = keepAliveSeconds;
        this.pushHandler = pushHandler;
        this.connectFuture = connectFuture;
        this.webSocket = webSocket;
        this.events = events;
    }

    @Override
    public long lastReceivedMs() {
        return lastReceivedMs;
    }

    @Override
    public long lastSentMs() {
        return lastSentMs;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        this.ctx = ctx;
        this.lastReceivedMs = System.currentTimeMillis();
        this.lastSentMs = System.currentTimeMillis();
        if (!webSocket) {
            sendConnect();
        }
    }

    private void sendConnect() {
        if (connectSent) {
            return;
        }
        connectSent = true;
        ConnectVariableHeader variableHeader = new ConnectVariableHeader(1, (byte) 0, keepAliveSeconds);
        RmttMessage connect = RmttMessageFactory.newMessage(
                new FixedHeader(RmttMessageType.CONNECT, false, false, false, false, 0),
                variableHeader,
                new ConnectPayload(credential).credential().getBytes(java.nio.charset.StandardCharsets.UTF_8));
        ctx.writeAndFlush(connect);
        lastSentMs = System.currentTimeMillis();
    }

    @Override
    public void sendPing() {
        ctx.writeAndFlush(RmttMessageFactory.PINGREQ);
        lastSentMs = System.currentTimeMillis();
    }

    @Override
    public void push(RmttMessage msg) {
        ctx.writeAndFlush(msg);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RmttMessage msg) {
        lastReceivedMs = System.currentTimeMillis();
        switch (msg.fixedHeader().messageType()) {
            case CONNACK:
                handleConnAck((ConnAckMessage) msg);
                break;
            case PUSH:
                handlePush((PushMessage) msg);
                break;
            case PINGRESP:
                LOG.debug("PINGRESP received (credential={})", credential);
                lastReceivedMs = System.currentTimeMillis();
                break;
            case DISCONNECT:
                DisconnectReturnCode code = DisconnectReturnCode.valueOf(
                        ((DisconnectMessage) msg).variableHeader().returnCode());
                if (events != null) {
                    events.onDisconnect(code);
                }
                ctx.close();
                break;
            default:
                break;
        }
    }

    private void handleConnAck(ConnAckMessage msg) {
        ConnectReturnCode code = msg.variableHeader().connectReturnCode();
        serverKp = msg.variableHeader().serverKeepaliveSeconds();
        connected = code == ConnectReturnCode.CONNECT_ACCEPTED;
        connectFuture.complete(code);
        if (events != null) {
            events.onConnAck(code, serverKp);
        }
        if (!connected) {
            ctx.close();
        }
    }

    private void handlePush(PushMessage msg) {
        if (pushHandler != null) {
            pushHandler.onPush(msg.payload());
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        if (evt instanceof WebSocketClientProtocolHandler.ClientHandshakeStateEvent) {
            WebSocketClientProtocolHandler.ClientHandshakeStateEvent event =
                    (WebSocketClientProtocolHandler.ClientHandshakeStateEvent) evt;
            if (event == WebSocketClientProtocolHandler.ClientHandshakeStateEvent.HANDSHAKE_COMPLETE) {
                sendConnect();
            }
        } else if (evt instanceof IdleStateEvent && ((IdleStateEvent) evt).state() == IdleState.WRITER_IDLE) {
            ctx.writeAndFlush(RmttMessageFactory.PINGREQ);
            lastSentMs = System.currentTimeMillis();
        } else {
            ctx.fireUserEventTriggered(evt);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (!connectFuture.isDone()) {
            connectFuture.completeExceptionally(cause);
        }
        ctx.close();
    }

    ChannelHandlerContext ctx() {
        return ctx;
    }
}
