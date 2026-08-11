package net.czqu.rmtt.client.aio;

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
import net.czqu.rmtt.protocol.RmttWireCodec;
import net.czqu.rmtt.logging.InternalLogger;
import net.czqu.rmtt.logging.InternalLoggerFactory;
import net.czqu.rmtt.transport.aio.AioConnection;
import net.czqu.rmtt.transport.aio.AioFrameHandler;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

/** AIO client session: sends CONNECT on ready, resolves CONNACK, dispatches PUSH. */
final class AioClientSession implements AioFrameHandler {

    private static final InternalLogger LOG = InternalLoggerFactory.getLogger(AioClientSession.class);

    private final String credential;
    private final int keepAliveSeconds;
    private final RmttPushHandler pushHandler;
    private final CompletableFuture<ConnectReturnCode> connectFuture;
    private final SessionEvents events;

    private volatile AioConnection conn;
    private volatile boolean connectSent;
    private volatile boolean connected;
    private volatile long lastReceivedMs = System.currentTimeMillis();
    private volatile long lastSentMs;

    interface SessionEvents {
        void onConnAck(ConnectReturnCode code, int serverKp);
        void onDisconnect(DisconnectReturnCode code);
    }

    AioClientSession(String credential,
                     int keepAliveSeconds,
                     RmttPushHandler pushHandler,
                     CompletableFuture<ConnectReturnCode> connectFuture,
                     SessionEvents events) {
        this.credential = credential;
        this.keepAliveSeconds = keepAliveSeconds;
        this.pushHandler = pushHandler;
        this.connectFuture = connectFuture;
        this.events = events;
    }

    void bind(AioConnection conn) {
        this.conn = conn;
    }

    long lastReceivedMs() {
        return lastReceivedMs;
    }

    long lastSentMs() {
        return lastSentMs;
    }

    boolean isConnected() {
        return connected;
    }

    @Override
    public synchronized void onReady() {
        if (connectSent) {
            return;
        }
        connectSent = true;
        byte[] credentialBytes = credential == null ? new byte[0] : credential.getBytes(StandardCharsets.UTF_8);
        RmttMessage connect = RmttMessageFactory.newMessage(
                new FixedHeader(RmttMessageType.CONNECT, false, false, false, false, 0),
                new ConnectVariableHeader(1, (byte) 0, keepAliveSeconds),
                new ConnectPayload(credential).credential() == null ? null
                        : new ConnectPayload(credential).credential().getBytes(StandardCharsets.UTF_8));
        conn.writeFrame(RmttWireCodec.encodeToBytes(connect));
        lastSentMs = System.currentTimeMillis();
    }

    @Override
    public synchronized void onMessage(RmttMessage msg) {
        lastReceivedMs = System.currentTimeMillis();
        switch (msg.fixedHeader().messageType()) {
            case CONNACK:
                handleConnAck((ConnAckMessage) msg);
                break;
            case PUSH:
                if (pushHandler != null) {
                    pushHandler.onPush(((PushMessage) msg).payload());
                }
                break;
            case DISCONNECT:
                DisconnectReturnCode code = DisconnectReturnCode.valueOf(((DisconnectMessage) msg).variableHeader().returnCode());
                if (events != null) {
                    events.onDisconnect(code);
                }
                conn.close();
                break;
            case PINGRESP:
                LOG.debug("PINGRESP received (credential={})", credential);
                lastReceivedMs = System.currentTimeMillis();
                break;
            default:
                break;
        }
    }

    private void handleConnAck(ConnAckMessage msg) {
        ConnectReturnCode code = msg.variableHeader().connectReturnCode();
        int serverKp = msg.variableHeader().serverKeepaliveSeconds();
        connected = code == ConnectReturnCode.CONNECT_ACCEPTED;
        connectFuture.complete(code);
        if (events != null) {
            events.onConnAck(code, serverKp);
        }
        if (!connected) {
            conn.close();
        }
    }

    void sendPing() {
        if (conn != null) {
            conn.writeFrame(RmttWireCodec.encodeToBytes(RmttMessageFactory.PINGREQ));
            lastSentMs = System.currentTimeMillis();
        }
    }

    @Override
    public void onClosed(Throwable cause) {
        connected = false;
        if (!connectFuture.isDone()) {
            connectFuture.completeExceptionally(cause == null
                    ? new java.io.IOException("connection closed") : cause);
        }
    }
}