package net.czqu.rmtt.client.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import kcp.KcpListener;
import kcp.Ukcp;
import net.czqu.rmtt.codec.netty.ByteBufRmttByteReader;
import net.czqu.rmtt.codec.netty.ByteBufRmttByteWriter;
import net.czqu.rmtt.protocol.ConnAckMessage;
import net.czqu.rmtt.protocol.ConnectPayload;
import net.czqu.rmtt.protocol.ConnectReturnCode;
import net.czqu.rmtt.protocol.ConnectVariableHeader;
import net.czqu.rmtt.protocol.DisconnectMessage;
import net.czqu.rmtt.protocol.DisconnectReturnCode;
import net.czqu.rmtt.protocol.FixedHeader;
import net.czqu.rmtt.protocol.RmttByteReader.Underflow;
import net.czqu.rmtt.protocol.RmttMessage;
import net.czqu.rmtt.protocol.RmttMessageFactory;
import net.czqu.rmtt.protocol.RmttMessageType;
import net.czqu.rmtt.protocol.RmttProtocol;
import net.czqu.rmtt.protocol.RmttWireCodec;

import net.czqu.rmtt.logging.InternalLogger;
import net.czqu.rmtt.logging.InternalLoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

/**
 * Client-side KCP session (kcp-base). Mirrors {@link ClientRmttHandler}: frames the RMTT stream out
 * of the KCP receive buffers, sends CONNECT on {@code onConnected} and completes the
 * CONNECT/CONNACK handshake through the shared future.
 */
class KcpClientSession implements KcpListener, ClientSession {

    private static final InternalLogger LOG = InternalLoggerFactory.getLogger(KcpClientSession.class);

    private final String credential;
    private final int keepAliveSeconds;
    private final RmttPushHandler pushHandler;
    private final CompletableFuture<ConnectReturnCode> connectFuture;
    private final ClientRmttHandler.SessionEvents events;
    private final int maxBytesInMessage;

    private final ByteBuf pending = Unpooled.buffer();
    private volatile Ukcp ukcp;
    private volatile boolean connectSent;
    private volatile boolean connected;
    private volatile int serverKp;
    private volatile long lastReceivedMs;
    private volatile long lastSentMs;

    KcpClientSession(String credential,
                     int keepAliveSeconds,
                     RmttPushHandler pushHandler,
                     CompletableFuture<ConnectReturnCode> connectFuture,
                     ClientRmttHandler.SessionEvents events) {
        this.credential = credential;
        this.keepAliveSeconds = keepAliveSeconds;
        this.pushHandler = pushHandler;
        this.connectFuture = connectFuture;
        this.events = events;
        this.maxBytesInMessage = RmttProtocol.DEFAULT_MAX_BYTES_IN_MESSAGE;
    }

    boolean isSessionActive() {
        Ukcp u = ukcp;
        return u != null && u.isActive();
    }

    @Override
    public long lastSentMs() {
        return lastSentMs;
    }

    @Override
    public long lastReceivedMs() {
        return lastReceivedMs;
    }

    @Override
    public void onConnected(Ukcp ukcp) {
        this.ukcp = ukcp;
        long now = System.currentTimeMillis();
        this.lastReceivedMs = now;
        this.lastSentMs = now;
        sendConnect();
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
                new ConnectPayload(credential).credential().getBytes(StandardCharsets.UTF_8));
        push(connect);
        lastSentMs = System.currentTimeMillis();
    }

    @Override
    public void handleReceive(ByteBuf data, Ukcp ukcp) {
        pending.writeBytes(data);
        try {
            while (true) {
                if (pending.readableBytes() < 1) {
                    break;
                }
                pending.markReaderIndex();
                ByteBufRmttByteReader reader = new ByteBufRmttByteReader(pending);
                FixedHeader header;
                try {
                    header = RmttWireCodec.decodeHeader(reader);
                } catch (Underflow u) {
                    pending.resetReaderIndex();
                    break;
                }
                if (header.remainingLength() < 0 || header.remainingLength() > maxBytesInMessage) {
                    pending.resetReaderIndex();
                    closeSession("message too large");
                    break;
                }
                if (pending.readableBytes() < header.remainingLength()) {
                    pending.resetReaderIndex();
                    break;
                }
                RmttMessage msg = RmttWireCodec.decodeBody(header, reader);
                pending.discardReadBytes();
                lastReceivedMs = System.currentTimeMillis();
                process(msg);
            }
        } catch (Throwable t) {
            closeSession("decode error");
        }
    }

    private void process(RmttMessage msg) {
        switch (msg.fixedHeader().messageType()) {
            case CONNACK:
                handleConnAck((ConnAckMessage) msg);
                break;
            case PUSH:
                if (pushHandler != null) {
                    pushHandler.onPush(msg.payload());
                }
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
                closeSession("disconnect " + code);
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
            closeSession("connack rejected");
        }
    }

    @Override
    public void handleException(Throwable cause, Ukcp ukcp) {
        if (!connectFuture.isDone()) {
            connectFuture.completeExceptionally(cause);
        }
        closeSession(cause.getMessage());
    }

    @Override
    public void handleClose(Ukcp ukcp) {
        if (events != null) {
            events.onClosed();
        }
    }

    private void closeSession(String reason) {
        Ukcp u = ukcp;
        if (u != null) {
            u.close();
        }
    }

    @Override
    public void sendPing() {
        push(RmttMessageFactory.PINGREQ);
        lastSentMs = System.currentTimeMillis();
    }

    @Override
    public void push(RmttMessage msg) {
        Ukcp u = ukcp;
        if (u == null || !u.isActive()) {
            return;
        }
        ByteBuf out = Unpooled.buffer();
        RmttWireCodec.encode(msg, new ByteBufRmttByteWriter(out));
        u.write(out);
        out.release();
    }
}
