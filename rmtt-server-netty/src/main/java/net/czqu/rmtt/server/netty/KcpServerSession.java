package net.czqu.rmtt.server.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import kcp.KcpListener;
import kcp.Ukcp;
import net.czqu.rmtt.api.AuthResult;
import net.czqu.rmtt.api.Authenticator;
import net.czqu.rmtt.api.ConnectionListener;
import net.czqu.rmtt.api.ConnectionStore;
import net.czqu.rmtt.api.DeviceConnection;
import net.czqu.rmtt.api.RmttMessageHandler;
import net.czqu.rmtt.codec.netty.ByteBufRmttByteReader;
import net.czqu.rmtt.codec.netty.ByteBufRmttByteWriter;
import net.czqu.rmtt.protocol.ConnAckVariableHeader;
import net.czqu.rmtt.protocol.ConnectMessage;
import net.czqu.rmtt.protocol.ConnectReturnCode;
import net.czqu.rmtt.protocol.DisconnectReturnCode;
import net.czqu.rmtt.protocol.FixedHeader;
import net.czqu.rmtt.protocol.RmttByteReader.Underflow;
import net.czqu.rmtt.protocol.RmttMessage;
import net.czqu.rmtt.protocol.RmttMessageFactory;
import net.czqu.rmtt.protocol.RmttMessageType;
import net.czqu.rmtt.protocol.RmttProtocol;
import net.czqu.rmtt.protocol.RmttWireCodec;
import net.czqu.rmtt.protocol.RmttWireCodec.BadProtocolVersionViolation;
import net.czqu.rmtt.protocol.RmttWireCodec.MagicNumberViolation;
import net.czqu.rmtt.protocol.RmttWireCodec.ProtocolViolation;
import net.czqu.rmtt.protocol.ServerKeepalivePolicy;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import net.czqu.rmtt.logging.InternalLogger;
import net.czqu.rmtt.logging.InternalLoggerFactory;

/**
 * kcp-base {@link KcpListener} for the RMTT server. Mirrors {@link ServerRmttHandler}: frames the
 * RMTT stream out of the KCP receive buffers, runs the CONNECT/CONNACK handshake, registers devices
 * in the shared {@link ConnectionStore} and enforces the server keepalive timeout (1.5x negotiated
 * server_kp).
 */
final class KcpServerSession implements KcpListener {

    private static final InternalLogger LOG = InternalLoggerFactory.getLogger(KcpServerSession.class);

    private final ConnectionStore connectionStore;
    private final Authenticator authenticator;
    private final RmttMessageHandler messageHandler;
    private final ConnectionListener connectionListener;
    private final ServerKeepalivePolicy keepalivePolicy;
    private final ScheduledExecutorService scheduler;
    private final int maxBytesInMessage;

    private final ConcurrentHashMap<Ukcp, Session> sessions = new ConcurrentHashMap<>();

    private static final class Session {
        final Ukcp ukcp;
        final ByteBuf pending;
        String deviceId;
        long serverKp;
        KcpDeviceConnection conn;
        long lastReadNanos;

        Session(Ukcp ukcp) {
            this.ukcp = ukcp;
            this.pending = Unpooled.buffer();
            this.lastReadNanos = System.nanoTime();
        }
    }

    KcpServerSession(ConnectionStore connectionStore,
                     Authenticator authenticator,
                     RmttMessageHandler messageHandler,
                     ConnectionListener connectionListener,
                     ServerKeepalivePolicy keepalivePolicy,
                     ScheduledExecutorService scheduler) {
        this.connectionStore = connectionStore;
        this.authenticator = authenticator;
        this.messageHandler = messageHandler;
        this.connectionListener = connectionListener;
        this.keepalivePolicy = keepalivePolicy;
        this.scheduler = scheduler;
        this.maxBytesInMessage = RmttProtocol.DEFAULT_MAX_BYTES_IN_MESSAGE;
    }

    void startReaper() {
        scheduler.scheduleAtFixedRate(this::reap, 1, 1, TimeUnit.SECONDS);
    }

    @Override
    public void onConnected(Ukcp ukcp) {
        sessions.put(ukcp, new Session(ukcp));
    }

    @Override
    public void handleReceive(ByteBuf data, Ukcp ukcp) {
        Session s = sessions.get(ukcp);
        if (s == null) {
            s = new Session(ukcp);
            sessions.put(ukcp, s);
        }
        s.pending.writeBytes(data);
        try {
            while (true) {
                if (s.pending.readableBytes() < 1) {
                    break;
                }
                s.pending.markReaderIndex();
                ByteBufRmttByteReader reader = new ByteBufRmttByteReader(s.pending);
                FixedHeader header;
                try {
                    header = RmttWireCodec.decodeHeader(reader);
                } catch (Underflow u) {
                    s.pending.resetReaderIndex();
                    break;
                }
                if (header.remainingLength() < 0 || header.remainingLength() > maxBytesInMessage) {
                    s.pending.resetReaderIndex();
                    protocolViolation(s, "message too large: " + header.remainingLength() + " bytes");
                    break;
                }
                if (s.pending.readableBytes() < header.remainingLength()) {
                    s.pending.resetReaderIndex();
                    break;
                }
                RmttMessage msg;
                try {
                    msg = RmttWireCodec.decodeBody(header, reader);
                } catch (MagicNumberViolation m) {
                    s.pending.resetReaderIndex();
                    closeNoReply(s, "bad magic");
                    break;
                } catch (BadProtocolVersionViolation v) {
                    s.pending.resetReaderIndex();
                    badVersionReply(s);
                    break;
                } catch (ProtocolViolation p) {
                    s.pending.resetReaderIndex();
                    protocolViolation(s, "protocol violation");
                    break;
                }
                s.pending.discardReadBytes();
                s.lastReadNanos = System.nanoTime();
                if (msg != null) {
                    process(s, msg);
                }
            }
        } catch (Throwable t) {
            LOG.warn("kcp receive processing error", t);
        }
    }

    private void process(Session s, RmttMessage msg) {
        switch (msg.fixedHeader().messageType()) {
            case CONNECT:
                handleConnect(s, (ConnectMessage) msg);
                break;
            case PUSH:
                if (s.deviceId != null && messageHandler != null) {
                    messageHandler.onMessage(s.deviceId, msg.payload());
                }
                break;
            case PINGREQ:
                LOG.debug("PINGREQ from device={} -> PINGRESP", s.deviceId);
                send(s.ukcp, RmttMessageFactory.PINGRESP);
                break;
            case DISCONNECT:
                unregisterAndNotify(s, "client disconnect");
                closeSoon(s.ukcp);
                break;
            default:
                break;
        }
    }

    private void handleConnect(Session s, ConnectMessage msg) {
        String credential = msg.connectPayload().credential();
        if (credential != null && credential.length() > RmttProtocol.DEFAULT_MAX_CREDENTIAL_LENGTH) {
            LOG.warn("CONNECT rejected: credential too long ({} > max {})", credential.length(),
                    RmttProtocol.DEFAULT_MAX_CREDENTIAL_LENGTH);
            send(s.ukcp, connAck(ConnectReturnCode.CONNECT_UNAUTHORIZED, 0));
            closeSoon(s.ukcp);
            return;
        }
        AuthResult authResult;
        try {
            authResult = authenticator.authenticate(credential);
        } catch (Exception e) {
            send(s.ukcp, connAck(ConnectReturnCode.CONNECT_SERVER_UNAVAILABLE, 0));
            closeSoon(s.ukcp);
            return;
        }
        if (authResult == null || !authResult.allowed()) {
            ConnectReturnCode code = authResult != null ? authResult.returnCode()
                    : ConnectReturnCode.CONNECT_UNAUTHORIZED;
            send(s.ukcp, connAck(code, 0));
            closeSoon(s.ukcp);
            return;
        }

        long kp = keepalivePolicy.decide(msg.variableHeader().keepAliveTimeSeconds());
        String deviceId = authResult.deviceId();
        KcpDeviceConnection conn = new KcpDeviceConnection(s.ukcp, () -> closeSoon(s.ukcp));
        s.conn = conn;
        s.serverKp = kp;
        s.deviceId = deviceId;
        // Send CONNACK before registering so a push cannot target the client
        // before it finishes the handshake (matches rmtt-go ordering).
        send(s.ukcp, connAck(ConnectReturnCode.CONNECT_ACCEPTED, (int) kp));
        Optional<DeviceConnection> previous = connectionStore.register(deviceId, conn);
        previous.ifPresent(old -> {
            if (old.isActive()) {
                old.sendDisconnect(DisconnectReturnCode.SESSION_TAKEN_OVER);
            }
        });
        connectionListener.onConnectionEstablished(deviceId);
        LOG.info("device {} connected via kcp (proposal={}s serverKp={}s)",
                deviceId, msg.variableHeader().keepAliveTimeSeconds(), kp);
    }

    private void reap() {
        long now = System.nanoTime();
        for (Session s : sessions.values()) {
            if (s.serverKp <= 0 || s.deviceId == null || s.conn == null) {
                continue;
            }
            if (!s.ukcp.isActive()) {
                continue;
            }
            if (now - s.lastReadNanos > s.serverKp * 1_500_000_000L) {
                LOG.warn("keepalive timeout reaping device={} (no packet for {}ms, serverKp={}s)",
                        s.deviceId, (now - s.lastReadNanos) / 1_000_000L, s.serverKp);
                s.conn.sendDisconnect(DisconnectReturnCode.KEEPALIVE_TIMEOUT);
                unregisterAndNotify(s, "keepalive timeout");
            }
        }
    }

    private void protocolViolation(Session s, String reason) {
        send(s.ukcp, RmttMessageFactory.disconnect(DisconnectReturnCode.PROTOCOL_VIOLATION));
        unregisterAndNotify(s, reason);
        closeSoon(s.ukcp);
    }

    private void badVersionReply(Session s) {
        send(s.ukcp, connAck(ConnectReturnCode.CONNECT_BAD_PROTOCOL_VERSION, 0));
        unregisterAndNotify(s, "bad protocol version");
        closeSoon(s.ukcp);
    }

    private void closeNoReply(Session s, String reason) {
        unregisterAndNotify(s, reason);
        closeSoon(s.ukcp);
    }

    private void unregisterAndNotify(Session s, String reason) {
        if (s.deviceId != null && s.conn != null) {
            connectionStore.remove(s.deviceId, s.conn);
            connectionListener.onConnectionClosed(s.deviceId, reason);
            LOG.info("device {} disconnected via kcp ({})", s.deviceId, reason);
        }
        s.deviceId = null;
        s.conn = null;
    }

    private void send(Ukcp ukcp, RmttMessage msg) {
        if (!ukcp.isActive()) {
            return;
        }
        ByteBuf out = Unpooled.buffer();
        RmttWireCodec.encode(msg, new ByteBufRmttByteWriter(out));
        ukcp.write(out);
        out.release();
    }

    private void closeSoon(Ukcp ukcp) {
        scheduler.schedule(() -> {
            if (ukcp.isActive()) {
                ukcp.close();
            }
        }, 50, TimeUnit.MILLISECONDS);
    }

    @Override
    public void handleException(Throwable cause, Ukcp ukcp) {
        LOG.warn("kcp session error", cause);
    }

    @Override
    public void handleClose(Ukcp ukcp) {
        Session s = sessions.remove(ukcp);
        if (s == null) {
            return;
        }
        unregisterAndNotify(s, "connection closed");
        s.pending.release();
    }

    private static RmttMessage connAck(ConnectReturnCode code, int serverKeepalive) {
        return RmttMessageFactory.newMessage(
                new FixedHeader(RmttMessageType.CONNACK, false, false, false, false, 3),
                new ConnAckVariableHeader(code, serverKeepalive), null);
    }
}
