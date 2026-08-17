package net.czqu.rmtt.server.aio;

import net.czqu.rmtt.api.AuthResult;
import net.czqu.rmtt.api.Authenticator;
import net.czqu.rmtt.api.ConnectionListener;
import net.czqu.rmtt.api.ConnectionStore;
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
import net.czqu.rmtt.protocol.RmttWireCodec;
import net.czqu.rmtt.protocol.ServerKeepalivePolicy;
import net.czqu.rmtt.logging.InternalLogger;
import net.czqu.rmtt.logging.InternalLoggerFactory;
import net.czqu.rmtt.transport.aio.AioConnection;
import net.czqu.rmtt.transport.aio.AioFrameHandler;

import java.util.Optional;

/** Per-connection AIO server handler: CONNECT handshake, PUSH dispatch, heartbeat. */
public final class AioServerSession implements AioFrameHandler {

    private static final InternalLogger LOG = InternalLoggerFactory.getLogger(AioServerSession.class);

    private AioConnection conn;
    private final ConnectionStore connectionStore;
    private final Authenticator authenticator;
    private final RmttMessageHandler messageHandler;
    private final ConnectionListener connectionListener;
    private final ServerKeepalivePolicy keepalivePolicy;

    private volatile String deviceId;
    private volatile AioDeviceConnection connection;
    private volatile long serverKp;
    private Runnable closeCallback;

    AioServerSession(ConnectionStore connectionStore,
                     Authenticator authenticator,
                     RmttMessageHandler messageHandler,
                     ConnectionListener connectionListener,
                     ServerKeepalivePolicy keepalivePolicy) {
        this.connectionStore = connectionStore;
        this.authenticator = authenticator;
        this.messageHandler = messageHandler;
        this.connectionListener = connectionListener;
        this.keepalivePolicy = keepalivePolicy;
    }

    /** Effective server-side keepalive for this session (0 = disabled). */
    long serverKp() {
        return serverKp;
    }

    void bind(AioConnection conn) {
        this.conn = conn;
    }

    void onSessionClosed(Runnable closeCallback) {
        this.closeCallback = closeCallback;
    }

    long lastReadTime() {
        return conn == null ? System.currentTimeMillis() : conn.lastReadTime();
    }

    void kick(DisconnectReturnCode code) {
        if (connection != null) {
            connection.sendDisconnect(code);
        }
    }

    /** Force-close the underlying connection (server shutdown). */
    void closed() {
        if (conn != null) {
            conn.close();
        }
    }

    @Override
    public synchronized void onMessage(RmttMessage msg) {
        switch (msg.fixedHeader().messageType()) {
            case CONNECT:
                handleConnect((ConnectMessage) msg);
                break;
            case PUSH:
                handlePush(msg);
                break;
            case PINGREQ:
                LOG.debug("PINGREQ from device={} -> PINGRESP", deviceId);
                conn.writeFrame(RmttWireCodec.encodeToBytes(RmttMessageFactory.PINGRESP));
                break;
            case DISCONNECT:
                conn.close();
                break;
            default:
                break;
        }
    }

    private void handleConnect(ConnectMessage msg) {
        String credential = msg.connectPayload().credential();
        if (credential != null && credential.length() > RmttProtocol.DEFAULT_MAX_CREDENTIAL_LENGTH) {
            LOG.warn("CONNECT rejected: credential too long ({} > max {})", credential.length(),
                    RmttProtocol.DEFAULT_MAX_CREDENTIAL_LENGTH);
            writeConnAck(ConnectReturnCode.CONNECT_UNAUTHORIZED, 0);
            conn.close();
            return;
        }
        AuthResult authResult;
        try {
            authResult = authenticator.authenticate(credential);
        } catch (Exception e) {
            LOG.error("authenticator failed for credential={}", credential, e);
            writeConnAck(ConnectReturnCode.CONNECT_SERVER_UNAVAILABLE, 0);
            conn.close();
            return;
        }
        if (authResult == null || !authResult.allowed()) {
            ConnectReturnCode code = authResult != null ? authResult.returnCode()
                    : ConnectReturnCode.CONNECT_UNAUTHORIZED;
            LOG.warn("rejecting connect credential={} code={}", credential, code);
            writeConnAck(code, 0);
            conn.close();
            return;
        }

        long kp = keepalivePolicy.decide(msg.variableHeader().keepAliveTimeSeconds());
        this.serverKp = kp;
        this.connection = new AioDeviceConnection(conn);
        this.deviceId = authResult.deviceId();
        // Send CONNACK before registering so a push cannot target the client
        // before it finishes the handshake (matches rmtt-go ordering).
        writeConnAck(ConnectReturnCode.CONNECT_ACCEPTED, kp);
        Optional<net.czqu.rmtt.api.DeviceConnection> previous = connectionStore.register(deviceId, connection);
        previous.ifPresent(old -> {
            if (old.isActive()) {
                old.sendDisconnect(DisconnectReturnCode.SESSION_TAKEN_OVER);
            }
        });
        connectionListener.onConnectionEstablished(deviceId);
        LOG.info("device {} connected via aio (proposal={}s serverKp={}s)",
                deviceId, msg.variableHeader().keepAliveTimeSeconds(), kp);
    }

    private void writeConnAck(ConnectReturnCode code, long serverKeepalive) {
        RmttMessage ack = RmttMessageFactory.newMessage(
                new FixedHeader(RmttMessageType.CONNACK, false, false, false, false, 3),
                new ConnAckVariableHeader(code, (int) serverKeepalive), null);
        conn.writeFrame(RmttWireCodec.encodeToBytes(ack));
    }

    private void handlePush(RmttMessage msg) {
        String id = this.deviceId;
        if (id == null || messageHandler == null) {
            return;
        }
        messageHandler.onMessage(id, msg.payload());
    }

    @Override
    public void onReady() {
        // server waits for the client CONNECT
    }

    @Override
    public void onClosed(Throwable cause) {
        String id = deviceId;
        AioDeviceConnection c = connection;
        deviceId = null;
        connection = null;
        if (id != null && c != null) {
            connectionStore.remove(id, c);
            connectionListener.onConnectionClosed(id,
                    cause == null ? "connection closed" : cause.getMessage());
            LOG.info("device {} disconnected via aio ({})", id,
                    cause == null ? "connection closed" : cause.getMessage());
        }
        if (closeCallback != null) {
            closeCallback.run();
        }
    }
}