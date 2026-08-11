package net.czqu.rmtt.server.netty;

import kcp.ChannelConfig;
import net.czqu.rmtt.api.Authenticator;
import net.czqu.rmtt.logging.InternalLogger;
import net.czqu.rmtt.logging.InternalLoggerFactory;
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

import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static net.czqu.rmtt.api.PushResult.DEVICE_OFFLINE;
import static net.czqu.rmtt.api.PushResult.REJECTED;
import static net.czqu.rmtt.api.PushResult.SUCCESS;

/**
 * KCP transport for the RMTT server, backed by kcp-base. The kcp-base {@link ChannelConfig} defaults
 * (address-based channel management, no FEC, no CRC32) match the kcp-go client defaults, so a kcp-go
 * device (which uses a random conv) interoperates directly. Devices are registered in the same
 * {@link ConnectionStore} as the TCP/WS listeners, allowing the push API to be transport-agnostic.
 */
public final class KcpServer {

    private static final InternalLogger LOG = InternalLoggerFactory.getLogger(KcpServer.class);

    private final ConnectionStore connectionStore;
    private final Authenticator authenticator;
    private final RmttMessageHandler messageHandler;
    private final ConnectionListener connectionListener;
    private final ServerKeepalivePolicy keepalivePolicy;
    private final int port;

    private final KcpServerSession sessionListener;
    private final ScheduledExecutorService scheduler;
    private kcp.KcpServer kcpServer;
    private volatile boolean started;

    KcpServer(ConnectionStore connectionStore,
              Authenticator authenticator,
              RmttMessageHandler messageHandler,
              ConnectionListener connectionListener,
              ServerKeepalivePolicy keepalivePolicy,
              int port) {
        this.connectionStore = connectionStore;
        this.authenticator = authenticator;
        this.messageHandler = messageHandler;
        this.connectionListener = connectionListener;
        this.keepalivePolicy = keepalivePolicy;
        this.port = port;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "rmtt-kcp");
            t.setDaemon(true);
            return t;
        });
        this.sessionListener = new KcpServerSession(connectionStore, authenticator, messageHandler,
                connectionListener, keepalivePolicy, scheduler);
    }

    /** Bind the KCP listener (synchronous). */
    public void start() {
        ChannelConfig config = new ChannelConfig();
        config.setUseConvChannel(true);
        kcpServer = new kcp.KcpServer(config, sessionListener);
        kcpServer.start(port);
        sessionListener.startReaper();
        started = true;
        LOG.info("RMTT server (netty) KCP listening on udp://0.0.0.0:{}", port);
    }

    /**
     * Whether the KCP listener is bound.
     *
     * @return true when started and not yet closed
     */
    public boolean isStarted() {
        return started;
    }

    /**
     * Stop the listener and the reaper scheduler.
     */
    public void closeAll() {
        if (kcpServer != null) {
            try {
                kcpServer.stop();
            } catch (Exception ignored) {
                // best-effort stop
            }
        }
        scheduler.shutdownNow();
        started = false;
    }

    /**
     * Synchronous downstream push; identical semantics to {@link RmttServer#push}.
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
     * Synchronous downstream push; identical semantics to {@link RmttServer#push}.
     *
     * @param deviceId the target device id
     * @param payload  the UTF-8 payload
     * @return the push outcome
     */
    public PushResult push(String deviceId, String payload) {
        return push(deviceId, payload.getBytes(StandardCharsets.UTF_8));
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
}
