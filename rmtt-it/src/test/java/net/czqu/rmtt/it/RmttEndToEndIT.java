package net.czqu.rmtt.it;

import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.codec.quic.Quic;
import io.netty.handler.codec.quic.QuicSslContext;
import io.netty.handler.codec.quic.QuicSslContextBuilder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.util.concurrent.AbstractScheduledEventExecutor;
import net.czqu.rmtt.api.AuthResult;
import net.czqu.rmtt.api.PushResult;
import net.czqu.rmtt.client.netty.RmttClient;
import net.czqu.rmtt.client.netty.RmttClientBuilder;
import net.czqu.rmtt.client.netty.RmttPushHandler;
import net.czqu.rmtt.protocol.ConnectReturnCode;
import net.czqu.rmtt.protocol.DisconnectReturnCode;
import net.czqu.rmtt.server.netty.RmttServer;
import net.czqu.rmtt.server.netty.RmttServerBuilder;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.lang.reflect.Field;
import java.math.BigInteger;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end integration tests: a real {@link RmttServer} and real {@link RmttClient}s
 * asserting full protocol round trips over every Netty transport: TCP, WebSocket,
 * secure WebSocket (WSS), KCP and QUIC (QUIC is skipped when the native library is
 * unavailable, e.g. on Windows).
 *
 * <p>Runs in the verify phase via the failsafe plugin (matching {@code *IT}).
 */
class RmttEndToEndIT {

    private static RmttServer server;
    private static int port;
    private static final AtomicReference<byte[]> UPSTREAM = new AtomicReference<>();
    private static final CountDownLatch UPSTREAM_LATCH = new CountDownLatch(1);
    private static final java.util.concurrent.ConcurrentLinkedQueue<String> UPSTREAM_LOG =
            new java.util.concurrent.ConcurrentLinkedQueue<>();

    private static SslContext wssServerCtx;
    private static SslContext wssClientCtx;
    private static QuicSslContext quicServerCtx;
    private static QuicSslContext quicClientCtx;

    private final List<RmttClient> clients = new ArrayList<>();

    @BeforeAll
    static void startServer() throws Exception {
        // self-signed certificate shared by the WSS and QUIC transports (generated
        // with BouncyCastle: netty's SelfSignedCertificate needs sun.misc internals)
        KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
        kpg.initialize(2048);
        KeyPair kp = kpg.generateKeyPair();
        X509Certificate cert = selfSigned(kp);
        wssServerCtx = SslContextBuilder.forServer(kp.getPrivate(), cert).build();
        wssClientCtx = SslContextBuilder.forClient().trustManager(cert).build();
        quicServerCtx = QuicSslContextBuilder.forServer(kp.getPrivate(), null, cert)
                .applicationProtocols("rmtt").build();
        // trust every cert: QUIC's OpenSSL stack rejects the self-signed cert even with
        // SAN, so the E2E test uses the standard insecure trust manager for clients
        quicClientCtx = QuicSslContextBuilder.forClient()
                .trustManager(io.netty.handler.ssl.util.InsecureTrustManagerFactory.INSTANCE)
                .applicationProtocols("rmtt").build();

        server = new RmttServerBuilder()
                .port(0)
                .authenticator(credential -> AuthResult.allow(credential))
                .messageHandler((deviceId, payload) -> {
                    String text = new String(payload, StandardCharsets.UTF_8);
                    UPSTREAM_LOG.add(deviceId + ":" + text);
                    if ("hello-up".equals(text)) {
                        UPSTREAM.set(payload);
                        UPSTREAM_LATCH.countDown();
                    }
                })
                .build();
        ChannelFuture bound = server.awaitStartup();
        port = ((InetSocketAddress) bound.channel().localAddress()).getPort();
    }

    @AfterAll
    static void stopServer() {
        if (server != null) {
            server.closeAll();
        }
    }

    @AfterEach
    void tearDown() {
        for (RmttClient c : clients) {
            try {
                c.disconnect();
            } catch (Exception ignored) {
                // best-effort cleanup
            }
            c.shutdown();
        }
        clients.clear();
    }

    private RmttClient connect(String credential, RmttPushHandler pushHandler) throws Exception {
        RmttClient c = new RmttClientBuilder()
                .host("127.0.0.1")
                .port(port)
                .credential(credential)
                .keepAliveSeconds(30)
                .connectTimeoutMillis(3000)
                .pushHandler(pushHandler)
                .build();
        ConnectReturnCode code = c.connect();
        assertEquals(ConnectReturnCode.CONNECT_ACCEPTED, code, "handshake should be accepted");
        clients.add(c);
        return c;
    }

    @Test
    void clientConnectsAndServerSeesItOnline() throws Exception {
        RmttClient c = connect("e2e-online", payload -> {
        });
        assertTrue(c.isConnected());
        assertTrue(server.isOnline("e2e-online"));
        assertEquals(1, server.onlineCount());
    }

    @Test
    void upstreamPushReachesServerHandler() throws Exception {
        connect("e2e-up", payload -> {
        });
        UPSTREAM.set(null);
        RmttClient c = clients.get(0);
        c.push("hello-up".getBytes(StandardCharsets.UTF_8)).awaitUninterruptibly(3000);
        assertTrue(UPSTREAM_LATCH.await(3, TimeUnit.SECONDS), "server should receive the push");
        assertArrayEquals("hello-up".getBytes(StandardCharsets.UTF_8), UPSTREAM.get());
    }

    @Test
    void downstreamPushReachesClientHandler() throws Exception {
        CountDownLatch down = new CountDownLatch(1);
        AtomicReference<byte[]> got = new AtomicReference<>();
        RmttClient c = connect("e2e-down", payload -> {
            got.set(payload);
            down.countDown();
        });
        PushResult r = server.push("e2e-down", "hello-down".getBytes(StandardCharsets.UTF_8));
        assertEquals(PushResult.SUCCESS, r);
        assertTrue(down.await(3, TimeUnit.SECONDS), "client should receive the push");
        assertArrayEquals("hello-down".getBytes(StandardCharsets.UTF_8), got.get());
        assertTrue(c.isConnected());
    }

    @Test
    void nullPayloadPushIsRejected() throws Exception {
        connect("e2e-null", payload -> {
        });
        PushResult r = server.push("e2e-null", (byte[]) null);
        assertEquals(PushResult.REJECTED, r);
    }

    @Test
    void kickDisconnectsClient() throws Exception {
        RmttClient c = connect("e2e-kick", payload -> {
        });
        server.kick("e2e-kick", DisconnectReturnCode.SERVER_SHUTDOWN);
        waitFor(() -> !c.isConnected(), "client to be kicked offline");
        assertFalse(c.isConnected());
        assertFalse(server.isOnline("e2e-kick"));
    }

    @Test
    void sessionTakeoverDisconnectsOldClient() throws Exception {
        RmttClient first = connect("e2e-takeover", payload -> {
        });
        assertTrue(first.isConnected());
        RmttClient second = connect("e2e-takeover", payload -> {
        });
        // the first connection must be dropped when the same credential reconnects
        waitFor(() -> !first.isConnected(), "old client to be dropped by session takeover");
        assertTrue(second.isConnected());
        assertFalse(first.isConnected());
    }

    @Test
    void noReaperTaskLeakAfterConnectDisconnectCycles() throws Exception {
        int baseline = scheduledTaskCount();
        for (int i = 0; i < 10; i++) {
            RmttClient c = new RmttClientBuilder()
                    .host("127.0.0.1")
                    .port(port)
                    .credential("e2e-cycle-" + i)
                    .keepAliveSeconds(30)
                    .connectTimeoutMillis(2000)
                    .build();
            c.connect();
            c.disconnect();
            c.shutdown();
        }
        Thread.sleep(200);
        assertEquals(baseline, scheduledTaskCount(), "no reaper task may leak per connection");
    }

    @Test
    void concurrentUpstreamAndDownstreamPush() throws Exception {
        final int n = 20;
        CountDownLatch downLatch = new CountDownLatch(n);
        java.util.concurrent.ConcurrentLinkedQueue<String> downSeen =
                new java.util.concurrent.ConcurrentLinkedQueue<>();
        RmttClient c = connect("e2e-concurrent", payload -> {
            downSeen.add(new String(payload, StandardCharsets.UTF_8));
            downLatch.countDown();
        });

        // downstream pushes from the server on a background thread while the
        // client pushes upstream on the main thread: both directions flow at once
        Thread downstream = new Thread(() -> {
            for (int i = 0; i < n; i++) {
                server.push("e2e-concurrent", ("down-" + i).getBytes(StandardCharsets.UTF_8));
            }
        });
        downstream.start();

        for (int i = 0; i < n; i++) {
            c.push(("up-" + i).getBytes(StandardCharsets.UTF_8)).awaitUninterruptibly(3000);
        }
        downstream.join(5000);

        assertTrue(downLatch.await(5, TimeUnit.SECONDS),
                "all downstream pushes must arrive, got " + downSeen.size() + " of " + n + ": " + downSeen);
        int up = 0;
        long deadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < deadline && up < n) {
            up = upstreamCount("e2e-concurrent", "up-");
            Thread.sleep(50);
        }
        assertEquals(n, up, "all upstream pushes must arrive");
        assertEquals(n, downSeen.size(), "client must receive every downstream push");
    }

    private static int upstreamCount(String deviceId, String prefix) {
        int count = 0;
        for (String entry : UPSTREAM_LOG) {
            String body = entry.startsWith(deviceId + ":") ? entry.substring(deviceId.length() + 1) : null;
            if (body != null && body.startsWith(prefix)) {
                count++;
            }
        }
        return count;
    }

    /** Transports exercised by the round-trip test. */
    enum Transport {
        TCP, WS, WSS, KCP, QUIC
    }

    @ParameterizedTest
    @EnumSource(Transport.class)
    void transportRoundTrip(Transport t) throws Exception {
        Assumptions.assumeTrue(t != Transport.QUIC || Quic.isAvailable(),
                "QUIC native library not available on this platform");
        String device = "e2e-" + t.name().toLowerCase();
        int tcpPort = freeTcpPort();
        int wsPort = freeTcpPort();
        int wssPort = freeTcpPort();
        int kcpPort = freeUdpPort();
        int quicPort = freeUdpPort();

        RmttServer srv = null;
        RmttClient c = null;
        try {
            RmttServerBuilder sb = new RmttServerBuilder()
                    .authenticator(credential -> AuthResult.allow(credential))
                    .messageHandler((deviceId, payload) -> {
                    });
            int clientPort = 0;
            switch (t) {
                case TCP:
                    sb.port(tcpPort);
                    clientPort = tcpPort;
                    break;
                case WS:
                    sb.port(0).wsPort(wsPort);
                    clientPort = wsPort;
                    break;
                case WSS:
                    sb.port(0).wssPort(wssPort).wssSslContext(wssServerCtx);
                    clientPort = wssPort;
                    break;
                case KCP:
                    sb.port(0).kcpPort(kcpPort);
                    clientPort = kcpPort;
                    break;
                case QUIC:
                    sb.port(0).quicPort(quicPort).quicSslContext(quicServerCtx);
                    clientPort = quicPort;
                    break;
            }
            srv = sb.build();
            srv.awaitStartup();

            CountDownLatch down = new CountDownLatch(1);
            AtomicReference<byte[]> got = new AtomicReference<>();
            RmttClientBuilder cb = new RmttClientBuilder()
                    .host("127.0.0.1")
                    .credential(device)
                    .keepAliveSeconds(30)
                    .connectTimeoutMillis(5000)
                    .pushHandler(payload -> {
                        got.set(payload);
                        down.countDown();
                    });
            switch (t) {
                case TCP:
                    cb.port(clientPort);
                    break;
                case WS:
                    cb.port(clientPort).webSocket(true);
                    break;
                case WSS:
                    cb.port(clientPort).webSocket(true).secure(true).sslContext(wssClientCtx);
                    break;
                case KCP:
                    cb.port(clientPort).kcp(true);
                    break;
                case QUIC:
                    cb.port(clientPort).quic(true).quicSslContext(quicClientCtx);
                    break;
            }
            c = cb.build();

            assertEquals(ConnectReturnCode.CONNECT_ACCEPTED, c.connect(), t + " handshake");
            assertTrue(c.isConnected(), t + " connected");
            assertTrue(srv.isOnline(device), t + " online");

            // upstream push then downstream push over the same transport
            c.push(("up-" + t.name()).getBytes(StandardCharsets.UTF_8)).awaitUninterruptibly(5000);
            byte[] downPayload = ("down-" + t.name()).getBytes(StandardCharsets.UTF_8);
            assertEquals(PushResult.SUCCESS, srv.push(device, downPayload), t + " downstream push");
            assertTrue(down.await(5, TimeUnit.SECONDS), t + " client must receive downstream");
            assertArrayEquals(downPayload, got.get(), t + " payload integrity");
        } finally {
            if (c != null) {
                try {
                    c.disconnect();
                } catch (Exception ignored) {
                    // best-effort cleanup
                }
                c.shutdown();
            }
            if (srv != null) {
                srv.closeAll();
            }
        }
    }

    private static int freeTcpPort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private static int freeUdpPort() throws Exception {
        try (DatagramSocket s = new DatagramSocket(0)) {
            return s.getLocalPort();
        }
    }

    private static X509Certificate selfSigned(KeyPair kp) throws Exception {
        long now = System.currentTimeMillis();
        X500Name subject = new X500Name("CN=rmtt-e2e");
        JcaX509v3CertificateBuilder builder = new JcaX509v3CertificateBuilder(
                subject, BigInteger.valueOf(now), new Date(now - 86_400_000L),
                new Date(now + 365L * 86_400_000L), subject, kp.getPublic());
        // SAN for 127.0.0.1 so the client-side hostname verification accepts it
        builder.addExtension(org.bouncycastle.asn1.x509.Extension.subjectAlternativeName, false,
                new org.bouncycastle.asn1.x509.GeneralNames(
                        new org.bouncycastle.asn1.x509.GeneralName(
                                org.bouncycastle.asn1.x509.GeneralName.iPAddress, "127.0.0.1")));
        ContentSigner signer = new JcaContentSignerBuilder("SHA256withRSA").build(kp.getPrivate());
        return new JcaX509CertificateConverter().getCertificate(builder.build(signer));
    }

    private static void waitFor(Check cond, String what) throws Exception {
        long deadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < deadline) {
            if (cond.matches()) {
                return;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("timed out waiting for " + what);
    }

    private interface Check {
        boolean matches();
    }

    private static int scheduledTaskCount() {
        try {
            Field f = RmttServer.class.getDeclaredField("workerGroup");
            f.setAccessible(true);
            EventLoopGroup wg = (EventLoopGroup) f.get(server);
            Field queue = AbstractScheduledEventExecutor.class.getDeclaredField("scheduledTaskQueue");
            queue.setAccessible(true);
            final int[] sum = {0};
            wg.forEach(exec -> {
                try {
                    Queue<?> q = (Queue<?>) queue.get(exec);
                    sum[0] += q == null ? 0 : q.size();
                } catch (IllegalAccessException ignored) {
                    // reflection only; ignore per-executor failures
                }
            });
            return sum[0];
        } catch (Exception e) {
            throw new IllegalStateException("cannot inspect server workerGroup", e);
        }
    }
}
