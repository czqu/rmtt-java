package net.czqu.rmtt.it;

import net.czqu.rmtt.api.AuthResult;
import net.czqu.rmtt.api.PushResult;
import net.czqu.rmtt.client.aio.RmttClient;
import net.czqu.rmtt.client.aio.RmttClientBuilder;
import net.czqu.rmtt.protocol.ConnectReturnCode;
import net.czqu.rmtt.protocol.ServerKeepalivePolicy;
import net.czqu.rmtt.server.aio.RmttServer;
import net.czqu.rmtt.server.aio.RmttServerBuilder;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.jupiter.api.Test;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static net.czqu.rmtt.it.RmttEndToEndIT.readFrame;
import static net.czqu.rmtt.it.RmttEndToEndIT.rawConnect;
import static net.czqu.rmtt.it.RmttEndToEndIT.waitFor;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * AIO-stack regression tests for the cross-language blackbox report findings:
 * F1 (8 KiB frame cap), F2 (23-byte credential cap), F3 (no DISCONNECT(0x03) on shutdown),
 * F5 (AIO client only detects a dead peer via keepalive timeout) and F10 (keepalive
 * reaper granularity). The Netty-stack counterparts live in {@link RmttEndToEndIT}.
 */
class RmttAioRegressionIT {

    private static final String LONG_CREDENTIAL;

    static {
        char[] raw = new char[24];
        Arrays.fill(raw, 'x');
        LONG_CREDENTIAL = new String(raw); // 24 bytes: exactly one byte above the old cap
    }

    // ---------------------------------------------------------------- F1 / F2 (AIO)

    @Test
    void largeFrameAndLongCredentialOverTcp() throws Exception {
        roundTrip(0, RmttEndToEndIT.freeTcpPort(), null, false);
    }

    @Test
    void largeFrameOverWs() throws Exception {
        roundTrip(RmttEndToEndIT.freeTcpPort(), 0, null, false);
    }

    @Test
    void largeFrameOverTls() throws Exception {
        KeyPair kpg = keyPair();
        X509Certificate cert = selfSigned(kpg);
        roundTrip(0, RmttEndToEndIT.freeTcpPort(), sslContexts(kpg, cert), true);
    }

    /** 1 MiB up/down plus a 24-byte credential, over the requested AIO transport. */
    private void roundTrip(int wsPort, int plainPort, SSLContext[] ssl, boolean tls) throws Exception {
        byte[] big = new byte[1024 * 1024];
        new java.util.Random(3).nextBytes(big);
        AtomicReference<byte[]> upstream = new AtomicReference<>();
        CountDownLatch upstreamLatch = new CountDownLatch(1);
        RmttServerBuilder sb = new RmttServerBuilder()
                .authenticator(AuthResult::allow)
                .messageHandler((deviceId, payload) -> {
                    upstream.set(payload);
                    upstreamLatch.countDown();
                });
        if (tls) {
            sb.port(0).tlsPort(plainPort).sslContext(ssl[0]);
        } else if (wsPort > 0) {
            sb.port(0).wsPort(wsPort);
        } else {
            sb.port(plainPort);
        }
        RmttServer server = sb.build();
        server.start();
        int clientPort = tls ? plainPort : (wsPort > 0 ? wsPort : plainPort);

        AtomicReference<byte[]> down = new AtomicReference<>();
        CountDownLatch downLatch = new CountDownLatch(1);
        RmttClientBuilder cb = new RmttClientBuilder()
                .host("127.0.0.1")
                .port(clientPort)
                .credential(LONG_CREDENTIAL)
                .connectTimeoutMillis(5000)
                .pushHandler(payload -> {
                    down.set(payload);
                    downLatch.countDown();
                });
        if (tls) {
            cb.tls(true).sslContext(ssl[1]);
        } else if (wsPort > 0) {
            cb.webSocket(true);
        }
        RmttClient client = cb.build();
        try {
            assertEquals(ConnectReturnCode.CONNECT_ACCEPTED, client.connect(),
                    "F2: 24-byte credential must be accepted");
            client.push(big);
            assertTrue(upstreamLatch.await(20, TimeUnit.SECONDS), "F1: 1 MiB upstream");
            assertArrayEquals(big, upstream.get(), "F1: upstream integrity");
            assertEquals(PushResult.SUCCESS, server.push(LONG_CREDENTIAL, big), "F1: downstream push");
            assertTrue(downLatch.await(20, TimeUnit.SECONDS), "F1: 1 MiB downstream");
            assertArrayEquals(big, down.get(), "F1: downstream integrity");
        } finally {
            client.shutdown();
            server.closeAll();
        }
    }

    // ---------------------------------------------------------------- F3 (AIO)

    @Test
    void shutdownSendsDisconnect03() throws Exception {
        int port = RmttEndToEndIT.freeTcpPort();
        RmttServer server = new RmttServerBuilder()
                .port(port)
                .authenticator(AuthResult::allow)
                .messageHandler((deviceId, payload) -> {
                })
                .build();
        server.start();
        try (Socket socket = new Socket("127.0.0.1", port)) {
            socket.setSoTimeout(5000);
            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();
            rawConnect(out, "aio-shutdown", 30);
            RmttEndToEndIT.WireFrame connack = readFrame(in);
            assertEquals(2, connack.type, "CONNACK expected");
            // CONNACK is written before the device is registered in the route table (so a
            // downstream push cannot beat the handshake); the shutdown notification walks that
            // table, so wait until registration completes before shutting down
            waitFor(() -> server.isOnline("aio-shutdown"), "device to register");
            server.closeAll();
            RmttEndToEndIT.WireFrame bye = readFrame(in);
            assertNotNull(bye, "DISCONNECT expected on shutdown, got EOF");
            assertEquals(14, bye.type, "DISCONNECT expected on shutdown");
            assertEquals(0x03, bye.body[0] & 0xFF, "SERVER_SHUTDOWN code expected");
        } finally {
            server.closeAll();
        }
    }

    // ---------------------------------------------------------------- F5 (AIO client FIN)

    @Test
    void clientReconnectsImmediatelyAfterPeerReset() throws Exception {
        // scripted server: first session gets CONNACK then an abortive close (RST, no DISCONNECT),
        // second session stays open - the client must reconnect well before any keepalive timeout
        ServerSocket fake = new ServerSocket(0);
        AtomicInteger accepts = new AtomicInteger();
        List<Socket> held = new ArrayList<>();
        Thread server = new Thread(() -> {
            while (!fake.isClosed()) {
                try {
                    Socket s = fake.accept();
                    held.add(s);
                    InputStream in = s.getInputStream();
                    readFrame(in); // CONNECT
                    OutputStream out = s.getOutputStream();
                    out.write(new byte[]{0x20, 3, 0, 0, 30}); // CONNACK accepted, server_kp=30
                    out.flush();
                    int n = accepts.incrementAndGet();
                    if (n == 1) {
                        Thread.sleep(100);
                        s.setSoLinger(true, 0); // abortive close: RST
                        s.close();
                    } else {
                        Thread.sleep(30_000); // hold the second session open
                    }
                } catch (Exception ignored) {
                    // loop until the server socket is closed
                }
            }
        }, "fake-rmtt");
        server.setDaemon(true);
        server.start();

        RmttClient client = new RmttClientBuilder()
                .host("127.0.0.1")
                .port(fake.getLocalPort())
                .credential("aio-fin")
                .keepAliveSeconds(30) // default keepalive: the old code would wait ~45s
                .connectTimeoutMillis(3000)
                .reconnectBaseMillis(100)
                .reconnectJitter(0f)
                .build();
        try {
            assertEquals(ConnectReturnCode.CONNECT_ACCEPTED, client.connect(), "initial handshake");
            long start = System.currentTimeMillis();
            while (accepts.get() < 2 && System.currentTimeMillis() - start < 5000) {
                Thread.sleep(20);
            }
            long elapsed = System.currentTimeMillis() - start;
            assertTrue(accepts.get() >= 2 && elapsed < 3000,
                    "F5: client must reconnect right after the peer FIN/RST (keepalive timeout would be ~45s), took "
                            + elapsed + "ms");
        } finally {
            client.shutdown();
            fake.close();
            for (Socket s : held) {
                try {
                    s.close();
                } catch (Exception ignored) {
                    // best-effort cleanup
                }
            }
        }
    }

    // ---------------------------------------------------------------- F10 (AIO keepalive reaper)

    @Test
    void keepaliveReaperStaysWithinOneHalfOfServerKp() throws Exception {
        int port = RmttEndToEndIT.freeTcpPort();
        RmttServer server = new RmttServerBuilder()
                .port(port)
                .keepalivePolicy(new ServerKeepalivePolicy(1, 600, 1, false))
                .authenticator(AuthResult::allow)
                .messageHandler((deviceId, payload) -> {
                })
                .build();
        server.start();
        try (Socket socket = new Socket("127.0.0.1", port)) {
            socket.setSoTimeout(6000);
            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();
            rawConnect(out, "aio-kp", 1);
            RmttEndToEndIT.WireFrame connack = readFrame(in);
            assertEquals(2, connack.type, "CONNACK expected");
            long silenceStart = System.currentTimeMillis();
            RmttEndToEndIT.WireFrame frame;
            do {
                frame = readFrame(in);
            } while (frame != null && frame.type != 14);
            long elapsed = System.currentTimeMillis() - silenceStart;
            assertTrue(frame != null && (frame.body[0] & 0xFF) == 0x05,
                    "F10: keepalive timeout DISCONNECT(0x05) expected");
            // 1.5x server_kp = 1500ms; the former 1s check granularity overshot to 2.5s
            assertTrue(elapsed >= 1300 && elapsed <= 2800,
                    "F10: reap must land near 1.5x server_kp (1500ms), took " + elapsed + "ms");
        } finally {
            server.closeAll();
        }
    }

    // ---------------------------------------------------------------- TLS helpers

    private static KeyPair keyPair() throws Exception {
        KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
        kpg.initialize(2048);
        return kpg.generateKeyPair();
    }

    private static X509Certificate selfSigned(KeyPair kp) throws Exception {
        long now = System.currentTimeMillis();
        X500Name subject = new X500Name("CN=rmtt-aio-it");
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

    /** Returns {server SSLContext, client SSLContext} built from a self-signed certificate. */
    private static SSLContext[] sslContexts(KeyPair kp, X509Certificate cert) throws Exception {
        char[] password = "changeit".toCharArray();
        KeyStore ks = KeyStore.getInstance("PKCS12");
        ks.load(null, null);
        ks.setKeyEntry("key", kp.getPrivate(), password, new java.security.cert.Certificate[]{cert});
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, password);
        SSLContext serverCtx = SSLContext.getInstance("TLS");
        serverCtx.init(kmf.getKeyManagers(), null, null);
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ks);
        SSLContext clientCtx = SSLContext.getInstance("TLS");
        clientCtx.init(null, tmf.getTrustManagers(), null);
        return new SSLContext[]{serverCtx, clientCtx};
    }
}
