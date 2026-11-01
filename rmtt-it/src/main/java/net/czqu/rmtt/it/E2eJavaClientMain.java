package net.czqu.rmtt.it;

import net.czqu.rmtt.client.netty.RmttClient;
import net.czqu.rmtt.client.netty.RmttClientBuilder;
import net.czqu.rmtt.protocol.ConnectReturnCode;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Standalone Java client for the reverse cross-language end-to-end check: it
 * connects to the rmtt-go v1.0.2 server (rmtt-it/go-server), pushes a message
 * upstream and verifies the echo that the Go server sends back downstream.
 *
 * <p>Prints {@code JAVA_CLIENT_CONNECTED} / {@code JAVA_CLIENT_ECHO_OK} /
 * {@code JAVA_CLIENT_E2E_PASS} on success; exits non-zero on any failure.
 */
public final class E2eJavaClientMain {

    /** Default server host when not supplied. */
    public static final String DEFAULT_HOST = "127.0.0.1";

    /** Default server port when not supplied. */
    public static final int DEFAULT_PORT = 18998;

    private E2eJavaClientMain() {
    }

    /**
     * Connect to the Go server, push upstream and verify the echo.
     *
     * @param args optional arguments: {@code [host] [port]}
     * @throws Exception when the run fails
     */
    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : DEFAULT_HOST;
        int port = args.length > 1 ? Integer.parseInt(args[1]) : DEFAULT_PORT;

        CountDownLatch down = new CountDownLatch(1);
        AtomicReference<byte[]> got = new AtomicReference<>();
        RmttClient c = new RmttClientBuilder()
                .host(host)
                .port(port)
                .credential("java-e2e")
                .keepAliveSeconds(30)
                .connectTimeoutMillis(5000)
                .pushHandler(payload -> {
                    got.set(payload);
                    down.countDown();
                })
                .build();
        if (c.connect() != ConnectReturnCode.CONNECT_ACCEPTED) {
            System.err.println("connect rejected by Go server");
            System.exit(1);
        }
        System.out.println("JAVA_CLIENT_CONNECTED");
        c.push("ping-from-java".getBytes(StandardCharsets.UTF_8)).awaitUninterruptibly(5000);
        if (!down.await(5, TimeUnit.SECONDS)) {
            System.err.println("timeout waiting for Go server echo");
            System.exit(1);
        }
        String echo = new String(got.get(), StandardCharsets.UTF_8);
        if (!"echo:ping-from-java".equals(echo)) {
            System.err.println("unexpected echo: " + echo);
            System.exit(1);
        }
        System.out.println("JAVA_CLIENT_ECHO_OK " + echo);
        c.disconnect();
        c.shutdown();

        // Phase 2: regression guards for the blackbox report - a JWT-like credential well above
        // the old 23-byte cap (F2) and a 1 MiB frame above the old 8 KiB cap (F1). The Go server
        // must accept both like the Java implementation now does.
        char[] raw = new char[300];
        java.util.Arrays.fill(raw, 'j');
        CountDownLatch down2 = new CountDownLatch(1);
        AtomicReference<byte[]> got2 = new AtomicReference<>();
        RmttClient c2 = new RmttClientBuilder()
                .host(host)
                .port(port)
                .credential(new String(raw))
                .keepAliveSeconds(30)
                .connectTimeoutMillis(5000)
                .pushHandler(payload -> {
                    got2.set(payload);
                    down2.countDown();
                })
                .build();
        if (c2.connect() != ConnectReturnCode.CONNECT_ACCEPTED) {
            System.err.println("long-credential connect rejected by Go server");
            System.exit(1);
        }
        System.out.println("JAVA_LONG_CRED_OK (300-byte credential accepted)");
        byte[] big = new byte[1024 * 1024];
        for (int i = 0; i < big.length; i++) {
            big[i] = (byte) ("0123456789abcdef".charAt(i % 16));
        }
        c2.push(big).awaitUninterruptibly(10000);
        if (!down2.await(10, TimeUnit.SECONDS)) {
            System.err.println("timeout waiting for 1 MiB echo from Go server");
            System.exit(1);
        }
        byte[] gotBytes = got2.get();
        if (gotBytes.length != big.length + 5) {
            System.err.println("1 MiB echo size mismatch: " + gotBytes.length);
            System.exit(1);
        }
        for (int i = 0; i < big.length; i++) {
            if (gotBytes[i + 5] != big[i]) {
                System.err.println("1 MiB echo content mismatch at " + i);
                System.exit(1);
            }
        }
        System.out.println("JAVA_LARGE_ECHO_OK (1 MiB round trip)");
        c2.disconnect();
        c2.shutdown();
        System.out.println("JAVA_CLIENT_E2E_PASS");
    }
}
