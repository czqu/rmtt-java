package net.czqu.rmtt.it;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * Standalone raw-wire RMTT probe used by CI to verify protocol-level behaviour of any RMTT server
 * without depending on a client library (the same philosophy as the blackbox suite's wire.py).
 *
 * <p>Modes:</p>
 * <ul>
 *   <li>{@code shutdown-code <host> <port>} - connect, then wait for DISCONNECT(0x03):
 *       the test driver is expected to stop the server gracefully after this probe connects.</li>
 *   <li>{@code oversize <host> <port>} - connect, declare a frame above the server's maximum
 *       (16 MiB + 1) and expect DISCONNECT(0x04) followed by a clean close.</li>
 * </ul>
 *
 * <p>Prints {@code WIRE_<MODE>_OK} and exits 0 on success; exits non-zero on any failure.</p>
 */
public final class WireProbeMain {

    private static final int DISCONNECT = 14;
    private static final int CONNACK = 2;
    private static final int PINGRESP = 6;

    private WireProbeMain() {
    }

    /**
     * Run one probe mode.
     *
     * @param args {@code <mode> <host> <port>}
     * @throws Exception when the probe fails
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("usage: WireProbeMain <shutdown-code|oversize> <host> <port>");
            System.exit(2);
        }
        String mode = args[0];
        String host = args[1];
        int port = Integer.parseInt(args[2]);
        if ("shutdown-code".equals(mode)) {
            shutdownCode(host, port);
        } else if ("oversize".equals(mode)) {
            oversize(host, port);
        } else {
            System.err.println("unknown mode: " + mode);
            System.exit(2);
        }
    }

    private static void shutdownCode(String host, int port) throws Exception {
        try (Socket socket = new Socket(host, port)) {
            socket.setSoTimeout(15000);
            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();
            connect(out, "wire-probe-shutdown", 30);
            Frame connack = readFrame(in);
            if (connack == null || connack.type != CONNACK) {
                fail("no CONNACK");
            }
            // PINGREQ -> PINGRESP round trip: the server processes inbound messages in order, so
            // the PINGRESP proves the CONNECT handling (including route-table registration)
            // completed before the driver shuts the server down (PINGREQ = type 5 -> 0x50)
            out.write(new byte[]{0x50, 0x00});
            out.flush();
            Frame pong = readFrame(in);
            if (pong == null || pong.type != PINGRESP) {
                fail("no PINGRESP after PINGREQ (got " + describe(pong) + ")");
            }
            System.out.println("WIRE_PROBE_CONNECTED");
            System.out.flush();
            // the driver stops the server gracefully after seeing WIRE_PROBE_CONNECTED
            Frame bye = readFrame(in);
            if (bye == null || bye.type != DISCONNECT) {
                fail("no DISCONNECT before close (got " + describe(bye) + ")");
            }
            if ((bye.body[0] & 0xFF) != 0x03) {
                fail("expected DISCONNECT code 0x03 (SERVER_SHUTDOWN), got 0x"
                        + Integer.toHexString(bye.body[0] & 0xFF));
            }
            System.out.println("WIRE_SHUTDOWN_CODE_OK code=0x03");
        }
    }

    private static void oversize(String host, int port) throws Exception {
        try (Socket socket = new Socket(host, port)) {
            socket.setSoTimeout(5000);
            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();
            connect(out, "wire-probe-oversize", 30);
            Frame connack = readFrame(in);
            if (connack == null || connack.type != CONNACK) {
                fail("no CONNACK");
            }
            // declare 16 MiB + 1 (above the aligned 16 MiB maximum) and send only the header
            ByteArrayOutputStream header = new ByteArrayOutputStream();
            header.write(0x30);
            writeVarint(header, 16 * 1024 * 1024 + 1);
            out.write(header.toByteArray());
            out.flush();

            Frame reply = readFrame(in);
            if (reply == null || reply.type != DISCONNECT) {
                fail("expected DISCONNECT, got " + describe(reply));
            }
            if ((reply.body[0] & 0xFF) != 0x04) {
                fail("expected DISCONNECT code 0x04 (PROTOCOL_VIOLATION), got 0x"
                        + Integer.toHexString(reply.body[0] & 0xFF));
            }
            // exactly one reply: the decoder must not parse the residual bytes again (F11)
            Frame extra = readFrame(in);
            if (extra != null) {
                fail("unexpected follow-up frame after the violation: " + describe(extra));
            }
            System.out.println("WIRE_OVERSIZE_OK code=0x04");
        }
    }

    private static void connect(OutputStream out, String credential, int keepalive) throws Exception {
        byte[] c = credential.getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        body.write(0x63);
        body.write(0x7a);
        body.write(0x71);
        body.write(0x75);
        body.write(1); // version
        body.write(0); // reserve
        body.write((keepalive >> 8) & 0xFF);
        body.write(keepalive & 0xFF);
        body.write((c.length >> 8) & 0xFF);
        body.write(c.length & 0xFF);
        body.write(c);
        ByteArrayOutputStream frame = new ByteArrayOutputStream();
        frame.write(0x10);
        writeVarint(frame, body.size());
        frame.write(body.toByteArray());
        out.write(frame.toByteArray());
        out.flush();
    }

    private static void writeVarint(ByteArrayOutputStream out, int value) {
        int v = value;
        while (true) {
            int digit = v % 128;
            v /= 128;
            if (v > 0) {
                digit |= 0x80;
            }
            out.write(digit);
            if (v == 0) {
                return;
            }
        }
    }

    private static Frame readFrame(InputStream in) throws Exception {
        int first = in.read();
        if (first < 0) {
            return null;
        }
        int mult = 1;
        int remaining = 0;
        for (int i = 0; i < 4; i++) {
            int digit = in.read();
            if (digit < 0) {
                throw new EOFException("eof in remaining length");
            }
            remaining += (digit & 0x7F) * mult;
            if ((digit & 0x80) == 0) {
                break;
            }
            mult *= 128;
        }
        byte[] body = new byte[Math.max(0, remaining)];
        int off = 0;
        while (off < body.length) {
            int n = in.read(body, off, body.length - off);
            if (n < 0) {
                throw new EOFException("eof in body");
            }
            off += n;
        }
        return new Frame((first >> 4) & 0x0F, body);
    }

    private static String describe(Frame f) {
        return f == null ? "eof" : "type=" + f.type + " body=" + f.body.length;
    }

    private static void fail(String reason) {
        System.err.println("WIRE_PROBE_FAIL: " + reason);
        System.exit(1);
    }

    private static final class Frame {
        final int type;
        final byte[] body;

        Frame(int type, byte[] body) {
            this.type = type;
            this.body = body;
        }
    }
}
