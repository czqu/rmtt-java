package net.czqu.rmtt.transport.aio;

import net.czqu.rmtt.protocol.RmttProtocol;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.ArrayDeque;

/**
 * Minimal RFC6455 WebSocket binary-frame codec (one RMTT packet = one binary frame).
 * Handles masking, fragmentation (continuation frames), and control frames (PING is answered
 * with a queued PONG via {@link Decoder#pollPong()}). Inbound frame size is capped.
 */
public final class WsFrameCodec {

    /** Continuation-frame opcode. */
    public static final byte OP_CONT = 0x0;
    /** Text-frame opcode. */
    public static final byte OP_TEXT = 0x1;
    /** Binary-frame opcode. */
    public static final byte OP_BINARY = 0x2;
    /** Close-frame opcode. */
    public static final byte OP_CLOSE = 0x8;
    /** Ping-frame opcode. */
    public static final byte OP_PING = 0x9;
    /** Pong-frame opcode. */
    public static final byte OP_PONG = 0xA;

    /** Mask keys and handshake keys must not be predictable. */
    private static final SecureRandom RANDOM = new SecureRandom();

    /**
     * Wire-level WebSocket framing violation (bad length, bad fragmentation, oversized frame).
     */
    public static final class Violation extends RuntimeException {
        /**
         * Construct with a description of the violation.
         *
         * @param message the violation description
         */
        public Violation(String message) {
            super(message);
        }
    }

    private WsFrameCodec() {
    }

    /**
     * Wrap an RMTT frame into a single binary WebSocket frame. Client frames are masked.
     *
     * @param payload the RMTT frame bytes
     * @param masked  true to mask the payload (client side)
     * @return the complete WebSocket binary frame
     */
    public static byte[] encodeFrame(byte[] payload, boolean masked) {
        return encode(OP_BINARY, payload, masked);
    }

    /**
     * Wrap a control payload (e.g. PONG echo) into a WebSocket control frame.
     *
     * @param opcode  one of {@link #OP_CLOSE}, {@link #OP_PING}, {@link #OP_PONG}
     * @param payload the control payload (at most 125 bytes)
     * @param masked  true to mask the payload (client side)
     * @return the complete WebSocket control frame
     */
    public static byte[] encodeControlFrame(byte opcode, byte[] payload, boolean masked) {
        return encode(opcode, payload, masked);
    }

    private static byte[] encode(byte opcode, byte[] payload, boolean masked) {
        int headerLen = 2;
        int len = payload.length;
        if (len >= 126 && len <= 0xFFFF) {
            headerLen += 2;
        } else if (len > 0xFFFF) {
            headerLen += 8;
        }
        if (masked) {
            headerLen += 4;
        }
        byte[] frame = new byte[headerLen + len];
        frame[0] = (byte) (0x80 | opcode);
        if (len < 126) {
            frame[1] = (byte) (len | (masked ? 0x80 : 0));
        } else if (len <= 0xFFFF) {
            frame[1] = (byte) (126 | (masked ? 0x80 : 0));
            frame[2] = (byte) (len >>> 8);
            frame[3] = (byte) len;
        } else {
            frame[1] = (byte) (127 | (masked ? 0x80 : 0));
            long l = len;
            for (int i = 0; i < 8; i++) {
                frame[2 + i] = (byte) (l >>> (56 - 8 * i));
            }
        }
        int off = headerLen - (masked ? 4 : 0);
        if (masked) {
            byte[] key = new byte[4];
            RANDOM.nextBytes(key);
            System.arraycopy(key, 0, frame, off, 4);
            for (int i = 0; i < len; i++) {
                frame[off + 4 + i] = (byte) (payload[i] ^ key[i & 3]);
            }
            return frame;
        }
        System.arraycopy(payload, 0, frame, off, len);
        return frame;
    }

    /** Inbound frame parser state (may need more bytes). */
    public static final class Decoder {
        private byte[] buf = new byte[512];
        private int count;
        private int pos;
        private final int maxFrameSize;
        /** Accumulated fragments of the message currently being reassembled. */
        private byte[] fragBuf = new byte[0];
        private int fragLen;
        private final ArrayDeque<byte[]> pendingPongs = new ArrayDeque<>();
        private boolean closeReceived;
        private byte[] closeReason;

        /**
         * Create an empty inbound decoder with the default frame size cap.
         */
        public Decoder() {
            this(RmttProtocol.DEFAULT_MAX_BYTES_IN_MESSAGE + 16);
        }

        /**
         * Create an empty inbound decoder with an explicit frame size cap.
         *
         * @param maxFrameSize maximum accepted WebSocket message payload size
         */
        public Decoder(int maxFrameSize) {
            this.maxFrameSize = maxFrameSize;
        }

        /**
         * Append received bytes.
         *
         * @param data the received bytes
         * @param len  number of valid bytes in {@code data}
         */
        public void feed(byte[] data, int len) {
            if (count + len > buf.length) {
                byte[] nb = new byte[Math.max(buf.length << 1, count + len)];
                System.arraycopy(buf, 0, nb, 0, count);
                buf = nb;
            }
            System.arraycopy(data, 0, buf, count, len);
            count += len;
        }

        /**
         * Returns the payload of the next complete message (fragmentation reassembled), or null if
         * more bytes are needed. PING frames queue a PONG reply retrievable via {@link #pollPong()}.
         *
         * @return the next unfragmented message payload, or null when incomplete
         */
        public byte[] decodeOne() {
            while (count - pos > 0) {
                if (count - pos < 2) {
                    return null;
                }
                int p = pos;
                int b0 = buf[p] & 0xFF;
                boolean fin = (b0 & 0x80) != 0;
                int opcode = b0 & 0x0F;
                int p1 = p + 1;
                boolean masked = (buf[p1] & 0x80) != 0;
                long len = buf[p1] & 0x7F;
                int header = 2;
                if (len == 126) {
                    if (count - pos < 4) {
                        return null;
                    }
                    len = ((buf[p1 + 1] & 0xFF) << 8) | (buf[p1 + 2] & 0xFF);
                    header = 4;
                } else if (len == 127) {
                    if (count - pos < 10) {
                        return null;
                    }
                    // RFC6455: the most significant bit of a 64-bit length MUST be 0
                    if ((buf[p1 + 1] & 0x80) != 0) {
                        throw new Violation("invalid 64-bit payload length (MSB set)");
                    }
                    len = 0;
                    for (int i = 0; i < 8; i++) {
                        len = (len << 8) | (buf[p1 + 1 + i] & 0xFF);
                    }
                    header = 10;
                }
                if (len > maxFrameSize) {
                    throw new Violation("ws frame too large: " + len + " bytes (max " + maxFrameSize + ")");
                }
                boolean control = opcode >= 0x8;
                if (control && (!fin || len > 125)) {
                    throw new Violation("invalid control frame (fin=" + fin + " len=" + len + ")");
                }
                if (masked && count - pos < header + 4) {
                    return null;
                }
                int maskStart = p + header;
                byte[] mask = null;
                if (masked) {
                    mask = new byte[]{buf[maskStart], buf[maskStart + 1], buf[maskStart + 2], buf[maskStart + 3]};
                    header += 4;
                }
                if (count - pos < header + len) {
                    return null;
                }
                int start = p + header;
                byte[] payload = new byte[(int) len];
                if (masked) {
                    for (int i = 0; i < len; i++) {
                        payload[i] = (byte) (buf[start + i] ^ mask[i & 3]);
                    }
                } else {
                    System.arraycopy(buf, start, payload, 0, (int) len);
                }
                pos = p + header + (int) len;
                switch (opcode) {
                    case OP_PING:
                        pendingPongs.add(payload);
                        continue;
                    case OP_PONG:
                        continue;
                    case OP_CLOSE:
                        closeReceived = true;
                        closeReason = payload;
                        continue;
                    case OP_BINARY:
                    case OP_TEXT:
                    case OP_CONT:
                        byte[] message = collectFragment(opcode, payload, fin);
                        if (message != null) {
                            return message;
                        }
                        continue;
                    default:
                        throw new Violation("unsupported opcode " + opcode);
                }
            }
            pos = 0;
            count = 0;
            return null;
        }

        private byte[] collectFragment(int opcode, byte[] payload, boolean fin) {
            if (opcode == OP_CONT) {
                if (fragLen == 0) {
                    throw new Violation("continuation frame without a started message");
                }
            } else {
                if (fragLen != 0) {
                    throw new Violation("new data frame interleaved with unfinished fragmented message");
                }
                if (fin) {
                    return payload; // unfragmented fast path
                }
            }
            if (fragLen + payload.length > maxFrameSize) {
                throw new Violation("fragmented message exceeds max frame size " + maxFrameSize);
            }
            if (fragLen + payload.length > fragBuf.length) {
                byte[] nb = new byte[Math.max(fragBuf.length << 1, fragLen + payload.length)];
                System.arraycopy(fragBuf, 0, nb, 0, fragLen);
                fragBuf = nb;
            }
            System.arraycopy(payload, 0, fragBuf, fragLen, payload.length);
            fragLen += payload.length;
            if (!fin) {
                return null;
            }
            byte[] message = new byte[fragLen];
            System.arraycopy(fragBuf, 0, message, 0, fragLen);
            fragLen = 0;
            return message;
        }

        /**
         * Poll a queued PONG reply produced by a received PING frame.
         *
         * @return the PONG payload to echo back, or null when none is pending
         */
        public byte[] pollPong() {
            return pendingPongs.poll();
        }

        /**
         * Whether a CLOSE frame has been received.
         *
         * @return true when a CLOSE frame has been received
         */
        public boolean isCloseReceived() {
            return closeReceived;
        }

        /**
         * The CLOSE frame payload (reason code + optional text).
         *
         * @return the CLOSE frame payload (reason), or null when no CLOSE was received
         */
        public byte[] closeReason() {
            return closeReason;
        }
    }

    /**
     * Build a server HTTP/1.1 101 Switching Protocols response for a handshake key.
     *
     * @param secWebSocketKey the client's Sec-WebSocket-Key header value
     * @return the full HTTP response bytes
     */
    public static byte[] buildHandshakeResponse(String secWebSocketKey) {
        String accept = java.util.Base64.getEncoder().encodeToString(
                sha1((secWebSocketKey + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11")
                        .getBytes(StandardCharsets.UTF_8)));
        return ("HTTP/1.1 101 Switching Protocols\r\n"
                + "Upgrade: websocket\r\n"
                + "Connection: Upgrade\r\n"
                + "Sec-WebSocket-Accept: " + accept + "\r\n"
                + "Sec-WebSocket-Protocol: rmtt\r\n\r\n").getBytes(StandardCharsets.US_ASCII);
    }

    /**
     * Build a client HTTP/1.1 GET upgrade request.
     *
     * @param host the Host header hostname
     * @param port the Host header port
     * @param path the request path, "/rmtt" when null or empty
     * @return the full HTTP request bytes
     */
    public static byte[] buildHandshakeRequest(String host, int port, String path) {
        byte[] keyBytes = new byte[16];
        RANDOM.nextBytes(keyBytes);
        String key = java.util.Base64.getEncoder().encodeToString(keyBytes);
        return ("GET " + (path == null || path.isEmpty() ? "/rmtt" : path) + " HTTP/1.1\r\n"
                + "Host: " + host + ":" + port + "\r\n"
                + "Upgrade: websocket\r\n"
                + "Connection: Upgrade\r\n"
                + "Sec-WebSocket-Key: " + key + "\r\n"
                + "Sec-WebSocket-Version: 13\r\n"
                + "Sec-WebSocket-Protocol: rmtt\r\n\r\n").getBytes(StandardCharsets.US_ASCII);
    }

    private static byte[] sha1(byte[] data) {
        try {
            java.security.MessageDigest md = java.security.MessageDigest.getInstance("SHA-1");
            return md.digest(data);
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
