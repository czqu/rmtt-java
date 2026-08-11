package net.czqu.rmtt.transport.aio;

import java.nio.charset.StandardCharsets;

/**
 * Minimal RFC6455 WebSocket binary-frame codec (one RMTT packet = one binary frame).
 * Handles masking, fragmentation (continuation frames), and control frames.
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

    private static final byte[] MASK_KEY = new byte[4];

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
        frame[0] = (byte) (0x80 | OP_BINARY);
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
            int key = (int) System.nanoTime();
            frame[off] = (byte) key;
            frame[off + 1] = (byte) (key >>> 8);
            frame[off + 2] = (byte) (key >>> 16);
            frame[off + 3] = (byte) (key >>> 24);
            for (int i = 0; i < len; i++) {
                frame[off + 4 + i] = (byte) (payload[i] ^ frame[off + (i & 3)]);
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

        /**
         * Create an empty inbound decoder.
         */
        public Decoder() {
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
         * Returns the payload of the next complete binary message, or null if more bytes needed.
         *
         * @return the next unfragmented binary message payload, or null when incomplete
         */
        public byte[] decodeOne() {
            while (count - pos > 0) {
                int p = pos;
                if (count - pos < 2) {
                    return null;
                }
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
                    len = 0;
                    for (int i = 0; i < 8; i++) {
                        len = (len << 8) | (buf[p1 + 1 + i] & 0xFF);
                    }
                    header = 10;
                }
                if (masked) {
                    if (count - pos < header + 4) {
                        return null;
                    }
                    int maskStart = p + header;
                    byte[] mask = new byte[4];
                    mask[0] = buf[maskStart];
                    mask[1] = buf[maskStart + 1];
                    mask[2] = buf[maskStart + 2];
                    mask[3] = buf[maskStart + 3];
                    header += 4;
                    int start = p + header;
                    if (count - pos < header + len) {
                        return null;
                    }
                    byte[] payload = new byte[(int) len];
                    for (int i = 0; i < len; i++) {
                        payload[i] = (byte) (buf[start + i] ^ mask[i & 3]);
                    }
                    pos = p + header + (int) len;
                    switch (opcode) {
                        case OP_PING:
                            continue;
                        case OP_PONG:
                        case OP_CLOSE:
                            continue;
                        case OP_BINARY:
                        case OP_TEXT:
                            return payload;
                        default:
                            continue;
                    }
                } else {
                    int start = p + header;
                    if (count - pos < header + len) {
                        return null;
                    }
                    byte[] payload = new byte[(int) len];
                    System.arraycopy(buf, start, payload, 0, (int) len);
                    pos = p + header + (int) len;
                    switch (opcode) {
                        case OP_PING:
                            continue;
                        case OP_PONG:
                        case OP_CLOSE:
                            continue;
                        case OP_BINARY:
                        case OP_TEXT:
                            return payload;
                        default:
                            continue;
                    }
                }
            }
            pos = 0;
            count = 0;
            return null;
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
     * @param path the upgrade request path, "/rmtt" when null or empty
     * @return the full HTTP request bytes
     */
    public static byte[] buildHandshakeRequest(String host, int port, String path) {
        byte[] keyBytes = new byte[16];
        new java.util.Random().nextBytes(keyBytes);
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