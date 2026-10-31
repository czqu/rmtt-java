package net.czqu.rmtt.server.netty;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.quic.QuicTokenHandler;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.net.InetSocketAddress;
import java.security.MessageDigest;
import java.security.SecureRandom;

/**
 * QUIC retry-token handler with real address validation: the token binds the verified
 * {@code (ip, port, timestamp)} tuple with an HMAC under a per-process random secret, so tokens
 * cannot be forged for arbitrary spoofed source addresses. A token embeds the original DCID at
 * the tail (returned by {@link #validateToken} as the prefix length).
 *
 * <p>Token layout: {@code ts(8) | ipLen(1) | ip | port(2) | mac(32) | originalDcid}.</p>
 */
final class HmacQuicTokenHandler implements QuicTokenHandler {

    private static final int TS_LEN = 8;
    private static final int MAC_LEN = 32;
    /** Tokens older than this are rejected (limits replay windows). */
    private static final long TOKEN_TTL_MILLIS = 10 * 60 * 1000L;

    private final byte[] secret = new byte[32];

    HmacQuicTokenHandler() {
        new SecureRandom().nextBytes(secret);
    }

    @Override
    public boolean writeToken(ByteBuf out, ByteBuf dcid, InetSocketAddress address) {
        byte[] ip = address.getAddress().getAddress();
        long now = System.currentTimeMillis();
        byte[] prefix = new byte[TS_LEN + 1 + ip.length + 2 + MAC_LEN];
        int i = 0;
        for (int s = 56; s >= 0; s -= 8) {
            prefix[i++] = (byte) (now >>> s);
        }
        prefix[i++] = (byte) ip.length;
        System.arraycopy(ip, 0, prefix, i, ip.length);
        i += ip.length;
        prefix[i++] = (byte) (address.getPort() >>> 8);
        prefix[i++] = (byte) address.getPort();
        byte[] mac = hmac(prefix, 0, i);
        System.arraycopy(mac, 0, prefix, i, MAC_LEN);
        out.writeBytes(prefix);
        out.writeBytes(dcid, dcid.readerIndex(), dcid.readableBytes());
        return true;
    }

    @Override
    public int validateToken(ByteBuf token, InetSocketAddress address) {
        byte[] ip = address.getAddress().getAddress();
        int prefixLen = TS_LEN + 1 + ip.length + 2 + MAC_LEN;
        if (token.readableBytes() < prefixLen) {
            return -1;
        }
        byte[] t = new byte[prefixLen];
        token.getBytes(token.readerIndex(), t);
        if ((t[TS_LEN] & 0xFF) != ip.length) {
            return -1;
        }
        for (int i = 0; i < ip.length; i++) {
            if (t[TS_LEN + 1 + i] != ip[i]) {
                return -1;
            }
        }
        int port = ((t[TS_LEN + 1 + ip.length] & 0xFF) << 8) | (t[TS_LEN + 1 + ip.length + 1] & 0xFF);
        if (port != address.getPort()) {
            return -1;
        }
        byte[] expected = hmac(t, 0, prefixLen - MAC_LEN);
        if (!MessageDigest.isEqual(expected, copyOfRange(t, prefixLen - MAC_LEN, prefixLen))) {
            return -1;
        }
        long ts = 0;
        for (int i = 0; i < TS_LEN; i++) {
            ts = (ts << 8) | (t[i] & 0xFF);
        }
        long now = System.currentTimeMillis();
        if (ts > now + TOKEN_TTL_MILLIS || now - ts > TOKEN_TTL_MILLIS) {
            return -1;
        }
        return prefixLen;
    }

    @Override
    public int maxTokenLength() {
        // prefix (max ip 16 bytes) + original DCID (<= 255 bytes)
        return TS_LEN + 1 + 16 + 2 + MAC_LEN + 255;
    }

    private byte[] hmac(byte[] data, int off, int len) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret, "HmacSHA256"));
            mac.update(data, off, len);
            return mac.doFinal();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static byte[] copyOfRange(byte[] src, int from, int to) {
        byte[] out = new byte[to - from];
        System.arraycopy(src, from, out, 0, out.length);
        return out;
    }
}
