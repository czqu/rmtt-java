package net.czqu.rmtt.codec.aio;

import net.czqu.rmtt.protocol.RmttByteReader.Underflow;
import net.czqu.rmtt.protocol.RmttCodecUtil;
import net.czqu.rmtt.protocol.RmttMessage;
import net.czqu.rmtt.protocol.RmttProtocol;
import net.czqu.rmtt.protocol.RmttWireCodec;
import net.czqu.rmtt.protocol.RmttWireCodec.ProtocolViolation;
import net.czqu.rmtt.logging.InternalLogger;
import net.czqu.rmtt.logging.InternalLoggerFactory;

/**
 * Byte-accumulating RMTT frame decoder for the AIO read loop. Grows a single byte[] as channel
 * reads arrive, extracts complete frames via the shared {@link RmttWireCodec} and compacts it so
 * reallocation is amortised.
 */
public final class AioMessageDecoder {

    private static final InternalLogger LOG = InternalLoggerFactory.getLogger(AioMessageDecoder.class);

    private byte[] buf;
    private int count;
    private int pos;
    private final int maxMessageSize;

    /**
     * Create with the default size cap.
     */
    public AioMessageDecoder() {
        this(RmttProtocol.DEFAULT_MAX_BYTES_IN_MESSAGE);
    }

    /**
     * Create with an explicit per-message size cap.
     *
     * @param maxMessageSize cap on the total size of a single message
     */
    public AioMessageDecoder(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
        this.buf = new byte[256];
    }

    /**
     * Append received bytes.
     *
     * @param data the received bytes
     * @param len  number of valid bytes in {@code data}
     */
    public void feed(byte[] data, int len) {
        ensure(count + len);
        System.arraycopy(data, 0, buf, count, len);
        count += len;
    }

    /**
     * Try to decode the next complete frame from the buffer. Returns null when more bytes are
     * needed. On a framing/protocol error the accumulated frame bytes that were read are dropped.
     *
     * @return the decoded message, or null when the frame is still incomplete
     */
    public RmttMessage decodeOne() {
        if (count - pos <= 0) {
            reset();
            return null;
        }
        int headerByEnd = count - pos;
        int lengthBytes = 1;
        int remaining = -1;
        long value = 0;
        long mult = 1;
        // scan the varint remaining-length to find total frame size without consuming
        int p = pos + 1;
        int scanned = 1;
        boolean ok = false;
        while (p < count && scanned <= RmttCodecUtil.MAX_LENGTH_BYTES) {
            int digit = buf[p++] & 0xFF;
            value += (digit & 0x7F) * mult;
            mult *= 128;
            scanned++;
            if ((digit & 0x80) == 0) {
                remaining = (int) value;
                lengthBytes = scanned;
                ok = true;
                break;
            }
        }
        if (!ok) {
            if (scanned > RmttCodecUtil.MAX_LENGTH_BYTES) {
                dropFrame();
            }
            return null; // header still incomplete
        }
        if (remaining < 0 || remaining > maxMessageSize) {
            LOG.warn("protocol violation on decode: message too large: {} bytes", remaining);
            dropFrame();
            throw new ProtocolViolation("message too large: " + remaining + " bytes");
        }
        int total = lengthBytes + remaining;
        if (count - pos < total) {
            return null; // body still incomplete
        }
        int start = pos;
        int oldN = count;
        // decode from [start, start+total)
        count = start + total;
        pos = start;
        RmttMessage msg = RmttWireCodec.decode(new net.czqu.rmtt.protocol.ByteArrayRmttByteReader(buf, start, total));
        count = oldN;
        pos = start + total;
        if (msg != null) {
            LOG.trace("decoded frame type={} lengthBytes={} remainingLength={}",
                    msg.fixedHeader().messageType(), lengthBytes, remaining);
        }
        if (count - pos == 0) {
            reset();
        } else if (pos > buf.length / 2) {
            compact();
        }
        return msg;
    }

    private void dropFrame() {
        // skip a single byte of the offending frame and continue (best-effort resync)
        pos++;
        if (count - pos == 0) {
            reset();
        }
    }

    private void reset() {
        pos = 0;
        count = 0;
    }

    private void compact() {
        int len = count - pos;
        System.arraycopy(buf, pos, buf, 0, len);
        pos = 0;
        count = len;
    }

    private void ensure(int cap) {
        if (cap > buf.length) {
            int n = buf.length;
            while (n < cap) {
                n <<= 1;
            }
            byte[] nb = new byte[n];
            System.arraycopy(buf, pos, nb, 0, count - pos);
            count -= pos;
            pos = 0;
            buf = nb;
        }
    }
}