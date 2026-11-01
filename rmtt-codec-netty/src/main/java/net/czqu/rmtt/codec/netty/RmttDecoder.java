package net.czqu.rmtt.codec.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import net.czqu.rmtt.protocol.FixedHeader;
import net.czqu.rmtt.protocol.RmttByteReader.Underflow;
import net.czqu.rmtt.protocol.RmttMessage;
import net.czqu.rmtt.protocol.RmttProtocol;
import net.czqu.rmtt.protocol.RmttWireCodec;
import net.czqu.rmtt.protocol.RmttWireCodec.ProtocolViolation;
import net.czqu.rmtt.logging.InternalLogger;
import net.czqu.rmtt.logging.InternalLoggerFactory;

import java.util.List;

/**
 * Netty inbound codec: waits for a complete RMTT frame in the incoming {@link ByteBuf}, then decodes
 * it via the shared {@link RmttWireCodec} reading straight from the buffer (no intermediate copy).
 */
public class RmttDecoder extends ByteToMessageDecoder {

    private static final InternalLogger LOG = InternalLoggerFactory.getLogger(RmttDecoder.class);

    private final int maxBytesInMessage;
    /**
     * Set once an oversized frame is seen: the stream is no longer trustworthy, so the remaining
     * bytes are dropped instead of being re-parsed as RMTT frames (which produced misleading
     * "unsupported packet type" logs during channel close).
     */
    private boolean discarding;

    /**
     * Create with the default size cap.
     */
    public RmttDecoder() {
        this(RmttProtocol.DEFAULT_MAX_BYTES_IN_MESSAGE);
    }

    /**
     * Create with a per-message size cap.
     *
     * @param maxBytesInMessage cap on the total size of a single message
     */
    public RmttDecoder(int maxBytesInMessage) {
        this.maxBytesInMessage = maxBytesInMessage;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (discarding) {
            in.skipBytes(in.readableBytes());
            return;
        }
        if (in.readableBytes() < 1) {
            return;
        }
        in.markReaderIndex();
        ByteBufRmttByteReader reader = new ByteBufRmttByteReader(in);
        FixedHeader header;
        try {
            header = RmttWireCodec.decodeHeader(reader);
        } catch (Underflow u) {
            in.resetReaderIndex();
            return;
        } catch (ProtocolViolation pv) {
            // malformed header (e.g. a remaining-length varint over 4 bytes): report once and
            // stop parsing, exactly like the oversized-frame path below
            discarding = true;
            in.skipBytes(in.readableBytes());
            throw pv;
        }
        if (header.remainingLength() > maxBytesInMessage) {
            // the declared frame cannot be honoured: report once, then drop every remaining byte
            discarding = true;
            in.skipBytes(in.readableBytes());
            throw new ProtocolViolation("message too large: " + header.remainingLength()
                    + " bytes (max " + maxBytesInMessage + ")");
        }
        if (in.readableBytes() < header.remainingLength()) {
            in.resetReaderIndex();
            return;
        }
        RmttMessage msg;
        try {
            msg = RmttWireCodec.decodeBody(header, reader);
        } catch (ProtocolViolation pv) {
            LOG.warn("protocol violation on decode: {}", pv.getMessage());
            throw pv;
        }
        if (msg != null) {
            LOG.trace("decoded frame type={} remainingLength={}", msg.fixedHeader().messageType(),
                    header.remainingLength());
            out.add(msg);
        }
    }
}