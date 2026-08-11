package net.czqu.rmtt.codec.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.DecoderException;
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
        }
        if (header.remainingLength() < 0 || header.remainingLength() > maxBytesInMessage) {
            throw new DecoderException("message too large: " + header.remainingLength() + " bytes");
        }        if (in.readableBytes() < header.remainingLength()) {
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