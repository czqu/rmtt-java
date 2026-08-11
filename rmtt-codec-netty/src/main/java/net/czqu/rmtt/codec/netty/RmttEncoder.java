package net.czqu.rmtt.codec.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import net.czqu.rmtt.protocol.RmttMessage;
import net.czqu.rmtt.protocol.RmttWireCodec;
import net.czqu.rmtt.logging.InternalLogger;
import net.czqu.rmtt.logging.InternalLoggerFactory;

import java.util.List;

/**
 * Netty outbound codec: encodes {@link RmttMessage} into a pooled {@link ByteBuf} in a single pass
 * (no intermediate byte[]), then hands it to the netty write path.
 */
@Sharable
public class RmttEncoder extends MessageToMessageEncoder<RmttMessage> {

    private static final InternalLogger LOG = InternalLoggerFactory.getLogger(RmttEncoder.class);

    /** Shared stateless encoder instance (safe for concurrent use). */
    public static final RmttEncoder INSTANCE = new RmttEncoder();

    private RmttEncoder() {
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, RmttMessage msg, List<Object> out) {
        LOG.trace("encoding frame type={}", msg.fixedHeader().messageType());
        ByteBuf buf = ctx.alloc().buffer(64);
        try {
            RmttWireCodec.encode(msg, new ByteBufRmttByteWriter(buf));
            out.add(buf);
        } catch (Exception e) {
            buf.release();
            throw e;
        }
    }
}