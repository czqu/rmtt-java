package net.czqu.rmtt.codec.netty.ws;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;

import java.util.List;

/**
 * Outbound converter: {@code ByteBuf} (already encoded by {@code RmttEncoder}) -&gt; binary
 * {@link BinaryWebSocketFrame}. One RMTT packet becomes exactly one WebSocket binary frame.
 */
public class ByteBufToWsFrame extends MessageToMessageEncoder<ByteBuf> {

    /**
     * Create the converter.
     */
    public ByteBufToWsFrame() {
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) {
        byte[] bytes = new byte[msg.readableBytes()];
        msg.readBytes(bytes);
        out.add(new BinaryWebSocketFrame(io.netty.buffer.Unpooled.wrappedBuffer(bytes)));
    }
}