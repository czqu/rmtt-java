package net.czqu.rmtt.codec.netty.ws;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;

import java.util.List;

/**
 * Inbound converter: binary {@link WebSocketFrame} -> {@code ByteBuf} so the stream-based
 * {@code RmttDecoder} can be reused. One WebSocket binary frame carries exactly one RMTT packet.
 */
public class WsFrameToByteBuf extends MessageToMessageDecoder<WebSocketFrame> {

    /**
     * Create the converter.
     */
    public WsFrameToByteBuf() {
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, WebSocketFrame frame, List<Object> out) {
        if (frame instanceof CloseWebSocketFrame) {
            ctx.close();
            return;
        }
        if (frame instanceof PingWebSocketFrame) {
            ctx.writeAndFlush(new PongWebSocketFrame(frame.content().retain()));
            return;
        }
        if (frame instanceof PongWebSocketFrame) {
            return;
        }
        if (!(frame instanceof BinaryWebSocketFrame)) {
            return;
        }
        byte[] bytes = new byte[frame.content().readableBytes()];
        frame.content().readBytes(bytes);
        out.add(io.netty.buffer.Unpooled.wrappedBuffer(bytes));
    }
}