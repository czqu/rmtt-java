package net.czqu.rmtt.codec.netty;

import io.netty.buffer.ByteBuf;
import net.czqu.rmtt.protocol.RmttByteReader;

/**
 * {@link RmttByteReader} over a netty {@link ByteBuf}. Reads straight from the buffer with no
 * intermediate copy; under a {@link io.netty.handler.codec.ReplayingDecoder} the ByteBuf read
 * methods throw the replay signal when not enough bytes are buffered, which is how the decoder
 * waits for a complete frame.
 */
public final class ByteBufRmttByteReader implements RmttByteReader {

    private final ByteBuf buf;

    /**
     * Wrap the given buffer as the data source for reads.
     *
     * @param buf the buffer to read from
     */
    public ByteBufRmttByteReader(ByteBuf buf) {
        this.buf = buf;
    }

    @Override
    public int readUnsignedByte() {
        return buf.readUnsignedByte();
    }

    @Override
    public int readUnsignedShort() {
        return buf.readUnsignedShort();
    }

    @Override
    public int readInt() {
        return buf.readInt();
    }

    @Override
    public void readBytes(byte[] dst, int off, int len) {
        buf.readBytes(dst, off, len);
    }

    @Override
    public int readableBytes() {
        return buf.readableBytes();
    }
}