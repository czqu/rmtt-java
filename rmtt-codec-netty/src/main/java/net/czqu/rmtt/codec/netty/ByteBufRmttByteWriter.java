package net.czqu.rmtt.codec.netty;

import io.netty.buffer.ByteBuf;
import net.czqu.rmtt.protocol.RmttByteWriter;

/** {@link RmttByteWriter} that writes directly into a netty {@link ByteBuf}. */
public final class ByteBufRmttByteWriter implements RmttByteWriter {

    private final ByteBuf buf;

    /**
     * Wrap the given buffer as the data sink for writes.
     *
     * @param buf the buffer to write into
     */
    public ByteBufRmttByteWriter(ByteBuf buf) {
        this.buf = buf;
    }

    @Override
    public void writeByte(int b) {
        buf.writeByte(b);
    }

    @Override
    public void writeUnsignedShort(int v) {
        buf.writeShort(v);
    }

    @Override
    public void writeInt(int v) {
        buf.writeInt(v);
    }

    @Override
    public void writeBytes(byte[] src, int off, int len) {
        buf.writeBytes(src, off, len);
    }
}