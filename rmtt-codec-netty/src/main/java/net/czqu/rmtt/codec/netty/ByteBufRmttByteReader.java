package net.czqu.rmtt.codec.netty;

import io.netty.buffer.ByteBuf;
import net.czqu.rmtt.protocol.RmttByteReader;

/**
 * {@link RmttByteReader} over a netty {@link ByteBuf}. Reads straight from the buffer with no
 * intermediate copy. Insufficient bytes raise {@link RmttByteReader.Underflow} so frame decoders
 * can wait for more input (same contract as the aio and byte-array readers).
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

    private void require(int n) {
        if (buf.readableBytes() < n) {
            throw new Underflow(n, buf.readableBytes());
        }
    }

    @Override
    public int readUnsignedByte() {
        require(1);
        return buf.readUnsignedByte();
    }

    @Override
    public int readUnsignedShort() {
        require(2);
        return buf.readUnsignedShort();
    }

    @Override
    public int readInt() {
        require(4);
        return buf.readInt();
    }

    @Override
    public void readBytes(byte[] dst, int off, int len) {
        require(len);
        buf.readBytes(dst, off, len);
    }

    @Override
    public int readableBytes() {
        return buf.readableBytes();
    }
}