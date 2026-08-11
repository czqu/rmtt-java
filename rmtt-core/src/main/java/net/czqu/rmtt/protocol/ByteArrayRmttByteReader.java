package net.czqu.rmtt.protocol;

/** {@link RmttByteReader} over a {@code byte[]} slice. */
public final class ByteArrayRmttByteReader implements RmttByteReader {

    private final byte[] buf;
    private final int end;
    private int pos;

    /**
     * Wrap the full array.
     *
     * @param buf the source array (read from offset 0)
     */
    public ByteArrayRmttByteReader(byte[] buf) {
        this(buf, 0, buf.length);
    }

    /**
     * Wrap a slice of the array.
     *
     * @param buf the source array
     * @param off the offset where reading starts
     * @param len the number of readable bytes
     */
    public ByteArrayRmttByteReader(byte[] buf, int off, int len) {
        this.buf = buf;
        this.pos = off;
        this.end = off + len;
    }

    private void require(int n) {
        if (pos + n > end) {
            throw new Underflow(n, end - pos);
        }
    }

    @Override
    public int readUnsignedByte() {
        require(1);
        return buf[pos++] & 0xFF;
    }

    @Override
    public int readUnsignedShort() {
        require(2);
        int v = ((buf[pos] & 0xFF) << 8) | (buf[pos + 1] & 0xFF);
        pos += 2;
        return v;
    }

    @Override
    public int readInt() {
        require(4);
        int v = ((buf[pos] & 0xFF) << 24) | ((buf[pos + 1] & 0xFF) << 16)
                | ((buf[pos + 2] & 0xFF) << 8) | (buf[pos + 3] & 0xFF);
        pos += 4;
        return v;
    }

    @Override
    public void readBytes(byte[] dst, int off, int len) {
        require(len);
        System.arraycopy(buf, pos, dst, off, len);
        pos += len;
    }

    @Override
    public int readableBytes() {
        return end - pos;
    }
}