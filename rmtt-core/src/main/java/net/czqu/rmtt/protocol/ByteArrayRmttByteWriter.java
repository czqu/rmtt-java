package net.czqu.rmtt.protocol;

/** Growable {@link RmttByteWriter} over a {@code byte[]}. */
public final class ByteArrayRmttByteWriter implements RmttByteWriter {

    private byte[] buf;
    private int count;

    /** Create a writer with the default initial capacity. */
    public ByteArrayRmttByteWriter() {
        this(32);
    }

    /**
     * Create a writer with the given initial capacity.
     *
     * @param capacity the initial backing-array capacity
     */
    public ByteArrayRmttByteWriter(int capacity) {
        this.buf = new byte[Math.max(8, capacity)];
    }

    private void ensure(int extra) {
        if (count + extra > buf.length) {
            int newCap = buf.length;
            while (newCap < count + extra) {
                newCap <<= 1;
            }
            byte[] next = new byte[newCap];
            System.arraycopy(buf, 0, next, 0, count);
            buf = next;
        }
    }

    @Override
    public void writeByte(int b) {
        ensure(1);
        buf[count++] = (byte) b;
    }

    @Override
    public void writeUnsignedShort(int v) {
        ensure(2);
        buf[count++] = (byte) (v >>> 8);
        buf[count++] = (byte) v;
    }

    @Override
    public void writeInt(int v) {
        ensure(4);
        buf[count++] = (byte) (v >>> 24);
        buf[count++] = (byte) (v >>> 16);
        buf[count++] = (byte) (v >>> 8);
        buf[count++] = (byte) v;
    }

    @Override
    public void writeBytes(byte[] src, int off, int len) {
        ensure(len);
        System.arraycopy(src, off, buf, count, len);
        count += len;
    }

    /**
     * The number of bytes written so far.
     *
     * @return the number of bytes written so far
     */
    public int size() {
        return count;
    }

    /**
     * A copy of the written bytes.
     *
     * @return a copy of the written bytes
     */
    public byte[] toByteArray() {
        byte[] out = new byte[count];
        System.arraycopy(buf, 0, out, 0, count);
        return out;
    }
}