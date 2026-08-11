package net.czqu.rmtt.protocol;

/** Sequential write cursor over a binary frame, backed by a transport-native buffer. */
public interface RmttByteWriter {

    /**
     * Write a single byte.
     *
     * @param b the byte value (only the low 8 bits are written)
     */
    void writeByte(int b);

    /**
     * Write an unsigned short, big-endian.
     *
     * @param v the short value (only the low 16 bits are written)
     */
    void writeUnsignedShort(int v);

    /**
     * Write an int, big-endian.
     *
     * @param v the int value
     */
    void writeInt(int v);

    /**
     * Write a slice of a byte array.
     *
     * @param src the source array
     * @param off the offset in {@code src} where the first byte is read
     * @param len the number of bytes to write
     */
    void writeBytes(byte[] src, int off, int len);

    /**
     * Write a full byte array.
     *
     * @param src the source array
     */
    default void writeBytes(byte[] src) {
        writeBytes(src, 0, src.length);
    }

    /**
     * Write a length-prefixed UTF-8 string. A null string is written as an empty one.
     *
     * @param s the string to write, may be null
     */
    default void writeString(String s) {
        byte[] b = s == null ? new byte[0] : s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        writeUnsignedShort(b.length);
        writeBytes(b);
    }
}