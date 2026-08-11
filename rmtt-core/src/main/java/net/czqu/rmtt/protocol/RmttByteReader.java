package net.czqu.rmtt.protocol;

/**
 * Minimal sequential read cursor over a binary frame. Implemented per transport over its own
 * native buffer (netty {@code ByteBuf} / aio {@code ByteBuffer} / {@code byte[]}) so no
 * cross-memory-kind conversion is required in hot paths.
 *
 * <p>Reads may raise {@link Underflow} when fewer bytes remain than requested; transports that
 * buffer partial frames (netty ReplayingDecoder, aio accumulator) rely on this to wait for more
 * input.</p>
 */
public interface RmttByteReader {

    /**
     * Read the next byte as an unsigned value.
     *
     * @return the next unsigned byte (0-255)
     */
    int readUnsignedByte();

    /**
     * Read the next two bytes as an unsigned big-endian short.
     *
     * @return the next unsigned short (0-65535), big-endian
     */
    int readUnsignedShort();

    /**
     * Read the next four bytes as a big-endian int.
     *
     * @return the next 4-byte big-endian int
     */
    int readInt();

    /**
     * Read the given number of bytes directly into {@code dst} at {@code off}.
     *
     * @param dst the destination array
     * @param off the offset in {@code dst} where the first byte is written
     * @param len the number of bytes to read
     */
    void readBytes(byte[] dst, int off, int len);

    /**
     * Copy the next bytes into a fresh array.
     *
     * @param len the number of bytes to read
     * @return a new array holding the next {@code len} bytes
     */
    default byte[] readBytes(int len) {
        byte[] b = new byte[len];
        readBytes(b, 0, len);
        return b;
    }

    /**
     * Read a length-prefixed UTF-8 string.
     *
     * @return the decoded string
     */
    default String readString() {
        int len = readUnsignedShort();
        return new String(readBytes(len), java.nio.charset.StandardCharsets.UTF_8);
    }

    /**
     * Remaining readable bytes.
     *
     * @return the number of readable bytes remaining
     */
    int readableBytes();

    /**
     * Raised when a read would exceed the readable region.
     */
    final class Underflow extends RuntimeException {
        /**
         * Construct with the request size and the available size.
         *
         * @param need the bytes requested
         * @param have the bytes actually available
         */
        public Underflow(int need, int have) {
            super("need " + need + " bytes but only " + have + " available");
        }
    }
}