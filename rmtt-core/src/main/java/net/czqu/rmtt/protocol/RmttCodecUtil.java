package net.czqu.rmtt.protocol;

/** MQTT-style variable-length integer helpers shared by encode/decode. */
public final class RmttCodecUtil {
    /**
     * Maximum number of bytes allowed for the variable-length remaining-length field.
     */
    public static final int MAX_LENGTH_BYTES = 4;

    private RmttCodecUtil() {
    }

    /**
     * Write the remaining length as a 1..4 byte varint.
     *
     * @param out    the write cursor
     * @param length the length value to encode
     */
    public static void encodeLength(RmttByteWriter out, int length) {
        int v = length;
        while (true) {
            int digit = v % 128;
            v /= 128;
            if (v > 0) {
                digit |= 0x80;
            }
            out.writeByte(digit);
            if (v == 0) {
                break;
            }
        }
    }

    /**
     * Read the remaining length varint.
     *
     * @param in the read cursor
     * @return the decoded length, or -1 when the varint exceeds the maximum field width
     * @throws RmttByteReader.Underflow when the input ends before the varint completes
     */
    public static int decodeLength(RmttByteReader in) {
        long rLength = 0;
        long multiplier = 1;
        for (int i = 0; i < MAX_LENGTH_BYTES; i++) {
            int digit = in.readUnsignedByte();
            rLength += (digit & 0x7F) * multiplier;
            if ((digit & 0x80) == 0) {
                return (int) rLength;
            }
            multiplier *= 128;
        }
        // too many continuation bytes -> protocol violation marker
        return -1;
    }
}