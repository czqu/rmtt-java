package net.czqu.rmtt.protocol;

/** Fixed header of every RMTT packet: 1 type nibble + 4 flags + remaining length. */
public final class FixedHeader {

    private static final int FLAG1 = 1 << 3; // dup
    private static final int FLAG2 = 1 << 2; // qos (unused)
    private static final int FLAG3 = 1 << 1;
    private static final int FLAG4 = 1 << 0;

    private final RmttMessageType messageType;
    private final boolean flag1;
    private final boolean flag2;
    private final boolean flag3;
    private final boolean flag4;
    private final int remainingLength;

    /**
     * Create a fixed header with an explicit remaining length.
     *
     * @param messageType     the packet type
     * @param flag1           the first flag bit (dup)
     * @param flag2           the second flag bit
     * @param flag3           the third flag bit
     * @param flag4           the fourth flag bit
     * @param remainingLength the length of everything after the fixed header
     * @throws IllegalArgumentException if {@code messageType} is null
     */
    public FixedHeader(RmttMessageType messageType, boolean flag1, boolean flag2, boolean flag3, boolean flag4, int remainingLength) {
        if (messageType == null) {
            throw new IllegalArgumentException("messageType must not be null");
        }
        this.messageType = messageType;
        this.flag1 = flag1;
        this.flag2 = flag2;
        this.flag3 = flag3;
        this.flag4 = flag4;
        this.remainingLength = remainingLength;
    }

    /**
     * Create a fixed header with a zero remaining length.
     *
     * @param messageType the packet type
     * @param flag1       the first flag bit (dup)
     * @param flag2       the second flag bit
     * @param flag3       the third flag bit
     * @param flag4       the fourth flag bit
     */
    public FixedHeader(RmttMessageType messageType, boolean flag1, boolean flag2, boolean flag3, boolean flag4) {
        this(messageType, flag1, flag2, flag3, flag4, 0);
    }

    /**
     * The packet type.
     *
     * @return the packet type
     */
    public RmttMessageType messageType() {
        return messageType;
    }

    /**
     * The first flag bit.
     *
     * @return the first flag bit (dup)
     */
    public boolean isFlag1() { return flag1; }

    /**
     * The second flag bit.
     *
     * @return the second flag bit
     */
    public boolean isFlag2() { return flag2; }

    /**
     * The third flag bit.
     *
     * @return the third flag bit
     */
    public boolean isFlag3() { return flag3; }

    /**
     * The fourth flag bit.
     *
     * @return the fourth flag bit
     */
    public boolean isFlag4() { return flag4; }

    /**
     * The length of everything after the fixed header.
     *
     * @return the length of everything after the fixed header
     */
    public int remainingLength() {
        return remainingLength;
    }

    /**
     * Encode the fixed-header first byte.
     *
     * @return the single fixed-header first byte (type nibble | flags)
     */
    public byte firstByte() {
        int b = (messageType.value() & 0x0F) << 4;
        if (flag1) b |= FLAG1;
        if (flag2) b |= FLAG2;
        if (flag3) b |= FLAG3;
        if (flag4) b |= FLAG4;
        return (byte) b;
    }

    @Override
    public String toString() {
        return "FixedHeader{" + messageType + ",remainingLength=" + remainingLength + '}';
    }
}