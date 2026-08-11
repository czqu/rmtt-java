package net.czqu.rmtt.protocol;

/** DISCONNECT variable header: single return-code byte. */
public final class DisconnectVariableHeader {
    private final byte returnCode;

    /**
     * Create a header from the raw wire byte.
     *
     * @param returnCode the raw wire byte of the return code
     */
    public DisconnectVariableHeader(byte returnCode) {
        this.returnCode = returnCode;
    }

    /**
     * The raw wire byte.
     *
     * @return the raw wire byte of the return code
     */
    public byte returnCode() {
        return returnCode;
    }

    /**
     * The parsed return code.
     *
     * @return the parsed {@link DisconnectReturnCode}, {@link DisconnectReturnCode#RESERVED} for
     *         unknown wire values
     */
    public DisconnectReturnCode asDisconnectReturnCode() {
        return DisconnectReturnCode.valueOf(returnCode);
    }

    @Override
    public String toString() {
        return "DisconnectVariableHeader{code=" + asDisconnectReturnCode() + '}';
    }
}