package net.czqu.rmtt.api;

import net.czqu.rmtt.protocol.ConnectReturnCode;

/** Result of {@link Authenticator#authenticate(String)}. */
public final class AuthResult {

    private final boolean allowed;
    private final String deviceId;
    private final ConnectReturnCode returnCode;

    private AuthResult(boolean allowed, String deviceId, ConnectReturnCode returnCode) {
        this.allowed = allowed;
        this.deviceId = deviceId;
        this.returnCode = returnCode;
    }

    /**
     * Accept the credential and bind it to the given device id.
     *
     * @param deviceId the device id reported by the client
     * @return an accepted result carrying {@code deviceId}
     */
    public static AuthResult allow(String deviceId) {
        return new AuthResult(true, deviceId, ConnectReturnCode.CONNECT_ACCEPTED);
    }

    /**
     * Reject the credential with the default {@link ConnectReturnCode#CONNECT_UNAUTHORIZED}.
     *
     * @param reason informational rejection reason (not sent to the client)
     * @return a rejected result
     */
    public static AuthResult reject(String reason) {
        return new AuthResult(false, null, ConnectReturnCode.CONNECT_UNAUTHORIZED);
    }

    /**
     * Reject the credential with an explicit return code.
     *
     * @param returnCode the CONNACK return code sent back to the client
     * @return a rejected result carrying {@code returnCode}
     */
    public static AuthResult reject(ConnectReturnCode returnCode) {
        return new AuthResult(false, null, returnCode);
    }

    /**
     * Whether the CONNECT was accepted.
     *
     * @return true if accepted
     */
    public boolean allowed() {
        return allowed;
    }

    /**
     * The device id bound to the accepted credential.
     *
     * @return the device id, or null when rejected
     */
    public String deviceId() {
        return deviceId;
    }

    /**
     * The CONNACK return code tied to this result.
     *
     * @return the return code to send back
     */
    public ConnectReturnCode returnCode() {
        return returnCode;
    }
}