package net.czqu.rmtt.protocol;

/**
 * CONNECT credential — arbitrary authentication payload carried on the wire
 * (can be a JWT token, a bare device id, username/password, etc.).
 */
public final class ConnectPayload {
    private final String credential;

    /**
     * Create a payload wrapper.
     *
     * @param credential the decoded credential string, or null when the wire payload was absent
     */
    public ConnectPayload(String credential) {
        this.credential = credential;
    }

    /**
     * The decoded credential.
     *
     * @return the decoded credential, or null when absent
     */
    public String credential() {
        return credential;
    }

    @Override
    public String toString() {
        return "ConnectPayload{credential=" + credential + '}';
    }
}