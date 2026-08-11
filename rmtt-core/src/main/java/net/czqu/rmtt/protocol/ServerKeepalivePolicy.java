package net.czqu.rmtt.protocol;

/**
 * Server keepalive decision policy. The server owns the final heartbeat interval: it
 * reads the client's proposed {@code client_kp} from CONNECT and overrides it (not {@code min()}),
 * returning the final value in CONNACK.ServerKeepalive.
 */
public final class ServerKeepalivePolicy {
    private final int minSeconds;
    private final int maxSeconds;
    private final int defaultSeconds;
    private final boolean allowDisable;

    /**
     * Create a policy with explicit bounds.
     *
     * @param minSeconds   lower bound for the final interval
     * @param maxSeconds   upper bound for the final interval
     * @param defaultSeconds interval used when the client does not propose one
     * @param allowDisable whether a client proposal of 0 may disable keepalive entirely
     */
    public ServerKeepalivePolicy(int minSeconds, int maxSeconds, int defaultSeconds, boolean allowDisable) {
        this.minSeconds = minSeconds;
        this.maxSeconds = maxSeconds;
        this.defaultSeconds = defaultSeconds;
        this.allowDisable = allowDisable;
    }

    /**
     * The lower bound.
     *
     * @return the configured lower bound in seconds
     */
    public int minSeconds() { return minSeconds; }

    /**
     * The upper bound.
     *
     * @return the configured upper bound in seconds
     */
    public int maxSeconds() { return maxSeconds; }

    /**
     * The fallback interval.
     *
     * @return the configured fallback in seconds
     */
    public int defaultSeconds() { return defaultSeconds; }

    /**
     * Whether keepalive may be disabled.
     *
     * @return true if a client proposal of 0 can disable keepalive
     */
    public boolean allowDisable() { return allowDisable; }

    /**
     * Decide the server_kp for a client that proposed {@code clientKp} (0 = unspecified).
     *
     * @param clientKp the client-proposed interval in seconds, 0 when unspecified
     * @return the final server-decision interval in seconds
     */
    public int decide(int clientKp) {
        if (clientKp <= 0) {
            return allowDisable ? 0 : defaultSeconds;
        }
        if (clientKp < minSeconds) {
            return minSeconds;
        }
        if (clientKp > maxSeconds) {
            return maxSeconds;
        }
        return clientKp;
    }

    /**
     * Suggested defaults: MIN=30s, MAX=600s, default=60s, disable not allowed.
     *
     * @return a policy with the suggested defaults
     */
    public static ServerKeepalivePolicy defaults() {
        return new ServerKeepalivePolicy(30, 600, 60, false);
    }
}