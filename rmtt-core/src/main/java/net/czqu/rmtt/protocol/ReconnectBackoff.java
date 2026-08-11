package net.czqu.rmtt.protocol;

import java.util.Random;

/**
 * Client auto-reconnect backoff. Formula:
 * {@code wait = MIN(base × 2^(attempt-1), max) × (1 ± jitter)}.
 */
public final class ReconnectBackoff {
    private final long baseMillis;
    private final long maxMillis;
    private final float jitter;
    private final Random random;
    private int attempt;

    /**
     * Create a backoff schedule starting at the first attempt.
     *
     * @param baseMillis base delay for the first attempt (>=1, clamped)
     * @param maxMillis  upper cap for the delay (>= baseMillis, clamped)
     * @param jitter     uniform jitter fraction in [0,1); 0 disables jitter
     */
    public ReconnectBackoff(long baseMillis, long maxMillis, float jitter) {
        this.baseMillis = Math.max(1, baseMillis);
        this.maxMillis = Math.max(this.baseMillis, maxMillis);
        this.jitter = jitter;
        this.random = new Random();
        this.attempt = 1;
    }

    /** Reset the exponential counter (e.g. after a successful reconnect). */
    public synchronized void reset() {
        attempt = 1;
    }

    /**
     * Compute the delay before the next (re)connect attempt without resetting the counter.
     *
     * @return the delay in milliseconds, at least 1
     */
    public synchronized long nextDelayMillis() {
        long cap = Math.min(maxMillis, baseMillis * (1L << (attempt - 1)));
        if (attempt < 63) {
            attempt++;
        }
        long delay = cap;
        if (jitter > 0) {
            double factor = 1.0 - jitter + random.nextDouble() * (2 * jitter);
            delay = (long) (cap * factor);
        }
        return Math.max(1, delay);
    }
}