package net.czqu.rmtt.api;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Shared admission counter for one transport stack: caps how many concurrent connections
 * (or sessions) a server accepts. A {@code maxConnections <= 0} disables the limit.
 */
public final class ConnectionLimiter {

    private final int maxConnections;
    private final AtomicInteger active = new AtomicInteger();

    /**
     * Create a limiter.
     *
     * @param maxConnections maximum concurrent connections, or {@code <= 0} for unlimited
     */
    public ConnectionLimiter(int maxConnections) {
        this.maxConnections = maxConnections;
    }

    /**
     * Try to admit one connection.
     *
     * @return true when the connection may proceed; false when the limit is reached
     */
    public boolean tryAcquire() {
        if (maxConnections <= 0) {
            return true;
        }
        if (active.incrementAndGet() > maxConnections) {
            active.decrementAndGet();
            return false;
        }
        return true;
    }

    /**
     * Release one previously admitted connection.
     */
    public void release() {
        if (maxConnections > 0) {
            active.decrementAndGet();
        }
    }

    /**
     * Number of currently admitted connections.
     *
     * @return the number of currently admitted connections
     */
    public int activeConnections() {
        return active.get();
    }

    /**
     * The configured connection cap.
     *
     * @return the configured maximum (0 = unlimited)
     */
    public int maxConnections() {
        return maxConnections;
    }
}
