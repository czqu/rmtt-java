package net.czqu.rmtt.protocol;

import net.czqu.rmtt.logging.InternalLogger;
import net.czqu.rmtt.logging.InternalLoggerFactory;

/**
 * Client-side adaptive heartbeat state machine (client-side policy — not mandated by the
 * protocol). The negotiated {@code server_kp} from CONNACK is treated as a <em>suggestion / upper
 * bound</em>: the client is free to heartbeat anywhere in {@code [shortSeconds, ceiling]} where
 * {@code ceiling = min(maxSeconds, server_kp)}.
 *
 * <p>Phases:</p>
 * <ol>
 *   <li><b>PROBE_SHORT</b> — send PINGREQ every {@code shortSeconds}; after {@code probeCount}
 *       consecutive successful interactions enter the adaptive phase. A failure here means the
 *       connection is dead → {@link #onProbeFailed()} (escalate to reconnect).</li>
 *   <li><b>PROBE_DOUBLING</b> — starting from {@code shortSeconds}, double the interval each round
 *       (capped at the ceiling) until a probe fails or the ceiling is reached.</li>
 *   <li><b>PROBE_FINE</b> — from the last successful interval, nudge up by {@code fineStepSeconds}
 *       one step at a time until a probe fails; that yields the final max interval.</li>
 *   <li><b>STABLE</b> — heartbeat at {@code successHeart = lastSuccess × 0.9} (floored to seconds).
 *       If a heartbeat is lost (no response within {@code successHeart × 1.5}), fall back to
 *       PROBE_SHORT and re-adapt; a re-probe failure escalates to reconnect.</li>
 * </ol>
 *
 * <p>The transport is abstracted via {@link Transport} so the same machine drives the netty, aio and
 * Go(-port) clients. Drive it by calling {@link #tick()} on a short fixed schedule (e.g. 250ms);
 * {@link #tick()} is cheap and stateful.</p>
 */
public final class AdaptiveHeartbeat {

    /** Abstraction over the live connection used to exchange PINGREQ/PINGRESP. */
    public interface Transport {
        /** Send a PINGREQ on the wire. */
        void sendPing();

        /**
         * Epoch of the last PINGREQ sent.
         *
         * @return ms epoch of the last PINGREQ sent, 0 if none
         */
        long lastSentMs();

        /**
         * Epoch of the last received packet.
         *
         * @return ms epoch of the last received packet, 0 if none
         */
        long lastReceivedMs();

        /**
         * Whether the socket is still open.
         *
         * @return true while the socket is open
         */
        boolean isConnected();

        /**
         * Current wall-clock time.
         *
         * @return ms epoch (injectable for tests; normally {@code System.currentTimeMillis()})
         */
        long nowMs();
    }

    /** Current phase of the adaptive heartbeat state machine. */
    public enum State {
        /** Short fixed-interval probing until enough successes accumulate. */
        PROBE_SHORT,
        /** Interval doubling until a probe fails or the ceiling is reached. */
        PROBE_DOUBLING,
        /** One-step nudging around the last successful interval. */
        PROBE_FINE,
        /** Settled at the last successful interval. */
        STABLE
    }

    private static final long TICK_GRANULARITY_MS = 250;
    private static final InternalLogger LOG = InternalLoggerFactory.getLogger(AdaptiveHeartbeat.class);

    private final Transport transport;
    private final long shortMillis;
    private final long maxMillis;
    private final long ceilingMillis;
    private final int probeCount;
    private final long responseWindowMillis;
    private final long fineStepMillis;

    private State state;
    private int shortOk;
    private long intervalMillis;
    private long lastSuccessMillis;
    private long successHeartMillis;
    private long sentAtMillis;
    private boolean awaitingResponse;

    /**
     * Create the state machine bound to a live connection.
     *
     * @param shortSeconds        preset short heartbeat period (&gt;=1s)
     * @param maxSeconds          configured adaptive max (&gt;= shortSeconds)
     * @param serverKp            negotiated server_kp from CONNACK (0 = keepalive disabled)
     * @param probeCount          consecutive short-heartbeat successes before probing (&gt;=1)
     * @param responseWindowMillis window to wait for a PINGRESP before counting a probe as failed
     * @param fineStepSeconds     nudge step used in PROBE_FINE (&gt;=1s)
     * @param transport           live connection
     */
    public AdaptiveHeartbeat(int shortSeconds, int maxSeconds, int serverKp,
                             int probeCount, long responseWindowMillis, int fineStepSeconds,
                             Transport transport) {
        this.transport = transport;
        long shortMs = Math.max(1000L, shortSeconds * 1000L);
        this.shortMillis = shortMs;
        this.maxMillis = Math.max(shortMs, maxSeconds * 1000L);
        long kpMs = serverKp > 0 ? serverKp * 1000L : Long.MAX_VALUE;
        this.ceilingMillis = Math.min(this.maxMillis, kpMs);
        this.probeCount = Math.max(1, probeCount);
        this.responseWindowMillis = Math.max(250, responseWindowMillis);
        this.fineStepMillis = Math.max(1000L, fineStepSeconds * 1000L);
        this.state = State.PROBE_SHORT;
        this.intervalMillis = this.shortMillis;
    }

    /**
     * The current phase of the state machine.
     *
     * @return the current phase
     */
    public State state() {
        return state;
    }

    /**
     * The current heartbeat interval to use.
     *
     * @return the current heartbeat interval: shortMillis in probe phases, successHeart in STABLE
     */
    public long intervalMillis() {
        return state == State.STABLE ? successHeartMillis : intervalMillis;
    }

    /**
     * The last confirmed sustainable interval.
     *
     * @return the last successful heartbeat interval, 0 before the first success
     */
    public long successHeartMillis() {
        return successHeartMillis;
    }

    /**
     * Whether adaptive probing can exceed the short period.
     *
     * @return true when the adaptive ceiling allows anything above the short period
     */
    public boolean hasRoom() {
        return ceilingMillis > shortMillis;
    }

    /** Reset for a fresh connection (new CONNACK → new negotiation). */
    public void reset() {
        state = State.PROBE_SHORT;
        shortOk = 0;
        intervalMillis = shortMillis;
        lastSuccessMillis = 0;
        successHeartMillis = 0;
        sentAtMillis = 0;
        awaitingResponse = false;
    }

    /**
     * Advance the machine. Called on a fixed short schedule.
     *
     * @return true when the caller should escalate to connection-lost (short heartbeat probing
     *         failed or the stable phase degraded beyond rescue)
     */
    public boolean tick() {
        long now = transport.nowMs();
        if (!transport.isConnected()) {
            return false;
        }
        switch (state) {
            case PROBE_SHORT:
                return tickProbeShort(now);
            case PROBE_DOUBLING:
                return tickProbing(now);
            case PROBE_FINE:
                return tickProbing(now);
            case STABLE:
                return tickStable(now);
            default:
                return false;
        }
    }

    private boolean tickProbeShort(long now) {
        if (awaitingResponse) {
            if (gotResponseAfter(sentAtMillis)) {
                awaitingResponse = false;
                shortOk++;
                if (shortOk >= probeCount) {
                    // all short heartbeats OK → start doubling from the short period
                    state = State.PROBE_DOUBLING;
                    intervalMillis = shortMillis;
                    lastSuccessMillis = shortMillis;
                    sentAtMillis = 0;
                    awaitingResponse = false;
                    LOG.debug("adaptive transition PROBE_SHORT -> PROBE_DOUBLING ({} short ok)", shortOk);
                    return false;
                }
                return false;
            }
            if (now - sentAtMillis >= responseWindowMillis) {
                // even short heartbeats fail → connection is dead
                LOG.warn("adaptive probe failed in PROBE_SHORT after {}ms -> escalate", responseWindowMillis);
                return onProbeFailed();
            }
            return false;
        }
        if (now - transport.lastSentMs() >= intervalMillis) {
            sendProbe();
        }
        return false;
    }

    private boolean tickProbing(long now) {
        if (awaitingResponse) {
            if (gotResponseAfter(sentAtMillis)) {
                awaitingResponse = false;
                lastSuccessMillis = intervalMillis;
                if (state == State.PROBE_DOUBLING) {
                    long next = intervalMillis * 2;
                    if (next > ceilingMillis) {
                        next = ceilingMillis;
                    }
                    if (next <= intervalMillis) {
                        // ceiling reached without failure → jump straight to stable
                        LOG.debug("adaptive doubling reached ceiling {}ms -> STABLE", ceilingMillis);
                        enterStable();
                        return false;
                    }
                    intervalMillis = next;
                    LOG.debug("adaptive probe ok, doubling interval -> {}s", intervalMillis / 1000);
                    return false;
                }
                // PROBE_FINE: keep nudging until failure
                long next = intervalMillis + fineStepMillis;
                if (next > ceilingMillis) {
                    LOG.debug("adaptive fine probe reached ceiling {}ms -> STABLE", ceilingMillis);
                    enterStable();
                    return false;
                }
                intervalMillis = next;
                LOG.debug("adaptive fine probe ok, nudge interval -> {}s", intervalMillis / 1000);
                return false;
            }
            if (now - sentAtMillis >= responseWindowMillis) {
                // probe failed → lastSuccessMillis is the max sustainable interval
                LOG.debug("adaptive probe failed at {}s -> STABLE at ~{}s",
                        intervalMillis / 1000, (long) (lastSuccessMillis * 0.9) / 1000);
                enterStable();
                return false;
            }
            return false;
        }
        if (now - transport.lastSentMs() >= intervalMillis) {
            sendProbe();
        }
        return false;
    }

    private boolean tickStable(long now) {
        if (now - transport.lastReceivedMs() >= (long) (successHeartMillis * 1.5)) {
            // heartbeat lost in stable state → fall back and re-adapt
            LOG.warn("adaptive heartbeat lost in STABLE ({}s heart, {}ms idle) -> re-adapt from PROBE_SHORT",
                    successHeartMillis / 1000, now - transport.lastReceivedMs());
            state = State.PROBE_SHORT;
            shortOk = 0;
            intervalMillis = shortMillis;
            lastSuccessMillis = 0;
            successHeartMillis = 0;
            sentAtMillis = 0;
            awaitingResponse = false;
            return false;
        }
        if (now - transport.lastSentMs() >= successHeartMillis) {
            sendProbe();
        }
        return false;
    }

    private void sendProbe() {
        transport.sendPing();
        sentAtMillis = transport.nowMs();
        awaitingResponse = true;
    }

    private boolean gotResponseAfter(long sent) {
        return transport.lastReceivedMs() >= sent;
    }

    private void enterStable() {
        if (lastSuccessMillis > 0) {
            successHeartMillis = Math.max(shortMillis, (long) (lastSuccessMillis * 0.9));
        } else {
            successHeartMillis = shortMillis;
        }
        state = State.STABLE;
        sentAtMillis = 0;
        awaitingResponse = false;
        LOG.info("adaptive heartbeat STABLE: max={}s heart={}s ceiling={}s",
                lastSuccessMillis / 1000, successHeartMillis / 1000, ceilingMillis / 1000);
    }

    private boolean onProbeFailed() {
        state = State.PROBE_SHORT;
        shortOk = 0;
        intervalMillis = shortMillis;
        sentAtMillis = 0;
        awaitingResponse = false;
        return true; // escalate to connection-lost
    }
}
