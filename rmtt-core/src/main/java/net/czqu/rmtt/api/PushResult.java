package net.czqu.rmtt.api;

/** Outcome of a downstream push to a device. */
public enum PushResult {
    /** Frame was written to the connection. */
    SUCCESS,
    /** No live connection for the device id. */
    DEVICE_OFFLINE,
    /** Write did not complete/fail within the configured timeout. */
    TIMEOUT,
    /** The connection rejected the write. */
    REJECTED
}