package net.czqu.rmtt.api;

/**
 * Lifecycle callbacks notified by the server. Implementations must be safe to call from the
 * transport event loop; {@link #NOOP} is provided for no-op cases.
 */
public interface ConnectionListener {

    /**
     * Called after a device connection is fully established (CONNACK accepted).
     *
     * @param deviceId the registered device id
     */
    void onConnectionEstablished(String deviceId);

    /**
     * Called when a device connection is closed, for any reason.
     *
     * @param deviceId the device id that was connected, if known
     * @param reason   short human-readable close reason
     */
    void onConnectionClosed(String deviceId, String reason);

    /**
     * Called when the server decides to stop keeping a device's connection alive.
     *
     * @param deviceId the device id being reaped
     */
    void onReconnecting(String deviceId);

    /** No-op listener for cases that do not care about lifecycle events. */
    ConnectionListener NOOP = new ConnectionListener() {
        @Override
        public void onConnectionEstablished(String deviceId) {
        }

        @Override
        public void onConnectionClosed(String deviceId, String reason) {
        }

        @Override
        public void onReconnecting(String deviceId) {
        }
    };
}