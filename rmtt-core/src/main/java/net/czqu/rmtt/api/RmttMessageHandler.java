package net.czqu.rmtt.api;

/**
 * Application hook for upstream PUSH messages received from a device. payload is the raw message
 * body (transport-agnostic byte[]); the connection owns it for the duration of the call.
 */
@FunctionalInterface
public interface RmttMessageHandler {

    /**
     * Handle an upstream PUSH message from a device.
     *
     * @param deviceId the device id that sent the message
     * @param payload  the raw message body; owned by the connection for the duration of the call
     */
    void onMessage(String deviceId, byte[] payload);
}