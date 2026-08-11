package net.czqu.rmtt.client.netty;

/** Application callback for downstream PUSH messages. */
@FunctionalInterface
public interface RmttPushHandler {

    /**
     * Invoked when a downstream PUSH arrives.
     *
     * @param payload the raw payload bytes
     */
    void onPush(byte[] payload);
}