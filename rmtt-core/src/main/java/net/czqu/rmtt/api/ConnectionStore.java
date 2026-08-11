package net.czqu.rmtt.api;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory route table mapping {@code device_id -> connection}. Transport-agnostic; both stacks
 * store their own {@link DeviceConnection} here.
 */
public final class ConnectionStore {

    private final ConcurrentHashMap<String, DeviceConnection> connections = new ConcurrentHashMap<>();

    /** Create an empty connection store. */
    public ConnectionStore() {
    }

    /**
     * Register a connection for a device id. If another connection holds the id it is returned, and
     * the new connection takes over the entry (the caller decides how to treat the old one,
     * typically kick-old-new sending {@code SESSION_TAKEN_OVER}).
     *
     * @param deviceId   the device id to register
     * @param connection the connection to store
     * @return the previous connection if present, otherwise {@link Optional#empty()}
     */
    public Optional<DeviceConnection> register(String deviceId, DeviceConnection connection) {
        return Optional.ofNullable(connections.put(deviceId, connection));
    }

    /**
     * Look up the connection registered for a device id.
     *
     * @param deviceId the device id to look up
     * @return the registered connection if present, otherwise {@link Optional#empty()}
     */
    public Optional<DeviceConnection> get(String deviceId) {
        return Optional.ofNullable(connections.get(deviceId));
    }

    /**
     * Remove the mapping only if it still points at the given connection.
     *
     * @param deviceId   the device id to remove
     * @param connection the connection the mapping must still reference
     * @return true if the mapping was removed
     */
    public boolean remove(String deviceId, DeviceConnection connection) {
        return connections.remove(deviceId, connection);
    }

    /**
     * Whether a device is registered and its connection is still alive.
     *
     * @param deviceId the device id to check
     * @return true if a connection is registered and still active
     */
    public boolean isOnline(String deviceId) {
        DeviceConnection c = connections.get(deviceId);
        return c != null && c.isActive();
    }

    /**
     * Number of registered device ids.
     *
     * @return the number of registered device ids
     */
    public int size() {
        return connections.size();
    }
}