package net.czqu.rmtt.server.aio;

import net.czqu.rmtt.api.DeviceConnection;
import net.czqu.rmtt.protocol.DisconnectReturnCode;
import net.czqu.rmtt.protocol.RmttMessageFactory;
import net.czqu.rmtt.protocol.RmttWireCodec;
import net.czqu.rmtt.transport.aio.AioConnection;

/** {@link DeviceConnection} backed by an {@link AioConnection}. */
public final class AioDeviceConnection implements DeviceConnection {

    private final AioConnection conn;

    /**
     * Wrap an existing AIO connection.
     *
     * @param conn the underlying AIO connection
     */
    public AioDeviceConnection(AioConnection conn) {
        this.conn = conn;
    }

    /**
     * The underlying AIO connection.
     *
     * @return the AIO connection
     */
    public AioConnection connection() {
        return conn;
    }

    @Override
    public boolean isActive() {
        return conn.channel().isOpen();
    }

    @Override
    public boolean write(byte[] frame) {
        if (!isActive()) {
            return false;
        }
        conn.writeFrame(frame);
        return true;
    }

    @Override
    public void sendDisconnect(DisconnectReturnCode code) {
        if (isActive()) {
            conn.writeFrame(RmttWireCodec.encodeToBytes(RmttMessageFactory.disconnect(code)));
        }
        conn.close();
    }

    @Override
    public void close() {
        conn.close();
    }
}