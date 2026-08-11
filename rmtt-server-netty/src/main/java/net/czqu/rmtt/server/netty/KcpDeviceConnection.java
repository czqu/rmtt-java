package net.czqu.rmtt.server.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import kcp.Ukcp;
import net.czqu.rmtt.api.DeviceConnection;
import net.czqu.rmtt.protocol.DisconnectReturnCode;
import net.czqu.rmtt.protocol.RmttMessageFactory;
import net.czqu.rmtt.protocol.RmttWireCodec;

/**
 * {@link DeviceConnection} backed by a kcp-base {@link Ukcp} session. The underlying
 * {@code ukcp.write} retains its own copy of the frame, so the temporary {@link ByteBuf} is
 * released right after the write.
 */
public final class KcpDeviceConnection implements DeviceConnection {

    private final Ukcp ukcp;
    private final Runnable closer;

    /**
     * Wrap an active KCP session.
     *
     * @param ukcp   the underlying KCP session
     * @param closer run when the connection is torn down
     */
    public KcpDeviceConnection(Ukcp ukcp, Runnable closer) {
        this.ukcp = ukcp;
        this.closer = closer;
    }

    /**
     * The underlying KCP session.
     *
     * @return the KCP session
     */
    public Ukcp ukcp() {
        return ukcp;
    }

    @Override
    public boolean isActive() {
        return ukcp.isActive();
    }

    @Override
    public boolean write(byte[] frame) {
        if (!ukcp.isActive()) {
            return false;
        }
        ByteBuf buf = Unpooled.wrappedBuffer(frame);
        ukcp.write(buf);
        buf.release();
        return true;
    }

    @Override
    public void sendDisconnect(DisconnectReturnCode code) {
        if (ukcp.isActive()) {
            write(RmttWireCodec.encodeToBytes(RmttMessageFactory.disconnect(code)));
        }
        closer.run();
    }

    @Override
    public void close() {
        ukcp.close();
    }
}
