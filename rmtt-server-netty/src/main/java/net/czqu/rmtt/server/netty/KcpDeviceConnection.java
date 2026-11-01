package net.czqu.rmtt.server.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import kcp.Ukcp;
import net.czqu.rmtt.api.DeviceConnection;
import net.czqu.rmtt.protocol.DisconnectReturnCode;
import net.czqu.rmtt.protocol.RmttMessageFactory;
import net.czqu.rmtt.protocol.RmttWireCodec;

/**
 * {@link DeviceConnection} backed by a kcp-base {@link Ukcp} session. Writes are chunked because
 * kcp-base refuses a single message above 255 segments (~350 KiB at the default MTU) and drops it,
 * which would stall any larger RMTT frame. The peer's RMTT layer parses frames from the byte
 * stream, so chunk boundaries are transparent.
 */
public final class KcpDeviceConnection implements DeviceConnection {

    /** Maximum bytes passed to a single {@code ukcp.write}: 256 KiB = 187 segments. */
    static final int MAX_KCP_WRITE_BYTES = 256 * 1024;

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
        writeChunked(ukcp, frame);
        return true;
    }

    /** Write an encoded frame to the KCP session in bounded chunks. */
    static void writeChunked(Ukcp ukcp, byte[] frame) {
        int offset = 0;
        while (offset < frame.length) {
            int n = Math.min(MAX_KCP_WRITE_BYTES, frame.length - offset);
            ByteBuf part = Unpooled.buffer(n);
            part.writeBytes(frame, offset, n);
            ukcp.write(part);
            part.release();
            offset += n;
        }
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
