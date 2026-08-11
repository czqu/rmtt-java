package net.czqu.rmtt.server.netty;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import net.czqu.rmtt.api.DeviceConnection;
import net.czqu.rmtt.protocol.DisconnectReturnCode;
import net.czqu.rmtt.protocol.RmttMessageFactory;

/** {@link DeviceConnection} backed by a netty {@link Channel}. */
public final class NettyDeviceConnection implements DeviceConnection {

    private final Channel channel;

    /**
     * Wrap an active channel.
     *
     * @param channel the netty channel
     */
    public NettyDeviceConnection(Channel channel) {
        this.channel = channel;
    }

    /**
     * The underlying channel.
     *
     * @return the netty channel
     */
    public Channel channel() {
        return channel;
    }

    @Override
    public boolean isActive() {
        return channel.isActive();
    }

    @Override
    public boolean write(byte[] frame) {
        if (!channel.isActive()) {
            return false;
        }
        channel.writeAndFlush(Unpooled.wrappedBuffer(frame), channel.voidPromise());
        return true;
    }

    @Override
    public void sendDisconnect(DisconnectReturnCode code) {
        if (channel.isActive()) {
            channel.writeAndFlush(RmttMessageFactory.disconnect(code))
                    .addListener(io.netty.channel.ChannelFutureListener.CLOSE);
        } else {
            channel.close();
        }
    }

    @Override
    public void close() {
        channel.close();
    }
}