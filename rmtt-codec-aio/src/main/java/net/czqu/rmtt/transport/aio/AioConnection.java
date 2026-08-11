package net.czqu.rmtt.transport.aio;

import net.czqu.rmtt.codec.aio.AioMessageDecoder;
import net.czqu.rmtt.protocol.ConnAckVariableHeader;
import net.czqu.rmtt.protocol.ConnectReturnCode;
import net.czqu.rmtt.protocol.DisconnectReturnCode;
import net.czqu.rmtt.protocol.FixedHeader;
import net.czqu.rmtt.protocol.RmttMessage;
import net.czqu.rmtt.protocol.RmttMessageFactory;
import net.czqu.rmtt.protocol.RmttMessageType;
import net.czqu.rmtt.protocol.RmttWireCodec;
import net.czqu.rmtt.protocol.RmttWireCodec.BadProtocolVersionViolation;
import net.czqu.rmtt.protocol.RmttWireCodec.MagicNumberViolation;
import net.czqu.rmtt.protocol.RmttWireCodec.ProtocolViolation;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * Layered AIO transport: raw TCP, optional TLS (via an injected {@link SSLEngine} factory) and
 * optional WebSocket framing. One RMTT packet = one WS binary frame. All handshakes complete before
 * {@link AioFrameHandler#onReady()} fires. Reads are accumulated in a reusable buffer; writes are
 * serialised through a queue.
 */
public final class AioConnection {

    private final AsynchronousSocketChannel channel;
    private final boolean clientSide;
    private final boolean wsEnabled;
    private final Supplier<SSLEngine> sslEngineFactory;
    private final AioFrameHandler handler;

    private final AioMessageDecoder frameDecoder = new AioMessageDecoder();
    private final WsFrameCodec.Decoder wsDecoder = new WsFrameCodec.Decoder();
    private final ConcurrentLinkedQueue<byte[]> writeQueue = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean writing = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();

    private final ByteBuffer readBuffer = ByteBuffer.allocateDirect(8192);
    private byte[] preWsBuffer = new byte[2048];
    private int preWsCount;

    private SSLEngine sslEngine;
    private ByteBuffer sslNetIn;
    private ByteBuffer sslApp;
    private boolean tlsReady;
    private boolean wsReady;
    private boolean readyNotified;
    private volatile long lastReadTime = System.currentTimeMillis();
    private String wsHost = "localhost";
    private int wsPort;
    private String wsPath = "/rmtt";

    /**
     * Create a connection over an already-accepted/connected channel.
     *
     * @param channel           the OS channel to multiplex
     * @param clientSide        true when this is the connecting side (client masks WS frames)
     * @param wsEnabled         true when frames travel inside a WebSocket binary stream
     * @param sslEngineFactory  TLS engine supplier, or null for plaintext
     * @param handler           callback surface for decoded messages and lifecycle events
     */
    public AioConnection(AsynchronousSocketChannel channel,
                         boolean clientSide,
                         boolean wsEnabled,
                         Supplier<SSLEngine> sslEngineFactory,
                         AioFrameHandler handler) {
        this.channel = channel;
        this.clientSide = clientSide;
        this.wsEnabled = wsEnabled;
        this.sslEngineFactory = sslEngineFactory;
        this.handler = handler;
    }

    /**
     * The underlying channel.
     *
     * @return the OS socket channel
     */
    public AsynchronousSocketChannel channel() {
        return channel;
    }

    /**
     * Time of the last read from this connection.
     *
     * @return epoch millis of the last read
     */
    public long lastReadTime() {
        return lastReadTime;
    }

    /**
     * Explicit WS Host header for client-side websocket upgrades.
     *
     * @param host the Host value hostname
     * @param port the Host value port
     */
    public void wsHost(String host, int port) {
        this.wsHost = host;
        this.wsPort = port;
    }

    /**
     * WebSocket endpoint path (default "/rmtt"). Must match the server's path.
     *
     * @param wsPath the endpoint path
     */
    public void wsPath(String wsPath) {
        this.wsPath = wsPath == null || wsPath.isEmpty() ? "/rmtt" : wsPath;
    }

    /** Kick off the connection: start TLS if configured, otherwise go straight to ready state. */
    public void start() {
        if (sslEngineFactory != null) {
            try {
                sslEngine = sslEngineFactory.get();
                if (clientSide) {
                    sslEngine.beginHandshake();
                    tlsStep();
                }
            } catch (SSLException e) {
                fail(e);
                return;
            }
        } else {
            afterTransportReady();
        }
        readLoop();
    }

    /**
     * Send one RMTT frame, applying the configured WS/TLS wrapping.
     *
     * @param rmtBytes the complete RMTT frame bytes
     */
    public void writeFrame(byte[] rmtBytes) {
        byte[] wire = wsEnabled ? WsFrameCodec.encodeFrame(rmtBytes, clientSide) : rmtBytes;
        if (tlsReady || sslEngine == null) {
            if (sslEngine != null) {
                writeTls(wire);
            } else {
                writeRaw(wire);
            }
        } else {
            writeRaw(wire);
        }
    }

    /**
     * Write raw bytes to the socket, bypassing WS encoding.
     *
     * @param bytes the bytes to write
     */
    public void writeRaw(byte[] bytes) {
        if (closed.get()) {
            return;
        }
        writeQueue.add(bytes);
        flushWrites();
    }

    private void flushWrites() {
        if (!writing.compareAndSet(false, true)) {
            return;
        }
        pumpWrites();
    }

    private void pumpWrites() {
        byte[] next = writeQueue.poll();
        if (next == null) {
            writing.set(false);
            if (!writeQueue.isEmpty()) {
                flushWrites();
            }
            return;
        }
        ByteBuffer bb = ByteBuffer.wrap(next);
        channel.write(bb, next, new CompletionHandler<Integer, byte[]>() {
            @Override
            public void completed(Integer result, byte[] attachment) {
                if (bb.hasRemaining()) {
                    channel.write(bb, attachment, this);
                } else {
                    pumpWrites();
                }
            }

            @Override
            public void failed(Throwable exc, byte[] attachment) {
                fail(exc);
            }
        });
    }

    private void writeTls(byte[] app) {
        try {
            ByteBuffer appBuf = ByteBuffer.wrap(app);
            while (appBuf.hasRemaining()) {
                ByteBuffer net = ByteBuffer.allocate(sslEngine.getSession().getPacketBufferSize());
                SSLEngineResult res = sslEngine.wrap(appBuf, net);
                net.flip();
                byte[] cipher = new byte[net.remaining()];
                net.get(cipher);
                writeRaw(cipher);
                switch (res.getStatus()) {
                    case OK:
                        break;
                    case BUFFER_OVERFLOW:
                        continue;
                    case CLOSED:
                        close();
                        return;
                    default:
                        return;
                }
            }
        } catch (SSLException e) {
            fail(e);
        }
    }

    // ------------------------------------------------------------------ inbound

    private void readLoop() {
        if (closed.get()) {
            return;
        }
        channel.read(readBuffer, null, new CompletionHandler<Integer, Void>() {
            @Override
            public void completed(Integer result, Void attachment) {
                if (result < 0) {
                    fail(new IOException("connection closed by peer"));
                    return;
                }
                lastReadTime = System.currentTimeMillis();
                readBuffer.flip();
                byte[] data = new byte[result];
                readBuffer.get(data);
                readBuffer.clear();
                onInbound(data);
                readLoop();
            }

            @Override
            public void failed(Throwable exc, Void attachment) {
                fail(exc);
            }
        });
    }

    private void onInbound(byte[] data) {
        if (sslEngine == null) {
            onPlain(data);
        } else {
            tlsInbound(data);
        }
    }

    private void tlsInbound(byte[] data) {
        try {
            int need = (sslNetIn == null ? 0 : sslNetIn.position()) + data.length + 1024;
            if (sslNetIn == null || sslNetIn.capacity() < need) {
                ByteBuffer nb = ByteBuffer.allocate(Math.max(8192, need));
                if (sslNetIn != null) {
                    sslNetIn.flip();
                    nb.put(sslNetIn);
                }
                sslNetIn = nb;
            }
            sslNetIn.put(data);
            sslNetIn.flip();
            sslApp = ByteBuffer.allocate(Math.max(8192, sslEngine.getSession().getApplicationBufferSize()));
            ByteBuffer appAcc = ByteBuffer.allocate(sslApp.capacity());
            boolean blocked = false;
            while (sslNetIn.hasRemaining() && !blocked) {
                sslApp.clear();
                int before = sslNetIn.position();
                SSLEngineResult res = sslEngine.unwrap(sslNetIn, sslApp);
                sslApp.flip();
                while (sslApp.hasRemaining()) {
                    if (appAcc.remaining() < sslApp.remaining()) {
                        ByteBuffer nb = ByteBuffer.allocate(appAcc.capacity() * 2);
                        appAcc.flip();
                        nb.put(appAcc);
                        appAcc = nb;
                    }
                    appAcc.put(sslApp);
                }
                boolean consumed = sslNetIn.position() > before;
                switch (res.getStatus()) {
                    case BUFFER_OVERFLOW:
                        sslApp = ByteBuffer.allocate(sslApp.capacity() * 2);
                        continue;
                    case CLOSED:
                        fail(new IOException("TLS connection closed"));
                        return;
                    case BUFFER_UNDERFLOW:
                        blocked = true;
                        break;
                    case OK:
                        switch (res.getHandshakeStatus()) {
                            case NEED_TASK: {
                                Runnable task;
                                while ((task = sslEngine.getDelegatedTask()) != null) {
                                    task.run();
                                }
                                if (!tlsReady) {
                                    tlsStep();
                                }
                                break;
                            }
                            case NEED_WRAP:
                                if (!tlsReady) {
                                    tlsStep();
                                }
                                break;
                            case NEED_UNWRAP:
                                if (!consumed) {
                                    blocked = true;
                                }
                                break;
                            case FINISHED:
                                if (!tlsReady) {
                                    tlsReady = true;
                                    afterTransportReady();
                                }
                                if (!consumed) {
                                    blocked = true;
                                }
                                break;
                            case NOT_HANDSHAKING:
                                if (!tlsReady) {
                                    tlsReady = true;
                                    afterTransportReady();
                                }
                                if (!consumed) {
                                    blocked = true;
                                }
                                break;
                            default:
                                blocked = true;
                                break;
                        }
                        break;
                }
            }
            if (!tlsReady) {
                tlsStep();
            }
            appAcc.flip();
            if (appAcc.hasRemaining()) {
                byte[] plain = new byte[appAcc.remaining()];
                appAcc.get(plain);
                onPlain(plain);
            }
            sslNetIn.compact();
        } catch (SSLException e) {
            fail(e);
        }
    }

    private void tlsStep() throws SSLException {
        if (sslEngine == null || tlsReady) {
            return;
        }
        while (true) {
            SSLEngineResult.HandshakeStatus hs = sslEngine.getHandshakeStatus();
            switch (hs) {
                case NEED_WRAP: {
                    ByteBuffer app = ByteBuffer.allocate(0);
                    ByteBuffer net = ByteBuffer.allocate(sslEngine.getSession().getPacketBufferSize());
                    SSLEngineResult res = sslEngine.wrap(app, net);
                    net.flip();
                    byte[] cipher = new byte[net.remaining()];
                    net.get(cipher);
                    writeRaw(cipher);
                    break;
                }
                case NEED_UNWRAP:
                    return;
                case NEED_TASK: {
                    Runnable task;
                    while ((task = sslEngine.getDelegatedTask()) != null) {
                        task.run();
                    }
                    break;
                }
                case FINISHED:
                case NOT_HANDSHAKING:
                    tlsReady = true;
                    afterTransportReady();
                    return;
                default:
                    return;
            }
        }
    }

    private void onPlain(byte[] data) {
        if (!wsEnabled) {
            if (!readyNotified) {
                readyNotified = true;
                handler.onReady();
            }
            feedRmt(data);
            return;
        }
        if (!wsReady) {
            appendPreWs(data);
            return;
        }
        wsDecoder.feed(data, data.length);
        drainWs();
    }

    private void appendPreWs(byte[] data) {
        int need = preWsCount + data.length;
        if (need > preWsBuffer.length) {
            byte[] nb = new byte[Math.max(preWsBuffer.length << 1, need)];
            System.arraycopy(preWsBuffer, 0, nb, 0, preWsCount);
            preWsBuffer = nb;
        }
        System.arraycopy(data, 0, preWsBuffer, preWsCount, data.length);
        preWsCount += data.length;
        int end = indexOfHeaderEnd(preWsBuffer, preWsCount);
        if (end < 0) {
            return;
        }
        String head = new String(preWsBuffer, 0, end, StandardCharsets.US_ASCII);
        wsReady = true;
        preWsCount = 0;
        if (clientSide) {
            if (!head.startsWith("HTTP/1.1 101")) {
                String firstLine = head.split("\\r?\\n", 2)[0];
                fail(new IOException("WebSocket handshake rejected: " + firstLine));
                return;
            }
        } else {
            String key = extractHeader(head, "sec-websocket-key");
            byte[] resp = WsFrameCodec.buildHandshakeResponse(key == null ? "" : key);
            if (sslEngine != null) {
                writeTls(resp);
            } else {
                writeRaw(resp);
            }
        }
        if (!readyNotified) {
            readyNotified = true;
            handler.onReady();
        }
        // any leftover after the header belongs to frames
        int leftover = preWsCount;
        preWsCount = 0;
        if (leftover > 0) {
            onPlain(new byte[0]);
        }
    }

    private void drainWs() {
        byte[] payload;
        while ((payload = wsDecoder.decodeOne()) != null) {
            feedRmt(payload);
        }
    }

    private void feedRmt(byte[] data) {
        frameDecoder.feed(data, data.length);
        RmttMessage msg;
        try {
            while ((msg = frameDecoder.decodeOne()) != null) {
                handler.onMessage(msg);
            }
        } catch (MagicNumberViolation e) {
            // magic wrong -> not RMTT at all, close without any RMTT message
            fail(e);
        } catch (BadProtocolVersionViolation e) {
            // bad version -> CONNACK(0x01) then close
            writeFrame(RmttWireCodec.encodeToBytes(
                    RmttMessageFactory.newMessage(
                            new FixedHeader(RmttMessageType.CONNACK, false, false, false, false, 3),
                            new ConnAckVariableHeader(ConnectReturnCode.CONNECT_BAD_PROTOCOL_VERSION, 0),
                            null)));
            fail(e);
        } catch (ProtocolViolation e) {
            // other violations -> DISCONNECT(0x04) then close
            writeFrame(RmttWireCodec.encodeToBytes(
                    RmttMessageFactory.disconnect(DisconnectReturnCode.PROTOCOL_VIOLATION)));
            fail(e);
        }
    }

    private static int indexOfHeaderEnd(byte[] buf, int len) {
        for (int i = 0; i + 3 < len; i++) {
            if (buf[i] == '\r' && buf[i + 1] == '\n' && buf[i + 2] == '\r' && buf[i + 3] == '\n') {
                return i;
            }
        }
        return -1;
    }

    private static String extractHeader(String head, String name) {
        for (String line : head.split("\r\n")) {
            int idx = line.indexOf(':');
            if (idx > 0 && line.substring(0, idx).trim().equalsIgnoreCase(name)) {
                return line.substring(idx + 1).trim();
            }
        }
        return null;
    }

    private void afterTransportReady() {
        if (wsEnabled && !wsReady) {
            if (clientSide) {
                byte[] req = WsFrameCodec.buildHandshakeRequest(wsHost, wsPort, wsPath);
                if (sslEngine != null) {
                    writeTls(req);
                } else {
                    writeRaw(req);
                }
            }
            return;
        }
        if (!readyNotified) {
            readyNotified = true;
            handler.onReady();
        }
    }

    /**
     * Close the connection if not already closed.
     */
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        try {
            channel.close();
        } catch (IOException ignored) {
        }
    }

    private void fail(Throwable cause) {
        close();
        try {
            handler.onClosed(cause);
        } catch (Exception ignored) {
        }
    }
}