package net.czqu.rmtt.protocol;

import net.czqu.rmtt.protocol.RmttByteReader.Underflow;

import java.nio.charset.StandardCharsets;

/**
 * Transport-agnostic RMTT wire codec. Writes to / reads from any {@link RmttByteWriter}/{@link
 * RmttByteReader}, so each stack (netty, aio) stays in its own native buffer with no heap/direct
 * round-trips.
 */
public final class RmttWireCodec {

    private RmttWireCodec() {
    }

    /**
     * Hard protocol violation (bad magic, bad version, oversized frame, ...).
     */
    public static class ProtocolViolation extends RuntimeException {
        /**
         * Construct with a description of the violation.
         *
         * @param message the violation description
         */
        public ProtocolViolation(String message) {
            super(message);
        }
    }

    /**
     * CONNECT magic number mismatch: the peer is not speaking RMTT, so the connection
     * MUST be closed without sending any RMTT message.
     */
    public static final class MagicNumberViolation extends ProtocolViolation {
        /**
         * Construct with a description of the mismatch.
         *
         * @param message the violation description
         */
        public MagicNumberViolation(String message) {
            super(message);
        }
    }

    /**
     * CONNECT protocol version not supported: the server MUST respond with
     * CONNACK(0x01) and close the connection.
     */
    public static final class BadProtocolVersionViolation extends ProtocolViolation {
        /**
         * Construct with a description of the version mismatch.
         *
         * @param message the violation description
         */
        public BadProtocolVersionViolation(String message) {
            super(message);
        }
    }

    /**
     * Parse just the fixed header (type/flags byte + varint remaining length).
     *
     * @param in the read cursor positioned at the first byte of the packet
     * @return the parsed fixed header
     */
    public static FixedHeader decodeHeader(RmttByteReader in) {
        int typeAndFlags = in.readUnsignedByte();
        if ((typeAndFlags & 0x0F) != 0) {
            throw new ProtocolViolation("fixed header flags must be 0");
        }
        int length = RmttCodecUtil.decodeLength(in);
        if (length < 0) {
            throw new ProtocolViolation("remaining length exceeds " + RmttCodecUtil.MAX_LENGTH_BYTES + " bytes");
        }
        RmttMessageType type = RmttMessageType.valueOf((byte) (typeAndFlags >> 4));
        boolean f1 = (typeAndFlags & 0x08) != 0;
        boolean f2 = (typeAndFlags & 0x04) != 0;
        boolean f3 = (typeAndFlags & 0x02) != 0;
        boolean f4 = (typeAndFlags & 0x01) != 0;
        return new FixedHeader(type, f1, f2, f3, f4, length);
    }

    /**
     * Decode one complete frame (header + remaining-length bytes already present in the reader).
     *
     * @param in the read cursor positioned at the first byte of the frame
     * @return the decoded message
     */
    public static RmttMessage decode(RmttByteReader in) {
        return decodeBody(decodeHeader(in), in);
    }

    /**
     * Decode the variable part + payload for an already-parsed header, continuing from the reader's
     * current position. Used by transports that check frame completion up front.
     *
     * @param header the already-parsed fixed header
     * @param in     the read cursor positioned after the fixed header
     * @return the decoded message
     */
    public static RmttMessage decodeBody(FixedHeader header, RmttByteReader in) {
        switch (header.messageType()) {
            case CONNECT: {
                int magic = in.readInt();
                if (magic != RmttProtocol.CONNECT_MAGIC_NUMBER) {
                    throw new MagicNumberViolation("bad CONNECT magic 0x" + Integer.toHexString(magic));
                }
                int version = in.readUnsignedByte();
                if (RmttVersion.valueOf(version) == null) {
                    throw new BadProtocolVersionViolation("unsupported protocol version " + version);
                }
                int reserve = in.readUnsignedByte();
                int keepalive = in.readUnsignedShort();
                String credential = in.readString();
                byte[] payload = credential == null ? null
                        : credential.getBytes(StandardCharsets.UTF_8);
                ConnectVariableHeader vh = new ConnectVariableHeader(
                        magic, version, (byte) reserve, keepalive);
                return new ConnectMessage(header, vh, payload);
            }
            case CONNACK: {
                int code = in.readUnsignedByte();
                int serverKeepalive = in.readUnsignedShort();
                ConnAckVariableHeader vh = new ConnAckVariableHeader(
                        ConnectReturnCode.valueOf((byte) code), serverKeepalive);
                return new ConnAckMessage(header, vh, null);
            }
            case PUSH: {
                int reserve = in.readUnsignedByte();
                // consume exactly the frame-declared payload length: the reader may hold
                // additional concatenated frames, so reading readableBytes() would swallow
                // the next packets into this payload
                int payloadLen = header.remainingLength() - 1;
                byte[] payload = payloadLen > 0 ? in.readBytes(payloadLen) : new byte[0];
                return new PushMessage(header, new PushVariableHeader((byte) reserve), payload);
            }
            case DISCONNECT: {
                int code = in.readUnsignedByte();
                DisconnectVariableHeader vh = new DisconnectVariableHeader((byte) code);
                return new DisconnectMessage(header, vh, null);
            }
            case PINGREQ:
                return RmttMessageFactory.PINGREQ;
            case PINGRESP:
                return RmttMessageFactory.PINGRESP;
            default:
                throw new ProtocolViolation("unsupported packet type " + header.messageType());
        }
    }

    /**
     * Encode a message into a standalone {@code byte[]} frame (shared push path).
     *
     * @param message the message to encode
     * @return the complete frame bytes
     */
    public static byte[] encodeToBytes(RmttMessage message) {
        ByteArrayRmttByteWriter out = new ByteArrayRmttByteWriter(64);
        encode(message, out);
        return out.toByteArray();
    }

    /**
     * Encode a message into the writer (one pass, into the transport's native buffer).
     *
     * @param message the message to encode
     * @param out     the write cursor
     */
    public static void encode(RmttMessage message, RmttByteWriter out) {
        if (message instanceof ConnectMessage) {
            encodeConnect((ConnectMessage) message, out);
            return;
        }
        if (message instanceof ConnAckMessage) {
            encodeConnAck((ConnAckMessage) message, out);
            return;
        }
        if (message instanceof PushMessage) {
            encodePush((PushMessage) message, out);
            return;
        }
        if (message instanceof DisconnectMessage) {
            encodeDisconnect((DisconnectMessage) message, out);
            return;
        }
        if (message.fixedHeader().messageType() == RmttMessageType.PINGREQ
                || message.fixedHeader().messageType() == RmttMessageType.PINGRESP) {
            out.writeByte(message.fixedHeader().firstByte());
            out.writeByte(0); // remaining length 0
            return;
        }
        throw new IllegalArgumentException("cannot encode " + message);
    }

    private static void encodeConnect(ConnectMessage m, RmttByteWriter out) {
        ConnectVariableHeader vh = m.variableHeader();
        ConnectPayload payload = m.connectPayload();
        byte[] credential = (payload == null || payload.credential() == null)
                ? new byte[0] : payload.credential().getBytes(StandardCharsets.UTF_8);
        int bodyLen = 4 + 1 + 1 + 2 + 2 + credential.length;
        out.writeByte(m.fixedHeader().firstByte());
        RmttCodecUtil.encodeLength(out, bodyLen);
        out.writeInt(vh.magicNumber());
        out.writeByte(vh.version());
        out.writeByte(vh.reserve());
        out.writeUnsignedShort(vh.keepAliveTimeSeconds());
        out.writeUnsignedShort(credential.length);
        out.writeBytes(credential);
    }

    private static void encodeConnAck(ConnAckMessage m, RmttByteWriter out) {
        out.writeByte(m.fixedHeader().firstByte());
        RmttCodecUtil.encodeLength(out, 3);
        out.writeByte(m.variableHeader().connectReturnCode().byteValue());
        out.writeUnsignedShort(m.variableHeader().serverKeepaliveSeconds());
    }

    private static void encodePush(PushMessage m, RmttByteWriter out) {
        byte[] payload = m.payload();
        int bodyLen = 1 + (payload == null ? 0 : payload.length);
        out.writeByte(m.fixedHeader().firstByte());
        RmttCodecUtil.encodeLength(out, bodyLen);
        out.writeByte(m.variableHeader().reserve());
        if (payload != null) {
            out.writeBytes(payload);
        }
    }

    private static void encodeDisconnect(DisconnectMessage m, RmttByteWriter out) {
        out.writeByte(m.fixedHeader().firstByte());
        RmttCodecUtil.encodeLength(out, 1);
        out.writeByte(m.variableHeader().returnCode());
    }
}