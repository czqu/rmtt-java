# rmtt-java

RMTT (Reliable Message Transport for Things) is a lightweight, extensible device messaging protocol for IoT. This repository provides the Java implementation: a transport-agnostic wire codec plus full server and client stacks backed by **AIO (JDK NIO.2)** and **Netty**, supporting **TCP / TLS / WebSocket / WSS / KCP / QUIC**.

## Features

- Compact fixed header with varint remaining length, magic-verified CONNECT (`0x637a7175` = "czqu")
- Application-defined credentials carried in CONNECT; authentication is fully pluggable via `Authenticator`
- Server-owned keepalive: the server reads the client proposal from CONNECT and overrides it, returning the final value in CONNACK.ServerKeepalive
- Fixed-interval and **adaptive heartbeat** (the client probes the maximum sustainable interval and settles at ~90% of it)
- Transport-agnostic push API: `server.push(deviceId, payload)` works identically for every transport
- Session takeover, explicit kick with DISCONNECT return codes, keepalive timeout reaping
- Reconnect with exponential backoff and jitter on the client side
- Strict javadoc: builds fail on missing or malformed API documentation

## Modules

| Module | Description |
|---|---|
| `rmtt-core` | Netty-free shared protocol model, wire codec, and application APIs |
| `rmtt-codec-netty` | Netty adapters for the byte readers/writers plus decoder/encoder pipeline |
| `rmtt-server-netty` | Netty server with TCP / WebSocket / KCP / QUIC transports |
| `rmtt-client-netty` | Netty client with TCP / WebSocket / KCP / QUIC transports |
| `rmtt-codec-aio` | AIO (JDK NIO.2) message decoder plus WebSocket frame codec |
| `rmtt-server-aio` | AIO server supporting tcp / tls / ws / wss |
| `rmtt-client-aio` | AIO client supporting tcp / tls / ws / wss |

## Transport matrix

| Transport | Netty server | Netty client | AIO server | AIO client |
|---|---|---|---|---|
| TCP | yes | yes | yes | yes |
| TLS | no | no | yes | yes |
| WebSocket | yes | yes | yes | yes |
| WSS | yes | yes | yes | yes |
| KCP (UDP) | yes | yes | no | no |
| QUIC (UDP) | yes | yes | no | no |

## Getting started

### Maven

All artifacts are published to Maven Central under `net.czqu.rmtt`:

```xml
<dependency>
    <groupId>net.czqu.rmtt</groupId>
    <artifactId>rmtt-server-netty</artifactId>
    <version>1.0.1</version>
</dependency>

<dependency>
    <groupId>net.czqu.rmtt</groupId>
    <artifactId>rmtt-client-netty</artifactId>
    <version>1.0.1</version>
</dependency>
```

Use `rmtt-server-aio` / `rmtt-client-aio` for the AIO stack, or `rmtt-core` alone if you only need the wire codec.

### Server (Netty)

```java
RmttServer server = new RmttServerBuilder()
        .port(18883)
        .authenticator(credential -> AuthResult.allow("device-" + credential))
        .messageHandler((deviceId, payload) ->
                System.out.println("PUSH from " + deviceId + ": "
                        + new String(payload, StandardCharsets.UTF_8)))
        .build();

server.awaitStartup();

// downstream push, transport-agnostic
PushResult result = server.push("device-abc", "hello".getBytes(StandardCharsets.UTF_8));

// kick a device with a DISCONNECT return code
server.kick("device-abc", DisconnectReturnCode.NORMAL_DISCONNECT);
```

Additional listeners: `wsPort(...)`, `wssPort(...)` (requires a caller-provided `SslContext`), `kcpPort(...)`, `quicPort(...)` (requires a caller-provided `QuicSslContext`).

### Client (Netty)

```java
RmttClient client = new RmttClientBuilder()
        .host("127.0.0.1")
        .port(18883)
        .credential("abc")
        .pushHandler(payload ->
                System.out.println("PUSH: " + new String(payload, StandardCharsets.UTF_8)))
        .build();

ConnectReturnCode code = client.connect(); // throws InterruptedException, TimeoutException
client.push("hello".getBytes(StandardCharsets.UTF_8));
client.disconnect();
client.shutdown();
```

### AIO

The AIO stack follows the same pattern: `net.czqu.rmtt.server.aio.RmttServerBuilder` and `net.czqu.rmtt.client.aio.RmttClientBuilder`, with tcp / tls / ws / wss listeners (`tlsPort(...)`, `wssPort(...)` require a caller-provided `SSLContext`).

### Keepalive negotiation

The client proposes an interval in CONNECT (`keepAliveSeconds(...)`), or enables adaptive probing (`adaptiveHeartbeat(shortSeconds, maxSeconds)`). The server makes the final decision via `ServerKeepalivePolicy` and returns it in CONNACK.ServerKeepalive:

- `server_kp == 0`: keepalive-based liveness is disabled
- `server_kp > 0`: liveness is enforced; connections are reaped when no packet arrives within `1.5 × server_kp`

## Building

Requires JDK 8+ and Maven 3.9+.

```bash
mvn clean install
```

The `quic` profile (native quiche dependency) is active by default. Build without it on unsupported platforms:

```bash
mvn clean install -P!quic
```

## Publishing

```bash
mvn clean deploy -P release
```

The `release` profile signs every artifact with GPG (key/passphrase resolved from `~/.m2/settings.xml`). Deployment targets Sonatype Central Portal using credentials from `~/.m2/settings.xml` (`<server><id>central</id></server>`).

## License

[MIT](LICENSE)
