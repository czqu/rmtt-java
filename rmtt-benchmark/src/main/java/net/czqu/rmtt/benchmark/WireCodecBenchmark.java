package net.czqu.rmtt.benchmark;

import net.czqu.rmtt.protocol.ByteArrayRmttByteReader;
import net.czqu.rmtt.protocol.FixedHeader;
import net.czqu.rmtt.protocol.PushMessage;
import net.czqu.rmtt.protocol.PushVariableHeader;
import net.czqu.rmtt.protocol.RmttMessage;
import net.czqu.rmtt.protocol.RmttMessageType;
import net.czqu.rmtt.protocol.RmttWireCodec;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * JMH microbenchmarks for the RMTT wire codec.
 *
 * <p>Covers the two hot paths of the shared codec ({@link RmttWireCodec}):
 *
 * <ul>
 *   <li>{@link #encodePush()} — encode a {@link PushMessage} into a standalone frame
 *       ({@code byte[]}), the path exercised by every downstream push.</li>
 *   <li>{@link #decodePush()} — decode a complete push frame back into a {@link RmttMessage},
 *       the path exercised by every received push.</li>
 * </ul>
 *
 * <p>Each benchmark runs with payload sizes of 16 B / 1 KiB / 8 KiB, selected through the
 * {@code payloadSize} parameter. Run the harness from the reactor root:
 *
 * <pre>{@code
 * mvn -pl rmtt-benchmark -am package
 * java -jar rmtt-benchmark/target/rmtt-benchmarks.jar
 * }</pre>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class WireCodecBenchmark {

    /**
     * Creates a benchmark instance. JMH requires a public no-arg constructor for
     * {@link State} classes; benchmark fields are populated by {@link #setup()}.
     */
    public WireCodecBenchmark() {
    }

    /**
     * Payload size in bytes for the push message under test: 16 B, 1 KiB and 8 KiB.
     */
    @Param({"16", "1024", "8192"})
    public int payloadSize;

    private byte[] payload;
    private PushMessage message;
    private byte[] frame;

    /**
     * Prepare the shared push message and its pre-encoded frame.
     */
    @Setup
    public void setup() {
        payload = new byte[payloadSize];
        ThreadLocalRandom.current().nextBytes(payload);
        message = new PushMessage(
                new FixedHeader(RmttMessageType.PUSH, false, false, false, false, 1 + payload.length),
                new PushVariableHeader((byte) 0), payload);
        frame = RmttWireCodec.encodeToBytes(message);
    }

    /**
     * Encode a push message into a standalone frame (ops/ms).
     *
     * @return the encoded frame bytes
     */
    @Benchmark
    public byte[] encodePush() {
        return RmttWireCodec.encodeToBytes(message);
    }

    /**
     * Decode a complete push frame into a message (ops/ms).
     *
     * @return the decoded message
     */
    @Benchmark
    public RmttMessage decodePush() {
        return RmttWireCodec.decode(new ByteArrayRmttByteReader(frame));
    }
}
