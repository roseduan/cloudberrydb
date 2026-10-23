/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.grpc.wire;

import cloud.elastic.dlagent.grpc.wire.HealthWire.ServingStatus;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Pins the hand-written health messages to bytes protoc produced from the
 * vendored {@code proto/health.proto}:
 *
 * <pre>
 *   printf 'status: SERVING' | protoc --proto_path=proto \
 *     --encode=grpc.health.v1.HealthCheckResponse proto/health.proto | xxd -p
 *   printf 'service: "dlagent"' | protoc --proto_path=proto \
 *     --encode=grpc.health.v1.HealthCheckRequest proto/health.proto | xxd -p
 * </pre>
 *
 * protoc 3.21.12, 2026-09-11. The C++ prober generates its client from that
 * same file, so a server framing these differently would answer in something
 * the prober cannot read and the agent would look permanently unready.
 */
public class HealthWireTest {

    @Test
    public void responseMatchesProtoc() {
        // UNKNOWN is the enum's default, so proto3 omits the field entirely.
        assertArrayEquals(new byte[0], HealthWire.encodeStatus(ServingStatus.UNKNOWN));
        assertArrayEquals(new byte[] {0x08, 0x01}, HealthWire.encodeStatus(ServingStatus.SERVING));
        assertArrayEquals(new byte[] {0x08, 0x02}, HealthWire.encodeStatus(ServingStatus.NOT_SERVING));
        assertArrayEquals(new byte[] {0x08, 0x03}, HealthWire.encodeStatus(ServingStatus.SERVICE_UNKNOWN));
    }

    @Test
    public void responseRoundTrips() {
        for (ServingStatus s : ServingStatus.values()) {
            assertEquals(s, HealthWire.decodeStatus(HealthWire.encodeStatus(s)));
        }
    }

    @Test
    public void requestMatchesProtoc() {
        assertArrayEquals(
                new byte[] {0x0a, 0x07, 'd', 'l', 'a', 'g', 'e', 'n', 't'},
                PayloadMarshaller.encode("dlagent".getBytes(StandardCharsets.UTF_8)));
        // The empty service name means "the whole server" and serializes to
        // nothing, same as any other default-valued proto3 field.
        assertArrayEquals(new byte[0], PayloadMarshaller.encode(new byte[0]));
    }
}
