/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.grpc;

import cloud.elastic.dlagent.api.model.RequestContext;
import cloud.elastic.dlagent.grpc.config.DlConfig;
import cloud.elastic.dlagent.grpc.wire.DlagentMethods;
import cloud.elastic.dlagent.service.RequestParser;
import cloud.elastic.dlagent.service.ServiceResult;
import cloud.elastic.dlagent.service.controller.ReadService;
import cloud.elastic.dlagent.transport.JsonResponseWriter;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.ClientCalls;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A data-plane stream over the real Netty transport runs at memory speed, not
 * at the pace of the writer's backpressure polling.
 *
 * <p>A scan is written in 64 KiB chunks, and every chunk can leave the call
 * briefly not ready. If waiting out that moment costs milliseconds, the
 * stream is capped at roughly chunk size per wait whatever the network and
 * the client can do.
 */
public class DataProxyThroughputTest {

    private static final int MIB = 1024 * 1024;
    private static final int TOTAL_MIB = 128;
    private static final int WARM_UP_MIB = 16;

    private volatile int streamMib;

    private DlAgentServer agent;
    private ManagedChannel channel;

    @AfterEach
    public void tearDown() throws Exception {
        if (channel != null) {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
        if (agent != null) {
            agent.shutdown();
        }
    }

    @Test
    public void aLoopbackStreamIsNotThrottledByBackpressurePolling() throws Exception {
        RequestParser<Map<String, List<String>>> parser = headers -> new RequestContext();
        ReadService scan = (context, out) -> {
            byte[] block = new byte[MIB];
            try {
                for (int i = 0; i < streamMib; i++) {
                    out.write(block);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
        DataProxyGrpcService service = new DataProxyGrpcService(parser, scan,
                new JsonResponseWriter(), new GrpcClientDisconnectPredicate());
        DlConfig config = DlConfig.load(new ByteArrayInputStream(
                        "server.address=localhost\ndlagent.grpc.port=0\n"
                                .getBytes(StandardCharsets.UTF_8)),
                Collections.emptyMap(), new String[0]);
        agent = new DlAgentServer(config, new HealthService(),
                new IcebergGrpcServices(
                        new StubOperations(ServiceResult.success(null), new ArrayList<>()),
                        new JsonResponseWriter(), new GrpcClientDisconnectPredicate()),
                service);
        agent.listen();
        agent.becomeReady();
        channel = NettyChannelBuilder.forAddress("localhost", agent.port())
                .usePlaintext().build();

        // One untimed stream first, so connection setup and JIT warm-up are
        // not counted against the transport.
        streamMib = WARM_UP_MIB;
        drain();
        streamMib = TOTAL_MIB;
        long start = System.nanoTime();
        long received = drain();
        double seconds = (System.nanoTime() - start) / 1e9;
        double mibPerSecond = received / (double) MIB / seconds;

        System.out.printf("DataProxyThroughputTest: %d MiB in %.2f s, %.1f MiB/s%n",
                TOTAL_MIB, seconds, mibPerSecond);
        assertEquals((long) TOTAL_MIB * MIB, received);
        assertTrue(mibPerSecond >= 100,
                String.format("%d MiB took %.2f s: %.1f MiB/s", TOTAL_MIB, seconds, mibPerSecond));
    }

    private long drain() {
        Iterator<byte[]> it = ClientCalls.blockingServerStreamingCall(channel,
                DlagentMethods.READ, CallOptions.DEFAULT.withDeadlineAfter(30, TimeUnit.SECONDS),
                "{}".getBytes(StandardCharsets.UTF_8));
        long received = 0;
        while (it.hasNext()) {
            received += it.next().length;
        }
        return received;
    }
}
