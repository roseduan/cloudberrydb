/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.grpc;

import cloud.elastic.dlagent.api.model.RequestContext;
import cloud.elastic.dlagent.grpc.config.DlConfig;
import cloud.elastic.dlagent.grpc.wire.DlagentMethods;
import cloud.elastic.dlagent.grpc.wire.HealthWire;
import cloud.elastic.dlagent.grpc.wire.HealthWire.ServingStatus;
import cloud.elastic.dlagent.service.RequestParser;
import cloud.elastic.dlagent.service.ServiceResult;
import cloud.elastic.dlagent.service.controller.ReadService;
import cloud.elastic.dlagent.transport.JsonResponseWriter;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.MetadataUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Properties of the server {@link DlAgentServer} actually binds, over a real
 * Netty socket: the header-list ceiling, the readiness gate in front of the
 * services, and the executor handlers run on. Each of them is decided by the
 * builder, so an in-process server configured in the test would only prove
 * what the test configured.
 */
public class DlAgentServerTransportTest {

    private DlAgentServer server;
    private ManagedChannel channel;
    private final AtomicReference<Map<String, List<String>>> headersSeen = new AtomicReference<>();
    private final AtomicInteger calls = new AtomicInteger();

    @AfterEach
    public void tearDown() throws Exception {
        if (channel != null) {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
        if (server != null) {
            server.shutdown();
        }
    }

    private static DlConfig config(String extra) throws IOException {
        return DlConfig.load(new ByteArrayInputStream(
                        ("server.address=localhost\ndlagent.grpc.port=0\n" + extra)
                                .getBytes(StandardCharsets.UTF_8)),
                Collections.emptyMap(), new String[0]);
    }

    /** Listens, but does not become ready: the caller decides that. */
    private void listen(String extra, long operationDelayMs) throws Exception {
        RequestParser<Map<String, List<String>>> parser = headers -> {
            headersSeen.set(headers);
            return new RequestContext();
        };
        ReadService read = (context, out) -> {
            try {
                out.write("ok".getBytes(StandardCharsets.UTF_8));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
        StubOperations ops = new StubOperations(ServiceResult.success(null), new ArrayList<>()) {
            @Override
            public ServiceResult<Object> tableExists(String prefix, String table,
                                                     Map<String, Object> request) {
                calls.incrementAndGet();
                if (operationDelayMs > 0) {
                    try {
                        Thread.sleep(operationDelayMs);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                return ServiceResult.success(null);
            }
        };
        server = new DlAgentServer(config(extra), new HealthService(),
                new IcebergGrpcServices(ops, new JsonResponseWriter(),
                        new GrpcClientDisconnectPredicate()),
                new DataProxyGrpcService(parser, read, new JsonResponseWriter(),
                        new GrpcClientDisconnectPredicate()));
        server.listen();
        channel = ManagedChannelBuilder.forAddress("localhost", server.port())
                .usePlaintext().build();
    }

    private byte[] read(Metadata md) {
        Channel ch = ClientInterceptors.intercept(channel,
                MetadataUtils.newAttachHeadersInterceptor(md));
        Iterator<byte[]> it = ClientCalls.blockingServerStreamingCall(ch, DlagentMethods.READ,
                CallOptions.DEFAULT.withDeadlineAfter(10, TimeUnit.SECONDS),
                "{}".getBytes(StandardCharsets.UTF_8));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        while (it.hasNext()) {
            byte[] chunk = it.next();
            out.write(chunk, 0, chunk.length);
        }
        return out.toByteArray();
    }

    private byte[] tableExists() {
        Metadata md = new Metadata();
        md.put(DlagentMethods.TABLE_KEY, "t");
        Channel ch = ClientInterceptors.intercept(channel,
                MetadataUtils.newAttachHeadersInterceptor(md));
        return ClientCalls.blockingUnaryCall(ch, DlagentMethods.TABLE_EXISTS,
                CallOptions.DEFAULT.withDeadlineAfter(10, TimeUnit.SECONDS),
                "{}".getBytes(StandardCharsets.UTF_8));
    }

    /** About {@code count * 250} bytes of x-gp-attr-* entries, the shape a wide scan sends. */
    private static Metadata attributes(int count) {
        Metadata md = new Metadata();
        StringBuilder value = new StringBuilder();
        while (value.length() < 200) {
            value.append("column_name_with_a_long_type_description|");
        }
        for (int i = 0; i < count; i++) {
            md.put(Metadata.Key.of("x-gp-attr-name" + i, Metadata.ASCII_STRING_MARSHALLER),
                    value.toString() + i);
        }
        return md;
    }

    /* ---- header-list ceiling ------------------------------------------ */

    /**
     * Netty's default header-list limit is 8 KiB; REST accepts
     * server.max-http-header-size (1 MiB). A 45-column scan's attribute
     * headers are over 8 KiB and must reach the parser.
     */
    @Test
    public void acceptsMetadataBeyondNettysDefault() throws Exception {
        listen("", 0);
        server.becomeReady();

        assertArrayEquals("ok".getBytes(StandardCharsets.UTF_8), read(attributes(45)));
        assertNotNull(headersSeen.get());
        assertTrue(headersSeen.get().containsKey("x-gp-attr-name44"),
                "the last attribute header did not reach the parser");
    }

    /** The ceiling is the REST setting, not a larger constant. */
    @Test
    public void honoursTheConfiguredHeaderSize() throws Exception {
        listen("server.max-http-header-size=4096\n", 0);
        server.becomeReady();

        read(attributes(5));
        StatusRuntimeException e = assertThrows(StatusRuntimeException.class,
                () -> read(attributes(45)));
        assertTrue(e.getStatus().getCode() != Status.Code.OK);
    }

    /* ---- readiness gate ----------------------------------------------- */

    @Test
    public void servicesAnswerUnavailableUntilReady() throws Exception {
        listen("", 0);

        // Health is exempt: it is how the window is observed.
        assertEquals(ServingStatus.NOT_SERVING, ClientCalls.blockingUnaryCall(channel,
                HealthWire.CHECK, CallOptions.DEFAULT, HealthService.DLAGENT_SLOT));

        StatusRuntimeException catalog = assertThrows(StatusRuntimeException.class,
                this::tableExists, "a catalog call was served before the agent was ready");
        assertEquals(Status.Code.UNAVAILABLE, catalog.getStatus().getCode());
        assertEquals(0, calls.get(), "the operation ran before the agent was ready");

        StatusRuntimeException data = assertThrows(StatusRuntimeException.class,
                () -> read(new Metadata()));
        assertEquals(Status.Code.UNAVAILABLE, data.getStatus().getCode());

        server.becomeReady();
        tableExists();
        assertEquals(1, calls.get());
        assertArrayEquals("ok".getBytes(StandardCharsets.UTF_8), read(new Metadata()));
    }

    /* ---- executor ------------------------------------------------------ */

    @Test
    public void handlersRunOnABoundedExecutorSizedLikeTomcat() throws Exception {
        listen("", 0);
        ThreadPoolExecutor executor = server.executor();
        assertNotNull(executor, "the server runs handlers on grpc's unbounded cached pool");
        assertEquals(200, executor.getMaximumPoolSize());

        server.shutdown();
        assertTrue(executor.isShutdown(), "shutdown() left the handler executor running");
    }

    /** Calls beyond the thread count wait for a thread instead of failing. */
    @Test
    public void excessCallsQueue() throws Exception {
        listen("server.tomcat.threads.max=1\n", 300);
        server.becomeReady();
        assertEquals(1, server.executor().getMaximumPoolSize());

        ExecutorService clients = Executors.newFixedThreadPool(3);
        try {
            List<Future<byte[]>> answers = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                answers.add(clients.submit(this::tableExists));
            }
            for (Future<byte[]> answer : answers) {
                answer.get(10, TimeUnit.SECONDS);
            }
        } finally {
            clients.shutdownNow();
        }
        assertEquals(3, calls.get());
    }
}
