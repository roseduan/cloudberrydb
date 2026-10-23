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
import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.ServerCallStreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * How a data-plane call ends: a stalled client still gets a status, a
 * cancelled one is recognised as a disconnect, and every call releases the
 * Iceberg catalogs its read built.
 */
public class DataProxyLifecycleTest {

    private DlAgentServer agent;
    private Server server;
    private ManagedChannel channel;

    @AfterEach
    public void tearDown() throws Exception {
        if (channel != null) {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
        if (server != null) {
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
        if (agent != null) {
            agent.shutdown();
        }
    }

    private static final RequestParser<Map<String, List<String>>> PARSER =
            headers -> new RequestContext();

    /* ---- a client that stops reading ----------------------------------- */

    /**
     * A client that stops reading without cancelling makes the read give up
     * after the stall timeout. The call must then END with a status: the
     * client is still there, and a call that is never closed leaves it
     * waiting forever once it resumes.
     */
    @Test
    public void aStalledClientIsToldTheReadWasAbandoned() throws Exception {
        CountDownLatch gaveUp = new CountDownLatch(1);
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        ReadService endless = (context, out) -> {
            byte[] chunk = new byte[64 * 1024];
            try {
                while (true) {
                    out.write(chunk);
                }
            } catch (IOException | RuntimeException e) {
                thrown.set(e);
                gaveUp.countDown();
                throw new RuntimeException(e);
            }
        };
        DataProxyGrpcService service = new DataProxyGrpcService(PARSER, endless,
                new JsonResponseWriter(), new GrpcClientDisconnectPredicate(), () -> { }, 500);
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

        // A fixed window, so HTTP/2 flow control stops the server promptly.
        channel = NettyChannelBuilder.forAddress("localhost", agent.port())
                .usePlaintext().flowControlWindow(65535).build();
        ClientCall<byte[], byte[]> call = channel.newCall(DlagentMethods.READ, CallOptions.DEFAULT);
        CompletableFuture<Status> closed = new CompletableFuture<>();
        call.start(new ClientCall.Listener<byte[]>() {
            @Override
            public void onClose(Status status, Metadata trailers) {
                closed.complete(status);
            }
        }, new Metadata());
        call.sendMessage("{}".getBytes(StandardCharsets.UTF_8));
        call.halfClose();
        // No request(): the client reads nothing, and does not cancel either.

        assertTrue(gaveUp.await(10, TimeUnit.SECONDS), "the read never noticed the stall");

        call.request(Integer.MAX_VALUE);
        Status status;
        try {
            status = closed.get(5, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            call.cancel("test over", null);
            fail("the call was never closed after the read gave up on a stalled client "
                    + "(the read failed with " + thrown.get() + ")");
            return;
        }
        assertEquals(Status.Code.DEADLINE_EXCEEDED, status.getCode(), status.toString());
    }

    /**
     * A read that gave up on a stalled client is released after ONE stall
     * timeout. Closing the stream must not wait for the same client again:
     * the handler thread and whatever the read holds would stay pinned for
     * twice the configured time.
     */
    @Test
    public void aStalledReadIsReleasedAfterOneStallTimeout() throws Exception {
        long stallTimeoutMs = 1000;
        AtomicReference<Long> readStarted = new AtomicReference<>();
        CountDownLatch released = new CountDownLatch(1);
        AtomicReference<Long> releasedAt = new AtomicReference<>();
        ReadService endless = (context, out) -> {
            readStarted.compareAndSet(null, System.nanoTime());
            byte[] chunk = new byte[64 * 1024];
            try {
                while (true) {
                    out.write(chunk);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
        DataProxyGrpcService service = new DataProxyGrpcService(PARSER, endless,
                new JsonResponseWriter(), new GrpcClientDisconnectPredicate(), () -> {
                    releasedAt.set(System.nanoTime());
                    released.countDown();
                }, stallTimeoutMs);
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
                .usePlaintext().flowControlWindow(65535).build();
        ClientCall<byte[], byte[]> call = channel.newCall(DlagentMethods.READ, CallOptions.DEFAULT);
        call.start(new ClientCall.Listener<byte[]>() {
        }, new Metadata());
        call.sendMessage("{}".getBytes(StandardCharsets.UTF_8));
        call.halfClose();
        // Never request(), never cancel: the client is simply stuck.

        try {
            assertTrue(released.await(10, TimeUnit.SECONDS), "the read was never released");
            long heldMs = TimeUnit.NANOSECONDS.toMillis(releasedAt.get() - readStarted.get());
            assertTrue(heldMs < stallTimeoutMs * 13 / 10,
                    "the handler was held " + heldMs + " ms for a " + stallTimeoutMs
                            + " ms stall timeout");
        } finally {
            call.cancel("test over", null);
        }
    }

    /* ---- a client that cancelled --------------------------------------- */

    /** A minimal observer whose cancellation state the test controls. */
    private static final class FakeObserver extends ServerCallStreamObserver<byte[]> {
        volatile boolean cancelled;

        @Override
        public boolean isCancelled() {
            return cancelled;
        }

        @Override
        public void setOnCancelHandler(Runnable onCancelHandler) {
        }

        @Override
        public void setCompression(String compression) {
        }

        @Override
        public boolean isReady() {
            return true;
        }

        @Override
        public void setOnReadyHandler(Runnable onReadyHandler) {
        }

        @Override
        public void disableAutoInboundFlowControl() {
        }

        @Override
        public void request(int count) {
        }

        @Override
        public void setMessageCompression(boolean enable) {
        }

        @Override
        public void onNext(byte[] value) {
        }

        @Override
        public void onError(Throwable t) {
        }

        @Override
        public void onCompleted() {
        }
    }

    /**
     * A cancelled read unwinds with an IOException -- the read path's own
     * catch blocks expect that type -- that the disconnect predicate
     * recognises, wrapped or not. Otherwise core logs every cancelled scan as
     * a failure with a stack trace.
     */
    @Test
    public void aCancelledReadIsClassifiedAsADisconnect() throws Exception {
        FakeObserver observer = new FakeObserver();
        observer.cancelled = true;
        GrpcChunkOutputStream stream = new GrpcChunkOutputStream(observer, 4);

        IOException e = assertThrows(IOException.class, () -> stream.write(new byte[16]));
        GrpcClientDisconnectPredicate predicate = new GrpcClientDisconnectPredicate();
        assertTrue(predicate.isClientDisconnect(e),
                "a client cancel is not recognised as a disconnect: " + e);
        assertTrue(predicate.isClientDisconnect(new RuntimeException(e)),
                "a wrapped client cancel is not recognised as a disconnect");
    }

    /* ---- per-request catalog release ----------------------------------- */

    private ManagedChannel serve(ReadService read, Runnable release) throws Exception {
        DataProxyGrpcService service = new DataProxyGrpcService(PARSER, read,
                new JsonResponseWriter(), new GrpcClientDisconnectPredicate(), release,
                GrpcChunkOutputStream.DEFAULT_STALL_TIMEOUT_MS);
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name).addService(service.definition())
                .intercept(DataProxyGrpcService.RequestMetadata.interceptor())
                .build().start();
        channel = InProcessChannelBuilder.forName(name).build();
        return channel;
    }

    private static void drain(ManagedChannel ch) {
        Iterator<byte[]> it = ClientCalls.blockingServerStreamingCall(ch, DlagentMethods.READ,
                CallOptions.DEFAULT, "{}".getBytes(StandardCharsets.UTF_8));
        while (it.hasNext()) {
            it.next();
        }
    }

    private static final ReadService WRITES_OK = (context, out) -> {
        try {
            out.write("ok".getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    };

    @Test
    public void releasesAfterEveryRead() throws Exception {
        List<String> order = Collections.synchronizedList(new ArrayList<>());
        ManagedChannel ch = serve((context, out) -> {
            order.add("read");
            WRITES_OK.readData(context, out);
        }, () -> order.add("release"));

        drain(ch);
        drain(ch);
        waitFor(() -> order.size() == 4);
        assertEquals(java.util.Arrays.asList("read", "release", "read", "release"), order);
    }

    @Test
    public void releasesWhenTheReadFails() throws Exception {
        List<String> order = Collections.synchronizedList(new ArrayList<>());
        ManagedChannel ch = serve((context, out) -> {
            throw new IllegalStateException("boom");
        }, () -> order.add("release"));

        assertThrows(io.grpc.StatusRuntimeException.class, () -> drain(ch));
        waitFor(() -> order.size() == 1);
        assertEquals(Collections.singletonList("release"), order);
    }

    @Test
    public void aFailingReleaseDoesNotFailTheRead() throws Exception {
        ManagedChannel ch = serve(WRITES_OK, () -> {
            throw new IllegalStateException("close failed");
        });
        drain(ch);
    }

    private static void waitFor(java.util.function.BooleanSupplier condition)
            throws InterruptedException {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean() && System.nanoTime() < until) {
            Thread.sleep(10);
        }
        assertNotNull(condition.getAsBoolean() ? Boolean.TRUE : null, "condition not reached");
    }
}
