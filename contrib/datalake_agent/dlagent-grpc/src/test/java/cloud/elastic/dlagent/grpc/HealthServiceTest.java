/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.grpc;

import cloud.elastic.dlagent.grpc.wire.HealthWire;
import cloud.elastic.dlagent.grpc.wire.HealthWire.ServingStatus;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.ClientCalls;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.grpc.stub.ServerCallStreamObserver;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The readiness gate datalake_proxy's readiness probe checks, and the wire
 * format it answers in.
 *
 * <p>The golden bytes come from protoc run against the vendored
 * {@code proto/health.proto}:
 *
 * <pre>
 *   printf 'status: SERVING' | protoc --proto_path=proto \
 *     --encode=grpc.health.v1.HealthCheckResponse proto/health.proto | xxd -p
 * </pre>
 *
 * protoc 3.21.12, 2026-09-11. They matter because the C++ prober generates its
 * client from the same file: a hand-written server that framed these
 * differently would answer in something the prober cannot read, and the agent
 * would look permanently unready.
 */
public class HealthServiceTest {

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
    }

    private Channel serve(HealthService health) throws Exception {
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name).directExecutor()
                .addService(health.definition()).build().start();
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        return channel;
    }

    private ServingStatus check(Channel ch, String service) {
        return ClientCalls.blockingUnaryCall(ch, HealthWire.CHECK, CallOptions.DEFAULT, service);
    }

    /**
     * The window the two-phase startup exists to close: the server is
     * listening, so datalake_proxy sees a live process, but the agent is not
     * ready. Probing the top-level slot would say SERVING here, which is why
     * the probe must name {@code dlagent}.
     */
    @Test
    public void dlagentSlotIsNotServingUntilStartupCompletes() throws Exception {
        HealthService health = new HealthService();
        Channel ch = serve(health);

        assertEquals(ServingStatus.NOT_SERVING, check(ch, HealthService.DLAGENT_SLOT),
                "the dlagent slot must not be SERVING before startup finishes");
        assertEquals(ServingStatus.SERVING, check(ch, HealthService.SERVER_SLOT),
                "the top-level slot is SERVING as soon as the server is up, which is "
                        + "exactly why the probe must not use it");

        health.serving();
        assertEquals(ServingStatus.SERVING, check(ch, HealthService.DLAGENT_SLOT));

        health.notServing();
        assertEquals(ServingStatus.NOT_SERVING, check(ch, HealthService.DLAGENT_SLOT));
    }

    /**
     * A Watch the client cancelled is forgotten. Otherwise every probe that
     * ever watched stays referenced, and is written to on each status change,
     * for the life of the agent.
     */
    @Test
    public void aCancelledWatchIsForgotten() throws Exception {
        HealthService health = new HealthService();
        String name = InProcessServerBuilder.generateName();
        // Not directExecutor: the cancel must travel to the server as it does
        // over a socket.
        server = InProcessServerBuilder.forName(name)
                .addService(health.definition()).build().start();
        channel = InProcessChannelBuilder.forName(name).build();

        ClientCall<String, ServingStatus> call =
                channel.newCall(HealthWire.WATCH, CallOptions.DEFAULT);
        CountDownLatch first = new CountDownLatch(1);
        call.start(new ClientCall.Listener<ServingStatus>() {
            @Override
            public void onMessage(ServingStatus message) {
                first.countDown();
            }
        }, new Metadata());
        call.request(Integer.MAX_VALUE);
        call.sendMessage(HealthService.DLAGENT_SLOT);
        call.halfClose();

        assertTrue(first.await(5, TimeUnit.SECONDS), "the watch never answered");
        assertEquals(1, health.watcherCount(HealthService.DLAGENT_SLOT));

        call.cancel("probe done", null);
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (health.watcherCount(HealthService.DLAGENT_SLOT) != 0 && System.nanoTime() < until) {
            Thread.sleep(10);
        }
        assertEquals(0, health.watcherCount(HealthService.DLAGENT_SLOT),
                "a cancelled watch is still subscribed");
    }

    /** A slot nobody registered is SERVICE_UNKNOWN, not NOT_SERVING. */
    @Test
    public void unknownSlotIsDistinguishable() throws Exception {
        Channel ch = serve(new HealthService());
        assertEquals(ServingStatus.SERVICE_UNKNOWN, check(ch, "dlagnet"));
    }

    /** Records what a Watch call was sent; hooks the first SERVING it sees. */
    private static final class RecordingWatch extends ServerCallStreamObserver<ServingStatus> {
        final List<ServingStatus> seen = new CopyOnWriteArrayList<>();
        volatile Runnable onFirstServing;

        @Override
        public void onNext(ServingStatus value) {
            seen.add(value);
            Runnable hook = onFirstServing;
            if (value == ServingStatus.SERVING && hook != null) {
                onFirstServing = null;
                hook.run();
            }
        }

        ServingStatus last() {
            return seen.get(seen.size() - 1);
        }

        @Override
        public boolean isCancelled() {
            return false;
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
        public void onError(Throwable t) {
        }

        @Override
        public void onCompleted() {
        }
    }

    /**
     * The status a watcher is sent is read under the watcher's lock, so the
     * last one it receives is the slot's current status however two changes
     * interleave.
     *
     * <p>The interleaving is forced: thread B is inside the watcher's send of
     * SERVING when thread A also sets SERVING and has to wait for the lock;
     * while A waits, the slot turns NOT_SERVING. A status A had read before
     * taking the lock would be stale by the time A sends it.
     */
    @Test
    public void aWatcherEndsOnTheCurrentStatusWhenChangesInterleave() throws Exception {
        HealthService health = new HealthService();
        RecordingWatch watch = new RecordingWatch();
        health.watch(HealthService.DLAGENT_SLOT, watch);

        AtomicReference<Thread> a = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        watch.onFirstServing = () -> {
            Thread t = new Thread(health::serving, "health-A");
            a.set(t);
            t.start();
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (t.getState() != Thread.State.BLOCKED && System.nanoTime() < until) {
                Thread.yield();
            }
            if (t.getState() != Thread.State.BLOCKED) {
                failure.set(new AssertionError("thread A never waited for the watcher"));
            }
            // Re-entrant: this thread already holds the watcher's lock.
            health.notServing();
        };
        Thread b = new Thread(health::serving, "health-B");
        b.start();
        b.join(TimeUnit.SECONDS.toMillis(10));
        a.get().join(TimeUnit.SECONDS.toMillis(10));

        if (failure.get() != null) {
            throw new AssertionError(failure.get());
        }
        assertEquals(ServingStatus.NOT_SERVING, health.status(HealthService.DLAGENT_SLOT));
        assertEquals(health.status(HealthService.DLAGENT_SLOT), watch.last(),
                "the watcher was left on a stale status; it saw " + watch.seen);
    }
}
