/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.grpc;

import cloud.elastic.dlagent.grpc.wire.HealthWire;
import cloud.elastic.dlagent.grpc.wire.HealthWire.ServingStatus;
import io.grpc.ServerMethodDefinition;
import io.grpc.ServerServiceDefinition;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Named;
import javax.inject.Singleton;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * The health service, and with it the agent's readiness gate.
 *
 * <p>TWO-PHASE STARTUP. The server starts listening
 * before it can serve, because binding the port is what tells
 * {@code datalake_proxy} the process is alive. Between those two moments the
 * {@code dlagent} slot answers NOT_SERVING, and only once configuration
 * validation, DI assembly, catalog plugin initialisation and Read/Write
 * registration have ALL succeeded does {@link #serving()} flip it. A client
 * that connects in the window gets a clean NOT_SERVING rather than a
 * half-initialised agent.
 *
 * <p>THE SLOT NAME MATTERS. datalake_proxy must probe {@code Check("dlagent")}
 * and not the empty top-level name: the top-level slot is SERVING as soon as
 * the server is up, which is exactly the window this design exists to close.
 * The probe itself is {@code waitForAgentReady()} in datalake_proxy.c.
 *
 * <p>Status changes are visible to {@code Watch} as well as {@code Check},
 * so a client can wait for readiness instead of polling.
 */
@Named
@Singleton
@Slf4j
public class HealthService {

    /** The per-service slot datalake_proxy probes. */
    public static final String DLAGENT_SLOT = "dlagent";

    /** The conventional whole-server slot. */
    public static final String SERVER_SLOT = "";

    private final Map<String, ServingStatus> statuses = new ConcurrentHashMap<>();
    private final Map<String, List<Watcher>> watchers = new ConcurrentHashMap<>();

    public HealthService() {
        // The server itself is up once this object exists; the agent is not.
        statuses.put(SERVER_SLOT, ServingStatus.SERVING);
        statuses.put(DLAGENT_SLOT, ServingStatus.NOT_SERVING);
    }

    /** Phase two completed: the agent can serve requests. */
    public void serving() {
        set(DLAGENT_SLOT, ServingStatus.SERVING);
    }

    /** The agent can no longer serve; used on shutdown and on init failure. */
    public void notServing() {
        set(DLAGENT_SLOT, ServingStatus.NOT_SERVING);
    }

    public ServingStatus status(String service) {
        return statuses.get(service);
    }

    /** How many Watch calls are subscribed to a slot. Visible for tests. */
    int watcherCount(String service) {
        return watchers.getOrDefault(service, Collections.emptyList()).size();
    }

    private void set(String service, ServingStatus status) {
        ServingStatus previous = statuses.put(service, status);
        if (previous != status) {
            log.info("health slot '{}' {} -> {}", service, previous, status);
        }
        for (Watcher watcher : watchers.getOrDefault(service, Collections.emptyList())) {
            watcher.sendCurrent();
        }
    }

    /**
     * One Watch call. {@link #sendCurrent} is serialized because a status
     * change and the watch's own first answer can come from different
     * threads, and a StreamObserver must not be called concurrently. The
     * status is read inside the lock: a value read before it could be
     * overtaken by a later change sent while this one waited, leaving the
     * watcher on a stale status.
     */
    private final class Watcher {
        private final String service;
        private final StreamObserver<ServingStatus> out;

        Watcher(String service, StreamObserver<ServingStatus> out) {
            this.service = service;
            this.out = out;
        }

        synchronized void sendCurrent() {
            ServingStatus status = statuses.get(service);
            try {
                out.onNext(status == null ? ServingStatus.SERVICE_UNKNOWN : status);
            } catch (RuntimeException e) {
                // A watcher that went away must not stop the others from being
                // told, nor fail the state change that triggered this.
                log.debug("dropping a health watcher for '{}'", service, e);
                remove();
            }
        }

        void remove() {
            List<Watcher> list = watchers.get(service);
            if (list != null) {
                list.remove(this);
            }
        }
    }

    public ServerServiceDefinition definition() {
        return ServerServiceDefinition.builder(HealthWire.SERVICE)
                .addMethod(ServerMethodDefinition.create(HealthWire.CHECK,
                        ServerCalls.asyncUnaryCall(this::check)))
                .addMethod(ServerMethodDefinition.create(HealthWire.WATCH,
                        ServerCalls.asyncServerStreamingCall(this::watch)))
                .build();
    }

    private void check(String service, StreamObserver<ServingStatus> out) {
        ServingStatus status = statuses.get(service);
        // An unknown slot is SERVICE_UNKNOWN, not NOT_SERVING: the two mean
        // different things to a prober -- "you asked about something that does
        // not exist" versus "it exists and is not ready" -- and conflating them
        // would make a typo in the probe look like an agent that never comes up.
        out.onNext(status == null ? ServingStatus.SERVICE_UNKNOWN : status);
        out.onCompleted();
    }

    /** Package-private for tests that drive a watch with their own observer. */
    void watch(String service, StreamObserver<ServingStatus> out) {
        Watcher watcher = new Watcher(service, out);
        // Forget the watch when the client goes away; nothing else would, and
        // the list would grow by one per probe for the life of the agent.
        ((ServerCallStreamObserver<ServingStatus>) out).setOnCancelHandler(watcher::remove);
        watchers.computeIfAbsent(service, k -> new CopyOnWriteArrayList<>()).add(watcher);
        watcher.sendCurrent();
    }
}
