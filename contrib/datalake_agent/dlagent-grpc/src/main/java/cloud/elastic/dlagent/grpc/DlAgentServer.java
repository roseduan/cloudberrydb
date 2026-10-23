/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.grpc;

import cloud.elastic.dlagent.grpc.config.DlConfig;
import cloud.elastic.dlagent.service.ParentProcessWatchdog;
import cloud.elastic.dlagent.grpc.wire.HealthWire;
import cloud.elastic.dlagent.grpc.wire.HealthWire.ServingStatus;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.netty.NettyServerBuilder;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The gRPC agent's lifecycle: bind the port, then become ready.
 *
 * <p>TWO PHASES, IN THIS ORDER, and the order is the design:
 *
 * <ol>
 * <li><b>Listen.</b> Binding the port is what tells {@code datalake_proxy} the
 *     process came up; it watches for a live child, not for a healthy one.
 *     The {@code dlagent} health slot is NOT_SERVING throughout.
 * <li><b>Become ready.</b> Configuration is validated, the object graph is
 *     assembled, the catalog plugins initialise and the services register.
 *     Only when all of that has succeeded does the slot flip to SERVING.
 * </ol>
 *
 * <p>A client connecting in between gets a clean NOT_SERVING from the health
 * service, and any other call is answered UNAVAILABLE by
 * {@link #readinessGate} without reaching a half-initialised agent. Doing it the other way round -- ready first, then
 * listen -- would make the proxy's restart watchdog fire during a slow
 * catalog initialisation and put the agent in a restart loop.
 *
 * <p>If phase two throws, the slot stays NOT_SERVING and the process exits
 * non-zero rather than lingering as a listening socket that can never answer:
 * a proxy that sees the port open but every call failing has no way to tell
 * that restarting would help.
 */
@Slf4j
public final class DlAgentServer {

    /**
     * The gRPC port. Deliberately not 3888: the REST jar listens there, the two
     * transports must never be confused for each other, and a leftover process
     * from the other transport should fail to connect rather than half-work.
     */
    public static final int DEFAULT_PORT = 3889;

    /**
     * The default message ceiling in megabytes, matching
     * {@code DATALAKE_AGENT_DEFAULT_RPC_MAX_MESSAGE_SIZE_MB} in
     * {@code contrib/datalake_agent/dlagent_transport.h}.
     *
     * <p>It is a default and not the value: datalake_proxy passes
     * {@code --dlagent.rpc_max_message_size} on the command line from the
     * {@code datalake.rpc_max_message_size} GUC, so an agent launched by the
     * proxy always gets the same number the FDW's client uses. This constant
     * only covers an agent started by hand.
     */
    public static final int DEFAULT_MAX_MESSAGE_SIZE_MB = 64;

    /**
     * The header-list ceiling when {@code server.max-http-header-size} is not
     * configured: the value the shared application.properties gives Tomcat.
     */
    static final long DEFAULT_MAX_HEADER_SIZE = 1024 * 1024;

    /** Tomcat's {@code server.tomcat.threads.max} default, used the same way. */
    static final int DEFAULT_MAX_THREADS = 200;

    /** How long shutdown waits for calls in flight before cancelling them. */
    private static final long SHUTDOWN_GRACE_SECONDS = 5;

    private final DlConfig config;
    private final HealthService health;
    private final IcebergGrpcServices services;
    private final DataProxyGrpcService dataProxy;
    private final ScheduledExecutorService watchdogExecutor;
    private final ThreadPoolExecutor handlerExecutor;

    private Server server;

    public DlAgentServer(DlConfig config, HealthService health, IcebergGrpcServices services,
                         DataProxyGrpcService dataProxy) {
        this.config = config;
        this.health = health;
        this.services = services;
        this.dataProxy = dataProxy;
        this.watchdogExecutor = Executors.newSingleThreadScheduledExecutor(daemon("dlagent-watchdog"));
        this.handlerExecutor = handlerExecutor(
                config.getInt("server.tomcat.threads.max", DEFAULT_MAX_THREADS));
    }

    /**
     * The threads handlers run on, bounded as Tomcat's are under REST.
     *
     * <p>Without an executor grpc-java runs handlers on an unbounded cached
     * pool, and every handler here blocks -- on a catalog, on object storage,
     * on a slow reader -- so a burst of calls would be a burst of threads.
     *
     * <p>The queue is unbounded on purpose: a call beyond the thread count
     * waits for a thread, as a connection beyond Tomcat's threads waits in
     * its accept queue, instead of failing. A bounded queue would have to
     * reject the excess, and the C client does not retry a rejected call.
     * Health calls share the pool, so a pool saturated by stalled reads
     * delays a probe the same way a busy Tomcat delays its actuator.
     */
    private static ThreadPoolExecutor handlerExecutor(int threads) {
        AtomicInteger n = new AtomicInteger();
        ThreadPoolExecutor executor = new ThreadPoolExecutor(threads, threads,
                60, TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
                r -> daemon("dlagent-grpc-" + n.incrementAndGet()).newThread(r));
        // Idle threads go away, so the bound is a ceiling and not a cost.
        executor.allowCoreThreadTimeOut(true);
        return executor;
    }

    private static ThreadFactory daemon(String name) {
        return r -> {
            Thread t = new Thread(r, name);
            // Daemon so a failure to shut this down cannot keep a dead agent
            // alive; the watchdog's whole job is to end the process.
            t.setDaemon(true);
            return t;
        };
    }

    /** Phase one: bind the port. The agent is not ready when this returns. */
    public void listen() throws IOException {
        String address = config.get("server.address", "localhost");
        int port = config.getInt("dlagent.grpc.port", DEFAULT_PORT);

        /*
         * THE RECEIVING SIDE OF THE ONE CEILING.
         *
         * grpc enforces a message limit wherever the message is RECEIVED, so
         * this line is what bounds REQUESTS -- and without it the server sat
         * on grpc-java's 4 MiB default while grpc_transport.cpp had already
         * raised the client to 64 MB.  A request carries the whole file list
         * as a single message (FDW_serialize_file_list_to_json, about 200
         * bytes per file), so a commit of more than ~21,000 files failed with
         * RESOURCE_EXHAUSTED under grpc and succeeded under rest.  The two
         * halves now come from the same setting; dlagent_transport.h has the
         * full reasoning.
         *
         * Only inbound is set here.  A response is bounded by the CLIENT's
         * inbound limit, which grpc_transport.cpp sets from the same value.
         */
        int maxMessageSize = config.getInt("dlagent.rpc_max_message_size",
                DEFAULT_MAX_MESSAGE_SIZE_MB) * 1024 * 1024;

        /*
         * The header-list ceiling.  The data plane sends its parameters as
         * metadata, one x-gp-attr-* entry per column among them, and Netty's
         * 8 KiB default refuses a scan of a few dozen columns that REST,
         * with Tomcat's max-http-header-size, accepts.  Same setting, same
         * value.
         */
        long maxHeaderSize = config.getDataSize("server.max-http-header-size",
                DEFAULT_MAX_HEADER_SIZE);

        server = NettyServerBuilder.forAddress(new InetSocketAddress(address, port))
                .executor(handlerExecutor)
                .maxInboundMessageSize(maxMessageSize)
                .maxInboundMetadataSize((int) Math.min(Integer.MAX_VALUE, maxHeaderSize))
                .addService(health.definition())
                .addService(services.icebergCatalog())
                .addService(services.catalogManagement())
                .addService(dataProxy.definition())
                .intercept(RoutingMetadata.interceptor())
                .intercept(ResponseTrailers.interceptor())
                .intercept(DataProxyGrpcService.RequestMetadata.interceptor())
                // Last added runs first: nothing else sees a call it refuses.
                .intercept(readinessGate(health))
                .build()
                .start();
        log.info("dlagent gRPC listening on {}:{}, max message {} MB, max metadata {} bytes, "
                        + "{} handler threads (health slot '{}' is NOT_SERVING until startup "
                        + "completes)",
                address, port, maxMessageSize / (1024 * 1024), maxHeaderSize,
                handlerExecutor.getMaximumPoolSize(), HealthService.DLAGENT_SLOT);
    }

    /**
     * Answers every call but the health service's with UNAVAILABLE while the
     * {@code dlagent} slot is not SERVING: between listen() and becomeReady(),
     * and again once shutdown has begun. Health stays reachable because it is
     * how a client tells this window apart.
     */
    /**
     * The description of the gate's UNAVAILABLE. The C++ client
     * (grpc_agent_client.cpp, NOT_READY_DESCRIPTION) retries a call refused
     * with exactly this text, because the call provably did not run; any
     * other UNAVAILABLE may have run and is not replayed. Change both or
     * neither.
     */
    public static final String NOT_READY_DESCRIPTION = "dlagent is not ready";

    static ServerInterceptor readinessGate(HealthService health) {
        return new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> call, Metadata headers,
                    ServerCallHandler<ReqT, RespT> next) {
                if (HealthWire.SERVICE.equals(call.getMethodDescriptor().getServiceName())
                        || health.status(HealthService.DLAGENT_SLOT) == ServingStatus.SERVING) {
                    return next.startCall(call, headers);
                }
                call.close(Status.UNAVAILABLE.withDescription(NOT_READY_DESCRIPTION),
                        new Metadata());
                return new ServerCall.Listener<ReqT>() {
                };
            }
        };
    }

    /** Phase two: everything that has to work before a request may be served. */
    public void becomeReady() {
        startParentWatchdog();
        health.serving();
        log.info("dlagent gRPC ready");
    }

    /**
     * The same parent-process watchdog the REST jar runs; it must keep working
     * under both transports. Without it a dead datalake_proxy leaves this JVM
     * orphaned and the restarted proxy starts another one.
     */
    private void startParentWatchdog() {
        String parentPid = config.get("parent.pid");
        if (parentPid == null || parentPid.trim().isEmpty()) {
            // Started by hand rather than by datalake_proxy. Refusing to run
            // would make the jar untestable; running without the watchdog
            // silently would hide a proxy that forgot to pass the pid.
            log.warn("no --parent.pid given, the parent-process watchdog is disabled");
            return;
        }
        ParentProcessWatchdog watchdog = new ParentProcessWatchdog(parentPid.trim());
        watchdogExecutor.scheduleAtFixedRate(() -> {
            try {
                watchdog.checkOnce();
            } catch (RuntimeException e) {
                log.error("parent-process watchdog tick failed", e);
            }
        }, 1, 1, TimeUnit.SECONDS);
        Runtime.getRuntime().addShutdownHook(new Thread(watchdog::shutdown, "dlagent-watchdog-stop"));
    }

    public void awaitTermination() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public void shutdown() {
        health.notServing();
        watchdogExecutor.shutdownNow();
        if (server != null) {
            server.shutdown();
            /*
             * Calls in flight still post work to the handler executor, so it
             * may only stop once the server has: a bounded wait for them,
             * then cancel the stragglers -- a cancelled read notices within
             * one chunk.
             */
            try {
                if (!server.awaitTermination(SHUTDOWN_GRACE_SECONDS, TimeUnit.SECONDS)) {
                    server.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                server.shutdownNow();
            }
        }
        handlerExecutor.shutdown();
    }

    /** The executor handlers run on. Visible for tests. */
    ThreadPoolExecutor executor() {
        return handlerExecutor;
    }

    /** The bound port, for tests and for logging when the port was 0. */
    public int port() {
        return server == null ? -1 : server.getPort();
    }
}
