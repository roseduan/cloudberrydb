/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.grpc;

import cloud.elastic.dlagent.grpc.config.DlConfig;
import cloud.elastic.dlagent.grpc.wire.HealthWire;
import cloud.elastic.dlagent.grpc.wire.HealthWire.ServingStatus;
import cloud.elastic.dlagent.service.ServiceResult;
import cloud.elastic.dlagent.transport.JsonResponseWriter;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.ClientCalls;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The readiness window: the port answers before the agent does, and the
 * {@code dlagent} slot is what tells them apart.
 *
 * <p>This is the behaviour datalake_proxy's readiness probe
 * ({@code waitForAgentReady()}) depends on, so it is worth proving over a
 * real socket rather than in-process: a probe that used the top-level slot
 * would see SERVING during the window and let the first query through to an
 * agent that cannot serve it.
 */
public class DlAgentServerStartupTest {

    private DlAgentServer server;
    private ManagedChannel channel;

    @AfterEach
    public void tearDown() throws Exception {
        if (channel != null) {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
        if (server != null) {
            server.shutdown();
        }
    }

    /**
     * A data-plane service whose collaborators are never reached: these tests
     * are about the readiness gate, and registering the real service is what
     * proves the server can be assembled with all three of them.
     */
    private DataProxyGrpcService dataProxy() {
        return new DataProxyGrpcService(null, null, new JsonResponseWriter(),
                new GrpcClientDisconnectPredicate());
    }

    private ServingStatus check(String slot) {
        return ClientCalls.blockingUnaryCall(channel, HealthWire.CHECK, CallOptions.DEFAULT, slot);
    }

    @Test
    public void portAnswersBeforeTheAgentIsReady() throws Exception {
        DlConfig config = DlConfig.load(
                new ByteArrayInputStream(
                        // Port 0: let the OS pick, so the test cannot collide
                        // with a real agent or another test run.
                        "server.address=localhost\ndlagent.grpc.port=0\n"
                                .getBytes(StandardCharsets.UTF_8)),
                Collections.emptyMap(), new String[0]);

        HealthService health = new HealthService();
        IcebergGrpcServices services = new IcebergGrpcServices(
                new StubOperations(ServiceResult.success(null), new ArrayList<>()),
                new JsonResponseWriter(), new GrpcClientDisconnectPredicate());

        server = new DlAgentServer(config, health, services, dataProxy());
        server.listen();

        assertTrue(server.port() > 0, "the server did not bind a port");
        channel = ManagedChannelBuilder.forAddress("localhost", server.port())
                .usePlaintext().build();

        // Phase one done: reachable, not ready.
        assertEquals(ServingStatus.NOT_SERVING, check(HealthService.DLAGENT_SLOT),
                "a client connecting during startup must be told NOT_SERVING");

        server.becomeReady();
        assertEquals(ServingStatus.SERVING, check(HealthService.DLAGENT_SLOT));

        server.shutdown();
        // The shutdown path puts the slot back, so a lingering connection sees
        // the agent leave rather than a silently failing call.
        assertEquals(ServingStatus.NOT_SERVING, health.status(HealthService.DLAGENT_SLOT));
    }

    /**
     * Started without {@code --parent.pid}, the agent must still come up: the
     * watchdog is disabled with a warning rather than refusing to start, or
     * the jar could not be run by hand at all. (datalake_proxy always passes
     * it.)
     */
    @Test
    public void startsWithoutAParentPid() throws Exception {
        DlConfig config = DlConfig.load(
                new ByteArrayInputStream("dlagent.grpc.port=0\n".getBytes(StandardCharsets.UTF_8)),
                Collections.emptyMap(), new String[0]);
        HealthService health = new HealthService();
        server = new DlAgentServer(config, health, new IcebergGrpcServices(
                new StubOperations(ServiceResult.success(null), new ArrayList<>()),
                new JsonResponseWriter(), new GrpcClientDisconnectPredicate()), dataProxy());
        server.listen();
        server.becomeReady();
        assertEquals(ServingStatus.SERVING, health.status(HealthService.DLAGENT_SLOT));
    }
}
