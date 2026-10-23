/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.grpc;

import cloud.elastic.dlagent.grpc.wire.DlagentMethods;
import cloud.elastic.dlagent.grpc.config.DlConfig;
import cloud.elastic.dlagent.service.ServiceResult;
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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The server's half of the one message-size ceiling.
 *
 * <p>gRPC enforces a message limit on whichever side RECEIVES, and the two
 * receivers here are different implementations with the same 4 MiB default:
 * grpc++ on the client, grpc-java on the server. {@code grpc_transport.cpp}
 * raised the client's; for the whole migration nothing raised the server's,
 * so a REQUEST above 4 MiB failed with {@code RESOURCE_EXHAUSTED} under grpc
 * while the identical request succeeded under rest.
 *
 * <p>That is reachable, not theoretical. A request carries the whole file
 * list as a single message -- {@code FDW_serialize_file_list_to_json} emits
 * about 200 bytes per file -- so 4 MiB runs out at roughly 21,000 files,
 * while {@code datalake.iceberg_max_file_removals_per_vacuum} alone defaults
 * to 100,000.
 *
 * <p>These tests go through the real {@link DlAgentServer} over a real
 * socket, because that is the only thing that proves the limit the agent
 * actually binds with. An in-process server would not: the limit is applied
 * by the builder, and testing a builder we configured in the test would only
 * prove the test configured it.
 */
public class MaxMessageSizeTest {

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

    /** Starts a ready agent on an ephemeral port with the given extra config. */
    private void startAgent(String extraConfig) throws Exception {
        DlConfig config = DlConfig.load(
                new ByteArrayInputStream(
                        ("server.address=localhost\ndlagent.grpc.port=0\n" + extraConfig)
                                .getBytes(StandardCharsets.UTF_8)),
                Collections.emptyMap(), new String[0]);

        server = new DlAgentServer(config, new HealthService(),
                new IcebergGrpcServices(
                        new StubOperations(ServiceResult.success(null), new ArrayList<>()),
                        new JsonResponseWriter(), new GrpcClientDisconnectPredicate()),
                new DataProxyGrpcService(null, null, new JsonResponseWriter(),
                        new GrpcClientDisconnectPredicate()));
        server.listen();
        server.becomeReady();

        channel = ManagedChannelBuilder.forAddress("localhost", server.port())
                .usePlaintext()
                // The client must not be the one that refuses, or the test
                // would pass for the wrong reason.
                .maxInboundMessageSize(256 * 1024 * 1024)
                .build();
    }

    /**
     * A file list of the shape the FDW actually sends, sized to at least
     * {@code atLeastBytes}. Real JSON rather than padding, so the request is
     * something the service could parse.
     */
    private static String fileListOfAtLeast(int atLeastBytes) {
        StringBuilder sb = new StringBuilder(atLeastBytes + 4096);
        sb.append("{\"files\":[");
        for (int i = 0; sb.length() < atLeastBytes; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append("{\"filePath\":\"s3a://warehouse/db/tbl/data/")
              .append(String.format("%05d", i))
              .append("-0-a1b2c3d4-e5f6-7890-abcd-ef0123456789-00001.parquet\"")
              .append(",\"format\":\"PARQUET\",\"content\":\"DATA_FILE\"")
              .append(",\"fileSize\":134217728,\"recordCount\":1048576}");
        }
        sb.append("]}");
        return sb.toString();
    }

    private byte[] loadTable(String json) {
        Metadata md = new Metadata();
        md.put(DlagentMethods.PREFIX_KEY, "p");
        md.put(DlagentMethods.TABLE_KEY, "t");
        Channel withMd = ClientInterceptors.intercept(
                channel, MetadataUtils.newAttachHeadersInterceptor(md));
        return ClientCalls.blockingUnaryCall(withMd, DlagentMethods.LOAD_TABLE,
                CallOptions.DEFAULT, json.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * THE REGRESSION. Without {@code maxInboundMessageSize} on the builder
     * this fails with {@code RESOURCE_EXHAUSTED: gRPC message exceeds maximum
     * size 4194304}.
     */
    @Test
    public void acceptsARequestLargerThanTheGrpcDefault() throws Exception {
        startAgent("");

        String json = fileListOfAtLeast(6 * 1024 * 1024);
        assertTrue(json.length() > 4 * 1024 * 1024,
                "the probe payload must exceed grpc's 4 MiB default to mean anything");

        // No exception is the assertion: the server accepted and answered it.
        loadTable(json);
    }

    /**
     * And the ceiling is the SETTING, not a larger constant. datalake_proxy
     * passes this key from the datalake.rpc_max_message_size GUC, so an
     * operator who lowers it has to see the agent lower with it -- otherwise
     * "client and server take the same value" is a comment, not a mechanism.
     */
    @Test
    public void honoursTheConfiguredCeiling() throws Exception {
        startAgent("dlagent.rpc_max_message_size=1\n");

        // Comfortably inside 1 MB.
        loadTable(fileListOfAtLeast(256 * 1024));

        StatusRuntimeException e = assertThrows(StatusRuntimeException.class,
                () -> loadTable(fileListOfAtLeast(2 * 1024 * 1024)),
                "a request above the configured ceiling must be refused");
        assertEquals(Status.Code.RESOURCE_EXHAUSTED, e.getStatus().getCode());
    }
}
