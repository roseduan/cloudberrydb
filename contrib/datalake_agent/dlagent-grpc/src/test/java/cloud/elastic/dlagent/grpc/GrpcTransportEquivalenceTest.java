/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.grpc;

import cloud.elastic.dlagent.grpc.wire.DlagentMethods;
import cloud.elastic.dlagent.service.ServiceResult;
import cloud.elastic.dlagent.transport.ErrorMapper;
import cloud.elastic.dlagent.transport.JsonResponseWriter;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.MetadataUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end over a real in-process gRPC stack: what a client receives must be
 * the bytes the REST transport would have put in its HTTP body, and the status
 * must be the classification of the same HTTP-shaped status code.
 *
 * <p>This is the Java half of the transport byte-identity check. It runs in
 * {@code mvn package} with no cluster and no sockets, so a divergence is
 * caught at build time rather than end to end; the C-side end-to-end diff still
 * has to happen, because only it covers the C++ client and the parsers above
 * agent_c_api.h.
 *
 * <p>The operations are stubbed rather than real: what is under test is the
 * transport shell -- marshalling, status mapping, streaming element
 * boundaries, metadata routing -- not Iceberg.
 */
public class GrpcTransportEquivalenceTest {

    private final JsonResponseWriter writer = new JsonResponseWriter();
    private Server server;
    private ManagedChannel channel;

    /** A service whose single method returns whatever the test set up. */
    private Channel serve(ServerServiceDefinition definition) throws Exception {
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name)
                .directExecutor()
                .addService(definition)
                .intercept(RoutingMetadata.interceptor())
                .build()
                .start();
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        return channel;
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (channel != null) {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
        if (server != null) {
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private IcebergGrpcServices servicesReturning(ServiceResult<Object> result,
                                                  List<String> routingSeen) {
        cloud.elastic.dlagent.service.iceberg.IcebergOperations ops =
                new StubOperations(result, routingSeen);
        return new IcebergGrpcServices(ops, writer, new GrpcClientDisconnectPredicate());
    }

    private static Metadata routing(String prefix, String table) {
        Metadata md = new Metadata();
        if (prefix != null) {
            md.put(DlagentMethods.PREFIX_KEY, prefix);
        }
        if (table != null) {
            md.put(DlagentMethods.TABLE_KEY, table);
        }
        return md;
    }

    private byte[] callUnary(Channel ch, io.grpc.MethodDescriptor<byte[], byte[]> m,
                             String json, Metadata md) {
        Channel withMd = io.grpc.ClientInterceptors.intercept(
                ch, MetadataUtils.newAttachHeadersInterceptor(md));
        return ClientCalls.blockingUnaryCall(withMd, m, CallOptions.DEFAULT,
                json.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * The core claim: for the same result, the bytes a gRPC client receives
     * are the bytes the REST body would have carried.
     */
    @Test
    public void successPayloadIsTheRestBody() throws Exception {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("metadata-location", "s3a://b/db/t/metadata/00001.metadata.json");
        body.put("current-snapshot-id", 7146290312398765432L);
        body.put("absent", null);
        ServiceResult<Object> result = ServiceResult.success((Object) body);

        Channel ch = serve(servicesReturning(result, new ArrayList<>()).icebergCatalog());
        byte[] got = callUnary(ch, DlagentMethods.LOAD_TABLE, "{\"namespace\":\"db\"}",
                routing("p", "t"));

        assertArrayEquals(writer.write(result), got,
                "the gRPC payload is not what the single serializer produced, so the "
                        + "two transports would hand the C side different documents");
        // And spelled out, because an int64 that survives Java but not the wire
        // is the exact failure mode measured for typed protobuf.
        assertTrue(new String(got, StandardCharsets.UTF_8).contains("7146290312398765432"),
                "the int64 snapshot id lost precision: " + new String(got, StandardCharsets.UTF_8));
    }

    /** A body-less success sends zero bytes, not the four bytes {@code null}. */
    @Test
    public void emptySuccessSendsNoBytes() throws Exception {
        Channel ch = serve(servicesReturning(ServiceResult.success(null), new ArrayList<>())
                .icebergCatalog());
        byte[] got = callUnary(ch, DlagentMethods.TABLE_EXISTS, "{\"namespace\":\"db\"}",
                routing("p", "t"));
        assertArrayEquals(new byte[0], got);
    }

    /**
     * A failure carries BOTH the gRPC status and the same error JSON the REST
     * path returns; the C side parses that body with
     * agent_cli_wrapper_check_exec_error_json() either way.
     */
    @Test
    public void errorsCarryStatusAndTheSameBody() throws Exception {
        ServiceResult<Object> notFound = ServiceResult.notFound("table", "db.t1");
        Channel ch = serve(servicesReturning(notFound, new ArrayList<>()).icebergCatalog());

        byte[] got = callUnary(ch, DlagentMethods.LOAD_TABLE, "{\"namespace\":\"db\"}",
                routing("p", "t"));
        assertArrayEquals(writer.write(notFound), got,
                "the error body differed between transports");
        assertEquals(Status.Code.NOT_FOUND,
                GrpcStatusMapper.toStatus(ErrorMapper.categorize(notFound)).getCode());
    }


    /** prefix and table reach the operation, from metadata rather than the body. */
    @Test
    public void routingValuesArriveFromMetadata() throws Exception {
        List<String> seen = new ArrayList<>();
        Channel ch = serve(servicesReturning(ServiceResult.success(null), seen).icebergCatalog());
        callUnary(ch, DlagentMethods.TABLE_EXISTS, "{\"namespace\":\"db\"}",
                routing("mycat", "mytable"));
        assertEquals(Arrays.asList("mycat", "mytable"), seen);
    }

    /**
     * The client percent-encodes the table name as it does for the REST path
     * segment; the operation must see it decoded as Spring decodes a path
     * variable, where '+' is a plus and not a space.
     */
    @Test
    public void routingValuesArePercentDecoded() throws Exception {
        List<String> seen = new ArrayList<>();
        Channel ch = serve(servicesReturning(ServiceResult.success(null), seen).icebergCatalog());
        callUnary(ch, DlagentMethods.TABLE_EXISTS, "{}",
                routing("my%5Fcat", "%E5%8C%97%E4%BA%AC%2Bx%20y"));
        assertEquals(Arrays.asList("my_cat", "\u5317\u4eac+x y"), seen);

        seen.clear();
        callUnary(ch, DlagentMethods.TABLE_EXISTS, "{}", routing(null, "a+b%25"));
        assertEquals(Arrays.asList(null, "a+b%"), seen);
    }

    /**
     * A payload REST would refuse to bind to {@code @RequestBody Map} is
     * answered as REST answers it: the 500 error document, through the same
     * mapper, rather than a bare gRPC error the C side has no body to read
     * from. PayloadParityTest compares the two transports directly.
     */
    @Test
    public void unreadablePayloadsAnswerWithTheErrorDocument() throws Exception {
        List<String> seen = new ArrayList<>();
        Channel ch = serve(servicesReturning(ServiceResult.success(null), seen).icebergCatalog());
        for (String payload : new String[] {"not json", "", "null", "[1]"}) {
            byte[] got = callUnary(ch, DlagentMethods.TABLE_EXISTS, payload, routing("p", "t"));
            com.fasterxml.jackson.databind.JsonNode error =
                    writer.objectMapper().readTree(got).get("error");
            assertTrue(error != null, "no error document for '" + payload + "'");
            assertEquals(500, error.get("code").asInt(), "for '" + payload + "'");
            assertEquals("InternalServerError", error.get("type").asText());
        }
        assertTrue(seen.isEmpty(), "the operation ran on an unreadable payload: " + seen);
    }
}
