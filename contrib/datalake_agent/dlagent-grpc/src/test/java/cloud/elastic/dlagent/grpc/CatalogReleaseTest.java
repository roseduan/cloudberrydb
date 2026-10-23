/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.grpc;

import cloud.elastic.dlagent.grpc.wire.DlagentMethods;
import cloud.elastic.dlagent.service.ServiceResult;
import cloud.elastic.dlagent.transport.JsonResponseWriter;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.MetadataUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Every call releases the Iceberg catalogs it built, after it has answered.
 *
 * <p>The gRPC half of dlagent-rest's IcebergCatalogReleaseFilter. Without it
 * the agent kept one GopherFileIO connection to gophermeta per request until
 * gophermeta ran out of file descriptors, and queries hung instead of failing.
 * Main fixed that for REST with a servlet filter; a gRPC agent without the
 * equivalent would have shipped with the leak.
 */
public class CatalogReleaseTest {

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

    private Channel serve(IcebergGrpcServices services) throws Exception {
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name).directExecutor()
                .addService(services.icebergCatalog())
                .intercept(RoutingMetadata.interceptor())
                .intercept(ResponseTrailers.interceptor())
                .build().start();
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        Metadata md = new Metadata();
        md.put(DlagentMethods.PREFIX_KEY, "p");
        md.put(DlagentMethods.TABLE_KEY, "t");
        return ClientInterceptors.intercept(channel, MetadataUtils.newAttachHeadersInterceptor(md));
    }

    private static byte[] call(Channel ch) {
        return ClientCalls.blockingUnaryCall(ch, DlagentMethods.TABLE_EXISTS, CallOptions.DEFAULT,
                "{\"namespace\":\"n\"}".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void releasesAfterEveryCall() throws Exception {
        AtomicInteger released = new AtomicInteger();
        Channel ch = serve(new IcebergGrpcServices(
                new StubOperations(ServiceResult.success(null), new ArrayList<>()),
                new JsonResponseWriter(), new GrpcClientDisconnectPredicate(),
                released::incrementAndGet));

        call(ch);
        call(ch);
        call(ch);
        assertEquals(3, released.get(), "one release per call");
    }

    @Test
    public void releasesWhenTheOperationThrows() throws Exception {
        AtomicInteger released = new AtomicInteger();
        StubOperations throwing = new StubOperations(ServiceResult.success(null), new ArrayList<>()) {
            @Override
            public ServiceResult<Object> tableExists(String prefix, String table, Map<String, Object> request) {
                throw new IllegalStateException("boom");
            }
        };
        Channel ch = serve(new IcebergGrpcServices(throwing, new JsonResponseWriter(),
                new GrpcClientDisconnectPredicate(), released::incrementAndGet));

        call(ch);   // answered with the mapped error document, not a bare gRPC error
        assertEquals(1, released.get(), "a failed call must release too");
    }

    @Test
    public void releasesOnlyAfterTheAnswerWasSent() throws Exception {
        List<String> order = new ArrayList<>();
        StubOperations recording = new StubOperations(ServiceResult.success(null), new ArrayList<>()) {
            @Override
            public ServiceResult<Object> tableExists(String prefix, String table, Map<String, Object> request) {
                order.add("operation");
                return ServiceResult.success(null);
            }
        };
        Channel ch = serve(new IcebergGrpcServices(recording, new JsonResponseWriter(),
                new GrpcClientDisconnectPredicate(), () -> order.add("release")));

        call(ch);
        assertEquals(2, order.size());
        assertEquals("operation", order.get(0));
        assertEquals("release", order.get(1));
    }

    @Test
    public void aFailingReleaseDoesNotFailTheCall() throws Exception {
        Channel ch = serve(new IcebergGrpcServices(
                new StubOperations(ServiceResult.success(null), new ArrayList<>()),
                new JsonResponseWriter(), new GrpcClientDisconnectPredicate(),
                () -> { throw new IllegalStateException("close failed"); }));

        byte[] answer = call(ch);
        assertTrue(answer != null, "the call is served even though cleanup threw");
    }
}
