/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.grpc;

import cloud.elastic.dlagent.api.model.RequestContext;
import cloud.elastic.dlagent.grpc.wire.DlagentMethods;
import cloud.elastic.dlagent.service.RequestParser;
import cloud.elastic.dlagent.service.controller.ReadService;
import cloud.elastic.dlagent.transport.JsonResponseWriter;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.ClientCalls;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The data plane is the only streaming in the contract, and the property that
 * matters is that the bytes survive the chunking: what
 * {@code ReadService.readData} writes is what the client concatenates, exactly
 * as libchurl delivers it over HTTP today.
 */
public class DataProxyStreamingTest {

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

    /** A read service that writes a fixed payload and records its context. */
    private static final class StubReadService implements ReadService {
        private final byte[] payload;
        final AtomicReference<RequestContext> seen = new AtomicReference<>();

        StubReadService(byte[] payload) {
            this.payload = payload;
        }

        @Override
        public void readData(RequestContext context, OutputStream out) {
            seen.set(context);
            try {
                out.write(payload);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private Channel serve(ReadService readService) throws Exception {
        RequestParser<Map<String, List<String>>> parser =
                (RequestParser<Map<String, List<String>>>) (RequestParser<?>)
                        (RequestParser<Map<String, List<String>>>) headers -> new RequestContext();
        DataProxyGrpcService service = new DataProxyGrpcService(
                parser, readService, new JsonResponseWriter(),
                new GrpcClientDisconnectPredicate());

        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name)
                .addService(service.definition())
                .intercept(DataProxyGrpcService.RequestMetadata.interceptor())
                .build()
                .start();
        channel = InProcessChannelBuilder.forName(name).build();
        return channel;
    }

    private byte[] readAll(Channel ch, String requestJson) {
        Iterator<byte[]> it = ClientCalls.blockingServerStreamingCall(
                ch, DlagentMethods.READ, CallOptions.DEFAULT,
                requestJson.getBytes(StandardCharsets.UTF_8));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        while (it.hasNext()) {
            byte[] chunk = it.next();
            out.write(chunk, 0, chunk.length);
        }
        return out.toByteArray();
    }

    /**
     * A payload several chunks long must arrive byte-identical. 200 KiB spans
     * four 64 KiB chunks plus a partial one, so it exercises the boundary
     * between a full buffer and a flush of the remainder -- where an
     * off-by-one drops or duplicates bytes silently, and the symptom would be
     * a corrupted parquet page rather than an error.
     */
    @Test
    public void aMultiChunkPayloadSurvivesChunking() throws Exception {
        byte[] payload = new byte[200 * 1024];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) (i % 253);
        }
        Channel ch = serve(new StubReadService(payload));
        assertArrayEquals(payload, readAll(ch, "{}"));
    }

    /** A payload smaller than one chunk still arrives, and exactly once. */
    @Test
    public void aShortPayloadArrivesInOneMessage() throws Exception {
        byte[] payload = "the quick brown fox".getBytes(StandardCharsets.UTF_8);
        Channel ch = serve(new StubReadService(payload));

        Iterator<byte[]> it = ClientCalls.blockingServerStreamingCall(
                ch, DlagentMethods.READ, CallOptions.DEFAULT, "{}".getBytes(StandardCharsets.UTF_8));
        List<byte[]> chunks = new ArrayList<>();
        while (it.hasNext()) {
            chunks.add(it.next());
        }
        assertEquals(1, chunks.size(), "a short payload should not be split");
        assertArrayEquals(payload, chunks.get(0));
    }

    /** An empty read completes cleanly rather than sending an empty message. */
    @Test
    public void anEmptyReadCompletesWithNoMessages() throws Exception {
        Channel ch = serve(new StubReadService(new byte[0]));
        assertEquals(0, readAll(ch, "{}").length);
    }

    /** The request body reaches the parser; the read path needs its config. */
    @Test
    public void theRequestBodyReachesTheParser() throws Exception {
        AtomicReference<String> body = new AtomicReference<>();
        RequestParser<Map<String, List<String>>> parser =
                new RequestParser<Map<String, List<String>>>() {
                    @Override
                    public RequestContext parseRequest(Map<String, List<String>> headers) {
                        return new RequestContext();
                    }

                    @Override
                    public RequestContext parseRequest(Map<String, List<String>> headers,
                                                       String requestBody) {
                        body.set(requestBody);
                        return new RequestContext();
                    }
                };
        DataProxyGrpcService service = new DataProxyGrpcService(
                parser, new StubReadService(new byte[] {1, 2, 3}), new JsonResponseWriter(),
                new GrpcClientDisconnectPredicate());
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name).addService(service.definition())
                .intercept(DataProxyGrpcService.RequestMetadata.interceptor())
                .build().start();
        channel = InProcessChannelBuilder.forName(name).build();

        readAll(channel, "{\"gopher\":{\"enabled\":true}}");
        assertTrue(body.get() != null && body.get().contains("gopher"),
                "the read path never saw its config: " + body.get());
    }
}
