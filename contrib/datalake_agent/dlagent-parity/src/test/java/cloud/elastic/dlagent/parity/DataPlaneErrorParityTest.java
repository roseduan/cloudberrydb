/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.parity;

import cloud.elastic.dlagent.api.error.DlRuntimeException;
import cloud.elastic.dlagent.api.model.RequestContext;
import cloud.elastic.dlagent.grpc.DataProxyGrpcService;
import cloud.elastic.dlagent.grpc.GrpcClientDisconnectPredicate;
import cloud.elastic.dlagent.grpc.ResponseTrailers;
import cloud.elastic.dlagent.grpc.wire.DlagentMethods;
import cloud.elastic.dlagent.service.RequestParser;
import cloud.elastic.dlagent.service.controller.ReadService;
import cloud.elastic.dlagent.service.rest.DlReadResource;
import cloud.elastic.dlagent.service.rest.IcebergExceptionHandler;
import cloud.elastic.dlagent.service.spring.DlExceptionHandler;
import cloud.elastic.dlagent.service.spring.SpringClientDisconnectPredicate;
import cloud.elastic.dlagent.transport.DlObjectMapperFactory;
import cloud.elastic.dlagent.transport.JsonResponseWriter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.ClientCalls;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.converter.ByteArrayHttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;

/**
 * A failed data-plane read, on both transports: REST's HTTP status and error
 * document against gRPC's {@code dlagent-status} and {@code dlagent-error-bin}
 * trailers.
 *
 * <p>The REST half is the real {@link DlReadResource} behind Spring MVC's
 * dispatcher with the agent's two controller advices, in the order the
 * running agent applies them (IcebergExceptionHandler answers a
 * DlRuntimeException; checked against a live agent). A read fails either
 * while the request is parsed, synchronously, or inside the streamed body,
 * which Spring runs asynchronously and re-dispatches; both are covered.
 *
 * <p>What may differ is {@code error.stack}, the server-side stack trace: the
 * frames above the failure are Spring's on one side and grpc-java's on the
 * other. Its first line, the exception reported, must match -- except that
 * for a java.lang.Error REST reports it wrapped in Spring's
 * NestedServletException, which the gRPC agent does not have on its
 * classpath, so there only the message after the class name is compared.
 */
class DataPlaneErrorParityTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private Server server;
    private ManagedChannel channel;

    @AfterEach
    void tearDown() throws Exception {
        if (channel != null) {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
        if (server != null) {
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    /** One transport's answer to the failed read. */
    private static final class Failure {
        final int status;
        final byte[] body;

        Failure(int status, byte[] body) {
            this.status = status;
            this.body = body;
        }

        @Override
        public String toString() {
            return "status=" + status + " body=" + new String(body, StandardCharsets.UTF_8);
        }
    }

    private static Failure rest(RequestParser<Map<String, List<String>>> parser, ReadService read)
            throws Exception {
        SpringClientDisconnectPredicate disconnects = new SpringClientDisconnectPredicate();
        MockMvc mvc = MockMvcBuilders
                .standaloneSetup(new DlReadResource(parser, read, disconnects))
                .setControllerAdvice(new IcebergExceptionHandler(), new DlExceptionHandler())
                .setMessageConverters(new ByteArrayHttpMessageConverter(),
                        new StringHttpMessageConverter(StandardCharsets.UTF_8),
                        new MappingJackson2HttpMessageConverter(DlObjectMapperFactory.create()))
                .build();
        MvcResult result = mvc.perform(get("/dlproxy/read")).andReturn();
        if (result.getRequest().isAsyncStarted()) {
            result.getAsyncResult(TimeUnit.SECONDS.toMillis(10));
            result = mvc.perform(asyncDispatch(result)).andReturn();
        }
        return new Failure(result.getResponse().getStatus(),
                result.getResponse().getContentAsByteArray());
    }

    private Failure grpc(RequestParser<Map<String, List<String>>> parser, ReadService read)
            throws Exception {
        DataProxyGrpcService service = new DataProxyGrpcService(parser, read,
                new JsonResponseWriter(), new GrpcClientDisconnectPredicate());
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name)
                .addService(service.definition())
                .intercept(ResponseTrailers.interceptor())
                .build().start();
        channel = InProcessChannelBuilder.forName(name).build();
        try {
            Iterator<byte[]> it = ClientCalls.blockingServerStreamingCall(channel,
                    DlagentMethods.READ, CallOptions.DEFAULT, new byte[0]);
            while (it.hasNext()) {
                it.next();
            }
        } catch (StatusRuntimeException e) {
            assertEquals(Status.Code.INTERNAL, e.getStatus().getCode(), e.toString());
            Metadata trailers = e.getTrailers();
            assertNotNull(trailers, "no trailers");
            String status = trailers.get(DlagentMethods.STATUS_KEY);
            byte[] body = trailers.get(DlagentMethods.ERROR_KEY);
            assertNotNull(status, "no " + DlagentMethods.STATUS_KEY.name() + " trailer: " + e);
            assertNotNull(body, "no " + DlagentMethods.ERROR_KEY.name() + " trailer: " + e);
            return new Failure(Integer.parseInt(status), body);
        }
        fail("the read did not fail");
        return null;
    }

    private void assertSame(String what, int expectedStatus,
                            RequestParser<Map<String, List<String>>> parser, ReadService read,
                            boolean wrappedError) throws Exception {
        Failure rest = rest(parser, read);
        Failure grpc = grpc(parser, read);

        assertEquals(rest.status, grpc.status,
                what + ": status differs\n  rest: " + rest + "\n  grpc: " + grpc);
        assertEquals(expectedStatus, rest.status, what + ": unexpected status " + rest);

        JsonNode restDoc = MAPPER.readTree(rest.body);
        JsonNode grpcDoc = MAPPER.readTree(grpc.body);
        String restStack = restDoc.path("error").path("stack").asText();
        String grpcStack = grpcDoc.path("error").path("stack").asText();
        assertTrue(!restStack.isEmpty() && !grpcStack.isEmpty(), what + ": a stack is missing");
        String restFirst = restStack.split("\n", 2)[0];
        String grpcFirst = grpcStack.split("\n", 2)[0];
        if (wrappedError) {
            assertEquals(restFirst.substring(restFirst.indexOf(':')),
                    grpcFirst.substring(grpcFirst.indexOf(':')),
                    what + ": the two transports reported different errors");
        } else {
            assertEquals(restFirst, grpcFirst,
                    what + ": the two transports reported different exceptions");
        }

        // Everything but the stack, byte for byte.
        ((ObjectNode) restDoc.get("error")).remove("stack");
        ((ObjectNode) grpcDoc.get("error")).remove("stack");
        assertEquals(MAPPER.writeValueAsString(restDoc), MAPPER.writeValueAsString(grpcDoc),
                what + ": error document differs\n  rest: " + rest + "\n  grpc: " + grpc);
        // And the bytes the stack does not touch: same key order, same escaping.
        assertEquals(withoutStack(rest.body), withoutStack(grpc.body),
                what + ": serialized bytes differ outside the stack");
    }

    /** The document's bytes with the stack's value cut out. */
    private static String withoutStack(byte[] body) {
        String s = new String(body, StandardCharsets.UTF_8);
        int key = s.indexOf("\"stack\":\"");
        int end = s.indexOf("\",\"", key);
        return s.substring(0, key) + s.substring(end);
    }

    private static final RequestParser<Map<String, List<String>>> PARSES =
            headers -> new RequestContext();

    @Test
    @DisplayName("a read that throws an unexpected exception: 500 on both")
    void readThrowsIllegalState() throws Exception {
        assertSame("IllegalStateException", 500, PARSES, (context, out) -> {
            throw new IllegalStateException("boom é中");
        }, false);
    }

    @Test
    @DisplayName("a read that throws NoSuchTableException: 404 on both")
    void readThrowsNoSuchTable() throws Exception {
        assertSame("NoSuchTableException", 404, PARSES, (context, out) -> {
            throw new NoSuchTableException("Table does not exist: db.t");
        }, false);
    }

    @Test
    @DisplayName("a read failing inside core (DlRuntimeException, as ReadServiceImpl throws)")
    void readThrowsWrapped() throws Exception {
        assertSame("DlRuntimeException", 500, PARSES, (context, out) -> {
            throw new DlRuntimeException(new NoSuchTableException("Table does not exist: db.t"));
        }, false);
    }

    @Test
    @DisplayName("a request that cannot be parsed: REST wraps it, so 500 on both")
    void parseFails() throws Exception {
        assertSame("parse failure", 500, headers -> {
            throw new IllegalArgumentException("Property PROFILE has no value in the request");
        }, (context, out) -> fail("the read ran"), false);
    }

    @Test
    @DisplayName("a read that throws a java.lang.Error: 500 on both")
    void readThrowsError() throws Exception {
        assertSame("NoClassDefFoundError", 500, PARSES, (context, out) -> {
            throw new NoClassDefFoundError("org/apache/iceberg/Missing");
        }, true);
    }

    @Test
    @DisplayName("a parser that throws a java.lang.Error: 500 on both")
    void parseThrowsError() throws Exception {
        assertSame("ExceptionInInitializerError", 500, headers -> {
            throw new ExceptionInInitializerError("static init failed");
        }, (context, out) -> fail("the read ran"), true);
    }
}
