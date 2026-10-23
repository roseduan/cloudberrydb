/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.parity;

import cloud.elastic.dlagent.grpc.GrpcClientDisconnectPredicate;
import cloud.elastic.dlagent.grpc.IcebergGrpcServices;
import cloud.elastic.dlagent.grpc.ResponseTrailers;
import cloud.elastic.dlagent.grpc.RoutingMetadata;
import cloud.elastic.dlagent.grpc.wire.DlagentMethods;
import cloud.elastic.dlagent.service.ServiceResult;
import cloud.elastic.dlagent.service.iceberg.IcebergOperations;
import cloud.elastic.dlagent.service.rest.IcebergExceptionHandler;
import cloud.elastic.dlagent.service.rest.IcebergRestController;
import cloud.elastic.dlagent.transport.DlObjectMapperFactory;
import cloud.elastic.dlagent.transport.JsonResponseWriter;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.MetadataUtils;
import org.springframework.http.MediaType;
import org.springframework.http.converter.ByteArrayHttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

/**
 * Runs one request through BOTH transports and hands back what each answered.
 *
 * <p>Both halves are the real thing. The REST half drives the actual
 * {@link IcebergRestController} through Spring MVC's own dispatcher and
 * message converters, with the actual {@link IcebergExceptionHandler}
 * installed as controller advice; the gRPC half runs the actual
 * {@link IcebergGrpcServices} on an in-process server with the actual
 * interceptors. Nothing about the answer is reconstructed by this class -- it
 * only carries the same input to both doors and reports what came back.
 *
 * <p>WHY MockMvc AND NOT A FULL SpringBootTest. Booting
 * {@code ServiceApplication} would also start {@code DlProcessMonitor}, whose
 * scheduled watchdog calls {@code System.exit(0)} within a second when
 * {@code /proc/<parent.pid>} does not look like a datalake proxy -- it would
 * take the surefire JVM with it. {@code CoreBeansTestConfiguration} in
 * dlagent-rest avoids the servlet layer for the same reason. A standalone
 * MockMvc gives the parts that decide the bytes (dispatcher, path variables,
 * argument resolvers, converters, controller advice) without the parts that
 * decide the process lifetime.
 *
 * <p>The converter list mirrors what the running agent has, and the ORDER is
 * load-bearing: {@code loadMetadataJson} answers with a {@code byte[]} that is
 * already JSON, so the byte-array converter has to come first or Jackson would
 * Base64-encode it -- the failure {@code JsonResponseWriter} exists to prevent
 * on the other side.
 */
public final class TransportHarness implements AutoCloseable {

    /** One transport's answer: the status it reported and the bytes it sent. */
    public static final class Answer {
        public final int status;
        public final byte[] body;
        public final String contentType;

        Answer(int status, byte[] body, String contentType) {
            this.status = status;
            this.body = body == null ? new byte[0] : body;
            this.contentType = contentType;
        }

        public String text() {
            return new String(body, StandardCharsets.UTF_8);
        }

        @Override
        public String toString() {
            return "status=" + status + " body=" + text();
        }
    }

    private final RecordingOperations operations;
    private final JsonResponseWriter writer = new JsonResponseWriter();
    private final MockMvc mockMvc;
    private final Server server;
    private final ManagedChannel channel;

    public TransportHarness(Supplier<ServiceResult<Object>> answer) throws Exception {
        this.operations = new RecordingOperations(answer);

        /* ---- the REST stack ------------------------------------------ */
        IcebergRestController controller = new IcebergRestController();
        ReflectionTestUtils.setField(controller, "operations", (IcebergOperations) operations);

        MappingJackson2HttpMessageConverter json =
                new MappingJackson2HttpMessageConverter(DlObjectMapperFactory.create());
        this.mockMvc = MockMvcBuilders.standaloneSetup(controller)
                .setControllerAdvice(new IcebergExceptionHandler())
                .setMessageConverters(new ByteArrayHttpMessageConverter(),
                                      new StringHttpMessageConverter(StandardCharsets.UTF_8),
                                      json)
                .build();

        /* ---- the gRPC stack ------------------------------------------ */
        IcebergGrpcServices services =
                new IcebergGrpcServices(operations, writer, new GrpcClientDisconnectPredicate());
        String name = InProcessServerBuilder.generateName();
        this.server = InProcessServerBuilder.forName(name)
                .directExecutor()
                .addService(services.icebergCatalog())
                .addService(services.catalogManagement())
                .intercept(RoutingMetadata.interceptor())
                .intercept(ResponseTrailers.interceptor())
                .build()
                .start();
        this.channel = InProcessChannelBuilder.forName(name).directExecutor().build();
    }

    public RecordingOperations operations() {
        return operations;
    }

    /**
     * A path segment as the C client escapes it: curl_easy_escape keeps
     * {@code [A-Za-z0-9-._~]} and writes every other UTF-8 byte as an
     * upper-case {@code %XX}. Both transports carry the table name in this
     * form (AgentClient::encode_path_segment), so the harness does too.
     */
    static String curlEscape(String segment) {
        if (segment == null) {
            return null;
        }
        StringBuilder out = new StringBuilder();
        for (byte b : segment.getBytes(StandardCharsets.UTF_8)) {
            int c = b & 0xFF;
            if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
                    || c == '-' || c == '.' || c == '_' || c == '~') {
                out.append((char) c);
            } else {
                out.append(String.format("%%%02X", c));
            }
        }
        return out.toString();
    }

    /**
     * What the REST transport answers for this request.
     *
     * <p>The URI is passed already encoded, not as a template, so MockMvc
     * neither re-encodes the {@code %} signs nor skips the decoding: Spring's
     * {@code UrlPathHelper} decodes the path variable exactly as it does
     * behind Tomcat. It decodes in the request's character encoding, which
     * the running agent forces to UTF-8 through spring-boot's
     * CharacterEncodingFilter; MockMvc has no such filter, so the request
     * says so itself.
     */
    public Answer rest(Op op, String prefix, String table, String requestJson) throws Exception {
        MvcResult result = mockMvc.perform(post(URI.create(op.url(prefix, curlEscape(table))))
                        .characterEncoding(StandardCharsets.UTF_8.name())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson.getBytes(StandardCharsets.UTF_8)))
                .andReturn();
        return new Answer(result.getResponse().getStatus(),
                          result.getResponse().getContentAsByteArray(),
                          result.getResponse().getContentType());
    }

    /**
     * What the gRPC transport answers for the same request.
     *
     * <p>The status comes out of the {@code dlagent-status} trailer, not out of
     * the gRPC status: a non-OK gRPC status would discard the body, and the C
     * client needs both -- which is exactly the arrangement this test exists to
     * keep honest.
     */
    public Answer grpc(Op op, String prefix, String table, String requestJson) {
        Metadata headers = new Metadata();
        if (prefix != null && !prefix.isEmpty()) {
            headers.put(DlagentMethods.PREFIX_KEY, prefix);
        }
        if (table != null && !table.isEmpty()) {
            headers.put(DlagentMethods.TABLE_KEY, curlEscape(table));
        }

        AtomicReference<Metadata> trailers = new AtomicReference<>();
        Channel intercepted = ClientInterceptors.intercept(channel,
                MetadataUtils.newAttachHeadersInterceptor(headers),
                MetadataUtils.newCaptureMetadataInterceptor(new AtomicReference<>(), trailers));

        try {
            byte[] body = ClientCalls.blockingUnaryCall(intercepted, op.method(),
                    CallOptions.DEFAULT, requestJson.getBytes(StandardCharsets.UTF_8));
            return new Answer(appStatus(trailers.get(), 200), body, null);
        }
        catch (StatusRuntimeException e) {
            // A transport-level rejection: no body, and the gRPC code is the
            // whole answer.
            return new Answer(appStatus(trailers.get(), -1), new byte[0],
                              e.getStatus().getCode().name());
        }
    }

    private static int appStatus(Metadata trailers, int fallback) {
        String status = trailers == null ? null : trailers.get(DlagentMethods.STATUS_KEY);
        return status == null ? fallback : Integer.parseInt(status);
    }

    @Override
    public void close() throws Exception {
        channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }
}
