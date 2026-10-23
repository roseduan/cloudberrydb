/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.grpc;

import cloud.elastic.dlagent.api.model.FileListRequest;
import cloud.elastic.dlagent.api.model.RequestContext;
import cloud.elastic.dlagent.grpc.wire.DlagentMethods;
import cloud.elastic.dlagent.plugins.iceberg.IcebergCatalogWrapper;
import cloud.elastic.dlagent.service.RequestParser;
import cloud.elastic.dlagent.service.ServiceResult;
import cloud.elastic.dlagent.service.controller.DlErrorReporter;
import cloud.elastic.dlagent.service.controller.ReadService;
import cloud.elastic.dlagent.service.utilities.ThrowingSupplier;
import cloud.elastic.dlagent.transport.ClientDisconnectPredicate;
import cloud.elastic.dlagent.transport.IcebergExceptionMapper;
import cloud.elastic.dlagent.transport.JsonResponseWriter;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerMethodDefinition;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The data plane: the gRPC counterpart of {@code DlReadResource}, replacing the
 * {@code libchurl.h} byte-stream boundary.
 *
 * <p>This is the only place in the contract where streaming is real. A scan
 * produces an unbounded sequence of bytes, so a message here is a CHUNK of that
 * stream with no structure of its own, and the client concatenates them --
 * exactly what libchurl does over HTTP today, which is what keeps the data
 * plane byte-identical between the transports.
 *
 * <p>The request parameters travel as gRPC metadata, because that is the shape
 * they already have: the REST resource parses them from HTTP headers with
 * {@code RequestParser<Map<String, List<String>>>}, and gRPC metadata is the
 * same multimap. The parser is therefore reused unchanged rather than
 * reimplemented.
 *
 * <p>Streaming is on the RESPONSE side only. Both upload paths in
 * {@code datalakeDoRPC_once} (protocol.c) POST a bounded JSON body and then
 * stream the reply back, so there is nothing for client-streaming to carry.
 */
@Named
@Singleton
@Slf4j
public class DataProxyGrpcService {

    private final RequestParser<Map<String, List<String>>> parser;
    private final ReadService readService;
    private final JsonResponseWriter writer;
    private final GrpcClientDisconnectPredicate disconnects;
    private final Runnable releaseRequestResources;
    private final long stallTimeoutMs;
    private final RequestErrors requestErrors;

    @Inject
    public DataProxyGrpcService(RequestParser<Map<String, List<String>>> parser,
                                ReadService readService,
                                JsonResponseWriter writer,
                                GrpcClientDisconnectPredicate disconnects,
                                IcebergCatalogWrapper catalogWrapper) {
        this(parser, readService, writer, disconnects, catalogWrapper::releaseRequestCatalogs,
                GrpcChunkOutputStream.DEFAULT_STALL_TIMEOUT_MS);
    }

    /** Without per-request release: for tests whose read path builds no catalog. */
    public DataProxyGrpcService(RequestParser<Map<String, List<String>>> parser,
                                ReadService readService,
                                JsonResponseWriter writer,
                                GrpcClientDisconnectPredicate disconnects) {
        this(parser, readService, writer, disconnects, () -> { },
                GrpcChunkOutputStream.DEFAULT_STALL_TIMEOUT_MS);
    }

    DataProxyGrpcService(RequestParser<Map<String, List<String>>> parser,
                         ReadService readService,
                         JsonResponseWriter writer,
                         GrpcClientDisconnectPredicate disconnects,
                         Runnable releaseRequestResources,
                         long stallTimeoutMs) {
        this.parser = parser;
        this.readService = readService;
        this.writer = writer;
        this.disconnects = disconnects;
        this.releaseRequestResources = releaseRequestResources;
        this.stallTimeoutMs = stallTimeoutMs;
        this.requestErrors = new RequestErrors(disconnects);
    }

    public ServerServiceDefinition definition() {
        return ServerServiceDefinition.builder(DlagentMethods.DATA_PROXY)
                .addMethod(method(DlagentMethods.READ, false))
                .addMethod(method(DlagentMethods.WRITE, true))
                .build();
    }

    private ServerMethodDefinition<byte[], byte[]> method(
            MethodDescriptor<byte[], byte[]> descriptor, boolean withFileList) {
        return ServerMethodDefinition.create(descriptor,
                ServerCalls.asyncServerStreamingCall(
                        (byte[] payload, StreamObserver<byte[]> out) ->
                                serve(descriptor, payload, out, withFileList)));
    }

    private void serve(MethodDescriptor<byte[], byte[]> descriptor, byte[] payload,
                       StreamObserver<byte[]> out, boolean withFileList) {
        try {
            answer(descriptor, payload, out, withFileList);
        } finally {
            /*
             * The gRPC half of IcebergCatalogReleaseFilter, as in
             * IcebergGrpcServices: the read ran start to finish on this
             * thread, so the catalogs it built are this thread's.  After the
             * call is closed, and never at the cost of a served call.
             */
            try {
                releaseRequestResources.run();
            } catch (RuntimeException e) {
                log.warn("Failed to release per-request iceberg catalogs", e);
            }
        }
    }

    private void answer(MethodDescriptor<byte[], byte[]> descriptor, byte[] payload,
                        StreamObserver<byte[]> out, boolean withFileList) {
        ServerCallStreamObserver<byte[]> observer = (ServerCallStreamObserver<byte[]>) out;
        String body = payload == null || payload.length == 0
                ? null
                : new String(payload, StandardCharsets.UTF_8);

        // Parsing fails the way it does in DlBaseResource: logged, and wrapped
        // in a DlRuntimeException, which decides the error document.
        RequestContext context;
        try {
            context = requestErrors.invoke(() -> parse(body, withFileList));
        } catch (Throwable t) {
            fail(descriptor, observer, t, UnhandledErrors.HANDLER_DISPATCH_FAILED);
            return;
        }

        try {
            try (GrpcChunkOutputStream stream = new GrpcChunkOutputStream(observer,
                    GrpcChunkOutputStream.DEFAULT_CHUNK_SIZE, stallTimeoutMs)) {
                readService.readData(context, stream);
            }
            out.onCompleted();
        } catch (Throwable t) {
            fail(descriptor, observer, t, UnhandledErrors.ASYNC_PROCESSING_FAILED);
        }
    }

    private RequestContext parse(String body, boolean withFileList) throws Exception {
        RequestContext context = parser.parseRequest(RequestMetadata.headers(), body);

        if (withFileList && body != null && !body.isEmpty()) {
            // The write path's body carries both the config and the file
            // list, and the REST resource parses it twice for the same
            // reason: the parser above picks up gopher.* so the catalog's
            // GopherFileIO can initialise, and the file list is a separate
            // field of the same document.
            FileListRequest fileList = writer.objectMapper()
                    .readValue(body, FileListRequest.class);
            if (fileList != null && fileList.getFiles() != null) {
                context.setFileList(fileList.getFiles());
            }
        }
        return context;
    }

    /**
     * Ends a failed call.
     *
     * <p>A call whose client is still there must end with a status whatever
     * went wrong -- a call left open hangs the client. A real failure ends
     * INTERNAL, carrying what REST would have answered: its HTTP status in
     * {@link DlagentMethods#STATUS_KEY} and its error document, built by the
     * same mapper and serializer, in {@link DlagentMethods#ERROR_KEY}.
     */
    private void fail(MethodDescriptor<byte[], byte[]> descriptor,
                      ServerCallStreamObserver<byte[]> observer, Throwable t, String context) {
        if (observer.isCancelled()) {
            // Routine: the query was cancelled, and a cancelled call cannot
            // be answered anyway. No stack trace on every cancelled scan.
            log.info("client went away during {}", descriptor.getFullMethodName());
            return;
        }
        if (disconnects.isClientDisconnect(t)) {
            // Such as the DEADLINE_EXCEEDED of a client that stopped reading:
            // it keeps its code and is not logged as a server fault.
            log.warn("{} abandoned: {}", descriptor.getFullMethodName(), t.getMessage());
            observer.onError(Status.fromThrowable(t).asRuntimeException());
            return;
        }
        Status status = Status.INTERNAL.withDescription(String.valueOf(t.getMessage())).withCause(t);
        Metadata trailers = new Metadata();
        try {
            // Logs the failure, as REST's controller advice does.
            ServiceResult<Object> failure =
                    IcebergExceptionMapper.toResult(UnhandledErrors.asException(t, context));
            trailers.put(DlagentMethods.STATUS_KEY, String.valueOf(failure.getStatusCode()));
            trailers.put(DlagentMethods.ERROR_KEY, boundedDocument(failure));
        } catch (RuntimeException e) {
            // Still close the call; the client gets the status without the
            // document rather than no answer at all.
            log.error("{} failed and its error document could not be built",
                    descriptor.getFullMethodName(), e);
            trailers = new Metadata();
        }
        observer.onError(status.asRuntimeException(trailers));
    }

    /**
     * Largest error document sent as a trailer. The C client accepts 4 MiB of
     * metadata (grpc_transport.cpp) and a -bin trailer is base64 on the wire,
     * so 1 MiB leaves room; anything near it is a stack trace, never the
     * message or the status the client classifies the error by.
     */
    static final int MAX_ERROR_DOCUMENT_BYTES = 1024 * 1024;

    /**
     * The error document for {@link DlagentMethods#ERROR_KEY}, with its
     * {@code stack} field shortened if the whole would exceed
     * {@link #MAX_ERROR_DOCUMENT_BYTES}.
     *
     * <p>A trailer has a size ceiling a REST body does not: over it, the
     * client drops the trailer and reports only "metadata size exceeds
     * limit", losing the real message and the HTTP status that maps it to
     * 404/409. Only the stack is cut, from the end, so what is kept is the
     * top of the trace and every other field is byte-identical to REST.
     */
    @SuppressWarnings("unchecked")
    byte[] boundedDocument(ServiceResult<Object> failure) {
        byte[] doc = writer.write(failure);
        if (doc.length <= MAX_ERROR_DOCUMENT_BYTES) {
            return doc;
        }
        Object error = failure.getErrorResponse() == null ? null : failure.getErrorResponse().get("error");
        if (!(error instanceof Map) || !(((Map<?, ?>) error).get("stack") instanceof String)) {
            return doc;
        }
        // Copies: the mapper's maps are not ours to change.
        Map<String, Object> fields = new HashMap<>((Map<String, Object>) error);
        String stack = (String) fields.get("stack");
        int excess = doc.length - MAX_ERROR_DOCUMENT_BYTES;
        // Characters, not bytes: cut a little more than the excess so that
        // multi-byte text and JSON escaping cannot leave it over the limit.
        int keep = Math.max(0, stack.length() - excess - 1024);
        fields.put("stack", stack.substring(0, keep)
                + "\n\t... stack truncated (" + (stack.length() - keep) + " characters)");
        Map<String, Object> body = new HashMap<>(failure.getErrorResponse());
        body.put("error", fields);
        return writer.write(ServiceResult.failureBody(body, failure.getStatusCode()));
    }

    /** DlBaseResource's error reporting, applied to request parsing. */
    private static final class RequestErrors extends DlErrorReporter<RequestContext> {
        RequestErrors(ClientDisconnectPredicate disconnects) {
            super(disconnects);
        }

        RequestContext invoke(ThrowingSupplier<RequestContext, Exception> action) {
            return invokeWithErrorHandling(action);
        }
    }

    /** Exposes a call's metadata to the handler as the header multimap the parser expects. */
    static final class RequestMetadata {

        private static final io.grpc.Context.Key<Map<String, List<String>>> HEADERS =
                io.grpc.Context.key("dlagent-headers");

        static Map<String, List<String>> headers() {
            Map<String, List<String>> headers = HEADERS.get();
            return headers == null ? new HashMap<>() : headers;
        }

        static io.grpc.ServerInterceptor interceptor() {
            return new io.grpc.ServerInterceptor() {
                @Override
                public <ReqT, RespT> io.grpc.ServerCall.Listener<ReqT> interceptCall(
                        io.grpc.ServerCall<ReqT, RespT> call, Metadata metadata,
                        io.grpc.ServerCallHandler<ReqT, RespT> next) {
                    Map<String, List<String>> headers = new HashMap<>();
                    for (String key : metadata.keys()) {
                        if (key.endsWith(Metadata.BINARY_HEADER_SUFFIX)) {
                            // The parser reads text parameters only; a binary
                            // header cannot be one and decoding it as ASCII
                            // would corrupt it.
                            continue;
                        }
                        Iterable<String> values = metadata.getAll(
                                Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER));
                        if (values == null) {
                            continue;
                        }
                        List<String> list = new ArrayList<>();
                        for (String v : values) {
                            list.add(v);
                        }
                        headers.put(key, list);
                    }
                    return io.grpc.Contexts.interceptCall(
                            io.grpc.Context.current().withValue(HEADERS, headers),
                            call, metadata, next);
                }
            };
        }

        private RequestMetadata() {
        }
    }
}
