/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.grpc;

import cloud.elastic.dlagent.api.model.CleanupFromMetadataRequest;
import cloud.elastic.dlagent.grpc.wire.DlagentMethods;
import cloud.elastic.dlagent.plugins.iceberg.IcebergCatalogWrapper;
import cloud.elastic.dlagent.service.ServiceResult;
import cloud.elastic.dlagent.service.iceberg.IcebergOperations;
import cloud.elastic.dlagent.transport.IcebergExceptionMapper;
import cloud.elastic.dlagent.transport.JsonResponseWriter;
import com.fasterxml.jackson.core.type.TypeReference;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerMethodDefinition;
import io.grpc.ServerServiceDefinition;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * The gRPC transport: a shell over {@link IcebergOperations}.
 *
 * <p>Every method does the same four things -- read the two routing values
 * from metadata, parse the payload as the JSON body the REST transport would
 * have received, call the operation, write the result back through the one
 * serializer. There is no business logic here, and there must not be: it would
 * be a second copy of what dlagent-rest runs.
 *
 * <p>WHY THE TWO ROUTING VALUES ARE IN METADATA. REST takes {@code prefix} and
 * {@code table} from the URL path. Putting them in the payload instead would
 * have made the gRPC request body differ from the REST one, and the C++ client
 * would need a second request-assembly path; in metadata, both transports send
 * byte-identical request bodies and the C side reuses the buffer cJSON already
 * builds.
 */
@Named
@Singleton
@Slf4j
public class IcebergGrpcServices {

    private final IcebergOperations operations;
    private final JsonResponseWriter writer;
    private final GrpcClientDisconnectPredicate disconnects;
    private final Runnable releaseRequestResources;

    /**
     * The agent's services, closing whatever Iceberg catalogs each call built
     * once that call has been answered.
     *
     * <p>This is the gRPC half of {@code IcebergCatalogReleaseFilter} in
     * dlagent-rest.  Every catalog type but hive is built fresh per request,
     * and the rest / polaris ones hold a GopherFileIO whose connection to the
     * gophermeta worker stays open until it is closed; without a release at
     * the end of each call the agent accumulated one connection per request
     * until gophermeta ran out of file descriptors, after which queries hung
     * rather than failed.  The catalogs are tracked per thread, and a unary
     * call runs start to finish on one executor thread, so the end of
     * {@link #invoke} is the request boundary here exactly as the servlet
     * filter is under REST.
     */
    @Inject
    public IcebergGrpcServices(IcebergOperations operations,
                               JsonResponseWriter writer,
                               GrpcClientDisconnectPredicate disconnects,
                               IcebergCatalogWrapper catalogWrapper) {
        this(operations, writer, disconnects, catalogWrapper::releaseRequestCatalogs);
    }

    /** Without per-request release: for tests whose operations build no catalog. */
    public IcebergGrpcServices(IcebergOperations operations,
                               JsonResponseWriter writer,
                               GrpcClientDisconnectPredicate disconnects) {
        this(operations, writer, disconnects, () -> { });
    }

    IcebergGrpcServices(IcebergOperations operations,
                        JsonResponseWriter writer,
                        GrpcClientDisconnectPredicate disconnects,
                        Runnable releaseRequestResources) {
        this.operations = operations;
        this.writer = writer;
        this.disconnects = disconnects;
        this.releaseRequestResources = releaseRequestResources;
    }

    /** An operation, once its routing values and request body are known. */
    @FunctionalInterface
    private interface Op {
        ServiceResult<Object> apply(String prefix, String table, Map<String, Object> request)
                throws Exception;
    }

    public ServerServiceDefinition icebergCatalog() {
        return ServerServiceDefinition.builder(DlagentMethods.ICEBERG_CATALOG)
                .addMethod(unary(DlagentMethods.CREATE_TABLE,
                        (p, t, r) -> operations.createTable(p, r)))
                .addMethod(unary(DlagentMethods.LOAD_TABLE,
                        (p, t, r) -> operations.loadTable(p, t, r, null)))
                .addMethod(unary(DlagentMethods.LOAD_METADATA_JSON,
                        (p, t, r) -> operations.loadMetadataJson(p, t, r)))
                .addMethod(unary(DlagentMethods.TABLE_EXISTS,
                        (p, t, r) -> operations.tableExists(p, t, r)))
                .addMethod(unary(DlagentMethods.DROP_TABLE,
                        (p, t, r) -> operations.dropTable(p, t, r)))
                .addMethod(unary(DlagentMethods.SUBMIT_MAINTENANCE,
                        (p, t, r) -> operations.submitMaintenance(p, t, r)))
                .addMethod(unary(DlagentMethods.TRUNCATE_TABLE,
                        (p, t, r) -> operations.truncateToTable(p, t, r)))
                .addMethod(unary(DlagentMethods.UPDATE_SCHEMA,
                        (p, t, r) -> operations.updateSchema(p, t, r)))
                .addMethod(unary(DlagentMethods.GET_STATISTICS,
                        (p, t, r) -> operations.getTableStatistics(p, t, r, null)))
                .addMethod(unary(DlagentMethods.GET_SNAPSHOT_SCHEMA,
                        (p, t, r) -> operations.getSnapshotSchema(p, t, r, null)))
                .addMethod(unary(DlagentMethods.GET_SNAPSHOTS,
                        (p, t, r) -> operations.getSnapshots(p, t, r, null)))
                .addMethod(unary(DlagentMethods.APPEND_TABLE,
                        (p, t, r) -> operations.appendToTable(p, t, r)))
                .addMethod(unary(DlagentMethods.UPDATE_TABLE,
                        (p, t, r) -> operations.updateToTable(p, t, r)))
                .addMethod(unary(DlagentMethods.COMMIT_APPEND,
                        (p, t, r) -> operations.commitAppend(p, t, r)))
                .addMethod(unary(DlagentMethods.COMMIT_UPDATE,
                        (p, t, r) -> operations.commitUpdate(p, t, r)))
                .addMethod(unary(DlagentMethods.COMMIT_REWRITE,
                        (p, t, r) -> operations.commitRewrite(p, t, r)))
                .addMethod(unary(DlagentMethods.GET_FRAGMENT,
                        (p, t, r) -> operations.getTableFragment(p, t, r, null)))
                .addMethod(unary(DlagentMethods.PLAN_FILE_GROUPS,
                        (p, t, r) -> operations.planFileGroups(p, t, r)))
                .addMethod(unary(DlagentMethods.COMMIT_FILE_GROUPS,
                        (p, t, r) -> operations.commitFileGroups(p, t, r)))
                .addMethod(unary(DlagentMethods.CLEANUP_METADATA, (p, t, r) -> {
                    // The one operation whose body is a typed POJO rather than
                    // a free-form map, because its REST twin declares
                    // @RequestBody CleanupFromMetadataRequest.
                    CleanupFromMetadataRequest typed = writer.objectMapper()
                            .convertValue(r, CleanupFromMetadataRequest.class);
                    return operations.cleanupFromMetadata(typed);
                }))
                .addMethod(unary(DlagentMethods.DELETE_FILE,
                        (p, t, r) -> operations.deleteFiles(r)))
                .build();
    }

    public ServerServiceDefinition catalogManagement() {
        return ServerServiceDefinition.builder(DlagentMethods.CATALOG_MANAGEMENT)
                .addMethod(unary(DlagentMethods.CREATE_CATALOG,
                        (p, t, r) -> operations.createCatalogWithNamespace(p, r)))
                .addMethod(unary(DlagentMethods.LIST_CATALOGS,
                        (p, t, r) -> operations.listCatalogs(p, r)))
                .addMethod(unary(DlagentMethods.CREATE_NAMESPACE,
                        (p, t, r) -> operations.createNamespace(p, r)))
                .addMethod(unary(DlagentMethods.LIST_NAMESPACES,
                        (p, t, r) -> operations.listNamespaces(p, r)))
                .build();
    }

    /* ------------------------------------------------------------------ */

    private ServerMethodDefinition<byte[], byte[]> unary(
            MethodDescriptor<byte[], byte[]> method, Op op) {
        return ServerMethodDefinition.create(method, ServerCalls.asyncUnaryCall(
                (byte[] payload, StreamObserver<byte[]> out) ->
                        invoke(method, op, payload, out)));
    }

    /*
     * Every method on both services is UNARY -- see dlagent.proto, which says
     * why GetFragment, PlanFileGroups and CommitFileGroups return one document
     * rather than a stream. There is deliberately no streaming variant here:
     * the only streaming in this jar is DataProxyGrpcService's, where
     * GrpcChunkOutputStream does the cancellation polling.
     */
    private void invoke(MethodDescriptor<byte[], byte[]> method, Op op,
                        byte[] payload, StreamObserver<byte[]> out) {
        try {
            answer(method, op, payload, out);
        } finally {
            /*
             * After the answer, as the REST filter does after flushing: closing
             * a GopherFileIO waits on the gophermeta worker, and a caller whose
             * work is done should not wait on that.  Never let cleanup turn a
             * served call into a failed one.
             */
            try {
                releaseRequestResources.run();
            } catch (RuntimeException e) {
                log.warn("Failed to release per-request iceberg catalogs", e);
            }
        }
    }

    private void answer(MethodDescriptor<byte[], byte[]> method, Op op,
                        byte[] payload, StreamObserver<byte[]> out) {
        try {
            Map<String, Object> request = parseRequest(payload);
            String prefix = RoutingMetadata.prefix();
            String table = RoutingMetadata.table();

            ServiceResult<Object> result = op.apply(prefix, table, request);

            // The application's status goes in a trailer, never in the gRPC
            // status: a non-OK gRPC status would discard the body, and the C
            // side needs both, exactly as it has both under REST. See
            // DlagentMethods.STATUS_KEY.
            Metadata trailers = new Metadata();
            trailers.put(DlagentMethods.STATUS_KEY, String.valueOf(result.getStatusCode()));
            ResponseTrailers.set(trailers);

            /*
             * A failed operation answers exactly like a successful one: an OK
             * gRPC status, the REST error document as the body, and the real
             * status in the trailer above. That is not a shortcut -- a non-OK
             * gRPC status would discard the body, and the C side parses that
             * body to build its message.
             */
            out.onNext(writer.write(result));
            out.onCompleted();
        } catch (Throwable t) {
            // Throwable, not Exception: an Error escaping here would end the
            // call as a bare UNKNOWN, with no body and nothing in the log,
            // where REST answers it with the 500 error document.
            Exception e = UnhandledErrors.asException(t, UnhandledErrors.HANDLER_DISPATCH_FAILED);
            if (disconnects.isClientDisconnect(e)) {
                log.info("client went away during {}", method.getFullMethodName());
                return;
            }
            if (e instanceof StatusRuntimeException || e instanceof StatusException) {
                // Already classified by whoever threw it -- the marshaller
                // says INTERNAL for a malformed message. Rewrapping here
                // would lose that classification.
                log.warn("{} rejected: {}", method.getFullMethodName(), e.getMessage());
                out.onError(e);
                return;
            }
            /*
             * An exception that escaped the operation answers WITH A BODY, not
             * with a bare gRPC error, and with the status REST would have
             * given it.
             *
             * IcebergExceptionMapper is the mapping dlagent-rest applies
             * through its @ControllerAdvice; core owns it so this transport
             * applies the same one. Answering 500 for everything here -- which
             * is what this did before -- turned "no such table" (404) and
             * "catalog already exists" (409) into a generic internal error,
             * and the C side decides what to retry from exactly that status.
             * A bare INTERNAL without a body would be worse still:
             * agent_cli_wrapper_check_exec_error_json() builds its message out
             * of the document.
             */
            ServiceResult<Object> failure = IcebergExceptionMapper.toResult(e);
            Metadata trailers = new Metadata();
            trailers.put(DlagentMethods.STATUS_KEY, String.valueOf(failure.getStatusCode()));
            ResponseTrailers.set(trailers);
            out.onNext(writer.write(failure));
            out.onCompleted();
        }
    }

    /**
     * The payload as the {@code @RequestBody Map} REST binds it to.
     *
     * <p>What REST cannot bind -- no body, the literal {@code null}, a body
     * that is not a JSON object -- it answers through its controller advice
     * with the 500 error document, the operation never running. Throwing a
     * plain exception here sends these cases down the same
     * {@link IcebergExceptionMapper} path in {@link #answer}, so gRPC answers
     * with the same status and document.  Not an IllegalArgumentException:
     * the mapper turns that into a 400, which REST does not give here.
     */
    private Map<String, Object> parseRequest(byte[] payload) throws IOException {
        Map<String, Object> request = payload == null || payload.length == 0
                ? null
                : writer.objectMapper().readValue(
                        new String(payload, StandardCharsets.UTF_8),
                        new TypeReference<Map<String, Object>>() { });
        if (request == null) {
            throw new IOException("Required request body is missing");
        }
        return request;
    }
}
