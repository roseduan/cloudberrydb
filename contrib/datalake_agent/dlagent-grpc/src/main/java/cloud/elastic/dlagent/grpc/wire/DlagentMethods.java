/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.grpc.wire;

import io.grpc.Metadata;
import io.grpc.MethodDescriptor;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The {@link MethodDescriptor}s of {@code proto/dlagent.proto}, built by hand.
 *
 * <p>dlagent-grpc does not run protoc (see {@link PayloadMarshaller} and the
 * module's pom for why), so the descriptors that would have been generated are
 * declared here instead. {@code ProtoContractTest} parses the .proto file and
 * fails if this list and the file ever disagree on the set of methods, their
 * service, or whether they stream -- the same consistency that has to hold
 * between the proto and {@code agent_c_api.h}.
 */
public final class DlagentMethods {

    public static final String ICEBERG_CATALOG = "dlagent.v1.IcebergCatalog";
    public static final String CATALOG_MANAGEMENT = "dlagent.v1.CatalogManagement";
    public static final String DATA_PROXY = "dlagent.v1.DataProxy";

    /**
     * The catalog prefix and table name, which REST carries in the URL path
     * and gRPC therefore has to carry somewhere else.
     *
     * <p>They go in metadata rather than in the payload on purpose: it keeps
     * the REQUEST payload byte-identical to the REST request body too, so the
     * C++ client can send the very same buffer cJSON already builds, with no
     * second code path for assembling requests. (Only the response payloads
     * are required to match; this makes the requests match as well, which is
     * free here and one less thing for the C++ gRPC client in agent_cli to get
     * wrong.)
     */
    public static final Metadata.Key<String> PREFIX_KEY =
            Metadata.Key.of("dlagent-prefix", Metadata.ASCII_STRING_MARSHALLER);
    public static final Metadata.Key<String> TABLE_KEY =
            Metadata.Key.of("dlagent-table", Metadata.ASCII_STRING_MARSHALLER);

    /**
     * The operation's HTTP-shaped status, sent as a trailer.
     *
     * <p>gRPC cannot carry both a response message and a non-OK status: a
     * non-OK status IS the end of the call and any message is discarded. REST
     * carries both -- an HTTP status and a JSON error body -- and the C side
     * uses both, mapping the status to an errcode and parsing the body with
     * agent_cli_wrapper_check_exec_error_json.
     *
     * <p>So the two are separated: the body stays the message (which is what
     * makes it byte-identical to the REST body), and
     * the application's status travels here. gRPC's own status is then left to
     * mean what it means everywhere else -- UNAVAILABLE, DEADLINE_EXCEEDED,
     * CANCELLED, i.e. the transport-level facts the retry and cancellation
     * logic keys on -- rather than being overloaded to also express "your
     * table already exists".
     */
    public static final Metadata.Key<String> STATUS_KEY =
            Metadata.Key.of("dlagent-status", Metadata.ASCII_STRING_MARSHALLER);

    /**
     * The JSON error document of a failed DataProxy call, as a binary
     * trailer.
     *
     * <p>A data-plane call streams its body as messages, so once it fails it
     * cannot answer with an error document the way the catalog plane does: it
     * ends with a non-OK gRPC status instead. The document REST would have
     * sent as the HTTP error body -- same mapper, same serializer, same bytes
     * -- travels here, with the HTTP status REST would have used in
     * {@link #STATUS_KEY}, so the C client can build the same error message,
     * detail and hint. Binary ({@code -bin}) because the document is UTF-8
     * and may carry non-ASCII text.
     */
    public static final Metadata.Key<byte[]> ERROR_KEY =
            Metadata.Key.of("dlagent-error-bin", Metadata.BINARY_BYTE_MARSHALLER);

    private static final Map<String, MethodDescriptor<byte[], byte[]>> ALL = new LinkedHashMap<>();

    private static MethodDescriptor<byte[], byte[]> declare(
            String service, String method, MethodDescriptor.MethodType type) {
        MethodDescriptor<byte[], byte[]> d = MethodDescriptor.<byte[], byte[]>newBuilder()
                .setType(type)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(service, method))
                .setRequestMarshaller(PayloadMarshaller.INSTANCE)
                .setResponseMarshaller(PayloadMarshaller.INSTANCE)
                .setSampledToLocalTracing(true)
                .build();
        ALL.put(d.getFullMethodName(), d);
        return d;
    }

    private static MethodDescriptor<byte[], byte[]> unary(String service, String method) {
        return declare(service, method, MethodDescriptor.MethodType.UNARY);
    }

    private static MethodDescriptor<byte[], byte[]> streaming(String service, String method) {
        return declare(service, method, MethodDescriptor.MethodType.SERVER_STREAMING);
    }

    /* IcebergCatalog. */
    public static final MethodDescriptor<byte[], byte[]> CREATE_TABLE = unary(ICEBERG_CATALOG, "CreateTable");
    public static final MethodDescriptor<byte[], byte[]> LOAD_TABLE = unary(ICEBERG_CATALOG, "LoadTable");
    public static final MethodDescriptor<byte[], byte[]> LOAD_METADATA_JSON = unary(ICEBERG_CATALOG, "LoadMetadataJson");
    public static final MethodDescriptor<byte[], byte[]> TABLE_EXISTS = unary(ICEBERG_CATALOG, "TableExists");
    public static final MethodDescriptor<byte[], byte[]> DROP_TABLE = unary(ICEBERG_CATALOG, "DropTable");
    public static final MethodDescriptor<byte[], byte[]> SUBMIT_MAINTENANCE = unary(ICEBERG_CATALOG, "SubmitMaintenance");
    public static final MethodDescriptor<byte[], byte[]> TRUNCATE_TABLE = unary(ICEBERG_CATALOG, "TruncateTable");
    public static final MethodDescriptor<byte[], byte[]> UPDATE_SCHEMA = unary(ICEBERG_CATALOG, "UpdateSchema");
    public static final MethodDescriptor<byte[], byte[]> GET_STATISTICS = unary(ICEBERG_CATALOG, "GetStatistics");
    public static final MethodDescriptor<byte[], byte[]> GET_SNAPSHOT_SCHEMA = unary(ICEBERG_CATALOG, "GetSnapshotSchema");
    public static final MethodDescriptor<byte[], byte[]> GET_SNAPSHOTS = unary(ICEBERG_CATALOG, "GetSnapshots");
    public static final MethodDescriptor<byte[], byte[]> APPEND_TABLE = unary(ICEBERG_CATALOG, "AppendTable");
    public static final MethodDescriptor<byte[], byte[]> UPDATE_TABLE = unary(ICEBERG_CATALOG, "UpdateTable");
    public static final MethodDescriptor<byte[], byte[]> COMMIT_APPEND = unary(ICEBERG_CATALOG, "CommitAppend");
    public static final MethodDescriptor<byte[], byte[]> COMMIT_UPDATE = unary(ICEBERG_CATALOG, "CommitUpdate");
    public static final MethodDescriptor<byte[], byte[]> COMMIT_REWRITE = unary(ICEBERG_CATALOG, "CommitRewrite");
    public static final MethodDescriptor<byte[], byte[]> GET_FRAGMENT = unary(ICEBERG_CATALOG, "GetFragment");
    public static final MethodDescriptor<byte[], byte[]> PLAN_FILE_GROUPS = unary(ICEBERG_CATALOG, "PlanFileGroups");
    public static final MethodDescriptor<byte[], byte[]> COMMIT_FILE_GROUPS = unary(ICEBERG_CATALOG, "CommitFileGroups");
    public static final MethodDescriptor<byte[], byte[]> CLEANUP_METADATA = unary(ICEBERG_CATALOG, "CleanupMetadata");
    public static final MethodDescriptor<byte[], byte[]> DELETE_FILE = unary(ICEBERG_CATALOG, "DeleteFile");

    /* CatalogManagement. */
    public static final MethodDescriptor<byte[], byte[]> CREATE_CATALOG = unary(CATALOG_MANAGEMENT, "CreateCatalog");
    public static final MethodDescriptor<byte[], byte[]> LIST_CATALOGS = unary(CATALOG_MANAGEMENT, "ListCatalogs");
    public static final MethodDescriptor<byte[], byte[]> CREATE_NAMESPACE = unary(CATALOG_MANAGEMENT, "CreateNamespace");
    public static final MethodDescriptor<byte[], byte[]> LIST_NAMESPACES = unary(CATALOG_MANAGEMENT, "ListNamespaces");

    /* DataProxy. */
    public static final MethodDescriptor<byte[], byte[]> READ = streaming(DATA_PROXY, "Read");
    public static final MethodDescriptor<byte[], byte[]> WRITE = streaming(DATA_PROXY, "Write");

    /** Every declared method, keyed by full name, in declaration order. */
    public static Map<String, MethodDescriptor<byte[], byte[]>> all() {
        return Collections.unmodifiableMap(ALL);
    }

    private DlagentMethods() {
    }
}
