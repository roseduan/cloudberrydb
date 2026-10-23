/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.parity;

import cloud.elastic.dlagent.api.model.CleanupFromMetadataRequest;
import cloud.elastic.dlagent.service.ServiceResult;
import cloud.elastic.dlagent.service.iceberg.IcebergOperations;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * An {@link IcebergOperations} that answers with whatever the test wants and
 * records exactly what it was handed.
 *
 * <p>WHY STUB THE OPERATIONS AT ALL. A parity test asks whether the two
 * transports turn the same request into the same bytes. The work underneath is
 * dlagent-core, shared by both by construction, so running it would add a
 * catalog and object storage to a question that does not need them -- and
 * would let an Iceberg failure masquerade as a transport difference. What is
 * NOT shared, and therefore what these tests are about, is everything above
 * the operation call: routing, parsing, serialization, status, headers,
 * trailers and error mapping.
 *
 * <p>The recording half matters as much as the answering half: it is how a
 * test asserts that {@code prefix} and {@code table} arrived identically
 * although REST reads them from the URL path and gRPC from call metadata.
 */
public class RecordingOperations extends IcebergOperations {

    /** What the handler passed down, for the last call. */
    public static final class Call {
        public String prefix;
        public String table;
        public Map<String, Object> request;
        public String ifNoneMatch;
        public CleanupFromMetadataRequest typed;
        public String operation;
    }

    private final Supplier<ServiceResult<Object>> answer;
    private final List<Call> calls = new ArrayList<>();

    public RecordingOperations(Supplier<ServiceResult<Object>> answer) {
        super(null, null, null, null, null);
        this.answer = answer;
    }

    public Call lastCall() {
        return calls.isEmpty() ? null : calls.get(calls.size() - 1);
    }

    public List<Call> calls() {
        return calls;
    }

    public void reset() {
        calls.clear();
    }

    private ServiceResult<Object> record(String operation, String prefix, String table,
                                         Map<String, Object> request, String ifNoneMatch) {
        Call call = new Call();
        call.operation = operation;
        call.prefix = prefix;
        call.table = table;
        // Copy: the handler may reuse or clear its map after the call returns.
        call.request = request == null ? null : new LinkedHashMap<>(request);
        call.ifNoneMatch = ifNoneMatch;
        calls.add(call);
        return answer.get();
    }

    /* ---- table lifecycle ------------------------------------------- */

    @Override
    public ServiceResult<Object> createTable(String prefix, Map<String, Object> request) {
        return record("createTable", prefix, null, request, null);
    }

    @Override
    public ServiceResult<Object> loadTable(String prefix, String table, Map<String, Object> request,
                                           String ifNoneMatch) {
        return record("loadTable", prefix, table, request, ifNoneMatch);
    }

    @Override
    public ServiceResult<Object> loadMetadataJson(String prefix, String table,
                                                  Map<String, Object> request) {
        return record("loadMetadataJson", prefix, table, request, null);
    }

    @Override
    public ServiceResult<Object> tableExists(String prefix, String table,
                                             Map<String, Object> request) {
        return record("tableExists", prefix, table, request, null);
    }

    @Override
    public ServiceResult<Object> dropTable(String prefix, String table,
                                           Map<String, Object> request) {
        return record("dropTable", prefix, table, request, null);
    }

    @Override
    public ServiceResult<Object> submitMaintenance(String prefix, String table,
                                                   Map<String, Object> request) {
        return record("submitMaintenance", prefix, table, request, null);
    }

    @Override
    public ServiceResult<Object> truncateToTable(String prefix, String table,
                                                 Map<String, Object> request) {
        return record("truncateToTable", prefix, table, request, null);
    }

    @Override
    public ServiceResult<Object> updateSchema(String prefix, String table,
                                              Map<String, Object> request) {
        return record("updateSchema", prefix, table, request, null);
    }

    /* ---- reads and statistics --------------------------------------- */

    @Override
    public ServiceResult<Object> getTableStatistics(String prefix, String table,
                                                    Map<String, Object> request, String ifNoneMatch) {
        return record("getTableStatistics", prefix, table, request, ifNoneMatch);
    }

    @Override
    public ServiceResult<Object> getSnapshotSchema(String prefix, String table,
                                                   Map<String, Object> request, String ifNoneMatch) {
        return record("getSnapshotSchema", prefix, table, request, ifNoneMatch);
    }

    @Override
    public ServiceResult<Object> getSnapshots(String prefix, String table,
                                              Map<String, Object> request, String ifNoneMatch) {
        return record("getSnapshots", prefix, table, request, ifNoneMatch);
    }

    /* ---- writes and commits ----------------------------------------- */

    @Override
    public ServiceResult<Object> appendToTable(String prefix, String table,
                                               Map<String, Object> request) {
        return record("appendToTable", prefix, table, request, null);
    }

    @Override
    public ServiceResult<Object> updateToTable(String prefix, String table,
                                               Map<String, Object> request) {
        return record("updateToTable", prefix, table, request, null);
    }

    @Override
    public ServiceResult<Object> commitAppend(String prefix, String table,
                                              Map<String, Object> request) {
        return record("commitAppend", prefix, table, request, null);
    }

    @Override
    public ServiceResult<Object> commitUpdate(String prefix, String table,
                                              Map<String, Object> request) {
        return record("commitUpdate", prefix, table, request, null);
    }

    @Override
    public ServiceResult<Object> commitRewrite(String prefix, String table,
                                               Map<String, Object> request) {
        return record("commitRewrite", prefix, table, request, null);
    }

    /* ---- planning ---------------------------------------------------- */

    @Override
    public ServiceResult<Object> getTableFragment(String prefix, String table,
                                                  Map<String, Object> request, String ifNoneMatch) {
        return record("getTableFragment", prefix, table, request, ifNoneMatch);
    }

    @Override
    public ServiceResult<Object> planFileGroups(String prefix, String table,
                                                Map<String, Object> request) {
        return record("planFileGroups", prefix, table, request, null);
    }

    @Override
    public ServiceResult<Object> commitFileGroups(String prefix, String table,
                                                  Map<String, Object> request) {
        return record("commitFileGroups", prefix, table, request, null);
    }

    /* ---- file-level cleanup ------------------------------------------ */

    @Override
    public ServiceResult<Object> deleteFiles(Map<String, Object> request) {
        return record("deleteFiles", null, null, request, null);
    }

    @Override
    public ServiceResult<Object> cleanupFromMetadata(CleanupFromMetadataRequest request) {
        Call call = new Call();
        call.operation = "cleanupFromMetadata";
        call.typed = request;
        calls.add(call);
        return answer.get();
    }

    /* ---- catalog management ------------------------------------------ */

    @Override
    public ServiceResult<Object> createCatalogWithNamespace(String prefix,
                                                            Map<String, Object> request) {
        return record("createCatalogWithNamespace", prefix, null, request, null);
    }

    @Override
    public ServiceResult<Object> listCatalogs(String prefix, Map<String, Object> request) {
        return record("listCatalogs", prefix, null, request, null);
    }

    @Override
    public ServiceResult<Object> createNamespace(String prefix, Map<String, Object> request) {
        return record("createNamespace", prefix, null, request, null);
    }

    @Override
    public ServiceResult<Object> listNamespaces(String prefix, Map<String, Object> request) {
        return record("listNamespaces", prefix, null, request, null);
    }
}
