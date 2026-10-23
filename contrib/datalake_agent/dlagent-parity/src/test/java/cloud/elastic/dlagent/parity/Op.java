/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.parity;

import cloud.elastic.dlagent.grpc.wire.DlagentMethods;
import io.grpc.MethodDescriptor;

import java.util.Arrays;
import java.util.List;

/**
 * The agent's operations, each with the two addresses it answers on.
 *
 * <p>REST addresses an operation by URL path and gRPC by method name, which is
 * exactly the seam a parity test has to cross: this table is the only place
 * that knows both spellings, so every test below can be written once and run
 * through both transports.
 *
 * <p>The list is the operation set {@code agent_c_api.h} declares. It is
 * asserted complete by {@link OperationCoverageTest} rather than trusted:
 * an operation missing from here would silently go untested, which is how
 * CleanupMetadata and DeleteFile stayed unimplemented on the C side for the
 * whole migration.
 */
enum Op {

    /* ---- table lifecycle ------------------------------------------- */
    CREATE_TABLE("tables/create", DlagentMethods.CREATE_TABLE, false),
    LOAD_TABLE("tables/{table}/load", DlagentMethods.LOAD_TABLE, true),
    LOAD_METADATA_JSON("tables/{table}/loadMetadataJson", DlagentMethods.LOAD_METADATA_JSON, true),
    TABLE_EXISTS("tables/{table}/exists", DlagentMethods.TABLE_EXISTS, true),
    DROP_TABLE("tables/{table}/drop", DlagentMethods.DROP_TABLE, true),
    SUBMIT_MAINTENANCE("tables/{table}/submitMaintenance", DlagentMethods.SUBMIT_MAINTENANCE, true),
    TRUNCATE_TABLE("tables/{table}/truncate", DlagentMethods.TRUNCATE_TABLE, true),
    UPDATE_SCHEMA("tables/{table}/updateSchema", DlagentMethods.UPDATE_SCHEMA, true),

    /* ---- reads and statistics --------------------------------------- */
    GET_STATISTICS("tables/{table}/getStatistics", DlagentMethods.GET_STATISTICS, true),
    GET_SNAPSHOT_SCHEMA("tables/{table}/getSnapshotSchema", DlagentMethods.GET_SNAPSHOT_SCHEMA, true),
    GET_SNAPSHOTS("tables/{table}/getSnapshots", DlagentMethods.GET_SNAPSHOTS, true),

    /* ---- writes and commits ----------------------------------------- */
    APPEND_TABLE("tables/{table}/append", DlagentMethods.APPEND_TABLE, true),
    UPDATE_TABLE("tables/{table}/update", DlagentMethods.UPDATE_TABLE, true),
    COMMIT_APPEND("tables/{table}/commitAppend", DlagentMethods.COMMIT_APPEND, true),
    COMMIT_UPDATE("tables/{table}/commitUpdate", DlagentMethods.COMMIT_UPDATE, true),
    COMMIT_REWRITE("tables/{table}/commitRewrite", DlagentMethods.COMMIT_REWRITE, true),

    /* ---- planning ---------------------------------------------------- */
    GET_FRAGMENT("tables/{table}/getFragment", DlagentMethods.GET_FRAGMENT, true),
    PLAN_FILE_GROUPS("tables/{table}/planFileGroups", DlagentMethods.PLAN_FILE_GROUPS, true),
    COMMIT_FILE_GROUPS("tables/{table}/commitFileGroups", DlagentMethods.COMMIT_FILE_GROUPS, true),

    /* ---- file-level cleanup ------------------------------------------ */
    /*
     * The only two endpoints with no {prefix} variant in their @PostMapping:
     * they address objects by path, not by catalog, so there is no prefix to
     * route on. url() must not invent one, or MockMvc answers 404.
     */
    CLEANUP_METADATA("files/cleanup-from-metadata", DlagentMethods.CLEANUP_METADATA, false, false),
    DELETE_FILE("files/delete", DlagentMethods.DELETE_FILE, false, false),

    /* ---- catalog management ------------------------------------------ */
    CREATE_CATALOG("catalogs/create", DlagentMethods.CREATE_CATALOG, false),
    LIST_CATALOGS("catalogs/list", DlagentMethods.LIST_CATALOGS, false),
    CREATE_NAMESPACE("namespaces/create", DlagentMethods.CREATE_NAMESPACE, false),
    LIST_NAMESPACES("namespaces/list", DlagentMethods.LIST_NAMESPACES, false);

    private final String restPath;
    private final MethodDescriptor<byte[], byte[]> method;
    private final boolean takesTable;
    private final boolean takesPrefix;

    Op(String restPath, MethodDescriptor<byte[], byte[]> method, boolean takesTable) {
        this(restPath, method, takesTable, true);
    }

    Op(String restPath, MethodDescriptor<byte[], byte[]> method, boolean takesTable,
       boolean takesPrefix) {
        this.restPath = restPath;
        this.method = method;
        this.takesTable = takesTable;
        this.takesPrefix = takesPrefix;
    }

    /** The URL the REST controller answers on, for the given routing values. */
    String url(String prefix, String table) {
        StringBuilder url = new StringBuilder("/api/v1/");
        if (takesPrefix && prefix != null && !prefix.isEmpty()) {
            url.append(prefix).append('/');
        }
        url.append(restPath.replace("{table}", table == null ? "" : table));
        return url.toString();
    }

    MethodDescriptor<byte[], byte[]> method() {
        return method;
    }

    boolean takesTable() {
        return takesTable;
    }

    boolean takesPrefix() {
        return takesPrefix;
    }

    /** The operations that address a table, i.e. everything but the six global ones. */
    static List<Op> all() {
        return Arrays.asList(values());
    }
}
