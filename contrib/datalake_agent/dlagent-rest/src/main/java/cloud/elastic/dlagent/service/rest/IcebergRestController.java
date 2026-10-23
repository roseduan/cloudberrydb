package cloud.elastic.dlagent.service.rest;

import cloud.elastic.dlagent.api.configuration.GopherConfigurationProperties;
import cloud.elastic.dlagent.api.model.BaseConfigurationFactory;
import cloud.elastic.dlagent.api.model.IcebergRequestConfigParser;
import cloud.elastic.dlagent.api.model.iceberg.IcebergRequestConfig;
import cloud.elastic.dlagent.constants.IcebergConfigConstants;
import cloud.elastic.dlagent.plugins.iceberg.utilities.IcebergUtilities;
import cloud.elastic.dlagent.service.iceberg.IcebergService;
import cloud.elastic.dlagent.service.iceberg.IcebergConfigurationBuilder;
import cloud.elastic.dlagent.service.iceberg.IcebergMetadataMapper;
import cloud.elastic.dlagent.service.iceberg.IcebergOperations;
import cloud.elastic.dlagent.service.iceberg.IcebergRequestContextFactory;
import cloud.elastic.dlagent.service.iceberg.MetadataFileCleaner;
import cloud.elastic.dlagent.service.iceberg.SchemaConverter;
import cloud.elastic.dlagent.service.iceberg.SchemaOp;
import cloud.elastic.dlagent.service.ServiceResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.SortField;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.hadoop.HadoopFileIO;
import cloud.elastic.dlagent.api.model.CleanupFromMetadataRequest;
import cloud.elastic.dlagent.api.model.CleanupFromMetadataResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import cloud.elastic.dlagent.api.model.RequestContext;
import cloud.elastic.dlagent.api.model.Fragment;
import cloud.elastic.dlagent.api.utilities.GpdbFragmentMetadata;
import cloud.elastic.dlagent.api.utilities.FragmentMetadata;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import cloud.elastic.dlagent.api.utilities.ColumnDescriptor;
import java.io.StringWriter;
import java.io.PrintWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import cloud.elastic.dlagent.service.rest.IcebergExceptionHandler;


/**
 * REST Controller for Iceberg API endpoints as defined in iceberg-openapi.yaml
 */
@RestController
@RequestMapping("/api/v1")
@Slf4j
public class IcebergRestController {


    /*
     * Only the four core collaborators: the helpers that need the schema
     * converter, configuration factory and gopher properties live in
     * dlagent-core and take those dependencies themselves.
     */
    /**
     * Every operation, in core. What is left in this class is the HTTP
     * mapping: a path, a body, and {@link #toResponse}.
     */
    @Autowired
    private IcebergOperations operations;

    /**
     * Sole iceberg-request parser. All endpoint properties extraction is
     * delegated here; legacy private extractors below are now thin shims that
     * call into this parser.
     */



    /**
     * Recognized {@code fileIOConfig.type} values posted by the datalake_fdw
     * consumer.  Kept as named constants so the supported storage schemes are
     * managed in one place rather than scattered as string literals.
     */

    /**
     * Check if a table exists in the given namespace
     *
     * @param prefix Catalog prefix
     * @param namespace Namespace identifier
     * @param table Table name
     * @param request Table exists request
     * @return 200 OK if table exists, 404 Not Found if it doesn't
     */
    @PostMapping({
        "/{prefix}/tables/{table}/exists",
        "/tables/{table}/exists"
    })
    public ResponseEntity<?> tableExists(
            @PathVariable(value = "prefix", required = false) String prefix,
            @PathVariable("table") String table,
            @RequestBody Map<String, Object> request) throws Exception {
        return toResponse(operations.tableExists(prefix, table, request));
    }

    /**
     * Load a table from the catalog
     *
     * @param prefix Catalog prefix
     * @param namespace Namespace identifier
     * @param table Table name
     * @param request Load table request
     * @return Table metadata result
     */
    @PostMapping({
        "/{prefix}/tables/{table}/load",
        "/tables/{table}/load"
    })
    public ResponseEntity<?> loadTable(
            @PathVariable(value = "prefix", required = false) String prefix,
            @PathVariable("table") String table,
            @RequestBody Map<String, Object> request,
            @RequestHeader(value = "If-None-Match", required = false) String ifNoneMatch) throws Exception {
        return toResponse(operations.loadTable(prefix, table, request, ifNoneMatch));
    }

    /**
     * Return the table's metadata.json verbatim.
     *
     * Separate endpoint rather than a new field on /load on purpose: /load is called on every
     * scan (pg_iceberg_am.c), and a metadata doc can be multiple MB. Only the REST catalog
     * gateway needs the full document, and only on loadTable.
     */
    @PostMapping({
        "/{prefix}/tables/{table}/loadMetadataJson",
        "/tables/{table}/loadMetadataJson"
    })
    public ResponseEntity<?> loadMetadataJson(
            @PathVariable(value = "prefix", required = false) String prefix,
            @PathVariable("table") String table,
            @RequestBody Map<String, Object> request) throws Exception {
        return toResponse(operations.loadMetadataJson(prefix, table, request));
    }

    /**
     * Get table metadata fragment
     *
     * @param prefix Catalog prefix
     * @param namespace Namespace identifier
     * @param table Table name
     * @param request Get fragment request
     * @return Table metadata fragment
     */
    @PostMapping({
        "/{prefix}/tables/{table}/getFragment",
        "/tables/{table}/getFragment"
    })
    public ResponseEntity<?> getTableFragment(
            @PathVariable(value = "prefix", required = false) String prefix,
            @PathVariable("table") String table,
            @RequestBody Map<String, Object> request,
            @RequestHeader(value = "If-None-Match", required = false) String ifNoneMatch) throws Exception {
        return toResponse(operations.getTableFragment(prefix, table, request, ifNoneMatch));
    }

    /**
     * Resolve the schema of a specific snapshot (time travel).
     *
     * <p>Called by the C describe callback at parse time so the result tuple
     * descriptor can be built for a snapshot whose schema differs from the
     * table's current schema (e.g. a column dropped after that snapshot).
     * Reads the pinned {@code metadata_location} and {@code snapshot_id} from
     * the request properties, mirroring getTableFragment's request handling.
     *
     * @param prefix Catalog prefix
     * @param table Table name
     * @param request Get snapshot schema request
     * @return snapshot schema JSON (snapshotSchemaId, currentSchemaId, columns)
     */
    @PostMapping({
        "/{prefix}/tables/{table}/getSnapshotSchema",
        "/tables/{table}/getSnapshotSchema"
    })
    public ResponseEntity<?> getSnapshotSchema(
            @PathVariable(value = "prefix", required = false) String prefix,
            @PathVariable("table") String table,
            @RequestBody Map<String, Object> request,
            @RequestHeader(value = "If-None-Match", required = false) String ifNoneMatch) throws Exception {
        return toResponse(operations.getSnapshotSchema(prefix, table, request, ifNoneMatch));
    }

    /**
     * List the snapshots recorded in the pinned metadata_location.
     *
     * Backs iceberg_toolkit.snapshots() and the AS OF TIMESTAMP resolution.
     *
     * @param prefix Catalog prefix
     * @param table Table name
     * @param request Get snapshots request (must carry metadata_location)
     * @return snapshot list JSON (currentSnapshotId, snapshots)
     */
    @PostMapping({
        "/{prefix}/tables/{table}/getSnapshots",
        "/tables/{table}/getSnapshots"
    })
    public ResponseEntity<?> getSnapshots(
            @PathVariable(value = "prefix", required = false) String prefix,
            @PathVariable("table") String table,
            @RequestBody Map<String, Object> request,
            @RequestHeader(value = "If-None-Match", required = false) String ifNoneMatch) throws Exception {
        return toResponse(operations.getSnapshots(prefix, table, request, ifNoneMatch));
    }

    /**
     * Get table statistics from the current snapshot summary
     */
    @PostMapping({
        "/{prefix}/tables/{table}/getStatistics",
        "/tables/{table}/getStatistics"
    })
    public ResponseEntity<?> getTableStatistics(
            @PathVariable(value = "prefix", required = false) String prefix,
            @PathVariable("table") String table,
            @RequestBody Map<String, Object> request,
            @RequestHeader(value = "If-None-Match", required = false) String ifNoneMatch) throws Exception {
        return toResponse(operations.getTableStatistics(prefix, table, request, ifNoneMatch));
    }

    /**
     * Plan file groups for vacuum/compaction
     *
     * @param prefix Catalog prefix
     * @param table Table name
     * @param request Plan file groups request containing minInputFiles and targetFileSizeMb
     * @return File groups for compaction
     */
    @PostMapping({
        "/{prefix}/tables/{table}/planFileGroups",
        "/tables/{table}/planFileGroups"
    })
    public ResponseEntity<?> planFileGroups(
            @PathVariable(value = "prefix", required = false) String prefix,
            @PathVariable("table") String table,
            @RequestBody Map<String, Object> request) throws Exception {
        return toResponse(operations.planFileGroups(prefix, table, request));
    }

    /**
     * Create a table in the given namespace
     *
     * @param prefix Catalog prefix
     * @param namespace Namespace identifier
     * @param request Create table request
     * @return Table metadata result
     */
    @PostMapping({
        "/{prefix}/tables/{table}/updateSchema",
        "/tables/{table}/updateSchema"
    })
    public ResponseEntity<?> updateSchema(
            @PathVariable(value = "prefix", required = false) String prefix,
            @PathVariable("table") String table,
            @RequestBody Map<String, Object> request) throws Exception {
        return toResponse(operations.updateSchema(prefix, table, request));
    }

    @PostMapping({
        "/{prefix}/tables/create",
        "/tables/create"
    })
    public ResponseEntity<?> createTable(
            @PathVariable(value = "prefix", required = false) String prefix,
            @RequestBody Map<String, Object> request) throws Exception {
        return toResponse(operations.createTable(prefix, request));
    }

    /**
     * Truncate a builtin iceberg table to empty.  No fragments: the service
     * commits a metadata-only delete of all rows and writes a new metadata.json.
     */
    @PostMapping({
        "/{prefix}/tables/{table}/truncate",
        "/tables/{table}/truncate"
    })
    public ResponseEntity<?> truncateToTable(
            @PathVariable(value = "prefix", required = false) String prefix,
            @PathVariable("table") String table,
            @RequestBody Map<String, Object> request) throws Exception {
        return toResponse(operations.truncateToTable(prefix, table, request));
    }

    @PostMapping({
        "/{prefix}/tables/{table}/append",
        "/tables/{table}/append"
    })
    public ResponseEntity<?> appendToTable(
            @PathVariable(value = "prefix", required = false) String prefix,
            @PathVariable("table") String table,
            @RequestBody Map<String, Object> request) throws Exception {
        return toResponse(operations.appendToTable(prefix, table, request));
    }

    /**
     * update data files to a table
     *
     * @param prefix Catalog prefix
     * @param namespace Namespace identifier
     * @param table Table name
     * @param request update request
     * @return update operation result
     */
    @PostMapping({
        "/{prefix}/tables/{table}/update",
        "/tables/{table}/update"
    })
    public ResponseEntity<?> updateToTable(
            @PathVariable(value = "prefix", required = false) String prefix,
            @PathVariable("table") String table,
            @RequestBody Map<String, Object> request) throws Exception {
        return toResponse(operations.updateToTable(prefix, table, request));
    }

    /**
     * Commit file groups for vacuum/compaction.
     * Atomically replaces old files with new files using Iceberg RewriteFiles API.
     *
     * @param prefix Catalog prefix
     * @param table Table name
     * @param request Commit file groups request containing fragments and rewrittenFragments
     * @return Commit operation result with metadata-content
     */
    @PostMapping({
        "/{prefix}/tables/{table}/commitFileGroups",
        "/tables/{table}/commitFileGroups"
    })
    public ResponseEntity<?> commitFileGroups(
            @PathVariable(value = "prefix", required = false) String prefix,
            @PathVariable("table") String table,
            @RequestBody Map<String, Object> request) throws Exception {
        return toResponse(operations.commitFileGroups(prefix, table, request));
    }

    /**
     * PRE_COMMIT append: normal AppendFiles commit that updates catalog.
     */
    @PostMapping({
        "/{prefix}/tables/{table}/commitAppend",
        "/tables/{table}/commitAppend"
    })
    public ResponseEntity<?> commitAppend(
            @PathVariable(value = "prefix", required = false) String prefix,
            @PathVariable("table") String table,
            @RequestBody Map<String, Object> request) throws Exception {
        return toResponse(operations.commitAppend(prefix, table, request));
    }

    /**
     * PRE_COMMIT update: normal RowDelta commit that updates catalog.
     */
    @PostMapping({
        "/{prefix}/tables/{table}/commitUpdate",
        "/tables/{table}/commitUpdate"
    })
    public ResponseEntity<?> commitUpdate(
            @PathVariable(value = "prefix", required = false) String prefix,
            @PathVariable("table") String table,
            @RequestBody Map<String, Object> request) throws Exception {
        return toResponse(operations.commitUpdate(prefix, table, request));
    }

    /**
     * VACUUM commit: RewriteFiles + commit to catalog.
     */
    @PostMapping({
        "/{prefix}/tables/{table}/commitRewrite",
        "/tables/{table}/commitRewrite"
    })
    public ResponseEntity<?> commitRewrite(
            @PathVariable(value = "prefix", required = false) String prefix,
            @PathVariable("table") String table,
            @RequestBody Map<String, Object> request) throws Exception {
        return toResponse(operations.commitRewrite(prefix, table, request));
    }

    /**
     * Ask the storage platform to run a maintenance job for this table
     * (submission only; see {@code IcebergOperations#submitMaintenance}).
     */
    @PostMapping({
        "/{prefix}/tables/{table}/submitMaintenance",
        "/tables/{table}/submitMaintenance"
    })
    public ResponseEntity<?> submitMaintenance(
            @PathVariable(value = "prefix", required = false) String prefix,
            @PathVariable("table") String table,
            @RequestBody Map<String, Object> request) throws Exception {
        return toResponse(operations.submitMaintenance(prefix, table, request));
    }

    /**
     * Drop a table
     *
     * @param prefix Catalog prefix
     * @param namespace Namespace identifier
     * @param table Table name
     * @param request Drop table request
     * @return Drop operation result
     */
    @PostMapping({
        "/{prefix}/tables/{table}/drop",
        "/tables/{table}/drop"
    })
    public ResponseEntity<?> dropTable(
            @PathVariable(value = "prefix", required = false) String prefix,
            @PathVariable("table") String table,
            @RequestBody Map<String, Object> request) throws Exception {
        return toResponse(operations.dropTable(prefix, table, request));
    }

    /**
     * Create a catalog
     *
     * @param prefix Catalog prefix
     * @param request Create catalog request
     * @return Create catalog result
     */
    @PostMapping({
        "/{prefix}/catalogs/create",
        "/catalogs/create"
    })
    public ResponseEntity<?> createCatalogWithNamespace(
            @PathVariable(value = "prefix", required = false) String prefix,
            @RequestBody Map<String, Object> request) throws Exception {
        return toResponse(operations.createCatalogWithNamespace(prefix, request));
    }

    /**
     * Create a namespace in a catalog
     *
     * @param prefix Catalog prefix
     * @param request Create namespace request
     * @return Create namespace result
     */
    @PostMapping({
        "/{prefix}/namespaces/create",
        "/namespaces/create"
    })
    public ResponseEntity<?> createNamespace(
            @PathVariable(value = "prefix", required = false) String prefix,
            @RequestBody Map<String, Object> request) throws Exception {
        return toResponse(operations.createNamespace(prefix, request));
    }

    /**
     * List all catalogs
     *
     * @param prefix Catalog prefix
     * @param request List catalogs request
     * @return List of catalogs
     */
    @PostMapping({
        "/{prefix}/catalogs/list",
        "/catalogs/list"
    })
    public ResponseEntity<?> listCatalogs(
            @PathVariable(value = "prefix", required = false) String prefix,
            @RequestBody Map<String, Object> request) throws Exception {
        return toResponse(operations.listCatalogs(prefix, request));
    }

    /**
     * List all namespaces in a catalog
     *
     * @param prefix Catalog prefix
     * @param request List namespaces request
     * @return List of namespaces
     */
    @PostMapping({
        "/{prefix}/namespaces/list",
        "/namespaces/list"
    })
    public ResponseEntity<?> listNamespaces(
            @PathVariable(value = "prefix", required = false) String prefix,
            @RequestBody Map<String, Object> request) throws Exception {
        return toResponse(operations.listNamespaces(prefix, request));
    }

    /**
     * Delete a set of individual files directly (DELETION_TYPE_FILE) -- e.g.
     * VACUUM's rewritten old files.  Unlike cleanup-from-metadata this does NOT
     * parse anything as metadata; it just deletes each given path via FileIO.
     * A file that is already gone counts as success (idempotent retry).
     * Body: { "paths": ["s3a://...", ...], "fileIOConfig": {...} }
     */
    @PostMapping({"/files/delete"})
    public ResponseEntity<?> deleteFiles(@RequestBody Map<String, Object> request) {
        return toResponse(operations.deleteFiles(request));
    }

    @PostMapping({"/files/cleanup-from-metadata"})
    public ResponseEntity<?> cleanupFromMetadata(
            @RequestBody CleanupFromMetadataRequest request) {
        return toResponse(operations.cleanupFromMetadata(request));
    }


    /**
     * Map a core {@link ServiceResult} onto an HTTP response.
     *
     * <p>The whole transport adapter, in five lines: core decides the status
     * code and the body, this decides how to put them on the wire. The gRPC
     * service does the same job with the same ServiceResult and its own status
     * mapping.
     *
     * <p>A null body sends no body rather than the four bytes {@code null} --
     * that is what {@code ResponseEntity.ok().build()} did before, and the C
     * side parses a zero-length response differently from a JSON null.
     */
    private ResponseEntity<?> toResponse(ServiceResult<?> result) {
        Object body = result.getData() != null ? result.getData() : result.getErrorResponse();
        ResponseEntity.BodyBuilder builder = ResponseEntity.status(result.getStatusCode());
        for (Map.Entry<String, String> header : result.getHeaders().entrySet()) {
            if (HttpHeaders.CONTENT_TYPE.equalsIgnoreCase(header.getKey())) {
                // A body the service already serialized (loadMetadataJson's
                // byte[]) has to keep announcing application/json. Left to
                // Spring, a byte[] body picks ByteArrayHttpMessageConverter
                // and would go out as application/octet-stream.
                builder = builder.contentType(MediaType.parseMediaType(header.getValue()));
            } else {
                builder = builder.header(header.getKey(), header.getValue());
            }
        }
        if (body == null) {
            return builder.build();
        }
        return builder.body(body);
    }


}
