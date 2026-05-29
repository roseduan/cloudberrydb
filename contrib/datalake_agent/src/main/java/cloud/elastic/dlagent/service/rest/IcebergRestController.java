package cloud.elastic.dlagent.service.rest;

import cloud.elastic.dlagent.api.configuration.GopherConfigurationProperties;
import cloud.elastic.dlagent.api.model.BaseConfigurationFactory;
import cloud.elastic.dlagent.api.model.IcebergRequestConfigParser;
import cloud.elastic.dlagent.api.model.iceberg.IcebergRequestConfig;
import cloud.elastic.dlagent.constants.IcebergConfigConstants;
import cloud.elastic.dlagent.plugins.iceberg.utilities.IcebergUtilities;
import cloud.elastic.dlagent.service.iceberg.IcebergService;
import cloud.elastic.dlagent.service.iceberg.SchemaConverter;
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
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.io.FileIO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
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
import java.io.StringWriter;
import java.io.PrintWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import cloud.elastic.dlagent.service.rest.IcebergExceptionHandler;
import cloud.elastic.dlagent.service.spring.IcebergRestConfig;

/**
 * REST Controller for Iceberg API endpoints as defined in iceberg-openapi.yaml
 */
@RestController
@RequestMapping("/api/v1")
@Slf4j
public class IcebergRestController {

    private final IcebergService icebergService;
    private final IcebergRestConfig icebergRestConfig;

    @Autowired
    public IcebergRestController(IcebergService icebergService, IcebergRestConfig icebergRestConfig) {
        this.icebergService = icebergService;
        this.icebergRestConfig = icebergRestConfig;
    }

    @Autowired
    private SchemaConverter schemaConverter;

    @Autowired
    private BaseConfigurationFactory configurationFactory;

    @Autowired
    private GopherConfigurationProperties gopherConfigurationProperties;

    /**
     * Sole iceberg-request parser. All endpoint properties extraction is
     * delegated here; legacy private extractors below are now thin shims that
     * call into this parser.
     */
    @Autowired
    private IcebergRequestConfigParser requestParser;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private static final Logger LOG = LoggerFactory.getLogger(IcebergRestController.class);

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

        // Extract namespace from request body
        String namespace = (String) request.get("namespace");
        if (namespace == null || namespace.isEmpty()) {
            Map<String, Object> errorResponse = new HashMap<>();
            Map<String, Object> error = new HashMap<>();
            error.put("message", "Namespace is required");
            error.put("type", "BadRequestException");
            error.put("code", 400);
            errorResponse.put("error", error);
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorResponse);
        }

        log.info("Checking if table exists: {}.{}", namespace, table);

        // Extract configurations from request
        Map<String, String> properties = extractProperties(request);

        // Create request context
        RequestContext context = createRequestContext(namespace, table, properties);

        // Check if the table exists and get detailed result
        icebergService.checkTableExists(namespace, table, properties, context);
        return ResponseEntity.ok().build();
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

        // Extract namespace from request body
        String namespace = (String) request.get("namespace");
        if (namespace == null || namespace.isEmpty()) {
            Map<String, Object> errorResponse = new HashMap<>();
            Map<String, Object> error = new HashMap<>();
            error.put("message", "Namespace is required");
            error.put("type", "BadRequestException");
            error.put("code", 400);
            errorResponse.put("error", error);
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorResponse);
        }

        log.info("Loading table: {}.{}", namespace, table);

        // Extract configurations from request
        Map<String, String> properties = extractProperties(request);

        // Create request context
        RequestContext context = createRequestContext(namespace, table, properties);

        // Load the table using ServiceResult
        Table icebergTable = icebergService.loadTable(namespace, table, properties, context);

        // Create response following the Iceberg REST API spec
        Map<String, Object> metadataMap = convertTableToMetadata(icebergTable);
        Map<String, Object> response = new HashMap<>();

        TableMetadata metadata = ((BaseTable) icebergTable).operations().current();
        String metadataLocation = metadata.metadataFileLocation();
        response.put("metadata", metadataMap);
        response.put("metadata-location", metadataLocation);
        response.put("table-location", icebergTable.location());

        // Add config to response - static S3 configuration (non-sensitive)
        Map<String, String> config = new HashMap<>();

        // Get S3 configuration from FileIO properties.
        // GopherFileIO does not implement FileIO.properties() (throws
        // UnsupportedOperationException); treat as empty in that case — the
        // S3 config/credential fields below are optional response payload.
        FileIO fileIO = icebergTable.io();
        Map<String, String> ioProps;
        try {
            ioProps = fileIO.properties();
        } catch (UnsupportedOperationException e) {
            ioProps = java.util.Collections.emptyMap();
        }

        // Extract S3 config from FileIO properties
        if (ioProps.containsKey("s3.endpoint")) {
            config.put("s3.endpoint", ioProps.get("s3.endpoint"));
        }
        if (ioProps.containsKey("s3.region")) {
            config.put("s3.region", ioProps.get("s3.region"));
        }
        if (ioProps.containsKey("s3.path-style-access")) {
            config.put("s3.path-style-access", ioProps.get("s3.path-style-access"));
        }
        if (ioProps.containsKey("client.region")) {
            config.put("client.region", ioProps.get("client.region"));
        }
        response.put("config", config);

        // Add storage-credentials - sensitive credentials from FileIO
        Map<String, String> credConfig = new HashMap<>();
        if (ioProps.containsKey("s3.access-key-id")) {
            credConfig.put("s3.access-key-id", ioProps.get("s3.access-key-id"));
        }
        if (ioProps.containsKey("s3.secret-access-key")) {
            credConfig.put("s3.secret-access-key", ioProps.get("s3.secret-access-key"));
        }
        if (ioProps.containsKey("s3.session-token")) {
            credConfig.put("s3.session-token", ioProps.get("s3.session-token"));
        }

        if (!credConfig.isEmpty()) {
            List<Map<String, Object>> storageCredentials = new ArrayList<>();
            Map<String, Object> credential = new HashMap<>();
            credential.put("prefix", icebergTable.location());
            credential.put("config", credConfig);
            storageCredentials.add(credential);
            response.put("storage-credentials", storageCredentials);
        }

        // Generate ETag header with debug information
        String etag = generateETag(icebergTable, "load");

        // Check if the ETag matches the If-None-Match header
        if (ifNoneMatch != null && ifNoneMatch.equals(etag)) {
            return ResponseEntity.status(HttpStatus.NOT_MODIFIED).build();
        }

        return ResponseEntity.ok()
                .header("ETag", etag)
                .body(response);
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

        // Extract namespace from request body
        String namespace = (String) request.get("namespace");
        if (namespace == null || namespace.isEmpty()) {
            Map<String, Object> errorResponse = new HashMap<>();
            Map<String, Object> error = new HashMap<>();
            error.put("message", "Namespace is required");
            error.put("type", "BadRequestException");
            error.put("code", 400);
            errorResponse.put("error", error);
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorResponse);
        }

        log.info("Getting table metadata fragment: {}.{}", namespace, table);

        // Extract configurations from request
        Map<String, String> properties = extractProperties(request);

        // convert to RequestContext and reuse the original iceberg logic
        RequestContext context = createRequestContext(namespace, table, properties);

        // Get table metadata fragment using ServiceResult
        String fragment = icebergService.getTableFragment(namespace, table, properties, context);

        // Generate ETag header with debug information for fragment
        String etag = "fragment-" + System.currentTimeMillis() + "-" + Math.abs(fragment.hashCode());
        log.debug("Generated ETag for getFragment: {}", etag);

        // Check if the ETag matches the If-None-Match header
        if (ifNoneMatch != null && ifNoneMatch.equals(etag)) {
            return ResponseEntity.status(HttpStatus.NOT_MODIFIED).build();
        }

        return ResponseEntity.ok()
                .header("ETag", etag)
                .body(fragment);
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

        // Extract namespace from request body
        String namespace = (String) request.get("namespace");
        if (namespace == null || namespace.isEmpty()) {
            Map<String, Object> errorResponse = new HashMap<>();
            Map<String, Object> error = new HashMap<>();
            error.put("message", "Namespace is required");
            error.put("type", "BadRequestException");
            error.put("code", 400);
            errorResponse.put("error", error);
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorResponse);
        }

        log.info("Getting table statistics: {}.{}", namespace, table);

        // Extract configurations from request
        Map<String, String> properties = extractProperties(request);

        // convert to RequestContext and reuse the original iceberg logic
        RequestContext context = createRequestContext(namespace, table, properties);

        // Get table statistics
        String statistics = icebergService.getTableStatistics(namespace, table, properties, context);

        // Generate ETag header
        String etag = "statistics-" + System.currentTimeMillis() + "-" + Math.abs(statistics.hashCode());
        log.debug("Generated ETag for getStatistics: {}", etag);

        // Check if the ETag matches the If-None-Match header
        if (ifNoneMatch != null && ifNoneMatch.equals(etag)) {
            return ResponseEntity.status(HttpStatus.NOT_MODIFIED).build();
        }

        return ResponseEntity.ok()
                .header("ETag", etag)
                .body(statistics);
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

        // Extract namespace from request body
        String namespace = (String) request.get("namespace");
        if (namespace == null || namespace.isEmpty()) {
            Map<String, Object> errorResponse = new HashMap<>();
            Map<String, Object> error = new HashMap<>();
            error.put("message", "Namespace is required");
            error.put("type", "BadRequestException");
            error.put("code", 400);
            errorResponse.put("error", error);
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorResponse);
        }

        log.info("Planning file groups for table: {}.{}", namespace, table);

        // Extract configurations from request
        Map<String, String> properties = extractProperties(request);

        // Extract vacuum parameters, falling back to config defaults
        int minInputFiles = icebergRestConfig.getVacuumMinInputFiles();
        int targetFileSizeMb = icebergRestConfig.getVacuumTargetFileSizeMb();
        if (request.containsKey("minInputFiles")) {
            minInputFiles = ((Number) request.get("minInputFiles")).intValue();
        }
        if (request.containsKey("targetFileSizeMb")) {
            targetFileSizeMb = ((Number) request.get("targetFileSizeMb")).intValue();
        }

        // Create request context
        RequestContext context = createRequestContext(namespace, table, properties);

        // Plan file groups
        String fileGroups = icebergService.planFileGroups(namespace, table, properties, context,
                minInputFiles, targetFileSizeMb);

        return ResponseEntity.ok().body(fileGroups);
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
        "/{prefix}/tables/create",
        "/tables/create"
    })
    public ResponseEntity<?> createTable(
            @PathVariable(value = "prefix", required = false) String prefix,
            @RequestBody Map<String, Object> request) throws Exception {

        // Extract namespace from request body
        String namespace = (String) request.get("namespace");
        if (namespace == null || namespace.isEmpty()) {
            Map<String, Object> errorResponse = new HashMap<>();
            Map<String, Object> error = new HashMap<>();
            error.put("message", "Namespace is required");
            error.put("type", "BadRequestException");
            error.put("code", 400);
            errorResponse.put("error", error);
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorResponse);
        }

        log.info("Creating table in namespace: {}", namespace);

        // Extract configurations from request
        Map<String, String> properties = extractProperties(request);

        // Extract table name from request
        String tableName = (String) request.get("name");
        if (tableName == null || tableName.isEmpty()) {
            Map<String, Object> errorResponse = new HashMap<>();
            Map<String, Object> error = new HashMap<>();
            error.put("message", "Table name is required");
            error.put("type", "BadRequestException");
            error.put("code", 400);
            errorResponse.put("error", error);
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorResponse);
        }

        // Extract schema from request
        Map<String, Object> schemaMap = (Map<String, Object>) request.get("schema");
        if (schemaMap == null) {
            Map<String, Object> errorResponse = new HashMap<>();
            Map<String, Object> error = new HashMap<>();
            error.put("message", "Schema is required");
            error.put("type", "BadRequestException");
            error.put("code", 400);
            errorResponse.put("error", error);
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorResponse);
        }

        // Convert schema map to Iceberg Schema (simplified for now)
        Schema schema = convertMapToSchema(schemaMap);

        // Create request context
        RequestContext context = createRequestContext(namespace, tableName, properties);

        // OSS Iceberg convention: persist only user-facing TBLPROPERTIES into
        // TableMetadata.properties. Runtime/catalog config must not be persisted.
        Map<String, String> userTableProperties = extractUserTableProperties(request);

        // Create the table using ServiceResult - pass null as location to use default warehouse
        Table icebergTable = icebergService.createTable(namespace, tableName, schema, context.getPath(), userTableProperties, context);

        // Create response following the Iceberg REST API spec
        Map<String, Object> metadataMap = convertTableToMetadata(icebergTable);
        Map<String, Object> response = new HashMap<>();
        TableMetadata metadata = ((BaseTable) icebergTable).operations().current();
        String metadataLocation = metadata.metadataFileLocation();
        response.put("metadata", metadataMap);
        response.put("metadata-location", metadataLocation);
        response.put("table-location", icebergTable.location());

        // Add config to response
        Map<String, String> config = new HashMap<>();
        response.put("config", config);

        // Generate ETag header with debug information
        String etag = generateETag(icebergTable, "create");

        return ResponseEntity.ok()
                .header("ETag", etag)
                .body(response);
    }

    /**
     * Append data files to a table
     *
     * @param prefix Catalog prefix
     * @param namespace Namespace identifier
     * @param table Table name
     * @param request Append request
     * @return Append operation result
     */
    @PostMapping({
        "/{prefix}/tables/{table}/append",
        "/tables/{table}/append"
    })
    public ResponseEntity<?> appendToTable(
            @PathVariable(value = "prefix", required = false) String prefix,
            @PathVariable("table") String table,
            @RequestBody Map<String, Object> request) throws Exception {

        // Extract namespace from request body
        String namespace = (String) request.get("namespace");
        if (namespace == null || namespace.trim().isEmpty()) {
            throw new IllegalArgumentException("Namespace is required in request body");
        }

        log.info("Appending to table: {}.{}", namespace, table);

        // Extract configurations from request
        Map<String, String> properties = extractProperties(request);

        // Extract fragments from request
        Object fragmentsObj = request.get("fragments");
        List<Map<String, Object>> fragmentMaps = null;

        if (fragmentsObj instanceof String) {
            // Parse JSON string to List
            ObjectMapper mapper = new ObjectMapper();
            fragmentMaps = mapper.readValue((String) fragmentsObj,
                new TypeReference<List<Map<String, Object>>>() {});
        } else {
            fragmentMaps = (List<Map<String, Object>>) fragmentsObj;
        }

        log.info("Received fragments: {}", fragmentMaps);
        log.info("Fragments null check: {}, empty check: {}",
            fragmentMaps == null, fragmentMaps != null ? fragmentMaps.isEmpty() : "N/A");

        if (fragmentMaps == null || fragmentMaps.isEmpty()) {
            log.warn("Empty or null fragments detected, returning error");
            Map<String, Object> errorResponse = new HashMap<>();
            Map<String, Object> error = new HashMap<>();
            error.put("message", "Fragments are required for append operation");
            error.put("type", "BadRequestException");
            error.put("code", 400);
            errorResponse.put("error", error);
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorResponse);
        }

        // Create request context
        RequestContext context = createRequestContext(namespace, table, properties);

        // Convert fragment maps to Fragment objects
        List<Fragment> fragments = new ArrayList<>();
        for (Map<String, Object> fragmentMap : fragmentMaps) {
            String path = (String) fragmentMap.get("path");
            if (path == null || path.isEmpty()) {
                Map<String, Object> errorResponse = new HashMap<>();
                Map<String, Object> error = new HashMap<>();
                error.put("message", "Fragment path is required");
                error.put("type", "BadRequestException");
                error.put("code", 400);
                errorResponse.put("error", error);
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorResponse);
            }

            // Create FragmentMetadata from fragment data
            long fileSize = fragmentMap.containsKey("file_size_in_bytes") ? 
                ((Number) fragmentMap.get("file_size_in_bytes")).longValue() : 0L;
            long recordCount = fragmentMap.containsKey("record_count") ? 
                ((Number) fragmentMap.get("record_count")).longValue() : 0L;
            String format = fragmentMap.containsKey("format") ? 
                (String) fragmentMap.get("format") : "PARQUET";

            GpdbFragmentMetadata metadata = new GpdbFragmentMetadata(fileSize, format, recordCount, "DATA_FILE");

            // Create Fragment with path and metadata
            Fragment fragment = new Fragment(path, metadata);
            fragments.add(fragment);
        }
        context.setFragments(fragments);

        // Perform append operation using ServiceResult
        Map<String, Object> appendResult = icebergService.appendToTable(namespace, table, properties, context);

        // Generate ETag header
        String etag = "append-" + System.currentTimeMillis() + "-" + Math.abs(appendResult.hashCode());

        return ResponseEntity.ok()
                .header("ETag", etag)
                .body(appendResult);
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

        // Extract namespace from request body
        String namespace = (String) request.get("namespace");
        if (namespace == null || namespace.trim().isEmpty()) {
            throw new IllegalArgumentException("Namespace is required in request body");
        }

        log.info("Update to table: {}.{}", namespace, table);

        // Extract configurations from request
        Map<String, String> properties = extractProperties(request);

        // Extract fragments from request
        Object fragmentsObj = request.get("updateFragments");
        List<Map<String, Object>> fragmentMaps = null;

        if (fragmentsObj instanceof String) {
            // Parse JSON string to List
            ObjectMapper mapper = new ObjectMapper();
            fragmentMaps = mapper.readValue((String) fragmentsObj,
                new TypeReference<List<Map<String, Object>>>() {});
        } else {
            fragmentMaps = (List<Map<String, Object>>) fragmentsObj;
        }

        log.info("Received fragments: {}", fragmentMaps);
        log.info("Fragments null check: {}, empty check: {}",
            fragmentMaps == null, fragmentMaps != null ? fragmentMaps.isEmpty() : "N/A");

        if (fragmentMaps == null || fragmentMaps.isEmpty()) {
            log.warn("Empty or null fragments detected, returning error");
            Map<String, Object> errorResponse = new HashMap<>();
            Map<String, Object> error = new HashMap<>();
            error.put("message", "Fragments are required for update operation");
            error.put("type", "BadRequestException");
            error.put("code", 400);
            errorResponse.put("error", error);
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorResponse);
        }

        // Create request context
        RequestContext context = createRequestContext(namespace, table, properties);

        // Convert fragment maps to Fragment objects
        List<Fragment> fragments = new ArrayList<>();
        for (Map<String, Object> fragmentMap : fragmentMaps) {
            String path = (String) fragmentMap.get("path");
            if (path == null || path.isEmpty()) {
                Map<String, Object> errorResponse = new HashMap<>();
                Map<String, Object> error = new HashMap<>();
                error.put("message", "Fragment path is required");
                error.put("type", "BadRequestException");
                error.put("code", 400);
                errorResponse.put("error", error);
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorResponse);
            }

            // Create FragmentMetadata from fragment data
            long fileSize = fragmentMap.containsKey("file_size_in_bytes") ? 
                ((Number) fragmentMap.get("file_size_in_bytes")).longValue() : 0L;
            long recordCount = fragmentMap.containsKey("record_count") ? 
                ((Number) fragmentMap.get("record_count")).longValue() : 0L;
            String format = fragmentMap.containsKey("format") ? 
                (String) fragmentMap.get("format") : "PARQUET";
            String ContentTypeStr = (String) fragmentMap.get("position_on_delete");

            GpdbFragmentMetadata metadata = new GpdbFragmentMetadata(fileSize, format, recordCount, ContentTypeStr);

            // Create Fragment with path and metadata
            Fragment fragment = new Fragment(path, metadata);
            fragments.add(fragment);
        }
        context.setFragments(fragments);

        // Perform append operation using ServiceResult
        Map<String, Object> appendResult = icebergService.rowUpdate(namespace, table, properties, context);

        // Generate ETag header
        String etag = "update-" + System.currentTimeMillis() + "-" + Math.abs(appendResult.hashCode());

        return ResponseEntity.ok()
                .header("ETag", etag)
                .body(appendResult);
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

        // Extract namespace from request body
        String namespace = (String) request.get("namespace");
        if (namespace == null || namespace.trim().isEmpty()) {
            throw new IllegalArgumentException("Namespace is required in request body");
        }

        log.info("Committing file groups for table: {}.{}", namespace, table);

        // Extract configurations from request
        Map<String, String> properties = extractProperties(request);

        // Create request context
        RequestContext context = createRequestContext(namespace, table, properties);

        // Extract new fragments (files to add)
        Object fragmentsObj = request.get("fragments");
        List<Map<String, Object>> fragmentMaps = parseFragmentList(fragmentsObj);

        if (fragmentMaps == null || fragmentMaps.isEmpty()) {
            return createErrorResponse("Fragments (new files) are required for commitFileGroups", "BadRequestException", 400);
        }

        List<Fragment> fragments = convertToFragments(fragmentMaps);
        context.setFragments(fragments);

        // Extract rewritten fragments (old files to remove)
        Object rewrittenObj = request.get("rewrittenFragments");
        List<Map<String, Object>> rewrittenMaps = parseFragmentList(rewrittenObj);

        if (rewrittenMaps == null || rewrittenMaps.isEmpty()) {
            return createErrorResponse("rewrittenFragments (old files) are required for commitFileGroups", "BadRequestException", 400);
        }

        List<Fragment> rewrittenFragments = convertToFragments(rewrittenMaps);
        context.setRewrittenFragments(rewrittenFragments);

        log.info("commitFileGroups: {} new fragments, {} rewritten fragments",
            fragments.size(), rewrittenFragments.size());

        // Perform commit file groups operation
        Map<String, Object> result = icebergService.commitFileGroups(namespace, table, properties, context);

        // Generate ETag header
        String etag = "commitFileGroups-" + System.currentTimeMillis() + "-" + Math.abs(result.hashCode());

        return ResponseEntity.ok()
                .header("ETag", etag)
                .body(result);
    }

    /**
     * Parse a fragment list from request body (handles both String and List forms).
     */
    private List<Map<String, Object>> parseFragmentList(Object fragmentsObj) throws Exception {
        if (fragmentsObj == null) {
            return null;
        }
        if (fragmentsObj instanceof String) {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue((String) fragmentsObj,
                new TypeReference<List<Map<String, Object>>>() {});
        }
        return (List<Map<String, Object>>) fragmentsObj;
    }

    /**
     * Convert a list of fragment maps to Fragment objects.
     */
    private List<Fragment> convertToFragments(List<Map<String, Object>> fragmentMaps) {
        List<Fragment> fragments = new ArrayList<>();
        for (Map<String, Object> fragmentMap : fragmentMaps) {
            String path = (String) fragmentMap.get("path");
            if (path == null || path.isEmpty()) {
                throw new IllegalArgumentException("Fragment path is required");
            }

            long fileSize = fragmentMap.containsKey("file_size_in_bytes") ?
                ((Number) fragmentMap.get("file_size_in_bytes")).longValue() : 0L;
            long recordCount = fragmentMap.containsKey("record_count") ?
                ((Number) fragmentMap.get("record_count")).longValue() : 0L;
            String format = fragmentMap.containsKey("format") ?
                (String) fragmentMap.get("format") : "PARQUET";

            GpdbFragmentMetadata metadata = new GpdbFragmentMetadata(fileSize, format, recordCount, "DATA_FILE");
            Fragment fragment = new Fragment(path, metadata);
            fragments.add(fragment);
        }
        return fragments;
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

        String namespace = (String) request.get("namespace");
        if (namespace == null || namespace.trim().isEmpty()) {
            throw new IllegalArgumentException("Namespace is required in request body");
        }

        log.info("commitAppend for table: {}.{}", namespace, table);

        Map<String, String> properties = extractProperties(request);
        RequestContext context = createRequestContext(namespace, table, properties);

        Object fragmentsObj = request.get("fragments");
        List<Map<String, Object>> fragmentMaps = parseFragmentList(fragmentsObj);
        if (fragmentMaps == null || fragmentMaps.isEmpty()) {
            return createErrorResponse("Fragments are required for commitAppend", "BadRequestException", 400);
        }

        List<Fragment> fragments = convertToFragments(fragmentMaps);
        context.setFragments(fragments);

        Map<String, Object> result = icebergService.commitAppend(namespace, table, properties, context);
        return ResponseEntity.ok().body(result);
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

        String namespace = (String) request.get("namespace");
        if (namespace == null || namespace.trim().isEmpty()) {
            throw new IllegalArgumentException("Namespace is required in request body");
        }

        log.info("commitUpdate for table: {}.{}", namespace, table);

        Map<String, String> properties = extractProperties(request);
        RequestContext context = createRequestContext(namespace, table, properties);

        Object fragmentsObj = request.get("updateFragments");
        List<Map<String, Object>> fragmentMaps = parseFragmentList(fragmentsObj);
        if (fragmentMaps == null || fragmentMaps.isEmpty()) {
            return createErrorResponse("updateFragments are required for commitUpdate", "BadRequestException", 400);
        }

        List<Fragment> fragments = new ArrayList<>();
        for (Map<String, Object> fragmentMap : fragmentMaps) {
            String path = (String) fragmentMap.get("path");
            if (path == null || path.isEmpty()) {
                throw new IllegalArgumentException("Fragment path is required");
            }
            long fileSize = fragmentMap.containsKey("file_size_in_bytes") ?
                ((Number) fragmentMap.get("file_size_in_bytes")).longValue() : 0L;
            long recordCount = fragmentMap.containsKey("record_count") ?
                ((Number) fragmentMap.get("record_count")).longValue() : 0L;
            String format = fragmentMap.containsKey("format") ?
                (String) fragmentMap.get("format") : "PARQUET";
            String contentTypeStr = (String) fragmentMap.get("position_on_delete");
            GpdbFragmentMetadata metadata = new GpdbFragmentMetadata(fileSize, format, recordCount, contentTypeStr);
            Fragment fragment = new Fragment(path, metadata);
            fragments.add(fragment);
        }
        context.setFragments(fragments);

        Map<String, Object> result = icebergService.commitUpdate(namespace, table, properties, context);
        return ResponseEntity.ok().body(result);
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

        String namespace = (String) request.get("namespace");
        if (namespace == null || namespace.trim().isEmpty()) {
            throw new IllegalArgumentException("Namespace is required in request body");
        }

        log.info("commitRewrite for table: {}.{}", namespace, table);

        Map<String, String> properties = extractProperties(request);
        RequestContext context = createRequestContext(namespace, table, properties);

        // Extract new fragments (files to add)
        Object fragmentsObj = request.get("fragments");
        List<Map<String, Object>> fragmentMaps = parseFragmentList(fragmentsObj);
        if (fragmentMaps == null || fragmentMaps.isEmpty()) {
            return createErrorResponse("Fragments (new files) are required for commitRewrite", "BadRequestException", 400);
        }
        List<Fragment> fragments = convertToFragments(fragmentMaps);
        context.setFragments(fragments);

        // Extract rewritten fragments (old files to remove)
        Object rewrittenObj = request.get("rewrittenFragments");
        List<Map<String, Object>> rewrittenMaps = parseFragmentList(rewrittenObj);
        if (rewrittenMaps == null || rewrittenMaps.isEmpty()) {
            return createErrorResponse("rewrittenFragments (old files) are required for commitRewrite", "BadRequestException", 400);
        }
        List<Fragment> rewrittenFragments = convertToFragments(rewrittenMaps);
        context.setRewrittenFragments(rewrittenFragments);

        Map<String, Object> result = icebergService.commitRewrite(namespace, table, properties, context);
        return ResponseEntity.ok().body(result);
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

        // Extract namespace from request body
        String namespace = (String) request.get("namespace");
        if (namespace == null || namespace.trim().isEmpty()) {
            throw new IllegalArgumentException("Namespace is required in request body");
        }

        log.info("Dropping table: {}.{}", namespace, table);

        // Extract configurations from request
        Map<String, String> properties = extractProperties(request);

        // Extract purgeRequested flag
        boolean purgeRequested = false;
        if (request.containsKey("purgeRequested")) {
            purgeRequested = (Boolean) request.get("purgeRequested");
        }

        // Create request context
        RequestContext context = createRequestContext(namespace, table, properties);

        // Drop the table
        boolean dropped = icebergService.dropTable(namespace, table, purgeRequested, properties, context);

        if (!dropped) {
            return createErrorResponse("Failed to drop table", "ServerException", 500);
        }

        return ResponseEntity.noContent().build();
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

        log.info("Creating catalog");

        // Extract configurations from request
        Map<String, String> properties = extractProperties(request);

        // Extract catalog information from new format
        Map<String, Object> catalogInfo = (Map<String, Object>) request.get("catalog");
        if (catalogInfo == null) {
            return createErrorResponse("Catalog information is required", "BadRequestException", 400);
        }

        String catalogName = (String) catalogInfo.get("name");
        if (catalogName == null || catalogName.isEmpty()) {
            return createErrorResponse("Catalog name is required", "BadRequestException", 400);
        }

        Map<String, String> catalogProperties = (Map<String, String>) catalogInfo.getOrDefault("properties", new HashMap<>());
        Map<String, Object> storageConfigInfo = (Map<String, Object>) catalogInfo.getOrDefault("storageConfigInfo", new HashMap<>());

        boolean catalogCreated = icebergService.createCatalog(catalogName, catalogProperties, storageConfigInfo, properties);

        Map<String, Object> response = new HashMap<>();
        response.put("success", catalogCreated);
        response.put("catalogName", catalogName);
        response.put("message", catalogCreated ? 
            "Catalog created successfully" : 
            "Catalog creation failed");

        return ResponseEntity.status(catalogCreated ? 201 : 409).body(response);
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

        log.info("Creating namespace");

        // Extract configurations from request
        Map<String, String> properties = extractProperties(request);

        String catalogName = (String) request.get("catalogName");
        String namespaceName = (String) request.get("namespaceName");

        if (catalogName == null || catalogName.isEmpty()) {
            return createErrorResponse("Catalog name is required", "BadRequestException", 400);
        }

        if (namespaceName == null || namespaceName.isEmpty()) {
            return createErrorResponse("Namespace name is required", "BadRequestException", 400);
        }

        Map<String, String> namespaceProperties = (Map<String, String>) request.getOrDefault("namespaceProperties", new HashMap<>());

        boolean namespaceCreated = icebergService.createNamespace(catalogName, namespaceName, namespaceProperties, properties);

        Map<String, Object> response = new HashMap<>();
        response.put("success", namespaceCreated);
        response.put("catalogName", catalogName);
        response.put("namespaceName", namespaceName);
        response.put("message", namespaceCreated ? 
            "Namespace created successfully" : 
            "Namespace creation failed");

        return ResponseEntity.status(namespaceCreated ? 201 : 409).body(response);
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

        log.info("Listing all catalogs");

        // Extract configurations from request
        Map<String, String> properties = extractProperties(request);

        List<String> catalogs = icebergService.listCatalogs(properties);

        Map<String, Object> response = new HashMap<>();
        response.put("catalogs", catalogs);

        return ResponseEntity.ok(response);
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

        log.info("Listing namespaces");

        // Extract configurations from request
        Map<String, String> properties = extractProperties(request);

        String catalogName = (String) request.get("catalogName");
        if (catalogName == null || catalogName.isEmpty()) {
            return createErrorResponse("Catalog name is required", "BadRequestException", 400);
        }

        List<String> namespaces = icebergService.listNamespaces(catalogName, properties);

        Map<String, Object> response = new HashMap<>();
        response.put("namespaces", namespaces);

        return ResponseEntity.ok(response);
    }

    /**
     * Helper method to create error response
     */
    private ResponseEntity<?> createErrorResponse(String message, String type, int code) {
        Map<String, Object> errorResponse = new HashMap<>();
        Map<String, Object> error = new HashMap<>();
        error.put("message", message);
        error.put("type", type);
        error.put("code", code);
        errorResponse.put("error", error);
        return ResponseEntity.status(code).body(errorResponse);
    }
    /**
     * Extract ONLY user-facing Iceberg TBLPROPERTIES from the request body.
     *
     * <p>Thin delegate to {@link IcebergRequestConfigParser#parseUserTableProperties(Map)};
     * the parser is the sole owner of this filtering logic.
     */
    private Map<String, String> extractUserTableProperties(Map<String, Object> request) {
        return requestParser.parseUserTableProperties(request);
    }

    /**
     * Build the legacy flat properties map consumed by downstream
     * {@code IcebergService} methods.
     *
     * <p>Thin delegate to {@link IcebergRequestConfigParser#parse(Map)}:
     * the parser is the sole entry point for interpreting an iceberg request
     * body. The verbatim {@code request.properties} pass-through preserves
     * legacy semantics where the C side piggy-backs {@code buildInCatalog.*}
     * plumbing keys into the same JSON {@code properties} field.
     */
    private Map<String, String> extractProperties(Map<String, Object> request) {
        IcebergRequestConfig cfg = requestParser.parse(request);
        Map<String, String> properties = cfg.toFlatPropertiesMap();

        // Preserve legacy: pass-through the raw request.properties unfiltered
        // so downstream code that reads buildInCatalog.* / other internal keys
        // continues to work.
        Object rawProps = request.get(IcebergConfigConstants.PROPERTIES);
        if (rawProps instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> userProps = (Map<String, Object>) rawProps;
            for (Map.Entry<String, Object> entry : userProps.entrySet()) {
                if (entry.getValue() != null) {
                    properties.put(entry.getKey(), entry.getValue().toString());
                }
            }
        }

        if (log.isDebugEnabled()) {
            log.debug("Extracted properties: {}", properties);
        }
        return properties;
    }

    /**
     * Helper method to convert Iceberg Table to metadata response
     */
    private Map<String, Object> convertTableToMetadata(Table table) {
        Map<String, Object> metadata = new HashMap<>();

        // Required fields
        metadata.put("format-version", 2); // Default to format version 2

        // Basic table information
        metadata.put("location", table.location());
        metadata.put("last-updated-ms", table.currentSnapshot() != null ?
                table.currentSnapshot().timestampMillis() : System.currentTimeMillis());

        // Schema tracking
        Schema currentSchema = table.schema();
        metadata.put("current-schema-id", currentSchema.schemaId());
        metadata.put("last-column-id", currentSchema.highestFieldId());

        // Convert schemas to list format
        List<Map<String, Object>> schemas = new ArrayList<>();
        for (Schema schema : table.schemas().values()) {
            Map<String, Object> schemaMap = new HashMap<>();
            schemaMap.put("schema-id", schema.schemaId());
            schemaMap.put("fields", convertSchemaFields(schema.columns()));
            schemas.add(schemaMap);
        }
        metadata.put("schemas", schemas);

        // Partition spec tracking
        PartitionSpec currentSpec = table.spec();
        metadata.put("default-spec-id", currentSpec.specId());

        List<Map<String, Object>> partitionSpecs = new ArrayList<>();
        for (PartitionSpec spec : table.specs().values()) {
            Map<String, Object> specMap = new HashMap<>();
            specMap.put("spec-id", spec.specId());
            specMap.put("fields", convertPartitionFields(spec.fields()));
            partitionSpecs.add(specMap);
        }
        metadata.put("partition-specs", partitionSpecs);

        // Sort order tracking
        SortOrder currentSortOrder = table.sortOrder();
        metadata.put("default-sort-order-id", currentSortOrder.orderId());

        List<Map<String, Object>> sortOrders = new ArrayList<>();
        for (SortOrder sortOrder : table.sortOrders().values()) {
            Map<String, Object> sortOrderMap = new HashMap<>();
            sortOrderMap.put("order-id", sortOrder.orderId());
            sortOrderMap.put("fields", convertSortFields(sortOrder.fields()));
            sortOrders.add(sortOrderMap);
        }
        metadata.put("sort-orders", sortOrders);

        // Snapshot tracking
        if (table.currentSnapshot() != null) {
            metadata.put("current-snapshot-id", table.currentSnapshot().snapshotId());
        }

        List<Map<String, Object>> snapshots = new ArrayList<>();
        for (Snapshot snapshot : table.snapshots()) {
            Map<String, Object> snapshotMap = new HashMap<>();
            snapshotMap.put("snapshot-id", snapshot.snapshotId());
            snapshotMap.put("timestamp-ms", snapshot.timestampMillis());
            snapshotMap.put("summary", snapshot.summary());
            snapshotMap.put("manifest-list", snapshot.manifestListLocation());
            snapshotMap.put("schema-id", snapshot.schemaId());
            snapshots.add(snapshotMap);
        }
        metadata.put("snapshots", snapshots);

        // Snapshot references
        Map<String, Object> refs = new HashMap<>();
        for (Map.Entry<String, SnapshotRef> entry : table.refs().entrySet()) {
            SnapshotRef ref = entry.getValue();
            Map<String, Object> refMap = new HashMap<>();
            refMap.put("snapshot-id", ref.snapshotId());
            refMap.put("type", ref.isBranch() ? "branch" : "tag");
            refs.put(entry.getKey(), refMap);
        }
        metadata.put("refs", refs);

        // Properties — expose only user-facing TBLPROPERTIES. Legacy tables
        // may have internal runtime config leaked into table.properties(); this
        // filter prevents re-exposure of e.g. secret_access_key or node-local
        // gopher paths over HTTP.
        metadata.put("properties",
                IcebergUtilities.stripInternalProperties(
                        new HashMap<>(table.properties())));

        return metadata;
    }

    private List<Map<String, Object>> convertSchemaFields(List<Types.NestedField> fields) {
        List<Map<String, Object>> fieldList = new ArrayList<>();
        for (Types.NestedField field : fields) {
            Map<String, Object> fieldMap = new HashMap<>();
            fieldMap.put("id", field.fieldId());
            fieldMap.put("name", field.name());
            fieldMap.put("required", field.isRequired());
            fieldMap.put("type", field.type().toString());
            if (field.doc() != null) {
                fieldMap.put("doc", field.doc());
            }
            fieldList.add(fieldMap);
        }
        return fieldList;
    }

    private List<Map<String, Object>> convertPartitionFields(List<PartitionField> fields) {
        List<Map<String, Object>> fieldList = new ArrayList<>();
        for (PartitionField field : fields) {
            Map<String, Object> fieldMap = new HashMap<>();
            fieldMap.put("source-id", field.sourceId());
            fieldMap.put("field-id", field.fieldId());
            fieldMap.put("name", field.name());
            fieldMap.put("transform", field.transform().toString());
            fieldList.add(fieldMap);
        }
        return fieldList;
    }

    private List<Map<String, Object>> convertSortFields(List<SortField> fields) {
        List<Map<String, Object>> fieldList = new ArrayList<>();
        for (SortField field : fields) {
            Map<String, Object> fieldMap = new HashMap<>();
            fieldMap.put("transform", field.transform().toString());
            fieldMap.put("source-id", field.sourceId());
            fieldMap.put("direction", field.direction().toString());
            fieldMap.put("null-order", field.nullOrder().toString());
            fieldList.add(fieldMap);
        }
        return fieldList;
    }

    /**
     * Helper method to generate ETag with debug information
     * 
     * ETag Format: {timestamp}-{schemaId}-{operation}-{metadataHash}
     * 
     * Components:
     * - timestamp: Table snapshot timestamp (or current time if no snapshot)
     *   Used for: Time-based debugging and version tracking
     * 
     * - schemaId: Table schema version identifier
     *   Used for: Identifying schema changes and structure versions
     * 
     * - operation: The operation that generated this ETag (load/create/fragment)
     *   Used for: Tracing the source operation for debugging
     * 
     * - metadataHash: Hash of table metadata for uniqueness
     *   Used for: Ensuring different table states have different ETags
     * 
     * Example ETags:
     * - "1694512345678-1-load-123456789" (loaded existing table)
     * - "1694512346789-2-create-987654321" (newly created table)
     * - "fragment-1694512347890-456789123" (table fragment)
     * 
     * Benefits:
     * - Enables HTTP caching with 304 Not Modified responses
     * - Provides debugging information for troubleshooting
     * - Supports concurrent access control
     * - Allows version tracking across operations
     */
    private String generateETag(Table table, String operation) {
        try {
            StringBuilder etagBuilder = new StringBuilder();

            // Add timestamp for debugging and version tracking
            long timestamp = table.currentSnapshot() != null ? 
                table.currentSnapshot().timestampMillis() : System.currentTimeMillis();
            etagBuilder.append(timestamp);

            // Add schema ID for structure version identification
            etagBuilder.append("-").append(table.schema().schemaId());

            // Add operation type for source tracing
            etagBuilder.append("-").append(operation);

            // Add hash of metadata for uniqueness guarantee
            int metadataHash = convertTableToMetadata(table).hashCode();
            etagBuilder.append("-").append(Math.abs(metadataHash));

            String etag = etagBuilder.toString();
            log.debug("Generated ETag for {} operation: {} (format: timestamp-schemaId-operation-metadataHash)", 
                     operation, etag);

            return etag;
        } catch (Exception e) {
            log.warn("Failed to generate detailed ETag, using fallback timestamp: {}", e.getMessage());
            return String.valueOf(System.currentTimeMillis());
        }
    }

    /**
     * Helper method to convert Map to Iceberg Schema
     */
    private Schema convertMapToSchema(Map<String, Object> schemaMap) {
        return schemaConverter.fromJson(schemaMap);
    }

    /**
     * Extract the gopher.* subset from the flat properties map.
     *
     * <p>The map is already populated with normalized {@code gopher.*} keys by
     * {@link IcebergRequestConfigParser} (which delegates the
     * Volume-to-gopher translation and normalization — s3 -> s3a, useHttps,
     * useVirtualHost, endpoint scheme stripping — to
     * {@code GopherPropertiesResolver}). This method is now a thin filter.
     */
    private Map<String, String> getGopherProps(Map<String, String> properties) {
        Map<String, String> gopherProps = new HashMap<>();
        String prefix = IcebergConfigConstants.GOPHER_CONFIG.GOPHER_HEADER + ".";
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith(prefix)) {
                gopherProps.put(entry.getKey(), entry.getValue());
            }
        }
        return gopherProps;
    }

    private Map<String, String> getBuildInCatalogProps(Map<String, String> properties) {
        Map<String, String> buildInCatalogProps = new HashMap<>();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith(IcebergConfigConstants.BUILDIN_CATALOG_OPTION.BUILDIN_CATALOG_STRING + ".")) {
                buildInCatalogProps.put(entry.getKey(), entry.getValue());
            }
        }
        return buildInCatalogProps;
    }

    private static String volKey(String key) {
        return IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.ICEBERG_VOLUME_CONFIG_STRING + "." + key;
    }

    private static String catKey(String key) {
        return IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.ICEBERG_CATALOG_CONFIG_STRING + "." + key;
    }

    /**
     * Mask secret-bearing entries so gopher param logs are diagnostic without
     * leaking credentials. Matches any key containing secret / access_key /
     * password / token / keytab (case-insensitive).
     */
    private static Map<String, String> redactSecrets(Map<String, String> props) {
        java.util.LinkedHashMap<String, String> safe = new java.util.LinkedHashMap<>();
        for (Map.Entry<String, String> e : props.entrySet()) {
            String k = e.getKey().toLowerCase();
            if (k.contains("secret") || k.contains("access_key") || k.contains("access-key")
                    || k.contains("password") || k.contains("token") || k.contains("keytab")) {
                safe.put(e.getKey(), "***");
            } else {
                safe.put(e.getKey(), e.getValue());
            }
        }
        return safe;
    }

    /**
     * Emit S3 storage credentials for the DATA domain from inline
     * IcebergVolumeConfig options.
     *
     * <p>Writes BOTH credential key families — iceberg-aws ({@code s3.*}) and
     * hadoop-aws ({@code fs.s3a.*}). The actual FileIO is chosen later by
     * {@code IcebergUtilities.composeCatalogProperties} (GopherFileIO when
     * gopher.enabled, otherwise the mixed ResolvingFileIO which dispatches s3://
     * to S3FileIO and other schemes to HadoopFileIO), so there is deliberately
     * no explicit FileIO-impl selection here.
     */
    private void emitS3Inline(Configuration configuration, Map<String, String> properties) {
        String accessKey = properties.get(volKey(IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.ACCESS_KEY_ID));
        String secretKey = properties.get(volKey(IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.SECRET_ACCESS_KEY));

        /* Diagnostic trace: see iceberg_volume_option.c step2 for context. */
        LOG.debug("[trace_ak] step5 emitS3Inline: read key=[{}] access_key len={}, secret len={}",
            volKey(IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.ACCESS_KEY_ID),
            accessKey == null ? -1 : accessKey.length(),
            secretKey == null ? -1 : secretKey.length());

        String endpoint = properties.get(volKey(IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.VOLUME_ENDPOINT));
        String region = properties.getOrDefault(volKey(IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.VOLUME_REGION),
                IcebergConfigConstants.DEFAULT_S3_REGION_VALUE);
        Boolean pathStyleAccess = Boolean.parseBoolean(
                properties.getOrDefault(volKey(IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.PATH_STYLE_ACCESS), "true"));

        // hadoop-aws (HadoopFileIO via S3AFileSystem)
        configuration.set(IcebergConfigConstants.FS_S3A_IMPL, IcebergConfigConstants.S3A_FILESYSTEM_IMPL);
        configuration.set(IcebergConfigConstants.FS_S3A_AWS_CREDENTIALS_PROVIDER, IcebergConfigConstants.S3A_CREDENTIALS_PROVIDER);
        if (accessKey != null) configuration.set(IcebergConfigConstants.FS_S3A_ACCESS_KEY, accessKey);
        if (secretKey != null) configuration.set(IcebergConfigConstants.FS_S3A_SECRET_KEY, secretKey);
        if (endpoint != null) configuration.set(IcebergConfigConstants.FS_S3A_ENDPOINT, endpoint);
        configuration.set(IcebergConfigConstants.FS_S3A_PATH_STYLE_ACCESS, pathStyleAccess.toString());

        // iceberg-aws (S3FileIO / ResolvingFileIO)
        if (accessKey != null) configuration.set(IcebergConfigConstants.S3FILEIO_ACCESS_KEY_ID, accessKey);
        if (secretKey != null) configuration.set(IcebergConfigConstants.S3FILEIO_SECRET_ACCESS_KEY, secretKey);
        if (endpoint != null) configuration.set(IcebergConfigConstants.S3FILEIO_ENDPOINT, endpoint);
        if (region != null) configuration.set(IcebergConfigConstants.S3FILEIO_REGION, region);
        configuration.set(IcebergConfigConstants.S3FILEIO_PATH_STYLE_ACCESS, pathStyleAccess.toString());

        // Warehouse location for Hive-metastore-backed catalogs.
        String bucketName = properties.get(volKey(IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.BUCKET_NAME));
        String basePath = properties.getOrDefault(volKey(IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.BASE_PATH), "/hive");
        if (bucketName != null) {
            configuration.set("hive.metastore.warehouse.dir", String.format("s3a://%s%s", bucketName, basePath));
        }

        // gopher path: IcebergUtilities.convertProtocolConfiguration only harvests
        // fs.gopher.* keys (the shape s3.conf produces). Mirror credentials there so
        // GopherFileIO/GopherFileSystem get endpoint + keys in the inline case too;
        // without this the gopher S3 path reaches gophermeta with empty OSS config.
        // These keys are inert when gopher is off (ResolvingFileIO reads s3.*).
        if (bucketName != null) configuration.set("fs.gopher.bucket", bucketName);
        if (accessKey != null) configuration.set("fs.gopher.access_key", accessKey);
        if (secretKey != null) configuration.set("fs.gopher.secret_key", secretKey);
        if (endpoint != null) {
            boolean useHttps = endpoint.startsWith("https://");
            String hostPort = endpoint.replaceFirst("^https?://", "");
            configuration.set("fs.gopher.endpoint", hostPort);
            configuration.set("fs.gopher.use_https", Boolean.toString(useHttps));
        }
        // Deliberately NOT setting fs.gopher.region: when present, gopher's native
        // OSS client builds an AWS-style host (s3.<region>.amazonaws.com) instead of
        // using fs.gopher.endpoint, which produces CURLE_COULDNT_RESOLVE_HOST against
        // self-hosted endpoints like minio. The working s3.conf shape omits region.
        configuration.set("fs.gopher.use_virtual_host", Boolean.toString(!pathStyleAccess));
        // Pass volume_server_type through verbatim — iceberg-gopher accepts
        // s3 / s3v2 natively. Hadoop fs.s3a URI/key naming is unaffected.
        String volType = properties.getOrDefault(
                volKey(IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.VOLUME_SERVER_TYPE), "");
        if (!volType.isEmpty()) configuration.set("fs.gopher.ufs_type", volType);

        // Pass through all FileIOConfig.properties to configuration.
        String prefix = IcebergConfigConstants.FILE_IO_CONFIG_PROPERTIES_PREFIX + ".";
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith(prefix)) {
                configuration.set(entry.getKey().substring(prefix.length()), entry.getValue());
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("data: emitS3Inline wrote fs.s3a.*/s3.* (endpoint={}, region={}, pathStyle={}, warehouse={})",
                    endpoint, region, pathStyleAccess,
                    bucketName == null ? "<none>" : "s3a://" + bucketName + basePath);
            // Also dump the fs.gopher.* keys we just set so future "gopher param wrong"
            // debugging (e.g. endpoint with scheme, ufs_type=s3, virtual-host flips)
            // can be checked at this layer without bumping native LIBOSS2 trace.
            Map<String, String> fsGopher = new java.util.LinkedHashMap<>();
            for (Map.Entry<String, String> e : configuration) {
                if (e.getKey().startsWith("fs.gopher.")) {
                    fsGopher.put(e.getKey(), e.getValue());
                }
            }
            LOG.debug("data: emitS3Inline fs.gopher.* set: {}", redactSecrets(fsGopher));
        }
    }

    private String getCatalogWarehouseLocationPrefix(Map<String, String> properties) {
        String warehouseLocationPrefix = IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.ICEBERG_CATALOG_CONFIG_STRING + "." + IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.WAREHOUSE_LOCATION_PERFIX;
        String basePath = properties.getOrDefault(warehouseLocationPrefix, "");
        return basePath;
    }

    /**
     * DATA domain: configure storage / FileIO access for reading and writing
     * Iceberg data + manifest files.
     *
     * <p>Per-domain source switch: if the volume specifies a config-file section
     * ({@code server_name}), the whole domain is read from that section of
     * s3.conf / gphdfs.conf and the inline IcebergVolumeConfig options are
     * ignored; otherwise the inline options are used.
     */
    private void buildDataConfig(Configuration configuration, Map<String, String> properties) {
        String volumeType = properties.getOrDefault(
                volKey(IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.VOLUME_SERVER_TYPE), "");
        if (volumeType.isEmpty()) {
            // No volume (e.g. some polaris setups). Storage credentials, if any,
            // reach the catalog through gopherProps; nothing to do here.
            LOG.debug("data: no volume_server_type; skipping data config (credentials, if any, via gopherProps)");
            return;
        }
        String serverName = properties.get(volKey(IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.SERVER_NAME));
        boolean useConfFile = serverName != null && !serverName.isEmpty();

        if (volumeType.equals(IcebergConfigConstants.VOLUME_TYPE_S3) ||
            volumeType.equals(IcebergConfigConstants.VOLUME_TYPE_S3V2) ||
            volumeType.equals(IcebergConfigConstants.VOLUME_TYPE_ABFSS)) {
            if (useConfFile) {
                String s3Location = deriveS3Location(configuration, properties);
                LOG.debug("data: type={}, source=conf-file s3.conf[{}] location={} (inline access_key/secret ignored)",
                        volumeType, serverName, s3Location);
                loadConfFile("s3.conf", serverName, "data", s3Location, configuration);
            } else {
                LOG.debug("data: type={}, source=inline", volumeType);
                emitS3Inline(configuration, properties);
            }
        } else if (volumeType.equals(IcebergConfigConstants.VOLUME_TYPE_HDFS)) {
            if (useConfFile) {
                LOG.debug("data: type=hdfs, source=conf-file gphdfs.conf[{}] (inline options ignored)", serverName);
                loadConfFile("gphdfs.conf", serverName, "data", "", configuration);
            } else {
                LOG.debug("data: type=hdfs, source=inline");
                emitHdfsInline(configuration, properties);
            }
        } else {
            throw new UnsupportedOperationException(
                    volKey(IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.VOLUME_SERVER_TYPE) + " '" + volumeType
                    + "' is not supported yet. Supported types are: ["
                    + IcebergConfigConstants.VOLUME_TYPE_S3 + ", " + IcebergConfigConstants.VOLUME_TYPE_S3V2 + ", "
                    + IcebergConfigConstants.VOLUME_TYPE_HDFS + ", " + IcebergConfigConstants.VOLUME_TYPE_ABFSS + "]");
        }
    }

    /**
     * Configure Hadoop {@link Configuration} for an HDFS-backed volume.
     *
     * The user-facing OPTIONS surface is:
     *   CREATE SERVER ... FOREIGN DATA WRAPPER iceberg_volume_fdw
     *     OPTIONS (type 'hdfs', endpoint 'hdfs://namenode:8020' [, hdfs_auth_method 'simple|kerberos'] ...);
     *
     * The C side ships these through as IcebergVolumeConfig.volume_endpoint
     * (and potentially auth-related keys for Kerberos).  We translate them
     * into the standard Hadoop keys here so HadoopFileIO and DistributedFileSystem
     * pick them up.  Simple auth is the default to match the C side's
     * iceberg_volume_fdw.parseVolumeOption fallback.
     */
    private void emitHdfsInline(Configuration configuration, Map<String, String> properties) {
        String endpointKey = IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.ICEBERG_VOLUME_CONFIG_STRING + "."
            + IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.VOLUME_ENDPOINT;
        String endpoint = properties.getOrDefault(endpointKey, "");

        if (!endpoint.isEmpty()) {
            // Accept "hdfs://host:port" or just "host:port".
            String defaultFS = endpoint.startsWith("hdfs://") ? endpoint : "hdfs://" + endpoint;
            // fs.defaultFS already set by setCatalogWarehouseInline for hadoop
            // catalog; only set when not already configured (e.g. builtin/hive
            // catalog with hdfs volume).
            if (configuration.get("fs.defaultFS") == null
                || configuration.get("fs.defaultFS").isEmpty()) {
                configuration.set("fs.defaultFS", defaultFS);
            }
        }

        // Default to simple auth when caller didn't specify; matches
        // the C-side iceberg_volume_fdw default in parseVolumeOption.
        if (configuration.get("hadoop.security.authentication") == null) {
            configuration.set("hadoop.security.authentication", "simple");
        }
    }

    private void setHiveMetaInline(Configuration configuration, Map<String, String> properties) {
        String hive_metastore_uris = properties.getOrDefault(
                catKey(IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.HIVE_METASTORE_URI), "");
        if (hive_metastore_uris.isEmpty()) {
            throw new IllegalArgumentException(
                    catKey(IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.HIVE_METASTORE_URI) + " is empty, must specify it");
        }
        configuration.set("hive.metastore.uris", hive_metastore_uris);

        String auth_method = properties.getOrDefault(
                catKey(IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.AUTH_METHOD), "");
        if (!auth_method.isEmpty()) {
            configuration.set("hadoop.security.authentication", auth_method);
        }
    }

    private void setPolarisInline(Configuration configuration, Map<String, String> properties) {
        String polarisServerUrlKey = IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.ICEBERG_CATALOG_CONFIG_STRING + "." + IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.POLARIS_SERVER_URL;
        String polaris_server_url = properties.getOrDefault(polarisServerUrlKey, "");
        if (!polaris_server_url.isEmpty()) {
            configuration.set(IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.POLARIS_SERVER_URL, polaris_server_url);
        }

        String clientIdKey = IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.ICEBERG_CATALOG_CONFIG_STRING + "." + IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.CLIENT_ID;
        String client_id = properties.getOrDefault(clientIdKey, "");
        if (!client_id.isEmpty()) {
            configuration.set(IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.CLIENT_ID, client_id);
        }

        String clientSecretKey = IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.ICEBERG_CATALOG_CONFIG_STRING + "." + IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.CLIENT_SECRET;
        String client_secret = properties.getOrDefault(clientSecretKey, "");
        if (!client_secret.isEmpty()) {
            configuration.set(IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.CLIENT_SECRET, client_secret);
        }

        String scopeKey = IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.ICEBERG_CATALOG_CONFIG_STRING + "." + IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.SCOPE;
        String scope = properties.getOrDefault(scopeKey, "");
        if (!scope.isEmpty()) {
            configuration.set(IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.SCOPE, scope);
        }
        // Volume credentials reach IcebergPolarisCatalog through
        // gopherProperties (translated from SQL options by
        // GopherPropertiesResolver via IcebergRequestConfigParser); Polaris
        // always uses GopherFileIO, so no S3A / S3FileIO config here.
    }

    /**
     * Initialize Configuration for s3/s3a/hadoop catalog servers from the
     * single user-facing warehouse_location_prefix property.
     *
     * The two iceberg-side catalog implementations have different warehouse
     * computation contracts that must be preserved here:
     *
     *   IcebergS3Catalog     warehouse = fs.defaultFS + "/" + fs.prefix
     *   IcebergHadoopCatalog warehouse = fs.defaultFS + catalogLocation
     *                        (catalogLocation = RequestContext.path)
     *
     * This helper splits the URL once and writes whichever Configuration keys
     * the target catalog reads. The hadoop case only needs fs.defaultFS here;
     * the path component is stamped into RequestContext.path by
     * {@link #createRequestContext}.
     *
     * Required because the original s3/s3a branch of processServerResource
     * was a no-op, so warehouse_location_prefix never reached the catalog
     * and IcebergS3Catalog computed warehouse as the literal "null/".
     */
    private void setCatalogWarehouseInline(Configuration configuration,
                                                  Map<String, String> properties,
                                                  boolean isHadoopCatalog) {
        String warehouse = getCatalogWarehouseLocationPrefix(properties);
        if (warehouse == null || warehouse.isEmpty()) {
            String key = IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.ICEBERG_CATALOG_CONFIG_STRING
                + "." + IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.WAREHOUSE_LOCATION_PERFIX;
            throw new IllegalArgumentException(
                key + " is required for s3/hadoop catalog server");
        }
        int schemeEnd = warehouse.indexOf("://");
        if (schemeEnd < 0) {
            throw new IllegalArgumentException(
                "warehouse_location_prefix must be a URL like scheme://host/path: " + warehouse);
        }
        int pathStart = warehouse.indexOf('/', schemeEnd + 3);
        String defaultFS;
        String pathPart; // includes leading '/'
        if (pathStart < 0) {
            defaultFS = warehouse;
            pathPart = "";
        } else {
            defaultFS = warehouse.substring(0, pathStart);
            pathPart = warehouse.substring(pathStart);
        }
        configuration.set("fs.defaultFS", defaultFS);

        if (!isHadoopCatalog) {
            // S3Catalog: warehouse = fs.defaultFS + "/" + fs.prefix.
            // The convention is no leading slash on fs.prefix. Strip
            // trailing slashes too to avoid double "//".
            String prefix = pathPart.startsWith("/") ? pathPart.substring(1) : pathPart;
            while (prefix.endsWith("/")) {
                prefix = prefix.substring(0, prefix.length() - 1);
            }
            configuration.set("fs.prefix", prefix);
        }

    }

    /**
     * META domain: configure how to reach Iceberg table metadata (catalog).
     *
     * <p>Per-domain source switch: a {@code hive} catalog that specifies a
     * config-file section ({@code server_name}) reads its metastore connection
     * from gphive.conf[server_name]; otherwise the inline IcebergCatalogConfig
     * options are used. polaris / hadoop / s3 catalogs have no gphive.conf form
     * and always use inline options. The DATA domain (volume credentials) is
     * configured separately by {@link #buildDataConfig}.
     */
    private void buildMetaConfig(Configuration configuration, Map<String, String> properties) {
        String catalogType = properties.getOrDefault(
                catKey(IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.SERVER_TYPE), "");
        String serverName = properties.get(catKey(IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.SERVER_NAME));
        boolean useConfFile = serverName != null && !serverName.isEmpty();

        if (catalogType.equals(IcebergConfigConstants.CATALOG_TYPE_HIVE)) {
            if (useConfFile) {
                LOG.debug("meta: type=hive, source=conf-file gphive.conf[{}]", serverName);
                loadConfFile("gphive.conf", serverName, "meta", "", configuration);
            } else {
                LOG.debug("meta: type=hive, source=inline");
                setHiveMetaInline(configuration, properties);
            }
        } else if (catalogType.equals(IcebergConfigConstants.CATALOG_TYPE_POLARIS)) {
            LOG.debug("meta: type=polaris, source=inline");
            setPolarisInline(configuration, properties);
        } else if (catalogType.equals(IcebergConfigConstants.CATALOG_TYPE_S3)) {
            LOG.debug("meta: type={}, source=inline (warehouse)", catalogType);
            setCatalogWarehouseInline(configuration, properties, false);
        } else if (catalogType.equals(IcebergConfigConstants.CATALOG_TYPE_HADOOP)) {
            LOG.debug("meta: type=hadoop, source=inline (warehouse)");
            setCatalogWarehouseInline(configuration, properties, true);
        } else if (catalogType.equals(IcebergConfigConstants.CATALOG_TYPE_BUILDIN)) {
            LOG.debug("meta: type=builtin, no catalog-side config");
        } else {
            throw new UnsupportedOperationException("This server type '" + catalogType + "' is not supported yet. " +
                    "Supported types are: [" +
                    IcebergConfigConstants.CATALOG_TYPE_S3 + ", " +
                    IcebergConfigConstants.CATALOG_TYPE_HADOOP + ", " +
                    IcebergConfigConstants.CATALOG_TYPE_HIVE + ", " + IcebergConfigConstants.CATALOG_TYPE_BUILDIN + ", " +
                    IcebergConfigConstants.CATALOG_TYPE_POLARIS + "]");
        }
    }

    /**
     * Load one $PGDATA config file section into the Configuration for a domain
     * (meta = gphive.conf, data = s3.conf / gphdfs.conf). The section is keyed
     * by that domain's own {@code server_name}. {@code location} is required
     * for s3.conf (BaseConfigurationFactory.transformS3Config parses bucket and
     * prefix from it); pass an empty string for non-s3 conf files. A missing
     * section fails fast with a clear message that names the file, section and
     * domain.
     */
    private void loadConfFile(String configFile, String serverName, String domain,
                              String location, Configuration configuration) {
        try {
            configurationFactory.processServerResource("", configFile, serverName, configuration, location);
        } catch (RuntimeException e) {
            throw new RuntimeException(String.format(
                    "%s config: failed to load section '%s' from '%s': %s",
                    domain, serverName, configFile, e.getMessage()), e);
        }
        LOG.debug("{}: loaded conf-file {}[{}] (location={})", domain, configFile, serverName, location);
    }

    /**
     * Derive the s3a:// URL that transformS3Config needs to extract bucket and
     * prefix when loading s3.conf for the DATA domain.
     *
     * <p>Preference order:
     * <ol>
     *   <li>{@code fs.defaultFS}+{@code fs.prefix} already set by the META phase
     *       (s3/hadoop catalog populates these from warehouse_location_prefix);</li>
     *   <li>{@code IcebergVolumeConfig.bucket_name}+{@code base_path} when the
     *       volume options are inline (e.g. hive catalog + s3 volume);</li>
     *   <li>empty string as a last resort (transformS3Config will reject it).</li>
     * </ol>
     */
    private String deriveS3Location(Configuration configuration, Map<String, String> properties) {
        String defaultFS = configuration.get("fs.defaultFS");
        if (defaultFS != null && (defaultFS.startsWith("s3a://") || defaultFS.startsWith("s3://"))) {
            String prefix = configuration.get("fs.prefix");
            if (prefix != null && !prefix.isEmpty()) {
                return defaultFS + "/" + prefix;
            }
            return defaultFS;
        }
        String bucket = properties.get(volKey(IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.BUCKET_NAME));
        if (bucket != null && !bucket.isEmpty()) {
            String basePath = properties.getOrDefault(
                    volKey(IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.BASE_PATH), "/");
            if (!basePath.startsWith("/")) {
                basePath = "/" + basePath;
            }
            return String.format("s3a://%s%s", bucket, basePath);
        }
        return "";
    }

    private Configuration getConfiguration(Map<String, String> properties) {
        LOG.debug("Initializing configuration for server");
        Configuration configuration = new Configuration(false);

        // gopher.enabled is a process-wide toggle from application.properties; it is
        // intentionally NOT taken from the per-request SQL options so a single call
        // cannot silently flip the FileIO stack.
        if (gopherConfigurationProperties != null
                && gopherConfigurationProperties.getEnabled() != null) {
            configuration.set("gopher.enabled",
                    String.valueOf(gopherConfigurationProperties.getEnabled()));
        }

        // Inject gopher.* system paths (connect_path / connect_plasma_path /
        // worker_path) the C side sends per request, so setupGopherConfiguration()
        // can reach gopher even without config files. These are gopher runtime
        // plumbing — not meta/data config — so they go in before both domains.
        // The request uses underscore keys (gopher.worker_path); Spring's
        // application.properties uses hyphen keys (gopher.worker-path) — different
        // namespaces, and the request's underscore keys are the ones consumed.
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith("gopher.") && !key.equals("gopher.enabled")) {
                configuration.set(key, entry.getValue());
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("gopher: enabled={}, system paths worker_path={}, connect_path={}, connect_plasma_path={}",
                    configuration.get("gopher.enabled"),
                    configuration.get("gopher.worker_path"),
                    configuration.get("gopher.connect_path"),
                    configuration.get("gopher.connect_plasma_path"));
        }

        // Two independent domains: META (catalog) then DATA (storage/FileIO).
        buildMetaConfig(configuration, properties);
        buildDataConfig(configuration, properties);

        logResolvedConfig(configuration, properties);
        return configuration;
    }

    /**
     * One-line INFO summary of how config was resolved this request, so a
     * misconfiguration that silently takes a different branch is visible in
     * the log without needing a stack trace. Contains no sensitive values.
     */
    private void logResolvedConfig(Configuration configuration, Map<String, String> properties) {
        if (!LOG.isInfoEnabled()) {
            return;
        }
        String metaType = properties.getOrDefault(catKey(IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.SERVER_TYPE), "");
        String metaServer = properties.get(catKey(IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.SERVER_NAME));
        String dataType = properties.getOrDefault(volKey(IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.VOLUME_SERVER_TYPE), "");
        String dataServer = properties.get(volKey(IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.SERVER_NAME));
        boolean gopherOn = "true".equalsIgnoreCase(configuration.get("gopher.enabled"));

        String metaSource = (IcebergConfigConstants.CATALOG_TYPE_HIVE.equals(metaType)
                && metaServer != null && !metaServer.isEmpty())
                ? "conf-file gphive.conf[" + metaServer + "]" : "inline";
        String dataSource;
        if (dataType.isEmpty()) {
            dataSource = "none";
        } else if (dataServer != null && !dataServer.isEmpty()) {
            dataSource = "conf-file[" + dataServer + "]";
        } else {
            dataSource = "inline";
        }
        LOG.info("iceberg config resolved: meta[type={}, source={}], data[type={}, source={}], gopher.enabled={}, fileIO={}",
                metaType.isEmpty() ? "<none>" : metaType, metaSource,
                dataType.isEmpty() ? "<none>" : dataType, dataSource,
                gopherOn, gopherOn ? "GopherFileIO" : "ResolvingFileIO");
    }

    /**
     * Helper method to create request context
     * create request context, this was created to
     * reuse the original iceberg logic later
     */
    private RequestContext createRequestContext(String namespace, String table, Map<String, String> properties) {
        if (LOG.isDebugEnabled()) {
            // Logging only keys to prevent sensitive data to be logged
            LOG.debug("Parsing request parameters: {}", String.join(", ", properties.keySet()));
        }
        // Set the properties in the context
        RequestContext context = new RequestContext();

        String totalSegmentKey = IcebergConfigConstants.ICEBERG_ADDITIONAL_CONFIG.ICEBERG_ADDITIONAL_CONFIG_STRING + "." + IcebergConfigConstants.ICEBERG_ADDITIONAL_CONFIG.TOTAL_SEGMENT;
        String splitSizeKey = IcebergConfigConstants.ICEBERG_ADDITIONAL_CONFIG.ICEBERG_ADDITIONAL_CONFIG_STRING + "." + IcebergConfigConstants.ICEBERG_ADDITIONAL_CONFIG.SPLIT_SIZE;
        String filterStringKey = IcebergConfigConstants.ICEBERG_ADDITIONAL_CONFIG.ICEBERG_ADDITIONAL_CONFIG_STRING + "." + IcebergConfigConstants.ICEBERG_ADDITIONAL_CONFIG.FILTER_STRING;

        int totalSegments = Integer.parseInt(properties.getOrDefault(totalSegmentKey, "1"));
        String splitSize = properties.getOrDefault(splitSizeKey, "128");
        String filterString = properties.getOrDefault(filterStringKey, "");
        context.setTotalSegments(totalSegments);
        context.setSplitSize(splitSize);
        if (!filterString.isEmpty()) {
            context.setFilterString(filterString);
        }

        String usernameKey = IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.ICEBERG_VOLUME_CONFIG_STRING + "." + IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.USERNAME;
        String username = properties.getOrDefault(usernameKey, "");
        context.setUser(username);

        // Set common properties
        context.setSchemaName(namespace);
        context.setTableName(table);

        //in hive dataSource is schemaName + tableName
        String catalogServerTypeKey = IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.ICEBERG_CATALOG_CONFIG_STRING + "." + 
                                    IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.SERVER_TYPE;
        String catalogServerType = properties.getOrDefault(catalogServerTypeKey, "");

        if (catalogServerType.equals(IcebergConfigConstants.CATALOG_TYPE_HIVE)
                || catalogServerType.equals(IcebergConfigConstants.CATALOG_TYPE_S3)
                || catalogServerType.equals(IcebergConfigConstants.CATALOG_TYPE_HADOOP)) {
            // For path-based and Hive catalogs the agent identifies tables by
            // <namespace>.<table>. RequestContext.getDataSource() falls back to
            // RequestContext.path when dataSource is null, which would incorrectly
            // hand the warehouse URL to IcebergUtilities.getIcebergTableIdentifier
            // and produce "default.<warehouse-url>" as the table identifier.
            String dataSource = namespace + "." + table;
            context.setDataSource(dataSource);
        } else if (catalogServerType.equals(IcebergConfigConstants.CATALOG_TYPE_POLARIS)) {
            String catalogNameKey = IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.ICEBERG_CATALOG_CONFIG_STRING + "." + 
            IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.CATALOG_NAME;
            String catalogName = properties.getOrDefault(catalogNameKey, "");
            if (catalogName.isEmpty()) {
                throw new IllegalStateException("iceberg polaris catalog catalog_name is empty, must specify catalog_name.");
            }
            if (namespace.isEmpty()) {
                throw new IllegalStateException("iceberg polaris catalog namespace is empty, must specify namespace.");
            }
            String dataSource = catalogName + "." + namespace + "." + table;
            context.setDataSource(dataSource);
        }

        String serverTypeKey = IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.ICEBERG_CATALOG_CONFIG_STRING + "." + IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.SERVER_TYPE;
        String server_type = properties.getOrDefault(serverTypeKey, "");
        if (server_type.isEmpty()) {
            throw new IllegalStateException("iceberg catalog server_type is empty, must specify server_type.");
        }
        context.setCatalogType(server_type);

        // Set Gopher properties
        Map<String, String> gopherProps = getGopherProps(properties);
        context.setGopherProperties(gopherProps);
        // Diagnostic INFO line: gopher params plumbed to catalog (secrets redacted).
        // Helps spot misrouted params like endpoint with scheme leftover, ufs_type=s3
        // (gopher rejects), missing useVirtualHost, etc. that previously caused
        // CURLE_COULDNT_RESOLVE_HOST / "unsupported UFS type: s3".
        if (LOG.isInfoEnabled() && !gopherProps.isEmpty()) {
            LOG.info("gopher props -> catalog: {}", redactSecrets(gopherProps));
            /* Diagnostic trace: see iceberg_volume_option.c step2 for context. */
            if (LOG.isDebugEnabled()) {
                String ak = gopherProps.get("gopher.access_key");
                String sk = gopherProps.get("gopher.secret_key");
                LOG.debug("[trace_ak] step6 gopherProps->catalog: gopher.access_key len={}, gopher.secret_key len={}",
                    ak == null ? -1 : ak.length(),
                    sk == null ? -1 : sk.length());
            }
        }

        // Set context configuration
        Configuration configuration = getConfiguration(properties);
        context.setConfiguration(configuration);

        // Get warehouse location and set it as path.
        //
        // For most catalog types this is the full URL.  For type='hadoop' the
        // path is consumed by IcebergHadoopCatalog as `catalogLocation` in the
        // formula `WAREHOUSE_LOCATION = fs.defaultFS + catalogLocation`, so we
        // store only the path component here (with leading '/').  fs.defaultFS
        // for hadoop catalog is set in setCatalogWarehouseInline.
        String warehouseLocation = getCatalogWarehouseLocationPrefix(properties);
        if (server_type.equals(IcebergConfigConstants.CATALOG_TYPE_HADOOP)
                && warehouseLocation != null && !warehouseLocation.isEmpty()) {
            int schemeEnd = warehouseLocation.indexOf("://");
            if (schemeEnd >= 0) {
                int pathStart = warehouseLocation.indexOf('/', schemeEnd + 3);
                warehouseLocation = (pathStart >= 0)
                    ? warehouseLocation.substring(pathStart)
                    : "";
            }
        }
        context.setPath(warehouseLocation);

        if (server_type.equalsIgnoreCase(IcebergConfigConstants.CATALOG_TYPE_BUILDIN)) {
            Map<String, String> buildInCatalogProps = getBuildInCatalogProps(properties);
            context.setBuildInCatalogProperties(buildInCatalogProps);
        }

        return context;
    }
}
