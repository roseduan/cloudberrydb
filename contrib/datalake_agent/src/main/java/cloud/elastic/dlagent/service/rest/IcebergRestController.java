package cloud.elastic.dlagent.service.rest;

import cloud.elastic.dlagent.api.model.BaseConfigurationFactory;
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

    private static final Map<String, String> PropertiesMapping = new HashMap<>();

    static {
        String volumePrefix = IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.ICEBERG_VOLUME_CONFIG_STRING + ".";
        PropertiesMapping.put(volumePrefix + IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.VOLUME_SERVER_TYPE, "gopher.ufs_type");
        PropertiesMapping.put(volumePrefix + IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.VOLUME_ENDPOINT, "gopher.endpoint");
        PropertiesMapping.put(volumePrefix + IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.VOLUME_REGION, "gopher.region");
        PropertiesMapping.put(volumePrefix + IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.BUCKET_NAME, "gopher.bucket_name");
        PropertiesMapping.put(volumePrefix + IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.PATH_STYLE_ACCESS, "gopher.path_style_access");
        PropertiesMapping.put(volumePrefix + IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.ACCESS_KEY_ID, "gopher.access_key_id");
        PropertiesMapping.put(volumePrefix + IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.SECRET_ACCESS_KEY, "gopher.secret_access_key");
    }


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

        // Get S3 configuration from FileIO properties
        FileIO fileIO = icebergTable.io();
        Map<String, String> ioProps = fileIO.properties();

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
     * Extract ONLY user-facing Iceberg TBLPROPERTIES from the request body, never
     * catalog/volume/gopher runtime configuration.
     *
     * <p>Mirrors the open-source Iceberg separation between
     * {@code Catalog.initialize(catalogProps)} (runtime-only) and
     * {@code Catalog.createTable(..., tableProps)} (persisted into metadata.json).
     * Historical note: the C side piggy-backs {@code buildInCatalog.*} plumbing keys
     * into the same JSON {@code properties} field, so filter those out here.
     */
    private Map<String, String> extractUserTableProperties(Map<String, Object> request) {
        Map<String, String> out = new HashMap<>();
        Object raw = request.get(IcebergConfigConstants.PROPERTIES);
        if (raw instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> userProps = (Map<String, Object>) raw;
            for (Map.Entry<String, Object> entry : userProps.entrySet()) {
                if (entry.getValue() == null) {
                    continue;
                }
                if (IcebergUtilities.isInternalConfigKey(entry.getKey())) {
                    continue;
                }
                out.put(entry.getKey(), entry.getValue().toString());
            }
        }
        return out;
    }

    private Map<String, String> extractProperties(Map<String, Object> request) {
        Map<String, String> properties = new HashMap<>();

        if (request.containsKey(IcebergConfigConstants.ICEBERG_CONFIG)) {
            Map<String, Object> icebergConfig = (Map<String, Object>) request.get(IcebergConfigConstants.ICEBERG_CONFIG);
            extractCatalogConfig(icebergConfig, properties);
            extractVolumeConfig(icebergConfig, properties);
            extractAdditionalConfig(icebergConfig, properties);

            // Extract config_files (e.g. "s3.conf", "gphdfs.conf") for server_name lookup
            if (icebergConfig.containsKey("config_files")) {
                properties.put("config_files", icebergConfig.get("config_files").toString());
            }

            // Extract gopherConfig (gopher system paths passed from C side)
            if (icebergConfig.containsKey("gopherConfig")) {
                Map<String, Object> gopherConfig = (Map<String, Object>) icebergConfig.get("gopherConfig");
                for (Map.Entry<String, Object> entry : gopherConfig.entrySet()) {
                    if (entry.getValue() != null) {
                        String key = entry.getKey();
                        String propKey = key.startsWith("gopher.") ? key : "gopher." + key;
                        properties.put(propKey, entry.getValue().toString());
                    }
                }
            }
        }

        if (request.containsKey(IcebergConfigConstants.PROPERTIES)) {
            Map<String, String> requestProps = (Map<String, String>) request.get(IcebergConfigConstants.PROPERTIES);
            properties.putAll(requestProps);
        }

        if (log.isDebugEnabled()) {
            log.debug("Extracted properties: {}", properties);
        }

        return properties;
    }

    /**
     * Extract catalog configuration
     */
    private void extractCatalogConfig(Map<String, Object> icebergConfig, Map<String, String> properties) {
        if (!icebergConfig.containsKey(IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.ICEBERG_CATALOG_CONFIG_STRING)) {
            return;
        }

        Map<String, Object> catalogConfig = (Map<String, Object>) icebergConfig.get(
            IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.ICEBERG_CATALOG_CONFIG_STRING);

        for (Map.Entry<String, Object> entry : catalogConfig.entrySet()) {
            if (entry.getValue() != null) {
                String configKey = IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.ICEBERG_CATALOG_CONFIG_STRING
                    + "." + entry.getKey();
                properties.put(configKey, entry.getValue().toString());
            }
        }
    }

    /**
     * Extract volume configuration
     */
    private void extractVolumeConfig(Map<String, Object> icebergConfig, Map<String, String> properties) {
        if (!icebergConfig.containsKey(IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.ICEBERG_VOLUME_CONFIG_STRING)) {
            return;
        }

        Map<String, Object> volumeConfig = (Map<String, Object>) icebergConfig.get(
            IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.ICEBERG_VOLUME_CONFIG_STRING);

        for (Map.Entry<String, Object> entry : volumeConfig.entrySet()) {
            if (entry.getValue() != null) {
                String configKey = IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.ICEBERG_VOLUME_CONFIG_STRING
                    + "." + entry.getKey();
                properties.put(configKey, entry.getValue().toString());
            }
        }
    }

    /**
     * Extract additional configuration including FileIO config
     */
    private void extractAdditionalConfig(Map<String, Object> icebergConfig, Map<String, String> properties) {
        if (!icebergConfig.containsKey(IcebergConfigConstants.ICEBERG_ADDITIONAL_CONFIG.ICEBERG_ADDITIONAL_CONFIG_STRING)) {
            return;
        }

        Map<String, Object> additionalConfig = (Map<String, Object>) icebergConfig.get(
            IcebergConfigConstants.ICEBERG_ADDITIONAL_CONFIG.ICEBERG_ADDITIONAL_CONFIG_STRING);

        // Extract non-FileIO properties
        for (Map.Entry<String, Object> entry : additionalConfig.entrySet()) {
            if (entry.getValue() != null && !IcebergConfigConstants.ICEBERG_ADDITIONAL_CONFIG.FILE_IO_CONFIG.equals(entry.getKey())) {
                String configKey = IcebergConfigConstants.ICEBERG_ADDITIONAL_CONFIG.ICEBERG_ADDITIONAL_CONFIG_STRING
                    + "." + entry.getKey();
                properties.put(configKey, entry.getValue().toString());
            }
        }

        // Extract FileIO config
        if (additionalConfig.containsKey(IcebergConfigConstants.ICEBERG_ADDITIONAL_CONFIG.FILE_IO_CONFIG)) {
            Map<String, Object> fileIOConfig = (Map<String, Object>) additionalConfig.get(
                IcebergConfigConstants.ICEBERG_ADDITIONAL_CONFIG.FILE_IO_CONFIG);
            extractFileIOConfig(fileIOConfig, properties);
        }
    }

    /**
     * Extract FileIO configuration based on impl_class
     */
    private void extractFileIOConfig(Map<String, Object> fileIOConfig, Map<String, String> properties) {
        String implClass = (String) fileIOConfig.get(IcebergConfigConstants.FILE_IO_CONFIG.IMPL_CLASS);

        if (implClass == null) {
            return;
        }

        if (IcebergConfigConstants.GOPHER_FILE_IO_CLASS_NAME.equals(implClass)) {
            extractGopherFileIOConfig(fileIOConfig, properties);
        } else {
            extractSimpleFileIOConfig(fileIOConfig, properties, implClass);
        }
    }

    /**
     * Extract SimpleFileIO configuration
     */
    private void extractSimpleFileIOConfig(Map<String, Object> fileIOConfig, Map<String, String> properties, String implClass) {
        String simpleFileIOConfigName = IcebergConfigConstants.FILE_IO_CONFIG.SIMPLE_FILEIO_CONFIG;
        properties.put(IcebergConfigConstants.FILE_IO_CONFIG_IMPL_CLASS, implClass);

        if (!fileIOConfig.containsKey(simpleFileIOConfigName)) {
            return;
        }

        Map<String, Object> simpleConfig = (Map<String, Object>) fileIOConfig.get(simpleFileIOConfigName);

        // Extract additional properties
        if (simpleConfig.containsKey(IcebergConfigConstants.PROPERTIES)) {
            Map<String, Object> simpleProps = (Map<String, Object>) simpleConfig.get(IcebergConfigConstants.PROPERTIES);
            for (Map.Entry<String, Object> entry : simpleProps.entrySet()) {
                if (entry.getValue() != null) {
                    String configKey = IcebergConfigConstants.FILE_IO_CONFIG_PROPERTIES_PREFIX + "." + entry.getKey();
                    properties.put(configKey, entry.getValue().toString());
                }
            }
        }
    }

    /**
     * Extract GopherFileIO configuration
     */
    private void extractGopherFileIOConfig(Map<String, Object> fileIOConfig, Map<String, String> properties) {
        String gopherFileIOConfigName = IcebergConfigConstants.FILE_IO_CONFIG.GOPHER_FILEIO_CONFIG;
        if (!fileIOConfig.containsKey(gopherFileIOConfigName)) {
            return;
        }

        properties.put(IcebergConfigConstants.FILE_IO_CONFIG_IMPL_CLASS, IcebergConfigConstants.GOPHER_FILE_IO);

        Map<String, Object> gopherFileIOConfig = (Map<String, Object>) fileIOConfig.get(gopherFileIOConfigName);

        // Extract gopherConfig
        if (gopherFileIOConfig.containsKey(IcebergConfigConstants.GOPHER_CONFIG.GOPHER_CONFIG_STRING)) {
            Map<String, Object> gopherConfig = (Map<String, Object>) gopherFileIOConfig.get(
                IcebergConfigConstants.GOPHER_CONFIG.GOPHER_CONFIG_STRING);
            extractGopherCommonConfig(gopherConfig, properties);
        }

        // Extract additional properties
        if (gopherFileIOConfig.containsKey(IcebergConfigConstants.PROPERTIES)) {
            Map<String, Object> gopherProps = (Map<String, Object>) gopherFileIOConfig.get(IcebergConfigConstants.PROPERTIES);
            for (Map.Entry<String, Object> entry : gopherProps.entrySet()) {
                if (entry.getValue() != null) {
                    String configKey = IcebergConfigConstants.FILE_IO_CONFIG_PROPERTIES_PREFIX + "." + entry.getKey();
                    properties.put(configKey, entry.getValue().toString());
                }
            }
        }
    }

    /**
     * Extract Gopher common configuration
     */
    private void extractGopherCommonConfig(Map<String, Object> gopherConfig, Map<String, String> properties) {
        if (!gopherConfig.containsKey(IcebergConfigConstants.COMMON)) {
            return;
        }

        Map<String, Object> common = (Map<String, Object>) gopherConfig.get(IcebergConfigConstants.COMMON);
        for (Map.Entry<String, Object> entry : common.entrySet()) {
            if (entry.getValue() != null) {
                String configKey = IcebergConfigConstants.GOPHER_COMMON_CONFIG_PREFIX + "." + entry.getKey();
                properties.put(configKey, entry.getValue().toString());
            }
        }
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

    private Map<String, String> getGopherProps(Map<String, String> properties) {
        // set gopher common propertis
        Map<String, String> gopherProps = new HashMap<>();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith(IcebergConfigConstants.GOPHER_CONFIG.GOPHER_HEADER + ".")) {
                gopherProps.put(entry.getKey(), entry.getValue());
            }
        }
        // set gopher connect propertis
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            String mappingKey = PropertiesMapping.get(key);
            if (mappingKey != null) {
                gopherProps.put(mappingKey, value);
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

    private void processVolumeS3FileIO(Configuration configuration, Map<String, String> properties) {
        String implClass = properties.getOrDefault(IcebergConfigConstants.FILE_IO_CONFIG_IMPL_CLASS, IcebergConfigConstants.HADOOP_FILE_IO_CLASS_NAME);
        configuration.set(IcebergConfigConstants.FILE_IO_CONFIG_IMPL_CLASS, implClass);

        // Get S3 credentials from volume config
        String accessKeyConfigKey = IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.ICEBERG_VOLUME_CONFIG_STRING + "." + IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.ACCESS_KEY_ID;
        String secretKeyConfigKey = IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.ICEBERG_VOLUME_CONFIG_STRING + "." + IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.SECRET_ACCESS_KEY;
        String endpointConfigKey = IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.ICEBERG_VOLUME_CONFIG_STRING + "." + IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.VOLUME_ENDPOINT;
        String pathStyleConfigKey = IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.ICEBERG_VOLUME_CONFIG_STRING + "." + IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.PATH_STYLE_ACCESS;

        String accessKey = properties.get(accessKeyConfigKey);
        String secretKey = properties.get(secretKeyConfigKey);
        String endpoint = properties.get(endpointConfigKey);
        Boolean pathStyleAccess = Boolean.parseBoolean(properties.getOrDefault(pathStyleConfigKey, "true"));

        // Determine FileIO type by impl_class
        if (IcebergConfigConstants.S3_FILE_IO_CLASS_NAME.equals(implClass) ||
            IcebergConfigConstants.ICEBERG_S3_FILE_IO_CLASS_NAME.equals(implClass)) {
            // Configure for S3FileIO
            String regionConfigKey = IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.ICEBERG_VOLUME_CONFIG_STRING + "." + IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.VOLUME_REGION;
            String region = properties.getOrDefault(regionConfigKey, IcebergConfigConstants.DEFAULT_S3_REGION_VALUE);

            if (accessKey != null) configuration.set(IcebergConfigConstants.S3FILEIO_ACCESS_KEY_ID, accessKey);
            if (secretKey != null) configuration.set(IcebergConfigConstants.S3FILEIO_SECRET_ACCESS_KEY, secretKey);
            if (endpoint != null) configuration.set(IcebergConfigConstants.S3FILEIO_ENDPOINT, endpoint);
            if (region != null) configuration.set(IcebergConfigConstants.S3FILEIO_REGION, region);
            configuration.set(IcebergConfigConstants.S3FILEIO_PATH_STYLE_ACCESS, pathStyleAccess.toString());
        } else {
            // Configure for HadoopFileIO with S3A (default)
            configuration.set(IcebergConfigConstants.FS_S3A_IMPL, IcebergConfigConstants.S3A_FILESYSTEM_IMPL);
            configuration.set(IcebergConfigConstants.FS_S3A_AWS_CREDENTIALS_PROVIDER, IcebergConfigConstants.S3A_CREDENTIALS_PROVIDER);

            if (accessKey != null) configuration.set(IcebergConfigConstants.FS_S3A_ACCESS_KEY, accessKey);
            if (secretKey != null) configuration.set(IcebergConfigConstants.FS_S3A_SECRET_KEY, secretKey);
            if (endpoint != null) configuration.set(IcebergConfigConstants.FS_S3A_ENDPOINT, endpoint);
            configuration.set(IcebergConfigConstants.FS_S3A_PATH_STYLE_ACCESS, pathStyleAccess.toString());
        }

        // Set warehouse location from volume config
        String bucketConfigKey = IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.ICEBERG_VOLUME_CONFIG_STRING + "." + IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.BUCKET_NAME;
        String basePathConfigKey = IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.ICEBERG_VOLUME_CONFIG_STRING + "." + IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.BASE_PATH;
        String bucketName = properties.get(bucketConfigKey);
        String basePath = properties.getOrDefault(basePathConfigKey, "/hive");
        if (bucketName != null) {
            String warehouseLocation = String.format("s3a://%s%s", bucketName, basePath);
            configuration.set("hive.metastore.warehouse.dir", warehouseLocation);
        }

        // Pass through all FileIOConfig.properties to configuration
        String prefix = IcebergConfigConstants.FILE_IO_CONFIG_PROPERTIES_PREFIX + ".";
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith(prefix)) {
                String configKey = entry.getKey().substring(prefix.length());
                configuration.set(configKey, entry.getValue());
            }
        }
    }

    private void processVolumeS3Resource(Configuration configuration, Map<String, String> properties) {
        String file_io_config = properties.getOrDefault(IcebergConfigConstants.FILE_IO_CONFIG_IMPL_CLASS, "");
        if (file_io_config.equals(IcebergConfigConstants.GOPHER_FILE_IO)) {
            //TODO: support GopherFileIO to read catalog io
        } else {
            processVolumeS3FileIO(configuration, properties);
        }
    }

    private void processHiveVolumeS3Resource(Configuration configuration, Map<String, String> properties) {
        String catalogConfigKey = IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.ICEBERG_VOLUME_CONFIG_STRING + "." + IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.CATALOG_FILE_IO_IMPL;
        String catalog_file_io_impl = properties.getOrDefault(catalogConfigKey, "");
        if (catalog_file_io_impl.equals(IcebergConfigConstants.GOPHER_FILE_IO)) {
            //TODO(liuxiaoyu): support GopherFileIO to read catalog io
        } else {
            // Configure S3A filesystem for Hive
            configuration.set(IcebergConfigConstants.FS_S3A_IMPL, IcebergConfigConstants.S3A_FILESYSTEM_IMPL);
            configuration.set(IcebergConfigConstants.FS_S3A_AWS_CREDENTIALS_PROVIDER, IcebergConfigConstants.S3A_CREDENTIALS_PROVIDER);

            // Set S3 credentials from volume config
            String accessKeyConfigKey = IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.ICEBERG_VOLUME_CONFIG_STRING + "." + IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.ACCESS_KEY_ID;
            String secretKeyConfigKey = IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.ICEBERG_VOLUME_CONFIG_STRING + "." + IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.SECRET_ACCESS_KEY;
            String endpointConfigKey = IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.ICEBERG_VOLUME_CONFIG_STRING + "." + IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.VOLUME_ENDPOINT;
            String regionConfigKey = IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.ICEBERG_VOLUME_CONFIG_STRING + "." + IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.VOLUME_REGION;

            String accessKey = properties.get(accessKeyConfigKey);
            String secretKey = properties.get(secretKeyConfigKey);
            String endpoint = properties.get(endpointConfigKey);
            String region = properties.getOrDefault(regionConfigKey, "us-east-1");

            // Configure for both S3A (HadoopFileIO) and S3 (S3FileIO)
            if (accessKey != null) {
                configuration.set(IcebergConfigConstants.FS_S3A_ACCESS_KEY, accessKey);
                configuration.set("iceberg.s3.access-key-id", accessKey);
            }
            if (secretKey != null) {
                configuration.set(IcebergConfigConstants.FS_S3A_SECRET_KEY, secretKey);
                configuration.set("iceberg.s3.secret-access-key", secretKey);
            }
            if (endpoint != null) {
                configuration.set(IcebergConfigConstants.FS_S3A_ENDPOINT, endpoint);
                configuration.set("iceberg.s3.endpoint", endpoint);
            }
            if (region != null) {
                configuration.set("fs.s3a.endpoint.region", region);
                configuration.set("iceberg.s3.region", region);
            }

            String pathStyleConfigKey = IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.ICEBERG_VOLUME_CONFIG_STRING + "." + IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.PATH_STYLE_ACCESS;
            Boolean pathStyleAccess = Boolean.parseBoolean(properties.getOrDefault(pathStyleConfigKey, "false"));
            configuration.set(IcebergConfigConstants.FS_S3A_PATH_STYLE_ACCESS, pathStyleAccess.toString());
            configuration.set("iceberg.s3.path-style-access", pathStyleAccess.toString());

            // Set warehouse location from volume config
            //TODO(liuxiaoyu): need set warehouse dir
            String bucketConfigKey = IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.ICEBERG_VOLUME_CONFIG_STRING + "." + IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.BUCKET_NAME;
            String basePathConfigKey = IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.ICEBERG_VOLUME_CONFIG_STRING + "." + IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.BASE_PATH;
            String bucketName = properties.get(bucketConfigKey);
            String basePath = properties.getOrDefault(basePathConfigKey, "/hive");
            if (bucketName != null) {
                String warehouseLocation = String.format("s3a://%s%s", bucketName, basePath);
                configuration.set("hive.metastore.warehouse.dir", warehouseLocation);
            }

            // Set FileIO implementation based on catalog_file_io_impl
            String catalogFileIoKey = IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.ICEBERG_CATALOG_CONFIG_STRING + ".catalog_file_io_impl";
            String fileIoImpl = properties.getOrDefault(catalogFileIoKey, "");
            if ("GopherFileIO".equals(fileIoImpl)) {
                configuration.set("iceberg.catalog.file-io-impl", "cloud.elastic.dlagent.plugins.iceberg.GopherFileIO");
            }
        }
    }

    private String getCatalogWarehouseLocationPrefix(Map<String, String> properties) {
        String warehouseLocationPrefix = IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.ICEBERG_CATALOG_CONFIG_STRING + "." + IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.WAREHOUSE_LOCATION_PERFIX;
        String basePath = properties.getOrDefault(warehouseLocationPrefix, "");
        return basePath;
    }

    private void processHiveVolumeServerResource(Configuration configuration, Map<String, String> properties) {
        String volumeServerTypeKey = IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.ICEBERG_VOLUME_CONFIG_STRING + "." + IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.VOLUME_SERVER_TYPE;
        String volume_server_type = properties.getOrDefault(volumeServerTypeKey, "");
        if (volume_server_type.isEmpty()) {
            String key = IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.ICEBERG_VOLUME_CONFIG_STRING + "." + IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.VOLUME_SERVER_TYPE;
            throw new IllegalArgumentException(key + " is empty, must specify it");
        }

        if (volume_server_type.equals(IcebergConfigConstants.VOLUME_TYPE_S3A) ||
            volume_server_type.equals(IcebergConfigConstants.VOLUME_TYPE_S3) ||
            volume_server_type.equals(IcebergConfigConstants.VOLUME_TYPE_ABFSS)) {
            processVolumeS3Resource(configuration, properties);
        } else if (volume_server_type.equals(IcebergConfigConstants.VOLUME_TYPE_HDFS)) {
            processVolumeHdfsResource(configuration, properties);
        } else {
            String key = IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.ICEBERG_VOLUME_CONFIG_STRING + "." + IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.VOLUME_SERVER_TYPE;
            throw new UnsupportedOperationException(key + " '" + volume_server_type + "' is not supported yet. " +
                    "Supported types are: [" + IcebergConfigConstants.VOLUME_TYPE_S3A + ", " +
                    IcebergConfigConstants.VOLUME_TYPE_S3 + ", " +
                    IcebergConfigConstants.VOLUME_TYPE_HDFS + ", " +
                    IcebergConfigConstants.VOLUME_TYPE_ABFSS + "]");
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
    private void processVolumeHdfsResource(Configuration configuration, Map<String, String> properties) {
        String endpointKey = IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.ICEBERG_VOLUME_CONFIG_STRING + "."
            + IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.VOLUME_ENDPOINT;
        String endpoint = properties.getOrDefault(endpointKey, "");

        if (!endpoint.isEmpty()) {
            // Accept "hdfs://host:port" or just "host:port".
            String defaultFS = endpoint.startsWith("hdfs://") ? endpoint : "hdfs://" + endpoint;
            // fs.defaultFS already set by processS3OrHadoopServerResource for hadoop
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

    private void processHiveServerResource(Configuration configuration, Map<String, String> properties) {
        String hiveMetastoreUriKey = IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.ICEBERG_CATALOG_CONFIG_STRING + "." + IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.HIVE_METASTORE_URI;
        String hive_metastore_uris = properties.getOrDefault(hiveMetastoreUriKey, "");
        if (!hive_metastore_uris.isEmpty()) {
            configuration.set("hive.metastore.uris", hive_metastore_uris);
        } else {
            String key = IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.ICEBERG_CATALOG_CONFIG_STRING + "." + IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.HIVE_METASTORE_URI;
            throw new IllegalArgumentException(key + " is empty, must specify it");
        }

        String authMethodKey = IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.ICEBERG_CATALOG_CONFIG_STRING + "." + IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.AUTH_METHOD;
        String auth_method = properties.getOrDefault(authMethodKey, "");
        if (!auth_method.isEmpty()) {
            configuration.set("hadoop.security.authentication", auth_method);
        }
        processHiveVolumeServerResource(configuration, properties);
    }

    private void processBuildInResource(Configuration configuration, Map<String, String> properties) {
        processHiveVolumeServerResource(configuration, properties);
    }

    private void processPolarisServerResource(Configuration configuration, Map<String, String> properties) {
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
        // gopherProperties (translated from SQL options by PropertiesMapping);
        // Polaris always uses GopherFileIO, so no S3A / S3FileIO config here.
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
    private void processS3OrHadoopServerResource(Configuration configuration,
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

        // Both s3 and hadoop catalogs delegate to processHiveVolumeServerResource
        // for the volume-side credentials (s3a access keys, HDFS auth, etc.).
        processHiveVolumeServerResource(configuration, properties);
    }

    private void processServerResource(Configuration configuration, Map<String, String> properties) {
        String catalogServerTypeKey = IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.ICEBERG_CATALOG_CONFIG_STRING + "." + IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.SERVER_TYPE;
        String catalog_server_type = properties.getOrDefault(catalogServerTypeKey, "");

        if (catalog_server_type.equals(IcebergConfigConstants.CATALOG_TYPE_S3A) ||
            catalog_server_type.equals(IcebergConfigConstants.CATALOG_TYPE_S3)) {
            processS3OrHadoopServerResource(configuration, properties, false);
        } else if (catalog_server_type.equals(IcebergConfigConstants.CATALOG_TYPE_HADOOP)) {
            processS3OrHadoopServerResource(configuration, properties, true);
        } else if (catalog_server_type.equals(IcebergConfigConstants.CATALOG_TYPE_HIVE)) {
            processHiveServerResource(configuration, properties);
        } else if (catalog_server_type.equals(IcebergConfigConstants.CATALOG_TYPE_BUILDIN)) {
            processBuildInResource(configuration, properties);
        } else if (catalog_server_type.equals(IcebergConfigConstants.CATALOG_TYPE_POLARIS)) {
            processPolarisServerResource(configuration, properties);
        } else {
            throw new UnsupportedOperationException("This server type '" + catalog_server_type + "' is not supported yet. " +
                    "Supported types are: [" + IcebergConfigConstants.CATALOG_TYPE_S3A + ", " +
                    IcebergConfigConstants.CATALOG_TYPE_S3 + ", " +
                    IcebergConfigConstants.CATALOG_TYPE_HADOOP + ", " +
                    IcebergConfigConstants.CATALOG_TYPE_HIVE + ", " + IcebergConfigConstants.CATALOG_TYPE_BUILDIN + ", " +
                    IcebergConfigConstants.CATALOG_TYPE_POLARIS + "]");
        }
    }

    private Configuration getConfiguration(Map<String, String> properties) {
        LOG.debug("Initializing configuration for server");
        Configuration configuration = new Configuration(false);

        // Inject gopher system paths from the request so that
        // setupGopherConfiguration() can find gopher even without config files.
        // Note: gopher.enabled is NOT injected here — it is controlled by
        // application.properties or the volume's fileIOConfig.
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith("gopher.") && !key.equals("gopher.enabled")) {
                configuration.set(key, entry.getValue());
            }
        }

        processServerResource(configuration, properties);

        // If server_name is specified, load config from $PGDATA config files
        // (s3.conf, gphdfs.conf, gphive.conf). Config file values override
        // the inline SQL parameters already in the Configuration.
        String configFiles = properties.get("config_files");
        String volumeServerName = properties.get(
            IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.ICEBERG_VOLUME_CONFIG_STRING + ".server_name");
        String catalogServerName = properties.get(
            IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.ICEBERG_CATALOG_CONFIG_STRING + ".server_name");
        if (configFiles != null) {
            String serverName = volumeServerName != null ? volumeServerName : catalogServerName;
            if (serverName != null) {
                String catalogType = properties.getOrDefault(
                    IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.ICEBERG_CATALOG_CONFIG_STRING + "."
                    + IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.SERVER_TYPE, "");
                String[] files = configFiles.split("0");
                for (String file : files) {
                    try {
                        configurationFactory.processServerResource(catalogType, file.trim(),
                            serverName, configuration, "");
                        LOG.debug("Loaded config from file '{}' for server '{}'", file, serverName);
                    } catch (Exception e) {
                        LOG.warn("Failed to load config from '{}' for server '{}': {}",
                            file, serverName, e.getMessage());
                    }
                }
            }
        }

        return configuration;
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
                || catalogServerType.equals(IcebergConfigConstants.CATALOG_TYPE_S3A)
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

        // Set context configuration
        Configuration configuration = getConfiguration(properties);
        context.setConfiguration(configuration);

        // Get warehouse location and set it as path.
        //
        // For most catalog types this is the full URL.  For type='hadoop' the
        // path is consumed by IcebergHadoopCatalog as `catalogLocation` in the
        // formula `WAREHOUSE_LOCATION = fs.defaultFS + catalogLocation`, so we
        // store only the path component here (with leading '/').  fs.defaultFS
        // for hadoop catalog is set in processS3OrHadoopServerResource.
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
