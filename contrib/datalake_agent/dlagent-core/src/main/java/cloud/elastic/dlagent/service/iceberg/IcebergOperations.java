/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.service.iceberg;

import cloud.elastic.dlagent.api.configuration.IcebergRestProperties;
import cloud.elastic.dlagent.api.model.CleanupFromMetadataRequest;
import cloud.elastic.dlagent.api.model.CleanupFromMetadataResponse;
import cloud.elastic.dlagent.api.model.Fragment;
import cloud.elastic.dlagent.api.model.RequestContext;
import cloud.elastic.dlagent.api.model.RequestedColumn;
import cloud.elastic.dlagent.api.utilities.FragmentMetadata;
import cloud.elastic.dlagent.api.utilities.GpdbFragmentMetadata;
import cloud.elastic.dlagent.constants.IcebergConfigConstants;
import cloud.elastic.dlagent.plugins.iceberg.S3TablesMaintenanceClient;
import cloud.elastic.dlagent.plugins.iceberg.utilities.IcebergUtilities;
import cloud.elastic.dlagent.service.ServiceResult;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Every Iceberg operation the agent offers, with no transport in sight.
 *
 * <p>THE SEAM. Both transports call these 24 operations instead of each
 * carrying its own copy of the business logic, so both are thin: the REST
 * controller maps HTTP to a call and a {@link ServiceResult} back to a
 * ResponseEntity, and the gRPC service does the same with a JSON payload and a
 * status code.
 *
 * <p>Each operation behaves exactly as the REST endpoint it serves always has
 * (REST is the rollback path): every {@code ResponseEntity} the endpoint
 * returned corresponds to a {@link ServiceResult} factory here.
 *
 * <p>Two things travel on a ServiceResult that look like transport but are
 * not: the HTTP-shaped status code, which {@code ErrorMapper} turns into a
 * gRPC status for the other transport, and the {@code ETag} header five
 * operations set. The ETag is dead weight for the real client -- agent_cli's
 * Response struct carries no headers at all -- but REST may not stop emitting
 * it.
 *
 * <p>See the note in {@code MetadataFileCleaner} on why both {@code log} and
 * {@code LOG} exist here.
 */
@Named
@Singleton
@Slf4j
public class IcebergOperations {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergOperations.class);

    private final IcebergService icebergService;
    private final IcebergRequestContextFactory requestContextFactory;
    private final IcebergMetadataMapper metadataMapper;
    private final MetadataFileCleaner metadataFileCleaner;
    private final IcebergRestProperties icebergRestConfig;

    @Inject
    public IcebergOperations(IcebergService icebergService,
                             IcebergRequestContextFactory requestContextFactory,
                             IcebergMetadataMapper metadataMapper,
                             MetadataFileCleaner metadataFileCleaner,
                             IcebergRestProperties icebergRestConfig) {
        this.icebergService = icebergService;
        this.requestContextFactory = requestContextFactory;
        this.metadataMapper = metadataMapper;
        this.metadataFileCleaner = metadataFileCleaner;
        this.icebergRestConfig = icebergRestConfig;
    }

    public ServiceResult<Object> tableExists(
            String prefix,
            String table,
            Map<String, Object> request) throws Exception {

        // Extract namespace from request body
        String namespace = (String) request.get("namespace");
        if (namespace == null || namespace.isEmpty()) {
            Map<String, Object> errorResponse = new HashMap<>();
            Map<String, Object> error = new HashMap<>();
            error.put("message", "Namespace is required");
            error.put("type", "BadRequestException");
            error.put("code", 400);
            errorResponse.put("error", error);
            return ServiceResult.failureBody(errorResponse, 400);
        }

        log.info("Checking if table exists: {}.{}", namespace, table);

        // Extract configurations from request
        Map<String, String> properties = requestContextFactory.extractProperties(request);

        // Create request context
        RequestContext context = requestContextFactory.createRequestContext(namespace, table, properties);
        context.setTxnId(extractTxnId(request));

        // Check if the table exists and get detailed result
        icebergService.checkTableExists(namespace, table, properties, context);
        return ServiceResult.success(null);
    }

    public ServiceResult<Object> loadTable(
            String prefix,
            String table,
            Map<String, Object> request,
            String ifNoneMatch) throws Exception {

        // Extract namespace from request body
        String namespace = (String) request.get("namespace");
        if (namespace == null || namespace.isEmpty()) {
            Map<String, Object> errorResponse = new HashMap<>();
            Map<String, Object> error = new HashMap<>();
            error.put("message", "Namespace is required");
            error.put("type", "BadRequestException");
            error.put("code", 400);
            errorResponse.put("error", error);
            return ServiceResult.failureBody(errorResponse, 400);
        }

        log.info("Loading table: {}.{}", namespace, table);

        /*
         * issue #393 external-catalog metadata cache.  The caller (PG) passes
         * the head it last resolved (metadata_location), when it did so
         * (metadata_timestamp, epoch ms) and the per-table TTL (metadata_cache,
         * minutes; 0 = no cache).  While that head is younger than the TTL,
         * reuse it verbatim and skip the external catalog entirely -- this is
         * the "agent decides" reuse path.  Only the head + timestamp are echoed
         * because the head-resolution caller consumes only those.
         *
         * Ported here from IcebergRestController when loadTable's body moved
         * into core.  Left on the REST side it would have meant the same SQL
         * answering from cache under one transport and going to the catalog
         * under the other.
         */
        int metadataCacheTtlMin = asInt(request.get("metadata_cache"), 0);
        long callerMetadataTs = asLong(request.get("metadata_timestamp"), 0L);
        String cachedMetadataLocation = asString(request.get("metadata_location"), null);
        if (metadataCacheTtlMin > 0 && callerMetadataTs > 0
                && cachedMetadataLocation != null && !cachedMetadataLocation.isEmpty()
                && (System.currentTimeMillis() - callerMetadataTs)
                        < (long) metadataCacheTtlMin * 60_000L) {
            log.info("Reusing cached metadata head for {}.{} (age {}ms < ttl {}min)",
                    namespace, table,
                    System.currentTimeMillis() - callerMetadataTs, metadataCacheTtlMin);
            Map<String, Object> reuse = new HashMap<>();
            reuse.put("metadata-location", cachedMetadataLocation);
            reuse.put("metadata-timestamp", callerMetadataTs);
            return ServiceResult.success(reuse);
        }

        // Extract configurations from request
        Map<String, String> properties = requestContextFactory.extractProperties(request);

        // Create request context
        RequestContext context = requestContextFactory.createRequestContext(namespace, table, properties);
        context.setTxnId(extractTxnId(request));

        // A mount (OPTIONS (table '...')) sends the local definition it is
        // about to bind as "schema"; the service refuses the load if it does
        // not describe the table (cloud#610).  Plain loads send none.
        Object requestedSchema = request.get("schema");
        if (requestedSchema != null) {
            if (!(requestedSchema instanceof Map)) {
                throw new IllegalArgumentException("\"schema\" must be an object");
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> schemaObject = (Map<String, Object>) requestedSchema;
            context.setRequestedColumns(RequestedColumn.fromSchemaMap(schemaObject, null));
        }

        // Load the table using ServiceResult
        Table icebergTable = icebergService.loadTable(namespace, table, properties, context);

        // Create response following the Iceberg REST API spec
        Map<String, Object> metadataMap = metadataMapper.convertTableToMetadata(icebergTable);
        Map<String, Object> response = new HashMap<>();

        TableMetadata metadata = ((BaseTable) icebergTable).operations().current();
        String metadataLocation = metadata.metadataFileLocation();
        response.put("metadata", metadataMap);
        response.put("metadata-location", metadataLocation);
        response.put("table-location", icebergTable.location());
        response.put("partition-spec-summary", metadataMapper.partitionSpecSummary(icebergTable));
        // issue #393: stamp the refresh time so the caller can cache this head
        // for its TTL window (the agent's clock is the single source of truth).
        response.put("metadata-timestamp", System.currentTimeMillis());

        boolean restCatalog = IcebergConfigConstants.CATALOG_TYPE_REST.equals(context.getCatalogType());
        if (restCatalog) {
            putRestEnvelope(response, context.getCatalogProperties());
        }

        // Add config to response - static S3 configuration (non-sensitive)
        Map<String, String> config = new HashMap<>();

        // Get S3 configuration from FileIO properties.
        // GopherFileIO does not implement FileIO.properties() (throws
        // UnsupportedOperationException); treat as empty in that case — the
        // S3 config/credential fields below are optional response payload.
        //
        // Not for a generic REST catalog: what it vends -- config and
        // short-lived credentials -- is in the envelope written above, and
        // reading the FileIO here would overwrite it with values derived from
        // the table instead.
        Map<String, String> ioProps = java.util.Collections.emptyMap();
        if (!restCatalog) {
            FileIO fileIO = icebergTable.io();
            try {
                ioProps = fileIO.properties();
            } catch (UnsupportedOperationException e) {
                ioProps = java.util.Collections.emptyMap();
            }
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
        IcebergConfigurationBuilder.addResolvedVolumeConfig(config, properties);
        if (!restCatalog) {
            response.put("config", config);
        }

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

        IcebergConfigurationBuilder.addResolvedVolumeCredentials(credConfig, properties);

        if (!restCatalog && !credConfig.isEmpty()) {
            List<Map<String, Object>> storageCredentials = new ArrayList<>();
            Map<String, Object> credential = new HashMap<>();
            credential.put("prefix", icebergTable.location());
            credential.put("config", credConfig);
            storageCredentials.add(credential);
            response.put("storage-credentials", storageCredentials);
        }

        // Generate ETag header with debug information
        String etag = metadataMapper.generateETag(icebergTable, "load");

        // Check if the ETag matches the If-None-Match header
        if (ifNoneMatch != null && ifNoneMatch.equals(etag)) {
            return ServiceResult.successWithStatus(null, 304);
        }

        return ServiceResult.success((Object) response).withHeader("ETag", etag);
    }

    public ServiceResult<Object> loadMetadataJson(
            String prefix,
            String table,
            Map<String, Object> request) throws Exception {

        String namespace = (String) request.get("namespace");
        if (namespace == null || namespace.isEmpty()) {
            Map<String, Object> error = new HashMap<>();
            error.put("message", "Namespace is required");
            error.put("type", "BadRequestException");
            error.put("code", 400);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", error);
            return ServiceResult.failureBody(errorResponse, 400);
        }

        log.info("Loading metadata document: {}.{}", namespace, table);

        Map<String, String> properties = requestContextFactory.extractProperties(request);
        RequestContext context = requestContextFactory.createRequestContext(namespace, table, properties);
        context.setTxnId(extractTxnId(request));

        // Exactly one object storage read -- see IcebergServiceImpl.loadMetadataJson for why
        // this is not loadTable() followed by a read of the same object.
        byte[] doc = icebergService.loadMetadataJson(namespace, table, properties, context);

        // No X-Iceberg-Metadata-Location header: the only client is the kernel's agent_cli,
        // whose Response struct (agent_client.hpp) carries no headers at all, so nothing could
        // ever read it. The caller already knows the location -- it supplied it.
        return ServiceResult.success((Object) doc).withHeader("Content-Type", "application/json");
    }

    public ServiceResult<Object> getTableFragment(
            String prefix,
            String table,
            Map<String, Object> request,
            String ifNoneMatch) throws Exception {

        // Extract namespace from request body
        String namespace = (String) request.get("namespace");
        if (namespace == null || namespace.isEmpty()) {
            Map<String, Object> errorResponse = new HashMap<>();
            Map<String, Object> error = new HashMap<>();
            error.put("message", "Namespace is required");
            error.put("type", "BadRequestException");
            error.put("code", 400);
            errorResponse.put("error", error);
            return ServiceResult.failureBody(errorResponse, 400);
        }

        log.info("Getting table metadata fragment: {}.{}", namespace, table);

        // Extract configurations from request
        Map<String, String> properties = requestContextFactory.extractProperties(request);

        // convert to RequestContext and reuse the original iceberg logic
        RequestContext context = requestContextFactory.createRequestContext(namespace, table, properties);
        context.setTxnId(extractTxnId(request));

        // Predicate pushdown (AM path): the serialized filter references columns
        // by 0-based index, so the filter machinery needs a ColumnDescriptor list
        // in attno order.  Build it from the request's filterColumns array.
        if (context.hasFilter()) {
            requestContextFactory.populateFilterTupleDescription(request, context);
        }

        // Get table metadata fragment using ServiceResult
        String fragment = icebergService.getTableFragment(namespace, table, properties, context);

        // Generate ETag header with debug information for fragment
        String etag = "fragment-" + System.currentTimeMillis() + "-" + Math.abs(fragment.hashCode());
        log.debug("Generated ETag for getFragment: {}", etag);

        // Check if the ETag matches the If-None-Match header
        if (ifNoneMatch != null && ifNoneMatch.equals(etag)) {
            return ServiceResult.successWithStatus(null, 304);
        }

        return ServiceResult.success((Object) fragment).withHeader("ETag", etag);
    }

    public ServiceResult<Object> getSnapshotSchema(
            String prefix,
            String table,
            Map<String, Object> request,
            String ifNoneMatch) throws Exception {

        // Extract namespace from request body
        String namespace = (String) request.get("namespace");
        if (namespace == null || namespace.isEmpty()) {
            Map<String, Object> errorResponse = new HashMap<>();
            Map<String, Object> error = new HashMap<>();
            error.put("message", "Namespace is required");
            error.put("type", "BadRequestException");
            error.put("code", 400);
            errorResponse.put("error", error);
            return ServiceResult.failureBody(errorResponse, 400);
        }

        log.info("Getting snapshot schema: {}.{}", namespace, table);

        // Extract configurations from request
        Map<String, String> properties = requestContextFactory.extractProperties(request);

        // convert to RequestContext and reuse the original iceberg logic
        RequestContext context = requestContextFactory.createRequestContext(namespace, table, properties);
        context.setTxnId(extractTxnId(request));

        String schema = icebergService.getSnapshotSchema(namespace, table, properties, context);

        // Content-based ETag: no timestamp, so a re-request with an unchanged
        // snapshot schema hits the If-None-Match / 304 path instead of always
        // producing a fresh tag (a millisecond timestamp made 304 dead code).
        String etag = "snapshotSchema-" + Math.abs(schema.hashCode());
        if (ifNoneMatch != null && ifNoneMatch.equals(etag)) {
            return ServiceResult.successWithStatus(null, 304);
        }

        return ServiceResult.success((Object) schema).withHeader("ETag", etag);
    }

    public ServiceResult<Object> getSnapshots(
            String prefix,
            String table,
            Map<String, Object> request,
            String ifNoneMatch) throws Exception {

        // Extract namespace from request body
        String namespace = (String) request.get("namespace");
        if (namespace == null || namespace.isEmpty()) {
            Map<String, Object> errorResponse = new HashMap<>();
            Map<String, Object> error = new HashMap<>();
            error.put("message", "Namespace is required");
            error.put("type", "BadRequestException");
            error.put("code", 400);
            errorResponse.put("error", error);
            return ServiceResult.failureBody(errorResponse, 400);
        }

        log.info("Getting snapshots: {}.{}", namespace, table);

        // Extract configurations from request
        Map<String, String> properties = requestContextFactory.extractProperties(request);

        // convert to RequestContext and reuse the original iceberg logic
        RequestContext context = requestContextFactory.createRequestContext(namespace, table, properties);
        context.setTxnId(extractTxnId(request));

        String snapshots = icebergService.getSnapshots(namespace, table, properties, context);

        // Content-based ETag, like getSnapshotSchema: the snapshot list of a
        // pinned metadata.json is immutable, so an unchanged body hits 304.
        String etag = "snapshots-" + Math.abs(snapshots.hashCode());
        if (ifNoneMatch != null && ifNoneMatch.equals(etag)) {
            return ServiceResult.successWithStatus(null, 304);
        }

        return ServiceResult.success((Object) snapshots).withHeader("ETag", etag);
    }

    public ServiceResult<Object> getTableStatistics(
            String prefix,
            String table,
            Map<String, Object> request,
            String ifNoneMatch) throws Exception {

        // Extract namespace from request body
        String namespace = (String) request.get("namespace");
        if (namespace == null || namespace.isEmpty()) {
            Map<String, Object> errorResponse = new HashMap<>();
            Map<String, Object> error = new HashMap<>();
            error.put("message", "Namespace is required");
            error.put("type", "BadRequestException");
            error.put("code", 400);
            errorResponse.put("error", error);
            return ServiceResult.failureBody(errorResponse, 400);
        }

        log.info("Getting table statistics: {}.{}", namespace, table);

        // Extract configurations from request
        Map<String, String> properties = requestContextFactory.extractProperties(request);

        // convert to RequestContext and reuse the original iceberg logic
        RequestContext context = requestContextFactory.createRequestContext(namespace, table, properties);
        context.setTxnId(extractTxnId(request));

        // Get table statistics
        String statistics = icebergService.getTableStatistics(namespace, table, properties, context);

        // Generate ETag header
        String etag = "statistics-" + System.currentTimeMillis() + "-" + Math.abs(statistics.hashCode());
        log.debug("Generated ETag for getStatistics: {}", etag);

        // Check if the ETag matches the If-None-Match header
        if (ifNoneMatch != null && ifNoneMatch.equals(etag)) {
            return ServiceResult.successWithStatus(null, 304);
        }

        return ServiceResult.success((Object) statistics).withHeader("ETag", etag);
    }

    public ServiceResult<Object> planFileGroups(
            String prefix,
            String table,
            Map<String, Object> request) throws Exception {

        // Extract namespace from request body
        String namespace = (String) request.get("namespace");
        if (namespace == null || namespace.isEmpty()) {
            Map<String, Object> errorResponse = new HashMap<>();
            Map<String, Object> error = new HashMap<>();
            error.put("message", "Namespace is required");
            error.put("type", "BadRequestException");
            error.put("code", 400);
            errorResponse.put("error", error);
            return ServiceResult.failureBody(errorResponse, 400);
        }

        log.info("Planning file groups for table: {}.{}", namespace, table);

        // Extract configurations from request
        Map<String, String> properties = requestContextFactory.extractProperties(request);

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
        RequestContext context = requestContextFactory.createRequestContext(namespace, table, properties);
        context.setTxnId(extractTxnId(request));

        // Plan file groups
        String fileGroups = icebergService.planFileGroups(namespace, table, properties, context,
                minInputFiles, targetFileSizeMb);

        return ServiceResult.success((Object) fileGroups);
    }

    public ServiceResult<Object> updateSchema(
            String prefix,
            String table,
            Map<String, Object> request) throws Exception {

        String namespace = (String) request.get("namespace");
        Map<String, String> properties = requestContextFactory.extractProperties(request);
        RequestContext context = requestContextFactory.createRequestContext(namespace, table, properties);
        context.setTxnId(extractTxnId(request));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> rawOps = (List<Map<String, Object>>) request.get("operations");
        List<SchemaOp> ops = SchemaOp.fromRequestList(rawOps);

        String metadataLocation = icebergService.updateSchema(namespace, table, ops, properties, context);

        Map<String, Object> response = new HashMap<>();
        response.put("metadata-location", metadataLocation);
        return ServiceResult.success((Object) response);
    }

    public ServiceResult<Object> createTable(
            String prefix,
            Map<String, Object> request) throws Exception {

        // Extract namespace from request body
        String namespace = (String) request.get("namespace");
        if (namespace == null || namespace.isEmpty()) {
            Map<String, Object> errorResponse = new HashMap<>();
            Map<String, Object> error = new HashMap<>();
            error.put("message", "Namespace is required");
            error.put("type", "BadRequestException");
            error.put("code", 400);
            errorResponse.put("error", error);
            return ServiceResult.failureBody(errorResponse, 400);
        }

        log.info("Creating table in namespace: {}", namespace);

        // Extract configurations from request
        Map<String, String> properties = requestContextFactory.extractProperties(request);

        // Extract table name from request
        String tableName = (String) request.get("name");
        if (tableName == null || tableName.isEmpty()) {
            Map<String, Object> errorResponse = new HashMap<>();
            Map<String, Object> error = new HashMap<>();
            error.put("message", "Table name is required");
            error.put("type", "BadRequestException");
            error.put("code", 400);
            errorResponse.put("error", error);
            return ServiceResult.failureBody(errorResponse, 400);
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
            return ServiceResult.failureBody(errorResponse, 400);
        }

        // Convert schema map to Iceberg Schema (simplified for now)
        Schema schema = metadataMapper.convertMapToSchema(schemaMap);

        // Optional PARTITION BY declaration; null means unpartitioned
        @SuppressWarnings("unchecked")
        Map<String, Object> partitionSpecMap = (Map<String, Object>) request.get("partition_spec");
        PartitionSpec partitionSpec = metadataMapper.buildPartitionSpec(schema, partitionSpecMap);

        // Create request context
        RequestContext context = requestContextFactory.createRequestContext(namespace, tableName, properties);

        // Checked against the table if it already exists (cloud#610); the
        // fields' "pg-type" is what decides compatibility, see
        // service.iceberg.ExistingTableSchemaCheck.
        context.setRequestedColumns(RequestedColumn.fromSchemaMap(schemaMap, schema));

        // OSS Iceberg convention: persist only user-facing TBLPROPERTIES into
        // TableMetadata.properties. Runtime/catalog config must not be persisted.
        Map<String, String> userTableProperties =
            new HashMap<>(requestContextFactory.extractUserTableProperties(request));

        // Eliminate log-truncation orphans at the source: have Iceberg
        // physically delete superseded metadata.json files as the metadata-log
        // is trimmed (default write.metadata.previous-versions-max = 100).
        // Without this, a table with more than that many commits accumulates
        // orphaned metadata.json files that DROP-time cleanup -- which deletes
        // only the files reachable from the live metadata tree -- cannot reach.
        // Set as a default only; an explicit user TBLPROPERTY still wins.
        userTableProperties.putIfAbsent(
            "write.metadata.delete-after-commit.enabled", "true");

        // Create the table using ServiceResult - pass null as location to use default warehouse
        Table icebergTable = icebergService.createTable(namespace, tableName, schema, partitionSpec, context.getPath(), userTableProperties, context);

        // Create response following the Iceberg REST API spec
        Map<String, Object> metadataMap = metadataMapper.convertTableToMetadata(icebergTable);
        Map<String, Object> response = new HashMap<>();
        TableMetadata metadata = ((BaseTable) icebergTable).operations().current();
        String metadataLocation = metadata.metadataFileLocation();
        response.put("metadata", metadataMap);
        response.put("metadata-location", metadataLocation);
        response.put("table-location", icebergTable.location());
        response.put("partition-spec-summary", metadataMapper.partitionSpecSummary(icebergTable));

        // Only emitted when the table was adopted rather than created, so an
        // older client that does not know the key sees the response unchanged.
        if (context.isTableAlreadyExisted()) {
            response.put("already-existed", true);
        }

        // Add config to response
        Map<String, String> config = new HashMap<>();
        response.put("config", config);

        // Generate ETag header with debug information
        String etag = metadataMapper.generateETag(icebergTable, "create");

        return ServiceResult.success((Object) response).withHeader("ETag", etag);
    }

    public ServiceResult<Object> truncateToTable(
            String prefix,
            String table,
            Map<String, Object> request) throws Exception {

        String namespace = (String) request.get("namespace");
        if (namespace == null || namespace.trim().isEmpty()) {
            throw new IllegalArgumentException("Namespace is required in request body");
        }

        log.info("Truncating table: {}.{}", namespace, table);

        Map<String, String> properties = requestContextFactory.extractProperties(request);
        RequestContext context = requestContextFactory.createRequestContext(namespace, table, properties);
        context.setTxnId(extractTxnId(request));

        Map<String, Object> result = icebergService.truncateTable(namespace, table, properties, context);

        String etag = "truncate-" + System.currentTimeMillis() + "-" + Math.abs(result.hashCode());
        return ServiceResult.success((Object) result).withHeader("ETag", etag);
    }

    public ServiceResult<Object> appendToTable(
            String prefix,
            String table,
            Map<String, Object> request) throws Exception {

        // Extract namespace from request body
        String namespace = (String) request.get("namespace");
        if (namespace == null || namespace.trim().isEmpty()) {
            throw new IllegalArgumentException("Namespace is required in request body");
        }

        log.info("Appending to table: {}.{}", namespace, table);

        // Extract configurations from request
        Map<String, String> properties = requestContextFactory.extractProperties(request);

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

        logReceivedFragments(fragmentMaps);

        if (fragmentMaps == null || fragmentMaps.isEmpty()) {
            log.warn("Empty or null fragments detected, returning error");
            Map<String, Object> errorResponse = new HashMap<>();
            Map<String, Object> error = new HashMap<>();
            error.put("message", "Fragments are required for append operation");
            error.put("type", "BadRequestException");
            error.put("code", 400);
            errorResponse.put("error", error);
            return ServiceResult.failureBody(errorResponse, 400);
        }

        // Create request context
        RequestContext context = requestContextFactory.createRequestContext(namespace, table, properties);
        context.setTxnId(extractTxnId(request));

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
                return ServiceResult.failureBody(errorResponse, 400);
            }

            // Create FragmentMetadata from fragment data
            long fileSize = fragmentMap.containsKey("file_size_in_bytes") ? 
                ((Number) fragmentMap.get("file_size_in_bytes")).longValue() : 0L;
            long recordCount = fragmentMap.containsKey("record_count") ? 
                ((Number) fragmentMap.get("record_count")).longValue() : 0L;
            String format = fragmentMap.containsKey("format") ? 
                (String) fragmentMap.get("format") : "PARQUET";

            GpdbFragmentMetadata metadata = new GpdbFragmentMetadata(fileSize, format, recordCount, "DATA_FILE");
            setPartitionValuesFromMap(metadata, fragmentMap);
            setColumnStatsFromMap(metadata, fragmentMap);

            // Create Fragment with path and metadata
            Fragment fragment = new Fragment(path, metadata);
            fragments.add(fragment);
        }
        context.setFragments(fragments);

        // Perform append operation using ServiceResult
        Map<String, Object> appendResult = icebergService.appendToTable(namespace, table, properties, context);

        // Generate ETag header
        String etag = "append-" + System.currentTimeMillis() + "-" + Math.abs(appendResult.hashCode());

        return ServiceResult.success((Object) appendResult).withHeader("ETag", etag);
    }

    public ServiceResult<Object> updateToTable(
            String prefix,
            String table,
            Map<String, Object> request) throws Exception {

        // Extract namespace from request body
        String namespace = (String) request.get("namespace");
        if (namespace == null || namespace.trim().isEmpty()) {
            throw new IllegalArgumentException("Namespace is required in request body");
        }

        log.info("Update to table: {}.{}", namespace, table);

        // Extract configurations from request
        Map<String, String> properties = requestContextFactory.extractProperties(request);

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

        logReceivedFragments(fragmentMaps);

        if (fragmentMaps == null || fragmentMaps.isEmpty()) {
            log.warn("Empty or null fragments detected, returning error");
            Map<String, Object> errorResponse = new HashMap<>();
            Map<String, Object> error = new HashMap<>();
            error.put("message", "Fragments are required for update operation");
            error.put("type", "BadRequestException");
            error.put("code", 400);
            errorResponse.put("error", error);
            return ServiceResult.failureBody(errorResponse, 400);
        }

        // Create request context
        RequestContext context = requestContextFactory.createRequestContext(namespace, table, properties);
        context.setTxnId(extractTxnId(request));

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
                return ServiceResult.failureBody(errorResponse, 400);
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
            setPartitionValuesFromMap(metadata, fragmentMap);
            setColumnStatsFromMap(metadata, fragmentMap);

            // Create Fragment with path and metadata
            Fragment fragment = new Fragment(path, metadata);
            fragments.add(fragment);
        }
        context.setFragments(fragments);

        // Perform append operation using ServiceResult
        Map<String, Object> appendResult = icebergService.rowUpdate(namespace, table, properties, context);

        // Generate ETag header
        String etag = "update-" + System.currentTimeMillis() + "-" + Math.abs(appendResult.hashCode());

        return ServiceResult.success((Object) appendResult).withHeader("ETag", etag);
    }

    public ServiceResult<Object> commitFileGroups(
            String prefix,
            String table,
            Map<String, Object> request) throws Exception {

        // Extract namespace from request body
        String namespace = (String) request.get("namespace");
        if (namespace == null || namespace.trim().isEmpty()) {
            throw new IllegalArgumentException("Namespace is required in request body");
        }

        log.info("Committing file groups for table: {}.{}", namespace, table);

        // Extract configurations from request
        Map<String, String> properties = requestContextFactory.extractProperties(request);

        // Create request context
        RequestContext context = requestContextFactory.createRequestContext(namespace, table, properties);
        context.setTxnId(extractTxnId(request));

        // Extract new fragments (files to add)
        Object fragmentsObj = request.get("fragments");
        List<Map<String, Object>> fragmentMaps = parseFragmentList(fragmentsObj);

        if (fragmentMaps == null || fragmentMaps.isEmpty()) {
            return ServiceResult.failure("Fragments (new files) are required for commitFileGroups", "BadRequestException", 400);
        }

        List<Fragment> fragments = convertToFragments(fragmentMaps);
        context.setFragments(fragments);

        // Extract rewritten fragments (old files to remove)
        Object rewrittenObj = request.get("rewrittenFragments");
        List<Map<String, Object>> rewrittenMaps = parseFragmentList(rewrittenObj);

        if (rewrittenMaps == null || rewrittenMaps.isEmpty()) {
            return ServiceResult.failure("rewrittenFragments (old files) are required for commitFileGroups", "BadRequestException", 400);
        }

        List<Fragment> rewrittenFragments = convertToFragments(rewrittenMaps);
        context.setRewrittenFragments(rewrittenFragments);

        log.info("commitFileGroups: {} new fragments, {} rewritten fragments",
            fragments.size(), rewrittenFragments.size());

        // Perform commit file groups operation
        Map<String, Object> result = icebergService.commitFileGroups(namespace, table, properties, context);

        // Generate ETag header
        String etag = "commitFileGroups-" + System.currentTimeMillis() + "-" + Math.abs(result.hashCode());

        return ServiceResult.success((Object) result).withHeader("ETag", etag);
    }

    public ServiceResult<Object> commitAppend(
            String prefix,
            String table,
            Map<String, Object> request) throws Exception {

        String namespace = (String) request.get("namespace");
        if (namespace == null || namespace.trim().isEmpty()) {
            throw new IllegalArgumentException("Namespace is required in request body");
        }

        log.info("commitAppend for table: {}.{}", namespace, table);

        Map<String, String> properties = requestContextFactory.extractProperties(request);
        RequestContext context = requestContextFactory.createRequestContext(namespace, table, properties);
        context.setTxnId(extractTxnId(request));

        Object fragmentsObj = request.get("fragments");
        List<Map<String, Object>> fragmentMaps = parseFragmentList(fragmentsObj);
        if (fragmentMaps == null || fragmentMaps.isEmpty()) {
            return ServiceResult.failure("Fragments are required for commitAppend", "BadRequestException", 400);
        }

        List<Fragment> fragments = convertToFragments(fragmentMaps);
        context.setFragments(fragments);

        Map<String, Object> result = icebergService.commitAppend(namespace, table, properties, context);
        return ServiceResult.success((Object) result);
    }

    public ServiceResult<Object> commitUpdate(
            String prefix,
            String table,
            Map<String, Object> request) throws Exception {

        String namespace = (String) request.get("namespace");
        if (namespace == null || namespace.trim().isEmpty()) {
            throw new IllegalArgumentException("Namespace is required in request body");
        }

        log.info("commitUpdate for table: {}.{}", namespace, table);

        Map<String, String> properties = requestContextFactory.extractProperties(request);
        RequestContext context = requestContextFactory.createRequestContext(namespace, table, properties);
        context.setTxnId(extractTxnId(request));

        Object fragmentsObj = request.get("updateFragments");
        List<Map<String, Object>> fragmentMaps = parseFragmentList(fragmentsObj);
        if (fragmentMaps == null || fragmentMaps.isEmpty()) {
            return ServiceResult.failure("updateFragments are required for commitUpdate", "BadRequestException", 400);
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
            setPartitionValuesFromMap(metadata, fragmentMap);
            setColumnStatsFromMap(metadata, fragmentMap);
            Fragment fragment = new Fragment(path, metadata);
            fragments.add(fragment);
        }
        context.setFragments(fragments);

        Map<String, Object> result = icebergService.commitUpdate(namespace, table, properties, context);
        return ServiceResult.success((Object) result);
    }

    public ServiceResult<Object> commitRewrite(
            String prefix,
            String table,
            Map<String, Object> request) throws Exception {

        String namespace = (String) request.get("namespace");
        if (namespace == null || namespace.trim().isEmpty()) {
            throw new IllegalArgumentException("Namespace is required in request body");
        }

        log.info("commitRewrite for table: {}.{}", namespace, table);

        Map<String, String> properties = requestContextFactory.extractProperties(request);
        RequestContext context = requestContextFactory.createRequestContext(namespace, table, properties);
        context.setTxnId(extractTxnId(request));

        // Extract new fragments (files to add)
        Object fragmentsObj = request.get("fragments");
        List<Map<String, Object>> fragmentMaps = parseFragmentList(fragmentsObj);
        if (fragmentMaps == null || fragmentMaps.isEmpty()) {
            return ServiceResult.failure("Fragments (new files) are required for commitRewrite", "BadRequestException", 400);
        }
        List<Fragment> fragments = convertToFragments(fragmentMaps);
        context.setFragments(fragments);

        // Extract rewritten fragments (old files to remove)
        Object rewrittenObj = request.get("rewrittenFragments");
        List<Map<String, Object>> rewrittenMaps = parseFragmentList(rewrittenObj);
        if (rewrittenMaps == null || rewrittenMaps.isEmpty()) {
            return ServiceResult.failure("rewrittenFragments (old files) are required for commitRewrite", "BadRequestException", 400);
        }
        List<Fragment> rewrittenFragments = convertToFragments(rewrittenMaps);
        context.setRewrittenFragments(rewrittenFragments);

        Map<String, Object> result = icebergService.commitRewrite(namespace, table, properties, context);
        return ServiceResult.success((Object) result);
    }

    /**
     * Ask the storage platform to run a maintenance job for this table.
     *
     * <p>Submission only, and deliberately so: the platform queues the job and
     * runs it on its own schedule, so blocking here would tie a SQL session to
     * a compaction measured in minutes.  The reply reports whether the request
     * was accepted; 409 is passed through as-is because "a job already exists
     * for this table" is the platform's own throttle rather than an error.
     *
     * <p>Main added this as a REST controller method; it lives here so that the
     * gRPC transport answers it with the same bytes and the same status.
     */
    public ServiceResult<Object> submitMaintenance(
            String prefix,
            String table,
            Map<String, Object> request) throws Exception {

        String namespace = (String) request.get("namespace");
        if (namespace == null || namespace.trim().isEmpty()) {
            throw new IllegalArgumentException("Namespace is required in request body");
        }
        String taskType = (String) request.getOrDefault("taskType", "compaction");

        Map<String, String> properties = requestContextFactory.extractProperties(request);

        String declared = properties.get(
                catKey(IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.MAINTENANCE_API));
        if (declared == null || declared.isEmpty()) {
            return ServiceResult.failure(
                    "this catalog server declares no maintenance API",
                    "BadRequestException", 400);
        }
        if (!"s3tables".equalsIgnoreCase(declared)) {
            return ServiceResult.failure(
                    "unsupported maintenance API \"" + declared + "\"",
                    "BadRequestException", 400);
        }

        String endpoint = properties.get(
                catKey(IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.ENDPOINT));
        String region = properties.get(
                catKey(IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.REGION));
        String accessKey = properties.get(
                catKey(IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.ACCESS_KEY_ID));
        String secretKey = properties.get(
                catKey(IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.SECRET_ACCESS_KEY));
        String bucket = properties.get(
                catKey(IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.CATALOG_NAME));

        if (endpoint == null || region == null || accessKey == null
                || secretKey == null || bucket == null) {
            return ServiceResult.failure(
                    "maintenance needs endpoint, region, catalog_name and static "
                    + "credentials on the catalog server; vended credentials are "
                    + "scoped to the data plane and cannot sign this API",
                    "BadRequestException", 400);
        }

        log.info("submitMaintenance {} for {}.{}", taskType, namespace, table);
        S3TablesMaintenanceClient client =
                new S3TablesMaintenanceClient(endpoint, region, accessKey, secretKey);
        S3TablesMaintenanceClient.Result result =
                client.submit(bucket, namespace, table, taskType);

        Map<String, Object> body = new HashMap<>();
        body.put("status", result.status);
        body.put("body", result.body);
        /*
         * Top level on purpose: what the client worked out about a conflict is
         * read by the SQL side, and inside the quoted body above it would be
         * escaped past recognition.
         */
        if (result.details != null) {
            body.putAll(result.details);
        }
        return ServiceResult.successWithStatus((Object) body, result.status);
    }

    private static String catKey(String key) {
        return IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.ICEBERG_CATALOG_CONFIG_STRING + "." + key;
    }

    public ServiceResult<Object> dropTable(
            String prefix,
            String table,
            Map<String, Object> request) throws Exception {

        // Extract namespace from request body
        String namespace = (String) request.get("namespace");
        if (namespace == null || namespace.trim().isEmpty()) {
            throw new IllegalArgumentException("Namespace is required in request body");
        }

        log.info("Dropping table: {}.{}", namespace, table);

        // Extract configurations from request
        Map<String, String> properties = requestContextFactory.extractProperties(request);

        // Extract purgeRequested flag
        boolean purgeRequested = false;
        if (request.containsKey("purgeRequested")) {
            purgeRequested = (Boolean) request.get("purgeRequested");
        }

        // Create request context
        RequestContext context = requestContextFactory.createRequestContext(namespace, table, properties);
        context.setTxnId(extractTxnId(request));

        // Drop the table
        boolean dropped = icebergService.dropTable(namespace, table, purgeRequested, properties, context);

        if (!dropped) {
            return ServiceResult.failure("Failed to drop table", "ServerException", 500);
        }

        return ServiceResult.successWithStatus(null, 204);
    }

    public ServiceResult<Object> createCatalogWithNamespace(
            String prefix,
            Map<String, Object> request) throws Exception {

        log.info("Creating catalog");

        // Extract configurations from request
        Map<String, String> properties = requestContextFactory.extractProperties(request);

        // Extract catalog information from new format
        Map<String, Object> catalogInfo = (Map<String, Object>) request.get("catalog");
        if (catalogInfo == null) {
            return ServiceResult.failure("Catalog information is required", "BadRequestException", 400);
        }

        String catalogName = (String) catalogInfo.get("name");
        if (catalogName == null || catalogName.isEmpty()) {
            return ServiceResult.failure("Catalog name is required", "BadRequestException", 400);
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

        return ServiceResult.successWithStatus((Object) response, catalogCreated ? 201 : 409);
    }

    public ServiceResult<Object> createNamespace(
            String prefix,
            Map<String, Object> request) throws Exception {

        log.info("Creating namespace");

        // Extract configurations from request
        Map<String, String> properties = requestContextFactory.extractProperties(request);

        String catalogName = (String) request.get("catalogName");
        String namespaceName = (String) request.get("namespaceName");

        if (catalogName == null || catalogName.isEmpty()) {
            return ServiceResult.failure("Catalog name is required", "BadRequestException", 400);
        }

        if (namespaceName == null || namespaceName.isEmpty()) {
            return ServiceResult.failure("Namespace name is required", "BadRequestException", 400);
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

        return ServiceResult.successWithStatus((Object) response, namespaceCreated ? 201 : 409);
    }

    public ServiceResult<Object> listCatalogs(
            String prefix,
            Map<String, Object> request) throws Exception {

        log.info("Listing all catalogs");

        // Extract configurations from request
        Map<String, String> properties = requestContextFactory.extractProperties(request);

        List<String> catalogs = icebergService.listCatalogs(properties);

        Map<String, Object> response = new HashMap<>();
        response.put("catalogs", catalogs);

        return ServiceResult.success((Object) response);
    }

    public ServiceResult<Object> listNamespaces(
            String prefix,
            Map<String, Object> request) throws Exception {

        log.info("Listing namespaces");

        // Extract configurations from request
        Map<String, String> properties = requestContextFactory.extractProperties(request);

        String catalogName = (String) request.get("catalogName");
        if (catalogName == null || catalogName.isEmpty()) {
            return ServiceResult.failure("Catalog name is required", "BadRequestException", 400);
        }

        List<String> namespaces = icebergService.listNamespaces(catalogName, properties);

        Map<String, Object> response = new HashMap<>();
        response.put("namespaces", namespaces);

        return ServiceResult.success((Object) response);
    }

    public ServiceResult<Object> deleteFiles(Map<String, Object> request) {
        Object pathsObj = request.get("paths");
        @SuppressWarnings("unchecked")
        Map<String, String> fileIOConfig =
            (Map<String, String>) request.get("fileIOConfig");

        if (!(pathsObj instanceof List) || ((List<?>) pathsObj).isEmpty()) {
            return ServiceResult.failure("paths is required",
                                       "BadRequestException", 400);
        }
        if (fileIOConfig == null || fileIOConfig.isEmpty()) {
            return ServiceResult.failure("fileIOConfig is required",
                                       "BadRequestException", 400);
        }

        FileIO fileIO;
        try {
            fileIO = metadataFileCleaner.buildFileIOForCleanup(fileIOConfig);
        } catch (Exception e) {
            return ServiceResult.failure(
                "Failed to build FileIO from fileIOConfig: " + e.getMessage(),
                "BadRequestException", 400);
        }

        // Close the FileIO in a finally that swallows teardown errors, so a
        // failing HadoopFileIO.close() can never mask an otherwise-successful
        // response (the consumer treats a non-2xx reply as a delete failure).
        try {
            List<Map<String, String>> failed = new ArrayList<>();
            int deletedCount = 0;
            for (Object o : (List<?>) pathsObj) {
                String p = String.valueOf(o);
                try {
                    fileIO.deleteFile(p);
                    deletedCount++;
                } catch (NotFoundException nfe) {
                    deletedCount++;
                } catch (Exception e) {
                    MetadataFileCleaner.recordFailure(failed, p, e);
                }
            }

            Map<String, Object> resp = new HashMap<>();
            resp.put("deletedCount", deletedCount);
            resp.put("failed", failed);
            log.info("files/delete: requested={}, deleted={}, failed={}",
                     ((List<?>) pathsObj).size(), deletedCount, failed.size());
            return ServiceResult.success((Object) resp);
        } finally {
            MetadataFileCleaner.closeFileIOQuietly(fileIO);
        }
    }

    public ServiceResult<Object> cleanupFromMetadata(
            CleanupFromMetadataRequest request) {

        String metadataPath = request.getMetadataPath();
        Map<String, String> fileIOConfig = request.getFileIOConfig();

        if (metadataPath == null || metadataPath.isEmpty()) {
            return ServiceResult.failure("metadataPath is required",
                                       "BadRequestException", 400);
        }
        if (fileIOConfig == null || fileIOConfig.isEmpty()) {
            return ServiceResult.failure("fileIOConfig is required",
                                       "BadRequestException", 400);
        }

        log.info("Cleaning up metadata tree rooted at {}", metadataPath);

        // 1. Build a HadoopFileIO from the supplied config (s3/hdfs/abfss all
        //    funnel through Hadoop FS impls).
        FileIO fileIO;
        try {
            fileIO = metadataFileCleaner.buildFileIOForCleanup(fileIOConfig);
        } catch (Exception e) {
            return ServiceResult.failure(
                "Failed to build FileIO from fileIOConfig: " + e.getMessage(),
                "BadRequestException", 400);
        }

        // Close in a finally that swallows teardown errors so a failing
        // HadoopFileIO.close() cannot mask a successful cleanup (the consumer
        // treats a non-2xx reply as a delete failure and would re-queue / DLQ
        // an entry whose files were in fact already deleted).
        try {
            return metadataFileCleaner.cleanupMetadataTree(fileIO, metadataPath);
        } finally {
            MetadataFileCleaner.closeFileIOQuietly(fileIO);
        }
    }

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
     * How many fragments a debug-level log line will spell out.  Enough to see
     * the shape of a request, far short of rendering a commit.
     */
    private static final int FRAGMENT_LOG_SAMPLE = 5;

    /**
     * Note what arrived, without writing the request into the log.
     *
     * <p>This used to render the entire fragment list at INFO -- one line per
     * request, about two megabytes of it on a 6144-file commit, and growing
     * with the file count on exactly the commits that are already in trouble.
     * The count is what the line was actually useful for; the contents are
     * available at debug, and even then only the first few.
     */
    private void logReceivedFragments(List<Map<String, Object>> fragmentMaps) {
        int count = fragmentMaps == null ? 0 : fragmentMaps.size();

        log.info("Received {} fragments", count);

        if (log.isDebugEnabled() && count > 0) {
            log.debug("First {} of {} fragments: {}",
                      Math.min(FRAGMENT_LOG_SAMPLE, count), count,
                      fragmentMaps.subList(0, Math.min(FRAGMENT_LOG_SAMPLE, count)));
        }
    }

    /**
     * Pull the commit token out of a request body (issue #442).
     *
     * <p>Only commit requests carry one, and only commits act on it; reading
     * it everywhere costs nothing and keeps the endpoints uniform. Absent
     * means a client that predates the token, and the commit then behaves
     * exactly as it did before.
     */
    private String extractTxnId(Map<String, Object> request) {
        Object txnId = request.get("txn_id");
        return txnId instanceof String ? (String) txnId : null;
    }

    /**
     * Copy the write-time column statistics from a fragment map onto the
     * metadata (issue #442).  The object is carried through as parsed and
     * interpreted only where the manifest entry is built; absent means the
     * fragment carried none and the file's footer gets read instead.
     */
    private void setColumnStatsFromMap(GpdbFragmentMetadata metadata,
                                       Map<String, Object> fragmentMap) {
        Object statsObj = fragmentMap.get("column_stats");
        if (statsObj != null) {
            metadata.setColumnStats(statsObj);
        }
    }

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
            setPartitionValuesFromMap(metadata, fragmentMap);
            setColumnStatsFromMap(metadata, fragmentMap);

            Fragment fragment = new Fragment(path, metadata);
            fragments.add(fragment);
        }
        return fragments;
    }

    private void setPartitionValuesFromMap(GpdbFragmentMetadata metadata,
                                           Map<String, Object> fragmentMap) {
        Object pvObj = fragmentMap.get("partition_values");
        if (pvObj instanceof List) {
            List<?> pvRaw = (List<?>) pvObj;
            List<String> partitionValues = new ArrayList<>(pvRaw.size());
            for (Object o : pvRaw) {
                partitionValues.add(o == null ? null : o.toString());
            }
            metadata.setPartitionValues(partitionValues);
        }
    }

    /**
     * Coerce a JSON-decoded request value to int (issue #393 metadata cache).
     * Accepts both JSON numbers and numeric strings; returns def when absent or
     * unparsable.
     */
    private static int asInt(Object v, int def) {
        if (v instanceof Number) {
            return ((Number) v).intValue();
        }
        if (v instanceof String) {
            try {
                return Integer.parseInt(((String) v).trim());
            } catch (NumberFormatException ignore) {
                return def;
            }
        }
        return def;
    }

    /** Coerce a JSON-decoded request value to long; see {@link #asInt}. */
    private static long asLong(Object v, long def) {
        if (v instanceof Number) {
            return ((Number) v).longValue();
        }
        if (v instanceof String) {
            try {
                return Long.parseLong(((String) v).trim());
            } catch (NumberFormatException ignore) {
                return def;
            }
        }
        return def;
    }

    /**
     * Coerce a JSON-decoded request value to String; see {@link #asInt}.
     * Returns def for a null or non-string value instead of throwing
     * ClassCastException on a malformed request.
     */
    private static String asString(Object v, String def) {
        return (v instanceof String) ? (String) v : def;
    }

    /**
     * Writes a generic REST catalog's storage envelope into a load-table response.
     *
     * <p>The envelope goes in twice, on purpose. Its fields are flattened to the
     * top level because that is the legacy Polaris layout the existing load-table
     * parser reads, and it also travels whole under "catalog-properties" — the
     * same key get-fragments uses — because the write path needs it in a shape
     * the segment recognizes as an envelope: a volume-less INSERT has no
     * fragments to ask for, so load-table is where its credentials come from.
     *
     * <p>A null envelope leaves the response untouched rather than writing null
     * entries over the values derived from the table itself.
     *
     * <p>Here rather than in the REST controller, where main first put it,
     * because it shapes the response body, and the body is what both
     * transports must return byte for byte.
     */
    public static void putRestEnvelope(Map<String, Object> response, Map<String, Object> envelope) {
        if (envelope == null) {
            return;
        }

        response.put("metadata-location", envelope.get("metadata-location"));
        response.put("table-location", envelope.get("table-location"));
        if (envelope.containsKey("config")) {
            response.put("config", envelope.get("config"));
        }
        if (envelope.containsKey("storage-credentials")) {
            response.put("storage-credentials", envelope.get("storage-credentials"));
        }
        if (envelope.containsKey("local-data-access")) {
            response.put("local-data-access", envelope.get("local-data-access"));
        }
        response.put("envelope-version", envelope.get("envelope-version"));
        response.put("catalog-properties", envelope);
    }
}
