package cloud.elastic.dlagent.service.iceberg;

import cloud.elastic.dlagent.api.model.RequestContext;
import cloud.elastic.dlagent.api.model.CombinedTask;
import cloud.elastic.dlagent.api.model.Fragment;
import cloud.elastic.dlagent.api.model.FragmentDescription;
import cloud.elastic.dlagent.api.model.ScanTask;
import cloud.elastic.dlagent.plugins.iceberg.IcebergCatalog;
import cloud.elastic.dlagent.plugins.iceberg.IcebergCatalogFactory;
import cloud.elastic.dlagent.plugins.iceberg.IcebergMetadataFetcher;
import cloud.elastic.dlagent.plugins.iceberg.IcebergFileFragmentMetadata;
import cloud.elastic.dlagent.plugins.iceberg.utilities.IcebergUtilities;
import cloud.elastic.dlagent.plugins.iceberg.IcebergCatalogWrapper;
import cloud.elastic.dlagent.plugins.iceberg.IcebergPolarisCatalogManager;
import cloud.elastic.dlagent.service.ServiceResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.ArrayList;
import java.util.Collections;
import java.util.stream.Collectors;

import static org.apache.iceberg.FileContent.EQUALITY_DELETES;

/**
 * Implementation of IcebergService
 */
@Service
@Slf4j
public class IcebergServiceImpl implements IcebergService {
    @Autowired(required = false)
    private IcebergCatalogFactory icebergCatalogFactory;

    /**
     * IcebergMetadataFetcher is declared with prototype scope (see
     * IcebergPluginConfig). Inject an ObjectProvider so we obtain a fresh
     * instance for each request and never share the mutable RequestContext
     * across concurrent threads. Sharing a single fetcher caused concurrent
     * INSERTs into the same iceberg AM table to silently overwrite each
     * other's fragments — one writer's parquet would appear twice in the
     * manifest, the other's would be lost — because both threads raced on
     * setRequestContext().
     */
    @Autowired
    private ObjectProvider<IcebergMetadataFetcher> icebergMetadataFetcherProvider;

    private IcebergMetadataFetcher newFetcher(RequestContext context) {
        IcebergMetadataFetcher fetcher = icebergMetadataFetcherProvider.getObject();
        fetcher.setRequestContext(context);
        return fetcher;
    }

    @Autowired
    private IcebergCatalogWrapper icebergCatalogWrapper;

    @Autowired
    @Qualifier("polarisIcebergCatalogManager")
    private IcebergPolarisCatalogManager polarisCatalogManager;

    private final ObjectMapper objectMapper = new ObjectMapper();
    
    /**
     * Enable auto-creation of namespaces for Polaris catalog when they don't exist.
     * This feature is only supported for Polaris catalog type.
     * Other catalog types will throw exception if namespace doesn't exist.
     */
    @Value("${iceberg.polaris.auto-create-namespace:true}")
    private boolean polarisAutoCreateNamespace;

    @Override
    public Boolean checkTableExists(String namespace, String tableName, Map<String, String> properties,
            RequestContext context) throws Exception {
        IcebergCatalog catalog = icebergCatalogWrapper.getIcebergCatalog(context);
        TableIdentifier tableId = TableIdentifier.of(namespace, tableName);
        catalog.loadTable(tableId, context.getPath(), properties);
        return true;
    }

    @Override
    public Table loadTable(String namespace, String tableName, Map<String, String> properties, RequestContext context)
            throws Exception {
        IcebergCatalog catalog = icebergCatalogWrapper.getIcebergCatalog(context);
        TableIdentifier tableId = TableIdentifier.of(namespace, tableName);
        Table table = catalog.loadTable(tableId, context.getPath(), properties);
        return table;
    }

    @Override
    public Table createTable(String namespace, String tableName, Schema schema, PartitionSpec spec,
            String location, Map<String, String> properties, RequestContext context) throws Exception {
        TableIdentifier tableId = TableIdentifier.of(namespace, tableName);
        IcebergCatalog catalog = icebergCatalogWrapper.getIcebergCatalog(context);
        if (spec == null) {
            spec = PartitionSpec.unpartitioned();
        }

        // Set format version to 2 to support UPDATE/DELETE operations.
        // Belt-and-suspenders: the controller already filters user props via
        // extractUserTableProperties, but strip any internal runtime-config
        // key again here to guarantee nothing leaks into TableMetadata regardless
        // of how this service method is called.
        Map<String, String> tableProperties = new HashMap<>();
        tableProperties.put(TableProperties.FORMAT_VERSION, "2");
        if (properties != null) {
            tableProperties.putAll(IcebergUtilities.stripInternalProperties(properties));
        }
        try {
            Table table = catalog.createTable(tableId, schema, spec, location, tableProperties);
            return table;
        } catch (org.apache.iceberg.exceptions.AlreadyExistsException e) {
            log.info("Table {} already exists, loading existing table", tableId);
            return catalog.loadTable(tableId, context.getPath(), properties);
        } catch (org.apache.iceberg.exceptions.ValidationException e) {
            /*
             * Hive Metastore raises InvalidObjectException(message:database <ns>)
             * when the catalog has no database matching the iceberg namespace.
             * The iceberg-hive client wraps it as a ValidationException; for
             * users this manifests as an opaque "Invalid Hive object" error
             * that does not point at the namespace mismatch. Re-throw with a
             * message that lists the three places namespace can come from so
             * the operator knows which knob to turn.
             */
            if (isHmsMissingDatabaseError(e)) {
                String catalogType = context.getCatalogType();
                String msg = String.format(
                    "Iceberg catalog (%s) has no database matching namespace \"%s\". "
                    + "Resolution precedence is: (1) CREATE ICEBERG TABLE ... OPTIONS (namespace '<ns>') "
                    + "[per-table override], (2) CREATE FOREIGN CATALOG ... OPTIONS (default_namespace '<ns>') "
                    + "[per-catalog default], (3) PostgreSQL schema name [fallback]. "
                    + "Either CREATE the namespace in the underlying catalog (e.g. via spark-sql / beeline), "
                    + "or set OPTIONS namespace on the table, "
                    + "or set OPTIONS default_namespace on the foreign catalog, "
                    + "or move the table into a PostgreSQL schema whose name matches an existing namespace.",
                    catalogType, namespace);
                log.error("HMS missing database for namespace {}: {}", namespace, e.getMessage());
                throw new org.apache.iceberg.exceptions.NoSuchNamespaceException(msg);
            }
            throw e;
        } catch (NoSuchNamespaceException e) {
            String catalogType = context.getCatalogType();

            // Only support auto-create namespace for polaris catalog
            if (!"polaris".equals(catalogType)) {
                log.error("Namespace {} not found and auto-creation not supported for catalog type: {}",
                        namespace, catalogType);
                throw e;
            }

            // Check if auto-create is enabled for polaris
            if (!polarisAutoCreateNamespace) {
                log.error("Namespace {} not found and auto-creation is disabled for polaris catalog", namespace);
                throw e;
            }

            log.info("Namespace {} not found in polaris catalog, attempting to create it", namespace);

            // Auto-create namespace for polaris catalog
            String[] catalogAndNamespace = extractCatalogNameAndNamespace(context);
            String catalogName = catalogAndNamespace[0];

            boolean created = catalog.createNamespace(catalogName, namespace, null);

            log.info("Successfully auto-created namespace: {}", namespace);
            // Retry table creation
            Table table = catalog.createTable(tableId, schema, spec, location, tableProperties);
            return table;
        }
    }

    /**
     * Extract catalog name and namespace from full path
     * Format: catalog_name.namespace.tablename or catalog_name.namespace1.namespace2.namespace3.tablename
     * @param context RequestContext containing the full path in dataSource
     * @return String array [catalogName, namespace]
     */
    private String[] extractCatalogNameAndNamespace(RequestContext context) {
        String fullPath = context.getDataSource();
        String[] parts = fullPath.split("\\.");

        String catalogName = parts[0];

        // Join all parts except first (catalog) and last (table) as namespace
        StringBuilder namespace = new StringBuilder();
        for (int i = 1; i < parts.length - 1; i++) {
            if (i > 1) {
                namespace.append(".");
            }
            namespace.append(parts[i]);
        }

        return new String[]{catalogName, namespace.toString()};
    }

    /*
     * Detect "HMS database missing for iceberg namespace" inside a
     * ValidationException raised by iceberg-hive's HiveTableOperations.
     * The thrift cause is InvalidObjectException whose message is literally
     *   "database <catalog>.<namespace>"
     * which is distinct from a generic schema-validation failure.
     */
    private static boolean isHmsMissingDatabaseError(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            String cls = t.getClass().getName();
            String msg = t.getMessage();
            if (msg == null) continue;
            if (cls.endsWith(".InvalidObjectException") && msg.toLowerCase().contains("database ")) {
                return true;
            }
            /*
             * Belt-and-suspenders: some iceberg versions stringify the cause
             * into the outer message ("Invalid Hive object ...").
             */
            if (cls.endsWith(".ValidationException") && msg.toLowerCase().contains("invalid hive object")) {
                return true;
            }
        }
        return false;
    }

    /**
     * Cache of serialized fragment responses keyed by metadata location plus
     * everything else that shapes the response (table, filter, projection).
     *
     * An Iceberg metadata.json is immutable: any table change commits a NEW
     * metadata location, so a (location, filter, projection) key can never
     * serve stale fragments — entries are evicted purely for memory reasons.
     * This removes the per-scan loadTable + metadata.json read + manifest
     * scan (~70ms) that otherwise dominates short scans (dimension tables).
     */
    private final com.github.benmanes.caffeine.cache.Cache<String, String> fragmentCache =
            com.github.benmanes.caffeine.cache.Caffeine.newBuilder()
                    .maximumWeight(256L * 1024 * 1024)
                    .weigher((String k, String v) -> k.length() + v.length())
                    .expireAfterAccess(java.time.Duration.ofHours(6))
                    .build();

    private String fragmentCacheKey(String metadataLocation, RequestContext context) {
        StringBuilder sb = new StringBuilder(metadataLocation.length() + 128);
        // NUL separators: a filterString may legally contain '|' (bitwise ops,
        // regex alternation), which with a printable separator could collide
        // with another (table, filter) pair and serve wrong fragments.
        sb.append(metadataLocation)
          .append('\0').append(context.getDataSource())
          .append('\0').append(context.getFilterString() == null ? "" : context.getFilterString())
          .append('\0');
        if (context.getTupleDescription() != null) {
            context.getTupleDescription().forEach(
                c -> sb.append(c.columnName()).append(':').append(c.columnTypeName()).append(','));
        }
        return sb.toString();
    }

    @Override
    public String getTableFragment(String namespace, String tableName, Map<String, String> properties,
            RequestContext context) throws Exception {
        // Check for uncommitted metadata location (deferred commit Read-Your-Own-Writes)
        String uncommittedLocation = properties != null
            ? properties.get("metadata_location")
            : null;

        if (uncommittedLocation != null && !uncommittedLocation.isEmpty()) {
            String cacheKey = fragmentCacheKey(uncommittedLocation, context);
            String cached = fragmentCache.getIfPresent(cacheKey);
            if (cached != null) {
                log.debug("Fragment cache hit for {} at {}", context.getDataSource(), uncommittedLocation);
                return cached;
            }
            log.debug("Scanning with uncommitted metadata location: {}", uncommittedLocation);
            IcebergMetadataFetcher fetcher = newFetcher(context);
            FragmentDescription fragmentDescription =
                fetcher.getFragmentsByUncommittedMetadata(uncommittedLocation);
            String result = objectMapper.writeValueAsString(fragmentDescription);
            fragmentCache.put(cacheKey, result);
            return result;
        }

        IcebergMetadataFetcher fetcher = newFetcher(context);
        FragmentDescription fragmentDescription = fetcher.getFragments(null);
        return objectMapper.writeValueAsString(fragmentDescription);
    }

    @Override
    public Map<String, Object> appendToTable(String namespace, String tableName, Map<String, String> properties,
            RequestContext context) throws Exception {
        // For the logic of the AM, appending data should not involve creating tables.
        // The Iceberg AM is already different
        // from the FDW approach. Therefore, we should directly call appendTable here.
        IcebergMetadataFetcher fetcher = newFetcher(context);
        // Returns { metadata-location, written-metadata-files } so the C-side
        // tracker can clean up the metadata-layer files this append wrote on
        // ROLLBACK / drop the superseded ones on COMMIT (issue #399).
        return fetcher.onlyBatchAppend();
    }

    public Map<String, Object> rowUpdate(String namespace, String tableName, Map<String, String> properties, RequestContext context) throws Exception {
        IcebergMetadataFetcher fetcher = newFetcher(context);
        // Returns { metadata-location, written-metadata-files } — see appendToTable / issue #399.
        return fetcher.rowUpdateAndReturnLocation();
    }

    @Override
    public Map<String, Object> truncateTable(String namespace, String tableName, Map<String, String> properties,
            RequestContext context) throws Exception {
        IcebergMetadataFetcher fetcher = newFetcher(context);
        return fetcher.truncateAndReturnLocation();
    }

    @Override
    public boolean dropTable(String namespace, String tableName, boolean purgeRequested,
                             Map<String, String> properties, RequestContext context) throws Exception {
        IcebergCatalog catalog = icebergCatalogWrapper.getIcebergCatalog(context);
        return catalog.dropTable(tableName, purgeRequested);
    }

    @Override
    public boolean createCatalog(String catalogName, Map<String, String> catalogProperties,
            Map<String, Object> storageConfig, Map<String, String> properties) throws Exception {
        List<String> catalogs = polarisCatalogManager.listCatalogs(properties);
        if (catalogs.contains(catalogName)) {
            log.info("Catalog {} already exists, skipping creation", catalogName);
            return true;
        }
        if (storageConfig == null || storageConfig.isEmpty()) {
            throw new IllegalArgumentException("storageConfig cannot be null or empty for catalog creation");
        }
        return polarisCatalogManager.createCatalog(catalogName, catalogProperties, storageConfig, properties);
    }

    @Override
    public boolean createNamespace(String catalogName, String namespaceName,
            Map<String, String> namespaceProperties, Map<String, String> properties) throws Exception {
        try {
            RequestContext context = polarisCatalogManager.createRequestContextForCatalogOps(properties);
            IcebergCatalog catalog = icebergCatalogWrapper.getIcebergCatalog(context);
            return catalog.createNamespace(catalogName, namespaceName, namespaceProperties);
        } catch (Exception e) {
            log.error("Failed to create namespace: {} in catalog: {}", namespaceName, catalogName, e);
            // If namespace already exists, return true
            if (e.getMessage() != null && e.getMessage().contains("already exists")) {
                return true;
            }
            throw e;
        }
    }

    @Override
    public List<String> listCatalogs(Map<String, String> properties) throws Exception {
        return polarisCatalogManager.listCatalogs(properties);
    }

    @Override
    public List<String> listNamespaces(String catalogName, Map<String, String> properties) throws Exception {
        return polarisCatalogManager.listNamespaces(catalogName, properties);
    }

    @Override
    public String planFileGroups(String namespace, String tableName, Map<String, String> properties,
            RequestContext context, int minInputFiles, int targetFileSizeMb) throws Exception {
        IcebergMetadataFetcher fetcher = newFetcher(context);
        FragmentDescription fragmentDescription = fetcher.planFileGroups(minInputFiles, targetFileSizeMb);
        String result = objectMapper.writeValueAsString(fragmentDescription);
        return result;
    }

    @Override
    public Map<String, Object> commitFileGroups(String namespace, String tableName,
            Map<String, String> properties, RequestContext context) throws Exception {
        IcebergMetadataFetcher fetcher = newFetcher(context);
        String metadataLocation = fetcher.commitFileGroups();
        Map<String, Object> result = new HashMap<>();
        result.put("metadata-location", metadataLocation);
        return result;
    }

    @Override
    public Map<String, Object> commitAppend(String namespace, String tableName,
            Map<String, String> properties, RequestContext context) throws Exception {
        IcebergMetadataFetcher fetcher = newFetcher(context);
        String metadataLocation = fetcher.commitAppend();
        Map<String, Object> result = new HashMap<>();
        result.put("metadata-location", metadataLocation);
        return result;
    }

    @Override
    public Map<String, Object> commitUpdate(String namespace, String tableName,
            Map<String, String> properties, RequestContext context) throws Exception {
        IcebergMetadataFetcher fetcher = newFetcher(context);
        String metadataLocation = fetcher.commitUpdate();
        Map<String, Object> result = new HashMap<>();
        result.put("metadata-location", metadataLocation);
        return result;
    }

    @Override
    public Map<String, Object> commitRewrite(String namespace, String tableName,
            Map<String, String> properties, RequestContext context) throws Exception {
        IcebergMetadataFetcher fetcher = newFetcher(context);
        String metadataLocation = fetcher.commitRewrite();
        Map<String, Object> result = new HashMap<>();
        result.put("metadata-location", metadataLocation);
        return result;
    }

    @Override
    public String getTableStatistics(String namespace, String tableName, Map<String, String> properties,
            RequestContext context) throws Exception {
        IcebergMetadataFetcher fetcher = newFetcher(context);
        Map<String, String> summary = fetcher.getCurrentSnapshotSummary();
        if (summary == null) {
            summary = Collections.emptyMap();
        }
        Map<String, String> statistics = new HashMap<>();
        // The snapshot summary's "total-records" counts every row stored in
        // the data files, including rows superseded by delete files, so a
        // table whose rows were UPDATEd reports old + new versions.  Report
        // live rows instead: position deletes remove exactly one row each;
        // equality deletes are subtracted as a best-effort estimate.
        long totalRecords = parseLongOrZero(summary.get("total-records"));
        long positionDeletes = parseLongOrZero(summary.get("total-position-deletes"));
        long equalityDeletes = parseLongOrZero(summary.get("total-equality-deletes"));
        long liveRecords = Math.max(0L, totalRecords - positionDeletes - equalityDeletes);
        statistics.put("total-records", Long.toString(liveRecords));
        statistics.put("total-files-size", summary.getOrDefault("total-files-size", "0"));
        return objectMapper.writeValueAsString(statistics);
    }

    private static long parseLongOrZero(String value) {
        if (value == null) {
            return 0L;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    /**
     * Helper method to convert Iceberg Table to metadata map
     */
    private Map<String, Object> convertTableToMetadata(Table table) {
        Map<String, Object> metadata = new HashMap<>();

        // Basic table information

        // metadata.put("format-version", table.ops().current().formatVersion());
        // metadata.put("table-uuid", table.uuid());
        metadata.put("location", table.location());
        metadata.put("last-updated-ms", table.currentSnapshot() != null ? table.currentSnapshot().timestampMillis()
                : System.currentTimeMillis());

        // Schema information
        Schema schema = table.schema();
        metadata.put("current-schema-id", schema.schemaId());

        // Add more metadata fields as needed

        return metadata;
    }

}
