package cloud.elastic.dlagent.service.iceberg;

import cloud.elastic.dlagent.plugins.iceberg.IcebergCatalog;
import cloud.elastic.dlagent.api.model.RequestContext;
import cloud.elastic.dlagent.service.ServiceResult;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.Map;
import java.util.List;

/**
 * Service interface for Iceberg operations
 */
public interface IcebergService {

    /**
     * Check if a table exists
     */
    Boolean checkTableExists(String namespace, String tableName, Map<String, String> properties, RequestContext context) throws Exception;

    /**
     * Load a table from the catalog
     */
    Table loadTable(String namespace, String tableName, Map<String, String> properties, RequestContext context) throws Exception;

    /**
     * Return the table's current metadata.json bytes verbatim, with exactly ONE object
     * storage read. Not loadTable + read: a table load already GETs the document in order
     * to build TableMetadata, so going through it would fetch the same object twice.
     */
    byte[] loadMetadataJson(String namespace, String tableName, Map<String, String> properties,
            RequestContext context) throws Exception;

    /**
     * Create a new table.
     *
     * @param spec partition spec built against {@code schema}; null means
     *             unpartitioned
     */
    Table createTable(String namespace, String tableName, Schema schema, PartitionSpec spec,
                      String location, Map<String, String> properties, RequestContext context) throws Exception;

    /**
     * Get table fragment
     */
    String getTableFragment(String namespace, String tableName, Map<String, String> properties, RequestContext context) throws Exception;

    /**
     * Append data files to a table
     */
    Map<String, Object> appendToTable(String namespace, String tableName, Map<String, String> properties, RequestContext context) throws Exception;

    /**
     * Update data files to a table
     */
    Map<String, Object> rowUpdate(String namespace, String tableName, Map<String, String> properties, RequestContext context) throws Exception;

    /**
     * Truncate a builtin iceberg table to empty (metadata-only delete of all
     * rows + new metadata.json).  Returns metadata-location, written-metadata-
     * files, and a truncated flag (false when the table was already empty).
     */
    Map<String, Object> truncateTable(String namespace, String tableName, Map<String, String> properties, RequestContext context) throws Exception;

    /**
     * Drop a table
     */
    boolean dropTable(String namespace, String tableName, boolean purgeRequested, Map<String, String> properties, RequestContext context) throws Exception;

    /**
    * Create a catalog
     */
    boolean createCatalog(String catalogName, Map<String, String> catalogProperties,
                          Map<String, Object> storageConfig, Map<String, String> properties) throws Exception;

    /**
     * Create a namespace in the specified catalog
     */
    boolean createNamespace(String catalogName, String namespaceName,
                            Map<String, String> namespaceProperties, Map<String, String> properties) throws Exception;

    /**
     * List all catalogs
     */
    List<String> listCatalogs(Map<String, String> properties) throws Exception;

    /**
     * List all namespaces in the specified catalog
     */
    List<String> listNamespaces(String catalogName, Map<String, String> properties) throws Exception;

    /**
     * Get table statistics (record count, file size, etc.) from the current snapshot summary
     */
    String getTableStatistics(String namespace, String tableName, Map<String, String> properties, RequestContext context) throws Exception;

    /**
     * Plan file groups for vacuum/compaction
     * Groups small files that can be merged together based on partition and target file size
     */
    String planFileGroups(String namespace, String tableName, Map<String, String> properties,
                          RequestContext context, int minInputFiles, int targetFileSizeMb) throws Exception;

    /**
     * Commit file groups for vacuum/compaction
     * Atomically replaces old files with new files using Iceberg RewriteFiles API
     */
    Map<String, Object> commitFileGroups(String namespace, String tableName,
        Map<String, String> properties, RequestContext context) throws Exception;

    /**
     * PRE_COMMIT append: normal AppendFiles commit that updates catalog
     */
    Map<String, Object> commitAppend(String namespace, String tableName,
        Map<String, String> properties, RequestContext context) throws Exception;

    /**
     * PRE_COMMIT update: normal RowDelta commit that updates catalog
     */
    Map<String, Object> commitUpdate(String namespace, String tableName,
        Map<String, String> properties, RequestContext context) throws Exception;

    /**
     * VACUUM commit: RewriteFiles + commit to catalog
     */
    Map<String, Object> commitRewrite(String namespace, String tableName,
        Map<String, String> properties, RequestContext context) throws Exception;

    /**
     * ALTER TABLE schema evolution (builtin catalog only, issue #401): apply the given
     * ops via Iceberg UpdateSchema and commit. Returns the new metadata-location.
     * Builtin-only gating is enforced upstream on the datalake_fdw utility side.
     */
    String updateSchema(String namespace, String tableName, java.util.List<SchemaOp> ops,
        Map<String, String> properties, RequestContext context) throws Exception;
}
