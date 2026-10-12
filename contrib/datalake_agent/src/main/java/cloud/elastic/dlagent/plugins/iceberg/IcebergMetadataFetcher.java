/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package cloud.elastic.dlagent.plugins.iceberg;

import cloud.elastic.dlagent.api.error.UnsupportedTypeException;
import cloud.elastic.dlagent.api.filter.Operator;
import cloud.elastic.dlagent.api.filter.TreeTraverser;
import cloud.elastic.dlagent.api.filter.FilterParser;
import cloud.elastic.dlagent.api.filter.Node;
import cloud.elastic.dlagent.api.filter.SupportedDataTypePruner;
import cloud.elastic.dlagent.api.filter.SupportedOperatorPruner;
import cloud.elastic.dlagent.api.io.DataType;
import cloud.elastic.dlagent.api.model.BasePlugin;
import cloud.elastic.dlagent.api.model.MetadataFetcher;
import cloud.elastic.dlagent.api.model.ScanTask;
import cloud.elastic.dlagent.api.model.CombinedTask;
import cloud.elastic.dlagent.api.model.Partition;
import cloud.elastic.dlagent.api.model.Metadata;
import cloud.elastic.dlagent.api.model.Fragment;
import cloud.elastic.dlagent.api.model.FragmentDescription;
import cloud.elastic.dlagent.api.utilities.ColumnDescriptor;
import cloud.elastic.dlagent.api.utilities.GpdbFragmentMetadata;
import cloud.elastic.dlagent.api.utilities.SpringContext;
import cloud.elastic.dlagent.api.utilities.Utilities;
import cloud.elastic.dlagent.plugins.hudi.utilities.FilePathUtils;
import cloud.elastic.dlagent.plugins.iceberg.utilities.IcebergUtilities;
import cloud.elastic.dlagent.service.rest.FileListRequest;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.TableOperations;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NoSuchTableException;

import java.io.IOException;
import java.util.UUID;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import static org.apache.iceberg.FileContent.EQUALITY_DELETES;

public class IcebergMetadataFetcher extends BasePlugin implements MetadataFetcher {
    private static final Logger LOG = LoggerFactory.getLogger(IcebergMetadataFetcher.class);

    /* Serializes a snapshot summary map into the scalar the C SAX parser reads. */
    private final ObjectMapper objectMapper = new ObjectMapper();

    // ----- members for predicate pushdown handling -----
    static final EnumSet<Operator> SUPPORTED_OPERATORS =
            EnumSet.of(
                    Operator.NOOP,
                    Operator.LESS_THAN,
                    Operator.GREATER_THAN,
                    Operator.LESS_THAN_OR_EQUAL,
                    Operator.GREATER_THAN_OR_EQUAL,
                    Operator.EQUALS,
                    Operator.NOT_EQUALS,
                    // Operator.LIKE,
                    Operator.IS_NULL,
                    Operator.IS_NOT_NULL,
                    Operator.IN,
                    Operator.OR,
                    Operator.AND,
                    Operator.NOT
            );

    static final EnumSet<DataType> SUPPORTED_DATATYPES =
            EnumSet.of(
                    DataType.BIGINT,
                    DataType.INTEGER,
                    DataType.SMALLINT,
                    DataType.REAL,
                    DataType.NUMERIC,
                    DataType.FLOAT8,
                    DataType.TEXT,
                    DataType.VARCHAR,
                    DataType.BPCHAR,
                    DataType.BOOLEAN,
                    DataType.DATE,
                    DataType.TIMESTAMP,
                    DataType.TIMESTAMP_WITH_TIME_ZONE,
                    DataType.TIME,
                    DataType.BYTEA
            );

    private final IcebergCatalogWrapper icebergClientWrapper;
    protected final IcebergUtilities icebergUtilities;

    /**
     * Footers this commit read ahead of the loop that needs them, by path.
     *
     * <p>Held here rather than in IcebergUtilities because that class is a
     * singleton @Component while this fetcher is prototype-scoped: one
     * instance per request, so the map cannot be reached by a commit that did
     * not fill it.  An entry is consumed by the first lookup, and it is
     * deliberately not a cache across commits -- Metrics depend on the metrics
     * config they were computed under, and a hit that ignored that would write
     * bounds the table's configuration says should not exist.
     */
    private java.util.Map<String, org.apache.iceberg.Metrics> prefetchedMetrics = java.util.Collections.emptyMap();

    public IcebergMetadataFetcher() {
        this(SpringContext.getBean(IcebergUtilities.class), SpringContext.getBean(IcebergCatalogWrapper.class));
    }

    public IcebergMetadataFetcher(IcebergUtilities icebergUtilities, IcebergCatalogWrapper icebergClientWrapper) {
        this.icebergUtilities = icebergUtilities;
        this.icebergClientWrapper = icebergClientWrapper;
    }

    @Override
    public void afterPropertiesSet() {
        super.afterPropertiesSet();
    }

    /**
     * Snapshot summary key holding the token of the commit that produced it
     * (issue #442).  The in-process guard already turns a resent commit into a
     * wait on the first one; this is what survives an agent restart, when that
     * map is gone and the only remaining evidence that a commit landed is the
     * table itself.
     */
    private static final String TXN_ID_SUMMARY_KEY = "datalake.txn-id";

    /**
     * How far back a duplicate is looked for.  A resend can only arrive within
     * the client's retry budget -- three attempts of five minutes -- so half an
     * hour is generous, and bounding it keeps the scan cheap on a table with a
     * long history.
     */
    private static final long TXN_ID_LOOKBACK_MILLIS = 30 * 60 * 1000L;

    /**
     * Record this commit's token in the snapshot it is about to create.
     *
     * <p>Costs one summary entry and is what lets a later attempt recognize
     * its own work.  A client that sends no token stamps nothing, and nothing
     * about its commit changes.
     */
    private void stampTxnId(org.apache.iceberg.SnapshotUpdate<?> update) {
        String txnId = context.getTxnId();

        if (txnId != null && !txnId.isEmpty()) {
            update.set(TXN_ID_SUMMARY_KEY, txnId);
        }
    }

    /**
     * True when this exact commit already produced a snapshot on this table.
     *
     * <p>Only an equal token counts. A false positive here would drop a commit
     * the client believes succeeded, which is worse than the duplicate
     * snapshot this is guarding against -- so the tokens are per-attempt UUIDs
     * and the comparison is exact.
     */
    private boolean alreadyCommitted(Table table, String what) {
        String txnId = context.getTxnId();

        if (txnId == null || txnId.isEmpty() || table.currentSnapshot() == null) {
            return false;
        }

        long cutoff = System.currentTimeMillis() - TXN_ID_LOOKBACK_MILLIS;
        for (Snapshot snapshot : table.snapshots()) {
            if (snapshot.timestampMillis() < cutoff) {
                continue;
            }
            Map<String, String> summary = snapshot.summary();
            if (summary != null && txnId.equals(summary.get(TXN_ID_SUMMARY_KEY))) {
                LOG.info("{}: token {} is already recorded in snapshot {}; "
                         + "not committing it again",
                         what, txnId, snapshot.snapshotId());
                return true;
            }
        }
        return false;
    }

    /**
     * Read ahead the footers of the files that did not bring statistics with
     * them, so the loop that follows does not fetch them one at a time.
     *
     * <p>Also the commit-level signal for how much of this commit fell back:
     * when every file carries statistics there is nothing to read and nothing
     * is logged, and when files do fall back the count says how many and why
     * it cost what it cost.
     */
    private void prefetchFallbackMetrics(Table table, List<Fragment> fragments) {
        List<String> dataFiles = new ArrayList<>();
        List<String> deleteFiles = new ArrayList<>();
        int parquet = 0;

        for (Fragment fragment : fragments) {
            GpdbFragmentMetadata meta = (GpdbFragmentMetadata) fragment.getMetadata();

            if (meta == null || meta.getFileFormat() == null
                    || !"PARQUET".equalsIgnoreCase(meta.getFileFormat())) {
                continue;
            }
            parquet++;
            if (meta.getColumnStats() != null) {
                continue;
            }
            /*
             * Kept apart because the two are read under different metrics
             * configs, and a delete file read under the table's config loses
             * the Full file_path bound that scopes it to one data file.
             */
            if (meta.getContentType() == GpdbFragmentMetadata.ContentType.POSITION_DELETE) {
                deleteFiles.add(fragment.getSourceName());
            } else {
                dataFiles.add(fragment.getSourceName());
            }
        }

        if (dataFiles.isEmpty() && deleteFiles.isEmpty()) {
            return;
        }
        LOG.info("{} of {} parquet files in this commit carry no write-time "
                 + "statistics and need their footers read",
                 dataFiles.size() + deleteFiles.size(), parquet);
        prefetchedMetrics = icebergUtilities.prefetchParquetMetrics(
                table.io(),
                dataFiles, org.apache.iceberg.MetricsConfig.forTable(table),
                deleteFiles, org.apache.iceberg.MetricsConfig.forPositionDelete(table));
    }

    /**
     * The same read-ahead for the file-list paths.
     *
     * <p>A separate name rather than an overload: List<Fragment> and
     * List<FileEntry> erase to the same signature.
     *
     * <p>A FileEntry carries the same two facts a fragment does -- the format
     * and whether the writer sent statistics -- so the buckets are decided the
     * same way, and for the same reason: a delete file read under the table's
     * metrics config loses the Full file_path bound that scopes it to one data
     * file, which is issue #407.
     */
    private void prefetchFallbackMetricsForEntries(Table table,
                                                   List<FileListRequest.FileEntry> entries) {
        List<String> dataFiles = new ArrayList<>();
        List<String> deleteFiles = new ArrayList<>();
        int parquet = 0;

        if (entries == null) {
            return;
        }
        for (FileListRequest.FileEntry entry : entries) {
            if (entry == null || entry.getFormat() == null
                    || !"PARQUET".equalsIgnoreCase(entry.getFormat())) {
                continue;
            }
            parquet++;
            if (entry.getColumnStats() != null) {
                continue;
            }
            if ("POSITION_DELETE".equals(entry.getContent())) {
                deleteFiles.add(entry.getFilePath());
            } else {
                dataFiles.add(entry.getFilePath());
            }
        }

        if (dataFiles.isEmpty() && deleteFiles.isEmpty()) {
            return;
        }
        LOG.info("{} of {} parquet files in this commit carry no write-time "
                 + "statistics and need their footers read",
                 dataFiles.size() + deleteFiles.size(), parquet);
        prefetchedMetrics = icebergUtilities.prefetchParquetMetrics(
                table.io(),
                dataFiles, org.apache.iceberg.MetricsConfig.forTable(table),
                deleteFiles, org.apache.iceberg.MetricsConfig.forPositionDelete(table));
    }

    public FragmentDescription getFragments(String pattern) throws Exception {
        IcebergCatalog catalog = icebergClientWrapper.getIcebergCatalog(context);
        Table table;
        try {
            table = catalog.loadTable(context.getDataSource());
        } catch (NoSuchTableException e) {
            return new FragmentDescription(null, java.util.Collections.emptyList(), -1L);
        }
        captureRestEnvelope(catalog);

        /* Get current snapshot ID (cheap: reads already-loaded metadata) */
        Snapshot currentSnapshot = table.currentSnapshot();
        long snapshotId = (currentSnapshot != null) ? currentSnapshot.snapshotId() : -1;

        /* Check conditional cache: if client has same snapshot, skip planTasks */
        String ifSnapshotStr = context.getOptions().get("if-snapshot-id");
        if (ifSnapshotStr != null) {
            try {
                long ifSnapshotId = Long.parseLong(ifSnapshotStr);
                if (snapshotId == ifSnapshotId) {
                    FragmentDescription description = FragmentDescription.notModified(snapshotId);
                    description.setCatalogProperties(context.getCatalogProperties());
                    return description;
                }
            } catch (NumberFormatException e) {
                /* ignore malformed header, proceed normally */
            }
        }

        icebergUtilities.verifySchema(table.schema(), context);

        TableScan scan = table
                .newScan()
                .project(expectedSchema(table));

        // Predicate pushdown is best-effort: if a qual cannot be translated to an
        // Iceberg expression (unsupported type/value, e.g. a DATE literal), log
        // and scan without the filter rather than failing the query.  Correctness
        // is unaffected -- the executor re-applies the original quals.
        Expression expression = null;
        try {
            expression = filterExpression();
        } catch (Exception e) {
            LOG.warn("predicate pushdown skipped (filter build failed): {}", e.toString());
        }
        if (expression != null) {
            scan = scan.filter(expression);
        }

        scan = scan.option(TableProperties.SPLIT_SIZE, String.valueOf(context.getSplitSize()));

        List<CombinedScanTask> scanTasks;
        try (CloseableIterable<CombinedScanTask> tasksIterable = scan.planTasks()) {
            scanTasks = Lists.newArrayList(tasksIterable);
        } catch (IOException e) {
            throw e;
        }

        FragmentDescription description = transformTasks(table, scanTasks, snapshotId);
        description.setCatalogProperties(context.getCatalogProperties());
        return description;
    }

    public List<Partition> getPartitions(String pattern) throws Exception {
        throw new UnsupportedOperationException("Iceberg accessor does not support getPartitions operation.");
    }

    public Metadata getSchema(String pattern) throws Exception {
        Metadata.Item tblDesc = Utilities.extractTableFromName(context.getDataSource());
        Metadata metadata = new Metadata(tblDesc);

        IcebergCatalog catalog = icebergClientWrapper.getIcebergCatalog(context);
        Table table = catalog.loadTable(context.getDataSource());

        LOG.info("table properties {}", table.properties());

        try {
            for (Types.NestedField icebergCol : table.schema().columns()) {
                metadata.addField(icebergUtilities.mapIcebergType(icebergCol));
            }
        } catch (UnsupportedTypeException e) {
            String errorMsg = "Failed to retrieve metadata for table " + metadata.getItem() + ". " +
                    e.getMessage();
            throw new UnsupportedTypeException(errorMsg);
        }

        return metadata;
    }

    @Override
    public Boolean batchAppend() throws Exception {
        Table table = null;
        IcebergCatalog catalog = icebergClientWrapper.getIcebergCatalog(context);
        try {
            table = catalog.loadTable(context.getDataSource());
            healLegacyTableProperties(table);
        } catch (NoSuchTableException e) {
            Map<String, String> properties = new HashMap<String, String>();
            properties.put(TableProperties.FORMAT_VERSION, "2");
            properties.put(TableProperties.UPDATE_MODE, "merge-on-read");
            properties.put(TableProperties.DELETE_MODE, "merge-on-read");
            properties.put(TableProperties.MERGE_MODE, "merge-on-read");

            try {
                table = catalog.createTable(TableIdentifier.parse(context.getDataSource()),
                                            icebergUtilities.formSchemaFromTupleDes(context),
                                            null,
                                            null,
                                            properties);
            } catch (org.apache.iceberg.exceptions.AlreadyExistsException ae) {
                // Another segment created the table concurrently, just load it
                table = catalog.loadTable(context.getDataSource());
            }
        }
        prefetchFallbackMetricsForEntries(table, context.getFileList());
        AppendFiles batchAppend = table.newAppend();

        // Use file list from JSON POST request body
        for (FileListRequest.FileEntry fileEntry : context.getFileList()) {
            DataFile dataFile = icebergUtilities.transFileFromFileEntry(prefetchedMetrics, fileEntry, table.io(), org.apache.iceberg.MetricsConfig.forTable(table), table.schema());
            batchAppend.appendFile(dataFile);
        }

        batchAppend.commit();
        return true;
    }

    public Map<String, Object> onlyBatchAppend() throws Exception {
        IcebergCatalog catalog = icebergClientWrapper.getIcebergCatalog(context);
        Table table = catalog.loadTable(context.getDataSource());
        healLegacyTableProperties(table);
        prefetchFallbackMetrics(table, context.getFragments());

        // Transaction wrapper: commit() only writes manifest, does NOT update catalog.
        // This deferred-commit pattern is intentional and applies to ALL catalog
        // types: the C-side tracker calls this method potentially several times
        // per DML statement (per-statement rebase + final commit-time rebase) and
        // the per-call invocations must be idempotent. The actual catalog commit
        // happens in a separate PRE_COMMIT step that calls commitAppend() on
        // this fetcher, which goes through the catalog atomically.
        Transaction txn = table.newTransaction();
        AppendFiles batchAppend = txn.newAppend();
        for (Fragment fragment : context.getFragments()) {
            DataFile dataFile = icebergUtilities.transFileFromGpdb(prefetchedMetrics, fragment, table.spec(), table.io(), org.apache.iceberg.MetricsConfig.forTable(table), table.schema());
            batchAppend.appendFile(dataFile);
        }
        batchAppend.commit(); // writes manifest only

        // Extract metadata and write metadata.json to object storage
        HasTableOperations txnTableOps = (HasTableOperations) txn.table();
        TableMetadata updatedMetadata = txnTableOps.operations().current();
        String newLocation = writeMetadataFile(table, updatedMetadata);
        return buildWriteResult(table, updatedMetadata, newLocation);
    }

    /**
     * Build the deferred-write response: the new metadata.json location plus the
     * set of metadata-layer files THIS call newly wrote to object storage, so the
     * C-side transaction tracker can clean them up on ROLLBACK (and drop the
     * superseded intermediate ones on COMMIT) -- see issue #399.
     *
     * "written-metadata-files" contains ONLY metadata-layer files:
     *   - the new metadata.json,
     *   - this snapshot's manifest-list (snap-*.avro),
     *   - the manifest(s) ADDED by this snapshot (filtered by snapshotId so
     *     manifests inherited from earlier snapshots are NOT reported -- deleting
     *     those would corrupt the base table).
     * Data/delete parquet is deliberately excluded: it is written by the C
     * provider and cleaned up via the Class 1 PendingRelDelete path.  Callers on
     * COMMIT must keep every file belonging to the finally-committed metadata.json.
     */
    private Map<String, Object> buildWriteResult(Table table, TableMetadata updatedMetadata, String newLocation) {
        Map<String, Object> result = new HashMap<>();
        List<String> written = new ArrayList<>();
        if (newLocation != null) {
            written.add(newLocation);
        }
        Snapshot snap = (updatedMetadata != null) ? updatedMetadata.currentSnapshot() : null;
        if (snap != null) {
            if (snap.manifestListLocation() != null) {
                written.add(snap.manifestListLocation());
            }
            long currentSnapshotId = snap.snapshotId();
            for (ManifestFile mf : snap.allManifests(table.io())) {
                Long addedBy = mf.snapshotId();
                if (addedBy != null && addedBy.longValue() == currentSnapshotId && mf.path() != null) {
                    written.add(mf.path());
                }
            }
        }
        result.put("metadata-location", newLocation);
        result.put("written-metadata-files", written);
        return result;
    }


    @Override
    public Metadata getOrCreateSchema() throws Exception {
        Metadata.Item tblDesc = Utilities.extractTableFromName(context.getDataSource());
        Metadata metadata = new Metadata(tblDesc);
        Table table = null;
        IcebergCatalog catalog = icebergClientWrapper.getIcebergCatalog(context);
        try {
            table = catalog.loadTable(context.getDataSource());
        } catch (NoSuchTableException e) {
            table = catalog.createTable(TableIdentifier.parse(context.getDataSource()),
                                        icebergUtilities.formSchemaFromTupleDes(context),
                                        null,
                                        null,
                                        java.util.Collections.singletonMap(TableProperties.FORMAT_VERSION, "2"));
        }
        try {
            for (Types.NestedField icebergCol : table.schema().columns()) {
                metadata.addField(icebergUtilities.mapIcebergType(icebergCol));
            }
            metadata.setLocation(table.location());
        } catch (UnsupportedTypeException e) {
            String errorMsg = "Failed to retrieve metadata for table " + metadata.getItem() + ". " +
                    e.getMessage();
            throw new UnsupportedTypeException(errorMsg);
        }
        return metadata;
    }

    @Override
    public Boolean rowUpdate() throws Exception {
        IcebergCatalog catalog = icebergClientWrapper.getIcebergCatalog(context);
        Table table = catalog.loadTable(context.getDataSource());
        healLegacyTableProperties(table);
        prefetchFallbackMetricsForEntries(table, context.getFileList());
        RowDelta rowDelta = table.newRowDelta();

        // Use file list from JSON POST request body
        for (FileListRequest.FileEntry fileEntry : context.getFileList()) {
            String content = fileEntry.getContent();
            if ("POSITION_DELETE".equals(content)) {
                DeleteFile deleteFile = icebergUtilities.transPosDeleteFromFileEntry(prefetchedMetrics, fileEntry, table.io(), org.apache.iceberg.MetricsConfig.forPositionDelete(table));
                rowDelta.addDeletes(deleteFile);
            } else {
                // Default to DATA_FILE
                DataFile dataFile = icebergUtilities.transFileFromFileEntry(prefetchedMetrics, fileEntry, table.io(), org.apache.iceberg.MetricsConfig.forTable(table), table.schema());
                rowDelta.addRows(dataFile);
            }
        }

        rowDelta.commit();
        return true;
    }

    public Map<String, Object> rowUpdateAndReturnLocation() throws Exception {
        IcebergCatalog catalog = icebergClientWrapper.getIcebergCatalog(context);
        Table table = catalog.loadTable(context.getDataSource());
        healLegacyTableProperties(table);
        prefetchFallbackMetrics(table, context.getFragments());

        // Transaction wrapper: commit() only writes manifest, does NOT update catalog.
        // See onlyBatchAppend() for the rationale — this method must be idempotent
        // across the C-side per-statement and commit-time rebase cycles.
        Transaction txn = table.newTransaction();
        RowDelta rowDelta = txn.newRowDelta();
        for (Fragment fragment : context.getFragments()) {
            GpdbFragmentMetadata meta = (GpdbFragmentMetadata)fragment.getMetadata();
            if (meta.getContentType() == GpdbFragmentMetadata.ContentType.DATA_FILE) {
                DataFile dataFile = icebergUtilities.transFileFromGpdb(prefetchedMetrics, fragment, table.spec(), table.io(), org.apache.iceberg.MetricsConfig.forTable(table), table.schema());
                rowDelta.addRows(dataFile);
            } else if (meta.getContentType() == GpdbFragmentMetadata.ContentType.POSITION_DELETE) {
                DeleteFile deleteFile = icebergUtilities.transPosDeleteFromGpdb(
                        prefetchedMetrics,
                        fragment, table.spec(), table.io(),
                        org.apache.iceberg.MetricsConfig.forPositionDelete(table));
                rowDelta.addDeletes(deleteFile);
            }
        }
        rowDelta.commit(); // writes manifest only

        // Extract metadata and write metadata.json to object storage
        HasTableOperations txnTableOps = (HasTableOperations) txn.table();
        TableMetadata updatedMetadata = txnTableOps.operations().current();
        String newLocation = writeMetadataFile(table, updatedMetadata);
        return buildWriteResult(table, updatedMetadata, newLocation);
    }

    /**
     * Truncate a builtin iceberg table to empty: commit a metadata-only delete
     * of every row (newDelete + alwaysTrue), producing a new snapshot that
     * references no data files, then write the new metadata.json.  Same
     * deferred-commit pattern as rowUpdateAndReturnLocation (Transaction +
     * writeMetadataFile, no catalog commit).  The OLD data/metadata are not
     * deleted here -- the caller enqueues the pre-truncate metadata.json so the
     * async consumer removes them.
     *
     * Returns a map with:
     *   metadata-location       -- new metadata.json (NULL/current if no-op)
     *   written-metadata-files  -- files this commit wrote (new metadata.json +
     *                              new snapshot manifest-list), for the caller
     *                              to register for abort-time cleanup
     *   truncated               -- false when the table was already empty
     */
    public Map<String, Object> truncateAndReturnLocation() throws Exception {
        IcebergCatalog catalog = icebergClientWrapper.getIcebergCatalog(context);
        /* no catalog commit below, but healLegacyTableProperties() commits */
        Table table = CommitStateGuard.wrap(catalog.loadTable(context.getDataSource()));
        healLegacyTableProperties(table);

        Map<String, Object> result = new HashMap<>();
        List<String> written = new ArrayList<>();

        // Already-empty table (no snapshots): nothing to truncate, no orphans
        // to enqueue.  Report no-op so the caller skips the queue + pointer swap.
        if (table.currentSnapshot() == null) {
            HasTableOperations ops = (HasTableOperations) table;
            result.put("metadata-location", ops.operations().current().metadataFileLocation());
            result.put("written-metadata-files", written);
            result.put("truncated", false);
            return result;
        }

        // Transaction wrapper: commit() writes manifest(s) only, does NOT update
        // the catalog -- same deferred pattern as onlyBatchAppend.
        Transaction txn = table.newTransaction();
        txn.newDelete().deleteFromRowFilter(Expressions.alwaysTrue()).commit();

        HasTableOperations txnTableOps = (HasTableOperations) txn.table();
        TableMetadata updatedMetadata = txnTableOps.operations().current();
        String newLocation = writeMetadataFile(table, updatedMetadata);

        written.add(newLocation);
        Snapshot snap = updatedMetadata.currentSnapshot();
        if (snap != null && snap.manifestListLocation() != null) {
            written.add(snap.manifestListLocation());
        }

        result.put("metadata-location", newLocation);
        result.put("written-metadata-files", written);
        result.put("truncated", true);
        return result;
    }

    /**
     * Commit file groups for vacuum/compaction using Iceberg RewriteFiles API.
     * Atomically replaces old (rewritten) files with new files.
     */
    public String commitFileGroups() throws Exception {
        IcebergCatalog catalog = icebergClientWrapper.getIcebergCatalog(context);
        /* no catalog commit below, but healLegacyTableProperties() commits */
        Table table = CommitStateGuard.wrap(catalog.loadTable(context.getDataSource()));
        healLegacyTableProperties(table);

        /*
         * Same shape as commitRewrite: the rewritten files came from the
         * planner rather than from a writer, so none of them carries
         * statistics, and without this the loops below read every one of
         * those footers one at a time.
         */
        {
            List<Fragment> all = new ArrayList<>(context.getFragments());

            if (context.getRewrittenFragments() != null) {
                all.addAll(context.getRewrittenFragments());
            }
            prefetchFallbackMetrics(table, all);
        }

        // Use transaction: rewrite.commit() only writes manifests, NOT metadata.json
        Transaction txn = table.newTransaction();

        Snapshot startingSnapshot = table.currentSnapshot();
        if (startingSnapshot == null) {
            throw new IllegalStateException(
                    "Cannot commit file groups on an Iceberg table with no snapshots: "
                            + context.getDataSource());
        }
        long startingSnapshotId = startingSnapshot.snapshotId();
        RewriteFiles rewrite = txn.newRewrite().validateFromSnapshot(startingSnapshotId);

        long sequenceNumber = table.snapshot(startingSnapshotId).sequenceNumber();
        rewrite.dataSequenceNumber(sequenceNumber);
        stampTxnId(rewrite);

        for (Fragment fragment : context.getRewrittenFragments()) {
            DataFile dataFile = icebergUtilities.transFileFromGpdb(prefetchedMetrics, fragment, table.spec(), table.io(), org.apache.iceberg.MetricsConfig.forTable(table), table.schema());
            rewrite.deleteFile(dataFile);
        }
        for (Fragment fragment : context.getFragments()) {
            DataFile dataFile = icebergUtilities.transFileFromGpdb(prefetchedMetrics, fragment, table.spec(), table.io(), org.apache.iceberg.MetricsConfig.forTable(table), table.schema());
            rewrite.addFile(dataFile);
        }

        // Commit to transaction only (writes manifest files, NOT metadata.json).
        // Deferred for all catalog types — see onlyBatchAppend() for rationale.
        rewrite.commit();

        // Extract updated metadata from transaction (don't commit to table/catalog)
        HasTableOperations txnTableOps = (HasTableOperations) txn.table();
        TableMetadata updatedMetadata = txnTableOps.operations().current();

        // Write metadata file to object storage following Iceberg naming conventions
        // (NOT committed to catalog)
        TableOperations tableOps = ((HasTableOperations) table).operations();
        FileIO io = tableOps.io();

        // Respect metadata compression codec
        String codecName = updatedMetadata.property(
            TableProperties.METADATA_COMPRESSION,
            TableProperties.METADATA_COMPRESSION_DEFAULT);
        String fileExtension = TableMetadataParser.getFileExtension(codecName);

        // Filename: staged- prefix prevents HadoopCatalog version-hint scanning
        // from confusing temp files with committed metadata of the same version
        String filename = String.format("%s%s",
            java.util.UUID.randomUUID(),
            fileExtension);

        // Path: use updatedMetadata's properties to match Iceberg's writeNewMetadata semantics
        String newMetadataFilePath = tableOps.temp(updatedMetadata).metadataFileLocation(filename);

        OutputFile outputFile = io.newOutputFile(newMetadataFilePath);
        TableMetadataParser.overwrite(updatedMetadata, outputFile);

        return newMetadataFilePath;
    }

    /**
     * Self-heal: if the loaded table has internal runtime-config keys in its
     * persisted properties (from before this fix), strip them via a one-shot
     * UpdateProperties commit. After this runs once per legacy table, subsequent
     * commits produce clean metadata.json files.
     *
     * <p>No-op when the table is already clean, so the overhead is negligible.
     */
    private void healLegacyTableProperties(Table table) {
        Set<String> toRemove = new HashSet<>();
        for (String key : table.properties().keySet()) {
            if (IcebergUtilities.isInternalConfigKey(key)) {
                toRemove.add(key);
            }
        }
        if (toRemove.isEmpty()) {
            return;
        }
        LOG.info("Iceberg self-heal: removing {} internal runtime-config keys from table '{}' properties",
                toRemove.size(), table.name());
        UpdateProperties upd = table.updateProperties();
        for (String key : toRemove) {
            upd.remove(key);
        }
        try {
            upd.commit();
        } catch (CommitFailedException e) {
            // In MPP mode every QE segment opens the same table and races to
            // strip the same legacy keys; Iceberg's optimistic concurrency
            // lets one win and rejects the rest.  Losing the race is fine —
            // the keys are gone (re-checked via refresh below).
            LOG.info("Iceberg self-heal: lost commit race on '{}'; another caller "
                     + "removed the legacy keys", table.name());
        }
        table.refresh();
    }

    /**
     * Write metadata.json to object storage without updating catalog pointer.
     * Used by deferred commit operations (onlyBatchAppend, rowUpdateAndReturnLocation).
     */
    private String writeMetadataFile(Table table, TableMetadata metadata) {
        TableOperations tableOps = ((HasTableOperations) table).operations();
        FileIO io = tableOps.io();

        // If the in-memory metadata still carries internal runtime-config keys
        // (legacy data), rebuild it without them before serialising.
        Set<String> dirtyKeys = new HashSet<>();
        for (String k : metadata.properties().keySet()) {
            if (IcebergUtilities.isInternalConfigKey(k)) {
                dirtyKeys.add(k);
            }
        }
        if (!dirtyKeys.isEmpty()) {
            LOG.info("Iceberg self-heal: stripping {} internal keys from staged metadata for '{}'",
                    dirtyKeys.size(), table.name());
            metadata = TableMetadata.buildFrom(metadata)
                    .removeProperties(dirtyKeys)
                    .build();
        }

        // Respect metadata compression codec
        String codecName = metadata.property(
            TableProperties.METADATA_COMPRESSION,
            TableProperties.METADATA_COMPRESSION_DEFAULT);
        String fileExtension = TableMetadataParser.getFileExtension(codecName);

        // Filename: staged- prefix prevents HadoopCatalog version-hint scanning
        // from confusing temp files with committed metadata of the same version
        String filename = String.format("%s%s",
            UUID.randomUUID(),
            fileExtension);

        // Use Iceberg's path resolution (respects custom metadata location)
        String newMetadataFilePath = tableOps.metadataFileLocation(filename);

        OutputFile outputFile = io.newOutputFile(newMetadataFilePath);
        TableMetadataParser.overwrite(metadata, outputFile);

        return newMetadataFilePath;
    }

    /**
     * PRE_COMMIT append: normal commit that updates catalog.
     * Called during PRE_COMMIT phase to finalize INSERT operations.
     */
    public String commitAppend() throws Exception {
        IcebergCatalog catalog = icebergClientWrapper.getIcebergCatalog(context);
        Table table = CommitStateGuard.wrap(catalog.loadTable(context.getDataSource()));
        healLegacyTableProperties(table);
        if (alreadyCommitted(table, "commitAppend")) {
            return ((HasTableOperations) table).operations().current().metadataFileLocation();
        }
        prefetchFallbackMetrics(table, context.getFragments());
        Snapshot base = table.currentSnapshot();
        Set<String> newFilePaths = new HashSet<>();
        AppendFiles batchAppend = table.newAppend();
        stampTxnId(batchAppend);
        for (Fragment fragment : context.getFragments()) {
            DataFile dataFile = icebergUtilities.transFileFromGpdb(prefetchedMetrics, fragment, table.spec(), table.io(), org.apache.iceberg.MetricsConfig.forTable(table), table.schema());
            batchAppend.appendFile(dataFile);
            newFilePaths.add(dataFile.path().toString());
        }
        try {
            batchAppend.commit(); // normal commit — updates catalog
        } catch (CommitStateUnknownException unknown) {
            return reconcileLostCommitReply(catalog, base, newFilePaths, unknown);
        }
        TableMetadata metadata = ((HasTableOperations) table).operations().current();
        return metadata.metadataFileLocation();
    }

    /**
     * PRE_COMMIT update: normal commit that updates catalog.
     * Called during PRE_COMMIT phase to finalize UPDATE/DELETE operations.
     */
    public String commitUpdate() throws Exception {
        IcebergCatalog catalog = icebergClientWrapper.getIcebergCatalog(context);
        Table table = CommitStateGuard.wrap(catalog.loadTable(context.getDataSource()));
        healLegacyTableProperties(table);
        if (alreadyCommitted(table, "commitUpdate")) {
            return ((HasTableOperations) table).operations().current().metadataFileLocation();
        }
        prefetchFallbackMetrics(table, context.getFragments());
        Snapshot base = table.currentSnapshot();
        Set<String> newFilePaths = new HashSet<>();
        RowDelta rowDelta = table.newRowDelta();
        stampTxnId(rowDelta);
        for (Fragment fragment : context.getFragments()) {
            GpdbFragmentMetadata meta = (GpdbFragmentMetadata)fragment.getMetadata();
            if (meta.getContentType() == GpdbFragmentMetadata.ContentType.DATA_FILE) {
                DataFile dataFile = icebergUtilities.transFileFromGpdb(prefetchedMetrics, fragment, table.spec(), table.io(), org.apache.iceberg.MetricsConfig.forTable(table), table.schema());
                rowDelta.addRows(dataFile);
                newFilePaths.add(dataFile.path().toString());
            } else if (meta.getContentType() == GpdbFragmentMetadata.ContentType.POSITION_DELETE) {
                DeleteFile deleteFile = icebergUtilities.transPosDeleteFromGpdb(
                        prefetchedMetrics,
                        fragment, table.spec(), table.io(),
                        org.apache.iceberg.MetricsConfig.forPositionDelete(table));
                rowDelta.addDeletes(deleteFile);
                newFilePaths.add(deleteFile.path().toString());
            }
        }
        try {
            rowDelta.commit(); // normal commit — updates catalog
        } catch (CommitStateUnknownException unknown) {
            return reconcileLostCommitReply(catalog, base, newFilePaths, unknown);
        }
        TableMetadata metadata = ((HasTableOperations) table).operations().current();
        return metadata.metadataFileLocation();
    }

    private static final int LOST_REPLY_RECONCILE_ATTEMPTS = 3;
    private static final long LOST_REPLY_RECONCILE_DELAY_MS = 1000L;

    /**
     * The commit got no reply, so "succeeded" and "failed" are both live
     * possibilities; the table itself is the arbiter (see
     * {@link CommitStateGuard#commitLanded}).  Returns the metadata location
     * when the commit turns out to have landed; throws
     * {@link CommitFailedException} when it definitively did not (safe for
     * retryRestCommit() to rerun); rethrows the original unknown when the
     * table cannot be re-read conclusively.
     */
    private String reconcileLostCommitReply(IcebergCatalog catalog, Snapshot base,
            Set<String> newFilePaths, CommitStateUnknownException unknown) throws Exception {
        if (newFilePaths.isEmpty()) {
            /* nothing file-based to look for -- the outcome cannot be verified */
            throw unknown;
        }
        for (int attempt = 1; attempt <= LOST_REPLY_RECONCILE_ATTEMPTS; attempt++) {
            /* give an in-flight server-side commit a moment to settle */
            try {
                Thread.sleep(LOST_REPLY_RECONCILE_DELAY_MS);
            } catch (InterruptedException e) {
                /*
                 * Interrupted before the table could be consulted, so the
                 * outcome is exactly as unknown as it was on entry -- say so.
                 * Letting InterruptedException escape would report a plain
                 * failure for a commit that may have landed.  Thread.sleep
                 * clears the flag, so put it back for the container.
                 */
                Thread.currentThread().interrupt();
                throw unknown;
            }
            Table fresh;
            try {
                fresh = catalog.loadTable(context.getDataSource());
            } catch (Exception e) {
                /*
                 * Exception, not RuntimeException: loadTable() is declared to
                 * throw it, and this method is too, so a checked failure would
                 * sail past a narrower catch and leave the caller with a plain
                 * error for a commit whose outcome is still unknown -- the one
                 * answer that must never be given here, for the reason spelled
                 * out on the snapshot-scan guard below.
                 */
                LOG.warn("commit outcome for {} still unknown: reload failed (attempt {}/{}): {}",
                        context.getDataSource(), attempt, LOST_REPLY_RECONCILE_ATTEMPTS, e.toString());
                continue;
            }
            Boolean landed;
            try {
                landed = CommitStateGuard.commitLanded(fresh, base, newFilePaths);
            } catch (RuntimeException e) {
                /*
                 * commitLanded() walks the snapshot chain, and every step reads
                 * a manifest from object storage -- a second connection that can
                 * fail independently of the catalog reload just above.  Letting
                 * that escape would replace the unknown-outcome exception with a
                 * plain failure, which is the one answer that must never be
                 * given here: the commit may well have landed, and the caller
                 * would re-run it and duplicate the rows.
                 */
                LOG.warn("commit outcome for {} still unknown: snapshot scan failed (attempt {}/{}): {}",
                        context.getDataSource(), attempt, LOST_REPLY_RECONCILE_ATTEMPTS, e.toString());
                continue;
            }
            if (landed == null) {
                LOG.warn("commit outcome for {} still unknown: snapshot history inconclusive (attempt {}/{})",
                        context.getDataSource(), attempt, LOST_REPLY_RECONCILE_ATTEMPTS);
                continue;
            }
            if (landed) {
                LOG.warn("commit reply for {} was lost but the commit took effect; reporting success",
                        context.getDataSource());
                return ((HasTableOperations) fresh).operations().current().metadataFileLocation();
            }
            throw new CommitFailedException(unknown,
                    "commit reply was lost and the commit did not take effect for %s",
                    context.getDataSource());
        }
        throw unknown;
    }

    /**
     * VACUUM commit: RewriteFiles + commit to catalog.
     * Atomically replaces old files with new files and updates catalog.
     */
    public String commitRewrite() throws Exception {
        IcebergCatalog catalog = icebergClientWrapper.getIcebergCatalog(context);
        Table table = CommitStateGuard.wrap(catalog.loadTable(context.getDataSource()));
        healLegacyTableProperties(table);
        if (alreadyCommitted(table, "commitRewrite")) {
            return ((HasTableOperations) table).operations().current().metadataFileLocation();
        }
        /*
         * The rewritten files are the ones being removed, and they came from
         * the planner rather than from a writer, so none of them carries
         * statistics -- they are read alongside the new files rather than one
         * at a time after them.
         */
        {
            List<Fragment> all = new ArrayList<>(context.getFragments());

            if (context.getRewrittenFragments() != null) {
                all.addAll(context.getRewrittenFragments());
            }
            prefetchFallbackMetrics(table, all);
        }

        Transaction txn = table.newTransaction();
        Snapshot startingSnapshot = table.currentSnapshot();
        if (startingSnapshot == null) {
            throw new IllegalStateException(
                    "Cannot commit rewrite on an Iceberg table with no snapshots: "
                            + context.getDataSource());
        }
        long startingSnapshotId = startingSnapshot.snapshotId();
        RewriteFiles rewrite = txn.newRewrite().validateFromSnapshot(startingSnapshotId);
        long sequenceNumber = table.snapshot(startingSnapshotId).sequenceNumber();
        rewrite.dataSequenceNumber(sequenceNumber);
        stampTxnId(rewrite);

        for (Fragment fragment : context.getRewrittenFragments()) {
            DataFile dataFile = icebergUtilities.transFileFromGpdb(prefetchedMetrics, fragment, table.spec(), table.io(), org.apache.iceberg.MetricsConfig.forTable(table), table.schema());
            rewrite.deleteFile(dataFile);
        }
        Set<String> newFilePaths = new HashSet<>();
        for (Fragment fragment : context.getFragments()) {
            DataFile dataFile = icebergUtilities.transFileFromGpdb(prefetchedMetrics, fragment, table.spec(), table.io(), org.apache.iceberg.MetricsConfig.forTable(table), table.schema());
            rewrite.addFile(dataFile);
            newFilePaths.add(dataFile.path().toString());
        }

        try {
            rewrite.commit();
            txn.commitTransaction(); // normal commit — updates catalog
        } catch (CommitStateUnknownException unknown) {
            return reconcileLostCommitReply(catalog, startingSnapshot, newFilePaths, unknown);
        }

        TableMetadata metadata = ((HasTableOperations) table).operations().current();
        return metadata.metadataFileLocation();
    }

    /**
     * Scan using uncommitted metadata from object storage.
     * Unified approach for all catalog types: loads FileIO from catalog,
     * then reads uncommitted metadata.json from the given location.
     */
    public FragmentDescription getFragmentsByUncommittedMetadata(
            String uncommittedMetadataLocation, long requestedSnapshotId) throws Exception {
        FileIO io;
        if ("rest".equals(context.getCatalogType())) {
            /*
             * A generic REST catalog needs one thing more than a FileIO: the
             * storage envelope, since its segments have no volume to read
             * credentials from.  Both come from the same TTL-cached entry, so
             * a repeat scan reaches the catalog no more often than a Polaris
             * one does -- and when it does, it is a single load-table call
             * rather than the two-stage load, which only ever ran a second
             * time to produce a FileIO.
             */
            IcebergRestCatalog.ScanStorage storage =
                    icebergClientWrapper.getRestScanStorage(context, uncommittedMetadataLocation);
            context.setCatalogProperties(
                    storage.envelopeFor(uncommittedMetadataLocation));
            io = storage.io();
        } else {
            // 1. Obtain FileIO WITHOUT a loadTable()/external-catalog round-trip
            //    where possible (issue #393): Hive hands out its shared config
            //    FileIO; vending catalogs (Polaris) use the TTL-cached vended one.
            io = icebergClientWrapper.getScanFileIO(context);
        }

        // 2. Build scannable Table using uncommitted metadata location
        //    StaticTableOperations reads the metadata.json internally
        StaticTableOperations ops = new StaticTableOperations(uncommittedMetadataLocation, io);
        BaseTable table = new BaseTable(ops, context.getDataSource());

        // 4. Reuse existing scan logic.  Time travel: read requestedSnapshotId
        //    (0 = HEAD/current).  The projection MUST resolve against the
        //    snapshot's schema, not the current one: a column dropped after the
        //    requested snapshot is absent from table.schema() but still present
        //    (and readable) in that snapshot's data files.
        TableScan scan = table.newScan();
        Schema projSchema = table.schema();                 // current, default
        if (requestedSnapshotId > 0L) {
            /* Check existence BEFORE useSnapshot(): Iceberg validates the id
             * eagerly there and would throw its internal message first,
             * making this descriptive error (which names the pinned metadata
             * file) unreachable. */
            Snapshot snap = table.snapshot(requestedSnapshotId);
            if (snap == null) {
                throw new IllegalArgumentException(
                        "snapshot " + requestedSnapshotId + " not found in metadata "
                                + uncommittedMetadataLocation);
            }
            scan = scan.useSnapshot(requestedSnapshotId);
            Integer sid = snap.schemaId();
            /* Same fail-closed rule as getSnapshotSchema: with several schemas
             * in the metadata, guessing "current" would project the wrong
             * columns.  Unreachable through SQL (getSnapshotSchema aborts
             * planning first) -- defense in depth for direct REST calls. */
            if (sid == null && table.schemas().size() > 1) {
                throw new IllegalStateException(
                        "snapshot " + requestedSnapshotId + " records no schema id and the"
                        + " metadata holds multiple schemas; cannot resolve the snapshot schema");
            }
            if (sid != null && table.schemas().containsKey(sid)) {
                projSchema = table.schemas().get(sid);
            }
        }
        scan = scan.project(expectedSchema(table, projSchema));

        // Apply predicate pushdown so manifest column bounds prune data files,
        // matching getFragments().  The AM path (builtin catalog) always sends a
        // metadata_location and therefore reaches this uncommitted-metadata branch
        // (see IcebergServiceImpl.getTableFragment), so the filter must be applied
        // here too or AM scans would never prune.
        // Predicate pushdown is best-effort: if a qual cannot be translated to an
        // Iceberg expression (unsupported type/value, e.g. a DATE literal), log
        // and scan without the filter rather than failing the query.  Correctness
        // is unaffected -- the executor re-applies the original quals.
        Expression expression = null;
        try {
            expression = filterExpression();
        } catch (Exception e) {
            LOG.warn("predicate pushdown skipped (filter build failed): {}", e.toString());
        }
        if (expression != null) {
            scan = scan.filter(expression);
        }

        List<CombinedScanTask> scanTasks;
        try (CloseableIterable<CombinedScanTask> tasks = scan.planTasks()) {
            scanTasks = Lists.newArrayList(tasks);
        }
        long snapshotId = requestedSnapshotId > 0L
            ? requestedSnapshotId
            : (table.currentSnapshot() != null ? table.currentSnapshot().snapshotId() : 0L);
        FragmentDescription description = transformTasks(table, scanTasks, snapshotId);
        description.setCatalogProperties(context.getCatalogProperties());
        return description;
    }

    /**
     * FileIO for reading a pinned metadata.json, for the callers that need
     * nothing else from the catalog.
     *
     * <p>{@link StaticTableOperations} reads the metadata document the caller
     * names, so what the catalog is really being asked for here is storage
     * access, not the table.  A generic REST catalog answers that from the
     * cross-request cache, sparing these paths the load-table round trip; the
     * other types keep loading, since for them building the catalog is either
     * cheap or already cached and the load is what makes the FileIO.
     *
     * <p>No envelope is captured: both callers answer a coordinator-side
     * describe, and nothing they produce is dispatched to a segment.
     */
    private FileIO pinnedMetadataFileIO(String metadataLocation) throws Exception {
        if ("rest".equals(context.getCatalogType())) {
            return icebergClientWrapper.getRestScanStorage(context, metadataLocation).io();
        }
        IcebergCatalog catalog = icebergClientWrapper.getIcebergCatalog(context);
        return catalog.loadTable(context.getDataSource()).io();
    }

    private void captureRestEnvelope(IcebergCatalog catalog) {
        if (catalog instanceof IcebergRestCatalog) {
            context.setCatalogProperties(
                    ((IcebergRestCatalog) catalog).catalogPropertiesEnvelope());
        }
    }

    /**
     * Resolve the Iceberg schema of a given snapshot from pinned metadata.
     *
     * <p>Feeds the C describe callback at parse time: the C side needs the
     * snapshot's schema (which may differ from the table's current schema, e.g.
     * a column dropped after the snapshot) to build the result tuple descriptor
     * and decide main-vs-fallback.  FileIO is obtained from the catalog exactly
     * as in {@link #getFragmentsByUncommittedMetadata}, then the pinned
     * metadata.json is read via StaticTableOperations.
     *
     * @param metadataLocation pinned metadata.json location
     * @param snapshotId       target snapshot id (0 = current/HEAD)
     * @return map with keys snapshotSchemaId, currentSchemaId, columns
     *         (each column: name, type, fieldId, required — in field order)
     */
    public Map<String, Object> getSnapshotSchema(String metadataLocation, long snapshotId)
            throws Exception {
        // Get FileIO from the catalog, then read pinned metadata.json.
        FileIO io = pinnedMetadataFileIO(metadataLocation);

        StaticTableOperations ops = new StaticTableOperations(metadataLocation, io);
        BaseTable table = new BaseTable(ops, context.getDataSource());

        int currentSchemaId = table.schema().schemaId();
        int schemaId = currentSchemaId;
        if (snapshotId > 0L) {
            Snapshot snap = table.snapshot(snapshotId);
            if (snap == null) {
                throw new IllegalArgumentException(
                        "snapshot " + snapshotId + " not found in metadata " + metadataLocation);
            }
            Integer sid = snap.schemaId();
            /*
             * v1 metadata may omit the per-snapshot schema id.  With a single
             * schema in the metadata that is unambiguous; with several,
             * assuming "current" could hand out the wrong descriptor for a
             * historical read -- fail closed instead of guessing.
             */
            if (sid == null && table.schemas().size() > 1) {
                throw new IllegalStateException(
                        "snapshot " + snapshotId + " records no schema id and the metadata"
                        + " holds multiple schemas; cannot resolve the snapshot schema");
            }
            if (sid != null) {
                schemaId = sid;
            }
        }
        Schema snapSchema = table.schemas().get(schemaId);
        if (snapSchema == null) {
            snapSchema = table.schema();
            schemaId = currentSchemaId;
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("snapshotSchemaId", schemaId);
        result.put("currentSchemaId", currentSchemaId);

        List<Map<String, Object>> columns = new ArrayList<>();
        for (Types.NestedField column : snapSchema.columns()) {
            Map<String, Object> col = new LinkedHashMap<>();
            col.put("name", column.name());
            col.put("type", column.type().toString());
            col.put("fieldId", column.fieldId());
            col.put("required", column.isRequired());
            columns.add(col);
        }
        result.put("columns", columns);
        return result;
    }

    /**
     * List the snapshots recorded in a pinned metadata.json (time travel
     * discovery, and the source for resolving AS OF TIMESTAMP to a snapshot id).
     *
     * Read through StaticTableOperations on the SAME metadata_location the
     * describe callback and the scan use, not through the catalog: a catalog
     * reload could return newer metadata, so the listed snapshots would not be
     * the ones the accompanying read actually sees.
     *
     * Snapshots are returned in metadata order (chronological); the caller
     * orders them if it needs to.  "summary" is emitted pre-serialized as
     * "summaryJson" so the C SAX parser only ever sees scalars.
     *
     * @param metadataLocation pinned metadata.json to read
     * @return map with keys currentSnapshotId, snapshots (each: snapshotId,
     *         timestampMs, operation, schemaId, parentId, summaryJson)
     */
    public Map<String, Object> getSnapshots(String metadataLocation) throws Exception {
        // Get FileIO from the catalog, then read pinned metadata.json.
        FileIO io = pinnedMetadataFileIO(metadataLocation);

        StaticTableOperations ops = new StaticTableOperations(metadataLocation, io);
        BaseTable table = new BaseTable(ops, context.getDataSource());

        Snapshot current = table.currentSnapshot();

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("currentSnapshotId", current != null ? current.snapshotId() : 0L);

        List<Map<String, Object>> snapshots = new ArrayList<>();
        for (Snapshot snap : table.snapshots()) {
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("snapshotId", snap.snapshotId());
            entry.put("timestampMs", snap.timestampMillis());
            entry.put("operation", snap.operation() != null ? snap.operation() : "");
            Integer sid = snap.schemaId();
            entry.put("schemaId", sid != null ? sid : -1);
            Long parent = snap.parentId();
            entry.put("parentId", parent != null ? parent : 0L);
            Map<String, String> summary = snap.summary();
            entry.put("summaryJson",
                    summary != null ? objectMapper.writeValueAsString(summary) : "{}");
            snapshots.add(entry);
        }
        result.put("snapshots", snapshots);
        return result;
    }

    /**
     * Plan file groups for vacuum/compaction.
     * Groups small files by partition and applies bin-pack algorithm.
     *
     * @param minInputFiles minimum number of files in a group to be eligible for compaction
     * @param targetFileSizeMb target file size in MB for bin-packing
     * @return FragmentDescription with combined tasks representing file groups
     */
    public FragmentDescription planFileGroups(int minInputFiles, int targetFileSizeMb) throws Exception {
        IcebergCatalog catalog = icebergClientWrapper.getIcebergCatalog(context);
        Table table = catalog.loadTable(context.getDataSource());

        long targetFileSizeBytes = (long) targetFileSizeMb * 1024 * 1024;

        // Scan all files
        TableScan scan = table.newScan();
        List<FileScanTask> allTasks;
        try (CloseableIterable<FileScanTask> tasksIterable = scan.planFiles()) {
            allTasks = Lists.newArrayList(tasksIterable);
        }

        // Group by partition
        Map<String, List<FileScanTask>> partitionGroups = new HashMap<>();
        for (FileScanTask task : allTasks) {
            String partitionKey = task.file().partition().toString();
            partitionGroups.computeIfAbsent(partitionKey, k -> new ArrayList<>()).add(task);
        }

        List<CombinedTask> combinedTasks = new ArrayList<>();

        for (Map.Entry<String, List<FileScanTask>> entry : partitionGroups.entrySet()) {
            // Filter to only small files (smaller than target size)
            List<FileScanTask> smallFiles = entry.getValue().stream()
                    .filter(t -> t.file().fileSizeInBytes() < targetFileSizeBytes)
                    .collect(Collectors.toList());

            // Skip partitions without enough small files
            if (smallFiles.size() < minInputFiles) {
                continue;
            }

            // Bin-pack the small files
            List<List<FileScanTask>> bins = binPack(smallFiles, targetFileSizeBytes);

            // Merge adjacent small bins to ensure each group meets minInputFiles
            List<List<FileScanTask>> mergedBins = mergeSmallBins(bins, minInputFiles);

            for (List<FileScanTask> bin : mergedBins) {
                // Skip groups that still don't meet the minimum file count
                if (bin.size() < minInputFiles) {
                    continue;
                }
                List<ScanTask> scanTasks = new ArrayList<>();
                for (FileScanTask fileScanTask : bin) {
                    DataFile file = fileScanTask.file();
                    IcebergFileFragmentMetadata dataMeta =
                            new IcebergFileFragmentMetadata(file.format(), file.content(),
                                    file.recordCount(), null);
                    /*
                     * Stamp the rewrite input file's partition tuple so the QE
                     * carries it through to the rewrite result; the commit
                     * (RewriteFiles.deleteFile) needs it to attach the old file
                     * to its partition on a partitioned table.  Same as the
                     * normal scan plan (transformTasks).
                     */
                    dataMeta.setPartitionValues(partitionValuesOf(table.spec(), file.partition()));
                    Fragment data = new Fragment(file.path().toString(), dataMeta);

                    List<Fragment> deletes = Lists.newArrayList();
                    for (DeleteFile delete : fileScanTask.deletes()) {
                        List<String> deleteSchemas = null;
                        if (delete.content() == EQUALITY_DELETES) {
                            /* Compaction reads HEAD: current schema. */
                            deleteSchemas = getEqColumnNames(table.schema(), delete);
                        }
                        Fragment deleteFragment = new Fragment(delete.path().toString(),
                                new IcebergFileFragmentMetadata(delete.format(), delete.content(),
                                        delete.recordCount(), deleteSchemas));
                        deletes.add(deleteFragment);
                    }

                    scanTasks.add(new ScanTask(data, deletes,
                            fileScanTask.start(), fileScanTask.length(), null));
                }
                combinedTasks.add(new CombinedTask(scanTasks));
            }
        }

        return new FragmentDescription(null, combinedTasks);
    }

    /**
     * Bin-pack algorithm: groups files into bins targeting the given size.
     * Files are sorted by size descending and placed into the first bin that has room.
     */
    private List<List<FileScanTask>> binPack(List<FileScanTask> tasks, long targetSizeBytes) {
        // Sort by file size descending
        List<FileScanTask> sorted = new ArrayList<>(tasks);
        sorted.sort((a, b) -> Long.compare(b.file().fileSizeInBytes(), a.file().fileSizeInBytes()));

        List<List<FileScanTask>> bins = new ArrayList<>();
        List<Long> binSizes = new ArrayList<>();

        for (FileScanTask task : sorted) {
            long fileSize = task.file().fileSizeInBytes();
            boolean placed = false;

            // Try to fit into existing bin
            for (int i = 0; i < bins.size(); i++) {
                if (binSizes.get(i) + fileSize <= targetSizeBytes) {
                    bins.get(i).add(task);
                    binSizes.set(i, binSizes.get(i) + fileSize);
                    placed = true;
                    break;
                }
            }

            // Create new bin if needed
            if (!placed) {
                List<FileScanTask> newBin = new ArrayList<>();
                newBin.add(task);
                bins.add(newBin);
                binSizes.add(fileSize);
            }
        }

        return bins;
    }

    /**
     * Merge adjacent small bins so each resulting group has at least minInputFiles.
     * Leftover files that cannot form a full group are appended to the last emitted group.
     */
    private List<List<FileScanTask>> mergeSmallBins(List<List<FileScanTask>> bins, int minInputFiles) {
        List<List<FileScanTask>> merged = new ArrayList<>();
        List<FileScanTask> current = new ArrayList<>();

        for (List<FileScanTask> bin : bins) {
            current.addAll(bin);
            if (current.size() >= minInputFiles) {
                merged.add(current);
                current = new ArrayList<>();
            }
        }

        // Leftover files: append to last group if one exists
        if (!current.isEmpty()) {
            if (!merged.isEmpty()) {
                merged.get(merged.size() - 1).addAll(current);
            } else {
                // No group was emitted; return the leftover as a single group
                // (it will be filtered out by the minInputFiles check in the caller)
                merged.add(current);
            }
        }

        return merged;
    }

    @Override
    public Map<String, String> getCurrentSnapshotSummary() throws Exception {
        IcebergCatalog catalog = icebergClientWrapper.getIcebergCatalog(context);
        Table table;
        try {
            table = catalog.loadTable(context.getDataSource());
        } catch (NoSuchTableException e) {
            return Collections.emptyMap();
        }
        Snapshot currentSnapshot = table.currentSnapshot();
        if (currentSnapshot == null) {
            return Collections.emptyMap();
        }
        return currentSnapshot.summary();
    }


    public Schema expectedSchema(Table table) {
        return expectedSchema(table, table.schema());
    }

    /**
     * Resolve the projected PG columns against the PASSED schema instead of the
     * table's current schema.  Time travel to an older snapshot must project
     * against that snapshot's schema so a column dropped afterwards remains
     * projectable (it is missing from table.schema() but present here).
     */
    public Schema expectedSchema(Table table, Schema schema) {
        Map<String, Types.NestedField> columnMetadata = Maps.newHashMap();

        schema.columns().stream()
                .forEach(column -> columnMetadata.put(column.name(), column));

        List<Types.NestedField> projectedFields = context.getTupleDescription().stream()
                .filter(ColumnDescriptor::isProjected)
                .map(c -> {
                    Types.NestedField t = columnMetadata.get(c.columnName());
                    if (t == null) {
                        throw new IllegalArgumentException(
                                String.format("Column %s is missing from iceberg schema", c.columnName()));
                    }
                    return t;
                })
                .collect(Collectors.toList());
        return new Schema(projectedFields);
    }

    protected EnumSet<DataType> getSupportedDatatypesForPushdown() {
        return SUPPORTED_DATATYPES;
    }

    protected EnumSet<Operator> getSupportedOperatorsForPushdown() {
        return SUPPORTED_OPERATORS;
    }

    protected Expression filterExpression() throws Exception {
        if (!context.hasFilter()) {
            return null;
        }

        /* Predicate push-down configuration */
        IcebergExpressionBuilder expressionBuilder =
                new IcebergExpressionBuilder(context.getTupleDescription());

        // Parse the filter string into a expression tree Node
        Node root = new FilterParser().parse(FilePathUtils.unescapeString(context.getFilterString()));
        TreeTraverser traverser = new TreeTraverser();

        // Prune the parsed tree with valid supported datatypes and operators and then
        // traverse the pruned tree with the expressionBuilder to produce a Expression
        traverser.traverse(
                root,
                new SupportedDataTypePruner(context.getTupleDescription(), getSupportedDatatypesForPushdown()),
                new SupportedOperatorPruner(getSupportedOperatorsForPushdown()),
                expressionBuilder);

        Expression expression = expressionBuilder.build();
        if (expression == null) {
            return null;
        }

        LOG.debug("filter expression {}", expression.toString());

        return expression;
    }

    private List<String> getEqColumnNames(Schema schema, DeleteFile delete) {
        Set<Integer> deleteIds = Sets.newHashSet(delete.equalityFieldIds());
        Schema deleteSchema = TypeUtil.select(schema, deleteIds);

        List<String> eqColumnNames = deleteSchema.columns().stream()
                .map(Types.NestedField::name)
                .collect(Collectors.toList());

        return eqColumnNames;
    }

    /*
     * Schema of the snapshot being read (by schema-id), falling back to the
     * current schema for HEAD reads or metadata that predates per-snapshot
     * schema ids.  Equality-delete key columns are resolved by field-id against
     * this schema so a time-travel read whose eq-delete key column was later
     * renamed/dropped still maps to the column names the data files are
     * projected under (see getEqColumnNames / transformTasks).
     */
    private Schema snapshotSchemaOrCurrent(Table table, long snapshotId) {
        if (snapshotId > 0L) {
            Snapshot snap = table.snapshot(snapshotId);
            if (snap != null && snap.schemaId() != null) {
                Schema s = table.schemas().get(snap.schemaId());
                if (s != null) {
                    return s;
                }
            }
        }
        return table.schema();
    }

    private FragmentDescription transformTasks(Table table, List<CombinedScanTask> scanTasks, long snapshotId) {
        // Equality-delete key columns are resolved against the schema of the
        // snapshot being read (by field-id), so a time-travel read whose key
        // column was later renamed/dropped still lines up with the data files.
        Schema readSchema = snapshotSchemaOrCurrent(table, snapshotId);
        // Pass 1: collect all unique delete files by path
        Map<String, Fragment> uniqueDeleteMap = new LinkedHashMap<>();
        for (CombinedScanTask combinedScanTask : scanTasks) {
            for (FileScanTask fileScanTask : combinedScanTask.tasks()) {
                if (fileScanTask.isDataTask()) {
                    continue;
                }
                for (DeleteFile delete : fileScanTask.deletes()) {
                    String path = delete.path().toString();
                    if (!uniqueDeleteMap.containsKey(path)) {
                        List<String> deleteSchemas = null;
                        if (delete.content() == EQUALITY_DELETES) {
                            deleteSchemas = getEqColumnNames(readSchema, delete);
                        }
                        uniqueDeleteMap.put(path, new Fragment(path,
                                new IcebergFileFragmentMetadata(delete.format(), delete.content(), delete.recordCount(), deleteSchemas)));
                    }
                }
            }
        }

        // Build index lookup: path -> index in deleteFiles list
        List<Fragment> allDeleteFiles = new ArrayList<>(uniqueDeleteMap.values());
        Map<String, Integer> deleteIndexMap = new HashMap<>();
        for (int i = 0; i < allDeleteFiles.size(); i++) {
            deleteIndexMap.put(allDeleteFiles.get(i).getSourceName(), i);
        }

        // Pass 2: build tasks with delete index references
        List<CombinedTask> tasks = Lists.newArrayList();
        for (CombinedScanTask combinedScanTask : scanTasks) {
            List<ScanTask> combinedTask = Lists.newArrayList();
            for (FileScanTask fileScanTask : combinedScanTask.tasks()) {
                if (fileScanTask.isDataTask()) {
                    continue;
                }

                DataFile file = fileScanTask.file();
                IcebergFileFragmentMetadata dataMeta =
                        new IcebergFileFragmentMetadata(file.format(), file.content(), file.recordCount(), null);
                /*
                 * Stamp the data file's identity partition values (spec order)
                 * so the C side can map this file's fileId to its partition and
                 * attribute partition-aware position deletes on UPDATE/DELETE.
                 */
                dataMeta.setPartitionValues(partitionValuesOf(table.spec(), file.partition()));
                Fragment data = new Fragment(file.path().toString(), dataMeta);

                List<Integer> deleteIndexes = new ArrayList<>();
                for (DeleteFile delete : fileScanTask.deletes()) {
                    Integer deleteIdx = deleteIndexMap.get(delete.path().toString());
                    if (deleteIdx == null) {
                        LOG.warn("Delete file path {} not found in global index map, skipping", delete.path());
                        continue;
                    }
                    deleteIndexes.add(deleteIdx);
                }

                combinedTask.add(new ScanTask(data, deleteIndexes, fileScanTask.start(), fileScanTask.length()));
            }

            tasks.add(new CombinedTask(combinedTask));
        }

        LOG.info("transformTasks: {} unique delete files, {} combined tasks",
                allDeleteFiles.size(), tasks.size());

        return new FragmentDescription(null, allDeleteFiles, tasks, snapshotId);
    }

    /*
     * Convert a data file's partition tuple into identity value texts in
     * partition-spec order.  Returns null for an unpartitioned spec; a null
     * list element denotes a SQL NULL partition value.  The text form must
     * round-trip through Conversions.fromPartitionString() on the commit path
     * (integers -> "5", strings -> "east"), matching the identity values the
     * INSERT path emits from the segment.
     *
     * NOTE on String.valueOf(): it round-trips through fromPartitionString for
     * every partition-column type the write path actually allows.  Partition
     * columns are restricted to identity partitioning on integer and
     * character/text types (the C fanout writer's parsePartitionColumns rejects
     * everything else), so BINARY / FIXED / TIME columns -- whose toString()
     * would emit an unparseable JVM representation -- can never reach here.  If
     * that type restriction is ever widened, switch to Iceberg's canonical
     * partition-string conversion for those types.
     */
    private static List<String> partitionValuesOf(org.apache.iceberg.PartitionSpec spec,
                                                  org.apache.iceberg.StructLike partition) {
        if (spec == null || !spec.isPartitioned() || partition == null) {
            return null;
        }
        org.apache.iceberg.types.Types.StructType ptype = spec.partitionType();
        int n = ptype.fields().size();
        List<String> vals = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            Class<?> javaClass = ptype.fields().get(i).type().typeId().javaClass();
            Object v = partition.get(i, javaClass);
            vals.add(v == null ? null : String.valueOf(v));
        }
        return vals;
    }

    public boolean open() throws Exception {
        return true;
    }

    public void close() throws Exception {
    }
}
