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

import static org.apache.iceberg.FileContent.EQUALITY_DELETES;

public class IcebergMetadataFetcher extends BasePlugin implements MetadataFetcher {
    private static final Logger LOG = LoggerFactory.getLogger(IcebergMetadataFetcher.class);

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

    public FragmentDescription getFragments(String pattern) throws Exception {
        IcebergCatalog catalog = icebergClientWrapper.getIcebergCatalog(context);
        Table table;
        try {
            table = catalog.loadTable(context.getDataSource());
        } catch (NoSuchTableException e) {
            return new FragmentDescription(null, java.util.Collections.emptyList(), -1L);
        }

        /* Get current snapshot ID (cheap: reads already-loaded metadata) */
        Snapshot currentSnapshot = table.currentSnapshot();
        long snapshotId = (currentSnapshot != null) ? currentSnapshot.snapshotId() : -1;

        /* Check conditional cache: if client has same snapshot, skip planTasks */
        String ifSnapshotStr = context.getOptions().get("if-snapshot-id");
        if (ifSnapshotStr != null) {
            try {
                long ifSnapshotId = Long.parseLong(ifSnapshotStr);
                if (snapshotId == ifSnapshotId) {
                    return FragmentDescription.notModified(snapshotId);
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

        return transformTasks(table, scanTasks, snapshotId);
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
        AppendFiles batchAppend = table.newAppend();

        // Use file list from JSON POST request body
        for (FileListRequest.FileEntry fileEntry : context.getFileList()) {
            DataFile dataFile = icebergUtilities.transFileFromFileEntry(fileEntry, table.io(), org.apache.iceberg.MetricsConfig.forTable(table));
            batchAppend.appendFile(dataFile);
        }

        batchAppend.commit();
        return true;
    }

    public String onlyBatchAppend() throws Exception {
        IcebergCatalog catalog = icebergClientWrapper.getIcebergCatalog(context);
        Table table = catalog.loadTable(context.getDataSource());
        healLegacyTableProperties(table);

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
            DataFile dataFile = icebergUtilities.transFileFromGpdb(fragment, table.io(), org.apache.iceberg.MetricsConfig.forTable(table));
            batchAppend.appendFile(dataFile);
        }
        batchAppend.commit(); // writes manifest only

        // Extract metadata and write metadata.json to object storage
        HasTableOperations txnTableOps = (HasTableOperations) txn.table();
        TableMetadata updatedMetadata = txnTableOps.operations().current();
        return writeMetadataFile(table, updatedMetadata);
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
        RowDelta rowDelta = table.newRowDelta();

        // Use file list from JSON POST request body
        for (FileListRequest.FileEntry fileEntry : context.getFileList()) {
            String content = fileEntry.getContent();
            if ("POSITION_DELETE".equals(content)) {
                DeleteFile deleteFile = icebergUtilities.transPosDeleteFromFileEntry(fileEntry);
                rowDelta.addDeletes(deleteFile);
            } else {
                // Default to DATA_FILE
                DataFile dataFile = icebergUtilities.transFileFromFileEntry(fileEntry, table.io(), org.apache.iceberg.MetricsConfig.forTable(table));
                rowDelta.addRows(dataFile);
            }
        }

        rowDelta.commit();
        return true;
    }

    public String rowUpdateAndReturnLocation() throws Exception {
        IcebergCatalog catalog = icebergClientWrapper.getIcebergCatalog(context);
        Table table = catalog.loadTable(context.getDataSource());
        healLegacyTableProperties(table);

        // Transaction wrapper: commit() only writes manifest, does NOT update catalog.
        // See onlyBatchAppend() for the rationale — this method must be idempotent
        // across the C-side per-statement and commit-time rebase cycles.
        Transaction txn = table.newTransaction();
        RowDelta rowDelta = txn.newRowDelta();
        for (Fragment fragment : context.getFragments()) {
            GpdbFragmentMetadata meta = (GpdbFragmentMetadata)fragment.getMetadata();
            if (meta.getContentType() == GpdbFragmentMetadata.ContentType.DATA_FILE) {
                DataFile dataFile = icebergUtilities.transFileFromGpdb(fragment, table.io(), org.apache.iceberg.MetricsConfig.forTable(table));
                rowDelta.addRows(dataFile);
            } else if (meta.getContentType() == GpdbFragmentMetadata.ContentType.POSITION_DELETE) {
                DeleteFile deleteFile = icebergUtilities.transPosDeleteFromGpdb(fragment);
                rowDelta.addDeletes(deleteFile);
            }
        }
        rowDelta.commit(); // writes manifest only

        // Extract metadata and write metadata.json to object storage
        HasTableOperations txnTableOps = (HasTableOperations) txn.table();
        TableMetadata updatedMetadata = txnTableOps.operations().current();
        return writeMetadataFile(table, updatedMetadata);
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
        Table table = catalog.loadTable(context.getDataSource());
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
        Table table = catalog.loadTable(context.getDataSource());
        healLegacyTableProperties(table);

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

        for (Fragment fragment : context.getRewrittenFragments()) {
            DataFile dataFile = icebergUtilities.transFileFromGpdb(fragment, table.io(), org.apache.iceberg.MetricsConfig.forTable(table));
            rewrite.deleteFile(dataFile);
        }
        for (Fragment fragment : context.getFragments()) {
            DataFile dataFile = icebergUtilities.transFileFromGpdb(fragment, table.io(), org.apache.iceberg.MetricsConfig.forTable(table));
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
        Table table = catalog.loadTable(context.getDataSource());
        healLegacyTableProperties(table);
        AppendFiles batchAppend = table.newAppend();
        for (Fragment fragment : context.getFragments()) {
            DataFile dataFile = icebergUtilities.transFileFromGpdb(fragment, table.io(), org.apache.iceberg.MetricsConfig.forTable(table));
            batchAppend.appendFile(dataFile);
        }
        batchAppend.commit(); // normal commit — updates catalog
        TableMetadata metadata = ((HasTableOperations) table).operations().current();
        return metadata.metadataFileLocation();
    }

    /**
     * PRE_COMMIT update: normal commit that updates catalog.
     * Called during PRE_COMMIT phase to finalize UPDATE/DELETE operations.
     */
    public String commitUpdate() throws Exception {
        IcebergCatalog catalog = icebergClientWrapper.getIcebergCatalog(context);
        Table table = catalog.loadTable(context.getDataSource());
        healLegacyTableProperties(table);
        RowDelta rowDelta = table.newRowDelta();
        for (Fragment fragment : context.getFragments()) {
            GpdbFragmentMetadata meta = (GpdbFragmentMetadata)fragment.getMetadata();
            if (meta.getContentType() == GpdbFragmentMetadata.ContentType.DATA_FILE) {
                DataFile dataFile = icebergUtilities.transFileFromGpdb(fragment, table.io(), org.apache.iceberg.MetricsConfig.forTable(table));
                rowDelta.addRows(dataFile);
            } else if (meta.getContentType() == GpdbFragmentMetadata.ContentType.POSITION_DELETE) {
                DeleteFile deleteFile = icebergUtilities.transPosDeleteFromGpdb(fragment);
                rowDelta.addDeletes(deleteFile);
            }
        }
        rowDelta.commit(); // normal commit — updates catalog
        TableMetadata metadata = ((HasTableOperations) table).operations().current();
        return metadata.metadataFileLocation();
    }

    /**
     * VACUUM commit: RewriteFiles + commit to catalog.
     * Atomically replaces old files with new files and updates catalog.
     */
    public String commitRewrite() throws Exception {
        IcebergCatalog catalog = icebergClientWrapper.getIcebergCatalog(context);
        Table table = catalog.loadTable(context.getDataSource());
        healLegacyTableProperties(table);

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

        for (Fragment fragment : context.getRewrittenFragments()) {
            DataFile dataFile = icebergUtilities.transFileFromGpdb(fragment, table.io(), org.apache.iceberg.MetricsConfig.forTable(table));
            rewrite.deleteFile(dataFile);
        }
        for (Fragment fragment : context.getFragments()) {
            DataFile dataFile = icebergUtilities.transFileFromGpdb(fragment, table.io(), org.apache.iceberg.MetricsConfig.forTable(table));
            rewrite.addFile(dataFile);
        }

        rewrite.commit();
        txn.commitTransaction(); // normal commit — updates catalog

        TableMetadata metadata = ((HasTableOperations) table).operations().current();
        return metadata.metadataFileLocation();
    }

    /**
     * Scan using uncommitted metadata from object storage.
     * Unified approach for all catalog types: loads FileIO from catalog,
     * then reads uncommitted metadata.json from the given location.
     */
    public FragmentDescription getFragmentsByUncommittedMetadata(
            String uncommittedMetadataLocation) throws Exception {
        // 1. Load table from catalog to get FileIO
        IcebergCatalog catalog = icebergClientWrapper.getIcebergCatalog(context);
        Table catalogTable = catalog.loadTable(context.getDataSource());
        FileIO io = catalogTable.io();

        // 2. Build scannable Table using uncommitted metadata location
        //    StaticTableOperations reads the metadata.json internally
        StaticTableOperations ops = new StaticTableOperations(uncommittedMetadataLocation, io);
        BaseTable table = new BaseTable(ops, context.getDataSource());

        // 4. Reuse existing scan logic
        TableScan scan = table.newScan().project(expectedSchema(table));

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
        long snapshotId = table.currentSnapshot() != null ? table.currentSnapshot().snapshotId() : 0L;
        return transformTasks(table, scanTasks, snapshotId);
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
                    Fragment data = new Fragment(file.path().toString(),
                            new IcebergFileFragmentMetadata(file.format(), file.content(),
                                    file.recordCount(), null));

                    List<Fragment> deletes = Lists.newArrayList();
                    for (DeleteFile delete : fileScanTask.deletes()) {
                        List<String> deleteSchemas = null;
                        if (delete.content() == EQUALITY_DELETES) {
                            deleteSchemas = getEqColumnNames(table, delete);
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
        Map<String, Types.NestedField> columnMetadata = Maps.newHashMap();

        table.schema().columns().stream()
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

    private List<String> getEqColumnNames(Table table, DeleteFile delete) {
        Set<Integer> deleteIds = Sets.newHashSet(delete.equalityFieldIds());
        Schema deleteSchema = TypeUtil.select(table.schema(), deleteIds);

        List<String> eqColumnNames = deleteSchema.columns().stream()
                .map(Types.NestedField::name)
                .collect(Collectors.toList());

        return eqColumnNames;
    }

    private FragmentDescription transformTasks(Table table, List<CombinedScanTask> scanTasks, long snapshotId) {
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
                            deleteSchemas = getEqColumnNames(table, delete);
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
                Fragment data = new Fragment(file.path().toString(),
                        new IcebergFileFragmentMetadata(file.format(), file.content(), file.recordCount(), null));

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

    public boolean open() throws Exception {
        return true;
    }

    public void close() throws Exception {
    }
}
