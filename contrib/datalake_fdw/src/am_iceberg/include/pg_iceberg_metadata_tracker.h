/*-------------------------------------------------------------------------
 *
 * pg_iceberg_metadata_tracker.h
 *    Transaction-level metadata location tracker for Iceberg tables.
 *
 * This module tracks metadata location changes and accumulated data/delete
 * files for Iceberg tables within a PostgreSQL transaction, providing:
 *   - Per-table metadata location tracking (via HTAB)
 *   - Data file and delete file accumulation across statements
 *   - Savepoint/subtransaction rollback support (via level_history stack)
 *   - Automatic commit/abort handling (via xact callbacks)
 *
 * Design Overview (referencing Rust MetadataLocationTracker + pg_lake patterns):
 *
 *   Each tracked table has a TableMetadataState which records:
 *     - initial_base_metadata_location: the catalog value when first accessed
 *     - current_metadata_location: the latest metadata location in this txn
 *     - last_base_metadata_location: the global base used for rebase checks
 *     - data_files: List of accumulated data files (CONTENT_DATA)
 *     - delete_files: List of accumulated delete files (CONTENT_POSITION_DELETES)
 *     - level_history: List of snapshots for savepoint rollback
 *
 *   The level_history stack records both metadata locations AND file counts,
 *   so that ROLLBACK TO SAVEPOINT correctly undoes both metadata changes
 *   and file accumulations at that nesting level.
 *
 *   On PRE_COMMIT, the tracker persists all pending metadata changes to
 *   the catalog. On ABORT, all tracked changes are discarded. On
 *   subtransaction ABORT, only changes at that nesting level are rolled back.
 *
 * File Accumulation Pattern (inspired by pg_lake's IcebergSnapshotBuilder):
 *
 *   pg_lake separates data entries and positional delete entries in its
 *   IcebergSnapshotBuilder. We follow the same separation using PG List:
 *     - data_files:   List of TrackedDataFile* (parquet files with row data)
 *     - delete_files: List of TrackedDataFile* (marking rows for deletion)
 *
 *   Unlike pg_lake, we do NOT support partition tables, and we do NOT
 *   generate metadata files at transaction end. The accumulated files
 *   are available for the caller to use (e.g., for rebase operations).
 *
 * Usage:
 *   1. Call pg_iceberg_init_metadata_tracking() once during _PG_init
 *   2. Call pg_iceberg_tracker_register_table() when a table is modified
 *   3. Call pg_iceberg_tracker_apply_updates_with_rebase() after each statement to
 *      snapshot state, accumulate new files atomically, and generate new metadata
 *   4. The xact callbacks handle commit/abort automatically
 *
 * IDENTIFICATION
 *    contrib/datalake_fdw/src/am_iceberg/include/pg_iceberg_metadata_tracker.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef __PG_ICEBERG_METADATA_TRACKER_H__
#define __PG_ICEBERG_METADATA_TRACKER_H__

#include "postgres.h"
#include "nodes/pg_list.h"
#include "nodes/value.h"

/*
 * TrackedDataFile - represents a data or delete file accumulated in the tracker.
 *
 * Inspired by pg_lake's TableMetadataOperation for DATA_FILE_ADD, but
 * simplified since we don't need partition support.
 *
 * In pg_lake, each file has path, content type, partition info, and stats.
 * We keep path and basic stats; content type is implicit based on which
 * array (data_files vs delete_files) the entry belongs to.
 */
typedef struct TrackedDataFile
{
	char   *file_path;		/* Remote file path (e.g., s3://bucket/file.parquet) */
	int64	record_count;	/* Number of records in the file */
	int64	file_size;		/* File size in bytes */
	char   *file_format;	/* File format: "parquet", "orc", "avro", etc.
							 * Defaults to "parquet" if NULL. */
	/*
	 * Iceberg partition tuple for this file: List of String (identity values
	 * in partition-spec order); a NULL list element is a SQL NULL value.
	 * NIL for unpartitioned tables.  Preserved across CAS rebase retries so
	 * the agent can stamp each data file with its partition.
	 */
	List   *partition_values;
} TrackedDataFile;

/*
 * LevelHistoryEntry - state snapshot for savepoint rollback support.
 *
 * Each entry captures the table's metadata state BEFORE a change was made
 * at a particular transaction nesting level, allowing us to restore the
 * state if that subtransaction is rolled back.
 *
 * The file counts (prev_data_files_count, prev_delete_files_count) allow
 * us to truncate the accumulated file Lists on rollback, matching the
 * Rust code's pattern: accumulated_data_files.truncate(prev_files_len).
 *
 * Example:
 *   Level 1: BEGIN; INSERT -> produces data_file_1, metadata_v1
 *   Level 2: SAVEPOINT sp1; INSERT -> produces data_file_2, metadata_v2
 *   ROLLBACK TO sp1;
 *     -> Pops level 2 entry: restores metadata to v1, truncates files to 1
 */
typedef struct LevelHistoryEntry
{
	int		nest_level;				/* nesting level where change occurred */
	char   *prev_metadata_location;	/* metadata location BEFORE this change */
	char   *prev_last_base;			/* last_base_metadata_location BEFORE change */
	int		prev_data_files_count;	/* data_files count BEFORE this change */
	int		prev_delete_files_count;/* delete_files count BEFORE this change */
} LevelHistoryEntry;

/*
 * TrackedMetadataFile - a metadata-LAYER file (metadata.json / manifest /
 * manifest-list avro) that the agent physically wrote to object storage for
 * this table during the transaction.  Recorded so a ROLLBACK can delete every
 * one of them and a COMMIT can drop the superseded (non-final) ones -- they
 * are otherwise orphaned because the tracker only keeps the LATEST metadata
 * location in memory (issue #399).
 *
 * NEVER a data/delete parquet: those are written by the C provider on the QE
 * and cleaned via the Class 1 PendingRelDelete path.  The agent reports only
 * metadata-layer files it newly wrote (filtered by snapshot id), so files
 * inherited from the pre-transaction base table are never listed here.
 */
typedef struct TrackedMetadataFile
{
	char   *path;					/* object-storage path of the file */
	char   *tree_metadata_location;	/* the metadata.json this file belongs to */
	int		nest_level;				/* txn nesting level when written */
} TrackedMetadataFile;

/*
 * TableMetadataState - per-table metadata tracking state.
 *
 * This is the hash table entry for each tracked Iceberg table.
 * The relid field MUST be first as it serves as the hash key.
 *
 * File accumulation follows the pg_lake IcebergSnapshotBuilder pattern:
 *   - data_files   ~= builder->dataEntries (CONTENT_DATA)
 *   - delete_files ~= builder->positionalDeleteEntries (CONTENT_POSITION_DELETES)
 * But simplified: no partition spec grouping (HTAB -> flat array).
 */
typedef struct TableMetadataState
{
	Oid		relid;					/* hash key - MUST be first field */
	Oid		namespace_oid;			/* namespace (schema) OID */
	Oid		rel_num;				/* RelFileNumber for the relation */

	/*
	 * The metadata location when this transaction FIRST accessed the table.
	 * Remains unchanged throughout the transaction. Used as the absolute
	 * base for determining if a rebase is needed.
	 */
	char   *initial_base_metadata_location;

	/*
	 * Current metadata location in this transaction, generated by our
	 * latest statement or rebase operation.
	 */
	char   *current_metadata_location;

	/*
	 * The global metadata location that current_metadata_location was
	 * based on. Used to optimize subsequent rebase checks: if the global
	 * hasn't moved since last_base, we can skip the rebase.
	 */
	char   *last_base_metadata_location;

	/* Transaction nesting level at which this table was first modified */
	int		first_modified_at_level;

	/* is_internal comes from IcebergMetadataInfo during register_table(). */
	bool	is_internal;

	/*
	 * Accumulated data files from all statements in this transaction.
	 * List of TrackedDataFile* (CONTENT_DATA in Iceberg terms).
	 * Grows via lappend; truncated via list_truncate on rollback.
	 */
	List   *data_files;

	/*
	 * Accumulated delete files from all statements in this transaction.
	 * List of TrackedDataFile* (CONTENT_POSITION_DELETES).
	 * Used by DELETE and UPDATE operations to mark rows for deletion.
	 */
	List   *delete_files;

	/*
	 * Stack of state snapshots for savepoint rollback support.
	 * List of LevelHistoryEntry*.
	 * Each entry captures metadata location + file counts BEFORE a change.
	 */
	List   *level_history;

	/*
	 * True when this table's catalog is the builtin catalog. Only builtin
	 * tables defer metadata materialization (issue #323): on non-builtin
	 * catalogs (Polaris/Hive) apply_updates_with_rebase commits to the
	 * external catalog inside the agent call, so per-statement eager
	 * behavior must be preserved there.
	 */
	bool	is_builtin_catalog;

	/*
	 * True when data_files/delete_files hold accumulated changes that are
	 * not yet reflected in current_metadata_location (deferred
	 * materialization). The expensive rebase + agent metadata generation is
	 * triggered lazily on the next scan (read-your-own-writes) or at commit,
	 * instead of after every statement, avoiding O(N^2) rewrites (issue #323).
	 */
	bool	dirty;

	/*
	 * issue #399: metadata-layer files the agent wrote for this table this
	 * transaction (List of TrackedMetadataFile*), plus cached credentials to
	 * delete them.  Only populated for builtin-catalog tables (the deletion
	 * queue and this cleanup only manage builtin files; external catalogs own
	 * their own file lifecycle).
	 *
	 * cleanup_fileio_config is the dlagent fileIOConfig JSON, built ONCE while
	 * the transaction is still healthy, so the ABORT callback can delete the
	 * orphaned files via /v1/files/delete without touching the catalog (which
	 * is unsafe during abort).  cleanup_volume_server / cleanup_volume_name /
	 * cleanup_owner feed the deletion-queue enqueue of superseded files on
	 * COMMIT.  All are allocated in the tracker memory context.
	 */
	List   *written_metadata_files;
	char   *cleanup_fileio_config;
	char   *cleanup_volume_server;
	char   *cleanup_volume_name;
	char   *cleanup_owner;
} TableMetadataState;


/* --- Public API --- */

/*
 * Initialize the metadata tracking system.
 * Registers xact/subxact callbacks. Call once during _PG_init.
 */
extern void pg_iceberg_init_metadata_tracking(void);

/*
 * Register a table for metadata tracking in the current transaction.
 * Reads the current metadata location from the catalog as the initial base.
 * No-op if the table is already registered.
 */
extern void pg_iceberg_tracker_register_table(Oid relid,
											  Oid namespace_oid,
											  Oid rel_num);

/*
 * Apply updates with rebase on the latest global metadata.
 *
 * This is the C equivalent of Rust's apply_updates_with_rebase().
 * It is the core rebase function called in THREE contexts:
 *
 * 1. PER-STATEMENT (process_new_data_files): after each DML, to
 *    rebase ALL accumulated files + new files on the latest global
 *    metadata and generate an intermediate metadata file.
 *
 * 2. AT COMMIT (tracker_commit_all): final rebase before catalog update,
 *    to incorporate any concurrent commits since the last DML.
 *    Called with empty new_*_files arrays.
 *
 * 3. AT SCAN (get_scan_metadata_location): for tracked tables, to
 *    rebase on the latest global so SELECTs see Read Committed state.
 *    Called with empty new_*_files arrays.
 *
 * Flow:
 *   1. Read latest global metadata from catalog
 *   2. Optimization: skip if global unchanged AND no new files
 *   3. Append new files to accumulated arrays
 *   4. Serialize ALL accumulated files to JSON data_locations
 *   5. Call pg_iceberg_modify_data_with_catalog() directly (agent call)
 *   6. Update tracker state (history, last_base, current_metadata)
 *
 * Returns: the new metadata location (owned by tracker, do NOT pfree).
 *          Returns NULL if there's nothing to rebase.
 */
extern char *pg_iceberg_tracker_apply_updates_with_rebase(
												Oid relid,
												const TrackedDataFile *new_data_files,
												int num_new_data_files,
												const TrackedDataFile *new_delete_files,
												int num_new_delete_files);

/*
 * Accumulate new files into the tracker WITHOUT materializing metadata.
 *
 * Cheap (O(new files)) per call: pushes a savepoint-rollback history entry
 * and appends the files to the accumulated lists, marking the table dirty.
 * The expensive rebase + agent metadata generation is deferred to the next
 * scan (read-your-own-writes) or to commit. Used for builtin-catalog tables
 * to avoid O(N^2) per-statement intermediate metadata rewrites (issue #323).
 */
extern void pg_iceberg_tracker_accumulate_files(
												Oid relid,
												const TrackedDataFile *new_data_files,
												int num_new_data_files,
												const TrackedDataFile *new_delete_files,
												int num_new_delete_files);

/*
 * Get the full TableMetadataState for a table (read-only access).
 * Returns NULL if the table is not tracked.
 */
extern TableMetadataState *pg_iceberg_tracker_get_table_state(Oid relid);

/*
 * Get the metadata location to use for scanning a table.
 *
 * This is the C equivalent of Rust's get_or_rebase_metadata_location().
 * It provides the correct metadata location for Read Committed isolation:
 *
 *   - If the table is tracked (modified in this transaction):
 *     calls apply_updates_with_rebase() with empty files to incorporate
 *     any concurrent commits from other transactions since our last DML.
 *     Returns the (potentially rebased) metadata location.
 *
 *   - If the table is NOT tracked (not modified in this transaction):
 *     reads the latest metadata location from the catalog, which
 *     includes committed changes from other transactions.
 *
 * Returns a palloc'd copy of the metadata location string.
 * The caller is responsible for pfree'ing it.
 * Returns NULL if no metadata location is available.
 */
extern char *pg_iceberg_tracker_get_scan_metadata_location(Oid relid);

/*
 * Parse a data_locations JSON string (as returned by the volume FDW)
 * into arrays of TrackedDataFile.
 *
 * The JSON format is:
 *   {"fragments": [{...}, ...]}            for INSERT
 *   {"updateFragments": [{...}, ...]}      for UPDATE/DELETE
 *
 * Each fragment object has: path, format, record_count,
 * file_size_in_bytes, position_on_delete.
 *
 * Files with position_on_delete == "POSITION_DELETE" go to delete_files;
 * all others go to data_files.
 *
 * Output arrays are palloc'd in CurrentMemoryContext. Caller may pass
 * them to pg_iceberg_tracker_apply_updates_with_rebase() which copies
 * them into the tracker's memory context.
 */
extern void pg_iceberg_parse_data_locations(const char *json,
											TrackedDataFile **data_files_out,
											int *num_data_files_out,
											TrackedDataFile **delete_files_out,
											int *num_delete_files_out);

#endif /* __PG_ICEBERG_METADATA_TRACKER_H__ */
