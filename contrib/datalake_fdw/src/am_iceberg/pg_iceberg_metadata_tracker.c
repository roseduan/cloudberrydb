/*-------------------------------------------------------------------------
 *
 * pg_iceberg_metadata_tracker.c
 *    Transaction-level metadata location tracker for Iceberg tables.
 *
 * This module implements optimistic concurrency control for Iceberg
 * metadata by tracking metadata location changes and accumulated
 * data/delete files within a PostgreSQL transaction.
 *
 * Architecture:
 *   - A process-local HTAB maps table OID -> TableMetadataState
 *   - All tracker memory lives in a dedicated MemoryContext
 *   - RegisterXactCallback handles top-level commit/abort
 *   - RegisterSubXactCallback handles savepoint commit/abort
 *
 * File Accumulation (inspired by pg_lake's IcebergSnapshotBuilder):
 *   - data_files:   List of TrackedDataFile* (CONTENT_DATA)
 *   - delete_files: List of TrackedDataFile* (CONTENT_POSITION_DELETES)
 *   Each apply_updates_with_rebase() atomically saves file counts to history AND
 *   appends new files, ensuring correct rollback on SAVEPOINT abort.
 *
 * Per-Statement Flow (via apply_updates_with_rebase):
 *   1. DML handler calls register_table() for the target table
 *   2. DML produces data files and/or delete files (TrackedDataFile)
 *   3. DML handler calls apply_updates_with_rebase(relid, new_files)
 *      which internally:
 *      a. Reads latest global metadata from catalog
 *      b. Optimization: skips if global unchanged AND no new files
 *      c. Appends new files to accumulated Lists
 *      d. Serializes ALL accumulated files to JSON data_locations
 *      e. Calls pg_iceberg_modify_data_with_catalog() (agent) directly
 *      f. Updates tracker state (history, last_base, current_metadata)
 *   4. Subsequent SELECTs call get_scan_metadata_location() which
 *      triggers a rebase if needed (for Read Committed isolation)
 *
 * Lifecycle:
 *   1. _PG_init: pg_iceberg_init_metadata_tracking() registers callbacks
 *   2. First DML: ensure_tracker_initialized() creates HTAB + MemoryContext
 *   3. Each DML: apply_updates_with_rebase() calls agent + records state
 *   4. COMMIT: PRE_COMMIT does final rebase + catalog update
 *   5. COMMIT/ABORT: callback destroys tracker and frees memory
 *
 * Memory Management:
 *   All allocations (strings, file paths, Lists) go into tracker_context.
 *   On transaction end, MemoryContextDelete bulk-frees everything.
 *   Individual pfree() calls are used only during rollback to avoid
 *   unbounded memory growth in long transactions with many savepoints.
 *
 * IDENTIFICATION
 *    contrib/datalake_fdw/src/am_iceberg/pg_iceberg_metadata_tracker.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/table.h"
#include "access/xact.h"
#include "common/jsonapi.h"
#include "lib/stringinfo.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/json.h"
#include "utils/jsonfuncs.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#include "include/pg_iceberg_metadata_tracker.h"
#include "include/pg_iceberg_metadata.h"
#include "include/pg_iceberg_catalog.h"

/* Initial number of hash table entries */
#define TRACKER_HASH_INITIAL_SIZE 16

/* Max CAS retries for concurrent commits during tracker_commit_all */
#define TRACKER_MAX_COMMIT_RETRIES 10

/* --- Static Globals --- */

/*
 * The active hash table for the current transaction.
 * NULL when no tables are being tracked (i.e., no active tracker).
 */
static HTAB *tracker_table = NULL;

/*
 * Dedicated memory context for all tracker allocations.
 * Created as a child of TopMemoryContext so it survives across
 * statements within a transaction but is explicitly destroyed
 * on commit/abort via our xact callbacks.
 */
static MemoryContext tracker_context = NULL;

/*
 * Whether xact callbacks have been registered for this session.
 * Registration happens once in pg_iceberg_init_metadata_tracking()
 * and persists for the lifetime of the backend process.
 */
static bool xact_callbacks_registered = false;

/* --- Forward Declarations --- */

static void metadata_tracker_xact_callback(XactEvent event, void *arg);
static void metadata_tracker_subxact_callback(SubXactEvent event,
											  SubTransactionId mySubid,
											  SubTransactionId parentSubid,
											  void *arg);
static void ensure_tracker_initialized(void);
static void destroy_tracker(void);
static void tracker_commit_all(void);
static void tracker_rollback_to_level(int target_level);
static void table_state_init(TableMetadataState *state);
static void table_state_rollback_to_level(TableMetadataState *state,
										  int target_level);
static TrackedDataFile *copy_tracked_file(const TrackedDataFile *src);
static void free_tracked_file(TrackedDataFile *file);
static List *append_files_to_list(List *file_list,
								  const TrackedDataFile *new_files,
								  int num_new_files);
static List *truncate_file_list(List *file_list, int target_len);
static char *serialize_files_to_data_locations(List *data_files,
											   List *delete_files,
											   CmdType *cmd_out);
static const char *tracker_commit_external_table(TableMetadataState *state,
												 IcebergTableInfo *table_info,
												 const char *final_metadata_location);
static void tracker_sync_local_metadata_after_external_commit(
	TableMetadataState *state,
	IcebergTableInfo *table_info);


/* ================================================================
 *                     Helper: File List Operations
 * ================================================================
 */

/*
 * copy_tracked_file
 *    Palloc a copy of a TrackedDataFile, pstrdup'ing strings.
 *    Caller should be in the appropriate MemoryContext.
 */
static TrackedDataFile *
copy_tracked_file(const TrackedDataFile *src)
{
	TrackedDataFile *dst = (TrackedDataFile *) palloc(sizeof(TrackedDataFile));

	dst->file_path = pstrdup(src->file_path);
	dst->record_count = src->record_count;
	dst->file_size = src->file_size;
	dst->file_format = src->file_format ? pstrdup(src->file_format) : NULL;

	return dst;
}

/*
 * free_tracked_file
 *    Free a TrackedDataFile and its owned strings.
 */
static void
free_tracked_file(TrackedDataFile *file)
{
	if (file->file_path)
		pfree(file->file_path);
	if (file->file_format)
		pfree(file->file_format);
	pfree(file);
}

/*
 * append_files_to_list
 *    Append an array of TrackedDataFile entries to a List.
 *    Each entry is palloc-copied into the current MemoryContext.
 *    Returns the (possibly new) list head.
 */
static List *
append_files_to_list(List *file_list,
					 const TrackedDataFile *new_files,
					 int num_new_files)
{
	int		i;

	for (i = 0; i < num_new_files; i++)
		file_list = lappend(file_list, copy_tracked_file(&new_files[i]));

	return file_list;
}

/*
 * truncate_file_list
 *    Truncate a List of TrackedDataFile* to target_len entries.
 *    Frees the removed TrackedDataFile entries and their strings.
 *    Returns the truncated list.
 */
static List *
truncate_file_list(List *file_list, int target_len)
{
	while (list_length(file_list) > target_len)
	{
		TrackedDataFile *f = (TrackedDataFile *) llast(file_list);

		free_tracked_file(f);
		file_list = list_truncate(file_list,
								  list_length(file_list) - 1);
	}

	return file_list;
}


/* ================================================================
 *                  TableMetadataState Operations
 * ================================================================
 */

/*
 * table_state_init
 *    Initialize a newly-created hash table entry to default values.
 *
 * Called immediately after hash_search(HASH_ENTER). The relid field
 * is already set by the hash table machinery (it's the key).
 */
static void
table_state_init(TableMetadataState *state)
{
	state->namespace_oid = InvalidOid;
	state->rel_num = InvalidOid;
	state->initial_base_metadata_location = NULL;
	state->current_metadata_location = NULL;
	state->last_base_metadata_location = NULL;
	state->first_modified_at_level = 0;
	state->is_internal = false;
	state->is_builtin_catalog = false;
	state->dirty = false;
	state->data_files = NIL;
	state->delete_files = NIL;
	state->level_history = NIL;
}

/*
 * table_state_rollback_to_level
 *    Undo all changes at or above the target nesting level.
 *
 * Pops history entries from the stack, restoring:
 *   - current_metadata_location
 *   - last_base_metadata_location
 *   - data_files (truncated to saved count)
 *   - delete_files (truncated to saved count)
 *
 * This matches the Rust rollback_to_level() behavior:
 *   self.current_metadata_location = prev_meta;
 *   self.accumulated_data_files.truncate(prev_files_len);
 *   self.last_base_metadata_location = prev_base;
 *
 * Memory: Ownership of prev_metadata_location and prev_last_base
 * transfers from history entries back to the state fields.
 * Truncated file paths are pfree'd to reclaim memory.
 */
static void
table_state_rollback_to_level(TableMetadataState *state, int target_level)
{
	while (list_length(state->level_history) > 0)
	{
		LevelHistoryEntry *entry;

		entry = (LevelHistoryEntry *) llast(state->level_history);

		if (entry->nest_level < target_level)
			break;

		/*
		 * Restore metadata location from history.
		 * Ownership of prev_metadata_location transfers to state.
		 */
		if (state->current_metadata_location)
			pfree(state->current_metadata_location);
		state->current_metadata_location = entry->prev_metadata_location;

		if (state->last_base_metadata_location)
			pfree(state->last_base_metadata_location);
		state->last_base_metadata_location = entry->prev_last_base;

		/*
		 * Truncate file lists to the counts saved in history.
		 * This undoes all file accumulations at this nesting level.
		 */
		state->data_files = truncate_file_list(state->data_files,
											   entry->prev_data_files_count);
		state->delete_files = truncate_file_list(state->delete_files,
												 entry->prev_delete_files_count);

		/*
		 * The accumulated file set changed. Only builtin-catalog tables defer
		 * materialization (issue #323): there current_metadata_location may no
		 * longer reflect the truncated list, so force a re-materialize on the
		 * next scan/commit.
		 *
		 * Non-builtin catalogs (Polaris/Hive) take the eager per-statement path
		 * where apply_updates_with_rebase keeps current_metadata_location in
		 * sync with the accumulated files at all times; the restored location
		 * therefore already matches the truncated list. Forcing dirty there
		 * would defeat the commit-time skip optimisation and make
		 * apply_updates_with_rebase re-call the agent (which commits a snapshot
		 * to the external catalog) on top of the subsequent
		 * tracker_commit_external_table call -- a duplicate snapshot.
		 */
		if (state->is_builtin_catalog)
			state->dirty = true;

		/* Pop this history entry (don't pfree prev strings - transferred above) */
		pfree(entry);
		state->level_history = list_truncate(state->level_history,
											 list_length(state->level_history) - 1);
	}
}


/* ================================================================
 *                     Tracker Lifecycle
 * ================================================================
 */

/*
 * ensure_tracker_initialized
 *    Lazily create the tracker hash table and memory context.
 *
 * Called on first use within a transaction (when a table is registered).
 * Subsequent calls within the same transaction are no-ops.
 *
 * The memory context is created as a child of TopMemoryContext because
 * we manage its lifecycle explicitly in the xact callbacks. This gives
 * us precise control over when the memory is freed (rather than relying
 * on TopTransactionContext which may be freed at unpredictable times).
 */
static void
ensure_tracker_initialized(void)
{
	HASHCTL		hash_ctl;

	if (tracker_table != NULL)
		return;		/* Already initialized for this transaction */

	/*
	 * Create a dedicated memory context for all tracker allocations.
	 * We use TopMemoryContext as parent and manage lifecycle explicitly
	 * in the xact callbacks (destroy on commit/abort).
	 */
	tracker_context = AllocSetContextCreate(TopMemoryContext,
											"Iceberg Metadata Tracker",
											ALLOCSET_DEFAULT_SIZES);

	/*
	 * Create the hash table within the tracker memory context.
	 * Key: Oid (relid), Entry: TableMetadataState
	 * HASH_BLOBS: key is a fixed-size binary blob (Oid = uint32)
	 */
	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(Oid);
	hash_ctl.entrysize = sizeof(TableMetadataState);
	hash_ctl.hcxt = tracker_context;

	tracker_table = hash_create("Iceberg Metadata Tracker Table",
								TRACKER_HASH_INITIAL_SIZE,
								&hash_ctl,
								HASH_CONTEXT | HASH_ELEM | HASH_BLOBS);
}

/*
 * destroy_tracker
 *    Destroy the tracker and free all associated memory.
 *
 * Deleting the memory context bulk-frees everything: the hash table,
 * all TableMetadataState entries, all pstrdup'd strings, all file path
 * strings, and all List cells. This is much more efficient than freeing
 * each allocation individually.
 */
static void
destroy_tracker(void)
{
	if (tracker_context != NULL)
	{
		MemoryContextDelete(tracker_context);
		tracker_context = NULL;
	}

	/*
	 * The HTAB was allocated within tracker_context, so it's already freed.
	 * Just clear the pointer.
	 */
	tracker_table = NULL;
}


/* ================================================================
 *                    Commit / Rollback Logic
 * ================================================================
 */

/*
 * table_has_pending_changes
 *    Check if a table has any pending changes that need to be committed.
 *
 * A table has pending changes if either:
 *   (a) Its metadata location has changed from the initial base, OR
 *   (b) It has accumulated data files or delete files
 *
 * Case (a) covers the per-statement metadata generation pattern.
 * Case (b) covers the batch-commit pattern where files are accumulated
 * during the transaction and metadata is generated at commit time.
 */
static bool
table_has_pending_changes(const TableMetadataState *state)
{
	/*
	 * No pending changes only when there is neither a metadata location nor
	 * any accumulated files. Under deferred materialization (issue #323) files
	 * are accumulated without updating current_metadata_location, so the file
	 * counts must be checked before concluding there is nothing to commit --
	 * otherwise a table could be silently skipped at PRE_COMMIT and its
	 * accumulated data discarded. Checking the counts first also keeps the
	 * strcmp below from dereferencing a NULL current_metadata_location.
	 */
	if (state->current_metadata_location == NULL &&
		list_length(state->data_files) == 0 &&
		list_length(state->delete_files) == 0)
		return false;

	/* Has accumulated files? */
	if (list_length(state->data_files) > 0 ||
		list_length(state->delete_files) > 0)
		return true;

	/*
	 * Has metadata location changed?
	 * If initial was NULL but current is set, it's a new table modification.
	 */
	if (state->initial_base_metadata_location == NULL ||
		strcmp(state->current_metadata_location,
			   state->initial_base_metadata_location) != 0)
		return true;

	return false;
}

/*
 * Commit accumulated files to external catalog using commitAppend/commitUpdate.
 */
static const char *
tracker_commit_external_table(TableMetadataState *state,
							  IcebergTableInfo *table_info,
							  const char *final_metadata_location)
{
	CmdType		commit_cmd_type;
	char	   *data_locations_json;
	const char *committed_metadata_location;
	Relation	rel = NULL;

	data_locations_json = serialize_files_to_data_locations(state->data_files,
															state->delete_files,
															&commit_cmd_type);
	if (data_locations_json == NULL)
		return NULL;

	/*
	 * For internal tables on a non-builtin catalog (Polaris/Hive used as
	 * iceberg_default_catalog), table_info->opts->table is NULL because the
	 * table was created without explicit OPTIONS. The commit endpoint then
	 * needs the relation to derive namespace + table name. External tables
	 * with explicit OPTIONS keep the existing path and never open the rel.
	 */
	if (table_info->opts == NULL || table_info->opts->table == NULL)
		rel = table_open(state->relid, AccessShareLock);

	committed_metadata_location = pg_iceberg_commit_data_with_catalog(
		rel,
		table_info,
		data_locations_json,
		final_metadata_location,
		commit_cmd_type);

	if (rel != NULL)
		table_close(rel, AccessShareLock);

	pfree(data_locations_json);

	return committed_metadata_location;
}

/*
 * One-shot local metadata sync after an external commit succeeded.
 *
 * Local pg_iceberg_metadata is a mirror of the external catalog head for
 * external tables. If local CAS fails after external commit, we must not
 * retry external commit (that can duplicate commits). Instead, re-read the
 * current external head and update local metadata only.
 */
static void
tracker_sync_local_metadata_after_external_commit(TableMetadataState *state,
												  IcebergTableInfo *table_info)
{
	char   *external_head;
	bool	synced;

	external_head = pg_iceberg_get_latest_metadata_location(state->relid,
															table_info);
	synced = pg_iceberg_update_metadata_cas(state->relid,
											external_head,
											NULL);

	if (synced)
		elog(WARNING,
			 "metadata tracker: synchronized local metadata to external "
			 "head for table %u after external commit, head: %s",
			 state->relid,
			 external_head);
	else
		elog(WARNING,
			 "metadata tracker: external commit succeeded but failed to sync "
			 "local metadata for table %u, external head: %s",
			 state->relid,
			 external_head);

	pfree(external_head);
}

/*
 * tracker_commit_all
 *    Commit all tracked metadata changes at transaction end.
 *
 * Called during XACT_EVENT_PRE_COMMIT. For each tracked table with
 * pending changes:
 *
 * 1. Internal catalog tables:
 *    - Rebase all accumulated files on the latest global metadata
 *    - Attempt CAS update on local pg_iceberg_metadata
 *    - Retry on CAS conflict
 *
 * 2. External catalog tables:
 *    - Rebase once and commit once to external catalog
 *    - Update local metadata mirror
 *    - If local CAS fails, sync local mirror only (no external re-commit)
 *
 * This implements Iceberg's standard optimistic concurrency control,
 * matching the Rust commit_all() pattern where catalog updates use
 * CAS to detect and handle concurrent modifications.
 *
 * Internal tables keep CAS retry behavior and may ERROR on retry exhaustion.
 * External tables avoid external re-commit loops to prevent duplicate commits.
 */
static void
tracker_commit_all(void)
{
	HASH_SEQ_STATUS status;
	TableMetadataState *state;

	if (tracker_table == NULL)
		return;

	hash_seq_init(&status, tracker_table);

	while ((state = (TableMetadataState *) hash_seq_search(&status)) != NULL)
	{
		const char *final_metadata_location = NULL;
		IcebergTableInfo *table_info = NULL;
		int			retries = 0;
		bool		external_committed = false;
		bool		non_builtin_catalog = false;

		/* Skip tables with no pending changes */
		if (!table_has_pending_changes(state))
			continue;

		/*
		 * Look up table_info whenever we need to know which catalog backs
		 * this table. We need it for two cases now:
		 *   - is_internal=false: external table (existing logic)
		 *   - is_internal=true with a non-builtin catalog (Polaris/Hive used
		 *     as the engine's default catalog): apply_updates_with_rebase
		 *     calls the agent's append endpoint, which on non-builtin
		 *     catalogs commits to the catalog atomically. Re-running rebase
		 *     on a CAS retry would re-append the same parquet files and
		 *     duplicate the snapshot.
		 */
		table_info = pg_iceberg_get_table_info(state->relid);
		non_builtin_catalog = !pg_iceberg_is_builtin_catalog(
			table_info->catalog_server_name);

		/*
		 * CAS commit loop: rebase + conditional catalog update.
		 *
		 * Internal tables on builtin catalog keep full CAS-retry behavior:
		 *   apply_updates_with_rebase only writes a fresh metadata.json,
		 *   the CAS happens locally on pg_iceberg_metadata, and retries are
		 *   safe because no catalog state has been advanced yet.
		 *
		 * Tables on non-builtin catalogs (whether is_internal or not) must
		 * not retry the agent call: by the time apply_updates_with_rebase
		 * (or tracker_commit_external_table) returns successfully, the
		 * external catalog (Polaris/Hive) has already committed the new
		 * snapshot. Any further retry would duplicate appends.
		 */
		for (;;)
		{
			bool cas_succeeded = false;

			final_metadata_location = pg_iceberg_tracker_apply_updates_with_rebase(
				state->relid, NULL, 0, NULL, 0);

			/*
			 * Non-builtin catalogs (Polaris/Hive) own the current-metadata
			 * pointer via their own atomic update. Commit through the
			 * dedicated commitAppend endpoint exactly once per pre-commit
			 * pass: subsequent local-CAS retries must NOT re-call the agent
			 * or each retry would re-append the same parquet files into a
			 * fresh snapshot. This applies regardless of is_internal: the
			 * "explicit OPTIONS" external table path and the
			 * "iceberg_default_catalog points at non-builtin" internal-table
			 * path both go through here.
			 */
			if (non_builtin_catalog && !external_committed)
			{
				final_metadata_location = tracker_commit_external_table(
					state, table_info, state->last_base_metadata_location);
				external_committed = true;
			}

			/*
			 * Update local metadata catalog (CAS semantics).
			 */
			cas_succeeded = pg_iceberg_update_metadata_cas(
					state->relid,
					final_metadata_location,
					state->last_base_metadata_location);

			if (cas_succeeded)
			{
				elog(DEBUG1,
					 "metadata tracker: committed table %u, location: %s, "
					 "data_files: %d, delete_files: %d",
					 state->relid,
					 final_metadata_location,
					 list_length(state->data_files),
					 list_length(state->delete_files));
				break;	/* CAS succeeded */
			}

			/*
			 * Non-builtin catalog path: the external commit already succeeded
			 * once. Do NOT re-call the agent; only repair the local mirror.
			 */
			if (non_builtin_catalog && external_committed)
			{
				tracker_sync_local_metadata_after_external_commit(state,
																  table_info);
				break;
			}

			/* Internal table CAS failed: another transaction committed concurrently */
			if (++retries > TRACKER_MAX_COMMIT_RETRIES)
				ereport(ERROR,
						(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
						 errmsg("failed to commit iceberg metadata for "
								"table %u after %d retries due to "
								"concurrent updates",
								state->relid, retries)));

			elog(WARNING,
				 "metadata tracker: concurrent update detected for "
				 "table %u, rebasing (attempt %d/%d)",
				 state->relid, retries, TRACKER_MAX_COMMIT_RETRIES);

			/*
			 * Force the next pg_iceberg_tracker_apply_updates_with_rebase()
			 * to actually re-read the latest pg_iceberg_metadata.metadata_location
			 * from the catalog rather than reuse a stale snapshot. Two pieces
			 * are required:
			 *
			 *   1. Bump the command counter so any update we performed in
			 *      this transaction (or any committed concurrent xact whose
			 *      results are not yet visible to our catalog snapshot) is
			 *      visible to a subsequent systable_beginscan().
			 *   2. Invalidate the catalog snapshot so the next read picks
			 *      up the freshly-committed concurrent winner instead of
			 *      our cached "initial" view of pg_iceberg_metadata.
			 *
			 * Without these calls, apply_updates_with_rebase() reads the
			 * same stale latest_global_location on every retry, and its
			 * fast-path optimisation (last_base == latest && no new files)
			 * returns the un-rebased current_metadata_location. The CAS
			 * then loops forever or — under the wait=true heap_update path
			 * inside pg_iceberg_update_metadata_cas() — silently overwrites
			 * the concurrent winner's metadata_location, orphaning the
			 * loser's data files. Manifests as ~25% data loss on
			 * concurrent INSERTs against the same iceberg table.
			 */
			CommandCounterIncrement();
			InvalidateCatalogSnapshot();

			/*
			 * Drop our cached last_base so the rebase optimisation cannot
			 * trigger. The next apply_updates_with_rebase() will go through
			 * the full agent path with the freshly-read latest_global,
			 * picking up the concurrent winner's snapshot before we layer
			 * our accumulated data_files on top.
			 */
			if (state->last_base_metadata_location)
			{
				pfree(state->last_base_metadata_location);
				state->last_base_metadata_location = NULL;
			}
		}

		if (table_info != NULL)
			pg_iceberg_free_table_info(table_info);
	}
}

/*
 * tracker_rollback_to_level
 *    Rollback all tracked tables to a specific nesting level.
 *
 * This performs two cleanup operations for subtransaction abort:
 *
 * 1. REMOVE tables that were first modified at or above target_level.
 *    These tables are entirely new to this subtransaction, so we
 *    discard them completely.
 *
 * 2. ROLLBACK remaining tables' histories to undo changes made at
 *    or above target_level. This restores both metadata locations
 *    and file accumulations to their pre-subtransaction state.
 *
 * Example:
 *   BEGIN;
 *     INSERT INTO t1 ...;           (level 1, produces f1.parquet)
 *     SAVEPOINT sp1;                (level 2)
 *       INSERT INTO t2 ...;         (level 2, produces f2.parquet)
 *       INSERT INTO t1 ...;         (level 2, produces f3.parquet)
 *     ROLLBACK TO sp1;
 *       -> t2 removed entirely (first_modified_at_level=2 >= 2)
 *       -> t1: metadata + files rolled back (only f1.parquet remains)
 */
static void
tracker_rollback_to_level(int target_level)
{
	HASH_SEQ_STATUS status;
	TableMetadataState *state;

	if (tracker_table == NULL)
		return;

	hash_seq_init(&status, tracker_table);

	while ((state = (TableMetadataState *) hash_seq_search(&status)) != NULL)
	{
		if (state->first_modified_at_level >= target_level)
		{
			/*
			 * It is safe in PostgreSQL's dynahash implementation to remove the
			 * current entry during a hash_seq_search iteration.
			 */
			hash_search(tracker_table, &state->relid, HASH_REMOVE, NULL);
		}
		else
		{
			/*
			 * This table existed before this subtransaction, but may have
			 * additional changes (metadata + files) that need to be rolled back.
			 */
			table_state_rollback_to_level(state, target_level);
		}
	}
}


/* ================================================================
 *                       Xact Callbacks
 * ================================================================
 */

/*
 * metadata_tracker_xact_callback
 *    Top-level transaction callback for metadata tracking.
 *
 * Handles the full transaction lifecycle:
 *   PRE_COMMIT: Persist all tracked metadata changes to the catalog.
 *               If this fails, the transaction will be aborted.
 *   COMMIT:     Clean up the tracker (all changes are already persisted).
 *   ABORT:      Discard the tracker (catalog remains unchanged).
 */
static void
metadata_tracker_xact_callback(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_PRE_COMMIT:
			/*
			 * Pre-commit: flush all tracked metadata changes to catalog.
			 * Errors here will cause the transaction to abort, which is
			 * the correct behavior (metadata changes are not persisted).
			 */
			tracker_commit_all();
			break;

		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PARALLEL_COMMIT:
			/*
			 * Commit complete: destroy the tracker.
			 * All catalog updates were already done in PRE_COMMIT.
			 */
			destroy_tracker();
			break;

		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:
			/*
			 * Transaction aborted: discard all tracked changes.
			 * No catalog updates are made; the catalog retains the
			 * original metadata locations.
			 */
			destroy_tracker();
			break;

		case XACT_EVENT_PREPARE:
		case XACT_EVENT_PRE_PREPARE:
		case XACT_EVENT_PARALLEL_PRE_COMMIT:
			/* No special handling needed for these events */
			break;
	}
}

/*
 * metadata_tracker_subxact_callback
 *    Subtransaction callback for metadata tracking.
 *
 * Handles savepoint lifecycle:
 *   COMMIT_SUB: No action needed. Changes remain in the tracker and
 *               will be committed with the top-level transaction.
 *   ABORT_SUB:  Rollback changes made at or above the aborting level.
 *               Tables first modified at this level are removed entirely.
 *               Other tables have their metadata + files rolled back.
 */
static void
metadata_tracker_subxact_callback(SubXactEvent event,
								  SubTransactionId mySubid,
								  SubTransactionId parentSubid,
								  void *arg)
{
	int		nest_level;

	/* Early exit if no tracker is active */
	if (tracker_table == NULL)
		return;

	switch (event)
	{
		case SUBXACT_EVENT_COMMIT_SUB:
			/*
			 * Subtransaction committed: promote changes to parent level.
			 *
			 * The tracked changes (metadata + accumulated files) remain
			 * in the tracker. They'll be committed when the top-level
			 * transaction commits. No explicit action needed here.
			 */
			break;

		case SUBXACT_EVENT_ABORT_SUB:
			/*
			 * Subtransaction aborted: rollback changes at this level.
			 *
			 * GetCurrentTransactionNestLevel() still returns the level
			 * of the aborting subtransaction at this point in the
			 * abort processing.
			 */
			nest_level = GetCurrentTransactionNestLevel();
			tracker_rollback_to_level(nest_level);
			break;

		case SUBXACT_EVENT_START_SUB:
		case SUBXACT_EVENT_PRE_COMMIT_SUB:
			/* No action needed for these events */
			break;
	}
}


/* ================================================================
 *                         Public API
 * ================================================================
 */

/*
 * pg_iceberg_init_metadata_tracking
 *    Register xact callbacks for metadata tracking.
 *
 * Should be called once during extension initialization (_PG_init).
 * The callbacks persist for the lifetime of the backend process and
 * are invoked for every transaction, but they're no-ops when no
 * tables are being tracked (tracker_table == NULL).
 */
void
pg_iceberg_init_metadata_tracking(void)
{
	if (xact_callbacks_registered)
		return;

	RegisterXactCallback(metadata_tracker_xact_callback, NULL);
	RegisterSubXactCallback(metadata_tracker_subxact_callback, NULL);

	xact_callbacks_registered = true;
}

/*
 * pg_iceberg_tracker_register_table
 *    Register a table for metadata tracking in the current transaction.
 *
 * Reads the current metadata location and table mode from the catalog
 * layer and uses them as the initial base state.
 * If the table is already registered, this is a no-op.
 *
 * The table state is allocated in the tracker's memory context and
 * will be automatically freed when the transaction ends.
 */
void
pg_iceberg_tracker_register_table(Oid relid,
								  Oid namespace_oid,
								  Oid rel_num)
{
	TableMetadataState *state;
	bool				found;
	char			   *latest_metadata_location;
	bool				is_internal;
	int					nest_level;
	MemoryContext		oldcxt;

	ensure_tracker_initialized();

	/* Check if already registered (no-op) */
	state = (TableMetadataState *)
		hash_search(tracker_table, &relid, HASH_FIND, NULL);
	if (state != NULL)
		return;

	nest_level = GetCurrentTransactionNestLevel();

	/*
	 * Read current metadata location and table mode (internal/external) from
	 * catalog layer.
	 */
	latest_metadata_location =
		pg_iceberg_get_latest_metadata_and_mode(relid, &is_internal);

	/* Create a new hash table entry */
	state = (TableMetadataState *)
		hash_search(tracker_table, &relid, HASH_ENTER, &found);
	Assert(!found);

	/* Initialize all fields to defaults */
	table_state_init(state);
	state->relid = relid;
	state->namespace_oid = namespace_oid;
	state->rel_num = rel_num;
	state->first_modified_at_level = nest_level;

	/*
	 * Copy metadata location strings into our tracker memory context.
	 * These become the initial/current/last_base values for this table.
	 */
	oldcxt = MemoryContextSwitchTo(tracker_context);

	if (latest_metadata_location)
	{
		state->initial_base_metadata_location =
			pstrdup(latest_metadata_location);
		state->current_metadata_location =
			pstrdup(latest_metadata_location);
		state->last_base_metadata_location =
			pstrdup(latest_metadata_location);
	}

	state->is_internal = is_internal;

	MemoryContextSwitchTo(oldcxt);

	/*
	 * Determine whether this table is backed by the builtin catalog, using
	 * the same predicate as the commit path. Only builtin tables defer
	 * metadata materialization (issue #323); non-builtin catalogs commit to
	 * the external catalog inside apply_updates_with_rebase and must stay
	 * eager. Default to false (eager) if the catalog cannot be determined.
	 */
	{
		IcebergTableInfo *reg_table_info = pg_iceberg_get_table_info(relid);

		state->is_builtin_catalog =
			(reg_table_info != NULL &&
			 reg_table_info->catalog_server_name != NULL)
			? pg_iceberg_is_builtin_catalog(reg_table_info->catalog_server_name)
			: false;

		if (reg_table_info != NULL)
			pg_iceberg_free_table_info(reg_table_info);
	}

	if (latest_metadata_location)
		pfree(latest_metadata_location);

	elog(DEBUG1,
		 "metadata tracker: registered table %u at nest level %d, base: %s",
		 relid, nest_level,
		 state->initial_base_metadata_location
		 ? state->initial_base_metadata_location
		 : "(null)");
}


/*
 * pg_iceberg_tracker_get_table_state
 *    Get the full TableMetadataState for a table.
 *
 * Provides read-only access to the complete state for advanced
 * use cases (e.g., inspecting level_history or first_modified_at_level).
 * Returns NULL if the table is not tracked.
 */
TableMetadataState *
pg_iceberg_tracker_get_table_state(Oid relid)
{
	if (tracker_table == NULL)
		return NULL;

	return (TableMetadataState *)
		hash_search(tracker_table, &relid, HASH_FIND, NULL);
}


/* ================================================================
 *            Data Locations JSON Parsing (Volume FDW → Tracker)
 * ================================================================
 */

/*
 * SAX-style parsing state for data_locations JSON produced by the volume FDW.
 *
 * JSON schema:
 *   {"fragments": [ {fragment}, ... ]}           -- INSERT
 *   {"updateFragments": [ {fragment}, ... ]}     -- UPDATE / DELETE
 *
 * fragment := {
 *     "path": <string>,
 *     "format": <string>,                        -- "parquet", "orc", ...
 *     "record_count": <number>,
 *     "file_size_in_bytes": <number>,
 *     "position_on_delete": "DATA_FILE" | "POSITION_DELETE"
 * }
 */

typedef enum
{
	DL_FIELD_NONE = 0,
	DL_FIELD_PATH,
	DL_FIELD_FORMAT,
	DL_FIELD_RECORD_COUNT,
	DL_FIELD_FILE_SIZE,
	DL_FIELD_POSITION_ON_DELETE
} DLFieldType;

typedef struct
{
	DLFieldType	current_field;

	int			object_depth;
	int			array_depth;

	bool		next_array_is_fragments;
	bool		in_fragments_array;
	int			fragments_array_depth;

	bool		in_fragment_object;
	int			fragment_object_depth;

	/* Fields of the fragment currently being parsed */
	char	   *cur_path;
	char	   *cur_format;
	int64		cur_record_count;
	int64		cur_file_size;
	bool		cur_is_delete;

	/* Accumulated output (List of TrackedDataFile*) */
	List	   *data_files;
	List	   *delete_files;
} DLParseState;

static void
dl_object_start(void *semstate)
{
	DLParseState *s = (DLParseState *) semstate;

	s->object_depth++;

	if (s->in_fragments_array && !s->in_fragment_object)
	{
		s->in_fragment_object = true;
		s->fragment_object_depth = s->object_depth;
		s->cur_path = NULL;
		s->cur_format = NULL;
		s->cur_record_count = 0;
		s->cur_file_size = 0;
		s->cur_is_delete = false;
	}
}

static void
dl_object_end(void *semstate)
{
	DLParseState *s = (DLParseState *) semstate;

	if (s->in_fragment_object &&
		s->object_depth == s->fragment_object_depth)
	{
		TrackedDataFile *file = (TrackedDataFile *) palloc0(sizeof(TrackedDataFile));

		file->file_path = s->cur_path;
		file->file_format = s->cur_format;
		file->record_count = s->cur_record_count;
		file->file_size = s->cur_file_size;

		if (s->cur_is_delete)
			s->delete_files = lappend(s->delete_files, file);
		else
			s->data_files = lappend(s->data_files, file);

		s->in_fragment_object = false;
	}

	s->object_depth--;
}

static void
dl_array_start(void *semstate)
{
	DLParseState *s = (DLParseState *) semstate;

	s->array_depth++;

	if (s->next_array_is_fragments)
	{
		s->in_fragments_array = true;
		s->fragments_array_depth = s->array_depth;
		s->next_array_is_fragments = false;
	}
}

static void
dl_array_end(void *semstate)
{
	DLParseState *s = (DLParseState *) semstate;

	if (s->in_fragments_array &&
		s->array_depth == s->fragments_array_depth)
		s->in_fragments_array = false;

	s->array_depth--;
}

static void
dl_field_start(void *semstate, char *fname, bool isnull)
{
	DLParseState *s = (DLParseState *) semstate;

	if (s->in_fragment_object)
	{
		if (strcmp(fname, "path") == 0)
			s->current_field = DL_FIELD_PATH;
		else if (strcmp(fname, "format") == 0)
			s->current_field = DL_FIELD_FORMAT;
		else if (strcmp(fname, "record_count") == 0)
			s->current_field = DL_FIELD_RECORD_COUNT;
		else if (strcmp(fname, "file_size_in_bytes") == 0)
			s->current_field = DL_FIELD_FILE_SIZE;
		else if (strcmp(fname, "position_on_delete") == 0)
			s->current_field = DL_FIELD_POSITION_ON_DELETE;
		else
			s->current_field = DL_FIELD_NONE;
	}
	else
	{
		if (strcmp(fname, "fragments") == 0 ||
			strcmp(fname, "updateFragments") == 0)
			s->next_array_is_fragments = true;

		s->current_field = DL_FIELD_NONE;
	}
}

static void
dl_scalar(void *semstate, char *token, JsonTokenType tokentype)
{
	DLParseState *s = (DLParseState *) semstate;

	if (!s->in_fragment_object)
		return;

	switch (s->current_field)
	{
		case DL_FIELD_PATH:
			s->cur_path = pstrdup(token);
			break;
		case DL_FIELD_FORMAT:
			s->cur_format = pstrdup(token);
			break;
		case DL_FIELD_RECORD_COUNT:
			s->cur_record_count = strtoll(token, NULL, 10);
			break;
		case DL_FIELD_FILE_SIZE:
			s->cur_file_size = strtoll(token, NULL, 10);
			break;
		case DL_FIELD_POSITION_ON_DELETE:
			s->cur_is_delete = (strcmp(token, "POSITION_DELETE") == 0);
			break;
		default:
			break;
	}

	s->current_field = DL_FIELD_NONE;
}

/*
 * pg_iceberg_parse_data_locations
 *    Parse the JSON returned by the volume FDW into TrackedDataFile arrays.
 *
 * Caller passes pointers to receive the output arrays and their sizes.
 * Either output may be NULL/0 if no files of that type were found.
 * Output arrays are palloc'd in CurrentMemoryContext.
 */
void
pg_iceberg_parse_data_locations(const char *json,
								TrackedDataFile **data_files_out,
								int *num_data_files_out,
								TrackedDataFile **delete_files_out,
								int *num_delete_files_out)
{
	JsonLexContext	   *lex;
	JsonSemAction		sem;
	DLParseState		state;
	ListCell		   *lc;
	int					i;

	/* Defaults */
	*data_files_out = NULL;
	*num_data_files_out = 0;
	*delete_files_out = NULL;
	*num_delete_files_out = 0;

	if (json == NULL || json[0] == '\0')
		return;

	memset(&state, 0, sizeof(state));

	lex = makeJsonLexContextCstringLen((char *) json,
									   strlen(json),
									   GetDatabaseEncoding(),
									   true);

	memset(&sem, 0, sizeof(sem));
	sem.semstate = &state;
	sem.object_start = dl_object_start;
	sem.object_end = dl_object_end;
	sem.array_start = dl_array_start;
	sem.array_end = dl_array_end;
	sem.object_field_start = dl_field_start;
	sem.scalar = dl_scalar;

	pg_parse_json_or_ereport(lex, &sem);

	pfree(lex);

	/* Convert Lists to flat arrays for the tracker API */
	if (list_length(state.data_files) > 0)
	{
		int			n = list_length(state.data_files);
		TrackedDataFile *arr = (TrackedDataFile *)
			palloc(sizeof(TrackedDataFile) * n);

		i = 0;
		foreach(lc, state.data_files)
		{
			TrackedDataFile *src = (TrackedDataFile *) lfirst(lc);

			arr[i].file_path = src->file_path;
			arr[i].record_count = src->record_count;
			arr[i].file_size = src->file_size;
			arr[i].file_format = src->file_format;
			i++;
			pfree(src);
		}
		list_free(state.data_files);

		*data_files_out = arr;
		*num_data_files_out = n;
	}

	if (list_length(state.delete_files) > 0)
	{
		int			n = list_length(state.delete_files);
		TrackedDataFile *arr = (TrackedDataFile *)
			palloc(sizeof(TrackedDataFile) * n);

		i = 0;
		foreach(lc, state.delete_files)
		{
			TrackedDataFile *src = (TrackedDataFile *) lfirst(lc);

			arr[i].file_path = src->file_path;
			arr[i].record_count = src->record_count;
			arr[i].file_size = src->file_size;
			arr[i].file_format = src->file_format;
			i++;
			pfree(src);
		}
		list_free(state.delete_files);

		*delete_files_out = arr;
		*num_delete_files_out = n;
	}
}


/* ================================================================
 *                  Rebase: Agent Communication
 * ================================================================
 */

/*
 * serialize_files_to_data_locations
 *    Serialize accumulated TrackedDataFile arrays to JSON data_locations string.
 *
 * Produces the JSON format expected by pg_iceberg_modify_data_with_catalog:
 *
 *   For INSERT (data files only):
 *     {"fragments":[{"path":"...","format":"parquet",
 *       "record_count":N,"file_size_in_bytes":N,"position_on_delete":"DATA_FILE"}]}
 *
 *   For UPDATE (data + delete files) or DELETE (delete files only):
 *     {"updateFragments":[{"path":"...","format":"parquet",
 *       "record_count":N,"file_size_in_bytes":N,"position_on_delete":"POSITION_DELETE"}]}
 *
 * Also returns the appropriate CmdType via *cmd_out.
 */
static char *
serialize_files_to_data_locations(List *data_files,
								  List *delete_files,
								  CmdType *cmd_out)
{
	StringInfoData buf;
	bool		has_data = (list_length(data_files) > 0);
	bool		has_delete = (list_length(delete_files) > 0);
	bool		first = true;
	ListCell   *lc;
	const char *format;

	if (!has_data && !has_delete)
		return NULL;

	/* Determine the operation type and JSON key */
	if (has_data && !has_delete)
	{
		*cmd_out = CMD_INSERT;
	}
	else if (!has_data && has_delete)
	{
		*cmd_out = CMD_DELETE;
	}
	else
	{
		/* Mixed: both data and delete files -> UPDATE */
		*cmd_out = CMD_UPDATE;
	}

	initStringInfo(&buf);

	/*
	 * INSERT uses "fragments" key; UPDATE/DELETE use "updateFragments" key.
	 * This matches the format expected by iceberg_catalog_fdw.c:
	 *   createAppendFromRequest() reads "fragments"
	 *   createUpdateFromRequest() reads "updateFragments"
	 */
	if (*cmd_out == CMD_INSERT)
		appendStringInfoString(&buf, "{\"fragments\":[");
	else
		appendStringInfoString(&buf, "{\"updateFragments\":[");

	/* Serialize data files */
	foreach(lc, data_files)
	{
		TrackedDataFile *f = (TrackedDataFile *) lfirst(lc);

		if (!first)
			appendStringInfoChar(&buf, ',');
		first = false;

		format = f->file_format ? f->file_format : "parquet";

		appendStringInfoString(&buf, "{\"path\":");
		escape_json(&buf, f->file_path);
		appendStringInfoString(&buf, ",\"format\":");
		escape_json(&buf, format);
		appendStringInfo(&buf,
						 ",\"record_count\":" INT64_FORMAT ","
						 "\"file_size_in_bytes\":" INT64_FORMAT ","
						 "\"position_on_delete\":\"DATA_FILE\"}",
						 f->record_count,
						 f->file_size);
	}

	/* Serialize delete files */
	foreach(lc, delete_files)
	{
		TrackedDataFile *f = (TrackedDataFile *) lfirst(lc);

		if (!first)
			appendStringInfoChar(&buf, ',');
		first = false;

		format = f->file_format ? f->file_format : "parquet";

		appendStringInfoString(&buf, "{\"path\":");
		escape_json(&buf, f->file_path);
		appendStringInfoString(&buf, ",\"format\":");
		escape_json(&buf, format);
		appendStringInfo(&buf,
						 ",\"record_count\":" INT64_FORMAT ","
						 "\"file_size_in_bytes\":" INT64_FORMAT ","
						 "\"position_on_delete\":\"POSITION_DELETE\"}",
						 f->record_count,
						 f->file_size);
	}

	appendStringInfoString(&buf, "]}");

	return buf.data;
}

/*
 * pg_iceberg_tracker_apply_updates_with_rebase
 *    Apply new files with rebase on the latest global metadata.
 *
 * This is the C equivalent of Rust's apply_updates_with_rebase().
 * Called in three contexts:
 *   1. Per-statement (DML finish): with new files from this statement
 *   2. At commit (tracker_commit_all): with empty new files
 *   3. At scan (get_scan_metadata_location): with empty new files
 *
 * The function:
 *   1. Reads latest global metadata from catalog
 *   2. Optimization: skips if global unchanged AND no new files
 *   3. Appends new files to accumulated arrays
 *   4. Serializes ALL accumulated files to JSON
 *   5. Calls pg_iceberg_modify_data_with_catalog() (agent)
 *   6. Updates tracker state (history, last_base, current_metadata)
 *
 * Returns: the new metadata location (owned by tracker, do NOT pfree).
 *          Returns current_metadata_location if optimization skip.
 *          ERROR if something goes wrong.
 */
char *
pg_iceberg_tracker_apply_updates_with_rebase(Oid relid,
											 const TrackedDataFile *new_data_files,
											 int num_new_data_files,
											 const TrackedDataFile *new_delete_files,
											 int num_new_delete_files)
{
	TableMetadataState *state;
	char	   *latest_global_location;
	char	   *latest_global_location_copy;
	char	   *new_metadata_location;
	char	   *data_locations_json;
	CmdType		cmd_type;
	Relation	rel;
	IcebergTableInfo *table_info;
	int			nest_level;
	MemoryContext oldcxt;
	int			prev_data_count;
	int			prev_delete_count;

	state = (TableMetadataState *)
		hash_search(tracker_table, &relid, HASH_FIND, NULL);
	if (state == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("table %u not registered in metadata tracker",
						relid)));

	nest_level = GetCurrentTransactionNestLevel();
	table_info = pg_iceberg_get_table_info(state->relid);

	/*
	 * Step 1: Get the latest global metadata location from the catalog.
	 * This may differ from our last_base if another transaction committed.
	 */
	latest_global_location = pg_iceberg_get_latest_metadata_location(
		relid, table_info);

	/*
	 * Step 2: Optimization - skip rebase if:
	 *   a) No new files to append in this statement
	 *   b) Global metadata hasn't changed since our last rebase
	 * In this case, our current metadata is still valid.
	 */
	if (num_new_data_files == 0 && num_new_delete_files == 0 &&
		!state->dirty &&
		state->last_base_metadata_location != NULL &&
		strcmp(state->last_base_metadata_location,
			   latest_global_location) == 0)
	{
		elog(DEBUG1,
			 "metadata tracker: rebase skipped for table %u "
			 "(no new files, global unchanged, not dirty)",
			 relid);

		pfree(latest_global_location);
		pg_iceberg_free_table_info(table_info);

		return state->current_metadata_location;
	}

	/*
	 * Step 3: Push history entry BEFORE modifying state, then append files.
	 * By recording the undo point first, subtransaction rollback can
	 * correctly undo our changes even if the agent call in step 5 fails.
	 */
	prev_data_count = list_length(state->data_files);
	prev_delete_count = list_length(state->delete_files);

	oldcxt = MemoryContextSwitchTo(tracker_context);

	{
		LevelHistoryEntry *entry =
			(LevelHistoryEntry *) palloc(sizeof(LevelHistoryEntry));

		entry->nest_level = nest_level;
		entry->prev_metadata_location =
			state->current_metadata_location
			? pstrdup(state->current_metadata_location)
			: NULL;
		entry->prev_last_base =
			state->last_base_metadata_location
			? pstrdup(state->last_base_metadata_location)
			: NULL;
		entry->prev_data_files_count = prev_data_count;
		entry->prev_delete_files_count = prev_delete_count;

		state->level_history = lappend(state->level_history, entry);
	}

	state->data_files = append_files_to_list(state->data_files,
											 new_data_files,
											 num_new_data_files);
	state->delete_files = append_files_to_list(state->delete_files,
											   new_delete_files,
											   num_new_delete_files);

	MemoryContextSwitchTo(oldcxt);

	/*
	 * Step 4: Serialize ALL accumulated files to JSON data_locations.
	 */
	data_locations_json = serialize_files_to_data_locations(
		state->data_files, state->delete_files, &cmd_type);

	/*
	 * Step 5: Call the agent to generate a new metadata file.
	 * If this throws an error, the history entry from step 3 ensures
	 * subtransaction rollback can undo our file appends.
	 */
	rel = table_open(state->relid, AccessShareLock);

	new_metadata_location = pg_iceberg_modify_data_with_catalog(
		rel,
		table_info,
		data_locations_json,
		latest_global_location,
		state->is_internal,
		cmd_type);

	/*
	 * Step 6: Agent succeeded. Update metadata locations.
	 *
	 * Immediately copy the results into tracker_context before freeing
	 * resources or closing the relation.
	 */
	oldcxt = MemoryContextSwitchTo(tracker_context);

	new_metadata_location = pstrdup(new_metadata_location);
	latest_global_location_copy = pstrdup(latest_global_location);

	if (state->last_base_metadata_location)
		pfree(state->last_base_metadata_location);
	state->last_base_metadata_location = latest_global_location_copy;

	if (state->current_metadata_location)
		pfree(state->current_metadata_location);
	state->current_metadata_location = new_metadata_location;

	/* All accumulated files are now reflected in current_metadata_location. */
	state->dirty = false;

	MemoryContextSwitchTo(oldcxt);

	pg_iceberg_free_table_info(table_info);
	table_close(rel, AccessShareLock);
	pfree(latest_global_location);
	pfree(data_locations_json);

	elog(DEBUG1,
		 "metadata tracker: rebase for table %u at level %d, "
		 "base: %s, new_meta: %s, "
		 "+%d data files, +%d delete files (total: %d data, %d delete)",
		 relid, nest_level,
		 state->last_base_metadata_location,
		 state->current_metadata_location,
		 num_new_data_files, num_new_delete_files,
		 list_length(state->data_files),
		 list_length(state->delete_files));

	return state->current_metadata_location;
}

/*
 * pg_iceberg_tracker_accumulate_files
 *    Append new files to the tracker WITHOUT generating metadata.
 *
 * This is the deferred counterpart of apply_updates_with_rebase: it performs
 * only the cheap part (push a level-history undo point + append the new files
 * to the accumulated lists) and marks the table dirty. It does NOT serialize
 * the accumulated list or call the agent, so per-statement cost is O(new
 * files) instead of O(total files). The expensive materialization is deferred
 * to the next scan (pg_iceberg_tracker_get_scan_metadata_location) or to
 * commit (tracker_commit_all), both of which call apply_updates_with_rebase
 * with empty new-file arrays and will now act because the table is dirty.
 *
 * Only used for builtin-catalog tables (issue #323). Non-builtin catalogs
 * still go through apply_updates_with_rebase per statement, because there the
 * agent call commits to the external catalog and must not be deferred.
 */
void
pg_iceberg_tracker_accumulate_files(Oid relid,
									const TrackedDataFile *new_data_files,
									int num_new_data_files,
									const TrackedDataFile *new_delete_files,
									int num_new_delete_files)
{
	TableMetadataState *state;
	int			nest_level;
	int			prev_data_count;
	int			prev_delete_count;
	MemoryContext oldcxt;

	/* Nothing to accumulate. */
	if (num_new_data_files == 0 && num_new_delete_files == 0)
		return;

	state = (TableMetadataState *)
		hash_search(tracker_table, &relid, HASH_FIND, NULL);
	if (state == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("table %u not registered in metadata tracker",
						relid)));

	nest_level = GetCurrentTransactionNestLevel();
	prev_data_count = list_length(state->data_files);
	prev_delete_count = list_length(state->delete_files);

	oldcxt = MemoryContextSwitchTo(tracker_context);

	{
		LevelHistoryEntry *entry =
			(LevelHistoryEntry *) palloc(sizeof(LevelHistoryEntry));

		entry->nest_level = nest_level;
		entry->prev_metadata_location =
			state->current_metadata_location
			? pstrdup(state->current_metadata_location)
			: NULL;
		entry->prev_last_base =
			state->last_base_metadata_location
			? pstrdup(state->last_base_metadata_location)
			: NULL;
		entry->prev_data_files_count = prev_data_count;
		entry->prev_delete_files_count = prev_delete_count;

		state->level_history = lappend(state->level_history, entry);
	}

	state->data_files = append_files_to_list(state->data_files,
											 new_data_files,
											 num_new_data_files);
	state->delete_files = append_files_to_list(state->delete_files,
											   new_delete_files,
											   num_new_delete_files);

	MemoryContextSwitchTo(oldcxt);

	/* Defer materialization: regenerated lazily on next scan or at commit. */
	state->dirty = true;

	elog(DEBUG1,
		 "metadata tracker: accumulated +%d data, +%d delete files for "
		 "table %u at level %d (deferred; total %d data, %d delete)",
		 num_new_data_files, num_new_delete_files, relid, nest_level,
		 list_length(state->data_files),
		 list_length(state->delete_files));
}

/*
 * pg_iceberg_tracker_get_scan_metadata_location
 *    Get the metadata location to use for scanning (SELECT) a table.
 *
 * This is the C equivalent of Rust's get_or_rebase_metadata_location().
 *
 * For Read Committed isolation, each statement needs to see the latest
 * committed state:
 *
 * Case 1: Table is tracked (modified in this transaction)
 *   Calls apply_updates_with_rebase() with empty file arrays to
 *   incorporate any concurrent commits from other transactions.
 *   The optimization inside will skip the agent call if the global
 *   metadata hasn't changed since our last rebase.
 *
 * Case 2: Table is NOT tracked (read-only in this transaction)
 *   Reads the latest metadata location from the catalog.
 *
 * Returns a palloc'd string (caller must pfree). Returns NULL if no
 * metadata location is found.
 */
char *
pg_iceberg_tracker_get_scan_metadata_location(Oid relid)
{
	TableMetadataState *state;

	/*
	 * Case 1: Table is tracked in the current transaction.
	 * Trigger a rebase to incorporate any concurrent commits.
	 */
	if (tracker_table != NULL)
	{
		state = (TableMetadataState *)
			hash_search(tracker_table, &relid, HASH_FIND, NULL);

		if (state != NULL)
		{
			char *location;

			/*
			 * Call apply_updates_with_rebase with empty files.
			 * This will either:
			 * - Skip (optimization) if global hasn't changed → returns current
			 * - Rebase on latest global → returns new metadata
			 */
			location = pg_iceberg_tracker_apply_updates_with_rebase(
				relid, NULL, 0, NULL, 0);

			return pstrdup(location);
		}
	}

	/*
	 * Case 2: Table is NOT tracked (not modified in this transaction).
	 * Read the latest metadata location from the catalog.
	 */
	{
		IcebergMetadataInfo *meta_info;
		char	   *result = NULL;

		meta_info = pg_iceberg_get_metadata_info(relid);

		result = pstrdup(meta_info->metadata_location);

		pg_iceberg_free_metadata_info(meta_info);

		return result;
	}
}
