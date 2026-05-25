/*-------------------------------------------------------------------------
 *
 * pg_iceberg_am.c
 *    Implementation of Iceberg Access Method (AM) logic.
 *
 * This file contains the actual data access and modification logic for
 * Iceberg tables, including scanning, inserting, updating, and deleting tuples.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/am_iceberg/pg_iceberg_am.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/multixact.h"
#include "access/table.h"
#include "access/tableam.h"
#include "catalog/oid_dispatch.h"
#include "commands/vacuum.h"
#include "libpq/libpq-int.h"
#include "utils/guc.h"
#include "utils/guc_tables.h"
#include "utils/sampling.h"
#include "miscadmin.h"
#include "lib/stringinfo.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/pathnode.h"
#include "optimizer/optimizer.h"
#include "optimizer/cost.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "catalog/pg_type.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_am.h"
#include "executor/executor.h"
#include "nodes/plannodes.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "utils/hsearch.h"
#include "mb/pg_wchar.h"

#include "utils/builtins.h"
#include "utils/portal.h"
#include "utils/snapmgr.h"
#include "cdb/cdbdisp.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbdispatchresult.h"
#include "executor/execdesc.h"
#include "tcop/pquery.h"

#include "../datalake_def.h"
#include "include/pg_iceberg_am.h"
#include "include/pg_iceberg_catalog.h"
#include "include/pg_iceberg_catalog_helper.h"
#include "include/pg_iceberg_extensible.h"
#include "include/pg_iceberg_metadata.h"
#include "include/pg_iceberg_metadata_tracker.h"
#include "include/pg_iceberg_guc.h"
#include "include/pg_iceberg_rewrite_plan.h"
#include "../iceberg_catalog_fdw/iceberg_catalog_fdw.h"
#include "../iceberg_volume_fdw/iceberg_volume_fdw.h"
#include "../dlproxy/iceberg_common.h"
#include "../common/random_segment.h"

extern int external_table_limit_segment_num;

static FdwRoutine *
get_volume_fdw_routine(void)
{
	ForeignDataWrapper *fdw;

	fdw = GetForeignDataWrapperByName("iceberg_volume_fdw", false);
	return GetFdwRoutine(fdw->fdwhandler);
}

static List *
iceberg_get_all_attrs(Relation rel)
{
	List *attrs = NIL;
	TupleDesc tupdesc = RelationGetDescr(rel);
	int natts = tupdesc->natts;
	int i;

	for (i = 0; i < natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
		if (!attr->attisdropped)
		{
			attrs = lappend_int(attrs, i + 1);
		}
	}
	return attrs;
}

static List *
iceberg_build_fdw_private(List *retrieved_attrs, List *am_private)
{
	List *fdw_private = list_make2(makeString("iceberg_scan"), retrieved_attrs);

	fdw_private = list_concat(fdw_private, list_copy(am_private));

	return fdw_private;
}

static ForeignScanState *
iceberg_create_foreign_scan_state(IcebergScanDesc scanDesc,
								  struct PlanState *ps,
								  char *volumeServer,
								  char *volumeName,
								  List *am_private)
{
	ForeignScan *plan;
	ForeignScanState *scanState = makeNode(ForeignScanState);
	ScanState *parentScanState = (ScanState *) ps;

	scanState->ss.ss_currentRelation = scanDesc->rs_base.rs_rd;
	scanState->fdwroutine = get_volume_fdw_routine();
	scanState->ss.ss_ScanTupleSlot = parentScanState->ss_ScanTupleSlot;
	scanState->ss.ps.scandesc = parentScanState->ps.scandesc;

	plan = makeNode(ForeignScan);
	plan->scan.plan.qual = ps->plan->qual;
	plan->fdw_private = iceberg_build_fdw_private(
		iceberg_get_all_attrs(scanDesc->rs_base.rs_rd), am_private
	);
	scanState->ss.ps.plan = (Plan *) plan;

	icebergVolumeScanState *fdwState = (icebergVolumeScanState *) palloc0(sizeof(icebergVolumeScanState));
	fdwState->iceTable.volumn_server_name = volumeServer;
	fdwState->iceTable.volumn_name = volumeName;
	scanState->fdw_state = (void *) fdwState;

	return scanState;
}

/* --- Table Scan API --- */

TableScanDesc
pg_iceberg_beginscan(Relation rel,
					 Snapshot snapshot,
					 int nkeys,
					 struct ScanKeyData *key,
					 ParallelTableScanDesc pscan,
					 uint32 flags)
{
	return pg_iceberg_scan_begin_extractcolumns(rel, snapshot, nkeys, key, pscan, NULL, flags);
}

TableScanDesc
pg_iceberg_scan_begin_extractcolumns(Relation rel,
									 Snapshot snapshot,
									 int nkeys,
									 struct ScanKeyData *key,
									 ParallelTableScanDesc parallel_scan,
									 struct PlanState *ps,
									 uint32 flags)
{
	IcebergScanDesc scan;
	IcebergTableInfo *table_info;
	List *am_private = NIL;
	FdwRoutine *fdw_routine;

	/* Allocate scan descriptor */
	scan = (IcebergScanDesc) palloc0(sizeof(IcebergScanDescData));

	scan->rs_base.rs_rd = rel;
	scan->rs_base.rs_snapshot = snapshot;
	scan->rs_base.rs_nkeys = nkeys;
	scan->rs_base.rs_flags = flags;
	scan->rs_base.rs_parallel = parallel_scan;

	if (nkeys > 0)
	{
		scan->rs_base.rs_key = (ScanKey) palloc(sizeof(ScanKeyData) * nkeys);
		memcpy(scan->rs_base.rs_key, key, sizeof(ScanKeyData) * nkeys);
	}

	table_info = pg_iceberg_get_table_info(RelationGetRelid(rel));
	if (ps && ps->plan && IsA(ps->plan, CustomScan))
		am_private = ((CustomScan *) ps->plan)->custom_private;

	scan->scanState = iceberg_create_foreign_scan_state(scan, ps,
														table_info->volume_server_name,
														table_info->volume_name,
														am_private);
	fdw_routine = scan->scanState->fdwroutine;
	fdw_routine->BeginForeignScan(scan->scanState, 0);

	pg_iceberg_free_table_info(table_info);
	return (TableScanDesc) scan;
}

void
pg_iceberg_endscan(TableScanDesc sscan)
{
	IcebergScanDesc scan = (IcebergScanDesc) sscan;
	FdwRoutine *fdw_routine = scan->scanState->fdwroutine;

	fdw_routine->EndForeignScan(scan->scanState);
	pfree(scan);
}

void
pg_iceberg_rescan(TableScanDesc sscan, struct ScanKeyData *key, bool set_params, bool allow_strat,
				  bool allow_sync, bool allow_pagemode)
{
	IcebergScanDesc scan = (IcebergScanDesc) sscan;
	FdwRoutine *fdw_routine = scan->scanState->fdwroutine;

	fdw_routine->ReScanForeignScan(scan->scanState);
}

bool
pg_iceberg_getnextslot(TableScanDesc sscan, ScanDirection direction, TupleTableSlot *slot)
{
	IcebergScanDesc scan = (IcebergScanDesc) sscan;
	FdwRoutine *fdw_routine = scan->scanState->fdwroutine;

	slot = fdw_routine->IterateForeignScan(scan->scanState);
	if (TupIsNull(slot))
		return false;

	return true;
}

/* --- Planner Helpers --- */

List *
pg_iceberg_build_scan_am_private(Relation rel, struct PlanState *ps, int random_segment_num)
{
	List			   *am_private = NIL;
	char			   *fragments = NULL;
	char			   *scan_metadata_location;
	bool				is_internal;
	IcebergTableInfo   *table_info;
	TableMetadataState *tstate;
	int					segment_count = getgpsegmentCount();

	table_info = pg_iceberg_get_table_info(RelationGetRelid(rel));

	/*
	 * Obtain metadata location and is_internal for scan planning.
	 *
	 * Tracked tables (modified in this txn): the tracker provides both.
	 * get_scan_metadata_location() triggers a rebase to incorporate
	 * concurrent commits, ensuring Read-Your-Own-Writes semantics.
	 *
	 * Untracked tables: read directly from the catalog (single read).
	 */
	tstate = pg_iceberg_tracker_get_table_state(RelationGetRelid(rel));
	if (tstate != NULL)
	{
		scan_metadata_location =
			pg_iceberg_tracker_get_scan_metadata_location(RelationGetRelid(rel));
		is_internal = tstate->is_internal;
	}
	else
	{
		IcebergMetadataInfo *metadata_info;

		metadata_info = pg_iceberg_get_metadata_info(RelationGetRelid(rel));
		scan_metadata_location = pstrdup(metadata_info->metadata_location);
		is_internal = metadata_info->is_internal;
		pg_iceberg_free_metadata_info(metadata_info);
	}

	fragments = pg_iceberg_get_fragments_with_catalog(rel,
													  table_info,
													  scan_metadata_location,
													  is_internal,
													  NULL);

	pfree(scan_metadata_location);

	am_private = list_concat(am_private,
							 parseIcebergFragmentResponse(fragments, strlen(fragments)));

	if (!is_internal && checkIsPolarisCatalog(table_info->catalog_server_name, table_info->catalog_name))
	{
		IcebergLoadTableResult *load_result;

		load_result = pg_iceberg_load_table(table_info->opts->catalog,
											table_info->opts->namespace,
											table_info->opts->table,
											table_info->catalog_server_name,
											table_info->catalog_name,
											table_info->volume_server_name,
											table_info->volume_name);

		if (load_result == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("iceberg table \"%s.%s\" does not exist in external catalog \"%s\"",
							table_info->opts->namespace,
							table_info->opts->table,
							table_info->catalog_name)));

		if (load_result->catalog_properties)
		{
			am_private = lappend(am_private, makeString(load_result->catalog_properties));
			/* Ownership transferred to am_private entry. */
			load_result->catalog_properties = NULL;
		}
		pg_iceberg_free_load_table_result(load_result);
	}

	am_private = list_concat(am_private,
							 datalakeSelectRandomSegments(segment_count, random_segment_num));

	pg_iceberg_free_table_info(table_info);

	return am_private;
}

/*
 * pg_iceberg_list_data_fragments
 *		Return the raw fragment list ([ExternalTableMetadata, combinedTask0,
 *		combinedTask1, ...]) for the current Iceberg snapshot of `rel`.
 *
 * Strips the catalog_properties / random_segments trailers that
 * pg_iceberg_build_scan_am_private appends for the scan path, so the
 * result is directly consumable by icebergFileIndexMapPopulateFromAllFragments.
 *
 * Caller MUST run this on the QD: it consults the local pg_iceberg_metadata
 * catalog (which is QD-only populated) via pg_iceberg_get_metadata_info().
 * Used by the planner-time hook to seed the per-statement modify-fragments
 * cache that is then dispatched to every QE (issue #333).
 */
List *
pg_iceberg_list_data_fragments(Relation rel)
{
	char			   *fragments;
	char			   *scan_metadata_location;
	bool				is_internal;
	IcebergTableInfo   *table_info;
	TableMetadataState *tstate;
	List			   *result;

	table_info = pg_iceberg_get_table_info(RelationGetRelid(rel));

	tstate = pg_iceberg_tracker_get_table_state(RelationGetRelid(rel));
	if (tstate != NULL)
	{
		scan_metadata_location =
			pg_iceberg_tracker_get_scan_metadata_location(RelationGetRelid(rel));
		is_internal = tstate->is_internal;
	}
	else
	{
		IcebergMetadataInfo *metadata_info;

		metadata_info = pg_iceberg_get_metadata_info(RelationGetRelid(rel));
		scan_metadata_location = pstrdup(metadata_info->metadata_location);
		is_internal = metadata_info->is_internal;
		pg_iceberg_free_metadata_info(metadata_info);
	}

	fragments = pg_iceberg_get_fragments_with_catalog(rel,
													  table_info,
													  scan_metadata_location,
													  is_internal,
													  NULL);

	pfree(scan_metadata_location);
	pg_iceberg_free_table_info(table_info);

	result = parseIcebergFragmentResponse(fragments, strlen(fragments));
	return result;
}


/* ----------------------------------------------------------------
 * Modify-time fragments cache (issue #333)
 *
 * A process-local map keyed by relid that carries the fragment list
 * for an in-flight UPDATE/DELETE statement.  The QD's planner_hook
 * fills it via pg_iceberg_stash_modify_fragments(); on every QE an
 * ExecutorStart_hook walks the dispatched plan and re-populates the
 * same map from ModifyTable.fdwPrivLists.  iceberg_modify_init
 * consumes (and removes) the entry with pg_iceberg_take_modify_fragments()
 * so cross-statement reuse cannot leak a stale list.
 * ----------------------------------------------------------------
 */
typedef struct ModifyFragmentsEntry
{
	Oid		relid;			/* hash key */
	List   *fragments;
} ModifyFragmentsEntry;

static HTAB *modify_fragments_cache = NULL;

static void
modify_fragments_cache_ensure(void)
{
	HASHCTL ctl;

	if (modify_fragments_cache != NULL)
		return;

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(ModifyFragmentsEntry);
	ctl.hcxt = TopMemoryContext;

	modify_fragments_cache = hash_create("iceberg modify fragments cache",
										  16, &ctl,
										  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

void
pg_iceberg_stash_modify_fragments(Oid relid, List *fragments)
{
	ModifyFragmentsEntry   *entry;
	bool					found;

	if (!OidIsValid(relid) || fragments == NIL)
		return;

	modify_fragments_cache_ensure();
	entry = (ModifyFragmentsEntry *) hash_search(modify_fragments_cache,
												 &relid, HASH_ENTER, &found);
	entry->fragments = fragments;
}

List *
pg_iceberg_take_modify_fragments(Oid relid)
{
	ModifyFragmentsEntry   *entry;
	List				   *fragments;

	if (modify_fragments_cache == NULL || !OidIsValid(relid))
		return NIL;

	entry = (ModifyFragmentsEntry *) hash_search(modify_fragments_cache,
												 &relid, HASH_FIND, NULL);
	if (entry == NULL)
		return NIL;

	fragments = entry->fragments;
	hash_search(modify_fragments_cache, &relid, HASH_REMOVE, NULL);
	return fragments;
}

void
pg_iceberg_estimate_rel_size(Relation rel, int32 *attr_widths, BlockNumber *pages,
							  double *tuples, double *allvisfrac)
{
	IcebergTableInfo *table_info;
	IcebergMetadataInfo *metadata_info;
	IcebergTableStatistics *statistics;
	int64 total_size = 0;

	*tuples = 0.0;
	*pages = 1;
	*allvisfrac = 1.0;

	table_info = pg_iceberg_get_table_info(RelationGetRelid(rel));
	metadata_info = pg_iceberg_get_metadata_info(RelationGetRelid(rel));
	statistics = pg_iceberg_get_statistics_with_catalog(rel,
														table_info,
														metadata_info->metadata_location,
														metadata_info->is_internal);

	if (statistics->recordCount > 0)
		*tuples = (double) statistics->recordCount;

	if (statistics->bytesInDataFile > 0)
		total_size = statistics->bytesInDataFile;

	*pages = (total_size + (BLCKSZ - 1)) / BLCKSZ;
	if (*pages < 1)
		*pages = 1;

	pfree(statistics);
	pg_iceberg_free_metadata_info(metadata_info);
	pg_iceberg_free_table_info(table_info);
}

uint64
pg_iceberg_relation_size(Relation rel, ForkNumber forkNumber)
{
	IcebergTableInfo *table_info;
	IcebergMetadataInfo *metadata_info;
	IcebergTableStatistics *statistics;
	uint64 total_size = 0;

	if (forkNumber != MAIN_FORKNUM)
		return 0;

	/*
	 * Iceberg table size is fetched from catalog metadata maintained on QD.
	 * QE side should not try to read those catalogs directly.
	 */
	if (Gp_role != GP_ROLE_DISPATCH)
		return 0;

	table_info = pg_iceberg_get_table_info(RelationGetRelid(rel));
	metadata_info = pg_iceberg_get_metadata_info(RelationGetRelid(rel));
	statistics = pg_iceberg_get_statistics_with_catalog(rel,
														table_info,
														metadata_info->metadata_location,
														metadata_info->is_internal);

	if (statistics->bytesInDataFile > 0)
		total_size = statistics->bytesInDataFile;

	pfree(statistics);
	pg_iceberg_free_metadata_info(metadata_info);
	pg_iceberg_free_table_info(table_info);

	return total_size;
}

/*
 * pg_iceberg_refresh_pg_class_stats
 *
 * Read recordCount / bytesInDataFile from the Iceberg catalog metadata
 * and persist them into pg_class.reltuples and pg_class.relpages so that
 * ORCA can pick up accurate cardinality.
 *
 * ORCA bypasses the tableam relation_estimate_size callback and reads
 * rel->rd_rel->reltuples directly (see CTranslatorRelcacheToDXL.cpp).
 * Without this refresh, Iceberg tables stay at reltuples=-1 and ORCA
 * treats them as one-row relations, producing catastrophic Broadcast
 * Motion plans (see hashdata-lightning issue #339).
 *
 * The Iceberg catalog service is only reachable from the QD; callers
 * must ensure Gp_role == GP_ROLE_DISPATCH.
 */
void
pg_iceberg_refresh_pg_class_stats(Relation rel)
{
	IcebergTableInfo	   *table_info;
	IcebergMetadataInfo	   *metadata_info;
	IcebergTableStatistics *statistics;
	BlockNumber		pages = 1;
	double			tuples = 0.0;
	int64			total_size = 0;

	Assert(Gp_role == GP_ROLE_DISPATCH);

	table_info = pg_iceberg_get_table_info(RelationGetRelid(rel));
	metadata_info = pg_iceberg_get_metadata_info(RelationGetRelid(rel));
	statistics = pg_iceberg_get_statistics_with_catalog(rel,
														table_info,
														metadata_info->metadata_location,
														metadata_info->is_internal);

	if (statistics->recordCount > 0)
		tuples = (double) statistics->recordCount;

	if (statistics->bytesInDataFile > 0)
		total_size = statistics->bytesInDataFile;

	pages = (total_size + (BLCKSZ - 1)) / BLCKSZ;
	if (pages < 1)
		pages = 1;

	/*
	 * Persist into pg_class.  Passing isvacuum=false treats this as an
	 * ANALYZE-style refresh, which lets vac_update_relstats actually
	 * write num_pages / num_tuples on the QD (the VACUUM path on QD
	 * deliberately skips the update; see vacuum.c).  InvalidTransactionId
	 * / InvalidMultiXactId signal "do not touch relfrozenxid/relminmxid".
	 *
	 * Pass the relation's current relhasindex through unchanged.  Hardcoding
	 * `false` would cause vac_update_relstats to clear pg_class.relhasindex
	 * whenever this refresh runs on a relation that does have indexes (the
	 * function rewrites relhasindex on the !in_outer_xact path).  Reading
	 * the value from rel->rd_rel matches the analyze.c convention and keeps
	 * the write scope confined to relpages/reltuples.
	 */
	vac_update_relstats(rel,
						pages,
						tuples,
						0,			/* num_all_visible_pages */
						rel->rd_rel->relhasindex,
						InvalidTransactionId,
						InvalidMultiXactId,
						false,		/* in_outer_xact */
						false);		/* isvacuum */

	pfree(statistics);
	pg_iceberg_free_metadata_info(metadata_info);
	pg_iceberg_free_table_info(table_info);
}

char *
pg_iceberg_resolve_modify_location(Relation rel, CmdType operation)
{
	IcebergTableInfo *table_info;
	char	   *location = NULL;

	Assert(rel != NULL);
	Assert(operation == CMD_INSERT || operation == CMD_UPDATE || operation == CMD_DELETE);

	table_info = pg_iceberg_get_table_info(RelationGetRelid(rel));

	if (table_info->opts != NULL &&
		table_info->opts->location != NULL &&
		table_info->opts->location[0] != '\0')
		location = pstrdup(table_info->opts->location);

	pg_iceberg_free_table_info(table_info);

	return location;
}

/*
 * Iceberg ANALYZE does not run per-row sampling.  The planner obtains its
 * cardinality estimates from pg_iceberg_estimate_rel_size(), which reads
 * recordCount / bytesInDataFile directly from the Iceberg catalog on the
 * QD, so detailed pg_statistic entries for individual columns are not
 * collected here.
 *
 * datalake_ProcessUtility short-circuits ANALYZE on Iceberg relations before
 * it reaches do_analyze_rel, so this callback is effectively unreachable for
 * plain ANALYZE statements.  It is retained as a defensive no-op in case a
 * partitioned parent's ANALYZE path drags an Iceberg partition into the
 * kernel sampler.
 */
int
pg_iceberg_acquire_sample_rows(Relation relation, int elevel,
							   HeapTuple *rows, int targrows,
							   double *totalrows, double *totaldeadrows)
{
	*totalrows = 0;
	*totaldeadrows = 0;

	ereport(elevel,
			(errmsg("\"%s\": iceberg ANALYZE skips per-row sampling; stats come from metadata",
					RelationGetRelationName(relation))));

	return 0;
}

/* --- DML Implementation --- */

void
pg_iceberg_tuple_insert(IcebergModifyDesc *insertDesc, TupleTableSlot *slot, CommandId cid, int options,
						struct BulkInsertStateData *bistate)
{
	FdwRoutine *fdwRoutine = insertDesc->fdwroutine;
	fdwRoutine->ExecForeignInsert(NULL, insertDesc->resultRelInfo, slot, NULL);
}

TM_Result
pg_iceberg_tuple_update(IcebergModifyDesc *updateDesc, ItemPointer otid, TupleTableSlot *slot, CommandId cid,
						Snapshot snapshot, Snapshot crosscheck, bool wait, TM_FailureData *tmfd,
						LockTupleMode *lockmode, bool *update_indexes)
{
	FdwRoutine *fdwRoutine = updateDesc->fdwroutine;
	TupleTableSlot *planSlot = updateDesc->planSlot;

	ExecClearTuple(planSlot);
	planSlot->tts_values[0] = PointerGetDatum(otid);
	planSlot->tts_isnull[0] = false;
	ExecStoreVirtualTuple(planSlot);

	fdwRoutine->ExecForeignUpdate(NULL, updateDesc->resultRelInfo, slot, planSlot);

	return TM_Ok;
}

TM_Result
pg_iceberg_tuple_delete(IcebergModifyDesc *deleteDesc, ItemPointer tid, CommandId cid, Snapshot snapshot,
						Snapshot crosscheck, bool wait, TM_FailureData *tmfd, bool changingPart)
{
	FdwRoutine *fdwRoutine = deleteDesc->fdwroutine;
	TupleTableSlot *planSlot = deleteDesc->planSlot;

	ExecClearTuple(planSlot);
	planSlot->tts_values[0] = PointerGetDatum(tid);
	planSlot->tts_isnull[0] = false;
	ExecStoreVirtualTuple(planSlot);

	fdwRoutine->ExecForeignDelete(NULL, deleteDesc->resultRelInfo, NULL, planSlot);

	return TM_Ok;
}

static char *
pg_iceberg_rewrite_collect_local_input_fragments_json(List *vacuum_am_private)
{
	StringInfoData rewritten_json;
	int segment_count;
	int task_limit;
	bool first = true;
	int i;

	initStringInfo(&rewritten_json);

	if (vacuum_am_private == NIL)
		return rewritten_json.data;

	segment_count = getgpsegmentCount();
	if (segment_count <= 0)
		return rewritten_json.data;

	task_limit = list_length(vacuum_am_private) - segment_count;
	if (task_limit <= 1)
		return rewritten_json.data;

	for (i = 1; i < task_limit; i++)
	{
		Node *entry = (Node *) list_nth(vacuum_am_private, i);
		List *combined_task;
		ListCell *lc;

		if (((i - 1) % segment_count) != GpIdentity.segindex)
			continue;

		if (entry == NULL || !IsA(entry, List))
			continue;

		combined_task = (List *) entry;
		foreach(lc, combined_task)
		{
			FileScanTask *scan_task = (FileScanTask *) lfirst(lc);

			if (scan_task == NULL || !IsA(scan_task, FileScanTask) ||
				scan_task->dataFile == NULL)
				continue;

			if (!first)
				appendStringInfoChar(&rewritten_json, ',');

			pg_iceberg_rewrite_append_fragment_json(&rewritten_json,
													scan_task->dataFile,
													scan_task->length);
			first = false;
		}
	}

	return rewritten_json.data;
}

static char *
pg_iceberg_rewrite_build_qe_result(const char *added_result_json,
								   const char *rewritten_fragments_json)
{
	return pg_iceberg_rewrite_build_qe_result_json(added_result_json,
												   rewritten_fragments_json);
}

/* --- Vacuum Implementation --- */

static TableScanDesc
pg_iceberg_begin_vacuum_scan(Relation rel,
							 List *vacuum_am_private,
							 CustomScanState **out_scan_state,
							 CustomScan **out_scan_plan,
							 TupleTableSlot **out_scan_slot)
{
	CustomScan	   *scan_plan;
	CustomScanState *scan_state;
	TableScanDesc	scan_desc;

	/*
	 * Build a minimal local CustomScan/CustomScanState pair purely to carry
	 * the vacuum rewrite dispatch payload into
	 * pg_iceberg_scan_begin_extractcolumns through ps->plan->custom_private.
	 */
	scan_plan = makeNode(CustomScan);
	scan_plan->custom_private = vacuum_am_private;

	scan_state = makeNode(CustomScanState);
	scan_state->ss.ps.plan = (Plan *) scan_plan;
	scan_state->ss.ps.scandesc = RelationGetDescr(rel);
	scan_state->ss.ss_ScanTupleSlot = table_slot_create(rel, NULL);

	scan_desc = pg_iceberg_scan_begin_extractcolumns(rel,
													 GetLatestSnapshot(),
													 0,
													 NULL,
													 NULL,
													 (PlanState *) scan_state,
													 0);

	if (out_scan_state)
		*out_scan_state = scan_state;
	if (out_scan_plan)
		*out_scan_plan = scan_plan;
	if (out_scan_slot)
		*out_scan_slot = scan_state->ss.ss_ScanTupleSlot;

	return scan_desc;
}

/*
 * pg_iceberg_relation_vacuum:
 *
 * Tableam relation_vacuum entry point for Iceberg.  All QD↔QE coordination
 * lives entirely in the plugin to keep the kernel free of AM-specific
 * VACUUM machinery:
 *
 *   - On QD, build the rewrite task list, ship it to QEs as an
 *     ExtensibleNode via CdbDispatchUtilityStatement(), then collect each
 *     QE's per-relation result from CdbPgResults extras and feed them to
 *     pg_iceberg_commit_rewrite().
 *   - On QE, this function is a no-op; the rewrite is driven by
 *     pg_iceberg_handle_extensible_utility() invoked from
 *     datalake_ProcessUtility when the dispatched ExtensibleNode arrives.
 */
void
pg_iceberg_relation_vacuum(Relation rel, struct VacuumParams *params,
						   BufferAccessStrategy bstrategy)
{
	PgIcebergVacuumDispatchNode *dispatch_node;
	CdbPgResults	cdb_pgresults = {NULL, 0, 0};
	List		   *all_results = NIL;
	int				i;

	if (Gp_role != GP_ROLE_DISPATCH)
		return;

	dispatch_node = (PgIcebergVacuumDispatchNode *)
		newNode(sizeof(PgIcebergVacuumDispatchNode), T_ExtensibleNode);
	dispatch_node->node.extnodename = PG_ICEBERG_VACUUM_DISPATCH_NODE;
	dispatch_node->relId = RelationGetRelid(rel);
	dispatch_node->tasks =
		pg_iceberg_relation_vacuum_get_dispatch_tasks(rel, params);

	PG_TRY();
	{
		CdbDispatchUtilityStatement((Node *) dispatch_node,
									DF_CANCEL_ON_ERROR | DF_WITH_SNAPSHOT,
									GetAssignedOidsForDispatch(),
									&cdb_pgresults);

		for (i = 0; i < cdb_pgresults.numResults; i++)
		{
			struct pg_result *pgresult = cdb_pgresults.pg_results[i];
			List	   *qe_results;

			if (pgresult->extras == NULL ||
				pgresult->extraType != PGExtraTypeVacuumPrivate)
				continue;

			qe_results = (List *) stringToNode((char *) pgresult->extras);
			all_results = lappend(all_results, qe_results);
		}

		if (all_results != NIL)
			pg_iceberg_commit_rewrite(rel, all_results);
	}
	PG_FINALLY();
	{
		cdbdisp_clearCdbPgResults(&cdb_pgresults);
	}
	PG_END_TRY();
}

/*
 * pg_iceberg_execute_rewrite:
 *
 * QE-side executor for vacuum rewrite.
 */
char *
pg_iceberg_execute_rewrite(Relation rel, List *vacuum_am_private)
{
	TableScanDesc	 scan_desc = NULL;
	CustomScanState *scan_state = NULL;
	CustomScan		*scan_plan = NULL;
	TupleTableSlot	*scan_slot = NULL;
	IcebergModifyDesc *insert_desc;
	char *added_result_json;
	char *rewritten_fragments_json;
	char *result_json;

	/*
	 * Snapshot rewrite source fragments before scan/insert path to avoid
	 * potential side effects from downstream execution.
	 */
	rewritten_fragments_json =
		pg_iceberg_rewrite_collect_local_input_fragments_json(vacuum_am_private);

	scan_desc = pg_iceberg_begin_vacuum_scan(rel,
											 vacuum_am_private,
											 &scan_state,
											 &scan_plan,
											 &scan_slot);

	insert_desc = pg_iceberg_modify_init_for_vacuum(rel, CMD_INSERT);

	while (pg_iceberg_getnextslot(scan_desc, ForwardScanDirection, scan_slot))
	{
		pg_iceberg_tuple_insert(insert_desc,
								scan_slot,
								InvalidCommandId,
								0,
								NULL);
		ExecClearTuple(scan_slot);
	}

	pg_iceberg_endscan(scan_desc);
	ExecDropSingleTupleTableSlot(scan_slot);
	pfree(scan_state);
	pfree(scan_plan);

	/*
	 * Build QE->QD rewrite result in grouped shape:
	 *   - fragments: newly written files from this QE
	 *   - rewrittenFragments: source files assigned to this QE's rewrite tasks
	 */
	added_result_json = pg_iceberg_modify_finish_for_vacuum(insert_desc);
	result_json = pg_iceberg_rewrite_build_qe_result(added_result_json,
													 rewritten_fragments_json);

	if (added_result_json != NULL)
		pfree(added_result_json);
	if (rewritten_fragments_json != NULL)
		pfree(rewritten_fragments_json);

	return result_json;
}

/*
 * pg_iceberg_relation_vacuum_get_dispatch_tasks:
 *
 * Returns a list of dispatch tasks for vacuuming an Iceberg relation.
 */
List *
pg_iceberg_relation_vacuum_get_dispatch_tasks(Relation rel, struct VacuumParams *params)
{
	int min_files = pg_iceberg_vacuum_compact_min_input_files;
	int target_mb = pg_iceberg_vacuum_rewrite_target_file_size_mb;
	IcebergTableInfo *table_info;
	char *metadata_location;
	char *plan_json;
	bool is_internal;
	List *tasks = NIL;
	List *plan_private = NIL;
	int segment_count = getgpsegmentCount();

	table_info = pg_iceberg_get_table_info(RelationGetRelid(rel));
	is_internal =
		(table_info->opts == NULL || table_info->opts->table == NULL);
	metadata_location = pg_iceberg_get_latest_metadata_location(
		RelationGetRelid(rel), table_info);

	/*
	 * Keep vacuum private data compatible with scan am_private layout:
	 *   [table_metadata, combined_task_0, combined_task_1, ..., optional extras, segment flags]
	 */
	plan_json = pg_iceberg_get_rewrite_plan_with_catalog(rel,
														 table_info,
														 metadata_location,
														 is_internal,
														 params,
														 min_files,
														 target_mb);

	if (plan_json != NULL)
	{
		plan_private = parseIcebergFragmentResponse(plan_json, strlen(plan_json));
		pfree(plan_json);
	}

	if (plan_private != NIL)
	{
		tasks = list_concat(tasks, plan_private);
	}
	else
	{
		tasks = lappend(tasks, makeNode(ExternalTableMetadata));
	}

	/*
	 * Polaris and random segment distribution logic, similar to scan.
	 */
	if (!is_internal && checkIsPolarisCatalog(table_info->catalog_server_name, table_info->catalog_name))
	{
		IcebergLoadTableResult *load_result;

		load_result = pg_iceberg_load_table(table_info->opts->catalog,
											table_info->opts->namespace,
											table_info->opts->table,
											table_info->catalog_server_name,
											table_info->catalog_name,
											table_info->volume_server_name,
											table_info->volume_name);

		if (load_result == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("iceberg table \"%s.%s\" does not exist in external catalog \"%s\"",
							table_info->opts->namespace,
							table_info->opts->table,
							table_info->catalog_name)));

		if (load_result->catalog_properties)
		{
			tasks = lappend(tasks, makeString(load_result->catalog_properties));
			/* Ownership transferred to tasks entry. */
			load_result->catalog_properties = NULL;
		}
		pg_iceberg_free_load_table_result(load_result);
	}

	tasks = list_concat(tasks,
		datalakeSelectRandomSegments(segment_count, external_table_limit_segment_num));

	pg_iceberg_free_table_info(table_info);
	pfree(metadata_location);

	return tasks;
}

