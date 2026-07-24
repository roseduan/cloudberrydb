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
#include <math.h>

#include "access/sysattr.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/oid_dispatch.h"
#include "catalog/pg_statistic.h"
#include "commands/vacuum.h"
#include "libpq/libpq-int.h"
#include "utils/guc.h"
#include "utils/guc_tables.h"
#include "utils/sampling.h"
#include "common/base64.h"
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
#include "access/htup_details.h"
#include "nodes/plannodes.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "utils/hsearch.h"
#include "mb/pg_wchar.h"

#include "utils/array.h"
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
#include "../dlproxy/filters.h"
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

/*
 * iceberg_get_needed_attrs
 *
 * Column projection pushdown for the Iceberg AM scan: return only the
 * attributes actually referenced by the scan node's target list and quals,
 * so the reader decodes just those columns instead of every column in the
 * table.  Without this the reader always materialized the whole row (see the
 * previous unconditional iceberg_get_all_attrs() call), so even SELECT count(*)
 * paid to decode every column -- including the wide numeric/decimal columns.
 *
 * Falls back to all attributes when the plan is unavailable or references the
 * whole row (varattno 0).  An empty result (e.g. count(*)) is intentional and
 * correct: the reader drives its row count from the row-group metadata
 * (num_rows), so it still emits the right number of (empty) tuples without
 * opening any column scanner.
 */
static List *
iceberg_get_needed_attrs(Relation rel, struct PlanState *ps)
{
	List	   *attrs = NIL;
	TupleDesc	tupdesc = RelationGetDescr(rel);
	int			natts = tupdesc->natts;
	Bitmapset  *needed = NULL;
	Index		scanrelid;
	int			i;

	/* No plan context: read every column. */
	if (ps == NULL || ps->plan == NULL || !IsA(ps->plan, CustomScan))
		return iceberg_get_all_attrs(rel);

	scanrelid = ((Scan *) ps->plan)->scanrelid;

	/*
	 * ANALYZE sampling and VACUUM rewrite build a bare CustomScan
	 * (pg_iceberg_begin_vacuum_scan) with no target list and scanrelid 0 to
	 * carry the fragment payload; they read whole rows and need every column.
	 * Only a real planner scan (scanrelid >= 1) carries a target list/qual
	 * that bounds the referenced columns, so projection is only safe there.
	 */
	if (scanrelid == 0)
		return iceberg_get_all_attrs(rel);

	pull_varattnos((Node *) ps->plan->targetlist, scanrelid, &needed);
	pull_varattnos((Node *) ps->plan->qual, scanrelid, &needed);

	/* A whole-row reference needs every column. */
	if (bms_is_member(0 - FirstLowInvalidHeapAttributeNumber, needed))
	{
		bms_free(needed);
		return iceberg_get_all_attrs(rel);
	}

	for (i = 0; i < natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

		if (attr->attisdropped)
			continue;
		if (bms_is_member((i + 1) - FirstLowInvalidHeapAttributeNumber, needed))
			attrs = lappend_int(attrs, i + 1);
	}

	bms_free(needed);
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
		iceberg_get_needed_attrs(scanDesc->rs_base.rs_rd, ps), am_private
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
		am_private = pg_iceberg_materialize_am_private(
			((CustomScan *) ps->plan)->custom_private);

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

	/* Defensive: an FDW without a ReScan callback must not crash the segment. */
	if (fdw_routine->ReScanForeignScan != NULL)
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

/*
 * Fetch the current snapshot's fragment list for `rel` from the datalake
 * agent and return it as the agent's JSON wire format (which carries the
 * delete files deduplicated in a global "deleteFiles" array referenced by
 * index from each task).  Must run on the QD.
 */
static char *
iceberg_fetch_fragments_json(Relation rel, IcebergTableInfo *table_info,
							 const char *pushdown_filter,
							 bool *is_internal_out)
{
	char			   *fragments;
	char			   *scan_metadata_location;
	bool				is_internal;
	TableMetadataState *tstate;

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
													  pushdown_filter);

	pfree(scan_metadata_location);

	if (is_internal_out)
		*is_internal_out = is_internal;

	return fragments;
}

/*
 * pg_iceberg_materialize_am_private
 *
 * The QD ships the fragment list inside the plan as a single JSON String
 * (the agent's deduplicated wire format) rather than as an expanded
 * FileScanTask node tree: nodeToString() has no notion of shared nodes, so
 * it would copy every delete-file FileFragment into each task referencing
 * it.  On an unpartitioned table every delete file applies to every older
 * data file, which makes the serialized plan -- and its deserialized copy
 * in every QE's MessageContext -- O(data files x delete files) and was
 * observed at hundreds of MB per backend (issue #362).
 *
 * Detect the String carrier here, at scan open, and parse it back into the
 * in-memory form ([ExternalTableMetadata, combinedTask lists..., trailers]);
 * the parsed form re-shares the delete fragments via the deleteIndexes in
 * the JSON.  A list that already starts with ExternalTableMetadata (e.g.
 * the VACUUM/ANALYZE sampling paths build it directly) passes through.
 */
List *
pg_iceberg_materialize_am_private(List *am_private)
{
	char	   *json;
	List	   *parsed;

	if (am_private == NIL || !IsA(linitial(am_private), String))
		return am_private;

	json = strVal(linitial(am_private));
	parsed = parseIcebergFragmentResponse(json, strlen(json));

	/* Re-append the trailers (catalog properties / segment selection). */
	return list_concat(parsed, list_copy_tail(am_private, 1));
}

/*
 * True if `rel` has any dropped columns.  The serialized scan filter indexes
 * columns by physical varattno-1, while the filterColumns array shipped to the
 * agent is built from the Iceberg live schema (dropped PG attributes absent);
 * with a dropped column the two no longer line up, so a predicate could resolve
 * to the wrong column.  Callers skip predicate pushdown in that case.
 */
static bool
iceberg_rel_has_dropped_attrs(Relation rel)
{
	TupleDesc	tupdesc = RelationGetDescr(rel);
	int			i;

	for (i = 0; i < tupdesc->natts; i++)
		if (TupleDescAttr(tupdesc, i)->attisdropped)
			return true;
	return false;
}

List *
pg_iceberg_build_scan_am_private(Relation rel, struct PlanState *ps, int random_segment_num)
{
	List			   *am_private = NIL;
	char			   *fragments = NULL;
	char			   *pushdown_filter = NULL;
	bool				is_internal;
	IcebergTableInfo   *table_info;
	int					segment_count = getgpsegmentCount();

	table_info = pg_iceberg_get_table_info(RelationGetRelid(rel));

	/*
	 * Predicate pushdown: serialize the scan node's restriction quals into the
	 * dlproxy filter wire format so the agent can prune whole Iceberg data
	 * files via manifest column bounds at planning time.  serializeDlProxyFilterQuals
	 * is all-or-nothing -- it returns NULL if any qual is unsupported -- so a
	 * non-NULL result encodes the full AND of the scan restriction and can
	 * never drop a matching file.  The executor still re-applies plan->qual.
	 */
	if (pg_iceberg_enable_predicate_pushdown &&
		ps != NULL && ps->plan != NULL && ps->plan->qual != NIL &&
		!iceberg_rel_has_dropped_attrs(rel))
		pushdown_filter = serializeDlProxyFilterQuals(ps->plan->qual);

	fragments = iceberg_fetch_fragments_json(rel, table_info, pushdown_filter,
											 &is_internal);

	/*
	 * Carry the raw JSON in the plan; pg_iceberg_materialize_am_private()
	 * parses it at scan open (see commentary there -- issue #362).
	 */
	am_private = lappend(am_private, makeString(fragments));

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
 * pg_iceberg_list_data_fragments_json
 *		Return the raw fragment payload for the current Iceberg snapshot of
 *		`rel` as the agent's JSON wire format (no trailers).
 *
 * Caller MUST run this on the QD: it consults the local pg_iceberg_metadata
 * catalog (which is QD-only populated) via pg_iceberg_get_metadata_info().
 * Used by the planner-time hook to seed the per-statement modify-fragments
 * cache that is then dispatched to every QE (issue #333).  The JSON keeps
 * delete files deduplicated, so it is also the plan-carrier format that
 * avoids the O(data files x delete files) serialization blowup (issue #362);
 * parse with parseIcebergFragmentResponse() where the List form is needed.
 */
char *
pg_iceberg_list_data_fragments_json(Relation rel)
{
	char			   *fragments;
	IcebergTableInfo   *table_info;

	table_info = pg_iceberg_get_table_info(RelationGetRelid(rel));
	fragments = iceberg_fetch_fragments_json(rel, table_info, NULL, NULL);
	pg_iceberg_free_table_info(table_info);

	return fragments;
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

/* ----------------------------------------------------------------
 * ANALYZE-time fragments cache (issue #352)
 *
 * A process-local map keyed by relid carrying each target relation's
 * fragment-list JSON for an in-flight ANALYZE.  The QD expands every
 * fragment list (the datalake agent is QD-only) and ships them to the
 * QEs as a PgIcebergAnalyzeDispatch ExtensibleNode; the QE handler
 * resets and refills this cache, and pg_iceberg_acquire_sample_rows
 * consumes (and removes) the entry so a stale list cannot be reused
 * by a later statement.
 *
 * The strings live in TopMemoryContext because the dispatch and the
 * sampling query are separate statements on the QE; take hands the
 * string to the caller, which must pfree it.
 * ----------------------------------------------------------------
 */
typedef struct AnalyzeFragmentsEntry
{
	Oid		relid;			/* hash key */
	char   *fragments;		/* fragment-list JSON, in TopMemoryContext */
} AnalyzeFragmentsEntry;

static HTAB *analyze_fragments_cache = NULL;

static void
analyze_fragments_cache_ensure(void)
{
	HASHCTL ctl;

	if (analyze_fragments_cache != NULL)
		return;

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(AnalyzeFragmentsEntry);
	ctl.hcxt = TopMemoryContext;

	analyze_fragments_cache = hash_create("iceberg analyze fragments cache",
										  16, &ctl,
										  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

void
pg_iceberg_stash_analyze_fragments(Oid relid, const char *fragments)
{
	AnalyzeFragmentsEntry  *entry;
	bool					found;

	if (!OidIsValid(relid) || fragments == NULL || fragments[0] == '\0')
		return;

	analyze_fragments_cache_ensure();
	entry = (AnalyzeFragmentsEntry *) hash_search(analyze_fragments_cache,
												  &relid, HASH_ENTER, &found);
	if (found && entry->fragments != NULL)
		pfree(entry->fragments);
	entry->fragments = MemoryContextStrdup(TopMemoryContext, fragments);
}

char *
pg_iceberg_take_analyze_fragments(Oid relid)
{
	AnalyzeFragmentsEntry  *entry;
	char				   *fragments;

	if (analyze_fragments_cache == NULL || !OidIsValid(relid))
		return NULL;

	entry = (AnalyzeFragmentsEntry *) hash_search(analyze_fragments_cache,
												  &relid, HASH_FIND, NULL);
	if (entry == NULL)
		return NULL;

	fragments = entry->fragments;
	hash_search(analyze_fragments_cache, &relid, HASH_REMOVE, NULL);
	return fragments;
}

void
pg_iceberg_reset_analyze_fragments(void)
{
	HASH_SEQ_STATUS			status;
	AnalyzeFragmentsEntry  *entry;

	if (analyze_fragments_cache == NULL)
		return;

	hash_seq_init(&status, analyze_fragments_cache);
	while ((entry = (AnalyzeFragmentsEntry *) hash_seq_search(&status)) != NULL)
	{
		if (entry->fragments != NULL)
			pfree(entry->fragments);
		hash_search(analyze_fragments_cache, &entry->relid, HASH_REMOVE, NULL);
	}
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
BlockNumber
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

	/*
	 * vac_update_relstats writes pg_class with heap_inplace_update; the
	 * invalidation it queues is only executed at the next command counter
	 * increment.  When ANALYZE sampling follows this refresh in the same
	 * command (datalake_ProcessUtility falls through to standard ANALYZE),
	 * do_analyze_rel's own vac_update_relstats would otherwise compare the
	 * sampled row count against a stale syscache copy and skip its write,
	 * leaving our metadata-derived estimate in place even when the exact
	 * sampled count differs (issue #364).  Force the invalidation out now.
	 */
	CommandCounterIncrement();

	pfree(statistics);
	pg_iceberg_free_metadata_info(metadata_info);
	pg_iceberg_free_table_info(table_info);

	return pages;
}

/*
 * pg_iceberg_restore_relpages
 *
 * Standard ANALYZE sampling (which follows the metadata refresh above to
 * populate pg_statistic) finishes with its own vac_update_relstats, whose
 * relpages comes from the AM's dummy local storage -- i.e. 1 -- clobbering
 * the metadata-derived value.  With relpages=1 the planners cost every
 * Iceberg scan as nearly free, distorting plan choice across the board.
 * Re-apply the metadata-derived pages while keeping the sampled reltuples.
 */
void
pg_iceberg_restore_relpages(Oid relid, BlockNumber pages)
{
	Relation	rel;
	double		tuples;

	rel = try_table_open(relid, ShareUpdateExclusiveLock, false);
	if (rel == NULL)
		return;

	tuples = rel->rd_rel->reltuples;

	vac_update_relstats(rel,
						pages,
						tuples,
						0,			/* num_all_visible_pages */
						rel->rd_rel->relhasindex,
						InvalidTransactionId,
						InvalidMultiXactId,
						false,		/* in_outer_xact */
						false);		/* isvacuum */

	CommandCounterIncrement();

	table_close(rel, ShareUpdateExclusiveLock);
}

/*
 * pg_iceberg_snap_high_ndv_stats
 *
 * Iceberg ANALYZE deliberately raises the statistics target to make most NDV
 * estimates more stable.  Near the high-NDV end, however, Duj1 has a cliff:
 * a PAX-sized sample can see no repeated value and report the column unique,
 * while the larger Iceberg sample sees a handful of collision pairs and
 * reports a materially smaller NDV.  That difference is enough to change
 * ORCA join order and HashAgg sizing even though the two tables contain the
 * same data.
 *
 * For negative stadistinct values that imply an NDV above ten percent of the
 * table, invert the pair-dominant Duj1 approximation to estimate the number
 * of collisions in the Iceberg sample.  Collision probability scales with
 * the square of sample size, so project that count back to the session target
 * used by PAX.  If the projected count is below one, preserve plan parity by
 * recording the same unique verdict PAX would very likely have produced.
 * Other statistics from the larger Iceberg sample remain untouched.
 */
void
pg_iceberg_snap_high_ndv_stats(Oid relid, int used_target, int base_target)
{
	Relation	rel;
	Relation	statrel;
	TupleDesc	reldesc;
	double		totalrows;
	double		target_ratio;
	int			i;

	Assert(Gp_role == GP_ROLE_DISPATCH);
	Assert(used_target > base_target);

	rel = try_table_open(relid, AccessShareLock, false);
	if (rel == NULL)
		return;

	totalrows = rel->rd_rel->reltuples;
	if (totalrows <= 0)
	{
		table_close(rel, AccessShareLock);
		return;
	}

	reldesc = RelationGetDescr(rel);
	statrel = table_open(StatisticRelationId, RowExclusiveLock);
	target_ratio = (double) base_target / (double) used_target;

	for (i = 0; i < reldesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(reldesc, i);
		HeapTuple	oldtuple;
		HeapTuple	newtuple;
		Form_pg_statistic stats;
		Datum		values[Natts_pg_statistic];
		bool		nulls[Natts_pg_statistic];
		bool		replaces[Natts_pg_statistic];
		float4		old_distinct;
		float4		new_distinct;
		double		sample_rows;
		double		d_hat;
		double		collisions;
		double		base_collisions;
		int			column_target;
		int			ndv_slot;
		int			j;

		if (attr->attisdropped)
			continue;

		oldtuple = SearchSysCache3(STATRELATTINH,
									   ObjectIdGetDatum(relid),
									   Int16GetDatum(attr->attnum),
									   BoolGetDatum(false));
		if (!HeapTupleIsValid(oldtuple))
			continue;

		stats = (Form_pg_statistic) GETSTRUCT(oldtuple);
		old_distinct = stats->stadistinct;
		if (old_distinct <= -1.0 || old_distinct > -0.10)
		{
			ReleaseSysCache(oldtuple);
			continue;
		}

		column_target = attr->attstattarget >= 0 ?
			attr->attstattarget : used_target;
		if (column_target <= 0)
		{
			ReleaseSysCache(oldtuple);
			continue;
		}

		sample_rows = 300.0 * (double) column_target;
		if (totalrows <= sample_rows)
		{
			ReleaseSysCache(oldtuple);
			continue;
		}

		d_hat = -((double) old_distinct) * totalrows;
		collisions = (sample_rows * sample_rows / d_hat -
					  sample_rows * sample_rows / totalrows) / 2.0;
		if (collisions < 0)
			collisions = 0;

		/*
		 * A column-level statistics target overrides the session default on
		 * both storages, so a PAX-sized sample of such a column is the same
		 * size as ours and sees the same expected collision count.
		 */
		if (attr->attstattarget > 0)
			base_collisions = collisions;
		else
			base_collisions = collisions * target_ratio * target_ratio;
		if (base_collisions >= 1.0)
		{
			ReleaseSysCache(oldtuple);
			continue;
		}

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));
		memset(replaces, 0, sizeof(replaces));

		/* Match compute_scalar_stats()'s nmultiple == 0 convention. */
		new_distinct = -1.0 * (1.0 - stats->stanullfrac);
		values[Anum_pg_statistic_stadistinct - 1] =
			Float4GetDatum(new_distinct);
		replaces[Anum_pg_statistic_stadistinct - 1] = true;

		ndv_slot = -1;
		for (j = 0; j < STATISTIC_NUM_SLOTS; j++)
		{
			if ((&stats->stakind1)[j] == STATISTIC_KIND_NDV_BY_SEGMENTS)
			{
				ndv_slot = j;
				break;
			}
		}

		if (ndv_slot >= 0)
		{
			Datum		ndv = Float8GetDatum(totalrows);
			ArrayType  *ndv_array;

			/*
			 * analyze.c stores this kind as one FLOAT8 value in stavalues,
			 * not as a stanumbers array.  The value is the SUM of per-segment
			 * NDVs (see colNDVBySeg accumulation in analyze.c); ORCA uses it
			 * to cost partial aggregation.  A unique column is locally unique
			 * on every segment, so the per-segment NDVs sum to the table's
			 * row count.  Writing a smaller value makes ORCA believe a
			 * pre-motion partial aggregate reduces rows and pick a streaming
			 * two-stage plan that spills massively on near-unique group keys.
			 */
			ndv_array = construct_array(&ndv, 1, FLOAT8OID, sizeof(float8),
										FLOAT8PASSBYVAL, TYPALIGN_DOUBLE);
			values[Anum_pg_statistic_stavalues1 + ndv_slot - 1] =
				PointerGetDatum(ndv_array);
			replaces[Anum_pg_statistic_stavalues1 + ndv_slot - 1] = true;
		}

		newtuple = heap_modify_tuple(oldtuple,
								 RelationGetDescr(statrel),
								 values, nulls, replaces);
		ReleaseSysCache(oldtuple);
		CatalogTupleUpdate(statrel, &newtuple->t_self, newtuple);
		heap_freetuple(newtuple);
		CommandCounterIncrement();

		ereport(DEBUG1,
				(errmsg("snapped iceberg analyze statistic for %s.%s from %.6g to unique (c=%.6g, c_base=%.6g)",
						RelationGetRelationName(rel), NameStr(attr->attname),
						(double) old_distinct, collisions, base_collisions)));
	}

	table_close(statrel, RowExclusiveLock);
	table_close(rel, AccessShareLock);
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

/* Defined later in this file; reused here as a generic "scan with a
 * dispatched fragment list" helper for ANALYZE sampling. */
static TableScanDesc pg_iceberg_begin_vacuum_scan(Relation rel,
												  List *vacuum_am_private,
												  CustomScanState **out_scan_state,
												  CustomScan **out_scan_plan,
												  TupleTableSlot **out_scan_slot);

/*
 * Build a scan fragment list on the QE for ANALYZE sampling.  The datalake
 * agent runs only on the QD, so the QE cannot enumerate fragments itself;
 * the QD already expanded the list and shipped it as a
 * PgIcebergAnalyzeDispatch ExtensibleNode, whose handler stashed it into
 * the analyze fragments cache.  Here we just consume this relation's entry
 * and parse it -- no agent contact.  Returns NIL when the cache carries no
 * entry for this relation, so the caller can fall back.
 */
static List *
pg_iceberg_build_sample_am_private(Relation rel)
{
	char			   *fragments;
	List			   *am_private;
	int					i;

	fragments = pg_iceberg_take_analyze_fragments(RelationGetRelid(rel));
	if (fragments == NULL)
		return NIL;		/* no entry shipped for this relation */

	am_private = parseIcebergFragmentResponse(fragments, strlen(fragments));

	/*
	 * Append an all-segments-selected trailer directly instead of calling
	 * datalakeSelectRandomSegments(n, n).  That helper always returns all
	 * ones for the n-of-n case anyway, but reaches it through need_random(),
	 * whose srand(time(NULL)) reseeds the process-wide glibc random() state
	 * (srand and srandom share state in glibc).  This runs on every QE, so
	 * within one second every QE ends up with an identical random() stream,
	 * and the Vitter reservoir in pg_iceberg_acquire_sample_rows -- seeded
	 * from random() -- then picks the SAME local row positions on every
	 * segment.  Row position correlates with value order for bulk-loaded
	 * tables, so the merged ANALYZE sample was value-biased (skewed
	 * histograms/MCVs, e.g. date_dim.d_year selectivity off by 2-3x).
	 */
	for (i = 0; i < getgpsegmentCount(); i++)
		am_private = lappend(am_private, makeInteger(1));

	pfree(fragments);

	return am_private;
}

/*
 * pg_iceberg_acquire_sample_rows
 *
 * relation_acquire_sample_rows() implementation for the Iceberg AM (issue
 * #352).  Runs on each QE under the kernel's gp_acquire_sample_rows dispatch.
 *
 * We build this scan's fragment list from the expanded fragment payload the
 * QD shipped (pg_iceberg_build_sample_am_private), scan the segment-local
 * fragments, and Vitter-reservoir-sample up to targrows rows (same algorithm
 * as acquire_sample_rows() in analyze.c).  The kernel then computes real
 * per-column statistics (NDV/MCV/histogram) from the sample.
 *
 * This used to be a no-op: ANALYZE only refreshed pg_class.reltuples from
 * metadata and never populated pg_statistic, which left ORCA without column
 * selectivity and produced catastrophic join orders on low-cardinality
 * predicates (e.g. TPC-DS Q24 c_birth_country = upper(ca_country)).
 */
int
pg_iceberg_acquire_sample_rows(Relation relation, int elevel,
							   HeapTuple *rows, int targrows,
							   double *totalrows, double *totaldeadrows)
{
	List			   *am_private;
	TableScanDesc		scan_desc;
	CustomScanState	   *scan_state = NULL;
	CustomScan		   *scan_plan = NULL;
	TupleTableSlot	   *slot = NULL;
	int					numrows = 0;
	double				samplerows = 0;
	double				rowstoskip = -1;	/* -1 means not set yet */
	ReservoirStateData	rstate;
	MemoryContext		anl_cxt = CurrentMemoryContext;
	MemoryContext		temp_cxt;

	Assert(targrows > 0);

	*totalrows = 0;
	*totaldeadrows = 0;

	/*
	 * Build this scan's fragment list (+ segment-selection trailer) from the
	 * Iceberg catalog, exactly as the executor does for a normal scan.  The
	 * trailer plus GpIdentity.segindex makes each QE read only its own share.
	 */
	am_private = pg_iceberg_build_sample_am_private(relation);
	if (am_private == NIL)
	{
		/*
		 * No fragment list was shipped for this relation: ANALYZE sampling
		 * is disabled, or this analyze did not pass through
		 * datalake_ProcessUtility (e.g. gp_autostats issues ExecVacuum
		 * directly).  The QE cannot read the QD-only Iceberg metadata catalog,
		 * so skip sampling rather than erroring.  Best-effort: return this
		 * segment's reltuples if it happens to be set (it usually is not on a
		 * QE, since the refresh runs only on the QD), so we at least avoid
		 * making things worse than the pre-#352 no-op.  Run a plain
		 * ANALYZE <table> to actually collect column statistics.
		 */
		double		cur = relation->rd_rel->reltuples;

		*totalrows = (cur > 0) ? cur : 0;
		*totaldeadrows = 0;
		ereport(elevel,
				(errmsg("\"%s\": iceberg ANALYZE skipped per-row sampling; no metadata location shipped to this segment",
						RelationGetRelationName(relation))));
		return 0;
	}

	scan_desc = pg_iceberg_begin_vacuum_scan(relation, am_private,
											 &scan_state, &scan_plan, &slot);

	reservoir_init_selection_state(&rstate, targrows);

	/*
	 * Defense in depth: reservoir_init_selection_state() seeds from the
	 * process-wide random() stream, which third-party code can clobber via
	 * srand()/srandom() (glibc shares their state).  If that happens at the
	 * same wall-clock second on every QE, all segments draw identical
	 * reservoir decisions, so every segment samples the same local row
	 * positions and the merged sample becomes value-biased for bulk-loaded
	 * tables.  Mix per-process entropy into the seed so segments always
	 * decorrelate; W must be recomputed the same way the initializer does.
	 */
	sampler_random_init_state(random() ^
							  ((long) MyProcPid << 16) ^
							  (long) GpIdentity.segindex,
							  rstate.randstate);
	rstate.W = exp(-log(sampler_random_fract(rstate.randstate)) / targrows);

	temp_cxt = AllocSetContextCreate(CurrentMemoryContext,
									 "iceberg analyze sample",
									 ALLOCSET_DEFAULT_SIZES);

	while (pg_iceberg_getnextslot(scan_desc, ForwardScanDirection, slot))
	{
		MemoryContext	oldcontext;
		int				pos;

		vacuum_delay_point();

		oldcontext = MemoryContextSwitchTo(temp_cxt);

		if (numrows < targrows)
		{
			/* First targrows rows are always included into the sample. */
			pos = numrows++;
		}
		else
		{
			/*
			 * Vitter's t is the number of rows already processed, so a new
			 * skip count must use samplerows before this row increments it.
			 * This matches acquire_sample_rows() in analyze.c.
			 */
			if (rowstoskip < 0)
				rowstoskip = reservoir_get_next_S(&rstate, samplerows, targrows);

			if (rowstoskip <= 0)
			{
				pos = (int) (targrows * sampler_random_fract(rstate.randstate));
				Assert(pos >= 0 && pos < targrows);
				heap_freetuple(rows[pos]);
			}
			else
				pos = -1;		/* skip this row */

			rowstoskip -= 1;
		}

		if (pos >= 0)
		{
			/* Sample tuples must outlive the scan: copy into the ANALYZE cxt. */
			MemoryContextSwitchTo(anl_cxt);
			rows[pos] = ExecCopySlotHeapTuple(slot);
		}
		samplerows += 1;

		MemoryContextSwitchTo(oldcontext);
		ExecClearTuple(slot);
		MemoryContextReset(temp_cxt);
	}

	pg_iceberg_endscan(scan_desc);
	if (slot)
		ExecDropSingleTupleTableSlot(slot);
	if (scan_state)
		pfree(scan_state);
	if (scan_plan)
		pfree(scan_plan);
	MemoryContextDelete(temp_cxt);

	/*
	 * We scanned every local row, so samplerows is the exact per-segment live
	 * row count; the dispatcher sums these across segments for reltuples.
	 */
	*totalrows = samplerows;
	*totaldeadrows = 0;

	ereport(elevel,
			(errmsg("\"%s\": iceberg ANALYZE scanned %.0f rows, %d rows in sample",
					RelationGetRelationName(relation), samplerows, numrows)));

	return numrows;
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

