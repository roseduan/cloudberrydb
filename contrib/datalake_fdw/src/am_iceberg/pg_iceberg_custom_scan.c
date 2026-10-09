/*-------------------------------------------------------------------------
 *
 * pg_iceberg_custom_scan.c
 *	  CustomScan provider for Iceberg tables.
 *
 *	  Replaces the kernel-level SeqScan.am_private channel with a plugin-
 *	  owned CustomScan node.  Splits / catalog metadata computed on the QD
 *	  are stashed into CustomScan.custom_private, which is serialized by
 *	  the core out/read infrastructure and dispatched to every QE via the
 *	  normal PlannedStmt path.  The QE-side BeginCustomScan reuses the
 *	  existing Iceberg scan initializer.
 *
 *	  A planner_hook runs after standard_planner/ORCA, walks the resulting
 *	  PlannedStmt, and rewrites SeqScan nodes whose target relation uses
 *	  the "iceberg" table AM into Iceberg CustomScan nodes.  This keeps the
 *	  rewrite orthogonal to ORCA vs PG planner selection.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/am_iceberg/pg_iceberg_custom_scan.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/table.h"
#include "catalog/pg_am.h"
#include "catalog/pg_class.h"
#include "cdb/cdbvars.h"
#include "commands/explain.h"
#include "executor/executor.h"
#include "executor/nodeCustom.h"
#include "nodes/execnodes.h"
#include "nodes/extensible.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pg_list.h"
#include "nodes/plannodes.h"
#include "optimizer/planner.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"

#include "include/pg_iceberg_am.h"
#include "include/pg_iceberg_custom_scan.h"
#include "../dlproxy/iceberg_common.h"

#include "access/table.h"
#include "src/datalake_def.h"
#include "src/provider/iceberg/iceberg_file_index.h"

#define ICEBERG_CUSTOM_SCAN_NAME	"Iceberg Scan"

static bool is_iceberg_relation(Oid relid);
static void prepopulate_iceberg_file_index_map(List *fragments);

/*
 * issue #333: walk the plan for ModifyTable nodes whose result relations
 * use the iceberg AM, then list the data fragments and route them to every
 * QE so the global file_id -> file_path map can be populated on writer-only
 * QEs.
 *
 * The same carrier also ships the REST storage envelope: a write on a REST
 * catalog table has no FOREIGN VOLUME to draw object-store credentials from,
 * so what the catalog vended must travel with the plan.  That is why INSERT
 * is walked too -- it needs no fragments, only the envelope.
 *
 * Three carriers in the cache flow:
 *   - QD executor-time:  pg_iceberg_stash_modify_fragments/_envelope(relid, ...)
 *   - dispatched plan:   ModifyTable.fdwPrivLists[i], layout in pg_iceberg_am.h
 *   - QE executor-time:  ExecutorStart_hook re-stashes from fdwPrivLists.
 *
 * The QD side runs at ExecutorStart rather than planner_hook, for the same
 * reason IcebergBeginCustomScan recomputes the scan splits on every execution
 * (see its comment): a PlannedStmt can be cached and re-executed -- PREPARE /
 * EXECUTE, a generic plan, an SPI plan inside PL/pgSQL -- and the planner runs
 * only once for all of those.  Producing the carrier at plan time therefore
 * pinned both the snapshot AND the vended object-store credentials to the
 * first execution, so a loop that outlived the credential TTL kept
 * re-dispatching keys that had already expired.  ExecutorStart runs before
 * InitPlan, hence before CdbDispatchPlan serialises the plan to the QEs, so
 * refreshing here still reaches every segment.
 */
/*
 * fdwPrivLists holds one entry per result relation, in the same order as
 * resultRelations.  Standard PlanForeignModify fills it for foreign tables;
 * for native iceberg-AM relations it is left NIL, so the slots have to be
 * created before one can be assigned.
 *
 * Called only once something is actually going to be stored: INSERT reaches
 * this walker now, and every INSERT in the database goes through it, so a
 * plan with nothing to carry must come out byte-identical to before.  When
 * PlanForeignModify already built the list this is a no-op and cannot
 * overwrite what it put there.
 *
 * The list cells are allocated in the plan's own MemoryContext: for a cached
 * plan this walker runs once per execution while the list has to live as long
 * as the plan does, so the ambient (statement-scoped) context would leave
 * mt->fdwPrivLists dangling as soon as the statement ended.
 */
static void
ensure_modify_priv_slots(ModifyTable *mt, MemoryContext plancxt)
{
	MemoryContext	oldcxt;
	int				rel_count;
	int				i;

	if (mt->fdwPrivLists != NIL)
		return;

	oldcxt = MemoryContextSwitchTo(plancxt);
	rel_count = list_length(mt->resultRelations);
	for (i = 0; i < rel_count; i++)
		mt->fdwPrivLists = lappend(mt->fdwPrivLists, NIL);
	MemoryContextSwitchTo(oldcxt);
}

/*
 * Release the carrier this walker installed on a previous execution of the
 * same cached plan.
 *
 * Freeing matters because the replacement is allocated in the plan's context,
 * which for a cached plan is not reset between executions -- a PL/pgSQL loop
 * would otherwise accumulate one fragments JSON (issue #362 sized these in
 * hundreds of MB) plus one envelope per iteration for the life of the plan.
 *
 * Only the slot of an iceberg result relation reaches this, and datalake_fdw
 * implements no PlanForeignModify, so nothing but this walker ever writes one.
 * The List carrier of the pre-#362 layout is skipped rather than freed: it is
 * only ever produced on a QE, which does not run this path.
 */
static void
release_modify_priv_slot(List *priv)
{
	ListCell   *lc;

	if (priv == NIL)
		return;

	foreach(lc, priv)
	{
		Node   *node = (Node *) lfirst(lc);

		if (node == NULL || !IsA(node, String))
			continue;
		if (strVal(node) != NULL)
			pfree(strVal(node));
		pfree(node);
	}
	list_free(priv);
}

static void
stash_modify_private_for_iceberg_modifies(PlannedStmt *stmt)
{
	ModifyTable	   *mt;
	ListCell	   *lc;
	MemoryContext	plancxt;
	int				i;

	if (stmt->planTree == NULL || !IsA(stmt->planTree, ModifyTable))
		return;

	mt = (ModifyTable *) stmt->planTree;
	if (mt->operation != CMD_INSERT &&
		mt->operation != CMD_UPDATE &&
		mt->operation != CMD_DELETE)
		return;

	/*
	 * fdwPrivLists holds one entry per result relation, in the same order
	 * as resultRelations.  Standard PlanForeignModify fills it for foreign
	 * tables; for native iceberg-AM relations the slot is NIL.  We fill
	 * those slots here so the dispatched plan carries what the segments
	 * cannot ask the agent for themselves: the fragment list (UPDATE/DELETE
	 * only), the catalog properties every operation needs (issue #410), and,
	 * on a REST catalog, the storage envelope.
	 *
	 * Whatever is stored into the plan has to live exactly as long as the plan
	 * does.  For a one-shot plan that is the ambient context anyway; for a
	 * cached one it is the CachedPlan's context, which outlives this
	 * statement.
	 */
	plancxt = GetMemoryChunkContext(mt);

	i = 0;
	foreach(lc, mt->resultRelations)
	{
		Index	rti = lfirst_int(lc);
		Oid		relid;
		char   *fragments_json = NULL;
		char   *catalog_properties;
		char   *envelope;
		Relation rel;
		List   *priv = NIL;
		MemoryContext oldcxt;

		if (rti == 0 || rti > list_length(stmt->rtable))
		{
			i++;
			continue;
		}

		relid = ((RangeTblEntry *) list_nth(stmt->rtable, rti - 1))->relid;

		if (!is_iceberg_relation(relid))
		{
			i++;
			continue;
		}

		rel = table_open(relid, NoLock);

		/*
		 * INSERT touches no existing file, so it never lists fragments: that
		 * listing is what issue #362 had to make cheap, and paying it here
		 * would undo that for the commonest write.
		 *
		 * 0 is the current snapshot; a modify never time-travels.
		 */
		if (mt->operation == CMD_UPDATE || mt->operation == CMD_DELETE)
			fragments_json = pg_iceberg_list_data_fragments_json(rel, 0);

		catalog_properties = pg_iceberg_get_catalog_properties_json(rel);
		envelope = pg_iceberg_get_modify_rest_envelope(rel, fragments_json);
		table_close(rel, NoLock);

		/* QD-local caches first; these live in the statement's context. */
		if (catalog_properties != NULL)
			pg_iceberg_stash_modify_catalog_properties(relid, catalog_properties);

		if (fragments_json != NULL)
		{
			List	 *fragments;

			/* QD-local consumers need the parsed (delete-sharing) form. */
			fragments = parseIcebergFragmentResponse(fragments_json,
													 strlen(fragments_json));
			pg_iceberg_stash_modify_fragments(relid, fragments);

			/*
			 * The QD used to reach this through the ExecutorStart hook's
			 * restash pass, which no longer runs here now that the QD
			 * produces the carrier directly.
			 */
			prepopulate_iceberg_file_index_map(fragments);
		}

		if (envelope != NULL)
			pg_iceberg_stash_modify_envelope(relid, envelope);

		/*
		 * Everything spliced into the plan is built in the plan's context and
		 * owns its own copy of the strings; the originals above belong to the
		 * statement and go away with it.
		 */
		oldcxt = MemoryContextSwitchTo(plancxt);

		if (fragments_json != NULL || catalog_properties != NULL)
		{
			/*
			 * Splice the fragments into ModifyTable.fdwPrivLists so they get
			 * dispatched with the plan.  Ship the raw JSON (one String node)
			 * rather than the parsed FileScanTask tree: nodeToString() would
			 * duplicate every shared delete-file fragment into each task
			 * referencing it, making the dispatched plan and every QE's
			 * MessageContext O(data files x delete files) -- observed at
			 * ~500MB per backend on zipper-style workloads (issue #362).
			 * The slot is one list per relation (per ModifyTable contract),
			 * holding the fragments JSON then the catalog properties; the
			 * consumer unwraps both in
			 * restash_iceberg_modify_private_from_plan.  Either may be
			 * absent -- INSERT has no fragments, a builtin catalog vends no
			 * properties -- and an absent one travels as an empty string so
			 * the positions stay fixed.
			 */
			priv = list_make2(
				makeString(pstrdup(fragments_json != NULL ? fragments_json : "")),
				makeString(pstrdup(catalog_properties != NULL ? catalog_properties : "")));
		}

		if (envelope != NULL)
		{
			/*
			 * Positions 0-1 stay the fragments and catalog-properties slots
			 * even when both are empty, so the pre-existing consumer keeps
			 * reading each where it always did; the tagged pair after them is
			 * what a non-REST plan omits entirely.
			 *
			 * The placeholders are empty strings rather than NULL because the
			 * consumer treats slot 1 as the catalog properties positionally:
			 * shifting the tag into that slot would have it stashed as the
			 * properties of a REST write, which has none.
			 */
			if (priv == NIL)
				priv = list_make2(makeString(pstrdup("")),
								  makeString(pstrdup("")));

			priv = lappend(priv,
						   makeString(pstrdup(ICEBERG_MODIFY_PRIV_ENVELOPE_TAG)));
			priv = lappend(priv, makeString(pstrdup(envelope)));
		}

		MemoryContextSwitchTo(oldcxt);

		/*
		 * Drop what a previous execution of this same cached plan left here,
		 * then install whatever this execution produced -- including nothing.
		 * Nothing has read the old carrier yet this statement: this walker
		 * runs ahead of InitPlan.
		 *
		 * The clear happens even when priv is NIL, which is the whole point of
		 * refreshing here: an execution that finds no fragments (the table has
		 * since been emptied) must not re-dispatch the previous execution's
		 * file list, and on a REST catalog must not re-dispatch its
		 * credentials once they are past their TTL.  Leaving the slot NIL is
		 * exactly what a freshly planned statement would carry.
		 */
		ensure_modify_priv_slots(mt, plancxt);
		release_modify_priv_slot(
			(List *) lfirst(list_nth_cell(mt->fdwPrivLists, i)));
		lfirst(list_nth_cell(mt->fdwPrivLists, i)) = priv;

		i++;
	}
}

extern int external_table_limit_segment_num;


/* ------------------------------------------------------------------------
 * Executor-time state
 * ------------------------------------------------------------------------
 */
typedef struct IcebergCustomScanState
{
	CustomScanState	 css;			/* must be first */
	TableScanDesc	 scanDesc;
	bool			 vectorize;		/* EXEC_FLAG_VECTOR seen at BeginCustomScan */
} IcebergCustomScanState;


/* ------------------------------------------------------------------------
 * Forward declarations
 * ------------------------------------------------------------------------
 */
static Node *IcebergCreateCustomScanState(CustomScan *cscan);
static void IcebergBeginCustomScan(CustomScanState *node, EState *estate,
								   int eflags);
static TupleTableSlot *IcebergExecCustomScan(CustomScanState *node);
static void IcebergEndCustomScan(CustomScanState *node);
static void IcebergReScanCustomScan(CustomScanState *node);
static void IcebergExplainCustomScan(CustomScanState *node, List *ancestors,
									 ExplainState *es);

static CustomScanMethods IcebergCustomScanMethods = {
	ICEBERG_CUSTOM_SCAN_NAME,
	IcebergCreateCustomScanState,
};

static CustomExecMethods IcebergCustomExecMethods = {
	.CustomName			= ICEBERG_CUSTOM_SCAN_NAME,
	.BeginCustomScan	= IcebergBeginCustomScan,
	.ExecCustomScan		= IcebergExecCustomScan,
	.EndCustomScan		= IcebergEndCustomScan,
	.ReScanCustomScan	= IcebergReScanCustomScan,
	.ExplainCustomScan	= IcebergExplainCustomScan,
};


/* ------------------------------------------------------------------------
 * Iceberg AM OID cache
 *
 * The "iceberg" access method OID is stable across a session; cache it
 * on first lookup.  DROP/CREATE ACCESS METHOD within a running session
 * is not a supported workflow, so we do not install an invalidation
 * callback.
 * ------------------------------------------------------------------------
 */
static Oid iceberg_am_oid_cache = InvalidOid;

static Oid
get_relation_relam(Oid relid)
{
	HeapTuple	tup;
	Oid			relam;

	tup = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tup))
		return InvalidOid;

	relam = ((Form_pg_class) GETSTRUCT(tup))->relam;
	ReleaseSysCache(tup);
	return relam;
}

static bool
is_iceberg_relation(Oid relid)
{
	Oid			relam;

	if (!OidIsValid(relid))
		return false;

	if (!OidIsValid(iceberg_am_oid_cache))
	{
		iceberg_am_oid_cache = GetSysCacheOid1(AMNAME, Anum_pg_am_oid,
											   CStringGetDatum("iceberg"));
		if (!OidIsValid(iceberg_am_oid_cache))
			return false;
	}

	relam = get_relation_relam(relid);
	return OidIsValid(relam) && relam == iceberg_am_oid_cache;
}


/* ------------------------------------------------------------------------
 * Walker: replace Iceberg SeqScan with CustomScan
 * ------------------------------------------------------------------------
 */
static Plan *replace_iceberg_seqscan(Plan *plan, List *rtable);

static void
replace_iceberg_seqscan_list(List *plans, List *rtable)
{
	ListCell   *lc;

	foreach(lc, plans)
		lfirst(lc) = replace_iceberg_seqscan((Plan *) lfirst(lc), rtable);
}

static Plan *
make_iceberg_custom_scan(SeqScan *seq)
{
	CustomScan *cscan = makeNode(CustomScan);

	/*
	 * Struct-copy the Plan header (targetlist, qual, lefttree, costs, flow,
	 * directDispatch, parallel flags, …) then fix up node tag and CustomScan-
	 * specific fields.
	 */
	cscan->scan.plan = seq->scan.plan;
	cscan->scan.plan.type = T_CustomScan;
	cscan->scan.scanrelid = seq->scan.scanrelid;

	cscan->flags			= 0;
	cscan->custom_plans		= NIL;
	cscan->custom_exprs		= NIL;
	/*
	 * custom_private (the Iceberg file split list) is populated per-execution
	 * on the QD inside IcebergBeginCustomScan, mirroring the original kernel
	 * behaviour where nodeSeqscan.c called table_scan_get_am_private() in
	 * ExecInitSeqScan.  Leaving it NIL at plan time keeps cached PlannedStmts
	 * (prepared statements, plpgsql plans) free of stale splits.
	 */
	cscan->custom_private	= NIL;
	cscan->custom_scan_tlist = NIL;		/* scan tuple = relation tupdesc */
	cscan->custom_relids	= bms_make_singleton(seq->scan.scanrelid);
	cscan->methods			= &IcebergCustomScanMethods;

	return (Plan *) cscan;
}

static Plan *
replace_iceberg_seqscan(Plan *plan, List *rtable)
{
	if (plan == NULL)
		return NULL;

	if (IsA(plan, SeqScan))
	{
		SeqScan		   *seq = (SeqScan *) plan;
		RangeTblEntry  *rte;

		if (seq->scan.scanrelid > 0 &&
			seq->scan.scanrelid <= list_length(rtable))
		{
			rte = rt_fetch(seq->scan.scanrelid, rtable);
			if (rte->rtekind == RTE_RELATION &&
				is_iceberg_relation(rte->relid))
				return make_iceberg_custom_scan(seq);
		}
	}
	else if (IsA(plan, FunctionScan))
	{
		/*
		 * Time travel: a schema-changed iceberg_snapshot_scan() call stays a
		 * FunctionScan (the main path rewrites schema-unchanged calls to a
		 * relation SeqScan handled above).  Rewrite it into a self-contained
		 * Iceberg CustomScan (leaf node -- no children to recurse into).
		 */
		Plan	   *swapped = iceberg_tt_try_make_custom_scan(plan, rtable);

		if (swapped != plan)
			return swapped;
	}

	/* Recurse common plan tree structure. */
	plan->lefttree = replace_iceberg_seqscan(plan->lefttree, rtable);
	plan->righttree = replace_iceberg_seqscan(plan->righttree, rtable);

	if (IsA(plan, Append))
		replace_iceberg_seqscan_list(((Append *) plan)->appendplans, rtable);
	else if (IsA(plan, MergeAppend))
		replace_iceberg_seqscan_list(((MergeAppend *) plan)->mergeplans, rtable);
	else if (IsA(plan, BitmapAnd))
		replace_iceberg_seqscan_list(((BitmapAnd *) plan)->bitmapplans, rtable);
	else if (IsA(plan, BitmapOr))
		replace_iceberg_seqscan_list(((BitmapOr *) plan)->bitmapplans, rtable);
	else if (IsA(plan, SubqueryScan))
	{
		SubqueryScan *sub = (SubqueryScan *) plan;

		sub->subplan = replace_iceberg_seqscan(sub->subplan, rtable);
	}
	/*
	 * GPDB ORCA generates Sequence nodes for CTEs and shared input scans;
	 * its child plan trees live in Sequence.subplans rather than the generic
	 * lefttree/righttree slots, so we must recurse into them explicitly or
	 * Iceberg SeqScans inside CTEs end up un-rewritten and crash at
	 * BeginForeignScan with a truncated fdw_private list.
	 */
	else if (IsA(plan, Sequence))
		replace_iceberg_seqscan_list(((Sequence *) plan)->subplans, rtable);
	/*
	 * ModifyTable in this GPDB branch carries its single child subplan in
	 * plan->lefttree; no additional handling needed beyond the generic
	 * lefttree/righttree recursion above.
	 */

	return plan;
}


/* ------------------------------------------------------------------------
 * planner_hook
 *
 * standard_planner (or a previous hook) is called first.  The returned
 * PlannedStmt is then walked and any SeqScan targeting an Iceberg table
 * is rewritten in-place as an Iceberg CustomScan.  The rewrite is
 * independent of whether the plan was produced by PG planner or ORCA.
 * ------------------------------------------------------------------------
 */
static planner_hook_type prev_planner_hook = NULL;

static PlannedStmt *
iceberg_planner_hook(Query *parse,
					 const char *query_string,
					 int cursorOptions,
					 ParamListInfo boundParams,
					 OptimizerOptions *optimizer_options)
{
	PlannedStmt		   *stmt;
	ListCell		   *lc;
	IcebergTTStatsFrame	tt_stats;

	/*
	 * Time travel: resolve the per-snapshot row counts this statement needs
	 * before planning, so get_relation_info_hook can hand them to the planner
	 * without talking to the catalog service from inside it (issue #413).  The
	 * frame lives on this stack frame and is popped on every exit path, which
	 * is what makes nested planning and cached-plan re-planning safe.
	 *
	 * _begin runs inside the PG_TRY because it can raise (a cancel arriving
	 * during the prefetch is re-thrown); it pushes the frame before doing any
	 * work, so _end pops it on that path too.
	 */
	PG_TRY();
	{
		iceberg_tt_stats_begin(&tt_stats, parse);

		if (prev_planner_hook != NULL)
			stmt = prev_planner_hook(parse, query_string, cursorOptions,
									 boundParams, optimizer_options);
		else
			stmt = standard_planner(parse, query_string, cursorOptions,
									boundParams, optimizer_options);
	}
	PG_FINALLY();
	{
		iceberg_tt_stats_end(&tt_stats);
	}
	PG_END_TRY();

	/*
	 * Rewrite only on the QD: QE receives an already-planned PlannedStmt
	 * and never re-enters the planner.
	 */
	if (Gp_role != GP_ROLE_DISPATCH)
		return stmt;

	stmt->planTree = replace_iceberg_seqscan(stmt->planTree, stmt->rtable);

	foreach(lc, stmt->subplans)
	{
		Plan	   *sub = (Plan *) lfirst(lc);

		lfirst(lc) = replace_iceberg_seqscan(sub, stmt->rtable);
	}

	/*
	 * The write-side carrier is deliberately NOT built here; see the comment
	 * above stash_modify_private_for_iceberg_modifies.  It is produced from
	 * the ExecutorStart hook so that a cached PlannedStmt re-dispatches fresh
	 * fragments and fresh vended credentials on every execution.
	 */

	/*
	 * Time travel: resolve and inject the fragment list for every
	 * iceberg_snapshot_scan() call (QD-only, like fragment resolution above).
	 */
	iceberg_tt_inject_fragments(stmt->planTree);
	foreach(lc, stmt->subplans)
		iceberg_tt_inject_fragments((Plan *) lfirst(lc));

	return stmt;
}


/* ------------------------------------------------------------------------
 * ExecutorStart_hook (issue #333)
 *
 * Runs on every segment.  For ModifyTable nodes on iceberg-AM relations,
 * re-stash the per-relation fragments from ModifyTable.fdwPrivLists into
 * the process-local cache so iceberg_modify_init can find them.
 * ------------------------------------------------------------------------
 */
static ExecutorStart_hook_type prev_executor_start_hook = NULL;

/*
 * Pre-populate the global datalake_iceberg_file_index_map from a fragments
 * list.  The map is created if it does not yet exist on this process.
 *
 * Used both on writer QEs (where iceberg_modify_init will later clear and
 * repopulate via its own path) and on scanner-only QEs (where the map
 * would otherwise be NULL and the scan would encode file_id=0 into every
 * ctid -- exactly the corruption that motivated issue #333).
 *
 * The map is anchored in TopMemoryContext so its `entries` buffer survives
 * statement-scoped MemoryContext resets between successive UPDATE/DELETE
 * commands in the same transaction.  (BeginForeignModify's
 * MemoryContextRegisterResetCallback only fires when *it* allocated the map;
 * when we pre-create from this hook, no callback is registered and we'd
 * otherwise leave a dangling pointer behind.)
 */
static void
prepopulate_iceberg_file_index_map(List *fragments)
{
	if (fragments == NIL)
		return;

	if (datalake_iceberg_file_index_map == NULL)
	{
		MemoryContext oldcxt = MemoryContextSwitchTo(TopMemoryContext);

		datalake_iceberg_file_index_map = icebergCreateFileIndexMap();
		MemoryContextSwitchTo(oldcxt);
	}

	if (datalake_iceberg_file_index_map == NULL)
		return;					/* OOM, give up silently */

	/*
	 * Always clear before populating: across successive UPDATE/DELETE
	 * statements the Iceberg snapshot changes, so previously cached entries
	 * become stale.  icebergClearFileIndexMap resets numFiles to 0 and
	 * pfrees the path strings but keeps `entries` allocated.
	 */
	icebergClearFileIndexMap(datalake_iceberg_file_index_map);

	icebergFileIndexMapPopulateFromAllFragments(
		datalake_iceberg_file_index_map, fragments);
}

/*
 * Read the tagged envelope entry out of one fdwPrivLists slot; NULL when the
 * plan carries none (every non-REST catalog, and any pre-envelope plan).
 * See pg_iceberg_am.h for the layout this matches.
 */
static char *
envelope_from_modify_priv(List *priv)
{
	int		i;

	/*
	 * Search for the tag rather than reading it at a fixed offset.  The
	 * positional prefix ahead of the pair has already grown once -- issue #410
	 * added the catalog properties at slot 1, pushing the tag from 1 to 2 --
	 * and a pair that must still be found at a hardcoded index gains nothing
	 * from carrying a tag at all.  The failure that costs is silent: the next
	 * entry added ahead of the pair would simply stop the envelope being
	 * found, and a REST write would fail for want of credentials with nothing
	 * pointing at the cause.
	 *
	 * A false positive would need one of the JSON payloads to be exactly the
	 * tag string, which is neither valid JSON nor an empty placeholder.
	 */
	for (i = 0; i + 1 < list_length(priv); i++)
	{
		Node   *tag = (Node *) list_nth(priv, i);
		Node   *envelope;

		if (tag == NULL || !IsA(tag, String) ||
			strcmp(strVal(tag), ICEBERG_MODIFY_PRIV_ENVELOPE_TAG) != 0)
			continue;

		envelope = (Node *) list_nth(priv, i + 1);
		if (envelope == NULL || !IsA(envelope, String))
			return NULL;

		return strVal(envelope);
	}

	return NULL;
}

static void
restash_iceberg_modify_private_from_plan(PlannedStmt *stmt)
{
	ModifyTable	   *mt;
	ListCell	   *lc_rel;
	ListCell	   *lc_priv;

	if (stmt == NULL || stmt->planTree == NULL ||
		!IsA(stmt->planTree, ModifyTable))
		return;

	mt = (ModifyTable *) stmt->planTree;
	if (mt->operation != CMD_INSERT &&
		mt->operation != CMD_UPDATE &&
		mt->operation != CMD_DELETE)
		return;
	if (mt->fdwPrivLists == NIL)
		return;

	forboth(lc_rel, mt->resultRelations,
			lc_priv, mt->fdwPrivLists)
	{
		Index	rti = lfirst_int(lc_rel);
		List   *priv = (List *) lfirst(lc_priv);
		Oid		relid;
		Node   *carrier;
		char   *envelope;
		List   *fragments;

		if (priv == NIL || rti == 0 || rti > list_length(stmt->rtable))
			continue;

		relid = ((RangeTblEntry *) list_nth(stmt->rtable, rti - 1))->relid;
		if (!is_iceberg_relation(relid))
			continue;

		/*
		 * Slot 1 is the catalog properties the segments cannot ask the agent
		 * for themselves (issue #410), and the tagged pair after it is the
		 * REST storage envelope.  Both are taken before the fragments because
		 * the fragment handling below continues out of the loop in several
		 * cases that say nothing about whether either arrived -- an INSERT on
		 * a REST catalog carries an envelope and no fragments at all.
		 */
		if (list_length(priv) >= 2)
		{
			Node *props = (Node *) lsecond(priv);

			if (props != NULL && IsA(props, String))
				pg_iceberg_stash_modify_catalog_properties(relid,
														   strVal(props));
		}

		envelope = envelope_from_modify_priv(priv);
		if (envelope != NULL)
			pg_iceberg_stash_modify_envelope(relid, envelope);

		/* Unwrap the fragments carrier placed by the planner_hook. */
		carrier = (Node *) linitial(priv);
		if (carrier == NULL)
			continue;

		if (IsA(carrier, String))
		{
			/*
			 * JSON carrier (issue #362): parse the agent wire format back
			 * into the in-memory fragment list; deleteIndexes in the JSON
			 * re-share the delete-file fragments across tasks.
			 */
			char	   *json = strVal(carrier);

			/* Empty means this operation has no fragments, e.g. INSERT. */
			if (json == NULL || json[0] == '\0')
				continue;

			fragments = parseIcebergFragmentResponse(json, strlen(json));
		}
		else
		{
			/* Backward compat: pre-#362 QD placed a List * here directly. */
			Assert(IsA(carrier, List));
			fragments = (List *) carrier;
		}

		if (fragments == NIL)
			continue;

		pg_iceberg_stash_modify_fragments(relid, fragments);

		/*
		 * Also seed the global file-index map directly.  Scanner-only QEs
		 * never enter BeginForeignModify and so never create the map; without
		 * this they encode file_id=0 for every row, which then resolves to
		 * the first file on the writer QE and produces "delete went to wrong
		 * file" data corruption rather than the original NULL-lookup error.
		 */
		prepopulate_iceberg_file_index_map(fragments);
	}
}

static void
iceberg_executor_start_hook(QueryDesc *queryDesc, int eflags)
{
	/*
	 * Produce on the QD, consume everywhere else.  Both run before
	 * standard_ExecutorStart, hence before InitPlan dispatches the plan, so a
	 * carrier refreshed here still reaches every segment.
	 *
	 * EXPLAIN without ANALYZE touches no data, so it is not worth a catalog
	 * round trip (and, on a REST catalog, not worth vending credentials that
	 * would go unused).  The plan it prints keeps whatever the last real
	 * execution left behind, which EXPLAIN does not display.
	 */
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		if (!(eflags & EXEC_FLAG_EXPLAIN_ONLY))
			stash_modify_private_for_iceberg_modifies(queryDesc->plannedstmt);
	}
	else
		restash_iceberg_modify_private_from_plan(queryDesc->plannedstmt);

	if (prev_executor_start_hook)
		prev_executor_start_hook(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);
}


/* ------------------------------------------------------------------------
 * CustomScanMethods.CreateCustomScanState
 * ------------------------------------------------------------------------
 */
static Node *
IcebergCreateCustomScanState(CustomScan *cscan)
{
	IcebergCustomScanState *iss;

	iss = (IcebergCustomScanState *) newNode(sizeof(IcebergCustomScanState),
											 T_CustomScanState);
	iss->css.methods = &IcebergCustomExecMethods;
	iss->scanDesc = NULL;

	return (Node *) iss;
}


/* ------------------------------------------------------------------------
 * CustomExecMethods.BeginCustomScan
 *
 * Two jobs:
 *
 *   1. On the QD, (re)compute the Iceberg split list and stash it into the
 *      plan node's custom_private *before* CdbDispatchPlan serialises the
 *      PlannedStmt to the QE slices.  This mirrors the original kernel
 *      behaviour of table_scan_get_am_private() in nodeSeqscan.c, which ran
 *      on every ExecInitSeqScan and thus gave prepared statements fresh
 *      splits on every EXECUTE.  If we populated custom_private at
 *      planner_hook time instead, cached PlannedStmts would ship stale
 *      splits on the second and later EXECUTEs.
 *
 *   2. Defer the FDW scan open (pg_iceberg_scan_begin_extractcolumns) until
 *      the first tuple fetch in ExecCustomScan.  ExecInitCustomScan runs on
 *      the QD even for CustomScan nodes that belong to a remote (QE) slice
 *      (see execMain.c:InitPlan with eliminateAliens=false on QD), so an
 *      eager open here would needlessly open Iceberg readers on the QD for
 *      every segment-bound scan.
 * ------------------------------------------------------------------------
 */
static void
IcebergBeginCustomScan(CustomScanState *node, EState *estate, int eflags)
{
	IcebergCustomScanState *iss = (IcebergCustomScanState *) node;
	CustomScan *cscan = (CustomScan *) node->ss.ps.plan;
	Relation	rel = node->ss.ss_currentRelation;

	/*
	 * Pass the scan PlanState so build_scan_am_private can serialize the
	 * node's restriction quals (node->ss.ps.plan->qual) for predicate
	 * pushdown / data-file pruning on the agent.
	 */
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		int64		snapshot_id = 0;	/* 0 = HEAD */
		RangeTblEntry *rte = exec_rt_fetch(cscan->scan.scanrelid, estate);
		const char *aliasname = rte->alias ? rte->alias->aliasname
			: (rte->eref ? rte->eref->aliasname : NULL);

		/*
		 * Time travel: a rewritten iceberg_snapshot_scan() call carries its
		 * bound snapshot in the relation alias (see pg_iceberg_time_travel.c).
		 */
		if (aliasname != NULL && iceberg_tt_parse_alias(aliasname, &snapshot_id))
		{
			/*
			 * Normally the alias was stamped by the post_parse_analyze rewrite
			 * of an iceberg_snapshot_scan() call, whose describe callback ran
			 * tt_reject_unsupported_relation and whose schema gate only rewrites
			 * schema-unchanged snapshots.  But a user with SELECT could also
			 * hand-write the token (SELECT * FROM t AS "__icetts_<id>_1"),
			 * reaching here without either guard.  Replicate both:
			 *
			 *   - inheritance / partitioning: the planner expands children by
			 *     the current structure, which diverges from the historical
			 *     snapshot's file list and would return incorrect rows.  (RLS
			 *     still applies -- a direct relation scan is rewritten with its
			 *     policies -- so only the inheritance check is replicated.)
			 *   - schema drift: a snapshot whose schema differs from the current
			 *     schema would be decoded under the current tuple descriptor and
			 *     return corrupt rows; reject it (the rewrite would have kept it
			 *     on the FunctionScan fallback instead).
			 */
			if (rel->rd_rel->relhassubclass || rel->rd_rel->relispartition)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("iceberg time travel does not support inheritance or partitioned tables"),
						 errdetail("Relation \"%s\" participates in an inheritance or partition hierarchy.",
								   RelationGetRelationName(rel))));

			iceberg_tt_check_alias_schema(rel, snapshot_id);
		}

		cscan->custom_private =
			pg_iceberg_build_scan_am_private(rel, &node->ss.ps,
											 external_table_limit_segment_num,
											 snapshot_id);
	}

	/*
	 * The vectorization engine ORs EXEC_FLAG_VECTOR into eflags for the whole
	 * plan tree (its ExecutorStart hook).  When set, ExecInitVecCustomScan is
	 * driving this node and expects ABI RecordBatch slots, so remember it and
	 * flip the underlying read to the vectorized path.
	 */
	iss->vectorize = (eflags & ICEBERG_EXEC_FLAG_VECTOR) != 0;

	/*
	 * GPDB: the iceberg_volume_fdw layer reads ps.scandesc to decide which
	 * columns the batch/row reader emits.  For the vectorized path the engine
	 * (ExecInitScanTupleSlotVec) has already narrowed ss_ScanTupleSlot to the
	 * COMPACT scan descriptor -- only the referenced columns, in attr order --
	 * so the provider must emit exactly those columns, in that order, to line
	 * up with the engine's columnmap.
	 *
	 * But BuildDescForScanExtract renumbered that compact descriptor's attnums
	 * to 1..N, and builtin-iceberg parquet matching keys the Iceberg field-id
	 * off attnum (createFieldDescription).  So hand the provider a private copy
	 * of the compact descriptor with each attnum restored to the original
	 * relation attnum (matched by name); the engine keeps its own renumbered
	 * slot untouched.  The row path emits the full relation rowtype.
	 */
	if (iss->vectorize && node->ss.ss_ScanTupleSlot != NULL)
	{
		TupleDesc	compact = node->ss.ss_ScanTupleSlot->tts_tupleDescriptor;
		TupleDesc	full = RelationGetDescr(rel);
		TupleDesc	pdesc = CreateTupleDescCopy(compact);
		int			j;

		for (j = 0; j < pdesc->natts; j++)
		{
			Form_pg_attribute cattr = TupleDescAttr(pdesc, j);
			int			i;
			bool		matched = false;

			for (i = 0; i < full->natts; i++)
			{
				Form_pg_attribute fattr = TupleDescAttr(full, i);

				if (!fattr->attisdropped &&
					strcmp(NameStr(cattr->attname), NameStr(fattr->attname)) == 0)
				{
					cattr->attnum = fattr->attnum;
					matched = true;
					break;
				}
			}

			/*
			 * A compact-descriptor entry with no relation column of that name
			 * is not a stored column at all -- ExecInitScanTupleSlotVec()
			 * appends a synthetic INT8 "ctid" column when the targetlist needs
			 * a row identifier.  Leaving the renumbered compact position in
			 * attnum would make createFieldDescription() hand it to the
			 * builtin-iceberg field-id match, which then selects whatever
			 * physical column happens to carry that field-id.  Clear it so the
			 * reader can only fall back to a name match (and find nothing)
			 * instead of silently reading an unrelated column.  Plans that
			 * need ctid are refused vectorization outright in planner_vec.c.
			 */
			if (!matched)
				cattr->attnum = InvalidAttrNumber;
		}
		node->ss.ps.scandesc = pdesc;
	}
	else
		node->ss.ps.scandesc = RelationGetDescr(rel);

	iss->scanDesc = NULL;
}

static void
iceberg_ensure_scan_open(IcebergCustomScanState *iss)
{
	CustomScanState *node = &iss->css;
	Relation	rel;
	EState	   *estate;
	uint32		flags;

	if (iss->scanDesc != NULL)
		return;

	rel = node->ss.ss_currentRelation;
	estate = node->ss.ps.state;
	flags = SO_TYPE_SEQSCAN | SO_ALLOW_STRAT | SO_ALLOW_SYNC |
			SO_ALLOW_PAGEMODE;

	iss->scanDesc = pg_iceberg_scan_begin_extractcolumns(rel,
														 estate->es_snapshot,
														 0, NULL, NULL,
														 &node->ss.ps,
														 flags,
														 iss->vectorize);
}


/* ------------------------------------------------------------------------
 * CustomExecMethods.ExecCustomScan
 * ------------------------------------------------------------------------
 */
static TupleTableSlot *
IcebergAccessScan(CustomScanState *node)
{
	IcebergCustomScanState *iss = (IcebergCustomScanState *) node;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;

	iceberg_ensure_scan_open(iss);

	if (pg_iceberg_getnextslot(iss->scanDesc, ForwardScanDirection, slot))
		return slot;

	return NULL;
}

static bool
IcebergRecheckScan(CustomScanState *node, TupleTableSlot *slot)
{
	return true;
}

static TupleTableSlot *
IcebergExecCustomScan(CustomScanState *node)
{
	IcebergCustomScanState *iss = (IcebergCustomScanState *) node;

	/*
	 * Vectorized path: the vec engine (ExecVecCustomScan) drives us and wants
	 * the raw slot carrying an ABI RecordBatch, so skip ExecScan's row-at-a-
	 * time projection and quals -- the engine applies vectorized quals and
	 * projection on the imported batch.
	 */
	if (iss->vectorize)
		return IcebergAccessScan(node);

	return ExecScan(&node->ss,
					(ExecScanAccessMtd) IcebergAccessScan,
					(ExecScanRecheckMtd) IcebergRecheckScan);
}


/* ------------------------------------------------------------------------
 * CustomExecMethods.EndCustomScan
 * ------------------------------------------------------------------------
 */
static void
IcebergEndCustomScan(CustomScanState *node)
{
	IcebergCustomScanState *iss = (IcebergCustomScanState *) node;

	if (iss->scanDesc != NULL)
	{
		pg_iceberg_endscan(iss->scanDesc);
		iss->scanDesc = NULL;
	}
}


/* ------------------------------------------------------------------------
 * CustomExecMethods.ReScanCustomScan
 * ------------------------------------------------------------------------
 */
static void
IcebergReScanCustomScan(CustomScanState *node)
{
	IcebergCustomScanState *iss = (IcebergCustomScanState *) node;

	if (iss->scanDesc != NULL)
		pg_iceberg_rescan(iss->scanDesc, NULL, false, false, false, false);
}


/* ------------------------------------------------------------------------
 * CustomExecMethods.ExplainCustomScan
 * ------------------------------------------------------------------------
 */
static void
IcebergExplainCustomScan(CustomScanState *node, List *ancestors,
						 ExplainState *es)
{
	/*
	 * The core EXPLAIN machinery already prints the scanned relation
	 * name and the output targetlist for CustomScan nodes.  Nothing
	 * Iceberg-specific to add at this stage.
	 */
}


/* ------------------------------------------------------------------------
 * Install entry point (called once from _PG_init)
 * ------------------------------------------------------------------------
 */
void
pg_iceberg_install_custom_scan(void)
{
	RegisterCustomScanMethods(&IcebergCustomScanMethods);

	prev_planner_hook = planner_hook;
	planner_hook = iceberg_planner_hook;

	prev_executor_start_hook = ExecutorStart_hook;
	ExecutorStart_hook = iceberg_executor_start_hook;

	/* Time travel: rewrite iceberg_snapshot_scan() into native relation scans. */
	iceberg_tt_install_hooks();
}
