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

/*
 * issue #333: walk the plan for ModifyTable nodes whose result relations
 * use the iceberg AM, then list the data fragments and route them to every
 * QE so the global file_id -> file_path map can be populated on writer-only
 * QEs.
 *
 * Three carriers in the cache flow:
 *   - QD planner-time:   pg_iceberg_stash_modify_fragments(relid, fragments)
 *   - dispatched plan:   ModifyTable.fdwPrivLists[i] = list_make1(fragments)
 *   - QE executor-time:  ExecutorStart_hook re-stashes from fdwPrivLists.
 */
static void
stash_modify_fragments_for_iceberg_modifies(PlannedStmt *stmt)
{
	ModifyTable	   *mt;
	ListCell	   *lc;
	int				i;

	if (stmt->planTree == NULL || !IsA(stmt->planTree, ModifyTable))
		return;

	mt = (ModifyTable *) stmt->planTree;
	if (mt->operation != CMD_UPDATE && mt->operation != CMD_DELETE)
		return;

	/*
	 * fdwPrivLists holds one entry per result relation, in the same order
	 * as resultRelations.  Standard PlanForeignModify fills it for foreign
	 * tables; for native iceberg-AM relations the slot is NIL.  We fill
	 * those slots here with `list_make1(fragments)` so the dispatched plan
	 * carries the data.
	 */
	if (mt->fdwPrivLists == NIL)
	{
		int rel_count = list_length(mt->resultRelations);

		for (i = 0; i < rel_count; i++)
			mt->fdwPrivLists = lappend(mt->fdwPrivLists, NIL);
	}

	i = 0;
	foreach(lc, mt->resultRelations)
	{
		Index	rti = lfirst_int(lc);
		Oid		relid;
		char   *fragments_json;
		Relation rel;

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
		fragments_json = pg_iceberg_list_data_fragments_json(rel);
		table_close(rel, NoLock);

		if (fragments_json != NULL)
		{
			ListCell *prev_cell = list_nth_cell(mt->fdwPrivLists, i);
			List	 *fragments;

			/* QD-local consumers need the parsed (delete-sharing) form. */
			fragments = parseIcebergFragmentResponse(fragments_json,
													 strlen(fragments_json));
			pg_iceberg_stash_modify_fragments(relid, fragments);

			/*
			 * Splice the fragments into ModifyTable.fdwPrivLists so they get
			 * dispatched with the plan.  Ship the raw JSON (one String node)
			 * rather than the parsed FileScanTask tree: nodeToString() would
			 * duplicate every shared delete-file fragment into each task
			 * referencing it, making the dispatched plan and every QE's
			 * MessageContext O(data files x delete files) -- observed at
			 * ~500MB per backend on zipper-style workloads (issue #362).
			 * The slot is wrapped in list_make1 to keep one slot per relation
			 * (per ModifyTable contract); the consumer unwraps and parses it
			 * in restash_iceberg_modify_fragments_from_plan.
			 */
			lfirst(prev_cell) = list_make1(makeString(fragments_json));
		}

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
	cscan->scan.plan = seq->plan;
	cscan->scan.plan.type = T_CustomScan;
	cscan->scan.scanrelid = seq->scanrelid;

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
	cscan->custom_relids	= bms_make_singleton(seq->scanrelid);
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

		if (seq->scanrelid > 0 &&
			seq->scanrelid <= list_length(rtable))
		{
			rte = rt_fetch(seq->scanrelid, rtable);
			if (rte->rtekind == RTE_RELATION &&
				is_iceberg_relation(rte->relid))
				return make_iceberg_custom_scan(seq);
		}
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
	PlannedStmt	   *stmt;
	ListCell	   *lc;

	if (prev_planner_hook != NULL)
		stmt = prev_planner_hook(parse, query_string, cursorOptions,
								 boundParams, optimizer_options);
	else
		stmt = standard_planner(parse, query_string, cursorOptions,
								boundParams, optimizer_options);

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

	stash_modify_fragments_for_iceberg_modifies(stmt);

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

static void
restash_iceberg_modify_fragments_from_plan(PlannedStmt *stmt)
{
	ModifyTable	   *mt;
	ListCell	   *lc_rel;
	ListCell	   *lc_priv;

	if (stmt == NULL || stmt->planTree == NULL ||
		!IsA(stmt->planTree, ModifyTable))
		return;

	mt = (ModifyTable *) stmt->planTree;
	if (mt->operation != CMD_UPDATE && mt->operation != CMD_DELETE)
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
		List   *fragments;

		if (priv == NIL || rti == 0 || rti > list_length(stmt->rtable))
			continue;

		relid = ((RangeTblEntry *) list_nth(stmt->rtable, rti - 1))->relid;
		if (!is_iceberg_relation(relid))
			continue;

		/* Unwrap the list_make1(...) wrapper from the planner_hook. */
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
	restash_iceberg_modify_fragments_from_plan(queryDesc->plannedstmt);

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
		cscan->custom_private =
			pg_iceberg_build_scan_am_private(rel, &node->ss.ps,
											 external_table_limit_segment_num);

	/* GPDB: the iceberg_volume_fdw layer reads ps.scandesc. */
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
														 flags);
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
}
