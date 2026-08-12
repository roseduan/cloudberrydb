/*-------------------------------------------------------------------------
 *
 * ts_chunk_append.c
 *    ChunkAppend Custom Scan: ordered chunk-by-chunk iteration with
 *    demand-pull early-stop.
 *
 *    Model (M1, no index): the path-builder enumerates the chunks in
 *    the [ts_min, ts_max] window and emits one subpath per chunk.
 *    Each subpath is a Sort(<order pathkey>) wrapping a per-chunk
 *    ChunkScan (chunk_only_num = K).  ChunkAppend orchestrates the
 *    subplans: lazy-init the current one, drain it via ExecProcNode,
 *    advance on NULL, release the prior subplan's state.  An upstream
 *    Limit (single-segment context) stops pulling once N rows arrive,
 *    so subsequent chunks' Sort + ChunkScan are never initialised.
 *
 *    M1 limitations: single-node early-stop only.  MPP Motion sits
 *    above ChunkAppend in distributed plans and breaks the demand-pull
 *    backpressure — fixed in M2 by per-segment Limit injection.
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 * IDENTIFICATION
 *    contrib/time_series/src/access/ts_chunk_append.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/table.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "executor/nodeSort.h"
#include "miscadmin.h"
#include "nodes/extensible.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pathnodes.h"
#include "nodes/plannodes.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/tlist.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "cdb/cdbpathlocus.h"

#include "../include/time_series.h"
#include "../include/access/ts_tableam.h"
#include "../include/access/ts_func_cache.h"

/*
 *		Methods structs (filled in ts_chunk_append_init)
 */
static CustomPathMethods ts_chunk_append_path_methods;
static CustomScanMethods ts_chunk_append_scan_methods;
static CustomExecMethods ts_chunk_append_exec_methods;

/* GUC: default on (opt-out via time_series.enable_chunk_append=off) */
bool		ts_enable_chunk_append = true;

/* GUC: max number of per-chunk subplans the planner will materialise */
int			ts_chunk_append_max_chunks = 64;

/*
 * Previous planner_hook in the chain; saved in ts_chunk_append_init and
 * invoked from ts_chunk_append_planner_wrapper.
 */
static planner_hook_type prev_planner_hook_for_chunk_append = NULL;

/*
 *		State struct
 */
typedef struct ChunkAppendState
{
	CustomScanState css;
	List	   *subplans;			/* List<Plan*> from CustomScan.custom_plans */
	int			n_subplans;
	PlanState **subplan_states;		/* lazy: index < cur_idx ⇒ NULL after End */
	int			cur_idx;
	int			eflags;
	/*
	 * Eager mode: ExecInitNode every subplan at begin and keep them alive
	 * across cur_idx advances.  Enabled for plain EXPLAIN (no exec) and
	 * EXPLAIN ANALYZE so the full per-chunk subplan tree is visible.
	 * Pure exec stays lazy — only the current chunk's PlanState is alive,
	 * which preserves the LIMIT early-stop savings.
	 */
	bool		eager;
} ChunkAppendState;

/*
 * find_time_pathkey_for_rel
 *		Walk root->query_pathkeys, return the first PathKey whose
 *		expression resolves (via ts_sort_transform_expr — strips
 *		RelabelType and unwraps registered bucketing FuncExprs whose
 *		const-arg constraints hold) to the time column of `rel`.
 *		Sets *out_desc to true for DESC ordering.  Returns NULL on
 *		no match.
 *
 *		Recognised bucketing functions are registered in ts_func_cache.c
 *		— extend that table to teach this matcher about new functions
 *		(no edit required here).
 */
static PathKey *
find_time_pathkey_for_rel(PlannerInfo *root, RelOptInfo *rel, Index rti,
						   AttrNumber time_attno,
						   bool *out_desc)
{
	ListCell   *lcq;

	if (root->query_pathkeys == NIL)
		return NULL;
	foreach(lcq, root->query_pathkeys)
	{
		PathKey    *pk = lfirst_node(PathKey, lcq);
		ListCell   *lcm;

		foreach(lcm, pk->pk_eclass->ec_members)
		{
			EquivalenceMember *em = lfirst_node(EquivalenceMember, lcm);
			Expr	   *resolved;
			Var		   *v;

			if (em->em_is_const)
				continue;
			if (!bms_is_subset(em->em_relids, rel->relids))
				continue;

			resolved = ts_sort_transform_expr(em->em_expr);
			if (!IsA(resolved, Var))
				continue;

			v = (Var *) resolved;
			if ((Index) v->varno == rti && v->varattno == time_attno)
			{
				*out_desc = (pk->pk_strategy == BTGreaterStrategyNumber);
				return pk;
			}
		}
	}
	return NULL;
}

/*
 *		Path → Plan conversion
 */
static Plan *
ts_chunk_append_create_plan(PlannerInfo *root, RelOptInfo *rel,
							 CustomPath *best_path, List *tlist,
							 List *clauses, List *custom_plans)
{
	CustomScan *cscan;
	List	   *stripped;

	cscan = makeNode(CustomScan);
	cscan->scan.plan.targetlist = tlist;
	stripped = extract_actual_clauses(clauses, false);
	cscan->scan.plan.qual = stripped;
	cscan->scan.scanrelid = rel->relid;
	/*
	 * Critical: declare our scan-slot layout to match the child Sort's
	 * actual emit shape, NOT the base rel's full TupleDesc.  Sort adds
	 * its sort-key expression (e.g. time_bucket(time)) to the subpath's
	 * pathtarget, so child slots are [time, usage_user, time_bucket(time)]
	 * — *not* the upper-plan-requested layout [time_bucket(time), usage_user].
	 *
	 * Without this, framework's ExecAssignScanProjectionInfoWithVarno
	 * builds ps_ProjInfo assuming scan slot is base-rel-shaped, then
	 * tries to read tts_values[time_bucket_attno-1] from the child's
	 * 3-col slot using base-rel attno → reads wrong column → upstream
	 * GroupAggregate gets `time` (raw) instead of `time_bucket(time)`,
	 * producing 5 distinct buckets per ChunkAppend output instead of
	 * properly grouped output.
	 *
	 * Setting custom_scan_tlist switches framework to INDEX_VAR mode:
	 * scan slot uses ExecTypeFromTL(custom_scan_tlist), Vars in upper
	 * plans get resolved against scan slot positions, and ps_ProjInfo
	 * correctly maps from scan slot's actual layout to our advertised
	 * output `tlist`.
	 */
	/*
	 * custom_scan_tlist must mirror the SORT PLAN's targetlist (NOT the
	 * sort path's pathtarget), because Sort's create_plan adds sort-key
	 * expressions to the plan's tlist that aren't in the pathtarget.
	 * Using pathtarget produced "srcdesc->natts <= dstslot natts"
	 * FailedAssertion in ExecCopySlot.
	 */
	if (custom_plans != NIL)
	{
		Plan *first_plan = (Plan *) linitial(custom_plans);

		cscan->custom_scan_tlist = first_plan->targetlist;
	}
	else
		cscan->custom_scan_tlist = NIL;
	/* PG already converted best_path->custom_paths → custom_plans for us */
	cscan->custom_plans = custom_plans;
	cscan->flags = best_path->flags;
	cscan->custom_private = NIL;
	cscan->methods = &ts_chunk_append_scan_methods;

	return (Plan *) cscan;
}

/*
 *		CreateCustomScanState
 */
static Node *
ts_chunk_append_create_state(CustomScan *cscan)
{
	ChunkAppendState *s = (ChunkAppendState *) palloc0(sizeof(ChunkAppendState));

	NodeSetTag(s, T_CustomScanState);
	s->css.methods = &ts_chunk_append_exec_methods;
	s->subplans = cscan->custom_plans;
	s->n_subplans = list_length(s->subplans);
	s->cur_idx = 0;
	return (Node *) s;
}

/*
 *		Executor: Begin / Exec / End / ReScan / Explain
 */
static void
init_subplan_at(ChunkAppendState *s, int idx, EState *estate)
{
	Plan	   *p;

	Assert(idx >= 0 && idx < s->n_subplans);
	Assert(s->subplan_states[idx] == NULL);

	p = (Plan *) list_nth(s->subplans, idx);
	s->subplan_states[idx] = ExecInitNode(p, estate, s->eflags);
}

static void
ts_chunk_append_begin(CustomScanState *node, EState *estate, int eflags)
{
	ChunkAppendState *s = (ChunkAppendState *) node;
	int			i;

	s->eflags = eflags;
	if (s->n_subplans <= 0)
	{
		node->custom_ps = NIL;
		return;
	}
	s->subplan_states = (PlanState **) palloc0(sizeof(PlanState *) * s->n_subplans);

	/*
	 * Init policy:
	 *   - Eager: EXPLAIN (with or without ANALYZE) needs to see every
	 *     subplan so the per-chunk subplan tree renders fully.  PG's
	 *     ExplainNode walks node->custom_ps, so we init all subplans
	 *     up front and stuff them all into the list.  For pure EXPLAIN
	 *     no rows are pulled, so the extra ExecInitNode cost is paid
	 *     only once and the executor never runs.  For EXPLAIN ANALYZE,
	 *     subplans not reached by LIMIT get tagged "never executed"
	 *     by PG's instrumentation, mirroring TimescaleDB's display.
	 *   - Lazy: pure exec only inits the first subplan; the rest are
	 *     created on demand in ExecCustomScan, so an upstream LIMIT
	 *     satisfied within chunk 0 pays zero cost for chunks 1..N-1.
	 */
	s->eager = (eflags & EXEC_FLAG_EXPLAIN_ONLY) != 0
		|| estate->es_instrument != 0;

	if (s->eager)
	{
		List	   *ps_list = NIL;

		for (i = 0; i < s->n_subplans; i++)
		{
			init_subplan_at(s, i, estate);
			ps_list = lappend(ps_list, s->subplan_states[i]);
		}
		node->custom_ps = ps_list;
	}
	else
	{
		init_subplan_at(s, 0, estate);
		node->custom_ps = list_make1(s->subplan_states[0]);
	}
}

static TupleTableSlot *
ts_chunk_append_exec(CustomScanState *node)
{
	ChunkAppendState *s = (ChunkAppendState *) node;

	for (;;)
	{
		PlanState  *child;
		TupleTableSlot *slot;

		if (s->cur_idx >= s->n_subplans)
			return NULL;
		child = s->subplan_states[s->cur_idx];
		if (child == NULL)
		{
			/* defensive; init was supposed to happen at begin/advance */
			init_subplan_at(s, s->cur_idx, node->ss.ps.state);
			child = s->subplan_states[s->cur_idx];
		}

		slot = ExecProcNode(child);
		if (!TupIsNull(slot))
		{
			TupleTableSlot *scanslot = node->ss.ss_ScanTupleSlot;
			ProjectionInfo *projInfo = node->ss.ps.ps_ProjInfo;
			ExprContext *econtext = node->ss.ps.ps_ExprContext;

			/*
			 * With cscan->custom_scan_tlist set (in create_plan), the
			 * framework initialised scanslot's TupleDesc to match the
			 * subpath's pathtarget — same shape as `slot` returned by
			 * Sort.  ExecCopySlot is now safe and gives us a deep copy
			 * (deformed virtual slot + by-ref deep copy) so downstream
			 * Motion serialization sees self-contained data.
			 *
			 * Then ps_ProjInfo (auto-built from cscan->scan.plan.targetlist)
			 * projects scan-layout → output-layout, reordering columns as
			 * needed for upstream consumers (e.g. picking out
			 * time_bucket(time) for GroupAggregate's Group Key).
			 */
			if (scanslot == NULL)
			{
				slot_getallattrs(slot);
				return slot;
			}

			ExecClearTuple(scanslot);
			ExecCopySlot(scanslot, slot);

			if (projInfo != NULL)
			{
				econtext->ecxt_scantuple = scanslot;
				return ExecProject(projInfo);
			}
			return scanslot;
		}

		/*
		 * Current chunk exhausted.  In lazy mode (pure exec) release the
		 * PlanState now so its memory + instrumentation aren't carried
		 * forward, then lazy-init the next chunk's subplan.  In eager
		 * mode (EXPLAIN / EXPLAIN ANALYZE) leave all subplan_states
		 * alive — Instrumentation must survive until EndCustomScan so
		 * EXPLAIN ANALYZE can report per-chunk timings (and "never
		 * executed" for skipped chunks).
		 */
		if (s->eager)
		{
			s->cur_idx++;
			if (s->cur_idx >= s->n_subplans)
				return NULL;
			/* subplan already alive from begin; no init / no custom_ps refresh */
		}
		else
		{
			ExecEndNode(child);
			s->subplan_states[s->cur_idx] = NULL;
			s->cur_idx++;
			if (s->cur_idx >= s->n_subplans)
				return NULL;
			init_subplan_at(s, s->cur_idx, node->ss.ps.state);
			/* Refresh EXPLAIN view to the new active child */
			node->custom_ps = list_make1(s->subplan_states[s->cur_idx]);
		}
	}
}

static void
ts_chunk_append_end(CustomScanState *node)
{
	ChunkAppendState *s = (ChunkAppendState *) node;
	int			i;

	if (s->subplan_states == NULL)
		return;
	for (i = 0; i < s->n_subplans; i++)
	{
		if (s->subplan_states[i] != NULL)
		{
			ExecEndNode(s->subplan_states[i]);
			s->subplan_states[i] = NULL;
		}
	}
}

static void
ts_chunk_append_rescan(CustomScanState *node)
{
	ChunkAppendState *s = (ChunkAppendState *) node;
	int			i;

	for (i = 0; i < s->n_subplans; i++)
	{
		if (s->subplan_states[i] != NULL)
		{
			ExecEndNode(s->subplan_states[i]);
			s->subplan_states[i] = NULL;
		}
	}
	s->cur_idx = 0;
	if (s->n_subplans > 0)
	{
		init_subplan_at(s, 0, node->ss.ps.state);
		node->custom_ps = list_make1(s->subplan_states[0]);
	}
}

static void
ts_chunk_append_explain(CustomScanState *node, List *ancestors,
						 ExplainState *es)
{
	ChunkAppendState *s = (ChunkAppendState *) node;

	ExplainPropertyInteger("Subplans Total", NULL, s->n_subplans, es);
	ExplainPropertyInteger("Subplans Used", NULL, s->cur_idx + 1, es);
}

/*
 * chunk_append_enumerate_chunks
 *		Translate the WHERE-clause time bounds into a list of candidate
 *		chunk_nums.  Returns NIL when ts_max is unbounded (LIMIT-style
 *		queries always have a window) or when the resulting range
 *		exceeds the configured cap.  The returned list is ordered
 *		ascending by chunk_num, or descending when `desc` is true so the
 *		"latest chunk first" subpath order matches DESC sort output.
 */
static List *
chunk_append_enumerate_chunks(struct TSConfig *config, int64 ts_min, int64 ts_max, bool desc)
{
	int64		interval = config->interval_usec;
	int64		origin = config->origin_usec;
	int			cap_chunks = ts_chunk_append_max_chunks;
	ForkNumber	lo;
	ForkNumber	hi;
	List	   *chunks = NIL;

	if (ts_min != DT_NOBEGIN)
		lo = ts_calculate_chunk(ts_min, origin, interval);
	else
		lo = TS_FIRST_CHUNKNUM;

	if (ts_max != DT_NOEND)
		hi = ts_calculate_chunk(ts_max, origin, interval);
	else
	{
		elog(DEBUG2, "chunk_append: no ts_max, skipping");
		return NIL;
	}

	if (lo == InvalidForkNumber || lo < TS_FIRST_CHUNKNUM)
		lo = TS_FIRST_CHUNKNUM;

	if (hi == InvalidForkNumber || hi < lo)
		return NIL;

	if ((int) (hi - lo + 1) > cap_chunks)
	{
		elog(DEBUG2, "chunk_append: range [%d,%d] exceeds cap=%d",
			 (int) lo, (int) hi, cap_chunks);
		return NIL;
	}

	if (desc)
	{
		for (int k = (int) hi; k >= (int) lo; k--)
			chunks = lappend_int(chunks, k);
	}
	else
	{
		for (int k = (int) lo; k <= (int) hi; k++)
			chunks = lappend_int(chunks, k);
	}
	return chunks;
}

/*
 * Does `target` already contain a Var that matches `v` (same varno+varattno)?
 */
static bool
pathtarget_has_var(PathTarget *target, Var *v)
{
	ListCell   *lc;

	foreach(lc, target->exprs)
	{
		Node	   *e = (Node *) lfirst(lc);

		if (IsA(e, Var) &&
			((Var *) e)->varno == v->varno &&
			((Var *) e)->varattno == v->varattno)
			return true;
	}
	return false;
}

/*
 * chunk_append_build_extended_pathtarget
 *		Return either NULL (rel->reltarget already covers everything)
 *		or a clone of rel->reltarget with extra entries added so that
 *
 *		  (a) volatile-EC pathkeys (gapfill / time_bucket_gapfill) have
 *		      a TLE whose ressortgroupref matches ec_sortref — required
 *		      by prepare_sort_from_pathkeys; otherwise the per-chunk
 *		      Sort raises "ORDER/GROUP BY expression not found in
 *		      targetlist" at create-plan time.
 *
 *		  (b) every Var referenced by rel->baserestrictinfo is also
 *		      in the scan tlist — the wrapping ChunkAppend's
 *		      custom_scan_tlist mirrors this, and INDEX_VAR resolution
 *		      via set_plan_references silently leaves missing Vars
 *		      unresolved → exec-init NULL deref.
 *
 *		Cheap path returns NULL so the caller skips the clone + override
 *		and the per-chunk paths keep using rel->reltarget.
 */
static PathTarget *
chunk_append_build_extended_pathtarget(PlannerInfo *root, RelOptInfo *rel,
									   Index rti, PathKey *query_pk)
{
	EquivalenceClass *pk_ec = query_pk->pk_eclass;
	bool		need_ext = false;
	List	   *qual_vars = NIL;
	PathTarget *ext_target = NULL;
	ListCell   *rilc;
	ListCell   *vc;

	if (pk_ec->ec_has_volatile && pk_ec->ec_sortref != 0)
		need_ext = true;

	/*
	 * Collect every Var the quals reference on our rel and check
	 * whether reltarget already lists it.  One missing Var is enough
	 * to force the clone.
	 */
	foreach(rilc, rel->baserestrictinfo)
	{
		RestrictInfo *ri = lfirst_node(RestrictInfo, rilc);

		qual_vars = list_concat(qual_vars,
								pull_var_clause((Node *) ri->clause,
												PVC_RECURSE_AGGREGATES |
												PVC_RECURSE_PLACEHOLDERS));
	}
	foreach(vc, qual_vars)
	{
		Var		   *v = (Var *) lfirst(vc);

		if (!IsA(v, Var) || (Index) v->varno != rti)
			continue;
		if (!pathtarget_has_var(rel->reltarget, v))
		{
			need_ext = true;
			break;
		}
	}

	if (!need_ext)
	{
		list_free(qual_vars);
		return NULL;
	}

	ext_target = copy_pathtarget(rel->reltarget);

	if (pk_ec->ec_has_volatile && pk_ec->ec_sortref != 0)
	{
		EquivalenceMember *em;

		Assert(list_length(pk_ec->ec_members) == 1);
		em = linitial_node(EquivalenceMember, pk_ec->ec_members);
		add_column_to_pathtarget(ext_target, copyObject(em->em_expr), pk_ec->ec_sortref);
		ext_target->width += sizeof(Datum);
	}

	foreach(vc, qual_vars)
	{
		Var		   *v = (Var *) lfirst(vc);

		if (!IsA(v, Var) || (Index) v->varno != rti)
			continue;
		if (!pathtarget_has_var(ext_target, v))
		{
			add_column_to_pathtarget(ext_target, (Expr *) copyObject(v), 0);
			ext_target->width += sizeof(Datum);
		}
	}

	list_free(qual_vars);
	return ext_target;
}

/*
 * chunk_append_effective_limit
 *		Resolve the LIMIT count to pass to create_sort_path for the
 *		bounded-top-N discount.  root->limit_tuples is -1 at base-rel
 *		set_rel_pathlist time for grouped queries (LIMIT lives on the
 *		upper rel); fall back to the literal in parse->limitCount when
 *		that happens.  Returns the heuristic default (100) when both
 *		signals are absent.
 */
static double
chunk_append_effective_limit(PlannerInfo *root)
{
	Node	   *lc_expr = root->parse->limitCount;

	if (root->limit_tuples > 0)
		return root->limit_tuples;

	if (lc_expr != NULL && IsA(lc_expr, Const) && !((Const *) lc_expr)->constisnull)
	{
		int64		v = DatumGetInt64(((Const *) lc_expr)->constvalue);

		if (v > 0)
			return (double) v;
	}
	return 100.0;
}

/*
 * chunk_append_build_subpaths
 *		Build one Sort(query_pk) over ChunkScan(chunk_only=K) subpath
 *		per entry in `chunks`.  Returns the list and writes the first
 *		subpath's total_cost to *first_cost_out (used by the wrapping
 *		CustomPath as its own cost basis under the "first chunk
 *		satisfies LIMIT" assumption).  Returns NIL if ts_build_chunkscan
 *		fails for any chunk_num.
 */
static List *
chunk_append_build_subpaths(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte,
							List *chunks, PathKey *query_pk, PathTarget *ext_target,
							int64 ts_min, int64 ts_max,
							bool min_inclusive, bool max_inclusive,
							double per_chunk_rows, Cost *first_cost_out)
{
	List	   *sub_paths = NIL;
	double		eff_limit = chunk_append_effective_limit(root);
	ListCell   *lc;
	int			i = 0;

	*first_cost_out = 0.0;

	foreach(lc, chunks)
	{
		int32		chunk_num = lfirst_int(lc);
		Cost		base_startup = 0.0;
		Cost		base_total = per_chunk_rows * cpu_tuple_cost;
		CustomPath *cs_path;
		Path	   *sorted;

		if (i > 0)
			base_total *= 0.01;		/* "rarely executed" heuristic */

		cs_path = ts_build_chunkscan_path_for_chunk(root, rel, rte, chunk_num,
													ts_min, ts_max,
													min_inclusive, max_inclusive,
													per_chunk_rows,
													base_startup, base_total);
		if (cs_path == NULL)
			return NIL;

		/* Override with extended target when present (see helper above). */
		if (ext_target != NULL)
			cs_path->path.pathtarget = ext_target;

		sorted = (Path *) create_sort_path(root, rel, &cs_path->path,
										   list_make1(query_pk), eff_limit);
		if (i == 0)
			*first_cost_out = sorted->total_cost;
		sub_paths = lappend(sub_paths, sorted);
		i++;
	}

	return sub_paths;
}

Path *
ts_chunk_append_try_build_path(PlannerInfo *root, RelOptInfo *rel,
							   Index rti, RangeTblEntry *rte,
							   struct TSConfig *config,
							   int64 ts_min, int64 ts_max,
							   bool min_inclusive, bool max_inclusive)
{
	PathKey    *query_pk;
	bool		desc = false;
	List	   *chunks;
	int			n_chunks;
	List	   *sub_paths;
	CustomPath *cap;
	double		per_chunk_rows;
	Cost		first_cost = 0.0;
	PathTarget *ext_target;

	if (!ts_enable_chunk_append)
		return NULL;

	if (root->parse->limitCount == NULL)
		return NULL;

	/*
	 * Require an explicit ORDER BY.  Without sortClause, the planner sets
	 * root->query_pathkeys from group_pathkeys (or distinct/window
	 * pathkeys) as a *hint* — not an actual ordering requirement on output.
	 * In that case adding a per-chunk Sort to satisfy those pathkeys is
	 * pure overhead: it pays O(N log N) inside ChunkAppend just to make
	 * upper GroupAggregate cheaper than HashAggregate, which is itself
	 * cheap for small K (LIMIT N).  Only trigger ChunkAppend when the
	 * query genuinely needs ordered output.
	 */
	if (root->parse->sortClause == NIL)
		return NULL;

	/* query_pathkeys must reference time col (direct or time_bucket). */
	query_pk = find_time_pathkey_for_rel(root, rel, rti, config->ts_attnum, &desc);
	if (query_pk == NULL)
		return NULL;

	/*
	 * Enumerate candidate chunk_num range from WHERE time bounds.
	 *
	 * Chunk catalog (ts_chunk) lives on segments — QD planner can't see it.
	 * chunk_append_enumerate_chunks() derives [min_chunk, max_chunk] from
	 * time bounds using the same ts_calculate_chunk() helper that segments
	 * use, then emits every integer in the range (descending for DESC
	 * queries so subpath[0] is the latest chunk).  At exec time each
	 * ChunkScan subscan with chunk_only_num = K checks the local
	 * per-segment ts_chunk and emits zero rows if K doesn't exist;
	 * ChunkAppend simply advances past empty subscans.
	 */
	chunks = chunk_append_enumerate_chunks(config, ts_min, ts_max, desc);
	if (chunks == NIL)
		return NULL;

	n_chunks = list_length(chunks);

	/* Rough per-chunk row estimate from the relation's total */
	per_chunk_rows = (rel->rows > 0 && n_chunks > 0)
		? rel->rows / (double) n_chunks
		: 100.0;

	/*
	 * Volatile-EC pathkey + qual-Var accommodation (gapfill-style
	 * functions).  Returns NULL when rel->reltarget already covers
	 * everything; otherwise a clone with the extra entries.  All
	 * per-chunk subpaths share this single extended target — see helper
	 * comment for the two reasons this matters.
	 */
	ext_target = chunk_append_build_extended_pathtarget(root, rel, rti, query_pk);

	/*
	 * Build per-chunk Sort(query_pk) over ChunkScan(chunk_only=K)
	 * subpaths.  first_cost captures the first subpath's total_cost so
	 * the wrapping CustomPath can use it as a basis under the "first
	 * chunk satisfies LIMIT" cost model.
	 */
	sub_paths = chunk_append_build_subpaths(root, rel, rte, chunks, query_pk, ext_target,
											ts_min, ts_max, min_inclusive, max_inclusive,
											per_chunk_rows, &first_cost);
	if (sub_paths == NIL)
		return NULL;

	/* Build the ChunkAppend wrapper */
	cap = makeNode(CustomPath);
	cap->path.type = T_CustomPath;
	cap->path.pathtype = T_CustomScan;
	cap->path.parent = rel;
	cap->path.pathtarget = rel->reltarget;
	cap->path.param_info = NULL;
	cap->path.parallel_aware = false;
	cap->path.parallel_safe = rel->consider_parallel;
	cap->path.parallel_workers = 0;

	/*
	 * Rows estimate: under "first chunk satisfies LIMIT" assumption,
	 * cap output cardinality at limit_tuples * a small fan-out for the
	 * aggregate (HashAgg's input rows = bucket_rows × N buckets ≈
	 * LIMIT × 1 chunk's row count).  Use the smaller of:
	 *   - rel->rows / n_chunks  (one chunk worth)
	 *   - limit_tuples * 100 (heuristic for grouped queries)
	 */
	{
		double per_chunk = rel->rows / Max(n_chunks, 1);
		double from_limit = 100.0;
		Node *lc_expr = root->parse->limitCount;

		if (lc_expr != NULL && IsA(lc_expr, Const) &&
			!((Const *) lc_expr)->constisnull)
		{
			int64 v = DatumGetInt64(((Const *) lc_expr)->constvalue);
			if (v > 0)
				from_limit = (double) v * 200.0;  /* assume ~200 rows per bucket */
		}
		cap->path.rows = Min(per_chunk, from_limit);
		if (cap->path.rows < 10.0)
			cap->path.rows = 10.0;
	}
	cap->path.startup_cost = 0;
	/*
	 * "First chunk satisfies LIMIT" cost model.  Discount by n_chunks
	 * to reflect that, on average, only one chunk is scanned end-to-end
	 * (rest are lazy-init and skipped after LIMIT satisfaction).  This
	 * makes ChunkAppend win against parallel ChunkScan (which has
	 * scan-cost / parallel_workers division built in) on the LIMIT
	 * shape.
	 */
	cap->path.total_cost = first_cost / Max(n_chunks, 1) * 0.5;
	/*
	 * Advertise the query's pathkey (same object).  Upstream
	 * pathkey-satisfaction check uses pointer identity, so the Sort
	 * the planner would otherwise add is elided.  This is what makes
	 * early-stop work within a segment.
	 */
	cap->path.pathkeys = list_make1(query_pk);
	cap->path.locus = ((Path *) linitial(sub_paths))->locus;
	cap->flags = 0;
	cap->custom_paths = sub_paths;
	cap->custom_private = NIL;
	cap->methods = &ts_chunk_append_path_methods;

	return (Path *) cap;
}

/*
 *		planner_hook wrapper: inject outer Limit when ChunkAppend
 *		is the path leading into a Motion (Gather).
 *
 *	CB's grouping_planner elides the QD-side outer Limit when the
 *	cheapest path advertises matching pathkeys (since it thinks the
 *	per-segment Limit covers ORDER BY+LIMIT semantics).  That's wrong
 *	for distributed tables — each segment returns N rows, so the
 *	client sees N×n_segments without a top-level Limit.  Walk the
 *	produced PlannedStmt; if the top is a Motion whose subtree
 *	contains ChunkAppend AND the immediate child is a Limit, prepend
 *	an outer Limit at the top.
 *
 *	prev_planner_hook_for_chunk_append lives at the top of this file.
 */

static bool
tree_contains_chunk_append(Plan *plan)
{
	if (plan == NULL)
		return false;

	if (IsA(plan, CustomScan))
	{
		CustomScan *cs = (CustomScan *) plan;

		if (cs->methods != NULL && cs->methods->CustomName != NULL &&
			strcmp(cs->methods->CustomName, "ChunkAppend") == 0)
			return true;
	}
	if (tree_contains_chunk_append(plan->lefttree))
		return true;
	if (tree_contains_chunk_append(plan->righttree))
		return true;
	return false;
}

static Plan *
ts_chunk_append_inject_outer_limit(Plan *top, Query *parse)
{
	Motion	   *motion;
	Limit	   *inner_limit;
	Limit	   *outer_limit;

	if (top == NULL || !IsA(top, Motion))
		return top;
	motion = (Motion *) top;
	if (motion->motionType != MOTIONTYPE_GATHER)
		return top;
	if (motion->plan.lefttree == NULL || !IsA(motion->plan.lefttree, Limit))
		return top;
	if (!tree_contains_chunk_append(motion->plan.lefttree))
		return top;
	if (parse->limitCount == NULL)
		return top;

	inner_limit = (Limit *) motion->plan.lefttree;
	outer_limit = make_limit(top,
							  /* re-use the parse-level expressions; they're
							   * Const-foldable and idempotent under repeat
							   * evaluation (no side effects). */
							  parse->limitOffset,
							  parse->limitCount,
							  inner_limit->limitOption,
							  0, NULL, NULL, NULL);
	return (Plan *) outer_limit;
}

static PlannedStmt *
ts_chunk_append_planner_wrapper(Query *parse, const char *query_string,
								 int cursorOptions, ParamListInfo boundParams,
								 OptimizerOptions *optimizer_options)
{
	PlannedStmt *stmt;

	if (prev_planner_hook_for_chunk_append)
		stmt = prev_planner_hook_for_chunk_append(parse, query_string,
												   cursorOptions, boundParams,
												   optimizer_options);
	else
		stmt = standard_planner(parse, query_string, cursorOptions,
								 boundParams, optimizer_options);

	if (ts_enable_chunk_append && stmt != NULL && stmt->planTree != NULL)
		stmt->planTree = ts_chunk_append_inject_outer_limit(stmt->planTree,
															 parse);
	return stmt;
}

/*
 * Init: register methods with the executor
 */
void
ts_chunk_append_init(void)
{
	DefineCustomBoolVariable("time_series.enable_chunk_append",
							 "Generate ChunkAppend paths for ORDER BY <time> + LIMIT",
							 "When on and the planner sees ORDER BY on the time "
							 "column (or time_bucket(time)) + LIMIT, generates a "
							 "ChunkAppend CustomScan path whose subpaths are "
							 "Sort(time) wrapping per-chunk ChunkScan.  The "
							 "executor demand-pulls subplans one chunk at a "
							 "time, so an upstream Limit can stop pulling once "
							 "satisfied and subsequent chunks' Sort+Scan never "
							 "initialise.  M1: single-segment only; MPP Motion "
							 "barrier prevents end-to-end early-stop until M2.",
							 &ts_enable_chunk_append,
							 true,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	DefineCustomIntVariable("time_series.chunk_append_max_chunks",
							"Maximum number of per-chunk subplans ChunkAppend will build",
							"QD cannot read the per-segment ts_chunk catalog, so the "
							"planner enumerates chunk_nums in [lo, hi] derived from "
							"WHERE time bounds.  This cap bounds how many subplans "
							"the planner is willing to materialise; queries whose "
							"time range would produce more chunks than this fall "
							"back to plain ChunkScan.  Default 64 ≈ 21 days at the "
							"default 8h chunk_time.",
							&ts_chunk_append_max_chunks,
							64,
							1,
							INT_MAX,
							PGC_USERSET,
							0,
							NULL, NULL, NULL);

	ts_chunk_append_path_methods.CustomName = "ChunkAppend";
	ts_chunk_append_path_methods.PlanCustomPath = ts_chunk_append_create_plan;
	ts_chunk_append_path_methods.ReparameterizeCustomPathByChild = NULL;

	ts_chunk_append_scan_methods.CustomName = "ChunkAppend";
	ts_chunk_append_scan_methods.CreateCustomScanState = ts_chunk_append_create_state;
	RegisterCustomScanMethods(&ts_chunk_append_scan_methods);

	ts_chunk_append_exec_methods.CustomName = "ChunkAppend";
	ts_chunk_append_exec_methods.BeginCustomScan = ts_chunk_append_begin;
	ts_chunk_append_exec_methods.ExecCustomScan = ts_chunk_append_exec;
	ts_chunk_append_exec_methods.EndCustomScan = ts_chunk_append_end;
	ts_chunk_append_exec_methods.ReScanCustomScan = ts_chunk_append_rescan;
	ts_chunk_append_exec_methods.ExplainCustomScan = ts_chunk_append_explain;

	/* Chain planner_hook for outer-Limit injection (see comment above). */
	prev_planner_hook_for_chunk_append = planner_hook;
	planner_hook = ts_chunk_append_planner_wrapper;
}
