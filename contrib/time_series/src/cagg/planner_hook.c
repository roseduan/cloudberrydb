/*-------------------------------------------------------------------------
 *
 * planner_hook.c
 *    PG planner_hook entry that runs CAGG-specific parse-tree rewrites
 *    before delegating to the next hook in the chain.
 *
 *    Two walkers are invoked, in order:
 *
 *      1. constify_cagg_watermark_mutate (watermark_constify.c)
 *         Replaces VOLATILE cagg_watermark(N) FuncExpr nodes with a
 *         TimestampTz Const literal containing the global MIN watermark
 *         across all segments.  This unlocks PG's standard const-fold
 *         and chunk-pruning paths that VOLATILE would otherwise block.
 *
 *      2. time_bucket_pushdown_mutate (time_bucket_pushdown.c)
 *         Synthesizes equivalent bare-time-column predicates from
 *         outer-level `Var(sub.bucket) {>=,>,<=,<} Const` quals so the
 *         chunk pruner can use them (time_bucket() is opaque to
 *         standard chunk-range analysis).
 *
 *    Both walkers are no-ops on segments and on non-SELECT queries.
 *
 *    Why a separate hook (instead of extending ht_gapfill_planner):
 *
 *    ht_gapfill_planner (gapfill_plan.c) is the colleague-authored hook
 *    dedicated to gapfill custom-scan path injection and ORCA toggling.
 *    The CAGG rewrites here are orthogonal -- they fire on any cv view
 *    query, not just gapfill ones.  Keeping them in their own hook file
 *    preserves the gapfill module's single responsibility and makes the
 *    hook chain easy to follow:
 *
 *        PG -> cagg_planner_hook -> ht_gapfill_planner -> standard_planner
 *              (this file)           (gapfill_plan.c)
 *
 *    The chain order is established in time_series.c _PG_init: first
 *    ht_gapfill_planner_init() captures the original planner_hook and
 *    installs itself; then cagg_planner_hook_init() captures the just-
 *    installed gapfill hook as its prev and installs itself.
 *
 * Copyright (c) 2026 HashData Inc.
 * Licensed under Apache License 2.0
 *
 * IDENTIFICATION
 *    contrib/time_series/src/cagg/planner_hook.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "nodes/parsenodes.h"
#include "optimizer/planner.h"
#include "utils/relcache.h"

#include "../include/time_series.h"
#include "../include/access/ts_tableam.h"
#include "../include/cagg/cagg.h"

/* Previously-installed planner_hook (typically ht_gapfill_planner). */
static planner_hook_type prev_planner_hook = NULL;

/*
 * Reject UPDATE on a time_series table at plan time, before execution.
 *
 * time_series tables are append-only.  The table-AM layer already guards
 * DELETE/UPDATE/row-lock in ts_heap_tuple_{delete,update,lock}() with a
 * clean "cannot ... a time_series table" ERROR.  That guard fires for
 * DELETE, but NOT for UPDATE in the MPP executor: ExecUpdate must first
 * fetch+lock the old tuple by TID, and because time_series scatters rows
 * across chunk forks (the TID does not encode the fork), that fetch fails
 * in nodeModifyTable.c with the opaque internal error "failed to fetch
 * tuple being updated" before tuple_update() is ever reached.  Catching
 * the UPDATE here on the QD turns that into the same intentional,
 * user-facing error DELETE already produces.
 */
static void
reject_update_on_time_series(Query *parse)
{
	RangeTblEntry *rte;
	Relation	rel;
	bool		is_ts;

	if (parse->commandType != CMD_UPDATE || parse->resultRelation == 0)
		return;

	rte = (RangeTblEntry *) list_nth(parse->rtable, parse->resultRelation - 1);
	if (rte == NULL || rte->rtekind != RTE_RELATION || !OidIsValid(rte->relid))
		return;

	/*
	 * The result relation is already locked by the rewriter/planner, so a
	 * relcache lookup is cheap and does not change lock state.
	 */
	rel = RelationIdGetRelation(rte->relid);
	if (rel == NULL)
		return;
	is_ts = RelationIsTimeSeries(rel);
	RelationClose(rel);

	if (is_ts)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot update a time_series table"),
				 errhint("time_series tables are append-only")));
}

/*
 * The CAGG planner_hook.  Runs the two parse-tree rewrites, then
 * delegates to whatever planner_hook was installed before us (which
 * eventually reaches standard_planner).
 */
static PlannedStmt *
cagg_planner_hook(Query *parse,
				  const char *query_string,
				  int cursorOptions,
				  ParamListInfo boundParams,
				  OptimizerOptions * optimizer_options)
{
	/*
	 * Guard: UPDATE on an append-only time_series table must fail with a
	 * clean error here, not an opaque executor error later.  See the function
	 * comment for why this cannot be left to the table-AM layer.
	 */
	reject_update_on_time_series(parse);

	/*
	 * Coherence: SQL DML that targets time_series.cagg_watermark must
	 * broadcast the backend watermark-cache invalidation itself (manual
	 * watermark surgery has no other hook point -- CBDB lacks statement
	 * triggers and segment-side row triggers can't reach QD sinval). See
	 * watermark_constify.c.
	 */
	cagg_watermark_dml_inval(parse);

	/*
	 * Rewrite 1: const-fold cagg_watermark(N) FuncExpr to a Const literal
	 * containing the global MIN watermark across segments.  See
	 * watermark_constify.c for the full rationale.
	 */
	constify_cagg_watermark_mutate(parse);

	/*
	 * Rewrite 2: push down lower/upper-bound time_bucket predicates so the
	 * live CAGG branch can do chunk pruning on the bare time column.  Runs
	 * after constify so any Const produced there is visible to the walker.
	 * See time_bucket_pushdown.c.
	 */
	time_bucket_pushdown_mutate(parse);

	/* Delegate to the next hook in the chain. */
	if (prev_planner_hook != NULL)
		return prev_planner_hook(parse, query_string, cursorOptions,
								 boundParams, optimizer_options);
	return standard_planner(parse, query_string, cursorOptions,
							boundParams, optimizer_options);
}

/*
 * Install the CAGG planner_hook.  Must be called from _PG_init AFTER any
 * other hook (e.g. ht_gapfill_planner_init) so that this hook sits at the
 * OUTERMOST layer of the chain -- its rewrites must run before other
 * hooks see the parse tree, otherwise downstream planning will still see
 * the VOLATILE cagg_watermark FuncExpr and miss the chunk-pruning
 * opportunity.
 */
void
cagg_planner_hook_init(void)
{
	prev_planner_hook = planner_hook;
	planner_hook = cagg_planner_hook;
}
