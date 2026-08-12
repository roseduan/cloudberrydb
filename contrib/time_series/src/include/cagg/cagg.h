/*-------------------------------------------------------------------------
 *
 * cagg.h
 *    Public API of the continuous-aggregate subsystem.
 *
 *    Declares every symbol callable from outside src/cagg/: the
 *    _PG_init hook registrations, the SQL-callable Datum functions,
 *    the CAGG-owned GUC variables, and the planner-side rewrite
 *    helpers.  Kept separate from the extension-wide time_series.h
 *    so that non-CAGG translation units are not exposed to CAGG
 *    declarations.
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 * IDENTIFICATION
 *    contrib/time_series/src/include/cagg/cagg.h
 *-------------------------------------------------------------------------
 */
#ifndef TS_CAGG_H
#define TS_CAGG_H

#include "postgres.h"
#include "fmgr.h"
#include "nodes/parsenodes.h"

/* ---- Hook registration (called from _PG_init) ---- */

/* ProcessUtility hook for CREATE MATERIALIZED VIEW WITH (time_series.continuous). */
extern void ht_cagg_init(void);

/*
 * CAGG planner_hook for parse-tree rewrites (cagg_watermark const-fold +
 * time_bucket pushdown).  Chains to the previously-installed planner_hook.
 * Must be called AFTER ht_gapfill_planner_init.
 */
extern void cagg_planner_hook_init(void);

/* Relcache-invalidation callback for the backend-local watermark cache. */
extern void cagg_watermark_cache_init(void);

/* Define the CAGG-owned GUCs. */
extern void cagg_define_gucs(void);

/* ---- CAGG-owned GUC variables ---- */

/*
 * time_series.enable_cagg_create -- master switch for the CREATE
 * MATERIALIZED VIEW ... WITH (time_series.continuous) handler.  When
 * off, new CAGG creation is rejected; pre-existing CAGGs are
 * unaffected.
 */
extern bool guc_enable_cagg_create;

/*
 * time_series.materializations_per_refresh_window -- max number of
 * individual interval refreshes per REFRESH call.  If exceeded, all
 * intervals are merged into a single large refresh to avoid fragmented
 * I/O.  0 = unlimited.
 */
extern int	guc_materializations_per_refresh_window;

/* ---- SQL-callable functions ---- */

/* Row-level trigger: write dirty time ranges to L1. */
extern Datum cagg_invalidation_trigfn(PG_FUNCTION_ARGS);

/* Segment-local watermark initialization (dispatched via gp_dist_random). */
extern Datum cagg_init_segment_watermark(PG_FUNCTION_ARGS);

/* Per-segment watermark lookup (C, no SPI -- safe on segment QEs). */
extern Datum cagg_watermark_fn(PG_FUNCTION_ARGS);

/* Segment-local L1 -> L2 migration (called via dispatch). */
extern Datum cagg_segment_move_l1_to_l2(PG_FUNCTION_ARGS);

/* CALL time_series.refresh_continuous_aggregate(name, start, end). */
extern Datum cagg_refresh(PG_FUNCTION_ARGS);

/* ---- Planner-side rewrite helpers ---- */

/*
 * Const-fold cagg_watermark(N) FuncExpr -> Const literal at planner
 * time on QD.  Idempotent; no-op on segments and non-SELECT queries.
 */
extern void constify_cagg_watermark_mutate(Query *parse);

/*
 * SQL DML targeting cagg_watermark broadcasts the watermark-cache
 * invalidation itself (covers manual UPDATEs that the explicit
 * refresh/TRUNCATE call sites cannot see).
 */
extern void cagg_watermark_dml_inval(Query *parse);

/*
 * For outer quals of the form `Var(sub.bucket) {>=,>} Const` whose
 * subquery tlist entry is a `time_bucket(B, V)` FuncExpr, append the
 * equivalent bare-Var predicate `V {>=,>} Const` to the subquery's
 * WHERE.  Idempotent; no-op on segments and non-SELECT queries.
 */
extern void time_bucket_pushdown_mutate(Query *parse);

#endif							/* TS_CAGG_H */
