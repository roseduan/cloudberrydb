/*-------------------------------------------------------------------------
 *
 * gapfill.h
 *    Public API of the gapfill subsystem.
 *
 *    GapFill inserts synthetic rows for time buckets that contain no
 *    real data, filling gaps in time-series result sets.  It is
 *    implemented as a Custom Scan node so the planner can place it
 *    correctly within the plan tree.
 *
 *    Layout:
 *      src/gapfill/gapfill.c  -- SQL marker + bucket wrappers
 *      src/gapfill/plan.c     -- planner integration
 *      src/gapfill/exec.c     -- executor state machine (includes
 *                                LOCF and interpolate semantics)
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 * IDENTIFICATION
 *    contrib/time_series/src/include/gapfill/gapfill.h
 *-------------------------------------------------------------------------
 */
#ifndef TS_GAPFILL_H
#define TS_GAPFILL_H

#include "postgres.h"
#include "fmgr.h"
#include "nodes/extensible.h"

/* ---- SQL-callable functions ---- */

/*
 * Marker function used as a sentinel by locf() and interpolate().  Pass-
 * through at the SQL level; the gapfill executor recognises calls to it
 * and replaces the result with the appropriate filled value.
 */
extern Datum ht_gapfill_marker(PG_FUNCTION_ARGS);

/*
 * GapFill-aware bucket wrappers.  Delegate to the matching ts_*_bucket
 * function but are distinguished by name so the planner hook can
 * identify them as gapfill bucket expressions.
 */
extern Datum ht_gapfill_timestamp_bucket(PG_FUNCTION_ARGS);
extern Datum ht_gapfill_timestamptz_bucket(PG_FUNCTION_ARGS);
extern Datum ht_gapfill_int16_bucket(PG_FUNCTION_ARGS);
extern Datum ht_gapfill_int32_bucket(PG_FUNCTION_ARGS);
extern Datum ht_gapfill_int64_bucket(PG_FUNCTION_ARGS);
extern Datum ht_gapfill_date_bucket(PG_FUNCTION_ARGS);
extern Datum ht_gapfill_timestamptz_timezone_bucket(PG_FUNCTION_ARGS);

/* ---- Hook registration (called from _PG_init) ---- */

/* Registers the GapFill Custom Scan provider with the executor. */
extern void ht_gapfill_scan_init(void);

/* Installs the GapFill planner hook. */
extern void ht_gapfill_planner_init(void);

/*
 * CreateCustomScanState callback invoked by the executor when it
 * encounters a GapFill Custom Scan node in a plan tree.
 */
extern Node *ht_gapfill_create_state(CustomScan *cscan);

/*
 * Shared CustomScanMethods descriptor referenced by both the planning
 * and execution phases to identify GapFill plan nodes.
 */
extern const CustomScanMethods ht_gapfill_scan_methods;

#endif							/* TS_GAPFILL_H */
