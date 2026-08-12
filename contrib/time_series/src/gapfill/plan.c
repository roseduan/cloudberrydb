/*
 * gapfill_plan.c - GapFill Planner Hook
 *
 * Installs a create_upper_paths_hook that detects gapfill calls
 * (time_bucket_gapfill) in the GROUP BY target list during the
 * UPPERREL_GROUP_AGG stage, and wraps the Aggregate path with
 * a GapFill CustomPath.
 *
 * Clean-room implementation for time_series extension.
 *
 * Copyright (c) 2026 HashData Inc.
 * Licensed under Apache License 2.0
 */
#include "../include/time_series.h"
#include "../include/gapfill/gapfill.h"

#include "catalog/namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "nodes/makefuncs.h"
#include "utils/builtins.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pathnodes.h"
#include "nodes/pg_list.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/orcaopt.h"       /* OptimizerOptions */
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"         /* make_canonical_pathkey */
#include "optimizer/planner.h"
#include "access/stratnum.h"         /* BTLessStrategyNumber */
#include "utils/guc.h"              /* optimizer GUC */
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/date.h"
#include "utils/timestamp.h"
#include "optimizer/tlist.h"

#include "cdb/cdbpath.h"
#include "cdb/cdbpathlocus.h"
#include "cdb/cdbvars.h"

/* Previous hooks */
static planner_hook_type prev_planner_hook = NULL;
static create_upper_paths_hook_type prev_create_upper_paths_hook = NULL;

/* Forward declarations */
static void ht_gapfill_create_upper_paths(PlannerInfo *root,
										  UpperRelationKind stage,
										  RelOptInfo *input_rel,
										  RelOptInfo *output_rel,
										  void *extra);
static Plan *ht_gapfill_plan_create(PlannerInfo *root, RelOptInfo *rel,
									CustomPath *best_path, List *tlist,
									List *clauses, List *custom_plans);

/* Custom Path Methods for GapFill */
static const CustomPathMethods ht_gapfill_path_methods = {
	.CustomName = "GapFill",
	.PlanCustomPath = ht_gapfill_plan_create,
};

/* ================================================================
 * Helper: Check if a FuncExpr calls a specific time_series function
 * ================================================================ */

static bool
is_extension_function(Oid funcid, const char *target_name)
{
	Oid			ext_nsp;
	char	   *funcname;
	Oid			func_nsp;

	ext_nsp = ht_get_namespace_oid_cached();
	if (!OidIsValid(ext_nsp))
		return false;

	func_nsp = get_func_namespace(funcid);
	if (func_nsp != ext_nsp)
		return false;

	funcname = get_func_name(funcid);
	if (!funcname)
		return false;

	if (strcmp(funcname, target_name) == 0)
	{
		pfree(funcname);
		return true;
	}

	pfree(funcname);
	return false;
}

static bool
is_gapfill_function(Oid funcid)
{
	return is_extension_function(funcid, "time_bucket_gapfill");
}

static bool
is_locf_function(Oid funcid)
{
	return is_extension_function(funcid, "locf");
}

static bool
is_interpolate_function(Oid funcid)
{
	return is_extension_function(funcid, "interpolate");
}

/*
 * Walker to detect time_bucket_gapfill() nested inside an expression.
 * Uses expression_tree_walker (not query_tree_walker) so it does NOT
 * descend into subqueries — avoids false positives when gapfill is
 * used correctly as a top-level expression in an inner subquery.
 */
typedef struct
{
	bool	found;
} NestedGapfillWalkerContext;

static bool
nested_gapfill_walker(Node *node, NestedGapfillWalkerContext *ctx)
{
	if (node == NULL)
		return false;
	if (IsA(node, FuncExpr) && is_gapfill_function(((FuncExpr *) node)->funcid))
	{
		ctx->found = true;
		return true;	/* stop walking */
	}
	return expression_tree_walker(node, nested_gapfill_walker, ctx);
}

/*
 * Walker to detect locf()/interpolate() nested inside an expression.
 * Used to validate that these functions are top-level in the target list
 * and not wrapped in other expressions (e.g., 1 + locf(...), round(interpolate(...))).
 */
typedef struct
{
	bool	found;
	char   *funcname;	/* "locf" or "interpolate" — for error message */
} NestedMarkerWalkerContext;

static bool
nested_marker_walker(Node *node, NestedMarkerWalkerContext *ctx)
{
	if (node == NULL)
		return false;

	/*
	 * Do not descend into WindowFunc nodes — window functions operate
	 * AFTER the GapFill phase on already gap-filled results. Any
	 * locf/interpolate inside (e.g., lag(interpolate(min(x))) OVER ())
	 * is a valid reference to a gap-filled column, not a misplaced call.
	 */
	if (IsA(node, WindowFunc))
		return false;

	if (IsA(node, FuncExpr))
	{
		FuncExpr *fe = (FuncExpr *) node;
		if (is_locf_function(fe->funcid))
		{
			ctx->found = true;
			ctx->funcname = "locf";
			return true;
		}
		if (is_interpolate_function(fe->funcid))
		{
			ctx->found = true;
			ctx->funcname = "interpolate";
			return true;
		}
	}
	return expression_tree_walker(node, nested_marker_walker, ctx);
}

/*
 * Walk target list to find a gapfill FuncExpr.
 * Returns the FuncExpr if found, NULL otherwise.
 * Also returns the 0-based target entry index via *attno_out.
 *
 * Phase 1: Check if any target entry is a top-level gapfill FuncExpr.
 * Phase 2: If not found, recursively check if gapfill is nested inside
 *          an expression (e.g., EXTRACT(hour FROM gapfill(...))). If so,
 *          raise an error with a helpful hint to use a subquery.
 */
static FuncExpr *
locate_gapfill_funcexpr(List *targetlist, int *attno_out)
{
	ListCell   *lc;
	int			attno = 0;
	FuncExpr   *result_fe = NULL;
	int			result_attno = -1;
	int			gapfill_count = 0;

	/*
	 * Phase 1: Scan target entries for top-level gapfill calls.
	 * Count total occurrences — multiple calls are not allowed.
	 * Also check args of each top-level gapfill for nested gapfill
	 * calls (e.g., gapfill(gapfill(...))).
	 */
	foreach(lc, targetlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		Node	   *expr;

		if (tle->resjunk)
		{
			attno++;
			continue;
		}

		expr = (Node *) tle->expr;

		if (IsA(expr, FuncExpr))
		{
			FuncExpr *fe = (FuncExpr *) expr;
			if (is_gapfill_function(fe->funcid))
			{
				NestedGapfillWalkerContext nctx = {false};

				gapfill_count++;
				if (result_fe == NULL)
				{
					result_fe = fe;
					result_attno = attno;
				}

				/* Check for nested gapfill inside this gapfill's args */
				expression_tree_walker((Node *) fe->args,
									   nested_gapfill_walker, &nctx);
				if (nctx.found)
					gapfill_count++;
			}
		}
		attno++;
	}

	if (gapfill_count > 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("multiple time_bucket_gapfill calls not allowed")));

	if (result_fe != NULL)
	{
		*attno_out = result_attno;
		return result_fe;
	}

	/*
	 * Phase 2: gapfill not found as top-level expression.
	 * Recursively check each non-junk target entry for a nested gapfill
	 * call. If found, error out — gapfill must be top-level so the
	 * GapFill Custom Scan can intercept the plan node.
	 */
	foreach(lc, targetlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		NestedGapfillWalkerContext ctx = {false};

		if (tle->resjunk)
			continue;

		nested_gapfill_walker((Node *) tle->expr, &ctx);
		if (ctx.found)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("time_bucket_gapfill must be a top-level expression in the target list"),
					 errhint("Use a subquery to apply gap filling first, then wrap the result. "
							 "For example: SELECT EXTRACT(hour FROM bucket) FROM "
							 "(SELECT time_bucket_gapfill(...) AS bucket ... GROUP BY 1) sub")));
	}

	*attno_out = -1;
	return NULL;
}

/* ================================================================
 * Extract gapfill parameters into custom_private
 * ================================================================
 *
 * We serialize all gapfill parameters at planning time because
 * the executor's plan targetlist may contain Var references to
 * child plan output rather than the original FuncExpr nodes.
 * locf() and interpolate() also get stripped by the planner into
 * a Result projection above GapFill, so we detect them here.
 *
 * custom_private layout:
 *   [0] bucket_attno (Integer) — 0-based column index in output
 *   [1] time_type OID (Integer) — result type of the bucket function
 *   [2] bucket_width_hi (Integer) — high 32 bits
 *   [3] bucket_width_lo (Integer) — low 32 bits
 *   [4] num_locf_cols (Integer) — number of LOCF columns
 *   [5] start_usec_hi (Integer)
 *   [6] start_usec_lo (Integer)
 *   [7] finish_usec_hi (Integer)
 *   [8] finish_usec_lo (Integer)
 *   [9] month_period (Integer) — 0=non-month, N=N-month interval
 *   [10] timezone_name (String) — ""=no timezone
 *   [11..11+N*2-1] LOCF entries: (attno, typoid) pairs
 *   [11+N*2] num_interp_cols (Integer)
 *   [11+N*2+1..] Interpolate entries: (attno, typoid) pairs
 *   [end_of_interp] num_group_cols (Integer) — GROUP BY columns before bucket
 *   [end_of_interp+1..] group column attnos (Integer each, 0-based)
 */
static List *
serialize_gapfill_params(FuncExpr *gapfill_fe, int bucket_attno,
					  List *parse_targetlist, PlannerInfo *root)
{
	List	   *result = NIL;
	Oid			time_type;
	int64		bucket_width_usec = 0;
	int32		month_period = 0;
	char	   *timezone_name = NULL;
	int64		start_usec = PG_INT64_MIN;
	int64		finish_usec = PG_INT64_MIN;
	int			start_idx = 2;		/* default: arg[2]=start, arg[3]=finish */
	int			finish_idx = 3;
	List	   *args = gapfill_fe->args;

	time_type = gapfill_fe->funcresulttype;

	/*
	 * Detect timezone variant: (interval, timestamptz, text, start, finish)
	 * Standard variant:        (interval, ts, start, finish)
	 */
	if (list_length(args) >= 5)
	{
		Node *arg2 = (Node *) list_nth(args, 2);

		if (IsA(arg2, Const) && ((Const *) arg2)->consttype == TEXTOID)
		{
			Const *tz_const = (Const *) arg2;

			if (!tz_const->constisnull)
				timezone_name = TextDatumGetCString(tz_const->constvalue);

			/* Shift start/finish indices past timezone argument */
			start_idx = 3;
			finish_idx = 4;
		}
		else if (!IsA(arg2, Const) && exprType(arg2) == TEXTOID)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("time_bucket_gapfill timezone argument must be a constant"),
					 errhint("Use a literal timezone string, e.g., 'US/Pacific'.")));
		}
	}

	/* Extract bucket_width from first argument */
	if (list_length(args) >= 1)
	{
		Node *width_arg = (Node *) linitial(args);

		if (IsA(width_arg, Const))
		{
			Const *c = (Const *) width_arg;

			if (c->constisnull)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("time_bucket_gapfill: bucket_width cannot be NULL")));

			{
				if (c->consttype == INTERVALOID)
				{
					Interval *iv = DatumGetIntervalP(c->constvalue);

					if (iv->month != 0)
					{
						/* Month interval: must not mix with day/time */
						if (iv->day != 0 || iv->time != 0)
							ereport(ERROR,
									(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									 errmsg("month intervals cannot have day or time component")));
						month_period = iv->month;
						bucket_width_usec = 0;
					}
					else
					{
						bucket_width_usec = iv->time +
							(int64) iv->day * USECS_PER_DAY;
					}
				}
				else
				{
					/* Integer types */
					switch (c->consttype)
					{
						case INT2OID:
							bucket_width_usec = (int64) DatumGetInt16(c->constvalue);
							break;
						case INT4OID:
							bucket_width_usec = (int64) DatumGetInt32(c->constvalue);
							break;
						case INT8OID:
							bucket_width_usec = DatumGetInt64(c->constvalue);
							break;
						default:
							break;
					}
				}
			}
		}
	}

	/* Extract start argument */
	if (list_length(args) > start_idx)
	{
		Node *start_arg = (Node *) list_nth(args, start_idx);

		if (IsA(start_arg, Const) && !((Const *) start_arg)->constisnull)
		{
			Const *c = (Const *) start_arg;

			switch (time_type)
			{
				case TIMESTAMPOID:
					start_usec = DatumGetTimestamp(c->constvalue);
					break;
				case TIMESTAMPTZOID:
					start_usec = DatumGetTimestampTz(c->constvalue);
					break;
				case DATEOID:
					start_usec = (int64) DatumGetDateADT(c->constvalue) * USECS_PER_DAY;
					break;
				case INT2OID:
					start_usec = (int64) DatumGetInt16(c->constvalue);
					break;
				case INT4OID:
					start_usec = (int64) DatumGetInt32(c->constvalue);
					break;
				case INT8OID:
					start_usec = DatumGetInt64(c->constvalue);
					break;
				default:
					break;
			}
		}
		else if (!IsA(start_arg, Const))
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("time_bucket_gapfill start argument must be a constant value"),
					 errhint("Use a literal or a simple expression that the planner can fold to a constant.")));
		}
	}

	/* Extract finish argument */
	if (list_length(args) > finish_idx)
	{
		Node *finish_arg = (Node *) list_nth(args, finish_idx);

		if (IsA(finish_arg, Const) && !((Const *) finish_arg)->constisnull)
		{
			Const *c = (Const *) finish_arg;

			switch (time_type)
			{
				case TIMESTAMPOID:
					finish_usec = DatumGetTimestamp(c->constvalue);
					break;
				case TIMESTAMPTZOID:
					finish_usec = DatumGetTimestampTz(c->constvalue);
					break;
				case DATEOID:
					finish_usec = (int64) DatumGetDateADT(c->constvalue) * USECS_PER_DAY;
					break;
				case INT2OID:
					finish_usec = (int64) DatumGetInt16(c->constvalue);
					break;
				case INT4OID:
					finish_usec = (int64) DatumGetInt32(c->constvalue);
					break;
				case INT8OID:
					finish_usec = DatumGetInt64(c->constvalue);
					break;
				default:
					break;
			}
		}
		else if (!IsA(finish_arg, Const))
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("time_bucket_gapfill finish argument must be a constant value"),
					 errhint("Use a literal or a simple expression that the planner can fold to a constant.")));
		}
	}

	/*
	 * Count LOCF and interpolate columns from the parse targetlist.
	 * These function wrappers get separated into a Result node above
	 * GapFill by the planner, so we must detect them here.
	 */
	{
		ListCell   *lc2;
		int			num_locf = 0;
		int			num_interp = 0;
		int			col_idx;
		List	   *locf_inner_exprs = NIL;

		/* First pass: count and collect inner expressions for collision check */
		col_idx = 0;
		foreach(lc2, parse_targetlist)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(lc2);
			Node	   *expr;

			if (tle->resjunk)
			{
				col_idx++;
				continue;
			}
			expr = (Node *) tle->expr;
			if (IsA(expr, FuncExpr))
			{
				FuncExpr *fe = (FuncExpr *) expr;
				if (is_locf_function(fe->funcid))
				{
					NestedMarkerWalkerContext mctx = {false, NULL};

					num_locf++;
					if (list_length(fe->args) >= 1)
						locf_inner_exprs = lappend(locf_inner_exprs,
												   linitial(fe->args));

					/* Check for nested locf/interpolate inside this locf's args */
					expression_tree_walker((Node *) fe->args,
										   nested_marker_walker, &mctx);
					if (mctx.found)
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("multiple locf/interpolate function calls per column are not supported"),
								 errhint("Use only one locf() or interpolate() call per target list column.")));

					/* Check for window functions inside locf's args */
					if (contain_window_function((Node *) fe->args))
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("window functions must not be below %s", "locf"),
								 errhint("Window functions cannot be arguments to gap-fill functions.")));
				}
				else if (is_interpolate_function(fe->funcid))
				{
					NestedMarkerWalkerContext mctx = {false, NULL};

					num_interp++;

					/* Check for nested locf/interpolate inside this interpolate's args */
					expression_tree_walker((Node *) fe->args,
										   nested_marker_walker, &mctx);
					if (mctx.found)
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("multiple locf/interpolate function calls per column are not supported"),
								 errhint("Use only one locf() or interpolate() call per target list column.")));

					/* Check for window functions inside interpolate's args */
					if (contain_window_function((Node *) fe->args))
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("window functions must not be below %s", "interpolate"),
								 errhint("Window functions cannot be arguments to gap-fill functions.")));
				}
			}

			/*
			 * Check for nested (non-top-level) locf/interpolate calls.
			 * This catches cases like: 1 + locf(...), round(interpolate(...)),
			 * COALESCE(locf(...), 0), min(locf(t)), etc.
			 *
			 * Skip WindowFunc entries — window functions operate AFTER the
			 * GapFill phase on already gap-filled results.  Patterns like
			 * sum(interpolate(min(t))) OVER (...) are valid: the planner
			 * remaps the interpolate column reference for the WindowAgg.
			 */
			if (!IsA(expr, WindowFunc) &&
				(!IsA(expr, FuncExpr) ||
				 (!is_locf_function(((FuncExpr *) expr)->funcid) &&
				  !is_interpolate_function(((FuncExpr *) expr)->funcid))))
			{
				NestedMarkerWalkerContext mctx = {false, NULL};

				nested_marker_walker(expr, &mctx);
				if (mctx.found)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("%s must be a top-level expression in the target list",
									mctx.funcname),
							 errhint("Use a subquery to apply gap filling first, then wrap the result. "
									 "For example: SELECT round(val) FROM "
									 "(SELECT locf(avg(x)) AS val ... GROUP BY 1) sub")));
			}

			col_idx++;
		}

		/* Check for locf+interpolate collision on the same aggregate */
		if (num_locf > 0 && num_interp > 0)
		{
			foreach(lc2, parse_targetlist)
			{
				TargetEntry *tle = (TargetEntry *) lfirst(lc2);

				if (!tle->resjunk && IsA(tle->expr, FuncExpr))
				{
					FuncExpr *fe = (FuncExpr *) tle->expr;

					if (is_interpolate_function(fe->funcid) &&
						list_length(fe->args) >= 1)
					{
						ListCell *lci;

						foreach(lci, locf_inner_exprs)
						{
							if (equal(linitial(fe->args), lfirst(lci)))
								ereport(ERROR,
										(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
										 errmsg("cannot use both locf() and interpolate() on the same expression"),
										 errhint("Use locf() or interpolate(), not both, for each aggregate column.")));
						}
					}
				}
			}
		}

		/* Build result: first 5 fixed entries */
		result = list_make5(makeInteger(bucket_attno),
							makeInteger((long) time_type),
							makeInteger((long) (bucket_width_usec >> 32)),
							makeInteger((long) (bucket_width_usec & 0xFFFFFFFF)),
							makeInteger((long) num_locf));

		/* Append start_usec and finish_usec as high/low 32-bit pairs */
		result = lappend(result, makeInteger((long) (start_usec >> 32)));
		result = lappend(result, makeInteger((long) (start_usec & 0xFFFFFFFF)));
		result = lappend(result, makeInteger((long) (finish_usec >> 32)));
		result = lappend(result, makeInteger((long) (finish_usec & 0xFFFFFFFF)));

		/* Append month_period and timezone_name */
		result = lappend(result, makeInteger((long) month_period));
		result = lappend(result, makeString(timezone_name ? pstrdup(timezone_name) : pstrdup("")));

		/* Second pass: append LOCF (attno, typoid) pairs */
		col_idx = 0;
		foreach(lc2, parse_targetlist)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(lc2);
			Node	   *expr;

			if (tle->resjunk)
			{
				col_idx++;
				continue;
			}
			expr = (Node *) tle->expr;
			if (IsA(expr, FuncExpr))
			{
				FuncExpr *fe = (FuncExpr *) expr;
				if (is_locf_function(fe->funcid))
				{
					result = lappend(result, makeInteger(col_idx));
					result = lappend(result, makeInteger((long) fe->funcresulttype));
				}
			}
			col_idx++;
		}

		/* Append num_interp_cols */
		result = lappend(result, makeInteger((long) num_interp));

		/* Third pass: append interpolate (attno, typoid) pairs */
		col_idx = 0;
		foreach(lc2, parse_targetlist)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(lc2);
			Node	   *expr;

			if (tle->resjunk)
			{
				col_idx++;
				continue;
			}
			expr = (Node *) tle->expr;
			if (IsA(expr, FuncExpr))
			{
				FuncExpr *fe = (FuncExpr *) expr;
				if (is_interpolate_function(fe->funcid))
				{
					result = lappend(result, makeInteger(col_idx));
					result = lappend(result, makeInteger((long) fe->funcresulttype));
				}
			}
			col_idx++;
		}
	}

	/*
	 * Append GROUP BY column info.
	 * Collect all non-bucket GROUP BY columns. After ensure_gapfill_sort_order()
	 * wraps the subpath with a Sort node, the physical sort order is always
	 * (group_keys..., bucket ASC), so all non-bucket columns are safe group
	 * keys regardless of their position in the SQL GROUP BY clause.
	 */
	{
		int		num_group_cols = 0;
		int		max_group_cols = list_length(root->parse->groupClause);
		int	   *group_attnos = palloc(sizeof(int) * Max(max_group_cols, 1));
		ListCell *gc;
		int		i;

		foreach(gc, root->parse->groupClause)
		{
			SortGroupClause *sgc = lfirst_node(SortGroupClause, gc);
			TargetEntry *tle = get_sortgroupclause_tle(sgc,
													   root->parse->targetList);
			int col_attno = tle->resno - 1;	/* 0-based */

			/* Skip the bucket column */
			if (col_attno == bucket_attno)
				continue;

			group_attnos[num_group_cols++] = col_attno;
		}

		result = lappend(result, makeInteger(num_group_cols));
		for (i = 0; i < num_group_cols; i++)
			result = lappend(result, makeInteger(group_attnos[i]));
		pfree(group_attnos);
	}

	return result;
}

/* ================================================================
 * Sort Order Correction for GapFill
 * ================================================================
 *
 * GapFill executor assumes the input is sorted as (group_keys..., bucket ASC).
 * If the user writes GROUP BY bucket, device_id (bucket first) without an
 * explicit ORDER BY, the planner may produce (bucket, device_id) sort order,
 * causing the executor to skip rows (data loss).
 *
 * verify_gapfill_sort_order() checks if the subpath already has the correct order.
 * ensure_gapfill_sort_order() wraps the subpath with a Sort node if needed.
 *
 * validate_distribution_key() detects when the aggregate path's locus
 * includes the bucket column in its hash distribution key. This means
 * same-group data was split across segments by hash(group_key, bucket),
 * making GapFill produce incorrect results (duplicate/missing gap rows).
 */

/*
 * Check if the aggregate path's distribution key contains the bucket
 * function. If so, same-group time buckets are scattered across segments
 * and GapFill cannot work correctly.
 */
static void
validate_distribution_key(Path *agg_path, FuncExpr *gapfill_fe)
{
	ListCell   *lc_dk;

	if (!CdbPathLocus_IsHashed(agg_path->locus))
		return;

	foreach(lc_dk, agg_path->locus.distkey)
	{
		DistributionKey *dk = (DistributionKey *) lfirst(lc_dk);
		ListCell   *lc_ec;

		foreach(lc_ec, dk->dk_eclasses)
		{
			EquivalenceClass *ec = (EquivalenceClass *) lfirst(lc_ec);
			ListCell   *lc_em;

			foreach(lc_em, ec->ec_members)
			{
				EquivalenceMember *em = (EquivalenceMember *) lfirst(lc_em);

				if (IsA(em->em_expr, FuncExpr) &&
					((FuncExpr *) em->em_expr)->funcid == gapfill_fe->funcid)
				{
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("cannot use time_bucket_gapfill when the "
									"table distribution key does not match "
									"the GROUP BY columns"),
							 errhint("Ensure the table is DISTRIBUTED BY "
									 "the non-time-bucket GROUP BY column(s) "
									 "(e.g., DISTRIBUTED BY (device_id)). "
									 "When the distribution key differs, "
									 "same-group data may be split across "
									 "segments, causing incorrect gap-fill "
									 "results.")));
				}
			}
		}
	}
}

/*
 * Single-pass validation: verify that subpath's pathkeys match the
 * group_pathkeys with the bucket function in the final position (ASC).
 * Classifies each pathkey as either "bucket" or "group key" in one scan.
 */
static bool
verify_gapfill_sort_order(PlannerInfo *root, Path *subpath, FuncExpr *gapfill_fe)
{
	int			num_gp = list_length(root->group_pathkeys);
	int			num_pk = list_length(subpath->pathkeys);
	int			bucket_pos = -1;
	ListCell   *lc;
	int			pos = 0;

	if (num_pk != num_gp || num_gp == 0)
		return false;

	foreach(lc, subpath->pathkeys)
	{
		PathKey	   *pk = (PathKey *) lfirst(lc);
		bool		is_bucket = false;
		ListCell   *emc;

		foreach(emc, pk->pk_eclass->ec_members)
		{
			EquivalenceMember *em = lfirst(emc);

			if (IsA(em->em_expr, FuncExpr) &&
				((FuncExpr *) em->em_expr)->funcid == gapfill_fe->funcid)
			{
				is_bucket = true;
				break;
			}
		}

		if (is_bucket)
		{
			if (pk->pk_strategy != BTLessStrategyNumber)
				return false;
			bucket_pos = pos;
		}
		else if (!list_member(root->group_pathkeys, pk))
			return false;

		pos++;
	}

	return (bucket_pos == num_pk - 1);
}

/*
 * If subpath doesn't have correct order, wrap it with a Sort node
 * that puts all non-bucket group pathkeys first, then bucket ASC last.
 */
static Path *
ensure_gapfill_sort_order(PlannerInfo *root, Path *subpath, FuncExpr *gapfill_fe)
{
	List	   *new_order = NIL;
	PathKey	   *pk_func = NULL;
	ListCell   *lc;

	if (verify_gapfill_sort_order(root, subpath, gapfill_fe))
		return subpath;  /* already correct */

	foreach(lc, root->group_pathkeys)
	{
		PathKey *pk = (PathKey *) lfirst(lc);
		bool	is_bucket = false;
		ListCell *emc;

		if (pk->pk_eclass->ec_members == NIL)
			continue;

		/* Scan all EC members to find the bucket function,
		 * matching the logic in verify_gapfill_sort_order(). */
		if (!pk_func)
		{
			foreach(emc, pk->pk_eclass->ec_members)
			{
				EquivalenceMember *em = lfirst(emc);

				if (IsA(em->em_expr, FuncExpr) &&
					((FuncExpr *) em->em_expr)->funcid == gapfill_fe->funcid)
				{
					is_bucket = true;
					break;
				}
			}
		}

		if (is_bucket)
		{
			/* This is the bucket pathkey; ensure it is ASC */
			if (BTLessStrategyNumber == pk->pk_strategy)
				pk_func = pk;
			else
				pk_func = make_canonical_pathkey(root,
												 pk->pk_eclass,
												 pk->pk_opfamily,
												 BTLessStrategyNumber,
												 pk->pk_nulls_first);
		}
		else
		{
			new_order = lappend(new_order, pk);
		}
	}

	if (!pk_func)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("no time_bucket_gapfill in GROUP BY")));

	new_order = lappend(new_order, pk_func);  /* bucket last */

	return (Path *) create_sort_path(root, subpath->parent,
									 subpath, new_order,
									 root->limit_tuples);
}

/* ================================================================
 * GapFill CustomPath Creation
 * ================================================================ */

static CustomPath *
make_gapfill_path(PlannerInfo *root, RelOptInfo *output_rel,
						   Path *agg_path, FuncExpr *gapfill_fe,
						   int bucket_attno, List *parse_targetlist)
{
	CustomPath *gpath;
	double		estimated_rows;

	gpath = makeNode(CustomPath);

	gpath->path.pathtype = T_CustomScan;
	gpath->path.parent = output_rel;
	gpath->path.pathtarget = output_rel->reltarget;
	gpath->path.param_info = NULL;
	gpath->path.parallel_aware = false;
	gpath->path.parallel_safe = false;
	gpath->path.parallel_workers = 0;

	gpath->flags = 0;

	/*
	 * Wrap agg_path in a Gather Motion to the coordinator (Entry locus)
	 * unless it already runs there.  Pre-Bug-#4 GapFill inherited the
	 * Aggregate's locus and ran on every segment; with explicit
	 * start/finish args it then generated the full bucket sequence even
	 * on empty segments, multiplying output rows by the segment count
	 * (e.g., WHERE device_id = 1 with 3 segments → 3 × bucket_count).
	 * Running the GapFill on the coordinator with all aggregated
	 * tuples gathered first yields the correct row count.  Aggregated
	 * tuples are small (one per group) so the Gather cost is bounded.
	 */
	if (!CdbPathLocus_IsEntry(agg_path->locus))
	{
		CdbPathLocus entry_locus;

		CdbPathLocus_MakeEntry(&entry_locus);
		agg_path = cdbpath_create_motion_path(root, agg_path,
											  agg_path->pathkeys,
											  false, entry_locus);
	}
	gpath->custom_paths = list_make1(agg_path);

	/* Store all gapfill parameters in custom_private */
	gpath->custom_private = serialize_gapfill_params(gapfill_fe, bucket_attno,
												   parse_targetlist, root);

	/*
	 * Estimate output rows from (finish - start) / bucket_width when
	 * available, falling back to agg_path->rows * 2.0 otherwise.
	 */
	estimated_rows = agg_path->rows * 2.0;
	if (list_length(gpath->custom_private) >= 10)
	{
		int64	bw_hi = (int64) intVal(list_nth(gpath->custom_private, 2));
		int64	bw_lo = (int64) (uint32) intVal(list_nth(gpath->custom_private, 3));
		int64	bucket_width = (int64) (((uint64) (uint32) bw_hi << 32) | (uint64) (uint32) bw_lo);
		int64	s_hi = (int64) intVal(list_nth(gpath->custom_private, 5));
		int64	s_lo = (int64) (uint32) intVal(list_nth(gpath->custom_private, 6));
		int64	start_usec = (int64) (((uint64) (uint32) s_hi << 32) | (uint64) (uint32) s_lo);
		int64	f_hi = (int64) intVal(list_nth(gpath->custom_private, 7));
		int64	f_lo = (int64) (uint32) intVal(list_nth(gpath->custom_private, 8));
		int64	finish_usec = (int64) (((uint64) (uint32) f_hi << 32) | (uint64) (uint32) f_lo);

		if (bucket_width > 0 &&
			start_usec != PG_INT64_MIN && finish_usec != PG_INT64_MIN &&
			finish_usec > start_usec)
		{
			estimated_rows = (double) (finish_usec - start_usec) / (double) bucket_width;
			if (estimated_rows < 1.0)
				estimated_rows = 1.0;
		}
	}

	gpath->path.rows = estimated_rows;
	gpath->path.startup_cost = agg_path->startup_cost;
	gpath->path.total_cost = agg_path->total_cost +
		estimated_rows * cpu_tuple_cost;

	gpath->methods = &ht_gapfill_path_methods;

	/* GapFill runs wherever its (now possibly motion-wrapped) child
	 * runs.  See the agg_path wrap-in-Motion comment above for why
	 * we force Entry for the multi-segment case. */
	gpath->path.locus = agg_path->locus;

	return gpath;
}

/* ================================================================
 * Helper: find matching expression in a target list
 * ================================================================
 *
 * Returns 0-based index if found, -1 if not found.
 * Also checks inside locf()/interpolate() wrappers in tlist entries.
 */
static int
search_targetlist_for_expr(Expr *expr, List *tlist)
{
	ListCell   *lc;
	int			idx = 0;

	foreach(lc, tlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);

		/* Direct match */
		if (equal(expr, tle->expr))
			return idx;

		/* Check if tlist entry wraps the expression in locf/interpolate */
		if (IsA(tle->expr, FuncExpr))
		{
			FuncExpr *fe = (FuncExpr *) tle->expr;

			if ((is_locf_function(fe->funcid) ||
				 is_interpolate_function(fe->funcid)) &&
				list_length(fe->args) >= 1 &&
				equal(expr, linitial(fe->args)))
				return idx;
		}

		idx++;
	}

	return -1;
}

/*
 * Try to find the expression's position in child_tlist.
 * First checks if the expression at parse_col in child_tlist matches
 * (handles duplicate columns like "id, id" in GROUP BY).
 * Falls back to search_targetlist_for_expr for reordered columns.
 * Returns the 0-based index in child_tlist, or -1 if not found.
 */
static int
resolve_child_column(Expr *expr, int parse_col, List *child_tlist)
{
	/* First: try same position (handles duplicate columns) */
	if (parse_col >= 0 && parse_col < list_length(child_tlist))
	{
		TargetEntry *child_tle = list_nth(child_tlist, parse_col);

		if (equal(expr, child_tle->expr))
			return parse_col;
	}

	/* Fall back to expression search (handles reordered columns) */
	return search_targetlist_for_expr(expr, child_tlist);
}

/*
 * Remap ALL column indices in custom_private from parse_targetlist
 * positions to child plan targetlist positions.
 *
 * serialize_gapfill_params() serializes column indices based on
 * parse_targetlist positions. But the executor uses custom_scan_tlist
 * (= child plan tlist) which may have different column ordering and
 * fewer columns (PG optimizer strips locf/interpolate wrappers and
 * may reorder columns).
 *
 * This function remaps:
 *   [0] bucket_attno
 *   [11..] LOCF column indices (unwrapping locf() wrappers)
 *   [after LOCF] Interpolate column indices (unwrapping interpolate())
 *   [end] GROUP BY column indices
 *
 * For duplicate columns (e.g., GROUP BY id, id), the remapper first
 * checks the same position in child_tlist before falling back to
 * expression search, avoiding incorrect deduplication.
 *
 * Note: when both locf() and interpolate() reference the same aggregate,
 * they remap to the same child column. The executor processes LOCF first,
 * then Interpolate, so Interpolate's kind overwrites LOCF for that column.
 * This is a known limitation — use different aggregates for different fills.
 */
static void
adjust_column_indices(List *privdata, List *parse_tl, List *child_tlist)
{
	int			num_locf;
	int			base_idx;
	int			i;

	if (list_length(privdata) < 12)
		return;

	/* Remap bucket_attno [0] */
	{
		int		parse_col = intVal(list_nth(privdata, 0));

		if (parse_col >= 0 && parse_col < list_length(parse_tl))
		{
			TargetEntry *tle = list_nth(parse_tl, parse_col);
			int		child_col = resolve_child_column(tle->expr, parse_col,
													child_tlist);

			if (child_col >= 0)
				((Value *) list_nth(privdata, 0))->val.ival = child_col;
			/* else: column computed in Result node above, no remap needed */
		}
	}

	num_locf = intVal(list_nth(privdata, 4));
	base_idx = 11;

	/* Remap LOCF columns */
	for (i = 0; i < num_locf; i++)
	{
		int		parse_col = intVal(list_nth(privdata, base_idx + i * 2));

		if (parse_col >= 0 && parse_col < list_length(parse_tl))
		{
			TargetEntry *tle = list_nth(parse_tl, parse_col);
			Expr	   *inner = tle->expr;

			/* Unwrap locf() to get the inner expression (e.g., avg(value)) */
			if (IsA(inner, FuncExpr) &&
				is_locf_function(((FuncExpr *) inner)->funcid) &&
				list_length(((FuncExpr *) inner)->args) >= 1)
				inner = (Expr *) linitial(((FuncExpr *) inner)->args);

			{
				int		child_col = resolve_child_column(inner, parse_col,
														child_tlist);

				if (child_col >= 0)
					((Value *) list_nth(privdata, base_idx + i * 2))->val.ival = child_col;
				/* else: column computed in Result node above, no remap needed */
			}
		}
	}

	base_idx = 11 + num_locf * 2;

	if (base_idx >= list_length(privdata))
		return;

	{
		int		num_interp = intVal(list_nth(privdata, base_idx));

		base_idx++;

		/* Remap Interpolate columns */
		for (i = 0; i < num_interp; i++)
		{
			int		parse_col = intVal(list_nth(privdata, base_idx + i * 2));

			if (parse_col >= 0 && parse_col < list_length(parse_tl))
			{
				TargetEntry *tle = list_nth(parse_tl, parse_col);
				Expr	   *inner = tle->expr;

				/* Unwrap interpolate() */
				if (IsA(inner, FuncExpr) &&
					is_interpolate_function(((FuncExpr *) inner)->funcid) &&
					list_length(((FuncExpr *) inner)->args) >= 1)
					inner = (Expr *) linitial(((FuncExpr *) inner)->args);

				{
					int		child_col = resolve_child_column(inner, parse_col,
															child_tlist);

					if (child_col >= 0)
						((Value *) list_nth(privdata, base_idx + i * 2))->val.ival = child_col;
					/* else: column computed in Result node above, no remap needed */
				}
			}
		}

		base_idx += num_interp * 2;
	}

	/* Remap GROUP BY column indices */
	if (base_idx < list_length(privdata))
	{
		int		num_group = intVal(list_nth(privdata, base_idx));

		base_idx++;

		for (i = 0; i < num_group; i++)
		{
			int		parse_col = intVal(list_nth(privdata, base_idx + i));

			if (parse_col >= 0 && parse_col < list_length(parse_tl))
			{
				TargetEntry *tle = list_nth(parse_tl, parse_col);
				int		child_col = resolve_child_column(tle->expr, parse_col,
														child_tlist);

				if (child_col >= 0)
					((Value *) list_nth(privdata, base_idx + i))->val.ival = child_col;
				/* else: column computed in Result node above, no remap needed */
			}
		}
	}
}

/* ================================================================
 * PlanCustomPath Callback
 * ================================================================ */

static Plan *
ht_gapfill_plan_create(PlannerInfo *root, RelOptInfo *rel,
					   CustomPath *best_path, List *tlist,
					   List *clauses, List *custom_plans)
{
	CustomScan *cscan = makeNode(CustomScan);

	cscan->flags = best_path->flags;
	cscan->methods = &ht_gapfill_scan_methods;
	cscan->scan.scanrelid = 0;	/* not a base rel scan */
	cscan->scan.plan.targetlist = tlist;
	cscan->custom_private = copyObject(best_path->custom_private);

	/* Child plans (the Aggregate node) */
	cscan->custom_plans = custom_plans;

	/* Use the child plan's targetlist as custom_scan_tlist.
	 * This forces PG to add a Result (projection) node above GapFill
	 * whenever the plan tlist contains expressions not in the child's
	 * output (e.g., COALESCE, CASE). Without this, gap rows would have
	 * NULL for those expressions instead of the evaluated result.
	 *
	 * When child tlist and plan tlist are identical (no wrappers),
	 * PG skips the projection, so there's no overhead in that case. */
	if (custom_plans != NIL)
		cscan->custom_scan_tlist = ((Plan *) linitial(custom_plans))->targetlist;
	else
		cscan->custom_scan_tlist = tlist;

	/*
	 * Remap ALL column indices in custom_private from parse_targetlist
	 * positions to child plan tlist positions. This is needed because:
	 *   - serialize_gapfill_params() serializes indices from parse_targetlist
	 *   - The executor uses custom_scan_tlist (child tlist) for col_states
	 *   - PG optimizer may reorder columns and strip locf/interpolate
	 *     wrappers to upper nodes (Result or WindowAgg), so parse_targetlist
	 *     positions don't correspond to child tlist positions.
	 */
	if (custom_plans != NIL)
	{
		List *child_tlist = ((Plan *) linitial(custom_plans))->targetlist;

		adjust_column_indices(cscan->custom_private,
							  root->parse->targetList,
							  child_tlist);
	}

	/* No quals at the GapFill level */
	cscan->scan.plan.qual = NIL;

	return &cscan->scan.plan;
}

/* ================================================================
 * Main create_upper_paths_hook
 * ================================================================ */

static void
ht_gapfill_create_upper_paths(PlannerInfo *root,
							  UpperRelationKind stage,
							  RelOptInfo *input_rel,
							  RelOptInfo *output_rel,
							  void *extra)
{
	FuncExpr   *gapfill_fe;
	int			bucket_attno;
	ListCell   *lc;

	/* Call previous hook first */
	if (prev_create_upper_paths_hook)
		prev_create_upper_paths_hook(root, stage, input_rel, output_rel, extra);

	/*
	 * Bail out if the time_series extension is not currently installed
	 * (for example after DROP EXTENSION CASCADE — the .so stays
	 * resident but our catalog tables are gone).  Same guard pattern
	 * as cagg_process_utility; see time_series.c for the state
	 * machine.
	 */
	if (!extension_is_loaded_and_not_upgrading())
		return;

	/* Only interested in the GROUP_AGG stage */
	if (stage != UPPERREL_GROUP_AGG)
		return;

	/* Check if the query's parse target list contains a gapfill call */
	gapfill_fe = locate_gapfill_funcexpr(root->parse->targetList, &bucket_attno);
	if (!gapfill_fe)
		return;

	/*
	 * Replace all existing paths with GapFill-wrapped versions.
	 * We must replace (not add) because add_path would discard the
	 * higher-cost GapFill path in favor of the raw Aggregate path.
	 * When gapfill is requested, only GapFill paths are correct.
	 *
	 * NOTE: This discards paths from other create_upper_paths_hook
	 * handlers that ran before us. If another extension also hooks
	 * UPPERREL_GROUP_AGG, load time_series after it in
	 * shared_preload_libraries so this hook runs last.
	 */
	{
		List	   *existing_paths = list_copy(output_rel->pathlist);

		output_rel->pathlist = NIL;

		/*
		 * GapFill does not support parallel execution, so discard any
		 * parallel aggregate paths to prevent the planner from choosing
		 * a raw parallel path that bypasses GapFill.
		 */
		output_rel->partial_pathlist = NIL;

		foreach(lc, existing_paths)
		{
			Path	   *agg_path = (Path *) lfirst(lc);
			CustomPath *gpath;

			/* Reject if distribution key includes bucket (data split) */
			validate_distribution_key(agg_path, gapfill_fe);

			/* Ensure correct sort order: (group_keys..., bucket ASC) */
			agg_path = ensure_gapfill_sort_order(root, agg_path, gapfill_fe);

			gpath = make_gapfill_path(root, output_rel,
											   agg_path, gapfill_fe,
											   bucket_attno,
											   root->parse->targetList);
			add_path(output_rel, &gpath->path, root);
		}

		list_free(existing_paths);
	}
}

/* ================================================================
 * ORCA Auto-Fallback: detect gapfill and disable ORCA
 * ================================================================
 *
 * ORCA cannot handle Custom Scan (GapFill) nodes. When the query
 * contains time_bucket_gapfill(), we temporarily disable the ORCA
 * optimizer so the PG planner handles it instead.
 */

typedef struct
{
	bool found;
} GapfillDetectContext;

static bool
gapfill_detect_walker(Node *node, GapfillDetectContext *ctx)
{
	if (node == NULL)
		return false;
	if (ctx->found)
		return true;	/* short-circuit */
	if (IsA(node, FuncExpr))
	{
		FuncExpr *fe = (FuncExpr *) node;
		if (is_gapfill_function(fe->funcid))
		{
			ctx->found = true;
			return true;
		}
	}
	if (IsA(node, Query))
		return query_tree_walker((Query *) node,
								 gapfill_detect_walker, ctx, 0);
	return expression_tree_walker(node, gapfill_detect_walker, ctx);
}

static bool
query_contains_gapfill(Query *parse)
{
	GapfillDetectContext ctx = {false};

	if (!extension_is_loaded_and_not_upgrading())
		return false;
	query_tree_walker(parse, gapfill_detect_walker, &ctx, 0);
	return ctx.found;
}

/*
 * Planner hook: when ORCA is enabled and the query contains
 * time_bucket_gapfill(), temporarily disable ORCA so the PG
 * planner handles the Custom Scan path injection.
 *
 * We modify the optimizer GUC via set_config_option() to go
 * through the official GUC infrastructure (reporting, callbacks).
 * PG_FINALLY restores the original value on both success and error.
 */
static PlannedStmt *
ht_gapfill_planner(Query *parse,
					const char *query_string,
					int cursorOptions,
					ParamListInfo boundParams,
					OptimizerOptions *optimizer_options)
{
	PlannedStmt *result;
	bool		 need_restore = false;

	if (optimizer && query_contains_gapfill(parse))
	{
		set_config_option("optimizer", "off",
						  PGC_USERSET, PGC_S_SESSION,
						  GUC_ACTION_SET, true, 0, false);
		need_restore = true;
	}

	PG_TRY();
	{
		if (prev_planner_hook)
			result = prev_planner_hook(parse, query_string,
									   cursorOptions, boundParams,
									   optimizer_options);
		else
			result = standard_planner(parse, query_string,
									  cursorOptions, boundParams,
									  optimizer_options);
	}
	PG_FINALLY();
	{
		if (need_restore)
			set_config_option("optimizer", "on",
							  PGC_USERSET, PGC_S_SESSION,
							  GUC_ACTION_SET, true, 0, false);
	}
	PG_END_TRY();

	return result;
}

/* ================================================================
 * Hook Registration
 * ================================================================ */

void
ht_gapfill_planner_init(void)
{
	/* planner_hook: intercept before ORCA to disable it for gapfill */
	prev_planner_hook = planner_hook;
	planner_hook = ht_gapfill_planner;

	/* create_upper_paths_hook: inject GapFill CustomPath */
	prev_create_upper_paths_hook = create_upper_paths_hook;
	create_upper_paths_hook = ht_gapfill_create_upper_paths;
}
