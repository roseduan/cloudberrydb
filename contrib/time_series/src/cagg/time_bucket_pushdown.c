/*-------------------------------------------------------------------------
 *
 * time_bucket_pushdown.c
 *    Planner-time predicate pushdown for time_bucket() bound quals.
 *
 *    Problem: a CAGG view's live branch ends up scanning a hypertable
 *    through a chain of subqueries.  After our cagg_watermark const-fold
 *    pass, the outer qual looks like
 *
 *       Var(direct_view.bucket) {>=,>,<=,<} <const>
 *
 *    where `direct_view.bucket` is the alias of `time_bucket(B, cpu.time)`
 *    in a subquery.  Once the planner pushes this filter through the
 *    subquery, the final ChunkScan carries
 *
 *       time_bucket(B, cpu.time) {>=,>,<=,<} <const>
 *
 *    which the chunk pruner cannot use -- time_bucket() is opaque to the
 *    bare-column index/range analysis.
 *
 *    Fix: by monotonicity of time_bucket
 *       (1) bucket_start <= t   (floor property)
 *       (2) t < bucket_start + B   (strict, t in [b_start, b_start+B))
 *    we synthesize the equivalent bare-Var predicates and append them to
 *    the SUBQUERY's WHERE clause.  PG then pushes the bare-Var predicate
 *    down to ChunkScan, enabling chunk pruning.
 *
 *    Lower-bound synthesis (uses property 1):
 *       time_bucket(B, t) >= C   =>  t >= C
 *       time_bucket(B, t) >  C   =>  t >  C
 *
 *    Upper-bound synthesis (uses property 2):
 *       time_bucket(B, t) <= C   =>  t < C + B  (strict, even outer <=)
 *       time_bucket(B, t) <  C   =>  t < C + B
 *
 *    Tighter form when value lies on a bucket boundary AND outer is
 *    strict `<` AND time_bucket has no offset/origin shift:
 *       time_bucket(B, t) < (k*B)    =>   t < (k*B)     (no need to add B)
 *
 *    Safety: the synthesized predicate is strictly implied by the original
 *    (rows that pass the original outer qual are guaranteed to pass the
 *    inner synthesized qual).  Some rows pass the inner qual but fail the
 *    outer; those are correctly filtered by the outer qual which is kept
 *    intact.  Final result is identical to before optimization.
 *
 *    Commutator support: matches both `Var op Const` and `Const op Var`
 *    forms via get_commutator() -- e.g. user writing `'2024' <= bucket`
 *    gets the same optimization as `bucket >= '2024'`.
 *
 *    Overflow safety: for upper-bound synthesis we explicitly check that
 *    `C + B` won't overflow the value type, otherwise we silently skip
 *    optimization (planner would otherwise crash on const-fold).
 *
 * Copyright (c) 2026 HashData Inc.
 * Licensed under Apache License 2.0
 *
 * IDENTIFICATION
 *    contrib/time_series/src/cagg/time_bucket_pushdown.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <limits.h>
#include <math.h>
#include <string.h>

#include "access/stratnum.h"
#include "catalog/namespace.h"
#include "catalog/pg_operator.h"
#include "datatype/timestamp.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "parser/parse_oper.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "utils/typcache.h"
#include "cdb/cdbvars.h"

#include "../include/time_series.h"
#include "../include/cagg/cagg.h"

/*
 * Carries everything we know about an outer comparison qual we attempt
 * to push down.  Avoids passing 6 parameters around.
 */
typedef struct
{
	OpExpr	   *op;				/* original outer OpExpr (for opcollid etc.) */
	Var		   *var;			/* the Var side (may be op->args[0] or [1]) */
	Const	   *cst;			/* the Const side */
	Oid			effective_opno; /* op->opno or its commutator */
	int			btree_strategy; /* BTGreater/Less{Equal,}StrategyNumber */
}			ParsedQual;

typedef enum
{
	BOUND_NONE,
	BOUND_LOWER,				/* >= or > */
	BOUND_UPPER					/* <= or < */
}			BoundKind;

typedef struct
{
	int			added;
}			TbPushdownCtx;


/* Forward declaration -- used by push_through_var_chain. */
static bool push_through_var_chain(Query *subq, AttrNumber attno,
								   ParsedQual * pq, TbPushdownCtx * ctx);

/* Derive bound direction from a cached btree strategy number. */
static inline BoundKind
bound_from_strategy(int strat)
{
	if (strat == BTGreaterStrategyNumber ||
		strat == BTGreaterEqualStrategyNumber)
		return BOUND_LOWER;
	if (strat == BTLessStrategyNumber || strat == BTLessEqualStrategyNumber)
		return BOUND_UPPER;
	return BOUND_NONE;
}


/* Pattern recognition helpers */

/* Is `fe` a call to a function named "time_bucket"? */
static bool
is_time_bucket_funcexpr(FuncExpr *fe)
{
	char	   *name;
	bool		match;

	name = get_func_name(fe->funcid);
	if (name == NULL)
		return false;
	match = (strcmp(name, "time_bucket") == 0);
	pfree(name);
	return match;
}

/*
 * Classify a comparison operator into bound direction via btree strategy.
 * Uses the type's btree opfamily, which is the robust way (handles
 * user-defined types, operator aliases, etc.).
 *
 * out_strategy returns the raw BT*StrategyNumber for callers that need
 * to distinguish strict (<) from non-strict (<=).
 */
static BoundKind
classify_bound(Oid opno, Oid arg_type, int *out_strategy)
{
	TypeCacheEntry *tce;
	int			strategy;

	tce = lookup_type_cache(arg_type, TYPECACHE_BTREE_OPFAMILY);
	if (tce->btree_opf == InvalidOid)
		return BOUND_NONE;

	strategy = get_op_opfamily_strategy(opno, tce->btree_opf);
	if (out_strategy != NULL)
		*out_strategy = strategy;

	if (strategy == BTGreaterStrategyNumber ||
		strategy == BTGreaterEqualStrategyNumber)
		return BOUND_LOWER;
	if (strategy == BTLessStrategyNumber ||
		strategy == BTLessEqualStrategyNumber)
		return BOUND_UPPER;
	return BOUND_NONE;
}

/*
 * Parse an outer OpExpr qual: match `Var op Const` or `Const op Var`.
 * In the latter case, look up the commutator and treat as `Var op' Const`.
 * Fill in *pq.  Returns true on success.
 */
static bool
parse_outer_qual(OpExpr *op, ParsedQual * pq)
{
	Node	   *arg0;
	Node	   *arg1;

	if (list_length(op->args) != 2)
		return false;
	arg0 = (Node *) linitial(op->args);
	arg1 = (Node *) lsecond(op->args);

	if (IsA(arg0, Var) && IsA(arg1, Const))
	{
		pq->op = op;
		pq->var = (Var *) arg0;
		pq->cst = (Const *) arg1;
		pq->effective_opno = op->opno;
	}
	else if (IsA(arg0, Const) && IsA(arg1, Var))
	{
		/*
		 * Commutator form: `Const op Var`.  E.g. user writes WHERE
		 * '2024-01-01'::timestamptz <= bucket which is equivalent to WHERE
		 * bucket >= '2024-01-01'::timestamptz after swapping arg order and
		 * replacing op with its commutator.
		 */
		Oid			comm = get_commutator(op->opno);

		if (!OidIsValid(comm))
			return false;
		pq->op = op;
		pq->var = (Var *) arg1;
		pq->cst = (Const *) arg0;
		pq->effective_opno = comm;
	}
	else
	{
		return false;
	}

	if (pq->cst->constisnull)
		return false;

	return true;
}


/* Operator lookup helpers */

/*
 * Look up the `+` operator opno for (left_type + right_type).
 * Returns InvalidOid if not found.
 */
static Oid
get_plus_opno(Oid left_type, Oid right_type)
{
	return OpernameGetOprid(list_make1(makeString("+")),
							left_type, right_type);
}

/*
 * Look up the strict `<` operator opno for (left_type < right_type).
 * Returns InvalidOid if not found.
 */
static Oid
get_lt_opno(Oid left_type, Oid right_type)
{
	return OpernameGetOprid(list_make1(makeString("<")),
							left_type, right_type);
}


/* Overflow safety + boundary detection */

/*
 * For supported time types, check whether adding `width` to `value`
 * would overflow the type's representable range.  Returns true if safe
 * (the addition won't overflow); false if we should bail out and skip
 * the optimization.
 *
 * Supported types:
 *   - INT2/INT4/INT8 : width must also be the same integer type
 *   - DATE           : width must be INTERVAL with no month component
 *   - TIMESTAMP / TIMESTAMPTZ : width must be INTERVAL with no month
 *
 * Other type combinations: we conservatively return false (skip).
 */
static bool
upper_addition_is_safe(Const *value, Const *width)
{
	Oid			value_type = value->consttype;
	Oid			width_type = width->consttype;

	/* Integer + integer */
	if ((value_type == INT2OID || value_type == INT4OID ||
		 value_type == INT8OID) &&
		value_type == width_type)
	{
		int64		v,
					w,
					type_max;

		if (value_type == INT2OID)
		{
			v = (int64) DatumGetInt16(value->constvalue);
			w = (int64) DatumGetInt16(width->constvalue);
			type_max = INT16_MAX;
		}
		else if (value_type == INT4OID)
		{
			v = (int64) DatumGetInt32(value->constvalue);
			w = (int64) DatumGetInt32(width->constvalue);
			type_max = INT32_MAX;
		}
		else
		{
			v = DatumGetInt64(value->constvalue);
			w = DatumGetInt64(width->constvalue);
			type_max = INT64_MAX;
		}
		if (w <= 0)
			return false;		/* nonsensical width */
		if (v > type_max - w)
			return false;		/* would overflow */
		return true;
	}

	/* INTERVAL width -- applies to DATE / TIMESTAMP / TIMESTAMPTZ */
	if (width_type == INTERVALOID)
	{
		Interval   *iv = DatumGetIntervalP(width->constvalue);

		/*
		 * Month component has variable real-world length (28-31 days). Our
		 * monotonicity derivation (t < bucket_start + B) still holds if PG's
		 * `+` operator handles month addition correctly (it does --
		 * calendar-aware), so we could keep handling it.  But matching TSDB
		 * we skip month-component intervals to avoid subtle boundary
		 * mismatches in edge cases.
		 */
		if (iv->month != 0)
			return false;

		/* Convert width to microseconds (or days for DATE). */
		if (value_type == TIMESTAMPOID || value_type == TIMESTAMPTZOID)
		{
			int64		v = DatumGetTimestamp(value->constvalue);
			int64		w_usec;

			/* day component fits in int64 microseconds via multiplication */
			if (iv->day >= INT64_MAX / USECS_PER_DAY)
				return false;
			w_usec = iv->time + (int64) iv->day * USECS_PER_DAY;
			if (w_usec <= 0)
				return false;

			/*
			 * PG's timestamp range is [DT_NOBEGIN+1, DT_NOEND-1] in usec. Use
			 * DT_NOEND - 1 as practical max.
			 */
			if (v > (DT_NOEND - 1) - w_usec)
				return false;
			return true;
		}
		else if (value_type == DATEOID)
		{
			int32		v = DatumGetDateADT(value->constvalue);
			int64		w_days;

			/*
			 * Width in days = day component + ceil(time/USECS_PER_DAY).
			 * (Equivalent to TSDB's formula.)
			 */
			w_days = iv->day + (iv->time / USECS_PER_DAY)
				+ ((iv->time % USECS_PER_DAY) != 0 ? 1 : 0);
			if (w_days <= 0)
				return false;
			/* PG date is int32 days; DATEVAL_NOEND used as positive sentinel */
			if ((int64) v > (int64) (DATEVAL_NOEND - 1) - w_days)
				return false;
			return true;
		}
	}

	/* Unsupported type combo -- bail */
	return false;
}

/*
 * Check whether `value` lies exactly on a bucket boundary AND the
 * time_bucket call has no offset/origin/timezone shift.  When both
 * conditions hold AND the outer operator is STRICT `<` (not `<=`),
 * we can synthesize `t < value` directly instead of `t < value + B`,
 * giving a tighter chunk-pruning bound.
 *
 * Implemented for:
 *   - INT2/INT4/INT8: `value % width == 0`
 *   - DATE: `value (days) % width (days)`, width must be whole days
 *     (interval->month == 0, interval->time % USECS_PER_DAY == 0)
 *   - TIMESTAMP / TIMESTAMPTZ: `value (usec) % width (usec)`,
 *     width must have interval->month == 0
 *
 * Boundary alignment is measured from PG's epoch (2000-01-01 for DATE
 * and TIMESTAMP; 0 for integer columns).  If user passes a non-default
 * origin via 3-arg time_bucket, bucket boundaries shift and the % check
 * is no longer valid -- hence the strict `list_length(args) == 2` guard.
 */
static bool
value_on_bucket_boundary(Const *value, Const *width, FuncExpr *time_bucket)
{
	Oid			value_type;
	Oid			width_type;

	/*
	 * Optimization only applies when time_bucket has no extra args (no offset
	 * / origin / timezone).  Default origin is PG epoch (2000-01-01 for
	 * date/timestamp, 0 for integer), which is what our `value % width == 0`
	 * check assumes.  3+ arg variants can shift boundaries, so we skip.
	 */
	if (list_length(time_bucket->args) != 2)
		return false;

	value_type = value->consttype;
	width_type = width->consttype;

	/* Integer types: direct % check */
	if (value_type == INT2OID || value_type == INT4OID || value_type == INT8OID)
	{
		int64		v,
					w;

		if (value_type != width_type)
			return false;		/* mixed-int unsupported */
		if (value_type == INT2OID)
		{
			v = (int64) DatumGetInt16(value->constvalue);
			w = (int64) DatumGetInt16(width->constvalue);
		}
		else if (value_type == INT4OID)
		{
			v = (int64) DatumGetInt32(value->constvalue);
			w = (int64) DatumGetInt32(width->constvalue);
		}
		else
		{
			v = DatumGetInt64(value->constvalue);
			w = DatumGetInt64(width->constvalue);
		}
		if (w == 0)
			return false;
		return (v % w) == 0;
	}

	/* DATE: width must be INTERVAL with month=0 and whole-day time. */
	if (value_type == DATEOID && width_type == INTERVALOID)
	{
		Interval   *iv = DatumGetIntervalP(width->constvalue);
		int64		w_days;
		int64		v_days;

		if (iv->month != 0)
			return false;
		if (iv->time != 0 && (iv->time % USECS_PER_DAY) != 0)
			return false;		/* sub-day interval, doesn't align with date */
		w_days = (int64) iv->day + (iv->time / USECS_PER_DAY);
		if (w_days <= 0)
			return false;
		v_days = (int64) DatumGetDateADT(value->constvalue);
		return (v_days % w_days) == 0;
	}

	/* TIMESTAMP / TIMESTAMPTZ: width must be INTERVAL with month=0 */
	if ((value_type == TIMESTAMPOID || value_type == TIMESTAMPTZOID) &&
		width_type == INTERVALOID)
	{
		Interval   *iv = DatumGetIntervalP(width->constvalue);
		int64		w_usec;
		int64		v_usec;

		if (iv->month != 0)
			return false;
		if (iv->day < 0 || iv->day >= INT64_MAX / USECS_PER_DAY)
			return false;		/* overflow guard */
		w_usec = iv->time + (int64) iv->day * USECS_PER_DAY;
		if (w_usec <= 0)
			return false;
		v_usec = DatumGetTimestamp(value->constvalue);
		return (v_usec % w_usec) == 0;
	}

	return false;
}


/* Expression builders */

/*
 * Build OpExpr `outer_const + bucket_width` (RHS of the upper-bound
 * comparison).  Caller is responsible for overflow safety check;
 * we assume the addition is safe at this point.
 *
 * Returns NULL if no `+` operator exists for the type pair.
 */
static Node *
build_plus_expr(Const *outer_const, Const *bucket_width, Oid *out_rhs_type)
{
	Oid			const_type = outer_const->consttype;
	Oid			width_type = bucket_width->consttype;
	Oid			plus_opno;
	Oid			plus_func;
	Oid			result_type;
	OpExpr	   *plus_op;
	HeapTuple	op_tup;
	Form_pg_operator op_form;

	plus_opno = get_plus_opno(const_type, width_type);
	if (!OidIsValid(plus_opno))
		return NULL;

	op_tup = SearchSysCache1(OPEROID, ObjectIdGetDatum(plus_opno));
	if (!HeapTupleIsValid(op_tup))
		return NULL;
	op_form = (Form_pg_operator) GETSTRUCT(op_tup);
	plus_func = op_form->oprcode;
	result_type = op_form->oprresult;
	ReleaseSysCache(op_tup);

	plus_op = makeNode(OpExpr);
	plus_op->opno = plus_opno;
	plus_op->opfuncid = plus_func;
	plus_op->opresulttype = result_type;
	plus_op->opretset = false;
	plus_op->opcollid = InvalidOid;
	plus_op->inputcollid = InvalidOid;
	plus_op->args = list_make2(copyObject(outer_const),
							   copyObject(bucket_width));
	plus_op->location = -1;

	*out_rhs_type = result_type;
	return (Node *) plus_op;
}

/*
 * Build the comparison OpExpr `lhs_var < rhs_expr` for the upper-bound
 * case.  rhs_expr can be an OpExpr (planner const-folds) or a plain
 * Const (when we have the tightness optimization).
 *
 * Returns NULL if no `<` operator exists for (var_type, rhs_type).
 */
static OpExpr *
build_lt_compare(Var *lhs_var, Node *rhs_expr, Oid rhs_type)
{
	Oid			lt_opno;
	Oid			lt_func;
	Oid			cmp_result_type;
	OpExpr	   *cmp;
	HeapTuple	op_tup;
	Form_pg_operator op_form;

	lt_opno = get_lt_opno(lhs_var->vartype, rhs_type);
	if (!OidIsValid(lt_opno))
		return NULL;

	op_tup = SearchSysCache1(OPEROID, ObjectIdGetDatum(lt_opno));
	if (!HeapTupleIsValid(op_tup))
		return NULL;
	op_form = (Form_pg_operator) GETSTRUCT(op_tup);
	lt_func = op_form->oprcode;
	cmp_result_type = op_form->oprresult;
	ReleaseSysCache(op_tup);

	cmp = makeNode(OpExpr);
	cmp->opno = lt_opno;
	cmp->opfuncid = lt_func;
	cmp->opresulttype = cmp_result_type;
	cmp->opretset = false;
	cmp->opcollid = InvalidOid;
	cmp->inputcollid = InvalidOid;
	cmp->args = list_make2(copyObject(lhs_var), rhs_expr);
	cmp->location = -1;
	return cmp;
}


/* Subquery target injection */

/*
 * Append the synthesized OpExpr to subq->jointree->quals, growing into
 * an AND_EXPR if needed.
 */
static void
append_qual_to_subquery(Query *subq, OpExpr *new_op)
{
	if (subq->jointree == NULL)
		return;					/* defensive -- shouldn't happen */

	if (subq->jointree->quals == NULL)
	{
		subq->jointree->quals = (Node *) new_op;
	}
	else if (IsA(subq->jointree->quals, BoolExpr) &&
			 ((BoolExpr *) subq->jointree->quals)->boolop == AND_EXPR)
	{
		BoolExpr   *b = (BoolExpr *) subq->jointree->quals;

		b->args = lappend(b->args, new_op);
	}
	else
	{
		BoolExpr   *b = makeNode(BoolExpr);

		b->boolop = AND_EXPR;
		b->args = list_make2(subq->jointree->quals, new_op);
		b->location = -1;
		subq->jointree->quals = (Node *) b;
	}
}

/*
 * Collect every leaf RTE_SUBQUERY reachable from a SetOperationStmt tree.
 */
static void
collect_setop_leaves(Node *setop_node, Query *wrapping_subq,
					 List **out_branches)
{
	if (setop_node == NULL)
		return;

	if (IsA(setop_node, RangeTblRef))
	{
		RangeTblRef *ref = (RangeTblRef *) setop_node;
		RangeTblEntry *leaf_rte;

		if (ref->rtindex <= 0 ||
			ref->rtindex > list_length(wrapping_subq->rtable))
			return;
		leaf_rte = (RangeTblEntry *) list_nth(wrapping_subq->rtable,
											  ref->rtindex - 1);
		if (leaf_rte->rtekind == RTE_SUBQUERY && leaf_rte->subquery != NULL)
			*out_branches = lappend(*out_branches, leaf_rte->subquery);
	}
	else if (IsA(setop_node, SetOperationStmt))
	{
		SetOperationStmt *s = (SetOperationStmt *) setop_node;

		collect_setop_leaves(s->larg, wrapping_subq, out_branches);
		collect_setop_leaves(s->rarg, wrapping_subq, out_branches);
	}
}


/* Core pushdown logic */

/*
 * Attempt to push the synthesized predicate into the given subquery,
 * which is known to have a `time_bucket(Const, Var)` FuncExpr at
 * tlist[attno-1].  Returns true on success.
 */
static bool
push_into_subquery_leaf(Query *subq, AttrNumber attno,
						ParsedQual * pq, TbPushdownCtx * ctx)
{
	BoundKind	bound = bound_from_strategy(pq->btree_strategy);
	TargetEntry *te;
	FuncExpr   *fe;
	Node	   *bucket_width_node;
	Const	   *bucket_width;
	Node	   *time_arg;
	Var		   *inner_var;
	OpExpr	   *new_op;

	if (subq == NULL)
		return false;
	if (attno <= 0 || attno > list_length(subq->targetList))
		return false;
	te = (TargetEntry *) list_nth(subq->targetList, attno - 1);
	if (te == NULL || te->expr == NULL || !IsA(te->expr, FuncExpr))
		return false;
	fe = (FuncExpr *) te->expr;
	if (!is_time_bucket_funcexpr(fe))
		return false;

	/*
	 * time_bucket signature: at least 2 args (width, ts), optionally 3
	 * (origin / offset / timezone) or 5 (timezone + origin + offset).
	 * Multi-arg variants must have all extras as Const; the bucketing math we
	 * rely on still holds because additional args only shift the bucket
	 * boundary, never change bucket width.
	 */
	if (list_length(fe->args) != 2 && list_length(fe->args) != 3 &&
		list_length(fe->args) != 5)
		return false;
	if (list_length(fe->args) > 2 &&
		!IsA(lthird(fe->args), Const))
		return false;
	if (list_length(fe->args) == 5 &&
		(!IsA(lfourth(fe->args), Const) ||
		 !IsA(list_nth(fe->args, 4), Const)))
		return false;

	bucket_width_node = (Node *) linitial(fe->args);
	time_arg = (Node *) lsecond(fe->args);
	if (!IsA(bucket_width_node, Const) || !IsA(time_arg, Var))
		return false;
	bucket_width = (Const *) bucket_width_node;
	if (bucket_width->constisnull)
		return false;
	inner_var = (Var *) time_arg;

	if (bound == BOUND_LOWER)
	{
		/*
		 * Lower-bound synthesis: t {>=,>} C
		 *
		 * Reuse the (possibly commutator-resolved) effective_opno.  The
		 * inner_var has the same base type as time_bucket(B, V), and the
		 * comparison opno that worked for `time_bucket(...) op Const` also
		 * works for `inner_var op Const`.
		 */
		new_op = makeNode(OpExpr);
		new_op->opno = pq->effective_opno;
		new_op->opfuncid = InvalidOid;	/* will be looked up at planning */
		new_op->opresulttype = BOOLOID;
		new_op->opretset = false;
		new_op->opcollid = pq->op->opcollid;
		new_op->inputcollid = pq->op->inputcollid;
		new_op->args = list_make2(copyObject(inner_var),
								  copyObject(pq->cst));
		new_op->location = pq->op->location;
	}
	else						/* BOUND_UPPER */
	{
		Node	   *rhs_expr;
		Oid			rhs_type;

		/*
		 * Tightness optimization: if outer is STRICT `<` AND value is on a
		 * bucket boundary AND time_bucket has no offset/origin shift, we can
		 * use `t < value` directly (no `+ B`).
		 */
		bool		use_tight = (pq->btree_strategy == BTLessStrategyNumber) &&
		value_on_bucket_boundary(pq->cst, bucket_width, fe);

		if (use_tight)
		{
			rhs_expr = (Node *) copyObject(pq->cst);
			rhs_type = pq->cst->consttype;
		}
		else
		{
			/* Need to add bucket_width.  Check overflow safety first. */
			if (!upper_addition_is_safe(pq->cst, bucket_width))
				return false;

			rhs_expr = build_plus_expr(pq->cst, bucket_width, &rhs_type);
			if (rhs_expr == NULL)
				return false;
		}

		new_op = build_lt_compare(inner_var, rhs_expr, rhs_type);
		if (new_op == NULL)
			return false;
	}

	append_qual_to_subquery(subq, new_op);
	ctx->added++;
	return true;
}

/*
 * Recursive walker that descends through nested subqueries / setOp
 * branches until it finds a time_bucket() target, then injects the
 * synthesized qual at that level.  Handles cv-style UNION ALL view
 * structure (live branch wraps _direct_view_1 which contains time_bucket).
 */
static bool
push_through_var_chain(Query *subq, AttrNumber attno,
					   ParsedQual * pq, TbPushdownCtx * ctx)
{
	TargetEntry *te;
	Var		   *v;
	RangeTblEntry *next_rte;

	if (subq == NULL)
		return false;

	if (subq->setOperations != NULL)
	{
		List	   *branches = NIL;
		ListCell   *lc;
		bool		any_pushed = false;

		collect_setop_leaves(subq->setOperations, subq, &branches);
		foreach(lc, branches)
		{
			Query	   *branch = (Query *) lfirst(lc);

			if (push_through_var_chain(branch, attno, pq, ctx))
				any_pushed = true;
		}
		return any_pushed;
	}

	if (attno <= 0 || attno > list_length(subq->targetList))
		return false;
	te = (TargetEntry *) list_nth(subq->targetList, attno - 1);
	if (te == NULL || te->expr == NULL)
		return false;

	if (IsA(te->expr, FuncExpr))
		return push_into_subquery_leaf(subq, attno, pq, ctx);

	if (IsA(te->expr, Var))
	{
		v = (Var *) te->expr;
		if (v->varno <= 0 || v->varno > list_length(subq->rtable))
			return false;
		next_rte = (RangeTblEntry *) list_nth(subq->rtable, v->varno - 1);
		if (next_rte->rtekind != RTE_SUBQUERY || next_rte->subquery == NULL)
			return false;
		return push_through_var_chain(next_rte->subquery, v->varattno,
									  pq, ctx);
	}

	return false;
}

/*
 * Top-level qual-pushdown attempt.  Recognize `Var op Const` or
 * `Const op Var` outer qual, classify bound via btree strategy,
 * then dispatch to the recursive walker.
 */
static void
try_pushdown_for_qual(Query *q, Node *qual, TbPushdownCtx * ctx)
{
	OpExpr	   *op;
	ParsedQual	pq;
	BoundKind	bound;
	RangeTblEntry *rte;
	Query	   *subq;

	if (!IsA(qual, OpExpr))
		return;
	op = (OpExpr *) qual;

	if (!parse_outer_qual(op, &pq))
		return;

	bound = classify_bound(pq.effective_opno, pq.var->vartype,
						   &pq.btree_strategy);
	if (bound == BOUND_NONE)
		return;

	if (pq.var->varno <= 0 ||
		pq.var->varno > list_length(q->rtable))
		return;
	rte = (RangeTblEntry *) list_nth(q->rtable, pq.var->varno - 1);
	if (rte->rtekind != RTE_SUBQUERY || rte->subquery == NULL)
		return;
	subq = rte->subquery;

	/*
	 * push_through_var_chain handles all subquery structures.  We stash the
	 * bound classification inside pq so descendants can use the same decision
	 * (re-classifying at each level would be redundant).
	 */
	(void) push_through_var_chain(subq, pq.var->varattno, &pq, ctx);
}


/* Walker plumbing */

static void
walk_quals(Query *q, Node *node, TbPushdownCtx * ctx)
{
	if (node == NULL)
		return;
	if (IsA(node, BoolExpr))
	{
		BoolExpr   *b = (BoolExpr *) node;

		if (b->boolop == AND_EXPR)
		{
			ListCell   *lc;

			foreach(lc, b->args)
				walk_quals(q, (Node *) lfirst(lc), ctx);
		}
		return;
	}
	try_pushdown_for_qual(q, node, ctx);
}

static bool
tb_query_walker(Node *node, TbPushdownCtx * ctx)
{
	if (node == NULL)
		return false;

	if (IsA(node, Query))
	{
		Query	   *q = (Query *) node;

		if (q->jointree && q->jointree->quals)
			walk_quals(q, q->jointree->quals, ctx);
		return query_tree_walker(q, tb_query_walker, ctx, 0);
	}

	return expression_tree_walker(node, tb_query_walker, ctx);
}

/*
 * Public entry: scan the parse tree for outer-Query quals of the form
 * `Var(sub.bucket) {>=,>,<=,<} Const` (or its commutator) whose subquery
 * tlist entry is a time_bucket() call, and push an equivalent bare-time
 * predicate into that subquery's WHERE clause.
 *
 *      >= or >   ->   Var {>=,>} Const                       (lower)
 *      <= or <   ->   Var <  (Const + bucket_width)           (upper)
 *      <  with C on bucket boundary, 2-arg time_bucket:
 *                ->   Var <  Const                            (tight upper)
 *
 * Commutator form `Const op Var` is canonicalized first.
 *
 * No-op on segments and on non-SELECT queries.
 */
void
time_bucket_pushdown_mutate(Query *parse)
{
	TbPushdownCtx ctx;

	if (parse == NULL)
		return;
	if (Gp_role != GP_ROLE_DISPATCH)
		return;
	if (parse->commandType != CMD_SELECT)
		return;

	ctx.added = 0;
	(void) tb_query_walker((Node *) parse, &ctx);

	if (ctx.added > 0)
		elog(DEBUG2, "time_bucket_pushdown: added %d equivalent qual(s)",
			 ctx.added);
}
