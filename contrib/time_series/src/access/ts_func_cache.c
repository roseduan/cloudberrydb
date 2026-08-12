/*-------------------------------------------------------------------------
 *
 * ts_func_cache.c
 *    OID-keyed registry of bucketing functions and their sort_transform
 *    callbacks.  Used by ChunkAppend pathkey detection so we can match
 *    `time_bucket(const_period, time_col)`, `date_trunc(const_field,
 *    time_col)`, `time_bucket_gapfill(...)` etc. without string compare,
 *    and reject the same call when the period/origin/offset args are
 *    not Const (which would break monotonicity).
 *
 *    Adapted from TimescaleDB's src/func_cache.c (Apache 2.0).  Key
 *    differences:
 *      - Our extension lives in schema "time_series" instead of TSDB's
 *        extension schema; pg_catalog functions are handled identically.
 *      - We do not register first/last aggregates.
 *      - Initialization is lazy: the first cache lookup builds the
 *        hashtable, which sidesteps the chicken-and-egg of running
 *        before extension SQL has been replayed.
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 * IDENTIFICATION
 *    contrib/time_series/src/access/ts_func_cache.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type_d.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/primnodes.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"

#include "../include/access/ts_func_cache.h"

#define TS_FUNC_CACHE_NUM_FUNCS (sizeof(funcinfo) / sizeof(funcinfo[0]))

typedef struct TsFuncEntry
{
	Oid			funcid;
	TsFuncInfo *info;
} TsFuncEntry;

static HTAB *func_hash = NULL;

/*
 * do_sort_transform
 *		Unwrap arg #2 recursively; return it if it resolves to a plain
 *		Var, otherwise return the original FuncExpr unchanged.  Caller
 *		owns the const-checks on other args.
 */
static Expr *
do_sort_transform(FuncExpr *func)
{
	Expr	   *second;

	second = ts_sort_transform_expr((Expr *) lsecond(func->args));
	if (!IsA(second, Var))
		return (Expr *) func;
	return (Expr *) copyObject(second);
}

/*
 * time_bucket_sort_transform
 *		time_bucket(const_period, time [, const_origin_or_offset]) is
 *		monotonic iff every arg except `time` (arg #2) is Const.
 */
static Expr *
time_bucket_sort_transform(FuncExpr *func)
{
	Assert(list_length(func->args) >= 2);
	if (!IsA(linitial(func->args), Const))
		return (Expr *) func;
	if (list_length(func->args) >= 3 && !IsA(lthird(func->args), Const))
		return (Expr *) func;
	return do_sort_transform(func);
}

/*
 * time_bucket_tz_sort_transform
 *		time_bucket(period, ts, timezone, origin, offset) — 5-arg
 *		timezone variant.  All args except `ts` (arg #2) must be Const.
 */
static Expr *
time_bucket_tz_sort_transform(FuncExpr *func)
{
	Assert(list_length(func->args) == 5);
	if (!IsA(linitial(func->args), Const) ||
		!IsA(lthird(func->args), Const) ||
		!IsA(lfourth(func->args), Const) ||
		!IsA((Node *) list_nth(func->args, 4), Const))
		return (Expr *) func;
	return do_sort_transform(func);
}

/*
 * time_bucket_gapfill_sort_transform
 *		time_bucket_gapfill(period, ts, start, finish [, timezone]).
 *		Period (arg #1) must be Const; for the 5-arg timezone variant
 *		the timezone (arg #3) must also be Const.  start/finish may be
 *		anything — they only affect gap-fill output rows, not bucket
 *		monotonicity.
 */
static Expr *
time_bucket_gapfill_sort_transform(FuncExpr *func)
{
	Assert(list_length(func->args) == 4 || list_length(func->args) == 5);
	if (!IsA(linitial(func->args), Const))
		return (Expr *) func;
	if (list_length(func->args) == 5 && !IsA(lthird(func->args), Const))
		return (Expr *) func;
	return do_sort_transform(func);
}

/*
 * date_trunc_sort_transform
 *		date_trunc(const_field, time): monotonic iff the field
 *		argument is Const, so every row is truncated to the same
 *		granularity.
 */
static Expr *
date_trunc_sort_transform(FuncExpr *func)
{
	Expr	   *second;

	if (list_length(func->args) != 2 || !IsA(linitial(func->args), Const))
		return (Expr *) func;
	second = ts_sort_transform_expr((Expr *) lsecond(func->args));
	if (!IsA(second, Var))
		return (Expr *) func;
	return (Expr *) copyObject(second);
}

/* Registry of known bucketing functions. */
static TsFuncInfo funcinfo[] = {
	/* time_series.time_bucket — 16 overloads.  int variants first. */
	{
		.origin = TS_ORIGIN_TIMESERIES, .is_bucketing_func = true,
		.funcname = "time_bucket", .nargs = 2,
		.arg_types = {INT2OID, INT2OID},
		.sort_transform = time_bucket_sort_transform,
	},
	{
		.origin = TS_ORIGIN_TIMESERIES, .is_bucketing_func = true,
		.funcname = "time_bucket", .nargs = 3,
		.arg_types = {INT2OID, INT2OID, INT2OID},
		.sort_transform = time_bucket_sort_transform,
	},
	{
		.origin = TS_ORIGIN_TIMESERIES, .is_bucketing_func = true,
		.funcname = "time_bucket", .nargs = 2,
		.arg_types = {INT4OID, INT4OID},
		.sort_transform = time_bucket_sort_transform,
	},
	{
		.origin = TS_ORIGIN_TIMESERIES, .is_bucketing_func = true,
		.funcname = "time_bucket", .nargs = 3,
		.arg_types = {INT4OID, INT4OID, INT4OID},
		.sort_transform = time_bucket_sort_transform,
	},
	{
		.origin = TS_ORIGIN_TIMESERIES, .is_bucketing_func = true,
		.funcname = "time_bucket", .nargs = 2,
		.arg_types = {INT8OID, INT8OID},
		.sort_transform = time_bucket_sort_transform,
	},
	{
		.origin = TS_ORIGIN_TIMESERIES, .is_bucketing_func = true,
		.funcname = "time_bucket", .nargs = 3,
		.arg_types = {INT8OID, INT8OID, INT8OID},
		.sort_transform = time_bucket_sort_transform,
	},
	/* TIMESTAMP variants */
	{
		.origin = TS_ORIGIN_TIMESERIES, .is_bucketing_func = true,
		.funcname = "time_bucket", .nargs = 2,
		.arg_types = {INTERVALOID, TIMESTAMPOID},
		.sort_transform = time_bucket_sort_transform,
	},
	{
		.origin = TS_ORIGIN_TIMESERIES, .is_bucketing_func = true,
		.funcname = "time_bucket", .nargs = 3,
		.arg_types = {INTERVALOID, TIMESTAMPOID, TIMESTAMPOID},
		.sort_transform = time_bucket_sort_transform,
	},
	{
		.origin = TS_ORIGIN_TIMESERIES, .is_bucketing_func = true,
		.funcname = "time_bucket", .nargs = 3,
		.arg_types = {INTERVALOID, TIMESTAMPOID, INTERVALOID},
		.sort_transform = time_bucket_sort_transform,
	},
	/* TIMESTAMPTZ variants */
	{
		.origin = TS_ORIGIN_TIMESERIES, .is_bucketing_func = true,
		.funcname = "time_bucket", .nargs = 2,
		.arg_types = {INTERVALOID, TIMESTAMPTZOID},
		.sort_transform = time_bucket_sort_transform,
	},
	{
		.origin = TS_ORIGIN_TIMESERIES, .is_bucketing_func = true,
		.funcname = "time_bucket", .nargs = 3,
		.arg_types = {INTERVALOID, TIMESTAMPTZOID, TIMESTAMPTZOID},
		.sort_transform = time_bucket_sort_transform,
	},
	{
		.origin = TS_ORIGIN_TIMESERIES, .is_bucketing_func = true,
		.funcname = "time_bucket", .nargs = 3,
		.arg_types = {INTERVALOID, TIMESTAMPTZOID, INTERVALOID},
		.sort_transform = time_bucket_sort_transform,
	},
	/* TIMESTAMPTZ + timezone (5 args) */
	{
		.origin = TS_ORIGIN_TIMESERIES, .is_bucketing_func = true,
		.funcname = "time_bucket", .nargs = 5,
		.arg_types = {INTERVALOID, TIMESTAMPTZOID, TEXTOID,
			TIMESTAMPTZOID, INTERVALOID},
		.sort_transform = time_bucket_tz_sort_transform,
	},
	/* DATE variants */
	{
		.origin = TS_ORIGIN_TIMESERIES, .is_bucketing_func = true,
		.funcname = "time_bucket", .nargs = 2,
		.arg_types = {INTERVALOID, DATEOID},
		.sort_transform = time_bucket_sort_transform,
	},
	{
		.origin = TS_ORIGIN_TIMESERIES, .is_bucketing_func = true,
		.funcname = "time_bucket", .nargs = 3,
		.arg_types = {INTERVALOID, DATEOID, DATEOID},
		.sort_transform = time_bucket_sort_transform,
	},
	{
		.origin = TS_ORIGIN_TIMESERIES, .is_bucketing_func = true,
		.funcname = "time_bucket", .nargs = 3,
		.arg_types = {INTERVALOID, DATEOID, INTERVALOID},
		.sort_transform = time_bucket_sort_transform,
	},

	/* time_series.time_bucket_gapfill — 7 overloads. */
	{
		.origin = TS_ORIGIN_TIMESERIES, .is_bucketing_func = true,
		.funcname = "time_bucket_gapfill", .nargs = 4,
		.arg_types = {INTERVALOID, TIMESTAMPOID, TIMESTAMPOID, TIMESTAMPOID},
		.sort_transform = time_bucket_gapfill_sort_transform,
	},
	{
		.origin = TS_ORIGIN_TIMESERIES, .is_bucketing_func = true,
		.funcname = "time_bucket_gapfill", .nargs = 4,
		.arg_types = {INTERVALOID, TIMESTAMPTZOID, TIMESTAMPTZOID, TIMESTAMPTZOID},
		.sort_transform = time_bucket_gapfill_sort_transform,
	},
	{
		.origin = TS_ORIGIN_TIMESERIES, .is_bucketing_func = true,
		.funcname = "time_bucket_gapfill", .nargs = 4,
		.arg_types = {INT2OID, INT2OID, INT2OID, INT2OID},
		.sort_transform = time_bucket_gapfill_sort_transform,
	},
	{
		.origin = TS_ORIGIN_TIMESERIES, .is_bucketing_func = true,
		.funcname = "time_bucket_gapfill", .nargs = 4,
		.arg_types = {INT4OID, INT4OID, INT4OID, INT4OID},
		.sort_transform = time_bucket_gapfill_sort_transform,
	},
	{
		.origin = TS_ORIGIN_TIMESERIES, .is_bucketing_func = true,
		.funcname = "time_bucket_gapfill", .nargs = 4,
		.arg_types = {INT8OID, INT8OID, INT8OID, INT8OID},
		.sort_transform = time_bucket_gapfill_sort_transform,
	},
	{
		.origin = TS_ORIGIN_TIMESERIES, .is_bucketing_func = true,
		.funcname = "time_bucket_gapfill", .nargs = 4,
		.arg_types = {INTERVALOID, DATEOID, DATEOID, DATEOID},
		.sort_transform = time_bucket_gapfill_sort_transform,
	},
	{
		.origin = TS_ORIGIN_TIMESERIES, .is_bucketing_func = true,
		.funcname = "time_bucket_gapfill", .nargs = 5,
		.arg_types = {INTERVALOID, TIMESTAMPTZOID, TEXTOID,
			TIMESTAMPTZOID, TIMESTAMPTZOID},
		.sort_transform = time_bucket_gapfill_sort_transform,
	},

	/* pg_catalog.date_trunc — 2 overloads. */
	{
		.origin = TS_ORIGIN_POSTGRES, .is_bucketing_func = true,
		.funcname = "date_trunc", .nargs = 2,
		.arg_types = {TEXTOID, TIMESTAMPOID},
		.sort_transform = date_trunc_sort_transform,
	},
	{
		.origin = TS_ORIGIN_POSTGRES, .is_bucketing_func = true,
		.funcname = "date_trunc", .nargs = 2,
		.arg_types = {TEXTOID, TIMESTAMPTZOID},
		.sort_transform = date_trunc_sort_transform,
	},
};

/*
 * ts_func_cache_init
 *		Resolve each FuncInfo entry's OID via SearchSysCache3 on
 *		PROCNAMEARGSNSP and populate func_hash.  Entries whose
 *		function isn't found yet (e.g. extension SQL not yet
 *		replayed) are silently skipped; a later call after the SQL
 *		has been replayed will pick them up.
 */
void
ts_func_cache_init(void)
{
	HASHCTL		hashctl;
	Oid			ts_nsp;
	size_t		i;

	if (func_hash != NULL)
		return;

	MemSet(&hashctl, 0, sizeof(hashctl));
	hashctl.keysize = sizeof(Oid);
	hashctl.entrysize = sizeof(TsFuncEntry);
	hashctl.hcxt = CacheMemoryContext;

	func_hash = hash_create("ts_func_cache",
							TS_FUNC_CACHE_NUM_FUNCS,
							&hashctl,
							HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	/* Look up "time_series" namespace at init time; may not exist yet
	 * if the extension hasn't been CREATEd in this DB — in that case
	 * skip the time_series-origin entries and only register pg_catalog.
	 */
	ts_nsp = get_namespace_oid("time_series", true);

	for (i = 0; i < TS_FUNC_CACHE_NUM_FUNCS; i++)
	{
		TsFuncInfo *info = &funcinfo[i];
		Oid			nsp = PG_CATALOG_NAMESPACE;
		oidvector  *argtypes;
		HeapTuple	tuple;
		Form_pg_proc procform;
		Oid			funcid;
		TsFuncEntry *entry;
		bool		found;

		if (info->origin == TS_ORIGIN_TIMESERIES)
		{
			if (!OidIsValid(ts_nsp))
				continue;		/* extension not installed in this DB */
			nsp = ts_nsp;
		}

		argtypes = buildoidvector(info->arg_types, info->nargs);
		tuple = SearchSysCache3(PROCNAMEARGSNSP,
								PointerGetDatum(info->funcname),
								PointerGetDatum(argtypes),
								ObjectIdGetDatum(nsp));
		pfree(argtypes);

		if (!HeapTupleIsValid(tuple))
		{
			elog(DEBUG2,
				 "ts_func_cache: function \"%s\" (%d args) not found in nsp %u",
				 info->funcname, info->nargs, nsp);
			continue;
		}

		procform = (Form_pg_proc) GETSTRUCT(tuple);
		funcid = procform->oid;
		ReleaseSysCache(tuple);

		entry = hash_search(func_hash, &funcid, HASH_ENTER, &found);
		/* Duplicates within funcinfo[] would indicate a programmer
		 * error; on duplicate, last entry wins (no Assert: PG may
		 * recreate functions with the same signature on upgrade).
		 */
		entry->funcid = funcid;
		entry->info = info;
	}
}

TsFuncInfo *
ts_func_cache_get(Oid funcid)
{
	TsFuncEntry *entry;

	if (func_hash == NULL)
		ts_func_cache_init();

	entry = hash_search(func_hash, &funcid, HASH_FIND, NULL);
	return entry ? entry->info : NULL;
}

TsFuncInfo *
ts_func_cache_get_bucketing_func(Oid funcid)
{
	TsFuncInfo *info = ts_func_cache_get(funcid);

	if (info == NULL || !info->is_bucketing_func)
		return NULL;
	return info;
}

/*
 * ts_sort_transform_expr
 *		Strip RelabelType wrappers and recurse into known bucketing
 *		FuncExprs via their sort_transform callbacks.  Returns the
 *		resolved Var if the chain unwinds to one, else the original
 *		(or innermost-unrecognised) expression.  Caller decides what
 *		to do based on IsA(result, Var).
 */
Expr *
ts_sort_transform_expr(Expr *expr)
{
	if (expr == NULL)
		return NULL;

	if (IsA(expr, RelabelType))
		return ts_sort_transform_expr(((RelabelType *) expr)->arg);

	if (IsA(expr, FuncExpr))
	{
		FuncExpr   *fe = (FuncExpr *) expr;
		TsFuncInfo *info = ts_func_cache_get_bucketing_func(fe->funcid);

		if (info != NULL && info->sort_transform != NULL)
			return info->sort_transform(fe);
	}

	return expr;
}
