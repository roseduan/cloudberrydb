/*-------------------------------------------------------------------------
 *
 * ts_func_cache.h
 *    OID-keyed registry of known time-bucketing / monotonic-on-time
 *    functions used by planner-side optimizations (ChunkAppend pathkey
 *    matching, future cagg refresh planning, etc.).
 *
 *    Modelled after TimescaleDB's func_cache (src/func_cache.{c,h}).
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 * IDENTIFICATION
 *    contrib/time_series/src/include/access/ts_func_cache.h
 *-------------------------------------------------------------------------
 */
#ifndef TS_FUNC_CACHE_H
#define TS_FUNC_CACHE_H

#include "postgres.h"
#include "nodes/primnodes.h"

#define TS_FUNC_CACHE_MAX_FUNC_ARGS 6

typedef Expr *(*ts_sort_transform_func_cb) (FuncExpr *func);

typedef enum
{
	TS_ORIGIN_TIMESERIES = 0,
	TS_ORIGIN_POSTGRES = 1,
} TsFuncOrigin;

typedef struct TsFuncInfo
{
	const char *funcname;
	TsFuncOrigin origin;
	bool		is_bucketing_func;
	int			nargs;
	Oid			arg_types[TS_FUNC_CACHE_MAX_FUNC_ARGS];
	ts_sort_transform_func_cb sort_transform;
} TsFuncInfo;

extern void ts_func_cache_init(void);
extern TsFuncInfo *ts_func_cache_get(Oid funcid);
extern TsFuncInfo *ts_func_cache_get_bucketing_func(Oid funcid);
extern Expr *ts_sort_transform_expr(Expr *expr);

#endif							/* TS_FUNC_CACHE_H */
