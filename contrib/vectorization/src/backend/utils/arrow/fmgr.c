/*-------------------------------------------------------------------------
 *
 * fmgr.c
 * *
 * Copyright (c) 2016-Present Hashdata, Inc. 
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/arrow/fmgr.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_type.h"
#include "libpq/pqformat.h"
#include "parser/scansup.h"
#include "pgstat.h"
#include "string.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/datetime.h"
#include "utils/fmgroids.h"
#include "utils/fmgrtab.h"
#include "utils/numeric.h"

#include "utils/fmgr_vec.h"
#include "utils/vecfuncs.h"
#include "utils/wrapper.h"
#include "utils/arrow_fmgr.h"

static GArrowFunctionOptions*
build_empty(int numargs) { return NULL; }

static bool
alway_support(List* args) { 
	(void)args;
	return true;
}

static bool
check_like_function(List* args) {
	Expr  *first_expr = linitial(args);
	Expr  *second_expr = lsecond(args);
	if (!IsA(first_expr, Const) && !IsA(second_expr, Const))
	{
		return false;
	}
	return true;
}

static bool
check_floor_temporal(List* args) {
	Expr *first_expr;
	Const *const_expr;
	char *unit_str;

	if (list_length(args) != 2)
		return false;

	first_expr = (Expr *) linitial(args);
	if (!IsA(first_expr, Const))
		return false;
	const_expr = (Const *) first_expr;
	if (const_expr->constisnull || const_expr->consttype != TEXTOID)
		return false;

	unit_str = text_to_cstring(DatumGetTextP(const_expr->constvalue));
	/*
	 * "week"/"weeks" are intentionally excluded.  Arrow's floor_temporal with
	 * CalendarUnit::WEEK epoch-aligns against the Unix epoch (Thursday) while
	 * PG stores timestamps relative to 2000-01-01 (Saturday).  The 10957-day
	 * offset between the two epochs is not an integer number of weeks
	 * (10957 mod 7 = 2), so the kernel's output lands on Saturday in PG's
	 * frame, diverging from PG's Monday-based date_trunc('week', ...).  Let
	 * that case fall back to the row engine.
	 */
	if (pg_strcasecmp(unit_str, "nanosecond") != 0 &&
		pg_strcasecmp(unit_str, "nanoseconds") != 0 &&
		pg_strcasecmp(unit_str, "microsecond") != 0 &&
		pg_strcasecmp(unit_str, "microseconds") != 0 &&
		pg_strcasecmp(unit_str, "millisecond") != 0 &&
		pg_strcasecmp(unit_str, "milliseconds") != 0 &&
		pg_strcasecmp(unit_str, "second") != 0 &&
		pg_strcasecmp(unit_str, "seconds") != 0 &&
		pg_strcasecmp(unit_str, "minute") != 0 &&
		pg_strcasecmp(unit_str, "minutes") != 0 &&
		pg_strcasecmp(unit_str, "hour") != 0 &&
		pg_strcasecmp(unit_str, "hours") != 0 &&
		pg_strcasecmp(unit_str, "day") != 0 &&
		pg_strcasecmp(unit_str, "days") != 0 &&
		pg_strcasecmp(unit_str, "month") != 0 &&
		pg_strcasecmp(unit_str, "months") != 0 &&
		pg_strcasecmp(unit_str, "quarter") != 0 &&
		pg_strcasecmp(unit_str, "quarters") != 0 &&
		pg_strcasecmp(unit_str, "year") != 0 &&
		pg_strcasecmp(unit_str, "years") != 0)
	{
		pfree(unit_str);
		return false;
	}
	pfree(unit_str);
	return true;
}

static bool
check_numeric_round(List* args) {
	Expr *scale = (Expr *) lsecond(args);
	Const* const_expr = NULL;
	int32_t numeric_type;
	if (!IsA(scale, Const))
		return false;
	const_expr = (Const*) scale;
	numeric_type = const_expr->constbyval ? DatumGetInt32(const_expr->constvalue): const_expr->consttypmod;
	if (numeric_type > VEC_NUMERIC_MAX_PRECISION)
		return false;
	return true;

}

static bool
check_numeric_adjust(List* args) {
	Expr *scale = (Expr *) lsecond(args);
	Const* const_expr = NULL;
	int32_t numeric_type;
	int32_t precision;
	if (!IsA(scale, Const))
		return false;
	const_expr = (Const*) scale;
	numeric_type = const_expr->constbyval ? DatumGetInt32(const_expr->constvalue): const_expr->consttypmod;
	precision  = ((numeric_type  - VARHDRSZ) >> 16) & 0xffff;
	if (precision > VEC_NUMERIC_MAX_PRECISION)
		return false;
	return true;
}

const FuncTable arrow_func_tables[] = {ARROW_FMGR};
/*
 * Search pg_aggregate table by SQL to get PG aggregate function information
 * to fill funcName and fnoid.
 * SQL: select aggfnoid from pg_aggregate where aggfnoid = 2147; 
 */
/*
 * The 6th column (combinefn) is the Arrow function used for the middle stage of
 * a three-stage aggregate (AGGSPLIT_INTERMEDIATE): combine partial states and
 * re-emit a partial state without finalizing.  For aggregates whose partial
 * state equals the final representation (count, sum of int/float, min, max) the
 * combine is identical to the finalfn, so we reuse it.  Struct-state aggregates
 * map to avg_combine, which merges a struct<sum, count> partial and re-emits it;
 * sonic picks the variant from the sum-field type: avg(numeric)/sum(numeric)/
 * avg(int8) are numeric128 (avg(int8)'s partial is widened to numeric128 to
 * avoid int64 overflow, so it reuses the same combine as avg(numeric)),
 * avg(int2)/avg(int4) are int64, avg(float8) is double.  stddev_samp(int4)'s
 * partial has a third child (struct<sum, count, square>, Struct3) and so gets
 * its own dedicated stddev_avg_combine -> hash_avg_combine_stddev mapping.
 */
const ArrowAggFmgr arrow_agg_fmgr_builtins[] = {
	{ "min", F_MIN_INT2, "min", "min", "min", "min"},
	{ "min", F_MIN_INT4, "min", "min", "min", "min"},
	{ "min", F_MIN_INT8, "min", "min", "min", "min"},
	{ "min", F_MIN_DATE, "min", "min", "min", "min"},
	{ "min", F_MIN_TEXT, "min", "min", "min", "min"},
	{ "min", F_MIN_NUMERIC, "min", "min", "min", "min"},
	{ "max", F_MAX_INT2, "max", "max", "max", "max"},
	{ "max", F_MAX_INT4, "max", "max", "max", "max"},
	{ "max", F_MAX_INT8, "max", "max", "max", "max"},
	{ "max", F_MAX_DATE, "max", "max", "max", "max"},
	{ "max", F_MAX_NUMERIC, "max", "max", "max", "max"},
	{ "sum", F_SUM_INT2, "sum", "sum", "sum", "sum"},
	{ "sum", F_SUM_INT4, "sum", "sum", "sum", "sum"},
	{ "sum", F_SUM_INT8, "sum_64", "sum", "sum_64", "sum"},
	{ "sum", F_SUM_NUMERIC, "avg_trans", "sum_final", "sum", "avg_combine"},
	{ "count", F_COUNT_ANY, "count", "sum", "count", "sum"},
	{ "count", F_COUNT_, "count", "sum", "count", "sum"},
	{ "avg", F_AVG_INT8, "avg_trans", "avg_final", "mean_numeric", "avg_combine"},
	{ "avg", F_AVG_INT4, "avg_trans", "avg_final", "mean_numeric", "avg_combine"},
	{ "avg", F_AVG_INT2, "avg_trans", "avg_final", "mean_numeric", "avg_combine"},
        /* float8 */
	{ "avg", F_AVG_FLOAT8, "avg_trans", "avg_final", "mean", "avg_combine"},
	{ "stddev", F_STDDEV_SAMP_INT4, "stddev_avg_trans", "stddev_avg_final", "stddev_numeric", "stddev_avg_combine"},
	{ "min", F_MIN_FLOAT8, "min", "min", "min", "min"},
	{ "max", F_MAX_FLOAT8, "max", "max", "max", "max"},
	{ "sum", F_SUM_FLOAT8, "sum", "sum", "sum", "sum"},
        /* windowagg func */
	{ "rank", F_RANK_, NULL, NULL, "windowagg_rank", NULL},
	{ "row_number", F_ROW_NUMBER, NULL, NULL, "windowagg_row_number", NULL},
	{ "avg", F_AVG_NUMERIC, "avg_trans", "avg_final", "mean_numeric", "avg_combine"},
};

const AggFuncTable arrow_agg_func_tables[] = {
	/* agg func */
	{ "count", "hash_count", "count_distinct", "hash_count_distinct", build_all_count_options },
	{ "sum", "hash_sum", "sum_distinct", "hash_sum_distinct", build_empty},
	{ "sum_64", "hash_sum_64", "sum_distinct", "hash_sum_distinct", build_empty},
	{ "min", "hash_min", NULL, NULL, build_empty},
	{ "max", "hash_max", NULL, NULL, build_empty},
	{ "avg_trans", "hash_avg_trans", "avg_trans", "avg_trans", build_empty},
	{ "avg_final", "hash_avg_final", "avg_final", "avg_final", build_empty},
	{ "sum_final", "hash_sum_final", "sum_final", "sum_final", build_empty},
	{ "avg_combine", "hash_avg_combine", "avg_combine", "avg_combine", build_empty},
	/* windowagg func */
	{ "windowagg_rank", NULL, NULL, NULL, build_empty},
	{ "windowagg_row_number", NULL, NULL, NULL, build_empty},
	{ "stddev_avg_trans", "hash_avg_trans_stddev", "stddev_avg_trans", "stddev_avg_trans", build_empty},
	{ "stddev_avg_final", "hash_avg_final_stddev", "stddev_avg_final", "stddev_avg_final", build_empty},
	{ "stddev_avg_combine", "hash_avg_combine_stddev", "stddev_avg_combine", "stddev_avg_combine", build_empty},
	{ "mean", "hash_mean", "hash_mean", "hash_mean", build_empty},
        { "mean_numeric", "hash_mean_numeric", "hash_mean_numeric", "hash_mean_numeric", build_empty},
	{ "stddev_numeric", "hash_stddev_numeric", "hash_stddev_numeric", "hash_stddev_numeric", build_sample_stddev_options},
};

const FuncTable *
get_arrow_fmgr(Oid foid)
{
	int arrow_fmgr_length = sizeof(arrow_func_tables) / sizeof(FuncTable);
	const FuncTable *fmgr;

	for (int i = 0; i < arrow_fmgr_length; i++)
	{
		fmgr = &arrow_func_tables[i];
		if (foid == fmgr->procOid)
			return fmgr;
	}
	return NULL;
}

const ArrowAggFmgr *
get_arrow_agg_fmgr(Oid foid)
{
	int arrow_fmgr_length = sizeof(arrow_agg_fmgr_builtins) / sizeof(ArrowAggFmgr);
	const ArrowAggFmgr *fmgr;

	for (int i = 0; i < arrow_fmgr_length; i++)
	{
		fmgr = &arrow_agg_fmgr_builtins[i];
		if (foid == fmgr->fnoid)
			return fmgr;
	}
	return NULL;
}

const AggFuncTable *
get_arrow_agg_functable(const char* name)
{
	int i;
	int  arrow_functables_length = sizeof(arrow_agg_func_tables) / sizeof(AggFuncTable);
	const AggFuncTable *table;

	for (i = 0; i < arrow_functables_length; i++)
	{
		table = &arrow_agg_func_tables[i];
		if (strcasecmp(name, table->funcName) == 0)
			return table;
	}
	elog(ERROR, "Can not find Arrow aggregate functable, name: %s", name);
	return NULL;
}

/*
 * Notice! Function ID must be ordered in this table.
 */
const FmgrVecBuiltin fmgr_builtins_vec[] = {
  { 63,  "int2eq", 2, true, false, "equal" },
  { 64,  "int2lt", 2, true, false, "less_than" },
  { 65,  "int4eq", 2, true, false, "equal" },
  { 66,  "int4lt", 2, true, false, "less_than" },
  { 67,  "texteq", 2, true, false, "equal" },

  { 141, "int4mul",  2, true, false, "multiply" },
  { 144, "int4ne", 2, true, false, "not_equal" },
  { 145, "int2ne", 2, true, false, "not_equal" },
  { 146, "int2gt", 2, true, false, "greater_than" },
  { 147, "int4gt", 2, true, false, "greater_than" },
  { 148, "int2le", 2, true, false, "less_than_or_equal_to" },
  { 149, "int4le", 2, true, false, "less_than_or_equal_to" },

  { 150, "int4ge", 2, true, false, "greater_than_or_equal_to" },
  { 151, "int2ge", 2, true, false, "greater_than_or_equal_to" },
  { 154, "int4div", 2, true, false, "div" },
  { 157, "textne",  2, true, false, "not_equal" },

  { 158, "int24eq", 2, true, false, "equal" },
  { 159, "int42eq", 2, true, false, "equal" },
  { 160, "int24lt", 2, true, false, "less_than" },
  { 161, "int42lt", 2, true, false, "less_than" },
  { 162, "int24gt", 2, true, false, "greater_than" },
  { 163, "int42gt", 2, true, false, "greater_than" },
  { 164, "int24ne", 2, true, false, "not_equal" },
  { 165, "int42ne", 2, true, false, "not_equal" },
  { 166, "int24le", 2, true, false, "less_than_or_equal_to" },
  { 167, "int42le", 2, true, false, "less_than_or_equal_to" },
  { 168, "int24ge", 2, true, false, "greater_than_or_equal_to" },
  { 169, "int42ge", 2, true, false, "greater_than_or_equal_to" },

  { 177, "int4pl",  2, true, false , "add" },
  { 178, "int24pl", 2, true, false, "add" },

  { 181, "int4mi",  2, true, false, "subtract" },

  { 458, "text_larger",  2, true, false},
  { 459, "text_smaller", 2, true, false},

  { 463, "int8pl",  2, true, false, "add"},
  { 464, "int8mi",  2, true, false, "subtract" },
  { 465, "int8mul",  2, true, false, "multiply" },
  { 466, "int8div",  2, true, false, "div" },
  { 467, "int8eq", 2, true, false, "equal" },
  { 468, "int8ne", 2, true, false, "not_equal" },
  { 469, "int8lt", 2, true, false, "less_than" },

  { 470, "int8gt", 2, true, false, "greater_than" },
  { 471, "int8le", 2, true, false, "less_than_or_equal_to" },
  { 472, "int8ge", 2, true, false, "greater_than_or_equal_to" },
  { 474, "int84eq", 2, true, false, "equal" },
  { 475, "int84ne", 2, true, false, "not_equal" },
  { 476, "int84lt", 2, true, false, "less_than" },
  { 477, "int84gt", 2, true, false, "greater_than" },
  { 478, "int84le", 2, true, false, "less_than_or_equal_to" },
  { 479, "int84ge", 2, true, false, "greater_than_or_equal_to" },

  { 766, "int4inc",     3, false, false, ""},
  { 768, "int4larger", 2, true, false},
  { 769, "int4smaller", 2, true, false},
  { 770, "int2larger", 2, true, false},
  { 771, "int2smaller", 2, true, false},

  { 850, "textlike",2, true, false, "match_like" },
  { 851, "textnlike",2, true, false, "is_not_substr" },
  { 852, "int48eq", 2, true, false, "equal" },
  { 853, "int48ne", 2, true, false, "not_equal" },
  { 854, "int48lt", 2, true, false, "less_than" },
  { 855, "int48gt", 2, true, false, "greater_than" },
  { 856, "int48le", 2, true, false, "less_than_or_equal_to" },
  { 857, "int48ge", 2, true, false, "greater_than_or_equal_to" },

  {1048, "bpchareq",  2, true, false, "equal" },
  {1053, "bpcharne",  2, true, false, "not_equal" },
  {1086, "date_eq", 2, false, false, "equal" },
  {1087, "date_lt", 2, false, false, "less_than" },
  {1088, "date_le", 2, false, false, "less_than_or_equal_to" },
  {1089, "date_gt", 2, false, false, "greater_than" },
  {1090, "date_ge", 2, false, false, "greater_than_or_equal_to" },
  {1091, "date_ne", 2, false, false, "not_equal" },

  {1138, "date_larger",  2, true, false},
  {1139, "date_smaller", 2, true, false},

  {1219, "int8inc", 3, false, false, ""},
  {1236, "int8larger", 2, true, false},
  {1237, "int8smaller", 2, true, false},

  {1274, "int84pl", 2, true, false, "add" },
  {1275, "int84mi", 2, true, false, "subtract" },
  {1276, "int84mul", 2, true, false, "multiply" },
  {1277, "int84div", 2, true, false, "div" },
  {1278, "int48pl", 2, true, false, "add" },
  {1279, "int48mi", 2, true, false, "subtract" },

  {1280, "int48mul", 2, true, false, "multiply" },
  {1281, "int48div", 2, true, false, "div" },

  {1317, "textlen",  2, false, false},

  {1377, "time_larger",  2, true, false},
  {1378, "time_smaller", 2, true, false},

  {1631, "bpcharlike",2, true, false, "is_substr" },
  {1632, "bpcharnlike",2, true, false, "is_substr" },

  {1840, "int2_sum", 2, false, false },
  {1841, "int4_sum", 2, false, false },
  {1842, "int8_sum", 2, false, false },

  {1850, "int28eq", 2, true, false, "equal" },
  {1851, "int28ne", 2, true, false, "not_equal" },
  {1852, "int28lt", 2, true, false, "less_than" },
  {1853, "int28gt", 2, true, false, "greater_than" },
  {1854, "int28le", 2, true, false, "less_than_or_equal_to" },
  {1855, "int28ge", 2, true, false, "greater_than_or_equal_to" },

  {1856, "int82eq", 2, true, false, "equal" },
  {1857, "int82ne", 2, true, false, "not_equal" },
  {1858, "int82lt", 2, true, false, "less_than" },
  {1859, "int82gt", 2, true, false, "greater_than" },
  {1860, "int82le", 2, true, false, "less_than_or_equal_to" },
  {1861, "int82ge", 2, true, false, "greater_than_or_equal_to" },

  {1962, "int2_avg_accum", 1, false, false},
  {1963, "int4_avg_accum", 1, false, false},
  {1964, "int8_avg", 1, false, false},

  {2021, "timestamp_part", 1, false, false},

  {2035, "timestamp_smaller",  2, true, false},
  {2036, "timestamp_larger", 2, true, false},

  {2087, "replace_text", 3, false, false},

  {2746, "int8_avg_accum", 2, false, false},
  {2785, "int8_avg_combine", 2, false, false},
  {2786, "int8_avg_serialize", 1, true, false},
  {2787, "int8_avg_deserialize", 1, true, false},

  {2804, "int8inc_any", 3, false, false, ""},

  {3324, "int4_avg_combine", 2, false, false},

  {3388, "numeric_poly_sum", 2, false, false},
  {3389, "numeric_poly_avg", 2, false, false},
};

/* Note fmgr_nbuiltins excludes the dummy entry */
const int fmgr_nbuiltins_vec = (sizeof(fmgr_builtins_vec) / sizeof(FmgrVecBuiltin));

const FmgrVecBuiltin *
fmgr_isbuiltin_vec(Oid id)
{
	int			low = 0;
	int			high = fmgr_nbuiltins_vec - 1;

	/*
	 * Loop invariant: low is the first index that could contain target entry,
	 * and high is the last index that could contain it.
	 */
	while (low <= high)
	{
		int			i = (high + low) / 2;
		const FmgrVecBuiltin *ptr = &fmgr_builtins_vec[i];

		if (id == ptr->foid)
			return ptr;
		else if (id > ptr->foid)
			low = i + 1;
		else
			high = i - 1;
	}
	return NULL;
}

Datum
FunctionCall1Args(const char *fname, void *arg1)
{
	g_autoptr(GArrowDatum)		result;
	g_autoptr(GArrowFunction)	func;
	GList					*args  = NULL;
	g_autoptr(GError)      error = NULL;
	g_autoptr(GArrowDatum) arg = garrow_copy_ptr(GARROW_DATUM(arg1));

	args = garrow_list_append_ptr(args, arg);

	func = garrow_function_find(fname);
	if (!func)
		elog(ERROR, "%s() not found in arrow", fname);

	result = garrow_function_execute(func, args, NULL, NULL, &error);

	if (error != NULL)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("%s: %s", __FUNCTION__, error->message)));

	garrow_list_free_ptr(&args);

	PG_VEC_RETURN_POINTER(result);
}

Datum
FunctionCall2Args(const char *fname, void *arg1, void *arg2)
{
	g_autoptr(GArrowDatum)		result;
	g_autoptr(GArrowFunction)	func;
	g_autoptr(GArrowDatum) argl = garrow_copy_ptr(GARROW_DATUM(arg1));
	g_autoptr(GArrowDatum) argr = garrow_copy_ptr(GARROW_DATUM(arg2));
	GList					*args  = NULL;
	GError					*error = NULL;

	args = garrow_list_append_ptr(args, argl);
	args = garrow_list_append_ptr(args, argr);

	/* Todo: find function when init */
	func = garrow_function_find(fname);
	if (!func)
		elog(ERROR, "%s() not found in arrow", fname);

	result = garrow_function_execute(func, args, NULL, NULL, &error);

	if (error != NULL)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("%s: %s", __FUNCTION__, error->message)));

	garrow_list_free_ptr(&args);

	PG_VEC_RETURN_POINTER(result);
}

Datum
FunctionCall3Args(const char *fname, void *arg1, void *arg2, void* arg3)
{
	g_autoptr(GArrowDatum)		result = NULL;
	g_autoptr(GArrowFunction)	func = NULL;
	g_autoptr(GArrowDatum) argl = garrow_copy_ptr(GARROW_DATUM(arg1));
	g_autoptr(GArrowDatum) argm = garrow_copy_ptr(GARROW_DATUM(arg2));
	g_autoptr(GArrowDatum) argr = garrow_copy_ptr(GARROW_DATUM(arg3));
	GList	    *args = NULL;
	GError	    *error = NULL;

	args = garrow_list_append_ptr(args, argl);
	args = garrow_list_append_ptr(args, argm);
	args = garrow_list_append_ptr(args, argr);

	/* Todo: find function when init */
	func = garrow_function_find(fname);

	if (!func)
		elog(ERROR, "%s() not found in arrow", fname);

	result = garrow_function_execute(func, args, NULL, NULL, &error);

	if (error != NULL)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("%s: %s", __FUNCTION__, error->message)));

	garrow_list_free_ptr(&args);

	PG_VEC_RETURN_POINTER(result);
}

Datum DirectCallVecFunc2Args(const char *fname, void *arg1, void *arg2)
{
	g_autoptr(GArrowDatum)  result = NULL;

	result = GARROW_DATUM(DatumGetPointer(FunctionCall2Args(fname, arg1, arg2)));

	PG_VEC_RETURN_POINTER(result);
}

Datum DirectCallVecFunc1Args(const char *fname, void *arg1)
{
	g_autoptr(GArrowDatum)  result = NULL;

	result = GARROW_DATUM(DatumGetPointer(FunctionCall1Args(fname, arg1)));

	PG_VEC_RETURN_POINTER(result);
}

Datum DirectCallVecFunc2ArgsAndU32ArrayRes(const char *fname, void *arg1, void *arg2)
{
        g_autoptr(GArrowDatum)             result = NULL;
        g_autoptr(GArrowArray)          array = NULL;
        g_autoptr(GArrowUInt32DataType) uint32_cast_type = NULL;
        g_autoptr(GArrowUInt32Array)    uint32_cast_array = NULL;
        g_autoptr(GArrowCastOptions)    option = NULL;
        GError        *error = NULL;

        result = GARROW_DATUM(DatumGetPointer(FunctionCall2Args(fname, arg1, arg2)));

        array = garrow_datum_get_array(GARROW_ARRAY_DATUM(result));

        if (garrow_array_get_value_type(array) != GARROW_TYPE_UINT32)
        {
                option = garrow_cast_options_new();

                garrow_set_cast_options(option, GARROW_PROP_ALLOW_FLOAT_TRUNCATE, 1);

                uint32_cast_type = garrow_uint32_data_type_new();
                uint32_cast_array = GARROW_UINT32_ARRAY(
                                garrow_array_cast(array, GARROW_DATA_TYPE(uint32_cast_type), option, &error));
                garrow_store_func(result, garrow_array_datum_new(GARROW_ARRAY(uint32_cast_array)));
        }

        PG_VEC_RETURN_POINTER(result);
}

Datum DirectCallVecFunc3ArgsAndU32ArrayRes(const char *fname,
                                                                                   void *arg1,
                                                                                   void *arg2,
                                                                                   void *arg3)
{
        g_autoptr(GArrowDatum)             result = NULL;
        g_autoptr(GArrowArray)          array = NULL;
        g_autoptr(GArrowUInt32DataType) uint32_cast_type = NULL;
        g_autoptr(GArrowUInt32Array)    uint32_cast_array = NULL;
        g_autoptr(GArrowCastOptions)    option = NULL;
        GError        *error = NULL;

        result = GARROW_DATUM(DatumGetPointer(FunctionCall3Args(fname, arg1, arg2, arg3)));

        array = garrow_datum_get_array(GARROW_ARRAY_DATUM(result));

        if (garrow_array_get_value_type(array) != GARROW_TYPE_UINT32)
        {
                option = garrow_cast_options_new();

                garrow_set_cast_options(option, GARROW_PROP_ALLOW_FLOAT_TRUNCATE, 1);

                uint32_cast_type = garrow_uint32_data_type_new();
                uint32_cast_array = GARROW_UINT32_ARRAY(
                                garrow_array_cast(array, GARROW_DATA_TYPE(uint32_cast_type), option, &error));
                garrow_store_func(result, garrow_array_datum_new(GARROW_ARRAY(uint32_cast_array)));
        }

        PG_VEC_RETURN_POINTER(result);
}

/* end of file */
