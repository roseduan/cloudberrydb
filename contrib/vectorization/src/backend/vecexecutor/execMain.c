/*-------------------------------------------------------------------------
 *
 * execMain.c
 *	  top level executor interface routines and generate arrow expression tree.
 *
 * Copyright (c) 2016-Present Hashdata, Inc. 
 *
 * IDENTIFICATION
 *	  src/backend/vecexecutor/execMain.c
 *
 *-------------------------------------------------------------------------
 */
#include "arrow-glib/record-batch.h"
#include "arrow-dataset-glib/scanner.h"
#include "postgres.h"

#include "catalog/pg_operator_d.h"
#include "catalog/pg_tablespace_d.h"
#include "cdb/cdbplan.h"
#include "cdb/cdbvars.h"
#include "common/int.h"
#include "nodes/nodeFuncs.h"
#include "parser/parsetree.h"
#include "nodes/nodes.h"
#include "parser/scansup.h"
#include "optimizer/optimizer.h"
#include "utils/arrow_sort_options_vec.h"
#include "utils/fmgr_vec.h"
#include "utils/lsyscache.h"
#include "utils/tuptable_vec.h"
#include "utils/vecfuncs.h"
#include "utils/vecsort.h"
#include "vecexecutor/execslot.h"
#include "vecexecutor/executor.h"
#include "vecexecutor/vec_motion_direct_send.h"
#include "vecexecutor/vec_topk_bounds.h"
#include "utils/numeric.h"
#include "utils/guc_vec.h"
#include "storage/fd.h"
#include "storage/ipc.h"

#define VEC_SPILL_FILE_PREFIX "vec_spill"
#include "miscadmin.h"
#include "pgstat.h"

#include <dlfcn.h>

/* Resolve PaxCanFastFilterC from pax.so at runtime via dlsym to avoid
 * a hard link-time dependency on the pax_storage library. */
typedef bool (*PaxCanFastFilterFn)(Node *qual, TupleDesc desc);
static PaxCanFastFilterFn pax_can_fast_filter_fn = NULL;
static bool pax_can_fast_filter_resolved = false;

static bool
PaxCanFastFilterC(Node *qual, TupleDesc desc)
{
	if (!pax_can_fast_filter_resolved)
	{
		pax_can_fast_filter_fn =
			(PaxCanFastFilterFn) dlsym(RTLD_DEFAULT, "PaxCanFastFilterC");
		pax_can_fast_filter_resolved = true;
	}
	if (pax_can_fast_filter_fn)
		return pax_can_fast_filter_fn(qual, desc);
	return false;  /* pax.so not loaded, treat as non-fast-filterable */
}

typedef struct VecAggInfo
{
	Aggref *aggref;
	WindowFunc *wfunc;
	const char *aname; /* arrow aggregate function name */
	GArrowFunctionOptions *options;
	char inname[NAMEDATALEN + 30];
	char outname[NAMEDATALEN + 30];
} VecAggInfo;

typedef enum
{
	Plain,
	Orderby,
	Selectk,
	Mergesort,
	Consuming,
} SinkType;

typedef enum
{
	IN_MEMORY,
	IN_SINGLE,
	IN_CHUNK,
	IN_SINGLE_UNSAFE,
	IN_CHUNK_UNSAFE
}StoreType;

typedef struct PlanBuildContext
{
	GArrowExecutePlan *plan;
	PlanState *planstate;

	/* Aggregate information */
	List *agginfos;
	AggStrategy aggstrategy;
	const gchar **keys; /* column name of group keys, used by windowagg/windowhashagg also */
	int nkey;           /* number of groups, used by windowagg/windowhashagg also */
	bool ishaving;

	/* windowagg/windowhashagg fields */
	GArrowSortOptions *orderby_sortoption;

	/* schema of plan input batch*/
	GArrowSchema *inputschema;

	/* plan recipe */
	SinkType sinktype;
	int nlimit; /* the "k" of selectk */

	int *map;/* scan mapping col*/

	/* reader */
	bool pipeline;
	GArrowRecordBatchReader *reader;

	/* hashjoin related */
	bool is_hashjoin;
	bool is_hashjoin_after_node;
	bool is_left_schema;
	/* left and right raw schema */
	GArrowSchema *left_in_schema;
	GArrowSchema *right_in_schema;
	/* left and right proj schema */
	GArrowSchema *left_proj_schema;
	GArrowSchema *right_proj_schema;

	GList *left_hashkeys;
	GList *right_hashkeys;

	/* nestloopjoin related */
	bool is_nestloopjoin;

	/* is assertop node */
	 bool is_assertop;

	/* CaseTestExpr is case when placeholder */
	Expr *case_test_expr;
	bool is_case_when;
	GArrowExpression *whenexpr;
	GArrowExpression *not_and_whenexpr;
	Oid case_when_type;

	/* sequence related */
	int subplan_index;

	/* AppendNode fields */
	bool is_append;
	int sub_slice;
	int append_filed_index;

	/* MaterialNode fileds */
	bool is_materialize;
	char *store_file;
	StoreType store_type;
	int64_t chunk_size;
	bool cdb_strict;

	/* parallel scan relation schema */
	bool parallel_scan;
	Index table_oid;
	Index am_oid;
	GArrowSchema* relation_schema;

	/* ShareScanNode fields */
	bool is_sharescan;
	int gp_session_id;
	int gp_command_count;
	int share_id;
	int num_slices;
	int is_cross_slice;
	bool ready;
	bool is_producer;

	/* sonic motion direct-send: set true after BuildAggregatation when the
	 * downstream Acero schema carries a leading hidden target-segment
	 * column (named by sonic_target_segment_col_name) that must survive
	 * the post-Agg project node. */
	bool sonic_target_segment_passthrough;
	const char *sonic_target_segment_col_name;
} PlanBuildContext;

typedef struct SortKey
{
	GArrowSortOrder *orders;
	GArrowSortOrder *nulls_first;
} SortKey;

GArrowSchema *dummy_schema = NULL;
static inline VecAggInfo* new_agg_info(Aggref *aggref, int aggno, PlanBuildContext *pcontext);
static inline VecAggInfo* new_winagg_info(WindowFunc *wfunc, PlanBuildContext *pcontext);
static GArrowExpression *expr_to_arrow_expression(Expr *node, PlanBuildContext *pcontext);
static const char* get_agg_func_name(Aggref *aggref, const AggFuncTable *table, PlanBuildContext *pcontext);
static void CleanupArrowSpillFiles(int code, Datum arg);
static GArrowExecuteNode* BuildSource(PlanBuildContext *pcontext);
static GArrowExecuteNode* BuildScanNode(PlanBuildContext *pcontext);
static GArrowExecuteNode *BuildProject(List *targetList, List *qualList, GArrowExecuteNode *input, PlanBuildContext *pcontext);
static void BuildSink(GArrowExecuteNode *input, VecExecuteState *estate, PlanBuildContext *pcontext);
static GArrowExecuteNode *BuildAggregatation(List *aggInfos, GArrowExecuteNode *input, PlanBuildContext *pcontext);
static void *get_scan_next_batch(PlanState *node);
static void *get_foreign_next_batch(PlanState *node);
static void *get_current_next_batch(PlanState *node);
static GList* build_sort_keys(PlanState *planstate, GArrowSchema *schema);
static GArrowProjectNodeOptions* build_project_options(List *targetList, PlanBuildContext *pcontext);
static GArrowProjectNodeOptions* build_agg_project_options(List *targetList, List *aggInfos, PlanBuildContext *pcontext);
static GArrowProjectNodeOptions* build_windowagg_project_options(List *targetList, List *aggInfos, PlanBuildContext *pcontext);
static GArrowProjectNodeOptions* build_windowhashagg_project_options(List *targetList, List *aggInfos, PlanBuildContext *pcontext);
static GArrowAggregateNodeOptions* build_aggregatation_options(GList *aggregations, PlanBuildContext *pcontext, bool sonic_ok);
static GArrowFilterNodeOptions *build_filter_options(List *filterInfo, PlanBuildContext *pcontext);
static GArrowAssertOpNodeOptions *build_assertop_options(List *filterInfo, PlanBuildContext *pcontext);
static GArrowExecuteNode *BuildHashjoin(PlanBuildContext *pcontext, GArrowExecuteNode *left, GArrowExecuteNode *right, List *joinqual);
static GArrowExecuteNode *BuildJoinProject(List *hashkeys, GArrowExecuteNode *input, PlanBuildContext *pcontext);
static GArrowProjectNodeOptions *build_join_project_options(List *hashkeys, GArrowExecuteNode *input, PlanBuildContext *pcontext);
static void rewrite_tl_keys(List *targetList, PlanBuildContext *pcontext);
static GArrowExpression *build_cast_expression(List* args, PlanBuildContext *pcontext, GArrowDataType *to_type, bool allow_truncate);
static void get_windowagg_sortorder(PlanBuildContext *pcontext, SortKey *sortKey);
static GArrowExecuteNode *BuildNestLoopjoin(PlanBuildContext *pcontext, GArrowExecuteNode *left, GArrowExecuteNode *right, List *joinqual);
static void BuildJoinPlan(PlanBuildContext *pcontext, VecExecuteState *estate);
static void BuildSequencePlan(PlanBuildContext *pcontext, VecExecuteState *estate);
static const char *GetHashJoinProjectName(PlanBuildContext *pcontext, const char *name);
static GArrowExpression *build_is_distinct_expression(DistinctExpr *dex, PlanBuildContext *pcontext);
static GArrowExpression *build_null_if_expression(NullIfExpr *dex, PlanBuildContext *pcontext);
static GArrowExecuteNode*build_orderby_node(PlanState *planstate, GArrowExecutePlan *plan, GArrowExecuteNode *input);
static GArrowExecuteNode*build_topk_node(PlanState *planstate, GArrowExecutePlan *plan, GArrowExecuteNode *input, int64 topk_bound);
static GArrowExpression *build_literal_expression(Datum datum, bool isnull, Oid pg_type, int32 typmod);
static void BuildMaterializePlan(PlanBuildContext *pcontext, VecExecuteState *estate);
static GArrowStoreType to_arrow_storetype(StoreType type);
static void BuildShareScanPlan(PlanBuildContext *pcontext, VecExecuteState *estate);

static int plan_num = 0;

static char *
build_materialize_file_name()
{
	char *file_name = NULL;
	Oid tblspcOid = InvalidOid;
	char ts_path[PATH_MAX] = { "\0" };

	file_name = palloc0(PATH_MAX);
	tblspcOid = MyDatabaseTableSpace ? MyDatabaseTableSpace : DEFAULTTABLESPACE_OID;
	TempTablespacePath(ts_path, tblspcOid);
	snprintf(file_name, PATH_MAX - 1, "%s/%s_%d_%d_%s.%s.%d", ts_path, VECPATH, gp_session_id, gp_command_count, "vec", "materialize", MyProcPid);
	return file_name;
}

static PlanState *
get_sequence_exact_result_ps(PlanState *plan)
{
	SequenceState *node = castNode(SequenceState, plan);

	PlanState *lastPS = node->subplans[node->numSubplans - 1];

	if (IsA(lastPS, SequenceState))
	{
		return get_sequence_exact_result_ps(lastPS);
	}

	return lastPS;
}

static const char *
get_agg_func_name(Aggref *aggref, const AggFuncTable *table, PlanBuildContext *pcontext)
{
	int cols;
	const char *name = NULL;

	cols = list_length(aggref->aggdistinct);

	Assert(table != NULL);

	/* is a hash agg */
	if (pcontext->aggstrategy == AGG_HASHED)
		/* is a hash distinct agg */
		if (cols > 0)
			name = table->hashDistFuncName;
		else
			name = table->hashFuncName;
	else if (cols > 0) /* is a distinct agg */
		name = table->distFuncName;
	else
		name = table->funcName;

	if (!name)
		elog(ERROR, "Cann't find Arrow agg function name.");
	return name;
}

/*
 * New an arrow expression with PG function arguments and arrow function name.
 * such as:
 *  i + j - 1 will generate subtract(add(i, j), 1)
 */
GArrowExpression *
func_args_to_expression(List *args, PlanBuildContext *pcontext, const char* funcname)
{
	ListCell *l;
	GList *arguments = NULL;
	g_autoptr(GArrowExpression)  result_expr = NULL;

	foreach(l, args)
	{
		Expr  *fle = (Expr *) lfirst(l);
		g_autoptr(GArrowExpression) expr = expr_to_arrow_expression(fle, pcontext);

		if (!expr)
		{
			elog(ERROR, "Expression to arrow error.");
			return NULL;
		}

		/* No more than two arguments for arrow function.
		 * Combine the existing two arguments as a new arrow function.
		 */
		if (g_list_length(arguments) >= 2)
		{
			g_autoptr(GArrowExpression) funcexpr = NULL;

			funcexpr = GARROW_EXPRESSION(garrow_call_expression_new(
					funcname, arguments, NULL));
			if (!funcexpr)
					elog(ERROR, "Failed to new arrow call expression %s.", funcname);
			garrow_list_free_ptr(&arguments);
			arguments = garrow_list_append_ptr(arguments, funcexpr);
		}

		arguments = garrow_list_append_ptr(arguments, expr);
	}

	result_expr = GARROW_EXPRESSION(garrow_call_expression_new(
			funcname, arguments, NULL));
	if (!result_expr)
			elog(ERROR, "Failed to new arrow call expression %s.", funcname);
	garrow_list_free_ptr(&arguments);
	return garrow_move_ptr(result_expr);
}

static GArrowExpression *
//FIXME: For round(numeric, int), currently int supports only const cases
build_round_expr(List *args, PlanBuildContext *pcontext, bool has_precision)
{
	ListCell *l = NULL;
	GList *arguments = NULL;
	g_autoptr(GArrowExpression) round_expr = NULL;
	g_autoptr(GArrowRoundOptions) options = NULL;
	int scale = 0;
	foreach (l, args)
	{
		g_autoptr(GArrowExpression) cur_expr = NULL;
		Expr  *expr = (Expr *) lfirst(l);
		if (IsA(expr, Const))
		{
			Const* const_expr = (Const*) expr;
			int numeric_type = const_expr->constbyval ? DatumGetInt32(const_expr->constvalue): const_expr->consttypmod;
			if (has_precision)
				scale = (numeric_type - VARHDRSZ) & 0xffff;
			else
				scale = numeric_type;
			continue;
		}
		cur_expr = expr_to_arrow_expression(expr, pcontext);
		arguments = garrow_list_append_ptr(arguments, cur_expr);
	}
	options = garrow_round_options_new();
	garrow_round_options_set(options,GARROW_ROUND_HALF_UP, scale);
	round_expr = GARROW_EXPRESSION(garrow_call_expression_new("round", arguments, GARROW_FUNCTION_OPTIONS(options)));
	garrow_list_free_ptr(&arguments);
	return garrow_move_ptr(round_expr);
}

GArrowExpression *
build_round_with_precision(List *args, PlanBuildContext *pcontext, const char* name)
{
	return build_round_expr(args, pcontext, true);
}

GArrowExpression *
build_round_without_precision(List *args, PlanBuildContext *pcontext, const char* name)
{
	return build_round_expr(args, pcontext, false);
}

GArrowExpression *
build_floor_temporal_expr(List *args, PlanBuildContext *pcontext, const char* funcname)
{
	GList *arguments = NULL;
	g_autoptr(GArrowExpression) floor_temporal_expr = NULL;
	g_autoptr(GArrowRoundTemporalOptions) options = NULL;
	g_autoptr(GError) error = NULL;
	GArrowCalendarUnit unit = GARROW_CALENDAR_UNIT_SECOND;
	gint64 multiple = 1;

	/*
	 * The planner (check_floor_temporal) has already verified that args
	 * has exactly 2 elements, the first being a non-null text Const with
	 * a supported unit string.
	 */
	Const *const_expr = (Const *) linitial(args);
	text *unit_text = DatumGetTextP(const_expr->constvalue);
	char *unit_str = text_to_cstring(unit_text);

	/* Parse unit string to GArrowCalendarUnit */
	if (pg_strcasecmp(unit_str, "nanosecond") == 0 || pg_strcasecmp(unit_str, "nanoseconds") == 0)
		unit = GARROW_CALENDAR_UNIT_NANOSECOND;
	else if (pg_strcasecmp(unit_str, "microsecond") == 0 || pg_strcasecmp(unit_str, "microseconds") == 0)
		unit = GARROW_CALENDAR_UNIT_MICROSECOND;
	else if (pg_strcasecmp(unit_str, "millisecond") == 0 || pg_strcasecmp(unit_str, "milliseconds") == 0)
		unit = GARROW_CALENDAR_UNIT_MILLISECOND;
	else if (pg_strcasecmp(unit_str, "second") == 0 || pg_strcasecmp(unit_str, "seconds") == 0)
		unit = GARROW_CALENDAR_UNIT_SECOND;
	else if (pg_strcasecmp(unit_str, "minute") == 0 || pg_strcasecmp(unit_str, "minutes") == 0)
		unit = GARROW_CALENDAR_UNIT_MINUTE;
	else if (pg_strcasecmp(unit_str, "hour") == 0 || pg_strcasecmp(unit_str, "hours") == 0)
		unit = GARROW_CALENDAR_UNIT_HOUR;
	else if (pg_strcasecmp(unit_str, "day") == 0 || pg_strcasecmp(unit_str, "days") == 0)
		unit = GARROW_CALENDAR_UNIT_DAY;
	else if (pg_strcasecmp(unit_str, "month") == 0 || pg_strcasecmp(unit_str, "months") == 0)
		unit = GARROW_CALENDAR_UNIT_MONTH;
	else if (pg_strcasecmp(unit_str, "quarter") == 0 || pg_strcasecmp(unit_str, "quarters") == 0)
		unit = GARROW_CALENDAR_UNIT_QUARTER;
	else if (pg_strcasecmp(unit_str, "year") == 0 || pg_strcasecmp(unit_str, "years") == 0)
		unit = GARROW_CALENDAR_UNIT_YEAR;
	else
		/* Should not be reached: check_floor_temporal already validated the unit */
		elog(ERROR, "Unsupported calendar unit: %s", unit_str);
	pfree(unit_str);

	/* Second argument is the timestamp data */
	g_autoptr(GArrowExpression) cur_expr =
		expr_to_arrow_expression((Expr *) lsecond(args), pcontext);
	if (!cur_expr)
	{
		elog(ERROR, "Failed to convert timestamp expression to Arrow");
		return NULL;
	}
	arguments = garrow_list_append_ptr(arguments, cur_expr);

	/* Create RoundTemporalOptions with parsed unit and multiple */
	options = garrow_round_temporal_options_new();
	if (!garrow_round_temporal_options_set(options, multiple, unit, &error))
	{
		garrow_list_free_ptr(&arguments);
		elog(ERROR, "Failed to set RoundTemporalOptions: %s",
			 error ? error->message : "unknown");
	}

	/* Create the floor_temporal call expression with options */
	floor_temporal_expr = GARROW_EXPRESSION(garrow_call_expression_new(
		"floor_temporal", arguments, GARROW_FUNCTION_OPTIONS(options)));

	if (!floor_temporal_expr)
	{
		garrow_list_free_ptr(&arguments);
		elog(ERROR, "Failed to create floor_temporal expression");
	}

	garrow_list_free_ptr(&arguments);
	return garrow_move_ptr(floor_temporal_expr);
}

GArrowExpression *
build_text_join(List *args, PlanBuildContext *pcontext, const char *name)
{
	ListCell *l = NULL;
	GList *arguments = NULL;
	g_autoptr(GArrowExpression) text_join_expr = NULL;
	// add empty separator to impl text join
	g_autoptr(GArrowDatum) val_datum = NULL;
	g_autoptr(GArrowExpression) li_expr = NULL;
	g_autoptr(GArrowBuffer) empty_buffer = NULL;
	g_autoptr(GArrowScalar) empty_buffer_scalar = NULL;
	foreach (l, args)
	{
		g_autoptr(GArrowExpression) cur_expr = NULL;
		Expr *arg = (Expr *) lfirst(l);
		cur_expr = expr_to_arrow_expression(arg, pcontext);
		arguments = garrow_list_append_ptr(arguments, cur_expr);
	}
	empty_buffer = garrow_buffer_new((const guint8*)(""), 0);
	empty_buffer_scalar =GARROW_SCALAR(garrow_string_scalar_new(empty_buffer));
	val_datum = GARROW_DATUM(garrow_scalar_datum_new(empty_buffer_scalar));
	li_expr = GARROW_EXPRESSION(garrow_literal_expression_new(val_datum));
	arguments = garrow_list_append_ptr(arguments, li_expr);
	text_join_expr = GARROW_EXPRESSION(garrow_call_expression_new("binary_join_element_wise", arguments, NULL));
	garrow_list_free_ptr(&arguments);
	return garrow_move_ptr(text_join_expr);
}

static GArrowExpression *
build_is_in(GArrowExpression *expr, GArrowDatum *value_set, gboolean skip_nulls)
{
	g_autoptr(GArrowSetLookupOptions) options = NULL;
	g_autoptr(GArrowExpression) is_in_expr = NULL;
	g_autoptr(GArrowExpression) list_expr = garrow_copy_ptr(expr);
	GList *arguments = NULL;
	options = garrow_set_lookup_options_new(value_set, skip_nulls);
	arguments = garrow_list_append_ptr(arguments, list_expr);
	is_in_expr = GARROW_EXPRESSION(garrow_call_expression_new("is_in", arguments, GARROW_FUNCTION_OPTIONS(options)));
	garrow_list_free_ptr(&arguments);
	return garrow_move_ptr(is_in_expr);
}

static inline GArrowExpression *
func_arg_to_expression(Expr *fle, PlanBuildContext *pcontext, const char* funcname)
{
	g_autoptr(GArrowExpression) expr = expr_to_arrow_expression(fle, pcontext);
	GList		*arguments = NULL;

	if (expr)
	{
		arguments = garrow_list_append_ptr(arguments, expr);
		expr = GARROW_EXPRESSION(garrow_call_expression_new(funcname, arguments, NULL));
		garrow_list_free_ptr(&arguments);
		return garrow_move_ptr(expr);
	}
	else
	{
		elog(ERROR, "Expression to arrow error.");
		return NULL;
	}
}

static GArrowExpression *
get_function_expression(Oid funcOid, PlanBuildContext *pcontext, List* args)
{
	const FuncTable *fmgr = get_arrow_fmgr(funcOid);
	if (!fmgr)
	{
		elog(ERROR, "get_function_expression unrecognized id: %d", funcOid);
	}
	return fmgr->builFunc(args, (void*)pcontext, fmgr->arrowFuncName);
}

GArrowExpression *
build_cast_int4_expression_allow_truncate(List *args, PlanBuildContext *pcontext, const char *name)
{
	g_autoptr(GArrowDataType) to_type = GARROW_DATA_TYPE(garrow_int32_data_type_new());
	return build_cast_expression(args, pcontext, to_type, true);
}

GArrowExpression *
build_cast_int4_expression(List *args, PlanBuildContext *pcontext, const char *name)
{
	g_autoptr(GArrowDataType) to_type = GARROW_DATA_TYPE(garrow_int32_data_type_new());
	return build_cast_expression(args, pcontext, to_type, false);
}

GArrowExpression *
build_cast_int8_expression(List *args, PlanBuildContext *pcontext, const char *name)
{
	g_autoptr(GArrowDataType) to_type = GARROW_DATA_TYPE(garrow_int64_data_type_new());
	return build_cast_expression(args, pcontext, to_type, false);
}

GArrowExpression *
build_cast_float4_expression(List *args, PlanBuildContext *pcontext, const char *name)
{
	g_autoptr(GArrowDataType) to_type = GARROW_DATA_TYPE(garrow_float_data_type_new());
	return build_cast_expression(args, pcontext, to_type, false);
}

GArrowExpression *
build_cast_float8_expression(List *args, PlanBuildContext *pcontext, const char *name)
{
	g_autoptr(GArrowDataType) to_type = GARROW_DATA_TYPE(garrow_double_data_type_new());
	return build_cast_expression(args, pcontext, to_type, false);
}

GArrowExpression *
build_cast_numeric_expression(List *args, PlanBuildContext *pcontext, const char *name)
{
	g_autoptr(GArrowDataType) to_type = GARROW_DATA_TYPE(garrow_numeric128_data_type_new());
	return build_cast_expression(args, pcontext, to_type, false);
}

GArrowExpression *
build_cast_text_expression(List *args, PlanBuildContext *pcontext, const char *name)
{
	g_autoptr(GArrowDataType) to_type = GARROW_DATA_TYPE(garrow_string_data_type_new());
	return build_cast_expression(args, pcontext, to_type, false);
}

static GArrowExpression *
build_cast_expression(List* args, PlanBuildContext *pcontext, GArrowDataType *to_type, bool allow_truncate)
{
	g_autoptr(GArrowExpression) expr = NULL;
	g_autoptr(GList) cast_args = NULL;
	g_autoptr(GArrowExpression) cast_to_expr = NULL;
	g_autoptr(GArrowCastOptions) cast_options = NULL;

	Assert(list_length(args) == 1);
	expr = expr_to_arrow_expression(linitial(args), pcontext);
	cast_args = garrow_list_append_ptr(cast_args, expr);
	cast_options = GARROW_CAST_OPTIONS(g_object_new(GARROW_TYPE_CAST_OPTIONS, "to-data-type", to_type, NULL));
	if (allow_truncate)
		garrow_set_cast_options(cast_options, GARROW_PROP_ALLOW_FLOAT_TRUNCATE, true);
	cast_to_expr = GARROW_EXPRESSION(garrow_call_expression_new("cast", cast_args, GARROW_FUNCTION_OPTIONS(cast_options)));
	garrow_list_free_ptr(&cast_args);
	return garrow_move_ptr(cast_to_expr);
}

/*
 * Convert ScalarArrayOpExpr to GArrowExpression
 *
 * Fixme: Fully support of ScalarArrayOpExpr, only "IN" now.
 */
static GArrowExpression *
scalararray_to_expression(ScalarArrayOpExpr *arrayexpr, PlanBuildContext *pcontext)
{
	Oid			elemtype;
	Const *const_expr;
	Expr *second_expr;
	g_autoptr(GArrowArray) array = NULL;
	g_autoptr(GArrowArrayDatum) array_datum = NULL;
	g_autoptr(GArrowExpression) expr = NULL;
	g_autoptr(GArrowScalar) val_scalar = NULL;
	g_autoptr(GArrowDatum) val_datum = NULL;
	g_autoptr(GArrowDatum) val_array_datum = NULL;

	second_expr = (Expr *) lsecond(arrayexpr->args);
	if (nodeTag(second_expr) != T_Const)
	{
		elog(ERROR, "unrecognized node type: %d", (int) nodeTag(second_expr));
	}
	/* FIXME: Only support equal operator */
	Assert(!strcmp(get_opname(arrayexpr->opno), "="));
	const_expr = (Const *) second_expr;
	/* first GArrowExpression as is_in argument */
	expr = expr_to_arrow_expression(linitial(arrayexpr->args), pcontext);
	Assert(expr);

	elemtype = get_element_type(const_expr->consttype);
	if (elemtype == InvalidOid)
	{
		elog(ERROR, "is_in right is not an array.");
	}

	val_scalar = ArrowScalarNew(PGTypeToArrowID(const_expr->consttype),
								const_expr->constvalue, const_expr->constisnull, const_expr->consttype, const_expr->consttypmod);

	array = garrow_base_list_scalar_get_value(GARROW_BASE_LIST_SCALAR(val_scalar));
	array_datum = garrow_array_datum_new(array);
	/* is_in need skip null follow pg(in null is false) */
	val_array_datum = GARROW_DATUM(array_datum);
	return build_is_in(expr, garrow_move_ptr(val_array_datum), true);
}

static GArrowExpression *
build_not_expression(GArrowExpression *expr)
{
	g_autoptr(GArrowExpression) not_expr = NULL;
	g_autoptr(GArrowExpression) list_expr = garrow_copy_ptr(expr);
	GList *arguments = NULL;
	arguments = garrow_list_append_ptr(arguments, list_expr);
	not_expr = GARROW_EXPRESSION(garrow_call_expression_new("invert", arguments, NULL));
	garrow_list_free_ptr(&arguments);
	return garrow_move_ptr(not_expr);
}

static GArrowExpression *
build_is_not_null(GArrowExpression *expr)
{

	GList *arguments = NULL;
	g_autoptr(GArrowExpression) is_not_null_expr = NULL;
	g_autoptr(GArrowExpression) list_expr = garrow_copy_ptr(expr);
	arguments = garrow_list_append_ptr(arguments, list_expr);
	is_not_null_expr = GARROW_EXPRESSION(garrow_call_expression_new("is_valid", arguments, NULL));
	garrow_list_free_ptr(&arguments);
	return garrow_move_ptr(is_not_null_expr);
}

// NaN values can also be considered null by setting NullOptions::nan_is_null.
static GArrowExpression *
build_is_null(GArrowExpression *expr, gboolean nan_is_null)
{
	GList *arguments = NULL;
	g_autoptr(GArrowNullOptions) options = NULL;
	g_autoptr(GArrowExpression) is_null_expr = NULL;
	g_autoptr(GArrowExpression) list_expr = garrow_copy_ptr(expr);
	arguments = garrow_list_append_ptr(arguments, list_expr);
	options = garrow_null_options_new(nan_is_null);
	is_null_expr = GARROW_EXPRESSION(garrow_call_expression_new("is_null", arguments, GARROW_FUNCTION_OPTIONS(options)));
	garrow_list_free_ptr(&arguments);
	return garrow_move_ptr(is_null_expr);
}

/*
 * Convert NullTest to GArrowExpression
 */
static GArrowExpression *
nulltest_to_expression(NullTest *node, PlanBuildContext *pcontext)
{
	g_autoptr(GArrowExpression) expr = NULL;
	expr = expr_to_arrow_expression(node->arg, pcontext);
	if (!expr)
	{
		return NULL;
	}
	if (node->nulltesttype == IS_NULL)
	{
		return build_is_null(expr, true);
	}
	else
	{
		return build_is_not_null(expr);
	}
}

/*
 * Convert CaseExpr to GArrowExpression
 * format is:
 *  call("case_when", {
 * 		call("make_struct",
 * 			 {call("greater", {field_ref("a"), literal(1)})},
 * 			 MakeStructOptions({"case_0"})),
 * 		literal(1),
 * 		literal(2),
 *	});
 */
static GArrowExpression *
caseexpr_to_expression(CaseExpr* node, PlanBuildContext *pcontext)
{
	ListCell   *l;
	GList *when_arguments = NULL;
	GList *expr_arguments = NULL;
	GList *and_arguments = NULL;
	GList *not_arguments = NULL;
	g_autoptr(GArrowExpression) struct_expr;
	g_autoptr(GArrowExpression) default_expr;
	g_autoptr(GArrowExpression) expr;
	g_autoptr(GArrowExpression) and_condition_expr = NULL;
	g_autoptr(GArrowMakeStructOptions) options;
	int i = 0;
	gchar **fields;
	fields = palloc(list_length(node->args) * (sizeof(gchar *)));
	pcontext->case_test_expr = node->arg;
	pcontext->is_case_when = true;
	pcontext->whenexpr = NULL;
	pcontext->not_and_whenexpr = NULL;
	pcontext->case_when_type = exprType((Node *) node);
	foreach (l, node->args)
	{
		CaseWhen *when;
		g_autoptr(GArrowExpression) whenexpr;
		g_autoptr(GArrowExpression) whenexpr_copy;
		g_autoptr(GArrowExpression) resexpr;
		when = lfirst_node(CaseWhen, l);
		whenexpr = expr_to_arrow_expression(when->expr, pcontext);
		pcontext->whenexpr = expr_to_arrow_expression(when->expr, pcontext);
		whenexpr_copy = expr_to_arrow_expression(when->expr, pcontext);
		if (list_length(node->args) >= 2)
		{
			and_arguments = garrow_list_append_ptr(and_arguments, whenexpr_copy);
		}
		else
		{
			and_condition_expr = garrow_move_ptr(whenexpr_copy);
		}
		resexpr = expr_to_arrow_expression(when->result, pcontext);
		when_arguments = garrow_list_append_ptr(when_arguments, whenexpr);
		expr_arguments = garrow_list_append_ptr(expr_arguments, resexpr);
		fields[i] = palloc(sizeof(int) + 6);
		snprintf(fields[i], sizeof(int) + 6, "case_%d", i);
		i++;
	}

	if (list_length(node->args) >= 2)
		and_condition_expr = GARROW_EXPRESSION(garrow_call_expression_new("and_kleene", and_arguments, NULL));

	not_arguments = garrow_list_append_ptr(not_arguments, and_condition_expr);
	pcontext->not_and_whenexpr = GARROW_EXPRESSION(garrow_call_expression_new("invert", not_arguments, NULL));
	options = garrow_make_struct_options_new((const gchar **) fields, i);
	struct_expr = GARROW_EXPRESSION(garrow_call_expression_new(
			"make_struct", when_arguments, GARROW_FUNCTION_OPTIONS(options)));
	expr_arguments = garrow_list_prepend_ptr(expr_arguments, struct_expr);
	default_expr = expr_to_arrow_expression(node->defresult, pcontext);
	expr_arguments = garrow_list_append_ptr(expr_arguments, default_expr);
	expr = GARROW_EXPRESSION(garrow_call_expression_new("case_when", expr_arguments, NULL));
	pfree(fields);
	garrow_list_free_ptr(&when_arguments);
	garrow_list_free_ptr(&expr_arguments);
	if (and_arguments) 
		garrow_list_free_ptr(&and_arguments);
	garrow_list_free_ptr(&not_arguments);
	ARROW_FREE(GArrowExpression, &pcontext->not_and_whenexpr);
	ARROW_FREE(GArrowExpression, &pcontext->whenexpr);
	pcontext->is_case_when = false;
	pcontext->not_and_whenexpr = NULL;
	pcontext->whenexpr = NULL;
	return garrow_move_ptr(expr);
}

static GArrowExpression *
build_divide_casewhen(Expr *expr, PlanBuildContext *pcontext)
{
	GList *when_arguments = NULL;
	GList *expr_arguments = NULL;
	g_autoptr(GArrowExpression) casewhen_expr;
	g_autoptr(GArrowExpression) struct_expr;
	g_autoptr(GArrowExpression) default_expr;
	g_autoptr(GArrowMakeStructOptions) options;
	gchar **fields;
	fields = palloc((sizeof(gchar *)));
	g_autoptr(GArrowExpression) resexpr;
	resexpr = expr_to_arrow_expression(expr, pcontext);
	if (pcontext->not_and_whenexpr)
	{
		when_arguments = garrow_list_append_ptr(when_arguments, pcontext->not_and_whenexpr);
	}
	else
	{
		when_arguments = garrow_list_append_ptr(when_arguments, pcontext->whenexpr);
	}
	expr_arguments = garrow_list_append_ptr(expr_arguments, resexpr);
	fields[0] = palloc(sizeof(int) + 6);
	snprintf(fields[0], sizeof(int) + 6, "case_%d", 0);
	options = garrow_make_struct_options_new((const gchar **) fields, 1);
	struct_expr = GARROW_EXPRESSION(garrow_call_expression_new(
			"make_struct", when_arguments, GARROW_FUNCTION_OPTIONS(options)));
	expr_arguments = garrow_list_prepend_ptr(expr_arguments, struct_expr);
	default_expr = build_literal_expression(0, true, pcontext->case_when_type, -1);
	expr_arguments = garrow_list_append_ptr(expr_arguments, default_expr);
	casewhen_expr = GARROW_EXPRESSION(garrow_call_expression_new("case_when", expr_arguments, NULL));
	pfree(fields);
	garrow_list_free_ptr(&when_arguments);
	garrow_list_free_ptr(&expr_arguments);
	return garrow_move_ptr(casewhen_expr);
}


static GArrowExpression*
build_divide_casewhen_opexpr(List *args, PlanBuildContext *pcontext)
{
	Expr *divisor = (Expr *) linitial(args);
	Expr *dividend = (Expr *) lsecond(args);
	g_autoptr(GArrowExpression) expr;
	g_autoptr(GArrowExpression) divisor_expr;
	g_autoptr(GArrowExpression) case_when_expr;
	GList *expr_arguments = NULL;
	divisor_expr = expr_to_arrow_expression(divisor, pcontext);
	case_when_expr = build_divide_casewhen(dividend, pcontext);
	expr_arguments = garrow_list_append_ptr(expr_arguments, divisor_expr);
	expr_arguments = garrow_list_append_ptr(expr_arguments, case_when_expr);
	expr = GARROW_EXPRESSION(garrow_call_expression_new("divide", expr_arguments, NULL));
	garrow_list_free_ptr(&expr_arguments);
	return garrow_move_ptr(expr);
}

GArrowExpression *
extract_expression(List *args, PlanBuildContext *pcontext, const char* name)
{
	ListCell   *lc;
	int type, val;
	char	   *lowunits = NULL;
	type = UNKNOWN_FIELD;
	foreach(lc, args)
	{
		Expr	   *arg = (Expr *) lfirst(lc);

		if (IsA(arg, Const))
		{
			Const	   *con = (Const *) arg;
			text	   *units = DatumGetTextPP(con->constvalue);

			lowunits = downcase_truncate_identifier(VARDATA_ANY(units),
													VARSIZE_ANY_EXHDR(units),
														false);
			type = DecodeUnits(0, lowunits, &val);
		}
		else
		{
			if (type == UNITS)
			{
				g_autoptr(GArrowExpression) extract_expr = NULL;
				g_autoptr(GArrowCastOptions)  cast_options = NULL;
				g_autoptr(GArrowDataType) to_type = NULL;
				g_autoptr(GList) cast_args = NULL;
				g_autoptr(GArrowExpression) cast_decimal_expr = NULL;
				switch (val)
				{
					case DTK_MICROSEC:
						extract_expr = func_arg_to_expression(arg, pcontext, "microsecond");
						break;
					case DTK_MILLISEC:
						extract_expr = func_arg_to_expression(arg, pcontext, "millisecond");
						break;
					case DTK_SECOND:
						extract_expr = func_arg_to_expression(arg, pcontext, "second");
						break;
					case DTK_MINUTE:
						extract_expr = func_arg_to_expression(arg, pcontext, "minute");
						break;
					case DTK_HOUR:
						extract_expr = func_arg_to_expression(arg, pcontext, "hour");
						break;
					case DTK_DAY:
						extract_expr = func_arg_to_expression(arg, pcontext, "day");
						break;
					case DTK_MONTH:
						extract_expr = func_arg_to_expression(arg, pcontext, "month");
						break;
					case DTK_YEAR:
						extract_expr = func_arg_to_expression(arg, pcontext, "year");
						break;
					case DTK_DECADE:
						extract_expr = func_arg_to_expression(arg, pcontext, "decade");
						break;
					case DTK_CENTURY:
						extract_expr = func_arg_to_expression(arg, pcontext, "century");
						break;
					case DTK_MILLENNIUM:
						extract_expr = func_arg_to_expression(arg, pcontext, "millennium");
						break;
					default:
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									errmsg("timestamp units  not supported")));
				}
				// add cast expr(integer to decimal) to align pg extract expr output type
				to_type = GARROW_DATA_TYPE(garrow_numeric128_data_type_new());
				cast_args = garrow_list_append_ptr(cast_args, extract_expr);
				cast_options = GARROW_CAST_OPTIONS(g_object_new(GARROW_TYPE_CAST_OPTIONS, "to-data-type", to_type , NULL));
				cast_decimal_expr = GARROW_EXPRESSION(garrow_call_expression_new("cast", cast_args, GARROW_FUNCTION_OPTIONS(cast_options)));
				return garrow_move_ptr(cast_decimal_expr);
			}
			else
			{
				elog(ERROR, "Failed to call extract type (%d) ", type);
				return NULL;
			}

		}
	}
	elog(ERROR, "Failed to call extract_expression");
	return NULL;
}

GArrowExpression*
build_divide_expr(List *args, PlanBuildContext *pcontext, const char* funcname)
{
	if (pcontext->is_case_when)
	{
		return build_divide_casewhen_opexpr(args, pcontext);
	}
	return func_args_to_expression(args, pcontext, funcname);
}

GArrowExpression *
utf8_slice_codeunits_expression(List *args, PlanBuildContext *pcontext, const char* name)
{
	Expr *inner_expr = NULL;
	Expr *start_expr = NULL;
	Expr *len_expr = NULL;
	Const *const_expr_start = NULL;
	Const *const_expr_end = NULL;
	int start = 0, len = 0, l1 = 0, end = INT_MAX;

	g_autoptr(GArrowExpression) expr = NULL;
	g_autoptr(GArrowExpression) slice_expression = NULL;
	g_autoptr(GArrowSliceOptions) options = NULL;
	GList *arguments = NULL;
	inner_expr = (Expr *) linitial(args);
	expr = expr_to_arrow_expression(inner_expr, pcontext);
	Assert(expr);
	arguments = garrow_list_append_ptr(arguments, expr);
	start_expr = (Expr *) lsecond(args);
	/* FIXME: will fallback in the planner. */
	if (nodeTag(start_expr) != T_Const)
	{
		elog(ERROR, "unrecognized node type: %d", (int) nodeTag(start_expr));
	}
	const_expr_start =  (Const *) start_expr;
	if (const_expr_start->constbyval)
	{
		start = const_expr_start->constvalue;
		if (start < 1)
		{
			l1 = start - 1;
			start = 1;
		}
		start--;
	}
	/* regex plan fallback */
	if (list_length(args) == 3) {
		len_expr = (Expr *) lthird(args);
		if (nodeTag(len_expr) != T_Const)
		{
			elog(ERROR, "unrecognized node type: %d", (int) nodeTag(len_expr));
		}
		const_expr_end =  (Const *) len_expr;
		len = const_expr_end->constvalue;
		if (len < 0)
		{
			elog(ERROR, "negative substring length not allowed");
		}
		end = start + len + l1;
	}
	options = garrow_slice_options_new(start, end, 1);
	slice_expression = GARROW_EXPRESSION(garrow_call_expression_new(
		"utf8_slice_codeunits", arguments, GARROW_FUNCTION_OPTIONS(options)));
	garrow_list_free_ptr(&arguments);
	return garrow_move_ptr(slice_expression);
}

GArrowExpression *
replace_substring_regex_expression(List *args, PlanBuildContext *pcontext, const char *name)
{
	Expr *first_expr = NULL;
	Expr *second_expr = NULL;
	Expr *third_expr = NULL;
	Const *const_expr_pattern = NULL;
	Const *const_expr_replace = NULL;
	char *str_pattern = NULL;
	char *str_replace = NULL;
	struct varlena *s = NULL;

	g_autoptr(GArrowExpression) expr = NULL;
	g_autoptr(GArrowExpression) replace_expression = NULL;
	g_autoptr(GArrowReplaceSubstringOptions) options = NULL;
	GList *arguments = NULL;

	first_expr = (Expr *) linitial(args);
	if (nodeTag(first_expr) != T_Var)
	{
		elog(ERROR, "unrecognized node type: %d", (int) nodeTag(first_expr));
	}
	expr = expr_to_arrow_expression(first_expr, pcontext);
	Assert(expr);

	second_expr = (Expr *) lsecond(args);
	if (nodeTag(second_expr) != T_Const)
	{
		elog(ERROR, "unrecognized node type: %d", (int) nodeTag(second_expr));
	}
	const_expr_pattern =  (Const *) second_expr;

	third_expr = (Expr *) lthird(args);
	if (nodeTag(third_expr) != T_Const)
	{
		elog(ERROR, "unrecognized node type: %d", (int) nodeTag(third_expr));
	}
	const_expr_replace =  (Const *) third_expr;
	s = (struct varlena *) const_expr_pattern->constvalue;
	str_pattern = text_to_cstring(s);
	s = (struct varlena *) const_expr_replace->constvalue;
	str_replace = text_to_cstring(s);

	options = garrow_replace_substring_options_new(str_pattern, str_replace, -1);
	arguments = garrow_list_append_ptr(arguments, expr);
	replace_expression = GARROW_EXPRESSION(garrow_call_expression_new(
			"replace_substring_regex", arguments, GARROW_FUNCTION_OPTIONS(options)));

	garrow_list_free_ptr(&arguments);
	return garrow_move_ptr(replace_expression);
}

static GArrowExpression *
build_like_expr(List *args, PlanBuildContext *pcontext, bool is_notlike)
{
	g_autoptr(GArrowExpression)  result_expr = NULL;
	gboolean ignore_case = false;
	GList           *arguments = NULL;

	Expr  *first_expr = linitial(args);
	Expr  *second_expr = lsecond(args);
	char *value;

	Expr  *var_epxr = NULL;
	Const *const_expr = NULL;

	// Currently we only support like expressions where one of
	// the arguments is const.
	if (IsA(first_expr, Const))
	{
		const_expr = (Const *)first_expr;
		var_epxr = second_expr;
	}
	else
	{
		const_expr = (Const *)second_expr;
		var_epxr = first_expr;
	}

	value = text_to_cstring(DatumGetTextP(const_expr->constvalue));
	g_autoptr(GArrowMatchSubstringOptions) options =
			garrow_match_substring_options_new(value, ignore_case);
	g_autoptr(GArrowExpression) expr = expr_to_arrow_expression(var_epxr, pcontext);

	if (expr)
	{
		g_autoptr(GArrowExpression) not_expr = NULL;
		arguments = garrow_list_append_ptr(arguments, expr);
		expr = GARROW_EXPRESSION(garrow_call_expression_new(
				"match_like", arguments, GARROW_FUNCTION_OPTIONS(options)));
		garrow_list_free_ptr(&arguments);
		if (is_notlike)
		{
			not_expr = build_not_expression(expr);
			return garrow_move_ptr(not_expr);
		}
		return garrow_move_ptr(expr);
	}
	else
	{
		elog(ERROR, "Expression to arrow error.");
		return NULL;
	}
}

GArrowExpression *
build_like_expression(List *args, PlanBuildContext *pcontext, const char *name)
{
	return build_like_expr(args, pcontext, false);
}

GArrowExpression *
build_not_like_expression(List *args, PlanBuildContext *pcontext, const char *name)
{
	return build_like_expr(args, pcontext, true);
}

GArrowExpression *
replace_expression(List *args, PlanBuildContext *pcontext, const char* name)
{
	Expr *first_expr = NULL;
	Expr *second_expr = NULL;
	Expr *third_expr = NULL;
	Const *const_expr_pattern = NULL;
	Const *const_expr_replace = NULL;
	char *str_pattern = NULL;
	char *str_replace = NULL;
	struct varlena *s = NULL;

	g_autoptr(GArrowExpression) expr = NULL;
	g_autoptr(GArrowExpression) replace_expression = NULL;
	g_autoptr(GArrowReplaceSubstringOptions) options = NULL;
	GList *arguments = NULL;

	first_expr = (Expr *) linitial(args);
	if (!IsA(first_expr, Var))
	{
		elog(ERROR, "unrecognized node type: %d", (int) nodeTag(first_expr));
	}
	expr = expr_to_arrow_expression(first_expr, pcontext);
	Assert(expr);

	second_expr = (Expr *) lsecond(args);
	if (!IsA(second_expr,Const))
	{
		elog(ERROR, "unrecognized node type: %d", (int) nodeTag(second_expr));
	}
	const_expr_pattern =  (Const *) second_expr;

	third_expr = (Expr *) lthird(args);
	if (!IsA(third_expr,Const))
	{
		elog(ERROR, "unrecognized node type: %d", (int) nodeTag(third_expr));
	}
	const_expr_replace =  (Const *) third_expr;
	s = (struct varlena *) const_expr_pattern->constvalue;
	str_pattern = text_to_cstring(s);
	s = (struct varlena *) const_expr_replace->constvalue;
	str_replace = text_to_cstring(s);

	options = garrow_replace_substring_options_new(str_pattern, str_replace, -1);
	arguments = garrow_list_append_ptr(arguments, expr);
	replace_expression = GARROW_EXPRESSION(garrow_call_expression_new("replace_substring", arguments, GARROW_FUNCTION_OPTIONS(options)));

	pfree(str_pattern);
	pfree(str_replace);
	garrow_list_free_ptr(&arguments);
	return garrow_move_ptr(replace_expression);
}

static GArrowExpression *
build_literal_expression(Datum datum, bool isnull, Oid pg_type, int32 typmod)
{
	g_autoptr(GArrowScalar) val_scalar = NULL;
	g_autoptr(GArrowDatum) val_datum = NULL;
	val_scalar = ArrowScalarNew(PGTypeToArrowID(pg_type), datum, isnull, pg_type, typmod);
	val_datum = GARROW_DATUM(garrow_scalar_datum_new(val_scalar));
	return GARROW_EXPRESSION(garrow_literal_expression_new(val_datum));
}


static GArrowExpression *
coalesceexpr_expression(CoalesceExpr *coalexpr, PlanBuildContext *pcontext)
{
	GList *arguments = NULL;
	ListCell *l;
	g_autoptr(GArrowExpression)  result_expr = NULL;
	foreach(l, coalexpr->args)
	{
		Expr  *arg_expr = (Expr *) lfirst(l);
		g_autoptr(GArrowExpression) expr = expr_to_arrow_expression(arg_expr, pcontext);

		if (!expr)
		{
			elog(ERROR, "arg expr to arrow error.");
			return NULL;
		}
		arguments = garrow_list_append_ptr(arguments, expr);
	}
	result_expr = GARROW_EXPRESSION(garrow_call_expression_new(
		"coalesce", arguments, NULL));
	if (!result_expr)
		elog(ERROR, "Failed to new arrow call expression coalesce.");
	garrow_list_free_ptr(&arguments);
	return garrow_move_ptr(result_expr);
}


static GArrowExpression *
expr_to_arrow_expression(Expr *node, PlanBuildContext *pcontext)
{
	g_autoptr(GError) error = NULL;
	g_autoptr(GArrowExpression) expr = NULL;

	if (node == NULL)
		return NULL;

	/* Guard against stack overflow due to overly complex expressions */
	check_stack_depth();

	switch (nodeTag(node))
	{
		case T_Var:
			{
				const char *attname = NULL;
				Var *var = (Var *) node;

				if (var->varattno == SelfItemPointerAttributeNumber)
				{
					attname = GetCtidSchemaName(pcontext->inputschema);
				}
				else if (var->varattno == GpSegmentIdAttributeNumber)
				{
					expr = build_literal_expression(GpIdentity.segindex, false, var->vartype, -1);
					break;
				}
				else if (pcontext->is_nestloopjoin || (pcontext->is_hashjoin && pcontext->is_hashjoin_after_node))
				{
					if (pcontext->inputschema)
					{
						/*
						 * After the hashjoin node, resolve Var against the
						 * join output schema.  OUTER_VAR -> LEFT_PREFIX,
						 * INNER_VAR -> RIGHT_PREFIX (standard mapping).
						 */
						int effective_varno = var->varno;
						attname = GetSchemaNameByVarNo(pcontext->inputschema, var->varattno, effective_varno);
					}
					else
					{
						/*
						 * Before the hashjoin output schema is set,
						 * resolve against per-side proj schemas.
						 * OUTER_VAR -> left_proj, INNER_VAR -> right_proj.
						 */
						Assert(pcontext->left_proj_schema && pcontext->right_proj_schema);
						if (var->varno == OUTER_VAR)
							attname = GetSchemaName(pcontext->left_proj_schema, var->varattno, pcontext->map);
						else
							attname = GetSchemaName(pcontext->right_proj_schema, var->varattno, pcontext->map);
					}
				}
				else if (pcontext->is_append)
				{
					attname = GetSchemaName(pcontext->inputschema, pcontext->append_filed_index + 1, pcontext->map);
				} 
				else
					attname = GetSchemaName(pcontext->inputschema, var->varattno, pcontext->map);
				/*
				 * RIGHT_SEMI / RIGHT_ANTI hash join: Arrow's Mark Join emits
				 * only the build (Arrow right / PG inner) side, so the join
				 * output schema has no LEFT_PREFIX columns.  A surviving
				 * OUTER_VAR in the targetlist can therefore only be a hash key
				 * whose value is, by the equijoin condition, identical to the
				 * matching inner key.  Map it to the corresponding
				 * right_joinqual_N column that the build-side projection
				 * already carries.
				 */
				if (attname == NULL && var->varno == OUTER_VAR &&
					pcontext->is_hashjoin && pcontext->is_hashjoin_after_node &&
					pcontext->inputschema != NULL &&
					IsA(pcontext->planstate, HashJoinState))
				{
					HashJoinState *hjs = (HashJoinState *) pcontext->planstate;

					if (hjs->js.jointype == JOIN_RIGHT_SEMI ||
						hjs->js.jointype == JOIN_RIGHT_ANTI)
					{
						ListCell   *lc;
						int			idx = 0;

						foreach(lc, hjs->hj_OuterHashKeys)
						{
							ExprState  *keystate = (ExprState *) lfirst(lc);

							if (keystate != NULL && IsA(keystate->expr, Var) &&
								((Var *) keystate->expr)->varattno == var->varattno)
							{
								/*
								 * Build-side projection names the i-th hash key
								 * "right_joinqual_i" (see build_join_project_
								 * options).  The equijoin guarantees this column
								 * equals the requested probe-side key, so it is
								 * the correct substitute in the build-only output.
								 */
								char	   *jq = psprintf("%sjoinqual_%d",
													  RIGHT_PREFIX, idx);
								g_autoptr(GArrowField) f =
									garrow_schema_get_field_by_name(pcontext->inputschema, jq);

								if (f != NULL)
									attname = garrow_field_get_name(f);
								pfree(jq);
								break;
							}
							idx++;
						}
					}
				}
				if (attname == NULL)
					elog(ERROR, "failed to resolve Var (varno=%d varattno=%d) "
						 "to an arrow field in the join output schema",
						 var->varno, var->varattno);
				expr = GARROW_EXPRESSION(garrow_field_expression_new(attname, &error));
				if (error)
					elog(ERROR, "convert PG Var(name: %s) to arrow expression failed: %s",
							attname, error->message);
				break;
			}
		case T_Const:
			{
				Const * const_expr = (Const *) node;
				expr = build_literal_expression(const_expr->constvalue,
												const_expr->constisnull,
												const_expr->consttype,
												const_expr->consttypmod);
				break;
			}
		case T_OpExpr:
			{
				OpExpr	  *opexpr = (OpExpr *)node;
				return get_function_expression(opexpr->opfuncid, pcontext, opexpr->args);
				break;
			}
		case T_FuncExpr:
			{
				FuncExpr	  *funcexpr = (FuncExpr *)node;
				return get_function_expression(funcexpr->funcid, pcontext, funcexpr->args);
				break;
			}
		case T_WindowFunc:
			{
				WindowFunc *wfunc = (WindowFunc *) node;
				VecAggInfo *agginfo = NULL;
				ListCell *l;

				/* check whether current window agg is duplicated */
				foreach(l, pcontext->agginfos)
				{
					VecAggInfo *cinfo = (VecAggInfo *) lfirst(l);

					if (equal(wfunc, cinfo->wfunc) &&
						!contain_volatile_functions((Node *) wfunc))
					{
						agginfo = cinfo;
					}
				}

				if (!agginfo)
				{
					agginfo = new_winagg_info(wfunc, pcontext);
					pcontext->agginfos = lappend(pcontext->agginfos, agginfo);
				}

				/* winfunc in expression is a normal field for arrow, use out name */
				expr = GARROW_EXPRESSION(garrow_field_expression_new(
						agginfo->outname, &error));
				if (error)
					elog(ERROR, "convert PG winfunc(name: %s) to arrow expression failed: %s",
							agginfo->outname, error->message);

				break;
			}
		case T_Aggref:
			{
				Aggref *aggref = (Aggref *) node;
				VecAggInfo *agginfo = NULL;
				ListCell *l;

				/* check whether current agg is duplicated */
				foreach(l, pcontext->agginfos)
				{
					VecAggInfo *cinfo = (VecAggInfo *) lfirst(l);
					if (cinfo->aggref->aggno == aggref->aggno)
					{
						agginfo = cinfo;
					}
				}

				if (!agginfo)
				{
					agginfo = new_agg_info(aggref,
										   aggref->aggno,
										   pcontext);
					pcontext->agginfos = lappend(pcontext->agginfos, agginfo);
				}
				/* aggref in expression is a normal field for arrow, use out name*/
				expr = GARROW_EXPRESSION(garrow_field_expression_new(
						agginfo->outname, &error));
				if (error)
					elog(ERROR, "convert PG Aggref(name: %s) to arrow expression failed: %s",
							agginfo->outname, error->message);
				break;
			}
		case T_List:
			break;
		case T_RelabelType:
			{
				RelabelType	  *opexpr = (RelabelType *)node;
				return expr_to_arrow_expression(opexpr->arg, pcontext);
			}
		case T_BoolExpr:
			{
				BoolExpr *boolexpr = (BoolExpr *)node;
				const char *funcname;
				switch (boolexpr->boolop)
				{
					case AND_EXPR:
						funcname = "and_kleene"; /* 2 expression */
						break;
					case OR_EXPR:
						funcname = "or_kleene"; /* 2 expression */
						break;
					case NOT_EXPR:
						funcname = "invert"; /* 1 expression */
						break;
					default:
						elog(ERROR, "unrecognized boolop: %d",(int) boolexpr->boolop);
				}

				expr = func_args_to_expression(boolexpr->args, pcontext, funcname);

				break;
			}
		case T_ScalarArrayOpExpr:
			{
				ScalarArrayOpExpr *arrayexpr = (ScalarArrayOpExpr *) node;
				/* FIXME: todo: implement `not in` */
				expr = scalararray_to_expression(arrayexpr, pcontext);
				break;
			}
		case T_NullTest:
			{
				NullTest   *nt = (NullTest *) node;
				expr = nulltest_to_expression(nt, pcontext);
				break;
			}
		case T_CaseExpr:
			{
				CaseExpr *caseexpr = (CaseExpr *) node;
				expr = caseexpr_to_expression(caseexpr, pcontext);
				break;
			}
		case T_CaseTestExpr:
			{
				Assert(pcontext->case_test_expr);
				expr = expr_to_arrow_expression(pcontext->case_test_expr, pcontext);
				break;
			}
		case T_CoalesceExpr:
			{
				CoalesceExpr *coalexpr = (CoalesceExpr *) node;
				expr = coalesceexpr_expression(coalexpr, pcontext);
				break;
			}
		case T_DistinctExpr:
			{
				DistinctExpr *distinctexpr = (DistinctExpr* ) node;
				expr = build_is_distinct_expression(distinctexpr, pcontext);
				break;
			}
		case T_Param:
			{
				Param* param = (Param *) node;
				ExprContext* exprcontext = pcontext->planstate->ps_ExprContext;
				ParamExecData *prm = &(exprcontext->ecxt_param_exec_vals[param->paramid]);
				if (param->paramtype == NUMERICOID && prm->value == (Datum) 0)
					prm->value = NumericGetDatum(int64_to_numeric(0));
				expr = build_literal_expression(prm->value,
												prm->isnull,
												param->paramtype,
												param->paramtypmod);
				break;
			}
		case T_NullIfExpr:
			{
				NullIfExpr *nullifexpr = (NullIfExpr *) node;
				expr = build_null_if_expression(nullifexpr, pcontext);
				break;
			}

		
		default:
			elog(ERROR, "unrecognized node type: %d", (int)nodeTag(node));
	}
	return garrow_move_ptr(expr);
}

/* generate unique aggname and VecAggInfo*/
static inline VecAggInfo *
new_agg_info(Aggref *aggref, int aggno, PlanBuildContext *pcontext)
{
	VecAggInfo *agginfo;
	const ArrowAggFmgr *fmgr;
	const AggFuncTable *table;

	agginfo = palloc(sizeof(VecAggInfo));
	agginfo->aggref = aggref;

	fmgr = get_arrow_agg_fmgr(aggref->aggfnoid);
	if (!fmgr)
		elog(ERROR, "Can not find Arrow aggregate fmgr, aggfnoid: %d",
				aggref->aggfnoid);
	if (aggref->aggsplit == AGGSPLIT_FINAL_DESERIAL)
		table = get_arrow_agg_functable(fmgr->finalfn);
	else if (aggref->aggsplit == AGGSPLIT_INITIAL_SERIAL)
		table = get_arrow_agg_functable(fmgr->transfn);
	else if (aggref->aggsplit == AGGSPLIT_SIMPLE)
		table = get_arrow_agg_functable(fmgr->simplefn);
	else
		elog(ERROR, "doesn't support aggsplit: %d", aggref->aggsplit);

	agginfo->aname = get_agg_func_name(aggref, table, pcontext);
	agginfo->options = table->getOption(list_length(aggref->args));

	snprintf(agginfo->inname, sizeof(agginfo->inname),
			"_inagg_%d", aggno);
	snprintf(agginfo->outname, sizeof(agginfo->outname),
			"_outagg_%d", aggno);
	return agginfo;
}

/* generate unique aggname and VecAggInfo */
static inline VecAggInfo *
new_winagg_info(WindowFunc *wfunc, PlanBuildContext *pcontext)
{
	VecAggInfo *agginfo;
	const ArrowAggFmgr *fmgr;
	const AggFuncTable *table;

	agginfo = palloc(sizeof(VecAggInfo));
	agginfo->wfunc = wfunc;

	fmgr = get_arrow_agg_fmgr(wfunc->winfnoid);
	if (!fmgr)
		elog(ERROR, "Can not find Arrow aggregate fmgr, aggfnoid: %d", wfunc->winfnoid);

	table = get_arrow_agg_functable(fmgr->simplefn);
	agginfo->aname = table->funcName;
	agginfo->options = table->getOption(list_length(wfunc->args));

	int winaggno = list_length(pcontext->agginfos);
	snprintf(agginfo->inname, sizeof(agginfo->inname), "_inwinagg_%d", winaggno);
	snprintf(agginfo->outname, sizeof(agginfo->outname), "_outwinagg_%d", winaggno);

	return agginfo;
}

static void
BuildMaterializePlan(PlanBuildContext *pcontext, VecExecuteState *estate)
{
	g_autoptr(GArrowExecuteNode) source_node = NULL;
	g_autoptr(GArrowMaterializeNodeOptions)	materialize_node_options = NULL;
	g_autoptr(GArrowExecuteNode) materialize_exec_node = NULL;
	g_autoptr(GError) error = NULL;
	GArrowStoreType type;

	type = to_arrow_storetype(pcontext->store_type);

	source_node = BuildSource(pcontext);

	materialize_node_options =
			garrow_materialize_node_options_new(pcontext->store_file, type, pcontext->chunk_size, pcontext->cdb_strict, &error);

	if (error)
		elog(ERROR, "build materialize node option fail caused by %s", error->message);

	materialize_exec_node =
			garrow_execute_plan_build_materialize_node(pcontext->plan,
										source_node,
										materialize_node_options,
										&error);

	if (error)
		elog(ERROR, "build materialize node fail caused by %s", error->message);

	BuildSink(materialize_exec_node, estate, pcontext);

	return;
}

static void
BuildShareScanPlan(PlanBuildContext *pcontext, VecExecuteState *estate)
{
	g_autoptr(GArrowExecuteNode) source_node = NULL;
	g_autoptr(GArrowShareInputScanNodeOptions)	share_node_options = NULL;
	g_autoptr(GArrowSourceShareNodeOptions)	share_source_node_options = NULL;
	g_autoptr(GArrowExecuteNode) share_exec_node = NULL;
	g_autoptr(GError) error = NULL;

	if (pcontext->is_producer)
	{
		source_node = BuildSource(pcontext);
		share_node_options = garrow_shareinputscan_node_options_new(pcontext->gp_session_id,
													pcontext->gp_command_count,
													pcontext->share_id,
													pcontext->num_slices,
													pcontext->is_cross_slice,
													pcontext->ready,
													&error);
		if (error)
			elog(ERROR, "build share producter node option fail caused by %s", error->message);

		share_exec_node = garrow_execute_plan_build_shareinputscan_node(pcontext->plan,
												source_node,
												share_node_options,
												&error);
		if (error)
			elog(ERROR, "build share producter node fail caused by %s", error->message);

		BuildSink(share_exec_node, estate, pcontext);
	}
	else
	{
		share_source_node_options = garrow_sourceshare_node_options_new(pcontext->inputschema,
													pcontext->gp_session_id,
													pcontext->gp_command_count,
													pcontext->share_id,
													pcontext->num_slices,
													pcontext->is_cross_slice,
													pcontext->ready,
													&error);
		if (error)
			elog(ERROR, "build share consumer node option fail caused by %s", error->message);

		share_exec_node = garrow_execute_plan_build_sourceshare_node(pcontext->plan,
												share_source_node_options,
												&error);
		if (error)
			elog(ERROR, "build share consumer node fail caused by %s", error->message);

		BuildSink(share_exec_node, estate, pcontext);
	}
}

static GArrowExecuteNode *
BuildAppendPlan(PlanBuildContext *pcontext, VecExecuteState *estate)
{
	g_autoptr(GArrowExecuteNode) append_node = NULL;
	g_autoptr(GArrowProjectNodeOptions) project_options = NULL;
	g_autoptr(GArrowExecuteNode) project_node = NULL;
	g_autoptr(GError) error = NULL;
	g_autoptr(GArrowAppendNodeOptions) append_option = NULL;
	g_autoptr(GArrowExecuteNode) source = NULL;
	GList* nodes = NULL;
	Append *append = NULL;
	AppendState *appendstate = NULL;
	PlanState **appendplanstates = NULL;
	List *targetList = NULL;
	GArrowSchema *source_project_schema = NULL;
	int i;
	int nplans;

	appendstate = (AppendState*)pcontext->planstate;
	appendplanstates = appendstate->appendplans;
	append = (Append*)(appendstate->ps.plan);
	
	nplans = list_length(append->appendplans);
	
	for (i = 0; i < nplans; ++i)
	{
		pcontext->inputschema = GetSchemaFromSlot(appendplanstates[i]->ps_ResultTupleSlot);
		pcontext->sub_slice = i;
		source = BuildSource(pcontext);

		targetList = appendstate->ps.plan->targetlist;
		/*
		 * The purpose of projecting multiple input nodes with multiple appends
		 * here is to modify the schema of their respective inputs to ensure
		 * that the names of the schema can match the same.
		 * Continue to do the project on the source node. 
		 * The filter of the source has been processed. 
		 * The project here is only for the purpose of projecting and modifying the schema, and does not need to be filtered.
		 */
		project_node = BuildProject(targetList, NULL, source, pcontext);

		if (!source_project_schema)
			source_project_schema = garrow_execute_node_get_output_schema(project_node);

		nodes = garrow_list_append_ptr(nodes, project_node);
	}
	/*
	 * The input schema of the append operator is changed to be consistent with the schema of the source after project.
	 */
	pcontext->inputschema = source_project_schema;
	append_option = garrow_append_node_options_new();
	append_node = garrow_execute_plan_build_append_node(pcontext->plan,
								  	    nodes,
									    append_option,
									    &error);
	if (error)
		elog(ERROR, "Failed to create append node: %s.", error->message);
	garrow_list_free_ptr(&nodes);

	return garrow_move_ptr(append_node);
}

/* ------------------------------------------------------------------------
 *
 * Build Arrow plan execute state by cbdb planstate.
 *
 * Inputs:
 *   'planstate'is the initialized plan state used to build Arrow plan.
 *
 * Outputs:
 *   'estate' is output execute state, which is used by ExecutePlan.
 *
 * ------------------------------------------------------------------------
 */
void
BuildVecPlan(PlanState *planstate, VecExecuteState *estate)
{
	g_autoptr(GError) error = NULL;
	g_autoptr(GArrowExecuteNode) source = NULL;
	List *targetList = planstate->plan->targetlist;
	List *qualList = planstate->plan->qual;
	PlanBuildContext pcontext;
	bool support_parallel = true;

	/* init PlanBuildContext*/
	pcontext.agginfos = NULL;
	pcontext.keys = NULL;
	pcontext.nkey = 0;
	pcontext.planstate = planstate;
	pcontext.reader = NULL;
	pcontext.ishaving = false;
	pcontext.plan = NULL;
	pcontext.map = NULL;
	pcontext.sinktype = Plain;
	pcontext.pipeline = false;

	pcontext.is_hashjoin = false;
	pcontext.is_nestloopjoin = false;
	pcontext.is_hashjoin_after_node = false;
	pcontext.left_proj_schema = NULL;
	pcontext.right_proj_schema = NULL;
	pcontext.left_hashkeys = NULL;
	pcontext.right_hashkeys = NULL;
	pcontext.inputschema = NULL;
	pcontext.is_case_when = false;
	pcontext.is_assertop = false;
	pcontext.whenexpr = NULL;
	pcontext.not_and_whenexpr = NULL;
	pcontext.case_when_type = InvalidOid;
	pcontext.parallel_scan = false;
	pcontext.is_append = false;
	pcontext.append_filed_index = 0;
	pcontext.is_materialize = false;
	pcontext.store_file = NULL;
	pcontext.chunk_size = 0;
	pcontext.store_type = IN_MEMORY;
	pcontext.is_sharescan = false;

	/* set build recipe for different plan node */
	switch(nodeTag(planstate))
	{
		case T_HashJoinState:
		{
			pcontext.is_hashjoin = true;
		}
		break;
		case T_NestLoopState:
		{
			pcontext.is_nestloopjoin = true;
		}
		break;
		case T_AggState:
		{
			Agg *agg = (Agg *)planstate->plan;

			if (!outerPlanState(planstate))
				elog(ERROR, "Agg node can't be leaf in vector plan");
			pcontext.inputschema = GetSchemaFromSlot(
					outerPlanState(planstate)->ps_ResultTupleSlot);
			pcontext.aggstrategy = agg->aggstrategy;
			pcontext.ishaving = true;
		}
		break;
		case T_WindowAggState:
		{
			if (!outerPlanState(planstate))
				elog(ERROR, "WindowAgg node can't be leaf in vector plan");
			pcontext.inputschema = GetSchemaFromSlot(
					outerPlanState(planstate)->ps_ResultTupleSlot);

			if (DEBUG1 >= log_min_messages)
			{
				g_autofree gchar *str = garrow_schema_to_string(pcontext.inputschema);
				elog(DEBUG1, "input schema for Windowagg: %s", str);
			}
		}
		break;
		case T_WindowHashAggState:
		{
			/*
			 * Arrow's ParallelWindowGroupByNode maintains thread-local
			 * sink state and invokes PG callbacks concurrently from its
			 * internal executor.  Running it inside a parallel Arrow
			 * pipeline (pool_threads>0) layers a second source of PG
			 * callback concurrency on top, which races with the window
			 * node's own thread pool and SIGSEGVs (reproducible on Q17-
			 * shaped rewrites with pool_threads=8).  Force this plan to
			 * run single-threaded to avoid the interaction.
			 */
			support_parallel = false;

			if (!outerPlanState(planstate))
				elog(ERROR, "WindowHashAgg node can't be leaf in vector plan");
			pcontext.inputschema = GetSchemaFromSlot(
					outerPlanState(planstate)->ps_ResultTupleSlot);

			if (DEBUG1 >= log_min_messages)
			{
				g_autofree gchar *str = garrow_schema_to_string(pcontext.inputschema);
				elog(DEBUG1, "input schema for WindowHashAgg: %s", str);
			}
		}
		break;
		case T_MaterialState:
		{
			VecMaterialState *vmatstate = (VecMaterialState *)planstate;
			support_parallel = false;
			if (!outerPlanState(planstate))
				elog(ERROR, "Material node can't be leaf in vector plan");
			pcontext.inputschema = GetSchemaFromSlot(
					outerPlanState(planstate)->ps_ResultTupleSlot);
			pcontext.is_materialize = true;
			pcontext.store_file = build_materialize_file_name();
			pcontext.chunk_size = 0;
			pcontext.store_type = IN_SINGLE;
			pcontext.cdb_strict = vmatstate->cdb_strict;
			pcontext.pipeline = false;
			pcontext.sinktype = Plain;
		}
		break;
		case T_ShareInputScanState:
		{
			VecShareInputScanState *vmatstate = (VecShareInputScanState *)planstate;
			pcontext.inputschema = vmatstate->vecdesc->schema;
			pcontext.is_sharescan = true;
			pcontext.gp_session_id = vmatstate->gp_session_id;
			pcontext.gp_command_count = vmatstate->gp_command_count;
			pcontext.share_id = vmatstate->share_id;
			pcontext.num_slices = vmatstate->num_slices;
			pcontext.is_cross_slice = vmatstate->is_cross_slice;
			pcontext.is_producer = vmatstate->is_producer;
			pcontext.ready = vmatstate->ready;
			pcontext.pipeline = false;
			pcontext.sinktype = Plain;
		}
		break;
		case T_ResultState:
		{
			if (!outerPlanState(planstate))
				elog(ERROR, "Result node can't be leaf in vector plan");
			pcontext.inputschema = GetSchemaFromSlot(
					outerPlanState(planstate)->ps_ResultTupleSlot);
		}
		break;
		case T_SubqueryScanState:
		{
			SubqueryScanState *node = (SubqueryScanState *) planstate;
			if (!node->subplan)
				elog(ERROR, "SubqueryScan node can't be leaf in vector plan");
			pcontext.inputschema = GetSchemaFromSlot(
				node->subplan->ps_ResultTupleSlot);
		}
		break;
		case T_SeqScanState:
		{
			VecSeqScanState *scanstate = (VecSeqScanState *)planstate;
			/* If qualification and projection are both empty,
			 * no need to build plan.
			 *
			 * Todo : project is initialized as ps_ProjInfo for PG first,
			 *		then test here to find whether we need to build project.
			 *		The ps_ProjInfo initialization need to be rewrite for
			 *		arrow plan, to find whether project is need.
			 */
			pcontext.inputschema = scanstate->vecdesc->schema;
			pcontext.map = scanstate->columnmap;
			pcontext.table_oid = scanstate->base.ss.ss_currentRelation->rd_id;
			pcontext.am_oid = scanstate->base.ss.ss_currentRelation->rd_rel->relam;
			pcontext.relation_schema = TupDescToSchema(RelationGetDescr(scanstate->base.ss.ss_currentRelation));
            pcontext.parallel_scan = garrow_dataset_is_registered(pcontext.am_oid);
		}
		break;
		case T_ForeignScanState:
		{
			VecForeignScanState *scanstate = (VecForeignScanState *)planstate;
			pcontext.inputschema = scanstate->schema;
			pcontext.map = scanstate->columnmap;
		}
		break;
		case T_AssertOpState:
		{
			pcontext.inputschema = GetSchemaFromSlot(outerPlanState(planstate)->ps_ResultTupleSlot);
			pcontext.is_assertop = true;
		}
		break;
		case T_SortState:
		{
			SortState *sortstate = (SortState*)planstate;
			PlanState  *outerNode = outerPlanState(sortstate);
			if (IsA(outerNode, SequenceState))
				outerNode = get_sequence_exact_result_ps(outerNode);
			pcontext.inputschema = GetSchemaFromSlot(outerNode->ps_ResultTupleSlot);
		}
		break;
		case T_LimitState:
		{
			if (!outerPlanState(planstate))
				elog(ERROR, "Limit node can't be leaf in vector plan");
			pcontext.inputschema = GetSchemaFromSlot(
					outerPlanState(planstate)->ps_ResultTupleSlot);
		}
		break;
		case T_SequenceState:
		{
			SequenceState *node = castNode(SequenceState, planstate);
			if (!node->subplans)
				elog(ERROR, "Sequence node can't be leaf in vector plan");
			pcontext.inputschema = GetSchemaFromSlot(
				node->subplans[node->numSubplans - 1]->ps_ResultTupleSlot);
			pcontext.is_left_schema = true;
			pcontext.subplan_index = 0;
		}
		break;
		case T_AppendState:
		{
			pcontext.pipeline = false;
			pcontext.sinktype = Plain;
			pcontext.is_append = true;	
		}
		break;
		default:
			elog(ERROR, "Build arrow plan from (%d) type is not support yet.",
					nodeTag(planstate->plan));
	}
	if (pcontext.parallel_scan)
	{
		pcontext.sinktype = Plain;
		pcontext.pipeline = false;
	}

	if (pool_threads > 0 && support_parallel)
	/* switch thread on, all plan go threads*/
	{
	 	estate->exectx = garrow_execute_context_new(pool_threads);
	 	pcontext.plan = garrow_execute_plan_new_with_context(estate->exectx, &error);
	}
	
	/* build plan sequentially */
	else
	{
		pcontext.plan = garrow_execute_plan_new(&error);
	}

	if (error)
		elog(ERROR, "Build arrow plan failed: plan node type %d.",
				nodeTag(planstate->plan));

	if (pcontext.is_nestloopjoin)
		BuildJoinPlan(&pcontext, estate);
	else if (pcontext.is_hashjoin)
		BuildJoinPlan(&pcontext, estate);
	else if (IsA(planstate, SequenceState))
		BuildSequencePlan(&pcontext, estate);
	else if (pcontext.is_materialize)
		BuildMaterializePlan(&pcontext, estate);
	else if (pcontext.is_sharescan)
		BuildShareScanPlan(&pcontext, estate);
	else
	{
		g_autoptr(GArrowExecuteNode) curnode = NULL;
		g_autoptr(GArrowExecuteNode) tmpnode = NULL;
		/* build plan */
		pcontext.is_left_schema = true;
		if (pcontext.is_append)
			curnode = BuildAppendPlan(&pcontext, estate);
		else 
		{
			if (pcontext.parallel_scan)
				curnode = BuildScanNode(&pcontext);
			else
				curnode = BuildSource(&pcontext);
		}
		/*
		 * For parallel scan (PAX SeqScan), split quals into fast filter
		 * quals (handled by PAX storage layer) and arrow quals (handled
		 * by Arrow FilterNode). Only pass arrow quals to BuildProject.
		 */
		if (pcontext.parallel_scan && qualList && IsA(planstate, SeqScanState))
		{
			VecSeqScanState *scanstate = (VecSeqScanState *)planstate;
			TupleDesc desc = RelationGetDescr(scanstate->base.ss.ss_currentRelation);
			List *arrowQuals = NIL;
			List *flatQuals = qualList;
			ListCell *lc;

			/* Unwrap single BoolExpr(AND) wrapper if present */
			if (list_length(flatQuals) == 1 && IsA(linitial(flatQuals), BoolExpr))
			{
				BoolExpr *boolexpr = (BoolExpr *)linitial(flatQuals);
				if (boolexpr->boolop == AND_EXPR)
					flatQuals = boolexpr->args;
			}

			foreach(lc, flatQuals)
			{
				Node *qual = (Node *)lfirst(lc);
				if (!PaxCanFastFilterC(qual, desc))
					arrowQuals = lappend(arrowQuals, qual);
			}

			qualList = arrowQuals;
		}

		tmpnode = BuildProject(targetList, qualList, curnode, &pcontext);
		garrow_store_ptr(curnode, tmpnode);
		
		BuildSink(curnode, estate, &pcontext);
	}

	if(!garrow_execute_plan_validate(pcontext.plan, &error))
		elog(ERROR, "Built invalid arrow plan: %s", error->message);
	
	garrow_execute_plan_set_plan_id(pcontext.plan, plan_num++);
	/* set plan execute state */
	estate->started = false;
	estate->slot = planstate->ps_ResultTupleSlot;
	garrow_store_ptr(estate->plan, pcontext.plan);

	/* read from result queue for consuming sink */
	estate->pipeline = pcontext.pipeline;
	if (pcontext.pipeline)
		estate->resqueue = NIL;
	else
		garrow_store_ptr(estate->reader, pcontext.reader);

	if (Debug_print_plan)
	{
		g_autofree gchar *str = garrow_execute_plan_to_string(estate->plan);
		elog(LOG, "arrow plan in BuildVecPlan: %s", str);
	}

	{
		static bool spill_cleanup_registered = false;

		if (!spill_cleanup_registered)
		{
			on_proc_exit(CleanupArrowSpillFiles, (Datum) 0);
			spill_cleanup_registered = true;
		}
	}
}

static void 
BuildJoinPlan(PlanBuildContext *pcontext, VecExecuteState *estate)
{
	g_autoptr(GArrowExecuteNode) left_source_node = NULL;
	g_autoptr(GArrowExecuteNode) right_source_node = NULL;
	g_autoptr(GArrowExecuteNode) join_node = NULL;
	g_autoptr(GArrowExecuteNode) left_proj_node = NULL;
	g_autoptr(GArrowExecuteNode) right_proj_node = NULL;
	g_autoptr(GArrowExecuteNode) proj_node = NULL;
	GList *left_fields = NULL;
	GList *right_fields = NULL;
	List *targetList = pcontext->planstate->plan->targetlist;
	List *qualList = pcontext->planstate->plan->qual;
	List *joinqual = NIL;
	List *outerHashKeys = NIL;
	List *innerHashKeys = NIL;
	List *mergeQual = NIL;
	PlanState *planstate = pcontext->planstate;
	if (pcontext->is_nestloopjoin)
	{
		NestLoopState *nlstate = (NestLoopState *) pcontext->planstate;
		if (nlstate->js.joinqual)
			joinqual = (List *) nlstate->js.joinqual->expr;
	}
	else
	{
		HashJoinState *hjstate = (HashJoinState *) pcontext->planstate;
		HashState *hstate = (HashState *) innerPlanState(pcontext->planstate);
		if (hjstate->js.joinqual)
			joinqual = (List *) hjstate->js.joinqual->expr;
		outerHashKeys = hjstate->hj_OuterHashKeys;
		innerHashKeys = hstate->hashkeys;
	}

	/*
	 * Arrow Acero RIGHT_SEMI / RIGHT_ANTI: build hash table from inputs[1]
	 * (right), probe from inputs[0] (left), emit from inputs[1].
	 *
	 * Both ORCA (DXL translator swaps children in CTranslatorDXLToPlStmt.cpp)
	 * and the PG planner produce the same canonical layout: outer = probe
	 * side, inner = Hash(build/emit side).  The standard mapping (outer->Arrow
	 * left, inner->Arrow right) already puts the build side at inputs[1], so no
	 * swap is needed in the vectorization layer.
	 */
	PlanState *arrow_left_plan  = outerPlanState(planstate);
	PlanState *arrow_right_plan = innerPlanState(planstate);
	List *arrow_left_keys  = outerHashKeys;
	List *arrow_right_keys = innerHashKeys;

	/* build left tree (= Arrow left input) */
	pcontext->inputschema = GetSchemaFromSlot(arrow_left_plan->ps_ResultTupleSlot);
	left_fields = garrow_schema_get_fields(pcontext->inputschema);
	pcontext->left_in_schema = garrow_schema_new(left_fields);
	pcontext->is_left_schema = true;
	left_source_node = BuildSource(pcontext);
	left_proj_node = BuildJoinProject(arrow_left_keys, left_source_node, pcontext);

	/* build right tree (= Arrow right input; build side for RIGHT_SEMI/ANTI) */
	pcontext->inputschema = GetSchemaFromSlot(arrow_right_plan->ps_ResultTupleSlot);
	right_fields = garrow_schema_get_fields(pcontext->inputschema);
	pcontext->right_in_schema = garrow_schema_new(right_fields);
	pcontext->is_left_schema = false;
	right_source_node = BuildSource(pcontext);
	right_proj_node = BuildJoinProject(arrow_right_keys, right_source_node, pcontext);

	/* build hashjoin tree */
	pcontext->inputschema = NULL;
	pcontext->is_hashjoin_after_node = true;
	if (pcontext->is_nestloopjoin)
	{
		join_node = BuildNestLoopjoin(pcontext, left_proj_node, right_proj_node, joinqual);
		mergeQual = qualList;
	}
	else
	{
		HashJoinState *node = (HashJoinState *) pcontext->planstate;
		if (node->js.jointype == JOIN_INNER)
		{
			join_node = BuildHashjoin(pcontext, left_proj_node, right_proj_node, NULL);
			mergeQual = list_concat(joinqual, qualList);
		}
		else
		{
			join_node = BuildHashjoin(pcontext, left_proj_node, right_proj_node, joinqual);
			mergeQual = qualList;
		}
	}
	pcontext->inputschema = garrow_execute_node_get_output_schema(join_node);

	proj_node = BuildProject(targetList, mergeQual, join_node, pcontext);
	BuildSink(proj_node, estate, pcontext);

	/*
	 * When targetList is NULL (semi/anti joins used only for count(*)),
	 * the PG result slot TupleDesc has 0 attributes → TupDescToSchema
	 * creates a "dummy" null-type schema.  Parent nodes (e.g. AggState)
	 * that call GetSchemaFromSlot on our result slot then get the wrong
	 * schema.  Propagate the actual join output schema to the slot so
	 * the parent builds its Arrow plan against the real column types.
	 */
	if (!targetList)
	{
		GArrowSchema *join_out = garrow_execute_node_get_output_schema(join_node);
		VecTupleTableSlot *vslot = (VecTupleTableSlot *)
			planstate->ps_ResultTupleSlot;
		if (TTS_IS_VECTOR(&vslot->base))
		{
			garrow_store_ptr(vslot->vec_schema.schema, join_out);
			vslot->vec_schema.schema_rebuilt = true;
		}
		g_object_unref(join_out);
	}

	ARROW_FREE(GArrowSchema, &pcontext->left_in_schema);
	ARROW_FREE(GArrowSchema, &pcontext->right_in_schema);
	ARROW_FREE(GArrowSchema, &pcontext->inputschema);
	garrow_list_free_ptr(&left_fields);
	garrow_list_free_ptr(&right_fields);
}

static void
BuildSequencePlan(PlanBuildContext *pcontext, VecExecuteState *estate)
{
	bool pipeline;

	GList *sources = NULL;
	g_autoptr(GError) error = NULL;
	g_autoptr(GArrowExecuteNode) last_source = NULL;
	g_autoptr(GArrowSequenceNodeOptions) options = NULL;
	g_autoptr(GArrowExecuteNode) sequence_node = NULL;

	Assert(IsA(pcontext->planstate, SequenceState));
	SequenceState *ss = castNode(SequenceState, pcontext->planstate);

	/*
	 * The last subplan must be created as pipeline source and put into
	 * the source list firstly. It ensures that the last subplan will be
	 * run as the last source in arrow plan.
	 */
	pcontext->subplan_index = ss->numSubplans - 1;
	last_source = BuildSource(pcontext);
	sources = garrow_list_append_ptr(sources, last_source);

	/*
	 * save the origin value of pcontext->pipeline, and reset later
	 */
	pipeline = pcontext->pipeline;
	pcontext->pipeline = false;
	for (int i = 0; i < ss->numSubplans -1; ++i)
	{
		g_autoptr(GArrowExecuteNode) source = NULL;

		pcontext->subplan_index = i;
		source = BuildSource(pcontext);
		sources = garrow_list_append_ptr(sources, source);
	}
	/* reset pipeline */
	pcontext->pipeline = pipeline;

	options = garrow_sequence_node_options_new();
	sequence_node = garrow_execute_plan_build_sequence_node(pcontext->plan,
															sources,
															options,
															&error);
	if (error)
		elog(ERROR, "Failed to create the sequence node, cause: %s", error->message);

	BuildSink(sequence_node, estate, pcontext);

	garrow_list_free_ptr(&sources);
}

void
FreeVecExecuteState(VecExecuteState *estate)
{
	if (estate->plan)
	{
		glib_autoptr_cleanup_GArrowExecutePlan(&estate->plan);
		estate->plan = NULL;
	}

	if (!estate->pipeline)
	{
		if (estate->reader)
		{
			glib_autoptr_cleanup_GArrowRecordBatchReader(&estate->reader);
			estate->reader = NULL;
		}
	}
	else if (estate->resqueue != NIL)
	{
		list_free(estate->resqueue);
		estate->resqueue = NIL;
	}

	if (estate->arrow_node_to_planstate != NIL)
	{
		list_free(estate->arrow_node_to_planstate);
		estate->arrow_node_to_planstate = NIL;
	}
}

/* ------------------------------------------------------------------------
 *
 * Execute an Arrow plan to get batch result.
 *
 * ------------------------------------------------------------------------
 */
TupleTableSlot *
ExecuteVecPlan(VecExecuteState *estate)
{
	g_autoptr(GArrowRecordBatch) batch = NULL;
	g_autoptr(GError) error = NULL;

	if (estate->pipeline)
	{
		while (true)
		{
			if (list_length(estate->resqueue) > 0)
			{
				batch = (GArrowRecordBatch *)linitial(estate->resqueue);
				estate->resqueue = list_delete_first(estate->resqueue);
				return ExecStoreBatch(estate->slot, batch);
			}

			if (!estate->started)
			{
				bool isok;
				isok = garrow_execute_plan_pipeline_start(estate->plan, &error);
				if (!isok || error)
					elog(ERROR, "Start execution plan error: %s.", error->message);
				estate->started = true;
			}
			else
			{
				if (garrow_execute_plan_is_stop(estate->plan))
					return NULL;
				garrow_execute_plan_pipeline_continue(estate->plan, &error);
				if (error)
					elog(ERROR, "Continue pipeline execution plan error: %s.", error->message);
			}
		}
	}
	else
	{
		if (!estate->started)
		{
			bool isok;
			isok = garrow_execute_plan_start(estate->plan, &error);
			if (!isok || error)
				elog(ERROR, "Start plan for vector plan error: %s.", error->message);
			isok = garrow_execute_plan_wait(estate->plan, &error);
			if (!isok || error)
				elog(ERROR, "Execute plan for vector plan error: %s.", error->message);
			estate->started = true;
		}

		batch = garrow_record_batch_reader_read_next(estate->reader, &error);
		if (error)
			elog(ERROR, "Execute plan for garrow_record_batch_reader_read_next error: %s.", error->message);

		return ExecStoreBatch(estate->slot, batch);
	}
	pg_unreachable();
}

/* build sort keys
 *
 * Outputs:
 *   GList of GArrowSortKey.
 */
GList *
build_sort_keys(PlanState *planstate, GArrowSchema *schema)
{
	Assert(IsA(planstate->plan, Sort));
	SortSupport sortKeys;
	Sort *plannode = (Sort *)planstate->plan;
	int nkeys = plannode->numCols;
	GList *keys = NULL;
	PlanState  *outerNode = outerPlanState(planstate);
	if (IsA(outerNode, SequenceState))
		outerNode = get_sequence_exact_result_ps(outerNode);
	sortKeys = (SortSupport) palloc0(nkeys * sizeof(SortSupportData));
	for (int i = 0; i < nkeys; i++)
	{
		SortSupport sortKey = sortKeys + i;

		sortKey->ssup_cxt = CurrentMemoryContext;
		sortKey->ssup_collation = plannode->collations[i];
		sortKey->ssup_nulls_first = plannode->nullsFirst[i];
		sortKey->ssup_attno = plannode->sortColIdx[i];
		/* Convey if abbreviation optimization is applicable in principle */
		sortKey->abbreviate = (i == 0);

		PrepareSortSupportFromOrderingOp(plannode->sortOperators[i], sortKey);
	}
	keys = create_sort_keys(sortKeys, nkeys, schema);
	pfree(sortKeys);
	return keys;
}

/*
 * Should be called by arrow plan, the function is a producer of
 * #GArrowRecordBatch that be consumed by PG plan.
 */
static void
consumer(void *tc, void *rb)
{
	VecExecuteState *estate = (VecExecuteState *)tc;
	GArrowRecordBatch *result = GARROW_RECORD_BATCH(rb);

	estate->resqueue = lappend(estate->resqueue, result);
}

void static
BuildSink(GArrowExecuteNode *input, VecExecuteState *estate, PlanBuildContext *pcontext)
{
	Plan *plan = pcontext->planstate->plan;
	g_autoptr(GArrowSchema) schema = NULL;
	g_autoptr(GArrowExecuteNode) sink = NULL;
	g_autoptr(GArrowSinkNodeOptions) options = NULL;
	g_autoptr(GArrowRecordBatchReader) sinkreader = NULL;
	g_autoptr(GError) error = NULL;

	schema = garrow_execute_node_get_output_schema(input);

	switch(pcontext->sinktype)
	{
		case Plain:
		{
			options = garrow_sink_node_options_new();
			sink = garrow_execute_plan_build_sink_node(pcontext->plan,
																 input,
																 options,
																 &error);

		}
		break;
		case Consuming:
		{
			g_autoptr(GArrowSchema) output_schema = NULL;
			g_autoptr(GArrowConsumingSinkNodeOptions) coptions = NULL;

			Assert(pcontext->pipeline);
			output_schema = garrow_execute_node_get_output_schema(input);
			coptions = garrow_consuming_sink_node_options_new(output_schema,
															  consumer,
															  estate);
			sink = garrow_execute_plan_build_consuming_sink_node(pcontext->plan,
																 input,
																 coptions,
																 &error);
		}
		break;

		default:
			elog(ERROR, "Build arrow plan from (%d) type is not support yet.",
					nodeTag(plan));
	}

	if (!pcontext->pipeline)
	{
		sinkreader = garrow_sink_node_options_get_reader(
						GARROW_SINK_NODE_OPTIONS(options), schema);
		garrow_store_ptr(pcontext->reader, sinkreader);
	}

	if (error)
		elog(ERROR, "Failed to create sink node, cause: %s", error->message);
}

// is not distinct equals to  (a == b) or (a is null and b is null)
// is distinct equals to (a != b) or (b is null and a is not null) or (a is null and  b is not null)
// converts to coalesce(a != b, (b is null and a is not null) or (a is null and  b is not null))
static GArrowExpression *
build_is_distinct_expression(DistinctExpr *dex, PlanBuildContext *pcontext)
{
	GList *arguments = NULL;
	g_autoptr(GArrowExpression) a_expr = NULL;
	g_autoptr(GArrowExpression) b_expr = NULL;
	g_autoptr(GArrowExpression) is_distinct = NULL;

	Assert(list_length(dex->args) == 2);

	Expr *a = (Expr *) linitial(dex->args);
	Expr *b = (Expr *) lsecond(dex->args);
	a_expr = expr_to_arrow_expression(a, pcontext);
	b_expr = expr_to_arrow_expression(b, pcontext);

	is_distinct = garrow_is_distinct_expression_new(a_expr, b_expr);

	garrow_list_free_ptr(&arguments);
	return garrow_move_ptr(is_distinct);
}

static GArrowExpression *
build_null_if_expression(NullIfExpr *dex, PlanBuildContext *pcontext)
{
	// nullif can be regarded as a special casewhen expression, 
	// so casewhen is used here for processing
	GList *when_arguments = NULL;
	g_autoptr(GArrowExpression) expr = NULL;
	g_autoptr(GArrowExpression) struct_expr;
	g_autoptr(GArrowExpression) null_expr;
	g_autoptr(GArrowMakeStructOptions) options;
	GList *expr_arguments = NULL;
	gchar **fields;

	Assert(list_length(dex->args) == 2);
	expr = func_args_to_expression(dex->args, pcontext, "equal");
	when_arguments = garrow_list_append_ptr(when_arguments, expr);

	fields = palloc(1 * (sizeof(gchar *)));
	fields[0] = palloc(sizeof(int) + 6);
	snprintf(fields[0], sizeof(int) + 6, "case_%d", 0);
	options = garrow_make_struct_options_new((const gchar **) fields, 1);

	struct_expr = GARROW_EXPRESSION(garrow_call_expression_new(
			"make_struct", when_arguments, GARROW_FUNCTION_OPTIONS(options)));
	expr_arguments = garrow_list_prepend_ptr(expr_arguments, struct_expr);
	
	Oid type = exprType((Node *) linitial(dex->args));
	null_expr = build_literal_expression(0, true, type, -1);
	expr_arguments = garrow_list_append_ptr(expr_arguments, null_expr);
	
	// Use the first arg as the return value
	expr = expr_to_arrow_expression((Expr *) linitial(dex->args), pcontext);
	expr_arguments = garrow_list_append_ptr(expr_arguments, expr);
	
	expr = GARROW_EXPRESSION(garrow_call_expression_new("case_when", expr_arguments, NULL));

	pfree(fields);
	garrow_list_free_ptr(&when_arguments);
	garrow_list_free_ptr(&expr_arguments);
	return garrow_move_ptr(expr);
}


static GArrowExpression *build_filter_expression(List *filterInfo,
												 PlanBuildContext *pcontext)
{
	g_autoptr(GArrowExpression) expr = NULL;

	if (list_length(filterInfo) > 1)
		expr = func_args_to_expression(filterInfo, pcontext, "and_kleene");
	else
		expr = expr_to_arrow_expression((Expr *)linitial(filterInfo), pcontext);
	return garrow_move_ptr(expr);
}

static GArrowFilterNodeOptions *build_filter_options(List *filterInfo,
													 PlanBuildContext *pcontext)
{
	g_autoptr(GArrowFilterNodeOptions) filter_options = NULL;
	g_autoptr(GArrowExpression) expr = build_filter_expression(filterInfo, pcontext);
	filter_options = garrow_filter_node_options_new(expr);
	return garrow_move_ptr(filter_options);
}

static GArrowAssertOpNodeOptions *build_assertop_options(List *filterInfo, PlanBuildContext *pcontext)
{
        g_autoptr(GArrowAssertOpNodeOptions) assertop_filter_options = NULL;
        g_autoptr(GArrowExpression) expr = build_filter_expression(filterInfo, pcontext);
        assertop_filter_options = garrow_assertop_node_options_new(expr);
        return garrow_move_ptr(assertop_filter_options);
}

static void *
get_scan_next_batch(PlanState *node)
{
	VecSeqScanState *vnode = (VecSeqScanState *)node;
	TupleTableSlot *slot;

	while (true)
	{
		slot = ExecVecScanFetch((ScanState *)node,
								(ExecScanAccessMtd)VecSeqNext,
								(ExecScanRecheckMtd)VecSeqRecheck,
								vnode->vscanslot);
		if (TupIsNull(slot))
			return NULL;
		if (!GetNumRows(slot))
			continue;
		break;
	}

	Assert(!TTS_IS_DIRTY(slot));
	return (void *) GetBatch(slot);
}

static void *
get_foreign_next_batch(PlanState *node)
{
	VecForeignScanState *vnode = (VecForeignScanState *)node;
	TupleTableSlot *slot;

	while (true)
	{
		slot = ExecVecScanFetch((ScanState *)node,
								(ExecScanAccessMtd)ForeignNext,
								(ExecScanRecheckMtd)ForeignRecheck,
								vnode->vscanslot);
		if (TupIsNull(slot))
			return NULL;
		if (!GetNumRows(slot))
			continue;
		break;
	}

	Assert(!TTS_IS_DIRTY(slot));

	return (void *) GetBatch(slot);
}

static void *
get_current_next_batch(PlanState *node)
{
	TupleTableSlot *slot;

	while (true) {
		slot = ExecProcNode(node);
		if (TupIsNull(slot))
			return NULL;
		if (!GetNumRows(slot))
			continue;
		break;
	}

	Assert(!TTS_IS_DIRTY(slot));
	return (void *) GetBatch(slot);
}

static GArrowExecuteNode *
BuildSource(PlanBuildContext *pcontext)
{
	g_autoptr(GError) error = NULL;
	g_autoptr(GArrowRecordBatchReader)	reader = NULL;
	g_autoptr(GArrowSourceNodeOptions)	options = NULL;
	g_autoptr(GArrowExecuteNode) source = NULL;
	AppendState *appendstate = NULL;
	PlanState **appendplanstates = NULL;

	PlanState *pstate = pcontext->planstate;
	GetNextCallback callback;
	switch(nodeTag(pcontext->planstate))
	{
		case T_SeqScanState:
			callback = (GetNextCallback) get_scan_next_batch;
			pstate = pcontext->planstate;
			break;
		case T_ForeignScanState:
			callback = (GetNextCallback) get_foreign_next_batch;
			pstate = pcontext->planstate;
			break;
		case T_SequenceState:
			callback = (GetNextCallback) get_current_next_batch;
			SequenceState *ss = castNode(SequenceState, pcontext->planstate);
			pstate = ss->subplans[pcontext->subplan_index];
			pcontext->inputschema = GetSchemaFromSlot(pstate->ps_ResultTupleSlot);
			break;
		case T_AppendState:
			appendstate = (AppendState*)pcontext->planstate;
			appendplanstates = appendstate->appendplans;
			pstate = appendplanstates[pcontext->sub_slice];
			callback = (GetNextCallback) get_current_next_batch;
			break;
		case T_MotionState:
			callback = (GetNextCallback) get_current_next_batch;
			pstate = pcontext->planstate;
			break;
		case T_HashJoinState:
		case T_NestLoopState:
			callback = (GetNextCallback) get_current_next_batch;
			/*
			 * is_left_schema is in Arrow's frame (true = Arrow LEFT input).
			 * Normally Arrow LEFT input ← PG outer subtree and Arrow RIGHT
			 * input ← PG inner subtree. RIGHT_SEMI / RIGHT_ANTI swap (see
			 * BuildJoinPlan) feeds PG inner to Arrow LEFT and PG outer to
			 * Arrow RIGHT, so flip the lookup here too.
			 */
			if (pcontext->is_left_schema)
				pstate = outerPlanState(pcontext->planstate);
			else
				pstate = innerPlanState(pcontext->planstate);
			break;
		case T_SubqueryScanState:
			callback = (GetNextCallback) get_current_next_batch;
			SubqueryScanState *subscanstate = (SubqueryScanState *) pcontext->planstate;
			pstate = subscanstate->subplan;
			break;
		default:
			callback = (GetNextCallback) get_current_next_batch;
			pstate = outerPlanState(pcontext->planstate);
			break;
	}
	reader = garrow_record_batch_reader_new_callback(
		pcontext->inputschema,
		callback,
		pstate,
		&error);

	if (error)
		elog(ERROR, "Failed to create callback reader for source node, cause: %s.", error->message);

	options = garrow_source_node_options_new_record_batch_reader(reader);
	if (!pcontext->pipeline || !pcontext->is_left_schema)
		source = garrow_execute_plan_build_source_node(pcontext->plan,
													   options,
													  &error);
	else
		source = garrow_execute_plan_build_pipeline_source_node(pcontext->plan,
																options,
															   &error);
	if (error)
		elog(ERROR, "Failed to create source node, cause: %s.", error->message);

	return garrow_move_ptr(source);
}

static GArrowExecuteNode *
BuildScanNode(PlanBuildContext *pcontext)
{
	VecSeqScanState *state = (VecSeqScanState *)pcontext->planstate;
	Plan *plan = (Plan *)pcontext->planstate->plan;
	g_autoptr(GError) error = NULL;
	g_autoptr(GArrowScanNodeOptions) scan_node_options = NULL;
	g_autoptr(GArrowExecuteNode) scan_node = NULL;
	g_autoptr(GArrowExpression) qual_expr = NULL;

	Assert(IsA(state, SeqScanState));
	if (plan->qual)
		qual_expr = build_filter_expression(plan->qual, pcontext);
	scan_node_options = garrow_scan_node_options_new(pcontext->table_oid,
													 (void *) pcontext->planstate,
													 pcontext->am_oid,
													 pcontext->relation_schema,
													 pcontext->inputschema,
													 qual_expr,
													 &error);
	if (error)
		elog(ERROR, "Failed to create scan node, cause: %s.", error->message);
	scan_node = garrow_execute_plan_build_scan_node(pcontext->plan,
													scan_node_options,
													&error);
	garrow_store_ptr(state->scan_node_options, scan_node_options);
	if (error)
		elog(ERROR, "Failed to create scan node, cause: %s.", error->message);
	return garrow_move_ptr(scan_node);
}

/* build project for aggregate input */
static GArrowExecuteNode*
BuildAggProject(List *targetList, List *aggInfos, GArrowExecuteNode *input, PlanBuildContext *pcontext)
{
	g_autoptr(GArrowProjectNodeOptions) project_options = NULL;
	g_autoptr(GArrowExecuteNode) project = NULL;
	g_autoptr(GError) error = NULL;

	if (IsA(pcontext->planstate, WindowAggState))
		project_options = build_windowagg_project_options(targetList, aggInfos, pcontext);
	else if (IsA(pcontext->planstate, WindowHashAggState))
		project_options = build_windowhashagg_project_options(targetList, aggInfos, pcontext);
	else
		project_options = build_agg_project_options(targetList, aggInfos, pcontext);

	if (!project_options)
		return NULL;

	project = garrow_execute_plan_build_project_node(pcontext->plan,
													 input,
													 project_options,
													 &error);
	if (error)
		elog(ERROR, "Failed to create Arrow project node: %s.", error->message);
	return garrow_move_ptr(project);
}

static void free_agg_infos(List *agginfos)
{
	ListCell	   *l;

	foreach(l, agginfos)
	{
		VecAggInfo *agginfo = (VecAggInfo *) lfirst(l);

		glib_autoptr_cleanup_GArrowFunctionOptions(&agginfo->options);
	}
	list_free_deep(agginfos);
}

static GArrowExecuteNode*
BuildProject(List *targetList, List *qualList, GArrowExecuteNode *input, PlanBuildContext *pcontext)
{
	g_autoptr(GArrowProjectNodeOptions) project_options = NULL;
	g_autoptr(GArrowFilterNodeOptions) filter_options = NULL;
	g_autoptr(GArrowAssertOpNodeOptions) assertop_options = NULL;
	g_autoptr(GArrowExecuteNode) project = NULL;
	g_autoptr(GArrowExecuteNode) filter_node = NULL;
	g_autoptr(GArrowExecuteNode) assertop_node = NULL;
	g_autoptr(GArrowExecuteNode) orderby_node = NULL;
	g_autoptr(GArrowExecuteNode) current = garrow_copy_ptr(input);
	g_autoptr(GError) error = NULL;

	/*
	 * Sonic Motion Direct-Send: the passthrough flag is set authoritatively
	 * by build_aggregatation_options when a hint is found in the
	 * vec_motion_direct_send HTAB. Reset here to avoid stale state from a
	 * sibling plan node.
	 *
	 * project_options must be built BEFORE the agg block (its expressions
	 * reference agginfo->outname strings which the agg block populates,
	 * but free_agg_infos at the end of the agg block invalidates them).
	 * If the agg block then turns the passthrough flag on, we rebuild
	 * project_options below — slightly wasteful but correct.
	 */
	pcontext->sonic_target_segment_passthrough = false;
	pcontext->sonic_target_segment_col_name = NULL;

	if (targetList)
		project_options = build_project_options(targetList, pcontext);

	if (qualList && pcontext->is_assertop)
		assertop_options = build_assertop_options(qualList, pcontext);
	else if (qualList)
		filter_options = build_filter_options(qualList, pcontext);

	/* Build Agg or WindowAgg if any */
	if (IsA(pcontext->planstate->plan, Agg) ||
	    IsA(pcontext->planstate->plan, WindowAgg) ||
	    IsA(pcontext->planstate->plan, WindowHashAgg))
	{
		g_autoptr(GArrowExecuteNode) aggregation_input = NULL;
		g_autoptr(GArrowExecuteNode) aggregation = NULL;
		List *agginfos = pcontext->agginfos;

		/* build project for aggregate input */
		aggregation_input = BuildAggProject(targetList, agginfos, current, pcontext);
		if (aggregation_input)
			aggregation = BuildAggregatation(agginfos, aggregation_input, pcontext);
		else
			aggregation = BuildAggregatation(agginfos, current, pcontext);
		garrow_store_ptr(current, aggregation);

		/*
		 * Sonic Motion Direct-Send: if build_aggregatation_options turned on
		 * the passthrough flag, the upstream project_options built above ran
		 * with the flag still false and is missing the hidden-column
		 * field_ref. Rebuild here, while agginfos is still alive — Aggref
		 * resolution in expr_to_arrow_expression iterates agginfos to look
		 * up outname, so this MUST happen before free_agg_infos.
		 */
		if (pcontext->sonic_target_segment_passthrough && targetList)
		{
			g_clear_object(&project_options);
			project_options = build_project_options(targetList, pcontext);
		}

		free_agg_infos(agginfos);
	}

	/* Build Limit node if any */
	if (IsA(pcontext->planstate->plan, Limit))
	{
		g_autoptr(GArrowExecuteNode) limit = NULL;
		g_autoptr(GArrowLimitNodeOptions) options = NULL;

		LimitState *ls = castNode(LimitState, pcontext->planstate);

		options = garrow_limit_node_options_new(ls->offset, ls->count, ls->noCount);
		limit = garrow_execute_plan_build_limit_node(pcontext->plan, current, options, &error);
		if (error)
			elog(ERROR, "Failed to create limit node, cause: %s", error->message);

		garrow_store_ptr(current, limit);
	}

	/* having clause is always after agg and before upper agg project */
	if (qualList && pcontext->is_assertop)
	{
		assertop_node = garrow_execute_plan_build_assertop_node(pcontext->plan, current, assertop_options, &error);
		if (error)
			elog(ERROR, "Failed to create assertop node, %s.", error->message);
		garrow_store_ptr(current, assertop_node);
	}
	else if (qualList)
	{
		filter_node = garrow_execute_plan_build_filter_node(pcontext->plan, current, filter_options, &error);
		if (error)
			elog(ERROR, "Failed to create filter node, %s.", error->message);
		garrow_store_ptr(current, filter_node);
	}

	if (targetList)
	{
		project = garrow_execute_plan_build_project_node(pcontext->plan,
														 current,
														 project_options,
														 &error);
		if (error)
			elog(ERROR, "Failed to create Arrow project node: %s.", error->message);
		garrow_store_ptr(current, project);
	}

	if (IsA(pcontext->planstate->plan, Sort))
	{
		int64 k = vec_topk_bounds_lookup(pcontext->planstate->plan);
		g_autoptr(GArrowExecuteNode) sort = NULL;

		if (k > 0)
			sort = build_topk_node(pcontext->planstate, pcontext->plan,
								   current, k);
		else
			sort = build_orderby_node(pcontext->planstate, pcontext->plan, current);

		garrow_store_ptr(current, sort);
	}

	return garrow_move_ptr(current);
}

static const char *
GetHashJoinProjectName(PlanBuildContext *pcontext, const char *name)
{
	/* right_ len > left_ len */
	int prefix_maxlen = strlen(RIGHT_PREFIX);
	char *prefix_name = palloc(prefix_maxlen + strlen(name) + 1);
	snprintf(prefix_name, prefix_maxlen + strlen(name) + 1, "%s%s", pcontext->is_left_schema ? LEFT_PREFIX : RIGHT_PREFIX, name);
	return prefix_name;
}

static GArrowProjectNodeOptions *
build_join_project_options(List *hashkeys, GArrowExecuteNode *input, PlanBuildContext *pcontext)
{
	GList *expressions = NULL;
	g_autoptr(GArrowProjectNodeOptions) options = NULL;
	gsize			length;
	ListCell	   *l;
	const gchar		  **names;
	g_autoptr(GArrowSchema) schema = NULL;
	GList *fields = NULL;
	g_autoptr(GError) error = NULL;
	int i = 0;
	int j = 0;

	schema = garrow_execute_node_get_output_schema(input);
	fields = garrow_schema_get_fields(schema);

	length = g_list_length(fields) + list_length(hashkeys);
	names = palloc(length * sizeof(gchar *));

	/* convert source schema column to proj fields */
	for (j = 0; j < g_list_length(fields); j++)
	{
		g_autoptr(GArrowExpression) arrow_expr;
		g_autoptr(GArrowField) f = garrow_list_nth_data(fields, j);
		const char *name = garrow_field_get_name(f);
		arrow_expr = GARROW_EXPRESSION(garrow_field_expression_new(name, &error));
		if (error)
			elog(ERROR, "convert field to arrow expression failed: %s",
				 error->message);
		expressions = garrow_list_append_ptr(expressions, arrow_expr);
		names[j] = GetHashJoinProjectName(pcontext, name);
	}

	/* append hashkeys column to proj fields */
	foreach(l, hashkeys)
	{
		g_autoptr(GArrowExpression) arrow_expr = NULL;
		g_autoptr(GArrowField) field = NULL;
		g_autoptr(GArrowDataType) result_type = NULL; 
		ExprState *exprstate = (ExprState *)lfirst(l);
		int prefix_maxlen = strlen(RIGHT_PREFIX);
		char *prefix_name = palloc(prefix_maxlen + 20);
		/* The hashkey's projected column names are built together when building the expression */
		arrow_expr = expr_to_arrow_expression(exprstate->expr, pcontext);
		expressions = garrow_list_append_ptr(expressions, arrow_expr);
		snprintf(prefix_name, prefix_maxlen + 20, "%sjoinqual_%d", pcontext->is_left_schema ? LEFT_PREFIX : RIGHT_PREFIX, i++);
		result_type = PGTypeToArrow(exprType((Node *) exprstate->expr));
		field = garrow_field_new(prefix_name, result_type);
		if (pcontext->is_left_schema)
		{
			pcontext->left_hashkeys = garrow_list_append_ptr(pcontext->left_hashkeys, field);
		}
		else
		{
			pcontext->right_hashkeys = garrow_list_append_ptr(pcontext->right_hashkeys, field);
		}
		names[j++] = prefix_name;
	}
	options = garrow_project_node_options_new(expressions,
											  (gchar **)names,
											  length);
	pfree(names);
	garrow_list_free_ptr(&expressions);
	garrow_list_free_ptr(&fields);
	return garrow_move_ptr(options);
}

static GArrowExecuteNode*
BuildJoinProject(List *hashkeys, GArrowExecuteNode *input, PlanBuildContext *pcontext)
{
	g_autoptr(GArrowProjectNodeOptions) project_options = NULL;
	g_autoptr(GArrowExecuteNode) project = NULL;
	g_autoptr(GError) error = NULL;

	project_options = build_join_project_options(hashkeys, input, pcontext);

	project = garrow_execute_plan_build_project_node(pcontext->plan,
													 input,
													 project_options,
													 &error);
	if (error)
		elog(ERROR, "Failed to create Arrow project node: %s.", error->message);
	return garrow_move_ptr(project);
}

static const int
GetGroupKey(Agg *agg, Expr *expr)
{
	Var *var = (Var *)expr;

	if (!IsA(expr, Var))
		return -1;

	for(int i = 0; i < agg->numCols; i++)
	{
		if(agg->grpColIdx[i] == var->varattno)
			return i;
	}
	return -1;
}

/* Aggregate input columns is consists of two parts:
 * 1. The arg fo each aggref.
 * 2. The group keys columns if exists.
 * Make these expressions as a single projector.
 */
static GArrowProjectNodeOptions *
build_agg_project_options(List *targetList, List *aggInfos, PlanBuildContext *pcontext)
{
	GList *expressions = NULL;
	Agg *agg = (Agg *) pcontext->planstate->plan;
	gsize nkey = agg->numCols;
	g_autoptr(GArrowProjectNodeOptions) options = NULL;
	gsize			length;
	int				i = 0;
	bool 			need_project = false;
	ListCell	   *l;
	const gchar	  **names; /* project result names */

	length = list_length(aggInfos) + nkey;
	names = palloc(length * sizeof(gchar *));
	/* append group keys */
	for (i = 0; i < nkey; i++)
	{
		g_autoptr(GArrowExpression) arrow_expr = NULL;
		g_autoptr(GArrowField)	field = NULL;
		g_autoptr(GError) error = NULL;

		/* group key out name is same as input name */
		names[i] = GetSchemaName(pcontext->inputschema, agg->grpColIdx[i], pcontext->map);
		arrow_expr = GARROW_EXPRESSION(
				garrow_field_expression_new(names[i], &error));
		if (error)
			elog(ERROR, "New arrow expression from column: %s error: %s",
					names[i], error->message);
		expressions = garrow_list_append_ptr(expressions, arrow_expr);
	}
	if (nkey > 0)
		pcontext->keys = names;
	pcontext->nkey = nkey;

	if ((pcontext->nkey > 0) && (!aggInfos))
		rewrite_tl_keys(targetList, pcontext);

	/* append aggrefs */
	foreach(l, aggInfos)
	{
		g_autoptr(GArrowExpression) arrow_expr = NULL;
		VecAggInfo *agginfo = (VecAggInfo *) lfirst(l);
		Aggref *aggref = agginfo->aggref;
		int key;
		TargetEntry *source_tle;

		Assert(IsA(aggref, Aggref));
		/* Todo: single arg only now*/

		if(!aggref->args)
			continue;

		if (list_length(aggref->args) != 1) 
			elog(ERROR, "doesn't support aggref args len != 1");
		source_tle = (TargetEntry *) linitial(aggref->args);

		/* skip columns in group keys*/
		key = GetGroupKey(agg, source_tle->expr);
		if(key >= 0)
		{
			/* aggref is group key, update agg inname */
			memcpy(agginfo->inname, names[key], sizeof(agginfo->inname));
			continue;
		}

		arrow_expr = expr_to_arrow_expression(source_tle->expr, pcontext);

		/* FIXME: Add shared input expression later */

		/* child expression is input of agg, use inname */
		names[i] = agginfo->inname;

		expressions = garrow_list_append_ptr(expressions, arrow_expr);
		need_project = true;
		i++;
	}

	/* append aggrefs */
	foreach(l, aggInfos)
	{
		VecAggInfo *agginfo = (VecAggInfo *) lfirst(l);
		Aggref *aggref = agginfo->aggref;

		/* count() or count(*), needn't project this column.
		 * Update aggname as another valid column.
		 */
		if(!aggref->args)
		{
			ListCell	   *c;
			char *name = NULL;

			foreach(c, aggInfos)
			{
				VecAggInfo *cinfo = (VecAggInfo *) lfirst(c);

				if (cinfo->aggref->args)
				{
					name = cinfo->inname;
					break;
				}
			}

			/* If found other agg, count other column.
			 * If no other column, means a count(*), no project for agg input.
			 * So, use an arbitrary input column name as agg inname.
			 */
			/* Fixme: implement count(*) by arrow EmptyBatch */
			if (!name)
			{
				name = (char *)GetSchemaName(pcontext->inputschema, 1, pcontext->map);
			}
			memcpy(agginfo->inname, name, sizeof(agginfo->inname));
			continue;
		}
	}

	if (!need_project)
	{
		garrow_list_free_ptr(&expressions);
		return NULL;
	}

	options = garrow_project_node_options_new(expressions,
											  (gchar**)names,
											  i);

	/* if nkeys > 0, names will be used for group keys */
	if (nkey <= 0)
		pfree(names);
	garrow_list_free_ptr(&expressions);
	return garrow_move_ptr(options);
}

static GArrowProjectNodeOptions *
build_windowagg_project_options(List *targetList, List *aggInfos, PlanBuildContext *pcontext)
{
	GList *expressions = NULL;
	g_autoptr(GArrowProjectNodeOptions) options = NULL;
	gsize			length;
	bool 			need_project = false;
	ListCell	   *l;
	const gchar	  **names; /* project result names */

	WindowAgg *wagg = (WindowAgg *) pcontext->planstate->plan;

	/* find cols used by "order by" clause */
	pcontext->orderby_sortoption = NULL;
	if (wagg->ordNumCols > 0)
	{
		GList *orderby_keys = NULL;
		SortKey sortKey;
		sortKey.orders = palloc0(sizeof(GArrowSortOrder) * wagg->ordNumCols);
		sortKey.nulls_first = palloc0(sizeof(GArrowSortNullPlacement) * wagg->ordNumCols);
		get_windowagg_sortorder(pcontext, &sortKey);

		for (int i = 0; i < wagg->ordNumCols; i++)
		{
			g_autoptr(GArrowSortKey) key = NULL;
			const char *field_name = GetSchemaName(pcontext->inputschema, wagg->ordColIdx[i], NULL);
			key = garrow_sort_key_new((gchar *)pstrdup(field_name), sortKey.orders[i], GARROW_SORT_ORDER_Default, sortKey.nulls_first[i]);
			orderby_keys = garrow_list_append_ptr(orderby_keys, key);
		}

		pcontext->orderby_sortoption =  vec_sort_options_new(orderby_keys, partition_top_k, take_thread_num, two_phase_take);
		garrow_list_free_ptr(&orderby_keys);
		pfree(sortKey.orders);
		pfree(sortKey.nulls_first);
	}

	/* find cols used by "partition by" clause */
	const gchar **partcol_names = palloc0(sizeof(gchar *) * wagg->partNumCols);
	for (int i = 0; i < wagg->partNumCols; i++)
		partcol_names[i] = GetSchemaName(pcontext->inputschema, wagg->partColIdx[i], NULL);

	if (wagg->partNumCols > 0)
	{
		pcontext->keys = partcol_names;
		pcontext->nkey = wagg->partNumCols;
	}
	else
	{
		pcontext->keys = NULL;
		pcontext->nkey = 0;
	}

	/* append all input columns with windowagg */
	int num_fields = garrow_schema_n_fields(pcontext->inputschema);
	length = list_length(aggInfos) + num_fields;
	names = palloc(length * sizeof(gchar *));
	int col = 0;
	for (int i = 0; i < num_fields; i++)
	{
		g_autoptr(GArrowExpression) arrow_expr = NULL;
		g_autoptr(GArrowField)	field = NULL;
		g_autoptr(GError) error = NULL;

		/* group key out name is same as input name */
		const char *fname = GetSchemaName(pcontext->inputschema, i + 1, pcontext->map);
		names[col] = fname;
		arrow_expr = GARROW_EXPRESSION(garrow_field_expression_new(names[col], &error));
		if (error)
			elog(ERROR, "New arrow expression from column: %s error: %s",
					names[i], error->message);
		expressions = garrow_list_append_ptr(expressions, arrow_expr);
		++col;
	}

	/* append window aggrefs */
	foreach(l, aggInfos)
	{
		g_autoptr(GArrowExpression) arrow_expr = NULL;
		VecAggInfo *agginfo = (VecAggInfo *) lfirst(l);
		WindowFunc *wfunc = agginfo->wfunc;

		Assert(IsA(wfunc, WindowFunc));

		/* Todo: single arg only now*/
		if(!wfunc->args)
			continue;

		Assert(list_length(wfunc->args) == 1);
		arrow_expr = expr_to_arrow_expression(linitial(wfunc->args), pcontext);

		/* FIXME: Add shared input expression later */

		/* child expression is input of agg, use inname */
		names[col] = agginfo->inname;

		expressions = garrow_list_append_ptr(expressions, arrow_expr);
		need_project = true;
		col++;
	}

	/* append window aggrefs */
	foreach(l, aggInfos)
	{
		VecAggInfo *agginfo = (VecAggInfo *) lfirst(l);
		WindowFunc *wfunc = agginfo->wfunc;

		/*
		 * count() or count(*), needn't project this column.
		 * Update aggname as another valid column.
		 */
		if(!wfunc->args)
		{
			ListCell	   *c;
			char *name = NULL;

			foreach(c, aggInfos)
			{
				VecAggInfo *cinfo = (VecAggInfo *) lfirst(c);

				if (cinfo->wfunc->args)
				{
					name = cinfo->inname;
					break;
				}
			}

			/* If found other agg, count other column.
			 * If no other column, means a count(*), no project for agg input.
			 * So, use an arbitrary input column name as agg inname.
			 */
			/* Fixme: implement count(*) by arrow EmptyBatch */
			if (!name)
			{
				name = (char *)GetSchemaName(pcontext->inputschema, 1, pcontext->map);
			}
			memcpy(agginfo->inname, name, sizeof(agginfo->inname));
			continue;
		}
	}

	if (!need_project)
	{
		garrow_list_free_ptr(&expressions);
		return NULL;
	}

	options = garrow_project_node_options_new(expressions,
											  (gchar**)names,
											  col);

	if (DEBUG1 >= log_min_messages)
	{
		for (GList *node = expressions; node; node = node->next)
		{
			g_autofree gchar *str =
				garrow_expression_to_string(GARROW_EXPRESSION(node->data));
			elog(DEBUG1, "%s result expressions: %s", __func__, str);
		}
	}
	garrow_list_free_ptr(&expressions);

	return garrow_move_ptr(options);
}

static GArrowProjectNodeOptions *
build_windowhashagg_project_options(List *targetList, List *aggInfos, PlanBuildContext *pcontext)
{
	GList *expressions = NULL;
	g_autoptr(GArrowProjectNodeOptions) options = NULL;
	gsize			length;
	bool 			need_project = false;
	ListCell	   *l;
	const gchar	  **names; /* project result names */

	WindowHashAgg *wagg = (WindowHashAgg *) pcontext->planstate->plan;

	/* find cols used by "order by" clause */
	pcontext->orderby_sortoption = NULL;
	if (wagg->ordNumCols > 0)
	{
		Oid opfamily, opcintype;
		int16 strategy;
		GList *orderby_keys = NULL;

		for (int i = 0; i < wagg->ordNumCols; i++)
		{
			/* Find the operator in pg_amop */
			if (!get_ordering_op_properties(wagg->ordOperators[i], &opfamily, &opcintype, &strategy))
				elog(ERROR, "operator %u is not a valid ordering operator", wagg->ordOperators[i]);

			g_autoptr(GArrowSortKey) key = NULL;
			const char *field_name = GetSchemaName(pcontext->inputschema, wagg->ordColIdx[i], NULL);
			key = garrow_sort_key_new((gchar *)pstrdup(field_name),
									  (strategy == BTLessStrategyNumber) ? GARROW_SORT_ORDER_ASCENDING : GARROW_SORT_ORDER_DESCENDING,
									  GARROW_SORT_ORDER_Default,
									  wagg->ordNullsFirst[i] ? GARROW_SORT_ORDER_AT_START : GARROW_SORT_ORDER_AT_END);
			orderby_keys = garrow_list_append_ptr(orderby_keys, key);
		}

		pcontext->orderby_sortoption =  vec_sort_options_new(orderby_keys, partition_top_k, take_thread_num, two_phase_take);
		garrow_list_free_ptr(&orderby_keys);
	}

	/* find cols used by "partition by" clause */
	const gchar **partcol_names = palloc0(sizeof(gchar *) * wagg->partNumCols);
	for (int i = 0; i < wagg->partNumCols; i++)
		partcol_names[i] = GetSchemaName(pcontext->inputschema, wagg->partColIdx[i], NULL);

	if (wagg->partNumCols > 0)
	{
		pcontext->keys = partcol_names;
		pcontext->nkey = wagg->partNumCols;
	}
	else
	{
		pcontext->keys = NULL;
		pcontext->nkey = 0;
	}

	/* append all input columns with windowagg */
	int num_fields = garrow_schema_n_fields(pcontext->inputschema);
	length = list_length(aggInfos) + num_fields;
	names = palloc(length * sizeof(gchar *));
	int col = 0;
	for (int i = 0; i < num_fields; i++)
	{
		g_autoptr(GArrowExpression) arrow_expr = NULL;
		g_autoptr(GArrowField)	field = NULL;
		g_autoptr(GError) error = NULL;

		/* group key out name is same as input name */
		const char *fname = GetSchemaName(pcontext->inputschema, i + 1, pcontext->map);
		names[col] = fname;
		arrow_expr = GARROW_EXPRESSION(garrow_field_expression_new(names[col], &error));
		if (error)
			elog(ERROR, "New arrow expression from column: %s error: %s",
					names[i], error->message);
		expressions = garrow_list_append_ptr(expressions, arrow_expr);
		++col;
	}

	/* append window aggrefs */
	foreach(l, aggInfos)
	{
		g_autoptr(GArrowExpression) arrow_expr = NULL;
		VecAggInfo *agginfo = (VecAggInfo *) lfirst(l);
		WindowFunc *wfunc = agginfo->wfunc;

		Assert(IsA(wfunc, WindowFunc));

		/* Todo: single arg only now*/
		if(!wfunc->args)
			continue;

		Assert(list_length(wfunc->args) == 1);
		arrow_expr = expr_to_arrow_expression(linitial(wfunc->args), pcontext);

		/* FIXME: Add shared input expression later */

		/* child expression is input of agg, use inname */
		names[col] = agginfo->inname;

		expressions = garrow_list_append_ptr(expressions, arrow_expr);
		need_project = true;
		col++;
	}

	/* append window aggrefs */
	foreach(l, aggInfos)
	{
		VecAggInfo *agginfo = (VecAggInfo *) lfirst(l);
		WindowFunc *wfunc = agginfo->wfunc;

		/*
		 * count() or count(*), needn't project this column.
		 * Update aggname as another valid column.
		 */
		if(!wfunc->args)
		{
			ListCell	   *c;
			char *name = NULL;

			foreach(c, aggInfos)
			{
				VecAggInfo *cinfo = (VecAggInfo *) lfirst(c);

				if (cinfo->wfunc->args)
				{
					name = cinfo->inname;
					break;
				}
			}

			/* If found other agg, count other column.
			 * If no other column, means a count(*), no project for agg input.
			 * So, use an arbitrary input column name as agg inname.
			 */
			/* Fixme: implement count(*) by arrow EmptyBatch */
			if (!name)
			{
				name = (char *)GetSchemaName(pcontext->inputschema, 1, pcontext->map);
			}
			memcpy(agginfo->inname, name, sizeof(agginfo->inname));
			continue;
		}
	}

	if (!need_project)
	{
		garrow_list_free_ptr(&expressions);
		return NULL;
	}

	options = garrow_project_node_options_new(expressions,
											  (gchar**)names,
											  col);

	if (DEBUG1 >= log_min_messages)
	{
		for (GList *node = expressions; node; node = node->next)
		{
			g_autofree gchar *str =
				garrow_expression_to_string(GARROW_EXPRESSION(node->data));
			elog(DEBUG1, "%s result expressions: %s", __func__, str);
		}
	}
	garrow_list_free_ptr(&expressions);

	return garrow_move_ptr(options);
}

static GArrowProjectNodeOptions *
build_project_options(List *targetList, PlanBuildContext *pcontext)
{
	GList *expressions = NULL;
	g_autoptr(GArrowProjectNodeOptions) options = NULL;
	gsize			length;
	int				i = 0;
	ListCell	   *l;
	const gchar		  **names;

	length = list_length(targetList);
	names = palloc(length * sizeof(gchar *));

	foreach(l, targetList)
	{
		pcontext->append_filed_index = i;
		g_autoptr(GArrowExpression) arrow_expr = NULL;

		TargetEntry *tle = (TargetEntry *)lfirst(l);
		/* rebuild Target Entry to List*/
		Assert(IsA(tle, TargetEntry));
		/* use targetEntry name */
		arrow_expr = expr_to_arrow_expression(tle->expr, pcontext);
		if (pcontext->is_append)
		{
			char *name = (char *) palloc(20);
			snprintf(name, 20, "append_%d", i);
			names[i] = name;
		}
		else
			names[i] = (gchar *)GetUniqueAttrName(tle->resname, i + 1);
		expressions = garrow_list_append_ptr(expressions, arrow_expr);
		i++;
	}
	/*
	 * Sonic motion direct-send: when the upstream Sonic agg added a
	 * leading hidden target-segment column (named by
	 * sonic_target_segment_col_name), append a passthrough field_ref so
	 * the project carries it through to the Motion node. Append at the
	 * end so existing Var(varattno = i+1) references in targetList stay
	 * valid (they refer to positions 1..length in the post-project
	 * output, which we leave unchanged).
	 */
	if (pcontext->sonic_target_segment_passthrough)
	{
		const char *seg_name = pcontext->sonic_target_segment_col_name;
		Assert(seg_name != NULL);
		g_autoptr(GError) seg_err = NULL;
		GArrowExpression *seg_expr = GARROW_EXPRESSION(
			garrow_field_expression_new(seg_name, &seg_err));
		if (seg_err != NULL)
			elog(ERROR,
				 "sonic motion direct-send: build target_segment_col_name "
				 "field_ref failed: %s",
				 seg_err->message);

		const gchar **names_extended =
			palloc((length + 1) * sizeof(gchar *));
		for (gsize j = 0; j < length; ++j)
			names_extended[j] = names[j];
		names_extended[length] = seg_name;
		/* garrow_list_append_ptr requires an lvalue (it takes &C); use the
		 * named local seg_expr rather than a temporary. The list takes
		 * ownership, so we don't unref afterwards. */
		expressions = garrow_list_append_ptr(expressions, seg_expr);
		options = garrow_project_node_options_new(expressions,
												  (gchar**)names_extended,
												  length + 1);
		pfree(names_extended);
	}
	else
	{
		options = garrow_project_node_options_new(expressions,
												  (gchar**)names,
												  length);
	}
	pfree(names);
	pcontext->append_filed_index = 0;

	if (DEBUG1 >= log_min_messages)
	{
		for (GList *node = expressions; node; node = node->next)
		{
			g_autofree gchar *str =
				garrow_expression_to_string(GARROW_EXPRESSION(node->data));
			elog(DEBUG1, "%s result expressions: %s", __func__, str);
		}
	}
	garrow_list_free_ptr(&expressions);

	return garrow_move_ptr(options);
}

/*
 * The distinct of group by key needs to carry other fields, which is the default for pg, 
 * but it needs to be provided for arrow, otherwise the column will not be found when build _outagg_dummy agg.
 * 
 * case1: fallback
 * explain verbose SELECT DISTINCT count(unique1) FROM tenk1 group by unique1;
 * ->  HashAggregate  (cost=183.33..216.67 rows=3333 width=12)
 *       Output: (count(unique1)), unique1
 *       Group Key: (count(tenk1.unique1))
 *
 * case2: normal
 * explain verbose select unique1 from tenk1 a where unique1 in
 * (select unique1 from tenk1 b join tenk1 c using (unique1) where b.unique2 = 42);
 * ->  HashAggregate  
 *       Output: b.unique1, c.unique1
 *       Group Key: b.unique1
 * 
 * for case1, we should fallback since unique1 is junk column, we will get n rows, but acutal one result.
 * for case2, normal process.
 */
static void 
rewrite_tl_keys(List *targetList, PlanBuildContext *pcontext)
{
	Agg *agg = (Agg *) pcontext->planstate->plan;
	gsize nkey = agg->numCols;
	int length = list_length(targetList);
	const gchar	  **keys;
	int j;
	ListCell	   *l;

	/*
	 * targetList may be empty.
	 * Vec GroupAggregate  (cost=0.00..431.00 rows=1 width=1)
     * 	   Group Key: col_a
	 */
	if (!targetList)
		return;

	j = 0;
	keys = palloc(length * sizeof(gchar *));
	foreach(l, targetList)
	{
		TargetEntry *tle = lfirst(l);
		Node *node = (Node *)tle->expr;
		bool find = false;
		const char* cur;
		if (nodeTag(node) != T_Var)
			continue;
		Var *var = (Var *)node;
		int i = 0;
		for (i = 0; i < nkey; i++) 
		{
			AttrNumber attnum = agg->grpColIdx[i];
			if (var->varattno == attnum)
				break;
		}
		if (i >= nkey) /* not found */
			cur = GetSchemaName(pcontext->inputschema, var->varattno, pcontext->map);
		else
			cur = pcontext->keys[i];
		/*
		 * SELECT a/2, a/2 FROM test_missing_target group by a/2;
		 * ->  Vec GroupAggregate  (cost=0.00..431.00 rows=1 width=4)
         * Output: ((a / 2)), ((a / 2))
         * Group Key: ((test_missing_target.a / 2))
		 */
		/* targetList has the same key */
		for (int k = 0; k < j; k++)
		{
			if (strcmp(keys[k], cur) == 0)
			{
				find = true;
				break;
			}
		}
		if (!find)
			keys[j++] = cur;
	}

	/*
	 * If the loop above collected no key (j == 0), the targetList contained
	 * no Var that referenced a grouping column -- e.g. a UNION ALL branch
	 * that projects only a literal label like SELECT 'A' FROM t GROUP BY k
	 * where GPORCA pruned k out of the output.
	 *
	 * Overwriting pcontext->keys/nkey with the empty result wipes the keys
	 * established by build_agg_project_options. Downstream effect: the
	 * synthetic plain_distinct branch in BuildAggregatation is gated on
	 * (pcontext->nkey > 0) and gets skipped; the Arrow agg is then built
	 * with empty keys + empty aggregations and routes to
	 * ScalarAggregateNode (aggregate_node.cc:3027). Its Finish() hard-codes
	 * ExecBatch{values={}, length=1}, so each segment emits a single
	 * 1-row, 0-column batch regardless of the true group count, and the
	 * partial agg above counts N segments instead of the real cardinality
	 * (the reproducer SQL in agg/vec_const_targetlist returned A=3, B=3
	 * instead of A=100, B=200 on a 3-segment cluster).
	 *
	 * Keep the original keys in that case. Guarding on j (the loop's
	 * actual output) rather than on a pre-loop "does targetList contain
	 * Var" probe keeps the check valid if future changes to the loop add
	 * further filtering of Var entries.
	 */
	if (j == 0)
	{
		pfree(keys);
		return;
	}

	pcontext->keys = keys;
	pcontext->nkey = j;
}


static GArrowExecuteNode*
build_orderby_node(PlanState *planstate, GArrowExecutePlan *plan,  GArrowExecuteNode *input)
{
	int nkeys;
	GList *sort_keys = NULL;
	g_autoptr(GError) error = NULL;
	g_autoptr(GArrowSortOptions) sortoption = NULL;
	g_autoptr(GArrowOrderbyNodeOptions) orderby_options = NULL;
	GArrowExecuteNode *orderby_node = NULL;
	g_autoptr(GArrowSchema) schema = NULL;

	VecSortState *node = (VecSortState *) planstate;
	Sort	   *plannode = (Sort *) node->base.ss.ps.plan;

	schema = garrow_execute_node_get_output_schema(input);
	nkeys = plannode->numCols;

	/* sort option */
	sort_keys = build_sort_keys(planstate, schema);
	sortoption = garrow_sort_options_new(sort_keys, partition_top_k, take_thread_num, two_phase_take);
	orderby_options = garrow_orderby_node_options_new(sortoption);
	orderby_node = garrow_execute_plan_build_orderby_node(plan, input, orderby_options, &error);
	if (error)
		elog(ERROR, "Failed to create orderby node, cause: %s", error->message);
	garrow_list_free_ptr(&sort_keys);
	return orderby_node;
}

/*
 * find_seqscan_for_topk - Traverse from Sort's child to find SeqScan.
 * Supports Sort→SeqScan and Sort→Result→SeqScan topologies.
 * Returns NULL if SeqScan not found (TopK RF silently disabled).
 */
static VecSeqScanState *
find_seqscan_for_topk(PlanState *sort_planstate)
{
	PlanState *child = outerPlanState(sort_planstate);

	/* Skip through Result node (projection) */
	if (child && IsA(child, ResultState))
		child = outerPlanState(child);

	if (child && IsA(child, SeqScanState))
		return (VecSeqScanState *)child;

	return NULL;
}

static GArrowExecuteNode*
build_topk_node(PlanState *planstate, GArrowExecutePlan *plan,
				GArrowExecuteNode *input, int64 topk_bound)
{
	GList *sort_keys = NULL;
	g_autoptr(GError) error = NULL;
	g_autoptr(GArrowSortOptions) sortoption = NULL;
	g_autoptr(GArrowTopKNodeOptions) topk_options = NULL;
	GArrowExecuteNode *topk_node = NULL;
	g_autoptr(GArrowSchema) schema = NULL;

	schema = garrow_execute_node_get_output_schema(input);

	/* Reuse build_sort_keys to construct sort key list */
	sort_keys = build_sort_keys(planstate, schema);
	sortoption = garrow_sort_options_new(sort_keys, 0, take_thread_num, two_phase_take);

	/* TopK Runtime Filter: try to get threshold state from PAX */
	gpointer threshold_ptr = NULL;
	int sort_column_index = -1;
	Oid collation = InvalidOid;

	if (enable_topk_runtime_filter)
	{
		VecSeqScanState *scan_state = find_seqscan_for_topk(planstate);
		if (scan_state && scan_state->scan_node_options)
		{
			Sort *sort = (Sort *)planstate->plan;
			AttrNumber sort_attno = sort->sortColIdx[0]; /* 1-based in Sort output */
			bool nulls_first = sort->nullsFirst[0];

			/* Map sort column from Sort's output back to table's physical attnum.
			 * sortColIdx references a column in Sort's input (SeqScan's output).
			 * SeqScan may project only a subset of columns, so the Var in
			 * Sort's targetlist has varattno = position in SeqScan's output,
			 * not the original table column. We look up SeqScan's targetlist
			 * to find the original table varattno. */
			AttrNumber table_attno = InvalidAttrNumber;
			{
				Plan *scan_plan = outerPlan(sort);
				/*
				 * If a Result node sits between Sort and SeqScan, sort_attno
				 * is a resno in Result's output, not SeqScan's. Remap it
				 * through Result's targetlist before descending — Var.varattno
				 * inside Result's tlist is the resno in SeqScan's output, so
				 * it can be reused as the new sort_attno. If the entry is an
				 * expression (not a Var), we cannot map to a physical column
				 * and must bail out.
				 */
				if (scan_plan && IsA(scan_plan, Result))
				{
					TargetEntry *res_tle =
						get_tle_by_resno(scan_plan->targetlist, sort_attno);
					if (res_tle && IsA(res_tle->expr, Var))
						sort_attno = ((Var *) res_tle->expr)->varattno;
					else
						goto skip_topk_rf;
					scan_plan = outerPlan(scan_plan);
				}

				if (scan_plan)
				{
					TargetEntry *tle = get_tle_by_resno(scan_plan->targetlist, sort_attno);
					if (tle && IsA(tle->expr, Var))
					{
						Var *var = (Var *)tle->expr;
						table_attno = var->varattno; /* 1-based physical column */
					}
				}
			}

			if (!AttributeNumberIsValid(table_attno) || table_attno <= 0)
			{
				/* Cannot map to physical column (expression sort key, etc.) */
				goto skip_topk_rf;
			}

			collation = sort->collations[0];

			/*
			 * NULLs occupy the top-K positions iff they sort first, which is
			 * exactly NULLS FIRST — independent of ASC/DESC. The RF reads a
			 * scalar threshold and cannot represent NULL, so disable it
			 * whenever NULLs could be in the top-K.
			 */
			if (!nulls_first)
			{
				threshold_ptr =
					garrow_scan_node_options_get_topk_threshold_state(
						scan_state->scan_node_options);
				sort_column_index = table_attno - 1;  /* 0-based physical column */
			}
skip_topk_rf:
			;  /* label requires a statement */
		}
	}

	/* Create TopKNode with optional runtime filter */
	g_autoptr(GArrowTopKRuntimeFilterOptions) rf_options = NULL;
	if (threshold_ptr)
		rf_options = garrow_topk_runtime_filter_options_new(threshold_ptr,
															sort_column_index,
															(guint32)collation);
	topk_options = garrow_topk_node_options_new(topk_bound, sortoption,
												rf_options);
	topk_node = garrow_execute_plan_build_topk_node(plan, input,
													topk_options, &error);
	if (error)
	{
		garrow_list_free_ptr(&sort_keys);
		elog(ERROR, "Failed to create topk node, cause: %s", error->message);
	}

	/*
	 * Record the K value on the Sort's VecSortState so that show_sort_info
	 * can emit "Vec Sort Method: TopK K: N" in EXPLAIN output.  This is the
	 * single source of truth for whether a given Sort took the TopK path —
	 * the EXPLAIN display and the actual Arrow node construction both happen
	 * here.
	 */
	((VecSortState *) planstate)->topk_bound = topk_bound;

	garrow_list_free_ptr(&sort_keys);
	return topk_node;
}

/*
 * Returns true if every key column in the agg's input schema is one
 * sonic's SonicGroupByNode currently knows how to scatter / hash /
 * compare. Mirrors compute/sonic/exec_node.cc::ArrowTypeToKeyType.
 */
static bool
sonic_supports_key_type(GArrowType id)
{
	switch (id)
	{
		case GARROW_TYPE_INT16:
		case GARROW_TYPE_INT32:
		case GARROW_TYPE_INT64:
		case GARROW_TYPE_DATE32:
		case GARROW_TYPE_DATE64:
		case GARROW_TYPE_TIME32:
		case GARROW_TYPE_TIME64:
		case GARROW_TYPE_TIMESTAMP:
		case GARROW_TYPE_STRING:
		case GARROW_TYPE_BINARY:
		case GARROW_TYPE_NUMERIC128:
			return true;
		default:
			return false;
	}
}

/*
 * Sonic's hash_sum / hash_mean_numeric currently accept signed integer
 * inputs (and the date / time variants that map onto an integer width).
 */
static bool
sonic_supports_signed_int_input(GArrowType id)
{
	switch (id)
	{
		case GARROW_TYPE_INT16:
		case GARROW_TYPE_INT32:
		case GARROW_TYPE_INT64:
		case GARROW_TYPE_DATE32:
		case GARROW_TYPE_DATE64:
		case GARROW_TYPE_TIME32:
		case GARROW_TYPE_TIME64:
		case GARROW_TYPE_TIMESTAMP:
			return true;
		default:
			return false;
	}
}

/*
 * Sonic's hash_min currently accepts utf8 / binary inputs only.
 */
static bool
sonic_supports_text_input(GArrowType id)
{
	return id == GARROW_TYPE_STRING || id == GARROW_TYPE_BINARY;
}

/*
 * Decide whether sonic's SonicGroupByNode can handle this aggregate
 * shape. If anything is outside its current capability we fall back
 * to normal-mode hash agg so the query still runs (correctness over
 * speed). The set checked here MUST be a subset of what sonic's
 * MakeAggregateFunctions / ArrowTypeToKeyType actually implement —
 * keep these two in sync when extending sonic.
 */
static bool
is_sonic_compatible(List *aggInfos, PlanBuildContext *pcontext)
{
	ListCell *l;

	/*
	 * The "no agg, just GROUP BY" path appends a synthetic
	 * hash_distinct in BuildAggregatation; sonic doesn't implement
	 * hash_distinct, so route those queries to normal mode.
	 */
	if (aggInfos == NIL)
		return false;

	/* Group keys: every key column type must be one sonic supports. */
	for (int i = 0; i < pcontext->nkey; i++)
	{
		g_autoptr(GArrowField) field = garrow_schema_get_field_by_name(
			pcontext->inputschema, pcontext->keys[i]);
		if (!field)
			return false;
		g_autoptr(GArrowDataType) type = garrow_field_get_data_type(field);
		GArrowType id = garrow_data_type_get_id(type);
		if (!sonic_supports_key_type(id))
			return false;
	}

	/* Aggregates: function name + input column type must both be supported. */
	foreach(l, aggInfos)
	{
		VecAggInfo *agginfo = (VecAggInfo *) lfirst(l);
		const char *fn = agginfo->aname;

		if (strcmp(fn, "hash_count") == 0)
		{
			/*
			 * Sonic handles both shapes of count:
			 *   count(*)   -> CountOptions::ALL        -> CountStarAggregate
			 *   count(col) -> CountOptions::ONLY_VALID -> CountColAggregate
			 *                                            (reads only the
			 *                                             validity bitmap,
			 *                                             type-agnostic)
			 * MakeAggregateFunctions in compute/sonic/exec_node.cc picks
			 * between them on agg.options->mode, which our
			 * build_all_count_options sets per list_length(aggref->args).
			 * Neither path needs an input-type check here.
			 *
			 * count(distinct col) is the aggregate "hash_count_distinct"
			 * (a different function name); it falls through to the
			 * unconditional fallback at the bottom of this loop, routing
			 * DISTINCT counts to normal mode (sonic does not implement
			 * hash_count_distinct).
			 */
			continue;
		}

		/*
		 * Every other supported sonic agg requires a resolved input
		 * column. If the aggref has no args (shouldn't happen for
		 * hash_sum / hash_min / hash_mean_numeric), bail to be safe.
		 */
		if (!agginfo->aggref || !agginfo->aggref->args)
			return false;
		TargetEntry *tle = (TargetEntry *) linitial(agginfo->aggref->args);
		Oid pg_type = exprType((Node *) tle->expr);
		GArrowType in_id = PGTypeToArrowID(pg_type);

		if (strcmp(fn, "hash_sum") == 0)
		{
			/* signed int (sonic SumIntAggregate) or numeric128
			 * (SumNumeric128Aggregate, used by single-stage SUM(numeric)
			 * and final stage of two-stage SUM(int8)). */
			if (!sonic_supports_signed_int_input(in_id) &&
				in_id != GARROW_TYPE_NUMERIC128)
				return false;
		}
		else if (strcmp(fn, "hash_sum_64") == 0)
		{
			/* Partial stage of SUM(int8): int64 input → numeric128.
			 * Also accepts numeric128 input (Arrow registers it for
			 * symmetry; sonic routes it to SumNumeric128Aggregate). */
			if (in_id != GARROW_TYPE_INT64 &&
				in_id != GARROW_TYPE_DATE64 &&
				in_id != GARROW_TYPE_TIME64 &&
				in_id != GARROW_TYPE_TIMESTAMP &&
				in_id != GARROW_TYPE_NUMERIC128)
				return false;
		}
		else if (strcmp(fn, "hash_min") == 0)
		{
			if (!sonic_supports_text_input(in_id))
				return false;
		}
		else if (strcmp(fn, "hash_mean_numeric") == 0)
		{
			if (!sonic_supports_signed_int_input(in_id))
				return false;
		}
		else if (strcmp(fn, "hash_avg_trans") == 0)
		{
			/*
			 * Two-phase AVG partial. Sonic supports signed integer
			 * input (output struct<int64, int64>) and numeric128 input
			 * (output struct<numeric128, int64>). Float / decimal128
			 * not yet implemented.
			 */
			if (!sonic_supports_signed_int_input(in_id) &&
				in_id != GARROW_TYPE_NUMERIC128)
				return false;
		}
		else if (strcmp(fn, "hash_avg_final") == 0)
		{
			/* PG-level type for the partial state is bigint[]; Arrow
			 * sees struct<sum, count>. Sonic Consume validates the
			 * runtime struct layout — the PG type check is a facade. */
		}
		else
		{
			/* Any other function name (hash_max, hash_avg_trans_stddev, etc.). */
			return false;
		}
	}
	return true;
}

static GArrowExecuteNode *
BuildAggregatation(List *aggInfos, GArrowExecuteNode *input, PlanBuildContext *pcontext)
{
	ListCell	   *l;
	GList *aggregations = NULL;
	g_autoptr(GArrowExecuteNode) aggregate = NULL;
	g_autoptr(GArrowExecuteNode) orderbynode = NULL;
	g_autoptr(GArrowAggregation) agg_func = NULL;
	g_autoptr(GArrowAggregateNodeOptions) options = NULL;
	g_autoptr(GError) error = NULL;

	/* build aggregations */
	foreach(l, aggInfos)
	{
		VecAggInfo *agginfo = (VecAggInfo *) lfirst(l);

		/*Fixme only normal agg now. */
		if (true)
		{
			agg_func = garrow_aggregation_new(
					   agginfo->aname,
					   agginfo->options,
					   agginfo->inname,
					   agginfo->outname);
			aggregations = garrow_list_append_ptr(aggregations, agg_func);
		}
	}

	/* no aggref in targetlist, append plain/hash distinct for group by columns.
	 * Out name will not be used, use dummy.
	 */
	if (!IsA(pcontext->planstate, WindowAggState) && !IsA(pcontext->planstate, WindowHashAggState) && ((pcontext->nkey > 0) && (!aggregations)))
	{
		agg_func = garrow_aggregation_new(
			pcontext->aggstrategy == AGG_SORTED ? "plain_distinct" : "hash_distinct",
			NULL,
			pcontext->keys[0],
			"_outagg_dummy");
		aggregations = garrow_list_append_ptr(aggregations, agg_func);
	}

	bool sonic_ok = false;
	if (pcontext->aggstrategy == AGG_HASHED)
	{
		sonic_ok = is_sonic_compatible(aggInfos, pcontext);
		if (!sonic_ok)
			elog(DEBUG1, "sonic: query keys/aggs not all supported, "
						 "falling back to normal-mode hash agg");
	}

	options = build_aggregatation_options(aggregations, pcontext, sonic_ok);

	aggregate = garrow_execute_plan_build_aggregate_node(pcontext->plan,
														input,
														options,
														&error);
	if (error)
		elog(ERROR, "Failed to create agg node, cause: %s", error->message);

	/* release agginfos to prevent building aggregations repeatedly*/
	garrow_list_free_ptr(&aggregations);
	return garrow_move_ptr(aggregate);
}


/*
 * Walker context for find_agg_parent_limit().  base must come first so
 * the node can be passed to plan_tree_walker.
 */
typedef struct
{
	plan_tree_base_prefix	base;
	Plan				   *target;	/* HashAgg Plan we're searching for */
	Limit				   *found;	/* output: the parent Limit if any */
} agg_parent_limit_context;

/*
 * plan_tree_walker callback: stop and record the Limit node whose
 * outerPlan is our target Agg.  Pre-order so the *immediate* Limit
 * parent is the first one we hit (a chain "Limit -> ... -> Agg" can
 * only have one immediate parent anyway).
 */
static bool
agg_parent_limit_walker(Node *node, agg_parent_limit_context *ctx)
{
	if (node == NULL)
		return false;

	if (IsA(node, Limit) && outerPlan(node) == ctx->target)
	{
		ctx->found = (Limit *) node;
		return true;
	}
	return plan_tree_walker(node, agg_parent_limit_walker, ctx, true);
}

/*
 * Search the full Plan tree for a Limit whose immediate outerPlan is
 * 'target'.  Returns the parent Limit if found, else NULL.  Walks the
 * full tree (not just the slice this segment runs) because on the QD
 * ExecutorStart initializes every PlanState in the dispatched
 * PlannedStmt, including nodes that belong to remote slices.
 *
 * The MPP "push limit through motion" optimization produces
 *     Limit (top, count=O+C)
 *       -> Gather Motion
 *         -> Limit (segment, count=O+C, offset=0)
 *           -> HashAgg
 * so the Agg's *immediate* parent Limit lives below the motion, not at
 * the top of the tree.  A direct "topPlan->outerPlan == target" check
 * would miss it; the walker finds it regardless of intervening nodes.
 */
static Limit *
find_agg_parent_limit(PlannedStmt *stmt, Plan *target)
{
	agg_parent_limit_context ctx;

	exec_init_plan_tree_base(&ctx.base, stmt);
	ctx.target = target;
	ctx.found = NULL;
	agg_parent_limit_walker((Node *) stmt->planTree, &ctx);
	return ctx.found;
}

static GArrowAggregateNodeOptions *
build_aggregatation_options(GList *aggregations, PlanBuildContext *pcontext,
							bool sonic_ok)
{
	g_autoptr(GArrowAggregateNodeOptions) options = NULL;
	g_autoptr(GError) error = NULL;
	int64 limit_count = 0;

	/*
	 * Detect Limit+HashAgg fusion: walk the Plan tree to find any Limit
	 * whose direct child is our HashAgg, then pass that Limit's
	 * offset+count to Arrow so GroupByNode only tracks the first N
	 * groups.  In MPP the immediate parent Limit usually sits below a
	 * Gather Motion (see find_agg_parent_limit for the shape), so we
	 * cannot just inspect the top of the tree.
	 *
	 * We work on Plan, not PlanState: PlanState lacks a parent pointer,
	 * and the Agg and Limit are built in separate BuildVecPlan calls so
	 * no live PlanState chain exists at this point.
	 */
	if (limit_hashagg_max_total > 0 &&
		pcontext->aggstrategy == AGG_HASHED &&
		pcontext->planstate->state &&
		pcontext->planstate->state->es_plannedstmt)
	{
		Limit *limitPlan = find_agg_parent_limit(
			pcontext->planstate->state->es_plannedstmt,
			pcontext->planstate->plan);
		if (limitPlan)
		{
			/*
			 * Extract constant limit/offset from the Plan node.
			 * limitCount/limitOffset are expression nodes; for constant
			 * values they are Const nodes.
			 */
			int64 lim_offset = 0;
			int64 lim_count = 0;
			bool valid = true;

			if (limitPlan->limitCount && IsA(limitPlan->limitCount, Const))
			{
				Const *c = (Const *)limitPlan->limitCount;
				if (!c->constisnull)
					lim_count = DatumGetInt64(c->constvalue);
				else
					valid = false;
			}
			else if (limitPlan->limitCount)
			{
				/* Non-constant LIMIT expression — skip optimization */
				valid = false;
			}
			else
			{
				/* No LIMIT COUNT (LIMIT ALL) — skip */
				valid = false;
			}

			if (valid && limitPlan->limitOffset)
			{
				if (IsA(limitPlan->limitOffset, Const))
				{
					Const *c = (Const *)limitPlan->limitOffset;
					if (!c->constisnull)
						lim_offset = DatumGetInt64(c->constvalue);
				}
				else
				{
					/* Non-constant OFFSET expression — skip optimization */
					valid = false;
				}
			}

			if (valid && lim_count > 0)
			{
				int64 total;
				if (!pg_add_s64_overflow(lim_offset, lim_count, &total) &&
					total > 0)
				{
					if (total <= limit_hashagg_max_total)
					{
						limit_count = total;
						/*
						 * Record the effective limit on VecAggState so
						 * show_hashagg_info can surface the fusion in
						 * EXPLAIN output.  Cast is safe: the AGG_HASHED
						 * gate above guarantees planstate is an AggState
						 * and BuildVecPlan only invokes us through
						 * ExecInitVecAgg, which allocates
						 * sizeof(VecAggState).
						 */
						((VecAggState *) pcontext->planstate)->limit_count = limit_count;
						elog(DEBUG1, "LimitAgg: fusing Limit(%ld)+HashAgg, limit_count=%ld",
							 (long)lim_count, (long)limit_count);
					}
					else
					{
						elog(DEBUG1, "LimitAgg: total %ld exceeds vector.limit_hashagg_max_total %d, skipping",
							 (long)total, limit_hashagg_max_total);
					}
				}
				/* Overflow or non-positive total: silently skip fusion. */
			}
		}
	}

	if (IsA(pcontext->planstate, WindowAggState))
	{
		int mode = garrow_aggregate_get_window_mode();
		options =
			garrow_general_aggregate_node_options_new(aggregations,
													pcontext->keys,
													pcontext->nkey,
													mode,
													pcontext->orderby_sortoption,
													/* sonic_motion_direct_send */ NULL,
													&error);
	}
	else if (IsA(pcontext->planstate, WindowHashAggState))
	{
		if (winagg_spill_memory_mb <= 0)
		{
			/*
			 * Spill disabled by GUC — use the plain parallel-window variant
			 * (same call WindowAggState uses, no spill_file_base / no mkdir)
			 * so the spill plumbing never sees a zero budget.
			 */
			options =
				garrow_general_aggregate_node_options_new(aggregations,
														pcontext->keys,
														pcontext->nkey,
														garrow_aggregate_get_parallel_window_mode(),
														pcontext->orderby_sortoption,
														/* sonic_motion_direct_send */ NULL,
														&error);
		}
		else
		{
			char spill_dir[MAXPGPATH];
			char spill_file_base[MAXPGPATH];

			/*
			 * Build absolute path for SpillableBatchStore / Path B merge-sort.
			 * Files land in $DataDir/base/pgsql_tmp/ with prefix
			 * pgsql_tmp_vec_spill_{PID}_{plan_node_id}_ so on_proc_exit's
			 * CleanupArrowSpillFiles (and GP's post-crash RemovePgTempFiles
			 * sweep) pick them up. We deliberately do NOT wire any file_ops
			 * here: arrow substitutes its own POSIX-backed
			 * DefaultExternalSortFileOps() when the callback set is empty,
			 * matching the SortOptions pattern.
			 *
			 * pgsql_tmp is created lazily by OpenTemporaryFile, so on a fresh
			 * cluster (no prior temp file activity) the directory may not
			 * exist yet. Arrow's file_ops has a best-effort mkdir fallback,
			 * but do it explicitly here so permission / disk-full issues
			 * surface with a proper PG error message instead of a generic
			 * ENOENT from open().
			 */
			snprintf(spill_dir, sizeof(spill_dir), "%s/base/%s",
					 DataDir, PG_TEMP_FILES_DIR);
			if (MakePGDirectory(spill_dir) < 0 && errno != EEXIST)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not create temporary file directory \"%s\": %m",
								spill_dir)));
			snprintf(spill_file_base, sizeof(spill_file_base),
					 "%s/%s_%s_%d_%d", spill_dir,
					 PG_TEMP_FILE_PREFIX, VEC_SPILL_FILE_PREFIX,
					 MyProcPid, pcontext->planstate->plan->plan_node_id);

			options =
				garrow_general_aggregate_node_options_new_with_spill(
														aggregations,
														pcontext->keys,
														pcontext->nkey,
														garrow_aggregate_get_parallel_window_mode(),
														pcontext->orderby_sortoption,
														NULL,
														spill_file_base,
														(gint64) winagg_spill_memory_mb * 1024L * 1024L,
														&error);
		}
	}
	else if (pcontext->aggstrategy == AGG_SORTED)
		options = garrow_ordered_aggregate_node_options_new(aggregations,
															pcontext->keys,
															pcontext->nkey,
															&error);
	else if (limit_count > 0 || !sonic_ok)
	{
		/*
		 * Normal-mode hash agg fallback. Two reasons we land here:
		 *   (a) Limit+HashAgg fusion: GroupByNode tracks only the first N
		 *       groups via AggregateNodeOptions::limit_count. Sonic does
		 *       not yet honor limit_count, so keep the legacy normal-mode
		 *       path when the planner enables this fusion.
		 *   (b) The query uses a key type or aggregate function sonic
		 *       hasn't implemented yet (is_sonic_compatible returned
		 *       false). Falling back keeps the query running at normal-
		 *       mode speed instead of erroring out at runtime — this is
		 *       important for ClickBench full-suite runs.
		 */
		options = garrow_aggregate_node_options_new(aggregations,
													pcontext->keys,
													pcontext->nkey,
													limit_count,
													&error);
		if (pcontext->aggstrategy == AGG_HASHED)
			((VecAggState *) pcontext->planstate)->method =
				(limit_count > 0) ? VEC_AGG_METHOD_LIMIT_FUSION
								  : VEC_AGG_METHOD_NORMAL;
	}
	else
	{
		/*
		 * Default hash-agg path: route to Sonic
		 * (compute/sonic/exec_node.cc -> SonicGroupByNode).
		 * Sonic's per-thread share-nothing sink + 256-partition parallel
		 * finalize replaces Arrow's GrouperFastImpl + serial Merge.
		 *
		 * Sonic Motion Direct-Send: when the parent HASH Motion's
		 * ExecInitVecMotion has published a hint (all gates passed,
		 * see sonic_motion_direct_send_design.md §2.3), build a
		 * GArrowSonicMotionDirectSendOptions from it and pass it into
		 * the aggregate options constructor so Sonic emits each batch
		 * pre-bucketed by target segment. No hint → pass NULL, default
		 * per-partition emit behavior is preserved.
		 */
		int mode = garrow_aggregate_get_sonic_mode();
		g_autoptr(GArrowSonicMotionDirectSendOptions) ds_options = NULL;
		const VecMotionDirectSendHint *hint =
			vec_motion_direct_send_lookup(pcontext->planstate->plan);
		if (hint != NULL)
		{
			gint32 *idx = (gint32 *) palloc(sizeof(gint32) * hint->numHashCols);
			for (int i = 0; i < hint->numHashCols; ++i)
				idx[i] = (gint32) hint->hash_grpcol_idx[i];
			ds_options = garrow_sonic_motion_direct_send_options_new(
				(gint32) hint->numHashSegments,
				(gint32) hint->reduce_alg,
				idx, (gsize) hint->numHashCols,
				hint->target_segment_col_name);
			/*
			 * Only mark the project node as a passthrough for the hidden
			 * segment column when Sonic will actually emit it.  If the
			 * constructor failed (e.g. internal validation rejected the
			 * arguments and returned NULL), garrow_general_aggregate_node_options_new
			 * gets NULL too and Sonic never prepends the column — leaving the
			 * passthrough flag on would make build_project_options reference
			 * a non-existent field and fail Arrow plan construction.
			 */
			if (ds_options != NULL)
			{
				pcontext->sonic_target_segment_passthrough = true;
				pcontext->sonic_target_segment_col_name =
					hint->target_segment_col_name;
			}
			pfree(idx);
		}

		options = garrow_general_aggregate_node_options_new(aggregations,
															pcontext->keys,
															pcontext->nkey,
															mode,
															/* sort_options */ NULL,
															ds_options,
															&error);
		if (pcontext->aggstrategy == AGG_HASHED)
			((VecAggState *) pcontext->planstate)->method = VEC_AGG_METHOD_SONIC;
	}
	if (error)
		elog(ERROR, "Failed to create agg node options, cause: %s", error->message);

	if (pcontext->keys)
		pfree(pcontext->keys);

	return garrow_move_ptr(options);
}

static GArrowStoreType
to_arrow_storetype(StoreType type)
{
	switch (type)
	{
		case IN_MEMORY:
			return GARROW_IN_MEMORY;
			break;
		case IN_SINGLE:
			return GARROW_IN_SINGLE;
			break;
		case IN_CHUNK:
			return GARROW_IN_CHUNK;
			break;
		default:
			elog(ERROR, "store type not supported by arrow storefile %d", type);
			break;
	}

	/* never run here */
	return GARROW_IN_MEMORY;
}

static bool
check_innervar_walker(Node *node, void *context)
{
	if (node == NULL)
		return true;

	switch (nodeTag(node))
	{
		case T_Var:
		{
			Var *var = (Var *) node;
			if (var->varno == INNER_VAR)
			{
				return false;
			}  
		}
		default:
			break;
	}
	return expression_tree_walker(node, check_innervar_walker, context);
}

static GArrowJoinType
to_arrow_jointype(JoinType type, List *targetlist, List *joinqual)
{
	bool no_need_right_payload = check_innervar_walker((Node *) targetlist, NULL);
	switch (type)
	{
		case JOIN_INNER:
			return GARROW_INNER_JOIN;
		case JOIN_LEFT:
			return GARROW_LEFT_OUTER_JOIN;
		case JOIN_RIGHT:
			return GARROW_RIGHT_OUTER_JOIN;
		case JOIN_FULL:
			return GARROW_FULL_OUTER_JOIN;
		case JOIN_SEMI:
		{
			if (no_need_right_payload)
			{
				return GARROW_LEFT_SEMI_JOIN;
			}
			else
			{
				return GARROW_FULL_SEMI_JOIN;
			}
		}
		case JOIN_ANTI:
		{
			if (no_need_right_payload)
			{
				return GARROW_LEFT_ANTI_JOIN;
			}
			else
			{
				return GARROW_FULL_ANTI_JOIN;
			}
		}
		case JOIN_LASJ_NOTIN:
			return GARROW_LASJ_NOTIN_JOIN;

		/*
		 * Right-flipped semi/anti hash joins (PG18 JOIN_RIGHT_SEMI /
		 * JOIN_RIGHT_ANTI). Physically: build = LHS (left subtree), probe =
		 * RHS (right subtree). Arrow Acero's RIGHT_SEMI / RIGHT_ANTI
		 * algorithm puts build on its `right input` and emits right-side
		 * (build) rows after finalize -- so we map PG RIGHT_SEMI to Arrow
		 * RIGHT_SEMI, and the caller is responsible for SWAPPING the left/
		 * right plan subtrees + hash keys when feeding Arrow.
		 *
		 * The Mark Join algorithm itself (probe sets has_match visited bit,
		 * finalize scans HT emitting visited (SEMI) or unvisited (ANTI)
		 * build rows) is implemented upstream in
		 * Apache Arrow 5.x (swiss_join.cc:2252 FilterRightSemiAnti +
		 * :2942-3019 ScanTask). Zero changes needed inside Arrow itself.
		 */
		case JOIN_RIGHT_SEMI:
			return GARROW_RIGHT_SEMI_JOIN;
		case JOIN_RIGHT_ANTI:
			return GARROW_RIGHT_ANTI_JOIN;
		case JOIN_RIGHT_ANTI_NOTIN:
			/* ORCA cost model (M3) returns ∞ for this op -- planner should
			 * never produce it. Hard-fail if it does to catch regressions. */
			elog(ERROR, "JOIN_RIGHT_ANTI_NOTIN not implemented in vec engine "
						"(M3 cost should have suppressed it)");
			break;	/* unreachable, silence -Werror=implicit-fallthrough */

		default:
			elog(ERROR, "join type not supported by arrow join %d", type);
	}

	/* never run here */
	return GARROW_INNER_JOIN;
}

/*
 * on_proc_exit callback to clean up Arrow hash join spill files.
 *
 * Arrow creates spill files outside PG's VFD system, so AtProcExit_Files
 * won't clean them. On normal ERROR, C++ destructors handle cleanup via
 * VecExecEndNode. On FATAL, proc_exit runs this callback since C++
 * destructors are skipped by longjmp/proc_exit.
 */
static void
CleanupArrowSpillFiles(int code, Datum arg)
{
	char		spill_dir[MAXPGPATH];
	DIR		   *dir;
	struct dirent *de;
	char		pid_pattern[64];

	snprintf(spill_dir, MAXPGPATH, "%s/base/%s", DataDir, PG_TEMP_FILES_DIR);
	snprintf(pid_pattern, sizeof(pid_pattern),
			 "%s_%s_%d_", PG_TEMP_FILE_PREFIX, VEC_SPILL_FILE_PREFIX, MyProcPid);

	dir = AllocateDir(spill_dir);
	if (dir == NULL)
		return;

	while ((de = ReadDirExtended(dir, spill_dir, LOG)) != NULL)
	{
		if (strncmp(de->d_name, pid_pattern, strlen(pid_pattern)) == 0)
		{
			char	path[MAXPGPATH];

			snprintf(path, MAXPGPATH, "%s/%s", spill_dir, de->d_name);
			unlink(path);
			elog(DEBUG1, "cleaned up Arrow spill file: %s", path);
		}
	}
	FreeDir(dir);
}

static GArrowExecuteNode *
BuildHashjoin(PlanBuildContext *pcontext, GArrowExecuteNode *left, GArrowExecuteNode *right, List *joinqual)
{
	GError *error = NULL;
	HashJoinState *node;
	VecHashJoinState *vnode;
	GArrowJoinType type;
	GArrowExecuteNode *hashjoin_node;
	g_autolist(GObject) lkeys = NULL;
	g_autolist(GObject) rkeys = NULL;
	g_autoptr(GArrowSchema) left_schema = NULL;
	g_autoptr(GArrowSchema) right_schema = NULL;
	g_autoptr(GArrowHashJoinNodeOptions) hashjoin_options = NULL;
	g_autoptr(GArrowExpression) filter_expr = NULL;
	GList *keycmps = NULL;

	node = (HashJoinState *) pcontext->planstate;
	vnode = (VecHashJoinState *) node;
	type = to_arrow_jointype(node->js.jointype, node->js.ps.plan->targetlist, joinqual);

	/*
	 * NOTE: for RIGHT_SEMI/ANTI, the inputs+hashkeys are already swapped at
	 * the BuildJoinPlan top level (see swap_for_right_semi below) -- by the
	 * time we reach BuildHashjoin, 'left' is the Arrow probe input and
	 * 'right' is the Arrow build input (= PG LHS), with column naming
	 * (LEFT_ / RIGHT_ prefixes) and hashkeys all consistent with the
	 * post-swap layout. We must NOT swap again here.
	 */
	left_schema = garrow_execute_node_get_output_schema(left);
	right_schema = garrow_execute_node_get_output_schema(right);
	lkeys = garrow_schema_get_fields(left_schema);
	rkeys = garrow_schema_get_fields(right_schema);
	pcontext->left_proj_schema = left_schema;
	pcontext->right_proj_schema = right_schema;
	if (joinqual)
		filter_expr = build_filter_expression(joinqual, pcontext);
	if (node->hj_nonequijoin)
	{
		for (int i = 0; i < g_list_length(lkeys); ++i)
		{
			gpointer cmp = (gpointer)GARROW_JOIN_CMP_IS;
			keycmps = garrow_list_append_ptr(keycmps, cmp);
		}
	}
	{
		char spill_dir[MAXPGPATH];
		char spill_file_prefix[MAXPGPATH];

		snprintf(spill_dir, MAXPGPATH, "%s/base/%s", DataDir, PG_TEMP_FILES_DIR);
		snprintf(spill_file_prefix, sizeof(spill_file_prefix),
				 "%s_%s_%d_%d", PG_TEMP_FILE_PREFIX, VEC_SPILL_FILE_PREFIX, MyProcPid, node->js.ps.plan->plan_node_id);

		hashjoin_options =
			garrow_hash_join_node_options_new(type, pcontext->left_hashkeys, pcontext->right_hashkeys, keycmps, filter_expr,
											  "", "", false,
											  (gint64)hashjoin_spill_memory_mb * 1024 * 1024,
											  spill_dir,
											  spill_file_prefix,
											  &error);
	}
	if (error)
		elog(ERROR, "Failed to create the hashjoin node, cause: %s", error->message);

	hashjoin_node =
		garrow_execute_plan_build_hash_join_node(pcontext->plan, left, right,
												 hashjoin_options, &error);
	if (error)
		elog(ERROR, "Failed to create the hashjoin node, cause: %s", error->message);

	garrow_list_free_ptr(&pcontext->left_hashkeys);
	garrow_list_free_ptr(&pcontext->right_hashkeys);
	return hashjoin_node;
}

static bool *
get_sortorder_by_opoid(int num, Oid *ops)
{
	bool *order_reverse = palloc0(sizeof(bool) * num);
	for (int i = 0; i < num; ++i)
	{
		Oid			opfamily;
		Oid			opcintype;
		int16		strategy;

		/* Find the operator in pg_amop */
		if (!get_ordering_op_properties(ops[i], &opfamily, &opcintype, &strategy))
			elog(ERROR, "operator %u is not a valid ordering operator", ops[i]);
		order_reverse[i] = (strategy == BTGreaterStrategyNumber);
	}

	return order_reverse;
}

static bool *
create_order_reverse(Plan *plan, int *numCols, AttrNumber **sortColIdx)
{
	bool *order_reverse = NULL;

	if (IsA(plan, Sort))
	{
		Sort *sort   = (Sort *)plan;
		*numCols     = sort->numCols;
		*sortColIdx  = sort->sortColIdx;
		order_reverse = get_sortorder_by_opoid(sort->numCols,
		                                       sort->sortOperators);
	}
	else if (IsA(plan, Motion))
	{
		Motion *motion = (Motion *)plan;
		*numCols       = motion->numSortCols;
		*sortColIdx    = motion->sortColIdx;
		order_reverse = get_sortorder_by_opoid(motion->numSortCols,
		                                       motion->sortOperators);
	}
	else
	{
		Assert(0); /* never run here */
		const char *nodestr = nodeTypeToString(plan);
		elog(ERROR, "invalid child node type: %s under WindowAgg.", nodestr);
	}

	return order_reverse;
}

static bool *
get_nulls_first(Plan *plan) 
{

	if (IsA(plan, Sort))
	{
		Sort *sort   = (Sort *)plan;
		return sort->nullsFirst;
	}
	else if (IsA(plan, Motion))
	{
		Motion *motion = (Motion *)plan;
		return motion->nullsFirst;
	}
	return NULL;
}

static AttrNumber
find_input_attno_in_targetlist(Plan *plan, AttrNumber attno)
{
	if (attno > list_length(plan->targetlist))
		return -1;

	TargetEntry *tle = (TargetEntry *)list_nth(plan->targetlist, attno - 1);
	if (!IsA(tle->expr, Var))
		return -1;

	Var *var = (Var *)tle->expr;
	if (var->varattno == 0)
		return -1;

	return var->varattno;
}

static void
get_windowagg_sortorder(PlanBuildContext *pcontext, SortKey *sortKey)
{
	int numCols              = 0;
	AttrNumber *sortColIdx   = NULL;
	bool *order_reverse      = NULL;
	bool *nulls_first 		 = NULL;

	Assert(IsA(pcontext->planstate->plan, WindowAgg));
	WindowAgg *wagg = (WindowAgg *) pcontext->planstate->plan;

	for (int i = 0; i < wagg->ordNumCols; ++i)
	{
		/*
		 * for every sort column in wagg, do:
		 * 1. find attno in the input slot of wagg;
		 * 2. find attno in the input slot of sort/motion with attno in wagg by
		 *    the targetlist of sort/motion;
		 */
		AttrNumber attno = wagg->ordColIdx[i];

		Plan *child = (Plan *)outerPlan(wagg);
		while (child)
		{
			AttrNumber input_attno = -1;
			input_attno = find_input_attno_in_targetlist(child, attno);
			if (input_attno == -1)
				elog(ERROR, "order column: %d in WindowAgg NOT found"
				            " in the lower node", wagg->ordColIdx[i]);

			attno = input_attno;

			if ((IsA(child, Agg) && ((Agg *)child)->aggstrategy == AGG_SORTED) ||
				IsA(child, WindowAgg))
				child = outerPlan(child);
			else
				break;
		}

		if (!nulls_first)
			nulls_first = get_nulls_first(child);

		if (!order_reverse)
			order_reverse = create_order_reverse(child, &numCols, &sortColIdx);

		/*
		 * find the position of the sort order by the attno
		 */
		int nth = 0;
		for (; nth < numCols; ++nth)
		{
			if (sortColIdx[nth] == attno)
				break;
		}
		if (nth == numCols)
			elog(ERROR, "order column: %d in WindowAgg NOT found"
			            " in the lower node", wagg->ordColIdx[i]);

		if (order_reverse[nth])
			sortKey->orders[i] = GARROW_SORT_ORDER_DESCENDING;
		else
			sortKey->orders[i] = GARROW_SORT_ORDER_ASCENDING;
		
		if (nulls_first && nulls_first[i])
			sortKey->nulls_first[i] = GARROW_SORT_ORDER_AT_START;
		else
			sortKey->nulls_first[i] = GARROW_SORT_ORDER_AT_END;
	}
}

const char *
nodeTypeToString(const void *obj)
{
	char *nodestr = nodeToString(obj);
	nodestr++;
	char *end = strchr(nodestr, ' ');
	if (end)
		*end = '\0';

	return nodestr;
}
static GArrowExecuteNode *
BuildNestLoopjoin(PlanBuildContext *pcontext, GArrowExecuteNode *left, GArrowExecuteNode *right, List *joinqual)
{
	GError *error = NULL;
	NestLoopState *node;
	GArrowJoinType type;
	GArrowExecuteNode *nestedloopjoin_node;
	g_autoptr(GArrowSchema) left_schema = NULL;
	g_autoptr(GArrowSchema) right_schema = NULL;
	g_autoptr(GArrowNestedLoopJoinNodeOptions) nest_loop_options = NULL;
	g_autoptr(GArrowExpression) filter_expr = NULL;

	node = (NestLoopState *) pcontext->planstate;
	left_schema = garrow_execute_node_get_output_schema(left);
	right_schema = garrow_execute_node_get_output_schema(right);
	pcontext->left_proj_schema = left_schema;
	pcontext->right_proj_schema = right_schema;
	type  = to_arrow_jointype(node->js.jointype, node->js.ps.plan->targetlist, joinqual);
	if (joinqual)
		filter_expr = build_filter_expression(joinqual, pcontext);
	else 
		filter_expr = build_literal_expression(true, false, BOOLOID, -1);
	nest_loop_options =
		garrow_nested_loop_join_node_options_new(type, filter_expr, "", "", &error);
	if (error)
		elog(ERROR, "Failed to create the nestedloopjoin node, cause: %s", error->message);
	nestedloopjoin_node =
		garrow_execute_plan_build_nested_loop_join_node(pcontext->plan, left, right,
														nest_loop_options, &error);

	if (error)
		elog(ERROR, "Failed to create the nestedloopjoin node, cause: %s", error->message);
	return nestedloopjoin_node;
}

GArrowSchema *
GetDummySchema(void)
{
	if (dummy_schema)
		return dummy_schema;
	// only call once
	g_autoptr(GArrowDataType) null_type = NULL;
	g_autoptr(GArrowField) field = NULL;
	GList *fields = NULL;
	null_type = GARROW_DATA_TYPE(garrow_null_data_type_new());
	field = garrow_field_new("dummy", null_type);
	fields = garrow_list_append_ptr(fields, field);
	dummy_schema = garrow_schema_new(fields);
	garrow_list_free_ptr(&fields);
	return dummy_schema;
}

void
FreeDummySchema(void) 
{
	if (dummy_schema)
		ARROW_FREE(GArrowSchema, &dummy_schema);
	dummy_schema = NULL;	
}

void 
dummy_schema_xact_cb(XactEvent event, void *arg)
{
	FreeDummySchema();
}

static void
SetArrowPlan(PlanState *ps, GArrowExecutePlan *plan)
{
	Assert(plan);

	if (ps == NULL)
		return;

	if (IsA(ps, ResultState))
	{
		VecResultState *vrs = (VecResultState *)ps;
		vrs->estate.plan = plan;
	}
	else if (IsA(ps, SubqueryScanState))
	{
		VecSubqueryScanState *vsss = (VecSubqueryScanState *)ps;
		vsss->estate.plan = plan;
	}
	else if (IsA(ps, SortState))
	{
		VecSortState *vss = (VecSortState *)ps;
		vss->estate.plan = plan;
	}
	else if (IsA(ps, AggState))
	{
		VecAggState *vas = (VecAggState *)ps;
		vas->estate.plan = plan;
	}
	else if (IsA(ps, NestLoopState))
	{
		VecNestLoopState *vnls = (VecNestLoopState *)ps;
		vnls->estate.plan = plan;
	}
	else if (IsA(ps, WindowAggState))
	{
		VecWindowAggState *vwas = (VecWindowAggState *)ps;
		vwas->estate.plan = plan;
	}
	else if (IsA(ps, WindowHashAggState))
	{
		VecWindowHashAggState *vwhas = (VecWindowHashAggState *)ps;
		vwhas->estate.plan = plan;
	}
	else if (IsA(ps, HashJoinState))
	{
		VecHashJoinState *vhjs = (VecHashJoinState *)ps;
		vhjs->estate.plan = plan;
	}
	else if (IsA(ps, AssertOpState))
	{
		VecAssertOpState *vaos = (VecAssertOpState *)ps;
		vaos->estate.plan = plan;
	}
	else if (IsA(ps, LimitState))
	{
		VecLimitState *vlimit = (VecLimitState *)ps;
		vlimit->estate.plan = plan;
	}
	else if (IsA(ps, SequenceState))
	{
		VecSequenceState *vseq = (VecSequenceState *)ps;
		vseq->estate.plan = plan;
	}
	else if (IsA(ps, AppendState))
	{
		VecAppendState *vappend = (VecAppendState *)ps;
		vappend->estate.plan = plan;
	}
	else if (IsA(ps, MaterialState))
	{
		VecMaterialState *vmat = (VecMaterialState *)ps;
		vmat->estate.plan = plan;
	}
	else if (IsA(ps, ShareInputScanState))
	{
		VecShareInputScanState *vsis = (VecShareInputScanState *)ps;
		vsis->estate.plan = plan;
	}
	else
		return;
}

VecExecuteState *
GetVecExecuteState(PlanState *ps)
{
	if (ps == NULL)
		return NULL;

	if (IsA(ps, SeqScanState))
	{
		VecSeqScanState *vsss = (VecSeqScanState *)ps;
		return &vsss->vestate;
	}
	else if (IsA(ps, ForeignScanState))
	{
		VecForeignScanState *vfss = (VecForeignScanState *)ps;
		return &vfss->vestate;
	}
	else if (IsA(ps, ResultState))
	{
		VecResultState *vrs = (VecResultState *)ps;
		if (vrs->base.hashFilter)
			return NULL;
		return &vrs->estate;
	}
	else if (IsA(ps, AggState))
	{
		VecAggState *vas = (VecAggState *)ps;
		return &vas->estate;
	}
	else if (IsA(ps, SubqueryScanState))
	{
		VecSubqueryScanState *vsss = (VecSubqueryScanState *)ps;
		return &vsss->estate;
	}
	else if (IsA(ps, HashJoinState))
	{
		VecHashJoinState *vhjs = (VecHashJoinState *)ps;
		return &vhjs->estate;
	}
	else if (IsA(ps, AssertOpState))
	{
		VecAssertOpState *vaos = (VecAssertOpState *)ps;
		return &vaos->estate;
	}
	else if (IsA(ps, SortState))
	{
		VecSortState *vsort = (VecSortState *)ps;
		return &vsort->estate;
	}
	else if (IsA(ps, LimitState))
	{
		VecLimitState *vlimit = (VecLimitState *)ps;
		return &vlimit->estate;
	}
	else if (IsA(ps, WindowAggState))
	{
		VecWindowAggState *vwindow = (VecWindowAggState *)ps;
		return &vwindow->estate;
	}
	else if (IsA(ps, WindowHashAggState))
	{
		VecWindowHashAggState *vwindow = (VecWindowHashAggState *)ps;
		return &vwindow->estate;
	}
	else if (IsA(ps, SequenceState))
	{
		VecSequenceState *vseq = (VecSequenceState *)ps;
		return &vseq->estate;
	}
	else if (IsA(ps, AppendState))
	{
		VecAppendState *vappend = (VecAppendState *)ps;
		return &vappend->estate;
	}
	else if (IsA(ps, ShareInputScanState))
	{
		VecShareInputScanState *vsis = (VecShareInputScanState *)ps;
		return &vsis->estate;
	}
	else if (IsA(ps, NestLoopState))
	{
		VecNestLoopState *vnl = (VecNestLoopState *)ps;
		return &vnl->estate;
	}
	else if (IsA(ps, MaterialState))
	{
		VecMaterialState *vmat = (VecMaterialState *)ps;
		return &vmat->estate;
	}
	else
		return NULL;
}

/* like ExecSetTupleBound in execProcnode, Backpropagation setting vec_plan limits bound */ 
void
ExecVecSetTupleBound(int64 tuples_needed, PlanState *child_node, PlanState *limit_node)
{
	/*
	 * Since this function recurses, in principle we should check stack depth
	 * here.  In practice, it's probably pointless since the earlier node
	 * initialization tree traversal would surely have consumed more stack.
	 */

	if (IsA(child_node, SortState))
	{
		/*
		 * If it is a Sort node, notify it that it can use bounded sort.
		 *
		 * Note: it is the responsibility of nodeSort.c to react properly to
		 * changes of these parameters.  If we ever redesign this, it'd be a
		 * good idea to integrate this signaling with the parameter-change
		 * mechanism.
		 */
		SortState  *sortState = (SortState *) child_node;

		if (tuples_needed < 0)
		{
			/* make sure flag gets reset if needed upon rescan */
			sortState->bounded = false;
		}
		else
		{
			sortState->bounded = true;
			sortState->bound = tuples_needed;

			/*
			 * If the new HTAB-based TopK path already built a TopKNode for
			 * this Sort (vec_topk_bounds_set called during ExecInitVecLimit),
			 * the Arrow plan no longer contains an outer OrderByNode.
			 * garrow_execute_plan_make_topk_node scans the plan for the last
			 * OrderByNode and converts it to TopK; with the outer OrderByNode
			 * already gone it would pick up an inner sort (e.g. a GroupAgg's
			 * sort-by-key) and truncate unrelated data.  Skip the legacy
			 * make_topk_node when the new path owns this Sort.
			 */
			if (vec_topk_bounds_lookup(child_node->plan) > 0)
				return;

			GArrowExecutePlan* sort_plan = NULL;
			GArrowSchema* schema = NULL;
			g_autoptr(GArrowSelectKOptions) selectk_option = NULL;   
			g_autoptr(GArrowSinkNodeOptions) node_options = NULL;
			GList *keys = NULL;
			GError *error = NULL;
			schema = GetSchemaFromSlot(child_node->ps_ResultTupleSlot);
			sort_plan = GetVecExecuteState(limit_node)->plan;
			keys = build_sort_keys(child_node, schema);
			selectk_option = garrow_selectk_options_new(tuples_needed, keys);
			node_options = GARROW_SINK_NODE_OPTIONS(
						garrow_selectk_sink_node_options_new(selectk_option));
			garrow_execute_plan_make_topk_node(sort_plan, GARROW_SELECTK_SINK_NODE_OPTIONS(node_options), &error);
			if (error)
			{
				elog(DEBUG1, "Failed to set topk sort plan, cause: %s", error->message);
				return;
			}
			garrow_list_free_ptr(&keys);
		}
	}
	else if (IsA(child_node, IncrementalSortState))
	{
		/*
		 * If it is an IncrementalSort node, notify it that it can use bounded
		 * sort.
		 *
		 * Note: it is the responsibility of nodeIncrementalSort.c to react
		 * properly to changes of these parameters.  If we ever redesign this,
		 * it'd be a good idea to integrate this signaling with the
		 * parameter-change mechanism.
		 */
		IncrementalSortState *sortState = (IncrementalSortState *) child_node;

		if (tuples_needed < 0)
		{
			/* make sure flag gets reset if needed upon rescan */
			sortState->bounded = false;
		}
		else
		{
			sortState->bounded = true;
			sortState->bound = tuples_needed;
		}
	}
	else if (IsA(child_node, AppendState))
	{
		/*
		 * If it is an Append, we can apply the bound to any nodes that are
		 * children of the Append, since the Append surely need read no more
		 * than that many tuples from any one input.
		 */
		AppendState *aState = (AppendState *) child_node;
		int			i;

		for (i = 0; i < aState->as_nplans; i++)
			ExecVecSetTupleBound(tuples_needed, aState->appendplans[i], limit_node);
	}
	else if (IsA(child_node, MergeAppendState))
	{
		/*
		 * If it is a MergeAppend, we can apply the bound to any nodes that
		 * are children of the MergeAppend, since the MergeAppend surely need
		 * read no more than that many tuples from any one input.
		 */
		MergeAppendState *maState = (MergeAppendState *) child_node;
		int			i;

		for (i = 0; i < maState->ms_nplans; i++)
			ExecVecSetTupleBound(tuples_needed, maState->mergeplans[i], limit_node);
	}
	else if (IsA(child_node, ResultState))
	{
		/*
		 * Similarly, for a projecting Result, we can apply the bound to its
		 * child node.
		 *
		 * If Result supported qual checking, we'd have to punt on seeing a
		 * qual.  Note that having a resconstantqual is not a showstopper: if
		 * that condition succeeds it affects nothing, while if it fails, no
		 * rows will be demanded from the Result child anyway.
		 */
		if (outerPlanState(child_node))
			ExecVecSetTupleBound(tuples_needed, outerPlanState(child_node), limit_node);
	}
	else if (IsA(child_node, SubqueryScanState))
	{
		/*
		 * We can also descend through SubqueryScan, but only if it has no
		 * qual (otherwise it might discard rows).
		 */
		SubqueryScanState *subqueryState = (SubqueryScanState *) child_node;

		if (subqueryState->ss.ps.qual == NULL)
			ExecVecSetTupleBound(tuples_needed, subqueryState->subplan, limit_node);
	}
	else if (IsA(child_node, GatherState))
	{
		/*
		 * A Gather node can propagate the bound to its workers.  As with
		 * MergeAppend, no one worker could possibly need to return more
		 * tuples than the Gather itself needs to.
		 *
		 * Note: As with Sort, the Gather node is responsible for reacting
		 * properly to changes to this parameter.
		 */
		GatherState *gstate = (GatherState *) child_node;

		gstate->tuples_needed = tuples_needed;

		/* Also pass down the bound to our own copy of the child plan */
		ExecVecSetTupleBound(tuples_needed, outerPlanState(child_node), limit_node);
	}
	else if (IsA(child_node, GatherMergeState))
	{
		/* Same comments as for Gather */
		GatherMergeState *gstate = (GatherMergeState *) child_node;

		gstate->tuples_needed = tuples_needed;

		ExecVecSetTupleBound(tuples_needed, outerPlanState(child_node), limit_node);
	}
	/*
	 * In principle we could descend through any plan node type that is
	 * certain not to discard or combine input rows; but on seeing a node that
	 * can do that, we can't propagate the bound any further.  For the moment
	 * it's unclear that any other cases are worth checking here.
	 */
}

/*
 * is_cross_slice_share_input_scan
 *    True if this PlanState is a cross-slice ShareInputScan.
 *
 * Cross-slice ShareInputScan coordinates its producer slice and consumer
 * slices through GP's ConditionVariable mechanism, driven from
 * ExecVecShareInputScan() (the node's ExecProcNode): the consumer waits on
 * shareinput_reader_waitready_vec() and the producer signals
 * shareinput_writer_notifyready_vec() after materializing.  See
 * nodeShareInputScan.c.
 */
static bool
is_cross_slice_share_input_scan(PlanState *ps)
{
	return ps != NULL &&
		   IsA(ps, ShareInputScanState) &&
		   ((ShareInputScan *) ps->plan)->cross_slice;
}

/*
 * merge_child_is_xslice_sharescan
 *    True if any of ps's MergeChildren-merge children is a cross-slice
 *    ShareInputScan.  Mirrors the child-collection switch in
 *    PostBuildVecPlan() so that the two stay in lockstep.
 */
static bool
merge_child_is_xslice_sharescan(PlanState *ps)
{
	int i;

	if (IsA(ps, SubqueryScanState))
		return is_cross_slice_share_input_scan(((SubqueryScanState *) ps)->subplan);
	else if (IsA(ps, SequenceState))
	{
		SequenceState *seqState = (SequenceState *) ps;

		for (i = 0; i < seqState->numSubplans; i++)
			if (is_cross_slice_share_input_scan(seqState->subplans[i]))
				return true;
		return false;
	}
	else if (IsA(ps, AppendState))
	{
		AppendState *appendState = (AppendState *) ps;

		for (i = 0; i < appendState->as_nplans; i++)
			if (is_cross_slice_share_input_scan(appendState->appendplans[i]))
				return true;
		return false;
	}
	else if (IsA(ps, HashJoinState))
		return is_cross_slice_share_input_scan(ps->lefttree) ||
			   (ps->righttree &&
				is_cross_slice_share_input_scan(ps->righttree->lefttree));
	else if (IsA(ps, NestLoopState))
		return is_cross_slice_share_input_scan(ps->lefttree) ||
			   is_cross_slice_share_input_scan(ps->righttree);
	else
		return is_cross_slice_share_input_scan(ps->lefttree);
}

static void
MergeArrowNodeToPlanStateFromSource(List **arrow_node_to_planstate, VecExecuteState *source_estate, VecExecuteState *estate)
{
	if (source_estate && source_estate->arrow_node_to_planstate != NIL)
	{
		estate->arrow_node_num = estate->arrow_node_num + source_estate->arrow_node_num - 2;
		TraceNodeInfo* last_info = llast(source_estate->arrow_node_to_planstate);
		last_info->node_num--;

		TraceNodeInfo* cur_info = llast(estate->arrow_node_to_planstate);
		cur_info->node_num--;
		*arrow_node_to_planstate = list_concat(*arrow_node_to_planstate, source_estate->arrow_node_to_planstate);
	}
}

void
PostBuildVecPlan(PlanState *ps, VecExecuteState *estate)
{
	g_autoptr(GError) error = NULL;
	GArrowExecutePlan *result;
	List* arrow_node_to_planstate = NULL;
	int plan_id;
	TraceNodeInfo* info;
	GArrowExecutePlan *target_plan;
	VecExecuteState *target_state;
	List *child_estates = NIL;  /* List of VecExecuteState* */
	ListCell *lc;
	VecExecuteState *child_estate;
	int i;

	BuildVecPlan(ps, estate);

	plan_id = garrow_execute_plan_get_id(estate->plan);
	Assert(plan_id == plan_num - 1);
	info = (TraceNodeInfo*)palloc(sizeof(TraceNodeInfo));
	info->ps = ps;
	info->node_num = garrow_execute_plan_node_num(estate->plan);

	estate->time_collector = garrow_time_collector_new(info->node_num);
	estate->arrow_node_num = info->node_num;
	garrow_time_collector_table_push_back(estate->time_collector);
	estate->arrow_node_to_planstate = lappend(estate->arrow_node_to_planstate, info);

	target_state = GetVecExecuteState(ps);
	if (!target_state)
		return;
	target_plan = target_state->plan;

	/*
	 * Cross-slice ShareInputScan synchronizes its producer and consumer
	 * slices through GP's ConditionVariable coordination performed in
	 * ExecVecShareInputScan() (the node's ExecProcNode): the consumer waits
	 * for the producer (shareinput_reader_waitready_vec) and the producer
	 * notifies after materializing (shareinput_writer_notifyready_vec).
	 *
	 * MergeChildren splices a child's Arrow plan directly into this node's
	 * plan; afterwards only this (parent) boundary runs ExecuteVecPlan and the
	 * spliced child's ExecProcNode never fires.  For a cross-slice
	 * ShareInputScan that skips the GP-level wait/notify, so the consumer's
	 * Arrow SharedSourceNode::SyncReader() looks for the producer's ".ready"
	 * file before the producer slice has created it and fails with
	 * "ready file not found ... bug in cross-slice synchronization".
	 *
	 * Leave such a child un-merged: BuildVecPlan() above already built a bridge
	 * source (BuildSource -> ExecProcNode) for it, so ExecVecShareInputScan
	 * still runs and the coordination is preserved.  The whole merge for this
	 * node is skipped because MergeChildren pairs children positionally with
	 * the target's source nodes and cannot drop a single child.  Both the
	 * consumer (e.g. under a Result) and the producer (e.g. under a Sequence)
	 * must stay un-merged: a consumer boundary that waits would otherwise hang
	 * on a producer whose notify was merged away.
	 */
	if (merge_child_is_xslice_sharescan(ps))
		return;

	/*
	 * Collect child VecExecuteStates into a list based on node type.
	 * Different node types store their children in different places.
	 */
	if (IsA(ps, SubqueryScanState))
	{
		/* SubqueryScan: child is in subplan */
		SubqueryScanState *subqueryState = (SubqueryScanState *) ps;
		child_estate = GetVecExecuteState(subqueryState->subplan);
		if (child_estate)
			child_estates = lappend(child_estates, child_estate);
	}
	else if (IsA(ps, SequenceState))
	{
		/* Sequence: children are in subplans[] array */
		SequenceState *seqState = (SequenceState *) ps;
		for (i = seqState->numSubplans - 1; i >= 0; i--)
		{
			child_estate = GetVecExecuteState(seqState->subplans[i]);
			if (child_estate)
				child_estates = lappend(child_estates, child_estate);
		}
	}
	else if (IsA(ps, AppendState))
	{
		/* Append: children are in appendplans[] array */
		AppendState *appendState = (AppendState *) ps;
		for (i = 0; i < appendState->as_nplans; i++)
		{
			child_estate = GetVecExecuteState(appendState->appendplans[i]);
			if (child_estate)
				child_estates = lappend(child_estates, child_estate);
		}
	}
	else if (IsA(ps, HashJoinState) || IsA(ps, NestLoopState))
	{
		/* Join nodes: left and right children */
		VecExecuteState *left_estate = GetVecExecuteState(ps->lefttree);
		VecExecuteState *right_estate;

		/* HashJoin needs to skip the Hash node */
		if (IsA(ps, HashJoinState))
			right_estate = GetVecExecuteState(ps->righttree->lefttree);
		else
			right_estate = GetVecExecuteState(ps->righttree);

		if (left_estate)
			child_estates = lappend(child_estates, left_estate);
		if (right_estate)
			child_estates = lappend(child_estates, right_estate);
	}
	else
	{
		child_estate = GetVecExecuteState(ps->lefttree);
		if (child_estate)
			child_estates = lappend(child_estates, child_estate);
	}

	if (child_estates == NIL)
		return;

	/*
	 * Merge child plans into target_plan.  Both branches below funnel into
	 * garrow_execute_plan_merge_children().  The join case is kept separate
	 * only because it needs to peek at left/right by position (and may bail
	 * out if the left side is missing); the non-join case can simply iterate
	 * over child_estates in order.
	 */
	result = target_plan;

	if (IsA(ps, HashJoinState) || IsA(ps, NestLoopState))
	{
		/* Join nodes: merge left and right via the unified N-ary API. */
		g_autoptr(GArrowExecutePlan) left_plan = NULL;
		g_autoptr(GArrowExecutePlan) right_plan = NULL;
		GList *child_plans = NULL;

		if (list_length(child_estates) >= 1)
		{
			VecExecuteState *left_estate = (VecExecuteState *) linitial(child_estates);
			if (left_estate->plan)
			{
				left_plan = garrow_copy_ptr(left_estate->plan);
				MergeArrowNodeToPlanStateFromSource(&arrow_node_to_planstate, left_estate, estate);
			}
		}

		if (list_length(child_estates) >= 2)
		{
			VecExecuteState *right_estate = (VecExecuteState *) lsecond(child_estates);
			if (right_estate->plan)
			{
				right_plan = garrow_copy_ptr(right_estate->plan);
				MergeArrowNodeToPlanStateFromSource(&arrow_node_to_planstate, right_estate, estate);
			}
		}

		if (left_plan == NULL || right_plan == NULL)
		{
			list_free(child_estates);
			return;
		}

		/*
		 * No child-plan swap needed for RIGHT_SEMI/ANTI.  Both ORCA
		 * (CTranslatorDXLToPlStmt swaps DXL children) and the PG planner
		 * produce PG-canonical layout: outer=probe, inner=Hash(build/emit).
		 * Standard merge order (left_plan=lefttree→inputs[0], right_plan=
		 * righttree→inputs[1]) already puts build at Arrow right (inputs[1]).
		 */

		/* garrow_list_append_ptr steals the ref from each autoptr. */
		child_plans = garrow_list_append_ptr(child_plans, left_plan);
		child_plans = garrow_list_append_ptr(child_plans, right_plan);

		result = garrow_execute_plan_merge_children(target_plan, child_plans, &error);
		garrow_list_free_ptr(&child_plans);
		if (error)
		{
			elog(LOG, "Failed to merge plan, cause: %s", error->message);
			list_free(child_estates);
			return;
		}
	}
	else
	{
		/* Other nodes: merge all children at once via merge_children. */
		GList *child_plans = NULL;

		/* Collect all child plans into a GList */
		foreach(lc, child_estates)
		{
			VecExecuteState *source_estate = (VecExecuteState *) lfirst(lc);
			g_autoptr(GArrowExecutePlan) source_plan = NULL;

			if (source_estate->plan == NULL)
			{
				garrow_list_free_ptr(&child_plans);
				list_free(child_estates);
				return;
			}

			source_plan = garrow_copy_ptr(source_estate->plan);
			MergeArrowNodeToPlanStateFromSource(&arrow_node_to_planstate, source_estate, estate);
			child_plans = garrow_list_append_ptr(child_plans, source_plan);
		}

		/* Merge all child plans at once */
		result = garrow_execute_plan_merge_children(target_plan, child_plans, &error);

		/*
		 * garrow_list_append_ptr stole a +1 ref from each source_plan into
		 * the GList cell, so we must drop those refs (and free the cells)
		 * after the merge is done.  Skipping this would leak N plan refs
		 * per PostBuildVecPlan invocation.
		 */
		garrow_list_free_ptr(&child_plans);

		if (error)
		{
			elog(LOG, "Failed to merge child plans, cause: %s", error->message);
			list_free(child_estates);
			return;
		}
	}

	/*
	 * Record that this node consumed N children via MergeChildren.  Surfaced
	 * by show_vec_merge_info as "Vec Plan Merge:  N children" at EXPLAIN VERBOSE
	 * time, a regression footprint for the splice path (see explain.c).
	 */
	estate->merged_child_count = list_length(child_estates);

	list_free(child_estates);

	/*
	 * list_concat in PG14 memcpys list2's elements into list1's flat
	 * element array and leaves list2's cells intact, so the cells of
	 * estate->arrow_node_to_planstate must be freed separately.  The
	 * pointed-to TraceNodeInfo objects are shared with arrow_node_to_planstate
	 * via the memcpy and remain owned there.
	 */
	arrow_node_to_planstate = list_concat(arrow_node_to_planstate, estate->arrow_node_to_planstate);
	if (estate->arrow_node_to_planstate != NIL)
		list_free(estate->arrow_node_to_planstate);
	estate->arrow_node_to_planstate = arrow_node_to_planstate;
	garrow_execute_plan_set_plan_id(result, plan_num++);
	estate->time_collector = garrow_time_collector_new(estate->arrow_node_num);
	garrow_time_collector_table_push_back(estate->time_collector);
	SetArrowPlan(ps, result);

	if (Debug_print_plan)
	{
		g_autofree gchar *plan_str = garrow_execute_plan_to_string(result);
		elog(LOG, "arrow plan for plan(%d) in PostBuildVecPlan: %s", ps->plan->plan_node_id, plan_str);
	}
}
