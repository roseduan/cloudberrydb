/*
 * gapfill_exec.c - GapFill CustomScan Executor (state machine)
 *
 * Implements the executor-side of time_bucket_gapfill().
 * Sits on top of an Aggregate plan, detects missing time buckets,
 * and inserts synthetic gap rows with NULL aggregates (or LOCF /
 * interpolated values if those markers are present).
 *
 * State machine:
 *   1. Fetch row from child (Aggregate)
 *   2. Compare child's bucket timestamp with next_expected_timestamp
 *   3. If gap: emit synthetic NULL row for next_expected, advance next_expected
 *   4. If match: emit real row, advance next_expected
 *   5. If child exhausted: emit remaining gap rows until finish
 *
 * Clean-room implementation for time_series extension.
 *
 * Copyright (c) 2026 HashData Inc.
 * Licensed under Apache License 2.0
 */
#include "../include/time_series.h"
#include "../include/gapfill/gapfill.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "commands/explain.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include <math.h>
#include "nodes/extensible.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/datum.h"
#include "utils/fmgrprotos.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/numeric.h"
#include "utils/timestamp.h"

/* ================================================================
 * LOCF / Interpolate Column State
 * ================================================================ */

typedef enum GapFillColumnKind
{
	GFCOL_AGG,			/* no special treatment */
	GFCOL_CARRY_FORWARD,			/* last observation carried forward */
	GFCOL_LINEAR_FILL,	/* linear interpolation */
	GFCOL_GROUP_KEY			/* GROUP BY column (non-bucket) */
} GapFillColumnKind;

typedef struct GapFillColumnInfo
{
	GapFillColumnKind kind;
	int			resno;			/* 1-based target entry resno */
	Oid			typoid;
	int16		typlen;
	bool		typbyval;

	/* LOCF state */
	bool		locf_has_value;
	Datum		locf_value;
	bool		locf_isnull;

	/* Interpolate state */
	bool		interp_has_prev;
	int64		interp_prev_ts;		/* timestamp of previous sample point */
	double		interp_prev_val;	/* previous sample value (as float8) */
	bool		interp_has_next;
	int64		interp_next_ts;		/* timestamp of next sample point */
	double		interp_next_val;	/* next sample value */

	/* Group key state */
	int			group_key_idx;		/* index into current_group_values (-1 if not group) */

	/* Default value for PLAIN columns with constant expressions */
	bool		has_default;
	Datum		default_value;
	bool		default_isnull;

	/*
	 * For PLAIN columns with wrapper expressions (COALESCE, CASE, etc.):
	 * ExprState compiled from the original expression with Aggref nodes
	 * replaced by Const NULL.  Evaluated on gap rows so that e.g.
	 * COALESCE(NULL, 0) → 0 instead of plain NULL.
	 */
	ExprState  *gap_expr;
} GapFillColumnInfo;

/* ================================================================
 * GapFill Scan State
 * ================================================================ */

typedef struct GapFillExecState
{
	CustomScanState css;		/* must be first */

	/* Child plan state (Aggregate) */
	PlanState  *child_ps;

	/* Gapfill parameters */
	int64		start_ts;		/* start timestamp (as int64 usec) */
	int64		finish_ts;		/* finish timestamp (as int64 usec) */
	int64		bucket_width;	/* bucket width (as int64 usec) */
	int32		month_period;	/* month interval count (0=non-month) */
	char	   *timezone;		/* timezone name (NULL=no timezone) */
	Datum		timezone_datum;	/* cached CStringGetTextDatum(timezone) */
	Oid			time_type;		/* timestamp type OID */

	/* Current position */
	int64		next_ts;		/* next expected bucket timestamp */

	/* Bucket column index in target list (0-based) */
	int			bucket_attno;	/* 0-based index into tts_values */

	/* Pending child tuple (fetched ahead) */
	bool		have_pending;
	Datum	   *pending_values;
	bool	   *pending_isnull;
	int			num_attrs;

	/* LOCF / Interpolate column tracking */
	GapFillColumnInfo *col_states;
	int			num_col_states;

	bool		child_exhausted;
	bool		initialized;

	/* Group-aware tracking */
	int			num_group_keys;			/* number of non-bucket GROUP BY columns */
	int		   *group_key_attnos;		/* 0-based indices of group columns */
	Datum	   *current_group_values;	/* current group's key values */
	bool	   *current_group_isnull;	/* null flags for current group values */
	bool		group_initialized;		/* have we seen the first row? */

} GapFillExecState;

/* Forward declarations */
static void gapfill_begin(CustomScanState *node, EState *estate, int eflags);
static TupleTableSlot *gapfill_exec(CustomScanState *node);
static void gapfill_end(CustomScanState *node);
static void gapfill_rescan(CustomScanState *node);
static void gapfill_explain(CustomScanState *node, List *ancestors,
							ExplainState *es);

/* Custom Exec Methods */
static const CustomExecMethods ht_gapfill_exec_methods = {
	.CustomName = "GapFill",
	.BeginCustomScan = gapfill_begin,
	.ExecCustomScan = gapfill_exec,
	.EndCustomScan = gapfill_end,
	.ReScanCustomScan = gapfill_rescan,
	.ExplainCustomScan = gapfill_explain,
};

/* ================================================================
 * CreateCustomScanState (called from gapfill.c registration)
 * ================================================================ */

Node *
ht_gapfill_create_state(CustomScan *cscan)
{
	GapFillExecState *state;

	state = (GapFillExecState *) newNode(sizeof(GapFillExecState),
										 T_CustomScanState);
	state->css.methods = &ht_gapfill_exec_methods;
	state->child_ps = NULL;
	state->initialized = false;
	state->child_exhausted = false;
	state->have_pending = false;
	state->col_states = NULL;
	state->num_col_states = 0;
	state->num_group_keys = 0;
	state->group_key_attnos = NULL;
	state->current_group_values = NULL;
	state->current_group_isnull = NULL;
	state->group_initialized = false;
	return (Node *) state;
}

/* ================================================================
 * Helpers: convert timestamp Datum ↔ int64 microseconds
 *
 * Uses lookup tables instead of switch statements to decouple the
 * conversion logic from the control flow structure.
 * ================================================================ */

typedef struct TimeToInt64Entry {
	Oid		typoid;
	int64	(*convert)(Datum d);
} TimeToInt64Entry;

static int64 cvt_timestamp_to_i64(Datum d)   { return DatumGetTimestamp(d); }
static int64 cvt_timestamptz_to_i64(Datum d)  { return DatumGetTimestampTz(d); }
static int64 cvt_date_to_i64(Datum d)         { return (int64) DatumGetDateADT(d) * USECS_PER_DAY; }
static int64 cvt_int16_to_i64(Datum d)        { return (int64) DatumGetInt16(d); }
static int64 cvt_int32_to_i64(Datum d)        { return (int64) DatumGetInt32(d); }
static int64 cvt_int64_to_i64(Datum d)        { return DatumGetInt64(d); }

static const TimeToInt64Entry time_to_int64_table[] = {
	{ TIMESTAMPOID,   cvt_timestamp_to_i64 },
	{ TIMESTAMPTZOID, cvt_timestamptz_to_i64 },
	{ DATEOID,        cvt_date_to_i64 },
	{ INT2OID,        cvt_int16_to_i64 },
	{ INT4OID,        cvt_int32_to_i64 },
	{ INT8OID,        cvt_int64_to_i64 },
	{ InvalidOid,     NULL }
};

static int64
time_datum_to_int64(Datum d, Oid typoid)
{
	const TimeToInt64Entry *entry;

	for (entry = time_to_int64_table; entry->typoid != InvalidOid; entry++)
		if (entry->typoid == typoid)
			return entry->convert(d);

	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("unsupported time type for gapfill: type OID %u",
					typoid),
			 errhint("Supported types: timestamp, timestamptz, date, "
					 "smallint, integer, bigint.")));
	return 0;
}

typedef struct Int64ToTimeEntry {
	Oid		typoid;
	Datum	(*convert)(int64 val);
} Int64ToTimeEntry;

static Datum cvt_i64_to_timestamp(int64 v)   { return TimestampGetDatum((Timestamp) v); }
static Datum cvt_i64_to_timestamptz(int64 v)  { return TimestampTzGetDatum((TimestampTz) v); }
static Datum cvt_i64_to_date(int64 v)         { return DateADTGetDatum((DateADT) (v / USECS_PER_DAY)); }
static Datum cvt_i64_to_int16(int64 v)        { return Int16GetDatum((int16) v); }
static Datum cvt_i64_to_int32(int64 v)        { return Int32GetDatum((int32) v); }
static Datum cvt_i64_to_int64(int64 v)        { return Int64GetDatum(v); }

static const Int64ToTimeEntry int64_to_time_table[] = {
	{ TIMESTAMPOID,   cvt_i64_to_timestamp },
	{ TIMESTAMPTZOID, cvt_i64_to_timestamptz },
	{ DATEOID,        cvt_i64_to_date },
	{ INT2OID,        cvt_i64_to_int16 },
	{ INT4OID,        cvt_i64_to_int32 },
	{ INT8OID,        cvt_i64_to_int64 },
	{ InvalidOid,     NULL }
};

static Datum
int64_to_time_datum(int64 usec, Oid typoid)
{
	const Int64ToTimeEntry *entry;

	for (entry = int64_to_time_table; entry->typoid != InvalidOid; entry++)
		if (entry->typoid == typoid)
			return entry->convert(usec);

	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("unsupported time type for gapfill: type OID %u",
					typoid),
			 errhint("Supported types: timestamp, timestamptz, date, "
					 "smallint, integer, bigint.")));
	return (Datum) 0;
}

/* ================================================================
 * Month/Timezone-aware timestamp advance
 * ================================================================ */

/*
 * Advance a timestamp by N months. Uses j2date/date2j for calendar
 * arithmetic, matching bucket_month() behavior.
 */
static int64
month_advance_int64(int64 ts, int32 months, Oid time_type, Datum tz_datum)
{
	int32		year, month, day;
	DateADT		date, new_date;

	switch (time_type)
	{
		case DATEOID:
		{
			/* ts is stored as DateADT * USECS_PER_DAY */
			date = (DateADT) (ts / USECS_PER_DAY);
			j2date(date + POSTGRES_EPOCH_JDATE, &year, &month, &day);

			month += months - 1;	/* 0-based for arithmetic */
			year += month / 12;
			month = month % 12;
			if (month < 0)
			{
				month += 12;
				year -= 1;
			}
			new_date = date2j(year, month + 1, 1) - POSTGRES_EPOCH_JDATE;
			return (int64) new_date * USECS_PER_DAY;
		}

		case TIMESTAMPOID:
		{
			Datum d_date = DirectFunctionCall1(timestamp_date,
											   TimestampGetDatum((Timestamp) ts));
			date = DatumGetDateADT(d_date);
			j2date(date + POSTGRES_EPOCH_JDATE, &year, &month, &day);

			month += months - 1;
			year += month / 12;
			month = month % 12;
			if (month < 0)
			{
				month += 12;
				year -= 1;
			}
			new_date = date2j(year, month + 1, 1) - POSTGRES_EPOCH_JDATE;
			return DatumGetTimestamp(
				DirectFunctionCall1(date_timestamp, DateADTGetDatum(new_date)));
		}

		case TIMESTAMPTZOID:
		{
			if (DatumGetPointer(tz_datum) != NULL)
			{
				Datum	local_ts;
				Datum	d_date;
				Datum	new_local;
				Datum	new_utc;

				/* Convert UTC timestamptz to local timestamp */
				local_ts = DirectFunctionCall2(timestamptz_zone,
											   tz_datum,
											   TimestampTzGetDatum((TimestampTz) ts));
				d_date = DirectFunctionCall1(timestamp_date, local_ts);

				date = DatumGetDateADT(d_date);
				j2date(date + POSTGRES_EPOCH_JDATE, &year, &month, &day);

				month += months - 1;
				year += month / 12;
				month = month % 12;
				if (month < 0)
				{
					month += 12;
					year -= 1;
				}
				new_date = date2j(year, month + 1, 1) - POSTGRES_EPOCH_JDATE;

				/* Convert local date back to UTC timestamptz */
				new_local = DirectFunctionCall1(date_timestamp,
												DateADTGetDatum(new_date));
				new_utc = DirectFunctionCall2(timestamp_zone, tz_datum, new_local);
				return DatumGetTimestampTz(new_utc);
			}
			else
			{
				/*
				 * No timezone: use date_timestamp to produce UTC midnight,
				 * matching timestamptz_bucket() month-bucket behavior.
				 * The non-timezone variant uses UTC-based month bucketing.
				 */
				Datum	d_date;

				d_date = DirectFunctionCall1(timestamp_date,
											 TimestampTzGetDatum((TimestampTz) ts));
				date = DatumGetDateADT(d_date);
				j2date(date + POSTGRES_EPOCH_JDATE, &year, &month, &day);

				month += months - 1;
				year += month / 12;
				month = month % 12;
				if (month < 0)
				{
					month += 12;
					year -= 1;
				}
				new_date = date2j(year, month + 1, 1) - POSTGRES_EPOCH_JDATE;
				return DatumGetTimestampTz(
					DirectFunctionCall1(date_timestamp, DateADTGetDatum(new_date)));
			}
		}

		default:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("month/year interval not supported for "
							"gapfill on type OID %u", time_type),
					 errhint("Use a fixed-duration interval such as "
							 "'30 days' instead of '1 month'.")));
			return 0;	/* unreachable */
	}
}

/*
 * Advance a non-month timestamp with timezone awareness (DST-safe).
 * tz_datum is a pre-allocated Text Datum for the timezone name.
 */
static int64
tz_aware_advance(int64 ts, int64 bucket_width, Datum tz_datum)
{
	Datum	local_ts;
	Timestamp local_val;
	Datum	new_utc;

	/* Convert UTC timestamptz to local timestamp */
	local_ts = DirectFunctionCall2(timestamptz_zone, tz_datum,
								   TimestampTzGetDatum((TimestampTz) ts));
	local_val = DatumGetTimestamp(local_ts);

	/* Add bucket_width in local time */
	local_val += bucket_width;

	/* Convert back to UTC */
	new_utc = DirectFunctionCall2(timestamp_zone, tz_datum,
								  TimestampGetDatum(local_val));
	return DatumGetTimestampTz(new_utc);
}

/*
 * Unified stepping function: handles month intervals, timezone, and plain.
 */
static int64
step_to_next_bucket(GapFillExecState *state, int64 ts)
{
	if (state->month_period > 0)
		return month_advance_int64(ts, state->month_period,
								  state->time_type, state->timezone_datum);
	else if (state->timezone != NULL)
		return tz_aware_advance(ts, state->bucket_width, state->timezone_datum);
	else
		return ts + state->bucket_width;
}

/*
 * Convert a Datum to float8 for interpolation.
 */
static double
numeric_datum_to_double(Datum d, Oid typoid)
{
	switch (typoid)
	{
		case INT2OID:
			return (double) DatumGetInt16(d);
		case INT4OID:
			return (double) DatumGetInt32(d);
		case INT8OID:
			return (double) DatumGetInt64(d);
		case FLOAT4OID:
			return (double) DatumGetFloat4(d);
		case FLOAT8OID:
			return DatumGetFloat8(d);
		case NUMERICOID:
			return DatumGetFloat8(DirectFunctionCall1(numeric_float8, d));
		default:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("unsupported value type for interpolate: "
							"type OID %u", typoid),
					 errhint("Supported types: smallint, integer, "
							 "bigint, real, double precision, numeric.")));
			return 0.0;
	}
}

static Datum
double_to_numeric_datum(double val, Oid typoid)
{
	switch (typoid)
	{
		case INT2OID:
			val = rint(val);
			if (!isfinite(val) || val <= -32769.0 || val >= 32768.0)
				ereport(ERROR,
						(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
						 errmsg("interpolated value %g is out of range for smallint", val)));
			return Int16GetDatum((int16) val);
		case INT4OID:
			val = rint(val);
			if (!isfinite(val) || val <= -2147483649.0 || val >= 2147483648.0)
				ereport(ERROR,
						(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
						 errmsg("interpolated value %g is out of range for integer", val)));
			return Int32GetDatum((int32) val);
		case INT8OID:
			val = rint(val);
			if (!isfinite(val) ||
				val < (double) PG_INT64_MIN || val >= -(double) PG_INT64_MIN)
				ereport(ERROR,
						(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
						 errmsg("interpolated value %g is out of range for bigint", val)));
			return Int64GetDatum((int64) val);
		case FLOAT4OID:
			return Float4GetDatum((float4) val);
		case FLOAT8OID:
			return Float8GetDatum(val);
		case NUMERICOID:
			return DirectFunctionCall1(float8_numeric, Float8GetDatum(val));
		default:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("unsupported value type for interpolate: "
							"type OID %u", typoid),
					 errhint("Supported types: smallint, integer, "
							 "bigint, real, double precision, numeric.")));
			return (Datum) 0;
	}
}

/* ================================================================
 * Aggref → NULL mutator for gap-row expression evaluation
 *
 * Walks an expression tree and replaces every Aggref node with a
 * typed Const NULL.  The resulting expression is compiled into an
 * ExprState and evaluated per-column on synthetic gap rows so that
 * wrapper expressions (COALESCE, CASE, arithmetic, …) still fire.
 * ================================================================ */

/*
 * Walker: returns true if the expression tree contains any Aggref node.
 */
static bool
expr_contains_aggregate(Node *node, void *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Aggref))
		return true;
	return expression_tree_walker(node, expr_contains_aggregate, context);
}

static Node *
replace_aggrefs_with_null(Node *node, void *context)
{
	if (node == NULL)
		return NULL;

	if (IsA(node, Aggref))
	{
		Aggref *agg = (Aggref *) node;

		return (Node *) makeConst(agg->aggtype,
								  -1,			/* consttypmod */
								  agg->aggcollid,
								  get_typlen(agg->aggtype),
								  (Datum) 0,
								  true,			/* constisnull */
								  get_typbyval(agg->aggtype));
	}

	return expression_tree_mutator(node, replace_aggrefs_with_null, context);
}

/* ================================================================
 * Initialize: extract parameters and set up LOCF/interpolate state
 * ================================================================ */

/*
 * Reconstruct a signed int64 from two Integer nodes (high32, low32).
 * The planner stores int64 values split into two 32-bit halves
 * because Integer nodes only hold 'long' (which may be 32-bit).
 *
 * Uses uint64 cast before left-shift to avoid undefined behavior
 * (left-shifting negative signed integers is UB in C99/C11).
 */
static int64
reconstruct_int64(List *list, int high_idx, int low_idx)
{
	uint64		high = (uint64) (uint32) intVal(list_nth(list, high_idx));
	uint64		low = (uint64) (uint32) intVal(list_nth(list, low_idx));

	return (int64) ((high << 32) | low);
}

static void
init_gapfill_state(GapFillExecState *state)
{
	CustomScan *cscan = (CustomScan *) state->css.ss.ps.plan;
	List	   *tlist = cscan->custom_scan_tlist;
	List	   *privdata = cscan->custom_private;
	int			attno = 0;

	state->num_attrs = list_length(tlist);
	state->pending_values = (Datum *) palloc0(state->num_attrs * sizeof(Datum));
	state->pending_isnull = (bool *) palloc(state->num_attrs * sizeof(bool));
	memset(state->pending_isnull, true, state->num_attrs * sizeof(bool));

	/*
	 * Read gapfill parameters from custom_private (serialized by planner):
	 *   [0] bucket_attno     (Integer)
	 *   [1] time_type OID    (Integer)
	 *   [2] bucket_width hi  (Integer)
	 *   [3] bucket_width lo  (Integer)
	 *   [4] num_locf_cols    (Integer)
	 *   [5] start_usec hi    (Integer)
	 *   [6] start_usec lo    (Integer)
	 *   [7] finish_usec hi   (Integer)
	 *   [8] finish_usec lo   (Integer)
	 *   [9] month_period     (Integer)
	 *   [10] timezone_name   (String)
	 */
	if (list_length(privdata) < 11)
		elog(ERROR, "time_series gapfill: invalid custom_private");

	state->bucket_attno = intVal(list_nth(privdata, 0));
	if (state->bucket_attno < 0 || state->bucket_attno >= state->num_attrs)
		elog(ERROR, "time_series gapfill: bucket_attno %d out of range [0, %d)",
			 state->bucket_attno, state->num_attrs);

	state->time_type = (Oid) intVal(list_nth(privdata, 1));
	state->bucket_width = reconstruct_int64(privdata, 2, 3);
	state->start_ts = reconstruct_int64(privdata, 5, 6);
	state->finish_ts = reconstruct_int64(privdata, 7, 8);

	/* Read month_period and timezone */
	state->month_period = intVal(list_nth(privdata, 9));
	{
		char *tz_str = strVal(list_nth(privdata, 10));
		if (strlen(tz_str) > 0)
		{
			state->timezone = pstrdup(tz_str);
			state->timezone_datum = CStringGetTextDatum(state->timezone);
		}
		else
		{
			state->timezone = NULL;
			state->timezone_datum = (Datum) 0;
		}
	}

	/* Allocate col_states for all target entries */
	state->col_states = (GapFillColumnInfo *)
		palloc0(state->num_attrs * sizeof(GapFillColumnInfo));
	state->num_col_states = state->num_attrs;

	/* Initialize all columns as PLAIN */
	for (attno = 0; attno < state->num_attrs; attno++)
	{
		GapFillColumnInfo *cs = &state->col_states[attno];
		cs->kind = GFCOL_AGG;
		cs->locf_has_value = false;
		cs->locf_isnull = true;
		cs->interp_has_prev = false;
		cs->interp_has_next = false;
		cs->has_default = false;
	}

	/*
	 * Read LOCF/interpolate column info from custom_private.
	 * The planner serializes these because locf()/interpolate() FuncExpr
	 * nodes get separated into a Result projection above GapFill,
	 * so they don't appear in the GapFill's plan targetlist.
	 *
	 * Layout after index 10:
	 *   [4] = num_locf_cols (already read above as part of fixed header)
	 *   [11..11+N*2-1] = LOCF (attno, typoid) pairs
	 *   [11+N*2] = num_interp_cols
	 *   [11+N*2+1..] = Interpolate (attno, typoid) pairs
	 */
	{
		int		num_locf = intVal(list_nth(privdata, 4));
		int		base_idx = 11;
		int		num_interp;
		int		i;

		for (i = 0; i < num_locf; i++)
		{
			int col_attno = intVal(list_nth(privdata, base_idx + i * 2));

			/* Skip serialized typoid [base_idx + i*2 + 1] — we use actual
			 * type from the plan tlist instead, because the planner may
			 * strip the locf() wrapper and the inner expression (e.g.
			 * avg(value)) may have a different type than locf's return type. */

			if (col_attno >= 0 && col_attno < state->num_attrs)
			{
				TargetEntry *tle = list_nth(tlist, col_attno);
				Oid actual_typoid = exprType((Node *) tle->expr);
				GapFillColumnInfo *cs = &state->col_states[col_attno];
				cs->kind = GFCOL_CARRY_FORWARD;
				cs->typoid = actual_typoid;
				get_typlenbyval(cs->typoid, &cs->typlen, &cs->typbyval);
			}
		}

		base_idx = 11 + num_locf * 2;

		if (base_idx < list_length(privdata))
		{
			num_interp = intVal(list_nth(privdata, base_idx));
			base_idx++;

			for (i = 0; i < num_interp; i++)
			{
				int col_attno = intVal(list_nth(privdata, base_idx + i * 2));

				/* Same as LOCF: use actual type from plan tlist */

				if (col_attno >= 0 && col_attno < state->num_attrs)
				{
					TargetEntry *tle = list_nth(tlist, col_attno);
					Oid actual_typoid = exprType((Node *) tle->expr);
					GapFillColumnInfo *cs = &state->col_states[col_attno];

					if (cs->kind != GFCOL_AGG)
						continue;  /* Already assigned LOCF/GROUP — skip collision */

					cs->kind = GFCOL_LINEAR_FILL;
					cs->typoid = actual_typoid;
					get_typlenbyval(cs->typoid, &cs->typlen, &cs->typbyval);
				}
			}

			base_idx += num_interp * 2;
		}

		/*
		 * Read GROUP BY column info from custom_private.
		 * These are non-bucket GROUP BY columns that appear before the
		 * bucket column in the GroupAggregate sort order, enabling
		 * group-aware gapfill with correct group key propagation.
		 */
		if (base_idx < list_length(privdata))
		{
			state->num_group_keys = intVal(list_nth(privdata, base_idx));
			base_idx++;

			if (state->num_group_keys > 0)
			{
				state->group_key_attnos = palloc(state->num_group_keys * sizeof(int));
				state->current_group_values = palloc0(state->num_group_keys * sizeof(Datum));
				state->current_group_isnull = palloc(state->num_group_keys * sizeof(bool));
				memset(state->current_group_isnull, true,
					   state->num_group_keys * sizeof(bool));
				state->group_initialized = false;

				for (i = 0; i < state->num_group_keys; i++)
				{
					int col_attno = intVal(list_nth(privdata, base_idx + i));
					state->group_key_attnos[i] = col_attno;

					/* Mark column as GROUP in col_states */
					if (col_attno >= 0 && col_attno < state->num_attrs)
					{
						TargetEntry *tle = list_nth(tlist, col_attno);
						GapFillColumnInfo *cs = &state->col_states[col_attno];
						cs->kind = GFCOL_GROUP_KEY;
						cs->group_key_idx = i;
						cs->typoid = exprType((Node *) tle->expr);
						get_typlenbyval(cs->typoid, &cs->typlen, &cs->typbyval);
					}
				}
			}
		}
	}

	if (state->bucket_width <= 0 && state->month_period <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("time_bucket_gapfill: bucket_width must be greater than 0")));

	if (state->start_ts == PG_INT64_MIN || state->finish_ts == PG_INT64_MIN)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("time_bucket_gapfill requires start and finish arguments"),
				 errhint("Use time_bucket_gapfill(bucket_width, ts, start, finish)")));

	if (state->start_ts >= state->finish_ts)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("time_bucket_gapfill: start must be before finish")));

	/*
	 * For PLAIN columns (not bucket, LOCF, interpolate, or group),
	 * prepare expression evaluation:
	 * - For Const nodes: pre-compute and store the value
	 * - For other expressions (COALESCE, CASE, etc.): prepare ExprState
	 */
	for (attno = 0; attno < state->num_attrs; attno++)
	{
		GapFillColumnInfo *cs = &state->col_states[attno];

		if (cs->kind == GFCOL_AGG && attno != state->bucket_attno)
		{
			TargetEntry *tle = list_nth(tlist, attno);

			if (IsA(tle->expr, Const))
			{
				/* Constant: pre-compute value */
				Const *c = (Const *) tle->expr;

				cs->has_default = true;
				cs->default_value = c->constvalue;
				cs->default_isnull = c->constisnull;
			}
			else
			{
				cs->has_default = false;
			}
		}
	}

	/*
	 * For PLAIN columns with non-trivial expressions (COALESCE, CASE, etc.),
	 * build per-column ExprState for gap-row evaluation.
	 * Replace Aggref nodes with typed NULL constants so that wrapper
	 * expressions still fire: COALESCE(NULL, 0) → 0.
	 */
	for (attno = 0; attno < state->num_attrs; attno++)
	{
		GapFillColumnInfo *cs = &state->col_states[attno];

		if (cs->kind == GFCOL_AGG &&
			attno != state->bucket_attno &&
			!cs->has_default)
		{
			TargetEntry *tle = list_nth(tlist, attno);

			if (!IsA(tle->expr, Var) &&
				expr_contains_aggregate((Node *) tle->expr, NULL))
			{
				Node *mutated = replace_aggrefs_with_null((Node *) tle->expr,
													   NULL);
				cs->gap_expr = ExecInitExpr((Expr *) mutated,
											&state->css.ss.ps);
			}
		}
	}

	state->next_ts = state->start_ts;
	state->initialized = true;
}

/* ================================================================
 * Fetch next row from child and store in pending buffer
 * ================================================================ */

static bool
pull_next_aggregate_row(GapFillExecState *state)
{
	TupleTableSlot *slot;
	int			i;

	if (state->child_exhausted)
		return false;

	slot = ExecProcNode(state->child_ps);
	if (TupIsNull(slot))
	{
		state->child_exhausted = true;
		return false;
	}

	/*
	 * Deform into pending buffer.
	 * Note: for pass-by-reference types (TEXT, ARRAY, etc.), we copy only
	 * the Datum pointer here, not the underlying data. This is safe because
	 * the pending row is always consumed (emitted or used for group-change
	 * detection) before the next ExecProcNode call overwrites the child slot.
	 * If the execution flow ever changes to allow multiple unconsumed pending
	 * rows, this must be changed to use datumCopy.
	 */
	slot_getallattrs(slot);
	for (i = 0; i < state->num_attrs; i++)
	{
		state->pending_values[i] = slot->tts_values[i];
		state->pending_isnull[i] = slot->tts_isnull[i];
	}

	state->have_pending = true;
	return true;
}

/* ================================================================
 * Group-aware helpers: detect group changes and initialize new groups
 * ================================================================ */

/*
 * Compare pending row's GROUP BY columns with current group values.
 * Returns true if the pending row belongs to a different group.
 */
static bool
is_group_boundary(GapFillExecState *state)
{
	int			i;

	for (i = 0; i < state->num_group_keys; i++)
	{
		int			attno = state->group_key_attnos[i];
		GapFillColumnInfo *cs = &state->col_states[attno];

		if (state->pending_isnull[attno] != state->current_group_isnull[i])
			return true;
		if (state->pending_isnull[attno])
			continue;	/* both NULL = same */
		if (!datumIsEqual(state->pending_values[attno],
						  state->current_group_values[i],
						  cs->typbyval, cs->typlen))
			return true;
	}
	return false;
}

/*
 * Initialize current group state from the pending row.
 * Copies group key values and resets time tracking + LOCF/interpolate state.
 * Does NOT consume the pending row (have_pending stays true).
 */
static void
switch_to_new_group(GapFillExecState *state)
{
	MemoryContext oldcxt;
	int			i;

	oldcxt = MemoryContextSwitchTo(
		state->css.ss.ps.state->es_query_cxt);

	for (i = 0; i < state->num_group_keys; i++)
	{
		int			attno = state->group_key_attnos[i];
		GapFillColumnInfo *cs = &state->col_states[attno];

		/* Free previous pass-by-ref group key to avoid memory leak */
		if (!cs->typbyval && !state->current_group_isnull[i])
			pfree(DatumGetPointer(state->current_group_values[i]));

		state->current_group_isnull[i] = state->pending_isnull[attno];
		if (!state->pending_isnull[attno])
			state->current_group_values[i] = datumCopy(
				state->pending_values[attno], cs->typbyval, cs->typlen);
		else
			state->current_group_values[i] = (Datum) 0;
	}

	MemoryContextSwitchTo(oldcxt);

	/* Reset time tracking to start of range */
	state->next_ts = state->start_ts;

	/* Reset LOCF/interpolate state for new group */
	for (i = 0; i < state->num_attrs; i++)
	{
		GapFillColumnInfo *cs = &state->col_states[i];

		if (cs->kind == GFCOL_CARRY_FORWARD)
		{
			/* Free previous pass-by-ref LOCF value to avoid leak */
			if (cs->locf_has_value && !cs->typbyval && !cs->locf_isnull)
				pfree(DatumGetPointer(cs->locf_value));
			cs->locf_has_value = false;
			cs->locf_isnull = true;
		}
		if (cs->kind == GFCOL_LINEAR_FILL)
		{
			cs->interp_has_prev = false;
			cs->interp_has_next = false;
		}
	}

	state->group_initialized = true;
}

/* ================================================================
 * Build gap (synthetic) tuple
 * ================================================================ */

static TupleTableSlot *
build_synthetic_tuple(GapFillExecState *state)
{
	TupleTableSlot *slot = state->css.ss.ps.ps_ResultTupleSlot;
	int			i;

	ExecClearTuple(slot);

	for (i = 0; i < state->num_attrs; i++)
	{
		GapFillColumnInfo *cs = &state->col_states[i];

		if (i == state->bucket_attno)
		{
			/* Bucket column: use next_ts */
			slot->tts_values[i] = int64_to_time_datum(state->next_ts, state->time_type);
			slot->tts_isnull[i] = false;
		}
		else if (cs->kind == GFCOL_GROUP_KEY)
		{
			/* GROUP BY column: use current group value */
			int idx = cs->group_key_idx;
			slot->tts_values[i] = state->current_group_values[idx];
			slot->tts_isnull[i] = state->current_group_isnull[idx];
		}
		else if (cs->kind == GFCOL_CARRY_FORWARD && cs->locf_has_value)
		{
			/* LOCF: carry forward last non-NULL value */
			slot->tts_values[i] = cs->locf_value;
			slot->tts_isnull[i] = cs->locf_isnull;
		}
		else if (cs->kind == GFCOL_LINEAR_FILL &&
				 cs->interp_has_prev && cs->interp_has_next)
		{
			/* Interpolate: linear interpolation between prev and next */
			double		frac;
			double		val;
			int64		range_ts;

			range_ts = cs->interp_next_ts - cs->interp_prev_ts;
			if (range_ts > 0)
			{
				frac = (double)(state->next_ts - cs->interp_prev_ts) / (double) range_ts;
				val = cs->interp_prev_val + frac * (cs->interp_next_val - cs->interp_prev_val);
				slot->tts_values[i] = double_to_numeric_datum(val, cs->typoid);
				slot->tts_isnull[i] = false;
			}
			else
			{
				slot->tts_values[i] = (Datum) 0;
				slot->tts_isnull[i] = true;
			}
		}
		else if (cs->has_default)
		{
			/* Constant expression: use the pre-computed value */
			slot->tts_values[i] = cs->default_value;
			slot->tts_isnull[i] = cs->default_isnull;
		}
		else
		{
			/* NULL for non-tracked columns */
			slot->tts_values[i] = (Datum) 0;
			slot->tts_isnull[i] = true;
		}
	}

	ExecStoreVirtualTuple(slot);

	/*
	 * Evaluate per-column gap expressions (COALESCE, CASE, etc.).
	 * Each gap_expr has Aggref nodes replaced with typed NULL constants,
	 * so COALESCE(NULL, 0) evaluates to 0 on gap rows.
	 * We evaluate directly into the slot's tts_values/tts_isnull arrays,
	 * avoiding the ExecProject slot-clearing problem.
	 */
	{
		ExprContext *econtext = state->css.ss.ps.ps_ExprContext;
		bool		need_eval = false;

		for (i = 0; i < state->num_attrs; i++)
		{
			if (state->col_states[i].gap_expr != NULL)
			{
				need_eval = true;
				break;
			}
		}

		if (need_eval)
		{
			ResetExprContext(econtext);
			econtext->ecxt_scantuple = slot;

			for (i = 0; i < state->num_attrs; i++)
			{
				GapFillColumnInfo *cs = &state->col_states[i];

				if (cs->gap_expr != NULL)
				{
					bool	isnull;
					Datum	val = ExecEvalExpr(cs->gap_expr, econtext, &isnull);

					slot->tts_values[i] = val;
					slot->tts_isnull[i] = isnull;
				}
			}
		}
	}

	state->next_ts = step_to_next_bucket(state, state->next_ts);
	return slot;
}

/* ================================================================
 * Build real (child) tuple, updating LOCF/interpolate state
 * ================================================================ */

static TupleTableSlot *
forward_data_tuple(GapFillExecState *state)
{
	TupleTableSlot *slot = state->css.ss.ps.ps_ResultTupleSlot;
	int64			row_ts;
	int				i;

	ExecClearTuple(slot);

	/* Get the timestamp of this row */
	row_ts = time_datum_to_int64(state->pending_values[state->bucket_attno],
						   state->time_type);

	for (i = 0; i < state->num_attrs; i++)
	{
		GapFillColumnInfo *cs = &state->col_states[i];

		slot->tts_values[i] = state->pending_values[i];
		slot->tts_isnull[i] = state->pending_isnull[i];

		/* Update LOCF state */
		if (cs->kind == GFCOL_CARRY_FORWARD)
		{
			if (!state->pending_isnull[i])
			{
				MemoryContext oldcxt = MemoryContextSwitchTo(
					state->css.ss.ps.state->es_query_cxt);

				/* Free previous pass-by-ref value to avoid memory leak */
				if (cs->locf_has_value && !cs->typbyval && !cs->locf_isnull)
					pfree(DatumGetPointer(cs->locf_value));

				cs->locf_value = datumCopy(state->pending_values[i],
										   cs->typbyval, cs->typlen);
				cs->locf_isnull = false;
				cs->locf_has_value = true;

				MemoryContextSwitchTo(oldcxt);
			}
		}

		/* Update interpolate state (shift prev, update with current) */
		if (cs->kind == GFCOL_LINEAR_FILL && !state->pending_isnull[i])
		{
			cs->interp_prev_ts = row_ts;
			cs->interp_prev_val = numeric_datum_to_double(state->pending_values[i],
												   cs->typoid);
			cs->interp_has_prev = true;
		}
	}

	ExecStoreVirtualTuple(slot);

	state->have_pending = false;
	state->next_ts = step_to_next_bucket(state, row_ts);

	return slot;
}

/* ================================================================
 * Pre-scan for interpolation: peek at next non-NULL value
 * ================================================================
 * Note: For V1, interpolation lookahead is limited. We update
 * the "next" sample point whenever we fetch a pending row that
 * has a non-NULL value for the interpolate column. This means
 * interpolation works between consecutive data points but not
 * across multiple gaps without data.
 */

static void
capture_interpolation_target(GapFillExecState *state)
{
	int64	row_ts;
	int		i;

	if (!state->have_pending)
		return;

	row_ts = time_datum_to_int64(state->pending_values[state->bucket_attno],
						   state->time_type);

	for (i = 0; i < state->num_attrs; i++)
	{
		GapFillColumnInfo *cs = &state->col_states[i];

		if (cs->kind != GFCOL_LINEAR_FILL)
			continue;

		if (!state->pending_isnull[i])
		{
			cs->interp_next_ts = row_ts;
			cs->interp_next_val = numeric_datum_to_double(state->pending_values[i],
												   cs->typoid);
			cs->interp_has_next = true;
		}
	}
}

/* ================================================================
 * Executor Callbacks
 * ================================================================ */

static void
gapfill_begin(CustomScanState *node, EState *estate, int eflags)
{
	GapFillExecState *state = (GapFillExecState *) node;
	CustomScan *cscan = (CustomScan *) node->ss.ps.plan;
	ListCell   *lc;

	/* Initialize child plan states */
	node->custom_ps = NIL;
	foreach(lc, cscan->custom_plans)
	{
		Plan	   *child_plan = (Plan *) lfirst(lc);
		PlanState  *child_ps = ExecInitNode(child_plan, estate, eflags);

		node->custom_ps = lappend(node->custom_ps, child_ps);
	}

	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	if (node->custom_ps != NIL)
		state->child_ps = (PlanState *) linitial(node->custom_ps);

	/* Result tuple slot is already initialized by ExecInitCustomScan
	 * via ExecAssignScanProjectionInfo → ExecInitResultTupleSlotTL,
	 * using the plan's targetlist for the schema. */
}

/*
 * Main gapfill execution loop.
 */
static TupleTableSlot *
gapfill_exec(CustomScanState *node)
{
	GapFillExecState *state = (GapFillExecState *) node;

	/* Lazy initialization on first call */
	if (!state->initialized)
		init_gapfill_state(state);

	for (;;)
	{
		CHECK_FOR_INTERRUPTS();

		/*
		 * If we've passed the finish timestamp for the current group,
		 * look for a row from a new group to continue processing.
		 */
		if (state->next_ts >= state->finish_ts)
		{
			if (state->num_group_keys > 0)
			{
				/* Multi-group mode: skip same-group rows, find next group */
				for (;;)
				{
					if (!state->have_pending)
					{
						if (state->child_exhausted)
							return ExecClearTuple(state->css.ss.ps.ps_ResultTupleSlot);
						pull_next_aggregate_row(state);
						if (!state->have_pending)
							return ExecClearTuple(state->css.ss.ps.ps_ResultTupleSlot);
					}

					if (!state->group_initialized ||
						is_group_boundary(state))
					{
						/* New group (or first group): switch to it */
						switch_to_new_group(state);
						break;
					}

					/* Same group, beyond finish: discard and keep looking */
					state->have_pending = false;
				}
				continue;	/* re-enter main loop with new group */
			}
			return ExecClearTuple(state->css.ss.ps.ps_ResultTupleSlot);
		}

		/* Ensure we have a pending child row (if any remain) */
		if (!state->have_pending && !state->child_exhausted)
			pull_next_aggregate_row(state);

		if (state->have_pending)
		{
			int64	pending_ts;
			bool	is_new_group = false;

			/* Initialize first group from pending row */
			if (state->num_group_keys > 0 && !state->group_initialized)
				switch_to_new_group(state);

			/* Detect group change */
			if (state->num_group_keys > 0 && state->group_initialized)
				is_new_group = is_group_boundary(state);

			if (!is_new_group)
			{
				pending_ts = time_datum_to_int64(
					state->pending_values[state->bucket_attno],
					state->time_type);

				/* Skip rows before start */
				if (pending_ts < state->start_ts)
				{
					state->have_pending = false;
					continue;
				}

				/* Skip rows at or after finish */
				if (pending_ts >= state->finish_ts)
				{
					state->have_pending = false;
					if (state->num_group_keys == 0)
						state->child_exhausted = true;
					/* Fall through to emit remaining gaps */
				}
				else if (pending_ts > state->next_ts)
				{
					/* Gap detected: emit synthetic row for next_ts */
					capture_interpolation_target(state);
					return build_synthetic_tuple(state);
				}
				else if (pending_ts == state->next_ts)
				{
					/* Match: emit real row */
					return forward_data_tuple(state);
				}
				else
				{
					/* pending_ts < next_ts: duplicate or out-of-order, skip */
					state->have_pending = false;
					continue;
				}
			}
			/* else: is_new_group — fall through to emit remaining gaps
			 * for old group. When all gaps are emitted, next_ts >= finish_ts
			 * and the top-of-loop group transition logic handles the switch. */
		}

		/* No more child rows for current group: emit gaps until finish.
		 * In multi-group mode, only emit gaps if a group has been initialized
		 * (i.e., child produced at least one row). Empty segments should not
		 * generate gap rows with NULL group keys. */
		if (state->next_ts < state->finish_ts)
		{
			if (state->num_group_keys > 0 && !state->group_initialized)
				return ExecClearTuple(state->css.ss.ps.ps_ResultTupleSlot);
			return build_synthetic_tuple(state);
		}

		return ExecClearTuple(state->css.ss.ps.ps_ResultTupleSlot);
	}
}

static void
gapfill_end(CustomScanState *node)
{
	ListCell   *lc;

	foreach(lc, node->custom_ps)
	{
		PlanState  *child_ps = (PlanState *) lfirst(lc);
		ExecEndNode(child_ps);
	}
}

static void
gapfill_rescan(CustomScanState *node)
{
	GapFillExecState *state = (GapFillExecState *) node;
	ListCell   *lc;
	int			i;

	/*
	 * Free pass-by-reference Datum values before resetting state to avoid
	 * memory leaks across repeated rescans (e.g., in nested loops).
	 */
	if (state->initialized && state->col_states)
	{
		for (i = 0; i < state->num_col_states; i++)
		{
			GapFillColumnInfo *cs = &state->col_states[i];

			if (cs->kind == GFCOL_CARRY_FORWARD &&
				cs->locf_has_value && !cs->typbyval && !cs->locf_isnull)
			{
				pfree(DatumGetPointer(cs->locf_value));
				cs->locf_has_value = false;
			}
		}
	}

	/* Free pass-by-reference group key values */
	if (state->group_initialized && state->current_group_values)
	{
		for (i = 0; i < state->num_group_keys; i++)
		{
			int			attno = state->group_key_attnos[i];
			GapFillColumnInfo *cs = &state->col_states[attno];

			if (!cs->typbyval && !state->current_group_isnull[i])
				pfree(DatumGetPointer(state->current_group_values[i]));
		}
	}

	state->initialized = false;
	state->child_exhausted = false;
	state->have_pending = false;
	state->group_initialized = false;

	foreach(lc, node->custom_ps)
	{
		PlanState  *child_ps = (PlanState *) lfirst(lc);
		ExecReScan(child_ps);
	}
}

static void
gapfill_explain(CustomScanState *node, List *ancestors, ExplainState *es)
{
	GapFillExecState *state = (GapFillExecState *) node;

	ExplainPropertyText("Gap Fill", "time_bucket_gapfill", es);

	if (state->initialized)
	{
		int			locf_count = 0;
		int			interp_count = 0;
		int			i;

		for (i = 0; i < state->num_col_states; i++)
		{
			if (state->col_states[i].kind == GFCOL_CARRY_FORWARD)
				locf_count++;
			else if (state->col_states[i].kind == GFCOL_LINEAR_FILL)
				interp_count++;
		}

		if (locf_count > 0)
			ExplainPropertyText("LOCF Columns",
								psprintf("%d", locf_count), es);
		if (interp_count > 0)
			ExplainPropertyText("Interpolate Columns",
								psprintf("%d", interp_count), es);
		if (state->num_group_keys > 0)
			ExplainPropertyText("Group Keys",
								psprintf("%d", state->num_group_keys), es);
	}
}
