/*-------------------------------------------------------------------------
 *
 * refresh.c
 *    Continuous Aggregate REFRESH procedure.
 *
 *    Implements: CALL time_series.refresh_continuous_aggregate(name,
 *    start, end).
 *
 *    Two-transaction model via SPI_commit_and_chain:
 *      TX1 (short): advisory lock -> L1->L2 migration -> delete L1 ->
 *                   COMMIT
 *      TX2 (main):  advisory lock -> gather L2 -> interval merge ->
 *                   DELETE+INSERT materialization -> advance watermark
 *                   -> trim L2
 *
 * Copyright (c) 2026 HashData Inc.
 *
 * IDENTIFICATION
 *    contrib/time_series/src/cagg/refresh.c
 *
 *-------------------------------------------------------------------------
 */
#include "../include/time_series.h"
#include "../include/cagg/cagg.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/skey.h"
#include "access/table.h"
#include "access/xact.h"
#include "storage/lmgr.h"		/* LockRelationOid */
#include "miscadmin.h"			/* GetUserId */
#include "utils/acl.h"			/* pg_class_ownercheck, aclcheck_error */
#include "catalog/namespace.h"
#include "datatype/timestamp.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"		/* CacheInvalidateRelcacheByRelid */
#include "utils/lsyscache.h"
#include "tcop/pquery.h"		/* ActivePortal */
#include "utils/guc.h"			/* set_config_option */
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"

#include "nodes/parsenodes.h"

#include "cdb/cdbvars.h"

#ifdef FAULT_INJECTOR
#include "utils/faultinjector.h"
#endif

/*
 * Internal structures
 */

typedef struct CaggRefreshInfo
{
	int			cagg_id;
	Oid			source_table_oid;
	char	   *mat_table_schema;
	char	   *mat_table_name;
	char	   *partial_view_schema;
	char	   *partial_view_name;
	char	   *bucket_column;	/* source table time column (e.g. "time") */
	char	   *mat_bucket_col; /* mat table bucket column (e.g. "bucket") */
	Interval   *bucket_width;
	Oid			time_type;
	/* Origin/offset/timezone from cagg_bucket_function (NULL if not set) */
	bool		has_origin;
	TimestampTz bucket_origin;
	Interval   *bucket_offset;	/* NULL if not set */
	char	   *bucket_timezone;	/* NULL if not set */
}			CaggRefreshInfo;

typedef struct DirtyInterval
{
	TimestampTz start;
	TimestampTz end;			/* exclusive */
}			DirtyInterval;

/*
 * Parse a possibly schema-qualified name "schema.name" or just "name".
 * If no schema, defaults to "public".
 */

static void
cagg_parse_qualified_name(const char *input,
						  char *schema_out, char *name_out)
{
	const char *dot = strchr(input, '.');

	if (dot)
	{
		int			slen = dot - input;

		if (slen >= NAMEDATALEN)
			slen = NAMEDATALEN - 1;
		memcpy(schema_out, input, slen);
		schema_out[slen] = '\0';
		strlcpy(name_out, dot + 1, NAMEDATALEN);
	}
	else
	{
		strlcpy(schema_out, "public", NAMEDATALEN);
		strlcpy(name_out, input, NAMEDATALEN);
	}
}

/*
 * Look up CAGG metadata from catalog.
 * cagg_name can be "view_name" or "schema.view_name".
 */

static void
cagg_lookup_metadata(const char *cagg_name, CaggRefreshInfo * info)
{
	int			ret;
	Oid			argtypes[2] = {TEXTOID, TEXTOID};
	Datum		args[2];
	MemoryContext caller_cxt = CurrentMemoryContext;
	char		schema_buf[NAMEDATALEN];
	char		name_buf[NAMEDATALEN];

	cagg_parse_qualified_name(cagg_name, schema_buf, name_buf);

	args[0] = CStringGetTextDatum(schema_buf);
	args[1] = CStringGetTextDatum(name_buf);

	/*
	 * Query CAGG metadata + mat table's first column name (the bucket alias).
	 * Match on BOTH user_view_schema AND user_view_name to avoid ambiguity
	 * when multiple schemas have same-named CAGGs.
	 */
	ret = SPI_execute_with_args(
								"SELECT c.cagg_id, c.source_table_oid, "
								"c.mat_table_schema, c.mat_table_name, "
								"c.partial_view_schema, c.partial_view_name, "
								"c.bucket_column, c.bucket_width, bf.time_type, "
								"a.attname AS mat_bucket_col, "
								"bf.bucket_origin, bf.bucket_offset, bf.bucket_timezone "
								"FROM time_series.continuous_agg c "
								"JOIN time_series.cagg_bucket_function bf ON c.cagg_id = bf.cagg_id "
								"JOIN pg_class pc ON pc.relname = c.mat_table_name "
								"  AND pc.relnamespace = (SELECT oid FROM pg_namespace "
								"                          WHERE nspname = c.mat_table_schema) "
								"JOIN pg_attribute a ON a.attrelid = pc.oid AND a.attnum = 1 "
								"                   AND NOT a.attisdropped "
								"WHERE c.user_view_schema = $1 AND c.user_view_name = $2",
								2, argtypes, args, NULL, true, 1);

	if (ret != SPI_OK_SELECT || SPI_processed == 0)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("continuous aggregate \"%s\" does not exist",
						cagg_name)));

	{
		HeapTuple	tup = SPI_tuptable->vals[0];
		TupleDesc	desc = SPI_tuptable->tupdesc;
		bool		isnull;
		MemoryContext oldcxt;

		info->cagg_id = DatumGetInt32(
									  SPI_getbinval(tup, desc, 1, &isnull));
		info->source_table_oid = DatumGetObjectId(
												  SPI_getbinval(tup, desc, 2, &isnull));

		/* Copy strings into caller's context so they survive SPI_finish */
		oldcxt = MemoryContextSwitchTo(caller_cxt);
		info->mat_table_schema = pstrdup(SPI_getvalue(tup, desc, 3));
		info->mat_table_name = pstrdup(SPI_getvalue(tup, desc, 4));
		info->partial_view_schema = pstrdup(SPI_getvalue(tup, desc, 5));
		info->partial_view_name = pstrdup(SPI_getvalue(tup, desc, 6));
		info->bucket_column = pstrdup(SPI_getvalue(tup, desc, 7));
		/* Interval is fixed-size (16 bytes), pass-by-reference */
		info->bucket_width = DatumGetIntervalP(datumCopy(
														 SPI_getbinval(tup, desc, 8, &isnull), false, sizeof(Interval)));
		info->time_type = DatumGetObjectId(
										   SPI_getbinval(tup, desc, 9, &isnull));
		{
			char	   *mbc = SPI_getvalue(tup, desc, 10);

			info->mat_bucket_col = mbc ? pstrdup(mbc) : pstrdup("bucket");
		}

		/* bucket_origin (col 11) -- timestamptz, may be NULL */
		{
			Datum		d = SPI_getbinval(tup, desc, 11, &isnull);

			if (!isnull)
			{
				info->has_origin = true;
				info->bucket_origin = DatumGetTimestampTz(d);
			}
			else
			{
				info->has_origin = false;
				info->bucket_origin = DT_NOBEGIN;
			}
		}
		/* bucket_offset (col 12) -- interval, may be NULL */
		{
			Datum		d = SPI_getbinval(tup, desc, 12, &isnull);

			info->bucket_offset = isnull ? NULL :
				DatumGetIntervalP(datumCopy(d, false, sizeof(Interval)));
		}
		/* bucket_timezone (col 13) -- text, may be NULL */
		{
			char	   *tz = SPI_getvalue(tup, desc, 13);

			info->bucket_timezone = tz ? pstrdup(tz) : NULL;
		}

		MemoryContextSwitchTo(oldcxt);
	}
}

/*
 * Acquire advisory lock for this CAGG (per-transaction scope)
 */

static void
cagg_acquire_lock(int cagg_id)
{
	Oid			argtypes[1] = {INT4OID};
	Datum		args[1];

	args[0] = Int32GetDatum(cagg_id);
	SPI_execute_with_args(
						  "SELECT pg_advisory_xact_lock($1::bigint)",
						  1, argtypes, args, NULL, true, 0);
}

/*
 * Segment-local L1 -> L2 migration function.
 *
 * Called on EACH segment via:
 *   SELECT time_series._cagg_move_l1_to_l2($source_oid)
 *   FROM time_series.cagg_watermark WHERE cagg_id = $cagg_id;
 *
 * cagg_watermark is DISTRIBUTED RANDOMLY (one row per segment per CAGG),
 * so this SELECT naturally dispatches to every segment exactly once.
 *
 * On each segment, this function:
 *   1. Opens L1 (cagg_invalidation_log) and scans for source_table_oid
 *   2. Opens L2 (cagg_materialization_log)
 *   3. Reads all cagg_ids for this source from continuous_agg (REPLICATED)
 *   4. For each L1 entry: simple_heap_insert into L2 for every CAGG
 *   5. simple_heap_delete the L1 entry
 *
 * All operations are segment-local: zero network I/O, zero SPI overhead.
 */

PG_FUNCTION_INFO_V1(cagg_segment_move_l1_to_l2);

Datum
cagg_segment_move_l1_to_l2(PG_FUNCTION_ARGS)
{
	Oid			source_oid = PG_GETARG_OID(0);
	Oid			ns_oid;
	Oid			l1_oid,
				l2_oid,
				ca_oid;
	Relation	l1_rel,
				l2_rel,
				ca_rel;
	TableScanDesc l1_scan,
				ca_scan;
	HeapTuple	l1_tup,
				ca_tup;
	List	   *cagg_ids = NIL;
	ListCell   *lc;

	ns_oid = ht_get_namespace_oid_cached();

	/* Open all three relations */
	l1_oid = get_relname_relid("cagg_invalidation_log", ns_oid);
	l2_oid = get_relname_relid("cagg_materialization_log", ns_oid);
	ca_oid = get_relname_relid("continuous_agg", ns_oid);

	if (!OidIsValid(l1_oid) || !OidIsValid(l2_oid) || !OidIsValid(ca_oid))
		elog(ERROR, "_cagg_move_l1_to_l2: catalog tables not found");

	/*
	 * Lock order: L1 (RowExclusive) -> L2 (RowExclusive) -> continuous_agg
	 * (AccessShare).
	 *
	 * The trigger insert path acquires the catalog tables in the opposite
	 * order: continuous_agg (AccessShare) -> cagg_watermark (AccessShare) ->
	 * L1 (RowExclusive).  The shared pair {L1, continuous_agg} is therefore
	 * acquired in opposite orders here and there.
	 *
	 * This is currently safe because the lock modes don't pairwise conflict:
	 * AccessShare on continuous_agg never blocks against itself, and both
	 * paths take RowExclusive on L1 (compatible with itself).  If either mode
	 * is ever tightened (e.g. taking Share or stronger on continuous_agg
	 * during DDL) this becomes a deadlock.  Update both call sites together
	 * if you change either mode.
	 */
	l1_rel = table_open(l1_oid, RowExclusiveLock);
	l2_rel = table_open(l2_oid, RowExclusiveLock);
	ca_rel = table_open(ca_oid, AccessShareLock);

	/*
	 * Step 1: Collect all cagg_ids for this source from continuous_agg.
	 * continuous_agg is REPLICATED, so every segment has a full copy. We scan
	 * it to find which CAGGs reference this source_oid.
	 *
	 * Use the active snapshot (MVCC) so visibility rules are honored;
	 * SnapshotSelf would expose uncommitted writes from this xact and ignore
	 * xmax (a fragile choice if the surrounding lock scheme is ever relaxed).
	 * Mirrors upstream invalidation log scans.
	 */
	ca_scan = heap_beginscan(ca_rel, GetActiveSnapshot(), 0, NULL, NULL, 0);
	while ((ca_tup = heap_getnext(ca_scan, ForwardScanDirection)) != NULL)
	{
		bool		isnull;
		Datum		d_source;
		Datum		d_cagg_id;

		/* continuous_agg column 6 = source_table_oid (oid) */
		d_source = heap_getattr(ca_tup, 6, RelationGetDescr(ca_rel), &isnull);
		if (isnull || DatumGetObjectId(d_source) != source_oid)
			continue;

		/* continuous_agg column 1 = cagg_id (int4) */
		d_cagg_id = heap_getattr(ca_tup, 1, RelationGetDescr(ca_rel), &isnull);
		if (!isnull)
			cagg_ids = lappend_int(cagg_ids, DatumGetInt32(d_cagg_id));
	}
	heap_endscan(ca_scan);
	table_close(ca_rel, AccessShareLock);

	if (cagg_ids == NIL)
	{
		/* No CAGGs for this source -- just close and return */
		table_close(l2_rel, RowExclusiveLock);
		table_close(l1_rel, RowExclusiveLock);
		PG_RETURN_VOID();
	}

	/*
	 * Step 2: Scan L1 for entries matching source_oid. For each entry, insert
	 * into L2 for EVERY cagg_id, then delete from L1.
	 *
	 * L1 schema: (source_table_oid oid, lowest_modified timestamptz,
	 * greatest_modified timestamptz) L2 schema: (cagg_id int, lowest_modified
	 * timestamptz, greatest_modified timestamptz)
	 */
	/* MVCC for L1 scan -- see comment on the continuous_agg scan above. */
	l1_scan = heap_beginscan(l1_rel, GetActiveSnapshot(), 0, NULL, NULL, 0);
	while ((l1_tup = heap_getnext(l1_scan, ForwardScanDirection)) != NULL)
	{
		bool		isnull;
		Datum		d_src,
					d_low,
					d_high;
		TimestampTz lowest,
					greatest;

		/* Filter: only process entries for our source_oid */
		d_src = heap_getattr(l1_tup, 1, RelationGetDescr(l1_rel), &isnull);
		if (isnull || DatumGetObjectId(d_src) != source_oid)
			continue;

		d_low = heap_getattr(l1_tup, 2, RelationGetDescr(l1_rel), &isnull);
		lowest = DatumGetTimestampTz(d_low);
		d_high = heap_getattr(l1_tup, 3, RelationGetDescr(l1_rel), &isnull);
		greatest = DatumGetTimestampTz(d_high);

		/* Insert one L2 entry for each CAGG */
		foreach(lc, cagg_ids)
		{
			int			cagg_id = lfirst_int(lc);
			Datum		l2_vals[3];
			bool		l2_nulls[3] = {false, false, false};
			HeapTuple	l2_tup;

			l2_vals[0] = Int32GetDatum(cagg_id);
			l2_vals[1] = TimestampTzGetDatum(lowest);
			l2_vals[2] = TimestampTzGetDatum(greatest);

			l2_tup = heap_form_tuple(RelationGetDescr(l2_rel),
									 l2_vals, l2_nulls);
			simple_heap_insert(l2_rel, l2_tup);
			heap_freetuple(l2_tup);
		}

		/* Delete consumed L1 entry by its TID */
		simple_heap_delete(l1_rel, &l1_tup->t_self);
	}
	heap_endscan(l1_scan);

	table_close(l2_rel, RowExclusiveLock);
	table_close(l1_rel, RowExclusiveLock);

	PG_RETURN_VOID();
}

/*
 * TX1: Dispatch segment-local L1 -> L2 migration to all segments.
 *
 * Uses the cagg_watermark trick: cagg_watermark is DISTRIBUTED RANDOMLY
 * (one row per segment per CAGG), so SELECT ... FROM cagg_watermark
 * WHERE cagg_id = $1 naturally dispatches to every segment once.
 */

static void
cagg_migrate_l1_to_l2(CaggRefreshInfo * info)
{
	Oid			argtypes[2] = {OIDOID, INT4OID};
	Datum		args[2];

	args[0] = ObjectIdGetDatum(info->source_table_oid);
	args[1] = Int32GetDatum(info->cagg_id);

	SPI_execute_with_args(
						  "SELECT time_series._cagg_move_l1_to_l2($1) "
						  "FROM time_series.cagg_watermark WHERE cagg_id = $2",
						  2, argtypes, args, NULL, true, 0);
}

/*
 * Align a timestamp to bucket boundary using time_bucket().
 *
 * If origin is provided (has_origin=true), it is passed to
 * time_bucket($1, $2, $3) so origin-shifted CAGGs align correctly.
 * This matches upstream circumscribed alignment behavior.
 */

/*
 * Build and execute the appropriate time_bucket() SPI call based on
 * which bucket function parameters are set (origin, offset, timezone).
 *
 * time_bucket variants:
 *   time_bucket(interval, timestamptz)                         -- plain
 *   time_bucket(interval, timestamptz, timestamptz)            -- origin
 *   time_bucket(interval, timestamptz, interval)               -- offset
 *   time_bucket(interval, timestamptz, text)                   -- timezone
 *   time_bucket(interval, timestamptz, text, timestamptz)      -- tz+origin
 *
 * If suffix is non-NULL, it's appended to the SELECT (e.g. " + $1" for
 * bucket end).
 */
static TimestampTz
cagg_align_bucket_internal(TimestampTz ts, CaggRefreshInfo * info,
						   const char *suffix)
{
	bool		isnull;
	int			ret;
	StringInfoData sql;

	if (TIMESTAMP_NOT_FINITE(ts))
		return ts;

	initStringInfo(&sql);

	if (info->bucket_timezone != NULL)
	{
		/*
		 * Timezone variant: time_bucket($1, $2, $3) or with an origin
		 * time_bucket($1, $2, $3, $4).
		 */
		if (info->has_origin)
		{
			Oid			argtypes[4] = {
				INTERVALOID, TIMESTAMPTZOID, TEXTOID, TIMESTAMPTZOID
			};
			Datum		args[4];

			args[0] = IntervalPGetDatum(info->bucket_width);
			args[1] = TimestampTzGetDatum(ts);
			args[2] = CStringGetTextDatum(info->bucket_timezone);
			args[3] = TimestampTzGetDatum(info->bucket_origin);

			appendStringInfo(&sql, "SELECT time_series.time_bucket($1, $2, $3, $4)%s",
							 suffix ? suffix : "");
			ret = SPI_execute_with_args(sql.data, 4, argtypes, args, NULL, true, 1);
		}
		else
		{
			Oid			argtypes[3] = {INTERVALOID, TIMESTAMPTZOID, TEXTOID};
			Datum		args[3];

			args[0] = IntervalPGetDatum(info->bucket_width);
			args[1] = TimestampTzGetDatum(ts);
			args[2] = CStringGetTextDatum(info->bucket_timezone);

			appendStringInfo(&sql, "SELECT time_series.time_bucket($1, $2, $3)%s",
							 suffix ? suffix : "");
			ret = SPI_execute_with_args(sql.data, 3, argtypes, args, NULL, true, 1);
		}
	}
	else if (info->bucket_offset != NULL)
	{
		/* offset variant: time_bucket($1, $2, $3::interval) */
		Oid			argtypes[3] = {INTERVALOID, TIMESTAMPTZOID, INTERVALOID};
		Datum		args[3];

		args[0] = IntervalPGetDatum(info->bucket_width);
		args[1] = TimestampTzGetDatum(ts);
		args[2] = IntervalPGetDatum(info->bucket_offset);

		appendStringInfo(&sql, "SELECT time_series.time_bucket($1, $2, $3)%s",
						 suffix ? suffix : "");
		ret = SPI_execute_with_args(sql.data, 3, argtypes, args, NULL, true, 1);
	}
	else if (info->has_origin)
	{
		/* origin variant: time_bucket($1, $2, $3::timestamptz) */
		Oid			argtypes[3] = {INTERVALOID, TIMESTAMPTZOID, TIMESTAMPTZOID};
		Datum		args[3];

		args[0] = IntervalPGetDatum(info->bucket_width);
		args[1] = TimestampTzGetDatum(ts);
		args[2] = TimestampTzGetDatum(info->bucket_origin);

		appendStringInfo(&sql, "SELECT time_series.time_bucket($1, $2, $3)%s",
						 suffix ? suffix : "");
		ret = SPI_execute_with_args(sql.data, 3, argtypes, args, NULL, true, 1);
	}
	else
	{
		/* plain variant: time_bucket($1, $2) */
		Oid			argtypes[2] = {INTERVALOID, TIMESTAMPTZOID};
		Datum		args[2];

		args[0] = IntervalPGetDatum(info->bucket_width);
		args[1] = TimestampTzGetDatum(ts);

		appendStringInfo(&sql, "SELECT time_series.time_bucket($1, $2)%s",
						 suffix ? suffix : "");
		ret = SPI_execute_with_args(sql.data, 2, argtypes, args, NULL, true, 1);
	}

	pfree(sql.data);

	if (ret != SPI_OK_SELECT || SPI_processed == 0)
		elog(ERROR, "cagg_refresh: could not align timestamp to bucket");

	return DatumGetTimestampTz(
							   SPI_getbinval(SPI_tuptable->vals[0],
											 SPI_tuptable->tupdesc, 1, &isnull));
}

/*
 * cagg_align_bucket_fast
 *		Fast-path C inline of time_bucket alignment for the common case:
 *		plain or origin-shifted bucketing with a fixed-microsecond width.
 *
 *		Equivalent to:
 *			SELECT time_bucket(width, ts)              -- end=false
 *			SELECT time_bucket(width, ts) + width      -- end=true
 *		but without the SPI dispatch + planner + executor overhead
 *		(~1 ms per call) -- the actual math is a few nanoseconds.
 *
 *		Boundary formula: origin + floor((ts - origin) / width) * width.
 *		Default origin (when has_origin=false) is JAN_3_2000
 *		(2000-01-03 00:00 UTC = 2 * USECS_PER_DAY in PG epoch microseconds)
 *		to match time_bucket.c::DEFAULT_ORIGIN.
 *
 *		Returns true and writes *result on success.  Returns false (without
 *		writing *result) when the case requires the canonical SPI path:
 *			- bucket_timezone is set (DST / calendar arithmetic)
 *			- bucket_offset is set
 *			- bucket_width has a month component (variable length)
 *			- bucket_width is non-positive (defensive: caller error)
 *
 *		The infinite-timestamp short-circuit lives in the wrappers below
 *		so callers don't need to special-case before reading *result.
 */
static bool
cagg_align_bucket_fast(CaggRefreshInfo * info, TimestampTz ts,
					   bool end, TimestampTz *result)
{
	int64		width_usec;
	int64		origin_usec;
	int64		delta;
	int64		k;
	int64		aligned;

	if (info->bucket_timezone != NULL || info->bucket_offset != NULL)
		return false;

	if (info->bucket_width->month != 0)
		return false;

	width_usec = (int64) info->bucket_width->day * USECS_PER_DAY
		+ info->bucket_width->time;
	if (width_usec <= 0)
		return false;

	/*
	 * time_bucket.c uses Monday 2000-01-03 as the default origin so that
	 * weekly buckets align with a real Monday (PG epoch 2000-01-01 is a
	 * Saturday).  TimestampTz is microseconds since PG epoch, so the default
	 * in epoch microseconds is 2 days = 2 * USECS_PER_DAY.
	 */
	origin_usec = info->has_origin
		? (int64) info->bucket_origin
		: (2LL * USECS_PER_DAY);

	delta = (int64) ts - origin_usec;

	/*
	 * Floor division.  C99 integer division truncates toward zero, but for
	 * negative delta we want floor so timestamps before origin still land on
	 * a valid bucket boundary going backwards.
	 */
	k = delta / width_usec;
	if (delta < 0 && delta % width_usec != 0)
		k--;

	aligned = origin_usec + k * width_usec;

	if (end)
		aligned += width_usec;

	*result = (TimestampTz) aligned;
	return true;
}

static TimestampTz
cagg_align_to_bucket_start(CaggRefreshInfo * info, TimestampTz ts)
{
	TimestampTz result;

	if (TIMESTAMP_NOT_FINITE(ts))
		return ts;

	if (cagg_align_bucket_fast(info, ts, false, &result))
		return result;

	return cagg_align_bucket_internal(ts, info, NULL);
}

static TimestampTz
cagg_align_to_bucket_end(CaggRefreshInfo * info, TimestampTz ts)
{
	TimestampTz result;

	if (TIMESTAMP_NOT_FINITE(ts))
		return ts;

	if (cagg_align_bucket_fast(info, ts, true, &result))
		return result;

	return cagg_align_bucket_internal(ts, info, " + $1");
}

/*
 * qsort comparator: order DirtyInterval by start ascending.  Subtracting
 * the two TimestampTz (int64) values could overflow int, so compare them
 * explicitly.
 */
static int
cmp_dirty_interval_start(const void *a, const void *b)
{
	TimestampTz sa = ((const DirtyInterval *) a)->start;
	TimestampTz sb = ((const DirtyInterval *) b)->start;

	if (sa < sb)
		return -1;
	if (sa > sb)
		return 1;
	return 0;
}

/*
 * Gather L2 entries, align to bucket boundaries, merge overlapping
 * intervals (LeetCode 56 algorithm), and intersect with the
 * user-supplied refresh window.
 *
 * Returns the number of dirty intervals.  *intervals_out is palloc'd
 * in the caller's memory context.
 */

static int
cagg_gather_dirty_intervals(CaggRefreshInfo * info,
							TimestampTz window_start,
							TimestampTz window_end,
							DirtyInterval * *intervals_out)
{
	Oid			argtypes[1] = {INT4OID};
	Datum		args[1];
	int			ret;
	int			raw_count;
	DirtyInterval *raw = NULL;
	DirtyInterval *merged = NULL;
	int			num_merged = 0;
	int			i;

	args[0] = Int32GetDatum(info->cagg_id);

	ret = SPI_execute_with_args(
								"SELECT lowest_modified, greatest_modified "
								"FROM time_series.cagg_materialization_log "
								"WHERE cagg_id = $1 "
								"ORDER BY lowest_modified",
								1, argtypes, args, NULL, true, 0);

	if (ret != SPI_OK_SELECT || SPI_processed == 0)
	{
		*intervals_out = NULL;
		return 0;
	}

	raw_count = SPI_processed;
	raw = palloc(sizeof(DirtyInterval) * raw_count);

	/*
	 * Phase 1: Extract ALL values from SPI_tuptable FIRST. We must NOT call
	 * any SPI functions (like cagg_align_to_bucket_start) during this loop,
	 * because nested SPI calls invalidate SPI_tuptable.
	 */
	for (i = 0; i < raw_count; i++)
	{
		bool		null1,
					null2;

		raw[i].start = DatumGetTimestampTz(
										   SPI_getbinval(SPI_tuptable->vals[i],
														 SPI_tuptable->tupdesc, 1, &null1));
		raw[i].end = DatumGetTimestampTz(
										 SPI_getbinval(SPI_tuptable->vals[i],
													   SPI_tuptable->tupdesc, 2, &null2));

		if (null1 || null2)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("NULL value in materialization invalidation log"),
					 errhint("The cagg_materialization_log catalog may be corrupted.")));
	}

	/*
	 * Phase 2: Align to bucket boundaries AFTER all SPI results extracted.
	 * cagg_align_to_bucket_start/end use SPI internally.
	 */
	for (i = 0; i < raw_count; i++)
	{
		raw[i].start = cagg_align_to_bucket_start(info, raw[i].start);
		raw[i].end = cagg_align_to_bucket_end(info, raw[i].end);
	}

	/*
	 * Merge overlapping intervals (already sorted by start from ORDER BY).
	 * Classic LeetCode 56 algorithm.
	 */
	merged = palloc(sizeof(DirtyInterval) * raw_count);
	num_merged = 0;

	for (i = 0; i < raw_count; i++)
	{
		if (num_merged > 0 && raw[i].start <= merged[num_merged - 1].end)
		{
			/* Overlapping or adjacent -- extend the current merged interval */
			if (raw[i].end > merged[num_merged - 1].end)
				merged[num_merged - 1].end = raw[i].end;
		}
		else
		{
			/* New disjoint interval */
			merged[num_merged].start = raw[i].start;
			merged[num_merged].end = raw[i].end;
			num_merged++;
		}
	}

	pfree(raw);

	/*
	 * Intersect each merged interval with the user's refresh window. Skip
	 * intervals entirely outside the window; clamp those that partially
	 * overlap.
	 */
	{
		int			final_count = 0;

		for (i = 0; i < num_merged; i++)
		{
			TimestampTz s = merged[i].start;
			TimestampTz e = merged[i].end;

			/* Clamp to window */
			if (!TIMESTAMP_IS_NOBEGIN(window_start) && s < window_start)
				s = window_start;
			if (!TIMESTAMP_IS_NOEND(window_end) && e > window_end)
				e = window_end;

			/* Skip if no overlap */
			if (s >= e)
				continue;

			merged[final_count].start = s;
			merged[final_count].end = e;
			final_count++;
		}
		num_merged = final_count;
	}

	*intervals_out = merged;
	return num_merged;
}

/*
 * Refresh one dirty interval: DELETE old rows, INSERT new from
 * partial view.
 */

static void
cagg_refresh_one_interval(CaggRefreshInfo * info,
						  TimestampTz start, TimestampTz end)
{
	StringInfoData sql;
	char	   *start_str;
	char	   *end_str;
	uint64		deleted_rows;
	uint64		inserted_rows;

	/*
	 * Embed the bucket bounds as timestamptz literals rather than passing
	 * them as $1/$2 parameters.  The INSERT below scans the partial view,
	 * whose source scan filters on the bucket column (time_bucket(time)); the
	 * time_bucket_pushdown planner hook only synthesizes the prunable
	 * bare-time predicate from a `Var op Const`, so a Param there defeats
	 * source chunk pruning and forces a full scan of EVERY chunk on EVERY
	 * dirty interval.  Same reasoning and the same embed-literal remedy as
	 * the actual_boundary block in cagg_refresh() below.  start/end come from
	 * our own catalog-derived, merged, window-clamped dirty-interval
	 * computation -- never user input -- so embedding is injection-safe;
	 * timestamptz_out emits a canonical, timezone-qualified form that
	 * round-trips exactly, including +/-infinity.
	 */
	start_str = DatumGetCString(
								DirectFunctionCall1(timestamptz_out, TimestampTzGetDatum(start)));
	end_str = DatumGetCString(
							  DirectFunctionCall1(timestamptz_out, TimestampTzGetDatum(end)));

	initStringInfo(&sql);

	/*
	 * DELETE old materialized rows in this range. Use mat_bucket_col (the mat
	 * table's bucket column alias, e.g. "bucket"), NOT bucket_column (the
	 * source table's time column, e.g. "time").
	 */
	appendStringInfo(&sql,
					 "DELETE FROM %s.%s"
					 " WHERE %s >= '%s'::timestamptz"
					 "   AND %s <  '%s'::timestamptz",
					 quote_identifier(info->mat_table_schema),
					 quote_identifier(info->mat_table_name),
					 quote_identifier(info->mat_bucket_col), start_str,
					 quote_identifier(info->mat_bucket_col), end_str);

	SPI_execute(sql.data, false, 0);
	deleted_rows = SPI_processed;

	/*
	 * INSERT new rows from partial view for this range. The partial view's
	 * output column name matches mat_bucket_col.
	 */
	resetStringInfo(&sql);
	appendStringInfo(&sql,
					 "INSERT INTO %s.%s"
					 " SELECT * FROM %s.%s"
					 "  WHERE %s >= '%s'::timestamptz"
					 "    AND %s <  '%s'::timestamptz",
					 quote_identifier(info->mat_table_schema),
					 quote_identifier(info->mat_table_name),
					 quote_identifier(info->partial_view_schema),
					 quote_identifier(info->partial_view_name),
					 quote_identifier(info->mat_bucket_col), start_str,
					 quote_identifier(info->mat_bucket_col), end_str);

	SPI_execute(sql.data, false, 0);
	inserted_rows = SPI_processed;

	pfree(sql.data);
	pfree(start_str);
	pfree(end_str);

	/*
	 * Server-log audit of rows touched per interval.  Mirrors upstream
	 * materialize.c:597,607 "deleted/inserted N row(s)" at LOG level --
	 * default-visible diagnostic for CAGG drift in long-stability
	 * environments.
	 *
	 * The cagg_bgw_mock test framework's emit_log_hook captures every LOG
	 * message (server-side severity LOG > WARNING) into bgw_log. The
	 * mock_time_monotonic assertion is now PARTITION BY application_name, so
	 * cross-process LOG-level interleaving from multiple BGW workers no
	 * longer breaks msg_no ordering.
	 */
	elog(LOG,
		 "deleted " UINT64_FORMAT " row(s) from materialization table \"%s.%s\"",
		 deleted_rows, info->mat_table_schema, info->mat_table_name);
	elog(LOG,
		 "inserted " UINT64_FORMAT " row(s) into materialization table \"%s.%s\"",
		 inserted_rows, info->mat_table_schema, info->mat_table_name);
}

/*
 * Advance watermark: watermark = GREATEST(watermark, new_value)
 */

static void
cagg_advance_watermark(CaggRefreshInfo * info, TimestampTz new_wm)
{
	Oid			argtypes[2] = {TIMESTAMPTZOID, INT4OID};
	Datum		args[2];

	args[0] = TimestampTzGetDatum(new_wm);
	args[1] = Int32GetDatum(info->cagg_id);

	SPI_execute_with_args(
						  "UPDATE time_series.cagg_watermark "
						  "SET watermark = GREATEST(watermark, $1) "
						  "WHERE cagg_id = $2",
						  2, argtypes, args, NULL, false, 0);
}

/*
 * Gap materialization probe.
 *
 * Returns true if every source bucket in [lo, hi) is already present
 * in the mat table -- i.e. advancing the watermark to hi would hide
 * nothing.  lo may be DT_NOBEGIN (-infinity); hi must be finite.
 *
 * Bounds are embedded as timestamptz literals (not params) so
 * ts_extract_time_bounds can prune the source scan to just the gap's
 * chunks.  A Param on the time column defeats chunk pruning and
 * degrades this existence probe to a full ChunkScan.  Both bounds come
 * from our own catalog/argument state, never user input, so embedding
 * is injection-safe; timestamptz_out round-trips exactly, including a
 * -infinity lo (upper bound still prunes).  The bucket width stays a
 * param ($1): it sits inside time_bucket(), not in the prunable
 * Var-op-Const time predicate.
 */
static bool
cagg_gap_is_materialized(CaggRefreshInfo * info,
						 TimestampTz lo, TimestampTz hi)
{
	Oid			gap_argtypes[1] = {INTERVALOID};
	Datum		gap_args[1];
	StringInfoData gap_sql;
	int			gap_ret;
	char	   *lo_str;
	char	   *hi_str;
	bool		materialized;

	lo_str = DatumGetCString(
							 DirectFunctionCall1(timestamptz_out, TimestampTzGetDatum(lo)));
	hi_str = DatumGetCString(
							 DirectFunctionCall1(timestamptz_out, TimestampTzGetDatum(hi)));

	gap_args[0] = IntervalPGetDatum(info->bucket_width);

	initStringInfo(&gap_sql);
	appendStringInfo(&gap_sql,
					 "SELECT 1 FROM ("
					 "  SELECT DISTINCT time_series.time_bucket($1, %s) AS b "
					 "  FROM %s "
					 "  WHERE %s >= '%s'::timestamptz AND %s < '%s'::timestamptz"
					 ") src "
					 "WHERE NOT EXISTS ("
					 "  SELECT 1 FROM %s.%s mat WHERE mat.%s = src.b"
					 ") "
					 "LIMIT 1",
					 quote_identifier(info->bucket_column),
					 quote_qualified_identifier(
												get_namespace_name(get_rel_namespace(info->source_table_oid)),
												get_rel_name(info->source_table_oid)),
					 quote_identifier(info->bucket_column), lo_str,
					 quote_identifier(info->bucket_column), hi_str,
					 quote_identifier(info->mat_table_schema),
					 quote_identifier(info->mat_table_name),
					 quote_identifier(info->mat_bucket_col));

	gap_ret = SPI_execute_with_args(gap_sql.data, 1,
									gap_argtypes, gap_args,
									NULL, true, 1);
	pfree(gap_sql.data);
	pfree(lo_str);
	pfree(hi_str);

	/* No source bucket in the gap is missing from mat -> fully covered. */
	materialized = (gap_ret == SPI_OK_SELECT && SPI_processed == 0);
	return materialized;
}

/*
 * Trim L2 entries after REFRESH.
 *
 * Three cases for each L2 entry vs the refreshed window [start, end):
 *
 *   Case A: Fully inside window -> DELETE
 *           [lowest >= start AND greatest < end]
 *
 *   Case B: Spans left boundary -> UPDATE shrink to [lowest, start)
 *           [lowest < start AND greatest >= start AND greatest < end]
 *
 *   Case C: Spans right boundary -> UPDATE shrink to [end, greatest]
 *           [lowest >= start AND lowest < end AND greatest >= end]
 *
 *   Case D: Fully contains window -> split into two:
 *           [lowest, start) and [end, greatest]
 *           Implemented as UPDATE to [lowest, start) + INSERT [end, greatest]
 *
 *   Case E: Fully outside window -> no action
 */

static void
cagg_trim_l2(CaggRefreshInfo * info, TimestampTz start, TimestampTz end)
{
	Oid			argtypes[3] = {INT4OID, TIMESTAMPTZOID, TIMESTAMPTZOID};
	Datum		args[3];

	args[0] = Int32GetDatum(info->cagg_id);
	args[1] = TimestampTzGetDatum(start);
	args[2] = TimestampTzGetDatum(end);

	/* Case A: fully inside -> DELETE */
	SPI_execute_with_args(
						  "DELETE FROM time_series.cagg_materialization_log "
						  "WHERE cagg_id = $1 "
						  "AND lowest_modified >= $2 AND greatest_modified < $3",
						  3, argtypes, args, NULL, false, 0);

	/*
	 * Case B: spans left boundary -> shrink to [lowest, start - 1us].
	 *
	 * Mirrors upstream invalidation.c:375 which cuts to
	 * `refresh_window->start - 1` so the remainder is strictly disjoint from
	 * the refresh window.  Using exactly `$2` (= window_start) instead would
	 * leave greatest = start, and the bucket containing `start` is INSIDE the
	 * refresh window we just materialized; the entry would re-match Case B on
	 * every subsequent refresh and trigger a no-op DELETE+INSERT against the
	 * mat table. Subtracting 1us makes the entry covered region terminate
	 * strictly before any timestamp that could land in the refresh window.
	 */
	SPI_execute_with_args(
						  "UPDATE time_series.cagg_materialization_log "
						  "SET greatest_modified = $2 - interval '1 microsecond' "
						  "WHERE cagg_id = $1 "
						  "AND lowest_modified < $2 "
						  "AND greatest_modified >= $2 AND greatest_modified < $3",
						  3, argtypes, args, NULL, false, 0);

	/* Case C: spans right boundary -> shrink to [end, greatest] */
	SPI_execute_with_args(
						  "UPDATE time_series.cagg_materialization_log "
						  "SET lowest_modified = $3 "
						  "WHERE cagg_id = $1 "
						  "AND lowest_modified >= $2 AND lowest_modified < $3 "
						  "AND greatest_modified >= $3",
						  3, argtypes, args, NULL, false, 0);

	/*
	 * Case D: fully contains window -> split. First INSERT the right
	 * remainder [end, greatest], then UPDATE the original to [lowest, start -
	 * 1us].  Order matters: INSERT first so the UPDATE WHERE clause still
	 * matches the original row. Same `$2 - 1us` reasoning as Case B above.
	 */
	SPI_execute_with_args(
						  "INSERT INTO time_series.cagg_materialization_log "
						  "(cagg_id, lowest_modified, greatest_modified) "
						  "SELECT $1, $3, greatest_modified "
						  "FROM time_series.cagg_materialization_log "
						  "WHERE cagg_id = $1 "
						  "AND lowest_modified < $2 AND greatest_modified >= $3",
						  3, argtypes, args, NULL, false, 0);

	SPI_execute_with_args(
						  "UPDATE time_series.cagg_materialization_log "
						  "SET greatest_modified = $2 - interval '1 microsecond' "
						  "WHERE cagg_id = $1 "
						  "AND lowest_modified < $2 AND greatest_modified >= $3",
						  3, argtypes, args, NULL, false, 0);
}

/*
 * Get MIN(watermark) across all segments for this CAGG.
 * This is the most conservative materialization boundary -- data
 * above this hasn't been materialized on at least one segment.
 */

static TimestampTz
cagg_get_min_watermark(CaggRefreshInfo * info)
{
	Oid			argtypes[1] = {INT4OID};
	Datum		args[1];
	int			ret;
	bool		isnull;

	args[0] = Int32GetDatum(info->cagg_id);
	ret = SPI_execute_with_args(
								"SELECT MIN(watermark) FROM time_series.cagg_watermark "
								"WHERE cagg_id = $1",
								1, argtypes, args, NULL, true, 1);

	if (ret == SPI_OK_SELECT && SPI_processed > 0)
	{
		TimestampTz wm = DatumGetTimestampTz(
											 SPI_getbinval(SPI_tuptable->vals[0],
														   SPI_tuptable->tupdesc, 1, &isnull));

		if (!isnull)
			return wm;
	}

	/* No watermark rows found -- catalog is corrupted */
	ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("watermark not found for continuous aggregate %d",
					info->cagg_id),
			 errhint("The watermark catalog may be corrupted. "
					 "Re-create the watermark rows using "
					 "_cagg_init_segment_watermark().")));
	return DT_NOBEGIN;			/* unreachable, keeps compiler happy */
}

/*
 * Main REFRESH procedure entry point.
 *
 * CALL time_series.refresh_continuous_aggregate(
 *     cagg_name text,
 *     window_start timestamptz DEFAULT NULL,
 *     window_end   timestamptz DEFAULT NULL
 * )
 */

PG_FUNCTION_INFO_V1(cagg_refresh);

Datum
cagg_refresh(PG_FUNCTION_ARGS)
{
	char		cagg_name_buf[2 * NAMEDATALEN];
	char		cagg_disp_buf[NAMEDATALEN];
	TimestampTz window_start = DT_NOBEGIN;
	TimestampTz window_end = DT_NOEND;
	bool		force = false;
	CaggRefreshInfo info;
	DirtyInterval *intervals = NULL;
	int			n_intervals;
	TimestampTz max_end = DT_NOBEGIN;
	TimestampTz current_watermark;
	TimestampTz actual_boundary;
	int			i;

	/*
	 * Extract arguments.
	 *
	 * arg0 is the continuous aggregate as a regclass (matching upstream's
	 * refresh_continuous_aggregate signature).  Resolve the OID to a
	 * "schema.name" string that the rest of this function (and
	 * cagg_lookup_metadata) consumes; the buffer is sized for two NAMEDATALEN
	 * identifiers plus the dot.  Copying to a stack buffer lets the name
	 * survive SPI_commit_and_chain, which may reset per-transaction memory
	 * contexts in CBDB.
	 */
	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("continuous aggregate cannot be NULL")));

	{
		Oid			cagg_relid = PG_GETARG_OID(0);
		char	   *nspname = get_namespace_name(get_rel_namespace(cagg_relid));
		char	   *relname = get_rel_name(cagg_relid);

		if (nspname == NULL || relname == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_TABLE),
					 errmsg("continuous aggregate (OID %u) does not exist",
							cagg_relid)));

		snprintf(cagg_name_buf, sizeof(cagg_name_buf), "%s.%s",
				 nspname, relname);

		/*
		 * Bare relname for user-facing NOTICEs (matches upstream, which
		 * prints get_rel_name(cagg->relid), and keeps message output stable
		 * regardless of how the caller qualified the name).
		 */
		strlcpy(cagg_disp_buf, relname, NAMEDATALEN);
	}

	if (!PG_ARGISNULL(1))
		window_start = PG_GETARG_TIMESTAMPTZ(1);
	if (!PG_ARGISNULL(2))
		window_end = PG_GETARG_TIMESTAMPTZ(2);

	/*
	 * Optional 4th arg: force.  When true, treat the entire refresh window as
	 * one big invalidation regardless of L2 contents.  Used to recover from
	 * data drift (e.g. trigger missed an INSERT, or mat table got out of sync
	 * via direct manipulation).  Mirrors upstream
	 * continuous_aggs/refresh.c:648.
	 */
	if (PG_NARGS() >= 4 && !PG_ARGISNULL(3))
		force = PG_GETARG_BOOL(3);

	/* Validate window */
	if (!TIMESTAMP_IS_NOBEGIN(window_start) &&
		!TIMESTAMP_IS_NOEND(window_end) &&
		window_start >= window_end)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("refresh window start must be before end")));

	/*
	 * Ownership check: only the CAGG owner (or a superuser, which
	 * pg_class_ownercheck handles internally) may invoke
	 * refresh_continuous_aggregate.  Without this, any role with EXECUTE on
	 * the procedure (PUBLIC by default for CREATE PROCEDURE) could refresh
	 * another tenant's CAGG, advance its watermark, and consume scheduler
	 * resources.
	 *
	 * Mirrors upstream continuous_aggs/refresh.c which gates refresh on the
	 * user view's owner.  upstream on PG16+ uses object_ownercheck; on PG14
	 * (our base) the equivalent is pg_class_ownercheck.
	 *
	 * The check is by name lookup (not via the metadata SPI) so the "does not
	 * exist" path is reached *before* any state read; users who lack
	 * visibility get the same NOTICE as if the cagg was absent.
	 */
	{
		char		schema_buf[NAMEDATALEN];
		char		name_buf[NAMEDATALEN];
		Oid			ns_oid;
		Oid			view_oid;

		cagg_parse_qualified_name(cagg_name_buf, schema_buf, name_buf);
		ns_oid = get_namespace_oid(schema_buf, true);
		view_oid = OidIsValid(ns_oid)
			? get_relname_relid(name_buf, ns_oid)
			: InvalidOid;

		if (OidIsValid(view_oid) &&
			!pg_class_ownercheck(view_oid, GetUserId()))
			aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_MATVIEW,
						   cagg_name_buf);
	}

	/* Must run on coordinator */
	if (Gp_role == GP_ROLE_EXECUTE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("refresh_continuous_aggregate must be called on coordinator")));

	memset(&info, 0, sizeof(info));

	/*
	 * Check that we are in a non-atomic context.  SPI_commit_and_chain()
	 * requires the ability to commit and restart a transaction, which is only
	 * possible when called from a top-level CALL or a DO block.
	 *
	 * Calling from a FUNCTION, TRIGGER, or EXCEPTION block would crash
	 * (SIGSEGV) because those contexts run in atomic/subtransaction mode.
	 * Detect this early and raise a friendly error instead.
	 */
	{
		CallContext *callcontext = (CallContext *) fcinfo->context;

		if (callcontext == NULL ||
			!IsA(callcontext, CallContext) ||
			callcontext->atomic)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("refresh_continuous_aggregate cannot run in an atomic context"),
					 errhint("Call it from a top-level CALL statement or a DO block, "
							 "not from a function, trigger, or exception handler.")));
	}


	/*
	 * Use SPI_OPT_NONATOMIC so we can do SPI_commit_and_chain() to split the
	 * work into two transactions.
	 */
	SPI_connect_ext(SPI_OPT_NONATOMIC);


	/*
	 * In the normal psql path, ExecuteCallStmt's caller (PortalRunUtility)
	 * has an ActiveSnapshot tied to the user's Portal, so SPI's downstream
	 * SQL execution (pquery.c:EnsurePortalSnapshotExists) finds it and
	 * proceeds.
	 *
	 * In the BGW path, however, there is no user Portal and no
	 * ActiveSnapshot: job.c starts a transaction and calls
	 * SPI_execute_extended() directly. Without an active snapshot, our
	 * SPI_execute_with_args() calls below would fail with "cannot execute SQL
	 * without an outer snapshot or portal" (pquery.c:2075).
	 *
	 * Push a transaction snapshot only if one isn't already active.  We also
	 * Pop+Push around SPI_commit_and_chain() below, since that path tears
	 * down the active snapshot stack as part of commit.
	 */

	/*
	 * All callers (psql CALL, run_job, BGW worker) now arrive here with a
	 * valid ActivePortal -- the BGW worker creates a transient Portal before
	 * invoking us, mirroring upstream bgw_policy/job.c pattern.  This means
	 * snapshot management is delegated to the Portal infrastructure:
	 * PortalRun for psql, our manual setup + EnsurePortalSnapshotExists for
	 * BGW.  No path-specific snapshot stack management is needed below.
	 */


	/*
	 * Disable ORCA for the duration of this refresh.
	 *
	 * cagg_refresh issues many UPDATE statements against DISTRIBUTED
	 * REPLICATED catalog tables (cagg_invalidation_threshold etc.). ORCA
	 * emits "Operator Update on replicated tables not supported" and falls
	 * back to the Postgres planner, but that fallback path SIGSEGVs in BGW
	 * workers -- see signal-11 crash logs and the project convention in
	 * CLAUDE.md ("Always SET optimizer = off for CBDB").
	 *
	 * Forcing the Postgres planner avoids the entire ORCA-fallback path.
	 *
	 * Go through set_config_option() with GUC_ACTION_SET -- a plain
	 * top-level SET, not SET LOCAL / GUC_ACTION_SAVE -- rather than
	 * assigning the extern global directly.  This matters across the
	 * SPI_commit_and_chain() calls below: SET LOCAL and a nested/temp
	 * GUC_ACTION_SAVE assignment are both unwound by AtEOXact_GUC() at
	 * the very next transaction end, which is exactly why an earlier
	 * SET LOCAL via SPI attempt showed no effect under BGW NONATOMIC
	 * SPI (the next SPI_execute_with_args still triggered ORCA NOTICE
	 * and the same crash) -- a plain SET's effect is session-scoped and
	 * is NOT torn down at COMMIT.  A raw assignment to the extern global
	 * also survives the commit boundary, but leaves the GUC subsystem's
	 * own bookkeeping (source, stack) out of sync with the variable, so
	 * anything that consults the GUC record rather than the bare global
	 * (e.g. SHOW optimizer, a later SET issued in the same session) can
	 * disagree with reality; going through the real API keeps both in
	 * sync at essentially the same cost as the direct assignment.
	 *
	 * Save/restore via PG_TRY/PG_FINALLY: cagg_refresh is reachable from
	 * three call paths -- BGW worker (process exits, no leak), CALL run_job
	 * (stays in user session), direct CALL refresh_continuous_aggregate
	 * (stays in user session).  Without restore, the latter two would
	 * silently flip the caller's optimizer GUC to off for the rest of the
	 * session -- a hard-to-diagnose performance regression for any user still
	 * relying on ORCA.  PG_FINALLY ensures restore even on ERROR.
	 */
	{
		extern bool optimizer;
		bool		saved_optimizer = optimizer;

		set_config_option("optimizer", "off",
						  PGC_USERSET, PGC_S_SESSION,
						  GUC_ACTION_SET, true, 0, false);
		PG_TRY();
		{

			/* ---- Transaction 1: L1 -> L2 migration ---- */

			cagg_lookup_metadata(cagg_name_buf, &info);


			cagg_acquire_lock(info.cagg_id);


			/*
			 * Acquire source-level advisory lock to serialize L1->L2
			 * migration across all CAGGs on the same source table.
			 */
			{
				Oid			lock_argtypes[1] = {INT8OID};
				Datum		lock_args[1];

				lock_args[0] = Int64GetDatum(-(int64) info.source_table_oid);
				SPI_execute_with_args(
									  "SELECT pg_advisory_xact_lock($1)",
									  1, lock_argtypes, lock_args, NULL, true, 0);
			}

			/*
			 * Match upstream continuous_aggs/refresh.c:759 -- just the CAGG
			 * name, no window timestamps.  Window detail is logged at DEBUG1
			 * below (multiple sites with start/end values), so the NOTICE
			 * here stays deterministic for regression tests where window =
			 * now() - offset embeds a real-time timestamp.
			 */
			ereport(NOTICE,
					(errmsg("refreshing continuous aggregate \"%s\"",
							cagg_disp_buf)));

			cagg_migrate_l1_to_l2(&info);
			elog(DEBUG1, "cagg \"%s\": L1->L2 migration complete (TX1)", cagg_name_buf);

			/*
			 * Acquire a real relation-level lock on
			 * cagg_invalidation_threshold to serialize against concurrent DDL
			 * or out-of-band UPDATEs to the threshold row (e.g. an admin
			 * manually rewriting state).
			 *
			 * Mirrors upstream invalidation_threshold.c -- they take
			 * ShareUpdateExclusiveLock on the relation and RowExclusiveLock
			 * implicitly during the UPDATE.  Without this, advisory locks
			 * alone do not block PG-level DDL or non-cooperating DML.
			 *
			 * IMPORTANT lock-order note: this MUST be taken AFTER
			 * cagg_migrate_l1_to_l2() above, not before.  Migration
			 * dispatches _cagg_move_l1_to_l2() to every segment, where it
			 * holds a RowExclusiveLock on cagg_materialization_log (L2).
			 * TX2's materialize/consume path acquires the same L2 table
			 * (escalated to ExclusiveLock by CBDB for the DISTRIBUTED
			 * RANDOMLY catalog) and THEN updates cagg_invalidation_threshold.
			 * If TX1 held the threshold lock first and then went to L2 during
			 * migration, the two phases would acquire {L2, threshold} in
			 * opposite orders -- a classic ABBA deadlock.  Because both
			 * catalogs are DISTRIBUTED RANDOMLY the wait edges span
			 * coordinator + segment locks, so the cycle is cross-node;
			 * PostgreSQL's per-node deadlock detector cannot see it and
			 * gp_enable_global_deadlock_detector is off by default, which
			 * means the refresh workers hang forever instead of one being
			 * aborted.  Taking L2 (via migration) THEN threshold here matches
			 * TX2's order and breaks the cycle, without serializing
			 * concurrent refreshes of different CAGGs on the same source.
			 */
			{
				Oid			ns_oid = ht_get_namespace_oid_cached();
				Oid			th_oid = OidIsValid(ns_oid)
				? get_relname_relid("cagg_invalidation_threshold", ns_oid)
				: InvalidOid;

				if (OidIsValid(th_oid))
					LockRelationOid(th_oid, ShareUpdateExclusiveLock);
			}


			/*
			 * Advance invalidation threshold early (before commit) so that
			 * concurrent triggers see the new threshold as soon as TX1
			 * commits. This matches upstream behavior of setting threshold in
			 * TX1.
			 *
			 * Estimated threshold = LEAST(window_end, max-data bucket start).
			 * The data-boundary computation always runs (both finite and NULL
			 * window_end); a finite window_end only CAPS the estimate.  Two
			 * reasons for the cap:
			 *
			 * 1. The threshold is monotonic (both this GREATEST and the TX2
			 * reconcile only move it forward).  Without the cap, a refresh
			 * called with a far-future window_end (e.g. '2030-01-01') would
			 * pin the threshold there permanently, forcing every subsequent
			 * insert below it to write L1 forever.
			 *
			 * 2. Capping at the max-data bucket START (the hot bucket
			 * boundary) keeps hot-bucket appends on the no-L1 fast path even
			 * during a refresh.  The old `est = window_end` form let a BGW
			 * refresh (window_end ~ now) transiently raise the threshold
			 * above the hot bucket, making every concurrent append write a
			 * pointless L1 entry until the TX2 reconcile.
			 *
			 * Correctness: TX2 clamps its materialization end and watermark
			 * to the threshold committed here (see the actual_boundary clamp
			 * in TX2), so a lower estimate can only defer materialization to
			 * the next refresh -- never lose coverage.
			 */
			{
				TimestampTz est_threshold = DT_NOBEGIN;

				{
					/*
					 * Estimate threshold from the source table's max(time).
					 * The naive form is `SELECT time_bucket($1, max(time))
					 * FROM source`.  On a PG heap table with a btree(time)
					 * index the planner would emit "Index Only Scan Backward
					 * + Limit 1" and return in microseconds.  Our source is a
					 * time_series table (Custom Scan ChunkScan), which has no
					 * btree on the time column and no max-pushdown -- the
					 * naive form degrades to a full ChunkScan across every
					 * chunk (181K rows / ~350 ms on a 500K-row test set).
					 *
					 * Workaround: we already know which chunk holds the
					 * newest rows -- the one with the maximum chunk_number in
					 * ts_chunk catalog.  Scope the max() scan to just that
					 * chunk by adding `WHERE time >= <chunk_start>` as a
					 * literal Const (not a SPI parameter -- see below).
					 *
					 * Two-step SPI is required because ts_extract_time_bounds
					 * in ts_scan.c only recognises `Var op Const` patterns: a
					 * Param or SubLink there leaves the planner unable to
					 * compute [min_chunk, max_chunk] and the scan degrades
					 * back to full-table.  So step 1 fetches the chunk start
					 * via one SPI, step 2 embeds it as a string literal in
					 * the SQL text of the next SPI.  Embedding is safe here
					 * because the value comes from our own catalog (no user-
					 * supplied input on this path) and timestamptz_out is a
					 * deterministic PG built-in.
					 *
					 * Measured on a 504K-row / 6580-chunk test set: full-scan
					 * max(time):  ~590 ms (181K rows scanned) scoped
					 * max(time):       ~9 ms (1 chunk scanned)
					 *
					 * Use bucket START (not end) to exclude the "hot bucket"
					 * -- the last bucket still receiving data.  This ensures
					 * the hot bucket stays in the live branch of the UNION
					 * ALL view, preventing stale partial aggregates.
					 *
					 * NOTE: This catalog-based scoping is a stop-gap for the
					 * lack of btree(time) on time_series tables.  Once intra-
					 * chunk indexes are supported, revert this block to the
					 * plain `SELECT max(time) FROM source` form and let PG's
					 * native "Index Only Scan Backward + Limit 1" fire.
					 */
					Oid			chunk_argtypes[1] = {OIDOID};
					Datum		chunk_args[1];
					int			chunk_ret;
					bool		latest_isnull = true;
					TimestampTz latest_chunk_start = DT_NOBEGIN;
					Oid			max_argtypes[1] = {INTERVALOID};
					Datum		max_args[1];
					int			max_ret;
					bool		max_isnull;
					StringInfoData max_sql;

					/* Step 1: find latest chunk's range_start via catalog. */
					chunk_args[0] = ObjectIdGetDatum(info.source_table_oid);
					chunk_ret = SPI_execute_with_args(
													  "SELECT range_start FROM time_series.ts_chunk "
													  " WHERE table_oid = $1 "
													  " ORDER BY chunk_number DESC LIMIT 1",
													  1, chunk_argtypes, chunk_args, NULL, true, 1);

					if (chunk_ret == SPI_OK_SELECT && SPI_processed > 0)
					{
						Datum		d = SPI_getbinval(SPI_tuptable->vals[0],
													  SPI_tuptable->tupdesc, 1,
													  &latest_isnull);

						if (!latest_isnull)
							latest_chunk_start = DatumGetTimestampTz(d);
					}

					/*
					 * Step 2: max(time_bucket(width, time)) on source. When
					 * latest_chunk_start is known, embed it as a literal
					 * WHERE bound so the ChunkScan pruner can skip earlier
					 * chunks.  Empty-table case (no chunks) falls back to the
					 * unbounded scan, which is cheap because the table has no
					 * rows anyway.
					 */
					max_args[0] = IntervalPGetDatum(info.bucket_width);

					initStringInfo(&max_sql);

					/*
					 * Outer ::timestamptz cast is the type-safe glue.  When
					 * the source bucket column is DATE or TIMESTAMP,
					 * time_bucket returns that same type; DatumGetTimestampTz
					 * on a DATE (int32) misinterprets the bytes as
					 * TimestampTz (int64) and yields a garbage value near
					 * year 2000 - which on the caller side corrupts the
					 * threshold/watermark clamp. Casting in SQL lets PG do
					 * the proper conversion (DATE 00:00 -> midnight
					 * TIMESTAMPTZ in current TZ).
					 */
					if (latest_isnull)
					{
						appendStringInfo(&max_sql,
										 "SELECT time_series.time_bucket($1, max(%s))"
										 "  ::timestamptz "
										 "FROM %s",
										 quote_identifier(info.bucket_column),
										 quote_qualified_identifier(
																	get_namespace_name(
																					   get_rel_namespace(info.source_table_oid)),
																	get_rel_name(info.source_table_oid)));
					}
					else
					{
						char	   *start_str = DatumGetCString(
																DirectFunctionCall1(timestamptz_out,
																					TimestampTzGetDatum(latest_chunk_start)));

						appendStringInfo(&max_sql,
										 "SELECT time_series.time_bucket($1, max(%s))"
										 "  ::timestamptz "
										 "FROM %s "
										 " WHERE %s >= '%s'::timestamptz",
										 quote_identifier(info.bucket_column),
										 quote_qualified_identifier(
																	get_namespace_name(
																					   get_rel_namespace(info.source_table_oid)),
																	get_rel_name(info.source_table_oid)),
										 quote_identifier(info.bucket_column),
										 start_str);

						pfree(start_str);
					}

					max_ret = SPI_execute_with_args(max_sql.data,
													1, max_argtypes, max_args, NULL, true, 1);

					if (max_ret == SPI_OK_SELECT && SPI_processed > 0)
					{
						est_threshold = DatumGetTimestampTz(
															SPI_getbinval(SPI_tuptable->vals[0],
																		  SPI_tuptable->tupdesc, 1,
																		  &max_isnull));
						if (max_isnull)
							est_threshold = DT_NOBEGIN;
					}

					pfree(max_sql.data);
				}

				/*
				 * Finite refresh window: cap the estimate at window_end.  (A
				 * window_end at or above the data boundary leaves the
				 * boundary estimate in place -- see the block comment above.)
				 */
				if (!TIMESTAMP_IS_NOEND(window_end) &&
					!TIMESTAMP_IS_NOBEGIN(est_threshold) &&
					window_end < est_threshold)
					est_threshold = window_end;

				if (!TIMESTAMP_IS_NOBEGIN(est_threshold))
				{
					Oid			th_argtypes[2] = {TIMESTAMPTZOID, OIDOID};
					Datum		th_args[2];

					th_args[0] = TimestampTzGetDatum(est_threshold);
					th_args[1] = ObjectIdGetDatum(info.source_table_oid);

					SPI_execute_with_args(
										  "UPDATE time_series.cagg_invalidation_threshold "
										  "SET threshold = GREATEST(threshold, $1) "
										  "WHERE source_table_oid = $2",
										  2, th_argtypes, th_args, NULL, false, 0);
				}
			}

			SIMPLE_FAULT_INJECTOR("cagg_refresh_before_commit_and_chain");


			/*
			 * Hand off to TX2 via SPI_commit_and_chain.  With a valid
			 * ActivePortal, ForgetPortalSnapshots / push-new-snapshot are
			 * orchestrated by Portal + SPI machinery; we don't manipulate the
			 * snapshot stack manually here.
			 */
			SPI_commit_and_chain();

			SIMPLE_FAULT_INJECTOR("cagg_refresh_after_commit_and_chain");

			/* ---- Transaction 2: Materialize dirty intervals ---- */


			cagg_acquire_lock(info.cagg_id);


			/*
			 * Re-lookup metadata in the new transaction.  The previous
			 * transaction's data is no longer accessible.
			 */
			memset(&info, 0, sizeof(info));
			cagg_lookup_metadata(cagg_name_buf, &info);


			/*
			 * LOCK-ORDER NOTE (2026-07-24, cross-CAGG ABBA post-mortem):
			 * TX2 deliberately does NOT take the source-level advisory
			 * lock.  Its lock order is L2 -> threshold, while TX1's is
			 * advisory -> L2 -> threshold; the pairs shared between any
			 * two refresh transactions are therefore acquired in one
			 * global order and no ABBA is possible -- PROVIDED TX2's
			 * locks never survive into the next TX1, which the
			 * chain-commit at the end of this function now guarantees.
			 * (An earlier fix draft had TX2 re-acquire the advisory for
			 * belt-and-suspenders lock ordering; it was reverted because
			 * holding the advisory across TX2's mat writes serializes
			 * same-source refreshes of DIFFERENT caggs, breaking the
			 * concurrency contract pinned by iso2 cagg_concurrent_refresh
			 * -- a refresh stuck on one cagg's mat lock would stall every
			 * sibling cagg on the source.)  See
			 * doc/bugs/cagg-batched-refresh-cross-cagg-abba-deadlock.md.
			 */

			/*
			 * Unified refresh path (aligned with upstream behavior):
			 *
			 * 1. Gather dirty intervals from L2 (backfill/update/delete) 2.
			 * Add the "unmaterialized range" [MIN(watermark), window_end) as
			 * an additional dirty interval -- this covers new data that
			 * hasn't been materialized yet 3. Merge all intervals 4. Refresh
			 * only the merged dirty ranges
			 *
			 * This means NULL, NULL doesn't do a brute-force full rebuild;
			 * instead it processes only what's actually dirty or new. First
			 * refresh (watermark=-infinity) naturally becomes a full
			 * materialization because the unmaterialized range = everything.
			 */

			/*
			 * Inscribed bucket alignment (matches upstream behavior): Only
			 * refresh COMPLETE buckets that fall entirely within the user's
			 * window.  Partial buckets at the edges are excluded.
			 *
			 * start -> align UP to next bucket boundary (ceiling) end   ->
			 * align DOWN to previous bucket boundary (floor)
			 *
			 * Example: window [00:30, 02:30), 1-hour buckets start: 00:30 ->
			 * ceil to 01:00 end:   02:30 -> floor to 02:00 refreshes: [01:00,
			 * 02:00) -- only bucket 01:00
			 */
			if (!TIMESTAMP_IS_NOBEGIN(window_start))
			{
				TimestampTz aligned = cagg_align_to_bucket_start(&info, window_start);

				/* If start is not on a boundary, round UP to next boundary */
				if (aligned < window_start)
					window_start = cagg_align_to_bucket_end(&info, window_start);
				else
					window_start = aligned; /* already on boundary */
			}

			if (!TIMESTAMP_IS_NOEND(window_end))
			{
				/* Align end DOWN to bucket boundary (floor) */
				window_end = cagg_align_to_bucket_start(&info, window_end);
			}

			/*
			 * After alignment, start >= end means no complete buckets in
			 * window
			 */
			if (!TIMESTAMP_IS_NOBEGIN(window_start) &&
				!TIMESTAMP_IS_NOEND(window_end) &&
				window_start >= window_end)
			{
				SPI_finish();
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("refresh window too small"),
						 errdetail("The refresh window must cover at least one "
								   "bucket of data."),
						 errhint("Align the refresh window with the bucket "
								 "boundaries or use at least two buckets.")));
			}

			/* Get the current lowest watermark across all segments */
			current_watermark = cagg_get_min_watermark(&info);

			/* Gather dirty intervals from L2 */
			n_intervals = cagg_gather_dirty_intervals(&info,
													  window_start, window_end,
													  &intervals);

			/*
			 * Compute actual data boundary (bucket START of source max(time))
			 * up-front, before deciding what to materialize.  We need this in
			 * two places:
			 *
			 * 1. To clamp the unmaterialized-range append below.  Without
			 * this clamp, every refresh would re-materialize the "hot bucket"
			 * -- the bucket still receiving new data -- into the mat table,
			 * where the cagg_union_view filter (bucket < watermark)
			 * immediately discards it.  Pure waste: ~800 ms per refresh on a
			 * 12K-chunk source for zero observable effect.  Worse, watermark
			 * stays pinned at the hot bucket start (we clamp max_end to the
			 * same actual_boundary), so the wasted INSERT recurs every tick
			 * of the bgw policy.
			 *
			 * 2. To clamp max_end after materialization (the original
			 * post-loop clamp at the end of TX2).
			 *
			 * Computing once here lets us reuse the value and removes a
			 * duplicate two-step SPI later in this function.
			 *
			 * actual_boundary == DT_NOBEGIN means the source is empty (no
			 * chunks).  In that case unmat-range below skips and the post-
			 * loop clamp leaves max_end at DT_NOBEGIN, so watermark does not
			 * advance.
			 */
			actual_boundary = DT_NOBEGIN;
			{
				Oid			chunk_argtypes[1] = {OIDOID};
				Datum		chunk_args[1];
				int			chunk_ret;
				bool		latest_isnull = true;
				TimestampTz latest_chunk_start = DT_NOBEGIN;
				Oid			max_argtypes[1] = {INTERVALOID};
				Datum		max_args[1];
				int			max_ret;
				bool		max_isnull;
				StringInfoData max_sql;

				/*
				 * Two-step chunk-aware pattern: ts_extract_time_bounds in
				 * ts_scan.c only recognises `Var op Const`, so a Param or
				 * SubLink there defeats chunk pruning.  Step 1 pulls the
				 * latest chunk's range_start from our catalog; Step 2 embeds
				 * it as a literal in the max(time) SQL.  Embedding is safe --
				 * the value comes from our own catalog, never user input.
				 */
				chunk_args[0] = ObjectIdGetDatum(info.source_table_oid);
				chunk_ret = SPI_execute_with_args(
												  "SELECT range_start FROM time_series.ts_chunk "
												  " WHERE table_oid = $1 "
												  " ORDER BY chunk_number DESC LIMIT 1",
												  1, chunk_argtypes, chunk_args, NULL, true, 1);

				if (chunk_ret == SPI_OK_SELECT && SPI_processed > 0)
				{
					Datum		d = SPI_getbinval(SPI_tuptable->vals[0],
												  SPI_tuptable->tupdesc, 1,
												  &latest_isnull);

					if (!latest_isnull)
						latest_chunk_start = DatumGetTimestampTz(d);
				}

				max_args[0] = IntervalPGetDatum(info.bucket_width);

				initStringInfo(&max_sql);

				/*
				 * Outer ::timestamptz cast: see matching block in TX1 above.
				 * Without it, a DATE/TIMESTAMP source column causes the SPI
				 * result (which time_bucket returns in the source's own type)
				 * to be misinterpreted by DatumGetTimestampTz, producing a
				 * garbage TimestampTz that would corrupt the unmat-range
				 * clamp and silently skip materialization.
				 */
				if (latest_isnull)
				{
					appendStringInfo(&max_sql,
									 "SELECT time_series.time_bucket($1, max(%s))"
									 "  ::timestamptz "
									 "FROM %s",
									 quote_identifier(info.bucket_column),
									 quote_qualified_identifier(
																get_namespace_name(
																				   get_rel_namespace(info.source_table_oid)),
																get_rel_name(info.source_table_oid)));
				}
				else
				{
					char	   *start_str = DatumGetCString(
															DirectFunctionCall1(timestamptz_out,
																				TimestampTzGetDatum(latest_chunk_start)));

					appendStringInfo(&max_sql,
									 "SELECT time_series.time_bucket($1, max(%s))"
									 "  ::timestamptz "
									 "FROM %s "
									 " WHERE %s >= '%s'::timestamptz",
									 quote_identifier(info.bucket_column),
									 quote_qualified_identifier(
																get_namespace_name(
																				   get_rel_namespace(info.source_table_oid)),
																get_rel_name(info.source_table_oid)),
									 quote_identifier(info.bucket_column),
									 start_str);

					pfree(start_str);
				}

				max_ret = SPI_execute_with_args(max_sql.data,
												1, max_argtypes, max_args, NULL, true, 1);

				if (max_ret == SPI_OK_SELECT && SPI_processed > 0)
				{
					TimestampTz max_data = DatumGetTimestampTz(
															   SPI_getbinval(SPI_tuptable->vals[0],
																			 SPI_tuptable->tupdesc, 1, &max_isnull));

					if (!max_isnull)
						actual_boundary = max_data;
				}

				pfree(max_sql.data);
			}

			/*
			 * Clamp actual_boundary to the invalidation threshold committed
			 * by TX1 (or raised further by a concurrent refresh's TX1 -- the
			 * threshold is monotonic, so it cannot be below our TX1 value).
			 *
			 * Invariant: never materialize-and-watermark past the committed
			 * threshold.  Concurrent INSERTs with ts >= threshold skip L1 on
			 * the assumption that they lie in the not-yet-materialized
			 * region. actual_boundary is recomputed HERE in TX2, so data
			 * arriving between TX1 (which estimated and committed the
			 * threshold) and this point can push the boundary past the
			 * threshold; if we then materialized up to the new boundary, a
			 * row committing after our materialization snapshot -- but below
			 * the advanced watermark -- would have neither an L1 entry nor
			 * mat coverage: silently lost. Clamping defers those buckets to
			 * the next refresh (whose own TX1 will re-announce a higher
			 * threshold first).
			 *
			 * MIN(threshold) across segments: the rows are per-segment
			 * (DISTRIBUTED RANDOMLY) and normally identical; if they ever
			 * diverged, the lowest value is the weakest filter any segment's
			 * trigger could have applied, so it is the only safe bound.
			 */
			if (!TIMESTAMP_IS_NOBEGIN(actual_boundary))
			{
				Oid			th_argtypes[1] = {OIDOID};
				Datum		th_args[1];
				int			th_ret;

				th_args[0] = ObjectIdGetDatum(info.source_table_oid);
				th_ret = SPI_execute_with_args(
											   "SELECT MIN(threshold) "
											   "FROM time_series.cagg_invalidation_threshold "
											   "WHERE source_table_oid = $1",
											   1, th_argtypes, th_args, NULL, true, 1);

				if (th_ret == SPI_OK_SELECT && SPI_processed > 0)
				{
					bool		th_isnull;
					TimestampTz committed_th = DatumGetTimestampTz(
																   SPI_getbinval(SPI_tuptable->vals[0],
																				 SPI_tuptable->tupdesc, 1, &th_isnull));

					if (!th_isnull && committed_th < actual_boundary)
						actual_boundary = committed_th;
				}
			}

			/*
			 * force=TRUE: treat the entire refresh window as one big dirty
			 * interval, regardless of what L2 contains.  Mirrors upstream
			 * invalidation.c:896-908 which pre-seeds an "always-merged"
			 * invalidation entry covering the whole window before merging
			 * with the regular L2 entries.
			 *
			 * The unmat-range append below will merge this entry with any
			 * adjacent L2 entries; downstream cagg_refresh_one_interval
			 * unconditionally DELETEs+INSERTs the buckets, which is the
			 * recovery escape hatch for data drift after the trigger missed
			 * an INSERT or the mat table was manipulated directly.
			 */
			if (force)
			{
				TimestampTz forced_start =
				TIMESTAMP_IS_NOBEGIN(window_start) ? current_watermark : window_start;
				TimestampTz forced_end = window_end;

				/*
				 * If start is still -infinity, fall back to actual data
				 * boundary later (max_end clamp); for the synthetic interval
				 * here use the window_start we have.
				 */
				if (!TIMESTAMP_IS_NOEND(forced_end) && forced_start < forced_end)
				{
					elog(LOG, "cagg \"%s\": force refresh window [%s, %s)",
						 cagg_name_buf,
						 TIMESTAMP_IS_NOBEGIN(forced_start) ? "-infinity"
						 : DatumGetCString(DirectFunctionCall1(timestamptz_out,
															   TimestampTzGetDatum(forced_start))),
						 DatumGetCString(DirectFunctionCall1(timestamptz_out,
															 TimestampTzGetDatum(forced_end))));

					intervals = (intervals == NULL)
						? palloc(sizeof(DirtyInterval))
						: repalloc(intervals,
								   sizeof(DirtyInterval) * (n_intervals + 1));
					intervals[n_intervals].start = forced_start;
					intervals[n_intervals].end = forced_end;
					n_intervals++;
				}
			}

			/*
			 * Add the unmaterialized range [watermark, MIN(window_end,
			 * actual_boundary)) as an extra dirty interval.  This ensures new
			 * STABLE buckets (those fully behind the hot bucket) beyond the
			 * watermark get materialized even when there are no L2 entries
			 * for them.
			 *
			 * Clamping unmat_end to actual_boundary is the key
			 * correctness/perf fix: actual_boundary = bucket START of source
			 * max(time) = the hot bucket's start.  If we let unmat_end extend
			 * past it (e.g. +infinity for NULL/NULL refresh),
			 * refresh_one_interval would re-aggregate the hot bucket into the
			 * mat table on every refresh.  The cagg_union view's mat-branch
			 * filter (bucket < watermark) immediately discards those rows --
			 * because watermark itself is clamped to actual_boundary for the
			 * same hot-bucket-exclusion reason -- so the INSERT is pure
			 * waste.  Concretely, on a 12K-chunk source warm-refresh burns
			 * ~800 ms per tick for zero observable effect, and the waste
			 * recurs forever because watermark cannot advance past the hot
			 * bucket.
			 *
			 * Special cases: - actual_boundary == DT_NOBEGIN (empty source):
			 * nothing to materialize, skip entirely. - watermark >=
			 * actual_boundary: mat has already covered every stable bucket;
			 * the hot bucket is in live's branch.  Skip. - L2 entries that
			 * include the hot bucket are still processed (the L2 path's
			 * window is bounded by user-supplied window_end, not
			 * actual_boundary).  This is a minor remaining inefficiency but
			 * is bounded by actual data churn, not by refresh frequency, so
			 * unlike the unmat-range case it does not amplify under bgw
			 * periodic refresh.
			 */
			{
				TimestampTz unmat_start = current_watermark;
				TimestampTz unmat_end = window_end;

				/* Clamp unmaterialized range to window */
				if (!TIMESTAMP_IS_NOBEGIN(window_start) && unmat_start < window_start)
					unmat_start = window_start;

				/*
				 * Clamp unmat_end to actual_boundary (hot bucket start).
				 * actual_boundary == DT_NOBEGIN means empty source -> nothing
				 * to materialize via unmat-range.
				 */
				if (TIMESTAMP_IS_NOBEGIN(actual_boundary))
					unmat_end = DT_NOBEGIN; /* force empty range */
				else if (TIMESTAMP_IS_NOEND(unmat_end) || unmat_end > actual_boundary)
					unmat_end = actual_boundary;

				/* Only add if the range is non-empty */
				if (!TIMESTAMP_IS_NOBEGIN(unmat_end) &&
					(TIMESTAMP_IS_NOBEGIN(unmat_start) || unmat_start < unmat_end))
				{
					/* Align to bucket boundaries */
					if (!TIMESTAMP_IS_NOBEGIN(unmat_start))
						unmat_start = cagg_align_to_bucket_start(&info, unmat_start);

					/* Append to intervals array */
					if (intervals == NULL)
						intervals = palloc(sizeof(DirtyInterval));
					else
						intervals = repalloc(intervals,
											 sizeof(DirtyInterval) * (n_intervals + 1));

					intervals[n_intervals].start = unmat_start;
					intervals[n_intervals].end = unmat_end;
					n_intervals++;

					/*
					 * Re-sort and re-merge since we added an interval that
					 * might overlap with existing ones.  Simple approach:
					 * just sort by start and merge in-place.
					 */
					{
						DirtyInterval *merged;
						int			num_merged = 0;

						/*
						 * Sort by start so the single-pass merge below works.
						 * We just appended the unmaterialized-range (and
						 * force) interval to an already-sorted array, so this
						 * is a near-sorted input; qsort handles it in O(n log
						 * n) regardless, and replaces the old O(n^2) bubble
						 * sort which degraded badly when many disjoint
						 * invalidation intervals reached this point.
						 */
						qsort(intervals, n_intervals, sizeof(DirtyInterval),
							  cmp_dirty_interval_start);

						/* Merge */
						merged = palloc(sizeof(DirtyInterval) * n_intervals);
						for (i = 0; i < n_intervals; i++)
						{
							if (num_merged > 0 &&
								intervals[i].start <= merged[num_merged - 1].end)
							{
								if (intervals[i].end > merged[num_merged - 1].end)
									merged[num_merged - 1].end = intervals[i].end;
							}
							else
							{
								merged[num_merged].start = intervals[i].start;
								merged[num_merged].end = intervals[i].end;
								num_merged++;
							}
						}

						pfree(intervals);
						intervals = merged;
						n_intervals = num_merged;
					}

				}
			}

			/*
			 * If the number of disjoint intervals exceeds the GUC limit,
			 * merge them all into one range [min_start, max_end] to avoid
			 * excessive fragmented refreshes.  0 means unlimited.
			 */
			if (n_intervals > 1 &&
				guc_materializations_per_refresh_window > 0 &&
				n_intervals > guc_materializations_per_refresh_window)
			{
				TimestampTz merged_start = intervals[0].start;
				TimestampTz merged_end = intervals[n_intervals - 1].end;

				/*
				 * Find actual max end (intervals are sorted by start, not
				 * end)
				 */
				for (i = 0; i < n_intervals; i++)
				{
					if (intervals[i].end > merged_end)
						merged_end = intervals[i].end;
				}

				intervals[0].start = merged_start;
				intervals[0].end = merged_end;
				n_intervals = 1;
			}

			if (n_intervals == 0)
			{
				ereport(NOTICE,
						(errmsg("continuous aggregate \"%s\" is already up-to-date",
								cagg_disp_buf)));
			}

			if (n_intervals > 0)
			{
				elog(DEBUG1, "cagg \"%s\": refreshing %d dirty interval%s",
					 cagg_name_buf, n_intervals, n_intervals == 1 ? "" : "s");

				for (i = 0; i < n_intervals; i++)
				{
					elog(DEBUG1, "cagg \"%s\": interval %d/%d [%s, %s)",
						 cagg_name_buf, i + 1, n_intervals,
						 TIMESTAMP_IS_NOBEGIN(intervals[i].start) ? "-infinity"
						 : DatumGetCString(DirectFunctionCall1(timestamptz_out,
															   TimestampTzGetDatum(intervals[i].start))),
						 TIMESTAMP_IS_NOEND(intervals[i].end) ? "+infinity"
						 : DatumGetCString(DirectFunctionCall1(timestamptz_out,
															   TimestampTzGetDatum(intervals[i].end))));

					cagg_refresh_one_interval(&info,
											  intervals[i].start,
											  intervals[i].end);

					if (intervals[i].end > max_end)
						max_end = intervals[i].end;
				}

				/*
				 * Clamp watermark to actual data boundary.
				 *
				 * actual_boundary was computed up-front (see the chunk-aware
				 * SPI block earlier in this function); apply the same MIN
				 * here so watermark never advances past the hot bucket start.
				 *
				 * This is a defensive clamp -- the unmat-range append above
				 * already clamps its end to actual_boundary, so max_end is
				 * normally <= actual_boundary already.  But L2 entries can
				 * carry user-set window_end's value (e.g. explicit future
				 * date), so the clamp protects against those paths.
				 */
				if (TIMESTAMP_IS_NOBEGIN(actual_boundary))
					max_end = DT_NOBEGIN;	/* empty source -> don't advance */
				else if (TIMESTAMP_IS_NOEND(max_end) || max_end > actual_boundary)
					max_end = actual_boundary;
				/* else: max_end <= actual_boundary -> keep max_end as-is */


				SIMPLE_FAULT_INJECTOR("cagg_refresh_before_watermark_advance");

				/*
				 * Decide whether to advance the watermark.
				 *
				 * The mat branch of the real-time UNION ALL view returns rows
				 * with bucket < watermark.  Advancing watermark past source
				 * buckets that are not in the mat table would hide them (mat
				 * branch claims coverage; live branch covers only >=
				 * watermark).
				 *
				 * Three cases: 1. window_start <= current_watermark -- window
				 * is contiguous with what's already materialized; safe to
				 * advance. 2. window_start > current_watermark, but every
				 * source bucket in the gap [current_watermark, window_start)
				 * is present in the mat table -- advancing hides nothing.
				 * Safe. 3. window_start > current_watermark and source has
				 * buckets in the gap not covered by mat -- advancing would
				 * hide them.  Refuse.
				 *
				 * Why query mat directly: L2 (cagg_materialization_log) only
				 * tracks invalidations of buckets that *were* materialized.
				 * Buckets that were never materialized (e.g., on a fresh CAGG
				 * before any prior refresh covered them) leave no L2 trace. A
				 * LEFT JOIN of source-buckets-in-gap against mat catches both
				 * cases.
				 *
				 * For NULL/NULL refresh, window_start is DT_NOBEGIN, which is
				 * always <= any watermark, so case 1 applies and we advance.
				 */
				bool		advance_ok = (TIMESTAMP_IS_NOBEGIN(window_start) ||
										  window_start <= current_watermark);

				/*
				 * window_start > current_watermark: advancing is safe only if
				 * every source bucket in the gap [current_watermark,
				 * window_start) is already materialized (see the three-case
				 * analysis above).
				 */
				if (!advance_ok)
					advance_ok = cagg_gap_is_materialized(&info, current_watermark,
														  window_start);

				if (advance_ok)
				{
					cagg_advance_watermark(&info, max_end);
					elog(DEBUG1, "cagg \"%s\": watermark advanced to %s",
						 cagg_name_buf,
						 TIMESTAMP_IS_NOBEGIN(max_end) ? "-infinity"
						 : DatumGetCString(DirectFunctionCall1(timestamptz_out,
															   TimestampTzGetDatum(max_end))));

					/*
					 * Watermark actually moved forward: broadcast
					 * invalidations. sinval messages are transactional --
					 * queued here, delivered to other backends only when TX2
					 * commits, discarded on abort -- so receivers can never
					 * observe a watermark from an uncommitted refresh.
					 *
					 * - cagg_watermark relid: clears every backend's local
					 * watermark cache (watermark_constify.c registers a
					 * relcache callback on it); their next planning re-reads
					 * the new value.
					 *
					 * - mat table relid: plancache tracks the relations a
					 * cached plan references (the rewritten cv query
					 * references the mat table), so this forces prepared
					 * statements to replan and drop the old watermark Const
					 * they have baked in.  Mirrors upstream
					 * continuous_aggs_watermark.c, which invalidates the mat
					 * hypertable's relcache for the same reason.
					 *
					 * Skipped when the watermark did not actually advance
					 * (max_end <= current_watermark): nothing changed, so a
					 * broadcast would only cause pointless cache refills.
					 */
					if (!TIMESTAMP_IS_NOBEGIN(max_end) &&
						max_end > current_watermark)
					{
						Oid			ns_oid = ht_get_namespace_oid_cached();
						Oid			wm_oid = OidIsValid(ns_oid)
						? get_relname_relid("cagg_watermark", ns_oid)
						: InvalidOid;
						Oid			mat_ns_oid = get_namespace_oid(info.mat_table_schema,
																   true);
						Oid			mat_oid = OidIsValid(mat_ns_oid)
						? get_relname_relid(info.mat_table_name, mat_ns_oid)
						: InvalidOid;

						if (OidIsValid(wm_oid))
							CacheInvalidateRelcacheByRelid(wm_oid);
						if (OidIsValid(mat_oid))
							CacheInvalidateRelcacheByRelid(mat_oid);
					}
				}
				else
				{
					/*
					 * window_start > current_watermark and source has buckets
					 * in [current_watermark, window_start) not covered by
					 * mat.  We used to ereport() here to preserve a stronger
					 * correctness contract than upstream (TSDB's permissive
					 * gap semantics silently accept the gap).  That contract
					 * assumed refresh was atomic -- an assumption invalidated
					 * once batched refresh (per-batch commit) landed: a chaos
					 * event between batches can leave watermark advanced past
					 * mat holes, and the next policy tick would then hit this
					 * ereport() and stay permanently stuck because the refuse
					 * path had no matching self-heal.  Downgrade to a
					 * no-advance DEBUG log to match upstream: catch-up below
					 * (which is gap-safe) advances the watermark whenever the
					 * gap is actually materialized, and policy-side
					 * expand-back (bgw_policy/cagg_refresh_policy.c) closes
					 * the gap on the next tick by pulling window_start back
					 * to current_watermark.
					 */
					elog(DEBUG1, "cagg \"%s\": watermark NOT advanced "
						 "(unmaterialized buckets in [%s, %s))",
						 cagg_name_buf,
						 TIMESTAMP_IS_NOBEGIN(current_watermark) ? "-infinity"
						 : DatumGetCString(DirectFunctionCall1(timestamptz_out,
															   TimestampTzGetDatum(current_watermark))),
						 DatumGetCString(DirectFunctionCall1(timestamptz_out,
															 TimestampTzGetDatum(window_start))));
				}

				/* Trim L2 entries within the refresh window */
				cagg_trim_l2(&info, window_start, window_end);
			}

			/*
			 * Convergent watermark catch-up -- runs for EVERY refresh,
			 * whether or not this call materialized anything.
			 *
			 * The per-window advance above only moves the watermark to this
			 * refresh's own window end (max_end), and only when the gap below
			 * window_start is already materialized.  Under a newest-first
			 * batched policy that is never enough on a lagging CAGG: when a
			 * batch runs, the older buckets below it have not been
			 * materialized yet (the older batches run later in the same
			 * tick), so its gap check fails and it does not advance.  Only
			 * the oldest batch (empty gap) ever advances, and only to its own
			 * low window end -- so the watermark crawls one batch-width per
			 * tick at best and, with a sliding policy window under a
			 * continuous workload, can stay pinned at -infinity indefinitely.
			 * A pinned watermark makes the cagg_union view serve every query
			 * from the live branch (full source re-aggregation), never
			 * reading the mat table.
			 *
			 * By the time we reach here the materialization for this call is
			 * done, so the mat table reflects every bucket this refresh (and
			 * all prior ones) covered.  Re-read the current watermark and
			 * advance it all the way to actual_boundary whenever the whole
			 * gap [watermark, actual_boundary) is materialized.  Running this
			 * on the oldest batch -- after it fills the last hole -- lets the
			 * watermark reach actual_boundary within a single tick.  Same
			 * safety predicate as the per-window path (advancing past a
			 * materialized gap hides nothing); never past the hot bucket
			 * (actual_boundary is its start).
			 */
			if (!TIMESTAMP_IS_NOBEGIN(actual_boundary))
			{
				TimestampTz cur_wm = cagg_get_min_watermark(&info);

				if ((TIMESTAMP_IS_NOBEGIN(cur_wm) || actual_boundary > cur_wm) &&
					cagg_gap_is_materialized(&info, cur_wm, actual_boundary))
				{
					cagg_advance_watermark(&info, actual_boundary);
					elog(DEBUG1, "cagg \"%s\": catch-up watermark advanced to %s",
						 cagg_name_buf,
						 DatumGetCString(DirectFunctionCall1(timestamptz_out,
															 TimestampTzGetDatum(actual_boundary))));

					/*
					 * Broadcast the same relcache invalidations as the
					 * per-window advance path so backends drop their cached
					 * watermark Const and the mat branch becomes visible.
					 */
					{
						Oid			ns_oid = ht_get_namespace_oid_cached();
						Oid			wm_oid = OidIsValid(ns_oid)
						? get_relname_relid("cagg_watermark", ns_oid)
						: InvalidOid;
						Oid			mat_ns_oid = get_namespace_oid(info.mat_table_schema,
																   true);
						Oid			mat_oid = OidIsValid(mat_ns_oid)
						? get_relname_relid(info.mat_table_name, mat_ns_oid)
						: InvalidOid;

						if (OidIsValid(wm_oid))
							CacheInvalidateRelcacheByRelid(wm_oid);
						if (OidIsValid(mat_oid))
							CacheInvalidateRelcacheByRelid(mat_oid);
					}
				}
			}

			if (intervals)
				pfree(intervals);


			/*
			 * Reconcile invalidation threshold with MAX(watermark) --
			 * MONOTONIC.
			 *
			 * The threshold may only move FORWARD here (GREATEST), never
			 * back.
			 *
			 * Invariant: while any refresh is between its TX1 threshold
			 * advance and its watermark advance, the committed threshold must
			 * stay >= that refresh's materialization end.  Concurrent INSERTs
			 * whose ts is below the in-flight materialization end rely on (ts
			 * < threshold) to write an L1 entry; if another CAGG's refresh on
			 * the same source lowered the threshold mid-flight (the old
			 * unconditional SET did exactly that -- even from a no-op
			 * "already up-to-date" refresh, since this block runs regardless
			 * of n_intervals), such a row would skip L1, miss the in-flight
			 * materialization snapshot, and end up below the advanced
			 * watermark with no invalidation record: permanent silent data
			 * loss.  Reproduced by the cagg_threshold_reconcile_race
			 * isolation2 test.
			 *
			 * The cost of monotonicity -- threshold can sit above
			 * MAX(watermark) after a refresh whose TX1 estimate overshot --
			 * is bounded by the TX1-side clamp (est_threshold is capped at
			 * the source's max-data bucket), and over-threshold merely means
			 * extra L1 writes, never lost data.
			 */
			{
				Oid			argtypes[1] = {OIDOID};
				Datum		args[1];

				args[0] = ObjectIdGetDatum(info.source_table_oid);
				SPI_execute_with_args(
									  "UPDATE time_series.cagg_invalidation_threshold "
									  "SET threshold = GREATEST(threshold, COALESCE(("
									  "  SELECT MAX(w.watermark) "
									  "  FROM time_series.cagg_watermark w "
									  "  JOIN time_series.continuous_agg c ON w.cagg_id = c.cagg_id "
									  "  WHERE c.source_table_oid = $1"
									  "), '-infinity'::timestamptz)) "
									  "WHERE source_table_oid = $1",
									  1, argtypes, args, NULL, false, 0);
			}


			/*
			 * Commit TX2 before returning.  cagg_refresh used to leave TX2
			 * open for the caller to commit: in a batched policy execution
			 * the next batch's TX1 then ran INSIDE the same transaction,
			 * holding every TX2 write lock (ExclusiveLock under GDD-off)
			 * across the batch boundary while it waited for the source
			 * advisory -- the hold-and-wait half of the cross-CAGG ABBA
			 * deadlock that froze SOAK-20260723_170208 for 13h+.
			 * Chain-committing here releases all of this batch's locks at
			 * the boundary, so the next batch's TX1 starts lock-clean, and
			 * each finished batch becomes durable immediately (upstream's
			 * per-batch transaction semantics) instead of riding along
			 * uncommitted until the next batch's chain commit.  See
			 * doc/bugs/cagg-batched-refresh-cross-cagg-abba-deadlock.md.
			 */
			SPI_commit_and_chain();

			/*
			 * Unified cleanup.  All callers (psql, run_job, BGW worker) have
			 * a valid ActivePortal coordinating snapshot/SPI lifecycle, so a
			 * single SPI_finish suffices.
			 */
			SPI_finish();

		}
		PG_FINALLY();
		{
			set_config_option("optimizer", saved_optimizer ? "on" : "off",
							  PGC_USERSET, PGC_S_SESSION,
							  GUC_ACTION_SET, true, 0, false);
		}
		PG_END_TRY();
	}

	PG_RETURN_VOID();
}
