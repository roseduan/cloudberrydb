/*
 * cagg_refresh_policy.c
 *    CAGG automatic refresh policy procedure.
 *
 * This is the "policy procedure" registered in bgw_job.proc_name.  When a
 * BGW worker (or run_job caller) picks up a CAGG refresh job, it invokes
 *   CALL time_series.policy_refresh_cagg(job_id, config)
 * via ExecuteCallStmt (see bgw_job_execute_real).  This procedure
 * extracts the CAGG name and offsets from `config`, computes the refresh
 * window relative to the current (or mocked) timestamp, and dispatches
 *   CALL time_series.refresh_continuous_aggregate(name, start, end)
 * via a nested ExecuteCallStmt.
 *
 * Why ExecuteCallStmt and not SPI_execute("CALL ...")?
 *   The inner refresh_continuous_aggregate uses SPI_commit_and_chain to
 *   split into TX1 (L1 -> L2 migration) and TX2 (materialization +
 *   watermark advance).  CBDB's SPI CALL path forces
 *   callcontext->atomic = true, which the procedure refuses.  Using
 *   ExecuteCallStmt directly mirrors what psql's top-level CALL does and
 *   gives us NONATOMIC context as needed.  The FuncExpr/Const nodes are
 *   allocated in TopMemoryContext so they survive across the inner
 *   StartTransactionCommand performed by SPI_commit_and_chain.
 *
 * JSONB config format:
 *   { "cagg_name": "view_name",
 *     "start_offset": "1 day",   -- optional; absent => NULL => open window
 *     "end_offset": "0" }
 *
 * Copyright (c) 2026 HashData Inc.
 * Licensed under Apache License 2.0
 */
#include <postgres.h>

#include <access/xact.h>
#include <catalog/pg_type.h>
#include <executor/spi.h>
#include <nodes/makefuncs.h>
#include <nodes/parsenodes.h>
#include <parser/parse_func.h>
#include <commands/defrem.h>
#include <tcop/dest.h>
#include <tcop/utility.h>
#include <utils/builtins.h>
#include <utils/jsonb.h>
#include <utils/lsyscache.h>
#include <utils/memutils.h>
#include <utils/snapmgr.h>
#include <utils/timestamp.h>

#include <utils/datum.h>
#include <utils/faultinjector.h>
#include <lib/stringinfo.h>

#include "cagg_refresh_policy.h"
#include "../include/bgw/timer.h"
#include "../include/time_series.h" /* TS_EXTENSION_SCHEMA_NAME */

/* Forward declarations: policy_get_bucket_width below calls
 * policy_parse_qualified_name, which is defined later in the file. */
static void policy_parse_qualified_name(const char *input,
										char *schema_out, char *name_out);

PG_FUNCTION_INFO_V1(policy_refresh_cagg);

/*
 * Extract a text field from a JSONB object (palloc'd in caller context).
 * Returns NULL when the key is missing.
 */
static char *
jsonb_get_text(Jsonb *jb, const char *key)
{
	Datum		keydat = CStringGetTextDatum(key);

	LOCAL_FCINFO(fcinfo, 2);
	Datum		val;

	/*
	 * jsonb_object_field_text returns SQL NULL when the key is missing (sets
	 * fcinfo->isnull = true).  Calling it via DirectFunctionCall2 is wrong:
	 * fmgr's DirectFunctionCallN raises "function %p returned NULL" on a NULL
	 * return, so the caller sees a generic raw-pointer ERROR (instead of our
	 * friendly "job N config missing $key" message) and the post-call `if
	 * (val == NULL) return NULL` branch is unreachable.
	 *
	 * Use a manual LOCAL_FCINFO so we can inspect fcinfo->isnull ourselves
	 * and return NULL to the caller for the missing-key case, while still
	 * propagating real ERRORs (e.g. type mismatch) through ereport.
	 */
	InitFunctionCallInfoData(*fcinfo, NULL, 2, InvalidOid, NULL, NULL);
	fcinfo->args[0].value = JsonbPGetDatum(jb);
	fcinfo->args[0].isnull = false;
	fcinfo->args[1].value = keydat;
	fcinfo->args[1].isnull = false;

	val = jsonb_object_field_text(fcinfo);
	if (fcinfo->isnull)
		return NULL;
	return text_to_cstring(DatumGetTextP(val));
}

/*
 * Read an int4 field from JSONB.  Missing key or SQL NULL returns the
 * supplied default -- keeps callers free of branching on absence.
 * Stored as a JSON number so add_continuous_aggregate_policy's
 * jsonb_build_object(..., 'buckets_per_batch', int) round-trips
 * unchanged.
 */
static int32
jsonb_get_int4_default(Jsonb *jb, const char *key, int32 dflt)
{
	char	   *s = jsonb_get_text(jb, key);
	int32		v;

	if (s == NULL)
		return dflt;

	v = pg_strtoint32(s);
	pfree(s);
	return v;
}

/*
 * Read a bool field from JSONB.  Missing key or SQL NULL returns the
 * supplied default.  Stored as a JSON boolean by
 * jsonb_build_object(..., 'refresh_newest_first', bool), whose text form
 * is "true"/"false".
 */
static bool
jsonb_get_bool_default(Jsonb *jb, const char *key, bool dflt)
{
	char	   *s = jsonb_get_text(jb, key);
	bool		v;

	if (s == NULL)
		return dflt;

	v = (s[0] == 't' || s[0] == 'T');
	pfree(s);
	return v;
}

/*
 * One sub-window produced by policy_split_refresh_window().  Mirrors the
 * relevant fields of upstream's InternalTimeRange (tsl/src/continuous_aggs).
 */
typedef struct PolicyBatchWindow
{
	TimestampTz start;
	TimestampTz end;
	bool		start_isnull;
	bool		end_isnull;
}			PolicyBatchWindow;

/*
 * CAGG metadata needed to decide and drive batching.
 */
typedef struct PolicySplitMeta
{
	int32		cagg_id;
	Oid			source_oid;
	Interval   *bucket_width;	/* palloc'd in caller ctx */
	TimestampTz origin;
	bool		has_origin;
	bool		is_variable;	/* month != 0, or offset/timezone set */
}			PolicySplitMeta;

/*
 * Fetch the metadata required to split a refresh window into batches:
 * cagg_id, source table oid, bucket width, optional origin, and whether
 * the bucket is variable-width (monthly / offset / timezone).
 *
 * Returns false if the CAGG (or its bucket function row) cannot be found,
 * in which case the caller falls back to single-batch execution.
 *
 * SPI is opened and closed internally.  Caller must own a transaction.
 */
static bool
policy_get_split_meta(const char *cagg_name, PolicySplitMeta * meta)
{
	char		schema_buf[NAMEDATALEN];
	char		name_buf[NAMEDATALEN];
	Oid			argtypes[2] = {TEXTOID, TEXTOID};
	Datum		args[2];
	MemoryContext caller_cxt = CurrentMemoryContext;
	bool		found = false;
	int			ret;

	policy_parse_qualified_name(cagg_name, schema_buf, name_buf);
	args[0] = CStringGetTextDatum(schema_buf);
	args[1] = CStringGetTextDatum(name_buf);

	ret = SPI_connect();
	if (ret != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed: %s", SPI_result_code_string(ret));

	/*
	 * is_variable mirrors upstream's bucket_fixed_interval == false: a
	 * monthly bucket (width has a month component), or a bucket carrying a
	 * custom offset or timezone, needs the variable-width inscribe path which
	 * is not ported here -- such CAGGs degrade to single-batch.
	 */
	ret = SPI_execute_with_args(
								"SELECT c.cagg_id, c.source_table_oid, bf.bucket_width, "
								"       bf.bucket_origin, "
								"       (EXTRACT(month FROM bf.bucket_width) <> 0 "
								"        OR EXTRACT(year FROM bf.bucket_width) <> 0 "
								"        OR bf.bucket_offset IS NOT NULL "
								"        OR bf.bucket_timezone IS NOT NULL) AS is_variable "
								"FROM time_series.continuous_agg c "
								"JOIN time_series.cagg_bucket_function bf ON bf.cagg_id = c.cagg_id "
								"WHERE c.user_view_schema = $1 AND c.user_view_name = $2",
								2, argtypes, args, NULL, true, 1);

	if (ret != SPI_OK_SELECT)
		elog(ERROR, "could not read continuous aggregate split metadata: %s",
			 SPI_result_code_string(ret));

	if (SPI_processed == 1)
	{
		TupleDesc	desc = SPI_tuptable->tupdesc;
		HeapTuple	tup = SPI_tuptable->vals[0];
		bool		isnull;
		Datum		d;
		MemoryContext spi_cxt;

		found = true;

		meta->cagg_id = DatumGetInt32(SPI_getbinval(tup, desc, 1, &isnull));
		meta->source_oid = DatumGetObjectId(SPI_getbinval(tup, desc, 2, &isnull));

		d = SPI_getbinval(tup, desc, 3, &isnull);
		spi_cxt = MemoryContextSwitchTo(caller_cxt);
		meta->bucket_width = isnull ? NULL :
			(Interval *) datumCopy(d, false, sizeof(Interval));
		MemoryContextSwitchTo(spi_cxt);

		d = SPI_getbinval(tup, desc, 4, &isnull);
		meta->has_origin = !isnull;
		meta->origin = isnull ? 0 : DatumGetTimestampTz(d);

		d = SPI_getbinval(tup, desc, 5, &isnull);
		meta->is_variable = isnull ? true : DatumGetBool(d);
	}

	SPI_finish();
	return found;
}

/*
 * policy_split_refresh_window -- faithful port of upstream
 * continuous_agg_split_refresh_window (tsl/src/continuous_aggs/refresh.c).
 *
 * Splits [ws, we) into bucket-aligned sub-windows of (buckets_per_batch *
 * bucket_width), returning them in REVERSE chronological order (newest
 * batch first) so the most recent data is materialized -- and made
 * visible -- first.  Returns NULL (out_count = 0) to signal "fall back to
 * single batch", which the caller treats identically to upstream's NIL.
 *
 * The algorithm mirrors upstream step for step, adapted to our catalog:
 *   1. buckets_per_batch == 0           -> single batch (handled by caller)
 *   2. cap NULL window edges to the source's min/max chunk range
 *      (upstream: earliest/latest dimension slice)
 *   3. inscribe the capped window to whole bucket boundaries
 *   4. window_size <= batch_size        -> single batch
 *   5. emit only candidate ranges that overlap BOTH an existing chunk
 *      (data present) AND an invalidation entry (data dirty); skip the
 *      holes -- mirrors upstream's dimension_slice + invalidation_log
 *      double-EXISTS filter
 *   6. <= 1 batch survives              -> single batch
 *   7. preserve original NULL edges on the first/last surviving batch
 *
 * Variable-width buckets are NOT handled here (caller checks is_variable
 * and falls back), the one deliberate deviation from upstream which ports
 * ts_compute_inscribed_bucketed_refresh_window_variable.
 *
 * SPI is opened and closed internally.  The returned array is palloc'd in
 * the caller's context.
 */
static PolicyBatchWindow *
policy_split_refresh_window(const PolicySplitMeta * meta,
							TimestampTz ws, bool ws_isnull,
							TimestampTz we, bool we_isnull,
							int32 buckets_per_batch,
							bool refresh_newest_first,
							int *out_count)
{
	/*
	 * batch_interval = bucket_width * buckets_per_batch (month == 0 here).
	 *
	 * Param layout (origin is the only conditional param, appended last): $1
	 * source_oid   $2 cagg_id      $3 bucket_width $4 ws           $5 we $6
	 * batch_interval $7 origin       (only when meta->has_origin)
	 */
	Interval	batch_interval;
	Oid			argtypes[7];
	Datum		args[7];
	char		nulls[7];
	int			nargs;
	StringInfoData query;
	MemoryContext caller_cxt = CurrentMemoryContext;
	PolicyBatchWindow *batches = NULL;
	int			n = 0;
	int			ret;

	*out_count = 0;

	Assert(buckets_per_batch > 0);
	Assert(meta->bucket_width != NULL && meta->bucket_width->month == 0);

	batch_interval.month = 0;
	batch_interval.day = meta->bucket_width->day * buckets_per_batch;
	batch_interval.time = meta->bucket_width->time * buckets_per_batch;

	/*
	 * Single SQL statement performs cap (COALESCE NULL edges with chunk
	 * min/max), inscribe (time_bucket ceil/floor), candidate generation
	 * (generate_series stepping by batch_interval), and the hole-skip
	 * double-EXISTS, returning surviving batches newest-first.
	 *
	 * If the window is unbounded on a side and there are no chunks, the
	 * COALESCE yields NULL and generate_series produces nothing -> zero rows
	 * -> single batch, matching upstream's "no slice -> NIL".
	 *
	 * The inscribe uses time_bucket with the CAGG's origin when set so batch
	 * boundaries land exactly on bucket edges; stepping by an integer
	 * multiple of bucket_width keeps every later boundary aligned too.  The
	 * inner refresh re-inscribes defensively, so a boundary that is already
	 * aligned is a no-op there.
	 *
	 * Invalidation overlap extends the upper edge by one bucket_width so a
	 * single dirtied bucket (lowest == greatest) still overlaps; this is
	 * deliberately conservative -- a falsely-kept batch is a harmless no-op
	 * in the inner refresh, whereas a falsely-dropped batch would silently
	 * lose data.
	 */
	initStringInfo(&query);
	appendStringInfoString(&query,
						   "WITH raw AS ( "
						   "  SELECT "
						   "    COALESCE($4::timestamptz, "
						   "             (SELECT min(range_start) FROM time_series.ts_chunk "
						   "               WHERE table_oid = $1)) AS rstart, "
						   "    COALESCE($5::timestamptz, "
						   "             (SELECT max(range_end)   FROM time_series.ts_chunk "
						   "               WHERE table_oid = $1)) AS rend "
						   "), "
						   "aligned AS ( "
						   "  SELECT "
						   "    CASE WHEN ");
	/* ceil(start): bucket(start) == start ? start : bucket(start) + width */
	if (meta->has_origin)
		appendStringInfoString(&query,
							   "time_series.time_bucket($3, rstart, $7) = rstart THEN rstart "
							   "         ELSE time_series.time_bucket($3, rstart, $7) + $3 "
							   "         END AS astart, "
							   "    time_series.time_bucket($3, rend, $7) AS aend ");
	else
		appendStringInfoString(&query,
							   "time_series.time_bucket($3, rstart) = rstart THEN rstart "
							   "         ELSE time_series.time_bucket($3, rstart) + $3 "
							   "         END AS astart, "
							   "    time_series.time_bucket($3, rend) AS aend ");
	appendStringInfoString(&query,
						   "  FROM raw WHERE rstart IS NOT NULL AND rend IS NOT NULL "
						   "), "
						   "chunks AS ( "
						   "  SELECT DISTINCT range_start, range_end "
						   "    FROM time_series.ts_chunk WHERE table_oid = $1 "
						   "), "
						   "invalidations AS ( "
						   "  SELECT lowest_modified, greatest_modified "
						   "    FROM time_series.cagg_materialization_log WHERE cagg_id = $2 "
						   "  UNION ALL "
						   "  SELECT min(lowest_modified), max(greatest_modified) "
						   "    FROM time_series.cagg_invalidation_log "
						   "    WHERE source_table_oid = $1 "
						   "), "
						   "candidates AS ( "
						   "  SELECT gs AS bstart, LEAST(a.aend, gs + $6) AS bend "
						   "  FROM aligned a, "
						   "       LATERAL generate_series(a.astart, "
						   "                               a.aend - interval '1 microsecond', "
						   "                               $6) AS gs "
						   ") "
						   "SELECT c.bstart, c.bend "
						   "FROM candidates c "
						   "WHERE EXISTS (SELECT 1 FROM chunks ch "
						   "              WHERE tstzrange(c.bstart, c.bend, '[)') "
						   "                    && tstzrange(ch.range_start, "
						   "                                 ch.range_end, '[)')) "
						   "  AND EXISTS (SELECT 1 FROM invalidations iv "
						   "              WHERE iv.lowest_modified IS NOT NULL "
						   "                AND iv.greatest_modified IS NOT NULL "
						   "                AND tstzrange(c.bstart, c.bend, '[)') "
						   "                    && tstzrange(iv.lowest_modified, "
						   "                                 iv.greatest_modified + $3, "
						   "                                 '[)')) "
						   "ORDER BY c.bstart ");

	/*
	 * Batch order: newest-first (DESC, the default) makes the most recent
	 * data visible first; oldest-first (ASC) suits chronological historical
	 * backfill.  Mirrors upstream's refresh_newest_first knob.
	 */
	appendStringInfoString(&query, refresh_newest_first ? "DESC" : "ASC");

	argtypes[0] = OIDOID;
	args[0] = ObjectIdGetDatum(meta->source_oid);
	nulls[0] = ' ';
	argtypes[1] = INT4OID;
	args[1] = Int32GetDatum(meta->cagg_id);
	nulls[1] = ' ';
	argtypes[2] = INTERVALOID;
	args[2] = IntervalPGetDatum(meta->bucket_width);
	nulls[2] = ' ';
	argtypes[3] = TIMESTAMPTZOID;
	args[3] = TimestampTzGetDatum(ws);
	nulls[3] = ws_isnull ? 'n' : ' ';
	argtypes[4] = TIMESTAMPTZOID;
	args[4] = TimestampTzGetDatum(we);
	nulls[4] = we_isnull ? 'n' : ' ';
	argtypes[5] = INTERVALOID;
	args[5] = IntervalPGetDatum(&batch_interval);
	nulls[5] = ' ';
	nargs = 6;
	if (meta->has_origin)
	{
		argtypes[6] = TIMESTAMPTZOID;
		args[6] = TimestampTzGetDatum(meta->origin);
		nulls[6] = ' ';
		nargs = 7;
	}

	ret = SPI_connect();
	if (ret != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed: %s", SPI_result_code_string(ret));

	ret = SPI_execute_with_args(query.data, nargs, argtypes, args,
								nulls, true /* read_only */ , 0);
	if (ret != SPI_OK_SELECT)
		elog(ERROR, "could not produce batches for cagg refresh policy: %s",
			 SPI_result_code_string(ret));

	/*
	 * <= 1 surviving batch -> single batch (collapses upstream's "no slice",
	 * "window <= batch_size", and "only one batch produced" fallbacks into
	 * one check).
	 */
	if (SPI_processed > 1)
	{
		uint64		i2;
		MemoryContext spi_cxt = MemoryContextSwitchTo(caller_cxt);

		batches = (PolicyBatchWindow *)
			palloc0(sizeof(PolicyBatchWindow) * SPI_processed);
		MemoryContextSwitchTo(spi_cxt);

		for (i2 = 0; i2 < SPI_processed; i2++)
		{
			bool		s_null,
						e_null;
			Datum		s = SPI_getbinval(SPI_tuptable->vals[i2],
										  SPI_tuptable->tupdesc, 1, &s_null);
			Datum		e = SPI_getbinval(SPI_tuptable->vals[i2],
										  SPI_tuptable->tupdesc, 2, &e_null);

			batches[i2].start = DatumGetTimestampTz(s);
			batches[i2].end = DatumGetTimestampTz(e);
			batches[i2].start_isnull = false;
			batches[i2].end_isnull = false;
		}
		n = (int) SPI_processed;
	}

	SPI_finish();
	pfree(query.data);

	if (n <= 1)
		return NULL;

	/*
	 * Preserve the caller's open boundaries on the extreme batches so an
	 * unbounded original window stays unbounded at its open end.  The latest
	 * batch carries the (open) end; the earliest batch carries the (open)
	 * start.  Their array positions depend on the ordering: - newest-first:
	 * latest = index 0,     earliest = index n-1 - oldest-first: latest =
	 * index n-1,   earliest = index 0 Mirrors upstream's i==0 / i==(n-1) NULL
	 * fix-ups gated on refresh_newest_first.
	 */
	{
		int			latest_idx = refresh_newest_first ? 0 : (n - 1);
		int			earliest_idx = refresh_newest_first ? (n - 1) : 0;

		if (we_isnull)
			batches[latest_idx].end_isnull = true;
		if (ws_isnull)
			batches[earliest_idx].start_isnull = true;
	}

	*out_count = n;
	return batches;
}

static void
policy_parse_qualified_name(const char *input,
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

static bool
policy_get_min_watermark(const char *cagg_name, TimestampTz *watermark)
{
	char		schema_buf[NAMEDATALEN];
	char		name_buf[NAMEDATALEN];
	Oid			argtypes[2] = {TEXTOID, TEXTOID};
	Datum		args[2];
	int			ret;
	bool		isnull;

	policy_parse_qualified_name(cagg_name, schema_buf, name_buf);

	args[0] = CStringGetTextDatum(schema_buf);
	args[1] = CStringGetTextDatum(name_buf);

	ret = SPI_connect();
	if (ret != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed: %s", SPI_result_code_string(ret));

	ret = SPI_execute_with_args(
								"SELECT MIN(w.watermark) "
								"FROM time_series.continuous_agg c "
								"JOIN time_series.cagg_watermark w ON w.cagg_id = c.cagg_id "
								"WHERE c.user_view_schema = $1 AND c.user_view_name = $2",
								2, argtypes, args, NULL, true, 1);

	if (ret != SPI_OK_SELECT)
		elog(ERROR, "could not read continuous aggregate watermark: %s",
			 SPI_result_code_string(ret));

	if (SPI_processed == 0)
	{
		SPI_finish();
		return false;
	}

	*watermark = DatumGetTimestampTz(
									 SPI_getbinval(SPI_tuptable->vals[0],
												   SPI_tuptable->tupdesc, 1, &isnull));

	SPI_finish();

	return !isnull;
}

/*
 * Oldest pending obligation for this continuous aggregate, floored to
 * the start of its containing bucket.
 *
 * "Pending" is the union of the two invalidation ledgers:
 *   - cagg_materialization_log (L2): ranges already distributed to this
 *     cagg that no refresh has consumed yet;
 *   - cagg_invalidation_log (L1): ranges recorded by writers that no
 *     refresh has distributed yet (consulted too, so a fresh write is
 *     honoured on the very next tick instead of one tick later).
 *
 * The result is floored via time_bucket(): the refresh path aligns a
 * bounded window INSCRIBED (start rounds UP to the next bucket
 * boundary), so anchoring the window at the raw entry timestamp would
 * round the straddled bucket back OUT of the window -- the exact edge
 * that stranded the orphan bucket in SOAK-20260721_161133.
 *
 * A -infinity ledger entry (whole-range invalidation) is deliberately
 * mapped to "nothing pending" via NULLIF: expanding the window to
 * -infinity is the unbounded-first-tick footgun this function's caller
 * must never reintroduce.  Such entries are handled by the bounded
 * window as it slides, never by expansion.
 *
 * Returns false when there is nothing pending (both ledgers empty for
 * this cagg/source).  A brand-new CAGG therefore never expands, which
 * keeps the "first policy tick materializes all history" footgun fixed
 * with a sharper predicate than the old blanket -infinity-watermark
 * guard: expand iff work is actually owed.
 */
static bool
policy_get_oldest_pending(const char *cagg_name, TimestampTz *pending)
{
	char		schema_buf[NAMEDATALEN];
	char		name_buf[NAMEDATALEN];
	Oid			argtypes[2] = {TEXTOID, TEXTOID};
	Datum		args[2];
	int			ret;
	bool		isnull;

	policy_parse_qualified_name(cagg_name, schema_buf, name_buf);

	args[0] = CStringGetTextDatum(schema_buf);
	args[1] = CStringGetTextDatum(name_buf);

	ret = SPI_connect();
	if (ret != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed: %s", SPI_result_code_string(ret));

	/*
	 * LEAST() ignores NULL operands, so an empty ledger on either side
	 * defers to the other; NULL overall means nothing is pending.  The
	 * outer MIN() collapses replicated-catalog fanout.
	 */
	ret = SPI_execute_with_args(
								"SELECT MIN(time_series.time_bucket(c.bucket_width, NULLIF(LEAST("
								" (SELECT MIN(l.lowest_modified)"
								"    FROM time_series.cagg_materialization_log l"
								"   WHERE l.cagg_id = c.cagg_id),"
								" (SELECT MIN(i.lowest_modified)"
								"    FROM time_series.cagg_invalidation_log i"
								"   WHERE i.source_table_oid = c.source_table_oid)"
								"), '-infinity'::timestamptz))) "
								"FROM time_series.continuous_agg c "
								"WHERE c.user_view_schema = $1 AND c.user_view_name = $2",
								2, argtypes, args, NULL, true, 1);

	if (ret != SPI_OK_SELECT)
		elog(ERROR, "could not read continuous aggregate pending invalidations: %s",
			 SPI_result_code_string(ret));

	if (SPI_processed == 0)
	{
		SPI_finish();
		return false;
	}

	*pending = DatumGetTimestampTz(
								   SPI_getbinval(SPI_tuptable->vals[0],
												 SPI_tuptable->tupdesc, 1, &isnull));

	SPI_finish();

	return !isnull;
}

/*
 * Build + execute one inner CALL refresh_continuous_aggregate(text, ts,
 * ts, bool) for the supplied (cagg_name, window).  Each call runs in
 * its own AllocSet context anchored in TopMemoryContext so the
 * FuncExpr / Const nodes survive cagg_refresh's SPI_commit_and_chain
 * (which destroys CurTransactionContext mid-call) and so the per-batch
 * allocations are reclaimed on RETURN or ERROR via PG_FINALLY.
 *
 * Mirrors the legacy single-batch logic, just factored out so the
 * batching loop in policy_refresh_cagg can invoke it per sub-window.
 */
static void
policy_execute_one_refresh(const char *cagg_name,
						   TimestampTz ws, bool ws_isnull,
						   TimestampTz we, bool we_isnull)
{
	Oid			procoid;
	Const	   *arg0,
			   *arg1,
			   *arg2,
			   *arg3;
	FuncExpr   *funcexpr;
	CallStmt   *call;
	DestReceiver *dest;
	ParamListInfo params;
	MemoryContext oldctx;
	MemoryContext policy_ctx;

	policy_ctx = AllocSetContextCreate(TopMemoryContext,
									   "policy_refresh_cagg",
									   ALLOCSET_SMALL_SIZES);
	oldctx = MemoryContextSwitchTo(policy_ctx);

	{
		ObjectWithArgs *obj = makeNode(ObjectWithArgs);

		obj->objname = list_make2(makeString(TS_EXTENSION_SCHEMA_NAME),
								  makeString("refresh_continuous_aggregate"));
		obj->objargs = list_make4(SystemTypeName("regclass"),
								  SystemTypeName("timestamptz"),
								  SystemTypeName("timestamptz"),
								  SystemTypeName("bool"));
		procoid = LookupFuncWithArgs(OBJECT_ROUTINE, obj, false);
	}

	/*
	 * refresh_continuous_aggregate now takes the CAGG as a regclass (see
	 * upstream); resolve the stored (schema-qualified) name to its OID.
	 * regclassin honours search_path, and the policy stores a qualified name,
	 * so this resolves regardless of the worker's search_path.
	 */
	arg0 = makeConst(REGCLASSOID, -1, InvalidOid, 4,
					 DirectFunctionCall1(regclassin, CStringGetDatum(cagg_name)),
					 false, true);
	arg1 = ws_isnull
		? (Const *) makeNullConst(TIMESTAMPTZOID, -1, InvalidOid)
		: makeConst(TIMESTAMPTZOID, -1, InvalidOid, 8,
					TimestampTzGetDatum(ws), false, true);
	arg2 = we_isnull
		? (Const *) makeNullConst(TIMESTAMPTZOID, -1, InvalidOid)
		: makeConst(TIMESTAMPTZOID, -1, InvalidOid, 8,
					TimestampTzGetDatum(we), false, true);

	/*
	 * Policy-driven refresh always runs with force=FALSE -- force is the
	 * recovery escape hatch for manual data-drift repair, not a scheduled-job
	 * behavior.
	 */
	arg3 = makeConst(BOOLOID, -1, InvalidOid, 1,
					 BoolGetDatum(false), false, true);

	funcexpr = makeFuncExpr(procoid, VOIDOID,
							list_make4(arg0, arg1, arg2, arg3),
							InvalidOid, InvalidOid, COERCE_EXPLICIT_CALL);
	call = makeNode(CallStmt);
	call->funcexpr = funcexpr;
	dest = CreateDestReceiver(DestNone);
	params = makeParamList(0);

	MemoryContextSwitchTo(oldctx);

	PG_TRY();
	{
		ExecuteCallStmt(call, params, false /* atomic */ , dest);
	}
	PG_FINALLY();
	{
		MemoryContextDelete(policy_ctx);
	}
	PG_END_TRY();
}

/*
 * Policy procedure entry point:
 *		policy_refresh_cagg(job_id int4, config jsonb).
 */
Datum
policy_refresh_cagg(PG_FUNCTION_ARGS)
{
	int32		job_id = PG_GETARG_INT32(0);
	Jsonb	   *config = PG_ARGISNULL(1) ? NULL : PG_GETARG_JSONB_P(1);
	char	   *cagg_name;
	char	   *start_offset_str;
	char	   *end_offset_str;
	int32		buckets_per_batch;
	int32		max_batches;
	bool		refresh_newest_first;
	TimestampTz now_ts;
	TimestampTz window_start_ts = 0;
	TimestampTz window_end_ts = 0;
	bool		ws_isnull = true;
	bool		we_isnull = true;

	if (config == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("job %d has NULL config", job_id)));

	/* Extract parameters from JSONB config */
	cagg_name = jsonb_get_text(config, "cagg_name");
	if (cagg_name == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("job %d config missing \"cagg_name\"", job_id)));

	start_offset_str = jsonb_get_text(config, "start_offset");
	end_offset_str = jsonb_get_text(config, "end_offset");

	/*
	 * Batching params.  These fallbacks only apply to a config JSONB that
	 * predates the key (add_continuous_aggregate_policy always writes both),
	 * so they must track the SQL-level defaults -- which mirror upstream:
	 * split into 10-bucket batches, no per-execution batch cap.  A value of
	 * 0 for buckets_per_batch means "one batch for the whole window" and 0
	 * for max_batches means "unlimited"; see add_continuous_aggregate_policy
	 * for the user-facing description of the trade-off.
	 */
	buckets_per_batch = jsonb_get_int4_default(config, "buckets_per_batch", 10);
	max_batches = jsonb_get_int4_default(config,
										 "max_batches_per_execution", 0);
	/* Upstream default is newest-first (most recent data visible first). */
	refresh_newest_first = jsonb_get_bool_default(config,
												  "refresh_newest_first",
												  true);

	/*
	 * Compute refresh window relative to current (mockable) time. NULL offset
	 * -> open boundary on that side.
	 */
	now_ts = timer_get_current_timestamp();
	if (start_offset_str != NULL)
	{
		Interval   *si = DatumGetIntervalP(
										   DirectFunctionCall3(interval_in,
															   CStringGetDatum(start_offset_str),
															   ObjectIdGetDatum(InvalidOid),
															   Int32GetDatum(-1)));

		window_start_ts = DatumGetTimestampTz(
											  DirectFunctionCall2(timestamptz_mi_interval,
																  TimestampTzGetDatum(now_ts),
																  IntervalPGetDatum(si)));
		ws_isnull = false;
	}
	if (end_offset_str != NULL)
	{
		Interval   *ei = DatumGetIntervalP(
										   DirectFunctionCall3(interval_in,
															   CStringGetDatum(end_offset_str),
															   ObjectIdGetDatum(InvalidOid),
															   Int32GetDatum(-1)));

		window_end_ts = DatumGetTimestampTz(
											DirectFunctionCall2(timestamptz_mi_interval,
																TimestampTzGetDatum(now_ts),
																IntervalPGetDatum(ei)));
		we_isnull = false;
	}

	if (!ws_isnull)
	{
		TimestampTz current_watermark;

		/*
		 * Expand window_start backward to the current watermark when the
		 * policy schedule fell behind (e.g. BGW worker restart, scheduler
		 * delay): without this, buckets in [old_window_end, new_window_start)
		 * would never be refreshed by the policy.  This is a defensive
		 * enhancement over upstream -- TSDB's policy always uses exactly
		 * `now() - start_offset` and leaves gap-recovery to the user.
		 *
		 * IMPORTANT: skip when watermark = -infinity.  A brand-new CAGG has
		 * watermark = -infinity (see cagg_init_segment_watermark in
		 * insert.c). If we expanded to -infinity, the very first policy tick
		 * would materialize the ENTIRE source history in a single
		 * transaction, which (a) blows past scheduler lock_timeout, (b) holds
		 * bgw_job_stat locks for minutes/hours blocking other jobs, and (c)
		 * can OOM on large historical data.  Historical backfill is the
		 * user's job -- they must call refresh_continuous_aggregate(cv,
		 * '-infinity', NULL) explicitly, matching upstream behavior.
		 */
		if (policy_get_min_watermark(cagg_name, &current_watermark) &&
			!TIMESTAMP_IS_NOBEGIN(current_watermark) &&
			current_watermark < window_start_ts)
		{
			elog(DEBUG1, "CAGG refresh policy job %d expanding window start "
				 "from %s to %s to cover unmaterialized watermark gap",
				 job_id,
				 DatumGetCString(DirectFunctionCall1(timestamptz_out,
													 TimestampTzGetDatum(window_start_ts))),
				 DatumGetCString(DirectFunctionCall1(timestamptz_out,
													 TimestampTzGetDatum(current_watermark))));

			window_start_ts = current_watermark;
		}

		/*
		 * Ledger-driven expand-back: pending invalidations BELOW the
		 * sliding window are themselves the reason to pull the window
		 * down, independent of the watermark value.
		 *
		 * The wm-based expansion above is blind to two real cases:
		 *   (a) watermark still -infinity (cold start): the -infinity
		 *       guard above deliberately skips it, so a backfill that
		 *       landed below the window is parked in the ledgers with no
		 *       consumer, the catch-up gap probe keeps failing on its
		 *       bucket, and the watermark stays pinned at -infinity
		 *       while every refresh re-materializes a growing window
		 *       (SOAK-20260721_161133, cv_1hour: pinned for 12h, mat
		 *       unread, refresh cost O(elapsed));
		 *   (b) watermark ahead of window_start (steady state): a
		 *       backfill below now-start_offset never satisfies
		 *       wm < window_start, so its buckets stay silently stale
		 *       forever.
		 *
		 * Anchoring on the ledgers fixes both.  A brand-new CAGG has no
		 * ledger entries and therefore never expands: the unbounded
		 * first-tick footgun the -infinity guard protects against stays
		 * fixed, just with a sharper predicate -- expand iff work is
		 * actually owed.  The anchor is already bucket-floored (see
		 * policy_get_oldest_pending) so the refresh path's inscribed
		 * alignment cannot round the straddled bucket back out of the
		 * window.
		 */
		{
			TimestampTz pending_low;

			if (policy_get_oldest_pending(cagg_name, &pending_low) &&
				!TIMESTAMP_IS_NOBEGIN(pending_low) &&
				pending_low < window_start_ts)
			{
				elog(DEBUG1, "CAGG refresh policy job %d expanding window start "
					 "from %s to %s to cover pending invalidations below the window",
					 job_id,
					 DatumGetCString(DirectFunctionCall1(timestamptz_out,
														 TimestampTzGetDatum(window_start_ts))),
					 DatumGetCString(DirectFunctionCall1(timestamptz_out,
														 TimestampTzGetDatum(pending_low))));

				window_start_ts = pending_low;
			}
		}
	}

	elog(DEBUG1, "running CAGG refresh policy: job_id=%d cagg=\"%s\" "
		 "window=[%s, %s) (offsets: start=%s end=%s)",
		 job_id, cagg_name,
		 ws_isnull ? "-infinity"
		 : DatumGetCString(DirectFunctionCall1(timestamptz_out,
											   TimestampTzGetDatum(window_start_ts))),
		 we_isnull ? "+infinity"
		 : DatumGetCString(DirectFunctionCall1(timestamptz_out,
											   TimestampTzGetDatum(window_end_ts))),
		 start_offset_str ? start_offset_str : "NULL",
		 end_offset_str ? end_offset_str : "NULL");

	/*
	 * Dispatch (mirrors upstream policy_refresh_cagg_execute +
	 * continuous_agg_split_refresh_window).
	 *
	 * Try to split the window into bucket-aligned batches.  When the split
	 * declines (buckets_per_batch == 0, variable-width bucket, or the window
	 * yields <= 1 batch) it returns NULL and we run the whole window as a
	 * single batch -- identical to the legacy behavior and to upstream's
	 * "refresh_window_list == NIL" fallback.
	 *
	 * Otherwise we process the batches in the order returned (newest first,
	 * so recent data becomes visible first), each as its own TX1/TX2 cycle,
	 * and stop after max_batches_per_execution batches so a single policy
	 * tick cannot monopolize the bgw worker; the remainder resumes on the
	 * next schedule tick.
	 */
	{
		PolicyBatchWindow *batches = NULL;
		int			n_batches = 0;

		if (buckets_per_batch > 0)
		{
			PolicySplitMeta meta = {0};

			if (!policy_get_split_meta(cagg_name, &meta))
			{
				elog(LOG, "CAGG refresh policy job %d: continuous aggregate "
					 "\"%s\" metadata not found, falling back to single batch",
					 job_id, cagg_name);
			}
			else if (meta.is_variable || meta.bucket_width == NULL)
			{
				/*
				 * Variable-width buckets (monthly, or with offset/timezone)
				 * need the inscribed-window-variable math which is not
				 * ported; fall back to single batch.  This is the one
				 * deliberate deviation from upstream, which DOES batch
				 * variable buckets.
				 */
				elog(LOG, "CAGG refresh policy job %d: variable-width bucket "
					 "for \"%s\", falling back to single batch",
					 job_id, cagg_name);
			}
			else
			{
				batches = policy_split_refresh_window(&meta,
													  window_start_ts, ws_isnull,
													  window_end_ts, we_isnull,
													  buckets_per_batch,
													  refresh_newest_first,
													  &n_batches);
			}

			if (meta.bucket_width != NULL)
				pfree(meta.bucket_width);
		}

		if (batches == NULL)
		{
			/* Single-batch path (legacy + upstream NIL fallback). */
			policy_execute_one_refresh(cagg_name,
									   window_start_ts, ws_isnull,
									   window_end_ts, we_isnull);
		}
		else
		{
			int			processing_batch = 0;
			int			i;

			elog(LOG, "CAGG refresh policy job %d batching \"%s\" into %d "
				 "batch(es) (buckets_per_batch=%d, max_batches_per_execution=%d)",
				 job_id, cagg_name, n_batches, buckets_per_batch, max_batches);

			for (i = 0; i < n_batches; i++)
			{
				/*
				 * Deterministic batch-boundary abort for tests: fires
				 * BETWEEN batches -- batch i-1 has committed (its own
				 * TX1/TX2 cycle), batch i has not started.  An 'error'
				 * fault here reproduces exactly what a mid-execution
				 * worker kill leaves behind: the committed batches are
				 * durable, the remaining ones never ran, and (under
				 * newest-first) the mat now has an island with a hole
				 * below it.  See iso2 cagg_hole_batch_boundary.
				 */
				if (i > 0)
					SIMPLE_FAULT_INJECTOR("cagg_policy_batch_boundary");

				processing_batch++;
				policy_execute_one_refresh(cagg_name,
										   batches[i].start, batches[i].start_isnull,
										   batches[i].end, batches[i].end_isnull);

				/*
				 * Cap per-execution batch count (mirrors upstream's break
				 * condition exactly).  max_batches == 0 means unlimited.
				 */
				if (processing_batch >= max_batches &&
					processing_batch < n_batches &&
					max_batches > 0)
				{
					elog(LOG, "reached maximum number of batches per execution "
						 "(%d), batches not processed (%d)",
						 max_batches, n_batches - processing_batch);
					break;
				}
			}

			pfree(batches);
		}
	}

	pfree(cagg_name);
	if (start_offset_str)
		pfree(start_offset_str);
	if (end_offset_str)
		pfree(end_offset_str);

	PG_RETURN_VOID();
}
