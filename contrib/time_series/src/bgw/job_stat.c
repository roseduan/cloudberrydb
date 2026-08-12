/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 *
 * Portions Copyright (c) 2025-2026, HashData Technology Limited.
 *
 * Adapted from upstream: replaced scanner framework with SPI,
 * removed upstream-specific dependencies (catalog.h, scanner.h, etc.).
 * All algorithm logic (backoff, crash detection, next_start) preserved.
 */
#include <postgres.h>

#include <access/xact.h>
#include <executor/spi.h>
#include <utils/snapmgr.h>
#include <math.h>
#include <pgstat.h>
#include <stdlib.h>
#include <utils/builtins.h>
#include <utils/fmgrprotos.h>
#include <utils/resowner.h>
#include <utils/timestamp.h>


#include "job_stat.h"
#include "job_stat_history.h"
#include "scheduler.h"		/* guc_bgw_log_level / bgw_shutdown_flag */
#include "../include/bgw/timer.h"

#define MAX_INTERVALS_BACKOFF 5
#define MAX_FAILURES_MULTIPLIER 20
#define MIN_WAIT_AFTER_CRASH_MS (5 * 60 * 1000)

/* Bit manipulation helper (replace upstream utils.h) */
#define ts_flags_are_set_32(flags, bit)		(((flags) & (bit)) != 0)

/*
 * SPI helpers: parse a bgw_job_stat row from SPI result.
 */
static BgwJobStat *
bgw_job_stat_from_spi(MemoryContext mctx)
{
	MemoryContext oldcxt = MemoryContextSwitchTo(mctx);
	BgwJobStat *stat = palloc0(sizeof(BgwJobStat));
	TupleDesc tupdesc = SPI_tuptable->tupdesc;
	HeapTuple tuple = SPI_tuptable->vals[0];
	bool isnull;

	stat->fd.id = DatumGetInt32(SPI_getbinval(tuple, tupdesc,
						SPI_fnumber(tupdesc, "job_id"), &isnull));
	stat->fd.last_start = DatumGetTimestampTz(SPI_getbinval(tuple, tupdesc,
						SPI_fnumber(tupdesc, "last_start"), &isnull));
	if (isnull) stat->fd.last_start = DT_NOBEGIN;
	stat->fd.last_finish = DatumGetTimestampTz(SPI_getbinval(tuple, tupdesc,
						SPI_fnumber(tupdesc, "last_finish"), &isnull));
	if (isnull) stat->fd.last_finish = DT_NOBEGIN;
	stat->fd.next_start = DatumGetTimestampTz(SPI_getbinval(tuple, tupdesc,
						SPI_fnumber(tupdesc, "next_start"), &isnull));
	if (isnull) stat->fd.next_start = DT_NOBEGIN;
	stat->fd.last_successful_finish = DatumGetTimestampTz(
		SPI_getbinval(tuple, tupdesc,
					  SPI_fnumber(tupdesc, "last_successful_finish"),
					  &isnull));
	if (isnull) stat->fd.last_successful_finish = DT_NOBEGIN;
	stat->fd.last_run_success = DatumGetBool(SPI_getbinval(tuple, tupdesc,
						SPI_fnumber(tupdesc, "last_run_success"), &isnull));
	stat->fd.total_runs = DatumGetInt64(SPI_getbinval(tuple, tupdesc,
						SPI_fnumber(tupdesc, "total_runs"), &isnull));
	stat->fd.total_successes = DatumGetInt64(SPI_getbinval(tuple, tupdesc,
						SPI_fnumber(tupdesc, "total_successes"), &isnull));
	stat->fd.total_failures = DatumGetInt64(SPI_getbinval(tuple, tupdesc,
						SPI_fnumber(tupdesc, "total_failures"), &isnull));
	stat->fd.total_crashes = DatumGetInt64(SPI_getbinval(tuple, tupdesc,
						SPI_fnumber(tupdesc, "total_crashes"), &isnull));
	stat->fd.consecutive_failures = DatumGetInt32(SPI_getbinval(tuple, tupdesc,
						SPI_fnumber(tupdesc, "consecutive_failures"), &isnull));
	stat->fd.consecutive_crashes = DatumGetInt32(SPI_getbinval(tuple, tupdesc,
						SPI_fnumber(tupdesc, "consecutive_crashes"), &isnull));
	stat->fd.flags = DatumGetInt32(SPI_getbinval(tuple, tupdesc,
						SPI_fnumber(tupdesc, "flags"), &isnull));
	if (isnull) stat->fd.flags = 0;

	/* total_duration and total_duration_failures are Interval types */
	{
		Datum d = SPI_getbinval(tuple, tupdesc,
					SPI_fnumber(tupdesc, "total_duration"), &isnull);
		if (!isnull)
			stat->fd.total_duration = *DatumGetIntervalP(d);
		d = SPI_getbinval(tuple, tupdesc,
					SPI_fnumber(tupdesc, "total_duration_failures"), &isnull);
		if (!isnull)
			stat->fd.total_duration_failures = *DatumGetIntervalP(d);
	}

	MemoryContextSwitchTo(oldcxt);
	return stat;
}

/*
 * SPI-based data access (replaces scanner callbacks).
 */

BgwJobStat *
bgw_job_stat_find(int32 bgw_job_id)
{
	BgwJobStat *stat = NULL;
	int ret;
	/*
	 * IMPORTANT: capture caller's MemoryContext BEFORE entering SPI.
	 *
	 * SPI_connect() switches CurrentMemoryContext into
	 * SPI's procCxt.  If we passed CurrentMemoryContext to
	 * bgw_job_stat_from_spi(), the BgwJobStat would be palloc'd inside SPI's
	 * procCxt, and SPI_finish() would then delete that
	 * context -- leaving us with a dangling pointer.
	 *
	 * The dangling read manifests as stat->fd.last_start coming back as
	 * garbage (often a near-INT64_MAX value), which then causes
	 * "now - last_start" in mark_end() to underflow into a near-INT64_MIN
	 * interval.  That negative interval poisons total_duration_failures,
	 * and the next mark_end() add overflows int32 day -> ERROR
	 * "interval out of range".
	 */
	MemoryContext caller_cxt = CurrentMemoryContext;

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	/* read_only=false: ensure SPI uses a fresh snapshot rather than
	 * sharing the outer command's snapshot.  In the BGW worker path
	 * the outer transaction's initial snapshot was taken BEFORE the
	 * scheduler's mark_start UPDATE on bgw_job_stat became visible;
	 * a read-only SPI would then see "0 rows" and bgw_job_stat_find
	 * returns NULL even though the row was committed. */
	ret = SPI_execute_with_args(
		"SELECT * FROM " BGW_JOB_STAT_TABLE_FQ " WHERE job_id = $1",
		1,
		(Oid[]){ INT4OID },
		(Datum[]){ Int32GetDatum(bgw_job_id) },
		NULL, false, 1);

	if (ret == SPI_OK_SELECT && SPI_processed > 0)
		stat = bgw_job_stat_from_spi(caller_cxt);

	PopActiveSnapshot();
	SPI_finish();
	return stat;
}

static void
bgw_job_stat_spi_insert(int32 bgw_job_id, bool mark_start,
						TimestampTz next_start)
{
	TimestampTz now = timer_get_current_timestamp();

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	/* ON CONFLICT DO NOTHING: defends against the (theoretical) race
	 * where mark_start sees no row in its UPDATE-first probe but the
	 * INSERT then collides with a row another writer just committed --
	 * advisory locks normally prevent two workers running the same
	 * job concurrently, but a manually-inserted stat row (e.g. test
	 * setup) or extension drop/recreate can produce the same outcome.
	 * Without this clause the worker would die with a PK violation. */
	SPI_execute_with_args(
		"INSERT INTO " BGW_JOB_STAT_TABLE_FQ
		" (job_id, last_start, last_finish, next_start, last_successful_finish,"
		"  last_run_success, total_runs, total_duration, total_duration_failures,"
		"  total_successes, total_failures, total_crashes,"
		"  consecutive_failures, consecutive_crashes, flags)"
		" VALUES ($1, $2, '-infinity', $3, '-infinity',"
		"  $4, $5, '0'::interval, '0'::interval,"
		"  0, 0, $6, 0, $7, 0)"
		" ON CONFLICT (job_id) DO NOTHING",
		7,
		(Oid[]){ INT4OID, TIMESTAMPTZOID, TIMESTAMPTZOID, BOOLOID,
				 INT8OID, INT8OID, INT4OID },
		(Datum[]){
			Int32GetDatum(bgw_job_id),
			TimestampTzGetDatum(mark_start ? now : DT_NOBEGIN),
			TimestampTzGetDatum(next_start),
			BoolGetDatum(!mark_start),	/* last_run_success */
			Int64GetDatum(mark_start ? 1 : 0),	/* total_runs */
			Int64GetDatum(mark_start ? 1 : 0),	/* total_crashes */
			Int32GetDatum(mark_start ? 1 : 0),	/* consecutive_crashes */
		},
		NULL, false, 0);
	PopActiveSnapshot();
	SPI_finish();
}

/*
 * Fixed schedule slot alignment.
 *
 * Uses SPI to call time_series.time_bucket() for alignment, replacing
 * upstream DirectFunctionCall to timestamptz_bucket.
 */
static TimestampTz
spi_time_bucket(Interval *width, TimestampTz ts)
{
	TimestampTz result;
	int ret;

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	ret = SPI_execute_with_args(
		"SELECT time_series.time_bucket($1, $2)",
		2,
		(Oid[]){ INTERVALOID, TIMESTAMPTZOID },
		(Datum[]){ IntervalPGetDatum(width), TimestampTzGetDatum(ts) },
		NULL, true, 1);

	if (ret != SPI_OK_SELECT || SPI_processed == 0)
		elog(ERROR, "time_bucket SPI call failed");

	result = DatumGetTimestampTz(SPI_getbinval(SPI_tuptable->vals[0],
											   SPI_tuptable->tupdesc, 1, &(bool){false}));
	PopActiveSnapshot();
	SPI_finish();
	return result;
}

static TimestampTz
spi_time_bucket_origin(Interval *width, TimestampTz ts, TimestampTz origin)
{
	TimestampTz result;
	int ret;

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	ret = SPI_execute_with_args(
		"SELECT time_series.time_bucket($1, $2, $3)",
		3,
		(Oid[]){ INTERVALOID, TIMESTAMPTZOID, TIMESTAMPTZOID },
		(Datum[]){ IntervalPGetDatum(width), TimestampTzGetDatum(ts),
				   TimestampTzGetDatum(origin) },
		NULL, true, 1);

	if (ret != SPI_OK_SELECT || SPI_processed == 0)
		elog(ERROR, "time_bucket SPI call failed");

	result = DatumGetTimestampTz(SPI_getbinval(SPI_tuptable->vals[0],
											   SPI_tuptable->tupdesc, 1, &(bool){false}));
	PopActiveSnapshot();
	SPI_finish();
	return result;
}

static TimestampTz
spi_time_bucket_tz(Interval *width, TimestampTz ts, const char *tz)
{
	TimestampTz result;
	int ret;

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	ret = SPI_execute_with_args(
		"SELECT time_series.time_bucket($1, $2, $3::text)",
		3,
		(Oid[]){ INTERVALOID, TIMESTAMPTZOID, TEXTOID },
		(Datum[]){ IntervalPGetDatum(width), TimestampTzGetDatum(ts),
				   CStringGetTextDatum(tz) },
		NULL, true, 1);

	if (ret != SPI_OK_SELECT || SPI_processed == 0)
		elog(ERROR, "time_bucket SPI call failed");

	result = DatumGetTimestampTz(SPI_getbinval(SPI_tuptable->vals[0],
											   SPI_tuptable->tupdesc, 1, &(bool){false}));
	PopActiveSnapshot();
	SPI_finish();
	return result;
}

static TimestampTz
spi_time_bucket_tz_origin(Interval *width, TimestampTz ts, const char *tz,
						  TimestampTz origin)
{
	TimestampTz result;
	int ret;

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	ret = SPI_execute_with_args(
		"SELECT time_series.time_bucket($1, $2, $3::text, $4)",
		4,
		(Oid[]){ INTERVALOID, TIMESTAMPTZOID, TEXTOID, TIMESTAMPTZOID },
		(Datum[]){ IntervalPGetDatum(width), TimestampTzGetDatum(ts),
				   CStringGetTextDatum(tz), TimestampTzGetDatum(origin) },
		NULL, true, 1);

	if (ret != SPI_OK_SELECT || SPI_processed == 0)
		elog(ERROR, "time_bucket SPI call failed");

	result = DatumGetTimestampTz(SPI_getbinval(SPI_tuptable->vals[0],
											   SPI_tuptable->tupdesc, 1, &(bool){false}));
	PopActiveSnapshot();
	SPI_finish();
	return result;
}

/*
 * Algorithm functions -- preserved from upstream (Apache 2.0).
 */

/*
 * Compute the next scheduled execution slot for fixed-schedule jobs.
 * Uses time_bucket to align with initial_start + N * schedule_interval.
 */
TimestampTz
get_next_scheduled_execution_slot(BgwJob *job, TimestampTz finish_time)
{
	Datum schedint_datum = IntervalPGetDatum(&job->fd.schedule_interval);
	TimestampTz timebucket_fini, timebucket_init, result;

	Assert(job->fd.fixed_schedule == true);

	Interval one_month = { .day = 0, .time = 0, .month = 1 };

	if (job->fd.schedule_interval.month > 0)
	{
		if (job->fd.timezone == NULL)
		{
			timebucket_init = spi_time_bucket(&job->fd.schedule_interval,
											  job->fd.initial_start);
			timebucket_fini = spi_time_bucket(&job->fd.schedule_interval,
											  finish_time);
		}
		else
		{
			char *tz = text_to_cstring(job->fd.timezone);
			timebucket_fini = spi_time_bucket_tz(&job->fd.schedule_interval,
												 finish_time, tz);
			timebucket_init = spi_time_bucket_tz(&job->fd.schedule_interval,
												 job->fd.initial_start, tz);
		}
		/* always the next bucket */
		timebucket_fini = DatumGetTimestampTz(
			DirectFunctionCall2(timestamptz_pl_interval,
							   TimestampTzGetDatum(timebucket_fini),
							   schedint_datum));
		/* get the number of months between them */
		Datum year_init = DirectFunctionCall2(timestamptz_part,
			CStringGetTextDatum("year"), TimestampTzGetDatum(timebucket_init));
		Datum year_fini = DirectFunctionCall2(timestamptz_part,
			CStringGetTextDatum("year"), TimestampTzGetDatum(timebucket_fini));
		Datum month_init = DirectFunctionCall2(timestamptz_part,
			CStringGetTextDatum("month"), TimestampTzGetDatum(timebucket_init));
		Datum month_fini = DirectFunctionCall2(timestamptz_part,
			CStringGetTextDatum("month"), TimestampTzGetDatum(timebucket_fini));

		float8		month_diff =
			(DatumGetFloat8(year_fini) * 12) + DatumGetFloat8(month_fini) -
			((DatumGetFloat8(year_init) * 12) + DatumGetFloat8(month_init));

		Datum months_to_add = DirectFunctionCall2(interval_mul,
			IntervalPGetDatum(&one_month), Float8GetDatum(month_diff));

		result = DatumGetTimestampTz(
			DirectFunctionCall2(timestamptz_pl_interval,
							   TimestampTzGetDatum(job->fd.initial_start),
							   months_to_add));
	}
	else
	{
		if (job->fd.timezone == NULL)
		{
			result = spi_time_bucket_origin(&job->fd.schedule_interval,
											finish_time, job->fd.initial_start);
		}
		else
		{
			char *tz = text_to_cstring(job->fd.timezone);
			result = spi_time_bucket_tz_origin(&job->fd.schedule_interval,
											   finish_time, tz,
											   job->fd.initial_start);
		}
	}
	while (result <= finish_time)
	{
		result = DatumGetTimestampTz(
			DirectFunctionCall2(timestamptz_pl_interval,
							   TimestampTzGetDatum(result),
							   schedint_datum));
	}
	return result;
}

static TimestampTz
calculate_next_start_on_success_fixed(TimestampTz finish_time, BgwJob *job)
{
	return get_next_scheduled_execution_slot(job, finish_time);
}

static TimestampTz
calculate_next_start_on_success_drifting(TimestampTz last_finish, BgwJob *job)
{
	return DatumGetTimestampTz(
		DirectFunctionCall2(timestamptz_pl_interval,
						   TimestampTzGetDatum(last_finish),
						   IntervalPGetDatum(&job->fd.schedule_interval)));
}

static TimestampTz
calculate_next_start_on_success(TimestampTz finish_time, BgwJob *job)
{
	TimestampTz last_finish = finish_time;
	if (!IS_VALID_TIMESTAMP(finish_time))
		last_finish = timer_get_current_timestamp();

	if (job->fd.fixed_schedule)
		return calculate_next_start_on_success_fixed(last_finish, job);
	else
		return calculate_next_start_on_success_drifting(last_finish, job);
}

static float8
calculate_jitter_percent()
{
	/* returns a number in the range [-0.125, 0.125] */
	uint8 percent = rand();
	return ldexp((double) (16 - (int) (percent % 32)), -7);
}

/*
 * Failure backoff: retry_period * min(consecutive, 20),
 * capped at 5 * schedule_interval, with +/-12.5% jitter.
 */
static TimestampTz
calculate_next_start_on_failure(TimestampTz finish_time,
								int consecutive_failures,
								BgwJob *job,
								bool launch_failure)
{
	float8 jitter = calculate_jitter_percent();

	volatile TimestampTz res = 0;
	volatile bool res_set = false;
	volatile TimestampTz last_finish = finish_time;

	float8 multiplier = (consecutive_failures > MAX_FAILURES_MULTIPLIER ?
						 MAX_FAILURES_MULTIPLIER : consecutive_failures);
	Assert(consecutive_failures > 0 && multiplier < 63);

	MemoryContext oldctx = CurrentMemoryContext;
	ResourceOwner oldowner = CurrentResourceOwner;
	int64 max_slots = (INT64CONST(1) << (int64) multiplier) - INT64CONST(1);
	int64 rand_backoff = rand() % (max_slots * USECS_PER_SEC);

	if (!IS_VALID_TIMESTAMP(finish_time))
	{
		elog(LOG, "%s: invalid finish time", __func__);
		last_finish = timer_get_current_timestamp();
	}

	PG_TRY();
	{
		Datum ival;
		Datum ival_max;
		Interval interval_max = { .time = 60000000 };
		Interval retry_ival = { .time = 2000000 };
		retry_ival.time += rand_backoff;

		BeginInternalSubTransaction("next start on failure");

		if (launch_failure)
		{
			ival = IntervalPGetDatum(&retry_ival);
			ival_max = IntervalPGetDatum(&interval_max);
		}
		else
		{
			ival = DirectFunctionCall2(interval_mul,
				IntervalPGetDatum(&job->fd.retry_period),
				Float8GetDatum(multiplier));
			ival_max = DirectFunctionCall2(interval_mul,
				IntervalPGetDatum(&job->fd.schedule_interval),
				Float8GetDatum(MAX_INTERVALS_BACKOFF));
		}

		if (DatumGetInt32(DirectFunctionCall2(interval_cmp, ival, ival_max)) > 0)
			ival = ival_max;

		/* jitter +/-12.5% */
		ival = DirectFunctionCall2(interval_mul, ival,
								   Float8GetDatum(1.0 + jitter));

		res = DatumGetTimestampTz(
			DirectFunctionCall2(timestamptz_pl_interval,
							   TimestampTzGetDatum(last_finish), ival));
		res_set = true;
		ReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(oldctx);
		CurrentResourceOwner = oldowner;
	}
	PG_CATCH();
	{
		RollbackAndReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(oldctx);
		CurrentResourceOwner = oldowner;
		ErrorData *errdata = CopyErrorData();
		ereport(LOG,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not calculate next start on failure: resetting value"),
				 errdetail("Error: %s.", errdata->message)));
		FlushErrorState();
	}
	PG_END_TRY();
	Assert(CurrentMemoryContext == oldctx);

	if (!res_set)
	{
		TimestampTz nowt = timer_get_current_timestamp();
		res = DatumGetTimestampTz(
			DirectFunctionCall2(timestamptz_pl_interval,
							   TimestampTzGetDatum(nowt),
							   IntervalPGetDatum(&job->fd.retry_period)));
	}

	if (job->fd.fixed_schedule)
	{
		TimestampTz next_slot = get_next_scheduled_execution_slot(job, finish_time);
		if (res > next_slot)
			res = next_slot;
	}
	return res;
}

static TimestampTz
calculate_next_start_on_failed_launch(int consecutive_failed_launches,
									  BgwJob *job)
{
	TimestampTz now = timer_get_current_timestamp();

	return calculate_next_start_on_failure(now, consecutive_failed_launches,
										   job, true);
}

static TimestampTz
calculate_next_start_on_crash(int consecutive_crashes, BgwJob *job)
{
	TimestampTz now = timer_get_current_timestamp();
	TimestampTz failure_calc =
		calculate_next_start_on_failure(now, consecutive_crashes, job, false);
	TimestampTz min_time = TimestampTzPlusMilliseconds(now,
													   MIN_WAIT_AFTER_CRASH_MS);

	if (min_time > failure_calc)
		return min_time;
	return failure_calc;
}

/*
 * SPI-based mark_start / mark_end (replaces scanner callbacks).
 */

void
bgw_job_stat_mark_start(BgwJob *job)
{
	int ret;

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	/* Try UPDATE first */
	ret = SPI_execute_with_args(
		"UPDATE " BGW_JOB_STAT_TABLE_FQ " SET "
		"  last_start = now(),"
		"  last_finish = '-infinity',"
		"  next_start = '-infinity',"
		"  total_runs = total_runs + 1,"
		"  last_run_success = false,"
		"  total_crashes = total_crashes + 1,"
		"  consecutive_crashes = consecutive_crashes + 1,"
		"  flags = flags & ~$2"
		" WHERE job_id = $1",
		2,
		(Oid[]){ INT4OID, INT4OID },
		(Datum[]){ Int32GetDatum(job->fd.id), Int32GetDatum(LAST_CRASH_REPORTED) },
		NULL, false, 0);

	if (SPI_processed == 0)
	{
		/* Row doesn't exist yet -- INSERT */
		PopActiveSnapshot();
	SPI_finish();
		bgw_job_stat_spi_insert(job->fd.id, true, DT_NOBEGIN);
	}
	else
	{
		PopActiveSnapshot();
	SPI_finish();
	}

	job->job_history.execution_start = timer_get_current_timestamp();
	job->job_history.id = INVALID_BGW_JOB_STAT_HISTORY_ID;

	bgw_job_stat_history_update(JOB_STAT_HISTORY_UPDATE_START, job,
								JOB_SUCCESS, NULL);

	pgstat_report_activity(STATE_IDLE, NULL);
}

void
bgw_job_stat_mark_end(BgwJob *job, JobResult result, Jsonb *edata)
{
	TimestampTz now = timer_get_current_timestamp();
	TimestampTz next;

	/*
	 * We need to know current stat values to compute next_start,
	 * so SELECT first, then UPDATE.
	 */
	BgwJobStat *stat = bgw_job_stat_find(job->fd.id);
	if (stat == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("unable to find job statistics for job %d", job->fd.id)));

	/*
	 * Compute duration = now - last_start.
	 *
	 * Defensive: if last_start is somehow not finite (epoch 0, +/-infinity)
	 * or in the future, fall back to a zero interval rather than computing
	 * a garbage value that would later overflow when accumulated into
	 * total_duration_failures (interval->day is int32, so a few negative
	 * 1e8-day rows quickly hit "interval out of range" on the next add).
	 *
	 * This protects the catalog from poisoning when a transient state
	 * issue (e.g. failed mark_start visibility, snapshot anomaly) causes
	 * stat->fd.last_start to come back as DT_NOBEGIN/NOEND or 0.
	 */
	Interval *duration;
	if (TIMESTAMP_NOT_FINITE(stat->fd.last_start) ||
		stat->fd.last_start <= 0 ||
		stat->fd.last_start > now)
	{
		elog(WARNING,
			 "BGW job %d mark_end: invalid last_start (now=" INT64_FORMAT
			 ", last_start=" INT64_FORMAT "); using zero duration",
			 job->fd.id, (int64) now, (int64) stat->fd.last_start);
		duration = (Interval *) palloc0(sizeof(Interval));
	}
	else
	{
		duration = DatumGetIntervalP(
			DirectFunctionCall2(timestamp_mi,
							   TimestampTzGetDatum(now),
							   TimestampTzGetDatum(stat->fd.last_start)));
	}

	if (result == JOB_SUCCESS)
	{
		next = calculate_next_start_on_success(now, job);

		SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
		SPI_execute_with_args(
			"UPDATE " BGW_JOB_STAT_TABLE_FQ " SET "
			"  last_finish = $2,"
			"  last_run_success = true,"
			"  total_crashes = total_crashes - 1,"
			"  consecutive_crashes = 0,"
			"  flags = flags & ~$6,"
			"  total_successes = total_successes + 1,"
			"  consecutive_failures = 0,"
			"  last_successful_finish = $2,"
			"  total_duration = total_duration + $3,"
			"  next_start = CASE WHEN next_start = '-infinity'"
			"    THEN $4 ELSE next_start END"
			" WHERE job_id = $1",
			6,
			(Oid[]){ INT4OID, TIMESTAMPTZOID, INTERVALOID, TIMESTAMPTZOID,
					 INT4OID, INT4OID },
			(Datum[]){
				Int32GetDatum(job->fd.id),
				TimestampTzGetDatum(now),
				IntervalPGetDatum(duration),
				TimestampTzGetDatum(next),
				Int32GetDatum(0), /* unused placeholder */
				Int32GetDatum(LAST_CRASH_REPORTED),
			},
			NULL, false, 0);
		PopActiveSnapshot();
	SPI_finish();
	}
	else
	{
		int consec = stat->fd.consecutive_failures + 1;
		next = (result != JOB_FAILURE_TO_START) ?
			calculate_next_start_on_failure(now, consec, job, false) : DT_NOBEGIN;

		SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
		SPI_execute_with_args(
			"UPDATE " BGW_JOB_STAT_TABLE_FQ " SET "
			"  last_finish = $2,"
			"  last_run_success = false,"
			"  total_crashes = total_crashes - 1,"
			"  consecutive_crashes = 0,"
			"  flags = flags & ~$5,"
			"  total_failures = total_failures + 1,"
			"  consecutive_failures = consecutive_failures + 1,"
			"  total_duration_failures = total_duration_failures + $3,"
			"  next_start = CASE WHEN next_start = '-infinity' AND $6 != 0"
			"    THEN $4 ELSE next_start END"
			" WHERE job_id = $1",
			6,
			(Oid[]){ INT4OID, TIMESTAMPTZOID, INTERVALOID, TIMESTAMPTZOID,
					 INT4OID, INT4OID },
			(Datum[]){
				Int32GetDatum(job->fd.id),
				TimestampTzGetDatum(now),
				IntervalPGetDatum(duration),
				TimestampTzGetDatum(next),
				Int32GetDatum(LAST_CRASH_REPORTED),
				Int32GetDatum(result != JOB_FAILURE_TO_START ? 1 : 0),
			},
			NULL, false, 0);
		PopActiveSnapshot();
	SPI_finish();
	}

	bgw_job_stat_history_update(JOB_STAT_HISTORY_UPDATE_END, job, result, edata);
	pgstat_report_activity(STATE_IDLE, NULL);
}

/*
 * Called exactly once per crash observation (gated by LAST_CRASH_REPORTED
 * in bgw_job_stat_next_start).  Persists two things in the SAME UPDATE:
 *
 *   flags |= LAST_CRASH_REPORTED
 *       Latches "we have seen this crash", so the next_start path
 *       never re-enters this function for the same crash streak and
 *       falls through to reading the row's next_start column instead.
 *
 *   next_start = <caller-computed retry anchor>
 *       Fixes the retry deadline to a MOMENT (the timestamp the caller
 *       captured just before this call), not a moving 'now + backoff'
 *       target.  See bgw_job_stat_next_start for the livelock this
 *       replaces and doc/bugs/bgw-crashed-job-retry-livelock.md for
 *       the full incident report.
 *
 * Doing both writes together (rather than a second UPDATE from the
 * caller) preserves the invariant "if LAST_CRASH_REPORTED is set then
 * next_start is the persisted anchor" against concurrent readers that
 * might otherwise see the flag set but next_start still at its
 * mark_start default of -infinity.
 */
void
bgw_job_stat_mark_crash_reported(BgwJob *job, JobResult result,
								  TimestampTz next_start)
{
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	SPI_execute_with_args(
		"UPDATE " BGW_JOB_STAT_TABLE_FQ
		" SET flags = flags | $2, next_start = $3"
		" WHERE job_id = $1",
		3,
		(Oid[]){ INT4OID, INT4OID, TIMESTAMPTZOID },
		(Datum[]){ Int32GetDatum(job->fd.id),
					Int32GetDatum(LAST_CRASH_REPORTED),
					TimestampTzGetDatum(next_start) },
		NULL, false, 0);
	PopActiveSnapshot();
	SPI_finish();

	bgw_job_stat_history_update(JOB_STAT_HISTORY_UPDATE_END, job, result, NULL);
	pgstat_report_activity(STATE_IDLE, NULL);
}

bool
bgw_job_stat_end_was_marked(BgwJobStat *jobstat)
{
	return !TIMESTAMP_IS_NOBEGIN(jobstat->fd.last_finish);
}

void
bgw_job_stat_set_next_start(int32 job_id, TimestampTz next_start)
{
	int ret;

	if (next_start == DT_NOBEGIN)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot set next start to -infinity")));

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	ret = SPI_execute_with_args(
		"UPDATE " BGW_JOB_STAT_TABLE_FQ
		" SET next_start = $2 WHERE job_id = $1",
		2,
		(Oid[]){ INT4OID, TIMESTAMPTZOID },
		(Datum[]){ Int32GetDatum(job_id), TimestampTzGetDatum(next_start) },
		NULL, false, 0);
	PopActiveSnapshot();
	SPI_finish();

	/*
	 * SPI_processed == 0 means the bgw_job_stat row was concurrently
	 * DELETE'd (typically by remove_continuous_aggregate_policy /
	 * remove_compression_policy / DROP MATERIALIZED VIEW CASCADE) between
	 * the caller's existence check and this UPDATE.  Both callers
	 * (scheduler.c on_failure_to_start_job, job.c
	 * bgw_job_run_and_set_next_start) hold a share lock on bgw_job, not
	 * on bgw_job_stat, so under Read Committed this UPDATE legitimately
	 * sees the row as gone even when bgw_job is still locked from the
	 * caller's perspective.
	 *
	 * A missing stat row means the job is being torn down -- there is no
	 * scheduling to advance.  Re-raising ERROR escalates to the
	 * scheduler process exiting (the on_failure_to_start_job path runs
	 * outside any PG_TRY in the scheduler main loop), which terminates
	 * every in-flight refresh worker the scheduler owns and stalls CAGG
	 * refresh for 10-20 minutes.  Downgrade to WARNING and return; the
	 * caller's no-op is the right behavior, and the WARNING preserves
	 * observability without taking the scheduler down with it.
	 */
	if (SPI_processed == 0)
		ereport(WARNING,
				(errcode(ERRCODE_NO_DATA_FOUND),
				 errmsg("bgw_job_stat row for job %d is missing -- "
						"concurrent policy removal, skipping next_start update",
						job_id)));
}

bool
bgw_job_stat_update_next_start(int32 job_id, TimestampTz next_start,
								  bool allow_unset)
{
	int ret;

	if (!allow_unset && next_start == DT_NOBEGIN)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot set next start to -infinity")));

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	ret = SPI_execute_with_args(
		"UPDATE " BGW_JOB_STAT_TABLE_FQ
		" SET next_start = $2 WHERE job_id = $1",
		2,
		(Oid[]){ INT4OID, TIMESTAMPTZOID },
		(Datum[]){ Int32GetDatum(job_id), TimestampTzGetDatum(next_start) },
		NULL, false, 0);
	PopActiveSnapshot();
	SPI_finish();

	return (SPI_processed > 0);
}

/*
 * Priority-based next_start calculation:
 * 1. consecutive launch failures -> launch failure backoff
 * 2. never run -> run immediately (DT_NOBEGIN)
 * 3. consecutive crashes -> crash backoff (min 5 min)
 * 4. normal -> use persisted next_start
 */
TimestampTz
bgw_job_stat_next_start(BgwJobStat *jobstat, BgwJob *job,
						   int32 consecutive_failed_launches)
{
	if (consecutive_failed_launches > 0)
		return calculate_next_start_on_failed_launch(
			consecutive_failed_launches, job);

	if (jobstat == NULL)
		return DT_NOBEGIN;		/* Never previously run -- run right away */

	if (jobstat->fd.consecutive_crashes > 0)
	{
		/*
		 * FIRST observation of the crash: compute the retry anchor
		 * (now + max(MIN_WAIT_AFTER_CRASH_MS, failure_backoff), plus
		 * any fixed_schedule slot alignment) exactly ONCE and persist
		 * it into bgw_job_stat.next_start along with the
		 * LAST_CRASH_REPORTED flag.
		 *
		 * Previously calculate_next_start_on_crash was called on every
		 * entry here — and this function is re-entered by the
		 * scheduler every time the job list rebuilds, which under a
		 * busy relcache-invalidation stream is every few seconds.
		 * Recomputing 'now + 5 min' each time made the retry deadline
		 * slide forward faster than wall-clock time advanced, so the
		 * crashed job was never dispatched (SOAK-20260723_170208
		 * cv_1hour frozen 8.9 h; see
		 * doc/bugs/bgw-crashed-job-retry-livelock.md).
		 *
		 * Anchoring on first sight ties the deadline to a fixed past
		 * moment; the else branch below reads that persisted value
		 * from the row, so subsequent rebuilds cannot move the target.
		 * A successful run clears consecutive_crashes and the flag,
		 * so a later crash starts a fresh anchor from that later now.
		 *
		 * Regression: test/regress/sql/bgw_scheduler.sql DB-SCHED-14.
		 */
		if (!ts_flags_are_set_32(jobstat->fd.flags, LAST_CRASH_REPORTED))
		{
			TimestampTz anchor =
				calculate_next_start_on_crash(jobstat->fd.consecutive_crashes, job);
			bgw_job_stat_mark_crash_reported(job, JOB_FAILURE_IN_EXECUTION, anchor);
			return anchor;
		}

		return jobstat->fd.next_start;
	}

	return jobstat->fd.next_start;
}

/*
 * Test-only API: invoke bgw_job_stat_set_next_start from SQL.
 *
 * set_next_start is otherwise reachable only from forked BGW worker code
 * paths (scheduler on_failure_to_start_job and the BGW worker
 * entrypoint's bgw_job_run_and_set_next_start), so a SQL-callable shim
 * is the cleanest way to exercise the "stat row missing" defense
 * deterministically from a regression test.
 *
 * NOT registered in time_series--1.0.sql.  The isolation2 setup.sql
 * does the CREATE FUNCTION for this symbol so it never lands in a
 * user-installed catalog.  Mirrors the pattern around
 * bgw_job_set_scheduler_test_hook / bgw_job_set_job_entrypoint_function_name.
 */
PG_FUNCTION_INFO_V1(ts_test_bgw_job_stat_set_next_start);
Datum
ts_test_bgw_job_stat_set_next_start(PG_FUNCTION_ARGS)
{
	int32       job_id     = PG_GETARG_INT32(0);
	TimestampTz next_start = PG_GETARG_TIMESTAMPTZ(1);

	bgw_job_stat_set_next_start(job_id, next_start);
	PG_RETURN_VOID();
}
