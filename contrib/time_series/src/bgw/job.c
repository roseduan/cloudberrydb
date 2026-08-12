/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 *
 * Portions Copyright (c) 2025-2026, HashData Technology Limited.
 *
 * Adapted from upstream: replaced scanner framework with SPI,
 * removed cross-module dispatch, added CBDB Gp_role support.
 */
#include <postgres.h>

#include <unistd.h>
#include <access/xact.h>
#include <catalog/pg_authid.h>
#include <executor/executor.h>
#include <executor/spi.h>
#include <miscadmin.h>
#include <nodes/makefuncs.h>
#include <parser/parse_func.h>
#include <pgstat.h>
#include <postmaster/bgworker.h>
#include <storage/ipc.h>
#include <storage/lock.h>
#include <storage/proc.h>
#include <storage/procarray.h>
#include <storage/sinvaladt.h>
#include <tcop/pquery.h>
#include <tcop/tcopprot.h>
#include <utils/acl.h>
#include <commands/defrem.h>
#include <tcop/dest.h>
#include <utils/portal.h>
#include <utils/resscheduler.h>
#include <utils/builtins.h>
#include <utils/inval.h>		/* CacheInvalidateRelcacheByRelid */

/* ts_pax_register_per_backend_exit_capture */
#include "../include/compress/ts_compress.h"
#include <utils/jsonb.h>
#include <utils/lsyscache.h>
#include <utils/memutils.h>
#include <utils/snapmgr.h>
#include <utils/syscache.h>
#include <utils/timestamp.h>
#include <portability/instr_time.h>


#include "cdb/cdbvars.h"		/* Gp_role / GP_ROLE_DISPATCH */

#include "../include/time_series.h"
#include "../include/cagg/cagg.h"	/* cagg_refresh() */
#include "utils.h"
#include "job.h"
#include "job_stat.h"
#include "job_stat_history.h"
#include "scheduler.h"
#include "../include/bgw/timer.h"

static scheduler_test_hook_type scheduler_test_hook = NULL;
static char *job_entrypoint_function_name = "bgw_job_entrypoint";

/*
 * Global flag set by bgw_run_job() to indicate "we are running on the
 * synthetic in-session worker path".  cagg_refresh.c reads this in
 * addition to (ActivePortal == NULL) so it knows to manage snapshots
 * the same way it does in a real BGW worker -- even though we are nested
 * inside a PROCEDURE Portal at the SQL layer.
 *
 * Defined here (not in include/time_series.h) because cagg_refresh.c
 * declares its own extern. Visible to all modules linked into
 * time_series.so.
 */
bool		bgw_synthetic_run = false;

typedef enum JobLockLifetime
{
	SESSION_LOCK = 0,
	TXN_LOCK,
}			JobLockLifetime;

/*
 * SPI helper: extract a BgwJob from an SPI result row
 *
 * Uses SPI_getbinval with column names so we don't depend on
 * column-number constants (Anum_bgw_job_*).
 */
static BgwJob *
bgw_job_from_spi(HeapTuple tuple, TupleDesc tupdesc, MemoryContext mctx)
{
	BgwJob	   *job;
	bool		isnull;
	Datum		val;
	MemoryContext old_ctx;

	old_ctx = MemoryContextSwitchTo(mctx);
	job = (BgwJob *) palloc0(sizeof(BgwJob));

	val = SPI_getbinval(tuple, tupdesc,
						SPI_fnumber(tupdesc, "id"), &isnull);
	if (!isnull)
		job->fd.id = DatumGetInt32(val);

	val = SPI_getbinval(tuple, tupdesc,
						SPI_fnumber(tupdesc, "application_name"), &isnull);
	if (!isnull)
		namestrcpy(&job->fd.application_name, NameStr(*DatumGetName(val)));

	val = SPI_getbinval(tuple, tupdesc,
						SPI_fnumber(tupdesc, "schedule_interval"), &isnull);
	if (!isnull)
		job->fd.schedule_interval = *DatumGetIntervalP(val);

	val = SPI_getbinval(tuple, tupdesc,
						SPI_fnumber(tupdesc, "max_runtime"), &isnull);
	if (!isnull)
		job->fd.max_runtime = *DatumGetIntervalP(val);

	val = SPI_getbinval(tuple, tupdesc,
						SPI_fnumber(tupdesc, "max_retries"), &isnull);
	if (!isnull)
		job->fd.max_retries = DatumGetInt32(val);

	val = SPI_getbinval(tuple, tupdesc,
						SPI_fnumber(tupdesc, "retry_period"), &isnull);
	if (!isnull)
		job->fd.retry_period = *DatumGetIntervalP(val);

	val = SPI_getbinval(tuple, tupdesc,
						SPI_fnumber(tupdesc, "proc_schema"), &isnull);
	if (!isnull)
		namestrcpy(&job->fd.proc_schema, NameStr(*DatumGetName(val)));

	val = SPI_getbinval(tuple, tupdesc,
						SPI_fnumber(tupdesc, "proc_name"), &isnull);
	if (!isnull)
		namestrcpy(&job->fd.proc_name, NameStr(*DatumGetName(val)));

	val = SPI_getbinval(tuple, tupdesc,
						SPI_fnumber(tupdesc, "owner"), &isnull);
	if (!isnull)
		job->fd.owner = DatumGetObjectId(val);

	val = SPI_getbinval(tuple, tupdesc,
						SPI_fnumber(tupdesc, "scheduled"), &isnull);
	if (!isnull)
		job->fd.scheduled = DatumGetBool(val);

	val = SPI_getbinval(tuple, tupdesc,
						SPI_fnumber(tupdesc, "fixed_schedule"), &isnull);
	if (!isnull)
		job->fd.fixed_schedule = DatumGetBool(val);

	val = SPI_getbinval(tuple, tupdesc,
						SPI_fnumber(tupdesc, "initial_start"), &isnull);
	if (!isnull)
		job->fd.initial_start = DatumGetTimestampTz(val);
	else
		job->fd.initial_start = DT_NOBEGIN;

	val = SPI_getbinval(tuple, tupdesc,
						SPI_fnumber(tupdesc, "hypertable_id"), &isnull);
	if (!isnull)
		job->fd.hypertable_id = DatumGetInt32(val);
	else
		job->fd.hypertable_id = 0;

	val = SPI_getbinval(tuple, tupdesc,
						SPI_fnumber(tupdesc, "config"), &isnull);
	if (!isnull)
		job->fd.config = DatumGetJsonbPCopy(val);
	else
		job->fd.config = NULL;

	val = SPI_getbinval(tuple, tupdesc,
						SPI_fnumber(tupdesc, "check_schema"), &isnull);
	if (!isnull)
		namestrcpy(&job->fd.check_schema, NameStr(*DatumGetName(val)));

	val = SPI_getbinval(tuple, tupdesc,
						SPI_fnumber(tupdesc, "check_name"), &isnull);
	if (!isnull)
		namestrcpy(&job->fd.check_name, NameStr(*DatumGetName(val)));

	val = SPI_getbinval(tuple, tupdesc,
						SPI_fnumber(tupdesc, "timezone"), &isnull);
	if (!isnull)
		job->fd.timezone = DatumGetTextPCopy(val);
	else
		job->fd.timezone = NULL;

	MemoryContextSwitchTo(old_ctx);
	return job;
}

/*
 * Variant for the scheduler: skip config/check_schema/check_name to avoid
 * detoasting and simplify memory management (scheduler doesn't need them).
 */
static BgwJob *
bgw_job_from_spi_for_scheduler(HeapTuple tuple, TupleDesc tupdesc,
							   size_t alloc_size, MemoryContext mctx)
{
	BgwJob	   *job;
	bool		isnull;
	Datum		val;
	MemoryContext old_ctx;

	Assert(alloc_size >= sizeof(BgwJob));
	old_ctx = MemoryContextSwitchTo(mctx);
	job = (BgwJob *) palloc0(alloc_size);

	val = SPI_getbinval(tuple, tupdesc,
						SPI_fnumber(tupdesc, "id"), &isnull);
	if (!isnull)
		job->fd.id = DatumGetInt32(val);

	val = SPI_getbinval(tuple, tupdesc,
						SPI_fnumber(tupdesc, "application_name"), &isnull);
	if (!isnull)
		namestrcpy(&job->fd.application_name, NameStr(*DatumGetName(val)));

	val = SPI_getbinval(tuple, tupdesc,
						SPI_fnumber(tupdesc, "schedule_interval"), &isnull);
	if (!isnull)
		job->fd.schedule_interval = *DatumGetIntervalP(val);

	val = SPI_getbinval(tuple, tupdesc,
						SPI_fnumber(tupdesc, "max_runtime"), &isnull);
	if (!isnull)
		job->fd.max_runtime = *DatumGetIntervalP(val);

	val = SPI_getbinval(tuple, tupdesc,
						SPI_fnumber(tupdesc, "max_retries"), &isnull);
	if (!isnull)
		job->fd.max_retries = DatumGetInt32(val);

	val = SPI_getbinval(tuple, tupdesc,
						SPI_fnumber(tupdesc, "retry_period"), &isnull);
	if (!isnull)
		job->fd.retry_period = *DatumGetIntervalP(val);

	val = SPI_getbinval(tuple, tupdesc,
						SPI_fnumber(tupdesc, "proc_schema"), &isnull);
	if (!isnull)
		namestrcpy(&job->fd.proc_schema, NameStr(*DatumGetName(val)));

	val = SPI_getbinval(tuple, tupdesc,
						SPI_fnumber(tupdesc, "proc_name"), &isnull);
	if (!isnull)
		namestrcpy(&job->fd.proc_name, NameStr(*DatumGetName(val)));

	val = SPI_getbinval(tuple, tupdesc,
						SPI_fnumber(tupdesc, "owner"), &isnull);
	if (!isnull)
		job->fd.owner = DatumGetObjectId(val);

	val = SPI_getbinval(tuple, tupdesc,
						SPI_fnumber(tupdesc, "scheduled"), &isnull);
	if (!isnull)
		job->fd.scheduled = DatumGetBool(val);

	val = SPI_getbinval(tuple, tupdesc,
						SPI_fnumber(tupdesc, "fixed_schedule"), &isnull);
	if (!isnull)
		job->fd.fixed_schedule = DatumGetBool(val);

	val = SPI_getbinval(tuple, tupdesc,
						SPI_fnumber(tupdesc, "initial_start"), &isnull);
	if (!isnull)
		job->fd.initial_start = DatumGetTimestampTz(val);
	else
		job->fd.initial_start = DT_NOBEGIN;

	val = SPI_getbinval(tuple, tupdesc,
						SPI_fnumber(tupdesc, "hypertable_id"), &isnull);
	if (!isnull)
		job->fd.hypertable_id = DatumGetInt32(val);
	else
		job->fd.hypertable_id = 0;

	/* Scheduler doesn't need config -- skip to avoid detoasting */
	job->fd.config = NULL;

	val = SPI_getbinval(tuple, tupdesc,
						SPI_fnumber(tupdesc, "timezone"), &isnull);
	if (!isnull)
		job->fd.timezone = DatumGetTextPCopy(val);
	else
		job->fd.timezone = NULL;

	MemoryContextSwitchTo(old_ctx);
	return job;
}

/*
 * bgw_job_start -- preserved as-is
 */
BackgroundWorkerHandle *
bgw_job_start(BgwJob * job, Oid user_oid)
{
	BgwParams	bgw_params = {
		.job_id = Int32GetDatum(job->fd.id),
		.job_history_id = job->job_history.id,
		.job_history_execution_start = job->job_history.execution_start,
		.user_oid = user_oid,
	};

	strlcpy(bgw_params.bgw_main, job_entrypoint_function_name,
			sizeof(bgw_params.bgw_main));

	return bgw_start_worker(NameStr(job->fd.application_name), &bgw_params);
}

/*
 * job_execute_function -- preserved as-is
 */
static void
job_execute_function(FuncExpr *funcexpr)
{
	bool		isnull;

	EState	   *estate = CreateExecutorState();
	ExprContext *econtext = CreateExprContext(estate);

	ExprState  *es = ExecPrepareExpr((Expr *) funcexpr, estate);

	ExecEvalExpr(es, econtext, &isnull);
	FreeExprContext(econtext, true);
	FreeExecutorState(estate);
}

/*
 * bgw_job_run_config_check -- preserved as-is
 */
void
bgw_job_run_config_check(Oid check, int32 job_id, Jsonb *config)
{
	/* Nothing to check if there is no check function provided */
	if (!OidIsValid(check))
		return;

	/* NULL config may be valid */
	Const	   *arg;

	if (config == NULL)
		arg = makeNullConst(JSONBOID, -1, InvalidOid);
	else
		arg = makeConst(JSONBOID, -1, InvalidOid, -1,
						JsonbPGetDatum(config), false, false);

	List	   *args = list_make1(arg);
	FuncExpr   *funcexpr =
	makeFuncExpr(check, VOIDOID, args, InvalidOid, InvalidOid,
				 COERCE_EXPLICIT_CALL);

	if (get_func_prokind(check) == PROKIND_FUNCTION)
		job_execute_function(funcexpr);
	else
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("unsupported function type"),
				 errdetail("Only functions are allowed as custom configuration checks"),
				 errhint("Use a FUNCTION instead")));
}

/*
 * job_config_check -- preserved as-is
 */
static void
job_config_check(BgwJob * job, Jsonb *config)
{
	Oid			proc;
	List	   *funcname;

	/* Both should either be empty or contain a schema and name */
	Assert((strlen(NameStr(job->fd.check_schema)) == 0) ==
		   (strlen(NameStr(job->fd.check_name)) == 0));

	/* If there is no function, just return */
	if (strlen(NameStr(job->fd.check_name)) == 0)
		return;

	funcname = list_make2(makeString(NameStr(job->fd.check_schema)),
						  makeString(NameStr(job->fd.check_name)));

	Oid			argtypes[] = {JSONBOID};

	/*
	 * Only functions allowed as custom checks, as procedures can cause errors
	 * with COMMIT statements.
	 */
	proc = LookupFuncName(funcname, 1, argtypes, true);

	/*
	 * a check function has been registered but it can't be found anymore
	 * because it was dropped or renamed. Allow alter_job to run if that's the
	 * case without validating the config but also print a warning
	 */
	if (OidIsValid(proc))
		bgw_job_run_config_check(proc, job->fd.id, config);
	else
		elog(WARNING,
			 "function %s.%s(config jsonb) not found, skipping config validation for "
			 "job %d",
			 NameStr(job->fd.check_schema),
			 NameStr(job->fd.check_name),
			 job->fd.id);
}

/*
 * bgw_lock_job_id -- preserved as-is
 */
bool
bgw_lock_job_id(int32 job_id, LOCKMODE mode, bool session_lock,
				LOCKTAG *tag, bool block)
{
	/*
	 * Use a special pseudo-random field 4 value to avoid conflicting with
	 * user-advisory-locks.
	 */
	SET_LOCKTAG_ADVISORY(*tag, MyDatabaseId, (uint32) job_id, (uint32) 0, 0);

	return LockAcquire(tag, mode, session_lock, !block) != LOCKACQUIRE_NOT_AVAIL;
}

/*
 * bgw_job_find -- SPI rewrite
 */
BgwJob *
bgw_job_find(int32 bgw_job_id, MemoryContext mctx, bool fail_if_not_found)
{
	BgwJob	   *job = NULL;
	int			ret;
	Oid			argtypes[1] = {INT4OID};
	Datum		argvals[1];
	char		argnulls[1] = {' '};

	argvals[0] = Int32GetDatum(bgw_job_id);

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	ret = SPI_execute_with_args("SELECT * FROM " BGW_JOB_TABLE_FQ
								" WHERE id = $1",
								1, argtypes, argvals, argnulls,
								true,	/* read_only */
								0); /* count = all */

	if (ret != SPI_OK_SELECT)
		elog(ERROR, "SPI_execute_with_args failed: error code %d", ret);

	if (SPI_processed > 0)
	{
		HeapTuple	tuple = SPI_tuptable->vals[0];
		TupleDesc	tupdesc = SPI_tuptable->tupdesc;

		job = bgw_job_from_spi(tuple, tupdesc, mctx);
	}

	PopActiveSnapshot();
	SPI_finish();

	if (job == NULL && fail_if_not_found)

		/*
		 * Use ereport(ERROR, errcode+errmsg) instead of elog(ERROR,...): elog
		 * implicitly tags the message as ERRCODE_INTERNAL_ERROR, which makes
		 * PG append "(job.c:NNN)" to the user-visible output.  This is
		 * reached from user-facing run_job(job_id) when the caller passes a
		 * stale or removed id, where exposing our source line is noisy and
		 * the line number drifts on every unrelated edit.  ereport with a
		 * proper SQL errcode keeps the wire-level message clean ("job N not
		 * found").
		 */
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("job %d not found", bgw_job_id)));

	return job;
}

/*
 * bgw_job_find_with_lock -- advisory lock + SPI SELECT
 */
static BgwJob *
bgw_job_find_with_lock(int32 bgw_job_id, MemoryContext mctx,
					   LOCKMODE tuple_lock_mode, JobLockLifetime lock_type,
					   bool block, bool *got_lock)
{
	BgwJob	   *job = NULL;
	LOCKTAG		tag;
	int			ret;
	Oid			argtypes[1] = {INT4OID};
	Datum		argvals[1];
	char		argnulls[1] = {' '};

	/* take advisory lock before relation lock */
	*got_lock = bgw_lock_job_id(bgw_job_id, tuple_lock_mode,
								lock_type == SESSION_LOCK, &tag, block);
	if (!*got_lock)
	{
		/* return NULL if lock could not be acquired */
		Assert(!block);
		return NULL;
	}

	argvals[0] = Int32GetDatum(bgw_job_id);

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	ret = SPI_execute_with_args("SELECT * FROM " BGW_JOB_TABLE_FQ
								" WHERE id = $1",
								1, argtypes, argvals, argnulls,
								true,	/* read_only */
								0);

	if (ret != SPI_OK_SELECT)
		elog(ERROR, "SPI_execute_with_args failed: error code %d", ret);

	if (SPI_processed > 1)
	{
		uint64		i;
		TupleDesc	tupdesc = SPI_tuptable->tupdesc;

		for (i = 0; i < SPI_processed; i++)
		{
			BgwJob	   *j = bgw_job_from_spi(SPI_tuptable->vals[i], tupdesc, mctx);

			ereport(LOG,
					(errmsg("more than one job with same job_id %d",
							bgw_job_id),
					 errdetail("job_id: %d, application_name: %s,"
							   " procedure: %s.%s, scheduled: %s",
							   j->fd.id,
							   NameStr(j->fd.application_name),
							   quote_identifier(NameStr(j->fd.proc_schema)),
							   quote_identifier(NameStr(j->fd.proc_name)),
							   j->fd.scheduled ? "true" : "false")));
		}
	}

	/*
	 * We don't care about duplicate jobs in release builds and will take the
	 * last job
	 */
	Assert(SPI_processed <= 1);

	if (SPI_processed > 0)
	{
		HeapTuple	tuple = SPI_tuptable->vals[SPI_processed - 1];
		TupleDesc	tupdesc = SPI_tuptable->tupdesc;

		job = bgw_job_from_spi(tuple, tupdesc, mctx);
	}

	PopActiveSnapshot();
	SPI_finish();
	return job;
}

/*
 * bgw_job_get_share_lock -- preserved as-is (uses bgw_job_find_with_lock)
 */
bool
bgw_job_get_share_lock(int32 bgw_job_id, MemoryContext mctx)
{
	bool		got_lock;

	/* note the mode here is equivalent to FOR SHARE row locks */
	BgwJob	   *job = bgw_job_find_with_lock(bgw_job_id,
											 mctx,
											 RowShareLock,
											 TXN_LOCK,
											 true,	/* block */
											 &got_lock);

	if (job != NULL)
	{
		if (!got_lock)
		{
			/* since we blocked for a lock, this is an unexpected condition */
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("could not acquire share lock for job=%d", bgw_job_id)));
		}
		pfree(job);
		return true;
	}
	return false;
}

/*
 * bgw_job_get_scheduled -- SPI rewrite
 *
 * The scheduler requires jobs to be sorted by id.
 * We skip config, check_name, check_schema since the scheduler
 * doesn't need these.
 */
List *
bgw_job_get_scheduled(size_t alloc_size, MemoryContext mctx)
{
	List	   *jobs = NIL;
	int			ret;

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	ret = SPI_execute("SELECT * FROM " BGW_JOB_TABLE_FQ
					  " WHERE scheduled = true ORDER BY id",
					  true,		/* read_only */
					  0);

	if (ret != SPI_OK_SELECT)
		elog(ERROR, "SPI_execute failed: error code %d", ret);

	if (SPI_processed > 0)
	{
		uint64		i;
		MemoryContext old_ctx;
		TupleDesc	tupdesc = SPI_tuptable->tupdesc;

		for (i = 0; i < SPI_processed; i++)
		{
			HeapTuple	tuple = SPI_tuptable->vals[i];
			BgwJob	   *job = bgw_job_from_spi_for_scheduler(tuple, tupdesc,
															 alloc_size, mctx);

			old_ctx = MemoryContextSwitchTo(mctx);
			jobs = lappend(jobs, job);
			MemoryContextSwitchTo(old_ctx);
		}
	}

	PopActiveSnapshot();
	SPI_finish();
	return jobs;
}

/*
 * bgw_job_update_by_id -- SPI UPDATE
 *
 * Updates the fields modifiable with alter_job. Also recomputes
 * next_start if schedule_interval changed.
 */
bool
bgw_job_update_by_id(int32 job_id, BgwJob * job)
{
	int			ret;
	BgwJob	   *old_job;
	bool		interval_changed = false;

	/* First, fetch the old job to compare schedule_interval */
	old_job = bgw_job_find(job_id, CurrentMemoryContext, false);
	if (old_job == NULL)
		return false;

	/* Check if schedule_interval changed */
	interval_changed = !DatumGetBool(
									 DirectFunctionCall2(interval_eq,
														 IntervalPGetDatum(&old_job->fd.schedule_interval),
														 IntervalPGetDatum(&job->fd.schedule_interval)));

	if (interval_changed)
	{
		BgwJobStat *stat = bgw_job_stat_find(job->fd.id);

		if (stat != NULL)
		{
			TimestampTz next_start = DatumGetTimestampTz(
														 DirectFunctionCall2(timestamptz_pl_interval,
																			 TimestampTzGetDatum(stat->fd.last_finish),
																			 IntervalPGetDatum(&job->fd.schedule_interval)));

			/*
			 * Allow DT_NOBEGIN for next_start here through allow_unset=true
			 * in the case that last_finish is DT_NOBEGIN.
			 */
			bgw_job_stat_update_next_start(job->fd.id, next_start, true);
		}
	}

	pfree(old_job);

	/* Run the check function on config if provided */
	if (job->fd.config)
		job_config_check(job, job->fd.config);

	/* Build the UPDATE statement with 13 parameters */
	{
		Oid			argtypes[13] = {
			INTERVALOID,		/* $1  schedule_interval */
			INTERVALOID,		/* $2  max_runtime */
			INT4OID,			/* $3  max_retries */
			INTERVALOID,		/* $4  retry_period */
			BOOLOID,			/* $5  scheduled */
			BOOLOID,			/* $6  fixed_schedule */
			JSONBOID,			/* $7  config */
			NAMEOID,			/* $8  check_schema */
			NAMEOID,			/* $9  check_name */
			INT4OID,			/* $10 hypertable_id */
			TIMESTAMPTZOID,		/* $11 initial_start */
			TEXTOID,			/* $12 timezone */
			INT4OID,			/* $13 id (WHERE clause) */
		};
		Datum		argvals[13];
		char		argnulls[13];
		int			i;

		for (i = 0; i < 13; i++)
			argnulls[i] = ' ';

		argvals[0] = IntervalPGetDatum(&job->fd.schedule_interval);
		argvals[1] = IntervalPGetDatum(&job->fd.max_runtime);
		argvals[2] = Int32GetDatum(job->fd.max_retries);
		argvals[3] = IntervalPGetDatum(&job->fd.retry_period);
		argvals[4] = BoolGetDatum(job->fd.scheduled);
		argvals[5] = BoolGetDatum(job->fd.fixed_schedule);

		if (job->fd.config)
			argvals[6] = JsonbPGetDatum(job->fd.config);
		else
			argnulls[6] = 'n';

		if (strlen(NameStr(job->fd.check_schema)) > 0)
			argvals[7] = NameGetDatum(&job->fd.check_schema);
		else
			argnulls[7] = 'n';

		if (strlen(NameStr(job->fd.check_name)) > 0)
			argvals[8] = NameGetDatum(&job->fd.check_name);
		else
			argnulls[8] = 'n';

		if (job->fd.hypertable_id != 0)
			argvals[9] = Int32GetDatum(job->fd.hypertable_id);
		else
			argnulls[9] = 'n';

		if (!TIMESTAMP_NOT_FINITE(job->fd.initial_start))
			argvals[10] = TimestampTzGetDatum(job->fd.initial_start);
		else
			argnulls[10] = 'n';

		if (job->fd.timezone)
			argvals[11] = PointerGetDatum(job->fd.timezone);
		else
			argnulls[11] = 'n';

		argvals[12] = Int32GetDatum(job_id);

		SPI_connect();
		PushActiveSnapshot(GetTransactionSnapshot());
		ret = SPI_execute_with_args(
									"UPDATE " BGW_JOB_TABLE_FQ " SET "
									"schedule_interval = $1, "
									"max_runtime = $2, "
									"max_retries = $3, "
									"retry_period = $4, "
									"scheduled = $5, "
									"fixed_schedule = $6, "
									"config = $7, "
									"check_schema = $8, "
									"check_name = $9, "
									"hypertable_id = $10, "
									"initial_start = $11, "
									"timezone = $12 "
									"WHERE id = $13",
									13, argtypes, argvals, argnulls,
									false, 0);

		if (ret != SPI_OK_UPDATE)
			elog(ERROR, "SPI UPDATE of bgw_job failed: error code %d", ret);

		PopActiveSnapshot();
		SPI_finish();
	}

	/*
	 * Broadcast relcache inval on bgw_job so the scheduler's in-memory
	 * scheduled_jobs cache picks up the change (schedule_interval, scheduled
	 * flag, etc.) on its next iteration.  Without this, the UPDATE only flips
	 * DB state; the cache stays with the pre-update row and the scheduler
	 * keeps dispatching according to stale values until the next unrelated
	 * DDL on bgw_job triggers a reload.
	 *
	 * Plain DML does not fire relcache invalidations on its own (PG fires
	 * those on structural DDL only), so we send the explicit signal here. The
	 * same pattern is mirrored by the SQL helpers add_/remove_/alter_job that
	 * call time_series.bgw_invalidate_cache() after their own DML. Putting it
	 * inside bgw_job_update_by_id means every current and future C-side
	 * caller gets coverage.
	 *
	 * Transactional: CacheInvalidateRelcacheByRelid queues into the current
	 * transaction and broadcasts at commit; on abort the queue is discarded,
	 * exactly matching the row UPDATE lifecycle.
	 */
	{
		Oid			nsp_oid = get_namespace_oid(TS_EXTENSION_SCHEMA_NAME, true);
		Oid			bgw_job_oid = OidIsValid(nsp_oid)
		? get_relname_relid("bgw_job", nsp_oid)
		: InvalidOid;

		if (OidIsValid(bgw_job_oid))
			CacheInvalidateRelcacheByRelid(bgw_job_oid);
	}

	return true;
}

/*
 * bgw_job_check_max_retries -- preserved as-is
 */
static void
bgw_job_check_max_retries(BgwJob * job)
{
	BgwJobStat *job_stat;

	job_stat = bgw_job_stat_find(job->fd.id);

	/* stop to execute failing jobs after reached the "max_retries" option */
	if (job->fd.max_retries >= 0 &&
		job_stat->fd.consecutive_failures >= job->fd.max_retries)
	{
		ereport(WARNING,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("job %d reached max_retries after %d consecutive"
						" failures",
						job->fd.id,
						job_stat->fd.consecutive_failures),
				 errdetail("Job %d unscheduled as max_retries reached %d,"
						   " consecutive failures %d.",
						   job->fd.id,
						   job->fd.max_retries,
						   job_stat->fd.consecutive_failures),
				 errhint("Use alter_job(%d, scheduled => TRUE) SQL function"
						 " to reschedule.",
						 job->fd.id)));

		if (job->fd.scheduled)
		{
			job->fd.scheduled = false;
			bgw_job_update_by_id(job->fd.id, job);
		}
	}
}

/*
 * bgw_job_permission_check -- preserved as-is
 */
void
bgw_job_permission_check(BgwJob * job, const char *cmd)
{
	if (!has_privs_of_role(GetUserId(), job->fd.owner))
	{
		const char *owner_name = GetUserNameFromId(job->fd.owner, false);
		const char *user_name = GetUserNameFromId(GetUserId(), false);

		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("insufficient permissions to %s job %d", cmd, job->fd.id),
				 errdetail("Job %d is owned by role \"%s\" but user \"%s\""
						   " does not belong to that role.",
						   job->fd.id,
						   owner_name,
						   user_name)));
	}
}

/*
 * bgw_job_validate_job_owner -- preserved as-is
 */
void
bgw_job_validate_job_owner(Oid owner)
{
	HeapTuple	role_tup = SearchSysCache1(AUTHOID, ObjectIdGetDatum(owner));

	if (!HeapTupleIsValid(role_tup))
		elog(ERROR, "cache lookup failed for role %u", owner);

	Form_pg_authid rform = (Form_pg_authid) GETSTRUCT(role_tup);

	if (!rform->rolcanlogin)
	{
		ReleaseSysCache(role_tup);
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
				 errmsg("permission denied to start background process as role \"%s\"",
						NameStr(rform->rolname)),
				 errhint("Hypertable owner must have LOGIN permission to"
						 " run background tasks.")));
	}
	ReleaseSysCache(role_tup);
}

/*
 * bgw_job_execute_real -- the actual job-execution body, with no
 * test-hook interception.  Exposed (via job.h) so the test
 * dispatcher's default case can invoke it without recursing back
 * into the hook.
 */
JobResult
bgw_job_execute_real(BgwJob * job)
{
	/*
	 * Generic policy dispatcher (mirrors upstream bgw_policy/job.c::
	 * job_execute).  Resolves job->fd.proc_schema.proc_name with a fixed
	 * (int4 jobid, jsonb config) signature, then dispatches to the
	 * appropriate executor based on prokind.  Each policy procedure (e.g.
	 * policy_refresh_cagg) is responsible for parsing its own config.
	 *
	 * This is what makes BGW dispatch policy-agnostic: future retention /
	 * compression / reorder policies just register their own (jobid, config)
	 * PROCEDURE and write rows into bgw_job pointing at it -- no changes to
	 * this function are required.
	 */
	Const	   *arg1,
			   *arg2;
	bool		portal_created = false;
	char		prokind;
	Oid			proc;
	FuncExpr   *funcexpr;
	MemoryContext parent_ctx = CurrentMemoryContext;
	Portal		portal = ActivePortal;

	if (job->fd.config)
		elog(DEBUG1,
			 "Executing %s.%s for job %d with parameters %s",
			 NameStr(job->fd.proc_schema), NameStr(job->fd.proc_name),
			 job->fd.id,
			 DatumGetCString(DirectFunctionCall1(jsonb_out,
												 JsonbPGetDatum(job->fd.config))));
	else
		elog(DEBUG1, "Executing %s.%s for job %d with no config",
			 NameStr(job->fd.proc_schema), NameStr(job->fd.proc_name),
			 job->fd.id);

	/* Create a transient Portal if not already inside one. */
	if (!PortalIsValid(portal))
	{
		portal_created = true;
		portal = CreatePortal("", true, true);
		portal->visible = false;
		portal->resowner = CurrentResourceOwner;
		ActivePortal = portal;
		PortalContext = portal->portalContext;
		StartTransactionCommand();
		EnsurePortalSnapshotExists();
		/* CBDB resource queue (see Assert at resqueue.c:2415). */
		portal->queueId = GetResQueueId();
	}

	proc = bgw_job_get_funcid(job);
	if (!OidIsValid(proc))
	{
		if (portal_created)
		{
			if (ActiveSnapshotSet())
				PopActiveSnapshot();
			CommitTransactionCommand();
			PortalDrop(portal, false);
			ActivePortal = NULL;
			PortalContext = NULL;
		}
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("policy procedure %s.%s(int4, jsonb) not found",
						NameStr(job->fd.proc_schema),
						NameStr(job->fd.proc_name))));
	}
	prokind = get_func_prokind(proc);

	/*
	 * Build args in parent_ctx (NOT CurTransactionContext, which inner
	 * StartTransactionCommand calls inside the policy procedure may destroy).
	 * See note in src/bgw_policy/cagg_refresh_policy.c.
	 */
	MemoryContextSwitchTo(parent_ctx);
	arg1 = makeConst(INT4OID, -1, InvalidOid, 4,
					 Int32GetDatum(job->fd.id), false, true);
	if (job->fd.config == NULL)
		arg2 = (Const *) makeNullConst(JSONBOID, -1, InvalidOid);
	else
		arg2 = makeConst(JSONBOID, -1, InvalidOid, -1,
						 JsonbPGetDatum(job->fd.config), false, false);

	funcexpr = makeFuncExpr(proc, VOIDOID,
							list_make2(arg1, arg2),
							InvalidOid, InvalidOid, COERCE_EXPLICIT_CALL);

	switch (prokind)
	{
		case PROKIND_PROCEDURE:
			{
				CallStmt   *call = makeNode(CallStmt);
				DestReceiver *dest;
				ParamListInfo params;

				call->funcexpr = funcexpr;
				dest = CreateDestReceiver(DestNone);
				params = makeParamList(0);
				ExecuteCallStmt(call, params, false /* atomic */ , dest);
				break;
			}
		case PROKIND_FUNCTION:
			{
				EState	   *estate = CreateExecutorState();
				ExprContext *econtext = CreateExprContext(estate);
				ExprState  *es = ExecPrepareExpr((Expr *) funcexpr, estate);
				bool		isnull;

				ExecEvalExpr(es, econtext, &isnull);
				FreeExprContext(econtext, true);
				FreeExecutorState(estate);
				break;
			}
		default:
			if (portal_created)
			{
				if (ActiveSnapshotSet())
					PopActiveSnapshot();
				CommitTransactionCommand();
				PortalDrop(portal, false);
				ActivePortal = NULL;
				PortalContext = NULL;
			}
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("unsupported function type: %c", prokind)));
			break;
	}

	if (portal_created)
	{
		if (ActiveSnapshotSet())
			PopActiveSnapshot();
		CommitTransactionCommand();
		PortalDrop(portal, false);
		ActivePortal = NULL;
		PortalContext = NULL;
	}

	return JOB_SUCCESS;
}

/*
 * bgw_job_execute -- entry point with test-hook interception.
 *
 * In USE_ASSERT_CHECKING builds, mock-time tests can install a
 * scheduler_test_hook to dispatch by job proc_name.  The hook's default calls
 * bgw_job_execute_real to avoid recursion back into itself.
 * Replaces ts_cm_functions->job_execute cross-module dispatch.
 */
JobResult
bgw_job_execute(BgwJob * job)
{
#ifdef USE_ASSERT_CHECKING
	if (scheduler_test_hook != NULL)
		return scheduler_test_hook(job) ? JOB_SUCCESS : JOB_FAILURE_IN_EXECUTION;
#endif

	return bgw_job_execute_real(job);
}

/*
 * bgw_job_has_timeout -- preserved as-is
 */
bool
bgw_job_has_timeout(BgwJob * job)
{
	Interval	zero_val = {
		.time = 0,
	};

	return DatumGetBool(DirectFunctionCall2(interval_gt,
											IntervalPGetDatum(&job->fd.max_runtime),
											IntervalPGetDatum(&zero_val)));
}

/*
 * bgw_job_timeout_at -- preserved as-is
 */
TimestampTz
bgw_job_timeout_at(BgwJob * job, TimestampTz start_time)
{
	/* timestamptz plus interval */
	return DatumGetTimestampTz(DirectFunctionCall2(timestamptz_pl_interval,
												   TimestampTzGetDatum(start_time),
												   IntervalPGetDatum(&job->fd.max_runtime)));
}

PG_FUNCTION_INFO_V1(bgw_job_entrypoint);

/*
 * zero_guc -- preserved as-is
 */
static void
zero_guc(const char *guc_name)
{
	int			config_change = set_config_option(guc_name, "0", PGC_SUSET,
												  PGC_S_SESSION,
												  GUC_ACTION_SET, true,
												  0, false);

	if (config_change == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("guc \"%s\" does not exist", guc_name)));
	else if (config_change < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not set \"%s\" guc", guc_name)));
}

/*
 * bgw_job_get_funcid -- preserved as-is
 */
Oid
bgw_job_get_funcid(BgwJob * job)
{
	ObjectWithArgs *object = makeNode(ObjectWithArgs);

	object->objname = list_make2(makeString(NameStr(job->fd.proc_schema)),
								 makeString(NameStr(job->fd.proc_name)));
	object->objargs = list_make2(SystemTypeName("int4"),
								 SystemTypeName("jsonb"));

	/* Return InvalidOid if don't found */
	return LookupFuncWithArgs(OBJECT_ROUTINE, object, true);
}

/*
 * bgw_job_function_call_string was a upstream import that built a
 * "CALL/SELECT proc(jobid, config)" string for SPI dispatch.  Removed:
 *   1. No callers -- the modern dispatch path uses ExecuteCallStmt
 *      with proper parameter Const nodes (see bgw_job_execute_real
 *      in this file), which is parameterized by construction.
 *   2. The original used `'%d'` to splice the int job_id into the
 *      string surrounded by single quotes, relying on PG's implicit
 *      text->int cast -- defensible but fragile, and the kind of
 *      pattern future maintainers might lift into a place where it
 *      *does* matter.  Removing the function removes the temptation.
 */

/*
 * bgw_job_entrypoint -- background worker entry point
 *
 * Removed: MGCallbacks/mem_guard, ts_license_enable_module_loading,
 *          ts_is_tss_enabled, tss callbacks.
 * Added:   Gp_role = GP_ROLE_DISPATCH for CBDB.
 */
extern Datum
bgw_job_entrypoint(PG_FUNCTION_ARGS)
{
	Oid			db_oid = DatumGetObjectId(MyBgworkerEntry->bgw_main_arg);
	BgwParams	params;
	BgwJob	   *job;
	JobResult	res = JOB_FAILURE_IN_EXECUTION;
	bool		got_lock;
	instr_time	start;
	instr_time	duration;

	memcpy(&params, MyBgworkerEntry->bgw_extra, sizeof(BgwParams));
	if (!OidIsValid(params.user_oid) || params.job_id == 0)
		elog(ERROR,
			 "job id or user oid was zero - job_id: %d, user_oid: %d",
			 params.job_id,
			 params.user_oid);

	BackgroundWorkerBlockSignals();
	/* Setup any signal handlers here */

	/*
	 * do not use the default `bgworker_die` sigterm handler because it does
	 * not respect critical sections
	 */
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	/*
	 * Forward proc_exit() code through the inherited PAX atexit hook.
	 * Required for every bgworker that links libpaxformat -- without this the
	 * worker's proc_exit path runs C++ atexit destructors on
	 * pax::min_max_opers etc. and aborts with "terminate called without an
	 * active exception" (SIGABRT) on SIGTERM-triggered exits.  See
	 * ts_compress_pax.cc for the full two-step capture rationale.  Matches
	 * the pattern in bgw_launcher_main and bgw_scheduler_main.
	 */
	ts_pax_register_per_backend_exit_capture();

	BackgroundWorkerInitializeConnectionByOid(db_oid, params.user_oid, 0);

#ifdef GP_VERSION_NUM
	/* CBDB: set Gp_role to dispatch so we can run distributed queries */
	Gp_role = GP_ROLE_DISPATCH;
#endif

	log_min_messages = guc_bgw_log_level;

	elog(DEBUG2, "job %d started execution", params.job_id);

	INSTR_TIME_SET_CURRENT(start);

	StartTransactionCommand();

	/*
	 * Grab a session lock on the job row to prevent concurrent deletes. Lock
	 * is released when the job process exits.
	 */
	job = bgw_job_find_with_lock(params.job_id,
								 TopMemoryContext,
								 RowShareLock,
								 SESSION_LOCK,
								  /* block */ true,
								 &got_lock);
	if (job == NULL)
		/* If the job is not found, we can't proceed */
		elog(ERROR, "job %d not found when running the background worker",
			 params.job_id);

	/* get parameters from bgworker */
	job->job_history.id = params.job_history_id;
	job->job_history.execution_start = params.job_history_execution_start;
	bgw_job_stat_history_update(JOB_STAT_HISTORY_UPDATE_PID, job,
								JOB_SUCCESS, NULL);

	CommitTransactionCommand();

	elog(DEBUG2, "job %d (%s) found", params.job_id,
		 NameStr(job->fd.application_name));

	pgstat_report_appname(NameStr(job->fd.application_name));
	MemoryContext oldcontext = CurrentMemoryContext;

	bool		job_failed = false;

	PG_TRY();
	{
		/*
		 * we do not necessarily have a valid parallel worker context in
		 * background workers, so disable parallel execution by default
		 */
		zero_guc("max_parallel_workers_per_gather");
		zero_guc("max_parallel_workers");
		zero_guc("max_parallel_maintenance_workers");

		res = bgw_job_execute(job);

		/* The job is responsible for committing or aborting its own txns */
		if (IsTransactionState())
			elog(ERROR,
				 "background job \"%s\" failed to end the transaction",
				 NameStr(job->fd.application_name));
	}
	PG_CATCH();
	{
		ErrorData  *edata;
		NameData	proc_schema = {.data = {0}}, proc_name = {.data = {0}};

		if (IsTransactionState())
			/* If there was an error, rollback what was done before the error */
			AbortCurrentTransaction();
		StartTransactionCommand();

		/*
		 * Free the old job if it exists, it's no longer needed, and since
		 * it's in the TopMemoryContext it won't be freed otherwise.
		 */
		if (job != NULL)
		{
			pfree(job);
		}

		/* switch away from error context to not lose the data */
		MemoryContextSwitchTo(oldcontext);
		job_failed = true;
		edata = CopyErrorData();

		/*
		 * Note that the mark_start happens in the scheduler right before the
		 * job is launched.  Try to get a lock on the job again, because the
		 * error removed the session lock.  Don't block and only record if the
		 * lock was actually obtained.
		 */
		job = bgw_job_find_with_lock(params.job_id,
									 TopMemoryContext,
									 RowShareLock,
									 TXN_LOCK,
									  /* block */ false,
									 &got_lock);
		if (job != NULL)
		{
			namestrcpy(&proc_name, NameStr(job->fd.proc_name));
			namestrcpy(&proc_schema, NameStr(job->fd.proc_schema));

			job->job_history.id = params.job_history_id;
			job->job_history.execution_start = params.job_history_execution_start;

			/*
			 * Protect against a second ERROR thrown from inside mark_end
			 * (typically: bgw_job_stat row was concurrently DELETE'd by a
			 * remove_continuous_aggregate_policy that committed between the
			 * worker's mark_start and this point).  Re-raising inside a
			 * PG_CATCH block escalates to FATAL and tears down the worker
			 * noisily; downgrade to LOG instead so the original job error (in
			 * `edata`) still reaches the user via the subsequent
			 * ReThrowError.  Mirrors the scheduler-side guard already in
			 * worker_state_cleanup (P1-K).
			 */
			PG_TRY();
			{
				bgw_job_stat_mark_end(job,
									  JOB_FAILURE_IN_EXECUTION,
									  errdata_to_jsonb(edata,
													   &proc_schema,
													   &proc_name));
				bgw_job_check_max_retries(job);
			}
			PG_CATCH();
			{
				ErrorData  *inner;

				MemoryContextSwitchTo(oldcontext);
				inner = CopyErrorData();
				FlushErrorState();
				elog(LOG,
					 "could not record job %d failure in stat tables: %s",
					 job->fd.id, inner->message);
				FreeErrorData(inner);
			}
			PG_END_TRY();
			pfree(job);
		}

		/*
		 * the rethrow will log the error; but also log which job threw the
		 * error
		 */
		elog(LOG, "job %d threw an error", params.job_id);

		CommitTransactionCommand();
		FlushErrorState();
		ReThrowError(edata);
	}
	PG_END_TRY();

	elog(DEBUG2,
		 "BGW worker: job %d execute returned res=%d, IsTransactionState=%d",
		 params.job_id, res, IsTransactionState());

	if (!IsTransactionState())
		StartTransactionCommand();

	/*
	 * Note that the mark_start happens in the scheduler right before the job
	 * is launched
	 */
	bgw_job_stat_mark_end(job, res, NULL);

	if (IsTransactionState())
		CommitTransactionCommand();

	INSTR_TIME_SET_CURRENT(duration);
	INSTR_TIME_SUBTRACT(duration, start);

	elog(DEBUG1,
		 "job %d (%s) exiting with %s: execution time %.2f ms",
		 params.job_id,
		 NameStr(job->fd.application_name),
		 (res == JOB_SUCCESS ? "success" : "failure"),
		 INSTR_TIME_GET_MILLISEC(duration));

	if (job != NULL)
	{
		pfree(job);
		job = NULL;
	}

	PG_RETURN_VOID();
}

/*
 * bgw_run_job -- synchronous job runner, callable from SQL as a PROCEDURE.
 *
 * Mirrors the BGW worker path (mark_start -> execute -> mark_end) but runs
 * in the caller's session and transaction stack.  Intended for
 * deterministic regression testing of policy / refresh logic without
 * depending on BGW scheduler tick timing.
 *
 * Differences from a real BGW worker run:
 *   - Runs in caller's session (not a fork()'d worker process)
 *   - search_path / GUCs inherit from caller (BGW worker resets them)
 *   - No SESSION_LOCK on the job row
 *   - Same C path to cagg_refresh / mark_start / mark_end as the BGW worker
 *
 * Defined as a PROCEDURE so it can issue StartTransactionCommand /
 * CommitTransactionCommand internally (the same way bgw_job_entrypoint
 * does in the BGW path).
 */
PG_FUNCTION_INFO_V1(bgw_run_job);
extern Datum
bgw_run_job(PG_FUNCTION_ARGS)
{
	int32		job_id;
	BgwJob	   *job;
	JobResult	res = JOB_SUCCESS;
	MemoryContext caller_cxt = CurrentMemoryContext;
	StringInfoData sql;
	ErrorData  *edata = NULL;	/* captured by PG_CATCH on failure */

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("job_id must not be NULL")));
	job_id = PG_GETARG_INT32(0);

#ifdef GP_VERSION_NUM
	if (Gp_role != GP_ROLE_DISPATCH)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("time_series.run_job must be called on the coordinator")));
#endif

	/*
	 * run_job's failure-recovery path needs to issue AbortCurrentTransaction
	 * + StartTransactionCommand so mark_end can record the failure in a
	 * healthy outer transaction.  That is only legal in a non-atomic
	 * (autocommit) call context.  Reject up front when called inside an
	 * explicit transaction block (BEGIN/COMMIT) -- without this check, a
	 * failure inside the inner refresh CALL crashes the backend with a
	 * FailedAssertion when AbortCurrentTransaction trips PG's atomic-CALL
	 * guard.  This matches PG's own rule that a PROCEDURE cannot manage
	 * transactions when called atomically.
	 */
	{
		CallContext *cctx = (CallContext *) fcinfo->context;
		bool		atomic = (cctx == NULL || !IsA(cctx, CallContext) || cctx->atomic);

		/*
		 * Reject when called from any context where we cannot manage
		 * transactions: an explicit BEGIN/COMMIT block, OR from a DO block /
		 * plpgsql function (which calls the procedure atomically).
		 * IsTransactionBlock() handles the BEGIN case; the CallContext check
		 * handles DO/plpgsql.
		 */
		if (atomic || IsTransactionBlock())
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TRANSACTION_STATE),
					 errmsg("time_series.run_job cannot run in an atomic context"),
					 errhint("run_job must be called in autocommit mode "
							 "(no enclosing BEGIN/COMMIT block, DO block, "
							 "or function).")));
	}

	/*
	 * Look up the job in the caller's MemoryContext (via SPI; this works fine
	 * in the caller's transaction).
	 */
	job = bgw_job_find(job_id, caller_cxt, /* fail_if_not_found */ true);

	/*
	 * Owner check: caller must be a member of the job's owner role (which
	 * includes the owner itself and superuser).  Without this, any role
	 * granted EXECUTE on time_series.run_job(int) could fire any other user's
	 * policy in the caller's session.  Mirrors upstream
	 * bgw_policy/job_api.c::job_run.
	 */
	bgw_job_permission_check(job, "run");

	/*
	 * mark_start updates bgw_job_stat the same way the real BGW worker does:
	 * total_runs++, total_crashes++ (decremented again on mark_end), set
	 * last_start = now().  Uses SPI internally; reuses the caller's txn.
	 */
	bgw_job_stat_mark_start(job);

	/*
	 * Commit mark_start in its own transaction before running the body.
	 *
	 * mark_start's UPDATE on bgw_job_stat is a RowExclusiveLock on upstream
	 * PostgreSQL, but CBDB escalates UPDATE/DELETE to a table-level
	 * ExclusiveLock whenever gp_enable_global_deadlock_detector is off (the
	 * default).  Left in the body's transaction, that whole-table lock would
	 * be held across cagg_refresh's TX1 -- which dispatches the L1->L2
	 * migration to every segment over the interconnect -- until the first
	 * SPI_commit_and_chain.  A stall there pins the lock and blocks every
	 * other refresh's mark_start cluster-wide, stalling the per-source
	 * invalidation threshold and silently freezing every CAGG watermark on
	 * that source (observed: a wedged refresh held it 21 min).
	 *
	 * The BGW scheduler already commits mark_start separately before
	 * launching the worker (scheduler.c, JOB_STATE_STARTED); this gives the
	 * synchronous run_job path the same lock discipline.  SPI_commit (not a
	 * bare CommitTransactionCommand) is used so the active portal snapshot is
	 * handled correctly -- the same NONATOMIC commit cagg_refresh relies on.
	 * Legal because run_job already mandates a non-atomic context (checked
	 * above).
	 */
	if (SPI_connect_ext(SPI_OPT_NONATOMIC) != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed in run_job mark_start commit");
	SPI_commit();
	SPI_finish();
	MemoryContextSwitchTo(caller_cxt);

	/*
	 * Execute the job body via generic dispatch on bgw_job.proc_name --
	 * mirrors what bgw_job_execute_real does in the BGW worker path, but runs
	 * in the caller's session so cagg_refresh sees a valid ActivePortal.
	 *
	 * Build a literal SQL string ("CALL schema.name(jid, config::jsonb)" for
	 * procedures, "SELECT" for functions) and execute via SPI in NONATOMIC
	 * mode so any nested SPI_commit_and_chain (e.g. inside cagg_refresh) is
	 * allowed.  proc_schema/proc_name come from the bgw_job catalog, not user
	 * input, so no quoting attack surface beyond standard identifier quoting.
	 * The config jsonb is the only user-influenced piece and is wrapped via
	 * quote_literal_cstr.
	 */
	{
		Oid			procoid;
		char		prokind;
		ObjectWithArgs *obj;
		const char *call_or_select;
		char	   *q_config;

		obj = makeNode(ObjectWithArgs);
		obj->objname = list_make2(makeString(NameStr(job->fd.proc_schema)),
								  makeString(NameStr(job->fd.proc_name)));
		obj->objargs = list_make2(SystemTypeName("int4"),
								  SystemTypeName("jsonb"));
		procoid = LookupFuncWithArgs(OBJECT_ROUTINE, obj, /* missing_ok */ false);

		prokind = get_func_prokind(procoid);
		call_or_select = (prokind == PROKIND_PROCEDURE) ? "CALL" : "SELECT";

		if (job->fd.config != NULL)
			q_config = quote_literal_cstr(
										  JsonbToCString(NULL, &job->fd.config->root,
														 VARSIZE(job->fd.config)));
		else
			q_config = NULL;

		initStringInfo(&sql);
		appendStringInfo(&sql, "%s %s.%s(%d, %s::jsonb)",
						 call_or_select,
						 quote_identifier(NameStr(job->fd.proc_schema)),
						 quote_identifier(NameStr(job->fd.proc_name)),
						 job_id,
						 q_config != NULL ? q_config : "NULL");

		PG_TRY();
		{
			SPI_connect_ext(SPI_OPT_NONATOMIC);
			{
				SPIExecuteOptions opts;

				memset(&opts, 0, sizeof(opts));

				/*
				 * allow_nonatomic=true so the inner procedure may
				 * SPI_commit_and_chain (it does, in cagg_refresh).
				 */
				opts.allow_nonatomic = true;
				(void) SPI_execute_extended(sql.data, &opts);
			}
			SPI_finish();
		}
		PG_CATCH();
		{
			MemoryContextSwitchTo(caller_cxt);
			edata = CopyErrorData();
			FlushErrorState();

			/*
			 * The inner CALL aborted the current transaction.  We need a
			 * fresh transaction so mark_end can UPDATE bgw_job_stat (and so
			 * the caller's session is left in a healthy state).  This is safe
			 * inside a PROCEDURE -- the same pattern PL/pgSQL uses to recover
			 * from an exception in an in-procedure CALL.
			 */
			AbortCurrentTransaction();
			StartTransactionCommand();
			MemoryContextSwitchTo(caller_cxt);

			ereport(WARNING,
					(errmsg("job %d failed: %s", job_id, edata->message)));

			res = JOB_FAILURE_IN_EXECUTION;
		}
		PG_END_TRY();

		pfree(sql.data);
		if (q_config)
			pfree(q_config);
	}

	/*
	 * mark_end records success or failure; runs in the (now healthy) outer
	 * transaction either way.  Pass error_data so the failure is captured in
	 * bgw_job_stat_history.data->'error_data' and surfaces in the
	 * time_series.job_history view (instead of leaving DBAs to grep server
	 * logs).  edata is non-NULL only on the PG_CATCH path.
	 */
	{
		Jsonb	   *err_jsonb = NULL;

		if (edata != NULL)
		{
			err_jsonb = errdata_to_jsonb(edata,
										 &job->fd.proc_schema,
										 &job->fd.proc_name);
			FreeErrorData(edata);
		}
		bgw_job_stat_mark_end(job, res, err_jsonb);
	}

	pfree(job);

	PG_RETURN_VOID();
}

/*
 * bgw_job_set_scheduler_test_hook -- preserved as-is
 */
void
bgw_job_set_scheduler_test_hook(scheduler_test_hook_type hook)
{
	scheduler_test_hook = hook;
}

/*
 * bgw_job_set_job_entrypoint_function_name -- preserved as-is
 */
void
bgw_job_set_job_entrypoint_function_name(char *func_name)
{
	job_entrypoint_function_name = func_name;
}

/*
 * bgw_job_run_and_set_next_start -- preserved as-is
 */
extern bool bgw_job_run_and_set_next_start(BgwJob * job, job_main_func func,
										   int64 initial_runs,
										   Interval *next_interval,
										   bool atomic, bool mark);
bool
bgw_job_run_and_set_next_start(BgwJob * job, job_main_func func,
							   int64 initial_runs, Interval *next_interval,
							   bool atomic, bool mark)
{
	BgwJobStat *job_stat;
	bool		result;

	if (atomic)
		StartTransactionCommand();

	if (mark)
		bgw_job_stat_mark_start(job);

	result = func();

	if (mark)
		bgw_job_stat_mark_end(job,
							  result ? JOB_SUCCESS : JOB_FAILURE_IN_EXECUTION,
							  NULL);

	/* Now update next_start. */
	job_stat = bgw_job_stat_find(job->fd.id);

	/*
	 * Note that setting next_start explicitly from this function will
	 * override any backoff calculation due to failure.
	 */
	if (job_stat == NULL)
		elog(ERROR, "job status for job %d not found", job->fd.id);

	if (job_stat->fd.total_runs < initial_runs)
	{
		TimestampTz next_start =
		DatumGetTimestampTz(DirectFunctionCall2(timestamptz_pl_interval,
												TimestampTzGetDatum(job_stat->fd.last_start),
												IntervalPGetDatum(next_interval)));

		bgw_job_stat_set_next_start(job->fd.id, next_start);
	}

	if (atomic)
		CommitTransactionCommand();

	return result;
}

/*
 * bgw_job_validate_schedule_interval -- preserved as-is
 */
void
bgw_job_validate_schedule_interval(Interval *schedule_interval)
{
	bool		has_month,
				has_day,
				has_time;

	has_month = schedule_interval->month;
	has_day = schedule_interval->day;
	has_time = schedule_interval->time;

	if (has_month && (has_day || has_time))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("month intervals cannot have day or time component"),
				 errdetail("Fixed schedule jobs do not support such schedule intervals."),
				 errhint("Express the interval in terms of days or time instead.")));
}

/*
 * bgw_job_validate_timezone -- preserved as-is
 */
char *
bgw_job_validate_timezone(Datum timezone)
{
	DirectFunctionCall2(timestamp_zone,
						timezone,
						TimestampGetDatum(timer_get_current_timestamp()));
	return TextDatumGetCString(timezone);
}
