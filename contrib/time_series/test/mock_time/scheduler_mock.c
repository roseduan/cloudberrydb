/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 *
 * Adapted from TimescaleDB test/src/bgw/scheduler_mock.c (Apache 2.0).
 *
 * 1:1 port — the only differences vs upstream are:
 *   - PG_FUNCTION_INFO_V1 instead of TS_FUNCTION_INFO_V1
 *   - Cross-module function-table calls dropped (we have one module)
 *   - time_bucket call sites use time_series.time_bucket via SPI
 *     (TSDB calls C function timestamptz_bucket directly; we route
 *     through SPI because our time_bucket lives in this extension's
 *     SQL surface, not in core)
 *   - Header path adjustments
 *
 * Test job infrastructure (test_job_1, test_job_2_error, test_job_3_long,
 * test_job_4, test_job_dispatcher, bgw_test_job_sleep, and
 * test_next_scheduled_execution_slot) ports verbatim — required by
 * the heavy TSDB scheduler tests we are bringing over.
 */
#include <postgres.h>

#include <access/xact.h>
#include <catalog/namespace.h>
#include <executor/spi.h>
#include <miscadmin.h>
#include <pgstat.h>
#include <postmaster/bgworker.h>
#include <signal.h>
#include <storage/ipc.h>
#include <storage/latch.h>
#include <storage/lmgr.h>
#include <storage/lwlock.h>
#include <storage/proc.h>
#include <storage/shmem.h>
#include <utils/builtins.h>
#include <utils/jsonb.h>
#include <utils/memutils.h>
#include <utils/snapmgr.h>
#include <utils/timestamp.h>

#include "../../src/bgw/job.h"
#include "../../src/bgw/job_stat.h"
#include "../../src/bgw/scheduler.h"
#include "../../src/include/bgw/timer.h"
#include "log.h"
#include "params.h"
#include "timer_mock.h"

PG_FUNCTION_INFO_V1(bgw_db_scheduler_test_run_and_wait_for_scheduler_finish);
PG_FUNCTION_INFO_V1(bgw_db_scheduler_test_run);
PG_FUNCTION_INFO_V1(bgw_db_scheduler_test_wait_for_scheduler_finish);
PG_FUNCTION_INFO_V1(bgw_db_scheduler_test_main);
PG_FUNCTION_INFO_V1(bgw_job_execute_test);
PG_FUNCTION_INFO_V1(test_next_scheduled_execution_slot);
PG_FUNCTION_INFO_V1(bgw_test_job_sleep);

/* ----------------------------------------------------------------
 * TestJobType — synthetic test jobs registered in bgw_job by tests.
 * Tests INSERT a row with proc_name = 'bgw_test_job_1' (etc.) and
 * the dispatcher routes to the matching local function.
 * ---------------------------------------------------------------- */
typedef enum TestJobType
{
	TEST_JOB_TYPE_JOB_1 = 0,
	TEST_JOB_TYPE_JOB_2_ERROR,
	TEST_JOB_TYPE_JOB_3_LONG,
	TEST_JOB_TYPE_JOB_4,
	_MAX_TEST_JOB_TYPE
} TestJobType;

static const char *test_job_type_names[_MAX_TEST_JOB_TYPE] = {
	[TEST_JOB_TYPE_JOB_1] = "bgw_test_job_1",
	[TEST_JOB_TYPE_JOB_2_ERROR] = "bgw_test_job_2_error",
	[TEST_JOB_TYPE_JOB_3_LONG] = "bgw_test_job_3_long",
	[TEST_JOB_TYPE_JOB_4] = "bgw_test_job_4",
};

/* ----------------------------------------------------------------
 * Per-scheduler-launch params.  Embedded in BgwParams.bgw_extra so
 * the forked scheduler process reads them back.
 * ---------------------------------------------------------------- */
typedef struct TestSchedulerParams
{
	Oid user_oid;
	int32 ttl;
} TestSchedulerParams;

/* ----------------------------------------------------------------
 * test_next_scheduled_execution_slot — utility used by
 * scheduler_fixed.sql / bgw_db_scheduler_fixed.sql to verify the
 * fixed-schedule next-slot calculation matches expectations.
 *
 * Implementation note: TSDB calls timestamptz_bucket() as a
 * C function directly.  We route through SPI calling
 * time_series.time_bucket() because that's how time_bucket is
 * exposed in V1 (no internal C entry point exported).
 * ---------------------------------------------------------------- */
static Datum
call_time_bucket(Interval *width, TimestampTz ts, TimestampTz origin, text *tz)
{
	int ret;
	bool isnull;
	Datum out;
	StringInfoData q;

	initStringInfo(&q);
	if (tz == NULL)
		appendStringInfoString(&q,
			"SELECT time_series.time_bucket($1::interval, $2::timestamptz, $3::timestamptz)");
	else
		appendStringInfoString(&q,
			"SELECT time_series.time_bucket($1::interval, $2::timestamptz, $4::text, $3::timestamptz)");

	{
		Oid argtypes[4] = { INTERVALOID, TIMESTAMPTZOID, TIMESTAMPTZOID, TEXTOID };
		Datum args[4] = {
			IntervalPGetDatum(width),
			TimestampTzGetDatum(ts),
			TimestampTzGetDatum(origin),
			tz ? PointerGetDatum(tz) : (Datum) 0,
		};
		char nulls[4] = { ' ', ' ', ' ', tz ? ' ' : 'n' };

		SPI_connect();
		ret = SPI_execute_with_args(q.data, 4, argtypes, args, nulls, true, 1);
		if (ret != SPI_OK_SELECT || SPI_processed != 1)
			elog(ERROR, "time_bucket SPI call failed");
		out = SPI_getbinval(SPI_tuptable->vals[0],
							SPI_tuptable->tupdesc, 1, &isnull);
		out = TimestampTzGetDatum(DatumGetTimestampTz(out));
		SPI_finish();
	}
	return out;
}

extern Datum
test_next_scheduled_execution_slot(PG_FUNCTION_ARGS)
{
	Interval *schedule_interval = PG_GETARG_INTERVAL_P(0);
	TimestampTz finish_time = PG_GETARG_TIMESTAMPTZ(1);
	TimestampTz initial_start = PG_GETARG_TIMESTAMPTZ(2);
	text *timezone = PG_ARGISNULL(3) ? NULL : PG_GETARG_TEXT_PP(3);
	Datum schedint_datum = IntervalPGetDatum(schedule_interval);
	Datum result;

	if (schedule_interval->month > 0)
	{
		Datum init_b, fini_b;
		Interval one_month = { .day = 0, .time = 0, .month = 1 };

		/* For month-based schedules, bucket each end-point separately and
		 * count whole months between them, then add to initial_start. */
		init_b = call_time_bucket(schedule_interval, initial_start, 0, timezone);
		fini_b = call_time_bucket(schedule_interval, finish_time, 0, timezone);
		fini_b = DirectFunctionCall2(timestamptz_pl_interval, fini_b, schedint_datum);

		Datum y_init = DirectFunctionCall2(timestamptz_part,
										   CStringGetTextDatum("year"), init_b);
		Datum y_fini = DirectFunctionCall2(timestamptz_part,
										   CStringGetTextDatum("year"), fini_b);
		Datum m_init = DirectFunctionCall2(timestamptz_part,
										   CStringGetTextDatum("month"), init_b);
		Datum m_fini = DirectFunctionCall2(timestamptz_part,
										   CStringGetTextDatum("month"), fini_b);
		float8 month_diff = (DatumGetFloat8(y_fini) * 12) + DatumGetFloat8(m_fini)
						  - ((DatumGetFloat8(y_init) * 12) + DatumGetFloat8(m_init));
		Datum to_add = DirectFunctionCall2(interval_mul,
										   IntervalPGetDatum(&one_month),
										   Float8GetDatum(month_diff));
		result = DirectFunctionCall2(timestamptz_pl_interval,
									 TimestampTzGetDatum(initial_start), to_add);
	}
	else
	{
		result = call_time_bucket(schedule_interval, finish_time, initial_start, timezone);
	}
	while (DatumGetTimestampTz(result) <= finish_time)
		result = DirectFunctionCall2(timestamptz_pl_interval, result, schedint_datum);
	return result;
}

/* ----------------------------------------------------------------
 * Test scheduler entrypoint: forks a BGW that runs bgw_scheduler_process
 * with the mock timer installed.  The BGW process picks up jobs from
 * bgw_job and dispatches them through bgw_job_execute_test, which
 * routes synthetic test_job_1/2/3/4 to the in-process dispatcher and
 * everything else to the regular bgw_job_entrypoint path.
 * ---------------------------------------------------------------- */
extern Datum
bgw_db_scheduler_test_main(PG_FUNCTION_ARGS)
{
	Oid db_oid = DatumGetObjectId(MyBgworkerEntry->bgw_main_arg);
	TestSchedulerParams params;

	BackgroundWorkerBlockSignals();
	bgw_scheduler_register_signal_handlers();
	BackgroundWorkerUnblockSignals();
	bgw_scheduler_setup_callbacks();

	memcpy(&params, MyBgworkerEntry->bgw_extra, sizeof(params));

	elog(NOTICE, "scheduler user id %u", params.user_oid);
	elog(NOTICE, "running a test in the background: db=%u ttl=%d",
		 db_oid, params.ttl);

	BackgroundWorkerInitializeConnectionByOid(db_oid, params.user_oid, 0);

	StartTransactionCommand();
	(void) ts_params_get();
	ts_initialize_timer_latch();
	CommitTransactionCommand();

	bgw_log_set_application_name("DB Scheduler");
	ts_register_emit_log_hook();

	timer_set(&ts_mock_timer);

	/*
	 * 1:1 with TSDB: worker entrypoint is bgw_job_execute_test, which
	 * installs the test_job_dispatcher hook before calling
	 * bgw_job_entrypoint.  For synthetic test_job_* proc_names the
	 * dispatcher routes locally; otherwise it falls through to
	 * bgw_job_execute_real (real CAGG refresh path).
	 */
	bgw_job_set_job_entrypoint_function_name("bgw_job_execute_test");

	pgstat_report_appname("DB Scheduler Test");

	bgw_scheduler_setup_mctx();

	bgw_scheduler_process(params.ttl, timer_mock_register_bgw_handle);

	PG_RETURN_VOID();
}

static BackgroundWorkerHandle *
start_test_scheduler(int32 ttl, Oid user_oid)
{
	BackgroundWorker worker = {
		.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION,
		.bgw_start_time = BgWorkerStart_RecoveryFinished,
		.bgw_restart_time = BGW_NEVER_RESTART,
		.bgw_notify_pid = MyProcPid,
		.bgw_main_arg = ObjectIdGetDatum(MyDatabaseId),
	};
	TestSchedulerParams params = { .user_oid = user_oid, .ttl = ttl };
	BackgroundWorkerHandle *handle = NULL;

	strlcpy(worker.bgw_name, "bgw_db_scheduler_test", BGW_MAXLEN);
	strlcpy(worker.bgw_library_name, "time_series", BGW_MAXLEN);
	strlcpy(worker.bgw_function_name, "bgw_db_scheduler_test_main", BGW_MAXLEN);
	memcpy(worker.bgw_extra, &params, sizeof(params));

	bgw_scheduler_setup_mctx();

	/*
	 * Registration transiently fails when every bgworker slot is taken
	 * (schedulers and job workers of neighboring tests still winding
	 * down).  The old behavior returned NULL and the callers returned
	 * VOID silently, so retry-burst loops in the tests (DB-SCHED-04/05)
	 * spun through all their attempts in milliseconds without ever
	 * running a scheduler -- observed as the "total_runs >= 1 but
	 * total_successes = 0" full-suite flake.  Ride out the famine with
	 * a bounded retry, and if slots stay exhausted, fail LOUDLY: a test
	 * helper must not pretend it ran a scheduler it never started.
	 */
	for (int attempt = 0; attempt < 50; attempt++)
	{
		if (RegisterDynamicBackgroundWorker(&worker, &handle))
			return handle;
		pg_usleep(100000L);		/* 100 ms; 50 tries = 5 s of patience */
		CHECK_FOR_INTERRUPTS();
	}
	elog(ERROR,
		 "could not register test bgw scheduler after 5 s:"
		 " no free bgworker slots");
	return NULL;				/* unreachable */
}

extern Datum
bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(PG_FUNCTION_ARGS)
{
	BackgroundWorkerHandle *worker_handle;
	pid_t pid;
	BgwHandleStatus status;

	worker_handle = start_test_scheduler(PG_GETARG_INT32(0), GetUserId());
	if (worker_handle == NULL)
		PG_RETURN_VOID();

	status = WaitForBackgroundWorkerStartup(worker_handle, &pid);
	if (status != BGWH_STARTED)
		elog(ERROR, "test bgw scheduler did not start");

	status = WaitForBackgroundWorkerShutdown(worker_handle);
	if (status != BGWH_STOPPED)
		elog(ERROR, "test bgw scheduler did not stop");

	PG_RETURN_VOID();
}

static BackgroundWorkerHandle *current_handle = NULL;

extern Datum
bgw_db_scheduler_test_run(PG_FUNCTION_ARGS)
{
	pid_t pid;
	MemoryContext old_ctx;
	BgwHandleStatus status;

	old_ctx = MemoryContextSwitchTo(TopMemoryContext);
	current_handle = start_test_scheduler(PG_GETARG_INT32(0), GetUserId());
	MemoryContextSwitchTo(old_ctx);

	if (current_handle == NULL)
		PG_RETURN_VOID();

	status = WaitForBackgroundWorkerStartup(current_handle, &pid);
	if (status != BGWH_STARTED)
		elog(ERROR, "test bgw scheduler did not start");

	PG_RETURN_VOID();
}

extern Datum
bgw_db_scheduler_test_wait_for_scheduler_finish(PG_FUNCTION_ARGS)
{
	if (current_handle != NULL)
	{
		BgwHandleStatus status = WaitForBackgroundWorkerShutdown(current_handle);
		if (status != BGWH_STOPPED)
			elog(ERROR, "test bgw scheduler did not stop");
		current_handle = NULL;
	}
	PG_RETURN_VOID();
}

/* ----------------------------------------------------------------
 * Synthetic test jobs (1:1 with TSDB upstream)
 * ---------------------------------------------------------------- */

static bool
test_job_1(void)
{
	StartTransactionCommand();
	elog(WARNING, "Execute job 1");
	CommitTransactionCommand();
	return true;
}

static bool
test_job_2_error(void)
{
	StartTransactionCommand();
	elog(WARNING, "Before error job 2");

	elog(ERROR, "Error job 2");

	elog(WARNING, "After error job 2");

	CommitTransactionCommand();
	return true;
}

static pqsigfunc prev_signal_func = NULL;

static void
log_terminate_signal(SIGNAL_ARGS)
{
	write_stderr("job got term signal\n");
	if (prev_signal_func != NULL)
		prev_signal_func(postgres_signal_arg);
}

/*
 * bgw_test_job_sleep — used to test removing jobs that are running.
 * Sleeps 10s after committing the "Before sleep" elog so other
 * sessions can see the job is in progress.
 */
Datum
bgw_test_job_sleep(PG_FUNCTION_ARGS)
{
	BackgroundWorkerBlockSignals();

	if (prev_signal_func == NULL)
		prev_signal_func = pqsignal(SIGTERM, log_terminate_signal);
	BackgroundWorkerUnblockSignals();

	elog(WARNING, "Before sleep");
	PopActiveSnapshot();
	CommitTransactionCommand();

	StartTransactionCommand();
	DirectFunctionCall1(pg_sleep, Float8GetDatum(10));

	elog(WARNING, "After sleep");

	PG_RETURN_VOID();
}

static bool
test_job_3_long(void)
{
	BackgroundWorkerBlockSignals();

	if (prev_signal_func == NULL)
		prev_signal_func = pqsignal(SIGTERM, log_terminate_signal);
	BackgroundWorkerUnblockSignals();

	elog(WARNING, "Before sleep job 3");

	DirectFunctionCall1(pg_sleep, Float8GetDatum(0.5L));

	elog(WARNING, "After sleep job 3");
	return true;
}

/* Same as job 1, but a wrapper changes its next_start. */
static bool
test_job_4(void)
{
	elog(WARNING, "Execute job 4");
	return true;
}

static TestJobType
get_test_job_type_from_name(Name job_type_name)
{
	int i;
	for (i = 0; i < _MAX_TEST_JOB_TYPE; i++)
	{
		if (namestrcmp(job_type_name, test_job_type_names[i]) == 0)
			return i;
	}
	return _MAX_TEST_JOB_TYPE;
}

static bool
test_job_dispatcher(BgwJob *job)
{
	TestJobType jt = get_test_job_type_from_name(&job->fd.proc_name);

	/*
	 * Only register emit_log_hook for SYNTHETIC test jobs.  The hook
	 * does SPI INSERTs into public.bgw_log so the test framework can
	 * assert on log content.  That SPI write is safe inside the
	 * synthetic jobs (which don't themselves dispatch to segments) but
	 * is FATAL inside a real CAGG refresh:
	 *
	 *   1. bgw_job_execute_real fires gang dispatches (refresh runs
	 *      INSERT ... SELECT cross-segment).
	 *   2. Any elog(LOG, ...) inside that dispatch (e.g. "segment is in
	 *      reset/recovery mode" at cdbgang_async.c:256, or "session was
	 *      reset" at cdbgang.c:815) invokes emit_log_hook from
	 *      errfinish.
	 *   3. The hook attempts SPI_execute → cdbdisp_makeDispatcherState
	 *      sees numNonExtendedDispatcherState == 1 and raises ERROR
	 *      "query plan with multiple segworker groups is not supported".
	 *   4. That ERROR has no PG_exception_stack (we're inside errfinish
	 *      processing, the outer stack was unwound) → promoted to FATAL
	 *      at elog.c:433.
	 *   5. FATAL's errstart calls MemoryContextReset(ErrorContext),
	 *      wiping the outer elog's message buffer.
	 *   6. The outer errfinish proceeds to pfree(edata->message) on the
	 *      now-clobbered chunk → SIGSEGV → cluster reset.
	 *
	 * Skipping the hook for real refresh jobs eliminates the entire
	 * chain.  Synthetic jobs (test_job_1..4) still get the hook because
	 * their assertions in bgw_scheduler.sql et al. require log capture.
	 * The SCHEDULER process itself registers the hook independently at
	 * scheduler_mock.c:216, so "DB Scheduler" log lines still land in
	 * public.bgw_log for BGW-FULL-07/08 et al.
	 */
	if (jt != _MAX_TEST_JOB_TYPE)
	{
		ts_register_emit_log_hook();
		bgw_log_set_application_name(strdup(NameStr(job->fd.application_name)));
	}

	StartTransactionCommand();
	(void) ts_params_get();
	CommitTransactionCommand();

	switch (jt)
	{
		case TEST_JOB_TYPE_JOB_1:
			return test_job_1();
		case TEST_JOB_TYPE_JOB_2_ERROR:
			return test_job_2_error();
		case TEST_JOB_TYPE_JOB_3_LONG:
			return test_job_3_long();
		case TEST_JOB_TYPE_JOB_4:
		{
			/* Set next_start to 200ms */
			Interval new_interval = { .time = .2 * USECS_PER_SEC };
			return bgw_job_run_and_set_next_start(job, test_job_4, 3,
													 &new_interval,
													 /* atomic */ true,
													 /* mark   */ false);
		}
		default:
			/*
			 * Not a synthetic test job — call the no-hook execute
			 * path so we don't recurse back through this dispatcher.
			 * (TSDB's upstream version routes through cross_module_fn
			 * for the same reason.)
			 */
			return bgw_job_execute_real(job) == JOB_SUCCESS;
	}
	return false;
}

Datum
bgw_job_execute_test(PG_FUNCTION_ARGS)
{
	timer_set(&ts_mock_timer);
	bgw_job_set_scheduler_test_hook(test_job_dispatcher);

	return bgw_job_entrypoint(fcinfo);
}
