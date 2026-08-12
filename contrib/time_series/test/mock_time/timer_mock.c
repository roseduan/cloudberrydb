/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 *
 * Adapted from TimescaleDB test/src/bgw/timer_mock.c (Apache 2.0).
 *
 * Changes vs upstream:
 *   - Drop annotations.h (replace TS_FALLTHROUGH with __attribute__).
 *   - Drop bgw/launcher_interface.h (we don't use launcher).
 *   - Drop scanner.h, ts_catalog/catalog.h (no catalog scans here).
 *   - Path adjustments for include "params.h" / "timer_mock.h".
 */
#include <postgres.h>
#include <datatype/timestamp.h>
#include <postmaster/bgworker.h>
#include <storage/latch.h>
#include <storage/proc.h>
#include <utils/memutils.h>

#include "params.h"
#include "timer_mock.h"
#include "../../src/include/bgw/timer.h"

/*
 * The mock clock (params.current_time) is stored as Unix-epoch
 * microseconds -- that is the convention the SQL harness feeds via
 * bgw_params_reset_time(extract(epoch from ts) * 1000000) and the
 * value bgw_mock_time.sql asserts verbatim in bgw_log.  But a
 * TimestampTz is microseconds since the Postgres epoch (2000-01-01).
 * Convert at the TimestampTz boundary so the scheduler's refresh
 * window (which compares TimestampTz against source data) sees the
 * correct wall-clock time instead of one shifted ~30 years into the
 * future by the Unix-vs-Postgres epoch gap.
 */
#define MOCK_UNIX_TO_PG_USEC \
	(((int64) (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY) * \
	 USECS_PER_SEC)

#ifndef pg_attribute_fallthrough
#define pg_attribute_fallthrough() __attribute__((fallthrough))
#endif

static List *bgw_handles = NIL;

static bool mock_wait(TimestampTz until);
static TimestampTz mock_current_time(void);

const Timer ts_mock_timer = {
	.get_current_timestamp = mock_current_time,
	.wait = mock_wait,
};

void
timer_mock_register_bgw_handle(BackgroundWorkerHandle *handle,
								  MemoryContext scheduler_mctx)
{
	elog(WARNING, "[TESTING] Registered new background worker");
	MemoryContext old_context = MemoryContextSwitchTo(scheduler_mctx);
	bgw_handles = lappend(bgw_handles, handle);
	MemoryContextSwitchTo(old_context);
}

/*
 * WARNING: mock_wait must _only_ be called from the bgw_scheduler.  Calling
 * it from a worker will clobber the timer state.
 */
static bool
mock_wait(TimestampTz until)
{
	elog(WARNING,
		 "[TESTING] Wait until " INT64_FORMAT ", started at " INT64_FORMAT,
		 (int64) until,
		 (int64) ts_params_get()->current_time);

	ListCell *lc;

	switch (ts_params_get()->mock_wait_type)
	{
		case WAIT_ON_JOB:
			foreach (lc, bgw_handles)
			{
				BackgroundWorkerHandle *bgw_handle = lfirst(lc);
				WaitForBackgroundWorkerShutdown(bgw_handle);
			}
			bgw_handles = NIL;
			pg_attribute_fallthrough();
		case IMMEDIATELY_SET_UNTIL:
			/* 'until' is a TimestampTz (PG epoch); store it back in the
			 * Unix-epoch convention current_time uses. */
			ts_params_set_time(until + MOCK_UNIX_TO_PG_USEC, false);
			return true;
		case WAIT_FOR_OTHER_TO_ADVANCE:
			/* Wait for another process to set "next time" */
			ts_reset_and_wait_timer_latch();
			return true;
		case WAIT_FOR_STANDARD_WAITLATCH:
			get_standard_timer()->wait(until);
			return true;
		default:
			return false;
	}
}

static TimestampTz
mock_current_time(void)
{
	/* current_time is Unix-epoch microseconds; a TimestampTz is
	 * Postgres-epoch microseconds.  Convert so callers (the refresh
	 * window computation in particular) get a correct wall clock. */
	return (TimestampTz) (ts_params_get()->current_time - MOCK_UNIX_TO_PG_USEC);
}
