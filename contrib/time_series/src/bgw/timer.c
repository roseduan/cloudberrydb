/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 *
 * Portions Copyright (c) 2025-2026, HashData Technology Limited.
 */
#include <postgres.h>
#include <access/xact.h>
#include <fmgr.h>
#include <miscadmin.h>
#include <pgstat.h>
#include <postmaster/bgworker.h>
#include <storage/ipc.h>
#include <storage/latch.h>
#include <storage/lwlock.h>
#include <storage/proc.h>
#include <storage/shmem.h>
#include <utils/jsonb.h>
#include <utils/memutils.h>
#include <utils/snapmgr.h>
#include <utils/timestamp.h>

#include "../include/bgw/timer.h"

#define MAX_TIMEOUT (5 * INT64CONST(1000))
#define MILLISECS_PER_SEC INT64CONST(1000)
#define USECS_PER_MILLISEC INT64CONST(1000)

static inline void
on_postmaster_death(void)
{
	/*
	 * Don't call exit hooks cause we want to bail out quickly. We don't care
	 * about cleaning up shared memory in this case anyway since it's
	 * potentially corrupt.
	 */
	on_exit_reset();
	ereport(FATAL,
			(errcode(ERRCODE_ADMIN_SHUTDOWN),
			 errmsg("postmaster exited while time_series scheduler was working")));
}

static int64
get_timeout_millisec(TimestampTz by_time)
{
	long		timeout_sec = 0;
	int			timeout_usec = 0;

	if (TIMESTAMP_IS_NOBEGIN(by_time))
		return 0;

	if (TIMESTAMP_IS_NOEND(by_time))
		return PG_INT64_MAX;

	TimestampDifference(GetCurrentTimestamp(), by_time,
						&timeout_sec, &timeout_usec);

	if (timeout_sec < 0 || timeout_usec < 0)
		return 0;

	return (int64) ((timeout_sec * MILLISECS_PER_SEC) +
					(((int64) timeout_usec) / USECS_PER_MILLISEC));
}

static bool
wait_using_wait_latch(TimestampTz until)
{
	int			wl_rc;

	int64		timeout = get_timeout_millisec(until);

	Assert(timeout >= 0 && "get_timeout_millisec underflow");

	if (timeout > MAX_TIMEOUT)
		timeout = MAX_TIMEOUT;

	/* Wait latch requires timeout to be <= INT_MAX */
	if (timeout > (int64) INT_MAX)
		timeout = INT_MAX;

	wl_rc = WaitLatch(MyLatch,
					  WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					  timeout,
					  PG_WAIT_EXTENSION);
	ResetLatch(MyLatch);
	if (wl_rc & WL_POSTMASTER_DEATH)
		on_postmaster_death();

	return true;
}

static const Timer standard_timer = {
	.get_current_timestamp = GetCurrentTimestamp,
	.wait = wait_using_wait_latch,
};

static const Timer *current_timer_implementation = &standard_timer;

static inline const Timer *
timer_get()
{
	return current_timer_implementation;
}

bool
timer_wait(TimestampTz until)
{
	return timer_get()->wait(until);
}

TimestampTz
timer_get_current_timestamp()
{
	return timer_get()->get_current_timestamp();
}

void
timer_set(const Timer * timer)
{
	current_timer_implementation = timer;
}

const Timer*
get_standard_timer()
{
	return &standard_timer;
}

/*
 * ts_job_now -- SQL-callable accessor for the job timeline's clock.
 *
 * The extension SQL (add_job, alter_job, add_*_policy, the job history
 * retention policies) writes and compares timestamps that the scheduler
 * later reads back.  Those call sites must not use SQL now(): the
 * mock-time test suite replaces the timer implementation with a virtual
 * clock (timer_set in test/mock_time), and a now() taken from the real
 * wall clock would then be compared against, or subtracted from, a
 * virtual-clock timestamp.  That mix silently randomizes next_start
 * arithmetic -- see the fixed-schedule slot cap in
 * calculate_next_start_on_failure().
 *
 * In production current_timer_implementation is standard_timer, so this
 * is GetCurrentTimestamp(): identical to now() except that it advances
 * within a transaction (statement time rather than transaction start),
 * which is why the SQL declaration is VOLATILE.
 *
 * current_timer_implementation is process-local, so the value depends on
 * WHICH process evaluates the call.  Callers must resolve it into a local
 * variable on the coordinator and use that; calling it inline in a query
 * that also touches a distributed table lets a QE evaluate it, and a QE
 * has neither the mock timer nor (in production) the coordinator's host
 * clock.  SQL now() is immune because the coordinator's transaction
 * timestamp is dispatched to the segments.
 */
PG_FUNCTION_INFO_V1(ts_job_now);
Datum
ts_job_now(PG_FUNCTION_ARGS)
{
	PG_RETURN_TIMESTAMPTZ(timer_get_current_timestamp());
}
