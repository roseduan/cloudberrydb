/*-------------------------------------------------------------------------
 *
 * bgw_counter.c
 *    Shared-memory background-worker counter for time_series.
 *
 * Tracks the number of bgworkers (launcher + per-DB schedulers) the
 * extension has live in the cluster.  The launcher consults this counter
 * before calling RegisterDynamicBackgroundWorker so the extension does
 * not exceed its own cap (time_series.max_background_workers), which
 * sits inside PostgreSQL's global max_worker_processes.
 *
 * Adapted from TimescaleDB src/loader/bgw_counter.c (Apache 2.0).
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 * IDENTIFICATION
 *    contrib/time_series/src/bgw/bgw_counter.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "utils/guc.h"

#include "bgw_counter.h"

#define BGW_COUNTER_STATE_NAME "time_series_bgw_counter_state"

int			guc_bgw_max_background_workers = 16;

/*
 * Shared-memory counter.  Protected by an slock_t because every access
 * is a single read/write -- no need for the heavier LWLock machinery.
 */
typedef struct CounterState
{
	slock_t		mutex;
	int			total_workers;
}			CounterState;

static CounterState * ct = NULL;

void
ts_bgw_counter_setup_gucs(void)
{
	DefineCustomIntVariable("time_series.max_background_workers",
							"Maximum bgworker processes the time_series extension may hold",
							"Caps launcher + per-DB schedulers in aggregate.  "
							"Should be at least 1 (launcher) + number of databases "
							"that run CREATE EXTENSION time_series.  Must be set at "
							"postmaster start (PGC_POSTMASTER).",
							&guc_bgw_max_background_workers,
							guc_bgw_max_background_workers,
							0,
							1000,
							PGC_POSTMASTER,
							0,
							NULL, NULL, NULL);
}

void
ts_bgw_counter_shmem_request(void)
{
	RequestAddinShmemSpace(sizeof(CounterState));
}

void
ts_bgw_counter_shmem_startup(void)
{
	bool		found;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	ct = ShmemInitStruct(BGW_COUNTER_STATE_NAME, sizeof(CounterState), &found);
	if (!found)
	{
		memset(ct, 0, sizeof(CounterState));
		SpinLockInit(&ct->mutex);
		ct->total_workers = 0;
	}
	LWLockRelease(AddinShmemInitLock);
}

void
ts_bgw_counter_reinit(void)
{
	SpinLockAcquire(&ct->mutex);
	ct->total_workers = 0;
	SpinLockRelease(&ct->mutex);
}

bool
ts_bgw_total_workers_increment(void)
{
	bool		incremented = false;

	SpinLockAcquire(&ct->mutex);
	if (ct->total_workers + 1 <= guc_bgw_max_background_workers)
	{
		ct->total_workers++;
		incremented = true;
	}
	SpinLockRelease(&ct->mutex);
	return incremented;
}

void
ts_bgw_total_workers_decrement(void)
{
	/*
	 * The launcher occupies slot 1 for its entire lifetime; a per-DB
	 * scheduler decrement should never drive us below it.  Treat under- flow
	 * as a programming error.
	 */
	SpinLockAcquire(&ct->mutex);
	if (ct->total_workers >= 1)
	{
		ct->total_workers--;
		SpinLockRelease(&ct->mutex);
	}
	else
	{
		SpinLockRelease(&ct->mutex);
		ereport(WARNING,
				(errmsg("time_series bgw_counter underflow"),
				 errhint("decrement called with zero live workers; "
						 "counter accounting is out of sync.")));
	}
}

int
ts_bgw_total_workers_get(void)
{
	int			nworkers;

	SpinLockAcquire(&ct->mutex);
	nworkers = ct->total_workers;
	SpinLockRelease(&ct->mutex);
	return nworkers;
}
