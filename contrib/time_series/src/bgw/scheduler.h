/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 *
 * Portions Copyright (c) 2025-2026, HashData Technology Limited.
 */
#ifndef BGW_SCHEDULER_H
#define BGW_SCHEDULER_H

#include <postgres.h>
#include <fmgr.h>
#include <nodes/pg_list.h>
#include <postmaster/bgworker.h>

#include "../include/bgw/timer.h"

/* ----------------------------------------------------------------
 * BgwParams: parameter packet that the scheduler memcpy's into
 * BackgroundWorker.bgw_extra (PG limits this to 128 bytes) when
 * registering a dynamic worker.  The worker process reads it back
 * out in its entrypoint.
 *
 * Do not add fields that can't be memcpy'd verbatim — bgw_extra
 * is a fixed-size opaque blob, no send/recv hooks.
 * ----------------------------------------------------------------
 */
typedef struct BgwParams
{
	/* User oid to run the job as.  Used when initializing the
	 * database connection in the worker process. */
	Oid			user_oid;

	/* Job id to use when executing the job. */
	int32		job_id;

	/* Job-history bookkeeping: which bgw_job_stat_history row this
	 * worker should UPDATE on mark_end. */
	int64		job_history_id;
	TimestampTz	job_history_execution_start;

	/* Name of the C entrypoint function to invoke after the worker
	 * has connected to its database (e.g. "bgw_job_entrypoint"). */
	char		bgw_main[BGW_MAXLEN];
} BgwParams;

/* sizeof(BgwParams) must fit in BackgroundWorker.bgw_extra (128 bytes). */
StaticAssertDecl(sizeof(BgwParams) <= sizeof(((BackgroundWorker *) 0)->bgw_extra),
				 "sizeof(BgwParams) exceeds sizeof(bgw_extra) field of BackgroundWorker");

typedef struct ScheduledBgwJob ScheduledBgwJob;

/* callback used in testing */
typedef void (*register_background_worker_callback_type)(BackgroundWorkerHandle *,
														 MemoryContext scheduler_ctx);

/* Exposed for testing */
extern List *update_scheduled_jobs_list(List *cur_jobs_list, MemoryContext mctx);

extern void bgw_scheduler_process(int32 run_for_interval_ms,
									 register_background_worker_callback_type bgw_register);

/* exposed for access by mock */
extern void bgw_scheduler_setup_callbacks(void);

extern void bgw_job_cache_invalidate_callback(void);
extern void bgw_scheduler_register_signal_handlers(void);
extern void bgw_scheduler_setup_mctx(void);

extern BackgroundWorkerHandle *bgw_start_worker(const char *name, const BgwParams *bgw_params);

/* GUC + control flags defined in scheduler.c (referenced by job_stat.c
 * for elog level changes and the in-process shutdown signal). */
extern int  guc_bgw_log_level;
extern bool bgw_shutdown_flag;

/*
 * Define the scheduler-owned GUCs (bgw_enabled, bgw_launcher_poll_time,
 * max_background_workers, bgw_log_level, bgw_scheduler_restart_time,
 * shutdown_bgw_scheduler, debug_bgw_scheduler_exit_status).  Called
 * once from ts_bgw_init.
 */
extern void bgw_define_gucs(void);

#endif /* BGW_SCHEDULER_H */
