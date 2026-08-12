/*-------------------------------------------------------------------------
 *
 * bgw_init.c
 *    Umbrella initialization for the time_series bgworker subsystem.
 *
 *    Consolidates every step that _PG_init would otherwise have to
 *    perform to bring up the bgworker layer: GUC definitions
 *    (bgw_counter + scheduler), shared-memory reservations for the
 *    bgw_counter and bgw_message_queue segments, chaining of the
 *    shmem_startup_hook that initialises those segments, and
 *    registration of the per-cluster launcher.
 *
 *    Keeping this glue next to the rest of bgw/ (rather than inside
 *    time_series.c) means _PG_init only needs a single ts_bgw_init()
 *    call and no ownership of BGW-private state.
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 * IDENTIFICATION
 *    contrib/time_series/src/bgw/bgw_init.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"
#include "storage/ipc.h"

#include "bgw_counter.h"
#include "../include/bgw/bgw_init.h"
#include "bgw_message_queue.h"

#include "launcher.h"
#include "scheduler.h"

/*
 * Chain to any previously-installed shmem_startup_hook before running
 * our own.  shared_preload_libraries are loaded in listed order so a
 * library after us could have set the hook; preserve its work.
 */
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

static void
ts_shmem_startup_hook(void)
{
	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();
	ts_bgw_counter_shmem_startup();
	ts_bgw_message_queue_shmem_startup();
}

void
ts_bgw_init(void)
{
	/*
	 * Scheduler GUCs (bgw_enabled, bgw_launcher_poll_time,
	 * max_background_workers, bgw_log_level, bgw_scheduler_restart_time,
	 * shutdown_bgw_scheduler, debug_bgw_scheduler_exit_status).
	 */
	bgw_define_gucs();

	/*
	 * Bgworker counter GUC.  The matching shared-memory request only
	 * makes sense inside shared_preload_libraries, since
	 * RequestAddinShmemSpace is illegal outside that window.  Skipping
	 * it in the lazy CREATE EXTENSION case matches PostgreSQL's usual
	 * contrib pattern: the postmaster's existing shmem layout is
	 * reused, and the extension only adds bgworker slots when
	 * preloaded.
	 */
	ts_bgw_counter_setup_gucs();

	if (process_shared_preload_libraries_in_progress)
	{
		ts_bgw_counter_shmem_request();
		ts_bgw_message_queue_shmem_request();

		prev_shmem_startup_hook = shmem_startup_hook;
		shmem_startup_hook = ts_shmem_startup_hook;
	}

	/*
	 * Register the per-cluster launcher.  Coordinator-only; the
	 * launcher itself dynamically spawns one scheduler per database
	 * that has run CREATE EXTENSION time_series.
	 */
	bgw_register_launcher();
}
