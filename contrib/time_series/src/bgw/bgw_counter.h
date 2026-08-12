/*-------------------------------------------------------------------------
 *
 * bgw_counter.h
 *    Shared-memory background-worker counter for time_series.
 *
 * Caps the number of bgworkers (launcher + per-DB schedulers) the
 * extension can hold simultaneously, independent of PostgreSQL's global
 * max_worker_processes.  Mirrors TimescaleDB's bgw_counter.c.
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 * IDENTIFICATION
 *    contrib/time_series/src/bgw/bgw_counter.h
 *-------------------------------------------------------------------------
 */
#ifndef TIME_SERIES_BGW_COUNTER_H
#define TIME_SERIES_BGW_COUNTER_H

#include "postgres.h"

extern int guc_bgw_max_background_workers;

/* shared_preload_libraries-time setup */
extern void ts_bgw_counter_shmem_request(void);   /* called from shmem_request_hook */
extern void ts_bgw_counter_shmem_startup(void);   /* called from shmem_startup_hook */
extern void ts_bgw_counter_setup_gucs(void);

/* Reset counter to 0 — called once when the launcher (re)starts. */
extern void ts_bgw_counter_reinit(void);

/*
 * Atomically increment the live-worker count.  Returns true if the
 * increment kept the total at or below time_series.max_background_workers,
 * false otherwise (caller should not actually register the worker).
 */
extern bool ts_bgw_total_workers_increment(void);

/*
 * Decrement the live-worker count.  Refuses to go below 1 (the launcher
 * occupies the first slot for its entire lifetime).
 */
extern void ts_bgw_total_workers_decrement(void);

extern int  ts_bgw_total_workers_get(void);

#endif /* TIME_SERIES_BGW_COUNTER_H */
