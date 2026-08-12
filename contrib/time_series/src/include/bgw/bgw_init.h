/*-------------------------------------------------------------------------
 *
 * bgw_init.h
 *    Umbrella initialization entry for the time_series bgworker subsystem.
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 * IDENTIFICATION
 *    contrib/time_series/src/include/bgw/bgw_init.h
 *-------------------------------------------------------------------------
 */
#ifndef TS_BGW_INIT_H
#define TS_BGW_INIT_H

/*
 * Wire the bgworker subsystem into the postmaster.
 *
 * Defines every BGW-owned GUC, requests shared-memory slots for the
 * bgw_counter and bgw_message_queue when loaded via
 * shared_preload_libraries, chains a shmem_startup_hook that initialises
 * those slots, and registers the per-cluster launcher.  Must be called
 * exactly once from _PG_init.
 */
extern void ts_bgw_init(void);

#endif							/* TS_BGW_INIT_H */
