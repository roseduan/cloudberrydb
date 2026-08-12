/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 *
 * Portions Copyright (c) 2025-2026, HashData Technology Limited.
 */
#ifndef BGW_LAUNCHER_H
#define BGW_LAUNCHER_H

/*
 * Register the per-cluster launcher as a static background worker.
 * Coordinator-only; the launcher itself dynamically spawns one
 * scheduler per database that has run CREATE EXTENSION time_series.
 * Called once from ts_bgw_init.
 */
extern void bgw_register_launcher(void);

#endif /* BGW_LAUNCHER_H */
