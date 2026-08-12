/*-------------------------------------------------------------------------
 *
 * time_series.h
 *    Module-wide core header for the time_series extension.
 *
 * Declares the extension identity macros, the process-local namespace-OID
 * cache, and the extension state machine used by every hook installed
 * from _PG_init to short-circuit after DROP EXTENSION / pg_upgrade.
 *
 * Subsystem-specific APIs live in per-subsystem headers under
 * include/{bgw,cagg,compress,gapfill,storage,access,time_bucket}/;
 * consumers include only the narrow subsystem headers they need.
 *
 * Copyright (c) 2026 HashData Inc.
 * Licensed under Apache License 2.0
 *
 * IDENTIFICATION
 *    contrib/time_series/src/include/time_series.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TIME_SERIES_H
#define TIME_SERIES_H

#include "postgres.h"
#include "fmgr.h"
#include "nodes/extensible.h"
#include "nodes/pathnodes.h"
#include "optimizer/pathnode.h"
#include "optimizer/planner.h"
#include "utils/rel.h"
#include "utils/relcache.h"

/* ----------------------------------------------------------------
 * Extension identity — single source of truth so renames don't
 * require touching a dozen string literals.
 * ---------------------------------------------------------------- */
#define TS_EXTENSION_NAME         "time_series"
#define TS_EXTENSION_SCHEMA_NAME  "time_series"

/* ----------------------------------------------------------------
 * Namespace cache (time_series.c)
 * ---------------------------------------------------------------- */

/*
 * Returns the OID of the time_series schema, caching the result so repeated
 * lookups within the same session avoid repeated syscache searches.
 */
extern Oid ht_get_namespace_oid_cached(void);

/* ---- Extension state machine (time_series.c) ----
 *
 * Mirrors upstream pattern in src/extension.c: track whether
 * the time_series extension is installed in the current database
 * via a process-local state machine, refreshed lazily and
 * invalidated by relcache callbacks on the proxy table
 * (time_series.continuous_agg).
 *
 * Hooks installed by _PG_init outlive DROP EXTENSION CASCADE
 * (the .so stays loaded), so every hook entry must guard its
 * SPI / catalog access with extension_is_loaded_and_not_upgrading().
 */
extern bool extension_is_loaded(void);
extern bool extension_is_loaded_and_not_upgrading(void);
extern void extension_invalidate(void);

#endif /* TIME_SERIES_H */
