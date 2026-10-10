/*-------------------------------------------------------------------------
 *
 * pg_iceberg_metadata_refresh.h
 *    Refresh of the external-catalog metadata cache (issue #393 follow-up).
 *
 * Two entry points over one core: a batch cycle over every cached table whose
 * stamp is due, driven by a scheduled task through
 * iceberg_refresh_metadata_cache(), and a single-table force refresh behind
 * the owner-only iceberg_refresh_metadata().  See the .c file header for
 * why a refresh is needed on top of the read-path TTL, and why the cadence
 * belongs to the task scheduler rather than to autovacuum.
 *
 * IDENTIFICATION
 *    contrib/datalake_fdw/src/am_iceberg/include/pg_iceberg_metadata_refresh.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef __PG_ICEBERG_METADATA_REFRESH_H__
#define __PG_ICEBERG_METADATA_REFRESH_H__

#include "utils/relcache.h"

/* Define the GUCs that govern the batch cycle.  Call once from _PG_init. */
extern void pg_iceberg_metadata_refresh_init(void);

/*
 * Re-resolve the external-catalog head for every cached table whose stamp is
 * due, persisting each refresh via pg_iceberg_update_metadata_cache().
 * Returns the number of tables actually refreshed.
 *
 * Must run on the QD inside a transaction with an active snapshot.  Each table
 * is processed in its own internal subtransaction, so one unreachable catalog
 * cannot abort the batch.
 */
extern int pg_iceberg_refresh_metadata_cache_batch(int max_tables);

/*
 * Force one table's mirror to its catalog's current head, ignoring both the
 * TTL and the candidate filters the batch cycle applies.  Returns the stored
 * head, or NULL when there was nothing to store (built-in catalog, or an agent
 * that returned no location).  Caller holds a lock on rel.
 */
extern char *pg_iceberg_refresh_table_head(Relation rel);

#endif							/* __PG_ICEBERG_METADATA_REFRESH_H__ */
