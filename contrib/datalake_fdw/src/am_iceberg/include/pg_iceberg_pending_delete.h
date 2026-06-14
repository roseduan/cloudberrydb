/*-------------------------------------------------------------------------
 *
 * pg_iceberg_pending_delete.h
 *    Abort-time cleanup of iceberg staging files via the PendingRelDelete
 *    framework.
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 * IDENTIFICATION
 *        contrib/datalake_fdw/src/am_iceberg/include/pg_iceberg_pending_delete.h
 *-------------------------------------------------------------------------
 */
#ifndef PG_ICEBERG_PENDING_DELETE_H
#define PG_ICEBERG_PENDING_DELETE_H

#include "postgres.h"
#include "utils/rel.h"

/*
 * Register a staging file to be deleted from object storage if (and only if)
 * the current transaction aborts.  `gopher_opt` is the gopherOptions* used to
 * write the file; it is deep-copied into TopMemoryContext so the abort callback
 * can rebuild a filesystem handle without touching catalogs.
 *
 * Callable from the C++ provider writers via extern "C".
 */
extern void iceberg_register_staging_pending_delete(Relation rel,
													const char *path,
													void *gopher_opt);

#endif							/* PG_ICEBERG_PENDING_DELETE_H */
