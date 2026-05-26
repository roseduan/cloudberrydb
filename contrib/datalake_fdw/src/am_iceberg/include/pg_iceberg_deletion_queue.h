/*-------------------------------------------------------------------------
 *
 * pg_iceberg_deletion_queue.h
 *    Iceberg deletion queue management functions
 *
 * The deletion queue stores paths of files/metadata that need to be
 * cleaned up asynchronously. When an iceberg table is dropped, its
 * metadata location is enqueued here for later processing by a
 * background cleanup module.
 *
 * IDENTIFICATION
 *    contrib/datalake_fdw/src/am_iceberg/include/pg_iceberg_deletion_queue.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef __PG_ICEBERG_DELETION_QUEUE_H__
#define __PG_ICEBERG_DELETION_QUEUE_H__

#include "postgres.h"
#include "fmgr.h"
#include "utils/timestamp.h"

/*
 * Catalog OIDs for iceberg.pg_iceberg_deletion_queue are pinned at initdb
 * time by iceberg-cdbinit--1.0.sql and exposed through iceberg_oids.h
 * (ICEBERG_DELETION_QUEUE_RELID, ICEBERG_DELETION_QUEUE_PKEY_OID).
 * C call sites MUST use those constants; the legacy
 * PG_ICEBERG_DELETION_QUEUE_*_NAME macros are gone.
 */

/*
 * DeletionType - indicates how the path should be processed
 *
 *   DELETION_TYPE_FILE     - Delete this single file directly.
 *   DELETION_TYPE_METADATA - Parse this path as a metadata file and
 *                            delete all contents referenced within it.
 */
typedef enum DeletionType
{
	DELETION_TYPE_FILE = 0,
	DELETION_TYPE_METADATA = 1,
} DeletionType;

/*
 * Column attribute numbers for the deletion queue table
 * (1-indexed, matching TupleDescInitEntry order)
 */
#define Anum_deletion_queue_path			1
#define Anum_deletion_queue_table_name		2
#define Anum_deletion_queue_orphaned_at		3
#define Anum_deletion_queue_retry_count		4
#define Anum_deletion_queue_deletion_type	5

#define Natts_deletion_queue				5

/*
 * DeletionQueueEntry - in-memory representation of a deletion queue row
 */
typedef struct DeletionQueueEntry
{
	char		   *path;			/* file/metadata path (primary key) */
	Oid				table_oid;		/* OID of the originating table */
	TimestampTz		orphaned_at;	/* when the entry was orphaned */
	int32			retry_count;	/* number of failed removal attempts */
	DeletionType	deletion_type;	/* how to process the path */
} DeletionQueueEntry;

/* ----------------------------------------------------------------
 * Insert / Update / Delete  (catalog API)
 * ----------------------------------------------------------------
 */

/* Insert a new entry into the deletion queue */
extern void pg_iceberg_deletion_queue_insert(const char *path,
											 Oid table_oid,
											 TimestampTz orphaned_at,
											 DeletionType deletion_type);

/* Increment retry_count for the given path */
extern void pg_iceberg_deletion_queue_increment_retry(const char *path);

/* Remove an entry by path */
extern void pg_iceberg_deletion_queue_remove(const char *path);

/* ----------------------------------------------------------------
 * Query
 * ----------------------------------------------------------------
 */

/* Get all deletion queue entries (caller must pfree the list) */
extern List *pg_iceberg_deletion_queue_get_all(Oid relationId, bool isFull);

#endif /* __PG_ICEBERG_DELETION_QUEUE_H__ */
