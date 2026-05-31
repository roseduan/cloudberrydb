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
#define Anum_deletion_queue_volume_name		6
#define Anum_deletion_queue_server_name		7
#define Anum_deletion_queue_owner_username	8
#define Anum_deletion_queue_table_qname		9
#define Anum_deletion_queue_last_error		10

#define Natts_deletion_queue				10

/*
 * Column attribute numbers for the dead-letter queue table
 * (1-indexed, matching iceberg-cdbinit--1.0.sql order)
 */
#define Anum_deletion_failed_path			1
#define Anum_deletion_failed_table_name		2
#define Anum_deletion_failed_orphaned_at	3
#define Anum_deletion_failed_retry_count	4
#define Anum_deletion_failed_deletion_type	5
#define Anum_deletion_failed_volume_name	6
#define Anum_deletion_failed_server_name	7
#define Anum_deletion_failed_owner_username	8
#define Anum_deletion_failed_table_qname	9
#define Anum_deletion_failed_last_error		10
#define Anum_deletion_failed_failed_at		11

#define Natts_deletion_failed				11

/*
 * DeletionQueueEntry - in-memory representation of a deletion queue row
 */
typedef struct DeletionQueueEntry
{
	char		   *path;			/* file/metadata path (primary key) */
	Oid				table_oid;		/* OID of the originating table (stale post-DROP) */
	TimestampTz		orphaned_at;	/* when the entry was orphaned */
	int32			retry_count;	/* number of failed removal attempts */
	DeletionType	deletion_type;	/* how to process the path */
	char		   *volume_name;	/* pg_foreign_volume.fvname for fileIOConfig reconstruction */
	char		   *server_name;	/* pg_foreign_server.srvname for fileIOConfig reconstruction */
	char		   *owner_username;	/* rolname of DROP executor; pg_user_mapping lookup key */
	char		   *table_qname;	/* "nspname.relname" snapshot for audit/log */
	char		   *last_error;		/* most recent retry failure message (may be NULL) */
} DeletionQueueEntry;

/* ----------------------------------------------------------------
 * Insert / Update / Delete  (catalog API)
 * ----------------------------------------------------------------
 */

/* Insert a new entry into the deletion queue */
extern void pg_iceberg_deletion_queue_insert(const char *path,
											 Oid table_oid,
											 const char *volume_name,
											 const char *server_name,
											 const char *owner_username,
											 const char *table_qname,
											 TimestampTz orphaned_at,
											 DeletionType deletion_type);

/* Increment retry_count for the given path (legacy: no last_error update) */
extern void pg_iceberg_deletion_queue_increment_retry(const char *path);

/* Increment retry_count AND record the failure message in last_error */
extern void pg_iceberg_deletion_queue_record_failure(const char *path,
													 const char *errmsg);

/* Remove an entry by path */
extern void pg_iceberg_deletion_queue_remove(const char *path);

/*
 * Atomic move from queue to dead-letter queue: deletes the queue row and
 * inserts a mirror row (plus failed_at = now() and final_errmsg) into
 * pg_iceberg_deletion_failed.  Called by the consumer when retry_count
 * would exceed datalake_fdw.deletion_queue_max_retry.
 *
 * DLQ rows are terminal: there is no automatic replay path.  Operators
 * inspect pg_iceberg_deletion_failed for triage and clean up manually.
 */
extern void pg_iceberg_deletion_queue_move_to_failed(const char *path,
													 const char *final_errmsg);

/* ----------------------------------------------------------------
 * Query
 * ----------------------------------------------------------------
 */

/* Get all deletion queue entries (caller must pfree the list) */
extern List *pg_iceberg_deletion_queue_get_all(Oid relationId, bool isFull);

/*
 * Get a bounded batch of pending entries (retry_count < max_retry),
 * ordered by orphaned_at ascending so FIFO.  Returns List *DeletionQueueEntry.
 */
extern List *pg_iceberg_deletion_queue_get_batch(int batch_size, int max_retry);

#endif /* __PG_ICEBERG_DELETION_QUEUE_H__ */
