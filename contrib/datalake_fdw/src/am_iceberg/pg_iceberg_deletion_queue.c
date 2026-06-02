/*-------------------------------------------------------------------------
 *
 * pg_iceberg_deletion_queue.c
 *	  This file contains routines to support creation and management of
 *    the iceberg deletion queue table.
 *
 * The iceberg.pg_iceberg_deletion_queue table stores paths that need
 * to be cleaned up asynchronously. When an iceberg table is dropped,
 * its metadata location is enqueued here. A separate background module
 * can later process these entries: either deleting the file directly
 * or parsing the metadata file and deleting all referenced contents.
 *
 * Table schema:
 *   - path:           TEXT    (primary key) - file or metadata path
 *   - table_name:     OID    - OID of the originating relation
 *   - orphaned_at:    TIMESTAMPTZ - when the entry was created
 *   - retry_count:    INT4   - number of failed removal attempts
 *   - deletion_type:  INT4   - 0 = delete file, 1 = parse metadata
 *
 * All insert/update/delete operations are implemented through the
 * PostgreSQL catalog API (heap_form_tuple, CatalogTupleInsert, etc.)
 * following the same patterns as pg_iceberg_metadata.c.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/am_iceberg/pg_iceberg_deletion_queue.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/table.h"
#include "access/heapam.h"
#include "access/genam.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"
#include "utils/timestamp.h"
#include "utils/fmgroids.h"
#include "utils/syscache.h"

#include "include/iceberg_oids.h"
#include "include/pg_iceberg_metadata.h"
#include "include/pg_iceberg_deletion_queue.h"
#include "include/pg_iceberg_spi_utilities.h"

/* Default values for deletion queue cleanup logic */
#define OrphanedFileRetentionPeriod 3600
#define VacuumFileRemoveMaxRetries 5
#define PER_LOOP_FILE_CLEANUP_LIMIT 1000

/*
 * Qualified name used inside SPI SQL strings.  Hard-coded because the
 * table is pinned by OID (ICEBERG_DELETION_QUEUE_RELID) at initdb time;
 * renaming would require both the OID pin and this string to change.
 */
#define DELETION_QUEUE_TABLE "pg_ext_aux.pg_iceberg_deletion_queue"


/* ----------------------------------------------------------------
 * Helper: open the deletion queue table and its index by stable OIDs
 * pinned by iceberg-cdbinit--1.0.sql.  See iceberg_oids.h.
 * ----------------------------------------------------------------
 */
static void
open_deletion_queue_rel(Relation *rel_out, Oid *index_oid_out,
						LOCKMODE lockmode)
{
	*rel_out = table_open(ICEBERG_DELETION_QUEUE_RELID, lockmode);

	if (index_oid_out)
		*index_oid_out = ICEBERG_DELETION_QUEUE_PKEY_OID;
}

/* ================================================================
 * Insert
 * ================================================================
 */

/*
 * pg_iceberg_deletion_queue_insert
 *		Add a new entry into the deletion queue.
 *
 * Parameters:
 *   path            - the file or metadata path to enqueue
 *   table_oid       - OID of the originating iceberg relation (stale post-DROP)
 *   volume_name     - pg_foreign_volume.fvname; the consumer reconstructs
 *                     fileIOConfig through this + server_name (may be NULL)
 *   server_name     - pg_foreign_server.srvname; same (may be NULL)
 *   owner_username  - rolname of the DROP executor; pg_user_mapping lookup key
 *                     so the consumer reuses the same credentials (may be NULL)
 *   table_qname     - "nspname.relname" snapshot for audit/log because the
 *                     regclass loses meaning after DROP (may be NULL)
 *   orphaned_at     - timestamp when the entry was orphaned
 *   deletion_type   - DELETION_TYPE_FILE or DELETION_TYPE_METADATA
 *
 * last_error is left NULL on insert; it gets populated only after a
 * failed consumer attempt (see pg_iceberg_deletion_queue_record_failure).
 */
void
pg_iceberg_deletion_queue_insert(const char *path,
								 Oid table_oid,
								 const char *volume_name,
								 const char *server_name,
								 const char *owner_username,
								 const char *table_qname,
								 TimestampTz orphaned_at,
								 DeletionType deletion_type)
{
	Relation	queue_rel;
	Datum		values[Natts_deletion_queue];
	bool		nulls[Natts_deletion_queue];
	HeapTuple	tuple;

	open_deletion_queue_rel(&queue_rel, NULL, RowExclusiveLock);

	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));

	Assert(path != NULL);
	values[Anum_deletion_queue_path - 1] = CStringGetTextDatum(path);

	values[Anum_deletion_queue_table_name - 1] = ObjectIdGetDatum(table_oid);

	if (orphaned_at != 0)
		values[Anum_deletion_queue_orphaned_at - 1] =
			TimestampTzGetDatum(orphaned_at);
	else
		nulls[Anum_deletion_queue_orphaned_at - 1] = true;

	values[Anum_deletion_queue_retry_count - 1] = Int32GetDatum(0);

	values[Anum_deletion_queue_deletion_type - 1] =
		Int32GetDatum((int32) deletion_type);

	if (volume_name)
		values[Anum_deletion_queue_volume_name - 1] = CStringGetTextDatum(volume_name);
	else
		nulls[Anum_deletion_queue_volume_name - 1] = true;

	if (server_name)
		values[Anum_deletion_queue_server_name - 1] = CStringGetTextDatum(server_name);
	else
		nulls[Anum_deletion_queue_server_name - 1] = true;

	if (owner_username)
		values[Anum_deletion_queue_owner_username - 1] = CStringGetTextDatum(owner_username);
	else
		nulls[Anum_deletion_queue_owner_username - 1] = true;

	if (table_qname)
		values[Anum_deletion_queue_table_qname - 1] = CStringGetTextDatum(table_qname);
	else
		nulls[Anum_deletion_queue_table_qname - 1] = true;

	/* last_error is always NULL on insert */
	nulls[Anum_deletion_queue_last_error - 1] = true;

	tuple = heap_form_tuple(queue_rel->rd_att, values, nulls);
	CatalogTupleInsert(queue_rel, tuple);
	heap_freetuple(tuple);

	table_close(queue_rel, RowExclusiveLock);
}

/* ================================================================
 * Update - Record Failure (retry_count++ + last_error)
 * ================================================================
 */

/*
 * pg_iceberg_deletion_queue_record_failure
 *		Atomic retry_count++ AND last_error update for the given path.
 *
 * The consumer (pg_iceberg_av_consumer.c) calls this when an agent
 * cleanup attempt fails but retry_count + 1 < max_retry, so the entry
 * stays in the queue for another round.  errmsg is pstrdup'd into the
 * row; pass the message from CopyErrorData()->message.
 *
 * For pure retry-count bump without overwriting last_error, use
 * pg_iceberg_deletion_queue_increment_retry instead (legacy path).
 */
void
pg_iceberg_deletion_queue_record_failure(const char *path,
										 const char *err_text)
{
	Relation	queue_rel;
	Oid			index_oid;
	ScanKeyData skey[1];
	SysScanDesc scan;
	HeapTuple	tuple;
	HeapTuple	newtuple;
	Datum		values[Natts_deletion_queue];
	bool		nulls[Natts_deletion_queue];
	bool		replaces[Natts_deletion_queue];
	Datum		old_retry_datum;
	bool		old_retry_isnull;

	open_deletion_queue_rel(&queue_rel, &index_oid, RowExclusiveLock);

	ScanKeyInit(&skey[0],
				Anum_deletion_queue_path,
				BTEqualStrategyNumber, F_TEXTEQ,
				CStringGetTextDatum(path));

	scan = systable_beginscan(queue_rel, index_oid, true, NULL, 1, skey);
	tuple = systable_getnext(scan);

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("deletion queue entry not found for path \"%s\"", path)));

	old_retry_datum = heap_getattr(tuple,
								   Anum_deletion_queue_retry_count,
								   RelationGetDescr(queue_rel),
								   &old_retry_isnull);

	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));
	memset(replaces, false, sizeof(replaces));

	replaces[Anum_deletion_queue_retry_count - 1] = true;
	values[Anum_deletion_queue_retry_count - 1] =
		Int32GetDatum(old_retry_isnull ? 1 : DatumGetInt32(old_retry_datum) + 1);

	replaces[Anum_deletion_queue_last_error - 1] = true;
	if (err_text && err_text[0] != '\0')
		values[Anum_deletion_queue_last_error - 1] = CStringGetTextDatum(err_text);
	else
		nulls[Anum_deletion_queue_last_error - 1] = true;

	newtuple = heap_modify_tuple(tuple, RelationGetDescr(queue_rel),
								 values, nulls, replaces);
	CatalogTupleUpdate(queue_rel, &tuple->t_self, newtuple);
	heap_freetuple(newtuple);

	systable_endscan(scan);
	table_close(queue_rel, RowExclusiveLock);
}

/* ================================================================
 * Update - Increment Retry Count
 * ================================================================
 */

/*
 * pg_iceberg_deletion_queue_increment_retry
 *		Increment retry_count for the entry with the given path.
 */
void
pg_iceberg_deletion_queue_increment_retry(const char *path)
{
	Relation	queue_rel;
	Oid			index_oid;
	ScanKeyData skey[1];
	SysScanDesc scan;
	HeapTuple	tuple;
	HeapTuple	newtuple;
	Datum		values[Natts_deletion_queue];
	bool		nulls[Natts_deletion_queue];
	bool		replaces[Natts_deletion_queue];
	Datum		old_retry_datum;
	bool		old_retry_isnull;

	open_deletion_queue_rel(&queue_rel, &index_oid, RowExclusiveLock);

	/* Search by path using unique index */
	ScanKeyInit(&skey[0],
				Anum_deletion_queue_path,
				BTEqualStrategyNumber, F_TEXTEQ,
				CStringGetTextDatum(path));

	scan = systable_beginscan(queue_rel, index_oid, true, NULL, 1, skey);
	tuple = systable_getnext(scan);

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("deletion queue entry not found for path \"%s\"", path)));

	/* Get current retry_count */
	old_retry_datum = heap_getattr(tuple,
								   Anum_deletion_queue_retry_count,
								   RelationGetDescr(queue_rel),
								   &old_retry_isnull);

	/* Build replacement tuple */
	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));
	memset(replaces, false, sizeof(replaces));

	replaces[Anum_deletion_queue_retry_count - 1] = true;
	values[Anum_deletion_queue_retry_count - 1] =
		Int32GetDatum(old_retry_isnull ? 1 : DatumGetInt32(old_retry_datum) + 1);

	newtuple = heap_modify_tuple(tuple, RelationGetDescr(queue_rel),
								 values, nulls, replaces);
	CatalogTupleUpdate(queue_rel, &tuple->t_self, newtuple);
	heap_freetuple(newtuple);

	systable_endscan(scan);
	table_close(queue_rel, RowExclusiveLock);
}

/* ================================================================
 * Delete
 * ================================================================
 */

/*
 * pg_iceberg_deletion_queue_remove
 *		Remove the entry for the given path from the deletion queue.
 */
void
pg_iceberg_deletion_queue_remove(const char *path)
{
	Relation	queue_rel;
	Oid			index_oid;
	ScanKeyData skey[1];
	SysScanDesc scan;
	HeapTuple	tuple;

	open_deletion_queue_rel(&queue_rel, &index_oid, RowExclusiveLock);

	/* Search by path using unique index */
	ScanKeyInit(&skey[0],
				Anum_deletion_queue_path,
				BTEqualStrategyNumber, F_TEXTEQ,
				CStringGetTextDatum(path));

	scan = systable_beginscan(queue_rel, index_oid, true, NULL, 1, skey);
	tuple = systable_getnext(scan);

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("deletion queue entry not found for path \"%s\"", path)));

	CatalogTupleDelete(queue_rel, &tuple->t_self);

	systable_endscan(scan);
	table_close(queue_rel, RowExclusiveLock);
}

/* ================================================================
 * Query - Get All Entries
 * ================================================================
 */

/*
 * pg_iceberg_deletion_queue_get_all
 *		Retrieve all entries from the deletion queue.
 *
 * Returns a List of DeletionQueueEntry pointers allocated in the
 * caller's memory context.
 */
List *
pg_iceberg_deletion_queue_get_all(Oid relationId, bool isFull)
{
	MemoryContext caller_context = CurrentMemoryContext;
	List	   *result = NIL;
	StringInfo	query = makeStringInfo();

	appendStringInfo(query, "WITH del AS (");

	if (OidIsValid(relationId))
	{
		appendStringInfo(query,
						 "    SELECT ctid, path, orphaned_at, retry_count, deletion_type, table_name "
						 "    FROM " DELETION_QUEUE_TABLE " "
						 "    WHERE (orphaned_at IS NULL or pg_catalog.now() >= (orphaned_at + INTERVAL '%d seconds')) AND "
						 "		  table_name = %u AND retry_count <= %d FOR UPDATE",
						 OrphanedFileRetentionPeriod, relationId, VacuumFileRemoveMaxRetries);
	}
	else
	{
		/*
		 * This is for dropped tables, so join with pg_class to find all
		 * entries in the deletion queue table that are not associated with
		 * any existing table.
		 */
		appendStringInfo(query,
						 "    SELECT del.ctid, del.path, del.orphaned_at, del.retry_count, del.deletion_type, del.table_name "
						 "    FROM " DELETION_QUEUE_TABLE " del "
						 "    LEFT JOIN pg_catalog.pg_class c ON c.oid = del.table_name "
						 "    WHERE (del.orphaned_at IS NULL or pg_catalog.now() >= (del.orphaned_at + INTERVAL '%d seconds')) AND "
						 "          c.oid IS NULL AND retry_count <= %d FOR UPDATE OF del",
						 OrphanedFileRetentionPeriod, VacuumFileRemoveMaxRetries);
	}

	if (!isFull)
	{
		appendStringInfo(query, " LIMIT %d", PER_LOOP_FILE_CLEANUP_LIMIT);
	}

	appendStringInfo(query, ") SELECT path, orphaned_at, retry_count, deletion_type, table_name FROM del");

	SPI_START();

	bool readOnly = false;
	SPI_execute(query->data, readOnly, 0);

	for (int rowIndex = 0; rowIndex < SPI_processed; rowIndex++)
	{
		bool		isNull;
		MemoryContext old_context = MemoryContextSwitchTo(caller_context);

		DeletionQueueEntry *entry = palloc0(sizeof(DeletionQueueEntry));

		/* We need to use ColumnNumber (1-indexed) in GET_SPI_VALUE */
		entry->path = GET_SPI_VALUE(TEXTOID, rowIndex, 1, &isNull);
		entry->orphaned_at = GET_SPI_VALUE(TIMESTAMPTZOID, rowIndex, 2, &isNull);
		entry->retry_count = GET_SPI_VALUE(INT4OID, rowIndex, 3, &isNull);
		entry->deletion_type = (DeletionType) GET_SPI_VALUE(INT4OID, rowIndex, 4, &isNull);
		entry->table_oid = GET_SPI_VALUE(OIDOID, rowIndex, 5, &isNull);

		result = lappend(result, entry);

		MemoryContextSwitchTo(old_context);
	}

	SPI_END();

	pfree(query->data);
	pfree(query);

	return result;
}

/* ================================================================
 * Query - Get Batch (consumer-side)
 * ================================================================
 */

/*
 * pg_iceberg_deletion_queue_get_batch
 *		Return up to batch_size pending entries (retry_count < max_retry),
 *		FIFO ordered by orphaned_at.
 *
 * Unlike _get_all (which has orphan-detection semantics tied to a specific
 * relationId or filters on rows whose table OID no longer exists), the
 * consumer-side _get_batch is a simple bounded scan suitable for the
 * AutoVacWorkerPostHook loop.  All 10 columns are projected because the
 * consumer needs the credential context (volume/server/owner) to invoke
 * dlagent.
 *
 * Returns a List of DeletionQueueEntry pointers palloc'd in the caller's
 * memory context.  Strings inside each entry are also palloc'd in caller
 * context so they survive after SPI_END().
 */
List *
pg_iceberg_deletion_queue_get_batch(int batch_size, int max_retry)
{
	MemoryContext caller_context = CurrentMemoryContext;
	List	   *result = NIL;
	StringInfo	query = makeStringInfo();

	appendStringInfo(query,
					 "SELECT path, table_name, orphaned_at, retry_count, deletion_type, "
					 "       volume_name, server_name, owner_username, table_qname, last_error "
					 "FROM " DELETION_QUEUE_TABLE " "
					 "WHERE retry_count < %d "
					 "ORDER BY orphaned_at NULLS FIRST "
					 "LIMIT %d",
					 max_retry, batch_size);

	SPI_START();

	{
		bool readOnly = true;
		SPI_execute(query->data, readOnly, 0);
	}

	for (int rowIndex = 0; rowIndex < SPI_processed; rowIndex++)
	{
		bool		isNull;
		MemoryContext old_context = MemoryContextSwitchTo(caller_context);
		DeletionQueueEntry *entry = palloc0(sizeof(DeletionQueueEntry));

		entry->path           = GET_SPI_VALUE(TEXTOID,      rowIndex, 1, &isNull);
		entry->table_oid      = GET_SPI_VALUE(OIDOID,       rowIndex, 2, &isNull);
		entry->orphaned_at    = GET_SPI_VALUE(TIMESTAMPTZOID, rowIndex, 3, &isNull);
		entry->retry_count    = GET_SPI_VALUE(INT4OID,      rowIndex, 4, &isNull);
		entry->deletion_type  = (DeletionType) GET_SPI_VALUE(INT4OID, rowIndex, 5, &isNull);
		entry->volume_name    = GET_SPI_VALUE(TEXTOID,      rowIndex, 6, &isNull);
		entry->server_name    = GET_SPI_VALUE(TEXTOID,      rowIndex, 7, &isNull);
		entry->owner_username = GET_SPI_VALUE(TEXTOID,      rowIndex, 8, &isNull);
		entry->table_qname    = GET_SPI_VALUE(TEXTOID,      rowIndex, 9, &isNull);
		entry->last_error     = GET_SPI_VALUE(TEXTOID,      rowIndex, 10, &isNull);

		result = lappend(result, entry);

		MemoryContextSwitchTo(old_context);
	}

	SPI_END();

	pfree(query->data);
	pfree(query);

	return result;
}

/* ================================================================
 * Move to Dead-Letter Queue (consumer-side)
 * ================================================================
 */

/*
 * pg_iceberg_deletion_queue_move_to_failed
 *		Atomic move: copy the queue row into pg_iceberg_deletion_failed
 *		with failed_at = now() and last_error overwritten by final_errmsg,
 *		then delete the original row from the queue.
 *
 * Called by the consumer when retry_count + 1 would reach max_retry, so the
 * entry exits the active queue and lives in the DLQ until an operator
 * either triages it (SELECT * FROM pg_iceberg_deletion_failed WHERE ...)
 * or replays it (SELECT pg_iceberg_retry_failed_deletion(path)).
 *
 * Both INSERT and DELETE run in the same SPI sub-transaction so they're
 * atomic; if either fails the queue row stays, the DLQ row is rolled back,
 * and the consumer's outer PG_CATCH falls back to record_failure.
 */
void
pg_iceberg_deletion_queue_move_to_failed(const char *path,
										 const char *final_errmsg)
{
	StringInfo	insert_sql = makeStringInfo();
	StringInfo	delete_sql = makeStringInfo();
	Oid			argtypes[2];
	Datum		values[2];
	char		nulls[2];

	Assert(path != NULL);

	/*
	 * INSERT INTO failed (...) SELECT (..., last_error=$2, failed_at=now())
	 * FROM queue WHERE path = $1
	 *
	 * Doing it as one SQL statement keeps it readable and lets the planner
	 * handle the NULL coalescing if final_errmsg is NULL.
	 */
	appendStringInfo(insert_sql,
					 "INSERT INTO pg_ext_aux.pg_iceberg_deletion_failed "
					 "    (path, table_name, orphaned_at, retry_count, deletion_type, "
					 "     volume_name, server_name, owner_username, table_qname, "
					 "     last_error, failed_at) "
					 "SELECT path, table_name, orphaned_at, retry_count, deletion_type, "
					 "       volume_name, server_name, owner_username, table_qname, "
					 "       $2, pg_catalog.now() "
					 "FROM " DELETION_QUEUE_TABLE " WHERE path = $1");

	appendStringInfo(delete_sql,
					 "DELETE FROM " DELETION_QUEUE_TABLE " WHERE path = $1");

	argtypes[0] = TEXTOID;
	argtypes[1] = TEXTOID;
	values[0]   = CStringGetTextDatum(path);
	nulls[0]    = ' ';

	if (final_errmsg && final_errmsg[0] != '\0')
	{
		values[1] = CStringGetTextDatum(final_errmsg);
		nulls[1]  = ' ';
	}
	else
	{
		values[1] = (Datum) 0;
		nulls[1]  = 'n';
	}

	SPI_START();

	{
		bool readOnly = false;
		int rc;

		rc = SPI_execute_with_args(insert_sql->data, 2, argtypes,
								   values, nulls, readOnly, 0);
		if (rc != SPI_OK_INSERT)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("failed to insert into pg_iceberg_deletion_failed for path \"%s\": SPI rc=%d",
							path, rc)));

		rc = SPI_execute_with_args(delete_sql->data, 1, argtypes,
								   values, nulls, readOnly, 0);
		if (rc != SPI_OK_DELETE)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("failed to delete from pg_iceberg_deletion_queue for path \"%s\": SPI rc=%d",
							path, rc)));
	}

	SPI_END();

	pfree(insert_sql->data);
	pfree(insert_sql);
	pfree(delete_sql->data);
	pfree(delete_sql);
}
