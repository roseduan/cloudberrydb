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

#include "include/pg_iceberg_metadata.h"
#include "include/pg_iceberg_deletion_queue.h"
#include "include/pg_iceberg_spi_utilities.h"

/* Default values for deletion queue cleanup logic */
#define OrphanedFileRetentionPeriod 3600
#define VacuumFileRemoveMaxRetries 5
#define PER_LOOP_FILE_CLEANUP_LIMIT 1000
#define DELETION_QUEUE_TABLE PG_ICEBERG_SCHEMA_NAME "." PG_ICEBERG_DELETION_QUEUE_TABLE_NAME


/* ----------------------------------------------------------------
 * Helper: open the deletion queue table and its index
 * ----------------------------------------------------------------
 */
static void
open_deletion_queue_rel(Relation *rel_out, Oid *index_oid_out,
						LOCKMODE lockmode)
{
	Oid		namespaceid;
	Oid		relid;

	namespaceid = get_namespace_oid(PG_ICEBERG_SCHEMA_NAME, false);
	relid = get_relname_relid(PG_ICEBERG_DELETION_QUEUE_TABLE_NAME,
							  namespaceid);

	if (!OidIsValid(relid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("deletion queue table \"%s.%s\" does not exist",
						PG_ICEBERG_SCHEMA_NAME,
						PG_ICEBERG_DELETION_QUEUE_TABLE_NAME)));

	*rel_out = table_open(relid, lockmode);

	if (index_oid_out)
		*index_oid_out = get_relname_relid(
			PG_ICEBERG_DELETION_QUEUE_INDEX_NAME, namespaceid);
}

/*
 * The iceberg.pg_iceberg_deletion_queue table is now created from
 * datalake_fdw--1.0.sql as a plain CREATE TABLE so the DDL gets dispatched
 * to QEs (see issue #324).  The previous CreateIcebergDeletionQueueTable()
 * helper and its SQL-callable wrapper were removed.
 */

/* ================================================================
 * Insert
 * ================================================================
 */

/*
 * pg_iceberg_deletion_queue_insert
 *		Add a new entry into the deletion queue.
 *
 * Parameters:
 *   path          - the file or metadata path to enqueue
 *   table_oid     - OID of the originating iceberg relation
 *   orphaned_at   - timestamp when the entry was orphaned
 *   deletion_type - DELETION_TYPE_FILE or DELETION_TYPE_METADATA
 */
void
pg_iceberg_deletion_queue_insert(const char *path,
								 Oid table_oid,
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

	tuple = heap_form_tuple(queue_rel->rd_att, values, nulls);
	CatalogTupleInsert(queue_rel, tuple);
	heap_freetuple(tuple);

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
