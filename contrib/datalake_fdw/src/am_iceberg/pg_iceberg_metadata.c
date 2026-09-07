/*-------------------------------------------------------------------------
 *
 * pg_iceberg_metadata.c
 *	  This file contains routines to support creation and management of
 *    iceberg metadata table.
 *
 * The iceberg.pg_iceberg_metadata table stores metadata information
 * for iceberg tables including metadata location, previous metadata
 * location, internal flag, and default spec id.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/am_iceberg/pg_iceberg_metadata.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/table.h"
#include "access/heapam.h"
#include "access/genam.h"
#include "access/xact.h"
#include "executor/tuptable.h"
#include "catalog/index.h"		/* FormIndexDatum() */
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/syscache.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"
#include "include/iceberg_oids.h"
#include "include/pg_iceberg_metadata.h"

/*
 * The iceberg.pg_iceberg_metadata table is created at initdb time by
 * iceberg-cdbinit--1.0.sql (see #324 follow-up and #339 notes).  Its OID
 * is pinned to ICEBERG_METADATA_RELID; the unique index is pinned to
 * ICEBERG_METADATA_PKEY_OID.  All call sites below address the catalog
 * by these stable OIDs, mirroring how PostgreSQL's built-in BKI
 * catalogs (e.g. ForeignDataWrapperRelationId for
 * pg_foreign_data_wrapper) are accessed -- so name lookups and the
 * "iceberg metadata catalog is not available on this segment" failure
 * mode are gone.
 */

void
pg_iceberg_add_metadata(Oid relid, char *metadata_location,
					  char *previous_metadata_location,
					  bool is_internal, int32 default_spec_id)
{
	Relation	metadata_rel;
	Datum		values[6];
	bool		nulls[6];
	HeapTuple	tuple;
	Oid			metadata_relid;

	/* Iceberg native catalog: OIDs are pinned at initdb (iceberg-cdbinit). */
	metadata_relid = ICEBERG_METADATA_RELID;

	metadata_rel = table_open(metadata_relid, RowExclusiveLock);

	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));

	values[0] = ObjectIdGetDatum(relid);

	Assert(metadata_location != NULL);
	values[1] = CStringGetTextDatum(metadata_location);

	if (previous_metadata_location)
		values[2] = CStringGetTextDatum(previous_metadata_location);
	else
		nulls[2] = true;

	values[3] = BoolGetDatum(is_internal);
	values[4] = Int32GetDatum(default_spec_id);

	/*
	 * metadata_cached_at (issue #393): unset at CREATE so the first external
	 * read triggers a fresh catalog resolve.  Refreshes stamp it later via
	 * pg_iceberg_update_metadata_cache().
	 */
	nulls[5] = true;

	tuple = heap_form_tuple(metadata_rel->rd_att, values, nulls);
	CatalogTupleInsert(metadata_rel, tuple);
	heap_freetuple(tuple);

	table_close(metadata_rel, RowExclusiveLock);
}

/*
 * iceberg_metadata_heap_update
 *    Update a pg_iceberg_metadata tuple, returning the TM_Result instead of
 *    raising on a concurrent modification.
 *
 * CatalogTupleUpdate() goes through simple_heap_update(), which elog(ERROR)s
 * ("tuple concurrently updated") on TM_Updated/TM_Deleted.  Callers here must
 * instead treat a concurrent update gracefully -- a CAS retry, or last-writer-
 * wins for the read-path cache refresh -- so we call heap_update() directly and
 * hand the result code back.
 *
 * On TM_Ok the tuple's indexes are maintained exactly as CatalogTupleUpdate
 * would; CatalogIndexInsert() is static, so its logic is replicated here.
 */
static TM_Result
iceberg_metadata_heap_update(Relation metadata_rel, ItemPointer otid,
							 HeapTuple newtuple)
{
	TM_Result		result;
	TM_FailureData	tmfd;
	LockTupleMode	lockmode;

	result = heap_update(metadata_rel, otid, newtuple,
						 GetCurrentCommandId(true), InvalidSnapshot,
						 true /* wait for commit */,
						 &tmfd, &lockmode);

	if (result == TM_Ok && !HeapTupleIsHeapOnly(newtuple))
	{
		CatalogIndexState indstate;
		int			i;
		int			numIndexes;
		RelationPtr	indexDescs;
		IndexInfo **indexInfos;
		TupleTableSlot *slot;
		Datum		idxvalues[INDEX_MAX_KEYS];
		bool		idxisnull[INDEX_MAX_KEYS];

		indstate = CatalogOpenIndexes(metadata_rel);
		numIndexes = indstate->ri_NumIndices;
		indexDescs = indstate->ri_IndexRelationDescs;
		indexInfos = indstate->ri_IndexRelationInfo;

		slot = MakeSingleTupleTableSlot(RelationGetDescr(metadata_rel),
										&TTSOpsHeapTuple);
		ExecStoreHeapTuple(newtuple, slot, false);

		for (i = 0; i < numIndexes; i++)
		{
			IndexInfo  *indexInfo = indexInfos[i];
			Relation	index = indexDescs[i];

			if (!indexInfo->ii_ReadyForInserts)
				continue;

			FormIndexDatum(indexInfo, slot, NULL, idxvalues, idxisnull);
			index_insert(index, idxvalues, idxisnull,
						 &newtuple->t_self,
						 metadata_rel,
						 index->rd_index->indisunique ?
						 UNIQUE_CHECK_YES : UNIQUE_CHECK_NO,
						 false, indexInfo);
		}

		ExecDropSingleTupleTableSlot(slot);
		CatalogCloseIndexes(indstate);
	}

	return result;
}

/*
 * pg_iceberg_update_metadata_cas
 *    Update metadata location with optional Compare-And-Swap semantics.
 *
 * Atomically updates metadata_location and saves the old value as
 * previous_metadata_location (maintaining Iceberg's metadata lineage).
 *
 * When expected_base is not NULL, verifies that the current catalog
 * metadata_location matches expected_base before updating.  Returns
 * false on mismatch, allowing the caller to re-rebase and retry.
 *
 * This implements Iceberg's standard optimistic concurrency control
 * at commit time, matching the Rust IcebergMetadata::update(expected_base)
 * pattern.
 *
 * Concurrency notes:
 *   The CAS check uses the scan snapshot visible to this transaction.
 *   For concurrent commits already visible in the snapshot, the CAS
 *   check detects the conflict and returns false for a graceful retry.
 *   For truly concurrent commits not yet visible, heap_update's
 *   row-level locking provides the final safety net (ERROR on conflict).
 */
bool
pg_iceberg_update_metadata_cas(Oid relid,
							   const char *metadata_location,
							   const char *expected_base)
{
	Relation	metadata_rel;
	ScanKeyData skey[1];
	SysScanDesc scan;
	HeapTuple	tuple;
	HeapTuple	newtuple;
	Oid			metadata_relid;
	Oid			index_oid;
	Datum		values[6];
	bool		nulls[6];
	bool		replaces[6];
	Datum		old_metadata_datum;
	bool		old_metadata_isnull;

	/* Iceberg native catalog: OIDs are pinned at initdb (iceberg-cdbinit). */
	metadata_relid = ICEBERG_METADATA_RELID;
	index_oid = ICEBERG_METADATA_PKEY_OID;

	metadata_rel = table_open(metadata_relid, RowExclusiveLock);

	/* Search by relid using unique index */
	ScanKeyInit(&skey[0],
				1,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));

	scan = systable_beginscan(metadata_rel, index_oid, true,
							  NULL, 1, skey);
	tuple = systable_getnext(scan);

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("iceberg metadata entry not found for relation %u", relid)));

	/* Get the old metadata_location to save as previous */
	old_metadata_datum = heap_getattr(tuple, 2, RelationGetDescr(metadata_rel),
									  &old_metadata_isnull);
	Assert(!old_metadata_isnull);

	/*
	 * CAS check: verify the catalog hasn't changed since our last rebase.
	 * If the current metadata_location doesn't match what we based our
	 * new metadata on, another transaction committed in between.
	 */
	if (expected_base != NULL)
	{
		char   *old_location = TextDatumGetCString(old_metadata_datum);
		bool	conflict = (strcmp(old_location, expected_base) != 0);

		pfree(old_location);

		if (conflict)
		{
			systable_endscan(scan);
			table_close(metadata_rel, RowExclusiveLock);
			return false;
		}
	}

	/* Initialize replacement arrays */
	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));
	memset(replaces, false, sizeof(replaces));

	/* Update previous_metadata_location with old value */
	replaces[2] = true;  /* column index 3 - previous_metadata_location */
	values[2] = old_metadata_datum;

	/* Update metadata_location with new value */
	replaces[1] = true;  /* column index 2 - metadata_location */
	values[1] = CStringGetTextDatum(metadata_location);

	/* Create the updated tuple */
	newtuple = heap_modify_tuple(tuple, RelationGetDescr(metadata_rel),
								 values, nulls, replaces);

	/*
	 * Update the tuple without raising on a concurrent modification, so we can
	 * detect a conflict via TM_Result and return false for a graceful retry
	 * instead of the ERROR that CatalogTupleUpdate would raise.
	 */
	{
		TM_Result	result;

		result = iceberg_metadata_heap_update(metadata_rel, &tuple->t_self,
											  newtuple);

		switch (result)
		{
			case TM_Ok:
				break;

			case TM_Updated:
			case TM_Deleted:
				/*
				 * Another transaction modified/deleted this tuple.
				 * Clean up and return false to let the caller retry.
				 */
				heap_freetuple(newtuple);
				systable_endscan(scan);
				table_close(metadata_rel, RowExclusiveLock);
				return false;

			case TM_SelfModified:
				elog(ERROR, "iceberg metadata tuple already updated by self");
				break;

			default:
				elog(ERROR, "unrecognized heap_update status: %u", result);
				break;
		}
	}

	heap_freetuple(newtuple);
	systable_endscan(scan);
	table_close(metadata_rel, RowExclusiveLock);

	return true;
}

/*
 * pg_iceberg_update_metadata
 *    Unconditional metadata location update (backward-compatible wrapper).
 */
void
pg_iceberg_update_metadata(Oid relid, const char *metadata_location)
{
	(void) pg_iceberg_update_metadata_cas(relid, metadata_location, NULL);
}

/*
 * pg_iceberg_update_metadata_cache
 *    Write back a read-path external-catalog head refresh (issue #393).
 *
 * Sets metadata_location to the (possibly unchanged) head returned by the
 * agent and stamps metadata_cached_at with the agent-supplied epoch-millis so
 * subsequent reads within the TTL reuse it without hitting the external
 * catalog.  Unlike pg_iceberg_update_metadata_cas() this does NOT roll
 * previous_metadata_location: an external head change is not our own commit,
 * so it must not enter our commit lineage / orphan-cleanup bookkeeping.
 *
 * Last-writer-wins: two concurrent readers may both refresh; the loser's
 * heap_update sees TM_Updated and is silently dropped (see
 * iceberg_metadata_heap_update), which is harmless since both persist a freshly
 * resolved head.  Callers must guard against read-only transactions / hot
 * standby before invoking.
 */
void
pg_iceberg_update_metadata_cache(Oid relid,
								 const char *metadata_location,
								 int64 cached_at_ms)
{
	Relation	metadata_rel;
	ScanKeyData skey[1];
	SysScanDesc scan;
	HeapTuple	tuple;
	HeapTuple	newtuple;
	Oid			metadata_relid;
	Oid			index_oid;
	Datum		values[6];
	bool		nulls[6];
	bool		replaces[6];

	/* Iceberg native catalog: OIDs are pinned at initdb (iceberg-cdbinit). */
	metadata_relid = ICEBERG_METADATA_RELID;
	index_oid = ICEBERG_METADATA_PKEY_OID;

	metadata_rel = table_open(metadata_relid, RowExclusiveLock);

	ScanKeyInit(&skey[0],
				1,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));

	scan = systable_beginscan(metadata_rel, index_oid, true,
							  NULL, 1, skey);
	tuple = systable_getnext(scan);

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("iceberg metadata entry not found for relation %u", relid)));

	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));
	memset(replaces, false, sizeof(replaces));

	/* metadata_location (col 2) */
	if (metadata_location != NULL)
	{
		replaces[1] = true;
		values[1] = CStringGetTextDatum(metadata_location);
	}

	/* metadata_cached_at (col 6) */
	replaces[5] = true;
	values[5] = Int64GetDatum(cached_at_ms);

	newtuple = heap_modify_tuple(tuple, RelationGetDescr(metadata_rel),
								 values, nulls, replaces);

	/*
	 * Last-writer-wins: if a concurrent reader refreshed the same tuple first,
	 * heap_update returns TM_Updated/TM_Deleted.  Their write already persisted
	 * a freshly resolved head, so silently drop ours rather than let
	 * CatalogTupleUpdate raise "tuple concurrently updated" and fail the SELECT.
	 */
	(void) iceberg_metadata_heap_update(metadata_rel, &tuple->t_self, newtuple);

	heap_freetuple(newtuple);
	systable_endscan(scan);
	table_close(metadata_rel, RowExclusiveLock);
}

void
pg_iceberg_remove_metadata(Oid relid)
{
	Relation	metadata_rel;
	ScanKeyData skey[1];
	SysScanDesc scan;
	HeapTuple	tuple;
	Oid			metadata_relid;
	Oid			index_oid;

	/* Iceberg native catalog: OIDs are pinned at initdb (iceberg-cdbinit). */
	metadata_relid = ICEBERG_METADATA_RELID;
	index_oid = ICEBERG_METADATA_PKEY_OID;

	metadata_rel = table_open(metadata_relid, RowExclusiveLock);

	/* Search by relid using unique index */
	ScanKeyInit(&skey[0],
				1,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));

	scan = systable_beginscan(metadata_rel, index_oid, true,
							  NULL, 1, skey);
	tuple = systable_getnext(scan);

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("iceberg metadata entry not found for relation %u", relid)));

	CatalogTupleDelete(metadata_rel, &tuple->t_self);

	systable_endscan(scan);
	table_close(metadata_rel, RowExclusiveLock);
}

static IcebergMetadataInfo *
pg_iceberg_get_metadata_info_internal(Oid relid, bool missing_ok)
{
	Relation	metadata_rel;
	ScanKeyData skey[1];
	SysScanDesc scan;
	HeapTuple	tuple;
	Oid			metadata_relid;
	Oid			index_oid;
	bool		isnull;
	Datum		datum;
	IcebergMetadataInfo *info;

	/* Allocate memory for the result structure */
	info = (IcebergMetadataInfo *) palloc0(sizeof(IcebergMetadataInfo));

	/* Iceberg native catalog: OIDs are pinned at initdb (iceberg-cdbinit). */
	metadata_relid = ICEBERG_METADATA_RELID;
	index_oid = ICEBERG_METADATA_PKEY_OID;

	metadata_rel = table_open(metadata_relid, AccessShareLock);

	/* Search by relid using unique index */
	ScanKeyInit(&skey[0],
				1,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));

	scan = systable_beginscan(metadata_rel, index_oid, true,
							  NULL, 1, skey);
	tuple = systable_getnext(scan);

	if (!HeapTupleIsValid(tuple))
	{
		if (missing_ok)
		{
			systable_endscan(scan);
			table_close(metadata_rel, AccessShareLock);
			pfree(info);
			return NULL;
		}
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("iceberg metadata entry not found for relation %u", relid)));
	}

	/* metadata_location */
	datum = heap_getattr(tuple, 2, RelationGetDescr(metadata_rel), &isnull);
	Assert(!isnull);
	info->metadata_location = TextDatumGetCString(datum);

	/* previous_metadata_location */
	datum = heap_getattr(tuple, 3, RelationGetDescr(metadata_rel), &isnull);
	if (!isnull)
		info->previous_metadata_location = TextDatumGetCString(datum);

	/* is_internal */
	datum = heap_getattr(tuple, 4, metadata_rel->rd_att, &isnull);
	if (!isnull)
		info->is_internal = DatumGetBool(datum);

	/* default_spec_id */
	datum = heap_getattr(tuple, 5, metadata_rel->rd_att, &isnull);
	if (!isnull)
		info->default_spec_id = DatumGetInt32(datum);

	/* metadata_cached_at (issue #393); NULL -> treated as never refreshed */
	datum = heap_getattr(tuple, 6, metadata_rel->rd_att, &isnull);
	if (isnull)
	{
		info->metadata_cached_at = 0;
		info->metadata_cached_at_isnull = true;
	}
	else
	{
		info->metadata_cached_at = DatumGetInt64(datum);
		info->metadata_cached_at_isnull = false;
	}

	systable_endscan(scan);
	table_close(metadata_rel, AccessShareLock);

	return info;
}

IcebergMetadataInfo *
pg_iceberg_get_metadata_info(Oid relid)
{
	return pg_iceberg_get_metadata_info_internal(relid, false);
}

/*
 * Like pg_iceberg_get_metadata_info(), but returns NULL instead of raising
 * an error when the relation has no iceberg metadata entry.
 */
IcebergMetadataInfo *
pg_iceberg_get_metadata_info_missing_ok(Oid relid)
{
	return pg_iceberg_get_metadata_info_internal(relid, true);
}

void
pg_iceberg_free_metadata_info(IcebergMetadataInfo *info)
{
	if (info == NULL)
		return;

	if (info->metadata_location)
		pfree(info->metadata_location);
	if (info->previous_metadata_location)
		pfree(info->previous_metadata_location);

	pfree(info);
}
