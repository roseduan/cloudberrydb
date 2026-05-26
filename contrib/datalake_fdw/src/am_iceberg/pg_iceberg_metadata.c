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
	Datum		values[5];
	bool		nulls[5];
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

	tuple = heap_form_tuple(metadata_rel->rd_att, values, nulls);
	CatalogTupleInsert(metadata_rel, tuple);
	heap_freetuple(tuple);

	table_close(metadata_rel, RowExclusiveLock);
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
	Datum		values[5];
	bool		nulls[5];
	bool		replaces[5];
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
	 * Perform the catalog update manually instead of using CatalogTupleUpdate,
	 * so we can detect concurrent updates via TM_Result and return false
	 * instead of raising an ERROR.
	 *
	 * CatalogTupleUpdate calls simple_heap_update internally, which calls
	 * elog(ERROR) on TM_Updated/TM_Deleted.  By calling heap_update directly,
	 * we can inspect the result code and handle conflicts gracefully.
	 *
	 * After a successful heap_update, we still need to update indexes.
	 * We use CatalogOpenIndexes/CatalogCloseIndexes (public) and replicate the
	 * index insertion logic from CatalogIndexInsert (which is static).
	 */
	{
		TM_Result		result;
		TM_FailureData	tmfd;
		LockTupleMode	lockmode;

		result = heap_update(metadata_rel, &tuple->t_self, newtuple,
							 GetCurrentCommandId(true), InvalidSnapshot,
							 true /* wait for commit */,
							 &tmfd, &lockmode);

		switch (result)
		{
			case TM_Ok:
				{
					/* Success – update indexes like CatalogTupleUpdate does */
					CatalogIndexState indstate;

					indstate = CatalogOpenIndexes(metadata_rel);

					if (!HeapTupleIsHeapOnly(newtuple))
					{
						int			i;
						int			numIndexes = indstate->ri_NumIndices;
						RelationPtr	indexDescs = indstate->ri_IndexRelationDescs;
						IndexInfo **indexInfos = indstate->ri_IndexRelationInfo;
						TupleTableSlot *slot;
						Datum		idxvalues[INDEX_MAX_KEYS];
						bool		idxisnull[INDEX_MAX_KEYS];

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
					}

					CatalogCloseIndexes(indstate);
				}
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

IcebergMetadataInfo *
pg_iceberg_get_metadata_info(Oid relid)
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

	systable_endscan(scan);
	table_close(metadata_rel, AccessShareLock);

	return info;
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
