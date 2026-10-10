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
	Datum		values[Natts_pg_iceberg_metadata];
	bool		nulls[Natts_pg_iceberg_metadata];
	HeapTuple	tuple;
	Oid			metadata_relid;

	/* Iceberg native catalog: OIDs are pinned at initdb (iceberg-cdbinit). */
	metadata_relid = ICEBERG_METADATA_RELID;

	metadata_rel = table_open(metadata_relid, RowExclusiveLock);

	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));

	values[Anum_pg_iceberg_metadata_relid - 1] = ObjectIdGetDatum(relid);

	Assert(metadata_location != NULL);
	values[Anum_pg_iceberg_metadata_metadata_location - 1] =
		CStringGetTextDatum(metadata_location);

	if (previous_metadata_location)
		values[Anum_pg_iceberg_metadata_previous_metadata_location - 1] =
			CStringGetTextDatum(previous_metadata_location);
	else
		nulls[Anum_pg_iceberg_metadata_previous_metadata_location - 1] = true;

	values[Anum_pg_iceberg_metadata_is_internal - 1] = BoolGetDatum(is_internal);
	values[Anum_pg_iceberg_metadata_default_spec_id - 1] =
		Int32GetDatum(default_spec_id);

	/*
	 * metadata_cached_at (issue #393): unset at CREATE so the first external
	 * read triggers a fresh catalog resolve.  Refreshes stamp it later via
	 * pg_iceberg_update_metadata_cache().
	 */
	nulls[Anum_pg_iceberg_metadata_metadata_cached_at - 1] = true;

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
	TU_UpdateIndexes update_indexes;

	result = heap_update(metadata_rel, otid, newtuple,
						 GetCurrentCommandId(true), InvalidSnapshot,
						 true /* wait for commit */,
						 &tmfd, &lockmode, &update_indexes);

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
	Datum		values[Natts_pg_iceberg_metadata];
	bool		nulls[Natts_pg_iceberg_metadata];
	bool		replaces[Natts_pg_iceberg_metadata];
	Datum		old_metadata_datum;
	bool		old_metadata_isnull;

	/* Iceberg native catalog: OIDs are pinned at initdb (iceberg-cdbinit). */
	metadata_relid = ICEBERG_METADATA_RELID;
	index_oid = ICEBERG_METADATA_PKEY_OID;

	metadata_rel = table_open(metadata_relid, RowExclusiveLock);

	/* Search by relid using unique index */
	ScanKeyInit(&skey[0],
				Anum_pg_iceberg_metadata_relid,
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
	old_metadata_datum = heap_getattr(tuple,
									  Anum_pg_iceberg_metadata_metadata_location,
									  RelationGetDescr(metadata_rel),
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
	replaces[Anum_pg_iceberg_metadata_previous_metadata_location - 1] = true;
	values[Anum_pg_iceberg_metadata_previous_metadata_location - 1] =
		old_metadata_datum;

	/* Update metadata_location with new value */
	replaces[Anum_pg_iceberg_metadata_metadata_location - 1] = true;
	values[Anum_pg_iceberg_metadata_metadata_location - 1] =
		CStringGetTextDatum(metadata_location);

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
	Datum		values[Natts_pg_iceberg_metadata];
	bool		nulls[Natts_pg_iceberg_metadata];
	bool		replaces[Natts_pg_iceberg_metadata];

	/* Iceberg native catalog: OIDs are pinned at initdb (iceberg-cdbinit). */
	metadata_relid = ICEBERG_METADATA_RELID;
	index_oid = ICEBERG_METADATA_PKEY_OID;

	metadata_rel = table_open(metadata_relid, RowExclusiveLock);

	ScanKeyInit(&skey[0],
				Anum_pg_iceberg_metadata_relid,
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

	if (metadata_location != NULL)
	{
		replaces[Anum_pg_iceberg_metadata_metadata_location - 1] = true;
		values[Anum_pg_iceberg_metadata_metadata_location - 1] =
			CStringGetTextDatum(metadata_location);
	}

	replaces[Anum_pg_iceberg_metadata_metadata_cached_at - 1] = true;
	values[Anum_pg_iceberg_metadata_metadata_cached_at - 1] =
		Int64GetDatum(cached_at_ms);

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
				Anum_pg_iceberg_metadata_relid,
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
				Anum_pg_iceberg_metadata_relid,
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

	datum = heap_getattr(tuple, Anum_pg_iceberg_metadata_metadata_location,
						 RelationGetDescr(metadata_rel), &isnull);
	Assert(!isnull);
	info->metadata_location = TextDatumGetCString(datum);

	datum = heap_getattr(tuple,
						 Anum_pg_iceberg_metadata_previous_metadata_location,
						 RelationGetDescr(metadata_rel), &isnull);
	if (!isnull)
		info->previous_metadata_location = TextDatumGetCString(datum);

	datum = heap_getattr(tuple, Anum_pg_iceberg_metadata_is_internal,
						 metadata_rel->rd_att, &isnull);
	if (!isnull)
		info->is_internal = DatumGetBool(datum);

	datum = heap_getattr(tuple, Anum_pg_iceberg_metadata_default_spec_id,
						 metadata_rel->rd_att, &isnull);
	if (!isnull)
		info->default_spec_id = DatumGetInt32(datum);

	/*
	 * metadata_cached_at (issue #393); NULL -> treated as never refreshed.
	 *
	 * Guarded on natts.  This column was added to the initdb-time cdbinit
	 * script, so a cluster created before it whose binaries were swapped in
	 * place still carries a 5-column catalog.  heap_getattr() would then take
	 * its attnum > natts path into getmissingattr(), which reads
	 * TupleDescAttr(desc, 5) past the end of the attribute array -- an
	 * out-of-bounds read that faults on whatever follows it in memory and
	 * takes the whole cluster into recovery (issues #439, #443).  The
	 * CATALOG_VERSION_NO bump that goes with this change makes such a cluster
	 * refuse to start in the first place; the guard keeps the read in bounds
	 * regardless, and an absent column is exactly the "never refreshed" case
	 * the NULL branch below already handles.
	 */
	if (RelationGetDescr(metadata_rel)->natts >=
		Anum_pg_iceberg_metadata_metadata_cached_at)
		datum = heap_getattr(tuple, Anum_pg_iceberg_metadata_metadata_cached_at,
							 metadata_rel->rd_att, &isnull);
	else
	{
		datum = (Datum) 0;
		isnull = true;
	}

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
