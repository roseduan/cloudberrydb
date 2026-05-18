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
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_type.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_am.h"
#include "commands/defrem.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/syscache.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"
#include "include/pg_iceberg_metadata.h"

/*
 * CreateIcebergMetadataTable
 *		Create the global iceberg.pg_iceberg_metadata catalog table.
 *
 * This function creates a catalog table in the iceberg schema with
 * the following schema:
 *	 - relid: OID of the relation (primary key/index)
 *	 - metadata_location: TEXT - current metadata file location
 *	 - previous_metadata_location: TEXT - previous metadata file location
 *	 - is_internal: BOOL - flag indicating if table is internal
 *	 - default_spec_id: INT4 - default partition spec id
 *
 * This table stores metadata for all iceberg tables in the system.
 * The iceberg schema must be created before calling this function.
 * It should be called during extension installation phase.
 */
void
CreateIcebergMetadataTable(void)
{
	TupleDesc	tupdesc;
	Oid			metadata_relid = InvalidOid;
	Oid			metadata_idxid = InvalidOid;
	Oid			namespaceid;
	IndexInfo  *indexInfo;
	List	   *indexColNames;
	Oid		   *classObjectId;
	int16	   *coloptions;
	Oid		   *collationObjectId;
	Relation	metadata_rel;

	tupdesc = CreateTemplateTupleDesc(5);
	
	TupleDescInitEntry(tupdesc, (AttrNumber) 1,
					   "relid",
					   OIDOID,
					   -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2,
					   "metadata_location",
					   TEXTOID,
					   -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 3,
					   "previous_metadata_location",
					   TEXTOID,
					   -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 4,
					   "is_internal",
					   BOOLOID,
					   -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 5,
					   "default_spec_id",
					   INT4OID,
					   -1, 0);

	namespaceid = get_namespace_oid(PG_ICEBERG_SCHEMA_NAME, false);
	metadata_relid = heap_create_with_catalog(PG_ICEBERG_METADATA_TABLE_NAME,
											  namespaceid,
											  DEFAULTTABLESPACE_OID,
											  InvalidOid,
											  InvalidOid,
											  InvalidOid,
											  BOOTSTRAP_SUPERUSERID,
											  HEAP_TABLE_AM_OID,
											  tupdesc,
											  NIL,
											  RELKIND_RELATION,
											  RELPERSISTENCE_PERMANENT,
											  false,  /* shared_relation */
											  false,  /* mapped_relation */
											  ONCOMMIT_NOOP,
											  NULL,   /* GP Policy */
											  (Datum) 0,
											  false,  /* use_user_acl */
											  true,   /* allow_system_table_mods */
											  true,   /* is_internal */
											  InvalidOid,
											  NULL,   /* typeaddress */
											  false); /* valid_opts */

	/* Make this table visible, else index creation will fail */
	CommandCounterIncrement();

	metadata_rel = table_open(metadata_relid, ShareLock);

	/* Setup index information - unique index on relid column */
	indexInfo = makeNode(IndexInfo);
	indexInfo->ii_NumIndexAttrs = 1;
	indexInfo->ii_NumIndexKeyAttrs = 1;
	indexInfo->ii_IndexAttrNumbers[0] = 1; /* relid is first column */
	indexInfo->ii_Expressions = NIL;
	indexInfo->ii_ExpressionsState = NIL;
	indexInfo->ii_Predicate = NIL;
	indexInfo->ii_PredicateState = NULL;
	indexInfo->ii_Unique = true; /* relid should be unique - one entry per table */
	indexInfo->ii_ReadyForInserts = true;
	indexInfo->ii_Concurrent = false;
	indexInfo->ii_BrokenHotChain = false;
	indexInfo->ii_ParallelWorkers = 0;
	indexInfo->ii_Am = BTREE_AM_OID;
	indexInfo->ii_AmCache = NULL;
	indexInfo->ii_Context = CurrentMemoryContext;

	indexColNames = list_make1("relid");
	classObjectId = (Oid *) palloc(sizeof(Oid));
	classObjectId[0] = OID_BTREE_OPS_OID;
	collationObjectId = (Oid *) palloc0(sizeof(Oid));
	coloptions = (int16 *) palloc0(sizeof(int16));

	/* Create the unique index on relid - use same tablespace as table */
	metadata_idxid = index_create(metadata_rel,
								  PG_ICEBERG_METADATA_INDEX_NAME,
								  InvalidOid,
								  InvalidOid,
								  InvalidOid,
								  InvalidOid,
								  indexInfo,
								  indexColNames,
								  BTREE_AM_OID,
								  DEFAULTTABLESPACE_OID,
								  collationObjectId,
								  classObjectId,
								  coloptions,
								  (Datum) 0,
								  INDEX_CREATE_IS_PRIMARY,
								  0,
								  true,   /* allow_system_table_mods */
								  true,   /* is_internal */
								  NULL);

	table_close(metadata_rel, ShareLock);
	UnlockRelationOid(metadata_idxid, AccessExclusiveLock);

	/*
	 * Make changes visible
	 */
	CommandCounterIncrement();
}

PG_FUNCTION_INFO_V1(pg_iceberg_create_metadata_table);
Datum
pg_iceberg_create_metadata_table(PG_FUNCTION_ARGS)
{
	/* Check if we have permission to create tables */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to create iceberg metadata table")));

	/* Create the iceberg.pg_iceberg_metadata table */
	CreateIcebergMetadataTable();

	PG_RETURN_VOID();
}

void
pg_iceberg_add_metadata(Oid relid, char *metadata_location,
					  char *previous_metadata_location,
					  bool is_internal, int32 default_spec_id)
{
	Relation	metadata_rel;
	Datum		values[5];
	bool		nulls[5];
	HeapTuple	tuple;
	Oid			namespaceid;
	Oid			metadata_relid;

	/* Get iceberg.pg_iceberg_metadata table OID */
	namespaceid = get_namespace_oid(PG_ICEBERG_SCHEMA_NAME, false);
	metadata_relid = get_relname_relid(PG_ICEBERG_METADATA_TABLE_NAME, namespaceid);

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
	Oid			namespaceid;
	Oid			metadata_relid;
	Oid			index_oid;
	Datum		values[5];
	bool		nulls[5];
	bool		replaces[5];
	Datum		old_metadata_datum;
	bool		old_metadata_isnull;

	/* Get table and index OIDs */
	namespaceid = get_namespace_oid(PG_ICEBERG_SCHEMA_NAME, false);
	metadata_relid = get_relname_relid(PG_ICEBERG_METADATA_TABLE_NAME, namespaceid);
	index_oid = get_relname_relid(PG_ICEBERG_METADATA_INDEX_NAME, namespaceid);

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
	Oid			namespaceid;
	Oid			metadata_relid;
	Oid			index_oid;

	/* Get table and index OIDs */
	namespaceid = get_namespace_oid(PG_ICEBERG_SCHEMA_NAME, false);
	metadata_relid = get_relname_relid(PG_ICEBERG_METADATA_TABLE_NAME, namespaceid);
	index_oid = get_relname_relid(PG_ICEBERG_METADATA_INDEX_NAME, namespaceid);

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
	Oid			namespaceid;
	Oid			metadata_relid;
	Oid			index_oid;
	bool		isnull;
	Datum		datum;
	IcebergMetadataInfo *info;

	/* Allocate memory for the result structure */
	info = (IcebergMetadataInfo *) palloc0(sizeof(IcebergMetadataInfo));

	/* Get table and index OIDs */
	namespaceid = get_namespace_oid(PG_ICEBERG_SCHEMA_NAME, false);
	metadata_relid = get_relname_relid(PG_ICEBERG_METADATA_TABLE_NAME, namespaceid);
	index_oid = get_relname_relid(PG_ICEBERG_METADATA_INDEX_NAME, namespaceid);

	if (!OidIsValid(metadata_relid) || !OidIsValid(index_oid))
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("iceberg metadata catalog is not available on this segment")));
	}

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
