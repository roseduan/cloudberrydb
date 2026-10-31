/*-------------------------------------------------------------------------
 *
 * pg_iceberg_refresh.c
 *    iceberg_refresh_metadata(): re-point one table's local metadata
 *    mirror at its catalog's current head.
 *
 * pg_ext_aux.pg_iceberg_metadata is a mirror of the external catalog's head,
 * and the paths that do not go through the #393 read cache -- DML, time
 * travel, iceberg_snapshot_list() -- read the metadata.json it names rather
 * than asking the catalog.  That pinning is what gives a transaction
 * read-your-own-writes.  The mirror is advanced by this database's own commits
 * and, for plain SELECTs, by the read path's TTL refresh.  Nothing advances it
 * for a table nobody reads through that path.
 *
 * Storage platforms increasingly maintain Iceberg tables on their own side
 * (compaction, snapshot expiry, orphan-file cleanup).  When that happens the
 * mirror can keep naming a metadata.json whose manifests have been deleted,
 * and the pinned paths fail on a missing file.  A table that still gets
 * written recovers on its next write, which resolves force-fresh and rebases;
 * one that does not has no deliberate way back.
 *
 * This is that way back, and it is deliberately the same machinery the
 * scheduled refresh uses: pg_iceberg_refresh_table_head() resolves the head
 * force-fresh and stores it with pg_iceberg_update_metadata_cache(), which
 * does not roll previous_metadata_location -- an external writer's commit must
 * not enter this cluster's commit lineage.  Only the policy differs: no TTL,
 * no candidate filter, one named table, owner only.
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 * IDENTIFICATION
 *        contrib/datalake_fdw/src/am_iceberg/pg_iceberg_refresh.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/relation.h"
#include "access/table.h"
#include "catalog/pg_am.h"
#include "commands/defrem.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

#include "cdb/cdbvars.h"

#include "include/pg_iceberg_catalog.h"
#include "include/pg_iceberg_metadata.h"
#include "include/pg_iceberg_metadata_refresh.h"
#include "include/pg_iceberg_metadata_tracker.h"

PG_FUNCTION_INFO_V1(iceberg_refresh_metadata);

/* Name for a message, without assuming the syscache still has the relation. */
static inline const char *
refresh_relname(Oid relid)
{
	char	   *name = get_rel_name(relid);

	return name ? name : "?";
}

/*
 * iceberg_refresh_metadata(regclass)
 *      -> (refreshed bool, previous_head text, current_head text)
 *
 * Returning both locations rather than a bare boolean is deliberate: when a
 * table has been broken by outside maintenance the operator's next question is
 * how far behind the mirror had drifted, and a boolean cannot answer it.
 *
 * refreshed reports whether the head moved, not whether anything was written:
 * a table already on the catalog's head still gets its metadata_cached_at
 * stamp renewed, which is invisible here and is not what the operator is
 * asking about.
 */
Datum
iceberg_refresh_metadata(PG_FUNCTION_ARGS)
{
	Oid						relid = PG_GETARG_OID(0);
	Relation				rel;
	IcebergMetadataInfo	   *meta_info;
	char				   *previous_head;
	char				   *current_head;
	bool					refreshed;
	TupleDesc				tupdesc;
	Datum					values[3];
	bool					nulls[3] = {false, false, false};
	HeapTuple				tuple;

	/*
	 * pg_iceberg_metadata is only populated on the QD, and the head is
	 * resolved through dlagent from there.  Same constraint, and the same
	 * reason it cannot be expressed as EXECUTE ON COORDINATOR, as
	 * iceberg_refresh_metadata_cache() -- see the extension script.
	 */
	if (Gp_role != GP_ROLE_DISPATCH)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("iceberg_refresh_metadata() must run on the coordinator"),
				 errhint("Call it on its own (SELECT iceberg_refresh_metadata(...)) "
						 "rather than from a query whose plan is dispatched to the "
						 "segments.")));

	/*
	 * Permission before semantics, the usual postgres order: a caller who may
	 * not act on the relation is told that, whatever kind of relation it
	 * turns out to be.
	 *
	 * Ownership rather than SELECT because this rewrites catalog state that
	 * every session's reads depend on -- unlike iceberg_snapshot_list(),
	 * which only reports what a read would already have seen.
	 */
	if (!object_ownercheck(RelationRelationId, relid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_TABLE, get_rel_name(relid));

	/*
	 * A table this transaction has already written has pending state in the
	 * tracker that is about to be committed against a specific base.  Moving
	 * the mirror out from under it would make that commit race its own
	 * rebase, so refuse rather than corrupt the write.
	 */
	if (pg_iceberg_tracker_get_table_state(relid) != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("cannot refresh \"%s\": it has uncommitted changes in "
						"this transaction",
						refresh_relname(relid)),
				 errhint("Commit or roll back first; a refresh replaces the "
						 "base those changes are being committed against.")));

	/*
	 * Take the lock before deciding what kind of relation this is, and keep it
	 * for the whole refresh.  Checking first and locking afterwards would let
	 * an ALTER TABLE ... SET ACCESS METHOD land in between and leave us
	 * updating iceberg mirror state for what is no longer an iceberg table.
	 *
	 * try_relation_open rather than table_open: a view or a sequence must be
	 * reported as "not an iceberg table" by name, not as whatever table_open
	 * decides to say about it.
	 */
	rel = try_relation_open(relid, AccessShareLock, false);
	if (rel == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("relation %u does not exist", relid)));

	if (rel->rd_rel->relkind != RELKIND_RELATION ||
		rel->rd_rel->relam != get_am_oid("iceberg", true))
	{
		relation_close(rel, AccessShareLock);
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not an iceberg table",
						refresh_relname(relid))));
	}

	meta_info = pg_iceberg_get_metadata_info(relid);
	previous_head = pstrdup(meta_info->metadata_location);
	pg_iceberg_free_metadata_info(meta_info);

	/*
	 * NULL means there was no external head to adopt: a built-in-catalog
	 * table, where PG owns the head and a refresh is a well-defined no-op.
	 */
	current_head = pg_iceberg_refresh_table_head(rel);
	if (current_head == NULL)
		current_head = previous_head;

	refreshed = (strcmp(previous_head, current_head) != 0);
	if (refreshed)
		ereport(NOTICE,
				(errmsg("iceberg: \"%s\" metadata refreshed",
						refresh_relname(relid)),
				 errdetail("was %s, now %s", previous_head, current_head)));

	/*
	 * Held until commit, as in the scheduled refresh: the row just updated
	 * must not be deleted by a DROP TABLE that read it before the update.
	 */
	relation_close(rel, NoLock);

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	tupdesc = BlessTupleDesc(tupdesc);

	values[0] = BoolGetDatum(refreshed);
	values[1] = CStringGetTextDatum(previous_head);
	values[2] = CStringGetTextDatum(current_head);
	tuple = heap_form_tuple(tupdesc, values, nulls);

	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}
