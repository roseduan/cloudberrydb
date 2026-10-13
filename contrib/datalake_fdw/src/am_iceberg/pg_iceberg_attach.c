/*-------------------------------------------------------------------------
 *
 * pg_iceberg_attach.c
 *	  Re-point a freshly created builtin iceberg table at an existing
 *	  metadata.json (issue #457: gpbackup/gprestore metadata-only).
 *
 * gprestore does not copy iceberg data: the files are still on object
 * storage.  It recreates the table with CREATE ICEBERG TABLE, which writes an
 * empty v0 metadata.json, and then calls this to swap the catalog pointer to
 * the metadata recorded at backup time.  The v0 file is handed to the
 * deletion queue the same way TRUNCATE retires its old tree.
 *
 * COORDINATOR ONLY: pg_iceberg_metadata rows exist only on the QD.
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/am_iceberg/pg_iceberg_attach.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/relation.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "cdb/cdbvars.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/resowner.h"
#include "utils/timestamp.h"

#include "include/pg_iceberg_am_handler.h"
#include "include/pg_iceberg_catalog.h"
#include "include/pg_iceberg_catalog_helper.h"
#include "include/pg_iceberg_deletion_queue.h"
#include "include/pg_iceberg_metadata.h"

PG_FUNCTION_INFO_V1(pg_iceberg_attach_metadata_internal);

/*
 * Substrings that mean "the object is not there" in the error dlagent hands
 * back.  The gopher-backed reader reports a missing metadata.json as
 * "Gopher error: FileDoesNotExistException: ... headObject <path> failed";
 * the other two cover the plain S3 and Hadoop clients.  Anything else
 * (network, credentials, agent down) must stay a hard error: gprestore
 * downgrades only 58P01 to "skip this table", and a transient outage must
 * not silently skip every table.
 */
static const char *const missing_object_markers[] = {
	"FileDoesNotExistException",
	"NoSuchKey",
	"FileNotFoundException",
};

static bool
error_is_missing_object(const char *message)
{
	const char *frames;
	char	   *head;
	bool		found = false;
	int			i;

	if (message == NULL)
		return false;

	/*
	 * Look only at the exception lines, not the stack frames ("\n\tat ...")
	 * or the "Caused by" chain below them: a marker buried there belongs to
	 * some nested failure, not to "this file does not exist".
	 */
	frames = strstr(message, "\n\tat ");
	head = frames ? pnstrdup(message, frames - message) : pstrdup(message);
	for (i = 0; i < lengthof(missing_object_markers) && !found; i++)
		found = strstr(head, missing_object_markers[i]) != NULL;
	pfree(head);
	return found;
}

/*
 * The agent error embeds a full Java stack trace; its first line already
 * names the failing path, so that is all the detail needs.
 */
static char *
first_line(const char *message)
{
	const char *newline = strchr(message, '\n');

	if (newline == NULL)
		return pstrdup(message);
	return pnstrdup(message, newline - message);
}

/*
 * Fetch the metadata.json the catalog now points at, turning "object does
 * not exist" into ERRCODE_UNDEFINED_FILE and rethrowing anything else.
 */
static char *
load_attached_metadata(Oid relid, const char *new_location)
{
	MemoryContext oldcxt = CurrentMemoryContext;
	ResourceOwner oldowner = CurrentResourceOwner;
	char	   *loaded_location = NULL;
	char	   *metadata_json = NULL;

	BeginInternalSubTransaction(NULL);
	MemoryContextSwitchTo(oldcxt);

	PG_TRY();
	{
		pg_iceberg_load_metadata_json(relid, &loaded_location, &metadata_json);
		ReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(oldcxt);
		CurrentResourceOwner = oldowner;
	}
	PG_CATCH();
	{
		ErrorData  *edata;

		MemoryContextSwitchTo(oldcxt);
		edata = CopyErrorData();
		FlushErrorState();
		RollbackAndReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(oldcxt);
		CurrentResourceOwner = oldowner;

		if (error_is_missing_object(edata->message))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_FILE),
					 errmsg("iceberg metadata file \"%s\" does not exist",
							new_location),
					 errdetail("%s", first_line(edata->message))));
		ReThrowError(edata);
	}
	PG_END_TRY();

	return metadata_json;
}

/*
 * Refuse metadata that another table of this database already points at.
 * Two live tables on one metadata tree share every data file, so a DROP,
 * TRUNCATE or VACUUM of either deletes the other's data -- this is what a
 * restore next to its live source (e.g. --redirect-schema) would produce.
 * pg_iceberg_metadata is per database, so other databases are not seen.
 */
static void
reject_shared_metadata(Oid relid, const char *new_location)
{
	Oid			argtypes[2] = {TEXTOID, OIDOID};
	Datum		values[2];
	int			ret;

	values[0] = CStringGetTextDatum(new_location);
	values[1] = ObjectIdGetDatum(relid);

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	/*
	 * Check-then-act would race: two sessions attaching different tables to
	 * the same location could both pass the check before either commits.
	 * Serialize attaches of one location with a transaction-level advisory
	 * lock, and run the check non-read-only so it takes a fresh snapshot
	 * after the lock and sees a pointer the other session just committed.
	 * (A UNIQUE index on metadata_location is not an option: external
	 * catalog tables mounting the same table legitimately share it.)
	 */
	ret = SPI_execute_with_args("SELECT pg_catalog.pg_advisory_xact_lock("
								"pg_catalog.hashtextextended($1, 0))",
								1, argtypes, values, NULL, false, 0);
	if (ret != SPI_OK_SELECT)
		elog(ERROR, "could not lock iceberg metadata location: %d", ret);

	ret = SPI_execute_with_args("SELECT relid::regclass::text"
								"  FROM pg_ext_aux.pg_iceberg_metadata"
								" WHERE metadata_location = $1 AND relid <> $2"
								" LIMIT 1",
								2, argtypes, values, NULL, false, 1);
	if (ret != SPI_OK_SELECT)
		elog(ERROR, "could not look up iceberg metadata users: %d", ret);

	if (SPI_processed > 0)
	{
		char	   *other = SPI_getvalue(SPI_tuptable->vals[0],
										 SPI_tuptable->tupdesc, 1);

		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("iceberg metadata is already used by table %s",
						other ? other : "<unknown>"),
				 errdetail("Metadata location: %s.  Two tables on one metadata tree would delete each other's files.",
						   new_location),
				 errhint("Restore iceberg tables only where their source no longer exists.")));
	}

	SPI_finish();
}

/*
 * Refuse a table whose previous incarnation still has rows in the deletion
 * queue.  DROP only enqueues; the autovacuum-driven consumer deletes later,
 * so in that window the metadata file still exists and attaching it would
 * succeed -- until the consumer deletes the files under the restored table.
 * Reported like a missing file (58P01): the table is gone for restore
 * purposes, and gprestore skips it.  Must run before this call enqueues v0.
 */
static void
reject_pending_deletion(const char *table_qname, const char *new_location)
{
	Oid			argtypes[1] = {TEXTOID};
	Datum		values[1];
	int			ret;

	values[0] = CStringGetTextDatum(table_qname);

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	ret = SPI_execute_with_args("SELECT 1"
								"  FROM pg_ext_aux.pg_iceberg_deletion_queue"
								" WHERE table_qname = $1"
								" LIMIT 1",
								1, argtypes, values, NULL, true, 1);
	if (ret != SPI_OK_SELECT)
		elog(ERROR, "could not look up the iceberg deletion queue: %d", ret);

	if (SPI_processed > 0)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FILE),
				 errmsg("iceberg metadata file \"%s\" belongs to a table pending deletion",
						new_location),
				 errdetail("Table %s was dropped or truncated and its files are queued for deletion.",
						   table_qname),
				 errhint("Restore it after the deletion queue has drained, or not at all.")));

	SPI_finish();
}

Datum
pg_iceberg_attach_metadata_internal(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	char	   *new_location = text_to_cstring(PG_GETARG_TEXT_PP(1));
	Relation	rel;
	IcebergTableInfo *table_info;
	IcebergMetadataInfo *meta;
	char	   *old_location;
	char	   *table_qname;
	char	   *metadata_json;

	if (Gp_role == GP_ROLE_EXECUTE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("attach_metadata() must be executed on the coordinator")));

	/*
	 * Superuser only, checked before any lock is taken.  The caller names an
	 * arbitrary metadata path, and builtin tables of a volume share one
	 * directory, so an ordinary table owner could otherwise attach -- read,
	 * and on DROP delete -- another table's files.  gprestore runs as a
	 * superuser.
	 */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to attach iceberg metadata")));

	/* Exclusive: nobody may read or write the table while its root moves. */
	rel = relation_open(relid, AccessExclusiveLock);

	if (!is_iceberg_rel(rel))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("relation \"%s\" is not an iceberg table",
						RelationGetRelationName(rel))));

	table_info = pg_iceberg_get_table_info(relid);
	if (!pg_iceberg_is_builtin_catalog(table_info->catalog_server_name))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("iceberg table \"%s\" is not registered on the builtin catalog",
						RelationGetRelationName(rel)),
				 errhint("Tables on an external catalog are restored from that catalog; only builtin tables can be attached.")));

	/*
	 * Only a table fresh out of CREATE ICEBERG TABLE qualifies: any commit
	 * sets previous_metadata_location, and retiring a v0 that already has
	 * snapshots on top of it would delete live data files.
	 */
	meta = pg_iceberg_get_metadata_info(relid);
	if (meta->previous_metadata_location != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("iceberg table \"%s\" already has committed data",
						RelationGetRelationName(rel)),
				 errhint("attach_metadata() only accepts a table that was just created.")));

	old_location = pstrdup(meta->metadata_location);
	pg_iceberg_free_metadata_info(meta);

	if (strcmp(old_location, new_location) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("iceberg table \"%s\" already points at \"%s\"",
						RelationGetRelationName(rel), new_location)));

	reject_shared_metadata(relid, new_location);

	/* Same "nspname.relname" form DROP and TRUNCATE store in the queue. */
	table_qname = psprintf("%s.%s",
						   get_namespace_name(RelationGetNamespace(rel)),
						   RelationGetRelationName(rel));

	reject_pending_deletion(table_qname, new_location);

	/*
	 * Transactional, like TRUNCATE: if the attach fails the queue row rolls
	 * back with it and v0 stays the table's root.  The attached metadata is
	 * not reachable from v0, so the consumer never touches it.
	 */
	pg_iceberg_deletion_queue_insert(old_location,
									 relid,
									 table_info->volume_name,
									 table_info->volume_server_name,
									 GetUserNameFromId(GetUserId(), false),
									 table_qname,
									 GetCurrentTimestamp(),
									 DELETION_TYPE_METADATA);

	if (!pg_iceberg_update_metadata_cas(relid, new_location, old_location))
		ereport(ERROR,
				(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
				 errmsg("iceberg table \"%s\" changed while attaching metadata",
						RelationGetRelationName(rel))));
	CommandCounterIncrement();

	metadata_json = load_attached_metadata(relid, new_location);

	pg_iceberg_free_table_info(table_info);
	relation_close(rel, NoLock);
	PG_RETURN_TEXT_P(cstring_to_text(metadata_json));
}
