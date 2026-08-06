#include "postgres.h"

#include "access/tableam.h"
#include "access/relation.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_lake_table.h"
#include "commands/laketablecmds.h"
#include "miscadmin.h"
#include "nodes/parsenodes.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "cdb/cdbvars.h"
#include "utils/timestamp.h"

#include "include/pg_iceberg_ddl.h"
#include "include/pg_iceberg_am_handler.h"
#include "include/pg_iceberg_catalog.h"
#include "include/pg_iceberg_metadata_tracker.h"
#include "include/pg_iceberg_deletion_queue.h"

static object_access_hook_type old_objectaccess_hook = NULL;

static void iceberg_object_access_hook(ObjectAccessType access, Oid classId,
									   Oid objectId, int subId, void *arg);

void
pg_iceberg_setup_ddl_hooks(void)
{
	old_objectaccess_hook = object_access_hook;
	object_access_hook = iceberg_object_access_hook;

	pg_iceberg_init_metadata_tracking();
}

static void
iceberg_object_access_hook(ObjectAccessType access, Oid classId, Oid objectId,
							int subId, void *arg)
{
	Relation rel;

	if (access == OAT_POST_CREATE && classId == LakeTableRelationId)
	{
		rel = relation_open(objectId, AccessShareLock);

		if ((rel->rd_rel->relkind == RELKIND_RELATION ||
			rel->rd_rel->relkind == RELKIND_MATVIEW) &&
			(subId == 0) && is_iceberg_rel(rel))
		{
			/*
			 * Issue #337: also guard the LakeTable / CREATE ICEBERG TABLE
			 * path here.  iceberg_relation_set_new_filenode already covers
			 * plain CREATE TABLE ... USING iceberg, but the lake-table
			 * custom DDL would otherwise reach pg_iceberg_add_metadata
			 * before the iceberg.pg_iceberg_metadata catalog table exists.
			 */
			pg_iceberg_require_extension_installed();

			if (Gp_role == GP_ROLE_DISPATCH)
			{
				bool is_internal;
				char *metadata_location = pg_iceberg_create_table_with_catalog(rel, &is_internal);

				/*
				 * default_spec_id 0 is correct for both unpartitioned and
				 * PARTITION BY tables: a freshly created table's first spec
				 * (empty or not) always gets id 0.  Only spec evolution --
				 * which we do not support -- assigns higher ids; for adopted
				 * pre-existing tables the spec is cross-checked against the
				 * declaration in pg_iceberg_create_table_with_catalog.
				 */
				pg_iceberg_add_metadata(objectId, metadata_location, NULL, is_internal, 0);
				pfree(metadata_location);
			}
		}

		relation_close(rel, AccessShareLock);
	}
	else if (access == OAT_DROP && classId == RelationRelationId)
	{
		rel = relation_open(objectId, AccessShareLock);

		if ((rel->rd_rel->relkind == RELKIND_RELATION ||
			rel->rd_rel->relkind == RELKIND_MATVIEW) &&
			(subId == 0) && is_iceberg_rel(rel))
		{
			/*
			 * On QD we have to (a) gather the credential context that the
			 * async consumer will need (volume/server/owner_username/qname)
			 * BEFORE pg_lake_table is cleared, because pg_iceberg_get_table_info
			 * resolves the volume/server through pg_lake_table; and (b)
			 * enqueue + remove the metadata.  RemoveLakeTableEntry is the
			 * per-node tail step, so it runs after the QD-only block.
			 */
			if (Gp_role == GP_ROLE_DISPATCH)
			{
				IcebergMetadataInfo *meta_info =
					pg_iceberg_get_metadata_info_missing_ok(objectId);

				/*
				 * Tolerate a missing metadata entry (e.g. a table created
				 * with a bare CREATE TABLE ... USING iceberg before that
				 * path was blocked): the table must remain droppable.
				 */
				if (meta_info != NULL)
				{
					/*
					 * Only builtin (internal-catalog) iceberg tables own their
					 * files and must have them cleaned up on DROP.  External-
					 * catalog tables (is_internal == false) are managed by an
					 * external Iceberg catalog that owns the storage, so we never
					 * enqueue file deletion for them -- only the local pg_iceberg
					 * bookkeeping is dropped below.
					 */
					if (meta_info->is_internal)
					{
						IcebergTableInfo   *table_info;
						char               *owner_username;
						char               *table_qname;
						char               *nspname;
						const char         *relname;

						/*
						 * Resolve the storage context while pg_lake_table is still
						 * intact.  pg_iceberg_get_table_info goes pg_lake_table ->
						 * pg_foreign_volume -> pg_foreign_server to fill in the
						 * volume_name / volume_server_name we need to reconstruct
						 * fileIOConfig on the consumer side.
						 */
						table_info = pg_iceberg_get_table_info(objectId);

						owner_username = GetUserNameFromId(GetUserId(), false);

						nspname = get_namespace_name(rel->rd_rel->relnamespace);
						relname = NameStr(rel->rd_rel->relname);
						table_qname = nspname ?
							psprintf("%s.%s", nspname, relname) :
							psprintf("%s", relname);

						/*
						 * Enqueue the metadata path with full credential context.
						 * The autovacuum-driven consumer (pg_iceberg_av_consumer.c)
						 * will pick it up, reconstruct fileIOConfig, and call
						 * dlagent's /v1/files/cleanup-from-metadata endpoint.
						 */
						pg_iceberg_deletion_queue_insert(
							meta_info->metadata_location,
							objectId,
							table_info->volume_name,
							table_info->volume_server_name,
							owner_username,
							table_qname,
							GetCurrentTimestamp(),
							DELETION_TYPE_METADATA);

						pg_iceberg_free_table_info(table_info);

						if (nspname)
							pfree(nspname);
						pfree(table_qname);
					}

					pg_iceberg_free_metadata_info(meta_info);
					pg_iceberg_remove_metadata(objectId);
				}
				else
					ereport(WARNING,
						(errmsg("iceberg metadata entry not found for table \"%s\", skipping iceberg metadata cleanup",
							RelationGetRelationName(rel))));
			}

			/*
			 * pg_lake_table is a per-node catalog: clean on both QD and QE.
			 * Replaces the former hardcoded RemoveLakeTableEntry() call in
			 * heap_drop_with_catalog().  Must run after the QD-only block
			 * above because pg_iceberg_get_table_info reads pg_lake_table.
			 */
			RemoveLakeTableEntry(objectId);
		}

		relation_close(rel, AccessShareLock);
	}

	if (old_objectaccess_hook)
		old_objectaccess_hook(access, classId, objectId, subId, arg);
}

/*
 * pg_iceberg_truncate_table
 *    Truncate a builtin-catalog iceberg table: commit a metadata-only delete
 *    of all rows (new empty snapshot) and enqueue the pre-truncate metadata
 *    tree for async deletion.  Driven by the TRUNCATE ProcessUtility hook.
 *
 *    QD-only (catalog/agent reachable only from the dispatcher).  Both the
 *    deletion-queue insert and the metadata pointer swap are transactional, so
 *    an aborted TRUNCATE keeps the old files and leaves the table unchanged.
 *    Only the newly-written empty metadata.json would leak on abort -- the same
 *    orphan class tracked for the abort-time metadata cleanup follow-up.
 *
 *    Scope: only native iceberg tables on the builtin catalog.  A non-builtin
 *    (external-catalog) iceberg table errors out -- its storage is owned by the
 *    external catalog.
 */
void
pg_iceberg_truncate_table(Oid relid)
{
	Relation				rel;
	IcebergMetadataInfo	   *meta_info;
	IcebergTableInfo	   *table_info;
	char				   *old_metadata;
	char				   *new_metadata;

	if (Gp_role != GP_ROLE_DISPATCH)
		return;

	rel = relation_open(relid, AccessShareLock);

	if (!((rel->rd_rel->relkind == RELKIND_RELATION ||
		   rel->rd_rel->relkind == RELKIND_MATVIEW) &&
		  is_iceberg_rel(rel)))
	{
		relation_close(rel, AccessShareLock);
		return;
	}

	meta_info = pg_iceberg_get_metadata_info_missing_ok(relid);
	if (meta_info == NULL)
	{
		/* bare CREATE TABLE ... USING iceberg with no metadata: nothing to do */
		relation_close(rel, AccessShareLock);
		return;
	}

	table_info = pg_iceberg_get_table_info(relid);

	if (!pg_iceberg_is_builtin_catalog(table_info->catalog_server_name))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("TRUNCATE is only supported for builtin-catalog iceberg tables"),
				 errdetail("table \"%s\" is on an external catalog that owns its storage",
						   RelationGetRelationName(rel))));

	old_metadata = pstrdup(meta_info->metadata_location);

	/* Agent commits a metadata-only delete of all rows; returns new metadata. */
	new_metadata = pg_iceberg_truncate_with_catalog(rel, table_info, old_metadata);

	/*
	 * Already-empty table: the agent returns the unchanged location -> no-op.
	 * Must NOT enqueue the live metadata (that would delete the empty table's
	 * own files).
	 */
	if (new_metadata != NULL && strcmp(new_metadata, old_metadata) != 0)
	{
		char	   *owner_username = GetUserNameFromId(GetUserId(), false);
		char	   *nspname = get_namespace_name(rel->rd_rel->relnamespace);
		const char *relname = NameStr(rel->rd_rel->relname);
		char	   *table_qname = nspname ?
			psprintf("%s.%s", nspname, relname) : psprintf("%s", relname);

		/*
		 * Enqueue the OLD metadata tree for async deletion.  Transactional heap
		 * insert: rolls back with the txn, so an aborted TRUNCATE keeps the old
		 * files.  The new (empty) metadata is not reachable from the old one,
		 * so the consumer leaves it intact.
		 */
		pg_iceberg_deletion_queue_insert(old_metadata,
										 relid,
										 table_info->volume_name,
										 table_info->volume_server_name,
										 owner_username,
										 table_qname,
										 GetCurrentTimestamp(),
										 DELETION_TYPE_METADATA);

		/* Swap the catalog pointer to the new empty metadata (CAS on old). */
		pg_iceberg_update_metadata_cas(relid, new_metadata, old_metadata);

		if (nspname)
			pfree(nspname);
		pfree(table_qname);
	}

	pg_iceberg_free_table_info(table_info);
	pg_iceberg_free_metadata_info(meta_info);
	pfree(old_metadata);
	relation_close(rel, AccessShareLock);
}