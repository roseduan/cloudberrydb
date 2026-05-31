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
					pg_iceberg_free_metadata_info(meta_info);

					if (nspname)
						pfree(nspname);
					pfree(table_qname);

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