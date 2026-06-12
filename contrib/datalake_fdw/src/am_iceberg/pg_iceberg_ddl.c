#include "postgres.h"

#include "access/tableam.h"
#include "access/relation.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_lake_table.h"
#include "commands/laketablecmds.h"
#include "nodes/parsenodes.h"
#include "tcop/utility.h"
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
			 * pg_lake_table is a per-node catalog: clean on both QD and QE.
			 * Replaces the former hardcoded RemoveLakeTableEntry() call in
			 * heap_drop_with_catalog().
			 */
			RemoveLakeTableEntry(objectId);

			if (Gp_role == GP_ROLE_DISPATCH)
			{
				/*
				 * Before removing the metadata entry, enqueue the metadata
				 * location into the deletion queue so the background cleanup
				 * module can later parse it and delete all referenced files.
				 *
				 * Tolerate a missing metadata entry (e.g. a table created
				 * with a bare CREATE TABLE ... USING iceberg before that
				 * path was blocked): the table must remain droppable.
				 */
				IcebergMetadataInfo *info =
					pg_iceberg_get_metadata_info_missing_ok(objectId);

				if (info != NULL)
				{
					pg_iceberg_deletion_queue_insert(info->metadata_location,
													 objectId,
													 GetCurrentTimestamp(),
													 DELETION_TYPE_METADATA);
					pg_iceberg_free_metadata_info(info);

					pg_iceberg_remove_metadata(objectId);
				}
				else
					ereport(WARNING,
							(errmsg("iceberg metadata entry not found for table \"%s\", skipping iceberg metadata cleanup",
									RelationGetRelationName(rel))));
			}
		}

		relation_close(rel, AccessShareLock);
	}

	if (old_objectaccess_hook)
		old_objectaccess_hook(access, classId, objectId, subId, arg);
}