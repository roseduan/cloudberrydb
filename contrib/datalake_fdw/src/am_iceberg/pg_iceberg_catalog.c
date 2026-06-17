/*-------------------------------------------------------------------------
 *
 * pg_iceberg_catalog.c
 *	  This file contains routines to support high-level Iceberg catalog 
 *    operations and table information retrieval.
 *
 * It provides functions to interact with external Iceberg catalogs and
 * retrieve necessary information from local catalog tables like pg_lake_table.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/am_iceberg/pg_iceberg_catalog.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/table.h"
#include "access/heapam.h"
#include "access/genam.h"
#include "commands/vacuum.h"
#include "commands/dbcommands.h"
#include "catalog/namespace.h"
#include "catalog/indexing.h"
#include "catalog/pg_type.h"
#include "catalog/pg_lake_table.h"
#include "catalog/pg_foreign_catalog.h"
#include "catalog/pg_foreign_volume.h"
#include "catalog/pg_foreign_server.h"
#include "foreign/foreign.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/syscache.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"

#include "../iceberg_catalog_fdw/iceberg_catalog_fdw.h"
#include "../iceberg_volume_fdw/iceberg_volume_option.h"
#include "src/common/iceberg_constants.h"
#include "src/common/parser_option.h"
#include "include/pg_iceberg_catalog.h"
#include "include/pg_iceberg_catalog_helper.h"
#include "include/pg_iceberg_options.h"

bool
pg_iceberg_is_builtin_catalog(const char *catalog_server_name)
{
	ForeignServer *catalog_server;
	char	   *catalog_type;

	Assert(catalog_server_name != NULL);

	catalog_server = GetForeignServerByName(catalog_server_name, false);

	/*
	 * Check both "server_type" (legacy) and "type" (standard) option keys.
	 * Volume servers use "type", catalog servers historically used "server_type".
	 */
	catalog_type = getStringOption(catalog_server->options,
								   DATALAKEFDW_ICEBERG_KEY_SERVER_TYPE);
	if (catalog_type == NULL)
		catalog_type = getStringOption(catalog_server->options, "type");

	if (catalog_type != NULL)
		return pg_strcasecmp(catalog_type,
							 DATALAKEFDW_ICEBERG_SERVER_BUILTIN) == 0;

	return catalog_server->options == NIL;
}

static char *
pg_iceberg_get_builtin_volume_prefix(const char *volume_server_name,
									 const char *volume_name)
{
	IcebergVolumeOptions *opts;

	opts = getIcebergVolumeOptions(volume_server_name, volume_name);
	return buildVolumeBasePath(opts);
}

static char *
pg_iceberg_generate_builtin_location(IcebergTableInfo *table_info,
									 const char *nameSpace,
									 const char *tableName)
{
	char	   *prefix;
	const char *location_suffix;
	char	   *default_suffix = NULL;
	const char *db_name;

	Assert(table_info != NULL);
	Assert(nameSpace != NULL);
	Assert(tableName != NULL);

	prefix = pg_iceberg_get_builtin_volume_prefix(table_info->volume_server_name,
												  table_info->volume_name);

	if (table_info->opts != NULL &&
		table_info->opts->location != NULL &&
		table_info->opts->location[0] != '\0')
	{
		location_suffix = table_info->opts->location;
	}
	else
	{
		db_name = get_database_name(MyDatabaseId);
		if (db_name == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_DATABASE),
					 errmsg("failed to resolve current database name for builtin iceberg location")));

		default_suffix = psprintf("%s/%s/%s",
								  db_name,
								  nameSpace,
								  tableName);
		location_suffix = default_suffix;
	}

	while (*location_suffix == '/')
		location_suffix++;

	if (*location_suffix == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("empty iceberg table location suffix")));

	return psprintf("%s%s", prefix, location_suffix);
}

static void
get_catalog_name_and_server(Oid catalog_oid, char **catalog_name, 
							char **catalog_server_name)
{
	HeapTuple		catalog_tuple;
	HeapTuple		server_tuple;
	Form_pg_foreign_catalog catalog_form;
	Form_pg_foreign_server server_form;
	Oid				server_oid;

	/* Look up foreign catalog information */
	catalog_tuple = SearchSysCache1(FOREIGNCATALOGOID, 
									ObjectIdGetDatum(catalog_oid));
	if (!HeapTupleIsValid(catalog_tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("foreign catalog with OID %u does not exist", catalog_oid)));
	
	catalog_form = (Form_pg_foreign_catalog) GETSTRUCT(catalog_tuple);
	*catalog_name = pstrdup(NameStr(catalog_form->fcname));
	server_oid = catalog_form->fcserver;
	ReleaseSysCache(catalog_tuple);

	/* Look up catalog server name */
	server_tuple = SearchSysCache1(FOREIGNSERVEROID,
								   ObjectIdGetDatum(server_oid));
	if (!HeapTupleIsValid(server_tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("foreign server with OID %u does not exist", server_oid)));
	
	server_form = (Form_pg_foreign_server) GETSTRUCT(server_tuple);
	*catalog_server_name = pstrdup(NameStr(server_form->srvname));
	ReleaseSysCache(server_tuple);
}

static void
get_volume_name_and_server(Oid volume_oid, char **volume_name,
						   char **volume_server_name)
{
	HeapTuple		volume_tuple;
	HeapTuple		server_tuple;
	Form_pg_foreign_volume volume_form;
	Form_pg_foreign_server server_form;
	Oid				server_oid;

	/* Look up foreign volume information */
	volume_tuple = SearchSysCache1(FOREIGNVOLUMEOID,
								   ObjectIdGetDatum(volume_oid));
	if (!HeapTupleIsValid(volume_tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("foreign volume with OID %u does not exist", volume_oid)));
	
	volume_form = (Form_pg_foreign_volume) GETSTRUCT(volume_tuple);
	*volume_name = pstrdup(NameStr(volume_form->fvname));
	server_oid = volume_form->fvserver;
	ReleaseSysCache(volume_tuple);

	/* Look up volume server name */
	server_tuple = SearchSysCache1(FOREIGNSERVEROID,
								   ObjectIdGetDatum(server_oid));
	if (!HeapTupleIsValid(server_tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("foreign server with OID %u does not exist", server_oid)));
	
	server_form = (Form_pg_foreign_server) GETSTRUCT(server_tuple);
	*volume_server_name = pstrdup(NameStr(server_form->srvname));
	ReleaseSysCache(server_tuple);
}

IcebergTableInfo *
pg_iceberg_get_table_info(Oid relid)
{
	Oid catalog_oid;
	Oid volume_oid;
	IcebergTableOptions *opts;
	IcebergTableInfo *info;

	/* Allocate memory for the result structure */
	info = (IcebergTableInfo *) palloc0(sizeof(IcebergTableInfo));

	opts = get_iceberg_options(relid, &catalog_oid, &volume_oid);
	get_catalog_name_and_server(catalog_oid,
								&info->catalog_name,
								&info->catalog_server_name);
	get_volume_name_and_server(volume_oid,
							   &info->volume_name,
							   &info->volume_server_name);

	info->opts = opts;

	return info;
}

void
pg_iceberg_free_table_info(IcebergTableInfo *info)
{
	if (info == NULL)
		return;

	if (info->catalog_name)
		pfree(info->catalog_name);
	if (info->catalog_server_name)
		pfree(info->catalog_server_name);
	if (info->volume_name)
		pfree(info->volume_name);
	if (info->volume_server_name)
		pfree(info->volume_server_name);
	if (info->opts)
	{
		if (info->opts->catalog)
			pfree(info->opts->catalog);
		if (info->opts->namespace)
			pfree(info->opts->namespace);
		if (info->opts->table)
			pfree(info->opts->table);
		if (info->opts->location)
			pfree(info->opts->location);
		pfree(info->opts);
	}

	/* Free the structure itself */
	pfree(info);
}

/*
 * Get latest metadata location for both internal and external iceberg tables.
 */
char *
pg_iceberg_get_latest_metadata_location(Oid relid, IcebergTableInfo *table_info)
{
	IcebergMetadataInfo *meta_info;
	IcebergLoadTableResult *load_result;
	Relation	rel;
	const char *nameSpace;
	const char *tableName;
	const char *catalogName;
	char	   *latest_metadata_location;
	bool		is_internal;

	is_internal = (table_info->opts == NULL || table_info->opts->table == NULL);

	if (is_internal)
	{
		meta_info = pg_iceberg_get_metadata_info(relid);

		latest_metadata_location = pstrdup(meta_info->metadata_location);
		pg_iceberg_free_metadata_info(meta_info);
		return latest_metadata_location;
	}

	/*
	 * External path: resolve namespace via the 3-tier precedence so a missing
	 * OPTIONS namespace can still fall back to catalog default / PG schema.
	 * Open the relation briefly to get relnamespace for the final fallback;
	 * AccessShareLock is the standard read lock for catalog-only access.
	 */
	rel = relation_open(relid, AccessShareLock);
	nameSpace = pg_iceberg_resolve_namespace(table_info->opts->namespace,
											 table_info->catalog_server_name,
											 table_info->catalog_name,
											 rel);
	relation_close(rel, AccessShareLock);

	tableName = table_info->opts->table;
	catalogName = table_info->opts->catalog;

	load_result = pg_iceberg_load_table(catalogName,
										nameSpace,
										tableName,
										table_info->catalog_server_name,
										table_info->catalog_name,
										table_info->volume_server_name,
										table_info->volume_name);
	if (load_result == NULL || load_result->metadata_location == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("failed to load iceberg table metadata for relation %u",
						relid)));

	latest_metadata_location = pstrdup(load_result->metadata_location);
	pg_iceberg_free_load_table_result(load_result);

	return latest_metadata_location;
}

char *
pg_iceberg_get_latest_metadata_and_mode(Oid relid, bool *is_internal_out)
{
	IcebergTableInfo *table_info;
	char	   *latest_metadata_location;

	table_info = pg_iceberg_get_table_info(relid);

	if (is_internal_out != NULL)
		*is_internal_out =
			(table_info->opts == NULL || table_info->opts->table == NULL);

	latest_metadata_location =
		pg_iceberg_get_latest_metadata_location(relid, table_info);

	pg_iceberg_free_table_info(table_info);

	return latest_metadata_location;
}

char *
pg_iceberg_create_table_with_catalog(Relation rel, bool *is_internal)
{
	IcebergTableInfo *table_info;
	char	   *result;
	char	   *location = NULL;
	const char *nameSpace;
	const char *tableName;
	const char *catalogName;

	/* Get table information */
	table_info = pg_iceberg_get_table_info(RelationGetRelid(rel));

	nameSpace = pg_iceberg_resolve_namespace(
		table_info->opts ? table_info->opts->namespace : NULL,
		table_info->catalog_server_name,
		table_info->catalog_name,
		rel);

	if (table_info->opts == NULL || table_info->opts->table == NULL)
	{
		/* Internal table: name comes from PG; namespace already resolved above. */
		tableName = pstrdup(RelationGetRelationName(rel));
		catalogName = NULL;

		/*
		 * For builtin catalog, we generate the storage location locally
		 * from the volume base path.  For external catalogs (hive, polaris),
		 * the catalog itself determines the location, so we pass NULL and
		 * refresh afterwards to capture the catalog-assigned location.
		 */
		if (pg_iceberg_is_builtin_catalog(table_info->catalog_server_name))
			location = pg_iceberg_generate_builtin_location(table_info, nameSpace, tableName);

		result = pg_iceberg_create_table(rel,
										 catalogName,
										 nameSpace,
										 tableName,
										 table_info->catalog_server_name,
										 table_info->catalog_name,
										 table_info->volume_server_name,
										 table_info->volume_name,
										 location);

		if (location != NULL)
		{
			/* Builtin: location was pre-determined */
			pg_iceberg_upsert_location_option(RelationGetRelid(rel), location);
		}
		else
		{
			/*
			 * Hive/Polaris: refresh from catalog to capture the
			 * catalog-assigned table-location (the table root URI).
			 *
			 * The location ltoption stores the table root (e.g.
			 * "s3://bucket/db/table"), which parseVolumeUri() later
			 * combines with "/data" to derive the parquet write path.
			 * It must NOT be set to metadata_location (the
			 * "<root>/metadata/00000-xxx.metadata.json" file URI),
			 * otherwise parquet files end up under
			 * "<root>/metadata/00000-xxx.metadata.json/data/", which
			 * corrupts the warehouse layout and breaks reads.
			 */
			IcebergLoadTableResult *refresh_result;

			refresh_result = pg_iceberg_load_table(catalogName,
												   nameSpace,
												   tableName,
												   table_info->catalog_server_name,
												   table_info->catalog_name,
												   table_info->volume_server_name,
												   table_info->volume_name);
			if (refresh_result != NULL)
			{
				if (refresh_result->location == NULL ||
					refresh_result->location[0] == '\0')
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("external catalog \"%s\" returned empty table location for \"%s.%s\"",
									table_info->catalog_name,
									nameSpace,
									tableName)));
				pg_iceberg_upsert_location_option(RelationGetRelid(rel),
												  refresh_result->location);
				pg_iceberg_free_load_table_result(refresh_result);
			}
		}
		*is_internal = true;
	}
	else
	{
		IcebergLoadTableResult *load_result;

		/* External table: identifier names from OPTIONS; namespace already resolved above. */
		tableName = table_info->opts->table;
		catalogName = table_info->opts->catalog;

		/* Try to load it from the catalog */
		load_result = pg_iceberg_load_table(catalogName,
											nameSpace,
											tableName,
											table_info->catalog_server_name,
											table_info->catalog_name,
											table_info->volume_server_name,
											table_info->volume_name);

		if (load_result == NULL)
		{
			IcebergLoadTableResult *refresh_result;

			/* Table doesn't exist on external catalog, create it */
			result = pg_iceberg_create_table(rel,
											 catalogName,
											 nameSpace,
											 tableName,
											 table_info->catalog_server_name,
											 table_info->catalog_name,
											 table_info->volume_server_name,
											 table_info->volume_name,
											 location);

			/*
			 * Refresh once to capture catalog-returned table-location and persist
			 * it in ltoptions for later DML location resolution on QEs.
			 */
			refresh_result = pg_iceberg_load_table(catalogName,
												   nameSpace,
												   tableName,
												   table_info->catalog_server_name,
												   table_info->catalog_name,
												   table_info->volume_server_name,
												   table_info->volume_name);
			if (refresh_result == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_OBJECT),
						 errmsg("iceberg table \"%s.%s\" does not exist in external catalog \"%s\" after creation",
								nameSpace,
								tableName,
								table_info->catalog_name)));

			if (refresh_result->location == NULL ||
				refresh_result->location[0] == '\0')
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("external catalog \"%s\" returned empty table location for \"%s.%s\"",
								table_info->catalog_name,
								nameSpace,
								tableName)));
			pg_iceberg_upsert_location_option(RelationGetRelid(rel),
											  refresh_result->location);
			pg_iceberg_free_load_table_result(refresh_result);
		}
		else
		{
			result = load_result->metadata_location;
			/* Ownership transferred to result. */
			load_result->metadata_location = NULL;
			if (load_result->location == NULL || load_result->location[0] == '\0')
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("external catalog \"%s\" returned empty table location for \"%s.%s\"",
								table_info->catalog_name,
								nameSpace,
								tableName)));
			pg_iceberg_upsert_location_option(RelationGetRelid(rel),
											  load_result->location);
			pg_iceberg_free_load_table_result(load_result);
		}
		*is_internal = false;
	}

	if (location != NULL)
		pfree(location);

	/* Clean up allocated strings */
	pg_iceberg_free_table_info(table_info);

	return result;
}

char *
pg_iceberg_get_fragments_with_catalog(Relation rel,
									  IcebergTableInfo *table_info,
									  const char *metadata_location,
									  bool is_internal,
									  const char *pushdown_filter)
{
	const char *nameSpace;
	const char *tableName;
	const char *catalogName;

	nameSpace = pg_iceberg_resolve_namespace(
		table_info->opts ? table_info->opts->namespace : NULL,
		table_info->catalog_server_name,
		table_info->catalog_name,
		rel);

	if (table_info->opts == NULL || table_info->opts->table == NULL)
	{
		tableName = pstrdup(RelationGetRelationName(rel));
		catalogName = NULL;
	}
	else
	{
		tableName = table_info->opts->table;
		catalogName = table_info->opts->catalog;
	}

	return pg_iceberg_get_fragments(rel,
									catalogName,
									nameSpace,
									tableName,
									metadata_location,
									is_internal,
									pushdown_filter,
									table_info->catalog_server_name,
									table_info->catalog_name,
									table_info->volume_server_name,
									table_info->volume_name);
}

IcebergTableStatistics *
pg_iceberg_get_statistics_with_catalog(Relation rel,
									   IcebergTableInfo *table_info,
									   const char *metadata_location,
									   bool is_internal)
{
	const char *nameSpace;
	const char *tableName;
	const char *catalogName;

	nameSpace = pg_iceberg_resolve_namespace(
		table_info->opts ? table_info->opts->namespace : NULL,
		table_info->catalog_server_name,
		table_info->catalog_name,
		rel);

	if (table_info->opts == NULL || table_info->opts->table == NULL)
	{
		tableName = pstrdup(RelationGetRelationName(rel));
		catalogName = NULL;
	}
	else
	{
		tableName = table_info->opts->table;
		catalogName = table_info->opts->catalog;
	}

	return pg_iceberg_get_statistics(rel,
									 catalogName,
									 nameSpace,
									 tableName,
									 metadata_location,
									 is_internal,
									 table_info->catalog_server_name,
									 table_info->catalog_name,
									 table_info->volume_server_name,
									 table_info->volume_name);
}

char *
pg_iceberg_get_rewrite_plan_with_catalog(Relation rel,
										 IcebergTableInfo *table_info,
										 const char *metadata_location,
										 bool is_internal,
										 struct VacuumParams *params,
										 int min_input_files,
										 int target_file_size_mb)
{
	const char *nameSpace;
	const char *tableName;
	const char *catalogName;

	nameSpace = pg_iceberg_resolve_namespace(
		table_info->opts ? table_info->opts->namespace : NULL,
		table_info->catalog_server_name,
		table_info->catalog_name,
		rel);

	if (table_info->opts == NULL || table_info->opts->table == NULL)
	{
		tableName = pstrdup(RelationGetRelationName(rel));
		catalogName = NULL;
	}
	else
	{
		tableName = table_info->opts->table;
		catalogName = table_info->opts->catalog;
	}

	return pg_iceberg_get_rewrite_plan(rel,
									   metadata_location,
									   is_internal,
									   params,
									   min_input_files,
									   target_file_size_mb,
									   catalogName,
									   nameSpace,
									   tableName,
									   table_info->catalog_server_name,
									   table_info->catalog_name,
									   table_info->volume_server_name,
									   table_info->volume_name);
}

char *
pg_iceberg_modify_data_with_catalog(Relation rel,
									IcebergTableInfo *table_info,
									const char *data_locations,
									const char *metadata_location,
									bool is_internal,
									CmdType operation)
{
	IcebergCatalogOperation op;
	const char *nameSpace;
	const char *tableName;
	const char *catalogName;

	switch (operation)
	{
		case CMD_INSERT:
			op = ICEBERG_APPEND;
			break;
		case CMD_UPDATE:
			op = ICEBERG_UPDATE;
			break;
		case CMD_DELETE:
			op = ICEBERG_DELETE;
			break;
		default:
			elog(ERROR, "unrecognized operation: %d", (int) operation);
			return NULL;
	}

	nameSpace = pg_iceberg_resolve_namespace(
		table_info->opts ? table_info->opts->namespace : NULL,
		table_info->catalog_server_name,
		table_info->catalog_name,
		rel);

	if (table_info->opts == NULL || table_info->opts->table == NULL)
	{
		tableName = pstrdup(RelationGetRelationName(rel));
		catalogName = NULL;
	}
	else
	{
		tableName = table_info->opts->table;
		catalogName = table_info->opts->catalog;
	}

	return pg_iceberg_catalog_op(rel,
								 op,
								 catalogName,
								 nameSpace,
								 tableName,
								 data_locations,
								 metadata_location,
								 is_internal,
								 table_info->catalog_server_name,
								 table_info->catalog_name,
								 table_info->volume_server_name,
								 table_info->volume_name);
}

/*
 * pg_iceberg_truncate_with_catalog
 *    Truncate a builtin iceberg table: ask the agent to commit a metadata-only
 *    delete of all rows on top of `metadata_location`, returning the new
 *    metadata.json location.  No data_locations (no new files).  When the table
 *    is already empty the agent returns the unchanged location, so the caller
 *    detects a no-op by comparing against `metadata_location`.
 */
char *
pg_iceberg_truncate_with_catalog(Relation rel,
								 IcebergTableInfo *table_info,
								 const char *metadata_location)
{
	const char *nameSpace;
	const char *tableName;
	const char *catalogName;

	nameSpace = pg_iceberg_resolve_namespace(
		table_info->opts ? table_info->opts->namespace : NULL,
		table_info->catalog_server_name,
		table_info->catalog_name,
		rel);

	if (table_info->opts == NULL || table_info->opts->table == NULL)
	{
		tableName = pstrdup(RelationGetRelationName(rel));
		catalogName = NULL;
	}
	else
	{
		tableName = table_info->opts->table;
		catalogName = table_info->opts->catalog;
	}

	return pg_iceberg_catalog_op(rel,
								 ICEBERG_TRUNCATE,
								 catalogName,
								 nameSpace,
								 tableName,
								 NULL,	/* no data_locations */
								 metadata_location,
								 true,	/* is_internal: builtin only */
								 table_info->catalog_server_name,
								 table_info->catalog_name,
								 table_info->volume_server_name,
								 table_info->volume_name);
}

char *
pg_iceberg_commit_data_with_catalog(Relation rel,
									IcebergTableInfo *table_info,
									const char *data_locations,
									const char *metadata_location,
									CmdType operation)
{
	IcebergCatalogOperation op;
	const char *nameSpace;
	const char *tableName;
	const char *catalogName;

	switch (operation)
	{
		case CMD_INSERT:
			op = ICEBERG_COMMIT_APPEND;
			break;
		case CMD_UPDATE:
			op = ICEBERG_COMMIT_UPDATE;
			break;
		case CMD_DELETE:
			op = ICEBERG_COMMIT_DELETE;
			break;
		default:
			elog(ERROR, "unrecognized operation: %d", (int) operation);
			return NULL;
	}

	/*
	 * Resolve namespace / table / catalog. Tables created with explicit
	 * OPTIONS (catalog, namespace, table) carry the names in opts; tables
	 * created against the engine's default catalog (iceberg_default_catalog
	 * pointing at a non-builtin server) leave opts->table NULL and we fall
	 * back to the relation's own namespace + name, mirroring what
	 * pg_iceberg_modify_data_with_catalog already does.
	 */
	if (table_info->opts == NULL || table_info->opts->table == NULL)
	{
		if (rel == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("pg_iceberg_commit_data_with_catalog requires either an external iceberg table or a relation")));
		nameSpace = pg_iceberg_resolve_namespace(
			table_info->opts ? table_info->opts->namespace : NULL,
			table_info->catalog_server_name,
			table_info->catalog_name,
			rel);
		tableName = pstrdup(RelationGetRelationName(rel));
		catalogName = NULL;
	}
	else
	{
		nameSpace = pg_iceberg_resolve_namespace(
			table_info->opts->namespace,
			table_info->catalog_server_name,
			table_info->catalog_name,
			rel);
		tableName = table_info->opts->table;
		catalogName = table_info->opts->catalog;
	}

	return pg_iceberg_catalog_op(rel,
								 op,
								 catalogName,
								 nameSpace,
								 tableName,
								 data_locations,
								 metadata_location,
								 false,
								 table_info->catalog_server_name,
								 table_info->catalog_name,
								 table_info->volume_server_name,
								 table_info->volume_name);
}
