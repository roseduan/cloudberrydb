#include "datalake_fragment.h"
#include "datalake_option.h"
#include "dlproxy/protocol.h"
#include "dlproxy/iceberg_fragment_cache.h"
#include "common/fileSystemWrapper.h"
#include "common/partition_selector.h"
#include "dlproxy/datalake.h"
#include <curl/curl.h>
#include <uuid/uuid.h>


static List* SerializeFragmentList(datalakeFileInfo* lists, int count, int64_t *totalSize);
static List *get_partition_values(Relation relation, dataLakeOptions *options);
static List *convert_iceberg_hudi_options(dataLakeOptions *options);
static bool ignore_hidden_file(char* name);

static List*
SerializeFragmentList(datalakeFileInfo* lists, int count, int64_t *totalSize)
{
	List	   *serializedFragment = NIL;
	for (int i = 0; i < count; i++)
	{
		if (ignore_hidden_file(lists[i].path))
		{
			if (external_table_debug)
			{
				elog(LOG, "set guc datalake.external_table_ignore_hidden_file ignore hidden path %s", lists[i].path);
			}
			continue;
		}
		if (lists[i].length > 0)
		{
			List *fragment = NIL;
			fragment = lappend(fragment, makeString(pstrdup(lists[i].path)));
			char buf[20] = {0};
			sprintf(buf, "%ld", lists[i].length);
			fragment = lappend(fragment, makeString(pstrdup(buf)));
			serializedFragment = lappend(serializedFragment, fragment);
			if (totalSize)
				*totalSize += lists[i].length;
		}
	}
	return serializedFragment;
}

List *
datalakeGetFragmentList(dataLakeOptions *options, int64_t *totalSize)
{
	List *fragment = NIL;
	ossFileStream stream = datalakeCreateFileSystem((void*) options->gopher);
	int count = 0;
	datalakeFileInfo* lists = datalakeListDir(stream, options->prefix, &count, true);
	fragment = SerializeFragmentList(lists, count, totalSize);
	datalakeFreeFileInfo(lists, count);
	datalakeDestroyHandle(stream);

	return fragment;
}

void datalakeCommitExternalWrite(Relation relation, dataLakeFdwScanState *sstate, List *file_list)
{
	Oid relid = RelationGetRelid(relation);
	List *locations = convert_iceberg_hudi_options(sstate->options);

	if (sstate->cmd == CMD_UPDATE || sstate->cmd == CMD_DELETE)
	{
		internal_commit_external_update(relid, file_list, locations);
		iceberg_fragment_cache_invalidate(relid);
		return;
	}
	commit_external_write(relid, file_list, locations);
	iceberg_fragment_cache_invalidate(relid);
}

char *datalakeGetExternalWriteLocation(Oid relid)
{
	dataLakeOptions *opts = datalakeGetOptions(relid);
	StringInfoData filePrefix;
	initStringInfo(&filePrefix);

	if (opts->format == DL_ICEBERG_TABLE)
	{
		/*
		 * Issue #330: catalog_type and (in the non-hive/polaris path)
		 * table_identifier are required for iceberg writes.  Without these
		 * guards the pg_strcasecmp and datalakeSplitString2 calls below
		 * dereference NULL and crash the backend at INSERT plan time.
		 */
		if (opts->catalog_type == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
					 errmsg("foreign table option \"catalog_type\" is required for iceberg format"),
					 errhint("Specify catalog_type (e.g. 'hive' or 'polaris') in CREATE FOREIGN TABLE OPTIONS.")));

		if (pg_strcasecmp(opts->catalog_type, "hive") == 0 || pg_strcasecmp(opts->catalog_type, "polaris") == 0)
		{
			List *locations = convert_iceberg_hudi_options(opts);
			FDW_TableMeta *tableMeta = get_external_schema_or_create(relid, "iceberg", locations);
			char *real_path = strstr(tableMeta->location, "://") + 3;
			real_path = strchr(real_path, '/');
			appendStringInfo(&filePrefix, "%s/data/", real_path);
			freeFDWTableMeta(tableMeta);
		}
		else
		{
			if (opts->table_identifier == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
						 errmsg("foreign table option \"table_identifier\" is required for iceberg format with catalog_type \"%s\"",
								opts->catalog_type),
						 errhint("Specify table_identifier (e.g. 'schema.table') in CREATE FOREIGN TABLE OPTIONS.")));

			if (opts->prefix)
				appendStringInfoString(&filePrefix, opts->prefix);
			if (filePrefix.len == 0 || filePrefix.data[filePrefix.len - 1] != '/')
				appendStringInfoString(&filePrefix, "/");
			ListCell *lc;
			char *table_identifier = opts->table_identifier;
			List *iden_list = datalakeSplitString2(table_identifier, '.', '/');
			foreach(lc, iden_list)
			{
				appendStringInfo(&filePrefix, "%s/", (char *)lfirst(lc));
			}
			appendStringInfoString(&filePrefix, "data/");
		}
	}
	else
	{
		if (opts->prefix)
			appendStringInfoString(&filePrefix, opts->prefix);
		if (filePrefix.len == 0 || filePrefix.data[filePrefix.len - 1] != '/')
			appendStringInfoString(&filePrefix, "/");
	}
	return filePrefix.data;
}

IcebergTableStatistics *datalakeGetTableStatistics(Oid relid, dataLakeOptions *options)
{
	List *locations = convert_iceberg_hudi_options(options);
	return iceberg_get_current_snapshot_statistics(relid, locations);
}

bool
ignore_hidden_file(char* name)
{
	if (external_table_ignore_hidden_file)
	{
		if (name == NULL)
		{
			return false;
		}
		bool ignore_file = false;
		if (name[0] == '.')
		{
			ignore_file = true;
		}
		else if (strstr(name, "/.") != NULL)
		{
			ignore_file = true;
		}
		return ignore_file;
	}
	return false;
}

static List *
get_partition_values(Relation relation, dataLakeOptions *options)
{
	List *locations = NIL;
	ListCell   *cell;
	StringInfoData partitionkey;
	int index = 1;
	int count = list_length(options->hiveOption->hivePartitionKey);

	initStringInfo(&partitionkey);
	foreach(cell, options->hiveOption->hivePartitionKey)
	{
		char *def = (char *) lfirst(cell);
		appendStringInfo(&partitionkey, "%s%s",
							def,
							(index == count) ? "" : ",");
		index++;
	}

	StringInfoData buf;
	initStringInfo(&buf);
	if (options->hdfs_cluster_name == NULL)
	{
		appendStringInfo(&buf, "s3:/%s hive_cluster_name=%s datasource=%s " \
					"cache=%s transactional=%s partition_keys=%s",
			options->filePath,
			options->hive_cluster_name,
			options->hiveOption->datasource,
			(options->gopher->enableCache) ? "true" : "false",
			(options->hiveOption->transactional) ? "true" : "false",
			partitionkey.data);
	}
	else
	{
		appendStringInfo(&buf, "gphdfs:/%s hive_cluster_name=%s datasource=%s hdfs_cluster_name=%s" \
					"cache=%s transactional=%s partition_keys=%s",
			options->filePath,
			options->hive_cluster_name,
			options->hiveOption->datasource,
			options->hdfs_cluster_name,
			(options->gopher->enableCache) ? "true" : "false",
			(options->hiveOption->transactional) ? "true" : "false",
			partitionkey.data);
	}
	locations = lappend(locations, makeString(pstrdup(buf.data)));

	return datalake_get_external_fragments(RelationGetRelid(relation), 0, NIL, NIL,
								  locations, options->format, false);
}

List *
datalakeGetNextPartitionFragmentList(dataLakeOptions *options, int64_t *totalSize)
{
	ListCell *lckey;
	ListCell *lcvalue;
	List *serializedFragment = NIL;
	List *partitionKeys;
	List *partitionValues;
	datalakePartitionConstraint *pc;

	pc = (datalakePartitionConstraint*) list_nth(options->hiveOption->hivePartitionConstraints,
		options->hiveOption->curPartition);
	partitionValues = pc->partitionValues;
	partitionKeys = options->hiveOption->hivePartitionKey;


	StringInfoData prefix;
	initStringInfo(&prefix);
	if (options->prefix)
		appendStringInfoString(&prefix, options->prefix);
	if (prefix.len == 0 || prefix.data[prefix.len - 1] != '/')
		appendStringInfoString(&prefix, "/");

	forboth(lckey, partitionKeys, lcvalue, partitionValues)
	{
		char *partKey = (char *) lfirst(lckey);
		char *partValue = (char *) lfirst(lcvalue);
		char *escapedValue = curl_escape(partValue, strlen(partValue));

		appendStringInfo(&prefix, "%s=%s/", partKey, partValue);
		curl_free(escapedValue);
	}

	int count = 0;
	ossFileStream stream = datalakeCreateFileSystem((void*) options->gopher);

	datalakeFileInfo* lists = datalakeListDir(stream, prefix.data, &count, true);
	for (int i = 0; i < count; i++)
	{
		if (ignore_hidden_file(lists[i].path))
		{
			if (external_table_debug)
			{
				elog(LOG, "set guc datalake.external_table_ignore_hidden_file ignore hidden path %s", lists[i].path);
			}
			continue;
		}
		if (lists[i].length > 0)
		{
			List *fragment = NIL;
			fragment = lappend(fragment, makeString(pstrdup(lists[i].path)));
			char buf[64] = {0};
			sprintf(buf, "%ld", lists[i].length);
			fragment = lappend(fragment, makeString(pstrdup(buf)));
			serializedFragment = lappend(serializedFragment, fragment);

			if (totalSize)
				totalSize += lists[i].length;
		}
	}
	datalakeFreeFileInfo(lists, count);
	datalakeDestroyHandle(stream);

	return serializedFragment;
}

static List *
GetPartitionList(Relation relation, List *quals, dataLakeOptions *options)
{
	List *serializedFragment = NIL;
	List *partitionValues;

	partitionValues = get_partition_values(relation, options);
	serializedFragment = lappend(serializedFragment, partitionValues);

	/* set curPartition to zero for datalake foreign table to read from begin */
	options->hiveOption->curPartition = 0;
	return serializedFragment;
}

List *
datalakeGetExternalFragmentList(Relation relation, List *quals, dataLakeOptions *options, int64_t *totalSize)
{
	if (FORMAT_IS_ICEBERG(options->format) || FORMAT_IS_HUDI(options->format))
	{
		List *locations = convert_iceberg_hudi_options(options);
		return datalake_get_external_fragments(RelationGetRelid(relation), 0, NIL, NIL,
									  locations, options->format, false);
	}

	if (options->hiveOption->partitiontable)
	{
		return GetPartitionList(relation, quals, options);
	}
	else
	{
		return NIL;
	}
}

static List *
convert_iceberg_hudi_options(dataLakeOptions *options)
{
	List			*result = NIL;
	StringInfoData	buf;

	/*
	 * Issue #330: catalog_type is mandatory for iceberg/hudi.  The write path
	 * has its own up-front check, but this helper is also invoked from the
	 * SELECT path (datalakeGetExternalFragmentList) and from
	 * datalakeGetTableStatistics(), neither of which guarded the option.
	 * Without this check appendStringInfo("%s", NULL) is technically
	 * undefined behaviour (on glibc it materialises as "(null)" and the
	 * user later sees an opaque dlproxy "Internal Server Error" instead
	 * of a clear message about the missing option).  Fail fast here so
	 * every caller gets the same diagnostic.
	 */
	if (options->catalog_type == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
				 errmsg("foreign table option \"catalog_type\" is required for iceberg/hudi format"),
				 errhint("Specify catalog_type (e.g. 'hive' or 'polaris') in CREATE FOREIGN TABLE OPTIONS.")));

	/*
	 * filePath and server_name are likewise interpolated with %s below.
	 * Both are de facto required for iceberg/hudi (every sample in
	 * sql/iceberg_* and sql/hudi_* sets them); the option-name validator
	 * does not enforce presence, so guard here for the same reason as
	 * catalog_type.
	 */
	if (options->filePath == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
				 errmsg("foreign table option \"filePath\" is required for iceberg/hudi format"),
				 errhint("Specify filePath in CREATE FOREIGN TABLE OPTIONS.")));

	if (options->server_name == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
				 errmsg("foreign table option \"server_name\" is required for iceberg/hudi format"),
				 errhint("Specify server_name in CREATE FOREIGN TABLE OPTIONS.")));

	initStringInfo(&buf);
	appendStringInfo(&buf, "datalake://%s catalog_type=%s server_name=%s",
					 options->filePath, options->catalog_type,
					 options->server_name);
	if (options->table_identifier)
		appendStringInfo(&buf, " table_identifier=%s", options->table_identifier);
	if (options->split_size > 0)
		appendStringInfo(&buf, " split_size=%d", options->split_size);
	if (options->cache_enabled)
		appendStringInfo(&buf, " cache_enabled=%s", options->cache_enabled);
	if (options->query_type)
		appendStringInfo(&buf, " query_type=%s", options->query_type);
	if (options->metadata_table_enable)
		appendStringInfo(&buf, " metadata_table_enable=%s", options->metadata_table_enable);

	if (options->client_id)
		appendStringInfo(&buf, " client_id=%s", options->client_id);
	if (options->client_secret)
		appendStringInfo(&buf, " client_secret=%s", options->client_secret);
	if (options->scope)
		appendStringInfo(&buf, " scope=%s", options->scope);
	if (options->polaris_server_url)
		appendStringInfo(&buf, " polaris_server_url=%s", options->polaris_server_url);
	if (options->polaris_server_realm)
		appendStringInfo(&buf, " polaris_server_realm=%s", options->polaris_server_realm);

	result = lappend(result, makeString(pstrdup(buf.data)));
	return result;
}

List *
datalakeDeserializeExternalFragmentList(Relation relation, List *quals, dataLakeOptions *options, List *fragmentInfo)
{
	List *fragmentData = NIL;

	if (FORMAT_IS_ICEBERG(options->format) || FORMAT_IS_HUDI(options->format))
	{
		fragmentData = list_delete_first_n(fragmentInfo, FdwScanPrivateFragmentList);
		return fragmentData;
	}

	if (options->hiveOption->partitiontable)
	{
		List *partitionValues = list_nth(fragmentInfo, PrivatePartitionData);

		/* transfrom partition values */
		options->hiveOption->hivePartitionValues = datalakeTransfromHMSPartitions(partitionValues, options->hiveOption->specifyMaxPartitonValue);
		datalakeInitializeConstraints(options, quals, relation->rd_att);
	}

	return fragmentData;
}

void
datalakeFreeFragmentLists(List *fragments)
{
	ListCell *cell;
	foreach(cell, fragments)
	{
		List *fragment = (List*)lfirst(cell);
		char* filePath = strVal(list_nth(fragment, 0));
		if (filePath)
		{
			pfree(filePath);
		}
		char* length = strVal(list_nth(fragment, 1));
		if (length)
		{
			pfree(length);
		}
	}
	list_free_deep(fragments);
}
