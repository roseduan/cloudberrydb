#include "postgres.h"

#include <jansson.h>
#include "datalake.h"
#include "nodes/nodes.h"
#include "headers.h"
#include "protocol.h"
#include "iceberg_common.h"
#include "icebergConfig.h"
#include "cdb/cdbtm.h"
#include "cdb/cdbvars.h"
#include "utils/guc.h"
#include "uriparser.h"
#include "iceberg_fragment_cache.h"

static FileFormat
convertFileFormat(const char *fileFormat)
{
	FileFormat result;

	if (pg_strcasecmp(fileFormat, "ORC") == 0)
		result = ORC;
	else if (pg_strcasecmp(fileFormat, "PARQUET") == 0)
		result = PARQUET;
	else if (pg_strcasecmp(fileFormat, "AVRO") == 0)
		result = AVRO;
	else
		ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("unsupported iceberg file format \"%s\"", fileFormat)));

	return result;
}

static FileContent
convertFileContent(const char *fileContent)
{
	FileContent result;

	if (pg_strcasecmp(fileContent, "DATA") == 0)
		result = DATA;
	else if (pg_strcasecmp(fileContent, "POSITION_DELETES") == 0)
		result = POSITION_DELETES;
	else if (pg_strcasecmp(fileContent, "EQUALITY_DELETES") == 0)
		result = EQUALITY_DELETES;
	else
		ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("unsupported iceberg file content \"%s\"", fileContent)));

	return result;
}

static List *
parseDeletes(json_t *jdeletes)
{
	int i;
	int j;
	List *deletes = NIL;

	for (i = 0; i < json_array_size(jdeletes); i++)
	{
		json_t *jEqColumnNames;
		json_t *jdata = json_array_get(jdeletes, i);
		json_t *jmetadata = json_object_get(jdata, "metadata");

		FileFragment *fileFragment = palloc0(sizeof(FileFragment));

		fileFragment->filePath = pstrdup(json_string_value(json_object_get(jdata, "sourceName")));
		fileFragment->content = convertFileContent(json_string_value(json_object_get(jmetadata, "fileContent")));
		fileFragment->format = convertFileFormat(json_string_value(json_object_get(jmetadata, "fileFormat")));
		fileFragment->recordCount = json_integer_value(json_object_get(jmetadata, "recordCount"));

		jEqColumnNames = json_object_get(jmetadata, "eqColumnNames");
		if (jEqColumnNames)
		{
			List *eqColumnNames = NIL;
			for (j = 0; j < json_array_size(jEqColumnNames); j++)
			{
				const char *value = json_string_value(json_array_get(jEqColumnNames, j));
				eqColumnNames = lappend(eqColumnNames, makeString(pstrdup(value)));
			}

			fileFragment->eqColumnNames = eqColumnNames;
		}

		fileFragment->type = T_FileFragment;

		deletes = lappend(deletes, fileFragment);
	}

	return deletes;
}

List *
parseIcebergFragmentResponse(char *buffer, size_t buffer_size)
{
	List         *result = NIL;
	json_t       *jroot;
	json_t       *jcombinedTasks;
	json_error_t  jerror;
	int           i;
	int           j;

	jroot = json_loadb(buffer, buffer_size, 0, &jerror);
	if (jroot == NULL)
		elog(ERROR, "failed to decode message:\"%s\", errMessage: %s line: %d",
					pnstrdup(buffer, buffer_size), jerror.text, jerror.line);

	/* set the first element of the list as tableMetadata */
	ExternalTableMetadata *tableMetadata = makeNode(ExternalTableMetadata);
	result = lappend(result, tableMetadata);

	/*
	 * Parse global deduplicated delete files list (new format).
	 * If present, tasks reference deletes by index into this list.
	 */
	json_t *jDeleteFiles = json_object_get(jroot, "deleteFiles");
	List   *globalDeleteFiles = NIL;

	if (jDeleteFiles && json_is_array(jDeleteFiles))
	{
		globalDeleteFiles = parseDeletes(jDeleteFiles);
	}

	jcombinedTasks = json_object_get(jroot, "combinedTasks");
	for (i = 0; i < json_array_size(jcombinedTasks); i++)
	{
		List *combinedTask = NIL;
		json_t *jtasks = json_object_get(json_array_get(jcombinedTasks, i), "tasks");

		for (j = 0; j < json_array_size(jtasks); j++)
		{
			json_t       *jtask = json_array_get(jtasks, j);
			json_t       *jdata = json_object_get(jtask, "data");
			json_t       *jdeletes = json_object_get(jtask, "deletes");
			json_t       *jDeleteIndexes = json_object_get(jtask, "deleteIndexes");
			json_t       *jmetadata = json_object_get(jdata, "metadata");
			FileScanTask *fileScanTask = palloc0(sizeof(FileScanTask));
			FileFragment *dataFragment = palloc0(sizeof(FileFragment));

			fileScanTask->start = json_integer_value(json_object_get(jtask, "start"));
			fileScanTask->length = json_integer_value(json_object_get(jtask, "length"));

			dataFragment->type = T_FileFragment;
			dataFragment->filePath = pstrdup(json_string_value(json_object_get(jdata, "sourceName")));
			dataFragment->content = convertFileContent(json_string_value(json_object_get(jmetadata, "fileContent")));
			dataFragment->format = convertFileFormat(json_string_value(json_object_get(jmetadata, "fileFormat")));
			dataFragment->recordCount = json_integer_value(json_object_get(jmetadata, "recordCount"));

			fileScanTask->dataFile = dataFragment;

			/*
			 * Resolve delete files: prefer deleteIndexes (new dedup format),
			 * fall back to inline deletes array (backward compat).
			 */
			if (jDeleteIndexes && json_array_size(jDeleteIndexes) > 0 && globalDeleteFiles != NIL)
			{
				int k;
				fileScanTask->deletes = NIL;
				for (k = 0; k < json_array_size(jDeleteIndexes); k++)
				{
					int idx = json_integer_value(json_array_get(jDeleteIndexes, k));
					if (idx < 0 || idx >= list_length(globalDeleteFiles))
					{
						elog(WARNING, "invalid delete file index %d (total %d), skipping",
							 idx, list_length(globalDeleteFiles));
						continue;
					}
					fileScanTask->deletes = lappend(fileScanTask->deletes,
													list_nth(globalDeleteFiles, idx));
				}
			}
			else
			{
				fileScanTask->deletes = parseDeletes(jdeletes);
			}

			fileScanTask->type = T_FileScanTask;

			combinedTask = lappend(combinedTask, fileScanTask);
		}

		result = lappend(result, combinedTask);
	}

	json_decref(jroot);

	return result;
}

/*
 * Parse snapshotId and notModified from the JSON response.
 * Sets *snapshotId and *notModified. Returns false if JSON parse fails.
 */
static bool
parseFragmentMetadata(const char *buffer, size_t buffer_size,
					  int64 *snapshotId, bool *notModified)
{
	json_t       *jroot;
	json_error_t  jerror;
	json_t       *jval;

	*snapshotId = -1;
	*notModified = false;

	jroot = json_loadb(buffer, buffer_size, 0, &jerror);
	if (jroot == NULL)
		return false;

	jval = json_object_get(jroot, "snapshotId");
	if (jval && json_is_integer(jval))
		*snapshotId = json_integer_value(jval);

	jval = json_object_get(jroot, "notModified");
	if (jval && json_is_true(jval))
		*notModified = true;

	json_decref(jroot);
	return true;
}

extern bool enable_iceberg_fragment_cache;

List *
iceberg_get_external_fragments(Oid relid,
							   Index relno,
							   List *restrictInfo,
							   List *targetList,
							   List *locations)
{
	if (!enable_iceberg_fragment_cache)
	{
		return internal_get_external_fragments("iceberg",
												relid,
												relno,
												restrictInfo,
												targetList,
												locations,
												parseIcebergFragmentResponse);
	}

	/*
	 * Fragment caching with ETag-style snapshot ID validation.
	 *
	 * 1. Look up cache entry for this relid
	 * 2. If cached, send if-snapshot-id header to Java
	 * 3. If Java returns notModified, reparse from cached JSON
	 * 4. Otherwise, store new response and parse it
	 */
	{
		IcebergFragmentCacheEntry *cached;
		int64   cachedSnapshotId = -1;
		List   *result = NIL;
		volatile datalake_gphadoop_context *context = NULL;

		cached = iceberg_fragment_cache_lookup(relid);
		if (cached != NULL)
			cachedSnapshotId = cached->snapshot_id;

		PG_TRY();
		{
			char *catalogType = get_catalog_type("iceberg", locations);

			context = datalake_create_fragment_context(relid,
											  relno,
											  restrictInfo,
											  targetList,
											  strVal(linitial(locations)),
											  transform_datalake_options);
			datalake_churl_headers_append(context->churl_headers, "X-GP-OPTIONS-PROFILE", "iceberg");

			if (pg_strcasecmp(catalogType, "hive") == 0)
				datalake_churl_headers_append(context->churl_headers, "X-GP-OPTIONS-CONFIG", "gphive.conf0gphdfs.conf");
			else if (pg_strcasecmp(catalogType, "s3") == 0)
				datalake_churl_headers_append(context->churl_headers, "X-GP-OPTIONS-CONFIG", "s3.conf");
			else
				datalake_churl_headers_append(context->churl_headers, "X-GP-OPTIONS-CONFIG", "gphdfs.conf");

			datalake_churl_headers_append(context->churl_headers, "X-GP-OPTIONS-CATALOG-TYPE", catalogType);
			pfree(catalogType);

			datalake_churl_headers_append(context->churl_headers, "X-GP-OPTIONS-SCAN-TYPE", "snapshot");
			datalake_churl_headers_append(context->churl_headers, "X-GP-OPTIONS-METHOD", "getFragments");

			/*
			 * Attach the iceberg config JSON (gopher connection keys etc.)
			 * exactly like internal_get_external_fragments() does.  This
			 * cached variant used to omit it, so the agent had no
			 * gopher.worker_path / volume credentials and failed to
			 * initialize GopherFileIO for catalogs that need them, e.g.
			 * Polaris ("Gopher Worker path is required", issue #844).
			 */
			{
				char *jsonConfig = getIcebergConfigJsonString(relid);

				if (jsonConfig != NULL && strlen(jsonConfig) > 0)
				{
					context->request_body = pstrdup(jsonConfig);
					context->request_body_len = strlen(jsonConfig);
				}
			}

			/* Send conditional snapshot ID if we have a cached entry */
			if (cachedSnapshotId >= 0)
			{
				char buf[32];
				snprintf(buf, sizeof(buf), INT64_FORMAT, cachedSnapshotId);
				datalake_churl_headers_append(context->churl_headers, "X-GP-OPTIONS-IF-SNAPSHOT-ID", buf);
			}

			datalakeDoRPC((datalake_gphadoop_context *) context);

			/* Check response for notModified */
			{
				int64  respSnapshotId = -1;
				bool   respNotModified = false;

				parseFragmentMetadata(context->buffer, context->buffer_pos,
									  &respSnapshotId, &respNotModified);

				if (respNotModified && cached != NULL)
				{
					/* Cache hit: reparse from cached JSON */
					elog(DEBUG1, "iceberg fragment cache hit for relid %u (snapshot " INT64_FORMAT ")",
						 relid, cachedSnapshotId);
					result = parseIcebergFragmentResponse(cached->raw_json, cached->raw_json_size);
				}
				else
				{
					/* Cache miss: parse response and store in cache */
					elog(DEBUG1, "iceberg fragment cache miss for relid %u (snapshot " INT64_FORMAT ")",
						 relid, respSnapshotId);
					result = parseIcebergFragmentResponse(context->buffer, context->buffer_pos);

					if (respSnapshotId >= 0)
						iceberg_fragment_cache_store(relid, respSnapshotId,
													context->buffer, context->buffer_pos);
				}
			}

			datalake_destroy_context((datalake_gphadoop_context *) context, false);
			context = NULL;
		}
		PG_CATCH();
		{
			if (context)
				datalake_destroy_context((datalake_gphadoop_context *) context, true);
			PG_RE_THROW();
		}
		PG_END_TRY();

		return result;
	}
}

extern void
iceberg_commit_external_write(Oid relid,
							  List *file_list,
							  List *location)
{
	internal_commit_external_write(relid, file_list, location);
}

static List*
parseStatisticsResponse(char *buffer, size_t buffer_size)
{
	List		 *result = NIL;
	json_t       *jroot;
	json_error_t  jerror;

	jroot = json_loadb(buffer, buffer_size, 0, &jerror);
	if (jroot == NULL)
		elog(ERROR, "failed to decode message:\"%s\", errMessage: %s line: %d",
					pnstrdup(buffer, buffer_size), jerror.text, jerror.line);

	IcebergTableStatistics *icebergStatistics = palloc(sizeof(IcebergTableStatistics));
	const char *records = json_string_value(json_object_get(jroot, "total-records"));
	const char *fileSize = json_string_value(json_object_get(jroot, "total-files-size"));
	icebergStatistics->recordCount = records ? atoll(records) : 0;
	icebergStatistics->bytesInDataFile = fileSize ? atoll(fileSize) : 0;
	json_decref(jroot);
	result = lappend(result, icebergStatistics);

	return result;
}

IcebergTableStatistics*
iceberg_get_current_snapshot_statistics(Oid relid, List *locations)
{
	/* Assert relid is valid */
	Assert(OidIsValid(relid));

	/* Assert locations list is not empty */
	Assert(locations != NIL);
	Assert(list_length(locations) > 0);
	Assert(linitial(locations) != NULL);

	return internal_get_current_snapshot_statistics(relid, locations, parseStatisticsResponse);
}
