/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

#include "postgres.h"

#include <jansson.h>
#include "headers.h"
#include "protocol.h"
#include "filters.h"
#include "deparse.h"

#include "access/table.h"
#include "cdb/cdbtm.h"
#include "cdb/cdbvars.h"
#include "utils/guc.h"
#include "datalake.h"
#include "optimizer/optimizer.h"

#include "src/datalake_def.h"
#include "src/common/fileMetadata.h"
#include "icebergConfig.h"

#define BUFFER_SIZE 4096

typedef struct
{
	char *typeName;
	Oid  typeOid;
} TypeInfoElt;

/* helper function declarations */
static void build_uri_for_read(datalake_gphadoop_context *context);
static void build_uri_for_write(datalake_gphadoop_context *context);
static void add_querydata_to_http_headers(datalake_gphadoop_context *context, transform_callback transform);
static void add_write_querydata_to_http_headers(datalake_gphadoop_context *context, transform_callback transform);
static size_t fill_buffer(datalake_gphadoop_context *context, char *start, size_t size);
static void expand_buffer_if_needed(datalake_gphadoop_context *context);
static void read_all_response_data(datalake_gphadoop_context *context);
static void read_all_post_response_data(datalake_gphadoop_context *context);

static const TypeInfoElt typeInfoElts[] = {
	{"int4", INT4OID},
	{"int8", INT8OID},
	{"bool", BOOLOID},
	{"float4", FLOAT4OID},
	{"float8", FLOAT8OID},
	{"text", TEXTOID},
	{"bytea", BYTEAOID},
	{"timestamp", TIMESTAMPOID},
	{"timestamptz", TIMESTAMPTZOID},
	{"date", DATEOID},
	{"time", TIMEOID},
	{"uuid", UUIDOID},
	{"numeric", NUMERICOID}
};

static Oid
getFieldTypeOidByName(const char *typeName)
{
	int i;

	for (i = 0; i < lengthof(typeInfoElts); i++)
	{
		if (!strcmp(typeInfoElts[i].typeName, typeName))
			return typeInfoElts[i].typeOid;
	}

	ereport(ERROR,
			(errcode(ERRCODE_DATATYPE_MISMATCH),
			 errmsg("unsupported data type \"%s\"", typeName)));
}

char *
get_catalog_type(char *profile, List *locations)
{
	DatalakeGPHDUri *uri;
	char    *catalogType;

	uri = datalake_parseGPHDUri(strVal(linitial(locations)));
	catalogType = pstrdup(datalake_getOptionValue(uri, CATALOG_TYPE));

	if (!(pg_strcasecmp(profile, "hudi") == 0 &&
			pg_strcasecmp(catalogType, "hadoop") == 0))
	{
		List *coreOptions = list_make1(TABLE_IDENTIFIER);
		datalake_GPHDUri_verify_core_options_exist(uri, coreOptions);
		list_free(coreOptions);
	}

	datalake_freeGPHDUri(uri);

	return catalogType;
}

const char *
transform_datalake_options(const char *key)
{
	int i;
	static datalake_option_mapping configElts[] = {
		{"server_name", "server"},
		{"table_identifier", "data-source"},
		{"split_size", "split-size"},
		{"query_type", "query-type"},
		{"metadata_table_enable", "metadata-table-enable"}
	};

	for (i = 0; i < lengthof(configElts); i++)
	{
		if (!strcmp(configElts[i].option_name, key))
			return configElts[i].property_name;
	}

	return key;
}

static FDW_TableMeta *
parseSchemaResponse(char *buffer, size_t buffer_size)
{
	FDW_TableMeta *result = palloc0(sizeof(FDW_TableMeta));
	List         *fields = NIL;
	json_t       *jroot;
	json_t       *jfields;
	json_t		 *jlocation;
	json_error_t  jerror;
	int           i;

	jroot = json_loadb(buffer, buffer_size, 0, &jerror);
	if (jroot == NULL)
		elog(ERROR, "failed to decode message:\"%s\", errMessage: %s line: %d",
					pnstrdup(buffer, buffer_size), jerror.text, jerror.line);

	jfields = json_object_get(jroot, "fields");
	for (i = 0; i < json_array_size(jfields); i++)
	{
		json_t *jmods;
		json_t *jfield = json_array_get(jfields, i);
		datalakeTableFieldDefination *columnDef = palloc0(sizeof(datalakeTableFieldDefination));

		columnDef->fieldName = pstrdup(json_string_value(json_object_get(jfield, "name")));
		columnDef->fieldTypeName = pstrdup(json_string_value(json_object_get(jfield, "type")));
		columnDef->fieldTypeOid = getFieldTypeOidByName(columnDef->fieldTypeName);

		jmods = json_object_get(jfield, "modifiers");
		if (jmods)
		{
			columnDef->fieldTypeMod1 = atoi(json_string_value(json_array_get(jmods, 0)));
			columnDef->fieldTypeMod2 = atoi(json_string_value(json_array_get(jmods, 1)));
		}

		fields = lappend(fields, columnDef);
	}

	jlocation = json_object_get(jroot, "location");
	if (jlocation)
	{
		result->location = pstrdup(json_string_value(jlocation));
	}

	json_decref(jroot);

	return result;
}

/*
 * De-allocates context and dependent structures.
 */
void
datalake_destroy_context(datalake_gphadoop_context *context, bool afterError)
{
	if (context == NULL)
		return;

	datalake_churl_cleanup(context->churl_handle, afterError);
	context->churl_handle = NULL;

	datalake_churl_headers_cleanup(context->churl_headers);
	context->churl_headers = NULL;

	if (context->relation)
		heap_close(context->relation, NoLock);

	if (context->gphd_uri != NULL)
	{
		datalake_freeGPHDUri(context->gphd_uri);
		context->gphd_uri = NULL;
	}

	if (context->filterstr != NULL)
	{
		pfree(context->filterstr);
		context->filterstr = NULL;
	}

	/* Free request body if it was allocated */
	if (context->request_body != NULL)
	{
		pfree(context->request_body);
		context->request_body = NULL;
	}

	pfree(context->uri.data);
	pfree(context->buffer);
	pfree(context);
}

/*
 * Allocates context and sets values for the segment
 */

static datalake_gphadoop_context *
create_context_(Oid relid,
				Index relno,
				char *relName,
				char *schemaName,
				List *restrictInfo,
				List *targetList,
				const char *uriStr,
				transform_callback transform)
{
	List *remoteConds;
	List *localConds;
	Bitmapset  *attrsUsed = NULL;
	/* parse and set uri */
	DatalakeGPHDUri        *uri = datalake_parseGPHDUri(uriStr);

	/* set context */
	datalake_gphadoop_context *context = palloc0(sizeof(datalake_gphadoop_context));

	context->gphd_uri = uri;
	initStringInfo(&context->uri);

	context->relName = relName;
	context->schemaName = schemaName;

	/*
	 * Identify which baserestrictinfo clauses can be sent to the remote
	 * server and which can't.
	 */
	datalakeClassifyConditions(restrictInfo, &remoteConds, &localConds);
	context->filterstr = serializeDlProxyFilterQuals(remoteConds);
	context->quals = localConds;

	if (OidIsValid(relid))
	{
		ListCell *lc;
		List *retrieved_attrs = NIL;

		context->relation = table_open(relid, NoLock);

		/*
		 * Identify which attributes will need to be retrieved from the remote
		 * server
		 */
		pull_varattnos((Node *) targetList, relno, &attrsUsed);

		foreach(lc, localConds)
		{
			Node *rinfo = (Node *) lfirst(lc);

			pull_varattnos((Node *) rinfo, relno, &attrsUsed);
		}

		datalakeDeparseTargetList(context->relation, attrsUsed, &retrieved_attrs);
		context->retrieved_attrs = retrieved_attrs;
	}

	build_uri_for_read(context);
	context->churl_headers = datalake_churl_headers_init();
	add_querydata_to_http_headers(context, transform);

	context->buffer = palloc(BUFFER_SIZE);
	context->buffer_size = BUFFER_SIZE;

	/* Initialize request body fields */
	context->request_body = NULL;
	context->request_body_len = 0;

	return context;
}

datalake_gphadoop_context *
datalake_create_write_context_(Oid relid,
								List *fileList,
								const char *uriStr,
								transform_callback transform)
{
	/* parse and set uri */
	DatalakeGPHDUri        *uri = datalake_parseGPHDUri(uriStr);

	/* set context */
	datalake_gphadoop_context *context = palloc0(sizeof(datalake_gphadoop_context));

	context->gphd_uri = uri;
	initStringInfo(&context->uri);

	/*
	 * Open relation with NoLock.
	 *
	 * Safe here because this is only called from dataLakeEndForeignModify()
	 * during INSERT/UPDATE/DELETE, where RowExclusiveLock is already held
	 * on the relation by the executor. The outer lock prevents DROP/ALTER.
	 */
	if (OidIsValid(relid))
	{
		context->relation = table_open(relid, NoLock);
	}
	build_uri_for_write(context);
	context->churl_headers = datalake_churl_headers_init();
	context->file_list = fileList;
	add_write_querydata_to_http_headers(context, transform);

	context->buffer = palloc(BUFFER_SIZE);
	context->buffer_size = BUFFER_SIZE;

	return context;
}

datalake_gphadoop_context *
datalake_create_context(Oid relid, const char *uriStr, transform_callback transform)
{
	return create_context_(relid, 0, NULL, NULL, NULL, NULL, uriStr, transform);
}

datalake_gphadoop_context *
datalake_create_context2(char *relName, char *schemaName, const char *uriStr, transform_callback transform)
{
	return create_context_(InvalidOid, 0, relName, schemaName, NULL, NULL, uriStr, transform);
}


datalake_gphadoop_context *
datalake_create_fragment_context(Oid relid,
						Index relno,
						List *restrictInfo,
						List *targetList,
						const char *uriStr,
						transform_callback transform)
{
	return create_context_(relid, relno, NULL, NULL, restrictInfo, targetList, uriStr, transform);
}

/*
 * Perform a single RPC attempt (no retry).
 */
static void
datalakeDoRPC_once(datalake_gphadoop_context *context)
{
	/*
	 * Determine if this is a write operation with file_list to send.
	 * If file_list is present and non-empty, use POST with JSON body.
	 * Otherwise, use GET (download).
	 */
	if (context->file_list != NIL && list_length(context->file_list) > 0)
	{
		/* Write operation: use POST with file_list in request body */
		StringInfoData json_data;

		FDW_serialize_file_list_to_json(context->file_list, &json_data);

		/*
		 * Iceberg commit (batchAppend/rowUpdate) also needs the gopher socket
		 * config (gopher.worker_path etc.) so the agent can initialize the
		 * catalog's GopherFileIO; the commit path stores that config JSON in
		 * request_body. The file-list POST body would otherwise carry only
		 * {"files":[...]} and the agent fails with "Gopher Worker path is
		 * required". Merge the config object's members into the file-list
		 * object so the body is {"files":[...], <config keys incl. gopher>}.
		 */
		if (context->request_body != NULL && context->request_body_len > 2 &&
			context->request_body[0] == '{' &&
			strcmp(context->request_body, "{}") != 0 &&
			json_data.len > 0 && json_data.data[json_data.len - 1] == '}')
		{
			json_data.len--;					/* drop the file-list object's closing '}' */
			json_data.data[json_data.len] = '\0';
			appendStringInfoChar(&json_data, ',');
			/* skip the config object's leading '{'; keep its trailing '}' */
			appendStringInfoString(&json_data, context->request_body + 1);
		}

		elog(DEBUG2, "datalakeDoRPC: sending POST request with JSON body size: %d, file count: %d",
			 (int)json_data.len,
			 list_length(context->file_list));

		/* Use POST (upload) to send request body */
		context->churl_handle = datalake_churl_init_upload(context->uri.data, context->churl_headers);

		/*
		 * Override the default "Content-Type: application/octet-stream"
		 * set by datalake_churl_init_upload.  The dlagent /dlproxy/write
		 * endpoint is annotated with consumes=APPLICATION_JSON_VALUE and
		 * deserializes the body as a FileListRequest JSON object, so the
		 * request must advertise Content-Type: application/json or Spring
		 * will reject it with HttpMediaTypeNotSupportedException before
		 * the handler is invoked.
		 */
		datalake_churl_headers_override(context->churl_headers,
										"Content-Type", "application/json");

		/* Write JSON data to request body */
		datalake_churl_write(context->churl_handle, json_data.data, json_data.len);

		/* Read all response data */
		read_all_post_response_data(context);

		context->completed = true;

		/* Clean up resources */
		datalake_churl_cleanup(context->churl_handle, false);
		context->churl_handle = NULL;

		/* Clean up JSON buffer with NULL check for safety */
		if (json_data.data)
			pfree(json_data.data);
	}
	else if (context->request_body != NULL && context->request_body_len > 0)
	{
		/* For requests with body payload (POST with JSON configuration) */
		context->churl_handle = datalake_churl_init_upload(context->uri.data, context->churl_headers);

		/*
		 * datalake_churl_init_upload stamps "Content-Type:
		 * application/octet-stream", but this branch always carries the
		 * iceberg config JSON; the agent's POST /dlproxy/read mapping
		 * consumes application/json, so fix the header up (mirrors the
		 * /dlproxy/write handling in the file_list branch above).
		 */
		datalake_churl_headers_override(context->churl_headers,
										"Content-Type", "application/json");

		datalake_churl_write(context->churl_handle, context->request_body, context->request_body_len);
		/* Signal end of data */
		datalake_churl_write(context->churl_handle, NULL, 0);
		read_all_post_response_data(context);
		context->completed = true;
		datalake_churl_cleanup(context->churl_handle, false);
		context->churl_handle = NULL;
		/* Free request body */
		pfree(context->request_body);
		context->request_body = NULL;
		context->request_body_len = 0;
	}
	else
	{
		/* Read operation: use GET (download) */
		context->churl_handle = datalake_churl_init_download(context->uri.data, context->churl_headers);

		/* read some bytes to make sure the connection is established */
		datalake_churl_read_check_connectivity(context->churl_handle);

		/* Read all response data */
		read_all_response_data(context);

		context->completed = true;

		/* check if the connection terminated with an error */
		datalake_churl_read_check_connectivity(context->churl_handle);
	}
}

/*
 * Check if an error indicates that dlagent is not reachable (not yet started
 * or temporarily unavailable).  Only these transient connection errors are
 * worth retrying.
 */
static bool
is_dlagent_connect_error(ErrorData *edata)
{
	/*
	 * libchurl reports every dlproxy/dlagent HTTP error response with
	 * ERRCODE_CONNECTION_EXCEPTION, so the sqlerrcode alone cannot
	 * distinguish "agent not up yet" from a genuine server-side failure.
	 * Treating the latter as transient repeated it ten times under a
	 * misleading "dlagent not ready" message before surfacing the real
	 * error (issue #844).  Match the curl connect-failure texts instead.
	 */
	if (edata->message &&
		(strstr(edata->message, "Couldn't connect") ||
		 strstr(edata->message, "couldn't connect") ||
		 strstr(edata->message, "Connection refused")))
		return true;

	return false;
}

#define DLAGENT_CONNECT_MAX_RETRIES     10
#define DLAGENT_CONNECT_RETRY_INTERVAL_SEC 1

void
datalakeDoRPC(datalake_gphadoop_context *context)
{
	for (int attempt = 0; ; attempt++)
	{
		CHECK_FOR_INTERRUPTS();
		MemoryContext oldcontext = CurrentMemoryContext;
		bool should_retry = false;

		PG_TRY();
		{
			datalakeDoRPC_once(context);
			return; /* success */
		}
		PG_CATCH();
		{
			MemoryContextSwitchTo(oldcontext);

			ErrorData *edata = CopyErrorData();

			if (is_dlagent_connect_error(edata) &&
				attempt < DLAGENT_CONNECT_MAX_RETRIES)
			{
				FlushErrorState();
				FreeErrorData(edata);

				/* Clean up the failed connection */
				if (context->churl_handle)
				{
					datalake_churl_cleanup(context->churl_handle, true);
					context->churl_handle = NULL;
				}
				context->buffer_pos = 0;
				context->completed = false;

				elog(LOG, "dlagent not ready, retrying in %d second(s) (%d/%d)...",
					 DLAGENT_CONNECT_RETRY_INTERVAL_SEC,
					 attempt + 1, DLAGENT_CONNECT_MAX_RETRIES);

				pg_usleep(DLAGENT_CONNECT_RETRY_INTERVAL_SEC * 1000000L);
				should_retry = true;
			}
			else
			{
				FreeErrorData(edata);
				PG_RE_THROW();
			}
		}
		PG_END_TRY();

		if (!should_retry)
			break;
	}
}

/*
 * Expand buffer if needed by doubling its size
 */
static void
expand_buffer_if_needed(datalake_gphadoop_context *context)
{
	if (context->buffer_pos == context->buffer_size)
	{
		context->buffer_size *= 2;
		context->buffer = repalloc(context->buffer, context->buffer_size);
	}
}

/*
 * Read all response data into buffer for GET requests
 */
static void
read_all_response_data(datalake_gphadoop_context *context)
{
	size_t n;
	while ((n = fill_buffer(context,
							context->buffer + context->buffer_pos,
							context->buffer_size - context->buffer_pos)) != 0)
	{
		context->buffer_pos += n;
		expand_buffer_if_needed(context);
	}
}

/*
 * Read all response data into buffer for POST requests
 */
static void
read_all_post_response_data(datalake_gphadoop_context *context)
{
	size_t n;
	while ((n = datalake_churl_finish_upload_and_read(context->churl_handle,
													   context->buffer + context->buffer_pos,
													   context->buffer_size - context->buffer_pos)) != 0)
	{
		context->buffer_pos += n;
		expand_buffer_if_needed(context);
	}
}

/*
 * Format the URI for reading by adding dlproxy service endpoint details
 */
static void
build_uri_for_read(datalake_gphadoop_context *context)
{
	resetStringInfo(&context->uri);
	appendStringInfo(&context->uri, "http://%s/%s/read",
					 datalake_get_authority(), DLPROXY_SERVICE_PREFIX);

	if ((LOG >= log_min_messages) || (LOG >= client_min_messages))
	{
		appendStringInfo(&context->uri, "?trace=true");
	}

	elog(DEBUG2, "dlproxy: uri %s for read", context->uri.data);
}

static void 
build_uri_for_write(datalake_gphadoop_context *context)
{
	resetStringInfo(&context->uri);
	appendStringInfo(&context->uri, "http://%s/%s/write",
					 datalake_get_authority(), DLPROXY_SERVICE_PREFIX);

	if ((LOG >= log_min_messages) || (LOG >= client_min_messages))
	{
		appendStringInfo(&context->uri, "?trace=true");
	}

	elog(DEBUG2, "dlproxy: uri %s for write", context->uri.data);
}

/*
 * Add key/value pairs to connection header. These values are the context of the query and used
 * by the remote component.
 */
static void
add_querydata_to_http_headers(datalake_gphadoop_context *context, transform_callback transform)
{
	DlProxyInputData inputData = {0};

	inputData.headers   = context->churl_headers;
	inputData.gphduri   = context->gphd_uri;
	inputData.rel       = context->relation;
	inputData.filterstr = context->filterstr;
	inputData.retrieved_attrs = context->retrieved_attrs;
	inputData.quals = context->quals;
	inputData.relName = context->relName;
	inputData.schemaName     = context->schemaName;
	inputData.file_list = NIL;
	datalake_build_http_headers(&inputData, transform);
}

static void
add_write_querydata_to_http_headers(datalake_gphadoop_context *context, transform_callback transform)
{
	DlProxyInputData inputData = {0};

	inputData.headers		= context->churl_headers;
	inputData.gphduri		= context->gphd_uri;
	inputData.rel			= context->relation;
	inputData.relName		= context->relName;
	inputData.schemaName	= context->schemaName;
	inputData.file_list		= context->file_list;
	datalake_build_http_headers(&inputData, transform);
}

/*
 * Read data from churl until the buffer is full or there is no more data to be read
 */
static size_t
fill_buffer(datalake_gphadoop_context *context, char *start, size_t size)
{

	size_t		n;
	char	   *ptr = start;
	char	   *end = ptr + size;

	while (ptr < end)
	{
		n = datalake_churl_read(context->churl_handle, ptr, end - ptr);
		if (n == 0)
			break;

		ptr += n;
	}

	return ptr - start;
}

List *
internal_get_external_fragments(char *profile,
								Oid relid,
								Index relno,
								List *restrictInfo,
								List *targetList,
								List *locations,
								parse_callback parseFn)
{
	List *result = NIL;
	volatile datalake_gphadoop_context *context = NULL;

	PG_TRY();
	{
		char *catalogType = get_catalog_type(profile, locations);

		context = datalake_create_fragment_context(relid,
										  relno,
										  restrictInfo,
										  targetList,
										  strVal(linitial(locations)),
										  transform_datalake_options);
		datalake_churl_headers_append(context->churl_headers, "X-GP-OPTIONS-PROFILE", profile);

		if (pg_strcasecmp(catalogType, "hive") == 0)
		{
			datalake_churl_headers_append(context->churl_headers, "X-GP-OPTIONS-CONFIG", "gphive.conf0gphdfs.conf");
		}
		else if (pg_strcasecmp(catalogType, "s3") == 0)
		{
			datalake_churl_headers_append(context->churl_headers, "X-GP-OPTIONS-CONFIG", "s3.conf");
		}
		else
		{
			datalake_churl_headers_append(context->churl_headers, "X-GP-OPTIONS-CONFIG", "gphdfs.conf");
		}

		datalake_churl_headers_append(context->churl_headers, "X-GP-OPTIONS-CATALOG-TYPE", catalogType);
		pfree(catalogType);

		datalake_churl_headers_append(context->churl_headers, "X-GP-OPTIONS-SCAN-TYPE", "snapshot");
		datalake_churl_headers_append(context->churl_headers, "X-GP-OPTIONS-METHOD", "getFragments");

		/*
		 * Add JSON configuration to the request (Iceberg only).  Content-Type
		 * is fixed up inside datalakeDoRPC_once (the upload init would
		 * otherwise stamp octet-stream); no Content-Length here -- the upload
		 * path uses chunked transfer encoding and a conflicting length header
		 * makes servers reject the request.
		 */
		if (pg_strcasecmp(profile, "iceberg") == 0)
		{
			char *jsonConfig = getIcebergConfigJsonString(relid);
			if (jsonConfig != NULL && strlen(jsonConfig) > 0)
			{
				context->request_body = pstrdup(jsonConfig);
				context->request_body_len = strlen(jsonConfig);
			}
		}

		datalakeDoRPC((datalake_gphadoop_context *) context);
		result = parseFn(context->buffer, context->buffer_pos);

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

static void
internal_commit_external_common(Oid relid, List *file_list, List *locations,
								const char *method)
{
	volatile datalake_gphadoop_context *context = NULL;

	PG_TRY();
	{
		char *catalogType = get_catalog_type("iceberg", locations);

		context = datalake_create_write_context_(relid,
												file_list,
												strVal(linitial(locations)),
												transform_datalake_options);
		datalake_churl_headers_append(context->churl_headers, "X-GP-OPTIONS-PROFILE", "iceberg");

		/*
		 * The iceberg commit (batchAppend/rowUpdate) runs through the agent's
		 * iceberg service, which needs the gopher socket paths to reach the
		 * filesystem. The read/scan path supplies these via the request body
		 * (getIcebergConfigJsonString), but the commit path historically did
		 * not, so the agent fell back to a non-existent default gopher socket
		 * ("/tmp/.gopher.socket") and failed with
		 * "Failed to get file system for path: s3a://...". Send the same config
		 * here so the agent connects to the per-segment gopher socket.
		 */
		{
			char *jsonConfig = getIcebergConfigJsonString(relid);
			if (jsonConfig != NULL && strlen(jsonConfig) > 0)
			{
				context->request_body = pstrdup(jsonConfig);
				context->request_body_len = strlen(jsonConfig);
			}
		}

		if (pg_strcasecmp(catalogType, "hive") == 0)
		{
			datalake_churl_headers_append(context->churl_headers, "X-GP-OPTIONS-CONFIG", "gphive.conf0gphdfs.conf");
		}
		else if (pg_strcasecmp(catalogType, "s3") == 0)
		{
			datalake_churl_headers_append(context->churl_headers, "X-GP-OPTIONS-CONFIG", "s3.conf");
		}
		else
		{
			datalake_churl_headers_append(context->churl_headers, "X-GP-OPTIONS-CONFIG", "gphdfs.conf");
		}

		datalake_churl_headers_append(context->churl_headers, "X-GP-OPTIONS-CATALOG-TYPE", catalogType);
		pfree(catalogType);

		datalake_churl_headers_append(context->churl_headers, "X-GP-OPTIONS-METHOD", method);

		datalakeDoRPC((datalake_gphadoop_context *) context);

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
}


void
internal_commit_external_write(Oid relid,
								List *file_list,
								List *locations)
{
	internal_commit_external_common(relid, file_list, locations, "batchAppend");
}

void
internal_commit_external_update(Oid relid, List *file_list, List *locations)
{
	internal_commit_external_common(relid, file_list, locations, "rowUpdate");
}

IcebergTableStatistics*
internal_get_current_snapshot_statistics(Oid relid, List *locations, parse_callback parseFn)
{
	List				   *result = NIL;
	IcebergTableStatistics *statistics = NULL;
	volatile datalake_gphadoop_context *context = NULL;

	PG_TRY();
	{
		context = create_context_(relid,
								0,
								NULL,
								NULL,
								NULL,
								NULL,
								strVal(linitial(locations)),
								transform_datalake_options);

		datalake_churl_headers_append(context->churl_headers, "X-GP-OPTIONS-PROFILE", "iceberg");

		char *catalogType = get_catalog_type("iceberg", locations);
		if (pg_strcasecmp(catalogType, "hive") == 0)
		{
			datalake_churl_headers_append(context->churl_headers, "X-GP-OPTIONS-CONFIG", "gphive.conf0gphdfs.conf");
		}
		else if (pg_strcasecmp(catalogType, "s3") == 0)
		{
			datalake_churl_headers_append(context->churl_headers, "X-GP-OPTIONS-CONFIG", "s3.conf");
		}
		else
		{
			datalake_churl_headers_append(context->churl_headers, "X-GP-OPTIONS-CONFIG", "gphdfs.conf");
		}

		datalake_churl_headers_append(context->churl_headers, "X-GP-OPTIONS-CATALOG-TYPE", catalogType);
		pfree(catalogType);

		datalake_churl_headers_append(context->churl_headers, "X-GP-OPTIONS-METHOD", "getCurrentSnapshotSummary");

		datalakeDoRPC((datalake_gphadoop_context *) context);
		result = parseFn(context->buffer, context->buffer_pos);
		statistics = (IcebergTableStatistics *) linitial(result);

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

	return statistics;
}

List *
datalake_get_external_fragments(Oid relid,
					   Index relno,
					   List *restrictInfo,
					   List *targetList,
					   List *locations,
					   DLTblFmt formatType,
					   bool isWritable)
{
	if (isWritable)
		return NIL;

	if (FORMAT_IS_ICEBERG(formatType))
		return iceberg_get_external_fragments(relid, relno, restrictInfo, targetList, locations);
	else if (FORMAT_IS_HUDI(formatType))
		return hudi_get_external_fragments(relid, relno, restrictInfo, targetList, locations);
	else
		return hive_get_external_partitions(relid, locations);
}

void
commit_external_write(Oid relid, List *file_list, List *locations)
{
	iceberg_commit_external_write(relid, file_list, locations);
}

FDW_TableMeta*
get_external_schema_or_create(Oid relid, char *profile, List *locations)
{
	FDW_TableMeta *result;
	volatile datalake_gphadoop_context *context = NULL;

	PG_TRY();
	{
		char *catalogType = get_catalog_type(profile, locations);

		context = datalake_create_context(relid, strVal(linitial(locations)), transform_datalake_options);
		datalake_churl_headers_append(context->churl_headers, "X-GP-OPTIONS-PROFILE", profile);

		if (pg_strcasecmp(catalogType, "hive") == 0)
		{
			datalake_churl_headers_append(context->churl_headers, "X-GP-OPTIONS-CONFIG", "gphive.conf0gphdfs.conf");
		}
		else if (pg_strcasecmp(catalogType, "s3") == 0)
		{
			datalake_churl_headers_append(context->churl_headers, "X-GP-OPTIONS-CONFIG", "s3.conf");
		}
		else
		{
			datalake_churl_headers_append(context->churl_headers, "X-GP-OPTIONS-CONFIG", "gphdfs.conf");
		}

		datalake_churl_headers_append(context->churl_headers, "X-GP-OPTIONS-CATALOG-TYPE", catalogType);
		pfree(catalogType);

		datalake_churl_headers_append(context->churl_headers, "X-GP-OPTIONS-METHOD", "getOrCreateSchema");

		/*
		 * The iceberg getOrCreateSchema runs through the agent's iceberg
		 * catalog (e.g. IcebergHiveCatalog), which builds a GopherFileIO and
		 * therefore needs the gopher socket paths. Like the read/scan and
		 * commit paths, supply getIcebergConfigJsonString() in the request
		 * body; without it the agent has no gopher.worker_path and the write
		 * path fails at schema resolution with
		 * "Gopher Worker path is required".
		 */
		if (pg_strcasecmp(profile, "iceberg") == 0)
		{
			char *jsonConfig = getIcebergConfigJsonString(relid);
			if (jsonConfig != NULL && strlen(jsonConfig) > 0)
			{
				context->request_body = pstrdup(jsonConfig);
				context->request_body_len = strlen(jsonConfig);
			}
		}

		datalakeDoRPC((datalake_gphadoop_context *) context);
		result = parseSchemaResponse(context->buffer, context->buffer_pos);

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

List *
datalake_get_external_schema(char *profile, char *relName, char *schemaName, List *locations)
{
	List *result = NIL;
	volatile datalake_gphadoop_context *context = NULL;

	PG_TRY();
	{
		FDW_TableMeta *tableMeta;
		char *catalogType = get_catalog_type(profile, locations);

		context = datalake_create_context2(relName, schemaName, strVal(linitial(locations)), transform_datalake_options);
		datalake_churl_headers_append(context->churl_headers, "X-GP-OPTIONS-PROFILE", profile);

		if (pg_strcasecmp(catalogType, "hive") == 0)
		{
			datalake_churl_headers_append(context->churl_headers, "X-GP-OPTIONS-CONFIG", "gphive.conf0gphdfs.conf");
		}
		else if (pg_strcasecmp(catalogType, "s3") == 0)
		{
			datalake_churl_headers_append(context->churl_headers, "X-GP-OPTIONS-CONFIG", "s3.conf");
		}
		else
		{
			datalake_churl_headers_append(context->churl_headers, "X-GP-OPTIONS-CONFIG", "gphdfs.conf");
		}
		datalake_churl_headers_append(context->churl_headers, "X-GP-OPTIONS-CATALOG-TYPE", catalogType);
		pfree(catalogType);

		datalake_churl_headers_append(context->churl_headers, "X-GP-OPTIONS-METHOD", "getSchema");

		datalakeDoRPC((datalake_gphadoop_context *) context);
		tableMeta = parseSchemaResponse(context->buffer, context->buffer_pos);
		result = tableMeta->fields;
		tableMeta->fields = NIL;  /* prevent freeFDWTableMeta from releasing the list we keep */
		freeFDWTableMeta(tableMeta);

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


