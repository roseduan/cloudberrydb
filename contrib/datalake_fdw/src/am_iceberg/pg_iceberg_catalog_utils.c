/*-------------------------------------------------------------------------
 *
 * pg_iceberg_catalog_utils.c
 *    Utility functions for Iceberg catalog operations, including JSON parsing,
 *    schema mapping, and error handling.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/am_iceberg/pg_iceberg_catalog_utils.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_type.h"
#include "common/jsonapi.h"
#include "lib/stringinfo.h"
#include "mb/pg_wchar.h"
#include "utils/builtins.h"
#include "utils/int8.h"
#include "utils/json.h"
#include "utils/jsonfuncs.h"

#include "../datalake_def.h"
#include "src/common/iceberg_constants.h"
#include "include/pg_iceberg_catalog_utils.h"
#include "include/pg_iceberg_options.h"

/* JSON Parsing State */
typedef enum
{
	PARSE_START,
	PARSE_METADATA_LOCATION
} JsonParseState;

typedef struct
{
	JsonLexContext *lex;
	JsonParseState		state;
	char		   *metadata_location;
	/*
	 * issue #399: the append/update response also carries a
	 * "written-metadata-files" array listing the metadata-layer files
	 * (metadata.json + manifest-list + manifests) this agent call newly wrote.
	 * The transaction tracker collects them to clean up on ROLLBACK / drop the
	 * superseded ones on COMMIT.  in_written_files brackets the array.
	 */
	bool			in_written_files;
	List		   *written_files;
} MetadataParseState;

static void
metadata_object_field_start(void *state, char *fname, bool isnull)
{
	MetadataParseState *s = (MetadataParseState *) state;

	if (pg_strcasecmp(fname, "metadata-location") == 0)
	{
		s->state = PARSE_METADATA_LOCATION;
		s->in_written_files = false;
	}
	else if (pg_strcasecmp(fname, "written-metadata-files") == 0)
	{
		s->state = PARSE_START;
		s->in_written_files = true;
	}
	else
	{
		s->state = PARSE_START;
		s->in_written_files = false;
	}
}

static void
metadata_array_end(void *state)
{
	MetadataParseState *s = (MetadataParseState *) state;

	/* Leaving the "written-metadata-files" array (or any array). */
	s->in_written_files = false;
}

static void
metadata_scalar(void *state, char *token, JsonTokenType tokentype)
{
	MetadataParseState *s = (MetadataParseState *) state;

	if (s->state == PARSE_METADATA_LOCATION && tokentype == JSON_TOKEN_STRING)
	{
		s->metadata_location = pstrdup(token);
		s->state = PARSE_START;
	}
	else if (s->in_written_files && tokentype == JSON_TOKEN_STRING &&
			 token != NULL && token[0] != '\0')
	{
		s->written_files = lappend(s->written_files, pstrdup(token));
	}
}

/*
 * parse_metadata_location_ex
 *		Extract 'metadata-location' from a JSON response string and, when
 *		written_files_out is non-NULL, also collect the
 *		'written-metadata-files' array (List of palloc'd cstrings; NIL when
 *		the field is absent).  See issue #399.
 */
char *
parse_metadata_location_ex(char *json_response, List **written_files_out)
{
	JsonLexContext		   *lex;
	JsonSemAction			sem;
	MetadataParseState		parse_state;
	char				   *result;

	/* Initialize parse state */
	memset(&parse_state, 0, sizeof(parse_state));
	parse_state.state = PARSE_START;

	lex = makeJsonLexContextCstringLen(json_response,
									   strlen(json_response),
									   GetDatabaseEncoding(),
									   true);
	parse_state.lex = lex;

	memset(&sem, 0, sizeof(sem));
	sem.semstate = &parse_state;
	sem.object_field_start = metadata_object_field_start;
	sem.array_end = metadata_array_end;
	sem.scalar = metadata_scalar;

	pg_parse_json_or_ereport(lex, &sem);

	result = parse_state.metadata_location;
	pfree(lex);

	if (result == NULL)
		elog(ERROR, "Missing 'metadata-location' field in response");

	if (written_files_out != NULL)
		*written_files_out = parse_state.written_files;

	return result;
}

/*
 * parse_metadata_location
 *		Extract 'metadata-location' from a JSON response string.
 */
char *
parse_metadata_location(char *json_response)
{
	return parse_metadata_location_ex(json_response, NULL);
}

/*
 * PathCollectState - collect every "path" string from a fragments JSON array.
 */
typedef struct
{
	JsonLexContext *lex;
	bool			in_path;
	List		   *paths;
} PathCollectState;

static void
pathcollect_object_field_start(void *state, char *fname, bool isnull)
{
	PathCollectState *s = (PathCollectState *) state;

	s->in_path = (pg_strcasecmp(fname, "path") == 0);
}

static void
pathcollect_scalar(void *state, char *token, JsonTokenType tokentype)
{
	PathCollectState *s = (PathCollectState *) state;

	if (s->in_path && tokentype == JSON_TOKEN_STRING &&
		token != NULL && token[0] != '\0')
		s->paths = lappend(s->paths, pstrdup(token));
	s->in_path = false;
}

/*
 * pg_iceberg_collect_fragment_paths
 *		Collect every "path" string value from a fragments JSON array
 *		(e.g. VACUUM's rewrittenFragments).  Returns a List of palloc'd cstrings.
 */
List *
pg_iceberg_collect_fragment_paths(const char *json)
{
	JsonLexContext	   *lex;
	JsonSemAction		sem;
	PathCollectState	st;

	if (json == NULL || json[0] == '\0')
		return NIL;

	memset(&st, 0, sizeof(st));
	lex = makeJsonLexContextCstringLen((char *) json,
									   strlen(json),
									   GetDatabaseEncoding(),
									   true);
	st.lex = lex;

	memset(&sem, 0, sizeof(sem));
	sem.semstate = &st;
	sem.object_field_start = pathcollect_object_field_start;
	sem.scalar = pathcollect_scalar;

	pg_parse_json_or_ereport(lex, &sem);
	pfree(lex);

	return st.paths;
}

/*
 * LoadTableParseState - state for parsing load-table response
 *
 * Tracks JSON nesting depth to only capture top-level fields:
 *   - "metadata-location": extracted as a string
 *   - "table-location": extracted as a string
 *   - "config": captured as raw JSON substring
 *   - "storage-credentials": captured as raw JSON substring
 */
typedef enum
{
	LT_PARSE_NONE,
	LT_PARSE_METADATA_LOCATION,
	LT_PARSE_TABLE_LOCATION,
	LT_PARSE_PARTITION_SPEC_SUMMARY,
	LT_PARSE_CONFIG,
	LT_PARSE_STORAGE_CREDENTIALS
} LoadTableParseField;

typedef struct
{
	JsonLexContext	   *lex;
	int					depth;				/* current nesting depth (0 = top level) */
	LoadTableParseField	current_field;		/* which top-level field we are inside */
	int					capture_depth;		/* depth at which capture started */

	char			   *metadata_location;
	char			   *location;
	char			   *partition_spec_summary;

	/* raw JSON capture for config / storage-credentials */
	const char		   *config_start;		/* pointer into input */
	const char		   *config_end;
	const char		   *credentials_start;
	const char		   *credentials_end;
} LoadTableParseState;

static void
lt_object_start(void *state)
{
	LoadTableParseState *s = (LoadTableParseState *) state;
	s->depth++;
}

static void
lt_object_end(void *state)
{
	LoadTableParseState *s = (LoadTableParseState *) state;
	s->depth--;

	if (s->current_field == LT_PARSE_CONFIG && s->depth == s->capture_depth)
	{
		/* end of top-level "config" object — record end position */
		s->config_end = s->lex->prev_token_terminator;
		s->current_field = LT_PARSE_NONE;
	}
}

static void
lt_array_start(void *state)
{
	LoadTableParseState *s = (LoadTableParseState *) state;
	s->depth++;
}

static void
lt_array_end(void *state)
{
	LoadTableParseState *s = (LoadTableParseState *) state;
	s->depth--;

	if (s->current_field == LT_PARSE_STORAGE_CREDENTIALS && s->depth == s->capture_depth)
	{
		s->credentials_end = s->lex->prev_token_terminator;
		s->current_field = LT_PARSE_NONE;
	}
}

static void
lt_object_field_start(void *state, char *fname, bool isnull)
{
	LoadTableParseState *s = (LoadTableParseState *) state;

	/* Only care about top-level fields (depth == 1 means we are inside the root object) */
	if (s->depth != 1)
		return;

	if (pg_strcasecmp(fname, "metadata-location") == 0)
		s->current_field = LT_PARSE_METADATA_LOCATION;
	else if (pg_strcasecmp(fname, "table-location") == 0)
		s->current_field = LT_PARSE_TABLE_LOCATION;
	else if (pg_strcasecmp(fname, DATALAKEFDW_ICEBERG_KEY_PARTITION_SPEC_SUMMARY) == 0)
		s->current_field = LT_PARSE_PARTITION_SPEC_SUMMARY;
	else if (pg_strcasecmp(fname, "config") == 0)
	{
		s->current_field = LT_PARSE_CONFIG;
		s->capture_depth = s->depth;
		s->config_start = s->lex->token_terminator;
	}
	else if (pg_strcasecmp(fname, "storage-credentials") == 0)
	{
		s->current_field = LT_PARSE_STORAGE_CREDENTIALS;
		s->capture_depth = s->depth;
		s->credentials_start = s->lex->token_terminator;
	}
}

static void
lt_scalar(void *state, char *token, JsonTokenType tokentype)
{
	LoadTableParseState *s = (LoadTableParseState *) state;

	if (s->current_field == LT_PARSE_METADATA_LOCATION &&
		s->depth == 1 &&
		tokentype == JSON_TOKEN_STRING)
	{
		s->metadata_location = pstrdup(token);
		s->current_field = LT_PARSE_NONE;
	}
	else if (s->current_field == LT_PARSE_TABLE_LOCATION &&
			 s->depth == 1 &&
			 tokentype == JSON_TOKEN_STRING)
	{
		s->location = pstrdup(token);
		s->current_field = LT_PARSE_NONE;
	}
	else if (s->current_field == LT_PARSE_PARTITION_SPEC_SUMMARY &&
			 s->depth == 1 &&
			 tokentype == JSON_TOKEN_STRING)
	{
		s->partition_spec_summary = pstrdup(token);
		s->current_field = LT_PARSE_NONE;
	}
}

/*
 * parse_load_table_response
 *		Parse the full load-table JSON response from the catalog agent.
 *
 * Extracts:
 *   - metadata-location  → result->metadata_location
 *   - table-location     → result->location
 *   - config + storage-credentials + table-location → result->catalog_properties
 *
 * Returns a palloc'd IcebergLoadTableResult.
 */
IcebergLoadTableResult *
parse_load_table_response(char *json_response)
{
	JsonLexContext		   *lex;
	JsonSemAction			sem;
	LoadTableParseState		ps;
	IcebergLoadTableResult *result;
	StringInfoData			buf;

	memset(&ps, 0, sizeof(ps));

	lex = makeJsonLexContextCstringLen(json_response,
									   strlen(json_response),
									   GetDatabaseEncoding(),
									   true);
	ps.lex = lex;

	memset(&sem, 0, sizeof(sem));
	sem.semstate = &ps;
	sem.object_start = lt_object_start;
	sem.object_end = lt_object_end;
	sem.array_start = lt_array_start;
	sem.array_end = lt_array_end;
	sem.object_field_start = lt_object_field_start;
	sem.scalar = lt_scalar;

	pg_parse_json_or_ereport(lex, &sem);

	if (ps.metadata_location == NULL)
		elog(ERROR, "Missing 'metadata-location' field in load-table response");

	/* Build the catalog_properties JSON containing config, storage-credentials, and table-location */
	initStringInfo(&buf);
	appendStringInfoChar(&buf, '{');

	if (ps.config_start && ps.config_end)
	{
		appendStringInfo(&buf, "\"config\":%.*s",
						 (int)(ps.config_end - ps.config_start),
						 ps.config_start);
	}

	if (ps.credentials_start && ps.credentials_end)
	{
		if (ps.config_start && ps.config_end)
			appendStringInfoChar(&buf, ',');

		appendStringInfo(&buf, "\"storage-credentials\":%.*s",
						 (int)(ps.credentials_end - ps.credentials_start),
						 ps.credentials_start);
	}

	if (ps.location != NULL)
	{
		if ((ps.config_start && ps.config_end) ||
			(ps.credentials_start && ps.credentials_end))
			appendStringInfoChar(&buf, ',');

		appendStringInfoString(&buf, "\"table-location\":");
		escape_json(&buf, ps.location);
	}

	appendStringInfoChar(&buf, '}');

	result = (IcebergLoadTableResult *) palloc0(sizeof(IcebergLoadTableResult));
	result->metadata_location = ps.metadata_location;
	result->catalog_properties = buf.data;
	result->location = ps.location;
	result->partition_spec_summary = ps.partition_spec_summary;

	pfree(lex);

	return result;
}

typedef enum
{
	STATS_PARSE_NONE,
	STATS_PARSE_RECORD_COUNT,
	STATS_PARSE_TOTAL_BYTES
} StatsParseField;

typedef struct
{
	StatsParseField current_field;
	int64			record_count;
	int64			total_bytes;
	bool			has_record_count;
	bool			has_total_bytes;
} StatisticsParseState;

static void
statistics_object_field_start(void *state, char *fname, bool isnull)
{
	StatisticsParseState *s = (StatisticsParseState *) state;

	(void) isnull;

	if (pg_strcasecmp(fname, "total-records") == 0)
		s->current_field = STATS_PARSE_RECORD_COUNT;
	else if (pg_strcasecmp(fname, "total-files-size") == 0)
		s->current_field = STATS_PARSE_TOTAL_BYTES;
	else
		s->current_field = STATS_PARSE_NONE;
}

static void
statistics_scalar(void *state, char *token, JsonTokenType tokentype)
{
	StatisticsParseState *s = (StatisticsParseState *) state;
	int64 value;

	if (token == NULL)
		return;

	if (tokentype != JSON_TOKEN_STRING && tokentype != JSON_TOKEN_NUMBER)
		return;

	if (!scanint8(token, true, &value))
		elog(ERROR, "invalid int64 value in statistics response: %s", token);

	if (s->current_field == STATS_PARSE_RECORD_COUNT)
	{
		s->record_count = value;
		s->has_record_count = true;
	}
	else if (s->current_field == STATS_PARSE_TOTAL_BYTES)
	{
		s->total_bytes = value;
		s->has_total_bytes = true;
	}

	s->current_field = STATS_PARSE_NONE;
}

IcebergTableStatistics *
parse_statistics_response(char *json_response)
{
	JsonLexContext *lex;
	JsonSemAction sem;
	StatisticsParseState parse_state;
	IcebergTableStatistics *statistics;

	memset(&parse_state, 0, sizeof(parse_state));

	lex = makeJsonLexContextCstringLen(json_response,
									   strlen(json_response),
									   GetDatabaseEncoding(),
									   true);

	memset(&sem, 0, sizeof(sem));
	sem.semstate = &parse_state;
	sem.object_field_start = statistics_object_field_start;
	sem.scalar = statistics_scalar;

	pg_parse_json_or_ereport(lex, &sem);

	if (!parse_state.has_record_count || !parse_state.has_total_bytes)
		elog(ERROR,
			 "Missing statistics fields in response, response_body=%s",
			 json_response);

	statistics = (IcebergTableStatistics *) palloc0(sizeof(IcebergTableStatistics));
	statistics->recordCount = parse_state.record_count;
	statistics->bytesInDataFile = parse_state.total_bytes;

	pfree(lex);

	return statistics;
}

/*
 * extract_json_stack_trace
 *		Extract the "stack" field from JSON error response and convert
 *		JSON-escaped newlines (\n, \t) into real newlines/tabs so the
 *		Java stack trace prints readably in the PG log.
 *		Returns palloc'd string, or NULL if no stack found.
 */
static char *
extract_json_stack_trace(const char *body)
{
	const char *stack_key = "\"stack\":\"";
	const char *p;
	const char *start;
	const char *end;
	StringInfoData buf;

	if (!body)
		return NULL;

	p = strstr(body, stack_key);
	if (!p)
		return NULL;

	start = p + strlen(stack_key);

	/* Find the closing quote — skip escaped quotes */
	end = start;
	while (*end)
	{
		if (*end == '\\' && *(end + 1))
		{
			end += 2;	/* skip escaped char */
			continue;
		}
		if (*end == '"')
			break;
		end++;
	}

	if (end <= start)
		return NULL;

	/* Convert JSON escape sequences to real characters */
	initStringInfo(&buf);
	for (p = start; p < end; )
	{
		if (*p == '\\' && p + 1 < end)
		{
			switch (*(p + 1))
			{
				case 'n':
					appendStringInfoChar(&buf, '\n');
					p += 2;
					break;
				case 't':
					appendStringInfoChar(&buf, '\t');
					p += 2;
					break;
				case '\\':
					appendStringInfoChar(&buf, '\\');
					p += 2;
					break;
				case '"':
					appendStringInfoChar(&buf, '"');
					p += 2;
					break;
				default:
					appendStringInfoChar(&buf, *p);
					p++;
					break;
			}
		}
		else
		{
			appendStringInfoChar(&buf, *p);
			p++;
		}
	}

	return buf.data;
}

/*
 * check_fdw_execution_error
 *		Verify if an FDW catalog operation succeeded and report errors if not.
 */
void
check_fdw_execution_error(IcebergCatalogFdwState *fdwState, const char *error_prefix)
{
	StringInfoData errorBuf;

	if (fdwState->lastStatus != ICEBERG_SUCCESS)
	{
		char *stack_trace = NULL;

		initStringInfo(&errorBuf);

		appendStringInfo(&errorBuf,
						 "%s: HTTP Status: %d, CURL Code: %ld",
						 error_prefix,
						 fdwState->response.httpStatus,
						 fdwState->response.curlCode);

		if (fdwState->response.errorMessage && fdwState->response.errorMessage[0] != '\0')
			appendStringInfo(&errorBuf, ", Error: %s", fdwState->response.errorMessage);

		/* Try to extract and pretty-print Java stack trace from JSON response */
		if (fdwState->response.responseBody)
			stack_trace = extract_json_stack_trace(fdwState->response.responseBody);

		if (stack_trace)
		{
			appendStringInfo(&errorBuf, "\n%s", stack_trace);
			pfree(stack_trace);
		}
		else if (fdwState->response.responseBody)
		{
			/* Fallback: raw body (not JSON or no stack field) */
			appendStringInfo(&errorBuf, ", Response: %s", fdwState->response.responseBody);
		}

		elog(ERROR, "%s", errorBuf.data);
	}

	if (fdwState->response.responseBody == NULL || fdwState->response.responseBody[0] == '\0')
		elog(ERROR, "%s: No response body", error_prefix);
}

/*
 * is_table_not_found_error
 *		Check whether the FDW response indicates a "table not found" condition.
 *		Returns true if HTTP status is 404 or the response body contains
 *		"NoSuchTableException".
 */
bool
is_table_not_found_error(IcebergCatalogFdwState *fdwState)
{
	if (fdwState->response.httpStatus == 404)
		return true;

	if (fdwState->response.responseBody != NULL &&
		strstr(fdwState->response.responseBody, "NoSuchTableException") != NULL)
		return true;

	return false;
}

static IcebergColumnDef *
make_column_def(const char *name,
				Oid dataType,
				int32 typemod,
				bool nullable)
{
	IcebergColumnDef *colDef;

	colDef = (IcebergColumnDef *) palloc0(sizeof(IcebergColumnDef));

	colDef->columnName = pstrdup(name);
	colDef->dataType = dataType;
	colDef->typeModifier = typemod;
	colDef->isNullable = nullable;

	return colDef;
}

/*
 * build_schema_from_pg_table
 *		Create an IcebergTableSchema structure based on a PostgreSQL Relation.
 */
IcebergTableSchema *
build_schema_from_pg_table(Relation relation)
{
	TupleDesc			tupdesc;
	IcebergTableSchema *schema;
	int					i;

	tupdesc = RelationGetDescr(relation);

	schema = (IcebergTableSchema *) palloc0(sizeof(IcebergTableSchema));
	for (i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute	attr = TupleDescAttr(tupdesc, i);
		IcebergColumnDef   *colDef;

		if (attr->attisdropped)
			continue;

		colDef = make_column_def(
			NameStr(attr->attname),		/* column name */
			attr->atttypid,				/* data type OID */
			attr->atttypmod,			/* type modifier */
			!attr->attnotnull			/* nullable (inverse of NOT NULL) */
		);

		schema->columns = lappend(schema->columns, colDef);
	}

	/*
	 * PARTITION BY declaration, stored by CreateLakeTable in
	 * pg_lake_table.ltoptions as "iceberg_partition_by=col1,col2".  The
	 * create-table request translates these into identity partition fields
	 * of the Iceberg partition spec.
	 */
	{
		IcebergTableOptions *opts;
		Oid			catalog_oid;
		Oid			volume_oid;

		opts = get_iceberg_options(RelationGetRelid(relation),
								   &catalog_oid, &volume_oid);
		if (opts != NULL && opts->partition_by != NULL)
		{
			char	   *cols = pstrdup(opts->partition_by);
			char	   *tok;
			char	   *saveptr = NULL;

			for (tok = strtok_r(cols, ",", &saveptr);
				 tok != NULL;
				 tok = strtok_r(NULL, ",", &saveptr))
				schema->partitionColumns = lappend(schema->partitionColumns,
												   pstrdup(tok));
			pfree(cols);
		}
		if (opts != NULL)
			free_iceberg_table_options(opts);
	}

	return schema;
}

/*
 * free_schema_info
 *		Free memory allocated for an IcebergTableSchema.
 */
void
free_schema_info(IcebergTableSchema *schema)
{
	ListCell *lc;

	if (schema)
	{
		/* Free column definitions */
		foreach(lc, schema->columns)
		{
			IcebergColumnDef *colDef = (IcebergColumnDef *) lfirst(lc);
			if (colDef->columnName)
				pfree(colDef->columnName);
			if (colDef->comment)
				pfree(colDef->comment);
			pfree(colDef);
		}

		list_free(schema->columns);
		/* partitionColumns holds pstrdup'd name strings; free them too. */
		list_free_deep(schema->partitionColumns);
		pfree(schema);
	}
}


/* ------------------------------------------------------------------------
 * Time travel: parse the agent getSnapshotSchema response
 *   {"snapshotSchemaId":int,"currentSchemaId":int,
 *    "columns":[{"name":str,"type":str,"fieldId":int,"required":bool},...]}
 * with the project's native SAX JSON parser (jansson's function-like macros
 * clash with PostgreSQL's built-in JSON function prototypes).  Root object is
 * depth 1; the columns array is depth 2; each column object is depth 3.
 * ------------------------------------------------------------------------
 */
typedef enum
{
	SSS_NONE,
	SSS_SNAP_ID,
	SSS_CUR_ID,
	SSS_COL_NAME,
	SSS_COL_TYPE,
	SSS_COL_FIELD_ID
} SnapSchemaField;

typedef struct SnapSchemaParseState
{
	JsonLexContext *lex;
	int			depth;
	SnapSchemaField cur;
	int			snapshot_schema_id;
	int			current_schema_id;
	char	   *col_name;
	char	   *col_type;
	int			col_field_id;
	List	   *names;
	List	   *types;
	List	   *field_ids;
	/*
	 * True only inside the top-level "columns" array.  Column objects are
	 * detected as depth-3 objects, but ANY future nested object under a root
	 * field (e.g. "partitionSpec":{...}) also parses at depth 3; without this
	 * gate it would be appended as a phantom empty column.
	 */
	bool		in_columns;
} SnapSchemaParseState;

static void
sss_object_start(void *state)
{
	SnapSchemaParseState *s = (SnapSchemaParseState *) state;

	s->depth++;
	if (s->in_columns && s->depth == 3) /* entering a column object */
	{
		s->col_name = NULL;
		s->col_type = NULL;
		s->col_field_id = 0;
	}
}

static void
sss_object_end(void *state)
{
	SnapSchemaParseState *s = (SnapSchemaParseState *) state;

	if (s->in_columns && s->depth == 3) /* leaving a column object */
	{
		/*
		 * Fail closed on a column without a name or type: an empty name
		 * would silently enter the tuple descriptor (an empty type at least
		 * errors later, in iceberg_type_to_pg).
		 */
		if (s->col_name == NULL || s->col_type == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("iceberg time travel: malformed getSnapshotSchema response"),
					 errdetail("A column entry lacks the \"name\" or \"type\" key.")));
		s->names = lappend(s->names, s->col_name);
		s->types = lappend(s->types, s->col_type);
		s->field_ids = lappend_int(s->field_ids, s->col_field_id);
	}
	s->depth--;
	s->cur = SSS_NONE;
}

static void
sss_array_start(void *state)
{
	((SnapSchemaParseState *) state)->depth++;
}

static void
sss_array_end(void *state)
{
	SnapSchemaParseState *s = (SnapSchemaParseState *) state;

	s->depth--;
	if (s->depth == 1)
		s->in_columns = false;
}

static void
sss_object_field_start(void *state, char *fname, bool isnull)
{
	SnapSchemaParseState *s = (SnapSchemaParseState *) state;

	if (s->depth == 1)
	{
		if (pg_strcasecmp(fname, "snapshotSchemaId") == 0)
			s->cur = SSS_SNAP_ID;
		else if (pg_strcasecmp(fname, "currentSchemaId") == 0)
			s->cur = SSS_CUR_ID;
		else
			s->cur = SSS_NONE;
		s->in_columns = (pg_strcasecmp(fname, "columns") == 0);
	}
	else if (s->depth == 3)
	{
		if (pg_strcasecmp(fname, "name") == 0)
			s->cur = SSS_COL_NAME;
		else if (pg_strcasecmp(fname, "type") == 0)
			s->cur = SSS_COL_TYPE;
		else if (pg_strcasecmp(fname, "fieldId") == 0)
			s->cur = SSS_COL_FIELD_ID;
		else
			s->cur = SSS_NONE;
	}
}

static void
sss_scalar(void *state, char *token, JsonTokenType tokentype)
{
	SnapSchemaParseState *s = (SnapSchemaParseState *) state;

	if (s->depth == 1 && s->cur == SSS_SNAP_ID && tokentype == JSON_TOKEN_NUMBER)
		s->snapshot_schema_id = atoi(token);
	else if (s->depth == 1 && s->cur == SSS_CUR_ID && tokentype == JSON_TOKEN_NUMBER)
		s->current_schema_id = atoi(token);
	else if (s->depth == 3 && s->cur == SSS_COL_NAME &&
			 tokentype == JSON_TOKEN_STRING)
		s->col_name = pstrdup(token);
	else if (s->depth == 3 && s->cur == SSS_COL_TYPE &&
			 tokentype == JSON_TOKEN_STRING)
		s->col_type = pstrdup(token);
	else if (s->depth == 3 && s->cur == SSS_COL_FIELD_ID &&
			 tokentype == JSON_TOKEN_NUMBER)
		s->col_field_id = atoi(token);
	s->cur = SSS_NONE;
}

void
pg_iceberg_parse_snapshot_schema_response(char *json_response,
										  IcebergSnapshotSchema *out)
{
	JsonLexContext		   *lex;
	JsonSemAction			sem;
	SnapSchemaParseState	ps;
	ListCell			   *lc;
	int						i;

	memset(&ps, 0, sizeof(ps));
	lex = makeJsonLexContextCstringLen(json_response, strlen(json_response),
									   GetDatabaseEncoding(), true);
	ps.lex = lex;

	memset(&sem, 0, sizeof(sem));
	sem.semstate = &ps;
	sem.object_start = sss_object_start;
	sem.object_end = sss_object_end;
	sem.array_start = sss_array_start;
	sem.array_end = sss_array_end;
	sem.object_field_start = sss_object_field_start;
	sem.scalar = sss_scalar;

	pg_parse_json_or_ereport(lex, &sem);

	out->snapshot_schema_id = ps.snapshot_schema_id;
	out->current_schema_id = ps.current_schema_id;
	out->ncols = list_length(ps.names);
	out->colnames = (char **) palloc(sizeof(char *) * Max(out->ncols, 1));
	out->coltypes = (char **) palloc(sizeof(char *) * Max(out->ncols, 1));
	out->fieldids = (int *) palloc0(sizeof(int) * Max(out->ncols, 1));

	i = 0;
	foreach(lc, ps.names)
		out->colnames[i++] = (char *) lfirst(lc);
	i = 0;
	foreach(lc, ps.types)
		out->coltypes[i++] = (char *) lfirst(lc);
	i = 0;
	foreach(lc, ps.field_ids)
		out->fieldids[i++] = lfirst_int(lc);
}


/* ------------------------------------------------------------------------
 * Time travel: parse the agent getSnapshots response
 *   {"currentSnapshotId":long,
 *    "snapshots":[{"snapshotId":long,"timestampMs":long,"operation":str,
 *                  "schemaId":int,"parentId":long,"summaryJson":str},...]}
 * Same native SAX approach (and the same jansson caveat) as the snapshot
 * schema parser above.  Root object is depth 1; the snapshots array is depth
 * 2; each snapshot object is depth 3.  The agent pre-serializes the snapshot
 * summary into the summaryJson STRING so no nested object ever appears here.
 * ------------------------------------------------------------------------
 */
typedef enum
{
	SL_NONE,
	SL_CURRENT_ID,
	SL_SNAP_ID,
	SL_TIMESTAMP,
	SL_OPERATION,
	SL_SCHEMA_ID,
	SL_PARENT_ID,
	SL_SUMMARY
} SnapListField;

typedef struct SnapListParseState
{
	JsonLexContext *lex;
	int				depth;
	SnapListField	cur;
	int64			current_snapshot_id;
	IcebergSnapshotEntry entry;		/* the snapshot object being parsed */
	List		   *entries;		/* List of IcebergSnapshotEntry * */
} SnapListParseState;

static void
sl_object_start(void *state)
{
	SnapListParseState *s = (SnapListParseState *) state;

	s->depth++;
	if (s->depth == 3)			/* entering a snapshot object */
	{
		memset(&s->entry, 0, sizeof(s->entry));
		s->entry.schema_id = -1;
	}
}

static void
sl_object_end(void *state)
{
	SnapListParseState *s = (SnapListParseState *) state;

	if (s->depth == 3)			/* leaving a snapshot object */
	{
		IcebergSnapshotEntry *e = (IcebergSnapshotEntry *) palloc(sizeof(*e));

		*e = s->entry;
		/*
		 * A snapshot entry without a positive id or a commit time cannot be
		 * consumed safely: the id defaults to 0, which every consumer treats
		 * as HEAD, so an AS OF resolution matching such an entry would
		 * silently read current data.  Fail closed on the whole response.
		 */
		if (e->snapshot_id <= 0 || e->timestamp_ms <= 0)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("iceberg time travel: malformed getSnapshots response"),
					 errdetail("A snapshot entry lacks a valid snapshotId or timestampMs.")));
		if (e->operation == NULL)
			e->operation = pstrdup("");
		if (e->summary_json == NULL)
			e->summary_json = pstrdup("{}");
		s->entries = lappend(s->entries, e);
	}
	s->depth--;
	s->cur = SL_NONE;
}

static void
sl_array_start(void *state)
{
	((SnapListParseState *) state)->depth++;
}

static void
sl_array_end(void *state)
{
	((SnapListParseState *) state)->depth--;
}

static void
sl_object_field_start(void *state, char *fname, bool isnull)
{
	SnapListParseState *s = (SnapListParseState *) state;

	if (s->depth == 1)
	{
		if (pg_strcasecmp(fname, "currentSnapshotId") == 0)
			s->cur = SL_CURRENT_ID;
		else
			s->cur = SL_NONE;
	}
	else if (s->depth == 3)
	{
		if (pg_strcasecmp(fname, "snapshotId") == 0)
			s->cur = SL_SNAP_ID;
		else if (pg_strcasecmp(fname, "timestampMs") == 0)
			s->cur = SL_TIMESTAMP;
		else if (pg_strcasecmp(fname, "operation") == 0)
			s->cur = SL_OPERATION;
		else if (pg_strcasecmp(fname, "schemaId") == 0)
			s->cur = SL_SCHEMA_ID;
		else if (pg_strcasecmp(fname, "parentId") == 0)
			s->cur = SL_PARENT_ID;
		else if (pg_strcasecmp(fname, "summaryJson") == 0)
			s->cur = SL_SUMMARY;
		else
			s->cur = SL_NONE;
	}
	else
	{
		/*
		 * Any other depth is a shape we do not expect.  Clear the pending
		 * field so a stray nested object cannot make its scalars land in the
		 * slot selected by the enclosing snapshot object's last field name.
		 */
		s->cur = SL_NONE;
	}
}

/* Parse an int64 JSON scalar, erroring out rather than silently yielding 0. */
static int64
sl_int64(const char *token)
{
	int64		value;

	if (!scanint8(token, true, &value))
		elog(ERROR, "invalid int64 value in getSnapshots response: %s", token);
	return value;
}

static void
sl_scalar(void *state, char *token, JsonTokenType tokentype)
{
	SnapListParseState *s = (SnapListParseState *) state;
	SnapListField		field = s->cur;

	/* One field per scalar: clear the selector before any early return. */
	s->cur = SL_NONE;

	if (token == NULL)
		return;

	if (s->depth == 1)
	{
		if (field == SL_CURRENT_ID && tokentype == JSON_TOKEN_NUMBER)
			s->current_snapshot_id = sl_int64(token);
		return;
	}

	if (s->depth != 3)
		return;

	switch (field)
	{
		case SL_SNAP_ID:
			if (tokentype == JSON_TOKEN_NUMBER)
				s->entry.snapshot_id = sl_int64(token);
			break;
		case SL_TIMESTAMP:
			if (tokentype == JSON_TOKEN_NUMBER)
				s->entry.timestamp_ms = sl_int64(token);
			break;
		case SL_PARENT_ID:
			if (tokentype == JSON_TOKEN_NUMBER)
				s->entry.parent_id = sl_int64(token);
			break;
		case SL_SCHEMA_ID:
			if (tokentype == JSON_TOKEN_NUMBER)
				s->entry.schema_id = atoi(token);
			break;
		case SL_OPERATION:
			if (tokentype == JSON_TOKEN_STRING)
				s->entry.operation = pstrdup(token);
			break;
		case SL_SUMMARY:
			if (tokentype == JSON_TOKEN_STRING)
				s->entry.summary_json = pstrdup(token);
			break;
		default:
			break;
	}
}

void
pg_iceberg_parse_snapshots_response(char *json_response,
									IcebergSnapshotList *out)
{
	JsonLexContext	   *lex;
	JsonSemAction		sem;
	SnapListParseState	ps;
	ListCell		   *lc;
	int					i;

	memset(&ps, 0, sizeof(ps));
	lex = makeJsonLexContextCstringLen(json_response, strlen(json_response),
									   GetDatabaseEncoding(), true);
	ps.lex = lex;

	memset(&sem, 0, sizeof(sem));
	sem.semstate = &ps;
	sem.object_start = sl_object_start;
	sem.object_end = sl_object_end;
	sem.array_start = sl_array_start;
	sem.array_end = sl_array_end;
	sem.object_field_start = sl_object_field_start;
	sem.scalar = sl_scalar;

	pg_parse_json_or_ereport(lex, &sem);

	out->current_snapshot_id = ps.current_snapshot_id;
	out->nsnapshots = list_length(ps.entries);
	out->snapshots = (IcebergSnapshotEntry *)
		palloc(sizeof(IcebergSnapshotEntry) * Max(out->nsnapshots, 1));

	i = 0;
	foreach(lc, ps.entries)
		out->snapshots[i++] = *(IcebergSnapshotEntry *) lfirst(lc);
}


/* ------------------------------------------------------------------------
 * Snapshot summary map accessor
 *
 * IcebergSnapshotEntry.summary_json holds Snapshot.summary() as a flat JSON
 * object.  Iceberg types the map as Map<String,String>, so every value arrives
 * quoted ("total-records":"200000"), but engines other than the reference
 * implementation have been seen to emit bare numbers; accept both rather than
 * silently reporting "no statistics" for such a table.
 * ------------------------------------------------------------------------
 */
typedef struct SummaryParseState
{
	const char *key;			/* the field being looked for */
	int			depth;
	bool		want;			/* the next scalar is that field's value */
	bool		found;
	int64		value;
} SummaryParseState;

static void
sm_object_start(void *state)
{
	((SummaryParseState *) state)->depth++;
}

static void
sm_object_end(void *state)
{
	((SummaryParseState *) state)->depth--;
}

static void
sm_array_start(void *state)
{
	((SummaryParseState *) state)->depth++;
}

static void
sm_array_end(void *state)
{
	((SummaryParseState *) state)->depth--;
}

static void
sm_object_field_start(void *state, char *fname, bool isnull)
{
	SummaryParseState *s = (SummaryParseState *) state;

	/*
	 * Only the top-level map is of interest.  Clearing `want` at every other
	 * depth keeps a scalar nested inside an unexpected sub-object from landing
	 * in the slot selected by the enclosing object's last field name.
	 */
	s->want = (s->depth == 1 && strcmp(fname, s->key) == 0);
}

static void
sm_scalar(void *state, char *token, JsonTokenType tokentype)
{
	SummaryParseState *s = (SummaryParseState *) state;
	bool		want = s->want;

	/* One field per scalar: clear the selector before any early return. */
	s->want = false;

	if (!want || s->found || token == NULL || s->depth != 1)
		return;
	if (tokentype != JSON_TOKEN_STRING && tokentype != JSON_TOKEN_NUMBER)
		return;
	/* A non-integral value is treated as absent, never as zero. */
	if (scanint8(token, true, &s->value))
		s->found = true;
}

/*
 * Look up an integer-valued key in a snapshot summary map.  Returns false --
 * leaving *out untouched -- when the summary is absent, is not a JSON object,
 * does not carry the key, or carries a value that is not an integer.
 *
 * Deliberately non-throwing: the summary is agent-supplied text consumed on the
 * planning path, where a malformed map must degrade to "no statistics" rather
 * than fail the user's query.
 */
bool
pg_iceberg_summary_int64(const char *summary_json, const char *key, int64 *out)
{
	JsonLexContext	   *lex;
	JsonSemAction		sem;
	SummaryParseState	ps;

	if (summary_json == NULL || key == NULL || *summary_json == '\0')
		return false;

	memset(&ps, 0, sizeof(ps));
	ps.key = key;

	lex = makeJsonLexContextCstringLen((char *) summary_json,
									   strlen(summary_json),
									   GetDatabaseEncoding(), true);

	memset(&sem, 0, sizeof(sem));
	sem.semstate = &ps;
	sem.object_start = sm_object_start;
	sem.object_end = sm_object_end;
	sem.array_start = sm_array_start;
	sem.array_end = sm_array_end;
	sem.object_field_start = sm_object_field_start;
	sem.scalar = sm_scalar;

	if (pg_parse_json(lex, &sem) != JSON_SUCCESS)
		return false;

	if (ps.found)
		*out = ps.value;
	return ps.found;
}
