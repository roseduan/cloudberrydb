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
#include "include/pg_iceberg_catalog_utils.h"

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
} MetadataParseState;

static void
metadata_object_field_start(void *state, char *fname, bool isnull)
{
	MetadataParseState *s = (MetadataParseState *) state;

	if (pg_strcasecmp(fname, "metadata-location") == 0)
		s->state = PARSE_METADATA_LOCATION;
}

static void
metadata_scalar(void *state, char *token, JsonTokenType tokentype)
{
	MetadataParseState *s = (MetadataParseState *) state;

	if (s->state == PARSE_METADATA_LOCATION && tokentype == JSON_TOKEN_STRING)
		s->metadata_location = pstrdup(token);
}

/*
 * parse_metadata_location
 *		Extract 'metadata-location' from a JSON response string.
 */
char *
parse_metadata_location(char *json_response)
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
	sem.scalar = metadata_scalar;

	pg_parse_json_or_ereport(lex, &sem);

	result = parse_state.metadata_location;
	pfree(lex);

	if (result == NULL)
		elog(ERROR, "Missing 'metadata-location' field in response");

	return result;
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
		list_free(schema->partitionColumns);
		pfree(schema);
	}
}
