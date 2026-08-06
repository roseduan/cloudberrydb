/*-------------------------------------------------------------------------
 *
 * pg_iceberg_rewrite_plan.c
 *    Implementation of data contracts for Iceberg VACUUM rewrite.
 *
 * This file contains JSON contract helpers for QE rewrite results,
 * including both parsing and payload construction.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/am_iceberg/pg_iceberg_rewrite_plan.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "common/jsonapi.h"
#include "mb/pg_wchar.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/json.h"
#include "utils/jsonfuncs.h"

#include "include/pg_iceberg_rewrite_plan.h"

/* Internal parse state for JSON parser */
typedef enum
{
	RW_HELPER_PARSE_NONE,
	RW_HELPER_PARSE_ADDED_FRAGMENTS,
	RW_HELPER_PARSE_REWRITTEN_FRAGMENTS
} RewriteHelperParseField;

typedef struct
{
	JsonLexContext *lex;
	int depth;
	RewriteHelperParseField current_field;
	const char *added_fragments_start;
	const char *added_fragments_end;
	int			added_fragments_depth;
	const char *rewritten_fragments_start;
	const char *rewritten_fragments_end;
	int			rewritten_fragments_depth;
} RewriteHelperParseState;

/* --- Parser callbacks --- */

static void
rw_helper_object_start(void *state)
{
	RewriteHelperParseState *s = (RewriteHelperParseState *) state;
	s->depth++;
}

static void
rw_helper_object_end(void *state)
{
	RewriteHelperParseState *s = (RewriteHelperParseState *) state;
	s->depth--;
}

static void
rw_helper_array_start(void *state)
{
	RewriteHelperParseState *s = (RewriteHelperParseState *) state;

	if (s->current_field == RW_HELPER_PARSE_ADDED_FRAGMENTS &&
		s->added_fragments_start == NULL)
	{
		s->added_fragments_start = s->lex->token_start;
		s->added_fragments_depth = s->depth;
	}
	else if (s->current_field == RW_HELPER_PARSE_REWRITTEN_FRAGMENTS &&
			 s->rewritten_fragments_start == NULL)
	{
		s->rewritten_fragments_start = s->lex->token_start;
		s->rewritten_fragments_depth = s->depth;
	}

	s->depth++;
}

static void
rw_helper_array_end(void *state)
{
	RewriteHelperParseState *s = (RewriteHelperParseState *) state;

	/*
	 * Decrement first, then capture only when we are back at the depth where
	 * the tracked array opened.  Fragment objects now contain a nested
	 * "partition_values" array; without this guard the extractor would fire on
	 * that inner ']' and truncate the captured fragments JSON.
	 */
	s->depth--;

	if (s->current_field == RW_HELPER_PARSE_ADDED_FRAGMENTS &&
		s->depth == s->added_fragments_depth)
	{
		s->added_fragments_end = s->lex->prev_token_terminator;
		s->current_field = RW_HELPER_PARSE_NONE;
	}
	else if (s->current_field == RW_HELPER_PARSE_REWRITTEN_FRAGMENTS &&
			 s->depth == s->rewritten_fragments_depth)
	{
		s->rewritten_fragments_end = s->lex->prev_token_terminator;
		s->current_field = RW_HELPER_PARSE_NONE;
	}
}

static void
rw_helper_object_field_start(void *state, char *fname, bool isnull)
{
	RewriteHelperParseState *s = (RewriteHelperParseState *) state;

	if (s->depth == 1)
	{
		if (pg_strcasecmp(fname, "fragments") == 0 ||
			pg_strcasecmp(fname, "addedFragments") == 0)
			s->current_field = RW_HELPER_PARSE_ADDED_FRAGMENTS;
		else if (pg_strcasecmp(fname, "rewrittenFragments") == 0)
			s->current_field = RW_HELPER_PARSE_REWRITTEN_FRAGMENTS;
		else
			s->current_field = RW_HELPER_PARSE_NONE;
	}
}

static void
rw_helper_scalar(void *state, char *token, JsonTokenType tokentype)
{
	/* No scalars needed for now */
}

static char *
rw_helper_extract_array_inner(const char *array_start,
							  const char *array_end)
{
	/*
	 * array_start points to '[' and array_end points to the character after ']'.
	 * Return the array inner payload without surrounding brackets.
	 */
	if (array_start != NULL &&
		array_end != NULL &&
		array_end > array_start + 1)
	{
		return pnstrdup(array_start + 1,
						array_end - array_start - 2);
	}

	return pstrdup("");
}

static const char *
rw_helper_file_format_name(FileFormat format)
{
	switch (format)
	{
		case ORC:
			return "orc";
		case PARQUET:
			return "parquet";
		case AVRO:
			return "avro";
		default:
			return "unknown";
	}
}

static const char *
rw_helper_position_on_delete(FileContent content)
{
	switch (content)
	{
		case DATA:
			return "DATA_FILE";
		case POSITION_DELETES:
			return "POSITION_DELETE";
		case EQUALITY_DELETES:
			return "EQUALITY_DELETE";
		case DELTA_LOG:
			return "DELTA_LOG";
		default:
			return "unknown";
	}
}

void
pg_iceberg_rewrite_append_fragment_json(StringInfo buf,
										FileFragment *fragment,
										int64 fallback_file_size)
{
	const char *path;
	const char *format_name;
	const char *position_on_delete;
	int64 file_size;

	if (fragment == NULL)
		return;

	path = fragment->filePath ? fragment->filePath : "";
	format_name = rw_helper_file_format_name(fragment->format);
	position_on_delete = rw_helper_position_on_delete(fragment->content);
	file_size = (fragment->fileSize > 0) ? fragment->fileSize : fallback_file_size;
	if (file_size < 0)
		file_size = 0;

	appendStringInfoChar(buf, '{');
	appendStringInfoString(buf, "\"path\":");
	escape_json(buf, path);
	appendStringInfoString(buf, ",\"format\":");
	escape_json(buf, format_name);
	appendStringInfo(buf,
					 ",\"record_count\":" INT64_FORMAT ","
					 "\"file_size_in_bytes\":" INT64_FORMAT ","
					 "\"position_on_delete\":",
					 fragment->recordCount,
					 file_size);
	escape_json(buf, position_on_delete);

	/*
	 * Partition tuple of the rewritten (old) data file, identity values in
	 * spec order.  A NULL list element serializes as JSON null; an empty/NIL
	 * list (unpartitioned table) yields "partition_values":[].  The agent's
	 * commitFileGroups requires this on both new and old files to attach the
	 * partition via withPartition() when the table is partitioned; without it
	 * transFileFromGpdb() throws for a partitioned spec.  Same JSON shape as
	 * the write-result emit in iceberg_volume_fdw.c.
	 */
	appendStringInfoString(buf, ",\"partition_values\":[");
	{
		ListCell   *pvlc;
		bool		pvfirst = true;

		foreach(pvlc, fragment->partitionValues)
		{
			Node	   *pv = (Node *) lfirst(pvlc);

			if (!pvfirst)
				appendStringInfoChar(buf, ',');
			pvfirst = false;

			if (pv == NULL)
				appendStringInfoString(buf, "null");
			else
				escape_json(buf, strVal(pv));
		}
	}
	appendStringInfoChar(buf, ']');

	appendStringInfoChar(buf, '}');
}

char *
pg_iceberg_rewrite_build_qe_result_json(const char *added_result_json,
										const char *rewritten_fragments_json)
{
	char *added_fragments_json = NULL;
	char *result_json;
	bool has_added = false;
	bool has_rewritten = false;
	const char *rewritten_json =
		(rewritten_fragments_json != NULL) ? rewritten_fragments_json : "";

	if (added_result_json != NULL && added_result_json[0] != '\0')
		pg_iceberg_extract_rewrite_fragments_json(added_result_json,
												  &added_fragments_json);
	else
		added_fragments_json = pstrdup("");

	has_added = (added_fragments_json != NULL && added_fragments_json[0] != '\0');
	has_rewritten = (rewritten_json[0] != '\0');

	if (!has_added && !has_rewritten)
	{
		if (added_fragments_json != NULL)
			pfree(added_fragments_json);
		return NULL;
	}

	result_json = psprintf("{\"fragments\":[%s],\"rewrittenFragments\":[%s]}",
						   added_fragments_json ? added_fragments_json : "",
						   rewritten_json);

	if (added_fragments_json != NULL)
		pfree(added_fragments_json);

	return result_json;
}

/*
 * pg_iceberg_extract_rewrite_fragments_json
 *    Extract the fragments array from a rewrite result JSON.
 *    Expects JSON shape: {"fragments":[...]}.
 */
void
pg_iceberg_extract_rewrite_fragments_json(const char *json,
										  char **fragments_json)
{
	pg_iceberg_extract_rewrite_result_arrays(json, fragments_json, NULL);
}

void
pg_iceberg_extract_rewrite_result_arrays(const char *json,
										 char **added_fragments_json,
										 char **rewritten_fragments_json)
{
	JsonLexContext *lex;
	JsonSemAction sem;
	RewriteHelperParseState ps;

	memset(&ps, 0, sizeof(ps));

	if (json == NULL)
		return;

	lex = makeJsonLexContextCstringLen((char *) json,
								   strlen(json),
								   GetDatabaseEncoding(),
								   true);
	ps.lex = lex;

	memset(&sem, 0, sizeof(sem));
	sem.semstate = &ps;
	sem.object_start = rw_helper_object_start;
	sem.object_end = rw_helper_object_end;
	sem.array_start = rw_helper_array_start;
	sem.array_end = rw_helper_array_end;
	sem.object_field_start = rw_helper_object_field_start;
	sem.scalar = rw_helper_scalar;

	pg_parse_json_or_ereport(lex, &sem);

	if (added_fragments_json != NULL)
		*added_fragments_json = rw_helper_extract_array_inner(
			ps.added_fragments_start,
			ps.added_fragments_end);

	if (rewritten_fragments_json != NULL)
		*rewritten_fragments_json = rw_helper_extract_array_inner(
			ps.rewritten_fragments_start,
			ps.rewritten_fragments_end);

	pfree(lex);
}
