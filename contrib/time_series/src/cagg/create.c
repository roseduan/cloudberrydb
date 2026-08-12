/*-------------------------------------------------------------------------
 *
 * create.c
 *    Continuous Aggregate CREATE handling via ProcessUtility hook.
 *
 *    Intercepts CREATE MATERIALIZED VIEW ... WITH (time_series.continuous)
 *    and orchestrates: query validation, materialization table creation,
 *    three-view setup, trigger installation, and catalog registration.
 *
 * Copyright (c) 2026 HashData Inc.
 *
 * IDENTIFICATION
 *    contrib/time_series/src/cagg/create.c
 *
 *-------------------------------------------------------------------------
 */
#include "../include/time_series.h"
#include "../include/access/ts_tableam.h"
#include "../include/cagg/cagg.h"

#include "utils/guc.h"

#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/extension.h"
#include "miscadmin.h"			/* GetUserId */
#include "utils/acl.h"			/* pg_class_ownercheck, aclcheck_error */
#include "executor/spi.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"
#include "tcop/utility.h"
#include "datatype/timestamp.h"	/* TIMESTAMP_NOT_FINITE */
#include "pgtime.h"				/* pg_tzset for timezone validation */
#include "utils/builtins.h"
#include "utils/date.h"			/* DATE_NOT_FINITE */
#include "utils/fmgroids.h"
#include "utils/inval.h"		/* CacheInvalidateRelcacheByRelid */
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "access/table.h"

#include "cdb/cdbvars.h"

#ifdef FAULT_INJECTOR
#include "utils/faultinjector.h"
#endif

/* Previous ProcessUtility hook */
static ProcessUtility_hook_type prev_ProcessUtility = NULL;

/*
 * Information gathered during CAGG creation.
 */
typedef struct CaggCreateInfo
{
	/* Source table */
	const char *source_schema;
	const char *source_table;
	Oid			source_relid;

	/* User view (what the user typed) */
	const char *user_view_schema;
	const char *user_view_name;

	/* time_bucket info */
	Interval   *bucket_width;
	const char *bucket_column;
	Datum		bucket_origin;		/* optional: origin parameter */
	bool		has_origin;
	Interval   *bucket_offset;		/* optional: offset parameter */
	const char *bucket_timezone;	/* optional: timezone parameter */

	/* Generated names */
	int			cagg_id;
	char		mat_table_name[NAMEDATALEN];
	char		partial_view_name[NAMEDATALEN];
	char		direct_view_name[NAMEDATALEN];

	/* Distribution keys from source table (List of cstring), NIL if random */
	List	   *dist_keys;

	/* Time column type OID */
	Oid			time_type;

	/* The original user query (deparsed for view creation) */
	const char *original_query;

	/* Parsed query */
	Query	   *query;

	/* materialized_only option */
	bool		materialized_only;

	/* WITH NO DATA */
	bool		skip_data;
} CaggCreateInfo;

/* Forward declarations */
static bool cagg_check_continuous_option(List *options,
										 bool *materialized_only);
static void cagg_create(CreateTableAsStmt *stmt, const char *queryString);
static void cagg_validate_query(Query *query, CaggCreateInfo *info);
static bool is_time_bucket_funcexpr(FuncExpr *func);
static void cagg_extract_source_info(Query *query, CaggCreateInfo *info);
static List *cagg_get_dist_keys(Oid relid);
static void cagg_create_mat_table(CaggCreateInfo *info);
static void cagg_create_views(CaggCreateInfo *info);
static void cagg_install_trigger(CaggCreateInfo *info);
static int	cagg_register_catalog(CaggCreateInfo *info);
static bool cagg_apply_materialized_only(const char *cagg_name,
										 bool mat_only);

/*
 * Apply materialized_only toggle: rebuild the user view (UNION ALL
 * in real-time mode, plain passthrough in mat-only mode) and update
 * the catalog.
 *
 * Called from the ALTER VIEW hook.  Must be called within an active
 * SPI context (SPI_connect already done).
 *
 * Returns true if the view was found and (potentially) rebuilt;
 * false if cagg_name does not reference a CAGG user view.
 */

static bool
cagg_apply_materialized_only(const char *cagg_name, bool mat_only)
{
	int			ret;
	bool		isnull;
	int			cagg_id;
	char	   *user_view_schema;
	char	   *user_view_name;
	char	   *mat_table_schema;
	char	   *mat_table_name;
	char	   *direct_view_schema;
	char	   *direct_view_name;
	bool		current_mo;
	char	   *bucket_alias;
	StringInfoData sql;
	MemoryContext caller_cxt = CurrentMemoryContext;
	MemoryContext oldctx;

	/* Step 1: Look up CAGG metadata (schema-qualified) */
	{
		Oid			argtypes[2] = { TEXTOID, TEXTOID };
		Datum		args[2];
		HeapTuple	tup;
		TupleDesc	desc;
		const char *dot;
		char		schema_buf[NAMEDATALEN];
		char		name_buf_local[NAMEDATALEN];

		/* Parse "schema.name" or just "name" (default schema = "public") */
		dot = strchr(cagg_name, '.');
		if (dot)
		{
			int slen = dot - cagg_name;
			if (slen >= NAMEDATALEN) slen = NAMEDATALEN - 1;
			memcpy(schema_buf, cagg_name, slen);
			schema_buf[slen] = '\0';
			strlcpy(name_buf_local, dot + 1, NAMEDATALEN);
		}
		else
		{
			strlcpy(schema_buf, "public", NAMEDATALEN);
			strlcpy(name_buf_local, cagg_name, NAMEDATALEN);
		}

		args[0] = CStringGetTextDatum(schema_buf);
		args[1] = CStringGetTextDatum(name_buf_local);
		ret = SPI_execute_with_args(
			"SELECT cagg_id, user_view_schema, user_view_name, "
			"       mat_table_schema, mat_table_name, "
			"       direct_view_schema, direct_view_name, "
			"       materialized_only "
			"FROM time_series.continuous_agg "
			"WHERE user_view_schema = $1 AND user_view_name = $2",
			2, argtypes, args, NULL, true, 1);

		if (ret != SPI_OK_SELECT || SPI_processed == 0)
			return false;		/* not a CAGG user view */

		tup = SPI_tuptable->vals[0];
		desc = SPI_tuptable->tupdesc;

		cagg_id = DatumGetInt32(SPI_getbinval(tup, desc, 1, &isnull));
		current_mo = DatumGetBool(SPI_getbinval(tup, desc, 8, &isnull));

		/* Copy strings into caller's context (survive SPI_finish/nested SPI) */
		oldctx = MemoryContextSwitchTo(caller_cxt);
		user_view_schema = pstrdup(SPI_getvalue(tup, desc, 2));
		user_view_name = pstrdup(SPI_getvalue(tup, desc, 3));
		mat_table_schema = pstrdup(SPI_getvalue(tup, desc, 4));
		mat_table_name = pstrdup(SPI_getvalue(tup, desc, 5));
		direct_view_schema = pstrdup(SPI_getvalue(tup, desc, 6));
		direct_view_name = pstrdup(SPI_getvalue(tup, desc, 7));
		MemoryContextSwitchTo(oldctx);
	}

	/* Step 2: Short-circuit if already in desired mode */
	if (current_mo == mat_only)
		return true;

	/* Step 3: Look up bucket alias (mat table's first column name) */
	{
		Oid			argtypes[2] = { NAMEOID, NAMEOID };
		Datum		args[2];

		args[0] = DirectFunctionCall1(namein,
									   CStringGetDatum(mat_table_schema));
		args[1] = DirectFunctionCall1(namein,
									   CStringGetDatum(mat_table_name));

		ret = SPI_execute_with_args(
			"SELECT a.attname FROM pg_attribute a "
			"JOIN pg_class pc ON pc.oid = a.attrelid "
			"JOIN pg_namespace n ON n.oid = pc.relnamespace "
			"WHERE n.nspname = $1 AND pc.relname = $2 "
			"AND a.attnum = 1 AND NOT a.attisdropped",
			2, argtypes, args, NULL, true, 1);

		if (ret != SPI_OK_SELECT || SPI_processed == 0)
			ereport(ERROR,
					(errmsg("cagg \"%s\": mat table first column not found",
							cagg_name)));

		oldctx = MemoryContextSwitchTo(caller_cxt);
		bucket_alias = pstrdup(SPI_getvalue(SPI_tuptable->vals[0],
											SPI_tuptable->tupdesc, 1));
		MemoryContextSwitchTo(oldctx);
	}

	/* Step 4: Build CREATE OR REPLACE VIEW SQL */
	oldctx = MemoryContextSwitchTo(caller_cxt);
	initStringInfo(&sql);
	if (mat_only)
	{
		appendStringInfo(&sql,
			"CREATE OR REPLACE VIEW %s.%s AS SELECT * FROM %s.%s",
			quote_identifier(user_view_schema),
			quote_identifier(user_view_name),
			quote_identifier(mat_table_schema),
			quote_identifier(mat_table_name));
	}
	else
	{
		appendStringInfo(&sql,
			"CREATE OR REPLACE VIEW %s.%s AS "
			"SELECT * FROM %s.%s "
			" WHERE %s < time_series.cagg_watermark(%d) "
			"UNION ALL "
			"SELECT * FROM %s.%s "
			" WHERE %s >= time_series.cagg_watermark(%d)",
			quote_identifier(user_view_schema),
			quote_identifier(user_view_name),
			quote_identifier(mat_table_schema),
			quote_identifier(mat_table_name),
			quote_identifier(bucket_alias), cagg_id,
			quote_identifier(direct_view_schema),
			quote_identifier(direct_view_name),
			quote_identifier(bucket_alias), cagg_id);
	}
	MemoryContextSwitchTo(oldctx);

	/* Step 5: Execute the CREATE OR REPLACE VIEW */
	SPI_execute(sql.data, false, 0);

	/* Step 6: Update catalog */
	{
		Oid			argtypes[2] = { BOOLOID, INT4OID };
		Datum		args[2];

		args[0] = BoolGetDatum(mat_only);
		args[1] = Int32GetDatum(cagg_id);
		SPI_execute_with_args(
			"UPDATE time_series.continuous_agg SET materialized_only = $1 "
			"WHERE cagg_id = $2",
			2, argtypes, args, NULL, false, 0);
	}

	pfree(sql.data);
	return true;
}

/*
 * ProcessUtility Hook
 */

static void
cagg_process_utility(PlannedStmt *pstmt,
						const char *queryString,
						bool readOnlyTree,
						ProcessUtilityContext context,
						ParamListInfo params,
						QueryEnvironment *queryEnv,
						DestReceiver *dest,
						QueryCompletion *qc)
{
	Node *parsetree = pstmt->utilityStmt;
	bool altering_time_series = false;

	/*
	 * Pre-execution capture for ALTER ... SET SCHEMA on a CAGG user view
	 * or source table.  We must read the OLD namespace BEFORE
	 * standard_ProcessUtility runs, because the SET SCHEMA changes
	 * pg_class.relnamespace and post-execution lookups return the new
	 * namespace -- giving us no way to locate the matching catalog row.
	 * Filled below and consumed in the post-execution block at the
	 * bottom of this function.
	 */
	Oid			alterobjsch_relid = InvalidOid;
	char	   *alterobjsch_old_schema = NULL;
	ObjectType	alterobjsch_objtype = OBJECT_TABLE;	/* default unused */

	/*
	 * Early bail-out when the time_series extension is not (or no
	 * longer) installed, or while the extension is being altered.
	 *
	 * The shared library stays resident across DROP EXTENSION (PG
	 * never unloads preloaded libs), so the ProcessUtility_hook
	 * pointer still points at us -- but the various SPI queries we
	 * issue below reference extension-owned tables
	 *   time_series.continuous_agg
	 *   time_series.bgw_job
	 *   time_series.cagg_invalidation_log
	 * which were dropped along with the extension.  Without this
	 * guard the next DDL after DROP EXTENSION CASCADE -- even a
	 * plain TRUNCATE on an unrelated table -- fails with
	 *   ERROR: relation "time_series.continuous_agg" does not exist
	 *
	 * Modelled after upstream process_utility.c: the actual
	 * check is delegated to extension_is_loaded_and_not_upgrading()
	 * which is backed by a process-local state machine + relcache
	 * invalidation callback (see time_series.c).
	 *
	 * We additionally bypass the hook for ALTER EXTENSION
	 * time_series ... because upgrade scripts run inside the hook
	 * scope and may reference catalog tables that don't yet exist
	 * in the new shape.
	 */
	if (IsA(parsetree, AlterExtensionStmt))
	{
		AlterExtensionStmt *stmt = (AlterExtensionStmt *) parsetree;

		altering_time_series =
			(strcmp(stmt->extname, TS_EXTENSION_SCHEMA_NAME) == 0);
	}

	if (altering_time_series || !extension_is_loaded_and_not_upgrading())
	{
		if (prev_ProcessUtility)
			prev_ProcessUtility(pstmt, queryString, readOnlyTree,
								context, params, queryEnv, dest, qc);
		else
			standard_ProcessUtility(pstmt, queryString, readOnlyTree,
									context, params, queryEnv, dest, qc);
		return;
	}

	/*
	 * Intercept CREATE MATERIALIZED VIEW ... WITH (time_series.continuous).
	 */
	if (IsA(parsetree, CreateTableAsStmt))
	{
		CreateTableAsStmt *stmt = (CreateTableAsStmt *) parsetree;

		if (stmt->objtype == OBJECT_MATVIEW)
		{
			bool materialized_only = false;

			if (cagg_check_continuous_option(stmt->into->options,
											 &materialized_only))
			{
				/*
				 * Master toggle for the CAGG DDL handler.  Pre-existing
				 * CAGGs continue to refresh / serve queries even when
				 * this is off -- only new CREATE is rejected.
				 */
				if (!guc_enable_cagg_create)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("creation of continuous aggregates "
									"is disabled"),
							 errhint("Set time_series.enable_cagg_create = on "
									 "to enable.")));

				/*
				 * Only execute CAGG logic on the coordinator (QD).
				 * On segments the DDL is dispatched by the coordinator.
				 */
				if (Gp_role != GP_ROLE_EXECUTE)
				{
					/*
					 * Extract ONLY this statement from queryString.
					 * In multi-statement batches (psql -c "s1; s2; s3"),
					 * queryString contains ALL statements.  Using it
					 * directly would include unrelated SQL (e.g. INSERT)
					 * in the partial view definition -> infinite recursion.
					 *
					 * pstmt->stmt_location/stmt_len give this statement's
					 * boundaries within queryString.
					 */
					const char *stmt_sql;
					int stmt_loc = pstmt->stmt_location;
					int stmt_len = pstmt->stmt_len;

					if (stmt_loc >= 0)
					{
						if (stmt_len > 0)
							stmt_sql = pnstrdup(queryString + stmt_loc, stmt_len);
						else
							stmt_sql = pstrdup(queryString + stmt_loc);
					}
					else
					{
						stmt_sql = queryString;
					}

					cagg_create(stmt, stmt_sql);
					return;
				}
			}
		}
	}

	/*
	 * Intercept TRUNCATE on CAGG source tables.
	 *
	 * CBDB supports neither STATEMENT triggers nor event triggers for
	 * TRUNCATE, so the ROW-level invalidation trigger cannot detect it.
	 * We catch TRUNCATE here in the ProcessUtility hook (runs on QD
	 * before the actual TRUNCATE) and write a full-range L1 entry
	 * {-infinity, +infinity} via SPI.  Then pass through to the standard
	 * handler which does the actual TRUNCATE.  Both happen in the same
	 * transaction -- if TRUNCATE fails, the L1 write also rolls back.
	 */
	if (IsA(parsetree, TruncateStmt) && Gp_role != GP_ROLE_EXECUTE)
	{
		TruncateStmt *stmt = (TruncateStmt *) parsetree;
		ListCell   *lc;

		foreach(lc, stmt->relations)
		{
			RangeVar   *rv = lfirst_node(RangeVar, lc);
			Oid			relid = RangeVarGetRelid(rv, NoLock, true);

			if (OidIsValid(relid))
			{
				Oid		argtypes[1] = { OIDOID };
				Datum	args[1];
				int		ret;

				args[0] = ObjectIdGetDatum(relid);

				SPI_connect();
				ret = SPI_execute_with_args(
					"SELECT 1 FROM time_series.continuous_agg "
					"WHERE source_table_oid = $1 LIMIT 1",
					1, argtypes, args, NULL, true, 1);

				if (ret == SPI_OK_SELECT && SPI_processed > 0)
				{
					/* This table has a CAGG -- write full-range L1 */
					SPI_execute_with_args(
						"INSERT INTO time_series.cagg_invalidation_log "
						"(source_table_oid, lowest_modified, greatest_modified) "
						"VALUES ($1, '-infinity'::timestamptz, "
						"'infinity'::timestamptz)",
						1, argtypes, args, NULL, false, 0);

					/*
					 * Reset watermark and threshold to -infinity.
					 *
					 * Without this, the real-time view's mat branch
					 * serves stale data (watermark still advanced past
					 * the now-empty range) while the live branch returns
					 * nothing (source is empty).  Resetting ensures the
					 * next query goes through the live branch entirely.
					 */
					SPI_execute_with_args(
						"UPDATE time_series.cagg_watermark "
						"SET watermark = '-infinity'::timestamptz "
						"WHERE cagg_id IN ("
						"  SELECT cagg_id FROM time_series.continuous_agg "
						"  WHERE source_table_oid = $1)",
						1, argtypes, args, NULL, false, 0);

					SPI_execute_with_args(
						"UPDATE time_series.cagg_invalidation_threshold "
						"SET threshold = '-infinity'::timestamptz "
						"WHERE source_table_oid = $1",
						1, argtypes, args, NULL, false, 0);

					/*
					 * This is the ONE code path that moves the watermark
					 * BACKWARD (refresh only advances it with GREATEST),
					 * so the backend-local watermark cache in
					 * watermark_constify.c must be told: broadcast a
					 * relcache inval on the cagg_watermark table (the
					 * relid its callback listens on).  Transactional --
					 * delivered only if this TRUNCATE commits.  Without
					 * it, backends planning from a stale cache would keep
					 * the old (high) watermark and their mat branch would
					 * keep serving the stale aggregates that this reset
					 * just made invisible.
					 *
					 * Prepared plans need no extra handling here: core
					 * TRUNCATE assigns the source a new relfilenode and
					 * sends its own relcache inval, and every cv plan
					 * references the source table in its live branch.
					 */
					{
						Oid		wm_oid = get_relname_relid(
							"cagg_watermark",
							ht_get_namespace_oid_cached());

						if (OidIsValid(wm_oid))
							CacheInvalidateRelcacheByRelid(wm_oid);
					}
				}
				SPI_finish();
			}
		}
		/* Fall through to execute the actual TRUNCATE below */
	}

	/*
	 * Block TRUNCATE on CAGG materialization tables.
	 *
	 * Matches upstream behavior: "cannot TRUNCATE a hypertable
	 * underlying a continuous aggregate".  Truncating the mat table
	 * without resetting watermark causes permanent data loss that
	 * no REFRESH can recover.
	 */
	if (IsA(parsetree, TruncateStmt) && Gp_role != GP_ROLE_EXECUTE)
	{
		TruncateStmt *stmt = (TruncateStmt *) parsetree;
		ListCell   *lc;

		foreach(lc, stmt->relations)
		{
			RangeVar   *rv = lfirst_node(RangeVar, lc);
			Oid			relid = RangeVarGetRelid(rv, NoLock, true);

			if (OidIsValid(relid))
			{
				Datum	args[1];
				int		ret;
				char   *relname = get_rel_name(relid);

				/* Check if relname matches _mat_*_N pattern in time_series schema */
				if (relname && strncmp(relname, "_mat_", 5) == 0)
				{
					MemoryContext caller_cxt = CurrentMemoryContext;
					char   *cagg_name = NULL;

					args[0] = CStringGetTextDatum(relname);

					SPI_connect();
					ret = SPI_execute_with_args(
						"SELECT user_view_name FROM time_series.continuous_agg "
						"WHERE mat_table_name = $1 LIMIT 1",
						1, (Oid[]){ TEXTOID }, args, NULL, true, 1);

					if (ret == SPI_OK_SELECT && SPI_processed > 0)
					{
						char *raw = SPI_getvalue(
							SPI_tuptable->vals[0],
							SPI_tuptable->tupdesc, 1);
						/* Copy to caller context before SPI_finish releases SPI context */
						MemoryContextSwitchTo(caller_cxt);
						cagg_name = raw ? pstrdup(raw) : pstrdup("unknown");
						MemoryContextSwitchTo(SPI_tuptable->tuptabcxt);
					}
					SPI_finish();

					if (cagg_name != NULL)
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("cannot TRUNCATE a materialization table "
										"underlying a continuous aggregate"),
								 errhint("TRUNCATE the source table instead, "
										 "or drop and re-create the continuous "
										 "aggregate \"%s\".", cagg_name)));
				}
			}
		}
	}

	/*
	 * Block DROP COLUMN / ALTER COLUMN TYPE on the bucket column of a
	 * CAGG source table.
	 *
	 * For non-bucket columns, PG's native pg_depend machinery already
	 * blocks the change if any of our internal views reference it
	 * (without CASCADE) and tells the user to drop the view first.
	 * For columns that aren't referenced by the views (e.g. a sibling
	 * column never aggregated by the CAGG), the operation is safe and
	 * we let it through.
	 *
	 * The bucket column is special: it's stored by NAME in
	 * time_series.continuous_agg.bucket_column and used by
	 * cagg_refresh.c when generating SQL.  Renaming, dropping, or
	 * retyping it would silently break every subsequent REFRESH.
	 *
	 * Mirrors upstream process_altertable_drop_column guard,
	 * which similarly limits the block to partitioning columns
	 * (the time column and space-partition columns).
	 */
	if (IsA(parsetree, AlterTableStmt) && Gp_role != GP_ROLE_EXECUTE)
	{
		AlterTableStmt *stmt = (AlterTableStmt *) parsetree;

		if (stmt->objtype == OBJECT_TABLE && stmt->relation != NULL)
		{
			Oid			src_oid = RangeVarGetRelid(stmt->relation,
												   NoLock, true);

			if (OidIsValid(src_oid))
			{
				ListCell   *cell;
				char	   *bucket_col_to_check = NULL;
				int			subtype_to_check = -1;

				foreach(cell, stmt->cmds)
				{
					AlterTableCmd *cmd = lfirst_node(AlterTableCmd, cell);

					if ((cmd->subtype == AT_DropColumn ||
						 cmd->subtype == AT_AlterColumnType) &&
						cmd->name != NULL)
					{
						bucket_col_to_check = cmd->name;
						subtype_to_check = cmd->subtype;
						break;
					}
				}

				if (bucket_col_to_check != NULL)
				{
					Oid			argtypes[2] = { OIDOID, NAMEOID };
					Datum		args[2];
					int			ret;
					MemoryContext caller_cxt = CurrentMemoryContext;
					char	   *src_schema = get_namespace_name(
											get_rel_namespace(src_oid));
					char	   *src_name = get_rel_name(src_oid);
					char	   *vschema = NULL;
					char	   *vname = NULL;
					bool		blocks = false;
					NameData	col_name;

					namestrcpy(&col_name, bucket_col_to_check);
					args[0] = ObjectIdGetDatum(src_oid);
					args[1] = NameGetDatum(&col_name);

					SPI_connect();
					ret = SPI_execute_with_args(
						"SELECT user_view_schema, user_view_name "
						"FROM time_series.continuous_agg "
						"WHERE source_table_oid = $1 "
						"  AND bucket_column = $2 LIMIT 1",
						2, argtypes, args, NULL, true, 1);

					if (ret == SPI_OK_SELECT && SPI_processed > 0)
					{
						bool	isnull;
						Datum	d_schema = SPI_getbinval(SPI_tuptable->vals[0],
											SPI_tuptable->tupdesc, 1, &isnull);
						char   *s_raw = isnull ? "?" :
							NameStr(*DatumGetName(d_schema));
						Datum	d_name = SPI_getbinval(SPI_tuptable->vals[0],
											SPI_tuptable->tupdesc, 2, &isnull);
						char   *n_raw = isnull ? "?" :
							NameStr(*DatumGetName(d_name));
						MemoryContext old = MemoryContextSwitchTo(caller_cxt);

						vschema = pstrdup(s_raw);
						vname   = pstrdup(n_raw);
						blocks = true;
						MemoryContextSwitchTo(old);
					}
					SPI_finish();

					if (blocks)
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("cannot %s column \"%s\" of "
										"\"%s.%s\" -- it is the bucket "
										"column for continuous aggregate "
										"\"%s.%s\"",
										subtype_to_check == AT_DropColumn
											? "drop" : "change type of",
										bucket_col_to_check,
										src_schema, src_name,
										vschema, vname),
								 errhint("Drop the continuous aggregate "
										 "first (DROP VIEW %s.%s CASCADE), "
										 "then re-create it against the "
										 "new schema.",
										 quote_identifier(vschema),
										 quote_identifier(vname))));
				}
			}
		}
	}

	/*
	 * Intercept ALTER VIEW cv SET (time_series.materialized_only = bool).
	 *
	 * PG parses namespaced options `ns.name = val` into DefElem with
	 * defnamespace set.  At execute time, PG's transformRelOptions rejects
	 * unknown options on regular views.  We intercept here (before execute)
	 * and:
	 *   1. Locate our option in the DefElem list for AT_SetRelOptions.
	 *   2. Call set_materialized_only(name, bool) via SPI.
	 *   3. Strip our option from the list so PG's validator doesn't choke.
	 *   4. If no other options remain, drop the AT_SetRelOptions cmd.
	 *   5. If no commands remain, skip the standard handler entirely.
	 */
	if (IsA(parsetree, AlterTableStmt) && Gp_role != GP_ROLE_EXECUTE)
	{
		AlterTableStmt *stmt = (AlterTableStmt *) parsetree;

		if (stmt->objtype == OBJECT_VIEW && stmt->relation != NULL)
		{
			Oid			view_oid = RangeVarGetRelid(stmt->relation,
													NoLock, true);

			if (OidIsValid(view_oid))
			{
				ListCell   *cmd_cell;
				List	   *new_cmds = NIL;
				bool		handled = false;
				char	   *view_name = stmt->relation->relname;

				foreach(cmd_cell, stmt->cmds)
				{
					AlterTableCmd *cmd = lfirst_node(AlterTableCmd, cmd_cell);

					if (cmd->subtype == AT_SetRelOptions &&
						cmd->def != NULL && IsA(cmd->def, List))
					{
						List	   *options = (List *) cmd->def;
						List	   *remaining = NIL;
						ListCell   *opt_cell;

						foreach(opt_cell, options)
						{
							DefElem *de = lfirst_node(DefElem, opt_cell);
							bool	is_ours = (de->defnamespace != NULL &&
									strcmp(de->defnamespace, TS_EXTENSION_SCHEMA_NAME) == 0 &&
									strcmp(de->defname, "materialized_only") == 0);
							bool	is_cagg = false;
							bool	mo_value = false;

							if (is_ours)
							{
								mo_value = defGetBoolean(de);

								/*
								 * Owner check: the standard ALTER VIEW
								 * handler is bypassed (we strip the
								 * option below), so its built-in ACL
								 * check never fires.  Mirror PG's
								 * behavior by gating on
								 * pg_class_ownercheck of the user
								 * view here.  Without this, any
								 * role with USAGE on the schema
								 * could flip another user's CAGG
								 * mode.
								 */
								if (!pg_class_ownercheck(view_oid,
														 GetUserId()))
									aclcheck_error(ACLCHECK_NOT_OWNER,
												   OBJECT_VIEW,
												   view_name);

								SPI_connect();
								/* cagg_apply_materialized_only does the full
								 * lookup-and-rebuild; returns true if the view
								 * is a CAGG user view. */
								is_cagg = cagg_apply_materialized_only(
									view_name, mo_value);
								SPI_finish();
							}

							/* List manipulation outside SPI context to avoid
							 * allocating remaining list nodes in SPI's temp
							 * memory context, which gets freed on SPI_finish. */
							if (is_ours && is_cagg)
								handled = true;
							else
								remaining = lappend(remaining, de);
						}

						if (remaining != NIL)
						{
							cmd->def = (Node *) remaining;
							new_cmds = lappend(new_cmds, cmd);
						}
						/* else: drop this cmd entirely */
					}
					else
					{
						new_cmds = lappend(new_cmds, cmd);
					}
				}

				if (handled)
				{
					stmt->cmds = new_cmds;
					/*
					 * If nothing left to do, skip the standard handler.
					 *
					 * Trade-off: this also skips prev_ProcessUtility.
					 * An audit/logging hook chained earlier will not see
					 * this ALTER VIEW (the materialized_only=...) path --
					 * we've already executed its semantic effect via
					 * cagg_apply_materialized_only's view rebuild.  We
					 * accept this gap because:
					 *   1. running standard_ProcessUtility on a stmt
					 *      whose cmds list is NIL trips PG with
					 *      "ALTER TABLE has no commands";
					 *   2. invoking prev_ProcessUtility with a NIL cmds
					 *      list to "let audit see it" leaks the
					 *      half-stripped statement, and most audit
					 *      hooks expect the cmds list to be the actual
					 *      commands they should record;
					 *   3. the materialized_only flag flip is a
					 *      time_series-internal concept; auditing
					 *      requirements for it are tracked via
					 *      continuous_agg.materialized_only catalog
					 *      changes which most audit setups already
					 *      observe via INSERT/UPDATE on that table.
					 * If finer audit coverage is needed in future,
					 * emit our own audit row before returning.
					 */
					if (new_cmds == NIL)
						return;
				}
			}
		}
	}

	/*
	 * PRE-execution defense: block RENAME COLUMN on CAGG internal objects.
	 *
	 * A CAGG creates four objects: the user view (cv), and three internal
	 * objects in the time_series schema -- _mat_<name>_<N> (the materialization
	 * table), _partial_view_<N> (partial-aggregate over source), and
	 * _direct_view_<N> (final aggregate over mat table).  Users should never
	 * touch the latter three directly, but nothing in PG stops them.
	 *
	 * If a user does ALTER TABLE _mat_cv_1 RENAME COLUMN bucket TO mbucket,
	 * the rename succeeds (PG view dependents follow attnum, so SELECT cv
	 * keeps working), but cagg_refresh's hard-coded SQL "INSERT INTO mat_table
	 * SELECT * FROM partial_view WHERE bucket >= ..." still references the
	 * old column name and fails with an opaque "column does not exist" error
	 * hours/days later.  The causal link to the rename is essentially
	 * impossible to recover from logs alone.
	 *
	 * Mirror upstream process_utility.c handling (block columns of
	 * materialization tables and partial/direct views): fail fast with a
	 * clear message at RENAME time, so the user immediately sees what
	 * they did wrong.
	 *
	 * Identify internal objects by namespace (time_series) + name prefix.
	 * The catalog still holds the source-of-truth (continuous_agg row points
	 * at mat_table_name etc.) but the prefix check is cheaper and equivalent
	 * -- these prefixes are reserved by cagg_create.c's CAGG builder and will
	 * never collide with user-visible names.
	 */
	if (IsA(parsetree, RenameStmt) && Gp_role != GP_ROLE_EXECUTE)
	{
		RenameStmt *pre_rstmt = (RenameStmt *) parsetree;
		bool		is_internal_target = false;
		bool		is_column_rename = (pre_rstmt->renameType == OBJECT_COLUMN);
		bool		is_object_rename = (pre_rstmt->renameType == OBJECT_TABLE ||
										pre_rstmt->renameType == OBJECT_VIEW);

		/*
		 * Both column-renames (RENAME COLUMN) and object-renames
		 * (RENAME TABLE / RENAME VIEW) on internal CAGG objects break
		 * cagg_refresh, because its hard-coded SQL references
		 *   continuous_agg.mat_table_name (catalog) -> _mat_<name>_<N>
		 *   continuous_agg.bucket_column  (catalog) -> bucket
		 * by name.  Renaming any of these out from under the catalog
		 * causes opaque "does not exist" errors at the next refresh.
		 *
		 * MATVIEW renames (rstmt->renameType == OBJECT_MATVIEW) are NOT
		 * blocked here -- the user view is meant to be user-facing, and
		 * the existing post-execution hook already syncs
		 * continuous_agg.user_view_name on that path.
		 */
		if ((is_column_rename || is_object_rename) &&
			pre_rstmt->relation != NULL &&
			pre_rstmt->relation->relname != NULL)
		{
			Oid			rel_oid = RangeVarGetRelid(pre_rstmt->relation,
												   NoLock, true);

			if (OidIsValid(rel_oid))
			{
				Oid			ts_ns_oid = get_namespace_oid(TS_EXTENSION_SCHEMA_NAME, true);
				Oid			rel_ns_oid = get_rel_namespace(rel_oid);
				const char *relname = pre_rstmt->relation->relname;

				if (OidIsValid(ts_ns_oid) && rel_ns_oid == ts_ns_oid &&
					(strncmp(relname, "_mat_", 5) == 0 ||
					 strncmp(relname, "_partial_view_", 14) == 0 ||
					 strncmp(relname, "_direct_view_", 13) == 0))
				{
					is_internal_target = true;
				}
			}
		}

		if (is_internal_target)
		{
			if (is_column_rename)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot rename column \"%s\" of "
								"continuous aggregate internal object \"%s\"",
								pre_rstmt->subname,
								pre_rstmt->relation->relname),
						 errhint("Drop and re-create the continuous "
								 "aggregate to change column names.")));
			else
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot rename continuous aggregate "
								"internal object \"%s\"",
								pre_rstmt->relation->relname),
						 errhint("Drop and re-create the continuous "
								 "aggregate to rename internal objects.")));
		}
	}

	/*
	 * Capture the OLD namespace of an ALTER ... SET SCHEMA target so the
	 * post-execution block (below) can locate the matching continuous_agg
	 * row.  Must run BEFORE standard_ProcessUtility because that's the
	 * call that mutates pg_class.relnamespace -- afterwards we can no
	 * longer tell what the schema used to be.
	 *
	 * Covers OBJECT_VIEW / OBJECT_MATVIEW (the CAGG user view) and
	 * OBJECT_TABLE (a CAGG source).  For OBJECT_TABLE the catalog field
	 * we sync (source_table_schema) is currently informational only --
	 * runtime resolves everything by OID -- but we follow the rename to
	 * keep the catalog honest, matching TimescaleDB
	 * (src/process_utility.c process_alterobjectschema).
	 */
	if (IsA(parsetree, AlterObjectSchemaStmt) && Gp_role != GP_ROLE_EXECUTE)
	{
		AlterObjectSchemaStmt *stmt = (AlterObjectSchemaStmt *) parsetree;

		if (stmt->relation != NULL && stmt->newschema != NULL &&
			(stmt->objectType == OBJECT_VIEW ||
			 stmt->objectType == OBJECT_MATVIEW ||
			 stmt->objectType == OBJECT_TABLE))
		{
			Oid			relid = RangeVarGetRelid(stmt->relation, NoLock, true);

			if (OidIsValid(relid))
			{
				alterobjsch_relid = relid;
				alterobjsch_old_schema =
					get_namespace_name(get_rel_namespace(relid));
				alterobjsch_objtype = stmt->objectType;
			}
		}
	}

	/* Pass through to the previous hook or standard processing */
	if (prev_ProcessUtility)
		prev_ProcessUtility(pstmt, queryString, readOnlyTree, context,
							params, queryEnv, dest, qc);
	else
		standard_ProcessUtility(pstmt, queryString, readOnlyTree, context,
								params, queryEnv, dest, qc);

	/*
	 * POST-execution hook: sync CAGG catalog after ALTER ... SET SCHEMA.
	 *
	 * For an ALTER VIEW user_view SET SCHEMA new_ns, PG moves the view
	 * in pg_class but our continuous_agg catalog stores user_view_schema
	 * as a text column; without this sync, schema-qualified policy /
	 * refresh calls (refresh_continuous_aggregate('new_ns.cv', ...)) go
	 * through _resolve_cagg_id which looks up the row by (schema, name)
	 * pair and finds nothing -- silently breaking every schema-qualified
	 * API call on the moved CAGG.
	 *
	 * For an ALTER TABLE source SET SCHEMA, the corresponding
	 * source_table_schema field is informational (runtime uses OID), so
	 * the sync here is just to keep the catalog honest.
	 *
	 * Mirrors TimescaleDB src/process_utility.c process_alterviewschema
	 * + process_altertableschema which call ts_continuous_agg_rename_view
	 * to perform the same UPDATE.
	 */
	if (IsA(parsetree, AlterObjectSchemaStmt) &&
		OidIsValid(alterobjsch_relid) &&
		alterobjsch_old_schema != NULL)
	{
		AlterObjectSchemaStmt *stmt = (AlterObjectSchemaStmt *) parsetree;
		const char *new_schema = stmt->newschema;
		const char *name = stmt->relation->relname;
		Oid			ts_ns_oid;
		Oid			ca_oid = InvalidOid;

		/*
		 * Guard against firing during CREATE/DROP EXTENSION races: the
		 * hook can run before time_series is installed (e.g. another
		 * extension being created in the same backend session) or after
		 * a DROP EXTENSION CASCADE.  In either case our catalog table
		 * does not exist and the SPI UPDATE would abort the user's DDL.
		 */
		ts_ns_oid = get_namespace_oid(TS_EXTENSION_SCHEMA_NAME, true);
		if (OidIsValid(ts_ns_oid))
			ca_oid = get_relname_relid("continuous_agg", ts_ns_oid);

		if (OidIsValid(ca_oid))
		{
			if (alterobjsch_objtype == OBJECT_VIEW ||
				alterobjsch_objtype == OBJECT_MATVIEW)
			{
				Oid			argtypes[3] = { TEXTOID, TEXTOID, TEXTOID };
				Datum		args[3];
				int			ret;

				args[0] = CStringGetTextDatum(new_schema);
				args[1] = CStringGetTextDatum(alterobjsch_old_schema);
				args[2] = CStringGetTextDatum(name);

				SPI_connect();
				ret = SPI_execute_with_args(
					"UPDATE time_series.continuous_agg "
					"SET user_view_schema = $1 "
					"WHERE user_view_schema = $2 AND user_view_name = $3",
					3, argtypes, args, NULL, false, 0);
				if (ret == SPI_OK_UPDATE && SPI_processed > 0)
					elog(NOTICE,
						 "continuous aggregate moved: \"%s.%s\" -> \"%s.%s\"",
						 alterobjsch_old_schema, name, new_schema, name);
				SPI_finish();
			}
			else if (alterobjsch_objtype == OBJECT_TABLE)
			{
				Oid			argtypes[2] = { TEXTOID, OIDOID };
				Datum		args[2];
				int			ret;

				args[0] = CStringGetTextDatum(new_schema);
				args[1] = ObjectIdGetDatum(alterobjsch_relid);

				SPI_connect();
				ret = SPI_execute_with_args(
					"UPDATE time_series.continuous_agg "
					"SET source_table_schema = $1 "
					"WHERE source_table_oid = $2",
					2, argtypes, args, NULL, false, 0);
				if (ret == SPI_OK_UPDATE && SPI_processed > 0)
					elog(NOTICE,
						 "continuous_agg source schema followed: "
						 "\"%s\" -> \"%s\" for \"%s\"",
						 alterobjsch_old_schema, new_schema, name);
				SPI_finish();
			}
		}
	}

	/*
	 * POST-execution hook: sync CAGG catalog after ALTER VIEW RENAME.
	 *
	 * PG renames the view in pg_class but our continuous_agg catalog
	 * stores user_view_name as text.  We update it here AFTER the
	 * standard handler succeeds (so the pg_class rename is committed).
	 *
	 * We only care about OBJECT_VIEW renames on the QD.
	 */
	if (IsA(parsetree, RenameStmt) && Gp_role != GP_ROLE_EXECUTE)
	{
		RenameStmt *rstmt = (RenameStmt *) parsetree;

		if (rstmt->renameType == OBJECT_VIEW &&
			rstmt->relation != NULL &&
			rstmt->newname != NULL)
		{
			const char *old_name = rstmt->relation->relname;
			const char *old_schema = rstmt->relation->schemaname;
			Oid			argtypes[3] = { TEXTOID, TEXTOID, TEXTOID };
			Datum		args[3];
			int			ret;

			if (old_schema == NULL)
				old_schema = "public";

			args[0] = CStringGetTextDatum(rstmt->newname);
			args[1] = CStringGetTextDatum(old_schema);
			args[2] = CStringGetTextDatum(old_name);

			SPI_connect();
			ret = SPI_execute_with_args(
				"UPDATE time_series.continuous_agg "
				"SET user_view_name = $1 "
				"WHERE user_view_schema = $2 AND user_view_name = $3",
				3, argtypes, args, NULL, false, 0);

			if (ret == SPI_OK_UPDATE && SPI_processed > 0)
				elog(NOTICE, "continuous aggregate renamed: \"%s\" -> \"%s\"",
					 old_name, rstmt->newname);
			SPI_finish();
		}

		/*
		 * Also sync continuous_agg.bucket_column when the user renames a
		 * source table column that is referenced as a CAGG's time column.
		 *
		 * Without this, ALTER TABLE source RENAME COLUMN time TO ts
		 * silently corrupts the catalog: the rename succeeds, the
		 * dependent _partial_view_/_direct_view_ auto-update (PG views
		 * track columns by attnum), but continuous_agg.bucket_column
		 * still holds the old name 'time'.  The next INSERT on the
		 * source fires cagg_invalidation_trigger, which calls
		 * cagg_get_time_attnum to look up 'time' by name, fails, and
		 * raises "could not find time column" -- leaving the source
		 * table effectively read-only until the user manually rolls
		 * back the rename or drops the CAGG.
		 *
		 * We follow the rename rather than block it: column rename is
		 * a legitimate user operation; only the catalog needs touching.
		 */
		if (rstmt->renameType == OBJECT_COLUMN &&
			rstmt->relation != NULL &&
			rstmt->subname != NULL &&
			rstmt->newname != NULL)
		{
			Oid			source_oid = RangeVarGetRelid(rstmt->relation,
													  NoLock, true);
			Oid			ts_ns_oid;
			Oid			ca_oid = InvalidOid;

			/*
			 * Guard: only run the catalog sync when the time_series
			 * extension is actually installed in this database.  The
			 * ProcessUtility hook fires for every backend in the
			 * cluster, including those running CREATE EXTENSION
			 * gp_toolkit (which internally RENAMEs columns) BEFORE
			 * time_series itself is installed.  Without this guard the
			 * SPI UPDATE references a missing relation and aborts the
			 * outer DDL.
			 */
			ts_ns_oid = get_namespace_oid(TS_EXTENSION_SCHEMA_NAME, true);
			if (OidIsValid(ts_ns_oid))
				ca_oid = get_relname_relid("continuous_agg", ts_ns_oid);

			if (OidIsValid(source_oid) && OidIsValid(ca_oid))
			{
				Oid			argtypes[3] = { TEXTOID, OIDOID, NAMEOID };
				Datum		args[3];
				int			ret;

				args[0] = CStringGetTextDatum(rstmt->newname);
				args[1] = ObjectIdGetDatum(source_oid);
				args[2] = DirectFunctionCall1(namein,
											  CStringGetDatum(rstmt->subname));

				SPI_connect();
				ret = SPI_execute_with_args(
					"UPDATE time_series.continuous_agg "
					"   SET bucket_column = $1::name "
					" WHERE source_table_oid = $2 "
					"   AND bucket_column = $3",
					3, argtypes, args, NULL, false, 0);

				if (ret == SPI_OK_UPDATE && SPI_processed > 0)
					elog(NOTICE,
						 "continuous_agg.bucket_column renamed: \"%s\" -> \"%s\"",
						 rstmt->subname, rstmt->newname);
				SPI_finish();
			}
		}
	}
}

/*
 * Check WITH options for time_series.continuous
 */

static bool
cagg_check_continuous_option(List *options, bool *materialized_only)
{
	ListCell   *lc;
	bool		found_continuous = false;

	if (options == NIL)
		return false;

	foreach(lc, options)
	{
		DefElem *def = lfirst_node(DefElem, lc);

		if (def->defnamespace == NULL ||
			strcmp(def->defnamespace, TS_EXTENSION_SCHEMA_NAME) != 0)
			continue;

		if (strcmp(def->defname, "continuous") == 0)
			found_continuous = true;
		else if (strcmp(def->defname, "materialized_only") == 0)
			*materialized_only = defGetBoolean(def);
	}

	return found_continuous;
}

/*
 * Main CREATE flow
 */

static void
cagg_create(CreateTableAsStmt *stmt, const char *queryString)
{
	CaggCreateInfo info;
	Query		   *query;
	IntoClause	   *into = stmt->into;

	memset(&info, 0, sizeof(info));

	/* Parse the user view name */
	info.user_view_schema = into->rel->schemaname ?
							into->rel->schemaname : "public";
	info.user_view_name = into->rel->relname;

	/* Check materialized_only */
	cagg_check_continuous_option(into->options, &info.materialized_only);

	/* Check WITH NO DATA */
	info.skip_data = into->skipData;

	/*
	 * By the time we reach ProcessUtility, stmt->query has already been
	 * through parse_analyze and is a Query node.
	 */
	query = castNode(Query, stmt->query);
	info.query = query;

	/* Validate the query structure */
	cagg_validate_query(query, &info);

	/* Extract source table info */
	cagg_extract_source_info(query, &info);

	/* Get distribution keys from source table */
	info.dist_keys = cagg_get_dist_keys(info.source_relid);

	/*
	 * Extract the SELECT portion from the original query string.
	 * The queryString contains the full "CREATE MATERIALIZED VIEW ...
	 * AS SELECT ..."; we search backwards from "SELECT" to find the
	 * right AS boundary.
	 */
	{
		const char *p = queryString;
		const char *select_start = NULL;

		/*
		 * Scan for "SELECT" keyword (case-insensitive).
		 * The first SELECT in the string is the one we want
		 * (CREATE MATERIALIZED VIEW ... AS SELECT ...).
		 */
		while (*p)
		{
			if (pg_strncasecmp(p, "SELECT", 6) == 0)
			{
				/* Make sure it's a word boundary (not inside identifier) */
				if (p == queryString || !isalnum((unsigned char) *(p - 1)))
				{
					select_start = p;
					break;
				}
			}
			p++;
		}

		if (select_start == NULL)
			ereport(ERROR,
					(errmsg("could not extract SELECT from CREATE MATERIALIZED "
							"VIEW statement")));

		/* Strip trailing semicolons, whitespace, and WITH NO DATA */
		{
			char *q = pstrdup(select_start);
			int len = strlen(q);

			/* Strip trailing whitespace and semicolons */
			while (len > 0 && (q[len - 1] == ';' || q[len - 1] == ' ' ||
							   q[len - 1] == '\n' || q[len - 1] == '\t'))
				q[--len] = '\0';

			/* Strip trailing "WITH NO DATA" (case-insensitive) */
			if (len >= 12 &&
				pg_strncasecmp(q + len - 12, "WITH NO DATA", 12) == 0)
			{
				len -= 12;
				while (len > 0 && (q[len - 1] == ' ' || q[len - 1] == '\n'))
					len--;
				q[len] = '\0';
			}

			info.original_query = q;
		}
	}

	elog(LOG, "creating continuous aggregate \"%s.%s\" on source \"%s.%s\" "
		 "(materialized_only=%s, skip_data=%s)",
		 info.user_view_schema, info.user_view_name,
		 info.source_schema, info.source_table,
		 info.materialized_only ? "true" : "false",
		 info.skip_data ? "true" : "false");

	SPI_connect();

	/* 1. Register in catalog (to get cagg_id for naming) */
	info.cagg_id = cagg_register_catalog(&info);
	elog(DEBUG1, "cagg \"%s.%s\": registered cagg_id=%d",
		 info.user_view_schema, info.user_view_name, info.cagg_id);

	/* Generate internal names */
	snprintf(info.mat_table_name, NAMEDATALEN,
			 "_mat_%s_%d", info.user_view_name, info.cagg_id);
	snprintf(info.partial_view_name, NAMEDATALEN,
			 "_partial_view_%d", info.cagg_id);
	snprintf(info.direct_view_name, NAMEDATALEN,
			 "_direct_view_%d", info.cagg_id);

	/* Update catalog with generated names */
	{
		Oid		upd_argtypes[4] = { TEXTOID, TEXTOID, TEXTOID, INT4OID };
		Datum	upd_args[4];

		upd_args[0] = CStringGetTextDatum(info.mat_table_name);
		upd_args[1] = CStringGetTextDatum(info.partial_view_name);
		upd_args[2] = CStringGetTextDatum(info.direct_view_name);
		upd_args[3] = Int32GetDatum(info.cagg_id);

		SPI_execute_with_args(
			"UPDATE time_series.continuous_agg SET "
			"mat_table_schema = 'time_series', "
			"mat_table_name = $1, "
			"partial_view_schema = 'time_series', "
			"partial_view_name = $2, "
			"direct_view_schema = 'time_series', "
			"direct_view_name = $3 "
			"WHERE cagg_id = $4",
			4, upd_argtypes, upd_args, NULL, false, 0);
	}

	/* 2. Create materialization table */
	cagg_create_mat_table(&info);
	elog(DEBUG1, "cagg \"%s.%s\": materialization table time_series.%s created",
		 info.user_view_schema, info.user_view_name, info.mat_table_name);

	/* 3. Create the three views */
	cagg_create_views(&info);
	elog(DEBUG1, "cagg \"%s.%s\": partial/direct/user views created",
		 info.user_view_schema, info.user_view_name);

	/* 4. Install invalidation trigger on source table */
	SIMPLE_FAULT_INJECTOR("cagg_create_before_trigger_install");
	cagg_install_trigger(&info);
	elog(DEBUG1, "cagg \"%s.%s\": invalidation trigger installed on source",
		 info.user_view_schema, info.user_view_name);

	/* 5. Initialize watermark for this CAGG -- one row per segment.
	 *
	 * cagg_watermark is DISTRIBUTED RANDOMLY.  We need every segment to
	 * have a local row so the trigger's threshold computation (local heap
	 * scan) can find it.  Dispatch _cagg_init_segment_watermark() via
	 * gp_dist_random('gp_id') which runs once on each segment.
	 */
	{
		Oid		argtypes[1] = { INT4OID };
		Datum	args[1];

		args[0] = Int32GetDatum(info.cagg_id);
		SPI_execute_with_args(
			"SELECT time_series._cagg_init_segment_watermark($1) "
			"FROM gp_dist_random('gp_id')",
			1, argtypes, args, NULL, true, 0);
	}

	/*
	 * 5b. Initialize invalidation threshold for this source table.
	 *
	 * cagg_invalidation_threshold stores MAX(watermark) per source,
	 * pre-computed so the trigger only needs one heap scan.  Only create
	 * rows if this is the FIRST CAGG on this source (avoid duplicates).
	 */
	{
		Oid		argtypes[1] = { OIDOID };
		Datum	args[1];
		int		ret;

		args[0] = ObjectIdGetDatum(info.source_relid);
		ret = SPI_execute_with_args(
			"SELECT 1 FROM time_series.cagg_invalidation_threshold "
			"WHERE source_table_oid = $1 LIMIT 1",
			1, argtypes, args, NULL, true, 1);

		if (ret == SPI_OK_SELECT && SPI_processed == 0)
		{
			/* First CAGG on this source -> init threshold per segment */
			SPI_execute_with_args(
				"SELECT time_series._cagg_init_segment_threshold($1) "
				"FROM gp_dist_random('gp_id')",
				1, argtypes, args, NULL, true, 0);
		}
	}

	/* 6. Register bucket function metadata */
	{
		Oid		bf_argtypes[6] = { INT4OID, INTERVALOID, OIDOID,
								   TIMESTAMPTZOID, INTERVALOID, TEXTOID };
		Datum	bf_args[6];
		char	bf_nulls[6] = { ' ', ' ', ' ', ' ', ' ', ' ' };

		bf_args[0] = Int32GetDatum(info.cagg_id);
		bf_args[1] = IntervalPGetDatum(info.bucket_width);
		bf_args[2] = ObjectIdGetDatum(info.time_type);

		if (info.has_origin)
			bf_args[3] = info.bucket_origin;
		else
			bf_nulls[3] = 'n';

		if (info.bucket_offset != NULL)
			bf_args[4] = IntervalPGetDatum(info.bucket_offset);
		else
			bf_nulls[4] = 'n';

		if (info.bucket_timezone != NULL)
			bf_args[5] = CStringGetTextDatum(info.bucket_timezone);
		else
			bf_nulls[5] = 'n';

		SPI_execute_with_args(
			"INSERT INTO time_series.cagg_bucket_function "
			"(cagg_id, bucket_width, time_type, bucket_origin, "
			"bucket_offset, bucket_timezone) "
			"VALUES ($1, $2, $3, $4, $5, $6)",
			6, bf_argtypes, bf_args, bf_nulls, false, 0);
	}

	/*
	 * cagg_invalidation_threshold is initialized in step 5b above.
	 * REFRESH updates it to MAX(watermark) so the trigger can do a
	 * single-table heap scan instead of joining continuous_agg + cagg_watermark.
	 */

	SPI_finish();

	ereport(NOTICE,
			(errmsg("continuous aggregate \"%s\" successfully created",
					info.user_view_name),
			 errdetail("Materialization table: time_series.%s",
					   info.mat_table_name)));
}

/*
 * Query Validation
 */

static void
cagg_validate_query(Query *query, CaggCreateInfo *info)
{
	ListCell   *lc;
	bool		found_bucket = false;
	int			bucket_count = 0;

	/* Must be a SELECT */
	if (query->commandType != CMD_SELECT)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("continuous aggregate query must be a SELECT")));

	/* Must have GROUP BY */
	if (query->groupClause == NIL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("continuous aggregate query must have a GROUP BY "
						"clause with time_bucket")));

	/*
	 * Reject JOIN, CTE, and subquery FROM-items by scanning the rtable BEFORE
	 * any code that calls get_attname() on the rtable.  A CTE RTE has relid=0,
	 * so doing this later would crash with "cache lookup failed for attribute".
	 * Looking only at jointree->fromlist length is not enough: an explicit
	 * SQL JOIN (`FROM a JOIN b ON ...`) is parsed as a single JoinExpr in
	 * fromlist (length 1) but adds an RTE_JOIN entry to the rtable.
	 */
	{
		ListCell   *rtlc;
		int			rel_count = 0;

		foreach(rtlc, query->rtable)
		{
			RangeTblEntry *rte = (RangeTblEntry *) lfirst(rtlc);

			switch (rte->rtekind)
			{
				case RTE_RELATION:
					rel_count++;
					break;
				case RTE_JOIN:
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("continuous aggregate does not support JOIN")));
					break;
				case RTE_CTE:
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("continuous aggregate does not support WITH (CTE)")));
					break;
				case RTE_SUBQUERY:
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("continuous aggregate does not support subqueries in FROM")));
					break;
				default:
					/* RTE_FUNCTION, RTE_VALUES, etc. -- not a base table */
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("continuous aggregate FROM clause must "
									"reference a base table")));
					break;
			}
		}

		if (rel_count > 1)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("continuous aggregate does not support JOIN")));
	}

	/*
	 * Must have time_bucket() in GROUP BY clause (not just SELECT list).
	 * upstream enforces this same rule -- time_bucket defines the
	 * materialization granularity, so it must be a GROUP BY key.
	 */
	foreach(lc, query->groupClause)
	{
		SortGroupClause *sgc = (SortGroupClause *) lfirst(lc);
		TargetEntry *tle = get_sortgroupclause_tle(sgc, query->targetList);

		if (tle && IsA(tle->expr, FuncExpr))
		{
			FuncExpr *func = (FuncExpr *) tle->expr;

			if (is_time_bucket_funcexpr(func))
			{
				bucket_count++;

				if (bucket_count > 1)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("continuous aggregate must have exactly "
									"one time_bucket function")));

				found_bucket = true;

				/*
				 * Extract bucket_width: first argument should be an interval
				 * constant.
				 */
				Node *width_arg = linitial(func->args);

				if (IsA(width_arg, Const))
				{
					Const *c = (Const *) width_arg;

					if (c->consttype == INTERVALOID && !c->constisnull)
						info->bucket_width = DatumGetIntervalP(c->constvalue);
				}

				if (info->bucket_width == NULL)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("time_bucket width must be a constant interval")));

				/* Reject zero or negative bucket width */
				{
					Interval *bw = info->bucket_width;
					if (bw->month == 0 && bw->day == 0 && bw->time == 0)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("continuous aggregate bucket width must not be zero")));
					if (bw->month < 0 || bw->day < 0 || bw->time < 0)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("continuous aggregate bucket width must be positive")));

					/*
					 * Reject mixed-unit intervals: months combined with days
					 * or sub-day time.  Months are variable-length (28-31
					 * days), so a width like '1 month 1 day' has no
					 * consistent bucket boundary -- PG's time_bucket()
					 * itself ereports "month intervals cannot have day or
					 * time component" at runtime, leaving the CAGG
					 * dead-on-arrival: CREATE succeeds, but every SELECT,
					 * every REFRESH, and even INSERTs on the source (via
					 * the trigger's threshold-estimate time_bucket call)
					 * fail with the same error.  Reject at CREATE time so
					 * the user gets a clear message before the CAGG
					 * exists.  Aligns with TimescaleDB
					 * (tsl/src/continuous_aggs/common.c "invalid interval
					 * specified").
					 */
					if (bw->month != 0 && (bw->day != 0 || bw->time != 0))
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("invalid interval specified"),
								 errhint("Use either months or days and "
										 "hours, but not months, days and "
										 "hours together.  Months are "
										 "variable-length (28-31 days), so "
										 "a mixed-unit width has no "
										 "consistent bucket boundary.")));
				}

				/*
				 * Extract bucket_column: second argument should be a Var
				 * referencing the source table column.
				 */
				if (list_length(func->args) >= 2)
				{
					Node *col_arg = lsecond(func->args);

					if (IsA(col_arg, Var))
					{
						Var *var = (Var *) col_arg;
						RangeTblEntry *rte = list_nth(query->rtable,
													  var->varno - 1);
						info->bucket_column =
							pstrdup(get_attname(rte->relid, var->varattno,
												false));
						info->time_type = var->vartype;
					}
				}

				if (info->bucket_column == NULL)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("could not determine bucket column from time_bucket() call")));

				/*
				 * Restrict the time column type to the set the rest of the
				 * pipeline actually handles.  cagg_invalidation_trigfn ->
				 * cagg_extract_time only knows TIMESTAMPTZ / TIMESTAMP / DATE;
				 * the L1/L2/watermark catalogs store timestamptz.  Without
				 * this guard a user can sneak past the INTERVAL-bucket-width
				 * check (e.g. time_bucket(60::interval, ts_int)) and the
				 * trigger then ERRORs on every INSERT/UPDATE/DELETE on the
				 * source table -- a write-block.
				 */
				if (info->time_type != TIMESTAMPTZOID &&
					info->time_type != TIMESTAMPOID &&
					info->time_type != DATEOID)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("unsupported time column type for continuous aggregate: %s",
									format_type_be(info->time_type)),
							 errhint("Supported types: timestamp, timestamptz, date.")));

				/*
				 * Extract optional advanced parameters (3rd+ args).
				 */
				{
					ListCell *lc2;
					int		  argidx = 0;

					foreach(lc2, func->args)
					{
						Node *arg = lfirst(lc2);

						argidx++;
						if (argidx <= 2)
							continue;

						if (IsA(arg, Const))
						{
							Const *c = (Const *) arg;

							if (c->constisnull)
								continue;

							if (c->consttype == TEXTOID)
							{
								char *tz_name = TextDatumGetCString(c->constvalue);

								/*
								 * Validate the timezone name at CREATE time
								 * rather than letting time_bucket fail at
								 * REFRESH / query time.  pg_tzset() returns
								 * NULL for any name not in the system tzdata,
								 * so a typo like 'Asia/Shangai' is caught here
								 * instead of producing a CAGG that throws
								 * "time zone ... not recognized" the first
								 * time anyone touches it.  Aligns with
								 * TimescaleDB (tsl/src/continuous_aggs/common.c
								 * "invalid timezone name").
								 */
								if (tz_name == NULL ||
									tz_name[0] == '\0' ||
									pg_tzset(tz_name) == NULL)
									ereport(ERROR,
											(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
											 errmsg("invalid timezone name \"%s\"",
													tz_name ? tz_name : ""),
											 errhint("Use a valid timezone name such as "
													 "'UTC' or 'Asia/Shanghai'.  A bad "
													 "name would silently create a CAGG "
													 "that errors on every REFRESH and "
													 "every query.")));

								info->bucket_timezone = pstrdup(tz_name);
							}
							else if (c->consttype == INTERVALOID)
							{
								info->bucket_offset =
									DatumGetIntervalP(c->constvalue);
							}
							else if (c->consttype == TIMESTAMPTZOID ||
									 c->consttype == TIMESTAMPOID ||
									 c->consttype == DATEOID)
							{
								/*
								 * Reject +/-infinity origin: time_bucket computes
								 * bucket_start = origin + floor((t - origin) /
								 * width) * width, which produces garbage when
								 * origin is non-finite (the subtraction overflows
								 * int64 microseconds and the floor rounds toward
								 * INT64_MIN).  Buckets end up at values like
								 * '2023-12-31 23:00:54.775807+00' and the mat
								 * table holds data the user can't meaningfully
								 * query.  Aligns with TimescaleDB
								 * (tsl/src/continuous_aggs/common.c "invalid
								 * origin value: infinity").
								 */
								if (c->consttype == DATEOID)
								{
									if (DATE_NOT_FINITE(DatumGetDateADT(c->constvalue)))
										ereport(ERROR,
												(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
												 errmsg("invalid origin value: infinity"),
												 errhint("time_bucket origin must be a finite "
														 "timestamp; +/-infinity produces "
														 "overflow-driven bucket values that "
														 "make the materialization unusable.")));
								}
								else
								{
									if (TIMESTAMP_NOT_FINITE(DatumGetTimestamp(c->constvalue)))
										ereport(ERROR,
												(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
												 errmsg("invalid origin value: infinity"),
												 errhint("time_bucket origin must be a finite "
														 "timestamp; +/-infinity produces "
														 "overflow-driven bucket values that "
														 "make the materialization unusable.")));
								}

								info->bucket_origin = c->constvalue;
								info->has_origin = true;
							}
						}
					}
				}
			}
		}
	}

	if (!found_bucket)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("continuous aggregate view must include a valid "
						"time_bucket function in GROUP BY")));

	/*
	 * origin and offset are alternative ways to shift the bucket grid:
	 * each one alone is fine, but combining them is semantically
	 * ambiguous (we add them, TSDB doesn't combine them).  T44 in
	 * cagg_create.sql already documents this for the 4-arg
	 * time_bucket(interval, ts, timestamptz, interval) form -- PG
	 * function resolution blocks that one because the signature is
	 * not registered.  The 5-arg form
	 * time_bucket(interval, ts, text, timestamptz, interval) IS
	 * registered (origin and offset have DEFAULT NULL), so PG happily
	 * resolves the call when the user passes both non-NULL.  Without
	 * an explicit reject here, the CAGG silently combines them
	 * (effective_origin = origin + offset) and the user has no way
	 * to know which alignment grid is actually being used.  Aligns
	 * with TimescaleDB (tsl/src/continuous_aggs/common.c "using offset
	 * and origin in a time_bucket function at the same time is not
	 * supported").
	 */
	if (info->bucket_offset != NULL && info->has_origin)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("using offset and origin in a time_bucket "
						"function at the same time is not supported"),
				 errhint("Pass exactly one of origin and offset; the two "
						 "are alternative ways to shift the bucket grid "
						 "and combining them produces ambiguous bucket "
						 "boundaries.  If you do not need to shift the "
						 "grid at all, omit both (or pass NULL).")));

	/* --- Reject unsupported syntax --- */

	/* LIMIT / OFFSET */
	if (query->limitCount || query->limitOffset)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("continuous aggregate does not support LIMIT/OFFSET")));

	/* Subqueries */
	if (query->hasSubLinks)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("continuous aggregate does not support subqueries")));

	/* Window functions */
	if (query->hasWindowFuncs)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("continuous aggregate does not support window functions")));

	/* HAVING is allowed (V1 supports it) */

	/* DISTINCT / DISTINCT ON */
	if (query->distinctClause)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("continuous aggregate does not support DISTINCT")));

	/* CTE (WITH clause) */
	if (query->cteList)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("continuous aggregate does not support CTEs (WITH clause)")));

	/* Recursive CTE -- query-level flag stronger than cteList alone */
	if (query->hasRecursive)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("continuous aggregate does not support WITH RECURSIVE")));

	/* Modifying CTE -- WITH d AS (DELETE/UPDATE/INSERT ...) SELECT ... */
	if (query->hasModifyingCTE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("continuous aggregate does not support data-modifying CTEs")));

	/* Set-returning function in target list */
	if (query->hasTargetSRFs)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("continuous aggregate does not support set-returning "
						"functions in the target list")));

	/* Query-level Row-Level Security flag (covers RLS coming through
	 * inheritance / views in addition to the per-RTE relrowsecurity
	 * check below) */
	if (query->hasRowSecurity)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("continuous aggregate does not support row-level security")));

	/* GROUPING SETS / ROLLUP / CUBE */
	if (query->groupingSets)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("continuous aggregate does not support GROUPING SETS,"
						" ROLLUP, or CUBE")));

	/* UNION / EXCEPT / INTERSECT */
	if (query->setOperations)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("continuous aggregate does not support UNION, EXCEPT,"
						" or INTERSECT")));

	/* FOR UPDATE / FOR SHARE */
	if (query->rowMarks)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("continuous aggregate does not support FOR UPDATE/SHARE")));

	/* TABLESAMPLE / FROM ONLY / RLS -- check each RTE */
	{
		ListCell *rlc;

		foreach(rlc, query->rtable)
		{
			RangeTblEntry *rte = lfirst_node(RangeTblEntry, rlc);

			if (rte->tablesample)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("TABLESAMPLE is not supported in continuous aggregate")));

			/*
			 * FROM ONLY check is skipped: in CBDB regular tables have
			 * inh=false by default (not inherited), so checking !rte->inh
			 * would false-positive on every normal table. FROM ONLY is
			 * only relevant for hypertables/inheritance, which V1 doesn't
			 * use as source tables.
			 */

			if (rte->rtekind == RTE_RELATION && rte->relid != InvalidOid)
			{
				Relation	rel = table_open(rte->relid, NoLock);
				bool		has_rls = rel->rd_rel->relrowsecurity;

				table_close(rel, NoLock);
				if (has_rls)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("row-level security is not supported by"
									" continuous aggregate")));
			}
		}
	}

	/*
	 * Reject STABLE/VOLATILE expressions outside aggregates.
	 *
	 * A STABLE function (e.g., timestamptz::timestamp) depends on session
	 * settings like timezone.  If REFRESH runs in one timezone and the user
	 * queries in another, the mat branch and live branch of the UNION ALL
	 * view would compute different values for the same data -- causing data
	 * loss.  Same behavior as upstream pre-PG17.
	 *
	 * We walk each target list entry and GROUP BY expression.  Aggref nodes
	 * are skipped (aggregates are evaluated fresh by both branches).
	 */
	{
		ListCell *tlc;

		foreach(tlc, query->targetList)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(tlc);

			/* Skip resjunk entries (internal to GROUP BY, not projected) */
			if (tle->resjunk)
				continue;

			/*
			 * Check ALL target entries (including Aggrefs) for mutable
			 * functions.  contain_mutable_functions() walks the entire
			 * expression tree, including Aggref->args, so an aggregate
			 * with a VOLATILE/STABLE argument like sum(random()) or
			 * count(now()) is correctly rejected.
			 *
			 * Why this matters: the materialization stores the result
			 * of the aggregate computed ONCE at REFRESH time; the live
			 * branch re-computes it on every query.  A VOLATILE arg
			 * (random) makes the two branches disagree forever; a
			 * STABLE arg (now, ::timestamp on a TZ-dependent value)
			 * makes them disagree across sessions / transactions.
			 * Aligns with TimescaleDB which rejects the same
			 * (finalize.c contain_mutable_functions check).
			 */
			if (contain_mutable_functions((Node *) tle->expr))
			{
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("continuous aggregate SELECT expression contains "
								"a mutable (non-IMMUTABLE) function"),
						 errhint("Only IMMUTABLE functions are allowed in the "
								 "SELECT list of a continuous aggregate, "
								 "including inside aggregate arguments (e.g. "
								 "sum(random()) is not allowed).  STABLE/VOLATILE "
								 "expressions like '::timestamp', now(), random() "
								 "would produce inconsistent results between the "
								 "materialized and real-time branches.")));
			}
		}
	}

	/*
	 * Same check on WHERE and HAVING clauses.
	 *
	 * A WHERE clause filtering on a STABLE/VOLATILE function (most
	 * commonly `WHERE time > now() - interval '7 days'`) silently
	 * makes the materialization result depend on **when refresh is
	 * called**, not just on the source data -- past refresh runs
	 * include rows that the current refresh's WHERE excludes,
	 * producing "ghost buckets" in the materialization that the
	 * live branch can no longer see.  HAVING has the same problem
	 * for post-aggregate filters.
	 *
	 * target-list mutable functions are at least visible in the
	 * user-facing column values; mutable WHERE/HAVING is silent.
	 * Reject both at create time.
	 */
	if (query->jointree && query->jointree->quals &&
		contain_mutable_functions((Node *) query->jointree->quals))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("continuous aggregate WHERE clause contains "
						"a mutable (non-IMMUTABLE) function"),
				 errhint("WHERE clauses like 'time > now() - interval ...' "
						 "produce different row sets on each refresh, "
						 "leaving stale materialization rows that the "
						 "live branch can no longer reach.  Use a fixed "
						 "timestamp or apply the filter in the SELECT "
						 "querying the CAGG instead.")));
	}

	if (query->havingQual &&
		contain_mutable_functions((Node *) query->havingQual))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("continuous aggregate HAVING clause contains "
						"a mutable (non-IMMUTABLE) function"),
				 errhint("Filter on aggregate output in queries against "
						 "the continuous aggregate, not in its definition.")));
	}
}

/*
 * Helper: check if a FuncExpr is time_bucket()
 */

static bool
is_time_bucket_funcexpr(FuncExpr *func)
{
	char *funcname;

	funcname = get_func_name(func->funcid);
	if (funcname == NULL)
		return false;

	return strcmp(funcname, "time_bucket") == 0;
}

/*
 * Extract source table info from query
 */

static void
cagg_extract_source_info(Query *query, CaggCreateInfo *info)
{
	RangeTblEntry *rte;

	if (list_length(query->rtable) < 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("continuous aggregate query must reference a table")));

	rte = linitial(query->rtable);

	if (rte->rtekind != RTE_RELATION)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("continuous aggregate source must be a table")));

	/*
	 * Only allow plain tables (RELKIND_RELATION).  Materialized views,
	 * foreign tables, partitioned tables, etc. are rejected because:
	 *   - Triggers cannot fire on matviews (no INSERT path)
	 *   - Foreign tables have no local storage for trigger-based invalidation
	 *   - Partitioned tables need special handling (V2+)
	 */
	{
		char relkind = get_rel_relkind(rte->relid);

		if (relkind != RELKIND_RELATION)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("\"%s\" is not a plain table",
							get_rel_name(rte->relid)),
					 errdetail("Continuous aggregate source must be a plain table, "
							   "not a %s.",
							   relkind == RELKIND_MATVIEW ? "materialized view" :
							   relkind == RELKIND_VIEW ? "view" :
							   relkind == RELKIND_FOREIGN_TABLE ? "foreign table" :
							   relkind == RELKIND_PARTITIONED_TABLE ? "partitioned table" :
							   "non-table relation")));
	}

	/*
	 * Source must use the time_series table access method.  Plain heap
	 * (or any other non-time_series tableam) is rejected because:
	 *
	 *   - The whole CAGG model assumes append-only source data: mat is
	 *     filled at REFRESH time and trusted to match source for buckets
	 *     below the watermark.  time_series enforces this via the tableam
	 *     (ts_heap_tuple_delete / ts_heap_tuple_update each ereport
	 *     "cannot delete/update from a time_series table").  Plain heap
	 *     does not enforce it -- a stray UPDATE / DELETE on the source
	 *     leaves the mat table holding ghost rows with no way to recover.
	 *
	 *   - The cagg_invalidation_trigger is BEFORE INSERT only (see the
	 *     long comment around the CREATE TRIGGER below for the
	 *     trigger.c:2572 reason on time_series sources).  A trigger that
	 *     only catches INSERT cannot keep a non-append-only source in
	 *     sync; the right invariant is "source is append-only", which we
	 *     enforce by requiring the time_series tableam.
	 *
	 * Aligns with TimescaleDB which raises "table \"%s\" is not a
	 * hypertable" in tsl/src/continuous_aggs/common.c.
	 */
	{
		Relation source_rel = table_open(rte->relid, AccessShareLock);
		bool is_ts = RelationIsTimeSeries(source_rel);

		if (!is_ts)
		{
			table_close(source_rel, AccessShareLock);
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("\"%s\" is not a time_series table",
							get_rel_name(rte->relid)),
					 errdetail("Continuous aggregate source must use the "
							   "time_series table access method."),
					 errhint("CREATE TABLE ... USING time_series WITH "
							   "(ts_partition_column = ..., "
							   "ts_chunk_interval = ..., "
							   "ts_chunk_origin = ...).")));
		}

		/*
		 * The time_bucket() column must be the source table's partition
		 * (time) column.  cagg_validate_query has already extracted the
		 * bucketed column name into info->bucket_column; here we confirm
		 * it matches the table's ts_partition_column.
		 *
		 * Bucketing on any other column is accepted by the parser but is
		 * a trap: the source's chunks are range-partitioned on the
		 * partition column, so a live-branch predicate on a different
		 * column (time_bucket(other_col) >= watermark) can never prune
		 * chunks -- every query and every REFRESH degrades to a full scan
		 * of all chunks, defeating the entire point of a continuous
		 * aggregate.  It also makes the REFRESH threshold-estimation SQL
		 * (which scopes max(bucket_col) by the latest chunk's
		 * partition-column range) semantically meaningless.  Reject at
		 * CREATE time.  Aligns with TimescaleDB (tsl/src/continuous_aggs/
		 * common.c "time bucket function must reference the primary
		 * hypertable dimension column").
		 */
		{
			TSRelOptions *opts = (TSRelOptions *) source_rel->rd_options;

			if (opts != NULL && opts->ts_column != 0 &&
				info->bucket_column != NULL)
			{
				const char *part_col = (const char *) opts + opts->ts_column;

				if (strcmp(info->bucket_column, part_col) != 0)
				{
					/*
					 * Both part_col (points into source_rel->rd_options)
					 * and the relation name are invalidated by
					 * table_close, so copy them out before closing.
					 */
					char	   *part_col_copy = pstrdup(part_col);
					char	   *src_name = pstrdup(RelationGetRelationName(source_rel));

					table_close(source_rel, AccessShareLock);
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("time_bucket function must reference the "
									"partition column \"%s\" of time_series "
									"table \"%s\"",
									part_col_copy, src_name),
							 errdetail("The continuous aggregate buckets on "
									   "column \"%s\", but the source table is "
									   "partitioned on \"%s\".",
									   info->bucket_column, part_col_copy),
							 errhint("Bucketing on a non-partition column "
									 "prevents chunk pruning, so every query "
									 "and refresh would scan all chunks.  Use "
									 "the partition column in time_bucket().")));
				}
			}
		}

		table_close(source_rel, AccessShareLock);
	}

	info->source_relid = rte->relid;
	info->source_schema = get_namespace_name(get_rel_namespace(rte->relid));
	info->source_table = get_rel_name(rte->relid);
}

/*
 * Get distribution keys from source table
 *
 * Returns a List of cstring column names (allocated in the caller's
 * memory context) representing the source table's distribution key
 * columns, in distkey order.  Returns NIL if the source is distributed
 * randomly or replicated.
 */

static List *
cagg_get_dist_keys(Oid relid)
{
	int			ret;
	Oid			argtypes[1] = { OIDOID };
	Datum		args[1];
	MemoryContext caller_cxt = CurrentMemoryContext;
	List	   *result = NIL;

	args[0] = ObjectIdGetDatum(relid);

	SPI_connect();
	ret = SPI_execute_with_args(
		"SELECT a.attname "
		"FROM gp_distribution_policy dp "
		"JOIN LATERAL unnest(dp.distkey) WITH ORDINALITY AS u(attnum, ord) ON TRUE "
		"JOIN pg_attribute a ON a.attrelid = dp.localoid AND a.attnum = u.attnum "
		"WHERE dp.localoid = $1 "
		"ORDER BY u.ord",
		1, argtypes, args, NULL, true, 0);

	if (ret == SPI_OK_SELECT && SPI_processed > 0)
	{
		uint64	i;

		for (i = 0; i < SPI_processed; i++)
		{
			char	   *val = SPI_getvalue(SPI_tuptable->vals[i],
										  SPI_tuptable->tupdesc, 1);
			MemoryContext oldcxt;
			char	   *copied;

			if (!val)
				continue;

			/* Copy into caller's context so both the string and the list
			 * cells survive SPI_finish(). */
			oldcxt = MemoryContextSwitchTo(caller_cxt);
			copied = pstrdup(val);
			result = lappend(result, copied);
			MemoryContextSwitchTo(oldcxt);
		}
	}

	SPI_finish();
	return result;
}

/*
 * Create materialization table
 */

static void
cagg_create_mat_table(CaggCreateInfo *info)
{
	StringInfoData sql;
	ListCell   *lc;
	bool		first = true;

	initStringInfo(&sql);

	appendStringInfo(&sql, "CREATE TABLE time_series.%s (",
					 quote_identifier(info->mat_table_name));

	/* Build column list from query target list */
	foreach(lc, info->query->targetList)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);
		Oid			typid = exprType((Node *) tle->expr);
		const char *colname;

		if (tle->resjunk)
			continue;

		colname = tle->resname ? tle->resname :
				  psprintf("col_%d", tle->resno);

		if (!first)
			appendStringInfoString(&sql, ", ");
		first = false;

		appendStringInfo(&sql, "%s %s",
						 quote_identifier(colname),
						 format_type_be(typid));
	}

	appendStringInfoChar(&sql, ')');

	/*
	 * Distribution strategy for materialization table (co-location first):
	 *   Strategy 1: If ALL source distribution-key columns appear in the
	 *               target list as plain Var references, use exactly those
	 *               columns (same order).  REFRESH DML is then Motion-free
	 *               because partial aggregates can be written locally.
	 *   Strategy 2: Otherwise pick the FIRST GROUP BY column that is NOT a
	 *               time_bucket() expression.  This keeps data co-located by
	 *               tag dimension and avoids the hot-segment pathology of
	 *               hashing on time buckets.
	 *   Strategy 3: Fallback when GROUP BY contains only time_bucket().
	 *               Distribute by the bucket column -- the result set is tiny
	 *               (one row per bucket) so skew is irrelevant, and downstream
	 *               hierarchical CAGGs can co-locate on bucket.
	 */
	{
		List	   *chosen_cols = NIL;	/* list of cstring column names */
		ListCell   *lc;

		/* Strategy 1: match ALL source dist keys */
		if (info->dist_keys != NIL)
		{
			bool		all_matched = true;
			List	   *matched = NIL;

			foreach(lc, info->dist_keys)
			{
				const char *srckey = (const char *) lfirst(lc);
				const char *matched_alias = NULL;
				ListCell   *tlc;

				foreach(tlc, info->query->targetList)
				{
					TargetEntry *tle = lfirst_node(TargetEntry, tlc);
					Var		   *v;
					RangeTblEntry *r;
					const char *srcname;

					if (tle->resjunk || !IsA(tle->expr, Var))
						continue;

					v = (Var *) tle->expr;
					r = list_nth(info->query->rtable, v->varno - 1);
					srcname = get_attname(r->relid, v->varattno, true);
					if (srcname && strcmp(srcname, srckey) == 0)
					{
						matched_alias = tle->resname ? tle->resname : srcname;
						break;
					}
				}

				if (matched_alias == NULL)
				{
					all_matched = false;
					break;
				}
				matched = lappend(matched, (void *) matched_alias);
			}

			if (all_matched)
				chosen_cols = matched;
		}

		/* Strategy 2: first non-time_bucket GROUP BY column */
		if (chosen_cols == NIL)
		{
			foreach(lc, info->query->groupClause)
			{
				SortGroupClause *sgc = (SortGroupClause *) lfirst(lc);
				TargetEntry *tle = get_sortgroupclause_tle(sgc,
														   info->query->targetList);
				bool	is_bucket = false;

				if (tle == NULL || tle->resname == NULL)
					continue;

				if (IsA(tle->expr, FuncExpr) &&
					is_time_bucket_funcexpr((FuncExpr *) tle->expr))
					is_bucket = true;

				if (!is_bucket)
				{
					chosen_cols = list_make1((void *) tle->resname);
					break;
				}
			}
		}

		/* Strategy 3: GROUP BY is only time_bucket -- use the bucket column */
		if (chosen_cols == NIL)
		{
			TargetEntry *first_tle = linitial(info->query->targetList);
			const char *bucket_alias = first_tle->resname ?
									   first_tle->resname : "bucket";

			chosen_cols = list_make1((void *) bucket_alias);
		}

		appendStringInfoString(&sql, " DISTRIBUTED BY (");
		{
			bool	first_col = true;

			foreach(lc, chosen_cols)
			{
				const char *col = (const char *) lfirst(lc);

				if (!first_col)
					appendStringInfoString(&sql, ", ");
				first_col = false;
				appendStringInfoString(&sql, quote_identifier(col));
			}
		}
		appendStringInfoChar(&sql, ')');
	}

	SPI_execute(sql.data, false, 0);

	/*
	 * Create index on bucket column.  In the materialization table the bucket
	 * column uses the alias from the user's SELECT (typically "bucket"), not
	 * the original source column name.  Find it from the first target entry.
	 */
	{
		TargetEntry *first_tle = linitial(info->query->targetList);
		const char *idx_col = first_tle->resname ? first_tle->resname : "bucket";

		resetStringInfo(&sql);
		appendStringInfo(&sql,
			"CREATE INDEX ON time_series.%s (%s)",
			quote_identifier(info->mat_table_name),
			quote_identifier(idx_col));
		SPI_execute(sql.data, false, 0);
	}

	pfree(sql.data);
}

/*
 * Create the three views
 */

static void
cagg_create_views(CaggCreateInfo *info)
{
	StringInfoData sql;

	initStringInfo(&sql);

	/*
	 * View 1: Partial View -- same as user's SELECT, used by REFRESH.
	 * We use the original_query string (the user's SELECT).
	 */
	resetStringInfo(&sql);
	appendStringInfo(&sql,
		"CREATE VIEW time_series.%s AS %s",
		info->partial_view_name,
		info->original_query);
	SPI_execute(sql.data, false, 0);

	/*
	 * View 2: Direct View -- same as user's SELECT, used for real-time branch.
	 */
	resetStringInfo(&sql);
	appendStringInfo(&sql,
		"CREATE VIEW time_series.%s AS %s",
		info->direct_view_name,
		info->original_query);
	SPI_execute(sql.data, false, 0);

	/*
	 * View 3: User View.
	 *
	 * Two modes based on materialized_only:
	 *   - false (real-time): UNION ALL of mat table (bucket < watermark)
	 *     and direct view (bucket >= watermark).  Users see newly inserted
	 *     data immediately via the live-aggregated branch, without REFRESH.
	 *   - true (materialized-only): simple passthrough over the mat table.
	 *
	 * The bucket alias is the user-chosen resname for time_bucket() in
	 * their SELECT list (first target entry).
	 */
	{
		TargetEntry *first_tle = linitial_node(TargetEntry,
											   info->query->targetList);
		const char *bucket_alias = first_tle->resname ?
								   first_tle->resname : "bucket";

		resetStringInfo(&sql);
		if (info->materialized_only)
		{
			appendStringInfo(&sql,
				"CREATE VIEW %s.%s AS SELECT * FROM time_series.%s",
				quote_identifier(info->user_view_schema),
				quote_identifier(info->user_view_name),
				quote_identifier(info->mat_table_name));
		}
		else
		{
			appendStringInfo(&sql,
				"CREATE VIEW %s.%s AS "
				"SELECT * FROM time_series.%s "
				"WHERE %s < time_series.cagg_watermark(%d) "
				"UNION ALL "
				"SELECT * FROM time_series.%s "
				"WHERE %s >= time_series.cagg_watermark(%d)",
				quote_identifier(info->user_view_schema),
				quote_identifier(info->user_view_name),
				quote_identifier(info->mat_table_name),
				quote_identifier(bucket_alias), info->cagg_id,
				quote_identifier(info->direct_view_name),
				quote_identifier(bucket_alias), info->cagg_id);
		}
		SPI_execute(sql.data, false, 0);
	}

	pfree(sql.data);
}

/*
 * Install invalidation trigger on source table
 */

static void
cagg_install_trigger(CaggCreateInfo *info)
{
	StringInfoData sql;

	initStringInfo(&sql);

	/*
	 * Check if trigger already exists (shared trigger for multiple CAGGs
	 * on the same source table).
	 */
	{
		Oid		argtypes[1] = { OIDOID };
		Datum	args[1];
		int		ret;

		args[0] = ObjectIdGetDatum(info->source_relid);

		ret = SPI_execute_with_args(
			"SELECT 1 FROM pg_trigger t "
			"WHERE t.tgrelid = $1 "
			"AND t.tgname = 'cagg_invalidation_trigger'",
			1, argtypes, args, NULL, true, 1);

		if (ret == SPI_OK_SELECT && SPI_processed > 0)
		{
			pfree(sql.data);
			return;		/* trigger already installed */
		}
	}

	/*
	 * Why BEFORE (not AFTER) ROW trigger.
	 *
	 * == The problem with AFTER on hypertables ==
	 *
	 * On a time_series hypertable the physical row does NOT live in
	 * the main fork (forknum 0).  ts_heap_route_to_chunk decides which
	 * chunk fork (forknum = TS_FIRST_CHUNKNUM + chunk_idx, range
	 * EXTENSION_FIRST_FORKNUM..UINT16_MAX) the row belongs to, and
	 * ts_heap_insert writes it there.  The CTID handed back to the
	 * executor encodes only (block, offset) -- PG's ItemPointerData has
	 * no room for a fork number.
	 *
	 * PG's AFTER ROW trigger machinery queues an event per affected
	 * row and only fires the trigfn at end-of-statement.  When it
	 * fires, the framework hard-codes a re-fetch by CTID against the
	 * source relation:
	 *
	 *     table_tuple_fetch_row_version(rel, &event->ate_ctid1,
	 *                                   SnapshotAny, slot);
	 *
	 * For a standard heap this reads from the main fork -- unambiguous,
	 * always correct.  For our hypertable it dispatches into
	 * ts_heap_fetch_row_version, which walks all chunk forks looking
	 * for any tuple at that (block, offset).  Because different chunks
	 * can carry tuples at the same (block, offset) -- typical when each
	 * chunk only holds a handful of pages -- that scan can return the
	 * WRONG row from a different chunk (we observed bucket 00:05 being
	 * recorded as invalidated when the INSERT was actually at 2027) or
	 * fail outright with "failed to fetch tuple1 for AFTER trigger".
	 * Either outcome is unrecoverable for L1 invalidation correctness.
	 *
	 * == Why BEFORE sidesteps it ==
	 *
	 * BEFORE ROW triggers fire SYNCHRONOUSLY, before the row is even
	 * routed to the chunk fork.  cagg_invalidation_trigfn reads NEW.*
	 * directly from trigdata->tg_newslot (an in-memory TupleTableSlot)
	 * -- no CTID, no fork lookup, no chunk-aware fetch.  The trigger
	 * never enters the PG path that's broken on hypertables.
	 *
	 * == Why BEFORE is observationally equivalent to AFTER here ==
	 *
	 * BEFORE vs AFTER would matter for a trigger that (a) modifies the
	 * NEW tuple before write, (b) skips the write by returning NULL,
	 * or (c) needs to see post-write state such as serial-assigned IDs.
	 * cagg_invalidation_trigfn does none of these:
	 *
	 *   1. It only READS the bucket-time column.  The value of that
	 *      column is unchanged by the chunk-routing / write step that
	 *      sits between BEFORE and AFTER, so BEFORE sees the same
	 *      value AFTER would have seen.
	 *
	 *   2. It always returns NEW (RETURN-NEW for INSERT/UPDATE,
	 *      RETURN-OLD for DELETE) -- it never cancels the write.  PG
	 *      requires BEFORE triggers to RETURN NEW to let the operation
	 *      continue; we already comply.
	 *
	 *   3. L1 row writes do NOT happen inside the trigfn.  trigfn
	 *      calls cagg_l1_batch_add(source_oid, ts), which only updates
	 *      a per-backend, per-xact (top_xid-keyed) min/max accumulator
	 *      in static memory.  The actual simple_heap_insert into
	 *      cagg_invalidation_log fires from cagg_l1_xact_callback at
	 *      XACT_EVENT_PRE_COMMIT -- i.e. *after* all per-row trigger
	 *      fires have completed, regardless of BEFORE/AFTER.  So L1
	 *      visibility ordering relative to the source INSERT is
	 *      identical in both modes.
	 *
	 *   4. Failed INSERTs (NOT NULL / CHECK / FK violations, or any
	 *      ereport in a later phase) abort the transaction.  Our
	 *      registered XACT_EVENT_ABORT callback DISCARDS the
	 *      accumulator (sets has_data = false) without writing L1.
	 *      A BEFORE trigger that fires before the constraint check
	 *      therefore does NOT pollute L1 on failed rows -- same final
	 *      state as AFTER which never fires for such rows.
	 *
	 *   5. SAVEPOINT/ROLLBACK TO inside a transaction: the
	 *      accumulator widens (timestamps from rolled-back subxacts
	 *      stay in min/max).  REFRESH later re-materializes the
	 *      widened range, which is correct because the rolled-back
	 *      rows are no longer visible to the materialization SELECT.
	 *      AFTER would have produced the same widened range -- both
	 *      use GetTopTransactionId() for keying.
	 *
	 *   6. DEFAULT and GENERATED column values are computed BEFORE
	 *      BEFORE-row triggers in PG14, so the bucket-time column
	 *      sees the same value either way (assuming nobody put the
	 *      bucket-time column under a GENERATED expression that
	 *      depends on a later AFTER-only trigger -- we don't support
	 *      that).
	 *
	 *   7. COPY / multi-row INSERT VALUES: PG fires the row trigger
	 *      per row in both modes; cagg_l1_batch_add coalesces them
	 *      into one L1 range either way.
	 *
	 * The only externally visible difference is pg_trigger.tgtype
	 * (BEFORE vs AFTER bit).  No regress or isolation2 assertion
	 * tests that field directly.
	 *
	 * == Alternatives considered ==
	 *
	 *   - Direct call from ts_heap_insert (bypass PG trigger
	 *     framework entirely): works, but duplicates the bookkeeping
	 *     PG already does for triggers (recursion guard, event-trigger
	 *     interaction, REPLICA ROLE handling, etc.) and diverges from
	 *     mainline.  Rejected.
	 *
	 *   - Encode forknum in CTID by stealing high bits of BlockNumber:
	 *     pervasive PG-core change (vacuum, FK validation, EvalPlanQual,
	 *     bitmap scan all dereference BlockNumber as physical).  High
	 *     risk for marginal benefit.  Rejected.
	 *
	 *   - Per-xact (CTID -> forknum) cache so ts_heap_fetch_row_version
	 *     can find the right fork: extra memory per insert; cache miss
	 *     still has the original ambiguity.  Rejected.
	 *
	 * BEFORE is the minimal, correct fix.
	 */
	/*
	 * Only fire on INSERT.  time_series source tables are append-only:
	 * ts_heap_tuple_delete / ts_heap_tuple_update (ts_tableam.c) ereport
	 * ERROR.  But if the trigger is registered for DELETE/UPDATE too, PG's
	 * ExecBRDeleteTriggers / ExecBRUpdateTriggers run BEFORE tableam, and
	 * the trigger.c:2572 assertion `HeapTupleIsValid(fdw_trigtuple) ^
	 * ItemPointerIsValid(tupleid)` fails on MPP segments -- debug build
	 * crashes the segment; release build is undefined behavior (the
	 * Assert is a no-op, then GetTupleForTrigger reads garbage at an
	 * invalid TID, potentially corrupting the L1 invalidation log).
	 *
	 * Restricting to INSERT keeps PG from entering the BEFORE-DELETE /
	 * BEFORE-UPDATE preparation path entirely, so DELETE/UPDATE bypass
	 * trigger framework and land cleanly at the tableam's ereport.
	 */
	appendStringInfo(&sql,
		"CREATE TRIGGER cagg_invalidation_trigger "
		"BEFORE INSERT ON %s.%s "
		"FOR EACH ROW EXECUTE FUNCTION time_series.cagg_invalidation_trigfn()",
		quote_identifier(info->source_schema),
		quote_identifier(info->source_table));

	SPI_execute(sql.data, false, 0);

	/*
	 * TRUNCATE invalidation is handled by the ProcessUtility hook
	 * (cagg_process_utility) which intercepts TruncateStmt and writes
	 * a full-range {-infinity, +infinity} L1 entry before the actual
	 * TRUNCATE executes.  No STATEMENT trigger needed.
	 */

	pfree(sql.data);
}

/*
 * Register CAGG in catalog
 */

static int
cagg_register_catalog(CaggCreateInfo *info)
{
	Oid		argtypes[8] = { TEXTOID, TEXTOID, TEXTOID, TEXTOID,
							OIDOID, INTERVALOID, NAMEOID, BOOLOID };
	Datum	args[8];
	int		ret;
	int		cagg_id;

	/*
	 * Reject "same source, different time column" CAGGs.
	 *
	 * cagg_invalidation_trigfn picks ONE bucket_column per source table
	 * (see cagg_get_time_attnum: scans continuous_agg, takes the first
	 * matching CAGG, breaks).  If a second CAGG on the same source uses
	 * a different time column, the trigger only records dirty intervals
	 * by the FIRST CAGG's column -- the second CAGG's dirty buckets are
	 * misaligned and back-filled rows in those buckets are never
	 * materialized, leading to silent under-counts in user queries.
	 *
	 * We could in principle fix the trigger to record dirty intervals
	 * for every column at once, but: (a) the use case (two CAGGs on the
	 * same source aggregating by different time columns) is a rare
	 * anti-pattern, (b) upstream own architecture prevents this via its
	 * single hypertable time-partitioning column.  Reject at create time
	 * with a clear message -- much better than silently-wrong aggregates.
	 */
	{
		Oid		chk_argtypes[2] = { OIDOID, NAMEOID };
		Datum	chk_args[2];
		int		chk_ret;

		chk_args[0] = ObjectIdGetDatum(info->source_relid);
		chk_args[1] = DirectFunctionCall1(namein,
										  CStringGetDatum(info->bucket_column));

		chk_ret = SPI_execute_with_args(
			"SELECT user_view_name, bucket_column "
			"  FROM time_series.continuous_agg "
			" WHERE source_table_oid = $1 "
			"   AND bucket_column != $2 "
			" LIMIT 1",
			2, chk_argtypes, chk_args, NULL, true, 1);

		if (chk_ret == SPI_OK_SELECT && SPI_processed > 0)
		{
			bool	isnull;
			/* both columns are NAME type, not TEXT */
			char   *existing_view = NameStr(*DatumGetName(
				SPI_getbinval(SPI_tuptable->vals[0],
							  SPI_tuptable->tupdesc, 1, &isnull)));
			char   *existing_col = NameStr(*DatumGetName(
				SPI_getbinval(SPI_tuptable->vals[0],
							  SPI_tuptable->tupdesc, 2, &isnull)));

			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot create continuous aggregate using time "
							"column \"%s\" on source table \"%s.%s\"",
							info->bucket_column,
							info->source_schema, info->source_table),
					 errdetail("Existing continuous aggregate \"%s\" on this "
							   "source already uses time column \"%s\".",
							   existing_view, existing_col),
					 errhint("All continuous aggregates on the same source "
							 "table must use the same time column.  Drop the "
							 "existing CAGG or use the same column.")));
		}
	}

	args[0] = CStringGetTextDatum(info->user_view_schema);
	args[1] = CStringGetTextDatum(info->user_view_name);
	args[2] = CStringGetTextDatum(info->source_schema);
	args[3] = CStringGetTextDatum(info->source_table);
	args[4] = ObjectIdGetDatum(info->source_relid);
	args[5] = IntervalPGetDatum(info->bucket_width);
	args[6] = DirectFunctionCall1(namein,
								  CStringGetDatum(info->bucket_column));
	args[7] = BoolGetDatum(info->materialized_only);

	ret = SPI_execute_with_args(
		"INSERT INTO time_series.continuous_agg "
		"(user_view_schema, user_view_name, source_table_schema, "
		"source_table_name, source_table_oid, bucket_width, "
		"bucket_column, materialized_only) "
		"VALUES ($1, $2, $3, $4, $5, $6, $7, $8) "
		"RETURNING cagg_id",
		8, argtypes, args, NULL, false, 0);

	if (ret != SPI_OK_INSERT_RETURNING || SPI_processed != 1)
		ereport(ERROR,
				(errmsg("failed to register continuous aggregate in catalog")));

	{
		bool isnull;
		cagg_id = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0],
											  SPI_tuptable->tupdesc, 1,
											  &isnull));
	}

	return cagg_id;
}

/*
 * Hook registration (called from _PG_init)
 */

void
ht_cagg_init(void)
{
	prev_ProcessUtility = ProcessUtility_hook;
	ProcessUtility_hook = cagg_process_utility;
}

/* ============================================================
 * Continuous-aggregate GUC variables
 *
 * Defined here so all CAGG-owned knobs live next to the code
 * that consumes them.  cagg_define_gucs() is called from
 * _PG_init.
 * ============================================================ */

/* GUC: max individual materializations per REFRESH call */
int			guc_materializations_per_refresh_window = 10;

/*
 * GUC: time_series.enable_cagg_create
 *
 * Master toggle for the CREATE MATERIALIZED VIEW ... WITH
 * (time_series.continuous) handler.  When off, attempts to create a
 * new CAGG are rejected with a clear error.  Pre-existing CAGGs are
 * unaffected -- refresh / select / DROP all continue to work.
 *
 * Use case: during maintenance / migration windows, lock the schema
 * to prevent new CAGGs from being added while old ones are being
 * migrated.  Mirrors the upstream extension's GUC of the same name.
 */
bool		guc_enable_cagg_create = true;

void
cagg_define_gucs(void)
{
	DefineCustomIntVariable("time_series.materializations_per_refresh_window",
							"Max number of individual refreshes per REFRESH call",
							"If more intervals need to be refreshed, they are "
							"merged into a single large refresh to avoid "
							"excessive fragmented I/O.",
							&guc_materializations_per_refresh_window,
							10, /* default */
							0,	/* min (0 = unlimited) */
							INT_MAX,
							PGC_USERSET,
							0,
							NULL, NULL, NULL);

	DefineCustomBoolVariable("time_series.enable_cagg_create",
							 "Enable creation of new continuous aggregates",
							 "When off, CREATE MATERIALIZED VIEW WITH "
							 "(time_series.continuous) is rejected.  Pre-existing "
							 "CAGGs (refresh / select / DROP) are unaffected.  "
							 "Useful during maintenance windows to lock the schema.",
							 &guc_enable_cagg_create,
							 true,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);
}
