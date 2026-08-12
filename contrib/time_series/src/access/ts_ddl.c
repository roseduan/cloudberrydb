/*-------------------------------------------------------------------------
 *
 * ts_ddl.c
 *    ProcessUtility hook for time_series DDL validation and cleanup,
 *    relation_size hook, and ts_chunk_info SRF.
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 * IDENTIFICATION
 *    contrib/time_series/src/access/ts_ddl.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/stat.h>

#include "access/reloptions.h"
#include "access/table.h"
#include "access/tableam.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "catalog/namespace.h"
#include "commands/defrem.h"
#include "common/relpath.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodes.h"
#include "storage/smgr.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/timestamp.h"

#include "cdb/cdbvars.h"
#include "port.h"

#include "../include/time_series.h"
#include "../include/access/ts_tableam.h"
#include "../include/compress/ts_compress.h"

/* Saved pointer for chaining the ProcessUtility hook. */
static ProcessUtility_hook_type prev_ProcessUtility = NULL;

/* Saved pointer for chaining the relation_size hook. */
static relation_size_hook_type prev_relation_size_hook = NULL;

/* Saved pointer for chaining the object_access hook. */
static object_access_hook_type prev_object_access_hook = NULL;

/*
 * ts_process_utility
 *		Post-creation validation for time_series tables.  After the standard ProcessUtility has
 *		executed the CREATE TABLE, we re-open the newly created relation and verify:
 *		  - ts_partition_column exists and is a temporal type
 *		  - ts_chunk_interval parses to a positive value
 *
 *		This runs as a post-hook (execute first, validate after) so that the relation and
 *		its reloptions are already committed to the catalog when we inspect them.
 */
static void
ts_process_utility(PlannedStmt *pstmt,
				   const char *queryString,
				   bool readOnlyTree,
				   ProcessUtilityContext context,
				   ParamListInfo params,
				   QueryEnvironment *queryEnv,
				   DestReceiver *dest,
				   QueryCompletion *qc)
{
	Node	   *parsetree = pstmt->utilityStmt;
	bool		is_ts_create = false;
	bool		ts_active = extension_is_loaded_and_not_upgrading();

	/*
	 * ts_active gates every time_series-specific block below.  Hooks
	 * outlive DROP EXTENSION CASCADE / pg_upgrade --binary-upgrade,
	 * so without this guard the ALTER / RENAME guards (which can
	 * probe extension catalogs via ts_chunk_catalog_has_any,
	 * ts_compressed_chunk_has_any) could blow up after the catalog
	 * tables have been torn down.  In practice the existing
	 * RelationIsTimeSeries() check renders that path unreachable
	 * today — but the guard is the convention the comment block in
	 * time_series.h mandates, and matches what ts_object_access does.
	 *
	 * Chaining to prev_ProcessUtility / standard_ProcessUtility is
	 * NEVER skipped — only the ts-specific validation is.
	 */

	/* Detect CREATE TABLE ... USING time_series before executing */
	if (ts_active && nodeTag(parsetree) == T_CreateStmt)
	{
		CreateStmt *stmt = (CreateStmt *) parsetree;

		if (stmt->accessMethod != NULL &&
			strcmp(stmt->accessMethod, "time_series") == 0)
			is_ts_create = true;
	}

	/*
	 * NOTE: drop cleanup (catalog rows + PAX dir registration) lives in
	 * ts_object_access (OAT_DROP) below, not here.  Using object_access_hook
	 * instead of ProcessUtility_hook catches CASCADE drops — DROP SCHEMA
	 * CASCADE, DROP TABLE foo CASCADE that propagates to time_series
	 * children, etc. — which never re-enter ProcessUtility for the
	 * cascaded relations.
	 */

	/*
	 * Block dangerous ALTER TABLE subcommands on time_series tables.
	 * Must run BEFORE execution to prevent irreversible damage.
	 */
	if (ts_active && nodeTag(parsetree) == T_AlterTableStmt)
	{
		AlterTableStmt *atstmt = (AlterTableStmt *) parsetree;
		Oid				atrelid;

		atrelid = RangeVarGetRelid(atstmt->relation, AccessShareLock, true);
		if (OidIsValid(atrelid))
		{
			Relation	atrel = table_open(atrelid, AccessShareLock);

			if (RelationIsTimeSeries(atrel))
			{
				TSRelOptions   *opts = (TSRelOptions *) atrel->rd_options;
				const char	   *part_col = (const char *) opts + opts->ts_column;
				ListCell	   *lc;

				foreach(lc, atstmt->cmds)
				{
					AlterTableCmd  *cmd = lfirst_node(AlterTableCmd, lc);

					switch (cmd->subtype)
					{
						case AT_DropColumn:
						case AT_DropColumnRecurse:
							if (cmd->name != NULL &&
								strcmp(cmd->name, part_col) == 0)
							{
								table_close(atrel, AccessShareLock);
								ereport(ERROR,
										(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
										 errmsg("cannot drop partition column \"%s\" of a time_series table",
												cmd->name)));
							}
							break;

						case AT_AlterColumnType:
							if (cmd->name != NULL &&
								strcmp(cmd->name, part_col) == 0)
							{
								table_close(atrel, AccessShareLock);
								ereport(ERROR,
										(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
										 errmsg("cannot alter type of partition column \"%s\" on a time_series table",
												cmd->name)));
							}

							/*
							 * Non-partition columns: the PAX on-disk
							 * byte format is per-column type-specialised
							 * (Gorilla for floats, delta-delta for ints,
							 * dictionary/ZSTD for varlena).  Once a
							 * chunk is compressed, changing the column
							 * type would silently invalidate the encoded
							 * stream — reads would either decode garbage
							 * or segfault.  Block as long as any
							 * compressed chunk exists.
							 */
							if (ts_compressed_chunk_has_any(atrelid))
							{
								table_close(atrel, AccessShareLock);
								ereport(ERROR,
										(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
										 errmsg("cannot alter type of column \"%s\" on a time_series table with compressed chunks",
												cmd->name ? cmd->name : "?"),
										 errhint("Drop or recreate the table; "
												 "time_series v1 has no in-place decompress path.")));
							}
							break;

						case AT_SetRelOptions:
						case AT_ResetRelOptions:
							{
								/*
								 * Routing-defining reloptions
								 * (ts_partition_column, ts_chunk_interval,
								 * ts_chunk_origin) determine which fork
								 * an INSERT lands in.  Changing them
								 * after chunks exist would route new
								 * rows to different forks than the
								 * pre-existing rows, splitting one
								 * logical time range across two storage
								 * locations.  Allowed only on tables
								 * with zero chunks.
								 */
								List	   *def = (List *) cmd->def;
								ListCell   *defcell;
								const char *bad_opt = NULL;

								foreach(defcell, def)
								{
									DefElem *de = lfirst_node(DefElem, defcell);

									if (de->defnamespace != NULL)
										continue;
									if (strcmp(de->defname, "ts_partition_column") == 0 ||
										strcmp(de->defname, "ts_chunk_interval") == 0 ||
										strcmp(de->defname, "ts_chunk_origin") == 0)
									{
										bad_opt = de->defname;
										break;
									}
								}

								if (bad_opt != NULL && ts_chunk_catalog_has_any(atrelid))
								{
									table_close(atrel, AccessShareLock);
									ereport(ERROR,
											(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
											 errmsg("cannot change time_series option \"%s\" on a table with existing chunks",
													bad_opt),
											 errhint("This option is routing-sensitive; "
													 "new rows would land in different chunks than existing rows.")));
								}
							}
							break;

						case AT_SetAccessMethod:
							table_close(atrel, AccessShareLock);
							ereport(ERROR,
									(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									 errmsg("cannot change access method of a time_series table")));
							break;

						case AT_AddInherit:
							table_close(atrel, AccessShareLock);
							ereport(ERROR,
									(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									 errmsg("cannot add inheritance to a time_series table")));
							break;

						default:
							break;
					}
				}
			}
			table_close(atrel, AccessShareLock);
		}
	}

	/*
	 * Block RENAME COLUMN on the partition column of time_series tables.
	 */
	if (ts_active && nodeTag(parsetree) == T_RenameStmt)
	{
		RenameStmt *rstmt = (RenameStmt *) parsetree;

		if (rstmt->renameType == OBJECT_COLUMN && rstmt->relation != NULL)
		{
			Oid		rnrelid;

			rnrelid = RangeVarGetRelid(rstmt->relation, AccessShareLock, true);
			if (OidIsValid(rnrelid))
			{
				Relation	rnrel = table_open(rnrelid, AccessShareLock);

				if (RelationIsTimeSeries(rnrel))
				{
					TSRelOptions   *opts = (TSRelOptions *) rnrel->rd_options;
					const char	   *part_col = (const char *) opts + opts->ts_column;

					if (rstmt->subname != NULL &&
						strcmp(rstmt->subname, part_col) == 0)
					{
						table_close(rnrel, AccessShareLock);
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("cannot rename partition column \"%s\" of a time_series table",
										rstmt->subname)));
					}
				}
				table_close(rnrel, AccessShareLock);
			}
		}
	}

	/* Execute the command */
	if (prev_ProcessUtility)
		prev_ProcessUtility(pstmt, queryString, readOnlyTree, context, params, queryEnv, dest, qc);
	else
		standard_ProcessUtility(pstmt, queryString, readOnlyTree,
								context, params, queryEnv, dest, qc);

	/* Validate time_series table after creation */
	if (is_ts_create)
	{
		CreateStmt *stmt = (CreateStmt *) parsetree;
		RangeVar   *rv;
		Relation	rel;
		TSRelOptions *opts;
		const char *col_name;
		const char *interval_str;
		TupleDesc	tupdesc;
		int			i;
		bool		found = false;

		rv = makeRangeVar(stmt->relation->schemaname, stmt->relation->relname, -1);
		rel = table_openrv(rv, AccessShareLock);
		opts = (TSRelOptions *) rel->rd_options;

		if (opts == NULL || opts->ts_column == 0)
		{
			table_close(rel, AccessShareLock);
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("time_series tables require ts_partition_column option")));
		}

		col_name = (const char *) opts + opts->ts_column;
		tupdesc = RelationGetDescr(rel);

		for (i = 0; i < tupdesc->natts; i++)
		{
			Form_pg_attribute att = TupleDescAttr(tupdesc, i);

			if (att->attisdropped)
				continue;
			if (strcmp(NameStr(att->attname), col_name) == 0)
			{
				Oid		typid = att->atttypid;

				if (typid != TIMESTAMPTZOID && typid != TIMESTAMPOID && typid != DATEOID)
				{
					table_close(rel, AccessShareLock);
					ereport(ERROR,
							(errcode(ERRCODE_DATATYPE_MISMATCH),
							 errmsg("ts_partition_column \"%s\" must be timestamptz, "
									"timestamp, or date", col_name)));
				}
				found = true;
				break;
			}
		}

		if (!found)
		{
			table_close(rel, AccessShareLock);
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("ts_partition_column \"%s\" does not exist in table",
							col_name)));
		}

		if (opts->ts_interval != 0)
		{
			interval_str = (const char *) opts + opts->ts_interval;

			if (ts_parse_interval_usec(interval_str) <= 0)
			{
				table_close(rel, AccessShareLock);
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("ts_chunk_interval must be a positive interval")));
			}
		}

		table_close(rel, AccessShareLock);
	}
}

/*
 * ts_relation_size_hook
 *		Ensure pg_relation_size() uses the table AM callback for time_series
 *		tables.
 *
 *		relation_size_hook is a "value-returning" hook: exactly one
 *		registrant is expected to actually answer for a given
 *		relation, unlike ProcessUtility_hook / object_access_hook
 *		where every registrant fires unconditionally.  The relation-
 *		kind checks below MUST run before delegating to
 *		prev_relation_size_hook -- other extensions can register the
 *		same hook (e.g. contrib/unionstore_ext's neon_relation_size,
 *		loaded first when both are in shared_preload_libraries).  Its
 *		own hook body checks "is this relation mine?" before
 *		delegating further down the chain; if we unconditionally
 *		delegated whenever a prev hook exists, every time_series (and
 *		every non-block-AM) table's pg_relation_size() would route to
 *		that OTHER extension's fallback stat()-based path, which stats
 *		only the MAIN fork -- always 0 for a time_series table, since
 *		its data lives entirely in chunk forks past MAX_FORKNUM.
 */
static int64
ts_relation_size(Relation rel, ForkNumber forknum)
{
	if (RelationIsTimeSeries(rel))
		return (int64) table_relation_size(rel, forknum);

	if (RelationIsNonblockRelation(rel))
		return (int64) table_relation_size(rel, forknum);

	if (prev_relation_size_hook)
		return prev_relation_size_hook(rel, forknum);

	/* Replicate stat-based calculation for plain heap */
	{
		int64			totalsize = 0;
		char		   *relationpath;
		char			pathname[MAXPGPATH];
		unsigned int	segcount;

		relationpath = relpathbackend(rel->rd_node, rel->rd_backend, forknum);

		for (segcount = 0;; segcount++)
		{
			struct stat fst;

			CHECK_FOR_INTERRUPTS();

			if (segcount == 0)
				snprintf(pathname, MAXPGPATH, "%s", relationpath);
			else
				snprintf(pathname, MAXPGPATH, "%s.%u", relationpath, segcount);

			if (stat(pathname, &fst) < 0)
			{
				if (errno == ENOENT)
					break;
				else
					ereport(ERROR,
							(errcode_for_file_access(),
							 errmsg("could not stat file \"%s\": %m", pathname)));
			}
			totalsize += fst.st_size;
		}

		pfree(relationpath);
		return totalsize;
	}
}

/*
 *		object_access hook: clean up time_series state for any DROP path
 *
 *		Fires for every relation drop regardless of how it got there:
 *		top-level DROP TABLE, DROP TABLE CASCADE that pulls children,
 *		DROP SCHEMA CASCADE, DROP OWNED BY, etc.  ProcessUtility_hook
 *		only sees the top-level statement and misses every cascaded
 *		drop, so DROP SCHEMA on a schema containing a compressed
 *		time_series table used to orphan the PAX dir and leave
 *		ts_compressed_chunk / ts_compress_config / ts_chunk rows
 *		pointing at the deleted relid.
 *
 *		The hook runs BEFORE catalog deletion, so the rel is still
 *		open in relcache.  We hold NoLock — the dropping xact already
 *		owns AccessExclusiveLock on the rel, so its relcache entry
 *		is exclusively ours.
 */
static void
ts_object_access(ObjectAccessType access, Oid classId, Oid objectId,
				 int subId, void *arg)
{
	Relation	rel;
	bool		is_ts;
	char		relkind;

	if (prev_object_access_hook)
		prev_object_access_hook(access, classId, objectId, subId, arg);

	if (access != OAT_DROP)
		return;
	if (classId != RelationRelationId || subId != 0)
		return;

	/*
	 * DROP EXTENSION CASCADE iterates dependents in dependency order
	 * and frequently drops our catalog tables (time_series.ts_chunk,
	 * ts_compressed_chunk, ts_compress_config) BEFORE the user
	 * time_series tables.  By the time the cascade reaches a user
	 * relation, our catalog tables may no longer exist — the cached
	 * OIDs would then ERROR with "could not open relation with OID N".
	 *
	 * extension_is_loaded_and_not_upgrading() returns false once the
	 * extension's proxy table has been invalidated, which happens
	 * before the catalog tables are deleted.  Skipping cleanup in
	 * that case is correct: the cascade itself will wipe everything,
	 * and the PAX dir is going through the deferred-rmtree path
	 * which we shouldn't queue while the extension is being torn
	 * down (the xact callback may run after the extension's hooks
	 * are removed).
	 *
	 * Same comment applies to pg_upgrade --binary-upgrade; the guard
	 * mirrors what every other hook in this extension already uses.
	 */
	if (!extension_is_loaded_and_not_upgrading())
		return;

	/*
	 * RelationRelationId covers pg_class as a whole — also fires for
	 * indexes, sequences, toast tables, views, partitioned-table
	 * parents, etc.  Filter to ordinary tables before any table_open,
	 * because try_table_open errors on indexes ("X is an index").
	 * get_rel_relkind goes through syscache and is cheap.
	 */
	relkind = get_rel_relkind(objectId);
	if (relkind != RELKIND_RELATION)
		return;

	/*
	 * try_table_open with NoLock: we're inside the drop's
	 * AccessExclusiveLock so no concurrent access; if the relation
	 * has somehow vanished from relcache already, bail out quietly.
	 */
	rel = try_table_open(objectId, NoLock, false);
	if (rel == NULL)
		return;

	is_ts = RelationIsTimeSeries(rel);
	table_close(rel, NoLock);

	if (!is_ts)
		return;

	/*
	 * Catalog rows live in extension-owned tables; safe to delete
	 * even when the drop comes via CASCADE (catalog DML is
	 * transactional, rolls back with the outer xact).
	 */
	ts_chunk_catalog_delete(objectId);
	ts_compress_config_delete(objectId);
	ts_compressed_chunk_delete(objectId);

	/*
	 * PAX sidecar dir removal defers to xact COMMIT via the pending
	 * removals list (see ts_pax_register_pending_removal), so a
	 * ROLLBACK of the outer xact leaves the dir intact.
	 */
	ts_pax_register_pending_removal(MyDatabaseId, objectId, true);
}

/*
 * ts_hooks_init
 *		Install ProcessUtility, relation_size, and object_access hooks.
 *		Called from _PG_init().
 */
void
ts_hooks_init(void)
{
	prev_ProcessUtility = ProcessUtility_hook;
	ProcessUtility_hook = ts_process_utility;

	prev_relation_size_hook = relation_size_hook;
	relation_size_hook = ts_relation_size;

	prev_object_access_hook = object_access_hook;
	object_access_hook = ts_object_access;
}
