/*-------------------------------------------------------------------------
 *
 * pg_iceberg_time_travel.c
 *    Iceberg time travel via a record-returning SRF.
 *
 *    iceberg_snapshot_scan(regclass, snapshot_id bigint) reads a historical
 *    snapshot of an Iceberg table.  The result column set is resolved at
 *    parse analysis by a DESCRIBE callback (iceberg_snapshot_describe); the
 *    data files for the snapshot are resolved on the QD by the planner hook
 *    and shipped to every segment through a hidden third argument; each QE
 *    reads its shard by reusing the Iceberg AM's volume-fdw reader.
 *
 *    Increment 1 (this file): reads the current (HEAD) snapshot -- schema
 *    from the live relation, fragments from the current metadata.  Snapshot
 *    selection by snapshot_id is layered on once the agent understands the
 *    snapshot-id request (Phase 1).
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 * IDENTIFICATION
 *        contrib/datalake_fdw/src/am_iceberg/pg_iceberg_time_travel.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/relation.h"
#include "access/table.h"
#include "access/tupdesc.h"
#include "access/xact.h"
#include "catalog/pg_language.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "executor/executor.h"
#include "executor/tuptable.h"
#include "fmgr.h"
#include "foreign/fdwapi.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "nodes/extensible.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/plannodes.h"
#include "nodes/pathnodes.h"
#include "nodes/primnodes.h"
#include "nodes/supportnodes.h"
#include "optimizer/optimizer.h"
#include "optimizer/plancat.h"
#include "optimizer/planner.h"
#include "parser/analyze.h"
#include "parser/parse_func.h"
#include "parser/parsetree.h"
#include "rewrite/rewriteManip.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/resowner.h"
#include "utils/rls.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"

#include "cdb/cdbvars.h"

#include "include/pg_iceberg_am.h"
#include "include/pg_iceberg_catalog.h"
#include "include/pg_iceberg_catalog_utils.h"
#include "../dlproxy/iceberg_common.h"

PG_FUNCTION_INFO_V1(iceberg_snapshot_describe);
PG_FUNCTION_INFO_V1(iceberg_snapshot_scan);
PG_FUNCTION_INFO_V1(iceberg_snapshot_scan_support);
PG_FUNCTION_INFO_V1(iceberg_snapshot_list_json);

/*
 * Reject Var/Param/SubLink anywhere in the argument expression before folding
 * it to a constant: the core ExecIsExprUnsafeToConst check only inspects the
 * top node, so a nested Var (e.g. a lateral reference wrapped in an implicit
 * cast) would slip through and crash evaluate_expr at parse time (issue #406).
 */
static bool
tt_arg_unsafe_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Var) || IsA(node, Param) ||
		IsA(node, SubLink) || IsA(node, SubPlan))
		return true;
	return expression_tree_walker(node, tt_arg_unsafe_walker, context);
}

/*
 * True if `arg` cannot serve as a parse-time constant for time travel.
 *
 * Two distinct hazards are rejected together:
 *   - Var/Param/SubLink/SubPlan crash evaluate_expr when folded at parse time
 *     (core's ExecIsExprUnsafeToConst inspects only the top node -- issue #406);
 *     tt_arg_unsafe_walker catches those recursively.
 *   - A volatile or stable argument (random(), now(), anything reading external
 *     state) folds to a DIFFERENT value at each site that independently
 *     const-folds it -- the describe callback, the relation-rewrite gate and
 *     the FunctionScan planner hook -- so the snapshot id, the resolved schema
 *     and the fragment list could disagree, silently mixing snapshots.  Require
 *     an immutable (deterministically foldable) expression.  A literal, a
 *     literal cast, or an immutable operator over literals all pass; the common
 *     'schema.table'::regclass / bigint-literal calls fold to a plain Const and
 *     never reach contain_mutable_functions.
 */
static bool
tt_arg_not_constant(Node *arg)
{
	return tt_arg_unsafe_walker(arg, NULL) || contain_mutable_functions(arg);
}

/*
 * Same as tt_arg_not_constant, but for the SNAPSHOT SELECTOR argument, which is
 * allowed to be time-dependent.
 *
 * `iceberg_snapshot_scan(t, now() - interval '1 day')` is the most natural way
 * to ask for a historical read, and the equivalent is accepted by Spark
 * (`TIMESTAMP AS OF current_timestamp() - INTERVAL 1 DAY`); rejecting it left
 * the user no choice but to look a literal up first.  The blanket
 * contain_mutable_functions() test used for the table argument is too strong
 * here because it lumps STABLE in with VOLATILE:
 *
 *   - STABLE is by definition constant for the whole statement, so the sites
 *     that independently const-fold this argument (the describe callback, the
 *     rewrite gate, the planner hook, the prosupport function) still agree.
 *     Across re-planning of a *cached* parse tree they would not -- which is
 *     why the resolved instant is additionally frozen into a literal, see
 *     tt_freeze_selector_arg.
 *   - VOLATILE (random(), clock_timestamp()) genuinely differs between those
 *     sites and would silently mix snapshots; it stays rejected.
 *
 * This lands on the same rule Spark enforces: deterministic accepted,
 * non-deterministic rejected.  The Var/Param/SubLink rejection is unrelated to
 * volatility -- those crash evaluate_expr at parse time (issue #406) -- and
 * stays in force.
 */
static bool
tt_selector_arg_not_constant(Node *arg)
{
	return tt_arg_unsafe_walker(arg, NULL) || contain_volatile_functions(arg);
}

/*
 * True if `funcid` is this extension's iceberg_snapshot_scan C function.
 *
 * The planner hook and the relation rewrite must act only on our function, not
 * on a user/temp function that merely shares the unqualified name
 * "iceberg_snapshot_scan": rewriting an unrelated call into a relation scan --
 * or indexing its argument list past its length (lsecond on a one-argument
 * impostor) -- would corrupt the plan or crash the backend.  Identify our
 * function by its C language plus internal symbol name, which a SQL/PL/temp
 * function cannot match; matching by name alone (get_func_name) cannot tell
 * them apart.
 */
static bool
tt_is_snapshot_scan_func(Oid funcid)
{
	HeapTuple	tup;
	Form_pg_proc pform;
	bool		result = false;

	tup = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
	if (!HeapTupleIsValid(tup))
		return false;
	pform = (Form_pg_proc) GETSTRUCT(tup);
	if (pform->prolang == ClanguageId)
	{
		bool	isnull;
		Datum	prosrc = SysCacheGetAttr(PROCOID, tup,
										 Anum_pg_proc_prosrc, &isnull);

		if (!isnull &&
			strcmp(TextDatumGetCString(prosrc), "iceberg_snapshot_scan") == 0)
		{
			/*
			 * prosrc alone would also match a same-named symbol from another
			 * C library; require this extension's module too.  probin is
			 * "$libdir/datalake_fdw" as installed -- compare the last path
			 * component so an absolute-path variant still matches.
			 */
			Datum	probin = SysCacheGetAttr(PROCOID, tup,
											 Anum_pg_proc_probin, &isnull);

			if (!isnull)
			{
				const char *bin = TextDatumGetCString(probin);
				const char *base = strrchr(bin, '/');

				base = base ? base + 1 : bin;
				if (strcmp(base, "datalake_fdw") == 0 ||
					strcmp(base, "datalake_fdw.so") == 0)
					result = true;
			}
		}
	}
	ReleaseSysCache(tup);
	return result;
}

static bool tt_is_iceberg_relation(Oid relid);
static int64 tt_snapshot_id_from_arg(FuncExpr *fexpr, Oid relid);
static char *tt_build_fieldid_csv(Relation rel, int64 snapshot_id);
static int *tt_parse_fieldid_csv(const char *csv, int *nfids);

/*
 * Run fn(arg) under a subtransaction.  Returns true when it completed, false
 * when its error was swallowed.
 *
 * Both callers are cardinality estimation, where reaching the catalog service
 * is best-effort: an agent that predates an operation, or a transient outage,
 * must cost the query its estimate and nothing more.  A cancel, a shutdown
 * request or an out-of-memory condition is not a statistics problem and is
 * re-thrown unchanged.
 *
 * A bare PG_CATCH would leave the failed attempt's locks, relcache references
 * and resource-owner state behind; rolling back a subtransaction releases them
 * (same reasoning as pg_iceberg_av_consumer.c).
 */
static bool
tt_run_guarded(void (*fn) (void *), void *arg)
{
	MemoryContext	oldcontext = CurrentMemoryContext;
	ResourceOwner	oldowner = CurrentResourceOwner;
	volatile bool	ok = true;

	BeginInternalSubTransaction(NULL);
	MemoryContextSwitchTo(oldcontext);

	PG_TRY();
	{
		fn(arg);

		ReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(oldcontext);
		CurrentResourceOwner = oldowner;
	}
	PG_CATCH();
	{
		ErrorData  *edata;
		bool		rethrow;

		MemoryContextSwitchTo(oldcontext);
		CurrentResourceOwner = oldowner;
		edata = CopyErrorData();
		rethrow = (edata->sqlerrcode == ERRCODE_QUERY_CANCELED ||
				   edata->sqlerrcode == ERRCODE_ADMIN_SHUTDOWN ||
				   edata->sqlerrcode == ERRCODE_OUT_OF_MEMORY);

		FlushErrorState();
		RollbackAndReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(oldcontext);
		CurrentResourceOwner = oldowner;

		if (rethrow)
			ReThrowError(edata);

		ereport(DEBUG1,
				(errmsg("iceberg time travel: could not read snapshot statistics, "
						"falling back to the default cardinality estimate"),
				 errdetail("%s", edata->message)));
		FreeErrorData(edata);
		ok = false;
	}
	PG_END_TRY();

	return ok;
}

/*
 * Estimate the row count of `snapshot_id` for `relid` by summing the per-data-
 * file record counts of the snapshot's fragment list -- the same list the scan
 * will read, so the estimate matches what is actually sharded across the
 * segments.  It is a gross count (equality/position deletes are not
 * subtracted), which is acceptable for a planner cardinality estimate.
 * Returns 0 when the fragments cannot be resolved so the caller falls back to
 * the default.
 */
static double
tt_estimate_snapshot_rows(Oid relid, int64 snapshot_id)
{
	Relation	rel;
	char	   *fragments;
	List	   *parsed;
	ListCell   *lc;
	double		total = 0;

	rel = table_open(relid, AccessShareLock);
	fragments = pg_iceberg_list_data_fragments_json(rel, snapshot_id);
	table_close(rel, AccessShareLock);

	if (fragments == NULL)
		return 0;

	parsed = parseIcebergFragmentResponse(fragments, strlen(fragments));

	/*
	 * parsed = [ExternalTableMetadata, combinedTask List, ...]; each
	 * combinedTask is a List of FileScanTask.  IsA(entry, List) skips the
	 * metadata head (mirrors pg_iceberg_am.c's iteration of the same form).
	 */
	foreach(lc, parsed)
	{
		Node	   *entry = (Node *) lfirst(lc);
		ListCell   *lt;

		if (entry == NULL || !IsA(entry, List))
			continue;
		foreach(lt, (List *) entry)
		{
			FileScanTask *task = (FileScanTask *) lfirst(lt);

			if (task == NULL || !IsA(task, FileScanTask) ||
				task->dataFile == NULL)
				continue;
			total += (double) task->dataFile->recordCount;
		}
	}

	return total;
}

/* tt_run_guarded payload for tt_estimate_snapshot_rows. */
typedef struct TTEstimateCtx
{
	Oid			relid;
	int64		snapshot_id;
	double		rows;			/* left at 0 when the fetch fails */
} TTEstimateCtx;

static void
tt_estimate_snapshot_rows_cb(void *arg)
{
	TTEstimateCtx *ctx = (TTEstimateCtx *) arg;

	ctx->rows = tt_estimate_snapshot_rows(ctx->relid, ctx->snapshot_id);
}

/*
 * Planner support function for iceberg_snapshot_scan (SupportRequestRows).
 *
 * The schema-changed fallback runs as a FunctionScan; without a row estimate
 * the planner assumes the default 1000 rows, so a join over a historical
 * snapshot gets a poor join order (build/probe side, motion type).  Supply the
 * snapshot's real row count -- resolved from the constant snapshot_id argument
 * on the QD during planning -- so the estimate reflects the data.  Only
 * constant arguments are handled; anything else returns NULL to keep the
 * default (the arguments are already required to be constant elsewhere).
 */
Datum
iceberg_snapshot_scan_support(PG_FUNCTION_ARGS)
{
	Node	   *rawreq = (Node *) PG_GETARG_POINTER(0);
	Node	   *ret = NULL;

	if (IsA(rawreq, SupportRequestRows))
	{
		SupportRequestRows *req = (SupportRequestRows *) rawreq;
		FuncExpr   *fexpr = (FuncExpr *) req->node;

		if (fexpr != NULL && IsA(fexpr, FuncExpr) &&
			list_length(fexpr->args) >= 2 &&
			!tt_arg_not_constant((Node *) linitial(fexpr->args)) &&
			!tt_selector_arg_not_constant((Node *) lsecond(fexpr->args)))
		{
			bool	isnull;
			Oid		relid;

			relid = DatumGetObjectId(ExecEvalFunctionArgToConst(fexpr, 0, &isnull));
			if (!isnull && tt_is_iceberg_relation(relid))
			{
				TTEstimateCtx	ctx;
				double			rows;

				ctx.relid = relid;
				ctx.snapshot_id = tt_snapshot_id_from_arg(fexpr, relid);
				ctx.rows = 0;

				/*
				 * Guarded: this is only an estimate, so an unreachable catalog
				 * service must leave the planner on its default rather than
				 * abort the statement.
				 */
				(void) tt_run_guarded(tt_estimate_snapshot_rows_cb, &ctx);
				rows = ctx.rows;
				if (rows > 0)
				{
					int		nsegs = getgpsegmentCount();

					/*
					 * The function is EXECUTE ON ALL SEGMENTS, so the planner
					 * reads this estimate as rows-per-segment and multiplies by
					 * the segment count at the Gather.  The fragment list is
					 * sharded across segments (idx % numSegments), so report the
					 * per-segment share of the snapshot's total rows; the Gather
					 * then reconstructs the true total.
					 */
					req->rows = (nsegs > 0) ? rows / nsegs : rows;
					ret = (Node *) req;
				}
			}
		}
	}

	PG_RETURN_POINTER(ret);
}

/* True if relid is an Iceberg-AM relation. */
static bool
tt_is_iceberg_relation(Oid relid)
{
	Relation	rel;
	bool		result;

	if (!OidIsValid(relid))
		return false;
	rel = try_relation_open(relid, AccessShareLock, false);
	if (rel == NULL)
		return false;
	result = (rel->rd_rel->relkind == RELKIND_RELATION &&
			  rel->rd_rel->relam == get_am_oid("iceberg", true));
	relation_close(rel, AccessShareLock);
	return result;
}

/*
 * Reverse type mapping: Iceberg primitive type name (col.type().toString())
 * -> PostgreSQL type Oid + typmod.  Used to build the historical tuple
 * descriptor for a snapshot whose schema differs from the current relation.
 *
 *   iceberg          -> pg              notes
 *   ---------------------------------------------------------------------
 *   int              -> int4
 *   long             -> int8
 *   string           -> text           LOSSY (see below)
 *   boolean          -> bool
 *   double           -> float8
 *   float            -> float4
 *   date             -> date
 *   time             -> time
 *   timestamp        -> timestamp
 *   timestamptz      -> timestamptz
 *   uuid             -> uuid
 *   binary           -> bytea
 *   fixed[L]         -> bytea           LOSSY: fixed length L not enforced
 *   decimal(P,S)     -> numeric(P,S)    precision/scale preserved via typmod
 *
 * Lossy cases (Iceberg metadata cannot round-trip the original PG type):
 *   - Iceberg has one "string" type, so char(n)/varchar(n)/text all map to
 *     Iceberg string on write; a historical char(n)/varchar(n) column thus
 *     reads back as text (the length limit, and char(n) blank padding, are
 *     not recoverable).  This matches the CHAR(N)->string write semantics
 *     (issue #321): a time-travel read of such a column shows unpadded text.
 *   - fixed[L] reads back as bytea; the L-byte fixed width is not enforced.
 * Unsupported types (struct/list/map/etc.) raise a clear error rather than
 * guessing a mapping.
 */
static void
iceberg_type_to_pg(const char *itype, Oid *typid, int32 *typmod)
{
	*typmod = -1;
	if (strcmp(itype, "int") == 0)					*typid = INT4OID;
	else if (strcmp(itype, "long") == 0)			*typid = INT8OID;
	else if (strcmp(itype, "string") == 0)			*typid = TEXTOID;
	else if (strcmp(itype, "boolean") == 0)			*typid = BOOLOID;
	else if (strcmp(itype, "double") == 0)			*typid = FLOAT8OID;
	else if (strcmp(itype, "float") == 0)			*typid = FLOAT4OID;
	else if (strcmp(itype, "date") == 0)			*typid = DATEOID;
	else if (strcmp(itype, "timestamptz") == 0)		*typid = TIMESTAMPTZOID;
	else if (strcmp(itype, "timestamp") == 0)		*typid = TIMESTAMPOID;
	else if (strcmp(itype, "time") == 0)			*typid = TIMEOID;
	else if (strcmp(itype, "uuid") == 0)			*typid = UUIDOID;
	else if (strcmp(itype, "binary") == 0)			*typid = BYTEAOID;
	else if (strncmp(itype, "fixed", 5) == 0)		*typid = BYTEAOID;
	else if (strncmp(itype, "decimal", 7) == 0)
	{
		int		p = 0;
		int		s = 0;

		/*
		 * Accept both "decimal(P,S)" and "decimal(P, S)".  Iceberg always
		 * carries a precision and scale, so a decimal string we cannot parse
		 * is malformed: error out rather than silently leaving p=s=0, which
		 * would become an unconstrained numeric (typmod VARHDRSZ) with
		 * different semantics than the snapshot schema intends.
		 */
		if ((sscanf(itype, "decimal(%d,%d)", &p, &s) != 2 &&
			 sscanf(itype, "decimal(%d, %d)", &p, &s) != 2) ||
			p < 1 || p > 1000 || s < 0 || s > p)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("iceberg time travel: cannot parse decimal type \"%s\"",
							itype)));
		*typid = NUMERICOID;
		*typmod = (int32) (((p << 16) | (s & 0x7ff)) + VARHDRSZ);
	}
	else
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("iceberg time travel: unsupported column type \"%s\"",
						itype)));
}

/*
 * One-entry process-local cache for tt_fetch_snapshot_schema.  For a non-HEAD
 * snapshot the describe callback (builds the tuple descriptor) and the rewrite
 * gate (compares schema ids) both call it with the same (relid, snapshot_id)
 * back-to-back in one parse; caching the last result avoids the second
 * getSnapshotSchema round-trip.  Keyed also by the pinned metadata_location so a
 * changed metadata invalidates it -- and since a snapshot's schema is immutable,
 * a hit for the same key is always correct.  The copies live in a dedicated
 * context under TopMemoryContext.
 */
static MemoryContext tt_schema_cache_cxt = NULL;
static Oid		tt_schema_cache_relid = InvalidOid;
static int64	tt_schema_cache_snap = 0;
static char	   *tt_schema_cache_metaloc = NULL;
static IcebergSnapshotSchema tt_schema_cache_val;

static void
tt_copy_snapshot_schema(const IcebergSnapshotSchema *src,
						IcebergSnapshotSchema *dst, MemoryContext cxt)
{
	MemoryContext	old = MemoryContextSwitchTo(cxt);
	int				i;

	dst->snapshot_schema_id = src->snapshot_schema_id;
	dst->current_schema_id = src->current_schema_id;
	dst->ncols = src->ncols;
	dst->colnames = (char **) palloc(sizeof(char *) * src->ncols);
	dst->coltypes = (char **) palloc(sizeof(char *) * src->ncols);
	dst->fieldids = (int *) palloc0(sizeof(int) * Max(src->ncols, 1));
	for (i = 0; i < src->ncols; i++)
	{
		dst->colnames[i] = pstrdup(src->colnames[i]);
		dst->coltypes[i] = pstrdup(src->coltypes[i]);
		dst->fieldids[i] = src->fieldids ? src->fieldids[i] : 0;
	}
	MemoryContextSwitchTo(old);
}

/*
 * Fetch the schema of `snapshot_id` for `rel` from the agent (getSnapshotSchema
 * on the pinned scan metadata location).  Shared by the describe callback
 * (which builds the tuple descriptor) and the rewrite gate (which compares
 * schema ids to decide main path vs FunctionScan fallback).  The JSON is
 * parsed with the project's native SAX parser in pg_iceberg_catalog_utils.c
 * (jansson's macros clash with PostgreSQL's built-in JSON function protos).
 */
static void
tt_fetch_snapshot_schema(Relation rel, int64 snapshot_id,
						 IcebergSnapshotSchema *out)
{
	Oid					relid = RelationGetRelid(rel);
	char			   *metadata_location;
	IcebergTableInfo   *table_info;
	char			   *json;

	metadata_location = pg_iceberg_resolve_scan_metadata_location(relid, NULL);

	/* Cache hit: describe and the rewrite gate ask for the same schema. */
	if (tt_schema_cache_metaloc != NULL &&
		tt_schema_cache_relid == relid &&
		tt_schema_cache_snap == snapshot_id &&
		strcmp(tt_schema_cache_metaloc, metadata_location) == 0)
	{
		tt_copy_snapshot_schema(&tt_schema_cache_val, out, CurrentMemoryContext);
		pfree(metadata_location);
		return;
	}

	table_info = pg_iceberg_get_table_info(relid);
	json = pg_iceberg_get_snapshot_schema_with_catalog(rel, table_info,
													   metadata_location,
													   snapshot_id);
	pg_iceberg_free_table_info(table_info);

	if (json == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("iceberg time travel: empty getSnapshotSchema response")));

	pg_iceberg_parse_snapshot_schema_response(json, out);

	/* Refresh the one-entry cache (invalidate first so an error leaves a miss). */
	if (tt_schema_cache_cxt == NULL)
		tt_schema_cache_cxt = AllocSetContextCreate(TopMemoryContext,
													"iceberg tt schema cache",
													ALLOCSET_SMALL_SIZES);
	MemoryContextReset(tt_schema_cache_cxt);
	tt_schema_cache_metaloc = NULL;
	tt_copy_snapshot_schema(out, &tt_schema_cache_val, tt_schema_cache_cxt);
	tt_schema_cache_relid = relid;
	tt_schema_cache_snap = snapshot_id;
	tt_schema_cache_metaloc = MemoryContextStrdup(tt_schema_cache_cxt,
												  metadata_location);
	pfree(metadata_location);
}

/*
 * One-entry process-local cache for tt_fetch_snapshot_list, mirroring the
 * snapshot-schema cache above.  Resolving an AS OF TIMESTAMP argument folds it
 * to a snapshot id independently at each site that const-folds the call (the
 * describe callback, the rewrite gate, the planner hook, the prosupport
 * function); all of them must reach the SAME id.  They do, because the list is
 * read from the pinned scan metadata_location -- immutable for the statement --
 * and the key includes it, so a changed metadata invalidates the entry.
 */
static MemoryContext tt_snaplist_cache_cxt = NULL;
static Oid		tt_snaplist_cache_relid = InvalidOid;
static char	   *tt_snaplist_cache_metaloc = NULL;
static IcebergSnapshotList tt_snaplist_cache_val;

static void
tt_copy_snapshot_list(const IcebergSnapshotList *src,
					  IcebergSnapshotList *dst, MemoryContext cxt)
{
	MemoryContext	old = MemoryContextSwitchTo(cxt);
	int				i;

	dst->current_snapshot_id = src->current_snapshot_id;
	dst->nsnapshots = src->nsnapshots;
	dst->snapshots = (IcebergSnapshotEntry *)
		palloc(sizeof(IcebergSnapshotEntry) * Max(src->nsnapshots, 1));
	for (i = 0; i < src->nsnapshots; i++)
	{
		dst->snapshots[i] = src->snapshots[i];
		dst->snapshots[i].operation = pstrdup(src->snapshots[i].operation);
		dst->snapshots[i].summary_json = pstrdup(src->snapshots[i].summary_json);
	}
	MemoryContextSwitchTo(old);
}

/*
 * Fetch the snapshot list of `rel` from the agent (getSnapshots on the pinned
 * scan metadata location).  Backs both iceberg_snapshot_list() and the
 * AS OF TIMESTAMP resolution.
 */
static void
tt_fetch_snapshot_list(Relation rel, IcebergSnapshotList *out)
{
	Oid					relid = RelationGetRelid(rel);
	char			   *metadata_location;
	IcebergTableInfo   *table_info;
	char			   *json;

	metadata_location = pg_iceberg_resolve_scan_metadata_location(relid, NULL);

	if (tt_snaplist_cache_metaloc != NULL &&
		tt_snaplist_cache_relid == relid &&
		strcmp(tt_snaplist_cache_metaloc, metadata_location) == 0)
	{
		tt_copy_snapshot_list(&tt_snaplist_cache_val, out, CurrentMemoryContext);
		pfree(metadata_location);
		return;
	}

	table_info = pg_iceberg_get_table_info(relid);
	json = pg_iceberg_get_snapshots_with_catalog(rel, table_info,
												 metadata_location);
	pg_iceberg_free_table_info(table_info);

	if (json == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("iceberg time travel: empty getSnapshots response")));

	pg_iceberg_parse_snapshots_response(json, out);

	/* Refresh the one-entry cache (invalidate first so an error leaves a miss). */
	if (tt_snaplist_cache_cxt == NULL)
		tt_snaplist_cache_cxt = AllocSetContextCreate(TopMemoryContext,
													  "iceberg tt snapshot list cache",
													  ALLOCSET_SMALL_SIZES);
	MemoryContextReset(tt_snaplist_cache_cxt);
	tt_snaplist_cache_metaloc = NULL;
	tt_copy_snapshot_list(out, &tt_snaplist_cache_val, tt_snaplist_cache_cxt);
	tt_snaplist_cache_relid = relid;
	tt_snaplist_cache_metaloc = MemoryContextStrdup(tt_snaplist_cache_cxt,
													metadata_location);
	pfree(metadata_location);
}

/*
 * PostgreSQL timestamps count microseconds from 2000-01-01; Iceberg records a
 * snapshot's commit time as milliseconds from the Unix epoch.
 *
 * Divide with a floor, not C's truncation-toward-zero: for a pre-2000
 * timestamp truncation rounds the target time UP, which could admit a snapshot
 * committed in the millisecond after the requested instant.  "At or before"
 * must never look forward.
 */
static int64
tt_timestamptz_to_unix_ms(TimestampTz ts)
{
	int64		epoch_diff_ms =
		((int64) (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE)) * SECS_PER_DAY * 1000;
	int64		ms;

	if (ts >= 0)
		ms = ts / 1000;
	else
		ms = -(((-ts) + 999) / 1000);

	return ms + epoch_diff_ms;
}

/*
 * Resolve an AS OF TIMESTAMP argument to a snapshot id: the newest snapshot
 * committed at or before `ts`.  This is Iceberg's own TableScan.asOfTime()
 * semantics, so a CBDB time-travel read and a Spark/Trino read of the same
 * instant select the same snapshot.
 *
 * Ties (two snapshots sharing a commit millisecond) resolve to the later one in
 * metadata order, again matching Iceberg.
 *
 * No snapshot at or before the instant is an ERROR, never a silent HEAD read:
 * quietly returning current data for a point in time before the table existed
 * would be indistinguishable from a correct answer.
 */
static int64
tt_resolve_timestamp(Oid relid, TimestampTz ts)
{
	Relation	rel;
	IcebergSnapshotList sl;
	int64		target_ms;
	int64		best_id = 0;
	int64		best_ms = 0;
	bool		found = false;
	int			i;

	if (TIMESTAMP_IS_NOBEGIN(ts))
		target_ms = PG_INT64_MIN;
	else if (TIMESTAMP_IS_NOEND(ts))
		target_ms = PG_INT64_MAX;
	else
		target_ms = tt_timestamptz_to_unix_ms(ts);

	rel = table_open(relid, AccessShareLock);
	tt_fetch_snapshot_list(rel, &sl);
	table_close(rel, AccessShareLock);

	for (i = 0; i < sl.nsnapshots; i++)
	{
		int64	snap_ms = sl.snapshots[i].timestamp_ms;

		/* ">=" (not ">") so the LAST of equal-timestamp snapshots wins. */
		if (snap_ms <= target_ms && (!found || snap_ms >= best_ms))
		{
			best_ms = snap_ms;
			best_id = sl.snapshots[i].snapshot_id;
			found = true;
		}
	}

	if (!found)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("iceberg time travel: \"%s\" has no snapshot at or before %s",
						get_rel_name(relid) ? get_rel_name(relid) : "?",
						DatumGetCString(DirectFunctionCall1(timestamptz_out,
															TimestampTzGetDatum(ts)))),
				 errhint("Use iceberg_snapshot_list() to list the available snapshots.")));

	return best_id;
}

/*
 * Resolve argument 1 of an iceberg_snapshot_scan call to a snapshot id.
 *
 * Two SQL overloads share this C function:
 *   iceberg_snapshot_scan(regclass, bigint)       argument IS the snapshot id
 *   iceberg_snapshot_scan(regclass, timestamptz)  argument is an instant, folded
 *       here to the newest snapshot committed at or before it
 *
 * Every site that inspects the call -- the describe callback, the rewrite gate,
 * the planner hook and the prosupport function -- must go through this one
 * helper, so all of them agree on the id (see tt_arg_not_constant and
 * tt_fetch_snapshot_list for why that agreement holds).
 *
 * A NULL selector means "read HEAD" and yields 0, matching the bigint form.
 */
static int64
tt_snapshot_id_from_arg(FuncExpr *fexpr, Oid relid)
{
	Node	   *arg;
	Datum		value;
	bool		isnull;

	if (list_length(fexpr->args) < 2)
		return 0;

	arg = (Node *) lsecond(fexpr->args);

	/*
	 * By planner-hook time the argument is already const-folded; at parse time
	 * it may still be a cast expression, which ExecEvalFunctionArgToConst folds
	 * (its safety is guaranteed by the tt_arg_not_constant check callers run).
	 */
	if (IsA(arg, Const))
	{
		isnull = ((Const *) arg)->constisnull;
		value = ((Const *) arg)->constvalue;
	}
	else
		value = ExecEvalFunctionArgToConst(fexpr, 1, &isnull);

	if (isnull)
		return 0;

	if (exprType(arg) == TIMESTAMPTZOID)
		return tt_resolve_timestamp(relid, DatumGetTimestampTz(value));

	/*
	 * Only 0 (and NULL, handled above) means HEAD.  A negative id is not a
	 * snapshot that could ever exist and must not silently read current data.
	 */
	if (DatumGetInt64(value) < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("iceberg time travel: invalid snapshot id " INT64_FORMAT,
						DatumGetInt64(value)),
				 errhint("Use iceberg_snapshot_list() to list the available snapshots.")));

	return DatumGetInt64(value);
}

/* Inverse of tt_timestamptz_to_unix_ms.  Exact: milliseconds -> microseconds. */
static TimestampTz
tt_unix_ms_to_timestamptz(int64 ms)
{
	int64		epoch_diff_ms =
		((int64) (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE)) * SECS_PER_DAY * 1000;

	return (TimestampTz) ((ms - epoch_diff_ms) * INT64CONST(1000));
}

/*
 * Pin a time-dependent selector argument to the snapshot it just resolved to,
 * by replacing the expression with a literal timestamp: the commit time of that
 * very snapshot.
 *
 * Why this is needed only here.  The four sites that read the selector each
 * const-fold it independently.  Within one parse+plan of one statement a STABLE
 * expression yields the same instant everywhere, so they agree.  But a parse
 * tree can be REUSED -- PREPARE/EXECUTE, a plpgsql function body -- and then
 * planning runs again later, in another transaction, where now() is a different
 * instant.  On the main path that is harmless: the rewrite below discards the
 * FuncExpr entirely and stamps the snapshot into the RTE alias, so the id is
 * already frozen.  On the FunctionScan fallback the expression survives into
 * planning, so the tuple descriptor (frozen at parse time, from the OLD
 * instant) and the fragment list (resolved at plan time, from the NEW instant)
 * could come from different snapshots -- and if the schema changed in between,
 * that is silently wrong data.
 *
 * Replacing the expression with the resolved snapshot's own commit timestamp
 * closes it: re-resolving that literal ("newest snapshot committed at or before
 * T") necessarily selects the same snapshot again, because T is that snapshot's
 * commit time.  Equal-millisecond ties are resolved the same way on every pass
 * (last in metadata order), so they land on the same snapshot too.
 *
 * This mirrors Spark, which resolves the instant once during analysis and from
 * then on carries a Table bound to the chosen snapshot rather than the
 * expression (Analyzer.lookupRelation -> SparkCatalog.loadTable ->
 * SnapshotUtil.snapshotIdAsOfTime).
 *
 * Consequence to document: a prepared statement then keeps reading the snapshot
 * chosen when it was prepared.  That matches what the main path has always
 * done, and trades a silent-wrong-data hazard for a visible-in-EXPLAIN literal.
 */
static void
tt_freeze_selector_arg(FuncExpr *fexpr, Relation rel, int64 snapshot_id)
{
	Node	   *arg;
	IcebergSnapshotList sl;
	int			i;

	if (list_length(fexpr->args) < 2 || snapshot_id <= 0)
		return;

	arg = (Node *) lsecond(fexpr->args);

	/*
	 * Only the timestamptz form can drift: a bigint id is already the value
	 * itself, and an argument that is already a Const cannot re-fold.
	 */
	if (exprType(arg) != TIMESTAMPTZOID || IsA(arg, Const))
		return;

	tt_fetch_snapshot_list(rel, &sl);
	for (i = 0; i < sl.nsnapshots; i++)
	{
		if (sl.snapshots[i].snapshot_id != snapshot_id)
			continue;

		lsecond(fexpr->args) = (Node *)
			makeConst(TIMESTAMPTZOID, -1, InvalidOid, sizeof(TimestampTz),
					  TimestampTzGetDatum(
						  tt_unix_ms_to_timestamptz(sl.snapshots[i].timestamp_ms)),
					  false, FLOAT8PASSBYVAL);
		return;
	}
}

/*
 * iceberg_snapshot_list_json(regclass) -> text
 *
 * Raw getSnapshots JSON for the table's pinned scan metadata.  Internal: the
 * user-facing iceberg_snapshot_list() is a SQL wrapper that expands this
 * with jsonb_array_elements (the extension's other toolkit functions likewise
 * return text and are shaped in SQL).
 *
 * Reading the PINNED metadata, not a fresh catalog load, is deliberate: the
 * listed snapshots are then exactly the ones a time-travel read issued right
 * afterwards can select.
 */
Datum
iceberg_snapshot_list_json(PG_FUNCTION_ARGS)
{
	Oid					relid = PG_GETARG_OID(0);
	Relation			rel;
	char			   *metadata_location;
	IcebergTableInfo   *table_info;
	char			   *json;
	AclResult			aclresult;

	if (!tt_is_iceberg_relation(relid))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not an iceberg table",
						get_rel_name(relid) ? get_rel_name(relid) : "?")));

	/* Snapshot history is table metadata: require SELECT, like a read. */
	aclresult = pg_class_aclcheck(relid, GetUserId(), ACL_SELECT);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, OBJECT_TABLE, get_rel_name(relid));

	rel = table_open(relid, AccessShareLock);
	metadata_location = pg_iceberg_resolve_scan_metadata_location(relid, NULL);
	table_info = pg_iceberg_get_table_info(relid);
	json = pg_iceberg_get_snapshots_with_catalog(rel, table_info,
												 metadata_location);
	pg_iceberg_free_table_info(table_info);
	pfree(metadata_location);
	table_close(rel, AccessShareLock);

	if (json == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("iceberg: empty getSnapshots response for \"%s\"",
						get_rel_name(relid) ? get_rel_name(relid) : "?")));

	PG_RETURN_TEXT_P(cstring_to_text(json));
}

/*
 * Time travel is a plain historical read of a single Iceberg table.  Reject
 * relation shapes whose extra machinery either path would silently bypass or
 * mishandle:
 *
 *   - row-level security: the FunctionScan fallback reads the data files
 *     directly and never applies policies, so a restricted user could see
 *     rows RLS is meant to hide.  Block uniformly (both paths) rather than
 *     letting behavior depend on whether the snapshot's schema changed.
 *   - inheritance / partitioning: the relation rewrite sets rte->inh, which
 *     would expand child tables the historical snapshot does not describe.
 *
 * Called from the describe callback, the single parse-time choke point shared
 * by the ORCA-native relation path and the FunctionScan fallback.
 */
static void
tt_reject_unsupported_relation(Relation rel)
{
	Oid			relid = RelationGetRelid(rel);

	if (rel->rd_rel->relhassubclass || rel->rd_rel->relispartition)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("iceberg time travel does not support inheritance or partitioned tables"),
				 errdetail("Relation \"%s\" participates in an inheritance or partition hierarchy.",
						   RelationGetRelationName(rel))));

	/*
	 * noError=true: RLS_ENABLED is returned only when policies would actually
	 * apply to the current user.  Owners/superusers who bypass RLS anyway are
	 * not blocked (nothing is being bypassed for them).
	 */
	if (check_enable_rls(relid, InvalidOid, true) == RLS_ENABLED)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("iceberg time travel does not support tables with row-level security"),
				 errdetail("Row-level security policies on \"%s\" would not be enforced on a historical read.",
						   RelationGetRelationName(rel))));
}

/*
 * iceberg_snapshot_describe -- parse-time DESCRIBE callback: (internal)->internal.
 *
 * Resolves the result tuple descriptor from the constant first argument
 * (the target table) and validates the call.  Increment 1 uses the live
 * relation's schema (correct for the HEAD snapshot and for any snapshot whose
 * schema is unchanged); the snapshot-schema path is layered on in Phase 2.
 */
Datum
iceberg_snapshot_describe(PG_FUNCTION_ARGS)
{
	FuncExpr   *fexpr;
	Oid			relid;
	int64		snapshot_id;
	bool		isnull;
	Relation	rel;
	TupleDesc	tupdesc;
	AclResult	aclresult;

	if (PG_NARGS() != 1 || PG_ARGISNULL(0))
		elog(ERROR, "invalid invocation of iceberg_snapshot_describe");

	fexpr = (FuncExpr *) PG_GETARG_POINTER(0);
	if (!IsA(fexpr, FuncExpr))
		elog(ERROR, "iceberg_snapshot_describe: argument is not a FuncExpr");

	/*
	 * Both arguments must be immutable constants: the column set is fixed at
	 * parse time, and the same arguments are re-folded independently by the
	 * rewrite gate and the planner hook (see tt_arg_not_constant).
	 */
	if (tt_arg_not_constant((Node *) linitial(fexpr->args)) ||
		(list_length(fexpr->args) > 1 &&
		 tt_selector_arg_not_constant((Node *) lsecond(fexpr->args))))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("iceberg_snapshot_scan arguments must be constant expressions")));

	relid = DatumGetObjectId(ExecEvalFunctionArgToConst(fexpr, 0, &isnull));
	if (isnull)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("iceberg_snapshot_scan: table argument must not be null")));

	if (!tt_is_iceberg_relation(relid))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not an iceberg table",
						get_rel_name(relid) ? get_rel_name(relid) : "?")));

	/*
	 * EXECUTE on the function.  The native relation rewrite replaces the
	 * function RTE with a relation RTE, dropping the call's own privilege
	 * check; enforcing it here (the shared parse-time choke point, before the
	 * rewrite) keeps a REVOKE EXECUTE effective on both the rewritten and the
	 * FunctionScan-fallback paths rather than only the latter.
	 */
	aclresult = pg_proc_aclcheck(fexpr->funcid, GetUserId(), ACL_EXECUTE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, OBJECT_FUNCTION, get_func_name(fexpr->funcid));

	/* Time travel reads the table's data: require SELECT, like a table scan. */
	aclresult = pg_class_aclcheck(relid, GetUserId(), ACL_SELECT);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, OBJECT_TABLE, get_rel_name(relid));

	snapshot_id = tt_snapshot_id_from_arg(fexpr, relid);

	rel = table_open(relid, AccessShareLock);

	/* Reject RLS / inheritance shapes before resolving the tuple descriptor. */
	tt_reject_unsupported_relation(rel);

	if (snapshot_id <= 0)
	{
		/* HEAD read: the current relation schema. */
		tupdesc = CreateTupleDescCopy(RelationGetDescr(rel));
	}
	else
	{
		IcebergSnapshotSchema ss;

		tt_fetch_snapshot_schema(rel, snapshot_id, &ss);

		if (ss.snapshot_schema_id == ss.current_schema_id)
		{
			/* Schema unchanged since the snapshot: current relation schema. */
			tupdesc = CreateTupleDescCopy(RelationGetDescr(rel));
		}
		else
		{
			/*
			 * Schema changed after the snapshot: return the SNAPSHOT's own
			 * schema so columns dropped afterwards stay visible (the read then
			 * runs via the FunctionScan fallback -- see iceberg_tt_rewrite_rtable,
			 * which declines the relation rewrite for this case).
			 */
			int		i;

			tupdesc = CreateTemplateTupleDesc(ss.ncols);
			for (i = 0; i < ss.ncols; i++)
			{
				Oid		typid;
				int32	typmod;

				iceberg_type_to_pg(ss.coltypes[i], &typid, &typmod);
				TupleDescInitEntry(tupdesc, (AttrNumber) (i + 1),
								   ss.colnames[i], typid, typmod, 0);
			}
		}
	}

	table_close(rel, AccessShareLock);

	PG_RETURN_POINTER(tupdesc);
}

/* per-scan state carried across value-per-call invocations on a QE */
typedef struct TTScanState
{
	ForeignScanState *fss;
	Relation	rel;
}			TTScanState;

/*
 * Cleanup for the time-travel SRF: close the FDW scan and release the relation.
 * Registered as a reset callback on the multi-call context so it also runs when
 * the query stops before the SRF is exhausted (LIMIT, an error in a parent plan
 * node, a client abort) -- otherwise SRF_RETURN_DONE would be the only place
 * these ran, leaking the volume FDW's open readers/connections until the memory
 * context is destroyed at transaction end.  Both fields are NULLed so the done
 * path and the callback are idempotent (whichever fires first).  The relation is
 * closed with NoLock: the lock is owned by the resource owner and released at
 * transaction end, and dropping it here would risk a double-release warning when
 * the callback fires during transaction abort.
 */
static void
tt_scan_cleanup(void *arg)
{
	TTScanState *st = (TTScanState *) arg;

	if (st->fss != NULL)
	{
		st->fss->fdwroutine->EndForeignScan(st->fss);
		st->fss = NULL;
	}
	if (st->rel != NULL)
	{
		table_close(st->rel, NoLock);
		st->rel = NULL;
	}
}

/*
 * iceberg_snapshot_scan(regclass, bigint, __fragments text DEFAULT NULL)
 * RETURNS SETOF record, EXECUTE ON ALL SEGMENTS.
 *
 * The third argument is populated by the planner hook with the QD-resolved
 * fragment list; users never supply it.  Each QE reuses the Iceberg AM's
 * volume-fdw reader, which shards the fragment list across segments.
 */
Datum
iceberg_snapshot_scan(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	TTScanState *st;
	TupleTableSlot *slot;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		TupleDesc	tupdesc;
		Oid			relid;
		char	   *fragments;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("iceberg_snapshot_scan must be called in the FROM clause")));
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		relid = PG_GETARG_OID(0);

		/*
		 * Re-check SELECT at execution: unlike the ORCA-native relation path
		 * (whose RTE carries requiredPerms = ACL_SELECT), the FunctionScan
		 * fallback has no relation RTE for ExecCheckRTPerms to validate, so a
		 * cached/prepared plan could otherwise outlive a privilege revoke.
		 */
		{
			AclResult	aclresult;

			aclresult = pg_class_aclcheck(relid, GetUserId(), ACL_SELECT);
			if (aclresult != ACLCHECK_OK)
				aclcheck_error(aclresult, OBJECT_TABLE, get_rel_name(relid));
		}

		if (PG_NARGS() < 3 || PG_ARGISNULL(2))
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("iceberg_snapshot_scan: fragment list not resolved"),
					 errdetail("The planner hook did not inject the fragment payload.")));

		fragments = text_to_cstring(PG_GETARG_TEXT_PP(2));

		/*
		 * The 4th hidden argument carries the snapshot schema's field-ids
		 * (csv, QD-resolved).  Absent on plans cached before this column was
		 * added: degrade to the reader's positional default rather than fail.
		 */
		{
			int	   *fids = NULL;
			int		nfids = 0;

			if (PG_NARGS() >= 4 && !PG_ARGISNULL(3))
				fids = tt_parse_fieldid_csv(text_to_cstring(PG_GETARG_TEXT_PP(3)),
											&nfids);

			st = (TTScanState *) palloc0(sizeof(TTScanState));
			st->rel = table_open(relid, AccessShareLock);
			st->fss = pg_iceberg_snapshot_beginscan(st->rel, tupdesc, fragments,
													fids, nfids);
		}
		funcctx->user_fctx = st;

		/*
		 * Guarantee resource cleanup even if the query exits before the SRF is
		 * exhausted (LIMIT, parent-node error, client abort): a reset callback
		 * on the multi-call context runs tt_scan_cleanup on teardown as well as
		 * on the normal SRF_RETURN_DONE path.  Allocated in the multi-call
		 * context (current context here), so it lives as long as the scan.
		 */
		{
			MemoryContextCallback *cb;

			cb = (MemoryContextCallback *) palloc0(sizeof(MemoryContextCallback));
			cb->func = tt_scan_cleanup;
			cb->arg = st;
			MemoryContextRegisterResetCallback(funcctx->multi_call_memory_ctx, cb);
		}

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	st = (TTScanState *) funcctx->user_fctx;

	slot = st->fss->fdwroutine->IterateForeignScan(st->fss);
	if (!TupIsNull(slot))
	{
		HeapTuple	tuple = ExecCopySlotHeapTuple(slot);

		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}

	/* Normal completion: cleanup now (idempotent with the reset callback). */
	tt_scan_cleanup(st);
	SRF_RETURN_DONE(funcctx);
}

/*
 * Guard a surviving FunctionScan against the snapshot list changing between
 * parse analysis and (re)planning.  The call's output columns were fixed at
 * parse time from the snapshot the selector resolved to THEN; a cached plan
 * replans later, and expiration can make the same selector resolve to a
 * DIFFERENT surviving snapshot.  Reading it under the stale descriptor would
 * silently mis-decode, so require the resolved snapshot's schema to still
 * match the descriptor column-for-column and fail closed otherwise.  A
 * schema-identical drift (the normal "newest at or before" outcome after an
 * expire) stays readable.
 */
static void
tt_assert_call_schema_current(RangeTblFunction *rtf, Relation rel,
							  int64 snapshot_id)
{
	IcebergSnapshotSchema ss;
	ListCell   *lc;
	int			i;

	if (snapshot_id <= 0 || rtf->funccolcount <= 0)
		return;

	tt_fetch_snapshot_schema(rel, snapshot_id, &ss);
	if (ss.ncols == rtf->funccolcount)
	{
		i = 0;
		foreach(lc, rtf->funccolnames)
		{
			Oid		typid;
			int32	typmod;

			if (strcmp(strVal(lfirst(lc)), ss.colnames[i]) != 0)
				break;
			/*
			 * Names alone miss a type change between snapshots (#401 allows
			 * widening): decode under the stale descriptor would corrupt, so
			 * the types must match too.
			 */
			iceberg_type_to_pg(ss.coltypes[i], &typid, &typmod);
			if (typid != list_nth_oid(rtf->funccoltypes, i) ||
				typmod != list_nth_int(rtf->funccoltypmods, i))
				break;
			i++;
		}
		if (i == ss.ncols)
			return;
	}
	ereport(ERROR,
			(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
			 errmsg("iceberg time travel: the snapshot resolved at planning no longer matches the statement's column set"),
			 errdetail("The table's snapshot list changed (e.g. snapshots expired) after the statement was parsed."),
			 errhint("Re-prepare the statement.")));
}

/*
 * Snapshot field-ids ride from the QD to the QEs as a csv text ("1,3,4"),
 * parallel to the snapshot tupdesc: as a hidden FuncExpr argument on the
 * FunctionScan fallback, and as a custom_private entry on the Iceberg
 * Snapshot Scan.  Built from the (cached) getSnapshotSchema response.
 */
static char *
tt_build_fieldid_csv(Relation rel, int64 snapshot_id)
{
	IcebergSnapshotSchema ss;
	StringInfoData	buf;
	int				i;

	tt_fetch_snapshot_schema(rel, snapshot_id, &ss);
	/*
	 * All-or-nothing, matching the QE side (tt_parse_fieldid_csv rejects ids
	 * <= 0 and pg_iceberg_snapshot_beginscan requires a full set): an agent
	 * that predates the fieldId key -- or a partially-filled response --
	 * ships nothing and the reader falls back to its defaults, instead of a
	 * mixed csv failing on every QE at execution.
	 */
	if (ss.ncols == 0 || ss.fieldids == NULL)
		return NULL;
	for (i = 0; i < ss.ncols; i++)
		if (ss.fieldids[i] <= 0)
			return NULL;

	initStringInfo(&buf);
	for (i = 0; i < ss.ncols; i++)
		appendStringInfo(&buf, "%s%d", i > 0 ? "," : "", ss.fieldids[i]);
	return buf.data;
}

static int *
tt_parse_fieldid_csv(const char *csv, int *nfids)
{
	int		   *fids;
	int			n = 1;
	const char *p;

	*nfids = 0;
	if (csv == NULL || *csv == '\0')
		return NULL;
	for (p = csv; *p; p++)
		if (*p == ',')
			n++;
	fids = (int *) palloc0(sizeof(int) * n);
	n = 0;
	p = csv;
	while (*p)
	{
		char	   *endp;
		long		v;

		errno = 0;
		v = strtol(p, &endp, 10);
		/*
		 * Fail closed on anything but a clean positive number followed by a
		 * separator or the end: strtol does not advance past garbage (an
		 * unchecked loop here would spin forever on the QE), and a partial or
		 * out-of-range id must never silently degrade to positional matching.
		 */
		if (endp == p || errno == ERANGE || v <= 0 || v > PG_INT32_MAX ||
			(*endp != ',' && *endp != '\0'))
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("iceberg time travel: malformed field-id payload \"%s\"",
							csv)));
		fids[n++] = (int) v;
		p = endp;
		if (*p == ',')
		{
			p++;
			if (*p == '\0')
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("iceberg time travel: malformed field-id payload \"%s\"",
								csv)));
		}
	}
	*nfids = n;
	return fids;
}

/*
 * iceberg_tt_inject_fragments
 *
 * Called from the Iceberg planner hook on the QD.  Walks the finished plan
 * tree, and for every FunctionScan calling iceberg_snapshot_scan resolves the
 * snapshot's fragment list (the agent call lives here, QD-only) and injects it
 * as the hidden third argument so it is dispatched to every QE with the plan.
 */
static void
tt_rewrite_functions(List *functions)
{
	ListCell   *lf;

	foreach(lf, functions)
	{
		RangeTblFunction *rtfunc = (RangeTblFunction *) lfirst(lf);
		FuncExpr   *fexpr;
		Node	   *arg0;
		Oid			relid;
		int64		snapshot_id;
		Relation	rel;
		char	   *fragments;
		char	   *fids_csv;
		Const	   *payload;
		Const	   *fids_payload;

		if (!IsA(rtfunc->funcexpr, FuncExpr))
			continue;
		fexpr = (FuncExpr *) rtfunc->funcexpr;

		/*
		 * Match our C function by identity, not by name: a user/temp function
		 * sharing the name would otherwise be hijacked, and a shorter argument
		 * list would make the lsecond() below read past the list.  Our function
		 * always carries its two mandatory arguments (regclass, bigint).
		 */
		if (!tt_is_snapshot_scan_func(fexpr->funcid) ||
			list_length(fexpr->args) < 2)
			continue;

		arg0 = (Node *) linitial(fexpr->args);
		if (!IsA(arg0, Const) || ((Const *) arg0)->constisnull)
			continue;
		relid = DatumGetObjectId(((Const *) arg0)->constvalue);

		/*
		 * arg1 selects the snapshot: either the id itself or (the timestamptz
		 * overload) an instant folded to an id.  0/NULL means read HEAD.
		 */
		snapshot_id = tt_snapshot_id_from_arg(fexpr, relid);

		rel = table_open(relid, AccessShareLock);
		tt_assert_call_schema_current(rtfunc, rel, snapshot_id);
		fragments = pg_iceberg_list_data_fragments_json(rel, snapshot_id);
		fids_csv = tt_build_fieldid_csv(rel, snapshot_id);
		table_close(rel, AccessShareLock);

		payload = makeConst(TEXTOID, -1, InvalidOid, -1,
							CStringGetTextDatum(fragments), false, false);
		if (fids_csv != NULL)
			fids_payload = makeConst(TEXTOID, -1, InvalidOid, -1,
									 CStringGetTextDatum(fids_csv), false, false);
		else
			fids_payload = makeConst(TEXTOID, -1, InvalidOid, -1,
									 (Datum) 0, true, false);

		if (list_length(fexpr->args) >= 3)
			lthird(fexpr->args) = (Node *) payload;
		else
			fexpr->args = lappend(fexpr->args, payload);

		/* 4th hidden argument: the snapshot schema's field-ids (csv). */
		if (list_length(fexpr->args) >= 4)
			lfourth(fexpr->args) = (Node *) fids_payload;
		else
			fexpr->args = lappend(fexpr->args, fids_payload);
	}
}

void
iceberg_tt_inject_fragments(Plan *plan)
{
	if (plan == NULL)
		return;

	if (IsA(plan, FunctionScan))
		tt_rewrite_functions(((FunctionScan *) plan)->functions);
	else if (IsA(plan, SubqueryScan))
		iceberg_tt_inject_fragments(((SubqueryScan *) plan)->subplan);
	else if (IsA(plan, Append))
	{
		ListCell   *l;

		foreach(l, ((Append *) plan)->appendplans)
			iceberg_tt_inject_fragments((Plan *) lfirst(l));
	}
	else if (IsA(plan, MergeAppend))
	{
		ListCell   *l;

		foreach(l, ((MergeAppend *) plan)->mergeplans)
			iceberg_tt_inject_fragments((Plan *) lfirst(l));
	}

	iceberg_tt_inject_fragments(plan->lefttree);
	iceberg_tt_inject_fragments(plan->righttree);
}


/* ------------------------------------------------------------------------
 * Main path: rewrite iceberg_snapshot_scan() FROM-calls into ordinary
 * relation scans on the target Iceberg AM table so ORCA plans them natively
 * (parallel + pushdown + native stats), and bind the snapshot through the
 * relation's alias.
 *
 * The snapshot id is encoded into rte->alias->aliasname as a compact token
 * that survives ORCA's Query->DXL->PlannedStmt round trip (verified by the
 * tt_poc_a Phase-0 probe).  At execution the Iceberg CustomScan recovers the
 * snapshot from its range-table entry (iceberg_tt_parse_alias) and reads that
 * historical snapshot instead of HEAD.
 *
 * Increment-1's FunctionScan path (iceberg_tt_inject_fragments) remains the
 * fallback for calls this rewrite declines; a rewritten call is no longer a
 * FunctionScan, so that hook becomes a no-op for it.
 * ------------------------------------------------------------------------
 */
#define ICEBERG_TT_ALIAS_PREFIX "__icetts_"

static post_parse_analyze_hook_type prev_post_parse_analyze_hook = NULL;
static int	iceberg_tt_alias_counter = 0;

/*
 * Recover the bound snapshot id from a relation alias stamped by the rewrite.
 * Returns true and sets *snapshot_id when the alias is a time-travel token.
 */
bool
iceberg_tt_parse_alias(const char *aliasname, int64 *snapshot_id)
{
	size_t		plen = strlen(ICEBERG_TT_ALIAS_PREFIX);
	const char *p;
	char	   *endp;
	long long	v;

	if (aliasname == NULL ||
		strncmp(aliasname, ICEBERG_TT_ALIAS_PREFIX, plen) != 0)
		return false;

	/*
	 * Accept only the exact shape the rewrite stamps:
	 * __icetts_<digits>_<digits>.  A malformed numeric part must not be
	 * half-parsed into some other snapshot id (or 0 = HEAD): treat any
	 * deviation as "not a time-travel token" so the scan reads HEAD under
	 * the alias like any other relation alias.
	 */
	p = aliasname + plen;
	if (*p < '0' || *p > '9')
		return false;
	errno = 0;
	v = strtoll(p, &endp, 10);
	if (errno == ERANGE || v < 0 || *endp != '_')
		return false;
	for (p = endp + 1; *p >= '0' && *p <= '9'; p++)
		;
	if (p == endp + 1 || *p != '\0')
		return false;

	*snapshot_id = (int64) v;
	return true;
}

/*
 * Guard a time-travel snapshot bound through a relation alias, called from the
 * Iceberg CustomScan on the QD.
 *
 * The post_parse_analyze rewrite stamps the __icetts_ alias ONLY when the
 * snapshot's schema equals the current schema (a schema-changed snapshot stays
 * on the FunctionScan fallback, which returns the snapshot's own columns).  A
 * user with SELECT could hand-write the alias -- SELECT * FROM t AS
 * "__icetts_<id>_1" -- to reach the CustomScan directly, bypassing that gate:
 * the scan would then read the historical data files under the CURRENT tuple
 * descriptor, so a column dropped / renamed / retyped after the snapshot would
 * be decoded positionally wrong and return corrupt rows.  Re-assert the gate's
 * invariant here and fail loudly.  HEAD (snapshot_id <= 0) is always the current
 * schema and needs no check (and no agent round-trip).
 */
void
iceberg_tt_check_alias_schema(Relation rel, int64 snapshot_id)
{
	IcebergSnapshotSchema ss;

	if (snapshot_id <= 0)
		return;

	tt_fetch_snapshot_schema(rel, snapshot_id, &ss);
	if (ss.snapshot_schema_id != ss.current_schema_id)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot time-travel to a snapshot whose schema differs from the current schema through a relation alias"),
				 errdetail("Snapshot " INT64_FORMAT " of \"%s\" has a different schema id than the current table schema.",
						   snapshot_id, RelationGetRelationName(rel)),
				 errhint("Use iceberg_snapshot_scan() to read a historical snapshot under its own schema.")));
}

/* Rewrite one query level's iceberg_snapshot_scan function RTEs to relations. */
static void
iceberg_tt_rewrite_rtable(List *rtable)
{
	ListCell   *lc;

	foreach(lc, rtable)
	{
		RangeTblEntry	   *rte = (RangeTblEntry *) lfirst(lc);
		RangeTblFunction   *rtf;
		FuncExpr		   *fexpr;
		Oid					relid;
		int64				snapshot_id;
		bool				isnull;
		Relation			rel;
		char			   *token;

		if (rte->rtekind != RTE_FUNCTION || list_length(rte->functions) != 1)
			continue;
		rtf = (RangeTblFunction *) linitial(rte->functions);
		if (rtf->funcexpr == NULL || !IsA(rtf->funcexpr, FuncExpr))
			continue;
		fexpr = (FuncExpr *) rtf->funcexpr;
		/* Match our C function by identity, not name (see tt_rewrite_functions). */
		if (!tt_is_snapshot_scan_func(fexpr->funcid) ||
			list_length(fexpr->args) < 2)
			continue;

		/* Arguments must fold to parse-time constants (see #5 / #406). */
		if (tt_arg_not_constant((Node *) linitial(fexpr->args)) ||
			tt_selector_arg_not_constant((Node *) lsecond(fexpr->args)))
			continue;
		relid = DatumGetObjectId(ExecEvalFunctionArgToConst(fexpr, 0, &isnull));
		if (isnull || !tt_is_iceberg_relation(relid))
			continue;
		/*
		 * A NULL selector means "read HEAD", identical to 0 (normalized inside
		 * tt_snapshot_id_from_arg).  Take the native relation path for it, as
		 * the describe callback already does for the tuple descriptor, instead
		 * of leaving the call as a FunctionScan -- which would lose ORCA-native
		 * planning and could serve a stale cached HEAD.
		 */
		snapshot_id = tt_snapshot_id_from_arg(fexpr, relid);

		/*
		 * The RTE was born as a function, so the parser never locked the
		 * relation; acquire AccessShareLock now.
		 */
		rel = relation_open(relid, AccessShareLock);

		/*
		 * Gate: only schema-unchanged snapshots take the ORCA-native relation
		 * rewrite.  If the snapshot's schema differs from the current schema
		 * (e.g. a column was dropped afterwards), leave the call as a
		 * FunctionScan so the fallback returns the snapshot's own columns
		 * (describe already built that tuple descriptor).
		 */
		if (snapshot_id > 0)
		{
			IcebergSnapshotSchema ss;

			tt_fetch_snapshot_schema(rel, snapshot_id, &ss);
			if (ss.snapshot_schema_id != ss.current_schema_id)
			{
				/*
				 * Fallback path: the FuncExpr survives into planning, so a
				 * time-dependent selector must be pinned to the snapshot it
				 * just resolved to before the tree can be cached and replanned.
				 */
				tt_freeze_selector_arg(fexpr, rel, snapshot_id);
				relation_close(rel, AccessShareLock);
				continue;
			}
		}

		/* Main path: keep the AccessShareLock for the transaction. */
		relation_close(rel, NoLock);

		rte->rtekind = RTE_RELATION;
		rte->relid = relid;
		rte->relkind = get_rel_relkind(relid);
		rte->rellockmode = AccessShareLock;
		rte->inh = true;
		rte->lateral = false;
		rte->functions = NIL;
		rte->funcordinality = false;
		rte->requiredPerms = ACL_SELECT;
		rte->checkAsUser = InvalidOid;

		/* Stamp the snapshot into the alias -- the ORCA-surviving carrier. */
		token = (char *) palloc(NAMEDATALEN);
		snprintf(token, NAMEDATALEN, ICEBERG_TT_ALIAS_PREFIX INT64_FORMAT "_%d",
				 snapshot_id, ++iceberg_tt_alias_counter);
		if (rte->eref == NULL)
			rte->eref = makeAlias(token, NIL);
		else
			rte->eref->aliasname = token;
		rte->alias = makeAlias(token, NIL);
	}
}

static bool
iceberg_tt_query_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Query))
	{
		Query	   *q = (Query *) node;

		/* Do not descend into utility statements' raw parse nodes. */
		if (q->commandType == CMD_UTILITY)
			return false;
		iceberg_tt_rewrite_rtable(q->rtable);
		return query_tree_walker(q, iceberg_tt_query_walker, context, 0);
	}
	return expression_tree_walker(node, iceberg_tt_query_walker, context);
}

static void
iceberg_tt_post_parse_analyze(ParseState *pstate, Query *query,
							  JumbleState *jstate)
{
	if (prev_post_parse_analyze_hook)
		prev_post_parse_analyze_hook(pstate, query, jstate);

	/* Only the QD (or single-node utility) parses user SQL. */
	if (Gp_role != GP_ROLE_EXECUTE)
		(void) iceberg_tt_query_walker((Node *) query, NULL);
}

/* ------------------------------------------------------------------------
 * Fallback path as a CustomScan
 *
 * A schema-changed snapshot (a column dropped/renamed/retyped after it) stays
 * a FunctionScan because a relation scan can only emit the table's current
 * columns.  A plain FunctionScan on iceberg_snapshot_scan is opaque to the
 * planner (no predicate pushdown, "Function Scan" in EXPLAIN).  Rewrite it, on
 * the QD in the planner hook, into a self-contained Iceberg CustomScan that
 * carries the snapshot's own columns in custom_scan_tlist and reads the data
 * files with the same volume-fdw reader the SRF uses -- so the historical read
 * shows up as an "Iceberg Snapshot Scan" and can push predicates down, while
 * the Gather Motion the FunctionScan already had (EXECUTE ON ALL SEGMENTS)
 * carries the MPP distribution unchanged.  The function has no base relation,
 * so the CustomScan uses scanrelid = 0 and INDEX_VAR-based scan Vars.
 * ------------------------------------------------------------------------
 */
#define ICEBERG_TT_CUSTOM_SCAN_NAME "Iceberg Snapshot Scan"

typedef struct IcebergTTScanState
{
	CustomScanState		css;		/* must be first */
	ForeignScanState   *fss;
	Relation			rel;
	bool				opened;
}			IcebergTTScanState;

static Node *IcebergTTCreateCustomScanState(CustomScan *cscan);
static void IcebergTTBeginCustomScan(CustomScanState *node, EState *estate,
									 int eflags);
static TupleTableSlot *IcebergTTExecCustomScan(CustomScanState *node);
static void IcebergTTEndCustomScan(CustomScanState *node);
static void IcebergTTReScanCustomScan(CustomScanState *node);

static CustomScanMethods IcebergTTCustomScanMethods = {
	ICEBERG_TT_CUSTOM_SCAN_NAME,
	IcebergTTCreateCustomScanState,
};

static CustomExecMethods IcebergTTCustomExecMethods = {
	.CustomName			= ICEBERG_TT_CUSTOM_SCAN_NAME,
	.BeginCustomScan	= IcebergTTBeginCustomScan,
	.ExecCustomScan		= IcebergTTExecCustomScan,
	.EndCustomScan		= IcebergTTEndCustomScan,
	.ReScanCustomScan	= IcebergTTReScanCustomScan,
};

static Node *
IcebergTTCreateCustomScanState(CustomScan *cscan)
{
	IcebergTTScanState *s;

	s = (IcebergTTScanState *) newNode(sizeof(IcebergTTScanState),
									   T_CustomScanState);
	s->css.methods = &IcebergTTCustomExecMethods;
	s->fss = NULL;
	s->rel = NULL;
	s->opened = false;
	return (Node *) s;
}

static void
IcebergTTBeginCustomScan(CustomScanState *node, EState *estate, int eflags)
{
	/*
	 * Nothing to resolve here: relid + the QD-resolved fragment list were baked
	 * into custom_private at plan-rewrite time (the snapshot is immutable, so
	 * they are stable across re-executions of a cached plan).  The volume-fdw
	 * reader is opened lazily on the first tuple (a segment slice's scan is not
	 * driven on the QD, so the QD never opens a reader).
	 */
}

/* Open the volume-fdw reader for the snapshot, once, on first access. */
static void
iceberg_tt_ensure_open(IcebergTTScanState *s)
{
	CustomScan *cscan;
	Oid			relid;
	char	   *fragments;
	TupleDesc	scan_tupdesc;

	if (s->opened)
		return;

	cscan = (CustomScan *) s->css.ss.ps.plan;
	relid = DatumGetObjectId(((Const *) linitial(cscan->custom_private))->constvalue);
	fragments = strVal(lsecond(cscan->custom_private));
	scan_tupdesc = s->css.ss.ss_ScanTupleSlot->tts_tupleDescriptor;

	/*
	 * Re-check SELECT at execution.  This CustomScan replaced the SRF, so the
	 * SRF body's runtime ACL check no longer runs, and a scanrelid = 0 scan
	 * has no relation RTE for ExecCheckRTPerms either -- without this, a plan
	 * prepared by a privileged role could be executed after SET ROLE by one
	 * without SELECT.
	 */
	{
		AclResult	aclresult;

		aclresult = pg_class_aclcheck(relid, GetUserId(), ACL_SELECT);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult, OBJECT_TABLE, get_rel_name(relid));
	}

	/* Snapshot field-ids (3rd entry; absent on plans from older QDs). */
	{
		int	   *fids = NULL;
		int		nfids = 0;

		if (list_length(cscan->custom_private) >= 3)
			fids = tt_parse_fieldid_csv(strVal(lthird(cscan->custom_private)),
										&nfids);

		s->rel = table_open(relid, AccessShareLock);
		s->fss = pg_iceberg_snapshot_beginscan(s->rel, scan_tupdesc, fragments,
											   fids, nfids);
	}
	s->opened = true;
}

static TupleTableSlot *
IcebergTTAccessScan(CustomScanState *node)
{
	IcebergTTScanState *s = (IcebergTTScanState *) node;
	TupleTableSlot *fdwslot;
	TupleTableSlot *scanslot = node->ss.ss_ScanTupleSlot;

	iceberg_tt_ensure_open(s);
	fdwslot = s->fss->fdwroutine->IterateForeignScan(s->fss);
	if (TupIsNull(fdwslot))
		return NULL;

	/*
	 * The volume-fdw reader fills its own heap-tuple slot, but the core set up
	 * this CustomScan's scan slot as virtual (scanrelid = 0 + custom_scan_tlist),
	 * and downstream expression/aggregate evaluation asserts a virtual input
	 * slot.  Copy the tuple into our own scan slot and return that.
	 */
	return ExecCopySlot(scanslot, fdwslot);
}

static bool
IcebergTTRecheckScan(CustomScanState *node, TupleTableSlot *slot)
{
	return true;
}

static TupleTableSlot *
IcebergTTExecCustomScan(CustomScanState *node)
{
	/* ExecScan re-applies the scan qual and the projection. */
	return ExecScan(&node->ss,
					(ExecScanAccessMtd) IcebergTTAccessScan,
					(ExecScanRecheckMtd) IcebergTTRecheckScan);
}

static void
IcebergTTEndCustomScan(CustomScanState *node)
{
	IcebergTTScanState *s = (IcebergTTScanState *) node;

	if (s->fss != NULL)
	{
		s->fss->fdwroutine->EndForeignScan(s->fss);
		s->fss = NULL;
	}
	if (s->rel != NULL)
	{
		/* NoLock: the resource owner releases the lock at transaction end. */
		table_close(s->rel, NoLock);
		s->rel = NULL;
	}
	s->opened = false;
}

static void
IcebergTTReScanCustomScan(CustomScanState *node)
{
	IcebergTTScanState *s = (IcebergTTScanState *) node;

	if (s->fss != NULL && s->fss->fdwroutine->ReScanForeignScan != NULL)
		s->fss->fdwroutine->ReScanForeignScan(s->fss);
}

/*
 * If `plan` is a FunctionScan on a schema-changed iceberg_snapshot_scan() call,
 * return an equivalent Iceberg CustomScan; otherwise return `plan` unchanged.
 * Called on the QD from the Iceberg planner hook (see replace_iceberg_seqscan).
 */
Plan *
iceberg_tt_try_make_custom_scan(Plan *plan, List *rtable)
{
	FunctionScan	   *fscan;
	RangeTblFunction   *rtf;
	FuncExpr		   *fexpr;
	Oid					relid;
	int64				snapshot_id;
	bool				isnull;
	Relation			rel;
	char			   *fragments;
	char			   *fids_csv;
	List			   *tlist = NIL;
	CustomScan		   *cscan;
	int					i;

	if (!IsA(plan, FunctionScan))
		return plan;
	fscan = (FunctionScan *) plan;
	/* Use the plan node's own function list (authoritative post-planning). */
	if (list_length(fscan->functions) != 1)
		return plan;
	rtf = (RangeTblFunction *) linitial(fscan->functions);
	if (rtf->funcexpr == NULL || !IsA(rtf->funcexpr, FuncExpr))
		return plan;
	fexpr = (FuncExpr *) rtf->funcexpr;
	if (!tt_is_snapshot_scan_func(fexpr->funcid) ||
		list_length(fexpr->args) < 2)
		return plan;
	/* Need the describe-resolved column set to build custom_scan_tlist. */
	if (rtf->funccolcount <= 0 || rtf->funccoltypes == NIL)
		return plan;
	if (tt_arg_not_constant((Node *) linitial(fexpr->args)) ||
		tt_selector_arg_not_constant((Node *) lsecond(fexpr->args)))
		return plan;

	relid = DatumGetObjectId(ExecEvalFunctionArgToConst(fexpr, 0, &isnull));
	if (isnull || !tt_is_iceberg_relation(relid))
		return plan;
	snapshot_id = tt_snapshot_id_from_arg(fexpr, relid);

	/* Resolve the snapshot's fragment list on the QD (agent is QD-only). */
	rel = relation_open(relid, AccessShareLock);
	tt_assert_call_schema_current(rtf, rel, snapshot_id);
	fragments = pg_iceberg_list_data_fragments_json(rel, snapshot_id);
	fids_csv = tt_build_fieldid_csv(rel, snapshot_id);
	relation_close(rel, AccessShareLock);
	if (fragments == NULL)
		return plan;

	/*
	 * custom_scan_tlist = one entry per snapshot column, as INDEX_VAR Vars (the
	 * scan-slot namespace the core uses for a scanrelid = 0 CustomScan).  Types
	 * come from the describe result recorded on the RangeTblFunction.
	 */
	for (i = 0; i < rtf->funccolcount; i++)
	{
		Oid		typid = list_nth_oid(rtf->funccoltypes, i);
		int32	typmod = list_nth_int(rtf->funccoltypmods, i);
		Oid		coll = list_nth_oid(rtf->funccolcollations, i);
		char   *cname = strVal(list_nth(rtf->funccolnames, i));
		Var	   *var = makeVar(INDEX_VAR, (AttrNumber) (i + 1),
								 typid, typmod, coll, 0);

		tlist = lappend(tlist,
						makeTargetEntry((Expr *) var, (AttrNumber) (i + 1),
										pstrdup(cname), false));
	}

	cscan = makeNode(CustomScan);
	/* Struct-copy the Plan header (targetlist, qual, flow, costs, plan_rows). */
	cscan->scan.plan = fscan->scan.plan;
	cscan->scan.plan.type = T_CustomScan;
	cscan->scan.scanrelid = 0;			/* no base relation */
	cscan->flags = 0;
	cscan->custom_plans = NIL;
	cscan->custom_exprs = NIL;
	/*
	 * [relid, fragments, field-ids csv].  The third entry is the snapshot
	 * schema's field-ids for the scan tupdesc; "" when the agent predates the
	 * fieldId key (the reader then falls back to its positional default).
	 */
	cscan->custom_private =
		list_make3(makeConst(OIDOID, -1, InvalidOid, sizeof(Oid),
							  ObjectIdGetDatum(relid), false, true),
				   makeString(fragments),
				   makeString(fids_csv != NULL ? fids_csv : pstrdup("")));
	cscan->custom_scan_tlist = tlist;
	cscan->custom_relids = bms_make_singleton(fscan->scan.scanrelid);
	cscan->methods = &IcebergTTCustomScanMethods;

	/*
	 * The copied targetlist/qual reference the function via its range-table
	 * index; for a scanrelid = 0 CustomScan the scan tuple lives in the
	 * INDEX_VAR namespace, so retarget those Vars (positions are unchanged --
	 * custom_scan_tlist mirrors the function's output columns 1:1).  Copy first
	 * so the discarded FunctionScan's nodes are not mutated in place.
	 */
	cscan->scan.plan.targetlist = copyObject(fscan->scan.plan.targetlist);
	cscan->scan.plan.qual = copyObject(fscan->scan.plan.qual);
	ChangeVarNodes((Node *) cscan->scan.plan.targetlist,
				   fscan->scan.scanrelid, INDEX_VAR, 0);
	ChangeVarNodes((Node *) cscan->scan.plan.qual,
				   fscan->scan.scanrelid, INDEX_VAR, 0);

	return (Plan *) cscan;
}

/* ------------------------------------------------------------------------
 * Per-snapshot base-table cardinality for the planner (issue #413)
 *
 * A rewritten time-travel scan is an ordinary relation scan, so the planner
 * sizes it from pg_class.reltuples -- which describes HEAD.  Reading a 5-row
 * snapshot of a 200 000-row table is therefore estimated at 200 000 rows, and
 * the resulting join order / motion type is chosen for data that is not there
 * (the same failure mode as issue #339, four orders of magnitude wide).
 * pg_class has one slot per relid and a historical snapshot cannot be ANALYZEd,
 * so the count has to reach the planner out of band.
 *
 * The row count comes from the snapshot's own summary map ("total-records"),
 * already fetched and cached by tt_fetch_snapshot_list -- a manifest-list level
 * aggregate, not a walk of the data files, so the cost does not grow with the
 * table.
 *
 * Two-stage on purpose:
 *
 *   1. The planner hook prefetches every snapshot mentioned by the statement
 *      into a stack-allocated frame BEFORE handing the query to the planner.
 *      This is the only place that may talk to the agent or raise.
 *   2. get_relation_info_hook then does an allocation-free, non-throwing lookup
 *      and edits rel->tuples / rel->pages.
 *
 * The frame lives in the planner hook's stack frame and is popped in its
 * PG_FINALLY, so nested planning (a SQL/plpgsql function body re-entering the
 * planner) nests correctly and an error cannot leave a stale registry behind.
 * Building it in the planner hook rather than at parse analysis is what makes
 * PREPARE/EXECUTE work: a cached plan is re-planned without re-running parse
 * analysis, but always re-enters the planner.
 *
 * Identity is (relid, alias), never the alias alone: the alias is user-writable
 * text, and a plain table spelled "AS __icetts_1_1" must not pick up another
 * relation's snapshot statistics.  A lookup miss is a no-op, so an unregistered
 * or hand-written alias simply keeps the catalog estimate.
 * ------------------------------------------------------------------------
 */
static get_relation_info_hook_type prev_get_relation_info_hook = NULL;
static IcebergTTStatsFrame *tt_stats_top = NULL;

/*
 * One registered relation.  total_rows is the whole-table count, not a
 * per-segment share, and is an UPPER BOUND when the snapshot carries delete
 * files (see tt_stats_resolve).
 */
typedef struct IcebergTTRelStats
{
	Oid			relid;
	int64		snapshot_id;
	char	   *alias;			/* the __icetts_ token the rewrite stamped */
	double		total_rows;
	BlockNumber	pages;			/* 0 when the summary gives no size */
	bool		rows_valid;		/* false: no usable count, leave the estimate */
} IcebergTTRelStats;

/*
 * Collect the time-travel relations of one query level.  Pure bookkeeping: no
 * agent traffic, no catalog scan beyond the relcache lookup that identifies an
 * Iceberg relation.
 */
static void
tt_stats_collect_rtable(List *rtable, List **targets)
{
	ListCell   *lc;

	foreach(lc, rtable)
	{
		RangeTblEntry	   *rte = (RangeTblEntry *) lfirst(lc);
		IcebergTTRelStats  *st;
		ListCell		   *lc2;
		int64				snapshot_id;
		bool				dup = false;

		if (rte->rtekind != RTE_RELATION || rte->eref == NULL)
			continue;
		if (!iceberg_tt_parse_alias(rte->eref->aliasname, &snapshot_id))
			continue;
		/*
		 * snapshot_id <= 0 is a HEAD read stamped by the rewrite; pg_class
		 * already describes HEAD, so there is nothing to override.
		 */
		if (snapshot_id <= 0)
			continue;
		if (!tt_is_iceberg_relation(rte->relid))
			continue;

		foreach(lc2, *targets)
		{
			st = (IcebergTTRelStats *) lfirst(lc2);
			if (st->relid == rte->relid &&
				strcmp(st->alias, rte->eref->aliasname) == 0)
			{
				dup = true;
				break;
			}
		}
		if (dup)
			continue;

		st = (IcebergTTRelStats *) palloc0(sizeof(IcebergTTRelStats));
		st->relid = rte->relid;
		st->snapshot_id = snapshot_id;
		st->alias = pstrdup(rte->eref->aliasname);
		*targets = lappend(*targets, st);
	}
}

static bool
tt_stats_query_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Query))
	{
		Query	   *q = (Query *) node;

		if (q->commandType == CMD_UTILITY)
			return false;
		tt_stats_collect_rtable(q->rtable, (List **) context);
		return query_tree_walker(q, tt_stats_query_walker, context, 0);
	}
	return expression_tree_walker(node, tt_stats_query_walker, context);
}

/*
 * Fill in one target's row count from its snapshot summary.  May raise (agent
 * round trip); the caller contains that.
 *
 * Delete files are never subtracted, so on a snapshot that has them the count
 * is an upper bound.  An equality-delete predicate can match zero rows or many
 * and a position delete can be redundant, so subtracting the recorded delete
 * counts is as likely to under- as to over-correct -- and would manufacture a
 * zero-row estimate for the common "everything deleted" snapshot, whose summary
 * still reports every record its data files hold.
 */
static void
tt_stats_resolve(IcebergTTRelStats *st)
{
	Relation			rel;
	IcebergSnapshotList	sl;
	const char		   *summary = NULL;
	int64				records = 0;
	int64				files_size = 0;
	int					i;

	rel = table_open(st->relid, AccessShareLock);
	tt_fetch_snapshot_list(rel, &sl);
	table_close(rel, AccessShareLock);

	for (i = 0; i < sl.nsnapshots; i++)
	{
		if (sl.snapshots[i].snapshot_id == st->snapshot_id)
		{
			summary = sl.snapshots[i].summary_json;
			break;
		}
	}
	/* Unknown snapshot, or a summary without a record count: keep the default. */
	if (summary == NULL ||
		!pg_iceberg_summary_int64(summary, "total-records", &records) ||
		records < 0)
		return;

	st->total_rows = (double) records;
	st->rows_valid = true;

	/*
	 * total-files-size is the snapshot's on-disk footprint; convert it the same
	 * way pg_iceberg_refresh_pg_class_stats converts bytesInDataFile, so a
	 * historical scan and a HEAD scan are costed on one scale.  Absent, the
	 * catalog's page count is left alone rather than guessed.
	 */
	if (pg_iceberg_summary_int64(summary, "total-files-size", &files_size) &&
		files_size > 0)
	{
		int64	pages = (files_size + (BLCKSZ - 1)) / BLCKSZ;

		/* BlockNumber is 32-bit; a >32TB snapshot would wrap to a tiny count. */
		st->pages = (BlockNumber) Min(Max(pages, 1), (int64) MaxBlockNumber);
	}
}

/*
 * Resolve every target of one planner invocation.  Guarded as a unit: a target
 * resolved before the failure keeps its statistics, the rest stay on the
 * catalog estimate.
 */
static void
tt_stats_resolve_cb(void *arg)
{
	ListCell   *lc;

	foreach(lc, (List *) arg)
		tt_stats_resolve((IcebergTTRelStats *) lfirst(lc));
}

/* Look up a registered override.  Allocation-free and non-throwing. */
static IcebergTTRelStats *
tt_stats_lookup(Oid relid, const char *alias)
{
	ListCell   *lc;

	if (tt_stats_top == NULL || alias == NULL)
		return NULL;

	foreach(lc, tt_stats_top->entries)
	{
		IcebergTTRelStats *st = (IcebergTTRelStats *) lfirst(lc);

		if (st->rows_valid && st->relid == relid &&
			strcmp(st->alias, alias) == 0)
			return st;
	}
	return NULL;
}

static void
iceberg_tt_get_relation_info(PlannerInfo *root, Oid relationObjectId,
							 bool inhparent, RelOptInfo *rel)
{
	RangeTblEntry	   *rte;
	IcebergTTRelStats  *st;

	if (prev_get_relation_info_hook)
		prev_get_relation_info_hook(root, relationObjectId, inhparent, rel);

	if (tt_stats_top == NULL || tt_stats_top->entries == NIL)
		return;
	/*
	 * For an inheritance parent core deliberately leaves the size estimates
	 * empty -- set_append_rel_size() builds them from the children -- so there
	 * is nothing here to override.
	 */
	if (inhparent)
		return;
	if (root->simple_rte_array == NULL || rel->relid == 0 ||
		rel->relid >= (Index) root->simple_rel_array_size)
		return;
	rte = root->simple_rte_array[rel->relid];
	if (rte == NULL || rte->rtekind != RTE_RELATION ||
		rte->relid != relationObjectId || rte->eref == NULL)
		return;

	st = tt_stats_lookup(relationObjectId, rte->eref->aliasname);
	if (st == NULL)
		return;

	rel->tuples = st->total_rows;
	if (st->pages > 0)
	{
		rel->pages = st->pages;
		/*
		 * relallvisible describes HEAD's pages and has no meaning against a
		 * different page count; drop it rather than rescale a number that was
		 * never measured for this snapshot.
		 */
		rel->allvisfrac = 0;
	}
}

/*
 * Planner-hook entry point: prefetch the statistics of every snapshot this
 * statement reads and push them as the active frame.  Paired with
 * iceberg_tt_stats_end in the caller's PG_FINALLY.
 */
void
iceberg_tt_stats_begin(IcebergTTStatsFrame *frame, Query *parse)
{
	List	   *targets = NIL;

	/* Push first, so the caller's PG_FINALLY unwinds correctly if we raise. */
	frame->entries = NIL;
	frame->outer = tt_stats_top;
	tt_stats_top = frame;

	/* Only the QD parses user SQL and can reach the catalog service. */
	if (Gp_role == GP_ROLE_EXECUTE || parse == NULL)
		return;

	(void) tt_stats_query_walker((Node *) parse, (void *) &targets);
	if (targets == NIL)
		return;

	(void) tt_run_guarded(tt_stats_resolve_cb, (void *) targets);
	frame->entries = targets;
}

void
iceberg_tt_stats_end(IcebergTTStatsFrame *frame)
{
	IcebergTTStatsFrame *f;

	/*
	 * Pop to `frame`'s outer.  Walking the chain rather than asserting
	 * tt_stats_top == frame keeps an unbalanced nested push (which would only
	 * arise from a bug) from stranding a dead frame as the active registry.
	 */
	for (f = tt_stats_top; f != NULL; f = f->outer)
	{
		if (f == frame)
		{
			tt_stats_top = frame->outer;
			return;
		}
	}
}

void
iceberg_tt_install_hooks(void)
{
	RegisterCustomScanMethods(&IcebergTTCustomScanMethods);
	prev_post_parse_analyze_hook = post_parse_analyze_hook;
	post_parse_analyze_hook = iceberg_tt_post_parse_analyze;
	prev_get_relation_info_hook = get_relation_info_hook;
	get_relation_info_hook = iceberg_tt_get_relation_info;
}
