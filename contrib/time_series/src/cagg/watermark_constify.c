/*-------------------------------------------------------------------------
 *
 * watermark_constify.c
 *    Planner-time const-folding of cagg_watermark() calls in CAGG view
 *    queries.
 *
 *    Background: cagg_watermark(int) is declared VOLATILE because each
 *    segment reads its LOCAL cagg_watermark row (the table is
 *    DISTRIBUTED RANDOMLY).  VOLATILE prevents the PG planner from
 *    const-folding, so the filter
 *
 *        WHERE bucket >= cagg_watermark(N)
 *
 *    inside the cv_* view's live branch ends up applied POST-aggregate.
 *    The optimizer can't push it down to ChunkScan -> no chunk pruning
 *    -> live branch reads the entire user-WHERE window from source.
 *
 *    This file replaces VOLATILE-driven runtime evaluation with a
 *    planner walker that:
 *
 *      1. Walks the parse tree finding `cagg_watermark(<const_id>)`
 *         FuncExpr nodes (TSDB-style approach).
 *
 *      2. For each one, evaluates the *global MIN* watermark across
 *         all segments (QD-side SPI dispatch).  Global MIN is the
 *         safe lower bound for "all segments materialized up to here"
 *         -- correctness arg: any bucket >= global MIN is allowed to
 *         go through the live branch, which always re-aggregates
 *         from source (correct regardless of mat state).  Fast
 *         segments do slightly more live work, slow segments do same.
 *
 *      3. Physically replaces the FuncExpr node with a Const node
 *         containing the computed value.  Plan dispatched to segments
 *         carries the literal -- each segment's ChunkScan can use it
 *         for chunk pruning.
 *
 *    Result: live-branch scan rows drop from "user window size" to
 *    "user window above watermark" -- typically 5-10x reduction.
 *
 *    The original cagg_watermark() VOLATILE function is preserved
 *    untouched.  Refresh logic, segment-local writes, and any other
 *    code path that needs per-segment local watermark continues to
 *    work -- the walker only fires on user-query planning, identified
 *    by detecting the function call in a SELECT parse tree.
 *
 * Copyright (c) 2026 HashData Inc.
 * Licensed under Apache License 2.0
 *
 * IDENTIFICATION
 *    contrib/time_series/src/cagg/watermark_constify.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "catalog/namespace.h"
#include "datatype/timestamp.h"
#include "executor/spi.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/syscache.h"
#include "cdb/cdbvars.h"

#include "../include/time_series.h"
#include "../include/cagg/cagg.h"

/* Cached OID of the cagg_watermark function -- looked up once per backend */
static Oid	cagg_watermark_func_oid = InvalidOid;

/*
 * Look up the cagg_watermark(int) function OID, cached for the rest of
 * the backend's lifetime.  Returns InvalidOid if the extension is not
 * installed (in which case the caller should skip the walker).
 */
static Oid
get_cagg_watermark_func_oid(void)
{
	Oid			namespace_oid;
	Oid			argtypes[1] = {INT4OID};
	oidvector  *args;
	HeapTuple	tuple;

	if (OidIsValid(cagg_watermark_func_oid))
		return cagg_watermark_func_oid;

	namespace_oid = ht_get_namespace_oid_cached();
	if (!OidIsValid(namespace_oid))
		return InvalidOid;

	args = buildoidvector(argtypes, 1);
	tuple = SearchSysCache3(PROCNAMEARGSNSP,
							CStringGetDatum("cagg_watermark"),
							PointerGetDatum(args),
							ObjectIdGetDatum(namespace_oid));
	pfree(args);

	if (HeapTupleIsValid(tuple))
	{
		cagg_watermark_func_oid = ((Form_pg_proc) GETSTRUCT(tuple))->oid;
		ReleaseSysCache(tuple);
	}
	return cagg_watermark_func_oid;
}

/*
 * Backend-local watermark cache (cross-query, sinval-invalidated).
 *
 * The SPI dispatch below costs ~2.4 ms per planning (subtransaction + plan
 * + cross-segment Gather).  For dashboard-style short queries that is
 * ~40% of total latency.  This cache reduces it to a local hash lookup
 * for the common READ COMMITTED case.
 *
 * Coherence protocol: plain PG sinval.  cagg_refresh sends
 * CacheInvalidateRelcacheByRelid(cagg_watermark-table) after advancing
 * the watermark; sinval messages are TRANSACTIONAL (broadcast at commit,
 * discarded on abort), so a cached value can never reflect an aborted
 * refresh.  Our relcache callback below clears the cache on that relid
 * (and on relid == InvalidOid, the queue-overflow reset).  Next planning
 * falls through to the SPI path and re-fills.
 *
 * Why a stale (lower) value is safe: the watermark is MONOTONIC
 * (cagg_advance_watermark uses GREATEST).  The constified value only
 * sets the mat/live split point of the UNION ALL view -- a lower split
 * means a few buckets are re-aggregated live instead of read from the
 * mat table; the result set is identical.  The dangerous direction is
 * only a value HIGHER than what the query's snapshot can see in the mat
 * table, which cannot arise from a committed-then-cached value under
 * READ COMMITTED, because RC takes its execution snapshot in
 * PortalStart, AFTER planning -- i.e. the snapshot is always at least
 * as new as anything we cached.
 *
 * REPEATABLE READ / SERIALIZABLE are the exception: the transaction
 * snapshot may PREDATE a refresh whose (newer) watermark sits in this
 * cache; using it would claim mat coverage the snapshot cannot see.
 * Those isolation levels therefore BYPASS the cache entirely and take
 * the SPI path, whose read_only mode uses the prevailing transaction
 * snapshot -- watermark and mat contents stay mutually consistent.
 * (TimescaleDB's ts_cagg_watermark_get documents the same constraint:
 * the watermark "must be done using the transaction snapshot".)
 */

typedef struct WmCacheEntry
{
	int32		cagg_id;		/* hash key */
	TimestampTz wm;
}			WmCacheEntry;

static HTAB *wm_backend_cache = NULL;

/*
 * Relid of time_series.cagg_watermark, (re)learned on every cache
 * store (a path where catalog access is safe).  The relcache callback
 * compares incoming invalidation relids against it; callbacks must not
 * touch the catalogs themselves.
 */
static Oid	wm_table_relid = InvalidOid;

static bool
wm_cache_lookup(int32 cagg_id, TimestampTz *wm)
{
	WmCacheEntry *entry;

	if (wm_backend_cache == NULL)
		return false;

	entry = hash_search(wm_backend_cache, &cagg_id, HASH_FIND, NULL);
	if (entry == NULL)
		return false;

	*wm = entry->wm;
	return true;
}

static void
wm_cache_store(int32 cagg_id, TimestampTz wm)
{
	WmCacheEntry *entry;
	bool		found;

	if (wm_backend_cache == NULL)
	{
		HASHCTL		ctl;

		memset(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(int32);
		ctl.entrysize = sizeof(WmCacheEntry);
		ctl.hcxt = CacheMemoryContext;
		wm_backend_cache = hash_create("cagg watermark cache", 16, &ctl,
									   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}

	/*
	 * Refresh the relid the invalidation callback matches against.  Doing it
	 * on every store keeps it correct across DROP/CREATE EXTENSION within one
	 * backend (the old table's inval cleared the cache; the next store learns
	 * the new table's relid).
	 */
	wm_table_relid = get_relname_relid("cagg_watermark",
									   ht_get_namespace_oid_cached());

	entry = hash_search(wm_backend_cache, &cagg_id, HASH_ENTER, &found);

	/*
	 * Monotonic max: never let a concurrent older read overwrite a newer
	 * cached watermark.  Backfills come from committed reads, so the max is
	 * always a committed value.
	 */
	if (!found || wm > entry->wm)
		entry->wm = wm;
}

/*
 * Relcache invalidation callback: clear the whole cache when the
 * cagg_watermark table is invalidated (sent by cagg_refresh after a
 * watermark advance) or on a full-cache reset (relid == InvalidOid,
 * e.g. sinval queue overflow).  Pure memory ops only -- callbacks run
 * in contexts where catalog access is not safe.
 */
static void
wm_cache_relcache_callback(Datum arg, Oid relid)
{
	if (wm_backend_cache == NULL)
		return;

	if (relid != InvalidOid && relid != wm_table_relid)
		return;

	hash_destroy(wm_backend_cache);
	wm_backend_cache = NULL;
}

/*
 * Register the invalidation callback.  Called once from _PG_init --
 * under shared_preload_libraries the registration in the postmaster is
 * inherited by every forked backend.
 */
void
cagg_watermark_cache_init(void)
{
	CacheRegisterRelcacheCallback(wm_cache_relcache_callback, (Datum) 0);
}

/*
 * Planner-hook helper: any SQL-level DML whose target is the
 * cagg_watermark table broadcasts the cache invalidation itself.
 *
 * Refresh and the TRUNCATE hook send their invals explicitly, but the
 * watermark can also be written by hand (our own regression tests do
 * it in ~20 places to stage above/below-watermark scenarios, and an
 * operator can do the same).  CBDB gives us no statement-level
 * triggers ("Triggers for statements are not yet supported") and row
 * triggers fire on segments, whose sinval traffic never reaches QD
 * backends -- but every SQL DML statement IS planned on the QD, and we
 * are already in the planner hook chain.  Queueing the inval here puts
 * it in the writer's own transaction: broadcast at its commit,
 * discarded on abort, exactly like the explicit call sites.
 *
 * Limits (acceptable): DML hidden inside a writable CTE is not
 * detected (resultRelation == 0), and segment-local direct heap writes
 * (the _cagg_init_segment_* functions) bypass planning entirely -- both
 * only ever touch rows for brand-new cagg_ids that no backend has
 * cached yet.
 */
void
cagg_watermark_dml_inval(Query *parse)
{
	RangeTblEntry *rte;
	Oid			relid;

	if (parse == NULL || Gp_role != GP_ROLE_DISPATCH)
		return;
	if (parse->commandType != CMD_UPDATE &&
		parse->commandType != CMD_DELETE &&
		parse->commandType != CMD_INSERT)
		return;
	if (parse->resultRelation == 0)
		return;

	rte = (RangeTblEntry *) list_nth(parse->rtable,
									 parse->resultRelation - 1);
	if (rte == NULL || rte->rtekind != RTE_RELATION ||
		!OidIsValid(rte->relid))
		return;
	relid = rte->relid;

	/*
	 * Resolve the watermark table's relid lazily (once per backend in the
	 * common case; re-probed while the extension is absent, which costs one
	 * negative syscache lookup per DML statement).
	 */
	if (!OidIsValid(wm_table_relid))
	{
		Oid			ns_oid = ht_get_namespace_oid_cached();

		if (!OidIsValid(ns_oid))
			return;
		wm_table_relid = get_relname_relid("cagg_watermark", ns_oid);
		if (!OidIsValid(wm_table_relid))
			return;
	}

	if (relid == wm_table_relid)
		CacheInvalidateRelcacheByRelid(relid);
}

/*
 * Compute the global MIN(watermark) across all segments for a given
 * cagg_id via SPI dispatch.  Called from the planner on QD.
 *
 * Returns true on success and writes the watermark into *result.
 * Returns false when the SPI fails -- most commonly because the calling
 * role lacks SELECT on time_series.cagg_watermark (PG view-owner
 * permission model: a user with SELECT on the cv view but no direct
 * access to the time_series schema is legitimate, so we must NOT bubble
 * the permission error up).  In that case the caller MUST skip the
 * constify and leave the FuncExpr in place so the per-segment executor
 * runs cagg_watermark() at runtime (under the original VOLATILE
 * permission semantics).
 *
 * Other SPI errors (e.g. cagg_watermark table missing during a half-
 * installed extension) are also caught here for symmetry -- the safe
 * fall-back is identical: keep the FuncExpr, let the executor decide.
 *
 * An empty result set (cagg_id not in catalog) is NOT an error; we
 * return DT_NOBEGIN, which means "nothing materialized, everything
 * goes through live".
 */
static bool
get_global_watermark(int cagg_id, TimestampTz *result)
{
	MemoryContext oldcontext = CurrentMemoryContext;
	ResourceOwner oldowner = CurrentResourceOwner;
	volatile bool success = false;

	/*
	 * Wrap the SPI call in a subtransaction so a permission ereport inside
	 * SPI doesn't tear down the surrounding planner state.
	 */
	BeginInternalSubTransaction(NULL);
	PG_TRY();
	{
		int			rc;
		Oid			argtypes[1] = {INT4OID};
		Datum		args[1];
		bool		isnull;

		args[0] = Int32GetDatum(cagg_id);

		if ((rc = SPI_connect()) != SPI_OK_CONNECT)
			elog(ERROR, "constify_cagg_watermark: SPI_connect failed: %d", rc);

		/*
		 * CBDB dispatches this query to all segments (cagg_watermark table is
		 * DISTRIBUTED RANDOMLY).  Each segment returns its local MIN,
		 * coordinator aggregates final MIN across them.
		 */
		rc = SPI_execute_with_args(
								   "SELECT MIN(watermark) FROM time_series.cagg_watermark "
								   "WHERE cagg_id = $1",
								   1, argtypes, args, NULL, true, 1);

		if (rc != SPI_OK_SELECT || SPI_processed == 0)
		{
			TIMESTAMP_NOBEGIN(*result);
		}
		else
		{
			Datum		d = SPI_getbinval(SPI_tuptable->vals[0],
										  SPI_tuptable->tupdesc,
										  1, &isnull);

			if (isnull)
				TIMESTAMP_NOBEGIN(*result);
			else
				*result = DatumGetTimestampTz(d);
		}

		SPI_finish();
		ReleaseCurrentSubTransaction();
		success = true;
	}
	PG_CATCH();
	{
		/*
		 * Discard the error (typically ERRCODE_INSUFFICIENT_PRIVILEGE for
		 * users without time_series schema access) and roll back the
		 * subtransaction.  The caller will see success=false and leave the
		 * cagg_watermark() FuncExpr untouched.
		 */
		MemoryContextSwitchTo(oldcontext);
		FlushErrorState();
		RollbackAndReleaseCurrentSubTransaction();
		success = false;
	}
	PG_END_TRY();

	MemoryContextSwitchTo(oldcontext);
	CurrentResourceOwner = oldowner;

	return success;
}

/*
 * Context threaded through the constify mutator: the cached
 * cagg_watermark() OID to match calls against and a running count of
 * how many calls were replaced (used only for the DEBUG2 trace).
 *
 * Per-planning memo of (cagg_id -> global watermark): the cv view
 * references cagg_watermark(N) twice (mat branch `bucket < wm`, live
 * branch `bucket >= wm`), so without this every cv-view planning would
 * dispatch the same cross-segment MIN(watermark) SPI twice.  The memo is
 * a stack variable living only for one constify_cagg_watermark_mutate()
 * call, so there is nothing to invalidate across queries: a concurrent
 * refresh that advances the watermark is observed by the next planning,
 * exactly as before.  A small fixed array keyed by cagg_id covers the
 * realistic cases (one cagg, or a handful joined); distinct caggs beyond
 * the array fall back to a direct lookup -- correct, just not memoized.
 */
#define CONSTIFY_WM_CACHE_MAX 8

typedef struct
{
	Oid			cagg_watermark_oid;
	int			replaced_count;
	int			n_cached;
	bool		use_backend_cache;	/* false under RR/SERIALIZABLE -- see the
									 * backend-cache block comment above */
	int32		cached_id[CONSTIFY_WM_CACHE_MAX];
	TimestampTz cached_wm[CONSTIFY_WM_CACHE_MAX];
}			ConstifyContext;

/*
 * Mutator-based replacement: walks the parse tree and returns a new
 * tree where each cagg_watermark FuncExpr is replaced with a Const.
 *
 * Used instead of in-place mutation to avoid parent-link tracking.
 */
static Node *
constify_mutator(Node *node, ConstifyContext * ctx)
{
	if (node == NULL)
		return NULL;

	if (IsA(node, FuncExpr))
	{
		FuncExpr   *fe = (FuncExpr *) node;

		if (fe->funcid == ctx->cagg_watermark_oid &&
			list_length(fe->args) == 1 &&
			IsA(linitial(fe->args), Const))
		{
			Const	   *arg = (Const *) linitial(fe->args);

			if (!arg->constisnull)
			{
				int32		cagg_id = DatumGetInt32(arg->constvalue);
				TimestampTz watermark = DT_NOBEGIN;
				int			i;
				bool		found = false;

				/*
				 * Reuse a watermark already looked up in this planning pass.
				 * The cv view references cagg_watermark(N) in both UNION ALL
				 * branches, so this collapses the two identical cross-segment
				 * SPI dispatches into one per distinct cagg_id.
				 */
				for (i = 0; i < ctx->n_cached; i++)
				{
					if (ctx->cached_id[i] == cagg_id)
					{
						watermark = ctx->cached_wm[i];
						found = true;
						break;
					}
				}

				if (!found)
				{
					/*
					 * Second level: the cross-query backend cache
					 * (sinval-invalidated; bypassed under RR/SERIALIZABLE,
					 * see the cache block comment).
					 */
					if (ctx->use_backend_cache &&
						wm_cache_lookup(cagg_id, &watermark))
					{
						/* hit -- fall through to memoize + replace */
					}
					else
					{
						/*
						 * Skip the replacement if we cannot read the
						 * watermark catalog (e.g. caller lacks SELECT on
						 * time_series.cagg_watermark -- PG view-owner
						 * permission model lets non-time_series roles
						 * legitimately query a cv view they have SELECT on).
						 * Leaving the FuncExpr in place falls back to the
						 * original VOLATILE per- segment evaluation, which
						 * respects PG's normal view permission semantics.
						 */
						if (!get_global_watermark(cagg_id, &watermark))
							return node;

						if (ctx->use_backend_cache)
							wm_cache_store(cagg_id, watermark);
					}

					/*
					 * Memoize the successful lookup for the rest of this
					 * pass.
					 */
					if (ctx->n_cached < CONSTIFY_WM_CACHE_MAX)
					{
						ctx->cached_id[ctx->n_cached] = cagg_id;
						ctx->cached_wm[ctx->n_cached] = watermark;
						ctx->n_cached++;
					}
				}

				ctx->replaced_count++;

				return (Node *) makeConst(
										  TIMESTAMPTZOID,
										  -1,
										  InvalidOid,
										  sizeof(TimestampTz),
										  TimestampTzGetDatum(watermark),
										  false,	/* not null */
										  FLOAT8PASSBYVAL	/* by-value on 64-bit */
					);
			}
		}
	}

	if (IsA(node, Query))
	{
		return (Node *) query_tree_mutator((Query *) node,
										   constify_mutator,
										   ctx,
										   QTW_DONT_COPY_QUERY);
	}
	return expression_tree_mutator(node, constify_mutator, ctx);
}

/*
 * Public entry (mutator variant): returns the same Query* pointer with
 * in-tree FuncExprs replaced by Const nodes.  Idempotent -- running it
 * twice on the same Query does nothing the second time.
 */
void
constify_cagg_watermark_mutate(Query *parse)
{
	ConstifyContext ctx;

	if (parse == NULL)
		return;
	if (Gp_role != GP_ROLE_DISPATCH)
		return;
	if (parse->commandType != CMD_SELECT)
		return;

	ctx.cagg_watermark_oid = get_cagg_watermark_func_oid();
	if (!OidIsValid(ctx.cagg_watermark_oid))
		return;

	ctx.replaced_count = 0;
	ctx.n_cached = 0;

	/*
	 * RR/SERIALIZABLE must read the watermark under the transaction snapshot
	 * (the SPI path does) so it matches the mat data the snapshot can see;
	 * the backend cache holds the LATEST committed value and would break that
	 * consistency.  See the cache block comment for the full argument.
	 */
	ctx.use_backend_cache = !IsolationUsesXactSnapshot();

	(void) query_tree_mutator(parse, constify_mutator, &ctx,
							  QTW_DONT_COPY_QUERY);

	if (ctx.replaced_count > 0)
		elog(DEBUG2, "constify_cagg_watermark: replaced %d call(s) with Const",
			 ctx.replaced_count);
}
