/*-------------------------------------------------------------------------
 *
 * pg_iceberg_metadata_refresh.c
 *    Refresh of the external-catalog metadata cache: the batch cycle a
 *    scheduled task drives, and the per-table force refresh behind
 *    iceberg_refresh_metadata() (issue #393 follow-up).
 *
 * Why this exists
 * ---------------
 *
 * The #393 read path bounds staleness for an external-catalog Iceberg table by
 * a per-table TTL (the metadata_cache=N minutes option): a plain SELECT hands
 * the agent the head it last resolved plus pg_iceberg_metadata.metadata_cached_at,
 * and the agent re-resolves the external catalog once that stamp is older than
 * the TTL.
 *
 * What the read path does NOT do is write that answer down.  A read resolves
 * the head it needs and uses it; it never touches the mirror.  That is a
 * deliberate rule -- an ordinary SELECT should not mutate shared catalog state
 * as a side effect, riding on the reader's transaction and racing other readers
 * for the same row.
 *
 * Which leaves nobody to advance metadata_cached_at, and a stamp that never
 * advances is a cache that never hits: every read hands the agent the same
 * expired timestamp and pays the external-catalog round trip again.
 *
 * This module is that writer.  It re-resolves the head for tables whose stamp
 * is about to expire and persists it, so the reads in between find a fresh
 * stamp and are served from the agent's cache.  It is also what makes
 * metadata_cache worth setting at all: on a cluster where nothing refreshes,
 * the option has no effect.
 *
 * Note what this does NOT change: the read path keeps its own TTL check, so
 * correctness never depends on a refresh running.  If the refresher is
 * disabled, starved, or its catalog is unreachable, reads fall back to
 * resolving the head themselves -- slower, never staler.  That is deliberate:
 * the refresh is a latency optimisation, not the freshness bound.
 *
 * Cadence
 * -------
 *
 * The cycle runs from a link in core's AutoVacWorkerPostHook chain, the same
 * way pg_iceberg_av_consumer.c drives the deletion queue.  An autovacuum worker
 * on the QD is exactly the context this needs -- it may write, it is not in
 * recovery, and it already visits every database on a timer -- so the refresh
 * is on by default with nothing to configure.
 *
 * The price is the cadence.  The hook fires once per autovacuum worker
 * lifecycle, so the effective interval is autovacuum_naptime (default 60s),
 * NOT metadata_refresh_min_interval -- that GUC only throttles a worker that
 * somehow runs the hook more than once, and last_run_ts resets when the worker
 * forks fresh.  Refreshing ahead of expiry is therefore only possible when the
 * TTL is comfortably larger than autovacuum_naptime; with the default naptime,
 * metadata_cache values below a couple of minutes will keep expiring between
 * cycles and the read path will go on resolving them itself.
 *
 * For TTLs that short, drive the same cycle from the task scheduler instead,
 * where the period is whatever the DBA picks, and turn the hook off so the two
 * do not both spend round trips on the same tables:
 *
 *     ALTER SYSTEM SET datalake_fdw.metadata_refresh_enabled = off;
 *     CREATE TASK iceberg_metadata_refresh SCHEDULE '30 SECONDS'
 *       AS $$ SELECT iceberg_refresh_metadata_cache() $$;
 *
 * Two callers, one core
 * ---------------------
 *
 * pg_iceberg_refresh_metadata_cache_batch() walks every cached table and
 * refreshes the ones whose stamp is due -- this is what the scheduled task
 * calls.  pg_iceberg_refresh_table_head() refreshes one named table whether it
 * is due or not, and is what the owner-only iceberg_refresh_metadata()
 * calls to recover a table that an external maintenance pass has broken.  Both
 * resolve and store through refresh_resolve_and_store(), so a manual refresh
 * cannot write the mirror with semantics a scheduled one would not.
 *
 * Design notes
 * ------------
 *
 * Refresh-ahead.  Waiting for the stamp to actually expire would be useless:
 * the next reader hits the expired stamp first and resolves the head itself,
 * which is exactly what we are trying to spare it.  The batch cycle refreshes
 * once the stamp has burned metadata_refresh_ahead_percent of its TTL, so a
 * live table's cache ideally never expires from a reader's point of view.
 *
 * Candidates.  Every table on a non-builtin catalog -- with or without a
 * `table` OPTION -- whose effective TTL is > 0: its own metadata_cache, else
 * its FOREIGN CATALOG's, the same rule the read path applies (issue #461).
 * An earlier revision also required metadata_cached_at IS NOT NULL, using it
 * as a free "somebody actually reads this table" signal -- that
 * worked only while the read path stamped the row on first read, which it no
 * longer does.  Keeping the filter would leave every table NULL-stamped
 * forever and the cycle with nothing to do.  The cost of dropping it is one
 * round trip per TTL for a table nobody queries; the cure, if that ever
 * matters, is to leave metadata_cache unset on such tables, which takes them
 * out of the candidate set entirely.
 *
 * Force-fresh resolve.  We call pg_iceberg_load_table(), not the _cached()
 * variant.  Passing our own stamp down would let the agent answer "still fresh"
 * and echo it back unchanged, leaving metadata_cached_at untouched -- the cycle
 * would spin without ever refreshing anything.
 *
 * Write-back.  pg_iceberg_update_metadata_cache() is the only correct writer
 * here.  pg_iceberg_update_metadata_cas() would roll previous_metadata_location,
 * folding an external head change into this cluster's own commit lineage, which
 * the orphan-file bookkeeping reads.  An external writer's commit is not ours.
 *
 * Concurrency.  A refresh resolves the head first and stores it afterwards,
 * so a local DML on the same table can commit in between and store a newer
 * head than the one we hold.  Storing ours then would take the table back to
 * the older head, and every reader within the TTL would miss that commit
 * (issue #461).  So the stored head is read before resolving and handed to
 * pg_iceberg_update_metadata_cache() as the expected one: if it has moved,
 * nothing is written.  A DML still committing when we store makes our
 * heap_update wait and return TM_Updated, which is dropped the same way.  If
 * we store first, the DML's CAS sees an unexpected base and rebases.  A
 * dropped refresh leaves the stamp as it was, so the next cycle tries again.
 *
 * IDENTIFICATION
 *    contrib/datalake_fdw/src/am_iceberg/pg_iceberg_metadata_refresh.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/relation.h"
#include "access/table.h"
#include "access/xact.h"
#include "commands/extension.h"
#include "foreign/foreign.h"
#include "datatype/timestamp.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "postmaster/autovacuum.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/resowner.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"

#include "cdb/cdbvars.h"

#include "include/iceberg_oids.h"
#include "include/pg_iceberg_catalog.h"
#include "include/pg_iceberg_catalog_helper.h"
#include "include/pg_iceberg_metadata.h"
#include "include/pg_iceberg_metadata_refresh.h"
#include "include/pg_iceberg_options.h"

/* GUC state */
static bool metadata_refresh_enabled = true;
static int	metadata_refresh_min_interval = 30;
static int	metadata_refresh_max_tables = 100;
static int	metadata_refresh_ahead_percent = 70;

/*
 * Effective TTL, in minutes, of an external-catalog table neither it nor its
 * FOREIGN CATALOG sets metadata_cache on (issue #461).  Read by the read path
 * through pg_iceberg_catalog.c as well, hence not static; PGC_SIGHUP so that
 * readers and the autovacuum refresher always agree on it.
 */
int			metadata_cache_default = 5;

/* Hook-chaining state */
static AutoVacWorkerPostHook_type prev_av_hook = NULL;

/* Per-process throttle (resets when the worker process forks fresh) */
static TimestampTz last_run_ts = 0;

/* External GUC defined by datalake_fdw.c */
extern char *datalake_agent_server_url;

/*
 * One row of pg_iceberg_metadata worth considering, captured while the catalog
 * scan is open so the scan can be closed before we call out to the agent.
 */
typedef struct RefreshCandidate
{
	Oid			relid;
	int64		cached_at;		/* epoch ms, never 0/NULL here */
} RefreshCandidate;

static void datalake_metadata_refresh(Oid datid);
static RefreshCandidate *collect_refresh_candidates(int *ncandidates);
static bool refresh_one_table(Oid relid, int64 cached_at, int64 now_ms,
							  bool *wrote);
static char *refresh_resolve_and_store(Relation rel,
									   IcebergTableInfo *table_info,
									   bool require_agent_stamp);
static int	refresh_candidate_cmp(const void *a, const void *b);
static int64 current_epoch_ms(void);
static bool user_mapping_exists(const char *server_name);


/* ================================================================
 * Init: GUCs + hook registration
 * ================================================================
 */
void
pg_iceberg_metadata_refresh_init(void)
{
	DefineCustomBoolVariable(
		"datalake_fdw.metadata_refresh_enabled",
		"Enable the autovacuum-driven refresh of the iceberg external-catalog "
		"metadata cache.",
		"Only affects external-catalog tables whose effective metadata cache "
		"TTL is > 0 (see datalake_fdw.metadata_cache_default); "
		"reads stay correct when disabled, they just stop being cached.",
		&metadata_refresh_enabled,
		true,
		PGC_SIGHUP,
		0,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"datalake_fdw.metadata_refresh_min_interval",
		"Minimum seconds between refresh runs per autovacuum worker process.",
		"The practical cadence is autovacuum_naptime; this only guards a worker "
		"that runs the hook more than once in its lifetime.",
		&metadata_refresh_min_interval,
		30,
		1, 86400,
		PGC_SIGHUP,
		GUC_UNIT_S,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"datalake_fdw.metadata_refresh_max_tables_per_cycle",
		"Maximum number of tables refreshed per autovacuum cycle.",
		"Bounds how long one cycle can hold an autovacuum worker on external "
		"catalog round trips.  Only tables that needed a round trip count "
		"toward it; ones still inside their TTL are free to skip.  The SQL "
		"entry point takes its own max_tables argument instead.",
		&metadata_refresh_max_tables,
		100,
		1, 10000,
		PGC_SIGHUP,
		0,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"datalake_fdw.metadata_refresh_ahead_percent",
		"Refresh a cached head once this percentage of its TTL has elapsed.",
		"Below 100 the cache is renewed before readers can observe it expired, "
		"which is the point of running the refresh on a schedule at all; 100 "
		"means refresh only once already expired.  Only affects the batch "
		"cycle: iceberg_refresh_metadata() always resolves.",
		&metadata_refresh_ahead_percent,
		70,
		10, 100,
		PGC_SIGHUP,
		0,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"datalake_fdw.metadata_cache_default",
		"Default metadata cache TTL, in minutes, for external-catalog iceberg "
		"tables.",
		"Applies to a table when neither the table nor its FOREIGN CATALOG sets "
		"metadata_cache.  A read within the TTL uses the locally stored metadata "
		"location; past it, the location is resolved from the catalog for that "
		"query.  0 resolves on every read.",
		&metadata_cache_default,
		5,
		0, 10080,
		PGC_SIGHUP,
		GUC_UNIT_MIN,
		NULL, NULL, NULL);

	/* Chain ourselves into the hook (preserve any prior callback). */
	prev_av_hook = AutoVacWorkerPostHook;
	AutoVacWorkerPostHook = datalake_metadata_refresh;
}


/* ================================================================
 * Hook body
 * ================================================================
 */
static void
datalake_metadata_refresh(Oid datid)
{
	TimestampTz		now;
	MemoryContext	oldcontext = CurrentMemoryContext;

	/* Cheap gates first (avoid any catalog work when disabled). */
	if (!metadata_refresh_enabled)
		goto chain;
	if (Gp_role != GP_ROLE_DISPATCH)
		goto chain;

	/*
	 * pg_iceberg_metadata is only populated on the QD and every refresh needs
	 * dlagent; with no agent configured (e.g. CI regression clusters) there is
	 * nothing to resolve against, so skip before opening a transaction.
	 */
	if (datalake_agent_server_url == NULL || datalake_agent_server_url[0] == '\0')
		goto chain;

	now = GetCurrentTimestamp();
	if (last_run_ts != 0 &&
		!TimestampDifferenceExceeds(last_run_ts, now,
									metadata_refresh_min_interval * 1000))
		goto chain;
	last_run_ts = now;

	/*
	 * Wrap the whole cycle, transaction setup included: a raise escaping here
	 * would break the AutoVacWorkerPostHook chain and fall through to
	 * AutoVacWorkerMain's catch.  Per-table failures are isolated by
	 * subtransactions inside the batch, not here.
	 */
	PG_TRY();
	{
		StartTransactionCommand();
		PushActiveSnapshot(GetTransactionSnapshot());

		/* Only act if datalake_fdw is installed here (missing_ok lookup). */
		if (get_extension_oid("datalake_fdw", true) != InvalidOid)
		{
			int			n;

			n = pg_iceberg_refresh_metadata_cache_batch(metadata_refresh_max_tables);
			if (n > 0)
				elog(DEBUG1,
					 "iceberg metadata refresh: refreshed %d table(s)", n);
		}

		PopActiveSnapshot();
		CommitTransactionCommand();
	}
	PG_CATCH();
	{
		ErrorData  *edata;

		MemoryContextSwitchTo(oldcontext);
		edata = CopyErrorData();
		FlushErrorState();
		AbortCurrentTransaction();
		ereport(WARNING,
				(errmsg("iceberg metadata refresh cycle aborted; retrying next "
						"autovacuum cycle: %s",
						edata->message)));
		FreeErrorData(edata);
	}
	PG_END_TRY();

chain:
	if (prev_av_hook)
		prev_av_hook(datid);
}


/* ================================================================
 * Batch driver
 * ================================================================
 */
int
pg_iceberg_refresh_metadata_cache_batch(int max_tables)
{
	RefreshCandidate *cands;
	int				ncands;
	volatile int	refreshed = 0;
	volatile int	attempted = 0;
	volatile int	i;
	int64			now_ms;

	cands = collect_refresh_candidates(&ncands);
	if (ncands == 0)
	{
		pfree(cands);
		return 0;
	}

	/*
	 * Most-stale first.  A cycle capped by max_tables must not keep revisiting
	 * whatever happens to sit at the front of the heap; ordering by stamp puts
	 * the tables closest to expiry (and the ones a previous capped cycle could
	 * not reach) at the head of the queue.
	 */
	qsort(cands, ncands, sizeof(RefreshCandidate), refresh_candidate_cmp);

	now_ms = current_epoch_ms();

	for (i = 0; i < ncands && attempted < max_tables; i++)
	{
		MemoryContext	oldcontext = CurrentMemoryContext;
		ResourceOwner	oldowner = CurrentResourceOwner;
		Oid				relid = cands[i].relid;
		int64			cached_at = cands[i].cached_at;

		/*
		 * One table per subtransaction: an unreachable external catalog, a
		 * table dropped from under us, or a malformed options row must not
		 * abort the refreshes that already succeeded in this batch.  Rolling
		 * back also releases the locks and buffer pins the failed attempt
		 * held, which a bare PG_CATCH would not.
		 */
		BeginInternalSubTransaction(NULL);
		MemoryContextSwitchTo(oldcontext);

		PG_TRY();
		{
			bool		wrote = false;

			/*
			 * Count the round trip, not the write.  An agent that returns no
			 * timestamp (one predating the metadata cache) never produces a
			 * write, and capping on writes would let such a cluster re-resolve
			 * every candidate table on every single cycle.
			 */
			if (refresh_one_table(relid, cached_at, now_ms, &wrote))
				attempted++;
			if (wrote)
				refreshed++;

			ReleaseCurrentSubTransaction();
			MemoryContextSwitchTo(oldcontext);
			CurrentResourceOwner = oldowner;
		}
		PG_CATCH();
		{
			ErrorData  *edata;

			MemoryContextSwitchTo(oldcontext);
			CurrentResourceOwner = oldowner;
			edata = CopyErrorData();
			FlushErrorState();
			RollbackAndReleaseCurrentSubTransaction();
			MemoryContextSwitchTo(oldcontext);
			CurrentResourceOwner = oldowner;

			/*
			 * A raise almost certainly came from the external-catalog call, so
			 * charge it to the budget: an unreachable catalog must not let one
			 * cycle retry every candidate.
			 */
			attempted++;

			ereport(WARNING,
					(errmsg("iceberg metadata refresh skipped relation %u: %s",
							relid, edata->message)));
			FreeErrorData(edata);
		}
		PG_END_TRY();
	}

	pfree(cands);

	return refreshed;
}


/*
 * Scan pg_iceberg_metadata once and collect the rows worth examining.
 *
 * Deliberately a two-pass design: the per-table work calls out to dlagent over
 * the network, and holding a catalog scan open across that would pin the
 * snapshot and the relation for the whole cycle.
 */
static RefreshCandidate *
collect_refresh_candidates(int *ncandidates)
{
	Relation		metadata_rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	TupleDesc		tupdesc;
	RefreshCandidate *cands;
	int				n = 0;
	int				capacity = 64;

	cands = (RefreshCandidate *) palloc(capacity * sizeof(RefreshCandidate));

	/* OIDs are pinned at initdb (iceberg-cdbinit). */
	metadata_rel = table_open(ICEBERG_METADATA_RELID, AccessShareLock);
	tupdesc = RelationGetDescr(metadata_rel);

	/* Full scan: we are looking for every cached external table, not one relid. */
	scan = systable_beginscan(metadata_rel, InvalidOid, false, NULL, 0, NULL);

	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Datum		datum;
		bool		isnull;
		Oid			relid;
		int64		cached_at;

		datum = heap_getattr(tuple, Anum_pg_iceberg_metadata_relid,
							 tupdesc, &isnull);
		if (isnull)
			continue;
		relid = DatumGetObjectId(datum);

		/*
		 * Not filtered on is_internal: that flag is true for a table created
		 * without OPTIONS on a Polaris or Hive catalog, whose head another
		 * engine moves all the same.  Whether a table has an external head is
		 * a property of its catalog, which refresh_one_table() checks
		 * (issue #461).
		 */

		/*
		 * A NULL stamp means this table has never been refreshed.  Since a read
		 * stamps nothing (see the file header), that is the state every
		 * external table starts in and stays in until a cycle reaches it -- so
		 * NULL is treated as infinitely stale rather than skipped, and the
		 * first cycle after CREATE seeds it.
		 */
		datum = heap_getattr(tuple, Anum_pg_iceberg_metadata_metadata_cached_at,
							 tupdesc, &isnull);
		cached_at = isnull ? 0 : DatumGetInt64(datum);

		if (n >= capacity)
		{
			capacity *= 2;
			cands = (RefreshCandidate *)
				repalloc(cands, capacity * sizeof(RefreshCandidate));
		}

		cands[n].relid = relid;
		cands[n].cached_at = cached_at;
		n++;
	}

	systable_endscan(scan);
	table_close(metadata_rel, AccessShareLock);

	*ncandidates = n;
	return cands;
}


/*
 * Re-resolve and persist one table's head if its stamp is due.
 *
 * Returns true when an external-catalog round trip was made (what the
 * per-cycle budget is spent on), and sets *wrote when that round trip also
 * produced a persisted refresh.
 *
 * Runs inside its own subtransaction (see the batch driver), so raising is an
 * acceptable failure mode for anything unexpected.
 */
static bool
refresh_one_table(Oid relid, int64 cached_at, int64 now_ms, bool *wrote)
{
	Relation		rel;
	IcebergTableInfo *table_info;
	char		   *stored;
	int				ttl;
	int64			due_after_ms;
	bool			attempted = false;

	*wrote = false;

	/*
	 * Conditional lock, and give up the moment it is not free.
	 *
	 * Only an AccessExclusiveLock conflicts with us, so ordinary traffic --
	 * SELECT, INSERT, UPDATE, DELETE -- never sends us down this path; what
	 * does is DDL: ALTER TABLE, DROP, TRUNCATE, VACUUM FULL, REINDEX.  Waiting
	 * behind one of those would block inside an autovacuum worker, holding
	 * this cycle's transaction and its xmin open, occupying one of
	 * autovacuum_max_workers, and stalling every remaining candidate in the
	 * cycle behind one session's DDL.
	 *
	 * Skipping costs nothing: the refresh is a latency optimisation, the table
	 * keeps its old (or NULL) stamp, and candidates are ordered oldest-first,
	 * so the next cycle picks it up ahead of everything fresher.  This is what
	 * core autovacuum does for the same reason -- every non-wraparound
	 * autovacuum passes VACOPT_SKIP_LOCKED (autovacuum.c) and vacuum_rel()
	 * takes a conditional lock.
	 *
	 * NULL also covers "dropped between the catalog scan and now", which wants
	 * the same treatment.  The single-table iceberg_refresh_metadata() waits
	 * instead: there an operator named this relation and expects it refreshed.
	 */
	rel = try_relation_open(relid, AccessShareLock, true);
	if (rel == NULL)
	{
		elog(DEBUG1,
			 "iceberg metadata refresh: skipping relation %u (locked or gone)",
			 relid);
		return false;
	}

	table_info = pg_iceberg_get_table_info(relid);

	/*
	 * Builtin catalog: PG is the catalog, so there is no external head to
	 * re-resolve.  Every other catalog has one, whether or not the table
	 * carries a `table` OPTION (issue #461): one created without it on a
	 * Polaris or Hive catalog is moved by other engines all the same.  The
	 * same test the read path uses; see pg_iceberg_table_has_external_head().
	 */
	if (!pg_iceberg_table_has_external_head(table_info))
		goto out;

	/*
	 * The TTL readers use: the table's own metadata_cache, else the FOREIGN
	 * CATALOG's.  Reading only the table's left a TTL set on the catalog
	 * honoured by every reader and refreshed by nobody.  No TTL, nothing for a
	 * cache sweep to keep warm; bailing before the round trip means such a
	 * table costs a slot in the array and nothing else.
	 */
	ttl = pg_iceberg_effective_metadata_cache_ttl(table_info);
	if (ttl <= 0)
		goto out;

	/*
	 * Due once the stamp has burned metadata_refresh_ahead_percent of the TTL.
	 *
	 * now_ms comes from this node's clock while cached_at was stamped by the
	 * agent's, so skew shifts this boundary.  That is benign in both
	 * directions: refreshing early costs one extra resolve, refreshing late
	 * just leaves the work to the reader, which does its own TTL check anyway.
	 */
	due_after_ms = (int64) ttl * 60000 * metadata_refresh_ahead_percent / 100;
	if (now_ms - cached_at < due_after_ms)
		goto out;

	/*
	 * A refresh resolves the head with the catalog's and the volume's
	 * credentials.  Without a user mapping for either there is nothing to
	 * resolve with, every cycle: skip quietly rather than warn on each one.
	 * Such a table cannot be read or written either, which is where its owner
	 * finds out; the named-table iceberg_refresh_metadata() still raises.
	 */
	if (!user_mapping_exists(table_info->catalog_server_name) ||
		(table_info->volume_server_name != NULL &&
		 !user_mapping_exists(table_info->volume_server_name)))
	{
		elog(DEBUG1,
			 "iceberg metadata refresh: skipping relation %u (no user mapping)",
			 relid);
		goto out;
	}

	attempted = true;
	stored = refresh_resolve_and_store(rel, table_info, true);
	if (stored != NULL)
	{
		pfree(stored);
		*wrote = true;
	}

out:
	pg_iceberg_free_table_info(table_info);

	/*
	 * If this refresh updated the table's pg_iceberg_metadata row, keep the
	 * lock until the cycle commits.  Released here, a DROP TABLE could take
	 * its AccessExclusiveLock at once, read the row as it was before that
	 * update, and fail deleting it with "tuple concurrently updated" when the
	 * cycle commits (issue #461).  A table the cycle did not write has no such
	 * row change pending; release it now, so that DDL on it does not wait for
	 * the rest of the cycle and a long cycle does not pile up locks.
	 */
	relation_close(rel, *wrote ? NoLock : AccessShareLock);

	return attempted;
}


/*
 * pg_iceberg_refresh_table_head
 *    Force one table's mirror to the catalog's current head, TTL or no TTL.
 *
 * This is the single-table half of the module, behind the owner-only
 * iceberg_refresh_metadata().  It differs from the batch cycle in the two
 * ways a deliberate operator action should: no candidate filtering (the caller
 * named the relation, so a NULL stamp or metadata_cache=0 is no reason to skip
 * it) and no due check (the point is to recover now, not to pre-warm a cache).
 *
 * Returns the head that was stored, palloc'd in the caller's context, or NULL
 * when there was nothing to do -- a built-in-catalog table, whose head PG
 * itself owns, or an agent that returned none.  The caller holds a lock on rel.
 */
char *
pg_iceberg_refresh_table_head(Relation rel)
{
	IcebergTableInfo *table_info;
	char		   *stored = NULL;

	table_info = pg_iceberg_get_table_info(RelationGetRelid(rel));

	/*
	 * Built-in catalog: there is no external head to adopt, so a refresh is a
	 * well-defined no-op rather than an error.  Reported as "not refreshed"
	 * with the mirror unchanged, which is exactly what happened.
	 */
	if (pg_iceberg_table_has_external_head(table_info))
		stored = refresh_resolve_and_store(rel, table_info, false);

	pg_iceberg_free_table_info(table_info);

	return stored;
}


/*
 * Resolve the external head force-fresh and persist it.  Shared by both entry
 * points so a manual refresh cannot write the mirror with semantics a
 * scheduled one would not -- in particular, neither rolls
 * previous_metadata_location (see the file header).
 *
 * require_agent_stamp distinguishes the two callers on one point: what to do
 * with an agent too old to return a cache timestamp.  For the batch cycle the
 * answer is "nothing" -- writing a head without advancing metadata_cached_at
 * leaves the table due forever and the cycle would re-resolve it every run.
 * For an operator recovering a broken table the head itself is the whole
 * point, so it is stored and stamped from this node's clock; such an agent
 * ignores the stamp on the way back in anyway, which is why it is safe to
 * supply one from a different clock.
 *
 * Returns the stored head (palloc'd) or NULL when nothing was written.
 */
static char *
refresh_resolve_and_store(Relation rel, IcebergTableInfo *table_info,
						  bool require_agent_stamp)
{
	IcebergLoadTableResult *load_result;
	IcebergMetadataInfo *meta_info;
	char		   *expected;
	const char	   *nameSpace;
	const char	   *tableName;
	const char	   *catalogName;
	char		   *stored = NULL;

	/*
	 * Both entry points reach dlagent through this function and nowhere else,
	 * so the "is there an agent at all" guard belongs here rather than in each
	 * of them.  iceberg_refresh_metadata_cache() checks again up front, to
	 * fail a cycle before it scans the catalog; iceberg_refresh_metadata()
	 * deliberately does not, because a built-in-catalog table returns before
	 * this point and has to stay a well-defined no-op on a cluster that has no
	 * agent configured.
	 */
	if (datalake_agent_server_url == NULL || datalake_agent_server_url[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_CONFIG_FILE_ERROR),
				 errmsg("datalake.agent_server_url is not configured"),
				 errhint("A refresh resolves the head through dlagent; set the "
						 "agent URL before refreshing an external-catalog "
						 "table.")));

	nameSpace = pg_iceberg_resolve_namespace(
		table_info->opts ? table_info->opts->namespace : NULL,
		table_info->catalog_server_name,
		table_info->catalog_name,
		RelationGetNamespace(rel));

	/*
	 * A table created without a `table` OPTION is known to its catalog by the
	 * relation's own name, as on every other path that resolves it.
	 */
	if (table_info->opts == NULL || table_info->opts->table == NULL)
	{
		tableName = RelationGetRelationName(rel);
		catalogName = NULL;
	}
	else
	{
		tableName = table_info->opts->table;
		catalogName = table_info->opts->catalog;
	}

	/*
	 * The stored head before resolving: the write-back only proceeds if it is
	 * still the stored one, so a local commit made meanwhile is not undone
	 * (file header).
	 */
	meta_info = pg_iceberg_get_metadata_info(RelationGetRelid(rel));
	expected = meta_info->metadata_location ?
		pstrdup(meta_info->metadata_location) : NULL;
	pg_iceberg_free_metadata_info(meta_info);

	/* Force-fresh: never hand the agent our own stamp here (file header). */
	load_result = pg_iceberg_load_table(catalogName,
										nameSpace,
										tableName,
										table_info->catalog_server_name,
										table_info->catalog_name,
										table_info->volume_server_name,
										table_info->volume_name);
	if (load_result == NULL)
	{
		if (expected != NULL)
			pfree(expected);
		return NULL;
	}

	if (load_result->metadata_location != NULL &&
		load_result->metadata_location[0] != '\0' &&
		(load_result->timestamp_present || !require_agent_stamp))
	{
		int64		stamp = load_result->timestamp_present ?
			load_result->timestamp : current_epoch_ms();

		if (pg_iceberg_update_metadata_cache(RelationGetRelid(rel),
											 expected,
											 load_result->metadata_location,
											 stamp))
			stored = pstrdup(load_result->metadata_location);
		else
			elog(DEBUG1,
				 "iceberg metadata refresh: relation %u moved to a newer head "
				 "while it was being resolved; not storing %s",
				 RelationGetRelid(rel), load_result->metadata_location);
	}

	pg_iceberg_free_load_table_result(load_result);
	if (expected != NULL)
		pfree(expected);

	return stored;
}


/* Order candidates oldest-stamp-first. */
static int
refresh_candidate_cmp(const void *a, const void *b)
{
	int64		la = ((const RefreshCandidate *) a)->cached_at;
	int64		lb = ((const RefreshCandidate *) b)->cached_at;

	if (la < lb)
		return -1;
	if (la > lb)
		return 1;
	return 0;
}


/*
 * Current time as Unix epoch milliseconds, matching the domain the agent
 * stamps metadata_cached_at in (System.currentTimeMillis()).
 */
static int64
current_epoch_ms(void)
{
	TimestampTz now = GetCurrentTimestamp();

	/* TimestampTz counts microseconds from 2000-01-01; shift to 1970-01-01. */
	return (now / 1000) +
		((int64) (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) *
		 SECS_PER_DAY * INT64CONST(1000));
}


/* ================================================================
 * SQL entry point
 * ================================================================
 */

/*
 * iceberg_refresh_metadata_cache(max_tables int) -> int
 *
 * Run one refresh cycle synchronously and return the number of tables
 * refreshed.  This is how the refresh is meant to run -- as the body of a
 * task, on a schedule the DBA picks to suit the TTLs actually in use:
 *
 *     CREATE TASK iceberg_metadata_refresh SCHEDULE '30 SECONDS'
 *       AS $$ SELECT iceberg_refresh_metadata_cache() $$;
 *
 * Calling it by hand is a "refresh everything that is due, now" escape hatch;
 * to force one named table regardless of its TTL, use
 * iceberg_refresh_metadata(regclass) instead.
 *
 * EXECUTE is revoked from PUBLIC in the extension script: this writes a
 * shared catalog and issues external-catalog calls, so the DBA grants it
 * to whichever role runs the task.
 */
PG_FUNCTION_INFO_V1(iceberg_refresh_metadata_cache);

Datum
iceberg_refresh_metadata_cache(PG_FUNCTION_ARGS)
{
	int32		max_tables = PG_GETARG_INT32(0);
	int			refreshed;

	/*
	 * pg_iceberg_metadata is only populated on the QD, and the write-back has
	 * to happen where the CAS/last-writer-wins bookkeeping lives.
	 *
	 * This check is the only enforcement available: EXECUTE ON COORDINATOR is
	 * restricted to set-returning functions, so the extension script cannot
	 * declare the placement (see the note there).  A caller only lands here by
	 * burying the call in a query the planner dispatches, hence the hint.
	 */
	if (Gp_role != GP_ROLE_DISPATCH)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("iceberg_refresh_metadata_cache() must run on the coordinator"),
				 errhint("Call it on its own (SELECT iceberg_refresh_metadata_cache()) "
						 "rather than from a query whose plan is dispatched to the "
						 "segments.")));

	if (max_tables <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("max_tables must be greater than zero")));

	if (datalake_agent_server_url == NULL || datalake_agent_server_url[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_CONFIG_FILE_ERROR),
				 errmsg("datalake.agent_server_url is not configured"),
				 errhint("The refresh resolves heads through dlagent; set the "
						 "agent URL before calling this function.")));

	refreshed = pg_iceberg_refresh_metadata_cache_batch(max_tables);

	PG_RETURN_INT32(refreshed);
}


/*
 * Whether the current user can reach `server_name` through a user mapping --
 * its own or PUBLIC's -- without raising when there is none, which
 * GetUserMapping() would.
 */
static bool
user_mapping_exists(const char *server_name)
{
	ForeignServer *server;

	if (server_name == NULL)
		return false;

	server = GetForeignServerByName(server_name, true);
	if (server == NULL)
		return false;

	return SearchSysCacheExists2(USERMAPPINGUSERSERVER,
								 ObjectIdGetDatum(GetUserId()),
								 ObjectIdGetDatum(server->serverid)) ||
		SearchSysCacheExists2(USERMAPPINGUSERSERVER,
							  ObjectIdGetDatum(InvalidOid),
							  ObjectIdGetDatum(server->serverid));
}
