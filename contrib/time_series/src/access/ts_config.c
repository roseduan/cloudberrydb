/*-------------------------------------------------------------------------
 *
 * ts_config.c
 *    time_series table configuration: custom reloption registration,
 *    interval / origin string parsing, chunk number calculation, and
 *    relcache-keyed TSConfig lookup with a session-local hash cache.
 *
 *    Shared utility for the rest of the extension — every code path
 *    that maps (timestamp, table) → chunk_num or parses an
 *    ts_chunk_interval / ts_chunk_origin reloption string lands here.
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 * IDENTIFICATION
 *    contrib/time_series/src/access/ts_config.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/reloptions.h"
#include "access/table.h"
#include "access/tableam.h"
#include "catalog/namespace.h"
#include "common/relpath.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/timestamp.h"

#include "../include/time_series.h"
#include "../include/access/ts_tableam.h"

/*
 * Session-level TSConfig cache backed by a dynahash HTAB.  Per-row
 * config lookup would otherwise reparse the ts_chunk_interval /
 * ts_chunk_origin reloption strings on every INSERT and scan; this
 * cache makes the steady-state path O(1).  Lifetime is process-wide
 * (TopMemoryContext).
 *
 * Invalidated via ts_config_relcache_callback (registered lazily by
 * ts_get_config on first use): ts_partition_column / ts_chunk_interval
 * / ts_chunk_origin can be changed by ALTER TABLE ... SET (...) as
 * long as the table has zero chunks (see ts_ddl.c's AT_SetRelOptions
 * guard) -- e.g. CREATE TABLE, load and TRUNCATE away a misconfigured
 * batch, ALTER to fix ts_chunk_interval, then keep loading in the same
 * backend.  ts_get_config is on the critical path for INSERT routing
 * (ts_tableam.c), ChunkScan boundary pruning (ts_scan.c), and the
 * compress writer (ts_compress.c); without invalidation, an ALTER
 * that pg_class.reloptions genuinely picks up would still route rows
 * and prune scans using the config cached before the ALTER, for as
 * long as that backend process lives -- and a backend that had never
 * touched the table would pick up the new config immediately, so two
 * connections in the same pool could route the identical timestamp to
 * two different chunk numbers.
 */
typedef struct TSConfigCacheEntry
{
	Oid			relid;		/* hash key — must be first */
	TSConfig	config;
} TSConfigCacheEntry;

/* Custom reloption kind token, allocated during ts_reloptions_init(). */
relopt_kind ts_relopt_kind = 0;

static HTAB *ts_config_htab = NULL;
static bool	ts_config_cache_callback_registered = false;

/*
 * ts_config_relcache_callback
 *		Drop the cached TSConfig for relid (or the whole cache on a
 *		whole-cache invalidation, relid == InvalidOid).  Per PG
 *		convention for relcache callbacks, does no catalog/relcache
 *		access itself -- just a dynahash removal / hash_destroy, the
 *		same shape already used safely for the OID caches in
 *		ts_catalog.c and ts_compress.c (see those files' comments for
 *		why this is a different, safer mechanism than the per-call
 *		SearchSysCacheExists1 revalidation that was tried and reverted
 *		elsewhere in this extension).
 */
static void
ts_config_relcache_callback(Datum arg, Oid relid)
{
	if (ts_config_htab == NULL)
		return;

	if (!OidIsValid(relid))
	{
		hash_destroy(ts_config_htab);
		ts_config_htab = NULL;	/* recreated lazily on next cache miss */
		return;
	}

	(void) hash_search(ts_config_htab, &relid, HASH_REMOVE, NULL);
}

static void
ts_config_cache_callback_ensure_registered(void)
{
	if (ts_config_cache_callback_registered)
		return;
	CacheRegisterRelcacheCallback(ts_config_relcache_callback, (Datum) 0);
	ts_config_cache_callback_registered = true;
}

/*
 * ts_reloptions_init
 *		Register a custom reloption kind and three string-valued
 *		reloptions (ts_partition_column,
 *		ts_chunk_interval, ts_chunk_origin) for time_series tables.
 *		Must be called from _PG_init() before
 *		any table creation can reference these options.
 */
void
ts_reloptions_init(void)
{
	ts_relopt_kind = add_reloption_kind();

	add_string_reloption(ts_relopt_kind, "ts_partition_column",
						 "Name of the time-series timestamp column",
						 NULL, NULL,
						 ShareUpdateExclusiveLock);

	add_string_reloption(ts_relopt_kind, "ts_chunk_interval",
						 "Chunk interval for time-series partitioning",
						 NULL, NULL,
						 ShareUpdateExclusiveLock);

	add_string_reloption(ts_relopt_kind, "ts_chunk_origin",
						 "Origin timestamp for chunk alignment",
						 "2000-01-01 00:00:00+00", NULL,
						 ShareUpdateExclusiveLock);

	/*
	 * QD-readable chunk-state aggregates.  ts_chunk lives on segments
	 * (Distributed randomly) and the QD-local scan returns zero rows;
	 * the planner needs these counts to size the parameterized ChunkScan
	 * cost discount.  Maintained by compress_chunks and ANALYZE.
	 */
	add_int_reloption(ts_relopt_kind, "ts_n_total_chunks",
					  "Total chunks across all segments (for planner cost)",
					  0, 0, INT_MAX,
					  ShareUpdateExclusiveLock);

	add_int_reloption(ts_relopt_kind, "ts_n_compressed_chunks",
					  "Compressed chunks across all segments (for planner cost)",
					  0, 0, INT_MAX,
					  ShareUpdateExclusiveLock);
}

/*
 * ts_parse_interval_usec
 *		Parse an interval string such as "1 day" or "1 month" and return the equivalent
 *		number of microseconds.  Month is normalised to 30 days for deterministic arithmetic.
 */
int64
ts_parse_interval_usec(const char *interval_str)
{
	Datum		d;
	Interval   *ival;

	d = DirectFunctionCall3(interval_in,
							CStringGetDatum(interval_str),
							ObjectIdGetDatum(InvalidOid),
							Int32GetDatum(-1));
	ival = DatumGetIntervalP(d);

	return ival->time +
		   (int64) ival->day * USECS_PER_DAY +
		   (int64) ival->month * 30 * USECS_PER_DAY;
}

/*
 * ts_parse_origin_usec
 *		Parse a timestamptz string and return microseconds since the
 *		PostgreSQL epoch (2000-01-01 00:00:00 UTC).
 */
int64
ts_parse_origin_usec(const char *origin_str)
{
	Datum		d;

	d = DirectFunctionCall3(timestamptz_in,
							CStringGetDatum(origin_str),
							ObjectIdGetDatum(InvalidOid),
							Int32GetDatum(-1));
	return DatumGetTimestampTz(d);
}

/*
 * ts_calculate_chunk
 *		Map a timestamp (in microseconds) to a ForkNumber by computing
 *		which chunk interval it belongs to relative to origin_usec.
 *		Negative offsets (timestamps before origin) are handled via
 *		floor-division so that chunk numbers are always contiguous.
 *
 *		The same formula must be used by both INSERT routing and scan
 *		pruning so that data written to a chunk is always found again.
 */
ForkNumber
ts_calculate_chunk(int64 ts_usec, int64 origin_usec, int64 interval_usec)
{
	int64		offset;
	int64		chunk_idx;

	offset = ts_usec - origin_usec;
	if (offset >= 0)
		chunk_idx = offset / interval_usec;
	else
		chunk_idx = (offset - interval_usec + 1) / interval_usec;

	/*
	 * A negative chunk_idx means the timestamp is before origin.
	 * Return InvalidForkNumber so the caller can decide what to do:
	 * INSERT raises an error, scan pruning treats it as "no chunks".
	 */
	if (chunk_idx < 0)
		return InvalidForkNumber;

	return (ForkNumber) (TS_FIRST_CHUNKNUM + chunk_idx);
}

static void
ts_config_cache_init(void)
{
	HASHCTL		ctl;

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(TSConfigCacheEntry);
	ctl.hcxt = TopMemoryContext;

	ts_config_htab = hash_create("ts_config cache", 8, &ctl,
								 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

/*
 * ts_get_config
 *		Parse TSRelOptions from rd_options, resolve the named time-series
 *		column to an AttrNumber, and convert the interval/origin strings
 *		to microsecond integers.  The result is stored in *config.
 *
 *		Returns true on success, false if rel has no time_series reloptions.
 *		Throws ERROR if the reloptions exist but are invalid (e.g. the
 *		named column does not exist in the relation).
 */
bool
ts_get_config(Relation rel, TSConfig *config)
{
	TSRelOptions   *opts;
	const char	   *col_name;
	const char	   *interval_str;
	const char	   *origin_str;
	TupleDesc		tupdesc;
	Oid				relid = RelationGetRelid(rel);
	int				i;

	ts_config_cache_callback_ensure_registered();

	if (rel->rd_options == NULL)
		return false;

	/* Fast path: hash lookup */
	if (ts_config_htab != NULL)
	{
		TSConfigCacheEntry *entry;

		entry = (TSConfigCacheEntry *)
			hash_search(ts_config_htab, &relid, HASH_FIND, NULL);
		if (entry != NULL)
		{
			*config = entry->config;
			return true;
		}
	}

	opts = (TSRelOptions *) rel->rd_options;
	if (opts->ts_column == 0)
		return false;

	col_name = (const char *) opts + opts->ts_column;
	interval_str = (opts->ts_interval != 0)
		? (const char *) opts + opts->ts_interval
		: NULL;
	origin_str = (opts->ts_origin != 0)
		? (const char *) opts + opts->ts_origin
		: "2000-01-01 00:00:00+00";

	if (interval_str == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("ts_chunk_interval must be set for time-series tables")));

	/* Resolve column name to attnum */
	tupdesc = RelationGetDescr(rel);
	config->ts_attnum = InvalidAttrNumber;
	for (i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);

		if (att->attisdropped)
			continue;
		if (strcmp(NameStr(att->attname), col_name) == 0)
		{
			config->ts_attnum = att->attnum;
			break;
		}
	}

	if (config->ts_attnum == InvalidAttrNumber)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("time-series column \"%s\" not found", col_name)));

	config->interval_usec = ts_parse_interval_usec(interval_str);
	config->origin_usec = ts_parse_origin_usec(origin_str);

	/* Cache for subsequent calls */
	{
		TSConfigCacheEntry *entry;
		bool				found;

		if (ts_config_htab == NULL)
			ts_config_cache_init();

		entry = (TSConfigCacheEntry *)
			hash_search(ts_config_htab, &relid, HASH_ENTER, &found);
		entry->config = *config;
	}

	return true;
}
