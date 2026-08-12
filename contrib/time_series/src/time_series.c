/*-------------------------------------------------------------------------
 *
 * time_series.c
 *    Module initialization for the time_series extension.
 *
 *    Registers Custom Scan methods and planner hooks for GapFill and
 *    ChunkScan fork-pruning.  Must be loaded via shared_preload_libraries
 *    so that Custom Scan deserialization works on all segments.
 *
 * Copyright (c) 2026 HashData Inc.
 * Licensed under Apache License 2.0
 *
 * IDENTIFICATION
 *    contrib/time_series/src/time_series.c
 *
 *-------------------------------------------------------------------------
 */
#include "include/time_series.h"
#include "include/access/ts_tableam.h"
#include "include/bgw/bgw_init.h"
#include "include/cagg/cagg.h"
#include "include/compress/ts_compress.h"
#include "include/gapfill/gapfill.h"
#include "include/storage/ts_fork_name.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_extension.h"
#include "commands/extension.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#ifdef GP_VERSION_NUM
#include "cdb/cdbvars.h"
#endif

/*
 * Local aliases for the shared identity macros (defined in
 * include/time_series.h) plus the proxy-table name used only here.
 * The proxy table (time_series.continuous_agg) is owned by the
 * extension, created at install time, and dropped early in the
 * DROP EXTENSION CASCADE sequence -- see the extension state
 * machine below.
 */
#define TS_EXT_NAME           TS_EXTENSION_NAME
#define TS_EXT_PROXY_SCHEMA   TS_EXTENSION_SCHEMA_NAME
#define TS_EXT_PROXY_TABLE    "continuous_agg"

/*
 * The SO version baked into the shared library at compile time.
 * Compared against the SQL extension version on each CREATED
 * transition so that long-lived backends running with a stale .so
 * against an upgraded SQL schema are evicted with FATAL.
 *
 * Must be kept in sync with default_version in time_series.control.
 */
#define TIME_SERIES_SO_VERSION "1.0"

PG_MODULE_MAGIC;

/* Cached time_series namespace OID */
static Oid ht_cached_namespace_oid = InvalidOid;
static bool ht_namespace_oid_valid = false;

/*
 * ht_namespace_invalidation_cb
 *		Syscache invalidation callback that clears the cached
 *		time_series namespace OID when the namespace catalog changes.
 */
static void
ht_namespace_invalidation_cb(Datum arg, int cacheid, uint32 hashvalue)
{
	ht_namespace_oid_valid = false;
}

/*
 * ht_get_namespace_oid_cached
 *		Return the OID of the time_series schema, caching the result
 *		so repeated lookups within the same session avoid syscache
 *		searches.  Returns InvalidOid if the extension is not installed.
 */
Oid
ht_get_namespace_oid_cached(void)
{
	/*
	 * Validate the cached namespace OID on every call.  DROP EXTENSION
	 * CASCADE on time_series may also drop the time_series schema (if
	 * it's empty by then), invalidating the cached OID.  Without this
	 * recheck, a stale OID would silently pass OidIsValid() and any
	 * caller would then ask the syscache for a no-longer-existing
	 * schema.  Cheap because SearchSysCacheExists1 hits a small
	 * dedicated cache.
	 */
	if (ht_namespace_oid_valid &&
		OidIsValid(ht_cached_namespace_oid) &&
		!SearchSysCacheExists1(NAMESPACEOID,
							   ObjectIdGetDatum(ht_cached_namespace_oid)))
	{
		ht_namespace_oid_valid = false;
		ht_cached_namespace_oid = InvalidOid;
	}

	if (!ht_namespace_oid_valid)
	{
		ht_cached_namespace_oid = get_namespace_oid(
			TS_EXTENSION_SCHEMA_NAME, true);
		ht_namespace_oid_valid = true;
	}
	return ht_cached_namespace_oid;
}

/* ============================================================
 * Extension state machine
 *
 * Modelled after upstream src/extension.c.  Tracks whether
 * the time_series extension is currently installed in this
 * database so that hooks installed by _PG_init (which survive
 * DROP EXTENSION because PG keeps preloaded .so files resident)
 * can short-circuit before touching extension-owned catalog
 * tables.
 *
 * The "proxy table" is time_series.continuous_agg — owned by
 * the extension, created at install time, and dropped early in
 * the DROP EXTENSION CASCADE sequence.  We register a relcache
 * callback so that drops of that relation invalidate the
 * cached state immediately.
 * ============================================================ */

enum TsExtensionState
{
	TS_EXT_STATE_UNKNOWN = 0,
	TS_EXT_STATE_TRANSITIONING,
	TS_EXT_STATE_CREATED,
	TS_EXT_STATE_NOT_INSTALLED,
};

static enum TsExtensionState extstate = TS_EXT_STATE_UNKNOWN;
static Oid extension_proxy_oid = InvalidOid;

/*
 * Look up the SQL-script version recorded in pg_extension.extversion
 * for the given extension name.  Returns a palloc'd string in the
 * current memory context, or NULL if the extension is not installed.
 *
 * Modelled after upstream extension_version() in extension_utils.c.
 */
static char *
extension_sql_version(const char *extname)
{
	Relation		rel;
	SysScanDesc		scandesc;
	HeapTuple		tuple;
	ScanKeyData		entry[1];
	char		   *sql_version = NULL;

	rel = table_open(ExtensionRelationId, AccessShareLock);

	ScanKeyInit(&entry[0],
				Anum_pg_extension_extname,
				BTEqualStrategyNumber,
				F_NAMEEQ,
				CStringGetDatum(extname));

	scandesc = systable_beginscan(rel, ExtensionNameIndexId, true, NULL,
								  1, entry);

	tuple = systable_getnext(scandesc);
	if (HeapTupleIsValid(tuple))
	{
		bool	is_null = true;
		Datum	result = heap_getattr(tuple,
									  Anum_pg_extension_extversion,
									  RelationGetDescr(rel),
									  &is_null);

		if (!is_null)
			sql_version = pstrdup(TextDatumGetCString(result));
	}

	systable_endscan(scandesc);
	table_close(rel, AccessShareLock);

	return sql_version;
}

/*
 * Verify that the SQL-script version (pg_extension.extversion) and
 * the SO version compiled into this shared library agree.  Mismatch
 * means a long-lived backend is running stale C code against an
 * upgraded catalog schema; raise FATAL so the client reconnects and
 * picks up the new .so.
 *
 * Mirrors upstream extension_check_version().
 */
static void
extension_check_version(const char *so_version)
{
	char	   *sql_version;

	if (!IsNormalProcessingMode() || !IsTransactionState())
		return;

	sql_version = extension_sql_version(TS_EXT_NAME);
	if (sql_version == NULL)
		return;					/* extension not installed */

	if (strcmp(sql_version, so_version) != 0)
	{
		ereport(FATAL,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("extension \"%s\" version mismatch: "
						"shared library version %s; SQL version %s",
						TS_EXT_NAME, so_version, sql_version),
				 errhint("Reconnect so the new shared library is loaded.")));
	}

	pfree(sql_version);
}

static bool
extension_is_transitioning(void)
{
	/*
	 * creating_extension is set by ProcessUtility while a CREATE
	 * EXTENSION or ALTER EXTENSION ... UPDATE script is running
	 * (the name is a misnomer; it covers upgrades too).
	 */
	if (creating_extension)
		return get_extension_oid(TS_EXT_NAME, true) == CurrentExtensionObject;
	return false;
}

static enum TsExtensionState
extension_current_state(void)
{
	Oid			ns_oid;
	Oid			rel_oid;

	/*
	 * Avoid touching catalog before RelationCacheInitializePhase3
	 * has run; otherwise we may recurse through the cache machinery.
	 */
	if (!IsNormalProcessingMode() || !IsTransactionState() ||
		!OidIsValid(MyDatabaseId))
		return TS_EXT_STATE_UNKNOWN;

	if (extension_is_transitioning())
		return TS_EXT_STATE_TRANSITIONING;

	ns_oid = get_namespace_oid(TS_EXT_PROXY_SCHEMA, true);
	if (!OidIsValid(ns_oid))
		return TS_EXT_STATE_NOT_INSTALLED;

	rel_oid = get_relname_relid(TS_EXT_PROXY_TABLE, ns_oid);
	if (OidIsValid(rel_oid))
		return TS_EXT_STATE_CREATED;

	return TS_EXT_STATE_NOT_INSTALLED;
}

static void
extension_update_state(void)
{
	enum TsExtensionState new_state = extension_current_state();

	/*
	 * Never settle on NOT_INSTALLED: if the extension is dropped and
	 * re-created in another backend, the proxy table relid changes
	 * and we cannot reliably detect it through invalidation events.
	 * Falling back to UNKNOWN forces a fresh catalog lookup on the
	 * next state query.
	 */
	if (new_state == TS_EXT_STATE_NOT_INSTALLED)
		new_state = TS_EXT_STATE_UNKNOWN;

	if (new_state == TS_EXT_STATE_CREATED)
	{
		Oid			ns = get_namespace_oid(TS_EXT_PROXY_SCHEMA, true);

		/*
		 * Verify the SQL-script version matches the SO version on
		 * every transition into CREATED.  Mismatch raises FATAL,
		 * forcing the client to reconnect and reload the .so.
		 *
		 * Skip during TRANSITIONING (the script is mid-flight, the
		 * extversion column may not yet reflect the new value).
		 */
		if (extstate != TS_EXT_STATE_TRANSITIONING)
			extension_check_version(TIME_SERIES_SO_VERSION);

		extension_proxy_oid = OidIsValid(ns)
			? get_relname_relid(TS_EXT_PROXY_TABLE, ns)
			: InvalidOid;
	}
	else
	{
		extension_proxy_oid = InvalidOid;
	}

	extstate = new_state;
}

void
extension_invalidate(void)
{
	extstate = TS_EXT_STATE_UNKNOWN;
	extension_proxy_oid = InvalidOid;
}

bool
extension_is_loaded(void)
{
	if (extstate == TS_EXT_STATE_UNKNOWN ||
		extstate == TS_EXT_STATE_TRANSITIONING)
		extension_update_state();

	return extstate == TS_EXT_STATE_CREATED;
}

bool
extension_is_loaded_and_not_upgrading(void)
{
	/*
	 * During pg_upgrade --binary-upgrade, hooks must yield so that
	 * pg_upgrade can recreate state without our triggers and BGW
	 * scheduler firing on every replayed statement.
	 */
	if (IsBinaryUpgrade)
		return false;

	return extension_is_loaded();
}

/*
 * Relcache invalidation callback.
 *
 * Per PG conventions, this MUST NOT call functions that themselves
 * touch the relcache or syscache (risk of recursion / inconsistent
 * state).  We just flip the state back to UNKNOWN; the next call
 * to extension_is_loaded() will do the real lookup.
 */
static void
extension_relcache_callback(Datum arg, Oid relid)
{
	if (!OidIsValid(relid))
	{
		/* Whole-cache invalidation. */
		extension_invalidate();
		return;
	}

	if (OidIsValid(extension_proxy_oid) &&
		relid == extension_proxy_oid)
		extension_invalidate();
}

void		_PG_init(void);

void
_PG_init(void)
{
	/*
	 * Access / storage layer for the time_series Table AM.
	 */
	ts_tableam_init();

	/* GapFill: Custom Scan provider + planner hook.  Registered on both
	 * QD and QEs so plan nodes deserialise after dispatch. */
	ht_gapfill_scan_init();
	ht_gapfill_planner_init();

	/* CAGG planner_hook for parse-tree rewrites (cagg_watermark
	 * const-fold + time_bucket pushdown).  Chained AFTER
	 * ht_gapfill_planner_init so this one runs first and delegates to
	 * the gapfill hook via prev_planner_hook. */
	cagg_planner_hook_init();

	/* Relcache-inval callback that keeps the backend-local watermark
	 * cache (watermark_constify.c) coherent with cagg_refresh's
	 * watermark advances. */
	cagg_watermark_cache_init();

	/* CAGG ProcessUtility hook (CREATE MATERIALIZED VIEW WITH
	 * (time_series.continuous), REFRESH, DROP). */
	ht_cagg_init();

	/* Syscache callback for namespace OID invalidation. */
	CacheRegisterSyscacheCallback(NAMESPACENAME,
								  ht_namespace_invalidation_cb,
								  (Datum) 0);

	/* 8. Register relcache callback for extension state invalidation.
	 *     Fires whenever any relation's relcache entry changes; we
	 *     filter for the proxy table (time_series.continuous_agg)
	 *     inside the callback. */
	CacheRegisterRelcacheCallback(extension_relcache_callback,
								  (Datum) 0);

	/* 9. Define scan-path GUCs */

	/* CAGG-owned GUCs (defined in cagg/create.c). */
	cagg_define_gucs();

	/*
	 * 11. Bring up the bgworker subsystem: GUCs, shmem reservations,
	 * shmem_startup_hook chaining, and launcher registration.  See
	 * bgw/bgw_init.c for details.
	 */
	ts_bgw_init();

	elog(LOG, "time_series _PG_init: hooks installed (gapfill scan, "
		 "gapfill planner, CAGG ProcessUtility, namespace cache, "
		 "extension relcache); GUCs defined; BGW scheduler registered");
}
