/*-------------------------------------------------------------------------
 *
 * ts_compress.c
 * Chunk compression configuration and metadata management.
 *
 * Provides SQL-callable functions for configuring compression
 * parameters (segmentby/orderby), compressing eligible chunks,
 * and querying compressed chunk metadata.
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 * IDENTIFICATION
 * contrib/time_series/src/compress/ts_compress.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>

#include "access/heapam.h"
#include "access/stratnum.h"
#include "access/xact.h"
#include "executor/spi.h"
#include "parser/parser.h"
#include "nodes/parsenodes.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "access/tableam.h"
#include "catalog/indexing.h"
#include "catalog/storage.h"
#include "catalog/namespace.h"
#include "catalog/pg_am_d.h"
#include "commands/defrem.h"
#include "utils/inval.h"
#include "utils/syscache.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "utils/array.h"
#include "storage/bufmgr.h"
#include "storage/proc.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/acl.h"
#include "utils/faultinjector.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "utils/tuplesort.h"
#include "cdb/cdbvars.h"

#include "../include/time_series.h"
#include "../include/access/ts_tableam.h"
#include "../include/compress/ts_compress.h"
#include "../include/access/ts_wal.h"

PG_FUNCTION_INFO_V1(ts_set_compress_config);
PG_FUNCTION_INFO_V1(ts_compress_chunks);
PG_FUNCTION_INFO_V1(ts_compress_chunk);
PG_FUNCTION_INFO_V1(ts_compress_write_chunks);
PG_FUNCTION_INFO_V1(ts_reclaim_chunk_heaps);
PG_FUNCTION_INFO_V1(ts_reclaim_chunk_heaps_segment);
PG_FUNCTION_INFO_V1(ts_compressed_chunk_info);
PG_FUNCTION_INFO_V1(ts_truncate_chunk_fork);

/* Cached OIDs for compress catalog tables */
static Oid	ts_compress_config_relid = InvalidOid;
static Oid	ts_compressed_chunk_relid = InvalidOid;

/*
 * These OIDs are sticky for the backend's lifetime only as long as
 * ts_compress_config / ts_compressed_chunk never disappear out from
 * under them.  That breaks across a same-session DROP EXTENSION
 * time_series [CASCADE]; CREATE EXTENSION time_series; -- the
 * extension owns its schema, so DROP EXTENSION drops both tables and
 * CREATE EXTENSION recreates them under new OIDs.  Same fix as
 * ts_chunk_relid / ts_chunk_oid_chunknum_idx_relid in ts_catalog.c: a
 * relcache invalidation callback (no catalog access in its body, so
 * it doesn't reintroduce the SearchSysCacheExists1-correlated SIGSEGV
 * documented on ts_chunk_ensure_oid), registered lazily on first use.
 */
static bool	ts_compress_oid_callback_registered = false;

static void
ts_compress_oid_relcache_callback(Datum arg, Oid relid)
{
	if (!OidIsValid(relid) ||
		relid == ts_compress_config_relid ||
		relid == ts_compressed_chunk_relid)
	{
		ts_compress_config_relid = InvalidOid;
		ts_compressed_chunk_relid = InvalidOid;
	}
}

static void
ts_compress_oid_callback_ensure_registered(void)
{
	if (ts_compress_oid_callback_registered)
		return;
	CacheRegisterRelcacheCallback(ts_compress_oid_relcache_callback, (Datum) 0);
	ts_compress_oid_callback_registered = true;
}

/*
 * int32_datum_cmp — qsort comparator for a Datum array holding int32
 * chunk numbers.  Used by reclaim to sort input chunks ascending so
 * advisory ExclusiveLock acquisition order matches the ShareLock
 * order that multi-insert already follows for append-mostly streams
 * (see the call site in ts_reclaim_chunk_heaps_segment for the ABBA
 * argument).
 */
static int
int32_datum_cmp(const void *a, const void *b)
{
	int32	x = DatumGetInt32(*(const Datum *) a);
	int32	y = DatumGetInt32(*(const Datum *) b);
	return (x > y) - (x < y);
}

/*
 * ChunkInfoEntry — per-chunk metadata for ts_compressed_chunk_info SRF.
 * Collected in the first call and stored in funcctx->user_fctx.
 */
typedef struct ChunkInfoEntry
{
	int32			chunk_number;
	TimestampTz		range_start;
	TimestampTz		range_end;
	int16			status;
	char		   *pax_file;			/* NULL if not compressed */
	int64			numrows;			/* 0 if not compressed */
	bool			numrows_null;
	TimestampTz		compressed_at;
	bool			compressed_at_null;
} ChunkInfoEntry;

/*
 * ChunkToCompress — one chunk pending compression.
 */
typedef struct ChunkToCompress
{
	int32	chunk_num;
} ChunkToCompress;

/* ----------------------------------------------------------------
 *		OID cache helpers
 * ----------------------------------------------------------------
 */

static bool
ts_compress_config_ensure_oid(void)
{
	Oid		ns_oid;

	ts_compress_oid_callback_ensure_registered();

	if (OidIsValid(ts_compress_config_relid))
		return true;

	ns_oid = ht_get_namespace_oid_cached();
	if (!OidIsValid(ns_oid))
		return false;

	ts_compress_config_relid = get_relname_relid("ts_compress_config", ns_oid);
	return OidIsValid(ts_compress_config_relid);
}

static bool
ts_compressed_chunk_ensure_oid(void)
{
	Oid		ns_oid;

	ts_compress_oid_callback_ensure_registered();

	if (OidIsValid(ts_compressed_chunk_relid))
		return true;

	ns_oid = ht_get_namespace_oid_cached();
	if (!OidIsValid(ns_oid))
		return false;

	ts_compressed_chunk_relid = get_relname_relid("ts_compressed_chunk", ns_oid);
	return OidIsValid(ts_compressed_chunk_relid);
}

/* ----------------------------------------------------------------
 *		ts_compress_config_delete
 *
 *		Direct heap delete (no SPI) so we can be called from
 *		ProcessUtility-hook context on both master and segments.  SPI
 *		DELETE statements are forbidden on QE slices ("function cannot
 *		execute on a QE slice because it issues a non-SELECT statement").
 *
 *		Uses SnapshotSelf to see the latest committed state regardless
 *		of the enclosing DROP statement's distributed snapshot — see
 *		the rationale in ts_chunk_catalog_delete.
 * ----------------------------------------------------------------
 */
void
ts_compress_config_delete(Oid table_oid)
{
	Relation		rel;
	TableScanDesc	hscan;
	ScanKeyData		skey[1];
	HeapTuple		tup;

	if (!ts_compress_config_ensure_oid())
		return;

	rel = table_open(ts_compress_config_relid, RowExclusiveLock);

	ScanKeyInit(&skey[0], Anum_cc_table_oid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(table_oid));

	hscan = table_beginscan(rel, SnapshotSelf, 1, skey);
	while ((tup = heap_getnext(hscan, ForwardScanDirection)) != NULL)
		CatalogTupleDelete(rel, &tup->t_self);
	table_endscan(hscan);

	table_close(rel, RowExclusiveLock);
}

/* ----------------------------------------------------------------
 *		ts_compressed_chunk_delete
 *
 *		Same SnapshotSelf rationale as ts_compress_config_delete.
 * ----------------------------------------------------------------
 */
void
ts_compressed_chunk_delete(Oid table_oid)
{
	Relation		rel;
	TableScanDesc	hscan;
	ScanKeyData		skey[1];
	HeapTuple		tup;

	if (!ts_compressed_chunk_ensure_oid())
		return;

	rel = table_open(ts_compressed_chunk_relid, RowExclusiveLock);

	ScanKeyInit(&skey[0], Anum_cchunk_table_oid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(table_oid));

	hscan = table_beginscan(rel, SnapshotSelf, 1, skey);
	while ((tup = heap_getnext(hscan, ForwardScanDirection)) != NULL)
		CatalogTupleDelete(rel, &tup->t_self);
	table_endscan(hscan);

	table_close(rel, RowExclusiveLock);
}

/* ----------------------------------------------------------------
 *		ts_pax_remove_reldir
 *
 *		Drop the PAX sidecar directory for a relation, if present.
 *
 *		Used at DROP TABLE time: catalog rows are removed by
 *		ts_compressed_chunk_delete; this removes the on-disk
 *		chunk_<N>.pax[.seg<M>] files that live outside the heap.
 *
 *		access(F_OK) avoids rmtree's spurious WARNING when the
 *		directory was never created (uncompressed table).
 * ----------------------------------------------------------------
 */
void
ts_pax_remove_reldir(Oid dbid, Oid relid)
{
	char	dir[MAXPGPATH];

	snprintf(dir, sizeof(dir), TS_PAX_RELDIR_FMT, dbid, relid);
	if (access(dir, F_OK) == 0)
	{
		/*
		 * WAL the removal BEFORE rmtree.  rmtree is non-transactional
		 * and irreversible; if we shrunk the primary's fs and then
		 * crashed before the WAL reached stable storage, standbys
		 * would keep the PAX dir forever.  ts_wal_pax_remove_dir
		 * XLogFlush's internally — matches ts_wal_fork_truncate's
		 * pattern for the same reason.
		 */
		ts_wal_pax_remove_dir(dbid, relid);

		if (!rmtree(dir, true))
			ereport(WARNING,
					(errmsg("could not remove PAX directory \"%s\": %m", dir)));
	}
}

/* ----------------------------------------------------------------
 *		Pending PAX directory removals — piggybacked on PG core's
 *		smgrDoPendingDeletes() machinery via RegisterPendingDelete().
 *
 *		ts_pax_remove_reldir() runs rmtree() immediately and bypasses
 *		PG's pending-deletes machinery.  That worked for unconditional
 *		DROP-COMMIT, but a transactional DROP that ROLLBACKs would
 *		find the catalog rows restored while the PAX dir was already
 *		gone — silent data loss.
 *
 *		Modelled after src/backend/catalog/storage_directory_table.c's
 *		UFileAddPendingDelete: we attach a custom PendingRelDeleteAction
 *		whose do_pending_rel_delete callback fires rmtree, then push
 *		the entry into the same pendingDeletes list smgr uses.  Core
 *		then drives the entry through xact commit, xact abort, sub-xact
 *		abort (discards atCommit=true at that level), and sub-xact
 *		commit (promotes nestLevel to parent) without any extension-
 *		side bookkeeping.
 * ----------------------------------------------------------------
 */
typedef struct PaxPendingDelete
{
	PendingRelDelete	reldelete;	/* base struct — must be first */
	Oid					dbid;
	Oid					relid;
} PaxPendingDelete;

static void
ts_pax_do_pending_delete(PendingRelDelete *reldelete)
{
	PaxPendingDelete *p = (PaxPendingDelete *) reldelete;

	ts_pax_remove_reldir(p->dbid, p->relid);
}

static void
ts_pax_destroy_pending_delete(PendingRelDelete *reldelete)
{
	pfree(reldelete);
}

static struct PendingRelDeleteAction ts_pax_pending_delete_action = {
	.flags = PENDING_REL_DELETE_DEFAULT_FLAG,
	.destroy_pending_rel_delete = ts_pax_destroy_pending_delete,
	.do_pending_rel_delete = ts_pax_do_pending_delete,
};

void
ts_pax_register_pending_removal(Oid dbid, Oid relid, bool atCommit)
{
	PaxPendingDelete *p;

	/*
	 * TopMemoryContext: PG's pendingDeletes survives until
	 * smgrDoPendingDeletes runs at the outer xact end; allocate
	 * accordingly.  Mirrors UFileAddPendingDelete exactly.
	 */
	p = (PaxPendingDelete *) MemoryContextAllocZero(TopMemoryContext,
													sizeof(PaxPendingDelete));
	p->reldelete.action = &ts_pax_pending_delete_action;
	p->reldelete.atCommit = atCommit;
	p->reldelete.nestLevel = GetCurrentTransactionNestLevel();
	/* reldelete.relnode is unused — our action keys on (dbid, relid). */
	p->dbid = dbid;
	p->relid = relid;
	RegisterPendingDelete(&p->reldelete);
}

/* ----------------------------------------------------------------
 *		ts_compressed_chunk_has_any
 *
 *		Cheap existence probe: returns true on the first
 *		ts_compressed_chunk row seen for table_oid.  Used by DDL guards
 *		that need to gate on "table has PAX data on disk" without paying
 *		for a full numrows scan.
 * ----------------------------------------------------------------
 */
bool
ts_compressed_chunk_has_any(Oid table_oid)
{
	Relation		rel;
	TableScanDesc	hscan;
	ScanKeyData		skey[1];
	HeapTuple		tup;
	bool			has_any;

	if (!ts_compressed_chunk_ensure_oid())
		return false;

	rel = table_open(ts_compressed_chunk_relid, AccessShareLock);

	ScanKeyInit(&skey[0], Anum_cchunk_table_oid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(table_oid));

	/* SnapshotSelf — see scan_heap_to_pax_sorted rationale (CBDB distributed
	 * snapshot misses concurrently-committed rows; we always want the latest
	 * catalog state here). */
	hscan = table_beginscan(rel, SnapshotSelf, 1, skey);
	tup = heap_getnext(hscan, ForwardScanDirection);
	has_any = (tup != NULL);
	table_endscan(hscan);

	table_close(rel, AccessShareLock);
	return has_any;
}

/* ----------------------------------------------------------------
 *		ts_compressed_chunk_load_numrows
 *
 *		Load numrows for each COMPRESSED chunk in chunk_list.  Used by
 *		the count-only scan fast path to avoid opening PAX files.
 *		Returns a palloc'd int64 array; index i holds the row count for
 *		chunk_list[i] if status is COMPRESSED, otherwise 0.
 * ----------------------------------------------------------------
 */
int64 *
ts_compressed_chunk_load_numrows(Oid table_oid, ForkNumber *chunk_list,
								 int16 *chunk_status, int nchunks)
{
	int64		   *result;
	Relation		rel;
	TableScanDesc	hscan;
	ScanKeyData		skey[1];
	HeapTuple		tup;

	result = (int64 *) palloc0(sizeof(int64) * nchunks);

	if (!ts_compressed_chunk_ensure_oid())
		return result;

	rel = table_open(ts_compressed_chunk_relid, AccessShareLock);

	ScanKeyInit(&skey[0], Anum_cchunk_table_oid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(table_oid));

	/* SnapshotSelf — see scan_heap_to_pax_sorted rationale (CBDB distributed
	 * snapshot misses concurrently-committed rows; we always want the latest
	 * catalog state here). */
	hscan = table_beginscan(rel, SnapshotSelf, 1, skey);

	while ((tup = heap_getnext(hscan, ForwardScanDirection)) != NULL)
	{
		Datum	d_cnum, d_numrows;
		bool	isnull;
		int32	cnum;
		int64	numrows;

		d_cnum = heap_getattr(tup, Anum_cchunk_chunk_number,
							  RelationGetDescr(rel), &isnull);
		if (isnull)
			continue;
		cnum = DatumGetInt32(d_cnum);

		d_numrows = heap_getattr(tup, Anum_cchunk_numrows,
								 RelationGetDescr(rel), &isnull);
		if (isnull)
			continue;
		numrows = DatumGetInt64(d_numrows);

		/* Find the matching entry in chunk_list */
		for (int i = 0; i < nchunks; i++)
		{
			if (chunk_list[i] == (ForkNumber) cnum &&
				(chunk_status == NULL ||
				 chunk_status[i] == TS_CHUNK_COMPRESSED))
			{
				result[i] = numrows;
				break;
			}
		}
	}

	table_endscan(hscan);
	table_close(rel, AccessShareLock);

	return result;
}

/* ----------------------------------------------------------------
 *		ts_compress_config_load
 * ----------------------------------------------------------------
 */
bool
ts_compress_config_load(Oid table_oid, TSCompressConfig *config)
{
	Relation		rel;
	TableScanDesc	hscan;
	ScanKeyData		skey[1];
	HeapTuple		tup;
	TupleDesc		tupdesc;
	bool			found = false;

	memset(config, 0, sizeof(TSCompressConfig));
	config->table_oid = table_oid;

	if (!ts_compress_config_ensure_oid())
		return false;

	rel = table_open(ts_compress_config_relid, AccessShareLock);
	tupdesc = RelationGetDescr(rel);

	ScanKeyInit(&skey[0], Anum_cc_table_oid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(table_oid));

	/* SnapshotSelf — see scan_heap_to_pax_sorted rationale (CBDB distributed
	 * snapshot misses concurrently-committed rows; we always want the latest
	 * catalog state here). */
	hscan = table_beginscan(rel, SnapshotSelf, 1, skey);
	tup = heap_getnext(hscan, ForwardScanDirection);

	if (HeapTupleIsValid(tup))
	{
		Datum	d_segmentby, d_orderby, d_orderby_desc, d_orderby_nullsfirst;
		bool	isnull;

		/* segmentby text[] */
		d_segmentby = heap_getattr(tup, Anum_cc_segmentby, tupdesc, &isnull);
		if (!isnull)
		{
			ArrayType  *arr = DatumGetArrayTypeP(d_segmentby);
			int			nelems;
			Datum	   *elems;
			bool	   *nulls;

			deconstruct_array(arr, TEXTOID, -1, false, TYPALIGN_INT,
							  &elems, &nulls, &nelems);
			for (int i = 0; i < nelems && i < TS_MAX_COMPRESS_COLS; i++)
			{
				if (!nulls[i])
				{
					char	   *colname = TextDatumGetCString(elems[i]);
					AttrNumber	attnum = get_attnum(table_oid, colname);

					if (attnum != InvalidAttrNumber)
					{
						config->segmentby_attnums[i] = attnum;
						config->segmentby_types[i] = get_atttype(table_oid, attnum);
						config->n_segmentby = i + 1;
						found = true;
					}
				}
			}
		}

		/* orderby text[] */
		d_orderby = heap_getattr(tup, Anum_cc_orderby, tupdesc, &isnull);
		if (!isnull)
		{
			ArrayType  *arr = DatumGetArrayTypeP(d_orderby);
			int			nelems;
			Datum	   *elems;
			bool	   *nulls;

			deconstruct_array(arr, TEXTOID, -1, false, TYPALIGN_INT,
							  &elems, &nulls, &nelems);
			for (int i = 0; i < nelems && i < TS_MAX_COMPRESS_COLS; i++)
			{
				if (!nulls[i])
				{
					char	   *colname = TextDatumGetCString(elems[i]);
					AttrNumber	attnum = get_attnum(table_oid, colname);

					if (attnum != InvalidAttrNumber)
					{
						config->orderby_attnums[i] = attnum;
						config->orderby_types[i] = get_atttype(table_oid, attnum);
						config->n_orderby = i + 1;
						found = true;
					}
				}
			}
		}

		/* orderby_desc bool[] */
		d_orderby_desc = heap_getattr(tup, Anum_cc_orderby_desc, tupdesc, &isnull);
		if (!isnull && config->n_orderby > 0)
		{
			ArrayType  *arr = DatumGetArrayTypeP(d_orderby_desc);
			int			nelems;
			Datum	   *elems;
			bool	   *nulls;

			deconstruct_array(arr, BOOLOID, 1, true, TYPALIGN_CHAR,
							  &elems, &nulls, &nelems);
			for (int i = 0; i < nelems && i < config->n_orderby; i++)
			{
				if (!nulls[i])
					config->orderby_desc[i] = DatumGetBool(elems[i]);
			}
		}

		/*
		 * orderby_nullsfirst bool[] — defaults to PG ORDER BY default
		 * if the row was written by an older version of set_compress_config
		 * that didn't populate the column: ASC → NULLS LAST (false),
		 * DESC → NULLS FIRST (true).
		 */
		d_orderby_nullsfirst = heap_getattr(tup, Anum_cc_orderby_nullsfirst,
											 tupdesc, &isnull);
		if (!isnull && config->n_orderby > 0)
		{
			ArrayType  *arr = DatumGetArrayTypeP(d_orderby_nullsfirst);
			int			nelems;
			Datum	   *elems;
			bool	   *nulls;

			deconstruct_array(arr, BOOLOID, 1, true, TYPALIGN_CHAR,
							  &elems, &nulls, &nelems);
			for (int i = 0; i < nelems && i < config->n_orderby; i++)
			{
				if (!nulls[i])
					config->orderby_nullsfirst[i] = DatumGetBool(elems[i]);
				else
					config->orderby_nullsfirst[i] = config->orderby_desc[i];
			}
		}
		else
		{
			for (int i = 0; i < config->n_orderby; i++)
				config->orderby_nullsfirst[i] = config->orderby_desc[i];
		}
	}

	table_endscan(hscan);
	table_close(rel, AccessShareLock);

	return found;
}

/* ----------------------------------------------------------------
 *		Helper: trim leading/trailing whitespace in-place
 * ----------------------------------------------------------------
 */
static char *
trim_whitespace(char *str)
{
	char *end;

	while (*str == ' ' || *str == '\t') str++;

	if (*str == '\0')
		return str;

	end = str + strlen(str) - 1;
	while (end > str && (*end == ' ' || *end == '\t')) end--;
	*(end + 1) = '\0';

	return str;
}

/* ----------------------------------------------------------------
 *		ts_set_compress_config — SQL function
 * ----------------------------------------------------------------
 */

/*
 * parse_segmentby_list
 *		Comma-split the user's segmentby spec, look up each column
 *		on `relid`, and fill the (seg_names, seg_types) arrays.  Caller
 *		holds AccessShareLock on `userrel`; on error we release it
 *		before raising.  Returns the number of columns parsed.
 */
static int
parse_segmentby_list(Oid relid, Relation userrel, text *segmentby_text,
					 char **seg_names, Oid *seg_types)
{
	char	   *segmentby_str;
	char	   *token;
	char	   *saveptr;
	int			n_seg = 0;

	if (segmentby_text == NULL)
		return 0;

	segmentby_str = text_to_cstring(segmentby_text);
	token = strtok_r(segmentby_str, ",", &saveptr);

	while (token != NULL && n_seg < TS_MAX_COMPRESS_COLS)
	{
		char	   *colname = trim_whitespace(token);
		AttrNumber	attnum;

		if (*colname == '\0')
		{
			token = strtok_r(NULL, ",", &saveptr);
			continue;
		}

		attnum = get_attnum(relid, colname);
		if (attnum == InvalidAttrNumber)
		{
			char   *rn = pstrdup(RelationGetRelationName(userrel));

			table_close(userrel, AccessShareLock);
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("column \"%s\" does not exist in relation \"%s\"", colname, rn)));
		}

		seg_names[n_seg] = colname;
		seg_types[n_seg] = get_atttype(relid, attnum);
		n_seg++;
		token = strtok_r(NULL, ",", &saveptr);
	}
	return n_seg;
}

/*
 * orderby_parse_via_select
 *		Re-use PG's own ORDER BY parser by embedding the user's spec in
 *		`SELECT FROM rel ORDER BY <spec>` and running raw_parser().
 *		Returns the parsed SortBy list; raises a friendly error on
 *		syntax failure.
 */
static List *
orderby_parse_via_select(Oid relid, const char *orderby_str)
{
	StringInfoData	buf;
	List		   *parsed;
	RawStmt		   *raw;
	SelectStmt	   *select;
	MemoryContext	oldcontext = CurrentMemoryContext;

	initStringInfo(&buf);
	appendStringInfo(&buf, "SELECT FROM %s.%s ORDER BY %s",
					 quote_identifier(get_namespace_name(get_rel_namespace(relid))),
					 quote_identifier(get_rel_name(relid)),
					 orderby_str);

	PG_TRY();
	{
		parsed = raw_parser(buf.data, RAW_PARSE_DEFAULT);
	}
	PG_CATCH();
	{
		ErrorData *edata;

		/* Switch out of ErrorContext before copying. */
		MemoryContextSwitchTo(oldcontext);
		edata = CopyErrorData();
		if (edata->sqlerrcode == ERRCODE_SYNTAX_ERROR)
		{
			char *msg_copy = pstrdup(orderby_str);

			FlushErrorState();
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("could not parse orderby \"%s\"", msg_copy),
					 errhint("orderby accepts the SQL ORDER BY column-list "
							 "syntax: column [ASC|DESC] [NULLS FIRST|NULLS LAST],"
							 " comma-separated.")));
		}
		ReThrowError(edata);
	}
	PG_END_TRY();

	pfree(buf.data);

	if (list_length(parsed) != 1 || !IsA(linitial(parsed), RawStmt))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not parse orderby \"%s\"", orderby_str)));
	raw = linitial(parsed);
	if (!IsA(raw->stmt, SelectStmt))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not parse orderby \"%s\"", orderby_str)));
	select = (SelectStmt *) raw->stmt;
	if (select->sortClause == NIL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("orderby cannot be empty")));
	return select->sortClause;
}

/*
 * parse_orderby_list
 *		Drive orderby_parse_via_select, then walk the SortBy list and
 *		fill (ord_names, ord_types, ord_desc, ord_nullsfirst) arrays.
 *		Validates plain ColumnRef shape, rejects USING <op>, applies
 *		PG's default NULLS placement.  Returns the number of columns.
 */
static int
parse_orderby_list(Oid relid, Relation userrel, text *orderby_text,
				   char **ord_names, Oid *ord_types,
				   bool *ord_desc, bool *ord_nullsfirst)
{
	char	   *orderby_str;
	List	   *sort_clause;
	ListCell   *lc;
	int			n_ord = 0;

	if (orderby_text == NULL)
		return 0;

	orderby_str = text_to_cstring(orderby_text);
	if (orderby_str[0] == '\0')
	{
		table_close(userrel, AccessShareLock);
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("orderby cannot be empty")));
	}

	sort_clause = orderby_parse_via_select(relid, orderby_str);

	foreach(lc, sort_clause)
	{
		SortBy	   *sb;
		ColumnRef  *cf;
		char	   *colname;
		AttrNumber	attnum;
		bool		desc;

		if (n_ord >= TS_MAX_COMPRESS_COLS)
		{
			table_close(userrel, AccessShareLock);
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("too many orderby columns (max %d)", TS_MAX_COMPRESS_COLS)));
		}

		if (!IsA(lfirst(lc), SortBy))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("could not parse orderby \"%s\"", orderby_str)));
		sb = lfirst(lc);

		if (!IsA(sb->node, ColumnRef))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("orderby \"%s\" must reference plain column names",
							orderby_str)));
		cf = (ColumnRef *) sb->node;
		if (list_length(cf->fields) != 1 || !IsA(linitial(cf->fields), String))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("orderby \"%s\" must reference plain column names",
							orderby_str)));
		colname = pstrdup(strVal(linitial(cf->fields)));

		if (sb->sortby_dir != SORTBY_ASC && sb->sortby_dir != SORTBY_DESC &&
			sb->sortby_dir != SORTBY_DEFAULT)
		{
			table_close(userrel, AccessShareLock);
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("orderby USING <op> is not supported"),
					 errhint("Use ASC or DESC instead.")));
		}
		desc = (sb->sortby_dir == SORTBY_DESC);

		attnum = get_attnum(relid, colname);
		if (attnum == InvalidAttrNumber)
		{
			char   *rn = pstrdup(RelationGetRelationName(userrel));

			table_close(userrel, AccessShareLock);
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("column \"%s\" does not exist in relation \"%s\"",
							colname, rn)));
		}

		ord_names[n_ord] = colname;
		ord_types[n_ord] = get_atttype(relid, attnum);
		ord_desc[n_ord] = desc;
		/* PG default: ASC → NULLS LAST, DESC → NULLS FIRST. */
		if (sb->sortby_nulls == SORTBY_NULLS_DEFAULT)
			ord_nullsfirst[n_ord] = desc;
		else
			ord_nullsfirst[n_ord] = (sb->sortby_nulls == SORTBY_NULLS_FIRST);
		n_ord++;
	}
	return n_ord;
}

/*
 * insert_compress_config_row
 *		Build and execute the INSERT statement that persists the parsed
 *		configuration to time_series.ts_compress_config.  The unique
 *		key on table_oid ensures a duplicate call raises a unique
 *		violation — we don't pre-check on purpose.
 */
static void
insert_compress_config_row(Oid relid,
						   char **seg_names, int n_seg,
						   char **ord_names, bool *ord_desc, bool *ord_nullsfirst, int n_ord)
{
	StringInfoData	cmd;
	int				i;

	SPI_connect();
	initStringInfo(&cmd);
	appendStringInfo(&cmd, "INSERT INTO time_series.ts_compress_config"
					 " (table_oid, segmentby, orderby, orderby_desc, orderby_nullsfirst)"
					 " VALUES (%u, ", relid);

	if (n_seg > 0)
	{
		appendStringInfoString(&cmd, "ARRAY[");
		for (i = 0; i < n_seg; i++)
		{
			/*
			 * Route the column name through quote_literal_cstr so a
			 * legally-quoted identifier containing a single quote
			 * (e.g. "a''b") does not break out of the SQL string
			 * literal and derail SPI parsing.  Not an exploit path
			 * (only the table owner reaches here) but the raw
			 * appendStringInfo("'%s'") pattern silently fails on
			 * such identifiers.
			 */
			char   *quoted = quote_literal_cstr(seg_names[i]);
			appendStringInfo(&cmd, "%s%s", i > 0 ? "," : "", quoted);
			pfree(quoted);
		}
		appendStringInfoString(&cmd, "]::text[], ");
	}
	else
		appendStringInfoString(&cmd, "NULL, ");

	if (n_ord > 0)
	{
		appendStringInfoString(&cmd, "ARRAY[");
		for (i = 0; i < n_ord; i++)
		{
			char   *quoted = quote_literal_cstr(ord_names[i]);
			appendStringInfo(&cmd, "%s%s", i > 0 ? "," : "", quoted);
			pfree(quoted);
		}
		appendStringInfoString(&cmd, "]::text[], ");

		appendStringInfoString(&cmd, "ARRAY[");
		for (i = 0; i < n_ord; i++)
			appendStringInfo(&cmd, "%s%s", i > 0 ? "," : "", ord_desc[i] ? "true" : "false");
		appendStringInfoString(&cmd, "]::bool[], ");

		appendStringInfoString(&cmd, "ARRAY[");
		for (i = 0; i < n_ord; i++)
			appendStringInfo(&cmd, "%s%s",
							 i > 0 ? "," : "",
							 ord_nullsfirst[i] ? "true" : "false");
		appendStringInfoString(&cmd, "]::bool[])");
	}
	else
		appendStringInfoString(&cmd, "NULL, NULL, NULL)");

	SPI_execute(cmd.data, false, 0);
	pfree(cmd.data);
	SPI_finish();
}

Datum
ts_set_compress_config(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	text	   *segmentby_text = PG_ARGISNULL(1) ? NULL : PG_GETARG_TEXT_PP(1);
	text	   *orderby_text = PG_ARGISNULL(2) ? NULL : PG_GETARG_TEXT_PP(2);
	Relation	userrel;
	char	   *seg_names[TS_MAX_COMPRESS_COLS];
	Oid			seg_types[TS_MAX_COMPRESS_COLS];
	int			n_seg;
	char	   *ord_names[TS_MAX_COMPRESS_COLS];
	Oid			ord_types[TS_MAX_COMPRESS_COLS];
	bool		ord_desc[TS_MAX_COMPRESS_COLS];
	bool		ord_nullsfirst[TS_MAX_COMPRESS_COLS];
	int			n_ord;
	int			i;
	int			j;

	/* Only the table owner may change compression settings. */
	if (!pg_class_ownercheck(relid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_TABLE,
					   get_rel_name(relid));

	userrel = table_open(relid, AccessShareLock);

	if (!RelationIsTimeSeries(userrel))
	{
		char   *relname = pstrdup(RelationGetRelationName(userrel));

		table_close(userrel, AccessShareLock);
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("relation \"%s\" is not a time_series table", relname)));
	}

	n_seg = parse_segmentby_list(relid, userrel, segmentby_text, seg_names, seg_types);
	n_ord = parse_orderby_list(relid, userrel, orderby_text,
							   ord_names, ord_types, ord_desc, ord_nullsfirst);

	/* Validate no overlap between segmentby and orderby. */
	for (i = 0; i < n_seg; i++)
	{
		for (j = 0; j < n_ord; j++)
		{
			if (strcmp(seg_names[i], ord_names[j]) == 0)
			{
				table_close(userrel, AccessShareLock);
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("column \"%s\" cannot be both segmentby and orderby",
								seg_names[i])));
			}
		}
	}

	table_close(userrel, AccessShareLock);

	insert_compress_config_row(relid,
							   seg_names, n_seg,
							   ord_names, ord_desc, ord_nullsfirst, n_ord);

	PG_RETURN_VOID();
}

/*
 * build_minmax_col_idxs
 *		Collect 0-based column indexes for which PAX should track per-group
 *		min/max stats.  We restrict this to segmentby + orderby columns —
 *		those are the only ones that typically appear in WHERE predicates
 *		and benefit from sparse-filter group pruning.  Tracking every column
 *		adds per-row operator lookups that dominate compression time.
 *
 *		Returns number of indexes written to `out` (which must have space
 *		for at least 2 * TS_MAX_COMPRESS_COLS entries).
 */
static int
build_minmax_col_idxs(const TSCompressConfig *config, int *out)
{
	int n = 0;

	for (int i = 0; i < config->n_segmentby; i++)
		out[n++] = config->segmentby_attnums[i] - 1;  /* 1-based -> 0-based */

	for (int i = 0; i < config->n_orderby; i++)
		out[n++] = config->orderby_attnums[i] - 1;

	return n;
}

/*
 * scan_heap_to_pax_sorted
 *		Feed tuples from (optional prior PAX reader) + heap fork into a
 *		tuplesort keyed by (segmentby, orderby), then stream the sorted
 *		output to the PAX writer, flushing a group on every segmentby
 *		boundary or per-group row-limit hit.
 *
 *		`prior_reader` is non-NULL when we are recompressing a PARTIAL
 *		chunk whose previous PAX file already holds older rows — those
 *		must be merged with fresh heap rows so recompression is lossless.
 *		Per-group min/max stats driving scan-time pruning live in the PAX
 *		file footer itself.
 */
static void
scan_heap_to_pax_sorted(Relation rel, ForkNumber forknum, BlockNumber nblocks,
						TSPaxReader prior_reader,
						TSPaxWriter writer, TSCompressConfig *config, int64 *numrows_out)
{
	int				nsortkeys = config->n_segmentby + config->n_orderby;
	AttrNumber	   *tsortkeys = (AttrNumber *) palloc(sizeof(AttrNumber) * nsortkeys);
	Oid			   *sortops = (Oid *) palloc(sizeof(Oid) * nsortkeys);
	Oid			   *collations = (Oid *) palloc(sizeof(Oid) * nsortkeys);
	bool		   *nullsfirst = (bool *) palloc(sizeof(bool) * nsortkeys);
	Tuplesortstate *sortstate;
	TupleTableSlot *heap_slot, *sort_slot;
	Datum			prev_seg_vals[TS_MAX_COMPRESS_COLS];
	bool			prev_seg_nulls[TS_MAX_COMPRESS_COLS];
	bool			first_tuple = true;
	int				cur_group_rows = 0;
	/*
	 * SnapshotSelf, NOT GetTransactionSnapshot().  do_compress_one_chunk
	 * holds the per-chunk advisory ExclusiveLock before this scan, so any
	 * in-flight INSERT into the chunk must commit before we run -- but under
	 * Cloudberry the dispatched command's distributed snapshot is fixed
	 * BEFORE that lock wait.  A row that commits while compress is blocked on
	 * the lock is therefore invisible to GetTransactionSnapshot(); the
	 * visibility check below would skip it, it would never be written to the
	 * PAX file, and the heap-fork reclaim that follows compression would
	 * truncate it away -- a silent loss of a committed source row (with no
	 * invalidation emitted, leaving a continuous aggregate permanently ahead
	 * of the source).  SnapshotSelf consults clog directly and sees every
	 * committed row, which under the ExclusiveLock is exactly the set that
	 * must be compressed.  Matches the SnapshotSelf rationale already used by
	 * ts_chunk_catalog_is_compressed / ts_chunk_catalog_delete.
	 */
	Snapshot		snapshot = SnapshotSelf;

	*numrows_out = 0;

	/* Build sort keys: segmentby cols first, then orderby */
	for (int k = 0; k < config->n_segmentby; k++)
	{
		Oid opclass = GetDefaultOpClass(config->segmentby_types[k], BTREE_AM_OID);
		Oid opfamily = get_opclass_family(opclass);

		tsortkeys[k] = config->segmentby_attnums[k];
		sortops[k] = get_opfamily_member(opfamily, config->segmentby_types[k],
										 config->segmentby_types[k], BTLessStrategyNumber);
		collations[k] = TupleDescAttr(RelationGetDescr(rel),
									  config->segmentby_attnums[k] - 1)->attcollation;
		nullsfirst[k] = false;
	}
	for (int k = 0; k < config->n_orderby; k++)
	{
		int idx = config->n_segmentby + k;
		Oid opclass = GetDefaultOpClass(config->orderby_types[k], BTREE_AM_OID);
		Oid opfamily = get_opclass_family(opclass);
		int strategy = config->orderby_desc[k] ? BTGreaterStrategyNumber : BTLessStrategyNumber;

		tsortkeys[idx] = config->orderby_attnums[k];
		sortops[idx] = get_opfamily_member(opfamily, config->orderby_types[k],
										   config->orderby_types[k], strategy);
		collations[idx] = TupleDescAttr(RelationGetDescr(rel),
										config->orderby_attnums[k] - 1)->attcollation;
		nullsfirst[idx] = config->orderby_nullsfirst[k];
	}

	/* Create tuplesort and feed tuples into it */
	sortstate = tuplesort_begin_heap(RelationGetDescr(rel), nsortkeys, tsortkeys,
									 sortops, collations, nullsfirst, work_mem, NULL, false);

	/*
	 * Merge-in previously-compressed rows (PARTIAL recompression only).
	 * Without this, truncating heap after a successful compress would
	 * cause the next compress to overwrite the PAX file with only the
	 * new heap rows, losing everything that was compressed before.
	 */
	if (prior_reader != NULL)
	{
		TupleTableSlot *pax_slot = MakeTupleTableSlot(RelationGetDescr(rel),
													  &TTSOpsVirtual);
		while (ts_pax_reader_next(prior_reader, pax_slot))
		{
			tuplesort_puttupleslot(sortstate, pax_slot);
			(*numrows_out)++;
		}
		ExecDropSingleTupleTableSlot(pax_slot);
	}

	heap_slot = MakeTupleTableSlot(RelationGetDescr(rel), &TTSOpsHeapTuple);

	for (BlockNumber blkno = 0; blkno < nblocks; blkno++)
	{
		Buffer buf = ReadBufferExtended(rel, forknum, blkno, RBM_NORMAL, NULL);
		Page page;
		OffsetNumber maxoff;

		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);
		if (PageIsNew(page) || PageIsEmpty(page))
		{
			UnlockReleaseBuffer(buf);
			continue;
		}
		maxoff = PageGetMaxOffsetNumber(page);

		for (OffsetNumber off = FirstOffsetNumber; off <= maxoff; off++)
		{
			ItemId itemid = PageGetItemId(page, off);
			HeapTupleData loctup;

			if (!ItemIdIsNormal(itemid))
				continue;

			loctup.t_data = (HeapTupleHeader) PageGetItem(page, itemid);
			loctup.t_len = ItemIdGetLength(itemid);
			loctup.t_tableOid = RelationGetRelid(rel);
			ItemPointerSet(&loctup.t_self, blkno, off);
			if (!HeapTupleSatisfiesVisibility(rel, &loctup, snapshot, buf))
				continue;

			ExecStoreHeapTuple(&loctup, heap_slot, false);
			tuplesort_puttupleslot(sortstate, heap_slot);
		}
		UnlockReleaseBuffer(buf);
	}
	ExecDropSingleTupleTableSlot(heap_slot);
	tuplesort_performsort(sortstate);

	/* Read sorted tuples, flush groups on segmentby change or row limit */
	sort_slot = MakeTupleTableSlot(RelationGetDescr(rel), &TTSOpsMinimalTuple);
	memset(prev_seg_nulls, true, sizeof(prev_seg_nulls));

	while (tuplesort_gettupleslot(sortstate, true, false, sort_slot, NULL))
	{
		slot_getallattrs(sort_slot);

		/*
		 * On a segmentby boundary, flush the current group so the next group
		 * starts fresh — keeps PAX group's rows to a single segmentby value
		 * so per-group min/max stats can prune predicates exactly.
		 *
		 * Suppressed when cur_group_rows is below the floor: at that point
		 * the stripe would be too small for ORC stripe overhead and ZSTD
		 * block compression to amortise (sub-100KB column streams).  The
		 * adjacent slices coalesce and the resulting stripe's per-group
		 * min/max becomes a range instead of a point — sparse filter still
		 * prunes range-disjoint groups, just less aggressively.
		 */
		if (config->n_segmentby > 0 && !first_tuple)
		{
			bool	segmentby_group_changed = false;

			for (int k = 0; k < config->n_segmentby; k++)
			{
				AttrNumber attnum = config->segmentby_attnums[k];
				Form_pg_attribute attr = TupleDescAttr(RelationGetDescr(rel), attnum - 1);
				Datum val = sort_slot->tts_values[attnum - 1];
				bool isnull = sort_slot->tts_isnull[attnum - 1];

				if (isnull != prev_seg_nulls[k] ||
					(!isnull && !datumIsEqual(val, prev_seg_vals[k], attr->attbyval, attr->attlen)))
				{
					segmentby_group_changed = true;
					break;
				}
			}

			if (segmentby_group_changed &&
				cur_group_rows >= TS_MIN_GROUP_ROWS_FOR_SEGMENTBY_FLUSH)
			{
				ts_pax_writer_flush(writer);
				cur_group_rows = 0;
			}
		}

		ts_pax_writer_write_tuple(writer, sort_slot);
		cur_group_rows++;
		(*numrows_out)++;

		if (cur_group_rows >= TS_MAX_TUPLES_PER_GROUP)
		{
			ts_pax_writer_flush(writer);
			cur_group_rows = 0;
		}

		/* Save current segmentby values for next-row comparison */
		if (config->n_segmentby > 0)
		{
			for (int k = 0; k < config->n_segmentby; k++)
			{
				AttrNumber attnum = config->segmentby_attnums[k];
				Form_pg_attribute attr = TupleDescAttr(RelationGetDescr(rel), attnum - 1);
				prev_seg_nulls[k] = sort_slot->tts_isnull[attnum - 1];
				if (!prev_seg_nulls[k])
					prev_seg_vals[k] = datumCopy(sort_slot->tts_values[attnum - 1],
												 attr->attbyval, attr->attlen);
			}
		}
		first_tuple = false;
	}

	ExecDropSingleTupleTableSlot(sort_slot);
	tuplesort_end(sortstate);
	pfree(tsortkeys); pfree(sortops); pfree(collations); pfree(nullsfirst);
}

/*
 * scan_heap_to_pax_unsorted
 *		Stream (optional prior PAX reader) + heap fork in physical order
 *		into the PAX writer.  No sort; used when the config has neither
 *		segmentby nor orderby.  prior_reader is non-NULL on PARTIAL
 *		recompression to carry forward previously-compressed rows (the
 *		writer writes to a fresh path, so the reader can stream from the
 *		old file in parallel).
 */
static void
scan_heap_to_pax_unsorted(Relation rel, ForkNumber forknum, BlockNumber nblocks,
						  TSPaxReader prior_reader,
						  TSPaxWriter writer, int64 *numrows_out)
{
	TupleTableSlot *slot = MakeTupleTableSlot(RelationGetDescr(rel), &TTSOpsHeapTuple);
	/* SnapshotSelf, NOT GetTransactionSnapshot(): see the detailed rationale
	 * in scan_heap_to_pax_sorted -- a row committed while compress blocked on
	 * the per-chunk ExclusiveLock is invisible to the (Cloudberry, fixed-at-
	 * dispatch) transaction snapshot, so it would be skipped here and then
	 * truncated by reclaim, silently losing a committed row. */
	Snapshot		snapshot = SnapshotSelf;

	*numrows_out = 0;

	/* Carry forward previously-compressed rows (see sorted path). */
	if (prior_reader != NULL)
	{
		TupleTableSlot *pax_slot = MakeTupleTableSlot(RelationGetDescr(rel),
													  &TTSOpsVirtual);
		while (ts_pax_reader_next(prior_reader, pax_slot))
		{
			ts_pax_writer_write_tuple(writer, pax_slot);
			(*numrows_out)++;
		}
		ExecDropSingleTupleTableSlot(pax_slot);
	}

	for (BlockNumber blkno = 0; blkno < nblocks; blkno++)
	{
		Buffer buf = ReadBufferExtended(rel, forknum, blkno, RBM_NORMAL, NULL);
		Page page;
		OffsetNumber maxoff;

		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);
		if (PageIsNew(page) || PageIsEmpty(page))
		{
			UnlockReleaseBuffer(buf);
			continue;
		}
		maxoff = PageGetMaxOffsetNumber(page);

		for (OffsetNumber off = FirstOffsetNumber; off <= maxoff; off++)
		{
			ItemId itemid = PageGetItemId(page, off);
			HeapTupleData loctup;

			if (!ItemIdIsNormal(itemid))
				continue;

			loctup.t_data = (HeapTupleHeader) PageGetItem(page, itemid);
			loctup.t_len = ItemIdGetLength(itemid);
			loctup.t_tableOid = RelationGetRelid(rel);
			ItemPointerSet(&loctup.t_self, blkno, off);
			if (!HeapTupleSatisfiesVisibility(rel, &loctup, snapshot, buf))
				continue;

			ExecStoreHeapTuple(&loctup, slot, false);
			ts_pax_writer_write_tuple(writer, slot);
			(*numrows_out)++;
		}
		UnlockReleaseBuffer(buf);
	}

	ExecDropSingleTupleTableSlot(slot);
}

/*
 * compressed_chunk_row_exists
 *		Return true if ts_compressed_chunk has a row for (table_oid,
 *		chunk_number) on this segment.  Used to distinguish a legitimate
 *		PAX file (catalog-tracked, prior compress committed) from an
 *		orphan left behind by a failed compress whose rename(2)
 *		succeeded but whose catalog INSERT did not (rare: catalog write
 *		errored out, or the cluster crashed between rename and commit).
 *		Without this guard, the next compress would open the orphan as
 *		prior_reader and merge its rows with the heap rows that the
 *		failed compress had read but never reclaimed — duplicating every
 *		row in the chunk.
 */
static bool
compressed_chunk_row_exists(Oid table_oid, int32 chunk_number)
{
	Relation		rel;
	TableScanDesc	hscan;
	ScanKeyData		skey[2];
	HeapTuple		tup;
	bool			found;

	if (!ts_compressed_chunk_ensure_oid())
		return false;

	rel = table_open(ts_compressed_chunk_relid, AccessShareLock);

	ScanKeyInit(&skey[0], Anum_cchunk_table_oid,
				BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(table_oid));
	ScanKeyInit(&skey[1], Anum_cchunk_chunk_number,
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(chunk_number));

	/* SnapshotSelf — see scan_heap_to_pax_sorted rationale. */
	hscan = table_beginscan(rel, SnapshotSelf, 2, skey);
	tup = heap_getnext(hscan, ForwardScanDirection);
	found = HeapTupleIsValid(tup);
	table_endscan(hscan);

	table_close(rel, AccessShareLock);
	return found;
}

/*
 * compress_ensure_pax_directories
 *		Create $PGDATA/<TS_PAX_DBDIR>/<relid>/ on demand.  Both levels
 *		are mkdir-if-not-exist; EEXIST is the expected steady-state.
 *		Other errors warn but don't abort — the writer open below will
 *		raise a more specific error.
 */
static void
compress_ensure_pax_directories(Oid relid)
{
	char	dir_path[MAXPGPATH];

	snprintf(dir_path, sizeof(dir_path), TS_PAX_DBDIR_ABS_FMT, DataDir, MyDatabaseId);
	if (MakePGDirectory(dir_path) < 0 && errno != EEXIST)
		elog(WARNING, "could not create directory \"%s\": %m", dir_path);

	snprintf(dir_path, sizeof(dir_path), TS_PAX_RELDIR_ABS_FMT, DataDir, MyDatabaseId, relid);
	if (MakePGDirectory(dir_path) < 0 && errno != EEXIST)
		elog(WARNING, "could not create directory \"%s\": %m", dir_path);
}

/*
 * compress_write_chunk_metadata_row
 *		Upsert one row into ts_compressed_chunk for (relid, chunk_number).
 *		Recompress path deletes any pre-existing row first.  Caller must
 *		hold AccessShareLock on the user relation through this call —
 *		see the design note in do_compress_one_chunk about DROP TABLE
 *		racing with our rename + insert.
 */
static void
compress_write_chunk_metadata_row(Oid relid, int32 chunk_number,
								  TimestampTz range_start, TimestampTz range_end,
								  const char *pax_file_seg,
								  int64 numrows, int32 num_groups, int64 file_size)
{
	Relation		cc_rel;
	TupleDesc		cc_tupdesc;
	Datum			cc_values[Natts_cchunk];
	bool			cc_nulls[Natts_cchunk];
	HeapTuple		cc_tup;
	TableScanDesc	dscan;
	ScanKeyData		dkey[2];
	HeapTuple		dtup;

	/*
	 * Unlike every other user of ts_compressed_chunk_relid in this file,
	 * this call site has no ensure-OID guard of its own: it relies on the
	 * caller's earlier check in do_compress_one_chunk (well before the
	 * heap scan + PAX write + rename(2)s that run between there and
	 * here).  A relcache invalidation from ANY concurrent backend landing
	 * in that window resets the cached OID to InvalidOid (see
	 * ts_compress_oid_relcache_callback), and table_open(InvalidOid, ...)
	 * below would fail with "could not open relation with OID 0" -- after
	 * the PAX file has already been promoted, leaving an orphan file that
	 * the next compress's compressed_chunk_row_exists() check is designed
	 * to detect and recover from, but there's no reason to force that
	 * recovery path on a merely-transient cache miss.  Re-resolving here
	 * is normally a no-op cache hit; it only fails if the catalog table
	 * is genuinely gone.
	 */
	if (!ts_compressed_chunk_ensure_oid())
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("time_series.ts_compressed_chunk catalog table not found"),
				 errhint("The time_series extension appears to be missing or corrupted.")));

	cc_rel = table_open(ts_compressed_chunk_relid, RowExclusiveLock);
	cc_tupdesc = RelationGetDescr(cc_rel);

	/* Delete existing row for this chunk (recompress case) */
	ScanKeyInit(&dkey[0], Anum_cchunk_table_oid,
				BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relid));
	ScanKeyInit(&dkey[1], Anum_cchunk_chunk_number,
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(chunk_number));

	/* GetTransactionSnapshot (NOT SnapshotSelf) here: SCAN+DELETE pattern.
	 * SnapshotSelf would return HOT-pruned tuples whose ctid is freed,
	 * causing CatalogTupleDelete to error with "attempted to update
	 * invisible tuple".  The caller holds the per-chunk advisory
	 * ExclusiveLock so no concurrent xact is racing on this chunk's
	 * ts_compressed_chunk row; the txn snapshot is correct here. */
	dscan = table_beginscan(cc_rel, GetTransactionSnapshot(), 2, dkey);
	while ((dtup = heap_getnext(dscan, ForwardScanDirection)) != NULL)
		CatalogTupleDelete(cc_rel, &dtup->t_self);
	table_endscan(dscan);

	memset(cc_values, 0, sizeof(cc_values));
	memset(cc_nulls, false, sizeof(cc_nulls));
	cc_values[Anum_cchunk_table_oid - 1] = ObjectIdGetDatum(relid);
	cc_values[Anum_cchunk_chunk_number - 1] = Int32GetDatum(chunk_number);
	cc_values[Anum_cchunk_range_start - 1] = TimestampTzGetDatum(range_start);
	cc_values[Anum_cchunk_range_end - 1] = TimestampTzGetDatum(range_end);
	cc_values[Anum_cchunk_pax_file - 1] = CStringGetTextDatum(pax_file_seg);
	cc_values[Anum_cchunk_compressed_at - 1] = TimestampTzGetDatum(GetCurrentTimestamp());
	cc_values[Anum_cchunk_numrows - 1] = Int64GetDatum(numrows);
	cc_values[Anum_cchunk_num_groups - 1] = Int32GetDatum(num_groups);
	cc_values[Anum_cchunk_compressed_size - 1] = Int64GetDatum(file_size);
	cc_nulls[Anum_cchunk_uncompressed_size - 1] = true;

	cc_tup = heap_form_tuple(cc_tupdesc, cc_values, cc_nulls);
	CatalogTupleInsert(cc_rel, cc_tup);
	heap_freetuple(cc_tup);

	table_close(cc_rel, RowExclusiveLock);
}

/*
 * do_compress_one_chunk
 *		Core worker: scan heap, tuplesort, write PAX, insert metadata.
 *		Runs on a single segment for a single chunk.  Derives the PAX file
 *		path and chunk time range from (relid, chunk_number, configs) so the
 *		coordinator doesn't need to dispatch them.  All metadata (numrows,
 *		file_size, num_groups) is persisted directly to ts_compressed_chunk.
 */
static void
do_compress_one_chunk(Oid relid, int32 chunk_number,
					  TSConfig *ts_config, TSCompressConfig *comp_config)
{
	ForkNumber		forknum = (ForkNumber) chunk_number;
	Relation		rel;
	SMgrRelation	reln;
	TSPaxReader		prior_reader = NULL;
	TSPaxWriter		writer;
	char			pax_path[MAXPGPATH];
	char			seg_path[MAXPGPATH];
	char			seg_path_new[MAXPGPATH];
	char			seg_path_toast_new[MAXPGPATH];
	bool			has_toast = false;
	int64			numrows = 0;
	int64			file_size = 0;
	int32			num_groups = 0;
	TimestampTz		range_start;
	TimestampTz		range_end;
	bool			have_sort_config;
	struct stat		stbuf;

	have_sort_config = (comp_config->n_segmentby > 0 || comp_config->n_orderby > 0);

	elog(LOG, "compress chunk: start seg=%d rel=%u chunk=%d sorted=%s",
		 GpIdentity.segindex, relid, chunk_number,
		 have_sort_config ? "yes" : "no");

	/*
	 * Per-chunk advisory ExclusiveLock.  Conflicts with the ShareLock
	 * INSERT takes for the same chunk, so any in-flight INSERT xact must
	 * commit / abort before compress can proceed.  This is what closes
	 * the "INSERT during compress's heap scan" race that would otherwise
	 * leave new rows in heap pages that compress never read, only to be
	 * silently dropped after status flips to COMPRESSED.
	 */
	ts_chunk_lock(relid, chunk_number, ExclusiveLock);

	/*
	 * Coordinator dispatches a chunk to ALL segments when any segment's
	 * row is non-COMPRESSED.  A segment whose local row is already
	 * COMPRESSED would otherwise re-read its still-populated heap fork
	 * (containing the redundant pre-compress copy if reclaim was skipped)
	 * and merge it with the prior PAX, producing duplicates.  Skip the
	 * work and let the coordinator's UPDATE noop the status field.
	 */
	if (ts_chunk_catalog_is_compressed(relid, chunk_number))
	{
		elog(LOG, "compress chunk: skip (already COMPRESSED on this seg) "
			 "seg=%d rel=%u chunk=%d",
			 GpIdentity.segindex, relid, chunk_number);
		return;
	}

	/*
	 * Fail fast if the catalog table is missing — writing PAX files without
	 * the corresponding ts_compressed_chunk row would leave orphan files on
	 * disk with no record pointing at them.
	 */
	if (!ts_compressed_chunk_ensure_oid())
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("time_series.ts_compressed_chunk catalog table not found"),
				 errhint("The time_series extension appears to be missing or corrupted.")));

	/* PAX file path is deterministic from (db, relid, chunk). */
	snprintf(pax_path, sizeof(pax_path), TS_PAX_RELFILE_FMT,
			 MyDatabaseId, relid, chunk_number);
	snprintf(seg_path, sizeof(seg_path), "%s/%s.seg%d",
			 DataDir, pax_path, GpIdentity.segindex);
	/*
	 * Write to a sibling ".new" path and rename on success.  This avoids
	 * the reader/writer-on-same-path collision during PARTIAL recompress,
	 * and makes the per-chunk file replacement effectively atomic (rename
	 * is atomic on POSIX).
	 *
	 * The toast sidecar (PAX's own external-toast storage, used whenever
	 * a varlena column's value crosses pax_min_size_of_external_toast)
	 * gets the same ".new" treatment, but with ".new" appended AFTER
	 * ".toast" rather than before: this lets its promotion reuse
	 * ts_wal_pax_rename's generic "<name>.new" -> "<name>" logic with
	 * the live toast basename ("chunk_<N>.pax.seg<M>.toast") as-is,
	 * instead of teaching redo a second, different suffix-splicing rule.
	 */
	snprintf(seg_path_new, sizeof(seg_path_new), "%s.new", seg_path);
	snprintf(seg_path_toast_new, sizeof(seg_path_toast_new), "%s.toast.new", seg_path);

	/* Chunk time range derives from ts_config + chunk number. */
	range_start = (TimestampTz) (ts_config->origin_usec +
		(int64)(chunk_number - TS_FIRST_CHUNKNUM) * ts_config->interval_usec);
	range_end = (TimestampTz) (range_start + ts_config->interval_usec);

	compress_ensure_pax_directories(relid);

	rel = table_open(relid, AccessShareLock);
	RelationOpenSmgr(rel);
	reln = rel->rd_smgr;
	if (!smgrexists(reln, forknum))
	{
		elog(LOG, "compress chunk: skip (no heap fork) seg=%d rel=%u chunk=%d",
			 GpIdentity.segindex, relid, chunk_number);
		table_close(rel, AccessShareLock);
		return;
	}

	/*
	 * If a PAX file already exists for this (segment, chunk) AND the
	 * catalog row for it exists, open a reader so we can carry its rows
	 * forward into the new file.  Needed for correctness of PARTIAL
	 * recompress: heap is truncated post-commit, so on next compress the
	 * old PAX is the only source of previously-compressed rows.
	 *
	 * The catalog-row check guards against orphan files from a prior
	 * failed compress (rename succeeded, catalog INSERT failed or
	 * cluster crashed before commit).  Without it, this run would merge
	 * orphan-PAX rows with heap rows and duplicate every row in the
	 * chunk.  See compressed_chunk_row_exists() above.
	 */
	if (stat(seg_path, &stbuf) == 0 && stbuf.st_size > 0 &&
		compressed_chunk_row_exists(relid, chunk_number))
		prior_reader = ts_pax_reader_open(seg_path, RelationGetDescr(rel));

	elog(LOG, "compress chunk: scan seg=%d rel=%u chunk=%d nblocks=%u prior_pax=%s",
		 GpIdentity.segindex, relid, chunk_number,
		 smgrnblocks(reln, forknum),
		 prior_reader ? "yes" : "no");

	{
		int minmax_idxs[2 * TS_MAX_COMPRESS_COLS];
		int n_minmax = have_sort_config ? build_minmax_col_idxs(comp_config, minmax_idxs) : 0;

		writer = ts_pax_writer_open(seg_path_new, seg_path_toast_new,
									RelationGetDescr(rel),
									MyDatabaseId, relid,
									minmax_idxs, n_minmax);
	}
	if (writer == NULL)
	{
		if (prior_reader)
			ts_pax_reader_close(prior_reader);
		table_close(rel, AccessShareLock);
		elog(WARNING, "could not open PAX writer for \"%s\"", seg_path_new);
		return;
	}

	/*
	 * Scan heap and write to PAX (merging prior rows if present).
	 *
	 * ts_pax_writer_write_tuple / ts_pax_writer_flush (called inside
	 * scan_heap_to_pax_{sorted,unsorted}, once per row/group) are not
	 * individually PG_TRY-protected -- too costly per row.  If PAX
	 * raises mid-scan (corrupt input row, OOM, disk full on flush),
	 * the writer/reader handles must be released here rather than
	 * leaking until process exit: ts_pax_writer_open's own PG_CATCH
	 * only guards its own construction window, not this one.
	 */
	PG_TRY();
	{
		BlockNumber nblocks = smgrnblocks(reln, forknum);

		if (have_sort_config)
			scan_heap_to_pax_sorted(rel, forknum, nblocks, prior_reader,
									writer, comp_config, &numrows);
		else
			scan_heap_to_pax_unsorted(rel, forknum, nblocks, prior_reader,
									  writer, &numrows);

		file_size = ts_pax_writer_close(writer, &numrows, &num_groups);
		writer = NULL;		/* closed; PG_CATCH must not touch it again */
		if (prior_reader)
		{
			ts_pax_reader_close(prior_reader);
			prior_reader = NULL;	/* closed; ditto */
		}
	}
	PG_CATCH();
	{
		/*
		 * writer / prior_reader are set to NULL immediately after each
		 * successful close above, so if a LATER step in this same
		 * PG_TRY block raises (e.g. ts_pax_reader_close after a
		 * successful ts_pax_writer_close), the already-closed handle
		 * is not touched again here -- both *_abort functions no-op
		 * on NULL, but a stale (already-freed) non-NULL handle would
		 * be a double-free without this reset.
		 */
		ts_pax_writer_abort(writer);
		ts_pax_reader_abort(prior_reader);
		PG_RE_THROW();
	}
	PG_END_TRY();

	elog(LOG, "compress chunk: pax written seg=%d rel=%u chunk=%d "
		 "numrows=" INT64_FORMAT " num_groups=%d file_size=" INT64_FORMAT,
		 GpIdentity.segindex, relid, chunk_number,
		 numrows, num_groups, file_size);

	/*
	 * Fault points for the crash-during-recompress regression
	 * (iso2 recompress_crash_destroys_live_file).  Both sit in the
	 * same window -- ".new" fully written and closed, rename(2) not
	 * yet done -- because reproducing the bug needs TWO things to be
	 * true at the moment the process dies:
	 *
	 *   1. the XLOG_TS_PAX_WRITE records for ".new" are already
	 *      FLUSHED to disk (redo only replays flushed WAL), and
	 *   2. rename(2) has NOT run, so the catalog still points at the
	 *      previous, committed PAX file.
	 *
	 * A single fault point cannot express that: 'suspend' lets the
	 * test flush the WAL (pg_switch_wal) but resuming would proceed
	 * to the rename, while 'panic' alone would race the walwriter for
	 * condition 1.  So the test suspends on _after_pax_write, flushes
	 * WAL, arms _before_rename as 'panic', then resets the suspend --
	 * the backend wakes and dies here, faithfully mimicking a
	 * kill/OOM landing mid-recompress.  Same idiom as the kernel's
	 * isolation2 frozen_insert_crash test.
	 */
	SIMPLE_FAULT_INJECTOR("ts_recompress_after_pax_write");
	SIMPLE_FAULT_INJECTOR("ts_recompress_before_rename");

	/*
	 * Atomically replace the old file with the new one.  POSIX rename(2)
	 * is atomic: readers of the old path see either the old complete file
	 * or the new complete file, never a torn state.
	 *
	 * WAL the promotion first.  Every XLOG_TS_PAX_WRITE for this chunk
	 * names the ".new" file, so replay can only ever build the temporary
	 * copy; this record is the one and only thing in the PAX WAL stream
	 * that moves a live filename, and it is emitted here -- after the
	 * writer closed, so the file being promoted is structurally complete.
	 * A recompress killed before this point replays as writes into an
	 * orphan ".new" and leaves the live file, along with the committed
	 * catalog row describing it, exactly as it was.
	 *
	 * has_toast: a non-empty ".toast.new" means some row this run needed
	 * PAX's own external-toast storage (see pax_enable_toast /
	 * pax_min_size_of_external_toast).  The common case is an empty (or
	 * absent) sidecar, in which case there is nothing to promote -- any
	 * live ".toast" left over from a PREVIOUS compress becomes an orphan,
	 * harmlessly, because this run's main file has toastlength()==0 in
	 * every stripe and so never dereferences it.
	 *
	 * Note: the user-relation AccessShareLock is intentionally kept open
	 * across the renames + ts_compressed_chunk write below.  Releasing it
	 * before the catalog INSERT would let a concurrent DROP TABLE run its
	 * ProcessUtility cleanup hook (ts_compressed_chunk_delete) between
	 * our rename and INSERT, leaving a stale row pointing to the dropped
	 * relid.  AccessShareLock conflicts with the AccessExclusiveLock that
	 * DROP requires, so DROP queues behind us until table_close below.
	 */
	has_toast = (stat(seg_path_toast_new, &stbuf) == 0 && stbuf.st_size > 0);

	{
		char	pax_basename[MAXPGPATH];
		char	toast_basename[MAXPGPATH];

		snprintf(pax_basename, sizeof(pax_basename), TS_PAX_SEGFILE_BASE_FMT,
				 chunk_number, GpIdentity.segindex);
		if (has_toast)
			snprintf(toast_basename, sizeof(toast_basename), "%s.toast", pax_basename);

		ts_wal_pax_rename(MyDatabaseId, relid, pax_basename,
						  has_toast ? toast_basename : NULL);
	}

	if (rename(seg_path_new, seg_path) < 0)
	{
		table_close(rel, AccessShareLock);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not rename \"%s\" to \"%s\": %m",
						seg_path_new, seg_path)));
	}

	if (has_toast)
	{
		char	seg_path_toast[MAXPGPATH];

		snprintf(seg_path_toast, sizeof(seg_path_toast), "%s.toast", seg_path);
		if (rename(seg_path_toast_new, seg_path_toast) < 0)
		{
			table_close(rel, AccessShareLock);
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not rename \"%s\" to \"%s\": %m",
							seg_path_toast_new, seg_path_toast)));
		}
	}

	{
		char	pax_file_seg[MAXPGPATH];

		snprintf(pax_file_seg, sizeof(pax_file_seg), "%s.seg%d",
				 pax_path, GpIdentity.segindex);
		compress_write_chunk_metadata_row(relid, chunk_number, range_start, range_end,
										  pax_file_seg, numrows, num_groups, file_size);
	}

	elog(LOG, "compress chunk: done seg=%d rel=%u chunk=%d",
		 GpIdentity.segindex, relid, chunk_number);

	table_close(rel, AccessShareLock);
}

/* ----------------------------------------------------------------
 *		ts_compress_write_chunks — segment-side PAX writer
 *
 *		Runs on each segment (EXECUTE ON ALL SEGMENTS) for parallel arrays
 *		of chunks.  Writes one PAX file + one ts_compressed_chunk row per
 *		(chunk, segment).  Cloudberry requires EXECUTE ON ALL SEGMENTS
 *		functions to be set-returning, so we emit a single seg_id row per
 *		segment — callers discard it.
 * ----------------------------------------------------------------
 */
Datum
ts_compress_write_chunks(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext	oldcontext;
		Oid				relid = PG_GETARG_OID(0);
		ArrayType	   *chunks_arr = PG_GETARG_ARRAYTYPE_P(1);
		Datum		   *chunks;
		bool		   *c_nulls;
		int				nchunks;
		Relation		rel;
		TSConfig		ts_config;
		TSCompressConfig comp_config;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/*
		 * Defence-in-depth: this function is REVOKEd from PUBLIC and only
		 * dispatched by ts_compress_chunks on the coordinator, but the user
		 * context is propagated to segments so we can (and should) still
		 * verify the caller owns the table before touching PAX files.
		 */
		if (!pg_class_ownercheck(relid, GetUserId()))
			aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_TABLE,
						   get_rel_name(relid));

		deconstruct_array(chunks_arr, INT4OID, 4, true, TYPALIGN_INT,
						  &chunks, &c_nulls, &nchunks);

		/*
		 * Load both configs once per dispatch: ts_config for origin/interval
		 * (used to derive chunk time ranges), comp_config for segmentby /
		 * orderby (used to pick sorted vs unsorted write path).
		 */
		rel = table_open(relid, AccessShareLock);
		if (!ts_get_config(rel, &ts_config))
		{
			table_close(rel, AccessShareLock);
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("_ts_compress_write_chunks: relation %u is not a time_series table",
							relid)));
		}
		table_close(rel, AccessShareLock);
		ts_compress_config_load(relid, &comp_config);

		elog(LOG, "compress_write_chunks: seg=%d rel=%u nchunks=%d "
			 "segmentby=%d orderby=%d",
			 GpIdentity.segindex, relid, nchunks,
			 comp_config.n_segmentby, comp_config.n_orderby);

		for (int i = 0; i < nchunks; i++)
			do_compress_one_chunk(relid, DatumGetInt32(chunks[i]),
								  &ts_config, &comp_config);

		/*
		 * Flip status to COMPRESSED for every dispatched chunk here on the
		 * segment, via the direct-catalog helper, rather than a
		 * coordinator-side SQL "UPDATE ts_chunk SET status".
		 *
		 * Why not a SQL UPDATE: with the Global Deadlock Detector OFF (the
		 * cluster default), Greenplum/CBDB escalates UPDATE/DELETE to a
		 * table-level ExclusiveLock.  That conflicts with the
		 * RowExclusiveLock a concurrent INSERT holds on ts_chunk while
		 * creating a chunk, and because the two MPP operations acquire the
		 * lock independently on each segment they can invert order across
		 * segments (compress wins one segment, INSERT wins another) and
		 * deadlock.  The per-segment local deadlock detectors cannot see
		 * the cross-segment cycle, and with GDD off there is no global
		 * detector to break it, so the write path hangs indefinitely.
		 *
		 * ts_chunk_catalog_update_status is a heap CatalogTupleUpdate that
		 * takes RowExclusiveLock — self-compatible with INSERT's
		 * RowExclusiveLock — so the two never conflict and cannot deadlock
		 * regardless of GDD.  Set every dispatched chunk unconditionally,
		 * matching the old coordinator UPDATE's "set all" semantics: a
		 * no-op for chunks whose local rows are already COMPRESSED or
		 * absent on this segment.
		 */
		for (int i = 0; i < nchunks; i++)
			ts_chunk_catalog_update_status(relid, DatumGetInt32(chunks[i]),
										   TS_CHUNK_COMPRESSED);

		elog(LOG, "compress_write_chunks: done seg=%d rel=%u nchunks=%d",
			 GpIdentity.segindex, relid, nchunks);

		funcctx->max_calls = 1;
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();

	if (funcctx->call_cntr < funcctx->max_calls)
		SRF_RETURN_NEXT(funcctx, Int32GetDatum(GpIdentity.segindex));

	SRF_RETURN_DONE(funcctx);
}

/*
 * find_all_active_chunks
 *		Find all chunks needing compression in one SPI query.  Returns
 *		palloc'd array and sets *nchunks.  NULL if none.
 *
 *		Race-condition note: the SPI runs without holding any per-chunk
 *		lock, so the chunk's status can change between this read and
 *		the actual segment-side compression.  All resulting races are
 *		HARMLESS thanks to layered defenses, but worth understanding:
 *
 *		Race A: between SELECT and segment dispatch, another session
 *		        compresses chunk N (which we'd seen as ACTIVE/PARTIAL).
 *		        do_compress_one_chunk waits on its advisory ExclusiveLock,
 *		        then sees status = COMPRESSED via SnapshotSelf and early-
 *		        returns.  The coordinator UPDATE is a no-op for that row.
 *
 *		Race B: between SELECT and segment dispatch, an INSERT flips a
 *		        chunk we excluded (was COMPRESSED) to PARTIAL.  Our list
 *		        doesn't include it; user can run again to pick it up.
 *		        Missed opportunity, not data loss.
 *
 *		Race C: a brand new chunk gets created (INSERT to a fresh time
 *		        range) after our SELECT.  Same as B — picked up next run.
 *
 *		Race D: an INSERT to a chunk we DID pick up arrives during our
 *		        compress.  do_compress_one_chunk's advisory ExclusiveLock
 *		        forces the INSERT xact to commit first, then compress
 *		        scans the heap including the just-inserted row.
 *
 *		Consequence for the return value: nchunks is the number of
 *		chunks DISPATCHED, not necessarily the number actually
 *		(re)compressed in the rare race-A case.  Acceptable trade-off
 *		— a strict count would require a second SPI roundtrip with
 *		FOR UPDATE on ts_chunk, which is more expensive than the
 *		occasional minor over-count.
 */
static ChunkToCompress *
find_all_active_chunks(Oid relid, bool has_older_than,
					   Interval *older_than, int *nchunks)
{
	StringInfoData	cmd;
	int				ret;
	ChunkToCompress *result;

	*nchunks = 0;

	SPI_connect();
	initStringInfo(&cmd);

	/*
	 * Find chunks needing compression: ACTIVE (not yet compressed) or
	 * PARTIAL (compressed but has new heap data needing recompress).
	 * With DISTRIBUTED RANDOMLY, status is consistent across segments,
	 * so a simple filter + DISTINCT suffices.
	 */
	if (!has_older_than)
	{
		appendStringInfo(&cmd,
			"SELECT DISTINCT chunk_number"
			" FROM time_series.ts_chunk WHERE table_oid = %u"
			" AND status != %d"
			" ORDER BY chunk_number", relid, TS_CHUNK_COMPRESSED);
	}
	else
	{
		TimestampTz cutoff = DatumGetTimestampTz(DirectFunctionCall2(timestamptz_mi_interval,
			TimestampTzGetDatum(GetCurrentTimestamp()), IntervalPGetDatum(older_than)));
		Datum d = DirectFunctionCall1(timestamptz_out, TimestampTzGetDatum(cutoff));

		appendStringInfo(&cmd,
			"SELECT DISTINCT chunk_number"
			" FROM time_series.ts_chunk WHERE table_oid = %u"
			" AND range_end <= '%s'::timestamptz"
			" AND status != %d"
			" ORDER BY chunk_number", relid, DatumGetCString(d), TS_CHUNK_COMPRESSED);
	}

	ret = SPI_execute(cmd.data, true, 0);
	pfree(cmd.data);

	if (ret != SPI_OK_SELECT || SPI_processed == 0)
	{
		SPI_finish();
		return NULL;
	}

	{
		MemoryContext	oldctx = MemoryContextSwitchTo(TopTransactionContext);
		TupleDesc		td = SPI_tuptable->tupdesc;
		int				n = (int) SPI_processed;

		result = (ChunkToCompress *) palloc(sizeof(ChunkToCompress) * n);
		for (int i = 0; i < n; i++)
		{
			char   *val = SPI_getvalue(SPI_tuptable->vals[i], td, 1);

			result[i].chunk_num = val ? atoi(val) : 0;
		}
		*nchunks = n;
		MemoryContextSwitchTo(oldctx);
	}

	SPI_finish();
	return result;
}

/* ----------------------------------------------------------------
 *		ts_compress_chunks — SQL function (runs on coordinator)
 * ----------------------------------------------------------------
 */
Datum
ts_compress_chunks(PG_FUNCTION_ARGS)
{
	Oid				relid = PG_GETARG_OID(0);
	Relation		userrel;
	int				compressed_count = 0;
	bool			has_older_than = !PG_ARGISNULL(1);
	Interval	   *older_than = has_older_than ? PG_GETARG_INTERVAL_P(1) : NULL;

	/* Only the table owner may trigger compression. */
	if (!pg_class_ownercheck(relid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_TABLE,
					   get_rel_name(relid));

	/*
	 * Compress writes PAX via .new + rename(2), which is non-transactional.
	 * Wrapping the call in BEGIN/COMMIT and rolling back leaves the new
	 * PAX file in place while the catalog reverts to PARTIAL — readers
	 * would then merge the new (already-merged) PAX with heap and
	 * duplicate rows.  Same gating VACUUM / CREATE INDEX CONCURRENTLY
	 * use; allowed inside PROCEDURE bodies that perform internal COMMITs.
	 */
	PreventInTransactionBlock(true, "compress_chunks");

	/*
	 * AccessShareLock on the user relation, held for the ENTIRE function
	 * duration.  Self-compatible, so two compress_chunks on the same
	 * table targeting different chunks run in parallel — actual
	 * per-chunk mutual exclusion is delivered by the advisory
	 * ExclusiveLock in do_compress_one_chunk.  AccessShare conflicts
	 * with AccessExclusive, so DDL (ALTER / DROP) is blocked at the
	 * master for the whole compress run.  Without this, a concurrent
	 * DROP could slip in between the master's SPI dispatches and leave
	 * stale ts_compressed_chunk rows pointing to a dropped relid.
	 */
	userrel = table_open(relid, AccessShareLock);
	if (!RelationIsTimeSeries(userrel))
	{
		char *relname = pstrdup(RelationGetRelationName(userrel));
		table_close(userrel, AccessShareLock);
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("relation \"%s\" is not a time_series table", relname)));
	}

	elog(LOG, "compress_chunks: start rel=%u (\"%s\") older_than=%s",
		 relid, RelationGetRelationName(userrel),
		 has_older_than ? "yes" : "no");

	/*
	 * Find all eligible chunks and process them in a single batch call
	 * to the segments.  This amortises the per-chunk dispatch overhead.
	 */
	{
		ChunkToCompress *chunks;
		int				nchunks;

		chunks = find_all_active_chunks(relid, has_older_than, older_than, &nchunks);

		elog(LOG, "compress_chunks: rel=%u found %d chunk(s) needing compression",
			 relid, nchunks);

		if (nchunks > 0)
		{
			StringInfoData	cmd;
			StringInfoData	chunks_arr;
			int				rc;

			/* Build the chunk-number array literal for the batch call. */
			initStringInfo(&chunks_arr);
			appendStringInfoString(&chunks_arr, "ARRAY[");
			for (int i = 0; i < nchunks; i++)
			{
				if (i > 0)
					appendStringInfoChar(&chunks_arr, ',');
				appendStringInfo(&chunks_arr, "%d", chunks[i].chunk_num);
			}
			appendStringInfoString(&chunks_arr, "]::integer[]");

			initStringInfo(&cmd);
			appendStringInfo(&cmd,
				"SELECT time_series._ts_compress_write_chunks(%u, %s)",
				relid, chunks_arr.data);
			pfree(chunks_arr.data);

			SPI_connect();
			rc = SPI_execute(cmd.data, false, 0);
			pfree(cmd.data);
			if (rc < 0)
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("_ts_compress_write_chunks dispatch failed: %s",
								SPI_result_code_string(rc))));

			/*
			 * Status flip to COMPRESSED is done inside
			 * _ts_compress_write_chunks on each segment (direct catalog
			 * RowExclusiveLock), NOT here via a SQL UPDATE — a planned
			 * UPDATE escalates to a table-level ExclusiveLock under
			 * GDD-off and deadlocks cross-segment with concurrent INSERT.
			 * See the comment in ts_compress_write_chunks.
			 */
			SPI_finish();
			compressed_count = nchunks;

			/*
			 * Refresh the QD-readable chunk-state aggregate in
			 * pg_class.reloptions so the planner's parameterized
			 * ChunkScan cost discount picks up the new compressed
			 * ratio.  Internally dispatches to segments, computes the
			 * aggregate, and heap_inplace_updates the reloption +
			 * broadcasts relcache invalidation.
			 */
			ts_refresh_chunk_stats(relid);
		}

		if (chunks)
			pfree(chunks);
	}

	elog(LOG, "compress_chunks: done rel=%u compressed=%d", relid, compressed_count);

	table_close(userrel, AccessShareLock);

	PG_RETURN_INT32(compressed_count);
}

/* ----------------------------------------------------------------
 *		ts_compress_chunk — single-chunk compress entry
 *
 *		Chunks are forks identified by (table_oid, chunk_number); the
 *		signature takes the pair.  Equivalent to compress_chunks()
 *		restricted to one chunk.  Lets users / future BGW policies
 *		parallelise compression across different chunks: two sessions
 *		calling compress_chunk(t, 4) and compress_chunk(t, 5) hold
 *		disjoint advisory ExclusiveLocks and run truly in parallel
 *		(the SUEL → AccessShare downgrade in the coordinator entry
 *		already permits this).
 *
 *		Args:
 *		  rel               regclass — the time_series relation
 *		  chunk_number      integer  — the chunk to target (>= TS_FIRST_CHUNKNUM)
 *		  if_not_compressed boolean  — if true (default), silently skip
 *		                                when the chunk is already
 *		                                COMPRESSED.  If false, raise an
 *		                                error in that case.
 *
 *		Returns: 1 if the chunk was (re)compressed, 0 if skipped.
 * ----------------------------------------------------------------
 */
Datum
ts_compress_chunk(PG_FUNCTION_ARGS)
{
	Oid				relid = PG_GETARG_OID(0);
	int32			chunk_number = PG_GETARG_INT32(1);
	bool			if_not_compressed = PG_ARGISNULL(2) ? true : PG_GETARG_BOOL(2);
	Relation		userrel;
	bool			already_compressed = false;
	bool			row_exists = false;

	/* Only the table owner may trigger compression. */
	if (!pg_class_ownercheck(relid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_TABLE,
					   get_rel_name(relid));

	PreventInTransactionBlock(true, "compress_chunk");

	/*
	 * AccessShareLock on user rel held for the entire call — see
	 * matching comment in ts_compress_chunks above.
	 */
	userrel = table_open(relid, AccessShareLock);
	if (!RelationIsTimeSeries(userrel))
	{
		char *relname = pstrdup(RelationGetRelationName(userrel));
		table_close(userrel, AccessShareLock);
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("relation \"%s\" is not a time_series table", relname)));
	}

	elog(LOG, "compress_chunk: start rel=%u (\"%s\") chunk=%d if_not_compressed=%s",
		 relid, RelationGetRelationName(userrel), chunk_number,
		 if_not_compressed ? "true" : "false");

	if (chunk_number < TS_FIRST_CHUNKNUM)
	{
		table_close(userrel, AccessShareLock);
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("chunk_number %d is below the minimum of %d",
						chunk_number, TS_FIRST_CHUNKNUM)));
	}

	/*
	 * Look up the chunk in ts_chunk via SPI.  Direct heap access via
	 * ts_chunk_catalog_get_chunks_with_status would query the local
	 * (master) catalog, which is empty under DISTRIBUTED RANDOMLY —
	 * the rows live on segments.  SPI dispatches and aggregates.
	 *
	 * MIN(status) over all segments tells us whether any segment still
	 * has the chunk in pre-COMPRESSED state.  If MIN != COMPRESSED some
	 * segment has work to do.  If MIN == COMPRESSED every segment is
	 * already compressed → noop.
	 */
	{
		StringInfoData	cmd;
		int				rc;

		SPI_connect();

		initStringInfo(&cmd);
		appendStringInfo(&cmd,
			"SELECT count(*), MIN(status) FROM time_series.ts_chunk"
			" WHERE table_oid = %u AND chunk_number = %d",
			relid, chunk_number);
		rc = SPI_execute(cmd.data, true, 0);
		pfree(cmd.data);

		if (rc != SPI_OK_SELECT || SPI_processed != 1)
		{
			SPI_finish();
			table_close(userrel, AccessShareLock);
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("compress_chunk: chunk lookup failed")));
		}

		{
			TupleDesc	td = SPI_tuptable->tupdesc;
			HeapTuple	tup = SPI_tuptable->vals[0];
			bool		isnull;
			Datum		dcount = SPI_getbinval(tup, td, 1, &isnull);
			int64		nrows = isnull ? 0 : DatumGetInt64(dcount);

			if (nrows > 0)
			{
				Datum	dmin = SPI_getbinval(tup, td, 2, &isnull);
				int16	min_status = isnull ? -1 : DatumGetInt16(dmin);

				row_exists = true;
				already_compressed = (min_status == TS_CHUNK_COMPRESSED);
			}
		}

		SPI_finish();
	}

	if (!row_exists)
	{
		table_close(userrel, AccessShareLock);
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("chunk %d does not exist for relation \"%s\"",
						chunk_number, get_rel_name(relid))));
	}

	if (already_compressed)
	{
		if (if_not_compressed)
		{
			elog(LOG, "compress_chunk: rel=%u chunk=%d already compressed, skipping",
				 relid, chunk_number);
			table_close(userrel, AccessShareLock);
			PG_RETURN_INT32(0);
		}
		table_close(userrel, AccessShareLock);
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("chunk %d of \"%s\" is already compressed",
						chunk_number, get_rel_name(relid)),
				 errhint("Pass if_not_compressed => true (the default) to skip"
						 " silently, or wait for a PARTIAL state and call again"
						 " to re-compress.")));
	}

	/*
	 * Dispatch to segments for the actual PAX write.  The status flip to
	 * COMPRESSED happens inside _ts_compress_write_chunks on each segment
	 * (direct catalog RowExclusiveLock), NOT via a SQL UPDATE here — a
	 * planned UPDATE escalates to a table-level ExclusiveLock under
	 * GDD-off and deadlocks cross-segment with concurrent INSERT.  See
	 * the comment in ts_compress_write_chunks.  Same SPI pattern as
	 * compress_chunks() but with a single-element array.
	 */
	{
		StringInfoData	cmd;
		int				rc;

		initStringInfo(&cmd);
		appendStringInfo(&cmd,
			"SELECT time_series._ts_compress_write_chunks(%u, ARRAY[%d]::integer[])",
			relid, chunk_number);

		SPI_connect();
		rc = SPI_execute(cmd.data, false, 0);
		pfree(cmd.data);
		if (rc < 0)
		{
			SPI_finish();
			table_close(userrel, AccessShareLock);
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("_ts_compress_write_chunks dispatch failed: %s",
							SPI_result_code_string(rc))));
		}

		SPI_finish();

		/* See compress_chunks above for the rationale. */
		ts_refresh_chunk_stats(relid);
	}

	elog(LOG, "compress_chunk: done rel=%u chunk=%d", relid, chunk_number);

	table_close(userrel, AccessShareLock);

	PG_RETURN_INT32(1);
}

/* ----------------------------------------------------------------
 *		ts_reclaim_chunk_heaps — coordinator entry for heap-fork reclaim
 *
 *		Runs in its own transaction (the caller's, separate from any
 *		preceding compress_chunks call).  Queries ts_chunk for COMPRESSED
 *		chunks and dispatches to segments to truncate the per-segment heap
 *		forks.  Returns the number of chunks for which reclaim was
 *		attempted (segments may skip individual chunks if they've been
 *		transitioned to PARTIAL concurrently).
 * ----------------------------------------------------------------
 */
Datum
ts_reclaim_chunk_heaps(PG_FUNCTION_ARGS)
{
	Oid				relid = PG_GETARG_OID(0);
	Relation		userrel;
	StringInfoData	cmd;
	StringInfoData	chunks_arr;
	int				nchunks = 0;
	int32		   *chunk_nums = NULL;
	int				rc;

	/* Only the table owner may reclaim heap forks. */
	if (!pg_class_ownercheck(relid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_TABLE,
					   get_rel_name(relid));

	/*
	 * smgrtruncate is non-transactional; rolling back wouldn't restore
	 * the heap fork.  Block calls inside an explicit transaction block.
	 */
	PreventInTransactionBlock(true, "reclaim_chunk_heaps");

	/*
	 * AccessShareLock on user rel, held for the entire function — see
	 * comment in ts_compress_chunks above.
	 */
	userrel = table_open(relid, AccessShareLock);
	if (!RelationIsTimeSeries(userrel))
	{
		char *relname = pstrdup(RelationGetRelationName(userrel));
		table_close(userrel, AccessShareLock);
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("relation \"%s\" is not a time_series table", relname)));
	}

	elog(LOG, "reclaim_chunk_heaps: start rel=%u (\"%s\")",
		 relid, RelationGetRelationName(userrel));

	/* Look up COMPRESSED chunks via SPI. */
	SPI_connect();
	initStringInfo(&cmd);
	appendStringInfo(&cmd,
		"SELECT DISTINCT chunk_number FROM time_series.ts_chunk"
		" WHERE table_oid = %u AND status = %d ORDER BY chunk_number",
		relid, TS_CHUNK_COMPRESSED);
	rc = SPI_execute(cmd.data, true, 0);
	pfree(cmd.data);

	if (rc != SPI_OK_SELECT)
	{
		SPI_finish();
		table_close(userrel, AccessShareLock);
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("ts_reclaim_chunk_heaps: query failed: %s",
						SPI_result_code_string(rc))));
	}

	nchunks = (int) SPI_processed;
	if (nchunks == 0)
	{
		elog(LOG, "reclaim_chunk_heaps: rel=%u no COMPRESSED chunks to reclaim",
			 relid);
		SPI_finish();
		table_close(userrel, AccessShareLock);
		PG_RETURN_INT64(0);
	}

	elog(LOG, "reclaim_chunk_heaps: rel=%u found %d COMPRESSED chunk(s)",
		 relid, nchunks);

	{
		MemoryContext oldctx = MemoryContextSwitchTo(TopTransactionContext);
		TupleDesc	td = SPI_tuptable->tupdesc;

		chunk_nums = (int32 *) palloc(sizeof(int32) * nchunks);
		for (int i = 0; i < nchunks; i++)
		{
			char *val = SPI_getvalue(SPI_tuptable->vals[i], td, 1);
			chunk_nums[i] = val ? atoi(val) : 0;
		}
		MemoryContextSwitchTo(oldctx);
	}

	/* Build ARRAY[...] literal and dispatch. */
	initStringInfo(&chunks_arr);
	appendStringInfoString(&chunks_arr, "ARRAY[");
	for (int i = 0; i < nchunks; i++)
	{
		if (i > 0)
			appendStringInfoChar(&chunks_arr, ',');
		appendStringInfo(&chunks_arr, "%d", chunk_nums[i]);
	}
	appendStringInfoString(&chunks_arr, "]::integer[]");

	initStringInfo(&cmd);
	appendStringInfo(&cmd,
		"SELECT time_series._ts_reclaim_chunk_heaps_segment(%u, %s)",
		relid, chunks_arr.data);
	pfree(chunks_arr.data);

	rc = SPI_execute(cmd.data, false, 0);
	pfree(cmd.data);
	if (rc < 0)
	{
		SPI_finish();
		table_close(userrel, AccessShareLock);
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("_ts_reclaim_chunk_heaps_segment dispatch failed: %s",
						SPI_result_code_string(rc))));
	}

	SPI_finish();
	pfree(chunk_nums);

	elog(LOG, "reclaim_chunk_heaps: done rel=%u nchunks=%d", relid, nchunks);

	table_close(userrel, AccessShareLock);

	PG_RETURN_INT64((int64) nchunks);
}

/* ----------------------------------------------------------------
 *		ts_reclaim_chunk_heaps_segment — per-segment worker (SRF)
 *
 *		For each candidate chunk number: take a row-level exclusive lock
 *		on this segment's ts_chunk row to serialise with any concurrent
 *		INSERT-into-COMPRESSED-chunk (which flips status to PARTIAL).
 *		If the status is still COMPRESSED, truncate the chunk's heap fork
 *		to 0 blocks.  Otherwise skip — an INSERT added new data between
 *		compress and reclaim, so the heap rows are authoritative.
 * ----------------------------------------------------------------
 */
Datum
ts_reclaim_chunk_heaps_segment(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext	oldcontext;
		Oid				relid = PG_GETARG_OID(0);
		ArrayType	   *chunks_arr = PG_GETARG_ARRAYTYPE_P(1);
		Datum		   *chunks;
		bool		   *c_nulls;
		int				nchunks;
		Relation		rel;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/*
		 * Defence-in-depth: this function is REVOKEd from PUBLIC and only
		 * dispatched by ts_reclaim_chunk_heaps; still verify ownership so
		 * a compromised segment-side session can't truncate arbitrary heap
		 * forks.
		 */
		if (!pg_class_ownercheck(relid, GetUserId()))
			aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_TABLE,
						   get_rel_name(relid));

		deconstruct_array(chunks_arr, INT4OID, 4, true, TYPALIGN_INT,
						  &chunks, &c_nulls, &nchunks);

		/*
		 * Acquire per-chunk advisory ExclusiveLocks in strict ascending
		 * chunk_number order.  Concurrent multi-insert also groups its
		 * per-forknum ShareLock acquisitions in the tuple stream's
		 * order, which for typical append-mostly workloads is already
		 * ascending; sorting here guarantees the two sides never
		 * disagree on the lock-acquisition sequence, which would give
		 * a cross-segment ABBA deadlock invisible to per-segment local
		 * deadlock detection under GDD=off (the CBDB default).  Zero
		 * runtime cost when input already sorted.
		 */
		if (nchunks > 1)
			qsort(chunks, nchunks, sizeof(Datum), int32_datum_cmp);

		rel = table_open(relid, AccessShareLock);

		elog(LOG, "reclaim_chunk_heaps: seg=%d rel=%u nchunks=%d start",
			 GpIdentity.segindex, relid, nchunks);

		for (int i = 0; i < nchunks; i++)
		{
			int32			chunk_num = DatumGetInt32(chunks[i]);
			ForkNumber		forknum = (ForkNumber) chunk_num;

			/*
			 * Per-chunk advisory ExclusiveLock — conflicts with the
			 * Share that INSERT and ChunkScan take, so any in-flight
			 * INSERT to this chunk drains and any reader still
			 * referencing the heap fork must commit before we
			 * smgrtruncate.  Without the reader half of this barrier,
			 * a SELECT whose MVCC snapshot predates compress would
			 * observe the heap fork vanish mid-scan (mdread past EOF
			 * or silent 0 rows) — see compress_reclaim_reader_race
			 * isolation2 spec.
			 */
			ts_chunk_lock(relid, chunk_num, ExclusiveLock);

			/*
			 * Atomic lock + status check.  Blocks a concurrent INSERT's
			 * COMPRESSED→PARTIAL flip on the same ts_chunk row until we
			 * commit; if INSERT got the lock first, the row is now
			 * PARTIAL and this returns false (skip truncate).
			 */
			if (!ts_chunk_catalog_lock_if_compressed(relid, chunk_num))
			{
				elog(LOG, "reclaim_chunk_heaps: seg=%d rel=%u chunk=%d "
					 "skip (status no longer COMPRESSED)",
					 GpIdentity.segindex, relid, chunk_num);
				continue;
			}

			{
				SMgrRelation	smgr;
				ForkNumber		forks[1] = {forknum};
				BlockNumber		blocks[1] = {0};

				RelationOpenSmgr(rel);
				smgr = rel->rd_smgr;
				if (smgrexists(smgr, forknum))
				{
					BlockNumber nblocks = smgrnblocks(smgr, forknum);

					/*
					 * Cascade-delete TOAST rows referenced by the
					 * tuples we're about to discard.  Without this,
					 * pg_toast rows orphan after the heap fork is
					 * truncated — VACUUM can't find them because the
					 * user table's MAIN_FORKNUM is empty.
					 */
					ts_heap_fork_cleanup_toast(rel, forknum);

					Assert(!MyProc->delayChkptEnd);
					MyProc->delayChkptEnd = true;

					if (RelationNeedsWAL(rel))
						ts_wal_fork_truncate(rel, forknum, 0);

					smgrtruncate(smgr, forks, 1, blocks);

					MyProc->delayChkptEnd = false;

					elog(LOG, "reclaim_chunk_heaps: seg=%d rel=%u chunk=%d "
						 "truncated heap fork (was %u blocks)",
						 GpIdentity.segindex, relid, chunk_num, nblocks);
				}
				else
				{
					elog(LOG, "reclaim_chunk_heaps: seg=%d rel=%u chunk=%d "
						 "skip (no heap fork)",
						 GpIdentity.segindex, relid, chunk_num);
				}
			}
		}

		elog(LOG, "reclaim_chunk_heaps: seg=%d rel=%u done", GpIdentity.segindex, relid);

		table_close(rel, AccessShareLock);

		funcctx->max_calls = 1;
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	if (funcctx->call_cntr < funcctx->max_calls)
		SRF_RETURN_NEXT(funcctx, Int32GetDatum(GpIdentity.segindex));

	SRF_RETURN_DONE(funcctx);
}

/* ----------------------------------------------------------------
 *		_ts_truncate_chunk_fork — truncate a chunk's heap fork to 0 blocks
 *
 *		Runs on each segment (EXECUTE ON ALL SEGMENTS).
 *		Called after compression to clear heap data so only PAX holds it.
 * ----------------------------------------------------------------
 */
Datum
ts_truncate_chunk_fork(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext	oldcontext;
		Oid				relid = PG_GETARG_OID(0);
		int32			chunk_num = PG_GETARG_INT32(1);
		ForkNumber		forknum = (ForkNumber) chunk_num;
		Relation		rel;
		SMgrRelation	smgr;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		funcctx->max_calls = 1;

		/*
		 * Truncating a chunk fork is destructive: any rows still in the
		 * heap fork (i.e. not yet copied into PAX) would be lost.  Restrict
		 * to the table owner; the SQL wrapper is also REVOKEd from PUBLIC
		 * because callers must know the (rel, chunk_num) pair.
		 */
		if (!pg_class_ownercheck(relid, GetUserId()))
			aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_TABLE,
						   get_rel_name(relid));

		/* Truncate the chunk fork */
		rel = table_open(relid, AccessExclusiveLock);
		RelationOpenSmgr(rel);
		smgr = rel->rd_smgr;
		if (smgrexists(smgr, forknum))
		{
			ForkNumber	forks[1] = { forknum };
			BlockNumber	blocks[1] = { 0 };

			Assert(!MyProc->delayChkptEnd);
			MyProc->delayChkptEnd = true;

			if (RelationNeedsWAL(rel))
				ts_wal_fork_truncate(rel, forknum, 0);

			smgrtruncate(smgr, forks, 1, blocks);

			MyProc->delayChkptEnd = false;
		}

		table_close(rel, AccessExclusiveLock);
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();

	if (funcctx->call_cntr < funcctx->max_calls)
		SRF_RETURN_NEXT(funcctx, Int32GetDatum(GpIdentity.segindex));

	SRF_RETURN_DONE(funcctx);
}

/* ----------------------------------------------------------------
 *		ts_compressed_chunk_info — SQL SRF function
 * ----------------------------------------------------------------
 */
Datum
ts_compressed_chunk_info(PG_FUNCTION_ARGS)
{ FuncCallContext	   *funcctx;
	Oid					relid;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext	oldcontext;
		TupleDesc		tupdesc;
		Oid				ns_oid;
		Oid				ts_chunk_oid;
		Relation		chunk_rel;
		TableScanDesc	hscan;
		ScanKeyData		skey[1];
		HeapTuple		tup;
		TupleDesc		chunk_tupdesc;
		/* Temporary storage for collecting rows */

		ChunkInfoEntry *entries;
		int				capacity;
		int				count;
		int				i;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		relid = PG_GETARG_OID(0);

		/*
		 * Chunk metadata (file paths, sizes, row counts) is not sensitive
		 * data but it does reveal a table's storage layout.  Restrict to
		 * table owner to match ts_chunk_info's behaviour.
		 */
		if (!pg_class_ownercheck(relid, GetUserId()))
			aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_TABLE,
						   get_rel_name(relid));

		/* Build output tuple descriptor */
		tupdesc = CreateTemplateTupleDesc(7);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "chunk_number", INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "range_start", TIMESTAMPTZOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "range_end", TIMESTAMPTZOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "status", INT2OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "pax_file", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 6, "numrows", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 7, "compressed_at", TIMESTAMPTZOID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		/* Scan ts_chunk to get all chunks for this table */
		ns_oid = ht_get_namespace_oid_cached();
		if (!OidIsValid(ns_oid)) ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_SCHEMA),
					 errmsg("time_series schema not found")));

		ts_chunk_oid = get_relname_relid("ts_chunk", ns_oid);
		if (!OidIsValid(ts_chunk_oid)) ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_TABLE),
					 errmsg("time_series.ts_chunk table not found")));

		capacity = 64;
		entries = (ChunkInfoEntry *) palloc(sizeof(ChunkInfoEntry) * capacity);
		count = 0;

		chunk_rel = table_open(ts_chunk_oid, AccessShareLock);
		chunk_tupdesc = RelationGetDescr(chunk_rel);

		ScanKeyInit(&skey[0], Anum_ts_chunk_table_oid,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(relid));

		/* SnapshotSelf — see scan_heap_to_pax_sorted rationale. */
		hscan = table_beginscan(chunk_rel, SnapshotSelf, 1, skey);

		while ((tup = heap_getnext(hscan, ForwardScanDirection)) != NULL)
		{
			Datum	d_chunk, d_start, d_end, d_status;
			bool	isnull;

			d_chunk = heap_getattr(tup, Anum_ts_chunk_chunk_number, chunk_tupdesc, &isnull);
			if (isnull)
				continue;

			d_start = heap_getattr(tup, Anum_ts_chunk_range_start, chunk_tupdesc, &isnull);
			if (isnull)
				continue;

			d_end = heap_getattr(tup, Anum_ts_chunk_range_end,  chunk_tupdesc, &isnull);
			if (isnull)
				continue;

			d_status = heap_getattr(tup, Anum_ts_chunk_status, chunk_tupdesc, &isnull);
			if (isnull)
				continue;

			if (count >= capacity)
			{
				capacity *= 2;
				entries = (ChunkInfoEntry *) repalloc(entries,   sizeof(ChunkInfoEntry) * capacity);
			}

			entries[count].chunk_number = DatumGetInt32(d_chunk);
			entries[count].range_start = DatumGetTimestampTz(d_start);
			entries[count].range_end = DatumGetTimestampTz(d_end);
			entries[count].status = DatumGetInt16(d_status);
			entries[count].pax_file = NULL;
			entries[count].numrows = 0;
			entries[count].numrows_null = true;
			entries[count].compressed_at = 0;
			entries[count].compressed_at_null = true;
			count++;
		}

		table_endscan(hscan);
		table_close(chunk_rel, AccessShareLock);

		/* For COMPRESSED chunks, join with ts_compressed_chunk */
		if (ts_compressed_chunk_ensure_oid())
		{
			Relation		cc_rel;
			TupleDesc		cc_tupdesc;

			cc_rel = table_open(ts_compressed_chunk_relid, AccessShareLock);
			cc_tupdesc = RelationGetDescr(cc_rel);

			for (i = 0; i < count; i++)
			{
				TableScanDesc	cc_scan;
				ScanKeyData		cc_skey[2];
				HeapTuple		cc_tup;

				if (entries[i].status != TS_CHUNK_COMPRESSED)
					continue;

				ScanKeyInit(&cc_skey[0], Anum_cchunk_table_oid,
							BTEqualStrategyNumber, F_OIDEQ,
							ObjectIdGetDatum(relid));
				ScanKeyInit(&cc_skey[1], Anum_cchunk_chunk_number,
							BTEqualStrategyNumber, F_INT4EQ,
							Int32GetDatum(entries[i].chunk_number));

				/* SnapshotSelf — see scan_heap_to_pax_sorted rationale. */
				cc_scan = table_beginscan(cc_rel, SnapshotSelf,   2, cc_skey);
				cc_tup = heap_getnext(cc_scan, ForwardScanDirection);

				if (HeapTupleIsValid(cc_tup))
				{
					Datum	d_pax, d_numrows, d_cat;
					bool	pnull;

					d_pax = heap_getattr(cc_tup, Anum_cchunk_pax_file,  cc_tupdesc, &pnull);
					if (!pnull)
						entries[i].pax_file = TextDatumGetCString(d_pax);

					d_numrows = heap_getattr(cc_tup, Anum_cchunk_numrows,  cc_tupdesc, &pnull);
					if (!pnull)
					{
						entries[i].numrows = DatumGetInt64(d_numrows);
						entries[i].numrows_null = false;
					}

					d_cat = heap_getattr(cc_tup, Anum_cchunk_compressed_at,  cc_tupdesc, &pnull);
					if (!pnull)
					{
						entries[i].compressed_at = DatumGetTimestampTz(d_cat);
						entries[i].compressed_at_null = false;
					}
				}

				table_endscan(cc_scan);
			}

			table_close(cc_rel, AccessShareLock);
		}

		funcctx->max_calls = count;
		funcctx->user_fctx = entries;

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();

	if (funcctx->call_cntr < funcctx->max_calls)
	{

		ChunkInfoEntry *entries = (ChunkInfoEntry *) funcctx->user_fctx;
		ChunkInfoEntry *entry = &entries[funcctx->call_cntr];
		Datum			values[7];
		bool			nulls[7];
		HeapTuple		result_tup;

		memset(values, 0, sizeof(values));
		memset(nulls, false, sizeof(nulls));

		values[0] = Int32GetDatum(entry->chunk_number);
		values[1] = TimestampTzGetDatum(entry->range_start);
		values[2] = TimestampTzGetDatum(entry->range_end);
		values[3] = Int16GetDatum(entry->status);

		if (entry->pax_file != NULL)
			values[4] = CStringGetTextDatum(entry->pax_file);
		else
			nulls[4] = true;

		if (!entry->numrows_null)
			values[5] = Int64GetDatum(entry->numrows);
		else
			nulls[5] = true;

		if (!entry->compressed_at_null)
			values[6] = TimestampTzGetDatum(entry->compressed_at);
		else
			nulls[6] = true;

		result_tup = heap_form_tuple(funcctx->tuple_desc, values, nulls);

		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(result_tup));
	}

	SRF_RETURN_DONE(funcctx);
}
