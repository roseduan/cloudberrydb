/*-------------------------------------------------------------------------
 *
 * insert.c
 *    Continuous Aggregate invalidation trigger -- real logic.
 *
 *    BEFORE INSERT trigger on source tables: unconditionally batches
 *    all inserted rows' time range (min/max) per transaction.  The
 *    threshold check and actual L1 (cagg_invalidation_log) write
 *    happen at PRE_COMMIT via cagg_l1_xact_callback -- matching the
 *    TimescaleDB "deferred judgment" design to avoid stale-threshold
 *    races with concurrent REFRESH.
 *
 *    Also provides a STATEMENT-level trigger for TRUNCATE (full-range
 *    invalidation).
 *
 * Copyright (c) 2026 HashData Inc.
 *
 * IDENTIFICATION
 *    contrib/time_series/src/cagg/insert.c
 *
 *-------------------------------------------------------------------------
 */
#include "../include/time_series.h"
#include "../include/cagg/cagg.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "commands/trigger.h"
#include "datatype/timestamp.h"
#include "executor/tuptable.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"

#include "cdb/cdbvars.h"

#ifdef FAULT_INJECTOR
#include "utils/faultinjector.h"
#endif

/*
 * Transaction-level cache
 *
 * These statics are valid only within a single transaction.  We detect
 * the transaction boundary by comparing GetCurrentTransactionId() with
 * the cached XID.  On a new transaction all caches are invalidated.
 */

static TransactionId cached_xid = InvalidTransactionId;
static Oid	cached_source_oid = InvalidOid;
static TimestampTz cached_threshold = DT_NOBEGIN;
static AttrNumber cached_time_attnum = InvalidAttrNumber;

/* L1 relation OID -- cached per transaction, opened/closed per call */
static Oid	cached_l1_oid = InvalidOid;
static TransactionId cached_l1_xid = InvalidTransactionId;

/*
 * Invalidate the per-transaction cache if we're in a new transaction
 */

static void
cagg_invalidate_cache_if_needed(Oid source_oid)
{
	TransactionId cur_xid = GetCurrentTransactionId();

	if (cached_xid != cur_xid || cached_source_oid != source_oid)
	{
		cached_xid = cur_xid;
		cached_source_oid = source_oid;
		cached_threshold = DT_NOBEGIN;
		cached_time_attnum = InvalidAttrNumber;
	}
}

/*
 * Get threshold for this source table from cagg_invalidation_threshold.
 *
 * Pre-computed during REFRESH as MAX(watermark) across all CAGGs.
 * This is a simple single-table heap scan (vs the old two-table scan
 * of continuous_agg + cagg_watermark).
 *
 * Uses direct heap scan instead of SPI because segment QEs cannot
 * execute SPI queries against distributed tables.
 */

static TimestampTz
cagg_get_threshold(Oid source_oid)
{
	Oid			ns_oid;
	Oid			th_oid;
	Relation	th_rel;
	TableScanDesc scan;
	HeapTuple	tup;
	TimestampTz threshold = DT_NOBEGIN;
	bool		found = false;

	if (cached_threshold != DT_NOBEGIN)
		return cached_threshold;

	ns_oid = ht_get_namespace_oid_cached();
	th_oid = get_relname_relid("cagg_invalidation_threshold", ns_oid);

	if (!OidIsValid(th_oid))
	{
		cached_threshold = DT_NOBEGIN;
		return DT_NOBEGIN;
	}

	/* Scan cagg_invalidation_threshold (RANDOMLY -> local row only) */
	th_rel = table_open(th_oid, AccessShareLock);
	scan = heap_beginscan(th_rel, GetActiveSnapshot(), 0, NULL, NULL, 0);
	while ((tup = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		bool		isnull;
		Datum		d_src,
					d_th;

		/* column 1 = source_table_oid */
		d_src = heap_getattr(tup, 1, RelationGetDescr(th_rel), &isnull);
		if (isnull || DatumGetObjectId(d_src) != source_oid)
			continue;

		/* column 2 = threshold */
		d_th = heap_getattr(tup, 2, RelationGetDescr(th_rel), &isnull);
		if (!isnull)
			threshold = DatumGetTimestampTz(d_th);
		found = true;
		break;
	}
	heap_endscan(scan);
	table_close(th_rel, AccessShareLock);

	if (!found)
	{
		/* No threshold row -> no CAGGs on this source -> never write L1 */
		cached_threshold = DT_NOBEGIN;
		return DT_NOBEGIN;
	}

	/*
	 * If threshold is still -infinity (REFRESH hasn't happened yet, or
	 * watermark was manually updated without running REFRESH), fall back to
	 * scanning cagg_watermark directly.  This ensures correctness even when
	 * the threshold table is stale.
	 */
	if (TIMESTAMP_IS_NOBEGIN(threshold))
	{
		Oid			wm_oid;
		Oid			ca_oid;
		Relation	wm_rel,
					ca_rel;

		ca_oid = get_relname_relid("continuous_agg", ns_oid);
		wm_oid = get_relname_relid("cagg_watermark", ns_oid);

		if (OidIsValid(ca_oid) && OidIsValid(wm_oid))
		{
			List	   *cagg_ids = NIL;
			ListCell   *lc2;

			/* Find cagg_ids for this source (REPLICATED -> local scan) */
			ca_rel = table_open(ca_oid, AccessShareLock);
			scan = heap_beginscan(ca_rel, GetActiveSnapshot(), 0, NULL, NULL, 0);
			while ((tup = heap_getnext(scan, ForwardScanDirection)) != NULL)
			{
				bool		isnull2;
				Datum		d_src2 = heap_getattr(tup, 6, RelationGetDescr(ca_rel), &isnull2);

				if (!isnull2 && DatumGetObjectId(d_src2) == source_oid)
				{
					Datum		d_id2 = heap_getattr(tup, 1, RelationGetDescr(ca_rel), &isnull2);

					if (!isnull2)
						cagg_ids = lappend_int(cagg_ids, DatumGetInt32(d_id2));
				}
			}
			heap_endscan(scan);
			table_close(ca_rel, AccessShareLock);

			/* Scan watermark for MAX */
			if (cagg_ids != NIL)
			{
				wm_rel = table_open(wm_oid, AccessShareLock);
				scan = heap_beginscan(wm_rel, GetActiveSnapshot(), 0, NULL, NULL, 0);
				while ((tup = heap_getnext(scan, ForwardScanDirection)) != NULL)
				{
					bool		isnull2;
					Datum		d_id2 = heap_getattr(tup, 1, RelationGetDescr(wm_rel), &isnull2);

					if (isnull2)
						continue;
					{
						bool		id_found = false;

						foreach(lc2, cagg_ids)
						{
							if (lfirst_int(lc2) == DatumGetInt32(d_id2))
							{
								id_found = true;
								break;
							}
						}
						if (!id_found)
							continue;
					}
					{
						Datum		d_wm2 = heap_getattr(tup, 2, RelationGetDescr(wm_rel), &isnull2);

						if (!isnull2)
						{
							TimestampTz wm = DatumGetTimestampTz(d_wm2);

							if (wm > threshold)
								threshold = wm;
						}
					}
				}
				heap_endscan(scan);
				table_close(wm_rel, AccessShareLock);
				list_free(cagg_ids);
			}
		}
	}

	cached_threshold = threshold;
	return threshold;
}

/*
 * Get the AttrNumber of the time (bucket) column in the source table.
 *
 * Scans continuous_agg (REPLICATED) directly via heap scan -- no SPI.
 */

static AttrNumber
cagg_get_time_attnum(Oid source_oid, Relation source_rel)
{
	Oid			ns_oid;
	Oid			ca_oid;
	Relation	ca_rel;
	TableScanDesc scan;
	HeapTuple	tup;
	AttrNumber	attnum = InvalidAttrNumber;

	if (cached_time_attnum != InvalidAttrNumber)
		return cached_time_attnum;

	ns_oid = ht_get_namespace_oid_cached();
	ca_oid = get_relname_relid("continuous_agg", ns_oid);
	if (!OidIsValid(ca_oid))
		elog(ERROR, "cagg_invalidation_trigfn: continuous_agg table not found");

	ca_rel = table_open(ca_oid, AccessShareLock);
	scan = heap_beginscan(ca_rel, GetActiveSnapshot(), 0, NULL, NULL, 0);
	while ((tup = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		bool		isnull;
		Datum		d_src,
					d_col;

		/* column 6 = source_table_oid */
		d_src = heap_getattr(tup, 6, RelationGetDescr(ca_rel), &isnull);
		if (isnull || DatumGetObjectId(d_src) != source_oid)
			continue;

		/* column 14 = bucket_column (name type) */
		d_col = heap_getattr(tup, 14, RelationGetDescr(ca_rel), &isnull);
		if (!isnull)
		{
			char	   *colname = NameStr(*DatumGetName(d_col));
			int			i;

			for (i = 0; i < source_rel->rd_att->natts; i++)
			{
				Form_pg_attribute attr = TupleDescAttr(source_rel->rd_att, i);

				if (!attr->attisdropped &&
					strcmp(NameStr(attr->attname), colname) == 0)
				{
					attnum = attr->attnum;
					break;
				}
			}
		}
		break;					/* only need the first matching CAGG */
	}
	heap_endscan(scan);
	table_close(ca_rel, AccessShareLock);

	if (attnum == InvalidAttrNumber)
		elog(ERROR, "cagg_invalidation_trigfn: could not find time column");

	cached_time_attnum = attnum;
	return attnum;
}

/*
 * Get the OID of the L1 invalidation log table (cached per transaction)
 */

static Oid
cagg_get_l1_oid(void)
{
	TransactionId cur_xid = GetCurrentTransactionId();

	if (OidIsValid(cached_l1_oid) && cached_l1_xid == cur_xid)
		return cached_l1_oid;

	cached_l1_oid = get_relname_relid("cagg_invalidation_log",
									  ht_get_namespace_oid_cached());
	cached_l1_xid = cur_xid;

	if (!OidIsValid(cached_l1_oid))
		elog(ERROR, "cagg_invalidation_trigfn: L1 table not found");

	return cached_l1_oid;
}

/*
 * Write one invalidation entry to L1 via simple_heap_insert.
 *
 * L1 schema: (source_table_oid oid, lowest_modified timestamptz,
 *             greatest_modified timestamptz)
 */

static void
cagg_write_l1(Oid source_oid, TimestampTz lowest, TimestampTz greatest)
{
	Oid			l1_oid = cagg_get_l1_oid();
	Relation	l1_rel;
	Datum		values[3];
	bool		nulls[3] = {false, false, false};
	HeapTuple	tup;

	l1_rel = table_open(l1_oid, RowExclusiveLock);

	values[0] = ObjectIdGetDatum(source_oid);
	values[1] = TimestampTzGetDatum(lowest);
	values[2] = TimestampTzGetDatum(greatest);

	tup = heap_form_tuple(RelationGetDescr(l1_rel), values, nulls);

	SIMPLE_FAULT_INJECTOR("cagg_trigger_before_l1_write");

	simple_heap_insert(l1_rel, tup);
	heap_freetuple(tup);

	table_close(l1_rel, RowExclusiveLock);
}

/*
 * Transaction-level L1 batch accumulator.
 *
 * Instead of writing one L1 entry per row, we accumulate {min, max}
 * in static variables and flush once at transaction commit.  This
 * reduces 100K backfill rows from 100K L1 entries to just 1.
 *
 * Matches upstream XactCallback-based batching strategy.
 */

static struct
{
	TransactionId xid;
	Oid			source_oid;
	TimestampTz min_time;
	TimestampTz max_time;
	bool		has_data;
	bool		callback_registered;
}			l1_batch = {InvalidTransactionId, InvalidOid, 0, 0, false, false};

/*
 * Flush the batched L1 entry at PRE_COMMIT.
 *
 * Threshold check is deferred to this point -- we fresh-read the threshold
 * from cagg_invalidation_threshold NOW (not the stale trigger-time cache)
 * so that concurrent REFRESH threshold advances are visible.  This matches
 * the TimescaleDB design and eliminates the stale-cache race window.
 *
 * For REPEATABLE READ / SERIALIZABLE: write L1 unconditionally (we can't
 * see the latest threshold in those isolation levels anyway).
 */
static void
cagg_l1_batch_flush(void)
{
	TimestampTz threshold;

	if (!l1_batch.has_data)
		return;

	/*
	 * Strong isolation: always write L1 -- the threshold could have advanced
	 * without us seeing it (our snapshot is fixed at TX start). The
	 * materializer handles out-of-range invalidations gracefully.
	 */
	if (IsolationUsesXactSnapshot())
	{
		elog(DEBUG2, "cagg L1 batch flush (strong isolation): "
			 "source_oid=%u range=[%s, %s]",
			 l1_batch.source_oid,
			 DatumGetCString(DirectFunctionCall1(timestamptz_out,
												 TimestampTzGetDatum(l1_batch.min_time))),
			 DatumGetCString(DirectFunctionCall1(timestamptz_out,
												 TimestampTzGetDatum(l1_batch.max_time))));

		cagg_write_l1(l1_batch.source_oid,
					  l1_batch.min_time,
					  l1_batch.max_time);
		l1_batch.has_data = false;
		return;
	}

	/*
	 * READ COMMITTED: fresh-read threshold.  Invalidate the per-transaction
	 * cache so cagg_get_threshold() re-scans the catalog.
	 *
	 * At PRE_COMMIT time ActiveSnapshot has already been popped, so we must
	 * push GetLatestSnapshot() ourselves before scanning the catalog.
	 * GetLatestSnapshot() sees all committed data -- including any concurrent
	 * REFRESH that advanced the threshold during our TX. (TimescaleDB uses
	 * the same pattern in get_lowest_invalidated_time.)
	 */
	cached_threshold = DT_NOBEGIN;
	PushActiveSnapshot(GetLatestSnapshot());
	threshold = cagg_get_threshold(l1_batch.source_oid);
	PopActiveSnapshot();

	if (l1_batch.min_time < threshold)
	{
		elog(DEBUG2, "cagg L1 batch flush: source_oid=%u "
			 "threshold=[%s] range=[%s, %s]",
			 l1_batch.source_oid,
			 DatumGetCString(DirectFunctionCall1(timestamptz_out,
												 TimestampTzGetDatum(threshold))),
			 DatumGetCString(DirectFunctionCall1(timestamptz_out,
												 TimestampTzGetDatum(l1_batch.min_time))),
			 DatumGetCString(DirectFunctionCall1(timestamptz_out,
												 TimestampTzGetDatum(l1_batch.max_time))));

		cagg_write_l1(l1_batch.source_oid,
					  l1_batch.min_time,
					  l1_batch.max_time);
	}

	l1_batch.has_data = false;
}

static void
cagg_l1_xact_callback(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_PRE_COMMIT:
		case XACT_EVENT_PRE_PREPARE:
		case XACT_EVENT_PARALLEL_PRE_COMMIT:

			/*
			 * Flush batched L1 before the transaction commits / prepares.
			 *
			 * PRE_COMMIT: ordinary local commit (the common path).
			 * PRE_PREPARE: 2PC `PREPARE TRANSACTION` (user-level 2PC is
			 * disabled in Apache Cloudberry today, but CBDB's distributed
			 * transaction layer can invoke PrepareTransaction() internally;
			 * handle this defensively so the batch is not orphaned if a
			 * future code path takes the prepare branch).
			 * PARALLEL_PRE_COMMIT: a parallel worker reaches end of its
			 * transaction; rare in CAGG insert paths but harmless to handle.
			 * Mirrors TimescaleDB upstream insert.c.
			 */
			cagg_l1_batch_flush();
			break;

		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:
			/* Discard on abort -- no L1 should be written */
			if (l1_batch.has_data)
				elog(DEBUG2, "cagg L1 batch discarded on abort: "
					 "source_oid=%u", l1_batch.source_oid);
			l1_batch.has_data = false;
			break;

		default:
			break;
	}
}

static void
cagg_l1_batch_add(Oid source_oid, TimestampTz ts)
{
	/*
	 * Use GetTopTransactionId(), NOT GetCurrentTransactionId().
	 *
	 * In a subtransaction (PL/pgSQL EXCEPTION block),
	 * GetCurrentTransactionId() returns the subtransaction's own xid, which
	 * differs from the parent's. This would cause the batch to be
	 * re-initialized on each subtransaction boundary, losing previously
	 * accumulated min/max values.
	 *
	 * GetTopTransactionId() always returns the top-level transaction xid,
	 * ensuring the batch correctly accumulates across subtransactions.
	 *
	 * Side effect: rolled-back subtransaction INSERTs widen the batch range
	 * (their timestamps stay in min/max).  This is safe -- a wider L1 range
	 * just causes REFRESH to re-materialize extra buckets (correct but
	 * slightly wasteful).  upstream has the same behavior.
	 */
	TransactionId cur_xid = GetTopTransactionId();

	/*
	 * If source_oid changed within the same transaction (rare: INSERT into
	 * multiple CAGG source tables in one TX), flush the current batch before
	 * starting a new one.
	 */
	if (l1_batch.has_data && l1_batch.source_oid != source_oid)
		cagg_l1_batch_flush();

	if (!l1_batch.has_data || l1_batch.xid != cur_xid)
	{
		/* New transaction or first entry -> initialize batch */
		l1_batch.xid = cur_xid;
		l1_batch.source_oid = source_oid;
		l1_batch.min_time = ts;
		l1_batch.max_time = ts;
		l1_batch.has_data = true;
	}
	else
	{
		/* Same transaction, same source -> just update min/max */
		if (ts < l1_batch.min_time)
			l1_batch.min_time = ts;
		if (ts > l1_batch.max_time)
			l1_batch.max_time = ts;
	}

	/* Register XactCallback once per backend lifetime */
	if (!l1_batch.callback_registered)
	{
		RegisterXactCallback(cagg_l1_xact_callback, NULL);
		l1_batch.callback_registered = true;
	}
}

/*
 * Extract the time value from a HeapTuple and convert to TimestampTz.
 * Returns true if a valid value was extracted, false if NULL.
 */

static bool
cagg_extract_time(HeapTuple tuple, TupleDesc tupdesc,
				  AttrNumber time_attnum, TimestampTz *result)
{
	Datum		val;
	bool		isnull;
	Oid			typid;

	val = heap_getattr(tuple, time_attnum, tupdesc, &isnull);
	if (isnull)
		return false;

	typid = TupleDescAttr(tupdesc, time_attnum - 1)->atttypid;

	switch (typid)
	{
		case TIMESTAMPTZOID:
			*result = DatumGetTimestampTz(val);
			break;
		case TIMESTAMPOID:
			/* Convert timestamp (without tz) to timestamptz */
			*result = DatumGetTimestampTz(
										  DirectFunctionCall1(timestamp_timestamptz, val));
			break;
		case DATEOID:
			/* Convert date to timestamptz (midnight) */
			*result = DatumGetTimestampTz(
										  DirectFunctionCall1(date_timestamptz, val));
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("unsupported time column type for "
							"continuous aggregate: type OID %u",
							typid),
					 errhint("Supported types: timestamp, timestamptz, date.")));
			return false;
	}

	return true;
}

/*
 * Process one tuple: extract time and add to the per-transaction batch.
 *
 * No threshold comparison here -- all rows are unconditionally batched.
 * The threshold check is deferred to PRE_COMMIT (cagg_l1_batch_flush)
 * where we fresh-read the threshold to avoid stale-cache races with
 * concurrent REFRESH.  This matches the TimescaleDB design.
 */

static void
cagg_process_tuple(TriggerData *trigdata, HeapTuple tuple,
				   Oid source_oid, AttrNumber time_attnum)
{
	TimestampTz ts;

	if (!cagg_extract_time(tuple, trigdata->tg_relation->rd_att,
						   time_attnum, &ts))
		return;					/* NULL time column -- skip */

	cagg_l1_batch_add(source_oid, ts);
}

/*
 * Main BEFORE INSERT trigger function for invalidation.
 *
 * Fires on INSERT only (source tables are append-only; DELETE/UPDATE
 * are rejected by the time_series tableam).  For each row, extracts
 * the time value and unconditionally adds it to a per-transaction
 * batch (cagg_l1_batch_add).  No threshold comparison is done here.
 *
 * The threshold check is deferred to PRE_COMMIT (cagg_l1_batch_flush)
 * where a fresh read of cagg_invalidation_threshold catches any
 * concurrent REFRESH that advanced the threshold during this TX.
 *
 * Must be BEFORE (not AFTER) because the time_series tableam routes
 * rows to chunk forks after the INSERT; an AFTER trigger would need
 * to fetch the tuple via CTID, but the CTID does not encode the fork
 * number -- leading to wrong-chunk reads or "failed to fetch tuple"
 * errors.  BEFORE reads NEW.* from the in-memory TupleTableSlot,
 * bypassing CTID/fork lookup entirely.  See create.c for details.
 *
 * On the coordinator (QD), the trigger returns immediately without
 * writing L1 -- all real work happens on segments.
 */

PG_FUNCTION_INFO_V1(cagg_invalidation_trigfn);

Datum
cagg_invalidation_trigfn(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata;
	Oid			source_oid;
	AttrNumber	time_attnum;

	if (!CALLED_AS_TRIGGER(fcinfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cagg_invalidation_trigfn must be called as a trigger")));

	trigdata = (TriggerData *) fcinfo->context;

	/*
	 * Yield silently when the extension is not installed, is being upgraded,
	 * or while a logical dump is being replayed.  Without this guard
	 * pg_restore would re-invalidate every CAGG range as the dumped data is
	 * replayed (and may even reference a cagg_id that no longer matches the
	 * post-restore catalog).
	 */
	if (!extension_is_loaded_and_not_upgrading())
	{
		HeapTuple	ret_tuple = trigdata->tg_newtuple ?
		trigdata->tg_newtuple : trigdata->tg_trigtuple;

		if (ret_tuple == NULL && trigdata->tg_newslot != NULL &&
			!TupIsNull(trigdata->tg_newslot))
			ret_tuple = ExecFetchSlotHeapTuple(trigdata->tg_newslot,
											   false, NULL);
		if (ret_tuple == NULL && trigdata->tg_trigslot != NULL &&
			!TupIsNull(trigdata->tg_trigslot))
			ret_tuple = ExecFetchSlotHeapTuple(trigdata->tg_trigslot,
											   false, NULL);
		return PointerGetDatum(ret_tuple);
	}

	/*
	 * Only write L1 on segments.  On coordinator (QD) or utility mode, just
	 * return the tuple unchanged.  The source table is DISTRIBUTED BY
	 * something, so each segment has its own data partition; the trigger
	 * fires on whichever segment the tuple physically lives.
	 */
	if (Gp_role != GP_ROLE_EXECUTE)
	{
		HeapTuple	ret_tuple = trigdata->tg_newtuple ?
		trigdata->tg_newtuple : trigdata->tg_trigtuple;

		if (ret_tuple == NULL && trigdata->tg_newslot != NULL &&
			!TupIsNull(trigdata->tg_newslot))
			ret_tuple = ExecFetchSlotHeapTuple(trigdata->tg_newslot,
											   false, NULL);
		if (ret_tuple == NULL && trigdata->tg_trigslot != NULL &&
			!TupIsNull(trigdata->tg_trigslot))
			ret_tuple = ExecFetchSlotHeapTuple(trigdata->tg_trigslot,
											   false, NULL);
		return PointerGetDatum(ret_tuple);
	}

	source_oid = RelationGetRelid(trigdata->tg_relation);

	/* Refresh per-transaction cache if needed */
	cagg_invalidate_cache_if_needed(source_oid);

	/* Get time column attnum (cached per transaction) */
	time_attnum = cagg_get_time_attnum(source_oid, trigdata->tg_relation);

	/*
	 * Get HeapTuples from trigger data.  In PG14/CBDB, the trigger may only
	 * populate TupleTableSlots (tg_trigslot/tg_newslot) and leave
	 * tg_trigtuple/tg_newtuple as NULL -- especially on segment QEs. We must
	 * handle both cases.
	 */
	{
		HeapTuple	old_tuple = trigdata->tg_trigtuple;
		HeapTuple	new_tuple = trigdata->tg_newtuple;

		if (old_tuple == NULL && trigdata->tg_trigslot != NULL &&
			!TupIsNull(trigdata->tg_trigslot))
			old_tuple = ExecFetchSlotHeapTuple(trigdata->tg_trigslot,
											   false, NULL);

		if (new_tuple == NULL && trigdata->tg_newslot != NULL &&
			!TupIsNull(trigdata->tg_newslot))
			new_tuple = ExecFetchSlotHeapTuple(trigdata->tg_newslot,
											   false, NULL);

		/*
		 * CBDB quirk: for AFTER ROW INSERT, the inserted tuple is in
		 * tg_trigtuple (not tg_newtuple as in standard PG).  We handle this
		 * by using whichever tuple is available.
		 */

		/* Process based on event type */
		if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
		{
			HeapTuple	ins = new_tuple ? new_tuple : old_tuple;

			if (ins)
				cagg_process_tuple(trigdata, ins,
								   source_oid, time_attnum);
		}
		else if (TRIGGER_FIRED_BY_DELETE(trigdata->tg_event))
		{
			HeapTuple	del = old_tuple ? old_tuple : new_tuple;

			if (del)
				cagg_process_tuple(trigdata, del,
								   source_oid, time_attnum);
		}
		else if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
		{
			/* UPDATE: process both old and new if available */
			if (old_tuple)
				cagg_process_tuple(trigdata, old_tuple,
								   source_oid, time_attnum);
			if (new_tuple)
				cagg_process_tuple(trigdata, new_tuple,
								   source_oid, time_attnum);
		}

		/* Return whichever tuple is available */
		return PointerGetDatum(new_tuple ? new_tuple : old_tuple);
	}
}

/*
 * Segment-local watermark initialization.
 *
 * Called via: SELECT _cagg_init_segment_watermark(cagg_id)
 *            FROM gp_dist_random('gp_id');
 *
 * This dispatches to every segment, and each segment inserts one
 * watermark row locally via simple_heap_insert.  This ensures every
 * segment has a local watermark row for threshold computation.
 */

PG_FUNCTION_INFO_V1(cagg_init_segment_watermark);

Datum
cagg_init_segment_watermark(PG_FUNCTION_ARGS)
{
	int			cagg_id = PG_GETARG_INT32(0);
	Oid			ns_oid;
	Oid			wm_oid;
	Relation	wm_rel;
	Datum		values[2];
	bool		nulls[2] = {false, false};
	HeapTuple	tup;
	TimestampTz neg_inf;

	ns_oid = ht_get_namespace_oid_cached();
	wm_oid = get_relname_relid("cagg_watermark", ns_oid);
	if (!OidIsValid(wm_oid))
		elog(ERROR, "_cagg_init_segment_watermark: cagg_watermark table not found");

	wm_rel = table_open(wm_oid, RowExclusiveLock);

	TIMESTAMP_NOBEGIN(neg_inf);
	values[0] = Int32GetDatum(cagg_id);
	values[1] = TimestampTzGetDatum(neg_inf);

	tup = heap_form_tuple(RelationGetDescr(wm_rel), values, nulls);
	simple_heap_insert(wm_rel, tup);
	heap_freetuple(tup);

	table_close(wm_rel, RowExclusiveLock);

	elog(DEBUG2, "cagg watermark initialized: cagg_id=%d, segment-local",
		 cagg_id);
	PG_RETURN_VOID();
}

/*
 * Segment-local threshold initialization.
 *
 * Similar to cagg_init_segment_watermark: inserts one row per segment
 * into cagg_invalidation_threshold via simple_heap_insert.
 *
 * Called via: SELECT _cagg_init_segment_threshold(source_oid)
 *            FROM gp_dist_random('gp_id');
 */

PG_FUNCTION_INFO_V1(cagg_init_segment_threshold);

Datum
cagg_init_segment_threshold(PG_FUNCTION_ARGS)
{
	Oid			source_oid = PG_GETARG_OID(0);
	Oid			ns_oid;
	Oid			th_oid;
	Relation	th_rel;
	Datum		values[2];
	bool		nulls[2] = {false, false};
	HeapTuple	tup;
	TimestampTz neg_inf;

	ns_oid = ht_get_namespace_oid_cached();
	th_oid = get_relname_relid("cagg_invalidation_threshold", ns_oid);
	if (!OidIsValid(th_oid))
		elog(ERROR,
			 "_cagg_init_segment_threshold:"
			 " cagg_invalidation_threshold table not found");

	th_rel = table_open(th_oid, RowExclusiveLock);

	TIMESTAMP_NOBEGIN(neg_inf);
	values[0] = ObjectIdGetDatum(source_oid);
	values[1] = TimestampTzGetDatum(neg_inf);

	tup = heap_form_tuple(RelationGetDescr(th_rel), values, nulls);
	simple_heap_insert(th_rel, tup);
	heap_freetuple(tup);

	table_close(th_rel, RowExclusiveLock);

	elog(DEBUG2, "cagg threshold initialized: source_oid=%u, segment-local",
		 source_oid);
	PG_RETURN_VOID();
}

/*
 * cagg_watermark(cagg_id) -- per-segment watermark lookup via C.
 *
 * Called from the user view's WHERE clause (both on QD and segment QEs).
 * Uses direct heap scan (no SPI) because CBDB segment QEs cannot
 * execute SPI on distributed tables.
 *
 * MUST be registered as VOLATILE to prevent the planner from evaluating
 * it at plan time on QD (which would constant-fold to a single value).
 */

PG_FUNCTION_INFO_V1(cagg_watermark_fn);

Datum
cagg_watermark_fn(PG_FUNCTION_ARGS)
{
	int			target_cagg_id;
	Oid			ns_oid;
	Oid			wm_oid;

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("continuous aggregate ID cannot be NULL")));

	target_cagg_id = PG_GETARG_INT32(0);
	Relation	wm_rel;
	TableScanDesc scan;
	HeapTuple	tup;
	TimestampTz result;
	bool		found = false;

	TIMESTAMP_NOBEGIN(result);	/* default: -infinity */

	ns_oid = ht_get_namespace_oid_cached();
	wm_oid = get_relname_relid("cagg_watermark", ns_oid);

	if (!OidIsValid(wm_oid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("watermark table not found"),
				 errhint("Is the time_series extension installed?")));

	wm_rel = table_open(wm_oid, AccessShareLock);
	scan = heap_beginscan(wm_rel, GetActiveSnapshot(), 0, NULL, NULL, 0);

	while ((tup = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		bool		isnull;
		Datum		d_id,
					d_wm;
		int			cid;

		/* column 1 = cagg_id */
		d_id = heap_getattr(tup, 1, RelationGetDescr(wm_rel), &isnull);
		if (isnull)
			continue;
		cid = DatumGetInt32(d_id);

		if (cid != target_cagg_id)
			continue;

		/* column 2 = watermark */
		d_wm = heap_getattr(tup, 2, RelationGetDescr(wm_rel), &isnull);
		if (!isnull)
			result = DatumGetTimestampTz(d_wm);
		found = true;
		break;					/* found our row */
	}

	heap_endscan(scan);
	table_close(wm_rel, AccessShareLock);

	/*
	 * On segment QEs, watermark rows MUST exist (DISTRIBUTED RANDOMLY
	 * guarantees each segment has rows after _cagg_init_segment_watermark).
	 * Not finding them means catalog corruption.
	 *
	 * On QD, the table is RANDOMLY distributed so QD itself has no rows.
	 * Return -infinity (same as initial state) -- the QD never uses this
	 * value for the UNION ALL split (that happens on segments).
	 */
	if (!found && Gp_role == GP_ROLE_EXECUTE)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("watermark not found for continuous aggregate %d",
						target_cagg_id),
				 errhint("The watermark catalog may be corrupted. "
						 "Re-create the watermark rows using "
						 "_cagg_init_segment_watermark().")));

	PG_RETURN_TIMESTAMPTZ(result);
}
