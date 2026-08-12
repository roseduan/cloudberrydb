/*-------------------------------------------------------------------------
 *
 * ts_scan.c
 *    ChunkScan CustomScan: chunk-level time pruning for SELECT on time_series
 *    tables.  Extracts WHERE clause time bounds, calculates the set of
 *    qualifying chunks, and scans only those chunks.
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 * IDENTIFICATION
 *    contrib/time_series/src/access/ts_scan.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/parallel.h"
#include "access/table.h"
#include "access/tableam.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "common/relpath.h"
#include "datatype/timestamp.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "nodes/extensible.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pathnodes.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/plancat.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "optimizer/restrictinfo.h"
#include "utils/guc.h"
#include "nodes/makefuncs.h"
#include "catalog/namespace.h"
#include "utils/array.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"

#include "cdb/cdbpathlocus.h"
#include "cdb/cdbvars.h"
#include "catalog/pg_class.h"
#include "catalog/pg_operator_d.h"
#include "utils/syscache.h"
#include "utils/ruleutils.h"
#include "rewrite/rewriteManip.h"

#ifdef FAULT_INJECTOR
#include "utils/faultinjector.h"
#endif

#include "../include/time_series.h"
#include "../include/access/ts_tableam.h"
#include "../include/compress/ts_compress.h"

/* Forward declarations of the param-substitution helpers (defined below). */
static Node *ts_substitute_params_mutator(Node *node, ExprContext *econtext);
static List *ts_freeze_quals_for_sparse_filter(List *quals, ExprContext *econtext);
static bool ts_get_chunk_stats(Oid relid, int *n_total, int *n_compressed);

/* Forward declarations of the set_rel_pathlist helpers (defined below). */
static void ts_maybe_add_parallel_chunkscan_path(PlannerInfo *root, RelOptInfo *rel,
												 CustomPath *cpath);
static void ts_collect_param_pushdown_clauses(PlannerInfo *root, RelOptInfo *rel,
											  List **pushable_clauses, Relids *required_outer);
static double ts_compute_param_effective_factor(double selectivity, double frac_comp);
static void ts_maybe_add_param_chunkscan_path(PlannerInfo *root, RelOptInfo *rel,
											  RangeTblEntry *rte, List *priv,
											  Cost startup_cost, Cost total_cost);

/*
 *		CustomScan method structs
 */
static CustomPathMethods  ts_scan_path_methods;
static CustomScanMethods  ts_scan_methods;
static CustomExecMethods  ts_scan_exec_methods;

/* Forward declarations for exec callbacks */
static void ts_chunk_scan_begin(CustomScanState *node, EState *estate, int eflags);
static TupleTableSlot *ts_chunk_scan_exec(CustomScanState *node);
static void ts_chunk_scan_end(CustomScanState *node);
static void ts_chunk_scan_rescan(CustomScanState *node);
static void ts_chunk_scan_explain(CustomScanState *node, List *ancestors, ExplainState *es);
static Size ts_chunk_scan_estimate_dsm(CustomScanState *node, ParallelContext *pcxt);
static void ts_chunk_scan_initialize_dsm(CustomScanState *node, ParallelContext *pcxt, void *coord);
static void ts_chunk_scan_reinitialize_dsm(CustomScanState *node, ParallelContext *pcxt, void *coord);
static void ts_chunk_scan_initialize_worker(CustomScanState *node, shm_toc *toc, void *coord);

/* Previous set_rel_pathlist_hook */
static set_rel_pathlist_hook_type prev_set_rel_pathlist_hook = NULL;

/* Previous planner_hook (chained) */
static planner_hook_type prev_planner_hook = NULL;

/*
 * Flag bits for the inclusivity byte serialized into custom_private alongside
 * the time-bound int64 pair.  See ts_scan_set_rel_pathlist (encode) and
 * ts_chunk_scan_deserialize_priv (decode).
 */
#define TS_BOUND_FLAG_MIN_INCLUSIVE		0x01	/* ts >= const  (vs ts >  const) */
#define TS_BOUND_FLAG_MAX_INCLUSIVE		0x02	/* ts <= const  (vs ts <  const) */

/*
 *		ChunkScanParallelDSM -- shared state for Parallel ChunkScan
 *
 *		One atomic counter handed out at the chunk-list granularity.
 *		Every backend that participates in the scan (leader + workers)
 *		claims its next chunk by `pg_atomic_fetch_add_u32(..., 1)`.
 *		A claim that returns an index >= nchunks means the scan is
 *		drained — that backend has no more work.
 *
 *		Chunk-list granularity (not block-level like Parallel Seq Scan)
 *		is the natural unit: per-chunk init (smgr open, PAX reader
 *		open, count-only fast path) is non-trivial, so handing out
 *		whole chunks amortises that overhead.
 */
typedef struct ChunkScanParallelDSM
{
	pg_atomic_uint32 next_chunk_idx;
} ChunkScanParallelDSM;

/*
 *		ChunkScanState -- execution state for ChunkScan
 */
typedef struct ChunkScanState
{
	CustomScanState css;
	TSConfig	config;
	int64		ts_min;			/* lower bound in usec (DT_NOBEGIN) */
	int64		ts_max;			/* upper bound in usec (DT_NOEND) */
	bool		ts_min_inclusive;
	bool		ts_max_inclusive;
	ForkNumber	min_chunk;		/* pruned chunk range lower bound */
	ForkNumber	max_chunk;		/* pruned chunk range upper bound */
	ForkNumber *chunk_list;
	int			nchunks;
	int			cur_chunk_idx;
	BlockNumber cur_block;
	BlockNumber cur_chunk_nblocks;	/* cached smgrnblocks for current chunk;
									 * InvalidBlockNumber until first call into
									 * a chunk computes it.  Avoids per-tuple
									 * smgrnblocks → mdnblocks → lseek syscall
									 * (~5% of count(*) wall time on perf). */
	OffsetNumber cur_offset;
	Buffer		cur_buffer;
	Relation	rel;
	Snapshot	snapshot;
	ExprState  *recheck_quals;
	int16	   *chunk_status;		/* parallel to chunk_list: 0=ACTIVE, 2=COMPRESSED, 3=PARTIAL */
	TSPaxReader cur_pax_reader;		/* reader for current COMPRESSED/PARTIAL chunk */
	TupleTableSlot *pax_slot;		/* virtual slot for PAX reader output */
	bool		pax_exhausted;		/* true once PARTIAL chunk's PAX reader is done, heap next */
	/*
	 * Count-only fast path: when the scan has no filter and the upper plan
	 * only needs row counts (no column values), we can skip opening PAX
	 * and emit numrows empty virtual tuples per COMPRESSED chunk.
	 */
	bool		count_only;
	int64	   *chunk_numrows;		/* numrows per chunk (COMPRESSED only) */
	int64		count_emitted;		/* rows emitted for current chunk */
	/*
	 * Column projection for PAX reader: indexes [0..natts) of columns
	 * referenced by the query's targetlist + qual.  NULL means no
	 * projection (read all columns).
	 */
	bool	   *proj_bitmap;
	/*
	 * Auxiliary virtual slot for the count-only fast path.  scanslot is
	 * a Virtual slot used by the heap fetch path; count-only emits
	 * phantom all-NULL rows that we initialize once and bump per row.
	 * Created only when count_only is true.
	 */
	TupleTableSlot *count_slot;
	/*
	 * Page-mode visibility cache.  Mirror PG SeqScan's heapgetpage pattern:
	 * when we enter a new page we acquire share lock, walk all ItemIds,
	 * run HeapTupleSatisfiesVisibility once per tuple, store the visible
	 * offsets here, then drop the lock (keep the pin).  Subsequent per-row
	 * calls just consume vistuples[cur_vis++] — no visibility eval,
	 * no lock acquire/release, no maxoff probe.
	 */
	OffsetNumber vistuples[MaxHeapTuplesPerPage];
	int			nvistuples;
	int			cur_vis;
	/*
	 * Ring-buffer access strategy for sequential scans.  Without this the
	 * fork's pages would push useful state out of shared_buffers and pay
	 * a full BufTableLookup on every ReadBufferExtended.  Standard PG
	 * SeqScan uses BAS_BULKREAD; perf showed `hash_bytes`,
	 * `hash_search_with_hash_value`, `GetPrivateRefCountEntry` collectively
	 * at ~9% on time_series count(*) vs invisible on heap SeqScan, traced
	 * to this asymmetry.  Allocated in begin, released in end.
	 */
	BufferAccessStrategy bas_strategy;

	/*
	 * Parallel ChunkScan shared state, or NULL when running serially.
	 * Pointer into DSM owned by the leader's ParallelContext; valid for the
	 * lifetime of the scan.  All chunk-claim sites (initial claim in
	 * BeginCustomScan / InitializeDSM / InitializeWorker, and per-chunk
	 * advance in ts_chunk_scan_advance) read cur_chunk_idx from this
	 * counter via pg_atomic_fetch_add_u32 when non-NULL.  Each chunk index
	 * is therefore claimed by exactly one backend — eliminates the "every
	 * worker re-scans every chunk" duplication that naive per-backend
	 * cur_chunk_idx counters produce.
	 */
	ChunkScanParallelDSM *parallel_shared;
	/*
	 * True after Initialize{DSM,Worker} runs, false after the first
	 * chunk claim.  Deferring the first claim until the *executing*
	 * backend enters exec_unordered (instead of pre-claiming in
	 * Initialize{DSM,Worker}) is critical under Parallel Append: a
	 * worker that initializes a subplan is not guaranteed to be the
	 * one that executes it.  Pre-claiming chunk 0 in InitializeDSM
	 * burns chunk 0 — the initializer never runs the subplan, and the
	 * worker that actually does run it fetches index 1 (= EOF on
	 * single-chunk paths), losing all rows in chunk 0.
	 */
	bool		parallel_first_claim;
	/*
	 * Single-chunk restriction.  Default -1 (= scan whatever the
	 * [ts_min, ts_max] window selects, the normal multi-chunk path).
	 * When the encoded custom_private carries a fourth element, that
	 * value lands here and BeginCustomScan filters chunk_list down to a
	 * single entry — used by ChunkAppend to give each subpath a
	 * single-chunk view over the same logical relation.
	 */
	int32		chunk_only_num;
} ChunkScanState;

/*
 * ts_encode_int64
 *		Split a 64-bit value into two 32-bit Integer nodes for
 *		storage in custom_private lists.
 */
static List *
ts_encode_int64(int64 val)
{
	int32	high = (int32) (val >> 32);
	int32	low = (int32) (val & 0xFFFFFFFF);

	return list_make2(makeInteger(high), makeInteger(low));
}

/*
 * ts_decode_int64
 *		Reconstruct a 64-bit value from an Integer-pair list
 *		produced by ts_encode_int64().
 */
static int64
ts_decode_int64(List *pair)
{
	int64	high = (int64) (uint32) intVal(linitial(pair));
	int64	low = (int64) (uint32) intVal(lsecond(pair));

	return (high << 32) | low;
}

/*
 * Strip RelabelType wrappers from a clause argument.  The planner adds
 * these around Var/Const nodes when a binary-compatible cast appears in
 * the query (e.g. `cast(ts as timestamp) > '...'`); without peeling them
 * off the IsA(Var)/IsA(Const) checks below silently fail and the time
 * bound gets dropped.  Mirrors TSDB's expression_utils.c handling.
 */
static inline Expr *
ts_strip_relabel(Expr *e)
{
	while (e != NULL && IsA(e, RelabelType))
		e = ((RelabelType *) e)->arg;
	return e;
}

/*
 * Map a btree strategy number to a bound update on (ts_min, ts_max).
 * Preserves the original semantics:
 *   - >=  : raise ts_min iff strictly greater; min_inclusive=true
 *   - >   : raise ts_min if strictly greater OR equal-and-was-inclusive
 *           (this is the only path that flips inclusive from true to false)
 *   - <=  : symmetric, on ts_max
 *   - <   : symmetric, on ts_max
 *   - =   : overwrite both bounds (contradictory predicates are caller error)
 */
static void
ts_apply_strategy_to_bounds(StrategyNumber strategy, int64 val,
							int64 *ts_min, bool *min_inclusive,
							int64 *ts_max, bool *max_inclusive)
{
	switch (strategy)
	{
		case BTGreaterEqualStrategyNumber:
			if (val > *ts_min)
			{
				*ts_min = val;
				*min_inclusive = true;
			}
			break;
		case BTGreaterStrategyNumber:
			if (val > *ts_min || (val == *ts_min && *min_inclusive))
			{
				*ts_min = val;
				*min_inclusive = false;
			}
			break;
		case BTLessEqualStrategyNumber:
			if (val < *ts_max)
			{
				*ts_max = val;
				*max_inclusive = true;
			}
			break;
		case BTLessStrategyNumber:
			if (val < *ts_max || (val == *ts_max && *max_inclusive))
			{
				*ts_max = val;
				*max_inclusive = false;
			}
			break;
		case BTEqualStrategyNumber:
			*ts_min = val;
			*ts_max = val;
			*min_inclusive = true;
			*max_inclusive = true;
			break;
		default:
			/* unsupported strategy — leave bounds untouched */
			break;
	}
}

/*
 * If Var sits on the right side of the operator (i.e. `const OP var`),
 * the bound semantics flip: `const < var` means `var > const`.  Mirror
 * the strategy through the btree-strategy axis so the downstream apply
 * function sees the var-on-left form.
 */
static inline StrategyNumber
ts_swap_strategy_var_on_right(StrategyNumber s)
{
	switch (s)
	{
		case BTLessStrategyNumber:			return BTGreaterStrategyNumber;
		case BTLessEqualStrategyNumber:		return BTGreaterEqualStrategyNumber;
		case BTGreaterEqualStrategyNumber:	return BTLessEqualStrategyNumber;
		case BTGreaterStrategyNumber:		return BTLessStrategyNumber;
		case BTEqualStrategyNumber:			return BTEqualStrategyNumber;
		default:							return InvalidStrategy;
	}
}

/*
 * ts_extract_time_bounds
 *
 *		Walk baserestrictinfo to find OpExpr clauses comparing the
 *		ts_column (identified by ts_attnum and ts_typid) to a Const.
 *		Extract lower/upper bounds for chunk pruning.
 */
static void
ts_extract_time_bounds(PlannerInfo *root, RelOptInfo *rel,
					   AttrNumber ts_attnum, Oid ts_typid,
					   int64 *ts_min, bool *min_inclusive,
					   int64 *ts_max, bool *max_inclusive)
{
	ListCell   *lc;

	*ts_min = DT_NOBEGIN;
	*ts_max = DT_NOEND;
	*min_inclusive = true;
	*max_inclusive = true;

	foreach(lc, rel->baserestrictinfo)
	{
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);
		OpExpr	   *op;
		Expr	   *leftop;
		Expr	   *rightop;
		Var		   *var;
		Const	   *cnst;
		bool		var_on_left;
		List	   *opinfos;
		StrategyNumber strategy;
		int64		val;

		if (!IsA(rinfo->clause, OpExpr))
			continue;
		op = (OpExpr *) rinfo->clause;
		if (list_length(op->args) != 2)
			continue;

		/*
		 * Peel RelabelType off both sides before matching shape — the
		 * planner inserts these for binary-compatible casts (e.g.
		 * `ts::timestamp > '...'`).
		 */
		leftop = ts_strip_relabel((Expr *) linitial(op->args));
		rightop = ts_strip_relabel((Expr *) lsecond(op->args));

		if (IsA(leftop, Var) && IsA(rightop, Const))
		{
			var = (Var *) leftop;
			cnst = (Const *) rightop;
			var_on_left = true;
		}
		else if (IsA(rightop, Var) && IsA(leftop, Const))
		{
			var = (Var *) rightop;
			cnst = (Const *) leftop;
			var_on_left = false;
		}
		else
			continue;

		if (var->varattno != ts_attnum)
			continue;
		if (cnst->consttype != TIMESTAMPTZOID)
			continue;
		if (cnst->constisnull)
			continue;

		/*
		 * Resolve the operator's btree-family strategy number from its
		 * opfamily rather than strcmp'ing the operator name.  This is
		 * how TSDB's hypertable_restrict_info does it; any operator
		 * registered in a btree opfamily as <, <=, =, >=, > is picked
		 * up, regardless of the surface-syntax name.
		 */
		opinfos = get_op_btree_interpretation(op->opno);
		if (opinfos == NIL)
			continue;
		strategy = ((OpBtreeInterpretation *) linitial(opinfos))->strategy;
		list_free_deep(opinfos);

		if (!var_on_left)
			strategy = ts_swap_strategy_var_on_right(strategy);
		if (strategy == InvalidStrategy)
			continue;

		val = DatumGetTimestampTz(cnst->constvalue);
		ts_apply_strategy_to_bounds(strategy, val,
									ts_min, min_inclusive,
									ts_max, max_inclusive);
	}
}

/*
 * ts_scan_create_plan
 *		Convert the ChunkScan CustomPath into a CustomScan plan node.
 *		Serialized time bounds are passed through custom_private.
 */
static Plan *
ts_scan_create_plan(PlannerInfo *root, RelOptInfo *rel, CustomPath *best_path,
					List *tlist, List *clauses, List *custom_plans)
{
	CustomScan *cscan;
	List	   *stripped;

	cscan = makeNode(CustomScan);
	cscan->scan.plan.targetlist = tlist;

	/* Remove pseudoconstant quals, keep the rest as scan quals */
	stripped = extract_actual_clauses(clauses, false);
	cscan->scan.plan.qual = stripped;

	/*
	 * Set scanrelid to the RTE index so the executor treats this as a real table scan (like
	 * SeqScan).  When scanrelid > 0 and custom_scan_tlist is NIL, setrefs.c resolves Vars
	 * against the base relation directly.
	 */
	cscan->scan.scanrelid = rel->relid;
	cscan->custom_scan_tlist = NIL;
	cscan->custom_plans = NIL;
	cscan->flags = best_path->flags;
	cscan->methods = &ts_scan_methods;

	/* Pass serialized time bounds from path to plan */
	cscan->custom_private = best_path->custom_private;

	return (Plan *) cscan;
}

/*
 * ts_scan_create_state
 *		Allocate and initialise the ChunkScanState for a ChunkScan plan node.
 */
static Node *
ts_scan_create_state(CustomScan *cscan)
{
	ChunkScanState *state;

	state = (ChunkScanState *) palloc0(sizeof(ChunkScanState));
	NodeSetTag(state, T_CustomScanState);
	state->css.methods = &ts_scan_exec_methods;
	state->cur_buffer = InvalidBuffer;

	return (Node *) state;
}

/*
 * ts_chunk_scan_deserialize_priv
 *		Recover the time bounds the planner stored in custom_private:
 *		two int64 pairs (encoded as Integer-list pairs to survive plan
 *		dispatch) plus a flags byte for inclusivity.
 */
static void
ts_chunk_scan_deserialize_priv(ChunkScanState *state, CustomScan *cscan)
{
	List	   *priv = cscan->custom_private;
	List	   *min_pair = (List *) linitial(priv);
	List	   *max_pair = (List *) lsecond(priv);
	int			flags = intVal(lthird(priv));

	state->ts_min = ts_decode_int64(min_pair);
	state->ts_max = ts_decode_int64(max_pair);
	state->ts_min_inclusive = (flags & TS_BOUND_FLAG_MIN_INCLUSIVE) != 0;
	state->ts_max_inclusive = (flags & TS_BOUND_FLAG_MAX_INCLUSIVE) != 0;

	/*
	 * Optional 4th element: chunk_only_num (ChunkAppend subpath marker).
	 * Older callers emit 3-element private; ChunkAppend emits 4.  Default
	 * -1 = no restriction.
	 */
	if (list_length(priv) >= 4)
		state->chunk_only_num = intVal(lfourth(priv));
	else
		state->chunk_only_num = -1;
}

/*
 * ts_chunk_scan_compute_range
 *		Translate the deserialized time bounds into a [min_chunk, max_chunk]
 *		fork-number window.  Unbounded sides fall back to a generous
 *		[origin, now+interval] envelope so the catalog filter below still
 *		has something to clamp against.
 */
static void
ts_chunk_scan_compute_range(ChunkScanState *state)
{
	ForkNumber	min_chunk;
	ForkNumber	max_chunk;

	if (state->ts_min != DT_NOBEGIN)
	{
		min_chunk = ts_calculate_chunk(state->ts_min, state->config.origin_usec,
									   state->config.interval_usec);
		if (min_chunk == InvalidForkNumber)
			min_chunk = TS_FIRST_CHUNKNUM;
	}
	else
	{
		/*
		 * No lower time bound — scan all chunks from the origin forward.
		 * Origin is the minimum chunk boundary by construction (chunks
		 * before origin can't exist), so TS_FIRST_CHUNKNUM is tight.
		 */
		min_chunk = TS_FIRST_CHUNKNUM;
	}

	if (state->ts_max != DT_NOEND)
	{
		max_chunk = ts_calculate_chunk(state->ts_max, state->config.origin_usec,
									   state->config.interval_usec);
		if (max_chunk == InvalidForkNumber)
			max_chunk = TS_FIRST_CHUNKNUM - 1;
	}
	else
	{
		int64	now_usec = GetCurrentTimestamp();

		/*
		 * No upper time bound — scan up to "now + 1 interval", then add
		 * a 10-chunk buffer.  The buffer catches future-timestamped
		 * INSERTs (clock skew across segments, replicas with stale
		 * clocks, or deliberately back/forward-dated rows) that landed
		 * in chunks numerically ahead of `now`.  Without it, those
		 * chunks would be pruned out and the SELECT would silently miss
		 * rows.  10 is the same buffer width used for capacity-growth
		 * in ts_ensure_fork_capacity (ts_fork_storage.c).
		 */
		max_chunk = ts_calculate_chunk(now_usec + state->config.interval_usec,
									   state->config.origin_usec,
									   state->config.interval_usec);
		max_chunk += 10;
	}

	state->min_chunk = min_chunk;
	state->max_chunk = max_chunk;
}

/*
 * ts_chunk_scan_load_chunks
 *		Query the ts_chunk catalog for chunks in [min_chunk, max_chunk]
 *		with status, store result on `state`.  Range filter is pushed
 *		down to the catalog scan (ScanKeys), so we don't need a second
 *		pass on the caller side.
 *
 *		`state->chunk_list` / `state->chunk_status` are NULL when no
 *		chunk falls in range; iteration is gated on
 *		`state->cur_chunk_idx < state->nchunks` and never dereferences
 *		either pointer when n=0.
 */
static void
ts_chunk_scan_load_chunks(ChunkScanState *state)
{
	TSChunkList	list;

	list = ts_chunk_catalog_get_chunks_with_status(
		RelationGetRelid(state->rel),
		state->min_chunk,
		state->max_chunk);

	state->chunk_list = list.chunks;
	state->chunk_status = list.statuses;
	state->nchunks = list.n;
}

/*
 * ts_chunk_scan_setup_count_or_proj
 *		Two related plan-time optimisations driven by the targetlist:
 *
 *		  count-only: no qual + no column referenced → we can emit
 *		              numrows empty virtual tuples per COMPRESSED chunk
 *		              without opening the PAX file.  Typical trigger:
 *		              SELECT count(*) [WHERE time predicate].
 *
 *		  proj_bitmap: only decode the columns referenced by tlist or
 *		               qual; PAX skips decoding the rest.
 *
 *		Both are no-ops when the chunk list is empty.
 */
static void
ts_chunk_scan_setup_count_or_proj(ChunkScanState *state, CustomScanState *node)
{
	List	   *tlist;
	Index		scanrelid;
	Bitmapset  *tlist_attrs = NULL;
	Bitmapset  *qual_attrs = NULL;
	ListCell   *lc;
	int			natts;
	bool		count_only;

	state->count_only = false;
	state->chunk_numrows = NULL;
	state->count_emitted = 0;
	state->proj_bitmap = NULL;

	if (state->nchunks == 0)
		return;

	tlist = node->ss.ps.plan->targetlist;
	scanrelid = ((Scan *) node->ss.ps.plan)->scanrelid;
	natts = RelationGetDescr(state->rel)->natts;

	foreach(lc, tlist)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);
		pull_varattnos((Node *) te->expr, scanrelid, &tlist_attrs);
	}
	if (node->ss.ps.plan->qual != NIL)
		pull_varattnos((Node *) node->ss.ps.plan->qual, scanrelid, &qual_attrs);

	/*
	 * count-only: no qual, and tlist doesn't reference any relation column.
	 * pull_varattnos uses FirstLowInvalidHeapAttributeNumber-based numbering
	 * for system attrs; empty bitmap = no column referenced.
	 */
	count_only = (state->recheck_quals == NULL && bms_is_empty(tlist_attrs));

	if (count_only)
	{
		bool	any_compressed = false;
		int		i;

		for (i = 0; i < state->nchunks; i++)
		{
			if (state->chunk_status != NULL &&
				state->chunk_status[i] == TS_CHUNK_COMPRESSED)
			{
				any_compressed = true;
				break;
			}
		}

		if (any_compressed)
		{
			TupleTableSlot *cslot;

			state->count_only = true;
			state->chunk_numrows = ts_compressed_chunk_load_numrows(
				RelationGetRelid(state->rel), state->chunk_list,
				state->chunk_status, state->nchunks);

			/*
			 * Build an aux virtual slot whose tts_isnull starts all-true
			 * so count-only emit can flip TTS_EMPTY/nvalid without
			 * materializing columns.  scanslot is BufferHeapTuple (for the
			 * heap fetch fast path) and doesn't accept that manipulation.
			 */
			cslot = MakeTupleTableSlot(RelationGetDescr(state->rel),
									   &TTSOpsVirtual);
			memset(cslot->tts_isnull, true,
				   cslot->tts_tupleDescriptor->natts * sizeof(bool));
			state->count_slot = cslot;
		}
	}
	else
	{
		int		attno;
		bool	any_col = false;
		bool	all_cols = true;

		state->proj_bitmap = (bool *) palloc0(sizeof(bool) * natts);
		for (attno = 1; attno <= natts; attno++)
		{
			int		off = attno - FirstLowInvalidHeapAttributeNumber;
			bool	want = bms_is_member(off, tlist_attrs) ||
						   bms_is_member(off, qual_attrs);
			state->proj_bitmap[attno - 1] = want;
			if (want)
				any_col = true;
			else
				all_cols = false;
		}

		/*
		 * All-columns or zero-columns: skip the bitmap so PAX doesn't
		 * pay the per-row filter cost.
		 */
		if (all_cols || !any_col)
		{
			pfree(state->proj_bitmap);
			state->proj_bitmap = NULL;
		}
	}

	bms_free(tlist_attrs);
	bms_free(qual_attrs);
}

/*
 * ts_chunk_scan_begin
 *		BeginCustomScan callback.  Recovers planner-supplied time bounds,
 *		computes the chunk-fork range, loads the chunks-with-status, and
 *		prepares iteration / count-only / projection state.  Each step
 *		lives in its own helper above.
 */
static void
ts_chunk_scan_begin(CustomScanState *node, EState *estate, int eflags)
{
	ChunkScanState *state = (ChunkScanState *) node;
	CustomScan	   *cscan = (CustomScan *) node->ss.ps.plan;
	Oid				rel_oid;

	ts_chunk_scan_deserialize_priv(state, cscan);

	state->rel = node->ss.ss_currentRelation;
	state->snapshot = estate->es_snapshot;
	state->recheck_quals = node->ss.ps.qual;

	/*
	 * Defensive: ChunkScan plan only gets built from a time_series
	 * RelOptInfo (see ts_scan_set_rel_pathlist below), so both checks
	 * should be unreachable in practice.  Splitting them makes the
	 * error message accurate when the invariant does break.
	 */
	if (!RelationIsTimeSeries(state->rel))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("ChunkScan: relation \"%s\" is not a time_series table",
						RelationGetRelationName(state->rel))));

	if (!ts_get_config(state->rel, &state->config))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("ChunkScan: time_series table \"%s\" is missing required configuration",
						RelationGetRelationName(state->rel))));

	ts_chunk_scan_compute_range(state);
	ts_chunk_scan_load_chunks(state);

	/*
	 * ChunkAppend subpath restriction: keep only the single chunk this
	 * subpath was generated for.  Walk chunk_list, find the entry whose
	 * fork-number == chunk_only_num, compact list to that single entry.
	 * If the requested chunk isn't in the catalog (e.g. dropped between
	 * planning and execution), set nchunks=0 so the scan emits zero rows
	 * — the ChunkAppend parent then advances to the next subscan.
	 */
	if (state->chunk_only_num >= 0)
	{
		int		keep_idx = -1;
		ForkNumber	target = (ForkNumber) state->chunk_only_num;

		for (int i = 0; i < state->nchunks; i++)
		{
			if (state->chunk_list[i] == target)
			{
				keep_idx = i;
				break;
			}
		}

		if (keep_idx >= 0)
		{
			state->chunk_list[0] = state->chunk_list[keep_idx];
			if (state->chunk_status != NULL)
				state->chunk_status[0] = state->chunk_status[keep_idx];
			state->nchunks = 1;
		}
		else
		{
			state->nchunks = 0;
		}
	}

	/*
	 * Reader-vs-reclaim synchronisation.  For every chunk this scan
	 * may read from heap (ACTIVE / PARTIAL), take the per-chunk
	 * advisory ShareLock.  reclaim's matching ExclusiveLock cannot
	 * acquire while we hold Share, so smgrtruncate is fenced off the
	 * heap fork for the lifetime of this xact.  COMPRESSED chunks
	 * read only from the PAX file (immutable, written via
	 * .new+rename); no lock needed.
	 *
	 * Same namespace as INSERT / compress / reclaim — reuses the
	 * existing lock matrix.  Side effect: compress's Exclusive on
	 * this lock now also waits for in-flight reads, so a long-running
	 * SELECT delays compress for that long.  Accepted: simpler
	 * single-namespace semantics, and the same "maintenance ops
	 * yield to in-flight queries" rule INSERT vs compress already
	 * follows.
	 *
	 * Without this, ts_chunk_scan_use_pax → heap path dereferences
	 * a stale cached cur_chunk_nblocks (or recomputes to 0 after
	 * truncate) and returns either an mdread EOF error or zero rows
	 * — silent data loss for MVCC snapshots that predate compress.
	 * See compress_reclaim_reader_race isolation2 spec for the
	 * unfenced-truncate repro.
	 */
	rel_oid = RelationGetRelid(state->rel);

	/*
	 * Test-only suspension point.  Placed AFTER load_chunks but BEFORE
	 * the Share loop so the recompress_reader_double_count iso2 spec
	 * can force the race window: wedge a reader here (its snapshot
	 * already saw status=PARTIAL via SnapshotSelf above) and let a
	 * racing recompress commit + rename its PAX file.  On resume the
	 * reader takes Share instantly (Ex is released) and would go down
	 * the PAX+heap path with the stale PARTIAL cache — the fix below
	 * catches this by re-reading statuses after Share.
	 */
#ifdef FAULT_INJECTOR
	SIMPLE_FAULT_INJECTOR("ts_chunk_scan_before_share");
#endif

	for (int i = 0; i < state->nchunks; i++)
	{
		int16		status = state->chunk_status[i];

		if (status == TS_CHUNK_ACTIVE || status == TS_CHUNK_PARTIAL)
			ts_chunk_lock(rel_oid, state->chunk_list[i], ShareLock);
	}

	/*
	 * Recompress-vs-reader double-count fix.  Between load_chunks
	 * above (which reads status via SnapshotSelf, no lock) and the
	 * Share loop just now, a concurrent recompress may have already
	 * committed its status flip PARTIAL → COMPRESSED AND replaced the
	 * PAX file at seg_path via rename(2).  Using the stale PARTIAL
	 * status would send this scan down the PAX + heap path — but PAX
	 * now already contains the row that recompress merged in from the
	 * heap fork, AND the heap fork still holds the same row (reclaim
	 * is fenced behind our Share).  Result: that row is counted twice
	 * for the whole rest of this scan — mat > src, and refresh alone
	 * cannot re-heal because compress emits no L1.
	 *
	 * Re-reading statuses under the Share barrier catches the
	 * transition: chunks that flipped to COMPRESSED here take the
	 * PAX-only path further down, so the double-count is avoided.
	 * SnapshotSelf semantics are the same as the first read; only
	 * the timing changes.
	 *
	 * ts_chunk_catalog_get_chunks_with_status is idx-scan over
	 * ts_chunk sorted by (table_oid, chunk_number), so both calls
	 * return the same order when the chunk range is unchanged — a
	 * plain memcpy over `state->chunk_status` is safe as long as
	 * the row count matches.  If it doesn't (a rare add/drop
	 * concurrent with our scan begin), we conservatively keep the
	 * original statuses; that leaves a residual window but keeps
	 * the fast path branch-free.
	 */
	if (state->nchunks > 0)
	{
		/*
		 * ChunkAppend subpaths narrow chunk_list/nchunks (above) to a
		 * single chunk, but state->min_chunk/max_chunk still span the
		 * whole parent WHERE window (ts_build_chunkscan_path_for_chunk
		 * passes through the parent's ts_min/ts_max unchanged).  Re-
		 * querying that wide window here would almost always return
		 * fresh.n != state->nchunks (1), so the guard below would never
		 * fire and this scan would silently keep running with the
		 * stale, pre-narrowing status — reopening the exact
		 * PARTIAL->COMPRESSED double-count race this re-read exists to
		 * close.  Narrow the re-query to the single kept chunk so the
		 * row counts line up again.
		 */
		ForkNumber	refetch_min = state->min_chunk;
		ForkNumber	refetch_max = state->max_chunk;
		TSChunkList	fresh;

		if (state->chunk_only_num >= 0)
			refetch_min = refetch_max = state->chunk_list[0];

		fresh = ts_chunk_catalog_get_chunks_with_status(
									rel_oid, refetch_min, refetch_max);

		if (fresh.n == state->nchunks && fresh.statuses != NULL)
			memcpy(state->chunk_status, fresh.statuses,
				   sizeof(int16) * state->nchunks);
		if (fresh.chunks != NULL)
			pfree(fresh.chunks);
		if (fresh.statuses != NULL)
			pfree(fresh.statuses);
	}

	/*
	 * Test-only suspension point.  Placed AFTER the heapread Share
	 * acquisition so the compress_reclaim_reader_race spec can verify
	 * that reclaim's smgrtruncate is correctly fenced: a wedged
	 * reader holds Share, the racing reclaim call must hang on
	 * ExclusiveLock until the reader resumes.  Compiles to nothing
	 * in non-FAULT_INJECTOR builds.
	 */
#ifdef FAULT_INJECTOR
	SIMPLE_FAULT_INJECTOR("ts_chunk_scan_after_status_load");
#endif

	state->cur_chunk_idx = 0;
	state->cur_block = 0;
	state->cur_chunk_nblocks = InvalidBlockNumber;
	state->cur_offset = FirstOffsetNumber;
	state->cur_buffer = InvalidBuffer;
	state->cur_pax_reader = NULL;
	state->pax_slot = MakeTupleTableSlot(RelationGetDescr(state->rel),
										 &TTSOpsVirtual);
	state->count_slot = NULL;
	state->nvistuples = 0;
	state->cur_vis = 0;
	state->bas_strategy = GetAccessStrategy(BAS_BULKREAD);
	state->parallel_shared = NULL;	/* set by InitializeDSM/Worker if parallel */
	state->parallel_first_claim = false;

	ts_chunk_scan_setup_count_or_proj(state, node);
}

/*
 * Per-chunk source selection.
 *
 * The choice of source depends on chunk status:
 *   ACTIVE     → heap only.
 *   COMPRESSED → PAX only.
 *   PARTIAL    → PAX first (rows captured at compress time), then heap
 *                (rows INSERTed afterwards).  pax_exhausted records that
 *                the PAX half is done so the heap half can run.
 */
static inline bool
ts_chunk_scan_use_pax(ChunkScanState *state)
{
	int16	status;

	if (state->chunk_status == NULL)
		return false;

	status = state->chunk_status[state->cur_chunk_idx];
	return status == TS_CHUNK_COMPRESSED ||
		   (status == TS_CHUNK_PARTIAL && !state->pax_exhausted);
}

/*
 * The count-only fast path applies only to fully COMPRESSED chunks:
 * chunk_numrows[i] is the PAX row count, authoritative only when the
 * chunk has no heap-resident new rows.  PARTIAL chunks must fall
 * through to the regular PAX + heap scan.
 */
static inline bool
ts_chunk_scan_count_only_applies(ChunkScanState *state)
{
	return state->count_only &&
		   state->chunk_numrows != NULL &&
		   state->chunk_status[state->cur_chunk_idx] == TS_CHUNK_COMPRESSED;
}

/*
 * ts_chunk_scan_advance
 *		Move to the next chunk and reset all per-chunk iteration state.
 *		Caller does the `continue` in the outer loop.
 */
static inline void
ts_chunk_scan_advance(ChunkScanState *state)
{
	/*
	 * If the previous chunk's heap fork was interrupted mid-page (e.g.,
	 * LIMIT clause stopped consuming), the per-page buffer may still
	 * hold both a pin AND a content lock — we acquire BUFFER_LOCK_SHARE
	 * when the buffer first comes in and don't release it until we
	 * exhaust the page in the inner loop below.  Use UnlockReleaseBuffer
	 * to drop both atomically; ReleaseBuffer alone would assert in
	 * UnpinBuffer when the lock is still held.
	 */
	if (BufferIsValid(state->cur_buffer))
	{
		UnlockReleaseBuffer(state->cur_buffer);
		state->cur_buffer = InvalidBuffer;
	}
	/*
	 * Parallel mode: claim the next chunk via DSM atomic counter; serial:
	 * just bump the local index.  When the atomic returns >= nchunks the
	 * unordered exec loop sees `cur_chunk_idx >= nchunks` and exits, which
	 * is exactly the same termination condition as serial mode.
	 */
	if (state->parallel_shared != NULL)
		state->cur_chunk_idx = (int)
			pg_atomic_fetch_add_u32(&state->parallel_shared->next_chunk_idx, 1);
	else
		state->cur_chunk_idx++;
	state->count_emitted = 0;
	state->cur_block = 0;
	state->cur_chunk_nblocks = InvalidBlockNumber;
	state->cur_offset = FirstOffsetNumber;
	state->pax_exhausted = false;
	state->nvistuples = 0;
	state->cur_vis = 0;
}

/*
 * ts_chunk_scan_emit_count_only
 *		Emit one synthetic all-NULL row from the current chunk's count
 *		budget.  Returns the count_slot if a row was produced, NULL if
 *		the chunk's target has been reached.
 *
 *		count_slot's tts_isnull was set all-true in setup_count_or_proj;
 *		we just clear EMPTY and report nvalid so upper nodes see a real
 *		row without us having to materialise any columns.  We can't use
 *		scanslot for this because it is BufferHeapTuple (for the heap
 *		fetch fast path), and BufferHeapTuple slot ops don't tolerate
 *		manual flag manipulation.
 */
static TupleTableSlot *
ts_chunk_scan_emit_count_only(ChunkScanState *state)
{
	int64			target;
	TupleTableSlot *cslot = state->count_slot;

	target = state->chunk_numrows[state->cur_chunk_idx];
	if (state->count_emitted >= target)
		return NULL;

	state->count_emitted++;
	cslot->tts_flags &= ~TTS_FLAG_EMPTY;
	cslot->tts_nvalid = cslot->tts_tupleDescriptor->natts;
	return cslot;
}

/*
 * ts_pax_iter_next / reset_chunk / end
 *		Minimal full-scan PAX iterator for callers outside the CustomScan
 *		path (Table AM scan_getnextslot, ANALYZE sampling, COPY OUT).
 *		See ts_compress.h for the lifecycle contract.
 *
 *		Deliberately does NOT reuse ts_chunk_scan_pax_next: that function
 *		needs PlanState + ExprContext for projection + sparse filter, and
 *		Table AM callers have neither.  Sharing only the underlying PAX
 *		reader API (ts_pax_reader_open / _next / _close) avoids 99% of
 *		the duplication while keeping each path's hot loop tight.
 *
 *		The internal_slot is a virtual slot owned by this iterator so we
 *		don't constrain the caller to provide a PAX-compatible slot type.
 *		One slot is reused across all chunks in a scan; freed in
 *		ts_pax_iter_end.
 */
bool
ts_pax_iter_next(TsPaxIter *iter, Relation rel, ForkNumber chunk,
				 TupleTableSlot *out_slot)
{
	if (iter->reader == NULL)
	{
		char	path[MAXPGPATH];

		snprintf(path, sizeof(path), TS_PAX_SEGFILE_ABS_FMT,
				 DataDir, MyDatabaseId, RelationGetRelid(rel),
				 (int) chunk, GpIdentity.segindex);
		iter->reader = ts_pax_reader_open(path, RelationGetDescr(rel));
		if (iter->reader == NULL)
			return false;	/* no PAX file => nothing to read */

		if (iter->internal_slot == NULL)
			iter->internal_slot = MakeSingleTupleTableSlot(
				RelationGetDescr(rel), &TTSOpsVirtual);
	}

	ExecClearTuple(iter->internal_slot);
	if (!ts_pax_reader_next(iter->reader, iter->internal_slot))
	{
		ts_pax_reader_close(iter->reader);
		iter->reader = NULL;
		return false;
	}

	/*
	 * Copy from the iterator's virtual slot into the caller's slot so the
	 * caller is free to use any slot type (BufferHeapTuple / Virtual /
	 * MinimalTuple).  ExecCopySlot performs the deform+materialise dance;
	 * cost is O(natts) attr copy per row, dominated by PAX decode cost.
	 */
	ExecClearTuple(out_slot);
	slot_getallattrs(iter->internal_slot);
	ExecCopySlot(out_slot, iter->internal_slot);
	return true;
}

void
ts_pax_iter_reset_chunk(TsPaxIter *iter)
{
	if (iter->reader != NULL)
	{
		ts_pax_reader_close(iter->reader);
		iter->reader = NULL;
	}
}

void
ts_pax_iter_end(TsPaxIter *iter)
{
	ts_pax_iter_reset_chunk(iter);
	if (iter->internal_slot != NULL)
	{
		ExecDropSingleTupleTableSlot(iter->internal_slot);
		iter->internal_slot = NULL;
	}
}

/*
 * ts_chunk_scan_pax_next
 *		Read the next visible+qualifying tuple from the current chunk's
 *		PAX file.  Opens the reader lazily on first call.  Returns the
 *		ready-to-return slot (projected if a projection is configured),
 *		or NULL when PAX is exhausted (also closes the reader as a side
 *		effect).
 */
static TupleTableSlot *
ts_chunk_scan_pax_next(ChunkScanState *state, CustomScanState *node,
					   ProjectionInfo *projInfo, ExprContext *econtext)
{
	TupleTableSlot *pslot;

	if (state->cur_pax_reader == NULL)
	{
		ForkNumber	forknum = state->chunk_list[state->cur_chunk_idx];
		char		path[MAXPGPATH];
		List	   *frozen_qual;

		snprintf(path, sizeof(path), TS_PAX_SEGFILE_ABS_FMT,
				 DataDir, MyDatabaseId, RelationGetRelid(state->rel),
				 (int) forknum, GpIdentity.segindex);
		/*
		 * Pass WHERE clauses to PAX's sparse filter so it can skip groups
		 * whose per-column min/max can't satisfy the predicate.  Bound
		 * Params (NestedLoop param refs, PREPARE/EXECUTE values) are
		 * frozen into Const nodes first — PAX's sparse-filter walker
		 * doesn't recognise T_Param and would otherwise treat the
		 * containing OpExpr as un-prunable.
		 */
		frozen_qual = ts_freeze_quals_for_sparse_filter(node->ss.ps.plan->qual, econtext);
		state->cur_pax_reader = ts_pax_reader_open_filtered(path, state->rel,
															state->proj_bitmap, frozen_qual);
	}

	if (state->cur_pax_reader == NULL)
		return NULL;

	pslot = state->pax_slot;

	while (true)
	{
		ExecClearTuple(pslot);
		if (!ts_pax_reader_next(state->cur_pax_reader, pslot))
			break;

		econtext->ecxt_scantuple = pslot;
		if (state->recheck_quals != NULL &&
			!ExecQual(state->recheck_quals, econtext))
			continue;

		return projInfo ? ExecProject(projInfo) : pslot;
	}

	ts_pax_reader_close(state->cur_pax_reader);
	state->cur_pax_reader = NULL;
	return NULL;
}

/*
 * ts_chunk_scan_heap_next
 *		Read the next visible+qualifying tuple from the current chunk's
 *		heap fork, resuming from (cur_block, cur_offset).  Returns the
 *		ready-to-return slot (projected if a projection is configured),
 *		or NULL when the fork is exhausted.
 *
 *		Buffer is pinned + share-locked **once per page** in
 *		state->cur_buffer and held across rows on that page.  This
 *		mirrors PG's standard heap_getnextslot pattern and avoids the
 *		per-row ReadBuffer/LockBuffer/UnlockBuffer churn that previously
 *		dominated GetPrivateRefCountEntry / Pin/UnpinBuffer / LWLock in
 *		perf profiles (~6% of CPU on uncompressed scans).
 *
 *		The buffer stays pinned through ExecQual / ExecProject; share
 *		lock is compatible with itself so subqueries / deferred functions
 *		re-entering the buffer manager don't deadlock.  Buffer is
 *		released only at:
 *		  - page boundary (this function, when cur_offset > maxoff)
 *		  - chunk transition (ts_chunk_scan_advance below)
 *		  - end-scan / rescan (existing callbacks)
 */
static TupleTableSlot *
ts_chunk_scan_heap_next(ChunkScanState *state, TupleTableSlot *scanslot,
						ProjectionInfo *projInfo, ExprContext *econtext,
						SMgrRelation smgr, ForkNumber forknum)
{
	/*
	 * Cache the fork's block count once per chunk.  smgrnblocks has a
	 * per-fork in-memory cache (smgr_cached_nblocks[]), but in practice
	 * we observed it missing repeatedly for our extension forks, dropping
	 * into mdnblocks → FileSize → lseek (~5% of count(*) CPU).  Storing
	 * the value in chunk-scoped state eliminates the per-tuple call.
	 * Reset to InvalidBlockNumber on chunk advance / scan start.
	 */
	if (state->cur_chunk_nblocks == InvalidBlockNumber)
	{
		state->cur_chunk_nblocks = smgrnblocks(smgr, forknum);
	}

	while (state->cur_block < state->cur_chunk_nblocks)
	{
		Page		page;
		int			maxoff;

		/* Acquire the page buffer once; reuse across rows in this page. */
		if (!BufferIsValid(state->cur_buffer))
		{
			state->cur_buffer = ReadBufferExtended(state->rel, forknum,
												   state->cur_block,
												   RBM_NORMAL, NULL);
			LockBuffer(state->cur_buffer, BUFFER_LOCK_SHARE);
			state->cur_offset = FirstOffsetNumber;
		}
		page = BufferGetPage(state->cur_buffer);
		maxoff = PageGetMaxOffsetNumber(page);

		while (state->cur_offset <= maxoff)
		{
			ItemId			itemid;
			HeapTupleData	loctup;
			HeapTuple		copytup;
			bool			visible;

			CHECK_FOR_INTERRUPTS();

			itemid = PageGetItemId(page, state->cur_offset);
			state->cur_offset++;

			if (!ItemIdIsNormal(itemid))
				continue;

			loctup.t_tableOid = RelationGetRelid(state->rel);
			loctup.t_data = (HeapTupleHeader) PageGetItem(page, itemid);
			loctup.t_len = ItemIdGetLength(itemid);
			ItemPointerSet(&loctup.t_self, state->cur_block, state->cur_offset - 1);

			visible = HeapTupleSatisfiesVisibility(state->rel, &loctup,
												   state->snapshot,
												   state->cur_buffer);

			if (!visible)
				continue;

			/*
			 * Copy out of the buffer because the upper executor may keep
			 * the slot alive past the next page; pinning the buffer for
			 * arbitrary callers would defeat the per-page hold pattern.
			 * heap_copytuple is a single palloc+memcpy; cheap relative to
			 * what we save by not re-acquiring the buffer per row.
			 */
			copytup = heap_copytuple(&loctup);
			scanslot->tts_tableOid = RelationGetRelid(state->rel);
			/*
			 * Stash t_self before storing: ExecForceStoreHeapTuple with
			 * shouldFree=true pfrees `copytup` after deforming, so reading
			 * `copytup->t_self` afterwards is use-after-free.  loctup is
			 * stack-allocated above so its t_self is still valid.
			 */
			{
				ItemPointerData	tid = loctup.t_self;

				ExecForceStoreHeapTuple(copytup, scanslot, true);
				/*
				 * ExecForceStoreHeapTuple's Virtual-slot path calls
				 * ExecClearTuple → tts_virtual_clear which resets tts_tid
				 * to Invalid and never restores it from the source tuple's
				 * t_self.  Copy the raw (block, offnum) back so SELECT
				 * ctid shows the row's fork-local position instead of
				 * (InvalidBlock, 0).  Cross-chunk collisions are expected
				 * (each chunk's fork is independently numbered from 0)
				 * and accepted — ctid carries no row-level uniqueness for
				 * time_series tables; DML on this tableam is blocked.
				 */
				scanslot->tts_tid = tid;
			}
			econtext->ecxt_scantuple = scanslot;

			if (state->recheck_quals != NULL &&
				!ExecQual(state->recheck_quals, econtext))
			{
				ExecClearTuple(scanslot);
				continue;
			}

			return projInfo ? ExecProject(projInfo) : scanslot;
		}

		/* Page done — release the buffer and advance to the next block. */
		UnlockReleaseBuffer(state->cur_buffer);
		state->cur_buffer = InvalidBuffer;
		state->cur_block++;
		state->cur_offset = FirstOffsetNumber;
	}

	return NULL;
}

/*
 * ts_substitute_params_mutator
 *		Walk an expression tree and replace each Param node with a Const
 *		holding the param's current bound value (from econtext).  Used to
 *		freeze runtime-bound parameters into the qual list before handing
 *		it to PAX's sparse filter — the C++ filter walker only knows about
 *		T_Var / T_Const / T_OpExpr / T_ScalarArrayOpExpr / T_NullTest /
 *		T_FuncExpr / T_BoolExpr, so a bare Param would just be ignored
 *		(filter falls back to "can't prune") instead of pruning stripes.
 *
 *		Two param kinds we care about:
 *		  PARAM_EXEC  — execution-time param (NestedLoop outer ref,
 *		                init plan output, etc.); value lives in
 *		                econtext->ecxt_param_exec_vals[paramid].
 *		  PARAM_EXTERN — query-time parameter (PREPARE/EXECUTE,
 *		                 protocol-level bind); value lives in
 *		                 econtext->ecxt_param_list_info.
 *
 *		PARAM_SUBLINK / PARAM_MULTIEXPR shouldn't reach exec-time scan
 *		quals (they get rewritten earlier by the planner), so we leave
 *		them alone instead of erroring out.
 */
static Node *
ts_substitute_params_mutator(Node *node, ExprContext *econtext)
{
	if (node == NULL)
		return NULL;

	if (IsA(node, Param))
	{
		Param	   *param = (Param *) node;
		ParamExecData *prm_exec;
		ParamExternData *prm_extern;
		ParamExternData prmdata;
		Datum		value;
		bool		isnull;

		switch (param->paramkind)
		{
			case PARAM_EXEC:
				if (econtext->ecxt_param_exec_vals == NULL)
					return node;	/* not yet bound — leave for ExecQual */
				prm_exec = &econtext->ecxt_param_exec_vals[param->paramid];
				if (prm_exec->execPlan != NULL)
					return node;	/* needs subplan eval — skip */
				value = prm_exec->value;
				isnull = prm_exec->isnull;
				break;

			case PARAM_EXTERN:
				if (econtext->ecxt_param_list_info == NULL)
					return node;
				prm_extern = &econtext->ecxt_param_list_info->params[param->paramid - 1];
				if (!OidIsValid(prm_extern->ptype))
				{
					ParamListInfo paramInfo = econtext->ecxt_param_list_info;
					if (paramInfo->paramFetch != NULL)
						paramInfo->paramFetch(paramInfo, param->paramid,
											  false, &prmdata);
					else
						return node;
					prm_extern = &prmdata;
				}
				value = prm_extern->value;
				isnull = prm_extern->isnull;
				break;

			default:
				return node;
		}

		return (Node *) makeConst(param->paramtype,
								  param->paramtypmod,
								  param->paramcollid,
								  get_typlen(param->paramtype),
								  value,
								  isnull,
								  get_typbyval(param->paramtype));
	}

	return expression_tree_mutator(node,
								   ts_substitute_params_mutator,
								   (void *) econtext);
}

/*
 * ts_freeze_quals_for_sparse_filter
 *		Apply ts_substitute_params_mutator across a List of quals.
 *		Returns a fresh List on success; quals that don't contain Param
 *		nodes are unchanged but copied (cheap because expression_tree_mutator
 *		short-circuits when no substitution happened).
 *
 *		Returns the original list pointer if no Param node was found at
 *		all — saves an allocation in the common all-Const case.
 */
static List *
ts_freeze_quals_for_sparse_filter(List *quals, ExprContext *econtext)
{
	List	   *frozen;

	if (quals == NIL)
		return NIL;

	frozen = (List *) ts_substitute_params_mutator((Node *) quals, econtext);
	return frozen != NULL ? frozen : quals;
}

/*
 * ts_chunk_scan_exec
 *		Return the next visible tuple from the set of qualifying chunks.
 *		For each chunk, dispatch to the right source (PAX / heap / both)
 *		based on chunk status; the per-source helpers do the actual reads.
 */
static TupleTableSlot *
ts_chunk_scan_exec(CustomScanState *node)
{
	ChunkScanState	   *state = (ChunkScanState *) node;
	TupleTableSlot	   *scanslot = node->ss.ss_ScanTupleSlot;
	ProjectionInfo	   *projInfo = node->ss.ps.ps_ProjInfo;
	ExprContext		   *econtext = node->ss.ps.ps_ExprContext;
	SMgrRelation		smgr;

	ExecClearTuple(scanslot);

	RelationOpenSmgr(state->rel);
	smgr = state->rel->rd_smgr;

	/*
	 * Parallel: claim the first chunk lazily, on the executing backend.
	 * Done here rather than in Initialize{DSM,Worker} so we coordinate
	 * with Parallel Append — see parallel_first_claim comment in the
	 * struct.  Serial scans skip this entirely.
	 */
	if (state->parallel_first_claim)
	{
		state->cur_chunk_idx = (int)
			pg_atomic_fetch_add_u32(&state->parallel_shared->next_chunk_idx, 1);
		state->parallel_first_claim = false;
	}

	while (state->cur_chunk_idx < state->nchunks)
	{
		ForkNumber		forknum = state->chunk_list[state->cur_chunk_idx];
		TupleTableSlot *slot;

		/* PAX source: COMPRESSED chunks, or PARTIAL chunks before heap. */
		if (ts_chunk_scan_use_pax(state))
		{
			/* Fast path: emit synthetic empty rows without opening PAX. */
			if (ts_chunk_scan_count_only_applies(state))
			{
				TupleTableSlot *cslot = ts_chunk_scan_emit_count_only(state);

				if (cslot != NULL)
					return cslot;
				ts_chunk_scan_advance(state);
				continue;
			}

			slot = ts_chunk_scan_pax_next(state, node, projInfo, econtext);
			if (slot != NULL)
				return slot;

			/*
			 * PAX exhausted.  PARTIAL falls through to the heap scan in
			 * the same iteration; COMPRESSED moves on to the next chunk.
			 */
			if (state->chunk_status[state->cur_chunk_idx] == TS_CHUNK_PARTIAL)
				state->pax_exhausted = true;
			else
			{
				ts_chunk_scan_advance(state);
				continue;
			}
		}

		/* Heap source: ACTIVE chunks, or PARTIAL chunks after PAX. */
		slot = ts_chunk_scan_heap_next(state, scanslot, projInfo,
									   econtext, smgr, forknum);
		if (slot != NULL)
			return slot;

		ts_chunk_scan_advance(state);
	}

	return ExecClearTuple(scanslot);
}

/*
 * ts_chunk_scan_end
 *		Release any held buffer and free the chunk list.
 */
static void
ts_chunk_scan_end(CustomScanState *node)
{
	ChunkScanState *state = (ChunkScanState *) node;

	if (state->cur_pax_reader != NULL)
	{
		ts_pax_reader_close(state->cur_pax_reader);
		state->cur_pax_reader = NULL;
	}
	if (state->pax_slot != NULL)
	{
		ExecDropSingleTupleTableSlot(state->pax_slot);
		state->pax_slot = NULL;
	}
	if (state->count_slot != NULL)
	{
		ExecDropSingleTupleTableSlot(state->count_slot);
		state->count_slot = NULL;
	}
	if (BufferIsValid(state->cur_buffer))
	{
		UnlockReleaseBuffer(state->cur_buffer);
		state->cur_buffer = InvalidBuffer;
	}
	if (state->bas_strategy != NULL)
	{
		FreeAccessStrategy(state->bas_strategy);
		state->bas_strategy = NULL;
	}
	if (state->chunk_list)
	{
		pfree(state->chunk_list);
		state->chunk_list = NULL;
	}
	if (state->chunk_status)
	{
		pfree(state->chunk_status);
		state->chunk_status = NULL;
	}
	if (state->chunk_numrows)
	{
		pfree(state->chunk_numrows);
		state->chunk_numrows = NULL;
	}
	if (state->proj_bitmap)
	{
		pfree(state->proj_bitmap);
		state->proj_bitmap = NULL;
	}
}

/*
 * ts_chunk_scan_rescan
 *		Reset iteration state to the beginning of the chunk list
 *		so the scan can be re-executed (e.g. for a nested loop).
 */
static void
ts_chunk_scan_rescan(CustomScanState *node)
{
	ChunkScanState *state = (ChunkScanState *) node;

	if (state->cur_pax_reader != NULL)
	{
		ts_pax_reader_close(state->cur_pax_reader);
		state->cur_pax_reader = NULL;
	}
	if (BufferIsValid(state->cur_buffer))
	{
		UnlockReleaseBuffer(state->cur_buffer);
		state->cur_buffer = InvalidBuffer;
	}
	state->cur_chunk_idx = 0;
	state->cur_block = 0;
	state->cur_chunk_nblocks = InvalidBlockNumber;
	state->cur_offset = FirstOffsetNumber;
	state->pax_exhausted = false;
	state->count_emitted = 0;
}

/*
 * ts_chunk_scan_explain
 *		Add ChunkScan-specific properties to EXPLAIN output.
 *
 *		Only the storage type is emitted.  Per-status chunk counts are
 *		intentionally NOT shown:
 *
 *		  - Plan time: surfacing them requires a distributed SPI on
 *		    ts_chunk that dominated planning latency (~100 ms / query).
 *		  - Exec time: ChunkScanState is per-segment-backend state;
 *		    the QD's explain callback can't read it, and the executor
 *		    frame is still active so we can't dispatch a fresh SPI
 *		    ("query plan with multiple segworker groups is not
 *		    supported").
 *
 *		Use `SELECT chunk_number, status FROM time_series.ts_chunk
 *		WHERE table_oid = ...` directly if you need this info.
 */
static void
ts_chunk_scan_explain(CustomScanState *node, List *ancestors, ExplainState *es)
{
	ChunkScanState *state = (ChunkScanState *) node;

	ExplainPropertyText("Storage", "Time-Series Chunk", es);
	/*
	 * When this ChunkScan was generated as a ChunkAppend subpath, surface
	 * the target chunk_num so EXPLAIN can distinguish per-chunk subplans
	 * (which otherwise all read as `Custom Scan (ChunkScan) on cpu`).
	 */
	if (state->chunk_only_num >= 0)
		ExplainPropertyInteger("Chunk", NULL, state->chunk_only_num, es);
}

/*
 *		Parallel DSM callbacks
 *
 *		Pattern mirrors PG's Parallel Seq Scan (nodeSeqscan.c) but at
 *		chunk-list granularity rather than block-level:
 *
 *		  Estimate   → return sizeof(ChunkScanParallelDSM).
 *		  Initialize → leader sets atomic to 0, claims chunk 0 via
 *		               fetch_add (atomic ends at 1).  Subsequent
 *		               workers each claim 1,2,... in InitializeWorker.
 *		               When a backend's claim returns >= nchunks the
 *		               unordered exec loop falls through and EOFs.
 *		  ReInitialize → re-set atomic to 0 and re-claim for leader.
 *		                 Workers are restarted by the framework and
 *		                 hit InitializeWorker again.
 *		  Worker     → attach to DSM, do first claim.
 *
 *		The leader's BeginCustomScan already ran by the time we get
 *		here (it set cur_chunk_idx = 0 from the serial path); we
 *		overwrite it with the parallel claim.
 */
static Size
ts_chunk_scan_estimate_dsm(CustomScanState *node, ParallelContext *pcxt)
{
	return sizeof(ChunkScanParallelDSM);
}

static void
ts_chunk_scan_initialize_dsm(CustomScanState *node, ParallelContext *pcxt,
							 void *coord)
{
	ChunkScanState		   *state = (ChunkScanState *) node;
	ChunkScanParallelDSM   *dsm = (ChunkScanParallelDSM *) coord;

	pg_atomic_init_u32(&dsm->next_chunk_idx, 0);
	state->parallel_shared = dsm;
	state->parallel_first_claim = true;
}

static void
ts_chunk_scan_reinitialize_dsm(CustomScanState *node, ParallelContext *pcxt,
							   void *coord)
{
	ChunkScanState		   *state = (ChunkScanState *) node;
	ChunkScanParallelDSM   *dsm = (ChunkScanParallelDSM *) coord;

	pg_atomic_write_u32(&dsm->next_chunk_idx, 0);
	state->parallel_first_claim = true;
}

static void
ts_chunk_scan_initialize_worker(CustomScanState *node, shm_toc *toc,
								void *coord)
{
	ChunkScanState		   *state = (ChunkScanState *) node;
	ChunkScanParallelDSM   *dsm = (ChunkScanParallelDSM *) coord;

	state->parallel_shared = dsm;
	state->parallel_first_claim = true;
}

/*
 * ts_get_chunk_stats
 *		Read (n_total, n_compressed) from the hypertable's reloptions.
 *		Returns false if reloptions are absent (e.g. ANALYZE has not
 *		been run yet, or table was just loaded) — caller treats absent
 *		stats as frac_comp = 0 (no compressed-side discount).
 */
static bool
ts_get_chunk_stats(Oid relid, int *n_total, int *n_compressed)
{
	HeapTuple	cltup;
	Datum		d;
	bool		isnull = true;
	List	   *opts;
	ListCell   *lc;
	bool		found_any = false;

	*n_total = 0;
	*n_compressed = 0;

	if (Gp_role != GP_ROLE_DISPATCH)
		return false;

	cltup = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(cltup))
		return false;

	d = SysCacheGetAttr(RELOID, cltup, Anum_pg_class_reloptions, &isnull);
	if (isnull)
	{
		ReleaseSysCache(cltup);
		return false;
	}

	opts = untransformRelOptions(d);
	foreach(lc, opts)
	{
		DefElem	*def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "ts_n_total_chunks") == 0)
		{
			*n_total = atoi(defGetString(def));
			found_any = true;
		}
		else if (strcmp(def->defname, "ts_n_compressed_chunks") == 0)
		{
			*n_compressed = atoi(defGetString(def));
			found_any = true;
		}
	}
	list_free_deep(opts);
	ReleaseSysCache(cltup);

	return found_any;
}

/*
 * ts_scan_set_rel_pathlist
 *		Hook into the planner's path-generation phase.  For time_series
 *		relations, extract WHERE-clause time bounds, build a ChunkScan
 *		CustomPath, and **replace** the default SeqScan path so that
 *		EXPLAIN always shows chunk-level diagnostics and pruning is
 *		applied when time predicates are present.
 *
 *		Design note — why we replace rather than compete on cost:
 *		Our storage model is one relation with N forks (one per chunk),
 *		so MAIN_FORKNUM is empty and the standard SeqScan would scan
 *		zero rows.  Wiping rel->pathlist and adding only our CustomPath
 *		is the only way to force the executor onto a path that reads
 *		fork data.  This is intentionally non-standard.
 *
 *		Contrast: TimescaleDB hypertables are PG inheritance parents
 *		with separate child chunk tables, so each child has its own
 *		SeqScan path and TSDB's chunk_append CustomScan node coordinates
 *		them at a layer above.  Our fork-per-chunk design doesn't fit
 *		that pattern — there's no per-chunk RTE for the planner to see.
 */
static void
ts_scan_set_rel_pathlist(PlannerInfo *root, RelOptInfo *rel,
						 Index rti, RangeTblEntry *rte)
{
	Relation	table_rel;
	TSConfig	config;
	Oid			ts_typid;
	int64		ts_min;
	int64		ts_max;
	bool		min_inclusive;
	bool		max_inclusive;
	CustomPath *cpath;
	List	   *priv;
	int			flags;
	Cost		startup_cost;
	Cost		total_cost;
	Path	   *cap;

	/* Chain to previous hook first */
	if (prev_set_rel_pathlist_hook)
		prev_set_rel_pathlist_hook(root, rel, rti, rte);

	/* Only handle base relations with valid relid */
	if (rel->reloptkind != RELOPT_BASEREL ||
		rte->rtekind != RTE_RELATION ||
		rte->relid == InvalidOid)
	{
		return;
	}

	table_rel = table_open(rte->relid, AccessShareLock);

	if (!RelationIsTimeSeries(table_rel))
	{
		table_close(table_rel, AccessShareLock);
		return;
	}

	/* Get ts_config for time bounds extraction */
	if (!ts_get_config(table_rel, &config))
	{
		table_close(table_rel, AccessShareLock);
		return;
	}

	/* Determine type of ts column */
	ts_typid = TupleDescAttr(RelationGetDescr(table_rel), config.ts_attnum - 1)->atttypid;

	table_close(table_rel, AccessShareLock);

	/* Only handle timestamptz for now */
	if (ts_typid != TIMESTAMPTZOID)
		return;

	/* Extract time bounds from WHERE clause */
	ts_extract_time_bounds(root, rel, config.ts_attnum, ts_typid,
						   &ts_min, &min_inclusive, &ts_max, &max_inclusive);

	/* Build custom_private: [min_pair, max_pair, flags] */
	flags = 0;
	if (min_inclusive)
		flags |= TS_BOUND_FLAG_MIN_INCLUSIVE;
	if (max_inclusive)
		flags |= TS_BOUND_FLAG_MAX_INCLUSIVE;

	priv = list_make3(ts_encode_int64(ts_min), ts_encode_int64(ts_max),
					  makeInteger(flags));

	/*
	 * For time_series tables, ChunkScan is the canonical scan path: it shows chunk-level diagnostics
	 * in EXPLAIN and enables chunk pruning when time predicates are present.  We replace the
	 * existing SeqScan path rather than competing on cost, because both scan all chunks via
	 * the same Table AM callbacks — ChunkScan just adds pruning.
	 *
	 * Copy cost from the SeqScan path so planner comparisons with other join/agg paths remain
	 * consistent.
	 */
	if (rel->pathlist != NIL)
	{
		Path	   *seqpath = (Path *) linitial(rel->pathlist);

		startup_cost = seqpath->startup_cost;
		total_cost = seqpath->total_cost;
	}
	else
	{
		startup_cost = 0;
		total_cost = rel->rows * cpu_tuple_cost;
	}

	/* Create CustomPath */
	cpath = makeNode(CustomPath);
	cpath->path.type = T_CustomPath;
	cpath->path.pathtype = T_CustomScan;
	cpath->path.parent = rel;
	cpath->path.pathtarget = rel->reltarget;
	cpath->path.param_info = NULL;
	cpath->path.parallel_aware = false;
	cpath->path.parallel_safe = rel->consider_parallel;
	cpath->path.parallel_workers = 0;
	cpath->path.rows = rel->rows;
	cpath->path.startup_cost = startup_cost;
	cpath->path.total_cost = total_cost;
	cpath->path.pathkeys = NIL;

	/* Set locus from the relation's existing paths */
	if (rel->pathlist != NIL)
	{
		Path *existing = (Path *) linitial(rel->pathlist);

		cpath->path.locus = existing->locus;
	}
	else
		CdbPathLocus_MakeEntry(&cpath->path.locus);

	/*
	 * Do NOT set CUSTOMPATH_PREFER_TABLEAM_SLOT here.  Promoting the scan
	 * output from TTSOpsVirtual to the relation's native heap slot ops
	 * (TTSOpsBufferHeapTuple) cascades into UPDATE's TM_Updated path: on
	 * a concurrent-update conflict the executor now calls
	 * ExecBRUpdateTriggers → GetTupleForTrigger, whose first line is
	 * Assert(RelationIsHeap(relation)).  time_series uses a custom
	 * Table AM so the assertion fires and the segment PANICs (see
	 * cagg_multi_iso UPDATE-on-source step).  Stay on TTSOpsVirtual until
	 * either trigger.c learns the table-am abstraction or this scan path
	 * gates the flag on rel->trigdesc == NULL.
	 */
	cpath->flags = CUSTOMPATH_SUPPORT_BACKWARD_SCAN;
	cpath->custom_paths = NIL;
	cpath->custom_private = priv;

	/*
	 * Always use ts_scan_path_methods.  At execution time it dispatches
	 * per-chunk to heap or PAX based on each chunk's status, so the
	 * heap path correctly serves COMPRESSED chunks too.  A separate
	 * fully-columnar scan path would be a perf optimisation only
	 * worthwhile when every chunk is compressed, and selecting it
	 * requires a distributed status gather during planning that
	 * dominated plan time (~100 ms per plan, see v1 history).  Defer
	 * that optimisation to v1.1 with a cheaper signal.
	 *
	 * EXPLAIN consumers tolerate a 3-element custom_private (no
	 * trailing status counts): they gate on list_length(priv) >= 6
	 * and skip the per-status breakdown when absent.  "Chunks Scanned"
	 * is still reported from execution-time counters.
	 *
	 * Index paths: v1 disables ts_btree, so none reach this hook;
	 * the prior IndexScan-pruning filter is dead code.  Removed.
	 */
	cpath->methods = &ts_scan_path_methods;
	cpath->custom_private = priv;

	/*
	 * SeqScan reads MAIN_FORKNUM which is empty for time_series tables;
	 * drop all existing paths so the executor picks ChunkScan.  Also
	 * drop the partial pathlist — PG's parallel SeqScan paths there
	 * reference MAIN_FORKNUM too.
	 */
	rel->pathlist = NIL;
	rel->partial_pathlist = NIL;

	ts_maybe_add_parallel_chunkscan_path(root, rel, cpath);

	/*
	 * Add the serial ChunkScan path LAST.
	 *
	 * add_path() may pfree(cpath) when it is dominated by a cheaper path
	 * (e.g. an IndexScan on an indexed time_series table — IndexScan cost
	 * is far below ChunkScan's full-scan cost).  The parallel block above
	 * does `memcpy(ppath, cpath, ...)`, and makeNode(CustomPath) there can
	 * reuse cpath's freed chunk, turning the memcpy into a self-copy of
	 * zeroed memory (NULL pathtarget/methods) that later crashes the core
	 * planner in apply_scanjoin_target_to_paths.  So cpath must stay live
	 * until every reader above is done; only then hand it to add_path().
	 */
	add_path(rel, (Path *) cpath, root);

	/*
	 * ChunkAppend candidate path (M1).  Build one Sort+ChunkScan(chunk_only=K)
	 * subpath per chunk_num in the [ts_min, ts_max] window and wrap them in
	 * a ChunkAppend CustomScan with demand-pull early-stop.  Gated by GUC.
	 * Inserted AFTER add_path(cpath) so rel->pathlist has the canonical
	 * partitioned-locus path to copy locus from.
	 */
	cap = ts_chunk_append_try_build_path(root, rel, rti, rte, &config,
										 ts_min, ts_max,
										 min_inclusive, max_inclusive);
	if (cap != NULL)
		add_path(rel, cap, root);

	/*
	 * Parameterized ChunkScan path: register a CustomPath whose param_info
	 * captures the outer relids of join clauses that reference our rel,
	 * so a NestedLoop driver can ship the bound outer value into our scan
	 * qual.  PaxFilter then stripe-prunes by min/max.
	 *
	 *   WHERE tags_id IN (SELECT id FROM tags WHERE hostname=X)
	 *   AND time BETWEEN A AND B
	 *
	 *   HashJoin: ChunkScan(time)         → 244 K rows, 3036 ms
	 *   NestLoop: ChunkScan(time + tags_id=$1) → 360 rows,    33 ms
	 *
	 * Helper does the join-clause harvest, frac_comp gating, cost mixing,
	 * and add_path.
	 */
	ts_maybe_add_param_chunkscan_path(root, rel, rte, priv, startup_cost, total_cost);
}

/*
 * ts_maybe_add_parallel_chunkscan_path
 *		If the relation is parallel-eligible, register a partial-path
 *		copy of `cpath` whose locus carries the worker count and whose
 *		rows / cost are scaled to per-worker accounting.
 *
 *		Eligibility:
 *		  - rel->consider_parallel (set by the planner)
 *		  - max_parallel_workers_per_gather > 0 (GUC; 0 disables)
 *
 *		We can't know nchunks at planner time — the chunk list is
 *		materialised in BeginCustomScan.  Request the GUC cap; at exec
 *		time the atomic chunk-claim counter handles nchunks < workers
 *		(extra backends fetch_add an index ≥ nchunks on their first
 *		call and EOF immediately, costing only the worker-launch
 *		overhead).
 */
static void
ts_maybe_add_parallel_chunkscan_path(PlannerInfo *root, RelOptInfo *rel, CustomPath *cpath)
{
	CustomPath *ppath;
	int			n_workers;
	CdbPathLocus locus;

	if (!rel->consider_parallel || max_parallel_workers_per_gather <= 0)
		return;

	n_workers = max_parallel_workers_per_gather;

	ppath = makeNode(CustomPath);
	memcpy(ppath, cpath, sizeof(CustomPath));
	ppath->path.parallel_aware = true;
	ppath->path.parallel_safe = true;

	/*
	 * CB requires path->parallel_workers == path->locus.parallel_workers
	 * (createplan.c:625 asserts this).  Construct the locus from the baserel's distribution
	 * policy carrying our worker count, then use that locus's parallel_workers as the truth —
	 * the locus may clamp internally.
	 */
	locus = cdbpathlocus_from_baserel(root, rel, n_workers);
	ppath->path.locus = locus;
	ppath->path.parallel_workers = locus.parallel_workers;

	/*
	 * Divide rows + cost so the planner's Gather sees per-worker accounting matching parallel
	 * SeqScan.  +1 accounts for the leader also participating in the scan.
	 */
	ppath->path.rows = cpath->path.rows / (n_workers + 1);
	if (ppath->path.rows < 1)
		ppath->path.rows = 1;
	ppath->path.startup_cost = cpath->path.startup_cost;
	ppath->path.total_cost = cpath->path.startup_cost +
		(cpath->path.total_cost - cpath->path.startup_cost) / (n_workers + 1);

	add_partial_path(rel, (Path *) ppath);
}

/*
 * ts_collect_param_pushdown_clauses
 *		Gather restriction info that should drive a parameterized ChunkScan
 *		path.  Walks two sources:
 *
 *		  1. rel->joininfo — non-equijoin clauses already attached to the
 *		     relation (e.g. tags_id < tags.id).
 *
 *		  2. root->eq_classes — equijoin clauses live in equivalence
 *		     classes; the planner only materialises join RestrictInfos for
 *		     index columns, so for our (PAX/heap) columns we synthesise
 *		     `our_var = other_var` RestrictInfos by hand for every OTHER
 *		     EC member that lives in a different relation.
 *
 *		Returns the union of every outer relid those clauses reference in
 *		*required_outer (caller deletes our own relid from the set).
 */
static void
ts_collect_param_pushdown_clauses(PlannerInfo *root, RelOptInfo *rel,
								  List **pushable_clauses, Relids *required_outer)
{
	ListCell   *lc;
	ListCell   *ec_cell;

	*pushable_clauses = NIL;
	*required_outer = NULL;

	/*
	 * (1) rel->joininfo — clauses already attached to the rel.
	 *
	 * Compress config (segmentby/orderby attnums) is unreliable at planner time — both the
	 * cached and fresh paths into ts_compress_config can return empty in certain MPP/snapshot
	 * states.  Workaround: consider any Var on our relation as a pushdown candidate.  PaxFilter
	 * silently skips columns that lack per-stripe min/max metadata, so the worst case is the
	 * same speed as without parameterization.
	 */
	foreach(lc, rel->joininfo)
	{
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);
		List	   *vars;
		ListCell   *vc;
		bool		references_our_rel = false;

		vars = pull_var_clause((Node *) rinfo->clause,
							   PVC_RECURSE_AGGREGATES | PVC_RECURSE_PLACEHOLDERS);
		foreach(vc, vars)
		{
			Var		   *v = (Var *) lfirst(vc);

			if (IsA(v, Var) && (Index) v->varno == rel->relid)
			{
				references_our_rel = true;
				break;
			}
		}
		list_free(vars);

		if (!references_our_rel)
			continue;

		*pushable_clauses = lappend(*pushable_clauses, rinfo);
		*required_outer = bms_union(*required_outer, rinfo->required_relids);
	}

	/*
	 * (2) root->eq_classes — synthesise join RestrictInfos for ECs that touch our rel.
	 *
	 * `generate_implied_equalities_for_column` would do this cleaner but only consults
	 * rel->eclass_indexes, which is empty here because our columns aren't index columns from
	 * PG's point of view.
	 */
	foreach(ec_cell, root->eq_classes)
	{
		EquivalenceClass *ec = (EquivalenceClass *) lfirst(ec_cell);
		EquivalenceMember *our_em = NULL;
		ListCell   *mc;

		if (ec->ec_has_volatile)
			continue;

		/*
		 * Pick any Var member on our relation — PaxFilter will silently skip columns that lack
		 * per-stripe min/max metadata, so over-inclusive here only costs an extra parameterized
		 * path attempt at plan time, not wrong results.
		 */
		foreach(mc, ec->ec_members)
		{
			EquivalenceMember *em = (EquivalenceMember *) lfirst(mc);
			Var		   *v;

			if (!IsA(em->em_expr, Var))
				continue;
			v = (Var *) em->em_expr;
			if ((Index) v->varno != rel->relid)
				continue;
			our_em = em;
			break;
		}
		if (our_em == NULL)
			continue;

		/*
		 * For every OTHER member in this EC, synthesise a RestrictInfo our_var = other_var.
		 * The other member must reference a different relation (otherwise we'd just be deriving
		 * identities).
		 */
		foreach(mc, ec->ec_members)
		{
			EquivalenceMember *other = (EquivalenceMember *) lfirst(mc);
			RestrictInfo *rinfo;
			Oid			eq_opno;
			Oid			our_type;
			Oid			other_type;
			Expr	   *clause;
			Relids		combined;
			ListCell   *oc;

			if (other == our_em || other->em_is_child ||
				bms_overlap(other->em_relids, rel->relids) ||
				bms_is_empty(other->em_relids))
				continue;

			/*
			 * Build `our_var = other_var`.  Look up the matching "=" operator in the EC's
			 * opfamilies (typically the btree opfamily for the common case).
			 */
			our_type = exprType((Node *) our_em->em_expr);
			other_type = exprType((Node *) other->em_expr);
			eq_opno = InvalidOid;
			foreach(oc, ec->ec_opfamilies)
			{
				eq_opno = get_opfamily_member(lfirst_oid(oc), our_type, other_type,
											  BTEqualStrategyNumber);
				if (OidIsValid(eq_opno))
					break;
			}
			if (!OidIsValid(eq_opno))
				continue;

			clause = make_opclause(eq_opno, BOOLOID, false,
								   (Expr *) copyObject(our_em->em_expr),
								   (Expr *) copyObject(other->em_expr),
								   InvalidOid, ec->ec_collation);
			combined = bms_union(rel->relids, other->em_relids);
			rinfo = make_restrictinfo(root, clause,
									  true,		/* is_pushed_down */
									  false,	/* outerjoin_delayed */
									  false,	/* pseudoconstant */
									  0,		/* security_level */
									  combined,
									  NULL,		/* outer_relids */
									  NULL);	/* nullable_relids */
			*pushable_clauses = lappend(*pushable_clauses, rinfo);
			*required_outer = bms_union(*required_outer, other->em_relids);
		}
	}

	if (*required_outer != NULL)
		*required_outer = bms_del_member(*required_outer, rel->relid);
}

/*
 * ts_compute_param_effective_factor
 *		Blend the qual selectivity with frac_comp to get the cost-discount
 *		factor applied to (total_cost - startup_cost) for the parameterized
 *		path.  See call site for the mixing rationale; in short:
 *
 *		  effective = frac_comp × max(selectivity, 1/64)
 *		            + (1 − frac_comp) × max(selectivity, 0.05)
 *
 *		The compressed side gets the tighter floor because PaxFilter can
 *		stripe-prune; the heap side gets a softer 5% floor because we still
 *		touch every page in the time window regardless of selectivity.
 */
static double
ts_compute_param_effective_factor(double selectivity, double frac_comp)
{
	double		comp_factor;
	double		uncomp_factor;

	if (selectivity < 1.0 / 64.0)
		selectivity = 1.0 / 64.0;

	comp_factor = selectivity;
	uncomp_factor = Max(selectivity, 0.05);
	return frac_comp * comp_factor + (1.0 - frac_comp) * uncomp_factor;
}

/*
 * ts_maybe_add_param_chunkscan_path
 *		If join clauses can be pushed into a parameterized ChunkScan and
 *		the relation is mostly compressed (frac_comp ≥ 0.5), add a
 *		parameterized CustomPath that lets NestedLoop ship outer values
 *		into PaxFilter's stripe-prune.
 *
 *		Gated by frac_comp because heap chunks have no sparse-prune
 *		capability — a NestLoop driver against pure-heap rescans the
 *		full chunk N times and consistently loses to HashJoin.
 */
static void
ts_maybe_add_param_chunkscan_path(PlannerInfo *root, RelOptInfo *rel,
								  RangeTblEntry *rte, List *priv,
								  Cost startup_cost, Cost total_cost)
{
	List	   *pushable_clauses;
	Relids		required_outer;
	CustomPath *param_cpath;
	ParamPathInfo *ppi;
	double		selectivity;
	double		effective_factor;
	double		frac_comp = 0.0;
	double		frac_comp_gate = 0.0;
	Cost		param_total_cost;
	int			n_total = 0;
	int			n_compressed = 0;
	int			n_total_g = 0;
	int			n_compressed_g = 0;

	ts_collect_param_pushdown_clauses(root, rel, &pushable_clauses, &required_outer);

	if (pushable_clauses == NIL || required_outer == NULL || bms_is_empty(required_outer))
		return;

	/*
	 * Gate: skip the parameterized path entirely when the relation is mostly heap.  Heap chunks
	 * have no PaxFilter sparse-prune capability, so a NestLoop driver would rescan the full
	 * chunk N times — strictly worse than HashJoin's single scan + hash probe.  Threshold 0.5
	 * means: only add the param path when at least half the chunks are PAX-compressed.
	 *
	 * The previous behavior (always-add with selectivity floor 0.05) caused multi-host IN
	 * queries on uncompressed tables to regress 6-10× because the planner picked NL × 8 rescans
	 * instead of HashJoin × 1 scan.  Lastpoint LATERAL on pure-heap is already a TIMEOUT (no
	 * index path), so gating doesn't make it worse.
	 */
	if (ts_get_chunk_stats(rte->relid, &n_total_g, &n_compressed_g) && n_total_g > 0)
		frac_comp_gate = (double) n_compressed_g / n_total_g;
	if (frac_comp_gate < 0.5)
		return;

	ppi = get_baserel_parampathinfo(root, rel, required_outer);

	/*
	 * Cost adjustment.  QD-only path: ts_get_chunk_stats dispatches COUNT(*) to segments via
	 * CdbDispatchCommand (cached per session).  On QE or if the GUC is off it returns false and
	 * we leave frac_comp = 0 (no compressed-side discount).
	 */
	if (ts_get_chunk_stats(rte->relid, &n_total, &n_compressed) && n_total > 0)
		frac_comp = (double) n_compressed / n_total;

	selectivity = clauselist_selectivity(root, pushable_clauses, rel->relid,
										 JOIN_INNER, NULL, false);
	effective_factor = ts_compute_param_effective_factor(selectivity, frac_comp);
	param_total_cost = startup_cost + (total_cost - startup_cost) * effective_factor;

	param_cpath = makeNode(CustomPath);
	param_cpath->path.type = T_CustomPath;
	param_cpath->path.pathtype = T_CustomScan;
	param_cpath->path.parent = rel;
	param_cpath->path.pathtarget = rel->reltarget;
	param_cpath->path.param_info = ppi;
	param_cpath->path.parallel_aware = false;
	param_cpath->path.parallel_safe = rel->consider_parallel;
	param_cpath->path.parallel_workers = 0;
	param_cpath->path.rows = ppi->ppi_rows;
	param_cpath->path.startup_cost = startup_cost;
	param_cpath->path.total_cost = param_total_cost;
	param_cpath->path.pathkeys = NIL;

	if (rel->pathlist != NIL)
	{
		Path	   *existing = (Path *) linitial(rel->pathlist);

		param_cpath->path.locus = existing->locus;
	}
	else
		CdbPathLocus_MakeEntry(&param_cpath->path.locus);

	/*
	 * The tableam-slot opt-in (CUSTOMPATH_PREFER_TABLEAM_SLOT) must stay off until UPDATE's
	 * BEFORE-trigger re-fetch path tolerates non-heap relations — see the comment on the
	 * non-parameterized path above.
	 */
	param_cpath->flags = CUSTOMPATH_SUPPORT_BACKWARD_SCAN;
	param_cpath->custom_paths = NIL;
	param_cpath->custom_private = priv;
	param_cpath->methods = &ts_scan_path_methods;

	add_path(rel, (Path *) param_cpath, root);
}

/*
 * ts_scan_scan_init
 *		Register the ChunkScan CustomScan provider with the
 *		executor and set up the path, scan, and
 *		exec method structs.  Must be called from _PG_init().
 */
void
ts_scan_scan_init(void)
{
	/* Register CustomScan methods for plan deserialization */
	ts_scan_methods.CustomName = "ChunkScan";
	ts_scan_methods.CreateCustomScanState = ts_scan_create_state;
	RegisterCustomScanMethods(&ts_scan_methods);

	/* Set up exec methods */
	ts_scan_exec_methods.CustomName = "ChunkScan";
	ts_scan_exec_methods.BeginCustomScan = ts_chunk_scan_begin;
	ts_scan_exec_methods.ExecCustomScan = ts_chunk_scan_exec;
	ts_scan_exec_methods.EndCustomScan = ts_chunk_scan_end;
	ts_scan_exec_methods.ReScanCustomScan = ts_chunk_scan_rescan;
	ts_scan_exec_methods.MarkPosCustomScan = NULL;
	ts_scan_exec_methods.RestrPosCustomScan = NULL;
	ts_scan_exec_methods.EstimateDSMCustomScan = ts_chunk_scan_estimate_dsm;
	ts_scan_exec_methods.InitializeDSMCustomScan = ts_chunk_scan_initialize_dsm;
	ts_scan_exec_methods.ReInitializeDSMCustomScan = ts_chunk_scan_reinitialize_dsm;
	ts_scan_exec_methods.InitializeWorkerCustomScan = ts_chunk_scan_initialize_worker;
	ts_scan_exec_methods.ShutdownCustomScan = NULL;
	ts_scan_exec_methods.ExplainCustomScan = ts_chunk_scan_explain;

	/* Set up path methods */
	ts_scan_path_methods.CustomName = "ChunkScan";
	ts_scan_path_methods.PlanCustomPath = ts_scan_create_plan;
}

/*
 *		planner_hook: temporarily enable nestloop for time_series queries
 *
 *		Background — CB inherits Greenplum's enable_nestloop=off default
 *		(MPP-era guard against cross-segment NL data redistribution).
 *		Our parameterized ChunkScan path *requires* a Nested Loop driver
 *		to push the outer-side join key into PaxFilter as a Const at
 *		runtime (see ts_freeze_quals_for_sparse_filter).  When NL is
 *		globally disabled, the planner adds disable_cost (1e10) to every
 *		NL path, so HashJoin always wins despite our parameterized scan
 *		being 60x cheaper.  Selective tag-based queries then degrade
 *		~100x because PAX stripe pruning never gets a chance to apply.
 *
 *		Strategy — walk the parse tree; if any RTE references a
 *		time_series relation, flip enable_nestloop=on for the duration
 *		of this planner call only.  PG_TRY restores on error.  Non-
 *		time_series queries see no behavior change.
 */

static bool
ts_planner_rte_walker(Node *node, void *context)
{
	bool	   *found = (bool *) context;

	if (node == NULL || *found)
		return false;

	if (IsA(node, RangeTblEntry))
	{
		RangeTblEntry *rte = (RangeTblEntry *) node;

		if (rte->rtekind == RTE_RELATION && OidIsValid(rte->relid))
		{
			/*
			 * The parser already acquired AccessShareLock during
			 * parse-analyze, so this open is a cheap refcount bump.
			 * noWait=true protects against concurrent DROP races.
			 */
			Relation	rel = try_table_open(rte->relid, AccessShareLock, true);

			if (rel != NULL)
			{
				bool		is_ts = RelationIsTimeSeries(rel);

				table_close(rel, AccessShareLock);
				if (is_ts)
				{
					*found = true;
					return true;	/* short-circuit walk */
				}
			}
		}
		return false;
	}

	if (IsA(node, Query))
		return query_tree_walker((Query *) node,
								 ts_planner_rte_walker,
								 context,
								 QTW_EXAMINE_RTES_BEFORE);

	return expression_tree_walker(node, ts_planner_rte_walker, context);
}

static bool
ts_query_references_timeseries(Query *parse)
{
	bool		found = false;

	if (parse == NULL)
		return false;

	(void) query_tree_walker(parse,
							 ts_planner_rte_walker,
							 &found,
							 QTW_EXAMINE_RTES_BEFORE);
	return found;
}

static PlannedStmt *
ts_planner_hook(Query *parse, const char *query_string, int cursorOptions,
				ParamListInfo boundParams,
				OptimizerOptions *optimizer_options)
{
	PlannedStmt *result;
	int			save_nestlevel = -1;

	/*
	 * Use GUC nesting (NewGUCNestLevel/AtEOXact_GUC) rather than directly
	 * writing the enable_nestloop C variable.  This is the idiomatic PG
	 * pattern (pg_hint_plan, postgres_fdw, brin do the same): GUC stack
	 * is maintained, assign hooks fire normally, subtransaction rollback
	 * is respected, and SHOW reports a coherent value at all times.
	 */
	if (!enable_nestloop && ts_query_references_timeseries(parse))
	{
		save_nestlevel = NewGUCNestLevel();
		/*
		 * GUC_ACTION_SAVE (not GUC_ACTION_SET): only the SAVE form is
		 * rolled back by AtEOXact_GUC on commit.  Plain SetConfigOption
		 * uses ACTION_SET and would leak the override past this planner
		 * call.  Pattern matches pg_hint_plan / postgres_fdw.
		 */
		(void) set_config_option("enable_nestloop", "on",
								 PGC_USERSET, PGC_S_SESSION,
								 GUC_ACTION_SAVE, true, 0, false);
	}

	PG_TRY();
	{
		if (prev_planner_hook)
			result = prev_planner_hook(parse, query_string, cursorOptions,
									   boundParams, optimizer_options);
		else
			result = standard_planner(parse, query_string, cursorOptions,
									  boundParams, optimizer_options);
	}
	PG_CATCH();
	{
		if (save_nestlevel >= 0)
			AtEOXact_GUC(true, save_nestlevel);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (save_nestlevel >= 0)
		AtEOXact_GUC(true, save_nestlevel);

	return result;
}

/*
 * ts_scan_pathlist_init
 *		Install the set_rel_pathlist hook that injects ChunkScan chunk-pruning
 *		paths for time_series relations.  Called from _PG_init().
 */
void
ts_scan_pathlist_init(void)
{
	prev_set_rel_pathlist_hook = set_rel_pathlist_hook;
	set_rel_pathlist_hook = ts_scan_set_rel_pathlist;
}

/*
 * ts_build_chunkscan_path_for_chunk
 *		Build a ChunkScan CustomPath that scans only the given chunk_num.
 *		Used by ChunkAppend to construct per-chunk subpaths.
 *
 *		Encodes the time bounds + flags identically to the canonical
 *		ChunkScan generator above, plus an additional 4th element holding
 *		chunk_only_num.  The BeginCustomScan filter at runtime collapses
 *		the chunk_list to just that chunk.
 *
 *		Caller supplies the WHERE-derived ts_min/ts_max so the
 *		per-tuple time filter still runs (the chunk pruning narrows the
 *		scope to one chunk, but rows whose time falls outside the
 *		user-supplied range still get filtered out).
 *
 *		Returns NULL if the rel isn't time_series or no config exists.
 */
CustomPath *
ts_build_chunkscan_path_for_chunk(PlannerInfo *root, RelOptInfo *rel,
								  RangeTblEntry *rte, int32 chunk_num,
								  int64 ts_min, int64 ts_max,
								  bool min_inclusive, bool max_inclusive,
								  double rows_estimate, Cost startup_cost,
								  Cost total_cost)
{
	CustomPath *cpath;
	List	   *priv;
	int			flags = 0;

	if (min_inclusive)
		flags |= TS_BOUND_FLAG_MIN_INCLUSIVE;
	if (max_inclusive)
		flags |= TS_BOUND_FLAG_MAX_INCLUSIVE;

	priv = list_make4(ts_encode_int64(ts_min), ts_encode_int64(ts_max),
					  makeInteger(flags), makeInteger(chunk_num));

	cpath = makeNode(CustomPath);
	cpath->path.type = T_CustomPath;
	cpath->path.pathtype = T_CustomScan;
	cpath->path.parent = rel;
	cpath->path.pathtarget = rel->reltarget;
	cpath->path.param_info = NULL;
	cpath->path.parallel_aware = false;
	cpath->path.parallel_safe = false;	/* parallel disabled for subpaths */
	cpath->path.parallel_workers = 0;
	cpath->path.rows = rows_estimate;
	cpath->path.startup_cost = startup_cost;
	cpath->path.total_cost = total_cost;
	cpath->path.pathkeys = NIL;
	if (rel->pathlist != NIL)
		cpath->path.locus = ((Path *) linitial(rel->pathlist))->locus;
	else
		CdbPathLocus_MakeEntry(&cpath->path.locus);
	cpath->flags = CUSTOMPATH_SUPPORT_BACKWARD_SCAN;
	cpath->custom_paths = NIL;
	cpath->custom_private = priv;
	cpath->methods = &ts_scan_path_methods;

	return cpath;
}

/*
 * ts_scan_planner_init
 *		Install the planner_hook that locally enables nestloop for
 *		queries touching time_series tables.  Called from _PG_init().
 */
void
ts_scan_planner_init(void)
{
	prev_planner_hook = planner_hook;
	planner_hook = ts_planner_hook;
}
