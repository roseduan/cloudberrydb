/*-------------------------------------------------------------------------
 *
 * execMain.c
 *	  top level executor interface routines and generate arrow expression tree.
 *
 * Copyright (c) 2016-Present Hashdata, Inc. 
 *
 * IDENTIFICATION
 *	  src/backend/vecexecutor/execMain.c
 *
 *-------------------------------------------------------------------------
 */
#include "arrow-glib/record-batch.h"
#include "arrow-dataset-glib/scanner.h"
#include "arrow-glib/compute.h"
#include "postgres.h"

#include "catalog/pg_aggregate_d.h"
#include "catalog/pg_operator_d.h"
#include "catalog/pg_tablespace_d.h"
#include "cdb/cdbplan.h"
#include "cdb/cdbvars.h"
#include "common/int.h"
#include "nodes/nodeFuncs.h"
#include "nodes/makefuncs.h"
#include "utils/fmgroids.h"
#include "parser/parsetree.h"
#include "nodes/nodes.h"
#include "parser/scansup.h"
#include "optimizer/optimizer.h"
#include "utils/arrow_sort_options_vec.h"
#include "utils/fmgr_vec.h"
#include "utils/lsyscache.h"
#include "utils/tuptable_vec.h"
#include "utils/vecfuncs.h"
#include "utils/vecsort.h"
#include "executor/nodeSubplan.h"
#include "vecexecutor/execslot.h"
#include "vecexecutor/executor.h"
#include "vecexecutor/nodeCustomscan.h"
#include "vecexecutor/vec_motion_direct_send.h"
#include "vecexecutor/vec_topk_bounds.h"
#include "utils/numeric.h"
#include "utils/guc.h"
#include "utils/guc_vec.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "vecexecutor/vec_spill_stats.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "libpq/libpq-be.h"
#include "port/atomics.h"
#include "tcop/tcopprot.h"

#include <dlfcn.h>
#include <poll.h>
#include <time.h>

/* Resolve PaxCanFastFilterC from pax.so at runtime via dlsym to avoid
 * a hard link-time dependency on the pax_storage library. */
typedef bool (*PaxCanFastFilterFn)(Node *qual, TupleDesc desc);
static PaxCanFastFilterFn pax_can_fast_filter_fn = NULL;
static bool pax_can_fast_filter_resolved = false;

static bool
PaxCanFastFilterC(Node *qual, TupleDesc desc)
{
	if (!pax_can_fast_filter_resolved)
	{
		pax_can_fast_filter_fn =
			(PaxCanFastFilterFn) dlsym(RTLD_DEFAULT, "PaxCanFastFilterC");
		pax_can_fast_filter_resolved = true;
	}
	if (pax_can_fast_filter_fn)
		return pax_can_fast_filter_fn(qual, desc);
	return false;  /* pax.so not loaded, treat as non-fast-filterable */
}

typedef struct VecAggInfo
{
	Aggref *aggref;
	WindowFunc *wfunc;
	const char *aname; /* arrow aggregate function name */
	GArrowFunctionOptions *options;
	char inname[NAMEDATALEN + 30];
	char outname[NAMEDATALEN + 30];
} VecAggInfo;

typedef enum
{
	Plain,
	Orderby,
	Selectk,
	Mergesort,
	Consuming,
} SinkType;

typedef enum
{
	IN_MEMORY,
	IN_SINGLE,
	IN_CHUNK,
	IN_SINGLE_UNSAFE,
	IN_CHUNK_UNSAFE
}StoreType;

typedef struct PlanBuildContext
{
	GArrowExecutePlan *plan;
	PlanState *planstate;

	/* Aggregate information */
	List *agginfos;
	AggStrategy aggstrategy;
	const gchar **keys; /* column name of group keys, used by windowagg/windowhashagg also */
	int nkey;           /* number of groups, used by windowagg/windowhashagg also */
	bool ishaving;

	/* windowagg/windowhashagg fields */
	GArrowSortOptions *orderby_sortoption;

	/* schema of plan input batch*/
	GArrowSchema *inputschema;

	/* plan recipe */
	SinkType sinktype;
	int nlimit; /* the "k" of selectk */

	int *map;/* scan mapping col*/

	/* reader */
	GArrowRecordBatchReader *reader;

	/* hashjoin related */
	bool is_hashjoin;
	bool is_hashjoin_after_node;
	bool is_left_schema;
	/* left and right raw schema */
	GArrowSchema *left_in_schema;
	GArrowSchema *right_in_schema;
	/* left and right proj schema */
	GArrowSchema *left_proj_schema;
	GArrowSchema *right_proj_schema;

	GList *left_hashkeys;
	GList *right_hashkeys;

	/*
	 * Build-side (Arrow right) hash-key column names, in hj_InnerHashKeys
	 * order, recorded by build_join_project_options().  For RIGHT_SEMI /
	 * RIGHT_ANTI the join emits only the build side, so a surviving
	 * OUTER_VAR that is a hash key must resolve to the equal-valued build
	 * column; this list lets expr_to_arrow_expression() look that name up
	 * by key position instead of re-deriving it (which would have to
	 * duplicate the Diff-A / joinqual_N naming rules).
	 */
	List *right_key_names;

	/*
	 * Per hash key of the side build_join_project_options() is building:
	 * how its numeric key must be rewritten before hashing, see
	 * plan_numeric_join_keys().  NIL when no key needs it.
	 */
	List *numeric_key_fix;

	/*
	 * Agg group key positions whose numeric values have no fixed scale and
	 * are grouped by their numeric_trim_scale() form, see
	 * plan_numeric_group_keys().
	 */
	Bitmapset *numeric_canon_keys;

	/* nestloopjoin related */
	bool is_nestloopjoin;

	/* is assertop node */
	 bool is_assertop;

	/* CaseTestExpr is case when placeholder */
	Expr *case_test_expr;
	bool is_case_when;
	GArrowExpression *whenexpr;
	GArrowExpression *not_and_whenexpr;
	Oid case_when_type;

	/* sequence related */
	int subplan_index;

	/* AppendNode fields */
	bool is_append;
	int sub_slice;
	int append_filed_index;

	/* MaterialNode fileds */
	bool is_materialize;
	char *store_file;
	StoreType store_type;
	int64_t chunk_size;
	bool cdb_strict;

	/* parallel scan relation schema */
	bool parallel_scan;
	/*
	 * Set by BuildSource() / the ShareScan consumer path when this plan
	 * gets a source driven from the PG side (callback SourceNode or
	 * SharedSourceNode).  Copied to estate->has_callback_source, where
	 * it vetoes push_pipeline execution; see that field for why.
	 */
	bool has_callback_source;
	Index table_oid;
	Index am_oid;
	GArrowSchema* relation_schema;

	/*
	 * Identity-project elimination.  build_project_options() records here
	 * whether every projection expression is a bare passthrough (its string
	 * form equals its output name, which in arrow is true exactly for
	 * field_ref) together with those names.  BuildProject() then compares
	 * them against the schema of the node the project would sit on -- which
	 * is only known after the agg/limit/filter blocks have moved `current`.
	 */
	bool proj_all_passthrough;
	const gchar **proj_names;
	gsize proj_length;

	/* ShareScanNode fields */
	bool is_sharescan;
	int gp_session_id;
	int gp_command_count;
	int share_id;
	int num_slices;
	int is_cross_slice;
	bool ready;
	bool is_producer;

	/* sonic motion direct-send: set true after BuildAggregatation when the
	 * downstream Acero schema carries a leading hidden target-segment
	 * column (named by sonic_target_segment_col_name) that must survive
	 * the post-Agg project node. */
	bool sonic_target_segment_passthrough;
	const char *sonic_target_segment_col_name;
} PlanBuildContext;

typedef struct SortKey
{
	GArrowSortOrder *orders;
	GArrowSortOrder *nulls_first;
} SortKey;

GArrowSchema *dummy_schema = NULL;
static inline VecAggInfo* new_agg_info(Aggref *aggref, int aggno, PlanBuildContext *pcontext);
static inline VecAggInfo* new_winagg_info(WindowFunc *wfunc, PlanBuildContext *pcontext);
static GArrowExpression *expr_to_arrow_expression(Expr *node, PlanBuildContext *pcontext);
static const char* get_agg_func_name(Aggref *aggref, const AggFuncTable *table, PlanBuildContext *pcontext);
static void CleanupArrowSpillFiles(int code, Datum arg);
static GArrowExecuteNode* BuildSource(PlanBuildContext *pcontext);
static GArrowExecuteNode* BuildScanNode(PlanBuildContext *pcontext);
static GArrowExecuteNode *BuildProject(List *targetList, List *qualList, GArrowExecuteNode *input, PlanBuildContext *pcontext);
static void BuildSink(GArrowExecuteNode *input, VecExecuteState *estate, PlanBuildContext *pcontext);
static GArrowExecuteNode *BuildAggregatation(List *aggInfos, GArrowExecuteNode *input, PlanBuildContext *pcontext);
static void *get_scan_next_batch(PlanState *node);
static void *get_foreign_next_batch(PlanState *node);
static void *get_current_next_batch(PlanState *node);
static GList* build_sort_keys(PlanState *planstate, GArrowSchema *schema);
static GArrowProjectNodeOptions* build_project_options(List *targetList, PlanBuildContext *pcontext);
static bool ProjectIsIdentity(GArrowExecuteNode *input, PlanBuildContext *pcontext);
/*
 * Why a child could not be spliced into its parent's Arrow plan.  Only used to
 * attribute the un-merged boundaries in the Debug_print_plan tally, so the next
 * round of work can be aimed at whichever reason actually dominates.
 */
typedef enum ChildMergeVerdict
{
	CHILD_MERGEABLE = 0,
	CHILD_NO_ESTATE,			/* child was never vectorized */
	CHILD_NO_PLAN,				/* vectorized but built no Arrow plan */
	CHILD_DEFERRED,				/* build postponed to first ExecuteVecPlan */
	CHILD_XSLICE_SHARESCAN		/* cross-slice ShareInputScan handshake */
} ChildMergeVerdict;

static bool ChildMergeable(PlanState *child_ps, VecExecuteState *child_estate);
static ChildMergeVerdict ChildMergeVerdictOf(PlanState *child_ps,
											 VecExecuteState *child_estate);
static GArrowProjectNodeOptions* build_agg_project_options(List *targetList, List *aggInfos, PlanBuildContext *pcontext);
static GArrowProjectNodeOptions* build_windowagg_project_options(List *targetList, List *aggInfos, PlanBuildContext *pcontext);
static GArrowProjectNodeOptions* build_windowhashagg_project_options(List *targetList, List *aggInfos, PlanBuildContext *pcontext);
static GArrowAggregateNodeOptions* build_aggregatation_options(GList *aggregations, PlanBuildContext *pcontext, bool sonic_ok);
static GArrowFilterNodeOptions *build_filter_options(List *filterInfo, PlanBuildContext *pcontext);
static GArrowAssertOpNodeOptions *build_assertop_options(List *filterInfo, PlanBuildContext *pcontext);
static GArrowExecuteNode *BuildHashjoin(PlanBuildContext *pcontext, GArrowExecuteNode *left, GArrowExecuteNode *right, List *joinqual);
static GArrowExecuteNode *BuildJoinProject(List *hashkeys, GArrowExecuteNode *input, PlanBuildContext *pcontext);
static GArrowProjectNodeOptions *build_join_project_options(List *hashkeys, GArrowExecuteNode *input, PlanBuildContext *pcontext);
static void rewrite_tl_keys(List *targetList, PlanBuildContext *pcontext);
static GArrowExpression *build_cast_expression(List* args, PlanBuildContext *pcontext, GArrowDataType *to_type, bool allow_truncate);
static void get_windowagg_sortorder(PlanBuildContext *pcontext, SortKey *sortKey);
static GArrowExecuteNode *BuildNestLoopjoin(PlanBuildContext *pcontext, GArrowExecuteNode *left, GArrowExecuteNode *right, List *joinqual);
static void BuildJoinPlan(PlanBuildContext *pcontext, VecExecuteState *estate);
static void BuildSequencePlan(PlanBuildContext *pcontext, VecExecuteState *estate);
static const char *GetHashJoinProjectName(PlanBuildContext *pcontext, const char *name);
static GArrowExpression *build_is_distinct_expression(DistinctExpr *dex, PlanBuildContext *pcontext);
static GArrowExpression *build_null_if_expression(NullIfExpr *dex, PlanBuildContext *pcontext);
static GArrowExecuteNode*build_orderby_node(PlanState *planstate, GArrowExecutePlan *plan, GArrowExecuteNode *input);
static GArrowExecuteNode*build_topk_node(PlanState *planstate, GArrowExecutePlan *plan, GArrowExecuteNode *input, int64 topk_bound);
static GArrowExecuteNode*build_partition_topk_node(PlanState *planstate, GArrowExecutePlan *plan, GArrowExecuteNode *input);
static GArrowExpression *build_literal_expression(Datum datum, bool isnull, Oid pg_type, int32 typmod);
static GArrowExpression *build_not_expression(GArrowExpression *expr);
static void BuildMaterializePlan(PlanBuildContext *pcontext, VecExecuteState *estate);
static GArrowStoreType to_arrow_storetype(StoreType type);
static void BuildShareScanPlan(PlanBuildContext *pcontext, VecExecuteState *estate);
static bool HasPaxScanInSubtree(PlanState *planstate);
static bool SubtreeHasDatasetScan(PlanState *planstate);
static bool HasSameSliceShareScanInSubtree(PlanState *planstate);
static bool QueryHasSameSliceShareScan(PlanState *planstate);
static bool QueryHasAnyShareInputScan(PlanState *planstate);
static int  CountReceivingMotionsInSlice(PlanState *planstate);
static bool CanUsePushPipeline(PlanBuildContext *pcontext);

static int plan_num = 0;

static char *
build_materialize_file_name()
{
	char *file_name = NULL;
	Oid tblspcOid = InvalidOid;
	char ts_path[PATH_MAX] = { "\0" };

	file_name = palloc0(PATH_MAX);
	tblspcOid = MyDatabaseTableSpace ? MyDatabaseTableSpace : DEFAULTTABLESPACE_OID;
	TempTablespacePath(ts_path, tblspcOid);
	snprintf(file_name, PATH_MAX - 1, "%s/%s_%d_%d_%s.%s.%d", ts_path, VECPATH, gp_session_id, gp_command_count, "vec", "materialize", MyProcPid);
	return file_name;
}

static bool
CanUsePushPipeline(PlanBuildContext *pcontext)
{
	/*
	 * push_pipeline uses Plain SinkNode + PushGenerator backpressure.
	 * Requires thread-safe source nodes so they can run on the thread
	 * pool while the main thread reads from SinkNode's PushGenerator.
	 *
	 * Thread-safe sources:
	 * - ScanNode (PAX): Arrow-native parallel scan, no PG callbacks.
	 * - SharedSourceNode (ShareScan consumer): reads from shared
	 *   memory / unix socket via ConsumePacket(), no PG callbacks.
	 *   Supports PauseProducing/ResumeProducing for backpressure.
	 *
	 * NOT thread-safe: SourceNode callbacks (ExecProcNode uses PG
	 * globals like CurrentMemoryContext). These use pipeline mode
	 * (single-threaded) or batch mode instead.
	 */
	/*
	 * SharedSourceNode (ShareScan consumer) is thread-safe but cannot
	 * use push_pipeline yet: its RunConsumeLoop blocks waiting for the
	 * SharedScan producer to write data.  In push_pipeline, start()
	 * returns immediately (async), but the slice's Motion output may
	 * be needed by other slices — causing cross-slice deadlocks.
	 * Batch mode (start+wait) preserves correct inter-slice ordering.
	 * TODO: enable after implementing Arrow-native Motion operator.
	 */
	/*
	 * Exclude plans that use callback SourceNode instead of Arrow
	 * ScanNode.  HashJoin, NestLoopJoin, Materialize, ShareScan,
	 * and Append plans call BuildSource() which creates a callback
	 * SourceNode that invokes PG's ExecProcNode — not thread-safe
	 * for thread pool execution.  These must use batch mode.
	 */
	if (pcontext->is_hashjoin || pcontext->is_nestloopjoin ||
		pcontext->is_materialize || pcontext->is_sharescan ||
		pcontext->is_append)
		return false;

	return pcontext->parallel_scan && enable_vec_pipeline;
}

/*
 * Check if any child in the plan subtree contains a PAX scan,
 * indicated by push_pipeline=true in its VecExecuteState.
 * Children are built before parents, so their state is ready.
 */
static bool
HasPaxScanInSubtree(PlanState *planstate)
{
	VecExecuteState *vest;

	if (planstate == NULL)
		return false;

	/*
	 * Do not cross Motion nodes — children below Motion execute in
	 * a different process (segment).  Their VecExecuteState reflects
	 * the segment's execution mode, not the coordinator's.
	 */
	if (IsA(planstate, MotionState))
		return false;

	vest = GetVecExecuteState(planstate);
	if (vest && vest->push_pipeline)
		return true;

	return HasPaxScanInSubtree(planstate->lefttree) ||
		HasPaxScanInSubtree(planstate->righttree);
}

/*
 * VEC-SHARESCAN-SUBTREE
 *
 * True if this subtree reads a ShareInputScan whose producer runs in the same
 * slice.  Such a share has no ready handshake to rely on: Arrow's
 * SharedSourceNode::RunConsumeLoop() calls store_->SyncReader() only when
 * is_cross_ is false, and that call merely tests for the .ready file, so the
 * ordering has to come from the producer's plan having already run.  A
 * thread-pool execute context makes the producer start asynchronously, and the
 * consumer then fails with "ShareFileStore::SyncReader: ready file not found".
 *
 * Motion nodes are not crossed, for the same reason as HasPaxScanInSubtree:
 * children below a Motion execute in a different process.
 *
 * Sequence, Append, MergeAppend and SubqueryScan keep their children in a
 * type-specific array or field rather than in lefttree/righttree, so each is
 * descended into explicitly.  This mirrors plan_has_same_slice_share_walker
 * below; the two walkers have to agree, or the VEC-DIAG-CTX log reports a
 * different answer from the one the branch below actually took.
 */
/*
 * SubtreeHasDatasetScan
 *		Will this plan contain an Arrow Dataset ScanNode?
 *
 * BuildSink() needs this to decide whether SinkNode backpressure is safe to
 * arm: such a scan runs inline inside StartProducing() when the plan has no
 * thread pool, so pausing it parks the one thread that could drain the sink
 * and lift the pause (TPC-DS q31/q74/q79).
 *
 * This deliberately does NOT reuse HasPaxScanInSubtree(), whose two properties
 * are both wrong for that question:
 *
 *	- It keys off VecExecuteState.push_pipeline, a mutable flag that
 *	  PostBuildVecPlan() clears on every child it splices.  Children finish
 *	  ExecInitNode() before their parent, so by the time the parent reaches
 *	  BuildSink() the flag is already gone -- while the ScanNode it stood for
 *	  is now physically inside the parent's plan.  Ask the plan shape instead:
 *	  a SeqScan over a relation whose AM is a registered Arrow dataset is
 *	  exactly what BuildScanNode() turns into a ScanNode.
 *
 *	- It descends only lefttree/righttree, so a scan under an Append or a
 *	  Sequence is invisible to it.  Mirror HasSameSliceShareScanInSubtree()
 *	  below, which descends the type-specific child arrays for the same reason.
 *
 * Motion is not crossed: children below it execute in a different process, so
 * their scans are not in this plan.
 */
static bool
SubtreeHasDatasetScan(PlanState *planstate)
{
	int			i;

	if (planstate == NULL)
		return false;

	if (IsA(planstate, MotionState))
		return false;

	if (IsA(planstate, SeqScanState))
	{
		Relation	rel = ((ScanState *) planstate)->ss_currentRelation;

		if (rel != NULL && garrow_dataset_is_registered(rel->rd_rel->relam))
			return true;
	}

	if (IsA(planstate, SequenceState))
	{
		SequenceState *sstate = (SequenceState *) planstate;

		for (i = 0; i < sstate->numSubplans; i++)
			if (SubtreeHasDatasetScan(sstate->subplans[i]))
				return true;
	}

	if (IsA(planstate, AppendState))
	{
		AppendState *astate = (AppendState *) planstate;

		for (i = 0; i < astate->as_nplans; i++)
			if (SubtreeHasDatasetScan(astate->appendplans[i]))
				return true;
	}

	if (IsA(planstate, MergeAppendState))
	{
		MergeAppendState *mstate = (MergeAppendState *) planstate;

		for (i = 0; i < mstate->ms_nplans; i++)
			if (SubtreeHasDatasetScan(mstate->mergeplans[i]))
				return true;
	}

	if (IsA(planstate, SubqueryScanState))
	{
		SubqueryScanState *substate = (SubqueryScanState *) planstate;

		if (SubtreeHasDatasetScan(substate->subplan))
			return true;
	}

	return SubtreeHasDatasetScan(planstate->lefttree) ||
		SubtreeHasDatasetScan(planstate->righttree);
}

static bool
HasSameSliceShareScanInSubtree(PlanState *planstate)
{
	if (planstate == NULL)
		return false;

	if (IsA(planstate, MotionState))
		return false;

	if (IsA(planstate, ShareInputScanState))
	{
		ShareInputScan *sisc = (ShareInputScan *) planstate->plan;

		if (!sisc->cross_slice)
			return true;
	}

	if (IsA(planstate, SequenceState))
	{
		SequenceState  *sstate = (SequenceState *) planstate;
		int				i;

		for (i = 0; i < sstate->numSubplans; i++)
		{
			if (HasSameSliceShareScanInSubtree(sstate->subplans[i]))
				return true;
		}
	}

	if (IsA(planstate, AppendState))
	{
		AppendState	   *astate = (AppendState *) planstate;
		int				i;

		for (i = 0; i < astate->as_nplans; i++)
		{
			if (HasSameSliceShareScanInSubtree(astate->appendplans[i]))
				return true;
		}
	}

	if (IsA(planstate, MergeAppendState))
	{
		MergeAppendState *mstate = (MergeAppendState *) planstate;
		int				i;

		for (i = 0; i < mstate->ms_nplans; i++)
		{
			if (HasSameSliceShareScanInSubtree(mstate->mergeplans[i]))
				return true;
		}
	}

	if (IsA(planstate, SubqueryScanState))
	{
		SubqueryScanState *substate = (SubqueryScanState *) planstate;

		if (HasSameSliceShareScanInSubtree(substate->subplan))
			return true;
	}

	return HasSameSliceShareScanInSubtree(planstate->lefttree) ||
		HasSameSliceShareScanInSubtree(planstate->righttree);
}

/*
 * VEC-SHARESCAN-QUERYWIDE
 *
 * garrow_execute_context_new() does not create a private thread pool: it
 * takes the process-global CPU pool and calls SetCapacity(nthreads) on it
 * (see the comment at the allocation site below).  The effect is therefore
 * process-wide, not per plan.  A same-slice ShareInputScan needs its
 * producer plan to have run to completion before a consumer starts -- Arrow
 * only tests for the .ready file, it does not wait for it -- and that only
 * holds while the plans run synchronously.
 *
 * Looking down from the node being built is not enough: the nodes inside the
 * producer's own subtree (HashAggregate, HashJoin, SeqScan, ...) see no share
 * scan below them, so they would still take the pool branch and raise the
 * global capacity, which is enough to make the share scan's own plan
 * asynchronous even though that plan uses the default context.  Verified on
 * TPC-DS q39 at SF1000: with the subtree-only test, one QE process logged
 * both POOL_CTX and SYNC plans and the query still failed.
 *
 * So the question has to be asked about the whole query.  The walk is
 * O(plan size) and is not cached: see the commit message for why caching
 * on the raw PlannedStmt pointer is unsafe.
 */
static bool
plan_has_same_slice_share_walker(Plan *plan)
{
	ListCell   *lc;

	if (plan == NULL)
		return false;

	if (IsA(plan, ShareInputScan))
	{
		ShareInputScan *sisc = (ShareInputScan *) plan;

		if (!sisc->cross_slice)
			return true;
	}

	if (IsA(plan, Sequence))
	{
		foreach(lc, ((Sequence *) plan)->subplans)
		{
			if (plan_has_same_slice_share_walker((Plan *) lfirst(lc)))
				return true;
		}
	}

	if (IsA(plan, Append))
	{
		foreach(lc, ((Append *) plan)->appendplans)
		{
			if (plan_has_same_slice_share_walker((Plan *) lfirst(lc)))
				return true;
		}
	}

	if (IsA(plan, MergeAppend))
	{
		foreach(lc, ((MergeAppend *) plan)->mergeplans)
		{
			if (plan_has_same_slice_share_walker((Plan *) lfirst(lc)))
				return true;
		}
	}

	if (IsA(plan, SubqueryScan))
	{
		if (plan_has_same_slice_share_walker(((SubqueryScan *) plan)->subplan))
			return true;
	}

	return plan_has_same_slice_share_walker(plan->lefttree) ||
		plan_has_same_slice_share_walker(plan->righttree);
}

static bool
QueryHasSameSliceShareScan(PlanState *planstate)
{
	PlannedStmt	   *stmt;
	ListCell	   *lc;

	if (planstate == NULL || planstate->state == NULL)
		return false;

	stmt = planstate->state->es_plannedstmt;
	if (stmt == NULL)
		return false;

	if (plan_has_same_slice_share_walker(stmt->planTree))
		return true;

	foreach(lc, stmt->subplans)
	{
		if (plan_has_same_slice_share_walker((Plan *) lfirst(lc)))
			return true;
	}

	return false;
}

/*
 * Does the query contain any ShareInputScan at all, cross-slice included?
 *
 * plan_has_same_slice_share_walker answers a narrower question on purpose --
 * it skips cross_slice share scans because those were safe for the context
 * choice it guards.  They are not safe for yield mode: see the note on
 * VecExecuteState.yield_unsafe.  Same whole-query walk and the same
 * reasoning about why a subtree-only test is not enough.
 */
static bool
plan_has_any_share_walker(Plan *plan)
{
	ListCell   *lc;

	if (plan == NULL)
		return false;

	if (IsA(plan, ShareInputScan))
		return true;

	if (IsA(plan, Sequence))
	{
		foreach(lc, ((Sequence *) plan)->subplans)
		{
			if (plan_has_any_share_walker((Plan *) lfirst(lc)))
				return true;
		}
	}

	if (IsA(plan, Append))
	{
		foreach(lc, ((Append *) plan)->appendplans)
		{
			if (plan_has_any_share_walker((Plan *) lfirst(lc)))
				return true;
		}
	}

	if (IsA(plan, MergeAppend))
	{
		foreach(lc, ((MergeAppend *) plan)->mergeplans)
		{
			if (plan_has_any_share_walker((Plan *) lfirst(lc)))
				return true;
		}
	}

	if (IsA(plan, SubqueryScan))
	{
		if (plan_has_any_share_walker(((SubqueryScan *) plan)->subplan))
			return true;
	}

	return plan_has_any_share_walker(plan->lefttree) ||
		plan_has_any_share_walker(plan->righttree);
}

/*
 * Is `target` a subplan of some Sequence other than its last one?
 *
 * Sequence runs every subplan but the last only for its side effects and
 * throws their rows away, which is where a CTE producer sits.  Same
 * traversal as plan_has_any_share_walker.
 */
static bool
plan_output_discarded_walker(Plan *plan, Plan *target)
{
	ListCell   *lc;

	if (plan == NULL)
		return false;

	if (IsA(plan, Sequence))
	{
		List	   *subplans = ((Sequence *) plan)->subplans;

		foreach(lc, subplans)
		{
			Plan	   *sub = (Plan *) lfirst(lc);

			if (sub == target && lnext(subplans, lc) != NULL)
				return true;
			if (plan_output_discarded_walker(sub, target))
				return true;
		}
	}

	if (IsA(plan, Append))
	{
		foreach(lc, ((Append *) plan)->appendplans)
		{
			if (plan_output_discarded_walker((Plan *) lfirst(lc), target))
				return true;
		}
	}

	if (IsA(plan, MergeAppend))
	{
		foreach(lc, ((MergeAppend *) plan)->mergeplans)
		{
			if (plan_output_discarded_walker((Plan *) lfirst(lc), target))
				return true;
		}
	}

	if (IsA(plan, SubqueryScan))
	{
		if (plan_output_discarded_walker(((SubqueryScan *) plan)->subplan,
										 target))
			return true;
	}

	return plan_output_discarded_walker(plan->lefttree, target) ||
		plan_output_discarded_walker(plan->righttree, target);
}

/*
 * True when nothing reads the rows this plan node returns.
 */
static bool
PlanOutputDiscarded(PlanState *planstate)
{
	PlannedStmt	   *stmt;
	ListCell	   *lc;

	if (planstate == NULL || planstate->state == NULL)
		return false;

	stmt = planstate->state->es_plannedstmt;
	if (stmt == NULL)
		return false;

	if (plan_output_discarded_walker(stmt->planTree, planstate->plan))
		return true;

	foreach(lc, stmt->subplans)
	{
		if (plan_output_discarded_walker((Plan *) lfirst(lc),
										 planstate->plan))
			return true;
	}

	return false;
}

/*
 * How many Motion nodes does the locally executing slice read from?
 *
 * Each one becomes its own Acero SourceNode, and in yield mode a single
 * thread drives them all -- SourceNode::RunYieldLoop blocks inside
 * generator_().result(), which for a Motion source ends up in
 * RecvTupleChunkFromAnyUDPIFC.  Parked on one Motion, the thread is not
 * draining the others; their per-connection receive queues fill, the receiver
 * stops acknowledging, and those senders block in SendChunkUDPIFC waiting for
 * window credit that will not come.  Nothing breaks the cycle.
 *
 * Seen on TPC-DS q58 at SF1000 on PG16.9, intermittently (5 of 6 runs with
 * the default capacity-based flow control, 1 of 6 with loss_advance, which
 * advances on timeout and so usually escapes).  At the hang: slice1 has three
 * Redistribute Motions under it (slices 2, 5 and 8); 32 QEs of slice1 parked
 * in receiveChunksUDPIFC while 32 of slice2 and 14 of slice5 sat in
 * SendChunkUDPIFC's poll().  Kernel side confirmed the data had arrived and
 * nobody was reading it: Send-Q zero everywhere, Recv-Q up to 39 KB.
 *
 * One Motion is fine -- the thread blocks on it and that is the only thing it
 * has to do.  Two or more cannot be interleaved by one thread, so yield mode
 * is not usable for such a slice.  Stops at Motion boundaries: a Motion's own
 * subtree belongs to the child slice, not this one.
 */
static int
CountReceivingMotionsInSlice(PlanState *planstate)
{
	int			n = 0;
	ListCell   *lc;

	if (planstate == NULL)
		return 0;

	if (IsA(planstate, MotionState))
		return 1;				/* its subtree is the child slice's business */

	if (IsA(planstate, AppendState))
	{
		AppendState *a = (AppendState *) planstate;
		for (int i = 0; i < a->as_nplans; i++)
			n += CountReceivingMotionsInSlice(a->appendplans[i]);
	}
	else if (IsA(planstate, MergeAppendState))
	{
		MergeAppendState *m = (MergeAppendState *) planstate;
		for (int i = 0; i < m->ms_nplans; i++)
			n += CountReceivingMotionsInSlice(m->mergeplans[i]);
	}
	else if (IsA(planstate, SequenceState))
	{
		SequenceState *q = (SequenceState *) planstate;
		for (int i = 0; i < q->numSubplans; i++)
			n += CountReceivingMotionsInSlice(q->subplans[i]);
	}
	else if (IsA(planstate, SubqueryScanState))
	{
		n += CountReceivingMotionsInSlice(((SubqueryScanState *) planstate)->subplan);
	}

	foreach(lc, planstate->initPlan)
	{
		SubPlanState *sps = (SubPlanState *) lfirst(lc);
		n += CountReceivingMotionsInSlice(sps->planstate);
	}
	foreach(lc, planstate->subPlan)
	{
		SubPlanState *sps = (SubPlanState *) lfirst(lc);
		n += CountReceivingMotionsInSlice(sps->planstate);
	}

	n += CountReceivingMotionsInSlice(outerPlanState(planstate));
	n += CountReceivingMotionsInSlice(innerPlanState(planstate));
	return n;
}

static bool
QueryHasAnyShareInputScan(PlanState *planstate)
{
	PlannedStmt	   *stmt;
	ListCell	   *lc;

	if (planstate == NULL || planstate->state == NULL)
		return false;

	stmt = planstate->state->es_plannedstmt;
	if (stmt == NULL)
		return false;

	if (plan_has_any_share_walker(stmt->planTree))
		return true;

	foreach(lc, stmt->subplans)
	{
		if (plan_has_any_share_walker((Plan *) lfirst(lc)))
			return true;
	}

	return false;
}

static PlanState *
get_sequence_exact_result_ps(PlanState *plan)
{
	SequenceState *node = castNode(SequenceState, plan);

	PlanState *lastPS = node->subplans[node->numSubplans - 1];

	if (IsA(lastPS, SequenceState))
	{
		return get_sequence_exact_result_ps(lastPS);
	}

	return lastPS;
}

/*
 * Numeric hash keys.
 *
 * A numeric128 value carries its own scale, like PostgreSQL's dscale, so
 * 2.50 and 2.5000 are different bytes although numeric_eq() calls them
 * equal.  The vectorized hash join and hash aggregate hash and memcmp key
 * bytes, so such keys must first be brought to one representation:
 *
 *   - both sides of a join key provably have one scale each (see
 *     numeric_expr_scale): nothing to do when the scales match, otherwise
 *     rescale the smaller one up;
 *   - otherwise strip trailing zeros with numeric_trim_scale().
 *
 * Only the hashed key column is rewritten; the original values still flow
 * to the output, so results keep the representation PostgreSQL shows.
 */
#define NUMERIC_KEY_ASIS	(-1)
#define NUMERIC_KEY_TRIM	(-2)
#define NUMERIC_CANON_KEY_PREFIX	"_numkey_"

/*
 * The scale a numeric typmod fixes, or -1; *precision gets its precision.
 */
int
vec_numeric_typmod_scale(int32 typmod, int *precision)
{
	int			scale;

	if (typmod < (int32) VARHDRSZ)
		return -1;
	/* numeric typmod scale is an 11-bit signed field, see numeric.c */
	scale = (((typmod - VARHDRSZ) & 0x7ff) ^ 1024) - 1024;
	*precision = ((typmod - VARHDRSZ) >> 16) & 0xffff;
	return scale >= 0 ? scale : -1;
}

static int numeric_tlist_scale(Plan *plan, AttrNumber attno, int *precision,
							   int depth);

/*
 * The scale every value of numeric expression expr, evaluated in plan,
 * provably has, or -1 when unknown.  *precision gets the most digits a
 * value can have.
 *
 * A typmod alone is not proof: ORCA gives an Append the output typmods of
 * its first child, so "select n52 union all select n104" claims scale 2
 * for scale-4 values too.  So follow Vars down to where the values are
 * made: a table column, whose typmod PostgreSQL enforced when storing it,
 * or a numeric(p,s) cast, which rounds to scale s.
 */
static int
numeric_expr_scale(Plan *plan, Node *expr, int *precision, int depth)
{
	int32		typmod;

	while (expr && IsA(expr, RelabelType))
		expr = (Node *) ((RelabelType *) expr)->arg;
	if (expr == NULL || plan == NULL || depth > 64)
		return -1;

	if (IsA(expr, Var))
	{
		Var		   *var = (Var *) expr;

		if (var->varattno <= 0)
			return -1;
		if (var->varno == OUTER_VAR)
			return numeric_tlist_scale(outerPlan(plan), var->varattno,
									   precision, depth + 1);
		if (var->varno == INNER_VAR)
			return numeric_tlist_scale(innerPlan(plan), var->varattno,
									   precision, depth + 1);
		if (IS_SPECIAL_VARNO(var->varno))
			return -1;
		if (IsA(plan, SubqueryScan))
			return numeric_tlist_scale(((SubqueryScan *) plan)->subplan,
									   var->varattno, precision, depth + 1);
		/* stored table data; external data (foreign/custom scans) is not */
		if (IsA(plan, SeqScan) || IsA(plan, DynamicSeqScan))
			return vec_numeric_typmod_scale(var->vartypmod, precision);
		return -1;
	}

	if (exprIsLengthCoercion(expr, &typmod))
		return vec_numeric_typmod_scale(typmod, precision);

	return -1;
}

static int
numeric_tlist_scale(Plan *plan, AttrNumber attno, int *precision, int depth)
{
	TargetEntry *tle;

	if (plan == NULL || depth > 64)
		return -1;

	if (IsA(plan, Append) || IsA(plan, MergeAppend))
	{
		List	   *children = IsA(plan, Append) ?
			((Append *) plan)->appendplans : ((MergeAppend *) plan)->mergeplans;
		ListCell   *lc;
		int			scale = -1;

		*precision = 0;
		foreach(lc, children)
		{
			int			p;
			int			s = numeric_tlist_scale((Plan *) lfirst(lc), attno,
												&p, depth + 1);

			if (s < 0 || (foreach_current_index(lc) > 0 && s != scale))
				return -1;
			scale = s;
			*precision = Max(*precision, p);
		}
		return scale;
	}

	tle = get_tle_by_resno(plan->targetlist, attno);
	if (tle == NULL)
		return -1;
	return numeric_expr_scale(plan, (Node *) tle->expr, precision, depth);
}

/*
 * Decide, per hash key pair, how each side's numeric key is rewritten:
 * NUMERIC_KEY_ASIS, NUMERIC_KEY_TRIM, or the scale to rescale it to.
 * Both lists stay NIL when no key needs a rewrite.
 */
static void
plan_numeric_join_keys(Plan *left_parent, List *left_keys,
					   Plan *right_parent, List *right_keys,
					   List **left_fix, List **right_fix)
{
	ListCell   *ll;
	ListCell   *rl;
	List	   *lfix = NIL;
	List	   *rfix = NIL;
	bool		any = false;

	forboth(ll, left_keys, rl, right_keys)
	{
		Node	   *lexpr = (Node *) ((ExprState *) lfirst(ll))->expr;
		Node	   *rexpr = (Node *) ((ExprState *) lfirst(rl))->expr;
		int			lf = NUMERIC_KEY_ASIS;
		int			rf = NUMERIC_KEY_ASIS;

		if (exprType(lexpr) == NUMERICOID && exprType(rexpr) == NUMERICOID)
		{
			int			lp;
			int			rp;
			int			ls = numeric_expr_scale(left_parent, lexpr, &lp, 0);
			int			rs = numeric_expr_scale(right_parent, rexpr, &rp, 0);

			/*
			 * Rescaling up adds digits, and numeric128 silently wraps past
			 * VEC_NUMERIC_MAX_PRECISION of them, so trim when it may not fit.
			 */
			if (ls < 0 || rs < 0)
				lf = rf = NUMERIC_KEY_TRIM;
			else if (ls < rs)
				lf = rs;
			else if (rs < ls)
				rf = ls;
			if ((lf >= 0 && lp + (lf - ls) > VEC_NUMERIC_MAX_PRECISION) ||
				(rf >= 0 && rp + (rf - rs) > VEC_NUMERIC_MAX_PRECISION))
				lf = rf = NUMERIC_KEY_TRIM;
			elog(DEBUG1, "numeric hash join key %d: scales %d/%d, rewrite %d/%d",
				 foreach_current_index(ll), ls, rs, lf, rf);
		}
		any |= (lf != NUMERIC_KEY_ASIS || rf != NUMERIC_KEY_ASIS);
		lfix = lappend_int(lfix, lf);
		rfix = lappend_int(rfix, rf);
	}

	if (!any)
	{
		list_free(lfix);
		list_free(rfix);
		lfix = rfix = NIL;
	}
	*left_fix = lfix;
	*right_fix = rfix;
}

/*
 * Apply a plan_numeric_join_keys() decision to a key expression.  expr is
 * borrowed; returns a new reference.
 */
static GArrowExpression *
fix_numeric_key(GArrowExpression *expr, int fix)
{
	GList	   *arguments = NULL;
	g_autoptr(GArrowRoundOptions) options = NULL;
	GArrowExpression *arg = g_object_ref(expr);
	GArrowExpression *result;

	Assert(fix != NUMERIC_KEY_ASIS);
	arguments = garrow_list_append_ptr(arguments, arg);
	if (fix == NUMERIC_KEY_TRIM)
		result = GARROW_EXPRESSION(garrow_call_expression_new(
					"numeric_trim_scale", arguments, NULL));
	else
	{
		/* scale up only: every value has at most `fix` fractional digits */
		options = garrow_round_options_new();
		garrow_round_options_set(options, GARROW_ROUND_HALF_UP, fix);
		result = GARROW_EXPRESSION(garrow_call_expression_new(
					"round", arguments, GARROW_FUNCTION_OPTIONS(options)));
	}
	garrow_list_free_ptr(&arguments);
	if (!result)
		elog(ERROR, "Failed to new arrow call expression for a numeric hash key.");
	return result;
}

/*
 * Group numeric keys without a fixed scale by their numeric_trim_scale()
 * form.  The trimmed key gets a hidden name (see build_agg_project_options)
 * and the key's own output column becomes min() of the original values: all
 * of a group's values are equal, so min() returns one of the group's own
 * representations, as PostgreSQL returns the group's first value.
 *
 * Returns agginfos with the min() aggregates appended.
 */
static List *
plan_numeric_group_keys(List *agginfos, PlanBuildContext *pcontext)
{
	Agg		   *agg = (Agg *) pcontext->planstate->plan;
	Plan	   *child = outerPlan(agg);

	pcontext->numeric_canon_keys = NULL;
	if (agg->groupingSets != NIL || child == NULL)
		return agginfos;

	for (int i = 0; i < agg->numCols; i++)
	{
		AttrNumber	attno = agg->grpColIdx[i];
		TargetEntry *tle = get_tle_by_resno(child->targetlist, attno);
		Aggref	   *aggref;
		VecAggInfo *agginfo;
		Var		   *var;
		int			precision;

		if (tle == NULL ||
			exprType((Node *) tle->expr) != NUMERICOID ||
			numeric_tlist_scale(child, attno, &precision, 0) >= 0)
			continue;

		var = makeVar(OUTER_VAR, attno, NUMERICOID, -1, InvalidOid, 0);
		aggref = makeNode(Aggref);
		aggref->aggfnoid = F_MIN_NUMERIC;
		aggref->aggtype = NUMERICOID;
		aggref->aggtranstype = NUMERICOID;
		aggref->aggargtypes = list_make1_oid(NUMERICOID);
		aggref->args = list_make1(makeTargetEntry((Expr *) var, 1, NULL, false));
		aggref->aggkind = AGGKIND_NORMAL;
		aggref->aggsplit = AGGSPLIT_SIMPLE;
		/* never equal to a planner-assigned aggno */
		aggref->aggno = -1 - i;
		aggref->location = -1;

		agginfo = new_agg_info(aggref, aggref->aggno, pcontext);
		strlcpy(agginfo->outname,
				GetSchemaName(pcontext->inputschema, attno, pcontext->map),
				sizeof(agginfo->outname));
		agginfos = lappend(agginfos, agginfo);
		pcontext->numeric_canon_keys =
			bms_add_member(pcontext->numeric_canon_keys, i);
		elog(DEBUG1, "numeric group key %d has no fixed scale, grouping trimmed", i);
	}
	return agginfos;
}

static const char *
get_agg_func_name(Aggref *aggref, const AggFuncTable *table, PlanBuildContext *pcontext)
{
	int cols;
	const char *name = NULL;

	cols = list_length(aggref->aggdistinct);

	Assert(table != NULL);

	/* is a hash agg */
	if (pcontext->aggstrategy == AGG_HASHED)
		/* is a hash distinct agg */
		if (cols > 0)
			name = table->hashDistFuncName;
		else
			name = table->hashFuncName;
	else if (cols > 0) /* is a distinct agg */
		name = table->distFuncName;
	else
		name = table->funcName;

	if (!name)
		elog(ERROR, "Cann't find Arrow agg function name.");
	return name;
}

/*
 * New an arrow expression with PG function arguments and arrow function name.
 * such as:
 *  i + j - 1 will generate subtract(add(i, j), 1)
 */
GArrowExpression *
func_args_to_expression(List *args, PlanBuildContext *pcontext, const char* funcname)
{
	ListCell *l;
	GList *arguments = NULL;
	g_autoptr(GArrowExpression)  result_expr = NULL;

	foreach(l, args)
	{
		Expr  *fle = (Expr *) lfirst(l);
		g_autoptr(GArrowExpression) expr = expr_to_arrow_expression(fle, pcontext);

		if (!expr)
		{
			elog(ERROR, "Expression to arrow error.");
			return NULL;
		}

		/* No more than two arguments for arrow function.
		 * Combine the existing two arguments as a new arrow function.
		 */
		if (g_list_length(arguments) >= 2)
		{
			g_autoptr(GArrowExpression) funcexpr = NULL;

			funcexpr = GARROW_EXPRESSION(garrow_call_expression_new(
					funcname, arguments, NULL));
			if (!funcexpr)
					elog(ERROR, "Failed to new arrow call expression %s.", funcname);
			garrow_list_free_ptr(&arguments);
			arguments = garrow_list_append_ptr(arguments, funcexpr);
		}

		arguments = garrow_list_append_ptr(arguments, expr);
	}

	result_expr = GARROW_EXPRESSION(garrow_call_expression_new(
			funcname, arguments, NULL));
	if (!result_expr)
			elog(ERROR, "Failed to new arrow call expression %s.", funcname);
	garrow_list_free_ptr(&arguments);
	return garrow_move_ptr(result_expr);
}

static GArrowExpression *
//FIXME: For round(numeric, int), currently int supports only const cases
build_round_expr(List *args, PlanBuildContext *pcontext, bool has_precision)
{
	ListCell *l = NULL;
	GList *arguments = NULL;
	g_autoptr(GArrowExpression) round_expr = NULL;
	g_autoptr(GArrowRoundOptions) options = NULL;
	int scale = 0;
	int precision = 0;
	foreach (l, args)
	{
		g_autoptr(GArrowExpression) cur_expr = NULL;
		Expr  *expr = (Expr *) lfirst(l);
		if (IsA(expr, Const))
		{
			Const* const_expr = (Const*) expr;
			int numeric_type = const_expr->constbyval ? DatumGetInt32(const_expr->constvalue): const_expr->consttypmod;
			if (has_precision)
			{
				/* check_numeric_adjust() let only a non-negative scale through */
				scale = vec_numeric_typmod_scale(numeric_type, &precision);
				if (scale < 0)
					elog(ERROR, "unexpected numeric typmod %d", numeric_type);
			}
			else
				scale = numeric_type;
			continue;
		}
		cur_expr = expr_to_arrow_expression(expr, pcontext);
		arguments = garrow_list_append_ptr(arguments, cur_expr);
	}
	options = garrow_round_options_new();
	garrow_round_options_set(options,GARROW_ROUND_HALF_UP, scale);
	/* numeric(p,s) must raise overflow like numeric()'s apply_typmod() */
	garrow_round_options_set_precision(options, precision);
	round_expr = GARROW_EXPRESSION(garrow_call_expression_new("round", arguments, GARROW_FUNCTION_OPTIONS(options)));
	garrow_list_free_ptr(&arguments);
	return garrow_move_ptr(round_expr);
}

GArrowExpression *
build_round_with_precision(List *args, PlanBuildContext *pcontext, const char* name)
{
	return build_round_expr(args, pcontext, true);
}

GArrowExpression *
build_round_without_precision(List *args, PlanBuildContext *pcontext, const char* name)
{
	return build_round_expr(args, pcontext, false);
}

GArrowExpression *
build_floor_temporal_expr(List *args, PlanBuildContext *pcontext, const char* funcname)
{
	GList *arguments = NULL;
	g_autoptr(GArrowExpression) floor_temporal_expr = NULL;
	g_autoptr(GArrowRoundTemporalOptions) options = NULL;
	g_autoptr(GError) error = NULL;
	GArrowCalendarUnit unit = GARROW_CALENDAR_UNIT_SECOND;
	gint64 multiple = 1;

	/*
	 * The planner (check_floor_temporal) has already verified that args
	 * has exactly 2 elements, the first being a non-null text Const with
	 * a supported unit string.
	 */
	Const *const_expr = (Const *) linitial(args);
	text *unit_text = DatumGetTextP(const_expr->constvalue);
	char *unit_str = text_to_cstring(unit_text);

	/* Parse unit string to GArrowCalendarUnit */
	if (pg_strcasecmp(unit_str, "nanosecond") == 0 || pg_strcasecmp(unit_str, "nanoseconds") == 0)
		unit = GARROW_CALENDAR_UNIT_NANOSECOND;
	else if (pg_strcasecmp(unit_str, "microsecond") == 0 || pg_strcasecmp(unit_str, "microseconds") == 0)
		unit = GARROW_CALENDAR_UNIT_MICROSECOND;
	else if (pg_strcasecmp(unit_str, "millisecond") == 0 || pg_strcasecmp(unit_str, "milliseconds") == 0)
		unit = GARROW_CALENDAR_UNIT_MILLISECOND;
	else if (pg_strcasecmp(unit_str, "second") == 0 || pg_strcasecmp(unit_str, "seconds") == 0)
		unit = GARROW_CALENDAR_UNIT_SECOND;
	else if (pg_strcasecmp(unit_str, "minute") == 0 || pg_strcasecmp(unit_str, "minutes") == 0)
		unit = GARROW_CALENDAR_UNIT_MINUTE;
	else if (pg_strcasecmp(unit_str, "hour") == 0 || pg_strcasecmp(unit_str, "hours") == 0)
		unit = GARROW_CALENDAR_UNIT_HOUR;
	else if (pg_strcasecmp(unit_str, "day") == 0 || pg_strcasecmp(unit_str, "days") == 0)
		unit = GARROW_CALENDAR_UNIT_DAY;
	else if (pg_strcasecmp(unit_str, "month") == 0 || pg_strcasecmp(unit_str, "months") == 0)
		unit = GARROW_CALENDAR_UNIT_MONTH;
	else if (pg_strcasecmp(unit_str, "quarter") == 0 || pg_strcasecmp(unit_str, "quarters") == 0)
		unit = GARROW_CALENDAR_UNIT_QUARTER;
	else if (pg_strcasecmp(unit_str, "year") == 0 || pg_strcasecmp(unit_str, "years") == 0)
		unit = GARROW_CALENDAR_UNIT_YEAR;
	else
		/* Should not be reached: check_floor_temporal already validated the unit */
		elog(ERROR, "Unsupported calendar unit: %s", unit_str);
	pfree(unit_str);

	/* Second argument is the timestamp data */
	g_autoptr(GArrowExpression) cur_expr =
		expr_to_arrow_expression((Expr *) lsecond(args), pcontext);
	if (!cur_expr)
	{
		elog(ERROR, "Failed to convert timestamp expression to Arrow");
		return NULL;
	}
	arguments = garrow_list_append_ptr(arguments, cur_expr);

	/* Create RoundTemporalOptions with parsed unit and multiple */
	options = garrow_round_temporal_options_new();
	if (!garrow_round_temporal_options_set(options, multiple, unit, &error))
	{
		garrow_list_free_ptr(&arguments);
		elog(ERROR, "Failed to set RoundTemporalOptions: %s",
			 garrow_take_error(&error));
	}

	/* Create the floor_temporal call expression with options */
	floor_temporal_expr = GARROW_EXPRESSION(garrow_call_expression_new(
		"floor_temporal", arguments, GARROW_FUNCTION_OPTIONS(options)));

	if (!floor_temporal_expr)
	{
		garrow_list_free_ptr(&arguments);
		elog(ERROR, "Failed to create floor_temporal expression");
	}

	garrow_list_free_ptr(&arguments);
	return garrow_move_ptr(floor_temporal_expr);
}

GArrowExpression *
build_text_join(List *args, PlanBuildContext *pcontext, const char *name)
{
	ListCell *l = NULL;
	GList *arguments = NULL;
	g_autoptr(GArrowExpression) text_join_expr = NULL;
	// add empty separator to impl text join
	g_autoptr(GArrowDatum) val_datum = NULL;
	g_autoptr(GArrowExpression) li_expr = NULL;
	g_autoptr(GArrowBuffer) empty_buffer = NULL;
	g_autoptr(GArrowScalar) empty_buffer_scalar = NULL;
	foreach (l, args)
	{
		g_autoptr(GArrowExpression) cur_expr = NULL;
		Expr *arg = (Expr *) lfirst(l);
		cur_expr = expr_to_arrow_expression(arg, pcontext);
		arguments = garrow_list_append_ptr(arguments, cur_expr);
	}
	empty_buffer = garrow_buffer_new((const guint8*)(""), 0);
	empty_buffer_scalar =GARROW_SCALAR(garrow_string_scalar_new(empty_buffer));
	val_datum = GARROW_DATUM(garrow_scalar_datum_new(empty_buffer_scalar));
	li_expr = GARROW_EXPRESSION(garrow_literal_expression_new(val_datum));
	arguments = garrow_list_append_ptr(arguments, li_expr);
	text_join_expr = GARROW_EXPRESSION(garrow_call_expression_new("binary_join_element_wise", arguments, NULL));
	garrow_list_free_ptr(&arguments);
	return garrow_move_ptr(text_join_expr);
}

static GArrowExpression *
build_is_in(GArrowExpression *expr, GArrowDatum *value_set,
			GArrowSetLookupNullMatchingBehavior null_matching)
{
	g_autoptr(GArrowSetLookupOptions) options = NULL;
	g_autoptr(GArrowExpression) is_in_expr = NULL;
	g_autoptr(GArrowExpression) list_expr = garrow_copy_ptr(expr);
	GList *arguments = NULL;
	options = garrow_set_lookup_options_new_with_null_matching(value_set,
															   null_matching);
	arguments = garrow_list_append_ptr(arguments, list_expr);
	is_in_expr = GARROW_EXPRESSION(garrow_call_expression_new("is_in", arguments, GARROW_FUNCTION_OPTIONS(options)));
	garrow_list_free_ptr(&arguments);
	return garrow_move_ptr(is_in_expr);
}

static inline GArrowExpression *
func_arg_to_expression(Expr *fle, PlanBuildContext *pcontext, const char* funcname)
{
	g_autoptr(GArrowExpression) expr = expr_to_arrow_expression(fle, pcontext);
	GList		*arguments = NULL;

	if (expr)
	{
		arguments = garrow_list_append_ptr(arguments, expr);
		expr = GARROW_EXPRESSION(garrow_call_expression_new(funcname, arguments, NULL));
		garrow_list_free_ptr(&arguments);
		return garrow_move_ptr(expr);
	}
	else
	{
		elog(ERROR, "Expression to arrow error.");
		return NULL;
	}
}

static GArrowExpression *
get_function_expression(Oid funcOid, PlanBuildContext *pcontext, List* args)
{
	const FuncTable *fmgr = get_arrow_fmgr(funcOid);
	if (!fmgr)
	{
		elog(ERROR, "get_function_expression unrecognized id: %d", funcOid);
	}
	return fmgr->builFunc(args, (void*)pcontext, fmgr->arrowFuncName);
}

GArrowExpression *
build_cast_int4_expression_allow_truncate(List *args, PlanBuildContext *pcontext, const char *name)
{
	g_autoptr(GArrowDataType) to_type = GARROW_DATA_TYPE(garrow_int32_data_type_new());
	return build_cast_expression(args, pcontext, to_type, true);
}

GArrowExpression *
build_cast_int4_expression(List *args, PlanBuildContext *pcontext, const char *name)
{
	g_autoptr(GArrowDataType) to_type = GARROW_DATA_TYPE(garrow_int32_data_type_new());
	return build_cast_expression(args, pcontext, to_type, false);
}

GArrowExpression *
build_cast_int8_expression(List *args, PlanBuildContext *pcontext, const char *name)
{
	g_autoptr(GArrowDataType) to_type = GARROW_DATA_TYPE(garrow_int64_data_type_new());
	return build_cast_expression(args, pcontext, to_type, false);
}

GArrowExpression *
build_cast_float4_expression(List *args, PlanBuildContext *pcontext, const char *name)
{
	g_autoptr(GArrowDataType) to_type = GARROW_DATA_TYPE(garrow_float_data_type_new());
	return build_cast_expression(args, pcontext, to_type, false);
}

GArrowExpression *
build_cast_float8_expression(List *args, PlanBuildContext *pcontext, const char *name)
{
	g_autoptr(GArrowDataType) to_type = GARROW_DATA_TYPE(garrow_double_data_type_new());
	return build_cast_expression(args, pcontext, to_type, false);
}

GArrowExpression *
build_cast_numeric_expression(List *args, PlanBuildContext *pcontext, const char *name)
{
	g_autoptr(GArrowDataType) to_type = GARROW_DATA_TYPE(garrow_numeric128_data_type_new());
	return build_cast_expression(args, pcontext, to_type, false);
}

GArrowExpression *
build_cast_text_expression(List *args, PlanBuildContext *pcontext, const char *name)
{
	g_autoptr(GArrowDataType) to_type = GARROW_DATA_TYPE(garrow_string_data_type_new());
	return build_cast_expression(args, pcontext, to_type, false);
}

static GArrowExpression *
build_cast_expression(List* args, PlanBuildContext *pcontext, GArrowDataType *to_type, bool allow_truncate)
{
	g_autoptr(GArrowExpression) expr = NULL;
	g_autoptr(GList) cast_args = NULL;
	g_autoptr(GArrowExpression) cast_to_expr = NULL;
	g_autoptr(GArrowCastOptions) cast_options = NULL;

	Assert(list_length(args) == 1);
	expr = expr_to_arrow_expression(linitial(args), pcontext);
	cast_args = garrow_list_append_ptr(cast_args, expr);
	cast_options = GARROW_CAST_OPTIONS(g_object_new(GARROW_TYPE_CAST_OPTIONS, "to-data-type", to_type, NULL));
	if (allow_truncate)
		garrow_set_cast_options(cast_options, GARROW_PROP_ALLOW_FLOAT_TRUNCATE, true);
	cast_to_expr = GARROW_EXPRESSION(garrow_call_expression_new("cast", cast_args, GARROW_FUNCTION_OPTIONS(cast_options)));
	garrow_list_free_ptr(&cast_args);
	return garrow_move_ptr(cast_to_expr);
}

/*
 * Convert ScalarArrayOpExpr to GArrowExpression
 *
 * Fixme: Fully support of ScalarArrayOpExpr, only "IN" now.
 */
static GArrowExpression *
scalararray_to_expression(ScalarArrayOpExpr *arrayexpr, PlanBuildContext *pcontext)
{
	Oid			elemtype;
	Const *const_expr;
	Expr *second_expr;
	g_autoptr(GArrowArray) array = NULL;
	g_autoptr(GArrowArrayDatum) array_datum = NULL;
	g_autoptr(GArrowExpression) expr = NULL;
	g_autoptr(GArrowScalar) val_scalar = NULL;
	g_autoptr(GArrowDatum) val_datum = NULL;
	g_autoptr(GArrowDatum) val_array_datum = NULL;
	g_autoptr(GArrowExpression) is_in_expr = NULL;
	bool		is_not_in;

	second_expr = (Expr *) lsecond(arrayexpr->args);
	if (nodeTag(second_expr) != T_Const)
	{
		elog(ERROR, "unrecognized node type: %d", (int) nodeTag(second_expr));
	}

	/*
	 * PG spells `x IN (...)` as `= ANY` (useOr) and `x NOT IN (...)` as
	 * `<> ALL` (!useOr).  is_vectorable_expr() rejects every other shape, so
	 * those are the only two that reach here.
	 */
	is_not_in = !arrayexpr->useOr;
	Assert(!strcmp(get_opname(arrayexpr->opno), is_not_in ? "<>" : "="));
	const_expr = (Const *) second_expr;
	/* first GArrowExpression as is_in argument */
	expr = expr_to_arrow_expression(linitial(arrayexpr->args), pcontext);
	Assert(expr);

	elemtype = get_element_type(const_expr->consttype);
	if (elemtype == InvalidOid)
	{
		elog(ERROR, "is_in right is not an array.");
	}

	val_scalar = ArrowScalarNew(PGTypeToArrowID(const_expr->consttype),
								const_expr->constvalue, const_expr->constisnull, const_expr->consttype, const_expr->consttypmod);

	array = garrow_base_list_scalar_get_value(GARROW_BASE_LIST_SCALAR(val_scalar));
	array_datum = garrow_array_datum_new(array);
	val_array_datum = GARROW_DATUM(array_datum);

	/*
	 * NOT IN needs INCONCLUSIVE, SQL's three-valued set lookup: a null input
	 * yields null, and a null anywhere in the value set makes every unmatched
	 * input null as well.  NOT IN is then simply its inverse, because invert
	 * propagates null -- which is exactly why `3 NOT IN (1, 2, NULL)` has to be
	 * NULL and not true.  A two-valued is_in cannot express that at all, which
	 * is why NOT IN used to fall back to the row engine.
	 *
	 * IN keeps SKIP, the behavior it has always had (it used to pass
	 * skip_nulls=true).  SKIP is not PG's semantics either -- `3 IN (1, 2,
	 * NULL)` is NULL, not false -- but the difference is invisible wherever the
	 * result feeds a filter, since Arrow drops null and false alike.  It only
	 * shows when the result is used as a value, e.g. in a target list or under
	 * IS NULL.  Fixing that is a separate change to an already-working path.
	 */
	is_in_expr = build_is_in(expr, garrow_move_ptr(val_array_datum),
							 is_not_in
							 ? GARROW_SET_LOOKUP_NULL_MATCHING_INCONCLUSIVE
							 : GARROW_SET_LOOKUP_NULL_MATCHING_SKIP);
	if (!is_not_in)
		return garrow_move_ptr(is_in_expr);

	return build_not_expression(is_in_expr);
}

static GArrowExpression *
build_not_expression(GArrowExpression *expr)
{
	g_autoptr(GArrowExpression) not_expr = NULL;
	g_autoptr(GArrowExpression) list_expr = garrow_copy_ptr(expr);
	GList *arguments = NULL;
	arguments = garrow_list_append_ptr(arguments, list_expr);
	not_expr = GARROW_EXPRESSION(garrow_call_expression_new("invert", arguments, NULL));
	garrow_list_free_ptr(&arguments);
	return garrow_move_ptr(not_expr);
}

static GArrowExpression *
build_is_not_null(GArrowExpression *expr)
{

	GList *arguments = NULL;
	g_autoptr(GArrowExpression) is_not_null_expr = NULL;
	g_autoptr(GArrowExpression) list_expr = garrow_copy_ptr(expr);
	arguments = garrow_list_append_ptr(arguments, list_expr);
	is_not_null_expr = GARROW_EXPRESSION(garrow_call_expression_new("is_valid", arguments, NULL));
	garrow_list_free_ptr(&arguments);
	return garrow_move_ptr(is_not_null_expr);
}

// NaN values can also be considered null by setting NullOptions::nan_is_null.
static GArrowExpression *
build_is_null(GArrowExpression *expr, gboolean nan_is_null)
{
	GList *arguments = NULL;
	g_autoptr(GArrowNullOptions) options = NULL;
	g_autoptr(GArrowExpression) is_null_expr = NULL;
	g_autoptr(GArrowExpression) list_expr = garrow_copy_ptr(expr);
	arguments = garrow_list_append_ptr(arguments, list_expr);
	options = garrow_null_options_new(nan_is_null);
	is_null_expr = GARROW_EXPRESSION(garrow_call_expression_new("is_null", arguments, GARROW_FUNCTION_OPTIONS(options)));
	garrow_list_free_ptr(&arguments);
	return garrow_move_ptr(is_null_expr);
}

/*
 * Convert NullTest to GArrowExpression
 */
static GArrowExpression *
nulltest_to_expression(NullTest *node, PlanBuildContext *pcontext)
{
	g_autoptr(GArrowExpression) expr = NULL;
	expr = expr_to_arrow_expression(node->arg, pcontext);
	if (!expr)
	{
		return NULL;
	}
	if (node->nulltesttype == IS_NULL)
	{
		return build_is_null(expr, true);
	}
	else
	{
		return build_is_not_null(expr);
	}
}

/*
 * Convert CaseExpr to GArrowExpression
 * format is:
 *  call("case_when", {
 * 		call("make_struct",
 * 			 {call("greater", {field_ref("a"), literal(1)})},
 * 			 MakeStructOptions({"case_0"})),
 * 		literal(1),
 * 		literal(2),
 *	});
 */
static GArrowExpression *
caseexpr_to_expression(CaseExpr* node, PlanBuildContext *pcontext)
{
	ListCell   *l;
	GList *when_arguments = NULL;
	GList *expr_arguments = NULL;
	GList *and_arguments = NULL;
	GList *not_arguments = NULL;
	g_autoptr(GArrowExpression) struct_expr;
	g_autoptr(GArrowExpression) default_expr;
	g_autoptr(GArrowExpression) expr;
	g_autoptr(GArrowExpression) and_condition_expr = NULL;
	g_autoptr(GArrowMakeStructOptions) options;
	int i = 0;
	gchar **fields;
	fields = palloc(list_length(node->args) * (sizeof(gchar *)));
	pcontext->case_test_expr = node->arg;
	pcontext->is_case_when = true;
	pcontext->whenexpr = NULL;
	pcontext->not_and_whenexpr = NULL;
	pcontext->case_when_type = exprType((Node *) node);
	foreach (l, node->args)
	{
		CaseWhen *when;
		g_autoptr(GArrowExpression) whenexpr;
		g_autoptr(GArrowExpression) whenexpr_copy;
		g_autoptr(GArrowExpression) resexpr;
		when = lfirst_node(CaseWhen, l);
		whenexpr = expr_to_arrow_expression(when->expr, pcontext);
		pcontext->whenexpr = expr_to_arrow_expression(when->expr, pcontext);
		whenexpr_copy = expr_to_arrow_expression(when->expr, pcontext);
		if (list_length(node->args) >= 2)
		{
			and_arguments = garrow_list_append_ptr(and_arguments, whenexpr_copy);
		}
		else
		{
			and_condition_expr = garrow_move_ptr(whenexpr_copy);
		}
		resexpr = expr_to_arrow_expression(when->result, pcontext);
		when_arguments = garrow_list_append_ptr(when_arguments, whenexpr);
		expr_arguments = garrow_list_append_ptr(expr_arguments, resexpr);
		fields[i] = palloc(sizeof(int) + 6);
		snprintf(fields[i], sizeof(int) + 6, "case_%d", i);
		i++;
	}

	if (list_length(node->args) >= 2)
		and_condition_expr = GARROW_EXPRESSION(garrow_call_expression_new("and_kleene", and_arguments, NULL));

	not_arguments = garrow_list_append_ptr(not_arguments, and_condition_expr);
	pcontext->not_and_whenexpr = GARROW_EXPRESSION(garrow_call_expression_new("invert", not_arguments, NULL));
	options = garrow_make_struct_options_new((const gchar **) fields, i);
	struct_expr = GARROW_EXPRESSION(garrow_call_expression_new(
			"make_struct", when_arguments, GARROW_FUNCTION_OPTIONS(options)));
	expr_arguments = garrow_list_prepend_ptr(expr_arguments, struct_expr);
	default_expr = expr_to_arrow_expression(node->defresult, pcontext);
	expr_arguments = garrow_list_append_ptr(expr_arguments, default_expr);
	expr = GARROW_EXPRESSION(garrow_call_expression_new("case_when", expr_arguments, NULL));
	pfree(fields);
	garrow_list_free_ptr(&when_arguments);
	garrow_list_free_ptr(&expr_arguments);
	if (and_arguments) 
		garrow_list_free_ptr(&and_arguments);
	garrow_list_free_ptr(&not_arguments);
	ARROW_FREE(GArrowExpression, &pcontext->not_and_whenexpr);
	ARROW_FREE(GArrowExpression, &pcontext->whenexpr);
	pcontext->is_case_when = false;
	pcontext->not_and_whenexpr = NULL;
	pcontext->whenexpr = NULL;
	return garrow_move_ptr(expr);
}

static GArrowExpression *
build_divide_casewhen(Expr *expr, PlanBuildContext *pcontext)
{
	GList *when_arguments = NULL;
	GList *expr_arguments = NULL;
	g_autoptr(GArrowExpression) casewhen_expr;
	g_autoptr(GArrowExpression) struct_expr;
	g_autoptr(GArrowExpression) default_expr;
	g_autoptr(GArrowMakeStructOptions) options;
	gchar **fields;
	fields = palloc((sizeof(gchar *)));
	g_autoptr(GArrowExpression) resexpr;
	resexpr = expr_to_arrow_expression(expr, pcontext);
	if (pcontext->not_and_whenexpr)
	{
		when_arguments = garrow_list_append_ptr(when_arguments, pcontext->not_and_whenexpr);
	}
	else
	{
		when_arguments = garrow_list_append_ptr(when_arguments, pcontext->whenexpr);
	}
	expr_arguments = garrow_list_append_ptr(expr_arguments, resexpr);
	fields[0] = palloc(sizeof(int) + 6);
	snprintf(fields[0], sizeof(int) + 6, "case_%d", 0);
	options = garrow_make_struct_options_new((const gchar **) fields, 1);
	struct_expr = GARROW_EXPRESSION(garrow_call_expression_new(
			"make_struct", when_arguments, GARROW_FUNCTION_OPTIONS(options)));
	expr_arguments = garrow_list_prepend_ptr(expr_arguments, struct_expr);
	default_expr = build_literal_expression(0, true, pcontext->case_when_type, -1);
	expr_arguments = garrow_list_append_ptr(expr_arguments, default_expr);
	casewhen_expr = GARROW_EXPRESSION(garrow_call_expression_new("case_when", expr_arguments, NULL));
	pfree(fields);
	garrow_list_free_ptr(&when_arguments);
	garrow_list_free_ptr(&expr_arguments);
	return garrow_move_ptr(casewhen_expr);
}


static GArrowExpression*
build_divide_casewhen_opexpr(List *args, PlanBuildContext *pcontext)
{
	Expr *divisor = (Expr *) linitial(args);
	Expr *dividend = (Expr *) lsecond(args);
	g_autoptr(GArrowExpression) expr;
	g_autoptr(GArrowExpression) divisor_expr;
	g_autoptr(GArrowExpression) case_when_expr;
	GList *expr_arguments = NULL;
	divisor_expr = expr_to_arrow_expression(divisor, pcontext);
	case_when_expr = build_divide_casewhen(dividend, pcontext);
	expr_arguments = garrow_list_append_ptr(expr_arguments, divisor_expr);
	expr_arguments = garrow_list_append_ptr(expr_arguments, case_when_expr);
	expr = GARROW_EXPRESSION(garrow_call_expression_new("divide", expr_arguments, NULL));
	garrow_list_free_ptr(&expr_arguments);
	return garrow_move_ptr(expr);
}

GArrowExpression *
extract_expression(List *args, PlanBuildContext *pcontext, const char* name)
{
	ListCell   *lc;
	int type, val;
	char	   *lowunits = NULL;
	type = UNKNOWN_FIELD;
	foreach(lc, args)
	{
		Expr	   *arg = (Expr *) lfirst(lc);

		if (IsA(arg, Const))
		{
			Const	   *con = (Const *) arg;
			text	   *units = DatumGetTextPP(con->constvalue);

			lowunits = downcase_truncate_identifier(VARDATA_ANY(units),
													VARSIZE_ANY_EXHDR(units),
														false);
			type = DecodeUnits(0, lowunits, &val);
		}
		else
		{
			if (type == UNITS)
			{
				g_autoptr(GArrowExpression) extract_expr = NULL;
				g_autoptr(GArrowCastOptions)  cast_options = NULL;
				g_autoptr(GArrowDataType) to_type = NULL;
				g_autoptr(GList) cast_args = NULL;
				g_autoptr(GArrowExpression) cast_decimal_expr = NULL;
				switch (val)
				{
					case DTK_MICROSEC:
						extract_expr = func_arg_to_expression(arg, pcontext, "microsecond");
						break;
					case DTK_MILLISEC:
						extract_expr = func_arg_to_expression(arg, pcontext, "millisecond");
						break;
					case DTK_SECOND:
						extract_expr = func_arg_to_expression(arg, pcontext, "second");
						break;
					case DTK_MINUTE:
						extract_expr = func_arg_to_expression(arg, pcontext, "minute");
						break;
					case DTK_HOUR:
						extract_expr = func_arg_to_expression(arg, pcontext, "hour");
						break;
					case DTK_DAY:
						extract_expr = func_arg_to_expression(arg, pcontext, "day");
						break;
					case DTK_MONTH:
						extract_expr = func_arg_to_expression(arg, pcontext, "month");
						break;
					case DTK_YEAR:
						extract_expr = func_arg_to_expression(arg, pcontext, "year");
						break;
					case DTK_DECADE:
						extract_expr = func_arg_to_expression(arg, pcontext, "decade");
						break;
					case DTK_CENTURY:
						extract_expr = func_arg_to_expression(arg, pcontext, "century");
						break;
					case DTK_MILLENNIUM:
						extract_expr = func_arg_to_expression(arg, pcontext, "millennium");
						break;
					default:
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									errmsg("timestamp units  not supported")));
				}
				// add cast expr(integer to decimal) to align pg extract expr output type
				to_type = GARROW_DATA_TYPE(garrow_numeric128_data_type_new());
				cast_args = garrow_list_append_ptr(cast_args, extract_expr);
				cast_options = GARROW_CAST_OPTIONS(g_object_new(GARROW_TYPE_CAST_OPTIONS, "to-data-type", to_type , NULL));
				cast_decimal_expr = GARROW_EXPRESSION(garrow_call_expression_new("cast", cast_args, GARROW_FUNCTION_OPTIONS(cast_options)));
				return garrow_move_ptr(cast_decimal_expr);
			}
			else
			{
				elog(ERROR, "Failed to call extract type (%d) ", type);
				return NULL;
			}

		}
	}
	elog(ERROR, "Failed to call extract_expression");
	return NULL;
}

GArrowExpression*
build_divide_expr(List *args, PlanBuildContext *pcontext, const char* funcname)
{
	if (pcontext->is_case_when)
	{
		return build_divide_casewhen_opexpr(args, pcontext);
	}
	return func_args_to_expression(args, pcontext, funcname);
}

GArrowExpression *
utf8_slice_codeunits_expression(List *args, PlanBuildContext *pcontext, const char* name)
{
	Expr *inner_expr = NULL;
	Expr *start_expr = NULL;
	Expr *len_expr = NULL;
	Const *const_expr_start = NULL;
	Const *const_expr_end = NULL;
	int start = 0, len = 0, l1 = 0, end = INT_MAX;

	g_autoptr(GArrowExpression) expr = NULL;
	g_autoptr(GArrowExpression) slice_expression = NULL;
	g_autoptr(GArrowSliceOptions) options = NULL;
	GList *arguments = NULL;
	inner_expr = (Expr *) linitial(args);
	expr = expr_to_arrow_expression(inner_expr, pcontext);
	Assert(expr);
	arguments = garrow_list_append_ptr(arguments, expr);
	start_expr = (Expr *) lsecond(args);
	/* FIXME: will fallback in the planner. */
	if (nodeTag(start_expr) != T_Const)
	{
		elog(ERROR, "unrecognized node type: %d", (int) nodeTag(start_expr));
	}
	const_expr_start =  (Const *) start_expr;
	if (const_expr_start->constbyval)
	{
		start = const_expr_start->constvalue;
		if (start < 1)
		{
			l1 = start - 1;
			start = 1;
		}
		start--;
	}
	/* regex plan fallback */
	if (list_length(args) == 3) {
		len_expr = (Expr *) lthird(args);
		if (nodeTag(len_expr) != T_Const)
		{
			elog(ERROR, "unrecognized node type: %d", (int) nodeTag(len_expr));
		}
		const_expr_end =  (Const *) len_expr;
		len = const_expr_end->constvalue;
		if (len < 0)
		{
			elog(ERROR, "negative substring length not allowed");
		}
		end = start + len + l1;
	}
	options = garrow_slice_options_new(start, end, 1);
	slice_expression = GARROW_EXPRESSION(garrow_call_expression_new(
		"utf8_slice_codeunits", arguments, GARROW_FUNCTION_OPTIONS(options)));
	garrow_list_free_ptr(&arguments);
	return garrow_move_ptr(slice_expression);
}

GArrowExpression *
replace_substring_regex_expression(List *args, PlanBuildContext *pcontext, const char *name)
{
	Expr *first_expr = NULL;
	Expr *second_expr = NULL;
	Expr *third_expr = NULL;
	Const *const_expr_pattern = NULL;
	Const *const_expr_replace = NULL;
	char *str_pattern = NULL;
	char *str_replace = NULL;
	struct varlena *s = NULL;

	g_autoptr(GArrowExpression) expr = NULL;
	g_autoptr(GArrowExpression) replace_expression = NULL;
	g_autoptr(GArrowReplaceSubstringOptions) options = NULL;
	GList *arguments = NULL;

	first_expr = (Expr *) linitial(args);
	if (nodeTag(first_expr) != T_Var)
	{
		elog(ERROR, "unrecognized node type: %d", (int) nodeTag(first_expr));
	}
	expr = expr_to_arrow_expression(first_expr, pcontext);
	Assert(expr);

	second_expr = (Expr *) lsecond(args);
	if (nodeTag(second_expr) != T_Const)
	{
		elog(ERROR, "unrecognized node type: %d", (int) nodeTag(second_expr));
	}
	const_expr_pattern =  (Const *) second_expr;

	third_expr = (Expr *) lthird(args);
	if (nodeTag(third_expr) != T_Const)
	{
		elog(ERROR, "unrecognized node type: %d", (int) nodeTag(third_expr));
	}
	const_expr_replace =  (Const *) third_expr;
	s = (struct varlena *) const_expr_pattern->constvalue;
	str_pattern = text_to_cstring(s);
	s = (struct varlena *) const_expr_replace->constvalue;
	str_replace = text_to_cstring(s);

	options = garrow_replace_substring_options_new(str_pattern, str_replace, -1);
	arguments = garrow_list_append_ptr(arguments, expr);
	replace_expression = GARROW_EXPRESSION(garrow_call_expression_new(
			"replace_substring_regex", arguments, GARROW_FUNCTION_OPTIONS(options)));

	garrow_list_free_ptr(&arguments);
	return garrow_move_ptr(replace_expression);
}

static GArrowExpression *
build_like_expr(List *args, PlanBuildContext *pcontext, bool is_notlike)
{
	g_autoptr(GArrowExpression)  result_expr = NULL;
	gboolean ignore_case = false;
	GList           *arguments = NULL;

	Expr  *first_expr = linitial(args);
	Expr  *second_expr = lsecond(args);
	char *value;

	Expr  *var_epxr = NULL;
	Const *const_expr = NULL;

	// Currently we only support like expressions where one of
	// the arguments is const.
	if (IsA(first_expr, Const))
	{
		const_expr = (Const *)first_expr;
		var_epxr = second_expr;
	}
	else
	{
		const_expr = (Const *)second_expr;
		var_epxr = first_expr;
	}

	value = text_to_cstring(DatumGetTextP(const_expr->constvalue));
	g_autoptr(GArrowMatchSubstringOptions) options =
			garrow_match_substring_options_new(value, ignore_case);
	g_autoptr(GArrowExpression) expr = expr_to_arrow_expression(var_epxr, pcontext);

	if (expr)
	{
		g_autoptr(GArrowExpression) not_expr = NULL;
		arguments = garrow_list_append_ptr(arguments, expr);
		expr = GARROW_EXPRESSION(garrow_call_expression_new(
				"match_like", arguments, GARROW_FUNCTION_OPTIONS(options)));
		garrow_list_free_ptr(&arguments);
		if (is_notlike)
		{
			not_expr = build_not_expression(expr);
			return garrow_move_ptr(not_expr);
		}
		return garrow_move_ptr(expr);
	}
	else
	{
		elog(ERROR, "Expression to arrow error.");
		return NULL;
	}
}

GArrowExpression *
build_like_expression(List *args, PlanBuildContext *pcontext, const char *name)
{
	return build_like_expr(args, pcontext, false);
}

GArrowExpression *
build_not_like_expression(List *args, PlanBuildContext *pcontext, const char *name)
{
	return build_like_expr(args, pcontext, true);
}

GArrowExpression *
replace_expression(List *args, PlanBuildContext *pcontext, const char* name)
{
	Expr *first_expr = NULL;
	Expr *second_expr = NULL;
	Expr *third_expr = NULL;
	Const *const_expr_pattern = NULL;
	Const *const_expr_replace = NULL;
	char *str_pattern = NULL;
	char *str_replace = NULL;
	struct varlena *s = NULL;

	g_autoptr(GArrowExpression) expr = NULL;
	g_autoptr(GArrowExpression) replace_expression = NULL;
	g_autoptr(GArrowReplaceSubstringOptions) options = NULL;
	GList *arguments = NULL;

	first_expr = (Expr *) linitial(args);
	if (!IsA(first_expr, Var))
	{
		elog(ERROR, "unrecognized node type: %d", (int) nodeTag(first_expr));
	}
	expr = expr_to_arrow_expression(first_expr, pcontext);
	Assert(expr);

	second_expr = (Expr *) lsecond(args);
	if (!IsA(second_expr,Const))
	{
		elog(ERROR, "unrecognized node type: %d", (int) nodeTag(second_expr));
	}
	const_expr_pattern =  (Const *) second_expr;

	third_expr = (Expr *) lthird(args);
	if (!IsA(third_expr,Const))
	{
		elog(ERROR, "unrecognized node type: %d", (int) nodeTag(third_expr));
	}
	const_expr_replace =  (Const *) third_expr;
	s = (struct varlena *) const_expr_pattern->constvalue;
	str_pattern = text_to_cstring(s);
	s = (struct varlena *) const_expr_replace->constvalue;
	str_replace = text_to_cstring(s);

	options = garrow_replace_substring_options_new(str_pattern, str_replace, -1);
	arguments = garrow_list_append_ptr(arguments, expr);
	replace_expression = GARROW_EXPRESSION(garrow_call_expression_new("replace_substring", arguments, GARROW_FUNCTION_OPTIONS(options)));

	pfree(str_pattern);
	pfree(str_replace);
	garrow_list_free_ptr(&arguments);
	return garrow_move_ptr(replace_expression);
}

static GArrowExpression *
build_literal_expression(Datum datum, bool isnull, Oid pg_type, int32 typmod)
{
	g_autoptr(GArrowScalar) val_scalar = NULL;
	g_autoptr(GArrowDatum) val_datum = NULL;
	val_scalar = ArrowScalarNew(PGTypeToArrowID(pg_type), datum, isnull, pg_type, typmod);
	val_datum = GARROW_DATUM(garrow_scalar_datum_new(val_scalar));
	return GARROW_EXPRESSION(garrow_literal_expression_new(val_datum));
}


static GArrowExpression *
coalesceexpr_expression(CoalesceExpr *coalexpr, PlanBuildContext *pcontext)
{
	GList *arguments = NULL;
	ListCell *l;
	g_autoptr(GArrowExpression)  result_expr = NULL;
	foreach(l, coalexpr->args)
	{
		Expr  *arg_expr = (Expr *) lfirst(l);
		g_autoptr(GArrowExpression) expr = expr_to_arrow_expression(arg_expr, pcontext);

		if (!expr)
		{
			elog(ERROR, "arg expr to arrow error.");
			return NULL;
		}
		arguments = garrow_list_append_ptr(arguments, expr);
	}
	result_expr = GARROW_EXPRESSION(garrow_call_expression_new(
		"coalesce", arguments, NULL));
	if (!result_expr)
		elog(ERROR, "Failed to new arrow call expression coalesce.");
	garrow_list_free_ptr(&arguments);
	return garrow_move_ptr(result_expr);
}


static GArrowExpression *
expr_to_arrow_expression(Expr *node, PlanBuildContext *pcontext)
{
	g_autoptr(GError) error = NULL;
	g_autoptr(GArrowExpression) expr = NULL;

	if (node == NULL)
		return NULL;

	/* Guard against stack overflow due to overly complex expressions */
	check_stack_depth();

	switch (nodeTag(node))
	{
		case T_Var:
			{
				const char *attname = NULL;
				Var *var = (Var *) node;

				if (var->varattno == SelfItemPointerAttributeNumber)
				{
					attname = GetCtidSchemaName(pcontext->inputschema);
				}
				else if (var->varattno == GpSegmentIdAttributeNumber)
				{
					expr = build_literal_expression(GpIdentity.segindex, false, var->vartype, -1);
					break;
				}
				else if (pcontext->is_nestloopjoin || (pcontext->is_hashjoin && pcontext->is_hashjoin_after_node))
				{
					if (pcontext->inputschema)
					{
						/*
						 * After the hashjoin node, resolve Var against the
						 * join output schema.  OUTER_VAR -> LEFT_PREFIX,
						 * INNER_VAR -> RIGHT_PREFIX (standard mapping).
						 */
						int effective_varno = var->varno;
						attname = GetSchemaNameByVarNo(pcontext->inputschema, var->varattno, effective_varno);
					}
					else
					{
						/*
						 * Before the hashjoin output schema is set,
						 * resolve against per-side proj schemas.
						 * OUTER_VAR -> left_proj, INNER_VAR -> right_proj.
						 */
						Assert(pcontext->left_proj_schema && pcontext->right_proj_schema);
						if (var->varno == OUTER_VAR)
							attname = GetSchemaName(pcontext->left_proj_schema, var->varattno, pcontext->map);
						else
							attname = GetSchemaName(pcontext->right_proj_schema, var->varattno, pcontext->map);
					}
				}
				else if (pcontext->is_append)
				{
					attname = GetSchemaName(pcontext->inputschema, pcontext->append_filed_index + 1, pcontext->map);
				} 
				else
					attname = GetSchemaName(pcontext->inputschema, var->varattno, pcontext->map);
				/*
				 * RIGHT_SEMI / RIGHT_ANTI hash join: Arrow's Mark Join emits
				 * only the build (Arrow right / PG inner) side, so the join
				 * output schema has no LEFT_PREFIX columns.  A surviving
				 * OUTER_VAR in the targetlist can therefore only be a hash key
				 * whose value is, by the equijoin condition, identical to the
				 * matching inner key.  Map it to the build-side column that
				 * carries that inner key.
				 *
				 * The build-side projection does not name its key columns
				 * predictably: build_join_project_options() reuses a
				 * passthrough column (<RIGHT_PREFIX><attname>) for a plain-Var
				 * key and only synthesizes right_joinqual_N for expression keys,
				 * so the column name cannot be re-derived from the key position
				 * here.  Instead we read the exact name the producer recorded in
				 * pcontext->right_key_names, indexed by hash-key position.  The
				 * equijoin pairs outer key i with inner key i, so the outer key
				 * that matches this OUTER_VAR and the recorded build-side name
				 * share the same index.
				 */
				if (attname == NULL && var->varno == OUTER_VAR &&
					pcontext->is_hashjoin && pcontext->is_hashjoin_after_node &&
					pcontext->inputschema != NULL &&
					IsA(pcontext->planstate, HashJoinState))
				{
					HashJoinState *hjs = (HashJoinState *) pcontext->planstate;

					if (hjs->js.jointype == JOIN_RIGHT_SEMI ||
						hjs->js.jointype == JOIN_RIGHT_ANTI)
					{
						ListCell   *lc;
						int			idx = 0;

						foreach(lc, hjs->hj_OuterHashKeys)
						{
							ExprState  *keystate = (ExprState *) lfirst(lc);

							if (keystate != NULL && IsA(keystate->expr, Var) &&
								((Var *) keystate->expr)->varattno == var->varattno)
							{
								if (idx < list_length(pcontext->right_key_names))
								{
									char	   *keyname = (char *)
										list_nth(pcontext->right_key_names, idx);
									g_autoptr(GArrowField) f =
										garrow_schema_get_field_by_name(pcontext->inputschema, keyname);

									/*
									 * keyname is owned by right_key_names (freed
									 * after BuildProject), so it stays valid for
									 * the rest of this resolution; f is only used
									 * to confirm the column is present.
									 */
									if (f != NULL)
										attname = keyname;
								}
								break;
							}
							idx++;
						}
					}
				}
				if (attname == NULL)
					elog(ERROR, "failed to resolve Var (varno=%d varattno=%d) "
						 "to an arrow field in the join output schema",
						 var->varno, var->varattno);
				expr = GARROW_EXPRESSION(garrow_field_expression_new(attname, &error));
				if (error)
					elog(ERROR, "convert PG Var(name: %s) to arrow expression failed: %s",
							attname, garrow_take_error(&error));
				break;
			}
		case T_Const:
			{
				Const * const_expr = (Const *) node;
				expr = build_literal_expression(const_expr->constvalue,
												const_expr->constisnull,
												const_expr->consttype,
												const_expr->consttypmod);
				break;
			}
		case T_OpExpr:
			{
				OpExpr	  *opexpr = (OpExpr *)node;
				return get_function_expression(opexpr->opfuncid, pcontext, opexpr->args);
				break;
			}
		case T_FuncExpr:
			{
				FuncExpr	  *funcexpr = (FuncExpr *)node;
				return get_function_expression(funcexpr->funcid, pcontext, funcexpr->args);
				break;
			}
		case T_WindowFunc:
			{
				WindowFunc *wfunc = (WindowFunc *) node;
				VecAggInfo *agginfo = NULL;
				ListCell *l;

				/* check whether current window agg is duplicated */
				foreach(l, pcontext->agginfos)
				{
					VecAggInfo *cinfo = (VecAggInfo *) lfirst(l);

					if (equal(wfunc, cinfo->wfunc) &&
						!contain_volatile_functions((Node *) wfunc))
					{
						agginfo = cinfo;
					}
				}

				if (!agginfo)
				{
					agginfo = new_winagg_info(wfunc, pcontext);
					pcontext->agginfos = lappend(pcontext->agginfos, agginfo);
				}

				/* winfunc in expression is a normal field for arrow, use out name */
				expr = GARROW_EXPRESSION(garrow_field_expression_new(
						agginfo->outname, &error));
				if (error)
					elog(ERROR, "convert PG winfunc(name: %s) to arrow expression failed: %s",
							agginfo->outname, garrow_take_error(&error));

				break;
			}
		case T_Aggref:
			{
				Aggref *aggref = (Aggref *) node;
				VecAggInfo *agginfo = NULL;
				ListCell *l;

				/* check whether current agg is duplicated */
				foreach(l, pcontext->agginfos)
				{
					VecAggInfo *cinfo = (VecAggInfo *) lfirst(l);
					if (cinfo->aggref->aggno == aggref->aggno)
					{
						agginfo = cinfo;
					}
				}

				if (!agginfo)
				{
					agginfo = new_agg_info(aggref,
										   aggref->aggno,
										   pcontext);
					pcontext->agginfos = lappend(pcontext->agginfos, agginfo);
				}
				/* aggref in expression is a normal field for arrow, use out name*/
				expr = GARROW_EXPRESSION(garrow_field_expression_new(
						agginfo->outname, &error));
				if (error)
					elog(ERROR, "convert PG Aggref(name: %s) to arrow expression failed: %s",
							agginfo->outname, garrow_take_error(&error));
				break;
			}
		case T_List:
			break;
		case T_RelabelType:
			{
				RelabelType	  *opexpr = (RelabelType *)node;
				return expr_to_arrow_expression(opexpr->arg, pcontext);
			}
		case T_BoolExpr:
			{
				BoolExpr *boolexpr = (BoolExpr *)node;
				const char *funcname;
				switch (boolexpr->boolop)
				{
					case AND_EXPR:
						funcname = "and_kleene"; /* 2 expression */
						break;
					case OR_EXPR:
						funcname = "or_kleene"; /* 2 expression */
						break;
					case NOT_EXPR:
						funcname = "invert"; /* 1 expression */
						break;
					default:
						elog(ERROR, "unrecognized boolop: %d",(int) boolexpr->boolop);
				}

				expr = func_args_to_expression(boolexpr->args, pcontext, funcname);

				break;
			}
		case T_ScalarArrayOpExpr:
			{
				ScalarArrayOpExpr *arrayexpr = (ScalarArrayOpExpr *) node;

				expr = scalararray_to_expression(arrayexpr, pcontext);
				break;
			}
		case T_NullTest:
			{
				NullTest   *nt = (NullTest *) node;
				expr = nulltest_to_expression(nt, pcontext);
				break;
			}
		case T_CaseExpr:
			{
				CaseExpr *caseexpr = (CaseExpr *) node;
				expr = caseexpr_to_expression(caseexpr, pcontext);
				break;
			}
		case T_CaseTestExpr:
			{
				Assert(pcontext->case_test_expr);
				expr = expr_to_arrow_expression(pcontext->case_test_expr, pcontext);
				break;
			}
		case T_CoalesceExpr:
			{
				CoalesceExpr *coalexpr = (CoalesceExpr *) node;
				expr = coalesceexpr_expression(coalexpr, pcontext);
				break;
			}
		case T_DistinctExpr:
			{
				DistinctExpr *distinctexpr = (DistinctExpr* ) node;
				expr = build_is_distinct_expression(distinctexpr, pcontext);
				break;
			}
		case T_Param:
			{
				Param* param = (Param *) node;
				ExprContext* exprcontext = pcontext->planstate->ps_ExprContext;
				ParamExecData *prm = &(exprcontext->ecxt_param_exec_vals[param->paramid]);
				if (prm->execPlan != NULL)
				{
					/* Parameter not evaluated yet, so go do it */
					ExecSetParamPlan(prm->execPlan, exprcontext, NULL);
					/* ExecSetParamPlan should have processed this param... */
					Assert(prm->execPlan == NULL);
				}
				if (param->paramtype == NUMERICOID && prm->value == (Datum) 0)
					prm->value = NumericGetDatum(int64_to_numeric(0));
				expr = build_literal_expression(prm->value,
												prm->isnull,
												param->paramtype,
												param->paramtypmod);
				break;
			}
		case T_NullIfExpr:
			{
				NullIfExpr *nullifexpr = (NullIfExpr *) node;
				expr = build_null_if_expression(nullifexpr, pcontext);
				break;
			}

		
		default:
			elog(ERROR, "unrecognized node type: %d", (int)nodeTag(node));
	}
	return garrow_move_ptr(expr);
}

/* generate unique aggname and VecAggInfo*/
static inline VecAggInfo *
new_agg_info(Aggref *aggref, int aggno, PlanBuildContext *pcontext)
{
	VecAggInfo *agginfo;
	const ArrowAggFmgr *fmgr;
	const AggFuncTable *table;

	agginfo = palloc(sizeof(VecAggInfo));
	agginfo->aggref = aggref;

	fmgr = get_arrow_agg_fmgr(aggref->aggfnoid);
	if (!fmgr)
		elog(ERROR, "Can not find Arrow aggregate fmgr, aggfnoid: %d",
				aggref->aggfnoid);
	if (aggref->aggsplit == AGGSPLIT_FINAL_DESERIAL)
		table = get_arrow_agg_functable(fmgr->finalfn);
	else if (aggref->aggsplit == AGGSPLIT_INITIAL_SERIAL)
		table = get_arrow_agg_functable(fmgr->transfn);
	else if (aggref->aggsplit == AGGSPLIT_SIMPLE)
		table = get_arrow_agg_functable(fmgr->simplefn);
	else if (aggref->aggsplit == AGGSPLIT_INTERMEDIATE && fmgr->combinefn)
		table = get_arrow_agg_functable(fmgr->combinefn);
	else
		elog(ERROR, "doesn't support aggsplit: %d", aggref->aggsplit);

	agginfo->aname = get_agg_func_name(aggref, table, pcontext);
	agginfo->options = table->getOption(list_length(aggref->args));

	snprintf(agginfo->inname, sizeof(agginfo->inname),
			"_inagg_%d", aggno);
	snprintf(agginfo->outname, sizeof(agginfo->outname),
			"_outagg_%d", aggno);
	return agginfo;
}

/* generate unique aggname and VecAggInfo */
static inline VecAggInfo *
new_winagg_info(WindowFunc *wfunc, PlanBuildContext *pcontext)
{
	VecAggInfo *agginfo;
	const ArrowAggFmgr *fmgr;
	const AggFuncTable *table;

	agginfo = palloc(sizeof(VecAggInfo));
	agginfo->wfunc = wfunc;

	fmgr = get_arrow_agg_fmgr(wfunc->winfnoid);
	if (!fmgr)
		elog(ERROR, "Can not find Arrow aggregate fmgr, aggfnoid: %d", wfunc->winfnoid);

	table = get_arrow_agg_functable(fmgr->simplefn);
	agginfo->aname = table->funcName;
	agginfo->options = table->getOption(list_length(wfunc->args));

	int winaggno = list_length(pcontext->agginfos);
	snprintf(agginfo->inname, sizeof(agginfo->inname), "_inwinagg_%d", winaggno);
	snprintf(agginfo->outname, sizeof(agginfo->outname), "_outwinagg_%d", winaggno);

	return agginfo;
}

static void
BuildMaterializePlan(PlanBuildContext *pcontext, VecExecuteState *estate)
{
	g_autoptr(GArrowExecuteNode) source_node = NULL;
	g_autoptr(GArrowMaterializeNodeOptions)	materialize_node_options = NULL;
	g_autoptr(GArrowExecuteNode) materialize_exec_node = NULL;
	g_autoptr(GError) error = NULL;
	GArrowStoreType type;

	type = to_arrow_storetype(pcontext->store_type);

	source_node = BuildSource(pcontext);

	materialize_node_options =
			garrow_materialize_node_options_new(pcontext->store_file, type, pcontext->chunk_size, pcontext->cdb_strict, &error);

	if (error)
		elog(ERROR, "build materialize node option fail caused by %s", garrow_take_error(&error));

	materialize_exec_node =
			garrow_execute_plan_build_materialize_node(pcontext->plan,
										source_node,
										materialize_node_options,
										&error);

	if (error)
		elog(ERROR, "build materialize node fail caused by %s", garrow_take_error(&error));

	BuildSink(materialize_exec_node, estate, pcontext);

	return;
}

static void
BuildShareScanPlan(PlanBuildContext *pcontext, VecExecuteState *estate)
{
	g_autoptr(GArrowExecuteNode) source_node = NULL;
	g_autoptr(GArrowShareInputScanNodeOptions)	share_node_options = NULL;
	g_autoptr(GArrowSourceShareNodeOptions)	share_source_node_options = NULL;
	g_autoptr(GArrowExecuteNode) share_exec_node = NULL;
	g_autoptr(GError) error = NULL;
	bool		cross_slice = ((ShareInputScan *) pcontext->planstate->plan)->cross_slice;

	if (pcontext->is_producer)
	{
		source_node = BuildSource(pcontext);
		share_node_options = garrow_shareinputscan_node_options_new(pcontext->gp_session_id,
													pcontext->gp_command_count,
													pcontext->share_id,
													pcontext->num_slices,
													pcontext->is_cross_slice,
													pcontext->ready,
													&error);
		if (error)
			elog(ERROR, "build share producter node option fail caused by %s", garrow_take_error(&error));

		/*
		 * A CTE producer sits under Sequence, which throws its rows away.
		 * Forwarding them would only park a full second copy of the shared
		 * data in this plan's sink (batch mode: the whole plan runs before
		 * the sink is read), 2.4GB per segment on TPC-DS q67.  Limited to
		 * cross-slice producers, which are never spliced into the parent's
		 * Arrow plan, so this plan's sink is the only thing that would
		 * have received them.
		 */
		if (cross_slice && PlanOutputDiscarded(pcontext->planstate))
			garrow_shareinputscan_node_options_set_discard_output(share_node_options,
																  TRUE);

		share_exec_node = garrow_execute_plan_build_shareinputscan_node(pcontext->plan,
												source_node,
												share_node_options,
												&error);
		if (error)
			elog(ERROR, "build share producter node fail caused by %s", garrow_take_error(&error));

		BuildSink(share_exec_node, estate, pcontext);
	}
	else
	{
		share_source_node_options = garrow_sourceshare_node_options_new(pcontext->inputschema,
													pcontext->gp_session_id,
													pcontext->gp_command_count,
													pcontext->share_id,
													pcontext->num_slices,
													pcontext->is_cross_slice,
													pcontext->ready,
													&error);
		if (error)
			elog(ERROR, "build share consumer node option fail caused by %s", garrow_take_error(&error));

		share_exec_node = garrow_execute_plan_build_sourceshare_node(pcontext->plan,
												share_source_node_options,
												&error);
		if (error)
			elog(ERROR, "build share consumer node fail caused by %s", garrow_take_error(&error));

		/*
		 * SharedSourceNode's RunConsumeLoop relies on the yield protocol
		 * (or a blocking wait for a separate reader thread) to survive
		 * SinkNode backpressure -- same constraint as callback sources.
		 */
		pcontext->has_callback_source = true;

		BuildSink(share_exec_node, estate, pcontext);
	}
}

static GArrowExecuteNode *
BuildAppendPlan(PlanBuildContext *pcontext, VecExecuteState *estate)
{
	g_autoptr(GArrowExecuteNode) append_node = NULL;
	g_autoptr(GArrowProjectNodeOptions) project_options = NULL;
	g_autoptr(GArrowExecuteNode) project_node = NULL;
	g_autoptr(GError) error = NULL;
	g_autoptr(GArrowAppendNodeOptions) append_option = NULL;
	g_autoptr(GArrowExecuteNode) source = NULL;
	GList* nodes = NULL;
	Append *append = NULL;
	AppendState *appendstate = NULL;
	PlanState **appendplanstates = NULL;
	List *targetList = NULL;
	GArrowSchema *source_project_schema = NULL;
	int i;
	int nplans;

	appendstate = (AppendState*)pcontext->planstate;
	appendplanstates = appendstate->appendplans;
	append = (Append*)(appendstate->ps.plan);
	
	nplans = list_length(append->appendplans);
	
	for (i = 0; i < nplans; ++i)
	{
		pcontext->inputschema = GetSchemaFromSlot(appendplanstates[i]->ps_ResultTupleSlot);
		pcontext->sub_slice = i;
		source = BuildSource(pcontext);

		targetList = appendstate->ps.plan->targetlist;
		/*
		 * The purpose of projecting multiple input nodes with multiple appends
		 * here is to modify the schema of their respective inputs to ensure
		 * that the names of the schema can match the same.
		 * Continue to do the project on the source node. 
		 * The filter of the source has been processed. 
		 * The project here is only for the purpose of projecting and modifying the schema, and does not need to be filtered.
		 */
		project_node = BuildProject(targetList, NULL, source, pcontext);

		if (!source_project_schema)
			source_project_schema = garrow_execute_node_get_output_schema(project_node);

		nodes = garrow_list_append_ptr(nodes, project_node);
	}
	/*
	 * The input schema of the append operator is changed to be consistent with the schema of the source after project.
	 */
	pcontext->inputschema = source_project_schema;
	append_option = garrow_append_node_options_new();
	append_node = garrow_execute_plan_build_append_node(pcontext->plan,
								  	    nodes,
									    append_option,
									    &error);
	if (error)
		elog(ERROR, "Failed to create append node: %s.", garrow_take_error(&error));
	garrow_list_free_ptr(&nodes);

	return garrow_move_ptr(append_node);
}

/* ------------------------------------------------------------------------
 *
 * Build Arrow plan execute state by cbdb planstate.
 *
 * Inputs:
 *   'planstate'is the initialized plan state used to build Arrow plan.
 *
 * Outputs:
 *   'estate' is output execute state, which is used by ExecutePlan.
 *
 * ------------------------------------------------------------------------
 */
void
BuildVecPlan(PlanState *planstate, VecExecuteState *estate)
{
	g_autoptr(GError) error = NULL;
	g_autoptr(GArrowExecuteNode) source = NULL;
	List *targetList = planstate->plan->targetlist;
	List *qualList = planstate->plan->qual;
	PlanBuildContext pcontext;
	bool support_parallel = true;

	/*
	 * VEC-YIELD-GUARD: decide before anything is built.
	 *
	 * BuildSink arms SinkNode backpressure off enable_vec_pipeline alone, and
	 * the first BuildSink calls happen while this function is still walking
	 * the tree -- long before the ExecContext choice further down.  A plan
	 * that ends up in batch mode with backpressure armed self-deadlocks: the
	 * batch path is start() then wait(), so nobody reads the sink until wait()
	 * returns and a backpressure pause can never be answered.  That is what
	 * the note on the yield branch in ExecuteVecPlan warns about.  Computing
	 * this flag late was enough to hang TPC-DS q22 and q24 -- both fine with
	 * the pipeline off, and fine before this guard existed.
	 *
	 * So the answer has to be known here, and BuildSink has to honour it.
	 */
	{
		bool		share_unsafe = QueryHasAnyShareInputScan(planstate);
		int			nmotion = CountReceivingMotionsInSlice(planstate);

		estate->yield_unsafe = share_unsafe || nmotion >= 2;
		if (estate->yield_unsafe)
			elog(DEBUG1, "VEC-YIELD-GUARD pid=%d slice=%d share=%d motions=%d -> batch mode, no backpressure",
				 MyProcPid, currentSliceId, share_unsafe ? 1 : 0, nmotion);
	}

	/* init PlanBuildContext*/
	pcontext.agginfos = NULL;
	pcontext.keys = NULL;
	pcontext.nkey = 0;
	/*
	 * Only the SeqScan branch below fills this in, and the context is a stack
	 * local that nothing memsets -- so every other node type would leave it
	 * holding stack garbage.  It has to start NULL, because the release at the
	 * end of this function tests it.
	 */
	pcontext.relation_schema = NULL;
	/*
	 * Default for non-Agg nodes; the T_AggState case below overwrites it with
	 * the real strategy. Must be initialized: BuildAggregatation gates sonic
	 * routing on aggstrategy == AGG_HASHED, so leaving this as stack garbage
	 * could spuriously run is_sonic_compatible for a window / join / scan node.
	 */
	pcontext.aggstrategy = AGG_PLAIN;
	pcontext.planstate = planstate;
	pcontext.reader = NULL;
	pcontext.ishaving = false;
	pcontext.plan = NULL;
	pcontext.map = NULL;
	pcontext.sinktype = Plain;

	pcontext.is_hashjoin = false;
	pcontext.is_nestloopjoin = false;
	pcontext.is_hashjoin_after_node = false;
	pcontext.left_proj_schema = NULL;
	pcontext.right_proj_schema = NULL;
	pcontext.left_hashkeys = NULL;
	pcontext.right_hashkeys = NULL;
	pcontext.right_key_names = NIL;
	pcontext.numeric_key_fix = NIL;
	pcontext.numeric_canon_keys = NULL;
	pcontext.inputschema = NULL;
	pcontext.is_case_when = false;
	pcontext.is_assertop = false;
	pcontext.whenexpr = NULL;
	pcontext.not_and_whenexpr = NULL;
	pcontext.case_when_type = InvalidOid;
	pcontext.parallel_scan = false;
	pcontext.has_callback_source = false;
	pcontext.is_append = false;
	pcontext.append_filed_index = 0;
	pcontext.is_materialize = false;
	pcontext.store_file = NULL;
	pcontext.chunk_size = 0;
	pcontext.store_type = IN_MEMORY;
	pcontext.is_sharescan = false;

	/* set build recipe for different plan node */
	switch(nodeTag(planstate))
	{
		case T_HashJoinState:
		{
			pcontext.is_hashjoin = true;
		}
		break;
		case T_NestLoopState:
		{
			pcontext.is_nestloopjoin = true;
		}
		break;
		case T_AggState:
		{
			Agg *agg = (Agg *)planstate->plan;

			if (!outerPlanState(planstate))
				elog(ERROR, "Agg node can't be leaf in vector plan");
			pcontext.inputschema = GetSchemaFromSlot(
					outerPlanState(planstate)->ps_ResultTupleSlot);
			pcontext.aggstrategy = agg->aggstrategy;
			pcontext.ishaving = true;
		}
		break;
		case T_WindowAggState:
		{
			if (!outerPlanState(planstate))
				elog(ERROR, "WindowAgg node can't be leaf in vector plan");
			pcontext.inputschema = GetSchemaFromSlot(
					outerPlanState(planstate)->ps_ResultTupleSlot);

			if (DEBUG1 >= log_min_messages)
			{
				g_autofree gchar *str = garrow_schema_to_string(pcontext.inputschema);
				elog(DEBUG1, "input schema for Windowagg: %s", str);
			}
		}
		break;
		case T_WindowHashAggState:
		{
			/*
			 * Arrow's ParallelWindowGroupByNode maintains thread-local
			 * sink state and invokes PG callbacks concurrently from its
			 * internal executor.  Running it inside a parallel Arrow
			 * pipeline (pool_threads>0) layers a second source of PG
			 * callback concurrency on top, which races with the window
			 * node's own thread pool and SIGSEGVs (reproducible on Q17-
			 * shaped rewrites with pool_threads=8).  Force this plan to
			 * run single-threaded to avoid the interaction.
			 */
			support_parallel = false;

			if (!outerPlanState(planstate))
				elog(ERROR, "WindowHashAgg node can't be leaf in vector plan");
			pcontext.inputschema = GetSchemaFromSlot(
					outerPlanState(planstate)->ps_ResultTupleSlot);

			if (DEBUG1 >= log_min_messages)
			{
				g_autofree gchar *str = garrow_schema_to_string(pcontext.inputschema);
				elog(DEBUG1, "input schema for WindowHashAgg: %s", str);
			}
		}
		break;
		case T_MaterialState:
		{
			VecMaterialState *vmatstate = (VecMaterialState *)planstate;
			support_parallel = false;
			if (!outerPlanState(planstate))
				elog(ERROR, "Material node can't be leaf in vector plan");
			pcontext.inputschema = GetSchemaFromSlot(
					outerPlanState(planstate)->ps_ResultTupleSlot);
			pcontext.is_materialize = true;
			pcontext.store_file = build_materialize_file_name();
			pcontext.chunk_size = 0;
			pcontext.store_type = IN_SINGLE;
			pcontext.cdb_strict = vmatstate->cdb_strict;
		}
		break;
		case T_ShareInputScanState:
		{
			VecShareInputScanState *vmatstate = (VecShareInputScanState *)planstate;

			/*
			 * VEC-SHARESCAN-NOPARALLEL
			 *
			 * ShareInputScan materialises in the producer and reads back in
			 * the consumers, so it depends on the producer's plan having run
			 * to completion before a consumer starts -- exactly like
			 * T_MaterialState above.  For a same-slice share there is no
			 * handshake at all to fall back on: Arrow's
			 * SharedSourceNode::RunConsumeLoop() only calls
			 * store_->SyncReader() when is_cross_ is false, and that call
			 * just tests for the .ready file with access(); the ordering
			 * itself comes from Vec Sequence running the producer subtree
			 * first.  A thread-pool execute context
			 * (garrow_execute_plan_new_with_context) turns the producer into
			 * an asynchronous start, so it returns before its data is
			 * materialised and the consumer fails with
			 * "ShareFileStore::SyncReader: ready file not found".
			 *
			 * Reproduced on TPC-DS q39 at SF1000: pool_threads=0 succeeds,
			 * while 1, 2 and 4 all fail after ~0.6s, i.e. it is the
			 * asynchronous start and not the degree of parallelism that
			 * breaks it.  Keep these plans on the synchronous start+wait
			 * path.
			 */
			support_parallel = false;

			pcontext.inputschema = vmatstate->vecdesc->schema;
			pcontext.is_sharescan = true;
			pcontext.gp_session_id = vmatstate->gp_session_id;
			pcontext.gp_command_count = vmatstate->gp_command_count;
			pcontext.share_id = vmatstate->share_id;
			pcontext.num_slices = vmatstate->num_slices;
			pcontext.is_cross_slice = vmatstate->is_cross_slice;
			pcontext.is_producer = vmatstate->is_producer;
			pcontext.ready = vmatstate->ready;
					pcontext.sinktype = Plain;
		}
		break;
		case T_ResultState:
		{
			if (!outerPlanState(planstate))
				elog(ERROR, "Result node can't be leaf in vector plan");
			pcontext.inputschema = GetSchemaFromSlot(
					outerPlanState(planstate)->ps_ResultTupleSlot);
		}
		break;
		case T_SubqueryScanState:
		{
			SubqueryScanState *node = (SubqueryScanState *) planstate;
			if (!node->subplan)
				elog(ERROR, "SubqueryScan node can't be leaf in vector plan");
			pcontext.inputschema = GetSchemaFromSlot(
				node->subplan->ps_ResultTupleSlot);
		}
		break;
		case T_SeqScanState:
		{
			VecSeqScanState *scanstate = (VecSeqScanState *)planstate;
			/* If qualification and projection are both empty,
			 * no need to build plan.
			 *
			 * Todo : project is initialized as ps_ProjInfo for PG first,
			 *		then test here to find whether we need to build project.
			 *		The ps_ProjInfo initialization need to be rewrite for
			 *		arrow plan, to find whether project is need.
			 */
			pcontext.inputschema = scanstate->vecdesc->schema;
			pcontext.map = scanstate->columnmap;
			pcontext.table_oid = scanstate->base.ss.ss_currentRelation->rd_id;
			pcontext.am_oid = scanstate->base.ss.ss_currentRelation->rd_rel->relam;
			pcontext.relation_schema = TupDescToSchema(RelationGetDescr(scanstate->base.ss.ss_currentRelation));
            pcontext.parallel_scan = garrow_dataset_is_registered(pcontext.am_oid);
		}
		break;
		case T_ForeignScanState:
		{
			VecForeignScanState *scanstate = (VecForeignScanState *)planstate;
			pcontext.inputschema = scanstate->schema;
			pcontext.map = scanstate->columnmap;
		}
		break;
		case T_CustomScanState:
		{
			VecCustomScanState *scanstate =
				GetVecCustomScanState((CustomScanState *) planstate);
			pcontext.inputschema = scanstate->schema;
			pcontext.map = scanstate->columnmap;
		}
		break;
		case T_AssertOpState:
		{
			pcontext.inputschema = GetSchemaFromSlot(outerPlanState(planstate)->ps_ResultTupleSlot);
			pcontext.is_assertop = true;
		}
		break;
		case T_SortState:
		{
			SortState *sortstate = (SortState*)planstate;
			PlanState  *outerNode = outerPlanState(sortstate);
			if (IsA(outerNode, SequenceState))
				outerNode = get_sequence_exact_result_ps(outerNode);
			pcontext.inputschema = GetSchemaFromSlot(outerNode->ps_ResultTupleSlot);
		}
		break;
		case T_LimitState:
		{
			if (!outerPlanState(planstate))
				elog(ERROR, "Limit node can't be leaf in vector plan");
			pcontext.inputschema = GetSchemaFromSlot(
					outerPlanState(planstate)->ps_ResultTupleSlot);
		}
		break;
		case T_PartitionTopKState:
		{
			if (!outerPlanState(planstate))
				elog(ERROR, "PartitionTopK node can't be leaf in vector plan");
			pcontext.inputschema = GetSchemaFromSlot(
					outerPlanState(planstate)->ps_ResultTupleSlot);
		}
		break;
		case T_SequenceState:
		{
			SequenceState *node = castNode(SequenceState, planstate);
			if (!node->subplans)
				elog(ERROR, "Sequence node can't be leaf in vector plan");
			pcontext.inputschema = GetSchemaFromSlot(
				node->subplans[node->numSubplans - 1]->ps_ResultTupleSlot);
			pcontext.is_left_schema = true;
			pcontext.subplan_index = 0;
		}
		break;
		case T_AppendState:
		{
					pcontext.sinktype = Plain;
			pcontext.is_append = true;	
		}
		break;
		default:
			elog(ERROR, "Build arrow plan from (%d) type is not support yet.",
					nodeTag(planstate->plan));
	}
	/*
	 * Execution mode selection:
	 * 1. push_pipeline: PAX ScanNode async in thread pool + backpressure
	 * 2. batch: start + wait, with yield mode when backpressure is armed
	 */
	/* Non-scan top nodes inherit parallel_scan from child PAX scans */
	if (!pcontext.parallel_scan && enable_vec_pipeline)
		pcontext.parallel_scan = HasPaxScanInSubtree(planstate);

	if (CanUsePushPipeline(&pcontext))
	{
		pcontext.sinktype = Plain;
		}

	/*
	 * Thread pool allocation:
	 * - push_pipeline plans (PAX ScanNode): need thread pool for async
	 *   scan fragment tasks.  Default to pool_threads or 4.
	 * - Non-push_pipeline plans (HashJoin, Agg, etc.): use pool_threads
	 *   for Arrow-internal parallelism (parallel hash build/probe).
	 *   Note: garrow_execute_context_new uses the global CPU thread
	 *   pool (GetCpuThreadPool) with SetCapacity(nthreads).  Each
	 *   MPPEXEC process has its own pool, so total threads across all
	 *   processes = MPPEXEC_count × pool_threads.  Tune pool_threads
	 *   to balance parallelism vs memory (e.g., Q4 with 43 MPPEXEC:
	 *   pool_threads=4 → 24GB/3min OOM; pool_threads=2 → ~15GB OK).
	 */
	/*
	 * VEC-POOL1-DEADLOCK-FIX: a pool capacity of 1 always deadlocks.
	 *
	 * An acero worker synchronously Wait()s on another Future inside
	 * MapNode::InputFinished, and that Future can only be resolved by a
	 * task still sitting in the same pool's queue.  With a capacity of 1
	 * there is no second thread left to drain the queue, so the single
	 * worker and the main thread both block forever in pthread_cond_wait:
	 *
	 *   Thread 1: garrow_execute_plan_wait -> FutureImpl::Wait
	 *   Thread N: MapNode::InputFinished   -> FutureImpl::Wait
	 *
	 * Measured on TPC-DS q37 at SF1000: pool_threads=0 finishes in 3708ms,
	 * while pool_threads=1 has not returned after six minutes at 0% CPU,
	 * waiting on IPC/Interconnect.
	 *
	 * A single worker is equivalent to serial execution anyway, so
	 * normalise 1 to 0 and take the synchronous path that the full
	 * regression already covers, rather than silently promoting it to 2 and
	 * handing back a degree of parallelism the user did not ask for.
	 */
	const int	eff_pool_threads = (pool_threads == 1) ? 0 : pool_threads;

	/*
	 * VEC-DIAG-CTX: record which context branch this plan took.  The three
	 * predicates below each walk the plan tree, so ask whether anyone is
	 * listening before paying for them.
	 */
	if (message_level_is_interesting(DEBUG1))
	{
		bool		d_push = CanUsePushPipeline(&pcontext);
		bool		d_sub = HasSameSliceShareScanInSubtree(planstate);
		bool		d_q = QueryHasSameSliceShareScan(planstate);
		const char *d_branch = d_push && !d_q ? "PUSH_CTX"
			: ((eff_pool_threads > 0 && support_parallel && !d_sub && !d_q) ? "POOL_CTX" : "SYNC");

		elog(DEBUG1, "VEC-DIAG-CTX pid=%d slice=%d node=%d branch=%s pool_threads=%d(raw=%d) support_parallel=%d "
			 "push=%d subtree_sameslice_share=%d query_sameslice_share=%d is_sharescan=%d is_producer=%d "
			 "is_cross_slice=%d parallel_scan=%d",
			 MyProcPid, currentSliceId,
			 planstate ? (int) nodeTag(planstate) : -1,
			 d_branch, eff_pool_threads, pool_threads, support_parallel ? 1 : 0,
			 d_push ? 1 : 0, d_sub ? 1 : 0, d_q ? 1 : 0,
			 pcontext.is_sharescan ? 1 : 0, pcontext.is_producer ? 1 : 0,
			 pcontext.is_cross_slice ? 1 : 0, pcontext.parallel_scan ? 1 : 0);
	}

	/*
	 * VEC-YIELD-SHARESCAN-GUARD: decide here, while planstate is in hand, and
	 * carry it to ExecuteVecPlan on the estate -- that function only receives
	 * the VecExecuteState.
	 */

	if (CanUsePushPipeline(&pcontext) &&
		!QueryHasSameSliceShareScan(planstate))	/* VEC-SHARESCAN-QUERYWIDE */
	{
		int nthreads = eff_pool_threads > 0 ? eff_pool_threads : 4;
		estate->exectx = garrow_execute_context_new(nthreads);
		pcontext.plan = garrow_execute_plan_new_with_context(estate->exectx, &error);
	}
	else if (eff_pool_threads > 0 && support_parallel &&
			 !HasSameSliceShareScanInSubtree(planstate) &&	/* VEC-SHARESCAN-SUBTREE */
			 !QueryHasSameSliceShareScan(planstate))			/* VEC-SHARESCAN-QUERYWIDE */
	{
		estate->exectx = garrow_execute_context_new(eff_pool_threads);
		pcontext.plan = garrow_execute_plan_new_with_context(estate->exectx, &error);
	}
	else
	{
		pcontext.plan = garrow_execute_plan_new(&error);
	}

	if (error)
		elog(ERROR, "Build arrow plan failed: plan node type %d.",
				nodeTag(planstate->plan));

	if (pcontext.is_nestloopjoin)
		BuildJoinPlan(&pcontext, estate);
	else if (pcontext.is_hashjoin)
		BuildJoinPlan(&pcontext, estate);
	else if (IsA(planstate, SequenceState))
		BuildSequencePlan(&pcontext, estate);
	else if (pcontext.is_materialize)
		BuildMaterializePlan(&pcontext, estate);
	else if (pcontext.is_sharescan)
		BuildShareScanPlan(&pcontext, estate);
	else
	{
		g_autoptr(GArrowExecuteNode) curnode = NULL;
		g_autoptr(GArrowExecuteNode) tmpnode = NULL;
		/* build plan */
		pcontext.is_left_schema = true;
		if (pcontext.is_append)
			curnode = BuildAppendPlan(&pcontext, estate);
		else
		{
			/*
			 * BuildScanNode for PAX tables: Arrow-native parallel
			 * scan with cooperative backpressure (pause/resume via
			 * PauseProducing/ResumeProducing).
			 *
			 * BuildSource for non-PAX: callback-based, one batch
			 * per ExecProcNode call.
			 */
			if (pcontext.parallel_scan && IsA(planstate, SeqScanState))
				curnode = BuildScanNode(&pcontext);
			else
				curnode = BuildSource(&pcontext);
		}
		/*
		 * For parallel scan (PAX SeqScan), split quals into fast filter
		 * quals (handled by PAX storage layer) and arrow quals (handled
		 * by Arrow FilterNode). Only pass arrow quals to BuildProject.
		 */
		if (pcontext.parallel_scan && qualList && IsA(planstate, SeqScanState))
		{
			VecSeqScanState *scanstate = (VecSeqScanState *)planstate;
			TupleDesc desc = RelationGetDescr(scanstate->base.ss.ss_currentRelation);
			List *arrowQuals = NIL;
			List *flatQuals = qualList;
			ListCell *lc;

			/* Unwrap single BoolExpr(AND) wrapper if present */
			if (list_length(flatQuals) == 1 && IsA(linitial(flatQuals), BoolExpr))
			{
				BoolExpr *boolexpr = (BoolExpr *)linitial(flatQuals);
				if (boolexpr->boolop == AND_EXPR)
					flatQuals = boolexpr->args;
			}

			foreach(lc, flatQuals)
			{
				Node *qual = (Node *)lfirst(lc);
				if (!PaxCanFastFilterC(qual, desc))
					arrowQuals = lappend(arrowQuals, qual);
			}

			qualList = arrowQuals;
		}

		tmpnode = BuildProject(targetList, qualList, curnode, &pcontext);
		garrow_store_ptr(curnode, tmpnode);
		
		BuildSink(curnode, estate, &pcontext);
	}

	if(!garrow_execute_plan_validate(pcontext.plan, &error))
		elog(ERROR, "Built invalid arrow plan: %s", garrow_take_error(&error));
	
	garrow_execute_plan_set_plan_id(pcontext.plan, plan_num++);
	/* set plan execute state */
	estate->started = false;
	estate->slot = planstate->ps_ResultTupleSlot;
	garrow_store_ptr(estate->plan, pcontext.plan);

	estate->push_pipeline = CanUsePushPipeline(&pcontext);
	estate->has_callback_source = pcontext.has_callback_source;
	if (enable_vec_pipeline && !estate->push_pipeline)
		elog(DEBUG1, "Backpressure: yield mode (not push_pipeline)");
	garrow_store_ptr(estate->reader, pcontext.reader);

	if (Debug_print_plan)
	{
		g_autofree gchar *str = garrow_execute_plan_to_string(estate->plan);
		elog(LOG, "arrow plan in BuildVecPlan: %s", str);
	}

	{
		static bool spill_cleanup_registered = false;

		if (!spill_cleanup_registered)
		{
			on_proc_exit(CleanupArrowSpillFiles, (Datum) 0);
			spill_cleanup_registered = true;
		}
	}

	/*
	 * TupDescToSchema() above handed back a new reference and this context is
	 * a local, so the reference dies with the frame unless it is released.
	 * BuildScanNode() normally drops it as soon as it has handed the schema to
	 * garrow_scan_node_options_new(), which leaves this a no-op; it stays as
	 * the backstop for the paths that never reach BuildScanNode().
	 */
	if (pcontext.relation_schema != NULL)
		ARROW_FREE(GArrowSchema, &pcontext.relation_schema);
}

static void 
BuildJoinPlan(PlanBuildContext *pcontext, VecExecuteState *estate)
{
	g_autoptr(GArrowExecuteNode) left_source_node = NULL;
	g_autoptr(GArrowExecuteNode) right_source_node = NULL;
	g_autoptr(GArrowExecuteNode) join_node = NULL;
	g_autoptr(GArrowExecuteNode) left_proj_node = NULL;
	g_autoptr(GArrowExecuteNode) right_proj_node = NULL;
	g_autoptr(GArrowExecuteNode) proj_node = NULL;
	GList *left_fields = NULL;
	GList *right_fields = NULL;
	List *targetList = pcontext->planstate->plan->targetlist;
	List *qualList = pcontext->planstate->plan->qual;
	List *joinqual = NIL;
	List *outerHashKeys = NIL;
	List *innerHashKeys = NIL;
	List *mergeQual = NIL;
	PlanState *planstate = pcontext->planstate;
	if (pcontext->is_nestloopjoin)
	{
		NestLoopState *nlstate = (NestLoopState *) pcontext->planstate;

		/*
		 * Take the qual list from the plan node, not from the ExprState.
		 *
		 * `js.joinqual->expr` is only a List when the state was built by
		 * ExecInitQual(), which stores the implicit-AND list as-is
		 * (execExpr.c).  JOIN_LASJ_NOTIN is initialized with ExecInitCheck()
		 * instead -- that is how the row executor makes a NULL qual result
		 * count as a match (nodeNestloop.c, nl_qualResultForNull) -- and
		 * ExecInitCheck() stores make_ands_explicit()'s output, i.e. a bare
		 * OpExpr for one clause or a BoolExpr for several.  Casting either of
		 * those to List and walking it reads garbage.
		 *
		 * Join.joinqual is the same list ExecInitQual() would have stored,
		 * already stripped of the IS NOT FALSE wrapper by createplan.c
		 * (remove_isnotfalse), so this is equivalent for every other join
		 * type and correct for LASJ_NOTIN as well.
		 */
		joinqual = ((Join *) nlstate->js.ps.plan)->joinqual;
	}
	else
	{
		HashJoinState *hjstate = (HashJoinState *) pcontext->planstate;
		HashState *hstate = (HashState *) innerPlanState(pcontext->planstate);
		if (hjstate->js.joinqual)
			joinqual = (List *) hjstate->js.joinqual->expr;
		outerHashKeys = hjstate->hj_OuterHashKeys;
		innerHashKeys = hstate->hashkeys;
	}

	/*
	 * Arrow Acero RIGHT_SEMI / RIGHT_ANTI: build hash table from inputs[1]
	 * (right), probe from inputs[0] (left), emit from inputs[1].
	 *
	 * Both ORCA (DXL translator swaps children in CTranslatorDXLToPlStmt.cpp)
	 * and the PG planner produce the same canonical layout: outer = probe
	 * side, inner = Hash(build/emit side).  The standard mapping (outer->Arrow
	 * left, inner->Arrow right) already puts the build side at inputs[1], so no
	 * swap is needed in the vectorization layer.
	 */
	PlanState *arrow_left_plan  = outerPlanState(planstate);
	PlanState *arrow_right_plan = innerPlanState(planstate);
	List *arrow_left_keys  = outerHashKeys;
	List *arrow_right_keys = innerHashKeys;
	List *left_numeric_fix = NIL;
	List *right_numeric_fix = NIL;

	/*
	 * Outer hash keys are evaluated on the join's outer input, inner ones
	 * by the Hash node on its input: both reference their input as
	 * OUTER_VAR of the node holding them.
	 */
	plan_numeric_join_keys(planstate->plan, arrow_left_keys,
						   arrow_right_plan->plan, arrow_right_keys,
						   &left_numeric_fix, &right_numeric_fix);

	/* build left tree (= Arrow left input) */
	pcontext->inputschema = GetSchemaFromSlot(arrow_left_plan->ps_ResultTupleSlot);
	left_fields = garrow_schema_get_fields(pcontext->inputschema);
	pcontext->left_in_schema = garrow_schema_new(left_fields);
	pcontext->is_left_schema = true;
	left_source_node = BuildSource(pcontext);
	pcontext->numeric_key_fix = left_numeric_fix;
	left_proj_node = BuildJoinProject(arrow_left_keys, left_source_node, pcontext);

	/* build right tree (= Arrow right input; build side for RIGHT_SEMI/ANTI) */
	pcontext->inputschema = GetSchemaFromSlot(arrow_right_plan->ps_ResultTupleSlot);
	right_fields = garrow_schema_get_fields(pcontext->inputschema);
	pcontext->right_in_schema = garrow_schema_new(right_fields);
	pcontext->is_left_schema = false;
	/* Recorded by build_join_project_options() for the build side below;
	 * consumed by the RIGHT_SEMI/ANTI Var resolver in BuildProject(). */
	pcontext->right_key_names = NIL;
	right_source_node = BuildSource(pcontext);
	pcontext->numeric_key_fix = right_numeric_fix;
	right_proj_node = BuildJoinProject(arrow_right_keys, right_source_node, pcontext);
	pcontext->numeric_key_fix = NIL;
	list_free(left_numeric_fix);
	list_free(right_numeric_fix);

	/* build hashjoin tree */
	pcontext->inputschema = NULL;
	pcontext->is_hashjoin_after_node = true;
	if (pcontext->is_nestloopjoin)
	{
		join_node = BuildNestLoopjoin(pcontext, left_proj_node, right_proj_node, joinqual);
		mergeQual = qualList;
	}
	else
	{
		HashJoinState *node = (HashJoinState *) pcontext->planstate;
		if (node->js.jointype == JOIN_INNER)
		{
			join_node = BuildHashjoin(pcontext, left_proj_node, right_proj_node, NULL);
			mergeQual = list_concat(joinqual, qualList);
		}
		else
		{
			join_node = BuildHashjoin(pcontext, left_proj_node, right_proj_node, joinqual);
			mergeQual = qualList;
		}
	}
	pcontext->inputschema = garrow_execute_node_get_output_schema(join_node);

	proj_node = BuildProject(targetList, mergeQual, join_node, pcontext);
	BuildSink(proj_node, estate, pcontext);

	/*
	 * When targetList is NULL (semi/anti joins used only for count(*)),
	 * the PG result slot TupleDesc has 0 attributes → TupDescToSchema
	 * creates a "dummy" null-type schema.  Parent nodes (e.g. AggState)
	 * that call GetSchemaFromSlot on our result slot then get the wrong
	 * schema.  Propagate the actual join output schema to the slot so
	 * the parent builds its Arrow plan against the real column types.
	 */
	if (!targetList)
	{
		GArrowSchema *join_out = garrow_execute_node_get_output_schema(join_node);
		VecTupleTableSlot *vslot = (VecTupleTableSlot *)
			planstate->ps_ResultTupleSlot;
		if (TTS_IS_VECTOR(&vslot->base))
		{
			garrow_store_ptr(vslot->vec_schema.schema, join_out);
			vslot->vec_schema.schema_rebuilt = true;
		}

		/*
		 * VEC-JOINOUT-UNREF-FIX
		 *
		 * garrow_store_ptr() steals its source ("Notice that SRC pointer
		 * will be stolen, cannot be release" -- utils/arrow.h) and leaves
		 * join_out NULL, so the unconditional g_object_unref() below used
		 * to run as g_object_unref(NULL) whenever the slot was a vector
		 * slot.  That trips g_return_if_fail(G_IS_OBJECT(object)) inside
		 * GLib, which logged
		 *
		 *   g_object_unref: assertion 'G_IS_OBJECT (object)' failed
		 *
		 * on every such plan build.  g_clear_object() is NULL-safe and
		 * releases the schema on the path where store_ptr did not run.
		 */
		g_clear_object(&join_out);
	}

	ARROW_FREE(GArrowSchema, &pcontext->left_in_schema);
	ARROW_FREE(GArrowSchema, &pcontext->right_in_schema);
	ARROW_FREE(GArrowSchema, &pcontext->inputschema);
	garrow_list_free_ptr(&left_fields);
	garrow_list_free_ptr(&right_fields);
	list_free_deep(pcontext->right_key_names);
	pcontext->right_key_names = NIL;
}

static void
BuildSequencePlan(PlanBuildContext *pcontext, VecExecuteState *estate)
{

	GList *sources = NULL;
	g_autoptr(GError) error = NULL;
	g_autoptr(GArrowExecuteNode) last_source = NULL;
	g_autoptr(GArrowSequenceNodeOptions) options = NULL;
	g_autoptr(GArrowExecuteNode) sequence_node = NULL;

	Assert(IsA(pcontext->planstate, SequenceState));
	SequenceState *ss = castNode(SequenceState, pcontext->planstate);

	/*
	 * The last subplan must be created as pipeline source and put into
	 * the source list firstly. It ensures that the last subplan will be
	 * run as the last source in arrow plan.
	 */
	pcontext->subplan_index = ss->numSubplans - 1;
	last_source = BuildSource(pcontext);
	sources = garrow_list_append_ptr(sources, last_source);

	for (int i = 0; i < ss->numSubplans -1; ++i)
	{
		g_autoptr(GArrowExecuteNode) source = NULL;

		pcontext->subplan_index = i;
		source = BuildSource(pcontext);
		sources = garrow_list_append_ptr(sources, source);
	}

	options = garrow_sequence_node_options_new();
	sequence_node = garrow_execute_plan_build_sequence_node(pcontext->plan,
															sources,
															options,
															&error);
	if (error)
		elog(ERROR, "Failed to create the sequence node, cause: %s", garrow_take_error(&error));

	BuildSink(sequence_node, estate, pcontext);

	garrow_list_free_ptr(&sources);
}

/*
 * Drop this estate's time collector from Arrow's process-wide registry.
 *
 * The span exporter thread may still hold a reference to the collector, in
 * which case the object stays alive until that export finishes; it simply
 * stops receiving spans.
 */
static void
ReleaseVecTimeCollector(VecExecuteState *estate)
{
	if (estate->time_collector == NULL)
		return;

	garrow_time_collector_table_erase(estate->time_collector_plan_id);
	g_object_unref(estate->time_collector);
	estate->time_collector = NULL;
	estate->time_collector_plan_id = -1;
}

/*
 * Create the time collector of an Arrow plan and publish it under plan_id.
 *
 * PostBuildVecPlan() builds a second, merged plan for the same estate, so a
 * collector published earlier has to be released here rather than leaked.
 */
static void
SetVecTimeCollector(VecExecuteState *estate, int plan_id, int node_num)
{
	ReleaseVecTimeCollector(estate);

	estate->time_collector = garrow_time_collector_new(node_num);
	estate->time_collector_plan_id = plan_id;
	garrow_time_collector_table_put(estate->time_collector, plan_id);
}

void
FreeVecExecuteState(VecExecuteState *estate)
{
	ReleaseVecTimeCollector(estate);

	if (estate->plan)
	{
		glib_autoptr_cleanup_GArrowExecutePlan(&estate->plan);
		estate->plan = NULL;
	}

	/*
	 * Release the execute context after the plan, never before: ExecPlan
	 * holds a bare pointer to the arrow::compute::ExecContext that lives
	 * inside this GObject, and so do the nodes -- after a MergeChildren a
	 * node can still be pointing at the context of the plan it came from.
	 * The teardown order PostgreSQL uses makes that safe, because a parent
	 * node frees its own VecExecuteState before ending its children.
	 *
	 * Nothing released this before.  garrow_execute_context_new() is called
	 * once per plan whenever vector.pool_threads is above zero, so every
	 * such query leaked the context object and the ExecContext in it.
	 */
	if (estate->exectx)
		ARROW_FREE(GArrowExecuteContext, &estate->exectx);

	if (estate->reader)
	{
		glib_autoptr_cleanup_GArrowRecordBatchReader(&estate->reader);
		estate->reader = NULL;
	}

	if (estate->arrow_node_to_planstate != NIL)
	{
		list_free(estate->arrow_node_to_planstate);
		estate->arrow_node_to_planstate = NIL;
	}
}

/* ------------------------------------------------------------------------
 *
 * Execute an Arrow plan to get batch result.
 *
 * ------------------------------------------------------------------------
 */
/*
 * VEC-CANCEL-FIX: true only on the thread that drives ExecuteVecPlan.
 *
 * The PG-side source callbacks below (get_*_next_batch) are invoked by arrow.
 * Under SYNC they run on this backend's own thread, but once a thread pool is
 * attached they can run on an arrow worker.  CHECK_FOR_INTERRUPTS() must never
 * fire off-thread -- its longjmp would unwind a stack PostgreSQL does not own.
 * Thread-local storage settles it: workers never enter ExecuteVecPlan, so the
 * flag stays false for them.
 */
static __thread bool vec_is_plan_driver_thread = false;

/*
 * VecHostCancelPending
 *
 * True when a pending interrupt is one that will make CHECK_FOR_INTERRUPTS()
 * fail the query: a cancel (Ctrl+C, statement_timeout, pg_cancel_backend), a
 * termination request, or a client known to be gone.  InterruptPending alone
 * is not enough -- sinval catchup, ProcSignalBarrier and the like set it too,
 * and must not cost the query anything.  QueryFinishPending is deliberately
 * left out: on a QE it means the QD has seen enough rows, which ends the
 * slice normally.
 *
 * Only reads volatile sig_atomic_t flags, so any thread may call it.
 */
static inline bool
VecHostCancelPending(void)
{
	return QueryCancelPending || ProcDiePending || ClientConnectionLost;
}

/*
 * VecClientConnectionGone
 *
 * The probe client_connection_check_interval asks for, done from inside Arrow.
 *
 * Its timer only sets CheckClientConnectionPending; the socket is looked at
 * later, by pq_check_connection() in ProcessInterrupts().  A plan computing
 * inside Arrow never gets there, so a client that went away stayed unnoticed
 * until the computation ended -- possibly hours, with the whole gang waiting.
 * Look at the socket here instead, while the probe is pending.
 *
 * pq_check_connection() cannot be used: it edits the shared FeBeWaitSet and
 * may only run on the backend's main thread, while this may run on an Arrow
 * worker.  A zero-timeout poll() for POLLRDHUP asks the kernel the same
 * question (it is what WL_SOCKET_CLOSED maps to on Linux) and is safe from any
 * thread.  On a lost connection, set ClientConnectionLost exactly as
 * ProcessInterrupts() would; CHECK_FOR_INTERRUPTS() then raises the FATAL.
 * While the connection is fine the flag is left alone, so ProcessInterrupts()
 * still re-arms the timer the next time it runs.
 *
 * Until then the flag stays set, and Arrow calls this at every batch it hands
 * on -- thousands of times a second in a busy join.  Probe at most once per
 * client_connection_check_interval, the cadence PostgreSQL itself keeps.  The
 * timestamp is shared by all threads without a lock; a race only costs one
 * extra poll().
 */
static volatile int64 vec_last_client_probe_ms = 0;

/*
 * Whether the kernel reports that the peer closed the client socket.  Safe
 * from any thread: a zero-timeout poll() on the socket, nothing else.
 */
static bool
VecClientSocketClosed(void)
{
#ifdef POLLRDHUP
	struct pollfd pfd;

	if (MyProcPort == NULL || MyProcPort->sock == PGINVALID_SOCKET)
		return false;

	pfd.fd = MyProcPort->sock;
	pfd.events = POLLRDHUP;
	pfd.revents = 0;
	return poll(&pfd, 1, 0) > 0 &&
		(pfd.revents & (POLLRDHUP | POLLHUP | POLLERR | POLLNVAL)) != 0;
#else
	return false;
#endif
}

static bool
VecClientConnectionGone(void)
{
#ifdef POLLRDHUP
	struct timespec ts;
	int64		now_ms;

	if (!CheckClientConnectionPending || client_connection_check_interval <= 0)
		return false;

#ifdef CLOCK_MONOTONIC_COARSE
	clock_gettime(CLOCK_MONOTONIC_COARSE, &ts);
#else
	clock_gettime(CLOCK_MONOTONIC, &ts);
#endif
	now_ms = (int64) ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
	if (now_ms - vec_last_client_probe_ms < client_connection_check_interval)
		return false;
	vec_last_client_probe_ms = now_ms;

	if (VecClientSocketClosed())
	{
		/*
		 * Unlike a signal handler, this may run on another thread than the
		 * one that reads the flags, so make ClientConnectionLost visible no
		 * later than InterruptPending: ProcessInterrupts() clears
		 * InterruptPending before it looks at ClientConnectionLost.  The
		 * reading side is covered by VecHostCancelCheckpoint().
		 */
		ClientConnectionLost = true;
		pg_write_barrier();
		InterruptPending = true;
		return true;
	}
#endif
	return false;
}

/*
 * Nesting depth of VecPlanTeardownBegin() / VecPlanTeardownEnd().
 *
 * ExecutorRunWrapper() tears the vectorized plan tree down after an error was
 * raised -- often by CHECK_FOR_INTERRUPTS() or a Motion receiver deep inside an
 * Arrow callback, longjmp'ing straight over the Arrow frames.  Arrow never saw
 * that error, so the plan's thread-pool workers carry on with the tasks
 * already queued.  Nor can they learn it from the cancel flags: by the time
 * the error is raised ProcessInterrupts() has already cleared
 * QueryCancelPending.  Only the StopProducing() in the plan's destructor stops
 * them, and work in flight -- a join probe, a hash table build task -- does not
 * look at that; the destructor then waits for it, for at most 10 seconds.
 *
 * While this is non-zero, VecHostCancelTick() reports a cancel, so those
 * workers stop at their next batch handoff, inner join loop or scheduled task,
 * as they would on a real cancel, and the destructor's wait ends quickly.
 *
 * Written only by the backend's main thread, read by any thread.
 */
static volatile sig_atomic_t vec_plan_teardown_depth = 0;

void
VecPlanTeardownBegin(void)
{
	vec_plan_teardown_depth = vec_plan_teardown_depth + 1;
}

void
VecPlanTeardownEnd(void)
{
	Assert(vec_plan_teardown_depth > 0);
	vec_plan_teardown_depth = vec_plan_teardown_depth - 1;
}

/*
 * Is the plan tree being torn down after an error?
 *
 * Code that runs in a node's End function and would otherwise wait for
 * something another slice has to do needs this: on the teardown path the other
 * slices are aborting too, so the thing being waited for may never happen, and
 * ExecutorRunWrapper() holds interrupts off around the teardown, so the wait
 * cannot be broken by a cancel either.
 */
bool
VecPlanTeardownInProgress(void)
{
	return vec_plan_teardown_depth > 0;
}

/*
 * VecHostCancelTick
 *
 * Installed into Arrow as its host-cancel callback (HostCancelCheck()).  Arrow
 * asks it in its source nodes before each batch they produce, in its sink
 * nodes on each batch they receive, inside long join loops, before each
 * scheduled task, and while it waits for a shared-file lock or a paused scan
 * (see ExecPlan::CheckHostCancel()).  Returning FALSE makes Arrow stop the plan
 * with a Cancelled status through ordinary C++ returns -- nothing longjmps out
 * of Arrow -- and ExecuteVecPlan then raises the real reason through
 * CHECK_FOR_INTERRUPTS() (see VecHostCancelCheckpoint()).
 *
 * It also performs the client_connection_check_interval probe when one is
 * due (VecClientConnectionGone()), and stops plans that are being torn down
 * after an error (see vec_plan_teardown_depth).
 *
 * This runs on whichever thread is executing the plan, Arrow thread-pool
 * workers included, so it must not do anything but read flags, poll the
 * client socket, and -- on the owner thread only -- hand arrow's memory
 * figure to the vmem tracker.
 *
 * That last one is here because this is the only place the backend gets
 * control while a blocking node is running.  A hash join building its table
 * consumes its whole build side inside one ExecProcNode call, and the
 * reconcile that follows that call would otherwise be the first the tracker
 * heard of however many gigabytes went into it.  Arrow polls this callback
 * inside those loops, so the figure now moves while they run.
 *
 * It must not raise: a longjmp from here would unwind through arrow's C++
 * frames and skip every destructor.  So a refused reservation is not reported
 * here -- VecReconcileVmemNoRaise() leaves the accounting untouched and
 * returns false, and the reconcile after the enclosing ExecProcNode raises it
 * properly on the driver thread.  Returning FALSE instead would cancel the
 * plan, but with no interrupt pending the query would stop without a reason
 * to show for it.
 */
static gboolean
VecHostCancelTick(gpointer user_data pg_attribute_unused())
{
	if (vec_plan_teardown_depth > 0 || VecHostCancelPending() ||
		VecClientConnectionGone())
		return FALSE;

	(void) VecReconcileVmemNoRaise();

	return TRUE;
}

/*
 * VecHostCancelCheckpoint
 *
 * Call on the plan driver thread whenever Arrow hands back an error or the end
 * of a stream.  When the query is being cancelled, let CHECK_FOR_INTERRUPTS()
 * raise the real reason ("canceling statement due to user request",
 * "... statement timeout", "terminating connection ...") instead of Arrow's
 * Cancelled status.  If interrupts are held off and it returns, fail anyway:
 * whatever Arrow produced after the cancel may be cut short and must never be
 * handed upward as a complete result.
 */
static inline void
VecHostCancelCheckpoint(void)
{
	if (likely(!VecHostCancelPending()))
		return;

	/*
	 * An Arrow worker that saw the client go away set ClientConnectionLost
	 * and InterruptPending, but on a weakly ordered CPU this thread may have
	 * observed the second before the first and already consumed it in an
	 * earlier ProcessInterrupts() that found nothing to do.  Re-arm it from
	 * here: this thread has now seen ClientConnectionLost, and its own store
	 * is ordered before its own read in ProcessInterrupts().
	 */
	if (ClientConnectionLost)
		InterruptPending = true;

	CHECK_FOR_INTERRUPTS();
	ereport(ERROR,
			(errcode(ERRCODE_QUERY_CANCELED),
			 errmsg("canceling vectorized execution")));
}

/* elog(ERROR) for a failed Arrow call, reporting a pending cancel first. */
#define VEC_PLAN_ERROR(...) \
	do { \
		VecHostCancelCheckpoint(); \
		elog(ERROR, __VA_ARGS__); \
	} while (0)

/* Hand a batch upward; at end of stream make sure it was not a cancel. */
static inline TupleTableSlot *
VecStorePlanBatch(TupleTableSlot *slot, GArrowRecordBatch *batch)
{
	if (batch == NULL)
		VecHostCancelCheckpoint();
	return ExecStoreBatch(slot, batch);
}

/*
 * VEC-CANCEL-FIX: give PostgreSQL a chance to act on a pending interrupt.
 *
 * A vectorized query spends nearly all of its time inside arrow calls that
 * never reach a CHECK_FOR_INTERRUPTS() -- chiefly
 * garrow_record_batch_reader_read_next(), which blocks until the sink has a
 * batch.  Cancellation only sets InterruptPending and signals the process, so
 * a backend parked in there ignored pg_cancel_backend() and
 * statement_timeout outright; only pg_terminate_backend() could remove it.
 *
 * Call this right before each such blocking call.  When nothing is pending it
 * costs a single flag test.  When the pending interrupt will end the query,
 * stop the arrow plan first so its worker threads stop touching executor
 * state, then let CHECK_FOR_INTERRUPTS() unwind.  The post-stop wait is
 * bounded: cancelling must not itself be able to hang on a plan that refuses
 * to wind down.
 *
 * Only then: InterruptPending is also set by interrupts that end nothing --
 * the client_connection_check_interval timer, sinval catchup,
 * pg_log_backend_memory_contexts.  Stopping the plan for those made the sink
 * close its output as if the data had run out, CHECK_FOR_INTERRUPTS()
 * returned normally, and the query returned a truncated result -- with a
 * thread pool and client_connection_check_interval set, every query whose plan
 * ran past the first check.
 *
 * The client check is the one such interrupt that can turn fatal, and it
 * does so inside ProcessInterrupts(), where a FATAL exits without passing an
 * error handler that would stop the plan.  So when the check is due, look at
 * the socket here first; a lost connection then counts as a cancel.
 */
static inline void
VecCheckForInterrupts(VecExecuteState *estate)
{
	if (!INTERRUPTS_PENDING_CONDITION())
		return;

	if (CheckClientConnectionPending && client_connection_check_interval > 0 &&
		!ClientConnectionLost && VecClientSocketClosed())
		ClientConnectionLost = true;

	if (VecHostCancelPending() &&
		estate != NULL && estate->plan != NULL && estate->started)
	{
		garrow_execute_plan_stop(estate->plan);
		(void) garrow_execute_plan_wait_for(estate->plan, 5.0, NULL);
	}
	CHECK_FOR_INTERRUPTS();
}

TupleTableSlot *
ExecuteVecPlan(VecExecuteState *estate)
{
	g_autoptr(GArrowRecordBatch) batch = NULL;
	g_autoptr(GError) error = NULL;

	vec_is_plan_driver_thread = true;	/* VEC-CANCEL-FIX */

	/*
	 * Install the host-cancel callback once per process.  It is a single
	 * process-global std::function in Arrow that worker threads of a running
	 * plan may be calling at any moment; assigning it again while they do
	 * would be a data race.  Nothing ever clears it.
	 */
	{
		static bool host_cancel_installed = false;

		if (!host_cancel_installed)
		{
			garrow_set_host_cancel_callback(VecHostCancelTick, NULL);
			host_cancel_installed = true;
		}
	}

	/*
	 * Build deferred by PostBuildVecPlan(): exec params referenced by this
	 * node were still pending at ExecInitNode time.  They have been
	 * evaluated by now (preprocess_initplans() ran before local execution
	 * starts; utility mode evaluates through ExecSetParamPlan() inside
	 * expr_to_arrow_expression), so build the Arrow plan here.
	 */
	if (estate->plan == NULL && estate->build_deferred)
		PostBuildVecPlan(estate->deferred_ps, estate);

	/*
	 * push_pipeline assumes every source in the plan produces asynchronously
	 * on the thread pool (PAX ScanNode) so that start() can return and this
	 * thread becomes the sink reader.  A callback SourceNode however runs
	 * its whole legacy Loop inline during start(): its generator (PG
	 * ExecProcNode) completes futures synchronously.  Once SinkNode
	 * backpressure pauses it, the source blocks in pause_cv_.wait on this
	 * very thread -- the only thread that could drain the sink and resume
	 * it -- deadlocking the process (TPC-DS Q78: Result top node over a
	 * HashJoin with an un-merged Motion bridge classified push_pipeline
	 * because a PAX scan sits deeper in the same fragment).  Demote such
	 * plans to yield mode, where sources yield on pause and this thread
	 * interleaves reads with ContinueYieldedSources.
	 */
	if (!estate->started && estate->push_pipeline &&
		estate->has_callback_source)
	{
		elog(DEBUG1, "BP: demote push_pipeline to yield mode pid=%d plan=%d",
			 MyProcPid, garrow_execute_plan_get_id(estate->plan));
		estate->push_pipeline = false;
	}

	if (estate->push_pipeline)
	{
		/*
		 * Push-pipeline: ScanNode multi-threaded → SinkNode with backpressure.
		 *
		 * Call start() to kick off ScanNode threads (do NOT call wait()).
		 * ScanNode threads push batches into SinkNode's PushGenerator queue.
		 * Backpressure is automatic: when queue exceeds high watermark, the
		 * AsyncToggle calls PauseProducing → ScanNode threads block in
		 * WaitIfPaused(). When we read from the reader and queue drops below
		 * low watermark, ResumeProducing unblocks the scan threads.
		 */
		if (!estate->started)
		{
			bool isok;
			isok = garrow_execute_plan_start(estate->plan, &error);
			if (!isok || error)
				VEC_PLAN_ERROR("Start push-pipeline plan error: %s.",
				               garrow_take_error(&error));
			estate->started = true;
		}

		VecCheckForInterrupts(estate);
		batch = garrow_record_batch_reader_read_next(estate->reader, &error);
		if (error)
			VEC_PLAN_ERROR("Push-pipeline read error: %s.", garrow_take_error(&error));

		return VecStorePlanBatch(estate->slot, batch);
	}
	else if (enable_vec_pipeline && !estate->yield_unsafe)
	{
		/*
		 * Yield mode with backpressure.  Single-threaded by design, so it
		 * does not require a thread pool (estate->exectx may be NULL, e.g.
		 * pool_threads=0 or support_parallel=false plans).  It MUST cover
		 * every plan that can have SinkNode backpressure armed: BuildSink
		 * arms it whenever enable_vec_pipeline is on, and the batch-mode
		 * start+wait below cannot answer a backpressure pause (nobody
		 * reads the sink until wait() returns), which self-deadlocks just
		 * like the push_pipeline misclassification above.
		 *
		 * SourceNode's RunYieldLoop processes batches synchronously
		 * (one at a time) through the pipeline.  When SinkNode bp
		 * triggers PauseProducing, the SourceNode yields (returns).
		 * We read available batches from PushGenerator, then call
		 * ContinueYieldedSources to resume.
		 *
		 * This gives memory-bounded execution: at any moment, only
		 * a few batches are in-flight between SourceNode and SinkNode.
		 * Unlike async mode (Loop+executor->Submit) which submits
		 * batches to thread pool without waiting, yield mode processes
		 * each batch fully before producing the next.
		 *
		 * Combined with SinkNode backpressure (set in BuildSink),
		 * this prevents OOM from push model data accumulation.
		 */
		if (!estate->started)
		{
			bool isok;
			elog(DEBUG1, "BP: yield mode START pid=%d plan=%d node=%d",
				 MyProcPid,
				 garrow_execute_plan_get_id(estate->plan),
				 nodeTag(estate->slot->tts_ops));
			garrow_execute_plan_enable_source_yield(estate->plan);
			isok = garrow_execute_plan_start(estate->plan, &error);
			if (!isok || error)
				VEC_PLAN_ERROR("Start plan error: %s.",
				               garrow_take_error(&error));
			estate->started = true;
			estate->yield_remaining =
				garrow_execute_plan_get_yielded_batch_count(estate->plan);
		}

		/*
		 * Yield mode flow:
		 * 1. SourceNode produces batches synchronously via RunYieldLoop
		 * 2. After yield_batch_limit batches, it yields (returns)
		 * 3. We read those batches from SinkNode PushGenerator queue
		 * 4. When all read, we call ContinueYieldedSources to resume
		 * 5. Repeat until SourceNode finishes all data
		 *
		 * yield_remaining is the SinkNode PushGenerator queue depth
		 * (garrow_execute_plan_get_yielded_batch_count), so read_next is
		 * only called when a batch is actually queued and thus never
		 * blocks on an empty queue.  When the depth is 0 we resume the
		 * yielded source instead of reading — this is what keeps a
		 * blocking operator (HashAgg/HashJoin build) between source and
		 * sink from deadlocking the single yield thread.
		 */
		if (estate->yield_remaining > 0)
		{
			VecCheckForInterrupts(estate);
			batch = garrow_record_batch_reader_read_next(estate->reader,
														 &error);
			if (error)
				VEC_PLAN_ERROR("Read error: %s.", garrow_take_error(&error));
			estate->yield_remaining--;
			return VecStorePlanBatch(estate->slot, batch);
		}

		while (garrow_execute_plan_has_yielded_source(estate->plan))
		{
			bool isok;
			isok = garrow_execute_plan_continue_yielded(
						estate->plan, &error);
			if (!isok || error)
				VEC_PLAN_ERROR("Continue yielded error: %s.",
				               garrow_take_error(&error));
			estate->yield_remaining =
				garrow_execute_plan_get_yielded_batch_count(estate->plan);
			if (estate->yield_remaining > 0)
			{
				VecCheckForInterrupts(estate);
				batch = garrow_record_batch_reader_read_next(
							estate->reader, &error);
				if (error)
					VEC_PLAN_ERROR("Read error: %s.", garrow_take_error(&error));
				estate->yield_remaining--;
				return VecStorePlanBatch(estate->slot, batch);
			}
			/*
			 * yield_remaining == 0: blocking operator (e.g. HashJoin
			 * build phase) consumed all batches without producing
			 * output to SinkNode.  Continue yielding to feed more
			 * data to the blocking operator until it starts producing.
			 */
		}

		/* Source finished: read remaining batches from queue */
		VecCheckForInterrupts(estate);
		batch = garrow_record_batch_reader_read_next(estate->reader,
													 &error);
		if (error)
			VEC_PLAN_ERROR("Read error: %s.", garrow_take_error(&error));
		return VecStorePlanBatch(estate->slot, batch);
	}
	else
	{
		/*
		 * Pure batch mode: backpressure disabled (enable_vec_pipeline=off,
		 * so BuildSink never armed the SinkNode toggle).  Safe to
		 * start+wait only under that premise: sources run to completion
		 * on the calling thread and a pause that nobody could answer can
		 * never be requested.
		 */
		if (!estate->started)
		{
			bool isok;
			elog(DEBUG1, "BP: batch mode START pid=%d plan=%d",
				 MyProcPid,
				 garrow_execute_plan_get_id(estate->plan));
			isok = garrow_execute_plan_start(estate->plan, &error);
			if (!isok || error)
				VEC_PLAN_ERROR("Start plan for vector plan error: %s.",
				               garrow_take_error(&error));

			/*
			 * VEC-CANCEL-FIX: wait in slices so the query stays cancellable.
			 *
			 * garrow_execute_plan_wait() parks this thread in an unbounded
			 * arrow::FutureImpl::Wait().  A cancel request only sets
			 * InterruptPending and signals the process; it takes effect at the
			 * next CHECK_FOR_INTERRUPTS(), which a parked thread never reaches.
			 * That is why a vectorized query used to ignore
			 * pg_cancel_backend() and statement_timeout entirely, and only
			 * pg_terminate_backend() could get rid of it.
			 *
			 * Poll in 100ms slices instead.  When an interrupt is pending, stop
			 * the arrow plan first so its worker threads are no longer touching
			 * the executor state, then let CHECK_FOR_INTERRUPTS() unwind.  The
			 * post-stop wait is itself bounded: cancellation must not be able
			 * to hang on a plan that refuses to wind down.
			 */
			estate->started = true;
			for (;;)
			{
				if (garrow_execute_plan_wait_for(estate->plan, 0.1, &error))
					break;			/* finished; error carries its status */

				VecCheckForInterrupts(estate);
			}
			if (error)
				VEC_PLAN_ERROR("Execute plan for vector plan error: %s.",
				               garrow_take_error(&error));
		}

		VecCheckForInterrupts(estate);
		batch = garrow_record_batch_reader_read_next(estate->reader,
													 &error);
		if (error)
			VEC_PLAN_ERROR("Read error: %s.", garrow_take_error(&error));
		return VecStorePlanBatch(estate->slot, batch);
	}
	pg_unreachable();
}

/* build sort keys
 *
 * Outputs:
 *   GList of GArrowSortKey.
 */
GList *
build_sort_keys(PlanState *planstate, GArrowSchema *schema)
{
	Assert(IsA(planstate->plan, Sort));
	SortSupport sortKeys;
	Sort *plannode = (Sort *)planstate->plan;
	int nkeys = plannode->numCols;
	GList *keys = NULL;
	PlanState  *outerNode = outerPlanState(planstate);
	if (IsA(outerNode, SequenceState))
		outerNode = get_sequence_exact_result_ps(outerNode);
	sortKeys = (SortSupport) palloc0(nkeys * sizeof(SortSupportData));
	for (int i = 0; i < nkeys; i++)
	{
		SortSupport sortKey = sortKeys + i;

		sortKey->ssup_cxt = CurrentMemoryContext;
		sortKey->ssup_collation = plannode->collations[i];
		sortKey->ssup_nulls_first = plannode->nullsFirst[i];
		sortKey->ssup_attno = plannode->sortColIdx[i];
		/* Convey if abbreviation optimization is applicable in principle */
		sortKey->abbreviate = (i == 0);

		PrepareSortSupportFromOrderingOp(plannode->sortOperators[i], sortKey);
	}
	keys = create_sort_keys(sortKeys, nkeys, schema);
	pfree(sortKeys);
	return keys;
}

void static
BuildSink(GArrowExecuteNode *input, VecExecuteState *estate, PlanBuildContext *pcontext)
{
	Plan *plan = pcontext->planstate->plan;
	g_autoptr(GArrowSchema) schema = NULL;
	g_autoptr(GArrowExecuteNode) sink = NULL;
	g_autoptr(GArrowSinkNodeOptions) options = NULL;
	g_autoptr(GArrowRecordBatchReader) sinkreader = NULL;
	g_autoptr(GError) error = NULL;

	schema = garrow_execute_node_get_output_schema(input);

	switch(pcontext->sinktype)
	{
		case Plain:
		{
			options = garrow_sink_node_options_new();
			/*
			 * A Dataset ScanNode with no thread pool scans inline inside
			 * StartProducing(), so pausing it parks the one thread that could
			 * drain the sink and lift the pause: the query then hangs with no
			 * way back.  TPC-DS q31/q74/q79 did exactly that with
			 * enable_vec_pipeline on and pool_threads=0.  Leave backpressure
			 * off for those plans -- which is how they already behave with
			 * enable_vec_pipeline off, so it costs nothing that was working.
			 *
			 * Callback SourceNode plans keep it: they yield on pause instead
			 * of blocking, and ExecuteVecPlan's yield mode drives them.
			 *
			 * The test has to look at the whole subtree, not at whether this
			 * node built a ScanNode itself: MergeChildren splices a child's
			 * ScanNode into this plan after BuildSink has already run.
			 * SubtreeHasDatasetScan() stops at Motion, which is exactly the
			 * plan boundary we care about; see its header for why it asks the
			 * plan shape rather than reusing HasPaxScanInSubtree().
			 */
			if (enable_vec_pipeline && !estate->yield_unsafe &&
				backpressure_memory_mb > 0 &&
				(estate->exectx != NULL ||
				 !SubtreeHasDatasetScan(pcontext->planstate)))
			{
				/*
				 * Memory-based backpressure on SinkNode for ALL plans
				 * with backpressure enabled.
				 *
				 * For push_pipeline plans: controls ScanNode's async
				 * output via PushGenerator toggle.
				 *
				 * For yield-mode plans (non-push_pipeline with thread
				 * pool): SinkNode bp PauseProducing propagates through
				 * the Arrow node chain to Dataset ScanNode, which
				 * responds with cooperative pause (saves scan state,
				 * releases thread to pool).  ResumeProducing re-submits
				 * paused scan task when PushGenerator queue drains.
				 * This prevents ScanNode from producing unbounded data
				 * (Q4: 480M rows store_sales → 1.7GB/proc without bp).
				 *
				 * For callback SourceNode: bp PauseProducing → yield
				 * in RunYieldLoop.  yield_remaining may be 0 if bp
				 * triggers before batches reach SinkNode (HashJoin
				 * build absorbs them).  The while loop in ExecuteVecPlan
				 * handles this by re-continuing until data appears.
				 */
				int64 memory_budget = (int64)backpressure_memory_mb *
									  1024 * 1024;
				garrow_sink_node_options_set_backpressure(options,
					memory_budget);
			}
			sink = garrow_execute_plan_build_sink_node(pcontext->plan,
																 input,
																 options,
																 &error);

		}
		break;
		default:
			elog(ERROR, "Build arrow plan from (%d) type is not support yet.",
					nodeTag(plan));
	}

	sinkreader = garrow_sink_node_options_get_reader(
					GARROW_SINK_NODE_OPTIONS(options), schema);
	garrow_store_ptr(pcontext->reader, sinkreader);

	if (error)
		elog(ERROR, "Failed to create sink node, cause: %s", garrow_take_error(&error));
}

// is not distinct equals to  (a == b) or (a is null and b is null)
// is distinct equals to (a != b) or (b is null and a is not null) or (a is null and  b is not null)
// converts to coalesce(a != b, (b is null and a is not null) or (a is null and  b is not null))
static GArrowExpression *
build_is_distinct_expression(DistinctExpr *dex, PlanBuildContext *pcontext)
{
	GList *arguments = NULL;
	g_autoptr(GArrowExpression) a_expr = NULL;
	g_autoptr(GArrowExpression) b_expr = NULL;
	g_autoptr(GArrowExpression) is_distinct = NULL;

	Assert(list_length(dex->args) == 2);

	Expr *a = (Expr *) linitial(dex->args);
	Expr *b = (Expr *) lsecond(dex->args);
	a_expr = expr_to_arrow_expression(a, pcontext);
	b_expr = expr_to_arrow_expression(b, pcontext);

	is_distinct = garrow_is_distinct_expression_new(a_expr, b_expr);

	garrow_list_free_ptr(&arguments);
	return garrow_move_ptr(is_distinct);
}

static GArrowExpression *
build_null_if_expression(NullIfExpr *dex, PlanBuildContext *pcontext)
{
	// nullif can be regarded as a special casewhen expression, 
	// so casewhen is used here for processing
	GList *when_arguments = NULL;
	g_autoptr(GArrowExpression) expr = NULL;
	g_autoptr(GArrowExpression) struct_expr;
	g_autoptr(GArrowExpression) null_expr;
	g_autoptr(GArrowMakeStructOptions) options;
	GList *expr_arguments = NULL;
	gchar **fields;

	Assert(list_length(dex->args) == 2);
	expr = func_args_to_expression(dex->args, pcontext, "equal");
	when_arguments = garrow_list_append_ptr(when_arguments, expr);

	fields = palloc(1 * (sizeof(gchar *)));
	fields[0] = palloc(sizeof(int) + 6);
	snprintf(fields[0], sizeof(int) + 6, "case_%d", 0);
	options = garrow_make_struct_options_new((const gchar **) fields, 1);

	struct_expr = GARROW_EXPRESSION(garrow_call_expression_new(
			"make_struct", when_arguments, GARROW_FUNCTION_OPTIONS(options)));
	expr_arguments = garrow_list_prepend_ptr(expr_arguments, struct_expr);
	
	Oid type = exprType((Node *) linitial(dex->args));
	null_expr = build_literal_expression(0, true, type, -1);
	expr_arguments = garrow_list_append_ptr(expr_arguments, null_expr);
	
	// Use the first arg as the return value
	expr = expr_to_arrow_expression((Expr *) linitial(dex->args), pcontext);
	expr_arguments = garrow_list_append_ptr(expr_arguments, expr);
	
	expr = GARROW_EXPRESSION(garrow_call_expression_new("case_when", expr_arguments, NULL));

	pfree(fields);
	garrow_list_free_ptr(&when_arguments);
	garrow_list_free_ptr(&expr_arguments);
	return garrow_move_ptr(expr);
}


static GArrowExpression *build_filter_expression(List *filterInfo,
												 PlanBuildContext *pcontext)
{
	g_autoptr(GArrowExpression) expr = NULL;

	if (list_length(filterInfo) > 1)
		expr = func_args_to_expression(filterInfo, pcontext, "and_kleene");
	else
		expr = expr_to_arrow_expression((Expr *)linitial(filterInfo), pcontext);
	return garrow_move_ptr(expr);
}

static GArrowFilterNodeOptions *build_filter_options(List *filterInfo,
													 PlanBuildContext *pcontext)
{
	g_autoptr(GArrowFilterNodeOptions) filter_options = NULL;
	g_autoptr(GArrowExpression) expr = build_filter_expression(filterInfo, pcontext);
	filter_options = garrow_filter_node_options_new(expr);
	return garrow_move_ptr(filter_options);
}

static GArrowAssertOpNodeOptions *build_assertop_options(List *filterInfo, PlanBuildContext *pcontext)
{
        g_autoptr(GArrowAssertOpNodeOptions) assertop_filter_options = NULL;
        g_autoptr(GArrowExpression) expr = build_filter_expression(filterInfo, pcontext);
        assertop_filter_options = garrow_assertop_node_options_new(expr);
        return garrow_move_ptr(assertop_filter_options);
}

static void *
get_scan_next_batch(PlanState *node)
{
	/*
	 * VEC-CANCEL-FIX: arrow pulls batches through here.  Under SYNC the whole
	 * plan runs inside a single read_next(), so this callback is the only
	 * place PostgreSQL regains control often enough to honour a cancel.
	 * Guarded to the driver thread -- see vec_is_plan_driver_thread.
	 */
	if (vec_is_plan_driver_thread)
		CHECK_FOR_INTERRUPTS();

	VecSeqScanState *vnode = (VecSeqScanState *)node;
	TupleTableSlot *slot;

	while (true)
	{
		slot = ExecVecScanFetch((ScanState *)node,
								(ExecScanAccessMtd)VecSeqNext,
								(ExecScanRecheckMtd)VecSeqRecheck,
								vnode->vscanslot);
		if (TupIsNull(slot))
			return NULL;
		if (!GetNumRows(slot))
			continue;
		break;
	}

	Assert(!TTS_IS_DIRTY(slot));
	return (void *) GetBatch(slot);
}

static void *
get_foreign_next_batch(PlanState *node)
{
	/*
	 * VEC-CANCEL-FIX: arrow pulls batches through here.  Under SYNC the whole
	 * plan runs inside a single read_next(), so this callback is the only
	 * place PostgreSQL regains control often enough to honour a cancel.
	 * Guarded to the driver thread -- see vec_is_plan_driver_thread.
	 */
	if (vec_is_plan_driver_thread)
		CHECK_FOR_INTERRUPTS();

	VecForeignScanState *vnode = (VecForeignScanState *)node;
	TupleTableSlot *slot;

	while (true)
	{
		slot = ExecVecScanFetch((ScanState *)node,
								(ExecScanAccessMtd)ForeignNext,
								(ExecScanRecheckMtd)ForeignRecheck,
								vnode->vscanslot);
		if (TupIsNull(slot))
			return NULL;
		if (!GetNumRows(slot))
			continue;
		break;
	}

	Assert(!TTS_IS_DIRTY(slot));

	return (void *) GetBatch(slot);
}

static void *
get_custom_next_batch(PlanState *node)
{
	VecCustomScanState *vcss = GetVecCustomScanState((CustomScanState *) node);
	TupleTableSlot *slot;

	while (true)
	{
		slot = ExecVecScanFetch((ScanState *) node,
								(ExecScanAccessMtd) VecCustomNext,
								(ExecScanRecheckMtd) VecCustomRecheck,
								vcss->vscanslot);
		if (TupIsNull(slot))
			return NULL;
		if (!GetNumRows(slot))
			continue;
		break;
	}

	Assert(!TTS_IS_DIRTY(slot));
	return (void *) GetBatch(slot);
}

static void *
get_current_next_batch(PlanState *node)
{
	/*
	 * VEC-CANCEL-FIX: arrow pulls batches through here.  Under SYNC the whole
	 * plan runs inside a single read_next(), so this callback is the only
	 * place PostgreSQL regains control often enough to honour a cancel.
	 * Guarded to the driver thread -- see vec_is_plan_driver_thread.
	 */
	if (vec_is_plan_driver_thread)
		CHECK_FOR_INTERRUPTS();

	TupleTableSlot *slot;

	while (true) {
		slot = ExecProcNode(node);
		if (TupIsNull(slot))
			return NULL;
		if (!GetNumRows(slot))
			continue;
		break;
	}

	Assert(!TTS_IS_DIRTY(slot));
	return (void *) GetBatch(slot);
}

static GArrowExecuteNode *
BuildSource(PlanBuildContext *pcontext)
{
	g_autoptr(GError) error = NULL;
	g_autoptr(GArrowRecordBatchReader)	reader = NULL;
	g_autoptr(GArrowSourceNodeOptions)	options = NULL;
	g_autoptr(GArrowExecuteNode) source = NULL;
	AppendState *appendstate = NULL;
	PlanState **appendplanstates = NULL;

	PlanState *pstate = pcontext->planstate;
	GetNextCallback callback;
	switch(nodeTag(pcontext->planstate))
	{
		case T_SeqScanState:
			callback = (GetNextCallback) get_scan_next_batch;
			pstate = pcontext->planstate;
			break;
		case T_ForeignScanState:
			callback = (GetNextCallback) get_foreign_next_batch;
			pstate = pcontext->planstate;
			break;
		case T_CustomScanState:
			callback = (GetNextCallback) get_custom_next_batch;
			pstate = pcontext->planstate;
			break;
		case T_SequenceState:
			callback = (GetNextCallback) get_current_next_batch;
			SequenceState *ss = castNode(SequenceState, pcontext->planstate);
			pstate = ss->subplans[pcontext->subplan_index];
			pcontext->inputschema = GetSchemaFromSlot(pstate->ps_ResultTupleSlot);
			break;
		case T_AppendState:
			appendstate = (AppendState*)pcontext->planstate;
			appendplanstates = appendstate->appendplans;
			pstate = appendplanstates[pcontext->sub_slice];
			callback = (GetNextCallback) get_current_next_batch;
			break;
		case T_MotionState:
			callback = (GetNextCallback) get_current_next_batch;
			pstate = pcontext->planstate;
			break;
		case T_HashJoinState:
		case T_NestLoopState:
			callback = (GetNextCallback) get_current_next_batch;
			/*
			 * is_left_schema is in Arrow's frame (true = Arrow LEFT input).
			 * Normally Arrow LEFT input ← PG outer subtree and Arrow RIGHT
			 * input ← PG inner subtree. RIGHT_SEMI / RIGHT_ANTI swap (see
			 * BuildJoinPlan) feeds PG inner to Arrow LEFT and PG outer to
			 * Arrow RIGHT, so flip the lookup here too.
			 */
			if (pcontext->is_left_schema)
				pstate = outerPlanState(pcontext->planstate);
			else
				pstate = innerPlanState(pcontext->planstate);
			break;
		case T_SubqueryScanState:
			callback = (GetNextCallback) get_current_next_batch;
			SubqueryScanState *subscanstate = (SubqueryScanState *) pcontext->planstate;
			pstate = subscanstate->subplan;
			break;
		default:
			callback = (GetNextCallback) get_current_next_batch;
			pstate = outerPlanState(pcontext->planstate);
			break;
	}
	reader = garrow_record_batch_reader_new_callback(
		pcontext->inputschema,
		callback,
		pstate,
		&error);

	if (error)
		elog(ERROR, "Failed to create callback reader for source node, cause: %s.", garrow_take_error(&error));

	options = garrow_source_node_options_new_record_batch_reader(reader);
	source = garrow_execute_plan_build_source_node(pcontext->plan,
												   options,
												   &error);
	if (error)
		elog(ERROR, "Failed to create source node, cause: %s.", garrow_take_error(&error));

	/*
	 * This source pulls batches from PG's ExecProcNode on the calling
	 * thread; it cannot run under push_pipeline (start-only, no
	 * interleaved reads).  Flags the estate for demotion to yield mode.
	 */
	pcontext->has_callback_source = true;

	return garrow_move_ptr(source);
}

static GArrowExecuteNode *
BuildScanNode(PlanBuildContext *pcontext)
{
	VecSeqScanState *state = (VecSeqScanState *)pcontext->planstate;
	Plan *plan = (Plan *)pcontext->planstate->plan;
	g_autoptr(GError) error = NULL;
	g_autoptr(GArrowScanNodeOptions) scan_node_options = NULL;
	g_autoptr(GArrowExecuteNode) scan_node = NULL;
	g_autoptr(GArrowExpression) qual_expr = NULL;

	Assert(IsA(state, SeqScanState));
	if (plan->qual)
		qual_expr = build_filter_expression(plan->qual, pcontext);
	scan_node_options = garrow_scan_node_options_new(pcontext->table_oid,
													 (void *) pcontext->planstate,
													 pcontext->am_oid,
													 pcontext->relation_schema,
													 pcontext->inputschema,
													 qual_expr,
													 &error);
	/*
	 * garrow_scan_node_options_new() only reads the C++ schema out of the
	 * wrapper and keeps a shared_ptr of its own, so the wrapper is ours to
	 * drop the moment that call returns.  Drop it here rather than at the end
	 * of BuildScanPlan: the elog(ERROR)s below longjmp out of this frame, and
	 * a GObject allocated outside any PostgreSQL memory context is not
	 * reclaimed by the error machinery.
	 */
	if (pcontext->relation_schema != NULL)
		ARROW_FREE(GArrowSchema, &pcontext->relation_schema);

	if (error)
		elog(ERROR, "Failed to create scan node, cause: %s.", garrow_take_error(&error));
	scan_node = garrow_execute_plan_build_scan_node(pcontext->plan,
													scan_node_options,
													&error);
	garrow_store_ptr(state->scan_node_options, scan_node_options);
	if (error)
		elog(ERROR, "Failed to create scan node, cause: %s.", garrow_take_error(&error));
	return garrow_move_ptr(scan_node);
}

/* build project for aggregate input */
static GArrowExecuteNode*
BuildAggProject(List *targetList, List *aggInfos, GArrowExecuteNode *input, PlanBuildContext *pcontext)
{
	g_autoptr(GArrowProjectNodeOptions) project_options = NULL;
	g_autoptr(GArrowExecuteNode) project = NULL;
	g_autoptr(GError) error = NULL;

	if (IsA(pcontext->planstate, WindowAggState))
		project_options = build_windowagg_project_options(targetList, aggInfos, pcontext);
	else if (IsA(pcontext->planstate, WindowHashAggState))
		project_options = build_windowhashagg_project_options(targetList, aggInfos, pcontext);
	else
		project_options = build_agg_project_options(targetList, aggInfos, pcontext);

	if (!project_options)
		return NULL;

	project = garrow_execute_plan_build_project_node(pcontext->plan,
													 input,
													 project_options,
													 &error);
	if (error)
		elog(ERROR, "Failed to create Arrow project node: %s.", garrow_take_error(&error));
	return garrow_move_ptr(project);
}

static void free_agg_infos(List *agginfos)
{
	ListCell	   *l;

	foreach(l, agginfos)
	{
		VecAggInfo *agginfo = (VecAggInfo *) lfirst(l);

		glib_autoptr_cleanup_GArrowFunctionOptions(&agginfo->options);
	}
	list_free_deep(agginfos);
}

static GArrowExecuteNode*
BuildProject(List *targetList, List *qualList, GArrowExecuteNode *input, PlanBuildContext *pcontext)
{
	g_autoptr(GArrowProjectNodeOptions) project_options = NULL;
	g_autoptr(GArrowFilterNodeOptions) filter_options = NULL;
	g_autoptr(GArrowAssertOpNodeOptions) assertop_options = NULL;
	g_autoptr(GArrowExecuteNode) project = NULL;
	g_autoptr(GArrowExecuteNode) filter_node = NULL;
	g_autoptr(GArrowExecuteNode) assertop_node = NULL;
	g_autoptr(GArrowExecuteNode) orderby_node = NULL;
	g_autoptr(GArrowExecuteNode) current = garrow_copy_ptr(input);
	g_autoptr(GError) error = NULL;

	/*
	 * Sonic Motion Direct-Send: the passthrough flag is set authoritatively
	 * by build_aggregatation_options when a hint is found in the
	 * vec_motion_direct_send HTAB. Reset here to avoid stale state from a
	 * sibling plan node.
	 *
	 * project_options must be built BEFORE the agg block (its expressions
	 * reference agginfo->outname strings which the agg block populates,
	 * but free_agg_infos at the end of the agg block invalidates them).
	 * If the agg block then turns the passthrough flag on, we rebuild
	 * project_options below — slightly wasteful but correct.
	 */
	pcontext->sonic_target_segment_passthrough = false;
	pcontext->sonic_target_segment_col_name = NULL;

	pcontext->proj_all_passthrough = false;
	pcontext->proj_names = NULL;
	pcontext->proj_length = 0;

	if (targetList)
		project_options = build_project_options(targetList, pcontext);

	if (qualList && pcontext->is_assertop)
		assertop_options = build_assertop_options(qualList, pcontext);
	else if (qualList)
		filter_options = build_filter_options(qualList, pcontext);

	/* Build Agg or WindowAgg if any */
	if (IsA(pcontext->planstate->plan, Agg) ||
	    IsA(pcontext->planstate->plan, WindowAgg) ||
	    IsA(pcontext->planstate->plan, WindowHashAgg))
	{
		g_autoptr(GArrowExecuteNode) aggregation_input = NULL;
		g_autoptr(GArrowExecuteNode) aggregation = NULL;
		List *agginfos;

		if (IsA(pcontext->planstate->plan, Agg))
			pcontext->agginfos = plan_numeric_group_keys(pcontext->agginfos,
														  pcontext);
		agginfos = pcontext->agginfos;

		/* build project for aggregate input */
		aggregation_input = BuildAggProject(targetList, agginfos, current, pcontext);
		if (aggregation_input)
			aggregation = BuildAggregatation(agginfos, aggregation_input, pcontext);
		else
			aggregation = BuildAggregatation(agginfos, current, pcontext);
		garrow_store_ptr(current, aggregation);

		/*
		 * Sonic Motion Direct-Send: if build_aggregatation_options turned on
		 * the passthrough flag, the upstream project_options built above ran
		 * with the flag still false and is missing the hidden-column
		 * field_ref. Rebuild here, while agginfos is still alive — Aggref
		 * resolution in expr_to_arrow_expression iterates agginfos to look
		 * up outname, so this MUST happen before free_agg_infos.
		 */
		if (pcontext->sonic_target_segment_passthrough && targetList)
		{
			g_clear_object(&project_options);
			project_options = build_project_options(targetList, pcontext);
		}

		free_agg_infos(agginfos);
	}

	/* Build Limit node if any */
	if (IsA(pcontext->planstate->plan, Limit))
	{
		g_autoptr(GArrowExecuteNode) limit = NULL;
		g_autoptr(GArrowLimitNodeOptions) options = NULL;

		LimitState *ls = castNode(LimitState, pcontext->planstate);

		options = garrow_limit_node_options_new(ls->offset, ls->count, ls->noCount);
		limit = garrow_execute_plan_build_limit_node(pcontext->plan, current, options, &error);
		if (error)
			elog(ERROR, "Failed to create limit node, cause: %s", garrow_take_error(&error));

		garrow_store_ptr(current, limit);
	}

	/* having clause is always after agg and before upper agg project */
	if (qualList && pcontext->is_assertop)
	{
		assertop_node = garrow_execute_plan_build_assertop_node(pcontext->plan, current, assertop_options, &error);
		if (error)
			elog(ERROR, "Failed to create assertop node, %s.", garrow_take_error(&error));
		garrow_store_ptr(current, assertop_node);
	}
	else if (qualList)
	{
		filter_node = garrow_execute_plan_build_filter_node(pcontext->plan, current, filter_options, &error);
		if (error)
			elog(ERROR, "Failed to create filter node, %s.", garrow_take_error(&error));
		garrow_store_ptr(current, filter_node);
	}

	/* An identity projection would only copy its input; skip it. */
	if (targetList && !ProjectIsIdentity(current, pcontext))
	{
		project = garrow_execute_plan_build_project_node(pcontext->plan,
														 current,
														 project_options,
														 &error);
		if (error)
			elog(ERROR, "Failed to create Arrow project node: %s.", garrow_take_error(&error));
		garrow_store_ptr(current, project);
	}

	if (IsA(pcontext->planstate->plan, Sort))
	{
		int64 k = vec_topk_bounds_lookup(pcontext->planstate->plan);
		g_autoptr(GArrowExecuteNode) sort = NULL;

		if (k > 0)
			sort = build_topk_node(pcontext->planstate, pcontext->plan,
								   current, k);
		else
			sort = build_orderby_node(pcontext->planstate, pcontext->plan, current);

		garrow_store_ptr(current, sort);
	}

	if (IsA(pcontext->planstate->plan, PartitionTopK))
	{
		g_autoptr(GArrowExecuteNode) ptopk = NULL;

		ptopk = build_partition_topk_node(pcontext->planstate, pcontext->plan,
										  current);
		garrow_store_ptr(current, ptopk);
	}

	return garrow_move_ptr(current);
}

static const char *
GetHashJoinProjectName(PlanBuildContext *pcontext, const char *name)
{
	/* right_ len > left_ len */
	int prefix_maxlen = strlen(RIGHT_PREFIX);
	char *prefix_name = palloc(prefix_maxlen + strlen(name) + 1);
	snprintf(prefix_name, prefix_maxlen + strlen(name) + 1, "%s%s", pcontext->is_left_schema ? LEFT_PREFIX : RIGHT_PREFIX, name);
	return prefix_name;
}

static GArrowProjectNodeOptions *
build_join_project_options(List *hashkeys, GArrowExecuteNode *input, PlanBuildContext *pcontext)
{
	GList *expressions = NULL;
	g_autoptr(GArrowProjectNodeOptions) options = NULL;
	gsize			length;
	ListCell	   *l;
	const gchar		  **names;
	g_autoptr(GArrowSchema) schema = NULL;
	GList *fields = NULL;
	g_autoptr(GError) error = NULL;
	int i = 0;
	int j = 0;

	schema = garrow_execute_node_get_output_schema(input);
	fields = garrow_schema_get_fields(schema);

	/* a rewritten numeric key may also need its raw value, see below */
	length = g_list_length(fields) + 2 * list_length(hashkeys);
	names = palloc(length * sizeof(gchar *));

	/* convert source schema column to proj fields */
	for (j = 0; j < g_list_length(fields); j++)
	{
		g_autoptr(GArrowExpression) arrow_expr;
		g_autoptr(GArrowField) f = garrow_list_nth_data(fields, j);
		const char *name = garrow_field_get_name(f);
		arrow_expr = GARROW_EXPRESSION(garrow_field_expression_new(name, &error));
		if (error)
			elog(ERROR, "convert field to arrow expression failed: %s",
				 garrow_take_error(&error));
		expressions = garrow_list_append_ptr(expressions, arrow_expr);
		names[j] = GetHashJoinProjectName(pcontext, name);
	}

	/* append hashkeys column to proj fields */
	foreach(l, hashkeys)
	{
		g_autoptr(GArrowExpression) arrow_expr = NULL;
		g_autoptr(GArrowField) field = NULL;
		g_autoptr(GArrowDataType) result_type = NULL;
		ExprState *exprstate = (ExprState *)lfirst(l);
		Expr	   *kexpr = exprstate->expr;
		int prefix_maxlen = strlen(RIGHT_PREFIX);
		char *prefix_name;
		int numeric_fix = pcontext->numeric_key_fix ?
			list_nth_int(pcontext->numeric_key_fix, foreach_current_index(l)) :
			NUMERIC_KEY_ASIS;

		result_type = PGTypeToArrow(exprType((Node *) kexpr));

		if (numeric_fix != NUMERIC_KEY_ASIS)
		{
			g_autoptr(GArrowExpression) raw_expr = NULL;
			const char *raw_name;

			/*
			 * Hash a rewritten copy of the numeric key (see
			 * plan_numeric_join_keys); the passthrough columns above still
			 * carry the original values to the join output.  RIGHT_SEMI /
			 * RIGHT_ANTI resolve a probe-side hash key Var to the build
			 * key's column, so right_key_names must name the raw value, not
			 * the rewritten one: the Var's passthrough column, or else an
			 * extra column of the unrewritten expression.
			 */
			raw_expr = expr_to_arrow_expression(kexpr, pcontext);
			arrow_expr = fix_numeric_key(raw_expr, numeric_fix);
			if (IsA(kexpr, Var) && ((Var *) kexpr)->varattno > 0)
				raw_name = GetHashJoinProjectName(pcontext,
												  GetSchemaName(pcontext->inputschema,
																((Var *) kexpr)->varattno,
																pcontext->map));
			else
			{
				char	   *name = palloc(prefix_maxlen + 30);

				snprintf(name, prefix_maxlen + 30, "%sjoinqual_raw_%d",
						 pcontext->is_left_schema ? LEFT_PREFIX : RIGHT_PREFIX, i);
				expressions = garrow_list_append_ptr(expressions, raw_expr);
				names[j++] = name;
				raw_name = name;
			}

			expressions = garrow_list_append_ptr(expressions, arrow_expr);
			prefix_name = palloc(prefix_maxlen + 20);
			snprintf(prefix_name, prefix_maxlen + 20, "%sjoinqual_%d",
					 pcontext->is_left_schema ? LEFT_PREFIX : RIGHT_PREFIX, i++);
			field = garrow_field_new(prefix_name, result_type);
			if (pcontext->is_left_schema)
			{
				pcontext->left_hashkeys = garrow_list_append_ptr(pcontext->left_hashkeys, field);
			}
			else
			{
				pcontext->right_hashkeys = garrow_list_append_ptr(pcontext->right_hashkeys, field);
				pcontext->right_key_names = lappend(pcontext->right_key_names,
													pstrdup(raw_name));
			}
			names[j++] = prefix_name;
			continue;
		}

		/*
		 * Diff-A (build-row slimming): when the hashkey is a plain column
		 * reference, the passthrough loop above already emits that column
		 * as <prefix><name>.  Point the join key at that existing column
		 * instead of materializing a second, byte-identical joinqual_N
		 * column.  The duplicate widened every build row by one key width
		 * (and became dead payload), which DuckDB never does.  attname is
		 * resolved exactly as expr_to_arrow_expression() resolves a Var at
		 * this point (is_hashjoin_after_node is still false here), so the
		 * reused column carries the same value the joinqual_N would have.
		 * Any non-Var (expression) hashkey still gets a synthesized column.
		 */
		if (IsA(kexpr, Var) && ((Var *) kexpr)->varattno > 0)
		{
			Var		   *var = (Var *) kexpr;
			const char *attname = GetSchemaName(pcontext->inputschema,
												var->varattno, pcontext->map);
			const char *reuse_name = GetHashJoinProjectName(pcontext, attname);

			field = garrow_field_new(reuse_name, result_type);
			if (pcontext->is_left_schema)
			{
				pcontext->left_hashkeys = garrow_list_append_ptr(pcontext->left_hashkeys, field);
			}
			else
			{
				pcontext->right_hashkeys = garrow_list_append_ptr(pcontext->right_hashkeys, field);
				pcontext->right_key_names = lappend(pcontext->right_key_names,
													pstrdup(reuse_name));
			}
			continue;
		}

		prefix_name = palloc(prefix_maxlen + 20);
		/* The hashkey's projected column names are built together when building the expression */
		arrow_expr = expr_to_arrow_expression(kexpr, pcontext);
		expressions = garrow_list_append_ptr(expressions, arrow_expr);
		snprintf(prefix_name, prefix_maxlen + 20, "%sjoinqual_%d", pcontext->is_left_schema ? LEFT_PREFIX : RIGHT_PREFIX, i++);
		field = garrow_field_new(prefix_name, result_type);
		if (pcontext->is_left_schema)
		{
			pcontext->left_hashkeys = garrow_list_append_ptr(pcontext->left_hashkeys, field);
		}
		else
		{
			pcontext->right_hashkeys = garrow_list_append_ptr(pcontext->right_hashkeys, field);
			pcontext->right_key_names = lappend(pcontext->right_key_names,
												pstrdup(prefix_name));
		}
		names[j++] = prefix_name;
	}
	options = garrow_project_node_options_new(expressions,
											  (gchar **)names,
											  j);
	pfree(names);
	garrow_list_free_ptr(&expressions);
	garrow_list_free_ptr(&fields);
	return garrow_move_ptr(options);
}

static GArrowExecuteNode*
BuildJoinProject(List *hashkeys, GArrowExecuteNode *input, PlanBuildContext *pcontext)
{
	g_autoptr(GArrowProjectNodeOptions) project_options = NULL;
	g_autoptr(GArrowExecuteNode) project = NULL;
	g_autoptr(GError) error = NULL;

	project_options = build_join_project_options(hashkeys, input, pcontext);

	project = garrow_execute_plan_build_project_node(pcontext->plan,
													 input,
													 project_options,
													 &error);
	if (error)
		elog(ERROR, "Failed to create Arrow project node: %s.", garrow_take_error(&error));
	return garrow_move_ptr(project);
}

static const int
GetGroupKey(Agg *agg, Expr *expr)
{
	Var *var = (Var *)expr;

	if (!IsA(expr, Var))
		return -1;

	for(int i = 0; i < agg->numCols; i++)
	{
		if(agg->grpColIdx[i] == var->varattno)
			return i;
	}
	return -1;
}

/* Aggregate input columns is consists of two parts:
 * 1. The arg fo each aggref.
 * 2. The group keys columns if exists.
 * Make these expressions as a single projector.
 */
static GArrowProjectNodeOptions *
build_agg_project_options(List *targetList, List *aggInfos, PlanBuildContext *pcontext)
{
	GList *expressions = NULL;
	Agg *agg = (Agg *) pcontext->planstate->plan;
	gsize nkey = agg->numCols;
	List *extra_atts = NIL;
	g_autoptr(GArrowProjectNodeOptions) options = NULL;
	gsize			length;
	int				i = 0;
	bool 			need_project = false;
	ListCell	   *l;
	const gchar	  **names; /* project result names */

	/*
	 * The planner removes any GROUP BY column it can prove equal to a
	 * constant from the Agg node (e.g. "... WHERE k = 'x' GROUP BY k, j"
	 * keeps only j in grpColIdx, and "... WHERE k = 'x' GROUP BY k" leaves
	 * grpColIdx empty).  Arrow's aggregate emits only the grpColIdx columns
	 * plus the aggregate results, so a constant-folded column that the upper
	 * targetlist still references can no longer be resolved and plan
	 * construction fails with "No match for FieldRef ... in _outagg_0".
	 *
	 * Re-add every such column (a bare Var in the Agg targetlist that is not
	 * already a grpColIdx entry) as a grouping key.  Each is single-valued
	 * at this point -- it is either constant or functionally dependent on a
	 * real grouping column -- so grouping on it does not change the result
	 * and it stays in the aggregate's output schema for the projection above.
	 */
	if (targetList != NIL)
	{
		ListCell   *tc;

		foreach(tc, targetList)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(tc);
			Node	   *e = (Node *) tle->expr;
			AttrNumber	attno;
			bool		in_grpcols = false;

			if (tle->resjunk)
				continue;
			while (e != NULL && IsA(e, RelabelType))
				e = (Node *) ((RelabelType *) e)->arg;
			if (e == NULL || !IsA(e, Var) || ((Var *) e)->varattno <= 0)
				continue;
			attno = ((Var *) e)->varattno;

			for (int k = 0; k < agg->numCols; k++)
			{
				if (agg->grpColIdx[k] == attno)
				{
					in_grpcols = true;
					break;
				}
			}
			if (!in_grpcols && !list_member_int(extra_atts, attno))
				extra_atts = lappend_int(extra_atts, attno);
		}
		nkey += list_length(extra_atts);
	}

	length = list_length(aggInfos) + nkey +
		bms_num_members(pcontext->numeric_canon_keys);
	names = palloc(length * sizeof(gchar *));
	/* append group keys */
	for (i = 0; i < nkey; i++)
	{
		g_autoptr(GArrowExpression) arrow_expr = NULL;
		g_autoptr(GArrowField)	field = NULL;
		g_autoptr(GError) error = NULL;

		/* group key out name is same as input name */
		names[i] = GetSchemaName(pcontext->inputschema,
								 (i < agg->numCols)
									 ? agg->grpColIdx[i]
									 : list_nth_int(extra_atts, i - agg->numCols),
								 pcontext->map);
		arrow_expr = GARROW_EXPRESSION(
				garrow_field_expression_new(names[i], &error));
		if (error)
			elog(ERROR, "New arrow expression from column: %s error: %s",
					names[i], garrow_take_error(&error));
		expressions = garrow_list_append_ptr(expressions, arrow_expr);
	}
	if (nkey > 0)
		pcontext->keys = names;
	pcontext->nkey = nkey;

	/*
	 * Group numeric keys without a fixed scale by a hidden trimmed copy,
	 * see plan_numeric_group_keys().  names[] keeps the original key names:
	 * they are the min() inputs and the inputs of aggregates on a key.
	 */
	if (!bms_is_empty(pcontext->numeric_canon_keys))
	{
		const gchar **keys = palloc(nkey * sizeof(gchar *));

		for (int k = 0; k < nkey; k++)
		{
			g_autoptr(GArrowExpression) field_expr = NULL;
			g_autoptr(GArrowExpression) canon_expr = NULL;
			g_autoptr(GError) error = NULL;
			char	   *canon_name;

			keys[k] = names[k];
			if (!bms_is_member(k, pcontext->numeric_canon_keys))
				continue;

			field_expr = GARROW_EXPRESSION(
					garrow_field_expression_new(names[k], &error));
			if (error)
				elog(ERROR, "New arrow expression from column: %s error: %s",
					 names[k], garrow_take_error(&error));
			canon_expr = fix_numeric_key(field_expr, NUMERIC_KEY_TRIM);
			expressions = garrow_list_append_ptr(expressions, canon_expr);
			canon_name = psprintf(NUMERIC_CANON_KEY_PREFIX "%d", k);
			names[i++] = canon_name;
			keys[k] = canon_name;
		}
		pcontext->keys = keys;
		need_project = true;
	}

	if ((pcontext->nkey > 0) && (!aggInfos))
		rewrite_tl_keys(targetList, pcontext);

	/* append aggrefs */
	foreach(l, aggInfos)
	{
		g_autoptr(GArrowExpression) arrow_expr = NULL;
		VecAggInfo *agginfo = (VecAggInfo *) lfirst(l);
		Aggref *aggref = agginfo->aggref;
		int key;
		int precision;
		TargetEntry *source_tle;

		Assert(IsA(aggref, Aggref));
		/* Todo: single arg only now*/

		if(!aggref->args)
			continue;

		if (list_length(aggref->args) != 1) 
			elog(ERROR, "doesn't support aggref args len != 1");
		source_tle = (TargetEntry *) linitial(aggref->args);

		/* skip columns in group keys*/
		key = GetGroupKey(agg, source_tle->expr);
		if(key >= 0)
		{
			/* aggref is group key, update agg inname */
			memcpy(agginfo->inname, names[key], sizeof(agginfo->inname));
			continue;
		}

		arrow_expr = expr_to_arrow_expression(source_tle->expr, pcontext);

		/*
		 * DISTINCT hashes its input like a group key, so numeric values
		 * without a fixed scale are deduplicated in trimmed form (see
		 * plan_numeric_join_keys); 2.50 and 2.5000 count once.
		 */
		if (aggref->aggdistinct != NIL &&
			exprType((Node *) source_tle->expr) == NUMERICOID &&
			numeric_expr_scale(&agg->plan, (Node *) source_tle->expr,
							   &precision, 0) < 0)
		{
			GArrowExpression *trimmed = fix_numeric_key(arrow_expr, NUMERIC_KEY_TRIM);

			g_object_unref(arrow_expr);
			arrow_expr = trimmed;
		}

		/* FIXME: Add shared input expression later */

		/* child expression is input of agg, use inname */
		names[i] = agginfo->inname;

		expressions = garrow_list_append_ptr(expressions, arrow_expr);
		need_project = true;
		i++;
	}

	/* append aggrefs */
	foreach(l, aggInfos)
	{
		VecAggInfo *agginfo = (VecAggInfo *) lfirst(l);
		Aggref *aggref = agginfo->aggref;

		/* count() or count(*), needn't project this column.
		 * Update aggname as another valid column.
		 */
		if(!aggref->args)
		{
			ListCell	   *c;
			char *name = NULL;

			foreach(c, aggInfos)
			{
				VecAggInfo *cinfo = (VecAggInfo *) lfirst(c);

				if (cinfo->aggref->args)
				{
					name = cinfo->inname;
					break;
				}
			}

			/* If found other agg, count other column.
			 * If no other column, means a count(*), no project for agg input.
			 * So, use an arbitrary input column name as agg inname.
			 */
			/* Fixme: implement count(*) by arrow EmptyBatch */
			if (!name)
			{
				name = (char *)GetSchemaName(pcontext->inputschema, 1, pcontext->map);
			}
			memcpy(agginfo->inname, name, sizeof(agginfo->inname));
			continue;
		}
	}

	if (!need_project)
	{
		garrow_list_free_ptr(&expressions);
		return NULL;
	}

	options = garrow_project_node_options_new(expressions,
											  (gchar**)names,
											  i);

	/* if nkeys > 0, names will be used for group keys */
	if (nkey <= 0)
		pfree(names);
	garrow_list_free_ptr(&expressions);
	return garrow_move_ptr(options);
}

static GArrowProjectNodeOptions *
build_windowagg_project_options(List *targetList, List *aggInfos, PlanBuildContext *pcontext)
{
	GList *expressions = NULL;
	g_autoptr(GArrowProjectNodeOptions) options = NULL;
	gsize			length;
	bool 			need_project = false;
	ListCell	   *l;
	const gchar	  **names; /* project result names */

	WindowAgg *wagg = (WindowAgg *) pcontext->planstate->plan;

	/* find cols used by "order by" clause */
	pcontext->orderby_sortoption = NULL;
	if (wagg->ordNumCols > 0)
	{
		GList *orderby_keys = NULL;
		SortKey sortKey;
		sortKey.orders = palloc0(sizeof(GArrowSortOrder) * wagg->ordNumCols);
		sortKey.nulls_first = palloc0(sizeof(GArrowSortNullPlacement) * wagg->ordNumCols);
		get_windowagg_sortorder(pcontext, &sortKey);

		for (int i = 0; i < wagg->ordNumCols; i++)
		{
			g_autoptr(GArrowSortKey) key = NULL;
			const char *field_name = GetSchemaName(pcontext->inputschema, wagg->ordColIdx[i], NULL);
			key = garrow_sort_key_new((gchar *)pstrdup(field_name), sortKey.orders[i], GARROW_SORT_ORDER_Default, sortKey.nulls_first[i]);
			orderby_keys = garrow_list_append_ptr(orderby_keys, key);
		}

		pcontext->orderby_sortoption =  vec_sort_options_new(orderby_keys, partition_top_k, take_thread_num, two_phase_take,
													 pcontext->planstate->plan->plan_node_id);
		garrow_list_free_ptr(&orderby_keys);
		pfree(sortKey.orders);
		pfree(sortKey.nulls_first);
	}

	/* find cols used by "partition by" clause */
	const gchar **partcol_names = palloc0(sizeof(gchar *) * wagg->partNumCols);
	for (int i = 0; i < wagg->partNumCols; i++)
		partcol_names[i] = GetSchemaName(pcontext->inputschema, wagg->partColIdx[i], NULL);

	if (wagg->partNumCols > 0)
	{
		pcontext->keys = partcol_names;
		pcontext->nkey = wagg->partNumCols;
	}
	else
	{
		pcontext->keys = NULL;
		pcontext->nkey = 0;
	}

	/* append all input columns with windowagg */
	int num_fields = garrow_schema_n_fields(pcontext->inputschema);
	length = list_length(aggInfos) + num_fields;
	names = palloc(length * sizeof(gchar *));
	int col = 0;
	for (int i = 0; i < num_fields; i++)
	{
		g_autoptr(GArrowExpression) arrow_expr = NULL;
		g_autoptr(GArrowField)	field = NULL;
		g_autoptr(GError) error = NULL;

		/* group key out name is same as input name */
		const char *fname = GetSchemaName(pcontext->inputschema, i + 1, pcontext->map);
		names[col] = fname;
		arrow_expr = GARROW_EXPRESSION(garrow_field_expression_new(names[col], &error));
		if (error)
			elog(ERROR, "New arrow expression from column: %s error: %s",
					names[i], garrow_take_error(&error));
		expressions = garrow_list_append_ptr(expressions, arrow_expr);
		++col;
	}

	/* append window aggrefs */
	foreach(l, aggInfos)
	{
		g_autoptr(GArrowExpression) arrow_expr = NULL;
		VecAggInfo *agginfo = (VecAggInfo *) lfirst(l);
		WindowFunc *wfunc = agginfo->wfunc;

		Assert(IsA(wfunc, WindowFunc));

		/* Todo: single arg only now*/
		if(!wfunc->args)
			continue;

		Assert(list_length(wfunc->args) == 1);
		arrow_expr = expr_to_arrow_expression(linitial(wfunc->args), pcontext);

		/* FIXME: Add shared input expression later */

		/* child expression is input of agg, use inname */
		names[col] = agginfo->inname;

		expressions = garrow_list_append_ptr(expressions, arrow_expr);
		need_project = true;
		col++;
	}

	/* append window aggrefs */
	foreach(l, aggInfos)
	{
		VecAggInfo *agginfo = (VecAggInfo *) lfirst(l);
		WindowFunc *wfunc = agginfo->wfunc;

		/*
		 * count() or count(*), needn't project this column.
		 * Update aggname as another valid column.
		 */
		if(!wfunc->args)
		{
			ListCell	   *c;
			char *name = NULL;

			foreach(c, aggInfos)
			{
				VecAggInfo *cinfo = (VecAggInfo *) lfirst(c);

				if (cinfo->wfunc->args)
				{
					name = cinfo->inname;
					break;
				}
			}

			/* If found other agg, count other column.
			 * If no other column, means a count(*), no project for agg input.
			 * So, use an arbitrary input column name as agg inname.
			 */
			/* Fixme: implement count(*) by arrow EmptyBatch */
			if (!name)
			{
				name = (char *)GetSchemaName(pcontext->inputschema, 1, pcontext->map);
			}
			memcpy(agginfo->inname, name, sizeof(agginfo->inname));
			continue;
		}
	}

	if (!need_project)
	{
		garrow_list_free_ptr(&expressions);
		return NULL;
	}

	options = garrow_project_node_options_new(expressions,
											  (gchar**)names,
											  col);

	if (DEBUG1 >= log_min_messages)
	{
		for (GList *node = expressions; node; node = node->next)
		{
			g_autofree gchar *str =
				garrow_expression_to_string(GARROW_EXPRESSION(node->data));
			elog(DEBUG1, "%s result expressions: %s", __func__, str);
		}
	}
	garrow_list_free_ptr(&expressions);

	return garrow_move_ptr(options);
}

static GArrowProjectNodeOptions *
build_windowhashagg_project_options(List *targetList, List *aggInfos, PlanBuildContext *pcontext)
{
	GList *expressions = NULL;
	g_autoptr(GArrowProjectNodeOptions) options = NULL;
	gsize			length;
	bool 			need_project = false;
	ListCell	   *l;
	const gchar	  **names; /* project result names */

	WindowHashAgg *wagg = (WindowHashAgg *) pcontext->planstate->plan;

	/* find cols used by "order by" clause */
	pcontext->orderby_sortoption = NULL;
	if (wagg->ordNumCols > 0)
	{
		Oid opfamily, opcintype;
		int16 strategy;
		GList *orderby_keys = NULL;

		for (int i = 0; i < wagg->ordNumCols; i++)
		{
			/* Find the operator in pg_amop */
			if (!get_ordering_op_properties(wagg->ordOperators[i], &opfamily, &opcintype, &strategy))
				elog(ERROR, "operator %u is not a valid ordering operator", wagg->ordOperators[i]);

			g_autoptr(GArrowSortKey) key = NULL;
			const char *field_name = GetSchemaName(pcontext->inputschema, wagg->ordColIdx[i], NULL);
			key = garrow_sort_key_new((gchar *)pstrdup(field_name),
									  (strategy == BTLessStrategyNumber) ? GARROW_SORT_ORDER_ASCENDING : GARROW_SORT_ORDER_DESCENDING,
									  GARROW_SORT_ORDER_Default,
									  wagg->ordNullsFirst[i] ? GARROW_SORT_ORDER_AT_START : GARROW_SORT_ORDER_AT_END);
			orderby_keys = garrow_list_append_ptr(orderby_keys, key);
		}

		pcontext->orderby_sortoption =  vec_sort_options_new(orderby_keys, partition_top_k, take_thread_num, two_phase_take,
													 pcontext->planstate->plan->plan_node_id);
		garrow_list_free_ptr(&orderby_keys);
	}

	/* find cols used by "partition by" clause */
	const gchar **partcol_names = palloc0(sizeof(gchar *) * wagg->partNumCols);
	for (int i = 0; i < wagg->partNumCols; i++)
		partcol_names[i] = GetSchemaName(pcontext->inputschema, wagg->partColIdx[i], NULL);

	if (wagg->partNumCols > 0)
	{
		pcontext->keys = partcol_names;
		pcontext->nkey = wagg->partNumCols;
	}
	else
	{
		pcontext->keys = NULL;
		pcontext->nkey = 0;
	}

	/* append all input columns with windowagg */
	int num_fields = garrow_schema_n_fields(pcontext->inputschema);
	length = list_length(aggInfos) + num_fields + wagg->partNumCols;
	names = palloc(length * sizeof(gchar *));
	int col = 0;
	for (int i = 0; i < num_fields; i++)
	{
		g_autoptr(GArrowExpression) arrow_expr = NULL;
		g_autoptr(GArrowField)	field = NULL;
		g_autoptr(GError) error = NULL;

		/* group key out name is same as input name */
		const char *fname = GetSchemaName(pcontext->inputschema, i + 1, pcontext->map);
		names[col] = fname;
		arrow_expr = GARROW_EXPRESSION(garrow_field_expression_new(names[col], &error));
		if (error)
			elog(ERROR, "New arrow expression from column: %s error: %s",
					names[i], garrow_take_error(&error));
		expressions = garrow_list_append_ptr(expressions, arrow_expr);
		++col;
	}

	/*
	 * Partition on a hidden trimmed copy of numeric keys without a fixed
	 * scale, as plan_numeric_group_keys() does for GROUP BY; the original
	 * column above still carries the values the window emits.
	 */
	for (int i = 0; i < wagg->partNumCols; i++)
	{
		AttrNumber	attno = wagg->partColIdx[i];
		TargetEntry *tle = get_tle_by_resno(outerPlan(wagg)->targetlist, attno);
		g_autoptr(GArrowExpression) field_expr = NULL;
		g_autoptr(GArrowExpression) canon_expr = NULL;
		g_autoptr(GError) error = NULL;

		int			precision;

		if (tle == NULL ||
			exprType((Node *) tle->expr) != NUMERICOID ||
			numeric_tlist_scale(outerPlan(wagg), attno, &precision, 0) >= 0)
			continue;

		field_expr = GARROW_EXPRESSION(
				garrow_field_expression_new(partcol_names[i], &error));
		if (error)
			elog(ERROR, "New arrow expression from column: %s error: %s",
				 partcol_names[i], garrow_take_error(&error));
		canon_expr = fix_numeric_key(field_expr, NUMERIC_KEY_TRIM);
		expressions = garrow_list_append_ptr(expressions, canon_expr);
		names[col] = psprintf(NUMERIC_CANON_KEY_PREFIX "%d", i);
		partcol_names[i] = names[col];
		col++;
		need_project = true;
		elog(DEBUG1, "numeric partition key %d has no fixed scale, partitioning trimmed", i);
	}

	/* append window aggrefs */
	foreach(l, aggInfos)
	{
		g_autoptr(GArrowExpression) arrow_expr = NULL;
		VecAggInfo *agginfo = (VecAggInfo *) lfirst(l);
		WindowFunc *wfunc = agginfo->wfunc;

		Assert(IsA(wfunc, WindowFunc));

		/* Todo: single arg only now*/
		if(!wfunc->args)
			continue;

		Assert(list_length(wfunc->args) == 1);
		arrow_expr = expr_to_arrow_expression(linitial(wfunc->args), pcontext);

		/* FIXME: Add shared input expression later */

		/* child expression is input of agg, use inname */
		names[col] = agginfo->inname;

		expressions = garrow_list_append_ptr(expressions, arrow_expr);
		need_project = true;
		col++;
	}

	/* append window aggrefs */
	foreach(l, aggInfos)
	{
		VecAggInfo *agginfo = (VecAggInfo *) lfirst(l);
		WindowFunc *wfunc = agginfo->wfunc;

		/*
		 * count() or count(*), needn't project this column.
		 * Update aggname as another valid column.
		 */
		if(!wfunc->args)
		{
			ListCell	   *c;
			char *name = NULL;

			foreach(c, aggInfos)
			{
				VecAggInfo *cinfo = (VecAggInfo *) lfirst(c);

				if (cinfo->wfunc->args)
				{
					name = cinfo->inname;
					break;
				}
			}

			/* If found other agg, count other column.
			 * If no other column, means a count(*), no project for agg input.
			 * So, use an arbitrary input column name as agg inname.
			 */
			/* Fixme: implement count(*) by arrow EmptyBatch */
			if (!name)
			{
				name = (char *)GetSchemaName(pcontext->inputschema, 1, pcontext->map);
			}
			memcpy(agginfo->inname, name, sizeof(agginfo->inname));
			continue;
		}
	}

	if (!need_project)
	{
		garrow_list_free_ptr(&expressions);
		return NULL;
	}

	options = garrow_project_node_options_new(expressions,
											  (gchar**)names,
											  col);

	if (DEBUG1 >= log_min_messages)
	{
		for (GList *node = expressions; node; node = node->next)
		{
			g_autofree gchar *str =
				garrow_expression_to_string(GARROW_EXPRESSION(node->data));
			elog(DEBUG1, "%s result expressions: %s", __func__, str);
		}
	}
	garrow_list_free_ptr(&expressions);

	return garrow_move_ptr(options);
}

/*
 * Is the pending projection a no-op?
 *
 * A ProjectNode that emits exactly its input's fields, in order, under their
 * own names copies every batch for nothing.  Arrow keeps such a node in the
 * plan, so it costs an ExecNode, a thread hop and a full batch copy per
 * batch.  TPC-DS builds thousands of them: a scan's column-selection project
 * whose scan already applied the column map, and the project a Sort/Limit
 * emits over an Agg output it does not actually change.
 *
 * The test is deliberately literal.  build_project_options() already knows
 * whether every expression is a passthrough; all that is left is to check the
 * names line up one-for-one with the fields of `input`, which is only
 * settled once the agg/limit/filter nodes have been built.
 *
 * One projection is never a no-op, however identical it looks: the one sitting
 * directly on a bridge source node.  MergeChildren splices a child plan in by
 * rewiring the source's consumer to the child's last node, so after the splice
 * that consumer sees the child's own field names.  A ProjectNode survives this
 * because arrow binds its expressions to field positions when the node is
 * built and re-labels the output with the declared names -- it is a naming
 * barrier, not a copy.  Drop it and a consumer that resolves columns by name
 * at run time fails: TPC-DS q23 died with "Nonexistent sort key column:
 * c_last_name(att1)" in TopKNode once its ProjectNode was elided.
 *
 * A ScanNode is a source too but is never spliced -- it is already the real
 * producer -- so projections on top of a scan stay eligible, which is where
 * most of the eliminated nodes come from.
 */
static bool
ProjectIsIdentity(GArrowExecuteNode *input, PlanBuildContext *pcontext)
{
	g_autoptr(GArrowSchema) schema = NULL;
	const gchar *kind;
	gsize i;

	if (input == NULL || !pcontext->proj_all_passthrough ||
		pcontext->proj_length == 0 || pcontext->proj_names == NULL)
		return false;

	kind = garrow_execute_node_get_kind_name(input);
	if (kind != NULL &&
		(strcmp(kind, "SourceNode") == 0 ||
		 strcmp(kind, "SharedSourceNode") == 0))
		return false;

	schema = garrow_execute_node_get_output_schema(input);
	if (schema == NULL ||
		(gsize) garrow_schema_n_fields(schema) != pcontext->proj_length)
		return false;

	for (i = 0; i < pcontext->proj_length; i++)
	{
		g_autoptr(GArrowField) field = garrow_schema_get_field(schema, i);

		if (field == NULL ||
			strcmp(pcontext->proj_names[i], garrow_field_get_name(field)) != 0)
			return false;
	}
	return true;
}

static GArrowProjectNodeOptions *
build_project_options(List *targetList, PlanBuildContext *pcontext)
{
	GList *expressions = NULL;
	g_autoptr(GArrowProjectNodeOptions) options = NULL;
	gsize			length;
	int				i = 0;
	ListCell	   *l;
	const gchar		  **names;
	bool			all_passthrough = true;

	length = list_length(targetList);
	names = palloc(length * sizeof(gchar *));

	foreach(l, targetList)
	{
		pcontext->append_filed_index = i;
		g_autoptr(GArrowExpression) arrow_expr = NULL;

		TargetEntry *tle = (TargetEntry *)lfirst(l);
		/* rebuild Target Entry to List*/
		Assert(IsA(tle, TargetEntry));
		/* use targetEntry name */
		arrow_expr = expr_to_arrow_expression(tle->expr, pcontext);
		if (pcontext->is_append)
		{
			char *name = (char *) palloc(20);
			snprintf(name, 20, "append_%d", i);
			names[i] = name;
		}
		else
			names[i] = (gchar *)GetUniqueAttrName(tle->resname, i + 1);

		/*
		 * Passthrough detection for ProjectIsIdentity(): arrow prints a
		 * field_ref as the bare field name, so an expression whose string
		 * form equals the output name can only be field_ref(name).
		 */
		{
			g_autofree gchar *exprstr =
				garrow_expression_to_string(arrow_expr);

			if (exprstr == NULL || strcmp(exprstr, names[i]) != 0)
				all_passthrough = false;
		}

		expressions = garrow_list_append_ptr(expressions, arrow_expr);
		i++;
	}
	/*
	 * Sonic motion direct-send: when the upstream Sonic agg added a
	 * leading hidden target-segment column (named by
	 * sonic_target_segment_col_name), append a passthrough field_ref so
	 * the project carries it through to the Motion node. Append at the
	 * end so existing Var(varattno = i+1) references in targetList stay
	 * valid (they refer to positions 1..length in the post-project
	 * output, which we leave unchanged).
	 */
	if (pcontext->sonic_target_segment_passthrough)
	{
		const char *seg_name = pcontext->sonic_target_segment_col_name;
		Assert(seg_name != NULL);
		g_autoptr(GError) seg_err = NULL;
		GArrowExpression *seg_expr = GARROW_EXPRESSION(
			garrow_field_expression_new(seg_name, &seg_err));
		if (seg_err != NULL)
			elog(ERROR,
				 "sonic motion direct-send: build target_segment_col_name "
				 "field_ref failed: %s",
				 garrow_take_error(&seg_err));

		const gchar **names_extended =
			palloc((length + 1) * sizeof(gchar *));
		for (gsize j = 0; j < length; ++j)
			names_extended[j] = names[j];
		names_extended[length] = seg_name;
		/* garrow_list_append_ptr requires an lvalue (it takes &C); use the
		 * named local seg_expr rather than a temporary. The list takes
		 * ownership, so we don't unref afterwards. */
		expressions = garrow_list_append_ptr(expressions, seg_expr);
		options = garrow_project_node_options_new(expressions,
												  (gchar**)names_extended,
												  length + 1);
		pcontext->proj_names = names_extended;
		pcontext->proj_length = length + 1;
	}
	else
	{
		options = garrow_project_node_options_new(expressions,
												  (gchar**)names,
												  length);
		pcontext->proj_names = names;
		pcontext->proj_length = length;
	}
	/*
	 * names / names_extended are handed to pcontext for ProjectIsIdentity();
	 * both live in the per-plan memory context, so there is nothing to free.
	 */
	pcontext->proj_all_passthrough = all_passthrough;
	pcontext->append_filed_index = 0;

	if (DEBUG1 >= log_min_messages)
	{
		for (GList *node = expressions; node; node = node->next)
		{
			g_autofree gchar *str =
				garrow_expression_to_string(GARROW_EXPRESSION(node->data));
			elog(DEBUG1, "%s result expressions: %s", __func__, str);
		}
	}
	garrow_list_free_ptr(&expressions);

	return garrow_move_ptr(options);
}

/*
 * The distinct of group by key needs to carry other fields, which is the default for pg, 
 * but it needs to be provided for arrow, otherwise the column will not be found when build _outagg_dummy agg.
 * 
 * case1: fallback
 * explain verbose SELECT DISTINCT count(unique1) FROM tenk1 group by unique1;
 * ->  HashAggregate  (cost=183.33..216.67 rows=3333 width=12)
 *       Output: (count(unique1)), unique1
 *       Group Key: (count(tenk1.unique1))
 *
 * case2: normal
 * explain verbose select unique1 from tenk1 a where unique1 in
 * (select unique1 from tenk1 b join tenk1 c using (unique1) where b.unique2 = 42);
 * ->  HashAggregate  
 *       Output: b.unique1, c.unique1
 *       Group Key: b.unique1
 * 
 * for case1, we should fallback since unique1 is junk column, we will get n rows, but acutal one result.
 * for case2, normal process.
 */
static void 
rewrite_tl_keys(List *targetList, PlanBuildContext *pcontext)
{
	Agg *agg = (Agg *) pcontext->planstate->plan;
	gsize nkey = agg->numCols;
	int length = list_length(targetList);
	const gchar	  **keys;
	int j;
	ListCell	   *l;

	/*
	 * targetList may be empty.
	 * Vec GroupAggregate  (cost=0.00..431.00 rows=1 width=1)
     * 	   Group Key: col_a
	 */
	if (!targetList)
		return;

	j = 0;
	keys = palloc(length * sizeof(gchar *));
	foreach(l, targetList)
	{
		TargetEntry *tle = lfirst(l);
		Node *node = (Node *)tle->expr;
		bool find = false;
		const char* cur;
		if (nodeTag(node) != T_Var)
			continue;
		Var *var = (Var *)node;
		int i = 0;
		for (i = 0; i < nkey; i++) 
		{
			AttrNumber attnum = agg->grpColIdx[i];
			if (var->varattno == attnum)
				break;
		}
		if (i >= nkey) /* not found */
			cur = GetSchemaName(pcontext->inputschema, var->varattno, pcontext->map);
		else
			cur = pcontext->keys[i];
		/*
		 * SELECT a/2, a/2 FROM test_missing_target group by a/2;
		 * ->  Vec GroupAggregate  (cost=0.00..431.00 rows=1 width=4)
         * Output: ((a / 2)), ((a / 2))
         * Group Key: ((test_missing_target.a / 2))
		 */
		/* targetList has the same key */
		for (int k = 0; k < j; k++)
		{
			if (strcmp(keys[k], cur) == 0)
			{
				find = true;
				break;
			}
		}
		if (!find)
			keys[j++] = cur;
	}

	/*
	 * If the loop above collected no key (j == 0), the targetList contained
	 * no Var that referenced a grouping column -- e.g. a UNION ALL branch
	 * that projects only a literal label like SELECT 'A' FROM t GROUP BY k
	 * where GPORCA pruned k out of the output.
	 *
	 * Overwriting pcontext->keys/nkey with the empty result wipes the keys
	 * established by build_agg_project_options. Downstream effect: the
	 * synthetic plain_distinct branch in BuildAggregatation is gated on
	 * (pcontext->nkey > 0) and gets skipped; the Arrow agg is then built
	 * with empty keys + empty aggregations and routes to
	 * ScalarAggregateNode (aggregate_node.cc:3027). Its Finish() hard-codes
	 * ExecBatch{values={}, length=1}, so each segment emits a single
	 * 1-row, 0-column batch regardless of the true group count, and the
	 * partial agg above counts N segments instead of the real cardinality
	 * (the reproducer SQL in agg/vec_const_targetlist returned A=3, B=3
	 * instead of A=100, B=200 on a 3-segment cluster).
	 *
	 * Keep the original keys in that case. Guarding on j (the loop's
	 * actual output) rather than on a pre-loop "does targetList contain
	 * Var" probe keeps the check valid if future changes to the loop add
	 * further filtering of Var entries.
	 */
	if (j == 0)
	{
		pfree(keys);
		return;
	}

	pcontext->keys = keys;
	pcontext->nkey = j;
}


static GArrowExecuteNode*
build_orderby_node(PlanState *planstate, GArrowExecutePlan *plan,  GArrowExecuteNode *input)
{
	int nkeys;
	GList *sort_keys = NULL;
	g_autoptr(GError) error = NULL;
	g_autoptr(GArrowSortOptions) sortoption = NULL;
	g_autoptr(GArrowOrderbyNodeOptions) orderby_options = NULL;
	GArrowExecuteNode *orderby_node = NULL;
	g_autoptr(GArrowSchema) schema = NULL;

	VecSortState *node = (VecSortState *) planstate;
	Sort	   *plannode = (Sort *) node->base.ss.ps.plan;

	schema = garrow_execute_node_get_output_schema(input);
	nkeys = plannode->numCols;

	/*
	 * sort option
	 *
	 * Use vec_sort_options_new (rather than the bare garrow_sort_options_new)
	 * so the sort spill GUCs reach the orderby node. A standalone Sort
	 * feeding a sorted GroupAggregate (e.g. TPC-DS q67's ROLLUP under
	 * optimizer_enable_hashagg=off) otherwise accumulates the whole input in
	 * memory here; vector.sort_spill_memory_mb bounds it: past that budget
	 * the orderby node writes sorted runs and merges them while emitting.
	 */
	sort_keys = build_sort_keys(planstate, schema);
	sortoption = vec_sort_options_new(sort_keys, partition_top_k, take_thread_num, two_phase_take,
									planstate->plan->plan_node_id);
	orderby_options = garrow_orderby_node_options_new(sortoption);
	orderby_node = garrow_execute_plan_build_orderby_node(plan, input, orderby_options, &error);
	if (error)
		elog(ERROR, "Failed to create orderby node, cause: %s", garrow_take_error(&error));
	garrow_list_free_ptr(&sort_keys);
	return orderby_node;
}

/*
 * find_seqscan_for_topk - Traverse from Sort's child to find SeqScan.
 * Supports Sort→SeqScan and Sort→Result→SeqScan topologies.
 * Returns NULL if SeqScan not found (TopK RF silently disabled).
 */
static VecSeqScanState *
find_seqscan_for_topk(PlanState *sort_planstate)
{
	PlanState *child = outerPlanState(sort_planstate);

	/* Skip through Result node (projection) */
	if (child && IsA(child, ResultState))
		child = outerPlanState(child);

	if (child && IsA(child, SeqScanState))
		return (VecSeqScanState *)child;

	return NULL;
}

static GArrowExecuteNode*
build_topk_node(PlanState *planstate, GArrowExecutePlan *plan,
				GArrowExecuteNode *input, int64 topk_bound)
{
	GList *sort_keys = NULL;
	g_autoptr(GError) error = NULL;
	g_autoptr(GArrowSortOptions) sortoption = NULL;
	g_autoptr(GArrowTopKNodeOptions) topk_options = NULL;
	GArrowExecuteNode *topk_node = NULL;
	g_autoptr(GArrowSchema) schema = NULL;

	schema = garrow_execute_node_get_output_schema(input);

	/* Reuse build_sort_keys to construct sort key list */
	sort_keys = build_sort_keys(planstate, schema);
	sortoption = garrow_sort_options_new(sort_keys, 0, take_thread_num, two_phase_take);

	/* TopK Runtime Filter: try to get threshold state from PAX */
	gpointer threshold_ptr = NULL;
	int sort_column_index = -1;
	Oid collation = InvalidOid;

	if (topk_runtime_filter)
	{
		VecSeqScanState *scan_state = find_seqscan_for_topk(planstate);
		if (scan_state && scan_state->scan_node_options)
		{
			Sort *sort = (Sort *)planstate->plan;
			AttrNumber sort_attno = sort->sortColIdx[0]; /* 1-based in Sort output */
			bool nulls_first = sort->nullsFirst[0];

			/* Map sort column from Sort's output back to table's physical attnum.
			 * sortColIdx references a column in Sort's input (SeqScan's output).
			 * SeqScan may project only a subset of columns, so the Var in
			 * Sort's targetlist has varattno = position in SeqScan's output,
			 * not the original table column. We look up SeqScan's targetlist
			 * to find the original table varattno. */
			AttrNumber table_attno = InvalidAttrNumber;
			{
				Plan *scan_plan = outerPlan(sort);
				/*
				 * If a Result node sits between Sort and SeqScan, sort_attno
				 * is a resno in Result's output, not SeqScan's. Remap it
				 * through Result's targetlist before descending — Var.varattno
				 * inside Result's tlist is the resno in SeqScan's output, so
				 * it can be reused as the new sort_attno. If the entry is an
				 * expression (not a Var), we cannot map to a physical column
				 * and must bail out.
				 */
				if (scan_plan && IsA(scan_plan, Result))
				{
					TargetEntry *res_tle =
						get_tle_by_resno(scan_plan->targetlist, sort_attno);
					if (res_tle && IsA(res_tle->expr, Var))
						sort_attno = ((Var *) res_tle->expr)->varattno;
					else
						goto skip_topk_rf;
					scan_plan = outerPlan(scan_plan);
				}

				if (scan_plan)
				{
					TargetEntry *tle = get_tle_by_resno(scan_plan->targetlist, sort_attno);
					if (tle && IsA(tle->expr, Var))
					{
						Var *var = (Var *)tle->expr;
						table_attno = var->varattno; /* 1-based physical column */
					}
				}
			}

			if (!AttributeNumberIsValid(table_attno) || table_attno <= 0)
			{
				/* Cannot map to physical column (expression sort key, etc.) */
				goto skip_topk_rf;
			}

			collation = sort->collations[0];

			/*
			 * NULLs occupy the top-K positions iff they sort first, which is
			 * exactly NULLS FIRST — independent of ASC/DESC. The RF reads a
			 * scalar threshold and cannot represent NULL, so disable it
			 * whenever NULLs could be in the top-K.
			 */
			if (!nulls_first)
			{
				threshold_ptr =
					garrow_scan_node_options_get_topk_threshold_state(
						scan_state->scan_node_options);
				sort_column_index = table_attno - 1;  /* 0-based physical column */
			}
skip_topk_rf:
			;  /* label requires a statement */
		}
	}

	/* Create TopKNode with optional runtime filter */
	g_autoptr(GArrowTopKRuntimeFilterOptions) rf_options = NULL;
	if (threshold_ptr)
		rf_options = garrow_topk_runtime_filter_options_new(threshold_ptr,
															sort_column_index,
															(guint32)collation);
	topk_options = garrow_topk_node_options_new(topk_bound, sortoption,
												rf_options);
	topk_node = garrow_execute_plan_build_topk_node(plan, input,
													topk_options, &error);
	if (error)
	{
		garrow_list_free_ptr(&sort_keys);
		elog(ERROR, "Failed to create topk node, cause: %s", garrow_take_error(&error));
	}

	/*
	 * Record the K value on the Sort's VecSortState so that show_sort_info
	 * can emit "Vec Sort Method: TopK K: N" in EXPLAIN output.  This is the
	 * single source of truth for whether a given Sort took the TopK path —
	 * the EXPLAIN display and the actual Arrow node construction both happen
	 * here.
	 */
	((VecSortState *) planstate)->topk_bound = topk_bound;

	garrow_list_free_ptr(&sort_keys);
	return topk_node;
}

/*
 * Build an Arrow "partition_topk" node from a PartitionTopK plan node.
 *
 * Per distinct partition-key combination the Arrow node keeps every row
 * whose rank() by the sort keys is <= top_k (ties at rank k all kept),
 * matching the row executor's nodePartitionTopK.c semantics. It is a pure
 * pass-through filter, so the input schema is reused unchanged.
 */
static GArrowExecuteNode *
build_partition_topk_node(PlanState *planstate, GArrowExecutePlan *plan,
						  GArrowExecuteNode *input)
{
	PartitionTopK *node = (PartitionTopK *) planstate->plan;
	g_autoptr(GError) error = NULL;
	g_autoptr(GArrowSchema) schema = NULL;
	g_autoptr(GArrowSortOptions) sortoption = NULL;
	g_autoptr(GArrowPartitionTopKNodeOptions) options = NULL;
	GArrowExecuteNode *ptopk_node = NULL;
	GList *sort_keys = NULL;
	SortSupport sortKeys;
	gint *partition_cols;
	int i;

	schema = garrow_execute_node_get_output_schema(input);

	/* Build sort keys from the PartitionTopK plan fields */
	sortKeys = (SortSupport) palloc0(node->numSortCols * sizeof(SortSupportData));
	for (i = 0; i < node->numSortCols; i++)
	{
		SortSupport sortKey = sortKeys + i;

		sortKey->ssup_cxt = CurrentMemoryContext;
		sortKey->ssup_collation = node->collations[i];
		sortKey->ssup_nulls_first = node->nullsFirst[i];
		sortKey->ssup_attno = node->sortColIdx[i];
		sortKey->abbreviate = (i == 0);

		PrepareSortSupportFromOrderingOp(node->sortOperators[i], sortKey);
	}
	sort_keys = create_sort_keys(sortKeys, node->numSortCols, schema);
	pfree(sortKeys);
	sortoption = garrow_sort_options_new(sort_keys, 0, take_thread_num,
										 two_phase_take);

	/* Partition key attnos are 1-based positions in the input tuple */
	partition_cols = (gint *) palloc(node->numPartitionCols * sizeof(gint));
	for (i = 0; i < node->numPartitionCols; i++)
		partition_cols[i] = node->partitionColIdx[i] - 1;

	options = garrow_partition_topk_node_options_new(node->top_k, sortoption,
													 partition_cols,
													 node->numPartitionCols);
	pfree(partition_cols);

	ptopk_node = garrow_execute_plan_build_partition_topk_node(plan, input,
															   options, &error);
	if (error)
	{
		garrow_list_free_ptr(&sort_keys);
		elog(ERROR, "Failed to create partition_topk node, cause: %s",
			 garrow_take_error(&error));
	}

	garrow_list_free_ptr(&sort_keys);
	return ptopk_node;
}

/*
 * Returns true if every key column in the agg's input schema is one
 * sonic's SonicGroupByNode currently knows how to scatter / hash /
 * compare. Mirrors compute/sonic/exec_node.cc::ArrowTypeToKeyType.
 */
static bool
sonic_supports_key_type(GArrowType id)
{
	switch (id)
	{
		case GARROW_TYPE_BOOLEAN:
		case GARROW_TYPE_INT8:
		case GARROW_TYPE_INT16:
		case GARROW_TYPE_INT32:
		case GARROW_TYPE_INT64:
		case GARROW_TYPE_DATE32:
		case GARROW_TYPE_DATE64:
		case GARROW_TYPE_TIME32:
		case GARROW_TYPE_TIME64:
		case GARROW_TYPE_TIMESTAMP:
		case GARROW_TYPE_FLOAT:
		case GARROW_TYPE_DOUBLE:
		case GARROW_TYPE_STRING:
		case GARROW_TYPE_BINARY:
		case GARROW_TYPE_NUMERIC128:
			return true;
		default:
			return false;
	}
}

/*
 * Sonic's hash_sum / hash_mean_numeric currently accept signed integer
 * inputs (and the date / time variants that map onto an integer width).
 */
static bool
sonic_supports_signed_int_input(GArrowType id)
{
	switch (id)
	{
		case GARROW_TYPE_INT16:
		case GARROW_TYPE_INT32:
		case GARROW_TYPE_INT64:
		case GARROW_TYPE_DATE32:
		case GARROW_TYPE_DATE64:
		case GARROW_TYPE_TIME32:
		case GARROW_TYPE_TIME64:
		case GARROW_TYPE_TIMESTAMP:
			return true;
		default:
			return false;
	}
}

/*
 * Sonic's hash_min currently accepts utf8 / binary inputs only.
 */
static bool
sonic_supports_text_input(GArrowType id)
{
	return id == GARROW_TYPE_STRING || id == GARROW_TYPE_BINARY;
}

/*
 * Sonic's hash_min / hash_max over fixed-width comparable values
 * (MinMaxAggregate<T>): signed integers, date32, float8 (double) and
 * numeric128.  These are exactly the input types Cloudberry's fmgr.c maps
 * for F_MIN_* / F_MAX_* (int2/4/8, date, float8, numeric).  Text input is
 * handled separately (sonic_supports_text_input) and only for MIN.
 */
static bool
sonic_supports_minmax_value_input(GArrowType id)
{
	switch (id)
	{
		case GARROW_TYPE_INT16:
		case GARROW_TYPE_INT32:
		case GARROW_TYPE_INT64:
		case GARROW_TYPE_DATE32:
		case GARROW_TYPE_DOUBLE:
		case GARROW_TYPE_NUMERIC128:
			return true;
		default:
			return false;
	}
}

/*
 * Decide whether sonic's SonicGroupByNode can handle this aggregate
 * shape. If anything is outside its current capability we fall back
 * to normal-mode hash agg so the query still runs (correctness over
 * speed). The set checked here MUST be a subset of what sonic's
 * MakeAggregateFunctions / ArrowTypeToKeyType actually implement —
 * keep these two in sync when extending sonic.
 */
static bool
is_sonic_compatible(List *aggInfos, PlanBuildContext *pcontext)
{
	ListCell *l;

	/*
	 * The "no agg, just GROUP BY" path (aggInfos == NIL) is supported:
	 * SonicGroupByNode handles zero aggregates natively, deduplicating
	 * the group keys and emitting the unique rows.  BuildAggregatation
	 * skips synthesizing the hash_distinct placeholder when this returns
	 * true, so sonic receives an empty aggregate list.  The foreach over
	 * aggInfos below is simply a no-op in that case, leaving only the
	 * key-type check to gate routing.
	 */

	/* Group keys: every key column type must be one sonic supports. */
	for (int i = 0; i < pcontext->nkey; i++)
	{
		g_autoptr(GArrowField) field = NULL;

		/* a trimmed numeric key only exists after the agg input project */
		if (bms_is_member(i, pcontext->numeric_canon_keys))
		{
			if (!sonic_supports_key_type(GARROW_TYPE_NUMERIC128))
				return false;
			continue;
		}
		field = garrow_schema_get_field_by_name(pcontext->inputschema,
												pcontext->keys[i]);
		if (!field)
			return false;
		g_autoptr(GArrowDataType) type = garrow_field_get_data_type(field);
		GArrowType id = garrow_data_type_get_id(type);
		if (!sonic_supports_key_type(id))
			return false;
	}

	/* Aggregates: function name + input column type must both be supported. */
	foreach(l, aggInfos)
	{
		VecAggInfo *agginfo = (VecAggInfo *) lfirst(l);
		const char *fn = agginfo->aname;

		if (strcmp(fn, "hash_count") == 0)
		{
			/*
			 * Sonic handles both shapes of count:
			 *   count(*)   -> CountOptions::ALL        -> CountStarAggregate
			 *   count(col) -> CountOptions::ONLY_VALID -> CountColAggregate
			 *                                            (reads only the
			 *                                             validity bitmap,
			 *                                             type-agnostic)
			 * MakeAggregateFunctions in compute/sonic/exec_node.cc picks
			 * between them on agg.options->mode, which our
			 * build_all_count_options sets per list_length(aggref->args).
			 * Neither path needs an input-type check here.
			 *
			 * count(distinct col) is the aggregate "hash_count_distinct"
			 * (a different function name); it falls through to the
			 * unconditional fallback at the bottom of this loop, routing
			 * DISTINCT counts to normal mode (sonic does not implement
			 * hash_count_distinct).
			 */
			continue;
		}

		/*
		 * Every other supported sonic agg requires a resolved input
		 * column. If the aggref has no args (shouldn't happen for
		 * hash_sum / hash_min / hash_mean_numeric), bail to be safe.
		 */
		if (!agginfo->aggref || !agginfo->aggref->args)
			return false;
		TargetEntry *tle = (TargetEntry *) linitial(agginfo->aggref->args);
		Oid pg_type = exprType((Node *) tle->expr);
		GArrowType in_id = PGTypeToArrowID(pg_type);

		if (strcmp(fn, "hash_sum") == 0)
		{
			/* signed int (sonic SumIntAggregate), numeric128
			 * (SumNumeric128Aggregate, used by single-stage SUM(numeric)
			 * and final stage of two-stage SUM(int8)), or float8
			 * (SumDoubleAggregate, all stages of SUM(float8)). */
			if (!sonic_supports_signed_int_input(in_id) &&
				in_id != GARROW_TYPE_NUMERIC128 &&
				in_id != GARROW_TYPE_DOUBLE)
				return false;
		}
		else if (strcmp(fn, "hash_sum_64") == 0)
		{
			/* Partial stage of SUM(int8): int64 input → numeric128.
			 * Also accepts numeric128 input (Arrow registers it for
			 * symmetry; sonic routes it to SumNumeric128Aggregate). */
			if (in_id != GARROW_TYPE_INT64 &&
				in_id != GARROW_TYPE_DATE64 &&
				in_id != GARROW_TYPE_TIME64 &&
				in_id != GARROW_TYPE_TIMESTAMP &&
				in_id != GARROW_TYPE_NUMERIC128)
				return false;
		}
		else if (strcmp(fn, "hash_min") == 0)
		{
			/* MIN: text/binary (MinTextAggregate) or a fixed-width
			 * comparable value type (MinMaxAggregate<T>). */
			if (!sonic_supports_text_input(in_id) &&
				!sonic_supports_minmax_value_input(in_id))
				return false;
		}
		else if (strcmp(fn, "hash_max") == 0)
		{
			/* MAX: only fixed-width comparable value types — Cloudberry
			 * does not push MAX(text) (no F_MAX_TEXT mapping). */
			if (!sonic_supports_minmax_value_input(in_id))
				return false;
		}
		else if (strcmp(fn, "hash_mean_numeric") == 0)
		{
			/* Single-stage AVG: signed integer (widened to Numeric128 on
			 * accumulation) or numeric128 (read directly). Covers AVG(int*)
			 * and AVG(numeric) when GROUP BY is on the distribution key
			 * (no Motion).  AVG(float8) uses hash_mean, not this. */
			if (!sonic_supports_signed_int_input(in_id) &&
				in_id != GARROW_TYPE_NUMERIC128)
				return false;
		}
		else if (strcmp(fn, "hash_avg_trans") == 0)
		{
			/*
			 * Two-phase AVG partial. Sonic supports signed integer input
			 * (output struct<int64, int64>), numeric128 input (output
			 * struct<numeric128, int64>) and float8 input (output
			 * struct<double, int64>, the partial of AVG(float8)).
			 */
			if (!sonic_supports_signed_int_input(in_id) &&
				in_id != GARROW_TYPE_NUMERIC128 &&
				in_id != GARROW_TYPE_DOUBLE)
				return false;
		}
		else if (strcmp(fn, "hash_avg_final") == 0)
		{
			/*
			 * Two-phase AVG final over an Arrow struct<sum, count>. Sonic
			 * now implements the int64-sum (AvgFinalIntAggregate),
			 * numeric128-sum (AvgFinalNumeric128Aggregate) and double-sum
			 * (AvgFinalDoubleAggregate, the AVG(float8) final) variants —
			 * i.e. every AVG kind Cloudberry pushes — so no input-type
			 * check is needed here. Sonic's MakeAggregateFunctions selects
			 * the variant from the struct's sum-field type at build time.
			 */
		}
		else if (strcmp(fn, "hash_mean") == 0)
		{
			/* Single-stage AVG(float8): double input -> float64
			 * (MeanDoubleAggregate). Only float8 is pushed. */
			if (in_id != GARROW_TYPE_DOUBLE)
				return false;
		}
		else if (strcmp(fn, "hash_sum_final") == 0)
		{
			/*
			 * Two-phase SUM(numeric) final over a struct<numeric128,
			 * count> partial (SumFinalNumeric128Aggregate). The only
			 * aggregate Cloudberry maps to sum_final is F_SUM_NUMERIC
			 * (fmgr.c), whose partial sum is always numeric128, so gate on
			 * the aggfnoid: this keeps the routing precise and future-proof
			 * (a new sum_final variant with a different aggfnoid falls back
			 * rather than reaching sonic's numeric128-only implementation).
			 * The partial column name (_inagg_N) is not in inputschema, so
			 * we cannot inspect the Arrow struct directly.
			 */
			if (agginfo->aggref->aggfnoid != F_SUM_NUMERIC)
				return false;
		}
		else if (strcmp(fn, "hash_avg_combine") == 0)
		{
			/*
			 * Three-stage middle combine (AGGSPLIT_INTERMEDIATE) over a
			 * struct<sum, count> partial: merge partials and re-emit the struct
			 * without finalizing.  sonic selects the combine variant from the
			 * partial's sum-field type, so gate on the exact aggfnoids fmgr.c
			 * maps to avg_combine and whose partial type sonic implements:
			 *   F_AVG_NUMERIC / F_SUM_NUMERIC / F_AVG_INT8 -> numeric128
			 *     (avg(int8)'s partial is widened to numeric128 to avoid int64
			 *     overflow, so it reuses the same combine as avg(numeric)),
			 *   F_AVG_INT4 / F_AVG_INT2                    -> int64,
			 *   F_AVG_FLOAT8                                -> double.
			 * Any other combinefn mapping falls back to normal mode rather than
			 * reaching a sonic variant that does not exist.
			 */
			Oid combinefnoid = agginfo->aggref->aggfnoid;
			if (combinefnoid != F_AVG_NUMERIC && combinefnoid != F_SUM_NUMERIC &&
				combinefnoid != F_AVG_INT8 &&
				combinefnoid != F_AVG_INT4 && combinefnoid != F_AVG_INT2 &&
				combinefnoid != F_AVG_FLOAT8)
				return false;
		}
		else if (strcmp(fn, "hash_stddev_numeric") == 0)
		{
			/* Single-stage STDDEV_SAMP(int4) -> numeric128
			 * (StdDevSampNumericAggregate). Only int4 (INT32) is pushed
			 * (F_STDDEV_SAMP_INT4). */
			if (in_id != GARROW_TYPE_INT32)
				return false;
		}
		else if (strcmp(fn, "hash_avg_trans_stddev") == 0)
		{
			/* Two-stage STDDEV_SAMP partial: int4 -> struct<sum,count,
			 * square> (AvgTransStddevAggregate). Only int4 is pushed. */
			if (in_id != GARROW_TYPE_INT32)
				return false;
		}
		else if (strcmp(fn, "hash_avg_final_stddev") == 0)
		{
			/* Two-stage STDDEV_SAMP final over struct<sum,count,square>
			 * (AvgFinalStddevAggregate). Only STDDEV_SAMP(int4) produces
			 * this partial, so no input-type check is needed here; sonic's
			 * MakeAggregateFunctions validates the 3-field struct shape. */
		}
		else if (strcmp(fn, "hash_avg_combine_stddev") == 0)
		{
			/* Three-stage middle combine (AGGSPLIT_INTERMEDIATE) over a
			 * struct<sum,count,square> partial: merge partials and re-emit
			 * the struct without finalizing (no variance/sqrt --
			 * AvgCombineStddevAggregate). Only STDDEV_SAMP(int4) produces
			 * this partial, so no input-type check is needed here; sonic's
			 * MakeAggregateFunctions validates the 3-field struct shape. */
		}
		else
		{
			/*
			 * Any other function name (hash_count_distinct, etc.) is not
			 * implemented in sonic, so route to the Normal vectorized
			 * method (Arrow GroupByNode).
			 *
			 * Defensive / unreachable-by-design: the upstream
			 * vectorizability gate (is_aggfn_vectorable, fmgr.c) only
			 * vectorizes aggregates that sonic also supports, so anything
			 * sonic cannot do falls back to a row-mode HashAggregate before
			 * a vec agg node is ever built -- no SQL query currently reaches
			 * this branch. Kept so that if a future aggregate becomes
			 * vectorizable before sonic supports it, routing degrades
			 * gracefully to Normal instead of failing. A vectorizable query
			 * actually hitting this path signals a sonic coverage gap to
			 * close, not expected steady-state behavior.
			 */
			return false;
		}
	}
	return true;
}

static GArrowExecuteNode *
BuildAggregatation(List *aggInfos, GArrowExecuteNode *input, PlanBuildContext *pcontext)
{
	ListCell	   *l;
	GList *aggregations = NULL;
	g_autoptr(GArrowExecuteNode) aggregate = NULL;
	g_autoptr(GArrowExecuteNode) orderbynode = NULL;
	g_autoptr(GArrowAggregation) agg_func = NULL;
	g_autoptr(GArrowAggregateNodeOptions) options = NULL;
	g_autoptr(GError) error = NULL;

	/* build aggregations */
	foreach(l, aggInfos)
	{
		VecAggInfo *agginfo = (VecAggInfo *) lfirst(l);

		/*Fixme only normal agg now. */
		if (true)
		{
			agg_func = garrow_aggregation_new(
					   agginfo->aname,
					   agginfo->options,
					   agginfo->inname,
					   agginfo->outname);
			aggregations = garrow_list_append_ptr(aggregations, agg_func);
		}
	}

	/*
	 * Decide sonic routing up-front.  It depends only on aggInfos and the
	 * group-key types, not on the synthetic distinct appended below, so we
	 * can compute it before the synthesis step.  We need the answer here
	 * because a sonic-routed bare GROUP BY must skip the placeholder
	 * entirely (see below).
	 */
	bool sonic_ok = false;
	if (pcontext->aggstrategy == AGG_HASHED)
	{
		sonic_ok = is_sonic_compatible(aggInfos, pcontext);
		if (!sonic_ok)
			elog(DEBUG1, "sonic: query keys/aggs not all supported, "
						 "falling back to normal-mode hash agg");
	}

	/* no aggref in targetlist, append plain/hash distinct for group by columns.
	 * Out name will not be used, use dummy.
	 *
	 * Sonic does not need this placeholder: SonicGroupByNode handles zero
	 * aggregates natively and just emits the unique keys, whereas Arrow's
	 * hash_distinct kernel would compute a per-group distinct-value list
	 * only to have its output discarded.  So skip the synthesis when the
	 * bare GROUP BY is routed to sonic (sonic_ok is true only for the
	 * AGG_HASHED path, so the AGG_SORTED plain_distinct is unaffected).
	 */
	if (!IsA(pcontext->planstate, WindowAggState) && !IsA(pcontext->planstate, WindowHashAggState) && ((pcontext->nkey > 0) && (!aggregations)) && !sonic_ok)
	{
		agg_func = garrow_aggregation_new(
			pcontext->aggstrategy == AGG_SORTED ? "plain_distinct" : "hash_distinct",
			NULL,
			pcontext->keys[0],
			"_outagg_dummy");
		aggregations = garrow_list_append_ptr(aggregations, agg_func);
	}

	options = build_aggregatation_options(aggregations, pcontext, sonic_ok);

	aggregate = garrow_execute_plan_build_aggregate_node(pcontext->plan,
														input,
														options,
														&error);
	if (error)
		elog(ERROR, "Failed to create agg node, cause: %s", garrow_take_error(&error));

	/* release agginfos to prevent building aggregations repeatedly*/
	garrow_list_free_ptr(&aggregations);
	return garrow_move_ptr(aggregate);
}


/*
 * Walker context for find_agg_parent_limit().  base must come first so
 * the node can be passed to plan_tree_walker.
 */
typedef struct
{
	plan_tree_base_prefix	base;
	Plan				   *target;	/* HashAgg Plan we're searching for */
	Limit				   *found;	/* output: the parent Limit if any */
} agg_parent_limit_context;

/*
 * plan_tree_walker callback: stop and record the Limit node whose
 * outerPlan is our target Agg.  Pre-order so the *immediate* Limit
 * parent is the first one we hit (a chain "Limit -> ... -> Agg" can
 * only have one immediate parent anyway).
 */
static bool
agg_parent_limit_walker(Node *node, agg_parent_limit_context *ctx)
{
	if (node == NULL)
		return false;

	if (IsA(node, Limit) && outerPlan(node) == ctx->target)
	{
		ctx->found = (Limit *) node;
		return true;
	}
	return plan_tree_walker(node, agg_parent_limit_walker, ctx, true);
}

/*
 * Search the full Plan tree for a Limit whose immediate outerPlan is
 * 'target'.  Returns the parent Limit if found, else NULL.  Walks the
 * full tree (not just the slice this segment runs) because on the QD
 * ExecutorStart initializes every PlanState in the dispatched
 * PlannedStmt, including nodes that belong to remote slices.
 *
 * The MPP "push limit through motion" optimization produces
 *     Limit (top, count=O+C)
 *       -> Gather Motion
 *         -> Limit (segment, count=O+C, offset=0)
 *           -> HashAgg
 * so the Agg's *immediate* parent Limit lives below the motion, not at
 * the top of the tree.  A direct "topPlan->outerPlan == target" check
 * would miss it; the walker finds it regardless of intervening nodes.
 */
static Limit *
find_agg_parent_limit(PlannedStmt *stmt, Plan *target)
{
	agg_parent_limit_context ctx;

	exec_init_plan_tree_base(&ctx.base, stmt);
	ctx.target = target;
	ctx.found = NULL;
	agg_parent_limit_walker((Node *) stmt->planTree, &ctx);
	return ctx.found;
}

static GArrowAggregateNodeOptions *
build_aggregatation_options(GList *aggregations, PlanBuildContext *pcontext,
							bool sonic_ok)
{
	g_autoptr(GArrowAggregateNodeOptions) options = NULL;
	g_autoptr(GError) error = NULL;
	int64 limit_count = 0;

	/*
	 * Detect Limit+HashAgg fusion: walk the Plan tree to find any Limit
	 * whose direct child is our HashAgg, then pass that Limit's
	 * offset+count to Arrow so GroupByNode only tracks the first N
	 * groups.  In MPP the immediate parent Limit usually sits below a
	 * Gather Motion (see find_agg_parent_limit for the shape), so we
	 * cannot just inspect the top of the tree.
	 *
	 * We work on Plan, not PlanState: PlanState lacks a parent pointer,
	 * and the Agg and Limit are built in separate BuildVecPlan calls so
	 * no live PlanState chain exists at this point.
	 */
	if (limit_hashagg_max_total > 0 &&
		pcontext->aggstrategy == AGG_HASHED &&
		pcontext->planstate->state &&
		pcontext->planstate->state->es_plannedstmt)
	{
		Limit *limitPlan = find_agg_parent_limit(
			pcontext->planstate->state->es_plannedstmt,
			pcontext->planstate->plan);
		if (limitPlan)
		{
			/*
			 * Extract constant limit/offset from the Plan node.
			 * limitCount/limitOffset are expression nodes; for constant
			 * values they are Const nodes.
			 */
			int64 lim_offset = 0;
			int64 lim_count = 0;
			bool valid = true;

			if (limitPlan->limitCount && IsA(limitPlan->limitCount, Const))
			{
				Const *c = (Const *)limitPlan->limitCount;
				if (!c->constisnull)
					lim_count = DatumGetInt64(c->constvalue);
				else
					valid = false;
			}
			else if (limitPlan->limitCount)
			{
				/* Non-constant LIMIT expression — skip optimization */
				valid = false;
			}
			else
			{
				/* No LIMIT COUNT (LIMIT ALL) — skip */
				valid = false;
			}

			if (valid && limitPlan->limitOffset)
			{
				if (IsA(limitPlan->limitOffset, Const))
				{
					Const *c = (Const *)limitPlan->limitOffset;
					if (!c->constisnull)
						lim_offset = DatumGetInt64(c->constvalue);
				}
				else
				{
					/* Non-constant OFFSET expression — skip optimization */
					valid = false;
				}
			}

			if (valid && lim_count > 0)
			{
				int64 total;
				if (!pg_add_s64_overflow(lim_offset, lim_count, &total) &&
					total > 0)
				{
					if (total <= limit_hashagg_max_total)
					{
						limit_count = total;
						/*
						 * Record the effective limit on VecAggState so
						 * show_hashagg_info can surface the fusion in
						 * EXPLAIN output.  Cast is safe: the AGG_HASHED
						 * gate above guarantees planstate is an AggState
						 * and BuildVecPlan only invokes us through
						 * ExecInitVecAgg, which allocates
						 * sizeof(VecAggState).
						 */
						((VecAggState *) pcontext->planstate)->limit_count = limit_count;
						elog(DEBUG1, "LimitAgg: fusing Limit(%ld)+HashAgg, limit_count=%ld",
							 (long)lim_count, (long)limit_count);
					}
					else
					{
						elog(DEBUG1, "LimitAgg: total %ld exceeds vector.limit_hashagg_max_total %d, skipping",
							 (long)total, limit_hashagg_max_total);
					}
				}
				/* Overflow or non-positive total: silently skip fusion. */
			}
		}
	}

	if (IsA(pcontext->planstate, WindowAggState))
	{
		int mode = garrow_aggregate_get_window_mode();
		options =
			garrow_general_aggregate_node_options_new(aggregations,
													pcontext->keys,
													pcontext->nkey,
													mode,
													pcontext->orderby_sortoption,
													/* sonic_motion_direct_send */ NULL,
													&error);
	}
	else if (IsA(pcontext->planstate, WindowHashAggState))
	{
		if (winagg_spill_memory_mb <= 0)
		{
			/*
			 * Spill disabled by GUC — use the plain parallel-window variant
			 * (same call WindowAggState uses, no spill_file_base / no mkdir)
			 * so the spill plumbing never sees a zero budget.
			 */
			options =
				garrow_general_aggregate_node_options_new(aggregations,
														pcontext->keys,
														pcontext->nkey,
														garrow_aggregate_get_parallel_window_mode(),
														pcontext->orderby_sortoption,
														/* sonic_motion_direct_send */ NULL,
														&error);
		}
		else
		{
			char spill_file_base[MAXPGPATH];

			/*
			 * No file_ops are wired here: arrow substitutes its own
			 * POSIX-backed DefaultExternalSortFileOps() when the callback set
			 * is empty, matching the SortOptions pattern.
			 */
			VecSpillFileBase(spill_file_base, sizeof(spill_file_base),
							 pcontext->planstate->plan->plan_node_id);

			options =
				garrow_general_aggregate_node_options_new_with_spill(
														aggregations,
														pcontext->keys,
														pcontext->nkey,
														garrow_aggregate_get_parallel_window_mode(),
														pcontext->orderby_sortoption,
														NULL,
														spill_file_base,
														(gint64) winagg_spill_memory_mb * 1024L * 1024L,
														&error);
		}
	}
	else if (pcontext->aggstrategy == AGG_SORTED)
		options = garrow_ordered_aggregate_node_options_new(aggregations,
															pcontext->keys,
															pcontext->nkey,
															&error);
	else if (limit_count > 0 || !sonic_ok)
	{
		/*
		 * Normal-mode hash agg fallback. Two reasons we land here:
		 *   (a) Limit+HashAgg fusion: GroupByNode tracks only the first N
		 *       groups via AggregateNodeOptions::limit_count. Sonic does
		 *       not yet honor limit_count, so keep the legacy normal-mode
		 *       path when the planner enables this fusion.
		 *   (b) The query uses a key type or aggregate function sonic
		 *       hasn't implemented yet (is_sonic_compatible returned
		 *       false). Falling back keeps the query running at normal-
		 *       mode speed instead of erroring out at runtime — this is
		 *       important for ClickBench full-suite runs.
		 */
		options = garrow_aggregate_node_options_new(aggregations,
													pcontext->keys,
													pcontext->nkey,
													limit_count,
													&error);
		if (pcontext->aggstrategy == AGG_HASHED)
			((VecAggState *) pcontext->planstate)->method =
				(limit_count > 0) ? VEC_AGG_METHOD_LIMIT_FUSION
								  : VEC_AGG_METHOD_NORMAL;
	}
	else
	{
		/*
		 * Default hash-agg path: route to Sonic
		 * (compute/sonic/exec_node.cc -> SonicGroupByNode).
		 * Sonic's per-thread share-nothing sink + 256-partition parallel
		 * finalize replaces Arrow's GrouperFastImpl + serial Merge.
		 *
		 * Sonic Motion Direct-Send: when the parent HASH Motion's
		 * ExecInitVecMotion has published a hint (all gates passed,
		 * see sonic_motion_direct_send_design.md §2.3), build a
		 * GArrowSonicMotionDirectSendOptions from it and pass it into
		 * the aggregate options constructor so Sonic emits each batch
		 * pre-bucketed by target segment. No hint → pass NULL, default
		 * per-partition emit behavior is preserved.
		 */
		char spill_file_base[MAXPGPATH];
		int mode = garrow_aggregate_get_sonic_mode();
		g_autoptr(GArrowSonicMotionDirectSendOptions) ds_options = NULL;
		const VecMotionDirectSendHint *hint =
			vec_motion_direct_send_lookup(pcontext->planstate->plan);

		/*
		 * Before any GObject is created: this can ERROR, and the
		 * siglongjmp would skip the g_autoptr cleanup and leak them.
		 */
		if (sonicagg_spill_memory_mb > 0)
			VecSpillFileBase(spill_file_base, sizeof(spill_file_base),
							 pcontext->planstate->plan->plan_node_id);
		if (hint != NULL)
		{
			gint32 *idx = (gint32 *) palloc(sizeof(gint32) * hint->numHashCols);
			for (int i = 0; i < hint->numHashCols; ++i)
				idx[i] = (gint32) hint->hash_grpcol_idx[i];
			ds_options = garrow_sonic_motion_direct_send_options_new(
				(gint32) hint->numHashSegments,
				(gint32) hint->reduce_alg,
				idx, (gsize) hint->numHashCols,
				hint->target_segment_col_name);
			/*
			 * Only mark the project node as a passthrough for the hidden
			 * segment column when Sonic will actually emit it.  If the
			 * constructor failed (e.g. internal validation rejected the
			 * arguments and returned NULL), garrow_general_aggregate_node_options_new
			 * gets NULL too and Sonic never prepends the column — leaving the
			 * passthrough flag on would make build_project_options reference
			 * a non-existent field and fail Arrow plan construction.
			 */
			if (ds_options != NULL)
			{
				pcontext->sonic_target_segment_passthrough = true;
				pcontext->sonic_target_segment_col_name =
					hint->target_segment_col_name;
			}
			pfree(idx);
		}

		options = garrow_general_aggregate_node_options_new(aggregations,
															pcontext->keys,
															pcontext->nkey,
															mode,
															/* sort_options */ NULL,
															ds_options,
															&error);

		/*
		 * Wire spill onto the Sonic aggregate when enabled. We must NOT use
		 * garrow_general_aggregate_node_options_new_with_spill here because it
		 * drops ds_options; instead set the spill fields on the options just
		 * built. Files go where the window path puts them (VecSpillFileBase,
		 * reclaimed by CleanupArrowSpillFiles and the post-crash
		 * RemovePgTempFiles sweep); file_ops are left unset
		 * so arrow falls back to its POSIX temp-file implementation, same as
		 * the window path above. Routing through real PG temp-file callbacks
		 * (ResourceOwner-visible instead of relying on the exit/restart sweep)
		 * is a known, accepted gap shared by both spill paths — see the
		 * matching comment on garrow_general_aggregate_node_options_set_spill
		 * in compute.h — and would be a cross-cutting follow-up, not scoped
		 * to this commit.
		 */
		if (options != NULL && sonicagg_spill_memory_mb > 0)
		{
			garrow_general_aggregate_node_options_set_spill(
				GARROW_EXECUTE_NODE_OPTIONS(options),
				spill_file_base,
				(gint64) sonicagg_spill_memory_mb * 1024L * 1024L);
		}

		if (pcontext->aggstrategy == AGG_HASHED)
			((VecAggState *) pcontext->planstate)->method = VEC_AGG_METHOD_SONIC;
	}
	if (error)
		elog(ERROR, "Failed to create agg node options, cause: %s", garrow_take_error(&error));

	if (pcontext->keys)
		pfree(pcontext->keys);

	return garrow_move_ptr(options);
}

static GArrowStoreType
to_arrow_storetype(StoreType type)
{
	switch (type)
	{
		case IN_MEMORY:
			return GARROW_IN_MEMORY;
			break;
		case IN_SINGLE:
			return GARROW_IN_SINGLE;
			break;
		case IN_CHUNK:
			return GARROW_IN_CHUNK;
			break;
		default:
			elog(ERROR, "store type not supported by arrow storefile %d", type);
			break;
	}

	/* never run here */
	return GARROW_IN_MEMORY;
}

/*
 * contains_innervar_walker - true iff the expression tree references a Var
 * from the inner (build/right) side of the join.
 *
 * Used by to_arrow_jointype() to choose between Arrow's LEFT_SEMI/LEFT_ANTI
 * (probe-only output) and the heavier FULL_SEMI/FULL_ANTI (which also carry
 * the matched build-side payload through to the output).  A PG JOIN_SEMI /
 * JOIN_ANTI classically outputs only outer columns, but ORCA's nested
 * subquery pull-up can emit a semi/anti join whose targetlist references
 * inner columns; such a join MUST use FULL_* so the inner payload survives.
 */
static bool
contains_innervar_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, Var))
	{
		Var		   *var = (Var *) node;

		if (var->varno == INNER_VAR)
			return true;
	}
	return expression_tree_walker(node, contains_innervar_walker, context);
}

static GArrowJoinType
to_arrow_jointype(JoinType type, List *targetlist, List *joinqual)
{
	bool no_need_right_payload = !contains_innervar_walker((Node *) targetlist, NULL);
	switch (type)
	{
		case JOIN_INNER:
			return GARROW_INNER_JOIN;
		case JOIN_LEFT:
			return GARROW_LEFT_OUTER_JOIN;
		case JOIN_RIGHT:
			return GARROW_RIGHT_OUTER_JOIN;
		case JOIN_FULL:
			return GARROW_FULL_OUTER_JOIN;
		case JOIN_SEMI:
		{
			if (no_need_right_payload)
			{
				return GARROW_LEFT_SEMI_JOIN;
			}
			else
			{
				return GARROW_FULL_SEMI_JOIN;
			}
		}
		case JOIN_ANTI:
		{
			if (no_need_right_payload)
			{
				return GARROW_LEFT_ANTI_JOIN;
			}
			else
			{
				return GARROW_FULL_ANTI_JOIN;
			}
		}
		case JOIN_LASJ_NOTIN:
			return GARROW_LASJ_NOTIN_JOIN;

		/*
		 * Right-flipped semi/anti hash joins (PG18 JOIN_RIGHT_SEMI /
		 * JOIN_RIGHT_ANTI). Physically: build = LHS (left subtree), probe =
		 * RHS (right subtree). Arrow Acero's RIGHT_SEMI / RIGHT_ANTI
		 * algorithm puts build on its `right input` and emits right-side
		 * (build) rows after finalize -- so we map PG RIGHT_SEMI to Arrow
		 * RIGHT_SEMI, and the caller is responsible for SWAPPING the left/
		 * right plan subtrees + hash keys when feeding Arrow.
		 *
		 * The Mark Join algorithm itself (probe sets has_match visited bit,
		 * finalize scans HT emitting visited (SEMI) or unvisited (ANTI)
		 * build rows) is implemented upstream in
		 * Apache Arrow 5.x (swiss_join.cc:2252 FilterRightSemiAnti +
		 * :2942-3019 ScanTask). Zero changes needed inside Arrow itself.
		 */
		case JOIN_RIGHT_SEMI:
			return GARROW_RIGHT_SEMI_JOIN;
		case JOIN_RIGHT_ANTI:
			return GARROW_RIGHT_ANTI_JOIN;
		case JOIN_RIGHT_ANTI_NOTIN:
			/*
			 * Unreachable: nothing in the tree ever constructs this join
			 * type.  ORCA cannot express it -- EdxlJoinType
			 * (CDXLOperator.h) has EdxljtLeftAntiSemijoinNotIn and
			 * EdxljtRightAntiSemijoin but no right-anti *NotIn* variant, and
			 * CTranslatorDXLToPlStmt maps nothing to it -- and the PG planner
			 * has no path that produces it either.  Grep confirms the enum
			 * value is only ever compared against, never assigned.
			 *
			 * Keep the hard error as a tripwire for whoever adds it: the row
			 * executor rejects it too (nodeHashjoin.c), so supporting it
			 * means teaching both engines, and demote_right_semi_anti_to_left
			 * in planner_vec.c is the third place that would need updating.
			 */
			elog(ERROR, "JOIN_RIGHT_ANTI_NOTIN is not implemented in the vec "
						"engine (and nothing should be able to produce it)");
			break;	/* unreachable, silence -Werror=implicit-fallthrough */

		default:
			/*
			 * Also unreachable.  The join types that can reach a Plan node are
			 * exactly the ten handled above -- the same set explain.c prints.
			 * JOIN_UNIQUE_OUTER / JOIN_UNIQUE_INNER / JOIN_DEDUP_SEMI /
			 * JOIN_DEDUP_SEMI_REVERSE exist only at path level; joinpath.c
			 * rewrites them to JOIN_INNER before create_*_path stores the
			 * jointype, so createplan.c never sees them.
			 */
			elog(ERROR, "join type not supported by arrow join %d", type);
	}

	/* never run here */
	return GARROW_INNER_JOIN;
}

/*
 * on_proc_exit callback to clean up Arrow hash join spill files.
 *
 * Arrow creates spill files outside PG's VFD system, so AtProcExit_Files
 * won't clean them. On normal ERROR, C++ destructors handle cleanup via
 * VecExecEndNode. On FATAL, proc_exit runs this callback since C++
 * destructors are skipped by longjmp/proc_exit.
 */
static void
CleanupArrowSpillFiles(int code, Datum arg)
{
	char		spill_dir[MAXPGPATH];
	DIR		   *dir;
	struct dirent *de;
	char		pid_pattern[64];

	snprintf(spill_dir, MAXPGPATH, "%s/base/%s", DataDir, PG_TEMP_FILES_DIR);
	snprintf(pid_pattern, sizeof(pid_pattern),
			 "%s_%s_%d_", PG_TEMP_FILE_PREFIX, VEC_SPILL_FILE_PREFIX, MyProcPid);

	dir = AllocateDir(spill_dir);
	if (dir == NULL)
		return;

	while ((de = ReadDirExtended(dir, spill_dir, LOG)) != NULL)
	{
		if (strncmp(de->d_name, pid_pattern, strlen(pid_pattern)) == 0)
		{
			char	path[MAXPGPATH];

			snprintf(path, MAXPGPATH, "%s/%s", spill_dir, de->d_name);
			unlink(path);
			elog(DEBUG1, "cleaned up Arrow spill file: %s", path);
		}
	}
	FreeDir(dir);
}

/*
 * Sonic-join type gating.  Rather than mirror sonic_join_node.cc's type sets by
 * hand (which drifts, and where drift turns a routed-but-unsupported join into
 * a hard Status::NotImplemented -> GError -> elog(ERROR) instead of a
 * fallback), these predicates delegate to the Sonic engine itself via the
 * c_glib capability queries (garrow_sonic_join_supports_*), which read the one
 * authoritative copy of allowed_equal_key_types / allowed_types /
 * allowed_join_types.  Extending Sonic's type support in sonic_join_node.cc is
 * automatically reflected here, with no second list to keep in sync.
 *
 * Routing stays a strict pre-check (not a try-then-fallback): the planner only
 * sends a join to Sonic when these queries all say yes, so a plan-build
 * NotImplemented -- which would abort the query rather than degrade -- cannot
 * arise from a type/join-type mismatch.  LASJ_NOTIN stays excluded because
 * sonic_join_node.cc leaves it out of allowed_join_types: its SQL NOT IN
 * NULL-propagation semantics (a NULL build key suppresses all probe output)
 * are not modelled by Sonic's LEFT_ANTI, so it falls back to hash_join.cc.
 */
static bool
sonic_join_supports_equal_key_type(GArrowType id)
{
	return garrow_sonic_join_supports_key_type(id);
}

static bool
sonic_join_supports_payload_type(GArrowType id)
{
	return garrow_sonic_join_supports_payload_type(id);
}

static bool
sonic_join_supports_jointype(GArrowJoinType type)
{
	return garrow_sonic_join_supports_join_type(type);
}

/* true iff every GArrowField in `fields` has a type accepted by `pred` */
static bool
sonic_join_all_fields_supported(GList *fields, bool (*pred)(GArrowType))
{
	for (GList *lc = fields; lc != NULL; lc = lc->next)
	{
		GArrowField *field = (GArrowField *) lc->data;
		g_autoptr(GArrowDataType) dt = garrow_field_get_data_type(field);

		if (!pred(garrow_data_type_get_id(dt)))
			return false;
	}
	return true;
}

/*
 * sonic_join_filter_supported - true iff `joinqual` is a residual join filter
 * that Arrow's Sonic join can evaluate.
 *
 * Sonic (sonic_join_node.cc CheckFilters) accepts only a SINGLE comparison
 * between two top-level columns, one from each side of the join.  A filter
 * routed to sonic that it cannot handle is rejected at plan build with an
 * ERROR rather than falling back, so we mirror that whitelist here and
 * conservatively restrict the compared columns to integer types (enough for
 * the residual `<>` in TPC-H q21; widen as sonic is exercised on more types).
 * A NIL joinqual means no residual filter, which is always fine.
 */
static bool
sonic_join_filter_supported(List *joinqual)
{
	OpExpr	   *op;
	Var		   *lvar;
	Var		   *rvar;
	char	   *opname;
	Oid			argtype;

	if (joinqual == NIL)
		return true;
	/* sonic builds and_kleene() for >1 clause, which it cannot evaluate */
	if (list_length(joinqual) != 1)
		return false;
	if (!IsA(linitial(joinqual), OpExpr))
		return false;
	op = (OpExpr *) linitial(joinqual);
	if (list_length(op->args) != 2)
		return false;

	/* operator must be one of sonic's allowed_not_equal_operators */
	opname = get_opname(op->opno);
	if (opname == NULL)
		return false;
	{
		/* get_opname() palloc's a copy; free it on every path below */
		bool		op_ok = (strcmp(opname, "<>") == 0 ||
							 strcmp(opname, "<") == 0 ||
							 strcmp(opname, "<=") == 0 ||
							 strcmp(opname, ">") == 0 ||
							 strcmp(opname, ">=") == 0);

		pfree(opname);
		if (!op_ok)
			return false;
	}

	/* both arguments must be top-level Vars, one from each side */
	if (!IsA(linitial(op->args), Var) || !IsA(lsecond(op->args), Var))
		return false;
	lvar = (Var *) linitial(op->args);
	rvar = (Var *) lsecond(op->args);
	if (!((lvar->varno == INNER_VAR && rvar->varno == OUTER_VAR) ||
		  (lvar->varno == OUTER_VAR && rvar->varno == INNER_VAR)))
		return false;

	/* both sides must share an integer type sonic can compare */
	argtype = lvar->vartype;
	if (argtype != rvar->vartype)
		return false;
	if (argtype != INT2OID && argtype != INT4OID && argtype != INT8OID)
		return false;

	return true;
}

/*
 * Decide whether this hash join can be safely executed by Arrow's Sonic join.
 * Conservative: requires a supported join type, pure equi-join (no IS / null-
 * aware key comparison), only a sonic-supported residual join filter (see
 * sonic_join_filter_supported), all equi-key column types in
 * sonic's equal-key set, and all probe/build column types in sonic's payload
 * set.  left_keys/right_keys are the equi-key fields; left_fields/right_fields
 * are the full probe/build input schemas (the columns sonic must materialize).
 *
 * The residual-filter gate keys off joinqual (the PG qual list) directly, not
 * the built Arrow filter expression: joinqual is the authoritative "is there a
 * residual join filter" source, so we never depend on the Arrow expression
 * being non-NULL iff joinqual is non-NIL.
 */
static bool
is_sonic_join_compatible(GArrowJoinType type, bool nonequijoin,
						 List *joinqual,
						 GList *left_keys, GList *right_keys,
						 GList *left_fields, GList *right_fields)
{
	if (!sonic_join_supports_jointype(type))
		return false;
	if (nonequijoin)			/* sonic only supports JoinKeyCmp::EQ */
		return false;
	if (joinqual != NIL && !sonic_join_filter_supported(joinqual))
		return false;			/* residual filter sonic cannot evaluate */
	if (!sonic_join_all_fields_supported(left_keys, sonic_join_supports_equal_key_type) ||
		!sonic_join_all_fields_supported(right_keys, sonic_join_supports_equal_key_type))
		return false;
	if (!sonic_join_all_fields_supported(left_fields, sonic_join_supports_payload_type) ||
		!sonic_join_all_fields_supported(right_fields, sonic_join_supports_payload_type))
		return false;
	return true;
}

static GArrowExecuteNode *
BuildHashjoin(PlanBuildContext *pcontext, GArrowExecuteNode *left, GArrowExecuteNode *right, List *joinqual)
{
	GError *error = NULL;
	HashJoinState *node;
	VecHashJoinState *vnode;
	GArrowJoinType type;
	GArrowExecuteNode *hashjoin_node;
	g_autolist(GObject) lkeys = NULL;
	g_autolist(GObject) rkeys = NULL;
	g_autoptr(GArrowSchema) left_schema = NULL;
	g_autoptr(GArrowSchema) right_schema = NULL;
	g_autoptr(GArrowHashJoinNodeOptions) hashjoin_options = NULL;
	g_autoptr(GArrowExpression) filter_expr = NULL;
	GList *keycmps = NULL;
	char spill_dir[MAXPGPATH];
	char spill_file_prefix[MAXPGPATH];

	node = (HashJoinState *) pcontext->planstate;
	vnode = (VecHashJoinState *) node;

	/* Before any GObject is created: this can ERROR, which would leak them. */
	VecSpillFileDir(spill_dir, sizeof(spill_dir));
	VecSpillFilePrefix(spill_file_prefix, sizeof(spill_file_prefix),
					   node->js.ps.plan->plan_node_id);

	type = to_arrow_jointype(node->js.jointype, node->js.ps.plan->targetlist, joinqual);

	/*
	 * NOTE: for RIGHT_SEMI/ANTI, the inputs+hashkeys are already swapped at
	 * the BuildJoinPlan top level (see swap_for_right_semi below) -- by the
	 * time we reach BuildHashjoin, 'left' is the Arrow probe input and
	 * 'right' is the Arrow build input (= PG LHS), with column naming
	 * (LEFT_ / RIGHT_ prefixes) and hashkeys all consistent with the
	 * post-swap layout. We must NOT swap again here.
	 */
	left_schema = garrow_execute_node_get_output_schema(left);
	right_schema = garrow_execute_node_get_output_schema(right);
	lkeys = garrow_schema_get_fields(left_schema);
	rkeys = garrow_schema_get_fields(right_schema);
	pcontext->left_proj_schema = left_schema;
	pcontext->right_proj_schema = right_schema;
	if (joinqual)
		filter_expr = build_filter_expression(joinqual, pcontext);
	if (node->hj_nonequijoin)
	{
		for (int i = 0; i < g_list_length(lkeys); ++i)
		{
			gpointer cmp = (gpointer)GARROW_JOIN_CMP_IS;
			keycmps = garrow_list_append_ptr(keycmps, cmp);
		}
	}
	{
		hashjoin_options =
			/*
			 * Disable Arrow Acero's internal Bloom filter. In the vectorized MPP Hash
			 * Join path, it has been measured to add overhead; GPDB Runtime Bloom Filter
			 * uses a separate mechanism and is unaffected.
			 */
			garrow_hash_join_node_options_new(type, pcontext->left_hashkeys, pcontext->right_hashkeys, keycmps, filter_expr,
											  "", "", true,
											  (gint64)hashjoin_spill_memory_mb * 1024 * 1024,
											  spill_dir,
											  spill_file_prefix,
											  &error);
	}
	if (error)
		elog(ERROR, "Failed to create the hashjoin node, cause: %s", garrow_take_error(&error));

	if (enable_sonic_hashjoin &&
		is_sonic_join_compatible(type, node->hj_nonequijoin, joinqual,
								 pcontext->left_hashkeys, pcontext->right_hashkeys,
								 lkeys, rkeys))
	{
		hashjoin_node =
			garrow_execute_plan_build_sonic_join_node(pcontext->plan, left, right,
													  hashjoin_options, &error);
		vnode->method = VEC_HJ_METHOD_SONIC;
		elog(DEBUG1, "sonic: routed hash join (plan node %d) to sonic join",
			 node->js.ps.plan->plan_node_id);
	}
	else
	{
		hashjoin_node =
			garrow_execute_plan_build_hash_join_node(pcontext->plan, left, right,
													 hashjoin_options, &error);
		vnode->method = VEC_HJ_METHOD_NORMAL;
	}
	/*
	 * The node build has returned, so the key lists have served their purpose
	 * whether it succeeded or not.  Free them before the error check: the
	 * elog(ERROR) below longjmps, and these GList nodes are plain malloc'd
	 * links that PostgreSQL's error machinery knows nothing about.
	 */
	garrow_list_free_ptr(&pcontext->left_hashkeys);
	garrow_list_free_ptr(&pcontext->right_hashkeys);
	/*
	 * keycmps carries GArrowJoinCmp enum values cast to gpointer, not objects,
	 * so garrow_list_free_ptr() must not touch it -- that would unref an
	 * integer.  Drop the link nodes and leave the data alone.
	 */
	while (keycmps != NULL)
		keycmps = g_list_delete_link(keycmps, keycmps);

	if (error)
		elog(ERROR, "Failed to create the hashjoin node, cause: %s", garrow_take_error(&error));

	return hashjoin_node;
}

static bool *
get_sortorder_by_opoid(int num, Oid *ops)
{
	bool *order_reverse = palloc0(sizeof(bool) * num);
	for (int i = 0; i < num; ++i)
	{
		Oid			opfamily;
		Oid			opcintype;
		int16		strategy;

		/* Find the operator in pg_amop */
		if (!get_ordering_op_properties(ops[i], &opfamily, &opcintype, &strategy))
			elog(ERROR, "operator %u is not a valid ordering operator", ops[i]);
		order_reverse[i] = (strategy == BTGreaterStrategyNumber);
	}

	return order_reverse;
}

static bool *
create_order_reverse(Plan *plan, int *numCols, AttrNumber **sortColIdx)
{
	bool *order_reverse = NULL;

	if (IsA(plan, Sort))
	{
		Sort *sort   = (Sort *)plan;
		*numCols     = sort->numCols;
		*sortColIdx  = sort->sortColIdx;
		order_reverse = get_sortorder_by_opoid(sort->numCols,
		                                       sort->sortOperators);
	}
	else if (IsA(plan, Motion))
	{
		Motion *motion = (Motion *)plan;
		*numCols       = motion->numSortCols;
		*sortColIdx    = motion->sortColIdx;
		order_reverse = get_sortorder_by_opoid(motion->numSortCols,
		                                       motion->sortOperators);
	}
	else
	{
		Assert(0); /* never run here */
		const char *nodestr = nodeTypeToString(plan);
		elog(ERROR, "invalid child node type: %s under WindowAgg.", nodestr);
	}

	return order_reverse;
}

static bool *
get_nulls_first(Plan *plan) 
{

	if (IsA(plan, Sort))
	{
		Sort *sort   = (Sort *)plan;
		return sort->nullsFirst;
	}
	else if (IsA(plan, Motion))
	{
		Motion *motion = (Motion *)plan;
		return motion->nullsFirst;
	}
	return NULL;
}

static AttrNumber
find_input_attno_in_targetlist(Plan *plan, AttrNumber attno)
{
	if (attno > list_length(plan->targetlist))
		return -1;

	TargetEntry *tle = (TargetEntry *)list_nth(plan->targetlist, attno - 1);
	if (!IsA(tle->expr, Var))
		return -1;

	Var *var = (Var *)tle->expr;
	if (var->varattno == 0)
		return -1;

	return var->varattno;
}

static void
get_windowagg_sortorder(PlanBuildContext *pcontext, SortKey *sortKey)
{
	int numCols              = 0;
	AttrNumber *sortColIdx   = NULL;
	bool *order_reverse      = NULL;
	bool *nulls_first 		 = NULL;

	Assert(IsA(pcontext->planstate->plan, WindowAgg));
	WindowAgg *wagg = (WindowAgg *) pcontext->planstate->plan;

	for (int i = 0; i < wagg->ordNumCols; ++i)
	{
		/*
		 * for every sort column in wagg, do:
		 * 1. find attno in the input slot of wagg;
		 * 2. find attno in the input slot of sort/motion with attno in wagg by
		 *    the targetlist of sort/motion;
		 */
		AttrNumber attno = wagg->ordColIdx[i];

		Plan *child = (Plan *)outerPlan(wagg);
		while (child)
		{
			AttrNumber input_attno = -1;
			input_attno = find_input_attno_in_targetlist(child, attno);
			if (input_attno == -1)
				elog(ERROR, "order column: %d in WindowAgg NOT found"
				            " in the lower node", wagg->ordColIdx[i]);

			attno = input_attno;

			if ((IsA(child, Agg) && ((Agg *)child)->aggstrategy == AGG_SORTED) ||
				IsA(child, WindowAgg))
				child = outerPlan(child);
			else
				break;
		}

		if (!nulls_first)
			nulls_first = get_nulls_first(child);

		if (!order_reverse)
			order_reverse = create_order_reverse(child, &numCols, &sortColIdx);

		/*
		 * find the position of the sort order by the attno
		 */
		int nth = 0;
		for (; nth < numCols; ++nth)
		{
			if (sortColIdx[nth] == attno)
				break;
		}
		if (nth == numCols)
			elog(ERROR, "order column: %d in WindowAgg NOT found"
			            " in the lower node", wagg->ordColIdx[i]);

		if (order_reverse[nth])
			sortKey->orders[i] = GARROW_SORT_ORDER_DESCENDING;
		else
			sortKey->orders[i] = GARROW_SORT_ORDER_ASCENDING;
		
		if (nulls_first && nulls_first[i])
			sortKey->nulls_first[i] = GARROW_SORT_ORDER_AT_START;
		else
			sortKey->nulls_first[i] = GARROW_SORT_ORDER_AT_END;
	}
}

const char *
nodeTypeToString(const void *obj)
{
	char *nodestr = nodeToString(obj);
	nodestr++;
	char *end = strchr(nodestr, ' ');
	if (end)
		*end = '\0';

	return nodestr;
}
/*
 * Build `expr IS NOT FALSE`, i.e. make a NULL result count as true.
 *
 * Arrow's nested loop join treats a null filter result as "no match"
 * (nested_loop_join.cc: "We treat a null comparison result as false, like in
 * SQL").  That is right for an ordinary join, but SQL NOT IN is null-aware:
 * when the comparison against a build row is UNKNOWN the probe row must be
 * suppressed, which for an anti join means the row has to count as a MATCH.
 * The row executor achieves exactly this by compiling the qual with
 * ExecInitCheck() rather than ExecInitQual() (nodeNestloop.c,
 * nl_qualResultForNull).
 *
 * For a boolean expression, coalesce(expr, true) is precisely
 * `expr IS NOT FALSE`, and build_filter_expression() already combines several
 * clauses with and_kleene, so the three-valued logic underneath is correct.
 */
static GArrowExpression *
build_is_not_false(GArrowExpression *expr)
{
	g_autoptr(GArrowExpression) result = NULL;
	g_autoptr(GArrowExpression) arg = garrow_copy_ptr(expr);
	g_autoptr(GArrowExpression) true_literal =
		build_literal_expression(true, false, BOOLOID, -1);
	GList *arguments = NULL;

	arguments = garrow_list_append_ptr(arguments, arg);
	arguments = garrow_list_append_ptr(arguments, true_literal);
	result = GARROW_EXPRESSION(garrow_call_expression_new("coalesce", arguments, NULL));
	garrow_list_free_ptr(&arguments);
	if (!result)
		elog(ERROR, "Failed to new arrow call expression coalesce.");
	return garrow_move_ptr(result);
}

static GArrowExecuteNode *
BuildNestLoopjoin(PlanBuildContext *pcontext, GArrowExecuteNode *left, GArrowExecuteNode *right, List *joinqual)
{
	GError *error = NULL;
	NestLoopState *node;
	GArrowJoinType type;
	JoinType nl_jointype;
	bool is_lasj_notin;
	GArrowExecuteNode *nestedloopjoin_node;
	g_autoptr(GArrowSchema) left_schema = NULL;
	g_autoptr(GArrowSchema) right_schema = NULL;
	g_autoptr(GArrowNestedLoopJoinNodeOptions) nest_loop_options = NULL;
	g_autoptr(GArrowExpression) filter_expr = NULL;

	node = (NestLoopState *) pcontext->planstate;
	left_schema = garrow_execute_node_get_output_schema(left);
	right_schema = garrow_execute_node_get_output_schema(right);
	pcontext->left_proj_schema = left_schema;
	pcontext->right_proj_schema = right_schema;

	/*
	 * JOIN_LASJ_NOTIN (SQL "x NOT IN (subquery)") is an anti join plus one
	 * extra rule: a comparison that evaluates to UNKNOWN must suppress the
	 * probe row, i.e. count as a match.  A nested loop evaluates the whole
	 * condition for every (probe, build) pair, so once NULL counts as a match
	 * the three-valued semantics fall out of expression evaluation -- no
	 * whole-build-side bookkeeping is needed, unlike the hash join path
	 * (hash_join.cc, hash_table_has_null_).  That also makes multi-column
	 * NOT IN correct for free, which matters because the planner only ever
	 * produces a nested loop for it.
	 *
	 * Arrow's nested loop join has no LASJ_NOTIN join type, so map it to the
	 * plain anti join and carry the extra rule in the filter expression
	 * instead.  to_arrow_jointype() then picks LEFT_ANTI or FULL_ANTI
	 * depending on whether the targetlist needs inner columns, exactly as it
	 * does for JOIN_ANTI.  Note the hash join path keeps using
	 * GARROW_LASJ_NOTIN_JOIN -- the two paths implement the same SQL
	 * semantics in different places, so do not "unify" them.
	 */
	nl_jointype = node->js.jointype;
	is_lasj_notin = (nl_jointype == JOIN_LASJ_NOTIN);
	if (is_lasj_notin)
		nl_jointype = JOIN_ANTI;

	type  = to_arrow_jointype(nl_jointype, node->js.ps.plan->targetlist, joinqual);
	if (joinqual)
		filter_expr = build_filter_expression(joinqual, pcontext);
	else 
		filter_expr = build_literal_expression(true, false, BOOLOID, -1);

	if (is_lasj_notin)
	{
		g_autoptr(GArrowExpression) bare_filter = garrow_move_ptr(filter_expr);

		filter_expr = build_is_not_false(bare_filter);
	}
	nest_loop_options =
		garrow_nested_loop_join_node_options_new(type, filter_expr, "", "", &error);
	if (error)
		elog(ERROR, "Failed to create the nestedloopjoin node, cause: %s", garrow_take_error(&error));
	nestedloopjoin_node =
		garrow_execute_plan_build_nested_loop_join_node(pcontext->plan, left, right,
														nest_loop_options, &error);

	if (error)
		elog(ERROR, "Failed to create the nestedloopjoin node, cause: %s", garrow_take_error(&error));
	return nestedloopjoin_node;
}

GArrowSchema *
GetDummySchema(void)
{
	if (dummy_schema)
		return dummy_schema;
	// only call once
	g_autoptr(GArrowDataType) null_type = NULL;
	g_autoptr(GArrowField) field = NULL;
	GList *fields = NULL;
	null_type = GARROW_DATA_TYPE(garrow_null_data_type_new());
	field = garrow_field_new("dummy", null_type);
	fields = garrow_list_append_ptr(fields, field);
	dummy_schema = garrow_schema_new(fields);
	garrow_list_free_ptr(&fields);
	return dummy_schema;
}

void
FreeDummySchema(void) 
{
	if (dummy_schema)
		ARROW_FREE(GArrowSchema, &dummy_schema);
	dummy_schema = NULL;	
}

void 
dummy_schema_xact_cb(XactEvent event, void *arg)
{
	FreeDummySchema();
}

/*
 * Hand the merged plan to the node's VecExecuteState.
 *
 * Takes ownership: garrow_execute_plan_merge_children() returns a new
 * reference and this is the only place it is stored.  The slot is not empty
 * either -- BuildVecPlan() put the pre-merge plan there -- so the store has
 * to release what was in it.  This used to be a bare assignment, which left
 * one GArrowExecutePlan behind per merge boundary (nine of them on a TPC-DS
 * q8 execution), and dropped the merged plan outright on the two exits that
 * take no node.
 *
 * Releasing the old plan is safe: ExecPlan::MergeChildren() pops every node
 * out of the target and calls SetPlan(new_plan) on each one before adding it
 * to the merged plan, so nothing points at the old plan once it returns.
 * The ExecContext the nodes share lives in estate->exectx, not here, and is
 * released separately by FreeVecExecuteState().
 */
static void
SetArrowPlan(PlanState *ps, GArrowExecutePlan *plan)
{
	Assert(plan);

	/*
	 * Nobody takes the plan on these two exits, so it has to go back here --
	 * garrow_execute_plan_merge_children() is (transfer full).
	 */
	if (ps == NULL)
	{
		ARROW_FREE(GArrowExecutePlan, &plan);
		return;
	}

	if (IsA(ps, ResultState))
	{
		VecResultState *vrs = (VecResultState *)ps;
		garrow_store_ptr(vrs->estate.plan, plan);
	}
	else if (IsA(ps, SubqueryScanState))
	{
		VecSubqueryScanState *vsss = (VecSubqueryScanState *)ps;
		garrow_store_ptr(vsss->estate.plan, plan);
	}
	else if (IsA(ps, SortState))
	{
		VecSortState *vss = (VecSortState *)ps;
		garrow_store_ptr(vss->estate.plan, plan);
	}
	else if (IsA(ps, AggState))
	{
		VecAggState *vas = (VecAggState *)ps;
		garrow_store_ptr(vas->estate.plan, plan);
	}
	else if (IsA(ps, NestLoopState))
	{
		VecNestLoopState *vnls = (VecNestLoopState *)ps;
		garrow_store_ptr(vnls->estate.plan, plan);
	}
	else if (IsA(ps, WindowAggState))
	{
		VecWindowAggState *vwas = (VecWindowAggState *)ps;
		garrow_store_ptr(vwas->estate.plan, plan);
	}
	else if (IsA(ps, WindowHashAggState))
	{
		VecWindowHashAggState *vwhas = (VecWindowHashAggState *)ps;
		garrow_store_ptr(vwhas->estate.plan, plan);
	}
	else if (IsA(ps, HashJoinState))
	{
		VecHashJoinState *vhjs = (VecHashJoinState *)ps;
		garrow_store_ptr(vhjs->estate.plan, plan);
	}
	else if (IsA(ps, AssertOpState))
	{
		VecAssertOpState *vaos = (VecAssertOpState *)ps;
		garrow_store_ptr(vaos->estate.plan, plan);
	}
	else if (IsA(ps, LimitState))
	{
		VecLimitState *vlimit = (VecLimitState *)ps;
		garrow_store_ptr(vlimit->estate.plan, plan);
	}
	else if (IsA(ps, SequenceState))
	{
		VecSequenceState *vseq = (VecSequenceState *)ps;
		garrow_store_ptr(vseq->estate.plan, plan);
	}
	else if (IsA(ps, AppendState))
	{
		VecAppendState *vappend = (VecAppendState *)ps;
		garrow_store_ptr(vappend->estate.plan, plan);
	}
	else if (IsA(ps, MaterialState))
	{
		VecMaterialState *vmat = (VecMaterialState *)ps;
		garrow_store_ptr(vmat->estate.plan, plan);
	}
	else if (IsA(ps, ShareInputScanState))
	{
		VecShareInputScanState *vsis = (VecShareInputScanState *)ps;
		garrow_store_ptr(vsis->estate.plan, plan);
	}
	else
	{
		ARROW_FREE(GArrowExecutePlan, &plan);
		return;
	}
}

VecExecuteState *
GetVecExecuteState(PlanState *ps)
{
	if (ps == NULL)
		return NULL;

	if (IsA(ps, SeqScanState))
	{
		VecSeqScanState *vsss = (VecSeqScanState *)ps;
		return &vsss->vestate;
	}
	else if (IsA(ps, ForeignScanState))
	{
		VecForeignScanState *vfss = (VecForeignScanState *)ps;
		return &vfss->vestate;
	}
	else if (IsA(ps, ResultState))
	{
		VecResultState *vrs = (VecResultState *)ps;
		if (vrs->base.hashFilter)
			return NULL;
		return &vrs->estate;
	}
	else if (IsA(ps, AggState))
	{
		VecAggState *vas = (VecAggState *)ps;
		return &vas->estate;
	}
	else if (IsA(ps, SubqueryScanState))
	{
		VecSubqueryScanState *vsss = (VecSubqueryScanState *)ps;
		return &vsss->estate;
	}
	else if (IsA(ps, HashJoinState))
	{
		VecHashJoinState *vhjs = (VecHashJoinState *)ps;
		return &vhjs->estate;
	}
	else if (IsA(ps, AssertOpState))
	{
		VecAssertOpState *vaos = (VecAssertOpState *)ps;
		return &vaos->estate;
	}
	else if (IsA(ps, SortState))
	{
		VecSortState *vsort = (VecSortState *)ps;
		return &vsort->estate;
	}
	else if (IsA(ps, LimitState))
	{
		VecLimitState *vlimit = (VecLimitState *)ps;
		return &vlimit->estate;
	}
	else if (IsA(ps, WindowAggState))
	{
		VecWindowAggState *vwindow = (VecWindowAggState *)ps;
		return &vwindow->estate;
	}
	else if (IsA(ps, WindowHashAggState))
	{
		VecWindowHashAggState *vwindow = (VecWindowHashAggState *)ps;
		return &vwindow->estate;
	}
	else if (IsA(ps, SequenceState))
	{
		VecSequenceState *vseq = (VecSequenceState *)ps;
		return &vseq->estate;
	}
	else if (IsA(ps, AppendState))
	{
		VecAppendState *vappend = (VecAppendState *)ps;
		return &vappend->estate;
	}
	else if (IsA(ps, ShareInputScanState))
	{
		VecShareInputScanState *vsis = (VecShareInputScanState *)ps;
		return &vsis->estate;
	}
	else if (IsA(ps, NestLoopState))
	{
		VecNestLoopState *vnl = (VecNestLoopState *)ps;
		return &vnl->estate;
	}
	else if (IsA(ps, MaterialState))
	{
		VecMaterialState *vmat = (VecMaterialState *)ps;
		return &vmat->estate;
	}
	else
		return NULL;
}

/* like ExecSetTupleBound in execProcnode, Backpropagation setting vec_plan limits bound */ 
void
ExecVecSetTupleBound(int64 tuples_needed, PlanState *child_node, PlanState *limit_node)
{
	/*
	 * Since this function recurses, in principle we should check stack depth
	 * here.  In practice, it's probably pointless since the earlier node
	 * initialization tree traversal would surely have consumed more stack.
	 */

	if (IsA(child_node, SortState))
	{
		/*
		 * If it is a Sort node, notify it that it can use bounded sort.
		 *
		 * Note: it is the responsibility of nodeSort.c to react properly to
		 * changes of these parameters.  If we ever redesign this, it'd be a
		 * good idea to integrate this signaling with the parameter-change
		 * mechanism.
		 */
		SortState  *sortState = (SortState *) child_node;

		if (tuples_needed < 0)
		{
			/* make sure flag gets reset if needed upon rescan */
			sortState->bounded = false;
		}
		else
		{
			sortState->bounded = true;
			sortState->bound = tuples_needed;

			/*
			 * If the new HTAB-based TopK path already built a TopKNode for
			 * this Sort (vec_topk_bounds_set called during ExecInitVecLimit),
			 * the Arrow plan no longer contains an outer OrderByNode.
			 * garrow_execute_plan_make_topk_node scans the plan for the last
			 * OrderByNode and converts it to TopK; with the outer OrderByNode
			 * already gone it would pick up an inner sort (e.g. a GroupAgg's
			 * sort-by-key) and truncate unrelated data.  Skip the legacy
			 * make_topk_node when the new path owns this Sort.
			 */
			if (vec_topk_bounds_lookup(child_node->plan) > 0)
				return;

			GArrowExecutePlan* sort_plan = NULL;
			GArrowSchema* schema = NULL;
			g_autoptr(GArrowSelectKOptions) selectk_option = NULL;   
			g_autoptr(GArrowSinkNodeOptions) node_options = NULL;
			GList *keys = NULL;
			GError *error = NULL;
			schema = GetSchemaFromSlot(child_node->ps_ResultTupleSlot);
			sort_plan = GetVecExecuteState(limit_node)->plan;
			keys = build_sort_keys(child_node, schema);
			selectk_option = garrow_selectk_options_new(tuples_needed, keys);
			node_options = GARROW_SINK_NODE_OPTIONS(
						garrow_selectk_sink_node_options_new(selectk_option));
			garrow_execute_plan_make_topk_node(sort_plan, GARROW_SELECTK_SINK_NODE_OPTIONS(node_options), &error);
			if (error)
			{
				elog(DEBUG1, "Failed to set topk sort plan, cause: %s", garrow_take_error(&error));
				return;
			}
			garrow_list_free_ptr(&keys);
		}
	}
	else if (IsA(child_node, IncrementalSortState))
	{
		/*
		 * If it is an IncrementalSort node, notify it that it can use bounded
		 * sort.
		 *
		 * Note: it is the responsibility of nodeIncrementalSort.c to react
		 * properly to changes of these parameters.  If we ever redesign this,
		 * it'd be a good idea to integrate this signaling with the
		 * parameter-change mechanism.
		 */
		IncrementalSortState *sortState = (IncrementalSortState *) child_node;

		if (tuples_needed < 0)
		{
			/* make sure flag gets reset if needed upon rescan */
			sortState->bounded = false;
		}
		else
		{
			sortState->bounded = true;
			sortState->bound = tuples_needed;
		}
	}
	else if (IsA(child_node, AppendState))
	{
		/*
		 * If it is an Append, we can apply the bound to any nodes that are
		 * children of the Append, since the Append surely need read no more
		 * than that many tuples from any one input.
		 */
		AppendState *aState = (AppendState *) child_node;
		int			i;

		for (i = 0; i < aState->as_nplans; i++)
			ExecVecSetTupleBound(tuples_needed, aState->appendplans[i], limit_node);
	}
	else if (IsA(child_node, MergeAppendState))
	{
		/*
		 * If it is a MergeAppend, we can apply the bound to any nodes that
		 * are children of the MergeAppend, since the MergeAppend surely need
		 * read no more than that many tuples from any one input.
		 */
		MergeAppendState *maState = (MergeAppendState *) child_node;
		int			i;

		for (i = 0; i < maState->ms_nplans; i++)
			ExecVecSetTupleBound(tuples_needed, maState->mergeplans[i], limit_node);
	}
	else if (IsA(child_node, ResultState))
	{
		/*
		 * Similarly, for a projecting Result, we can apply the bound to its
		 * child node.
		 *
		 * If Result supported qual checking, we'd have to punt on seeing a
		 * qual.  Note that having a resconstantqual is not a showstopper: if
		 * that condition succeeds it affects nothing, while if it fails, no
		 * rows will be demanded from the Result child anyway.
		 */
		if (outerPlanState(child_node))
			ExecVecSetTupleBound(tuples_needed, outerPlanState(child_node), limit_node);
	}
	else if (IsA(child_node, SubqueryScanState))
	{
		/*
		 * We can also descend through SubqueryScan, but only if it has no
		 * qual (otherwise it might discard rows).
		 */
		SubqueryScanState *subqueryState = (SubqueryScanState *) child_node;

		if (subqueryState->ss.ps.qual == NULL)
			ExecVecSetTupleBound(tuples_needed, subqueryState->subplan, limit_node);
	}
	else if (IsA(child_node, GatherState))
	{
		/*
		 * A Gather node can propagate the bound to its workers.  As with
		 * MergeAppend, no one worker could possibly need to return more
		 * tuples than the Gather itself needs to.
		 *
		 * Note: As with Sort, the Gather node is responsible for reacting
		 * properly to changes to this parameter.
		 */
		GatherState *gstate = (GatherState *) child_node;

		gstate->tuples_needed = tuples_needed;

		/* Also pass down the bound to our own copy of the child plan */
		ExecVecSetTupleBound(tuples_needed, outerPlanState(child_node), limit_node);
	}
	else if (IsA(child_node, GatherMergeState))
	{
		/* Same comments as for Gather */
		GatherMergeState *gstate = (GatherMergeState *) child_node;

		gstate->tuples_needed = tuples_needed;

		ExecVecSetTupleBound(tuples_needed, outerPlanState(child_node), limit_node);
	}
	/*
	 * In principle we could descend through any plan node type that is
	 * certain not to discard or combine input rows; but on seeing a node that
	 * can do that, we can't propagate the bound any further.  For the moment
	 * it's unclear that any other cases are worth checking here.
	 */
}

/*
 * is_cross_slice_share_input_scan
 *    True if this PlanState is a cross-slice ShareInputScan.
 *
 * Cross-slice ShareInputScan coordinates its producer slice and consumer
 * slices through GP's ConditionVariable mechanism, driven from
 * ExecVecShareInputScan() (the node's ExecProcNode): the consumer waits on
 * shareinput_reader_waitready_vec() and the producer signals
 * shareinput_writer_notifyready_vec() after materializing.  See
 * nodeShareInputScan.c.
 */
static bool
is_cross_slice_share_input_scan(PlanState *ps)
{
	return ps != NULL &&
		   IsA(ps, ShareInputScanState) &&
		   ((ShareInputScan *) ps->plan)->cross_slice;
}

/*
 * ChildMergeVerdictOf
 *    May this child's Arrow plan be spliced into its parent's plan, and if
 *    not, why not?  ChildMergeable() is the yes/no form; the verdict itself
 *    is what the Debug_print_plan tally attributes un-merged boundaries by.
 *
 * Three cases keep a child out.  It may have no VecExecuteState or no plan at
 * all.  Its build may be deferred to the first ExecuteVecPlan() call, so there
 * is nothing to splice yet.  Or it may be a cross-slice ShareInputScan, whose
 * producer and consumer slices coordinate through GP's ConditionVariable
 * handshake inside ExecVecShareInputScan(): splicing it away means that
 * ExecProcNode never fires, the producer never writes its ".ready" file, and
 * the consumer's Arrow SharedSourceNode::SyncReader() waits for a file that
 * will not appear.
 *
 * In every case the parent keeps the bridge source BuildVecPlan() already
 * built for that input, so the child still runs the ordinary way.
 */
static ChildMergeVerdict
ChildMergeVerdictOf(PlanState *child_ps, VecExecuteState *child_estate)
{
	if (child_estate == NULL)
		return CHILD_NO_ESTATE;
	if (child_estate->plan == NULL)
		return CHILD_NO_PLAN;
	if (child_estate->build_deferred)
		return CHILD_DEFERRED;
	if (child_ps != NULL && is_cross_slice_share_input_scan(child_ps))
		return CHILD_XSLICE_SHARESCAN;
	return CHILD_MERGEABLE;
}

static bool
ChildMergeable(PlanState *child_ps, VecExecuteState *child_estate)
{
	return ChildMergeVerdictOf(child_ps, child_estate) == CHILD_MERGEABLE;
}

static void
MergeArrowNodeToPlanStateFromSource(List **arrow_node_to_planstate, VecExecuteState *source_estate, VecExecuteState *estate)
{
	if (source_estate && source_estate->arrow_node_to_planstate != NIL)
	{
		estate->arrow_node_num = estate->arrow_node_num + source_estate->arrow_node_num - 2;
		TraceNodeInfo* last_info = llast(source_estate->arrow_node_to_planstate);
		last_info->node_num--;

		TraceNodeInfo* cur_info = llast(estate->arrow_node_to_planstate);
		cur_info->node_num--;
		*arrow_node_to_planstate = list_concat(*arrow_node_to_planstate, source_estate->arrow_node_to_planstate);
	}
}

/*
 * exec_param_walker
 *    Report whether an expression tree references a PARAM_EXEC param.
 */
static bool
exec_param_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Param))
		return ((Param *) node)->paramkind == PARAM_EXEC;
	return expression_tree_walker(node, exec_param_walker, context);
}

static bool
vecplan_refs_exec_param(PlanState *ps)
{
	Plan	   *plan = ps->plan;

	if (exec_param_walker((Node *) plan->targetlist, NULL) ||
		exec_param_walker((Node *) plan->qual, NULL))
		return true;

	switch (nodeTag(plan))
	{
		case T_Limit:
			return exec_param_walker(((Limit *) plan)->limitOffset, NULL) ||
				exec_param_walker(((Limit *) plan)->limitCount, NULL);
		case T_Result:
			return exec_param_walker(((Result *) plan)->resconstantqual, NULL);
		case T_WindowAgg:
			/*
			 * Window-frame bound expressions (ROWS/RANGE BETWEEN <offset>
			 * ...) live on the plan node, not in targetlist/qual, and can
			 * reference a PARAM_EXEC (e.g. an initplan result), same as
			 * Limit's offset/count above.
			 */
			return exec_param_walker(((WindowAgg *) plan)->startOffset, NULL) ||
				exec_param_walker(((WindowAgg *) plan)->endOffset, NULL);
		default:
			break;
	}
	return false;
}

/*
 * PARAM_EXEC values consumed by nodes the QD itself executes (slice 0)
 * are not available at ExecInitNode time: initplans are evaluated later,
 * by preprocess_initplans() at ExecutorRun time (or lazily through
 * ExecSetParamPlan() in utility mode).  Building the Arrow plan now would
 * bake the not-yet-set param into the expression as a bogus literal
 * (TPC-DS Q24: HAVING sum(netpaid) > $0 silently became > 0, dropping
 * every non-positive group).  QEs are not affected: their param values
 * arrive with the dispatched plan before node init.
 */
static bool
should_defer_vecplan_build(PlanState *ps)
{
	if (Gp_role == GP_ROLE_EXECUTE)
		return false;
	if (Gp_role == GP_ROLE_DISPATCH && ps->state->currentSliceId != 0)
		return false;
	return vecplan_refs_exec_param(ps);
}

void
PostBuildVecPlan(PlanState *ps, VecExecuteState *estate)
{
	g_autoptr(GError) error = NULL;
	GArrowExecutePlan *result;
	List* arrow_node_to_planstate = NULL;
	int plan_id;
	TraceNodeInfo* info;
	GArrowExecutePlan *target_plan;
	VecExecuteState *target_state;
	List *child_estates = NIL;  /* List of VecExecuteState*, NULL-padded */
	List *child_pstates = NIL;  /* the matching PlanState*, same length */
	ListCell *lc;
	ListCell *lc2;
	GList *child_plans = NULL;
	int nmerge = 0;
	int nskip[CHILD_XSLICE_SHARESCAN + 1] = {0};
	int i;

	/*
	 * Defer the build until the first ExecuteVecPlan() call, which runs
	 * after the pending exec params have been evaluated.  The flag also
	 * serves as the re-entry guard: when ExecuteVecPlan() calls back in,
	 * build_deferred is already true and the build proceeds.
	 */
	if (!estate->build_deferred && should_defer_vecplan_build(ps))
	{
		estate->build_deferred = true;
		estate->deferred_ps = ps;
		return;
	}

	BuildVecPlan(ps, estate);

	plan_id = garrow_execute_plan_get_id(estate->plan);
	info = (TraceNodeInfo*)palloc(sizeof(TraceNodeInfo));
	info->ps = ps;
	info->node_num = garrow_execute_plan_node_num(estate->plan);

	estate->arrow_node_num = info->node_num;
	SetVecTimeCollector(estate, plan_id, info->node_num);
	estate->arrow_node_to_planstate = lappend(estate->arrow_node_to_planstate, info);

	target_state = GetVecExecuteState(ps);
	if (!target_state)
		return;
	target_plan = target_state->plan;

	/*
	 * Collect the children in the order MergeChildren pairs them with this
	 * plan's source nodes, keeping each child's PlanState alongside so the
	 * per-child mergeability test below can look at it.  Entries are never
	 * dropped -- a child with no VecExecuteState contributes NULL -- because
	 * the pairing is positional.
	 */
	if (IsA(ps, SubqueryScanState))
	{
		/* SubqueryScan: child is in subplan */
		SubqueryScanState *subqueryState = (SubqueryScanState *) ps;
		child_pstates = lappend(child_pstates, subqueryState->subplan);
		child_estates = lappend(child_estates,
								GetVecExecuteState(subqueryState->subplan));
	}
	else if (IsA(ps, SequenceState))
	{
		/* Sequence: children are in subplans[] array */
		SequenceState *seqState = (SequenceState *) ps;
		for (i = seqState->numSubplans - 1; i >= 0; i--)
		{
			child_pstates = lappend(child_pstates, seqState->subplans[i]);
			child_estates = lappend(child_estates,
									GetVecExecuteState(seqState->subplans[i]));
		}
	}
	else if (IsA(ps, AppendState))
	{
		/* Append: children are in appendplans[] array */
		AppendState *appendState = (AppendState *) ps;
		for (i = 0; i < appendState->as_nplans; i++)
		{
			child_pstates = lappend(child_pstates, appendState->appendplans[i]);
			child_estates = lappend(child_estates,
									GetVecExecuteState(appendState->appendplans[i]));
		}
	}
	else if (IsA(ps, HashJoinState) || IsA(ps, NestLoopState))
	{
		/* Join nodes: left and right children; HashJoin skips the Hash node */
		PlanState  *right_ps = IsA(ps, HashJoinState) ? ps->righttree->lefttree
													  : ps->righttree;

		child_pstates = lappend(child_pstates, ps->lefttree);
		child_estates = lappend(child_estates, GetVecExecuteState(ps->lefttree));
		child_pstates = lappend(child_pstates, right_ps);
		child_estates = lappend(child_estates, GetVecExecuteState(right_ps));
	}
	else if (ps->lefttree != NULL)
	{
		child_pstates = lappend(child_pstates, ps->lefttree);
		child_estates = lappend(child_estates, GetVecExecuteState(ps->lefttree));
	}
	/*
	 * A leaf (a scan) has no input, so it has no merge boundary at all.  The
	 * NULL-padding above exists to keep the positional pairing with the
	 * target's source nodes honest for nodes that do have inputs; padding a
	 * leaf would invent a boundary that no source node corresponds to.
	 */

	if (child_estates == NIL)
	{
		list_free(child_pstates);
		return;
	}

	/*
	 * Decide per child whether its Arrow plan may be spliced in, and build
	 * the child list MergeChildren consumes: the plan for a mergeable child,
	 * NULL for one that must keep running on its own.  MergeChildren leaves
	 * the source node paired with a NULL alone, so BuildVecPlan's bridge
	 * (BuildSource -> ExecProcNode) still drives that child.
	 *
	 * Until this became per-child, a single un-mergeable child left the whole
	 * node un-merged -- which is why a cross-slice ShareInputScan anywhere
	 * under a node broke the pipeline for all of its siblings too.
	 */
	forboth(lc, child_estates, lc2, child_pstates)
	{
		VecExecuteState *source_estate = (VecExecuteState *) lfirst(lc);
		PlanState  *child_ps = (PlanState *) lfirst(lc2);
		g_autoptr(GArrowExecutePlan) source_plan = NULL;
		ChildMergeVerdict v = ChildMergeVerdictOf(child_ps, source_estate);

		if (v == CHILD_MERGEABLE)
		{
			source_plan = garrow_copy_ptr(source_estate->plan);
			nmerge++;
		}
		else
		{
			nskip[v]++;
			/*
			 * Also name the child, so the tally can separate boundaries that
			 * are unavoidable by construction (a Motion is a different slice
			 * in a different process -- no Arrow plan can span it) from the
			 * ones that are merely unimplemented.
			 */
			if (Debug_print_plan)
				elog(LOG, "vec plan merge skip: parent plan(%d) child tag(%d) reason(%d)",
					 ps->plan->plan_node_id,
					 child_ps && child_ps->plan ? (int) nodeTag(child_ps->plan) : -1,
					 (int) v);
		}
		/* garrow_list_append_ptr steals the ref; NULL means "skip this one" */
		child_plans = garrow_list_append_ptr(child_plans, source_plan);
	}

	/*
	 * One line per merge boundary, so a single Debug_print_plan run yields both
	 * the post-change numbers (nmerge) and the pre-change ones: before partial
	 * merge a node spliced either all of its children or none, so a boundary
	 * counted as merged back then is exactly one where nmerge == total.
	 */
	if (Debug_print_plan)
		elog(LOG, "vec plan merge for plan(%d) tag(%d): %d/%d children spliced "
			 "(noestate=%d noplan=%d deferred=%d xslice=%d)",
			 ps->plan->plan_node_id, (int) nodeTag(ps->plan),
			 nmerge, list_length(child_estates),
			 nskip[CHILD_NO_ESTATE], nskip[CHILD_NO_PLAN],
			 nskip[CHILD_DEFERRED], nskip[CHILD_XSLICE_SHARESCAN]);

	if (nmerge == 0)
	{
		garrow_list_free_ptr(&child_plans);
		list_free(child_estates);
		list_free(child_pstates);
		return;
	}

	/*
	 * Only now, with the set of children to splice settled, fold their
	 * arrow_node_to_planstate / arrow_node_num into estate.
	 * MergeArrowNodeToPlanStateFromSource mutates estate->arrow_node_num and
	 * the last TraceNodeInfo, so doing it for a child that turns out not to
	 * be merged would leave the counts out of sync with the plan.
	 */
	forboth(lc, child_estates, lc2, child_pstates)
	{
		VecExecuteState *source_estate = (VecExecuteState *) lfirst(lc);

		if (ChildMergeable((PlanState *) lfirst(lc2), source_estate))
			MergeArrowNodeToPlanStateFromSource(&arrow_node_to_planstate,
												source_estate, estate);
	}

	result = garrow_execute_plan_merge_children(target_plan, child_plans,
											   true /* allow_missing_children */,
											   &error);
	garrow_list_free_ptr(&child_plans);
	if (error)
	{
		elog(LOG, "Failed to merge child plans, cause: %s", garrow_take_error(&error));
		list_free(child_estates);
		list_free(child_pstates);
		return;
	}

	/*
	 * Record that this node consumed N children via MergeChildren.  Surfaced
	 * by show_vec_merge_info as "Vec Plan Merge:  N children" at EXPLAIN VERBOSE
	 * time, a regression footprint for the splice path (see explain.c).
	 */
	estate->merged_child_count = nmerge;

	/*
	 * MergeChildren replaced every one of this plan's bridge sources with
	 * the corresponding child's plan content, so whether the merged plan
	 * still contains a callback/shared source is now solely a property of
	 * the children.  Recompute (not OR onto) the flag: a plan whose bridges
	 * were all replaced by pure ScanNode children (e.g. Agg over a PAX
	 * scan) must keep push_pipeline eligibility.
	 */
	{
		bool		has_cb = false;

		forboth(lc, child_estates, lc2, child_pstates)
		{
			VecExecuteState *child = (VecExecuteState *) lfirst(lc);

			/*
			 * A child left un-merged keeps this plan's bridge source, which
			 * is a callback source by construction -- so a partial merge
			 * cannot regain push_pipeline eligibility.
			 */
			if (!ChildMergeable((PlanState *) lfirst(lc2), child))
				has_cb = true;
			else
				has_cb |= child->has_callback_source;
		}
		estate->has_callback_source = has_cb;
	}

	/*
	 * Disable push_pipeline on merged children.  After merge, child
	 * ScanNodes are embedded in the parent plan; the child's original
	 * Arrow plan is stale and must not execute independently -- its
	 * push_pipeline would start async ScanNode threads feeding a stale
	 * PushGenerator queue, causing OOM (Q2: 20GB in 51s).  The parent
	 * plan now drives those ScanNodes via the merged plan.
	 */
	forboth(lc, child_estates, lc2, child_pstates)
	{
		VecExecuteState *merged_child = (VecExecuteState *) lfirst(lc);

		/* An un-merged child still executes its own plan -- leave it alone. */
		if (!ChildMergeable((PlanState *) lfirst(lc2), merged_child))
			continue;
		if (merged_child->push_pipeline)
			merged_child->push_pipeline = false;
	}

	list_free(child_estates);
	list_free(child_pstates);

	/*
	 * list_concat in PG14 memcpys list2's elements into list1's flat
	 * element array and leaves list2's cells intact, so the cells of
	 * estate->arrow_node_to_planstate must be freed separately.  The
	 * pointed-to TraceNodeInfo objects are shared with arrow_node_to_planstate
	 * via the memcpy and remain owned there.
	 */
	arrow_node_to_planstate = list_concat(arrow_node_to_planstate, estate->arrow_node_to_planstate);
	if (estate->arrow_node_to_planstate != NIL)
		list_free(estate->arrow_node_to_planstate);
	estate->arrow_node_to_planstate = arrow_node_to_planstate;
	garrow_execute_plan_set_plan_id(result, plan_num++);
	SetVecTimeCollector(estate, garrow_execute_plan_get_id(result),
						estate->arrow_node_num);
	SetArrowPlan(ps, result);

	if (Debug_print_plan)
	{
		g_autofree gchar *plan_str = garrow_execute_plan_to_string(result);
		elog(LOG, "arrow plan for plan(%d) in PostBuildVecPlan: %s", ps->plan->plan_node_id, plan_str);
	}
}
