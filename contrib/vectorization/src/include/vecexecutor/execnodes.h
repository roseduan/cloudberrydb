/*-------------------------------------------------------------------------
 * execnodes.h
 *	  definitions for vectorized executor state nodes
 *
 * Copyright (c) 2016-Present Hashdata, Inc. 
 *
 *
 * IDENTIFICATION
 *		src/include/vecexecutor/execnodes.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VEC_EXEC_NODES_H
#define VEC_EXEC_NODES_H

#include "access/heapam.h"
#include "nodes/execnodes.h"
#include "cdb/cdbappendonlyam.h"
#include "cdb/cdbhash.h"
#include "executor/nodeAgg.h"

#include "utils/arrow.h"
#include "utils/vecheap.h"
#include "nodes/execnodes.h"
#include "utils/tuptable_vec.h"

typedef struct TraceNodeInfo
{
	int node_num;
	PlanState *ps;
} TraceNodeInfo;

/* runtime Arrow plan state */
typedef struct VecExecuteState
{
	GArrowExecutePlan *plan;
	List* arrow_node_to_planstate;
	int arrow_node_num;
	GArrowTimeCollector *time_collector;
	bool started; /* plan execution has been started */
	TupleTableSlot *slot; /* slot for plan result*/
	bool pipeline;
	GArrowRecordBatchReader *reader;
	List *resqueue;
	GArrowExecuteContext *exectx;
	/*
	 * Number of child arrow plans spliced into this node by
	 * PostBuildVecPlan()->MergeChildren.  0 means no merge happened.
	 * Read by show_vec_merge_info at EXPLAIN VERBOSE time to display
	 * "Vec Plan Merge:  N children".
	 */
	int merged_child_count;
} VecExecuteState;

typedef struct VecSeqScanState
{
	SeqScanState base;
	VecExecuteState vestate;
	int       	*columnmap;
	/* base.ss.ss_ScanTupleSlot is TTSOpsVirtual for abi,
	 * this slot is TTSOpsVecTuple for imported batch. */
	TupleTableSlot *vscanslot;

	Size		pscan_len;		/* size of parallel heap scan descriptor */
	struct AOCSScanDescData *ss_currentScanDesc_aocs;
	VecDesc   	vecdesc;
	ResDesc   	resdesc;
	bool skip;
	int rows;
	GArrowScanNodeOptions *scan_node_options;
} VecSeqScanState;

typedef struct VecForeignScanState
{
	ForeignScanState base;
	VecExecuteState vestate;
	int       	*columnmap;
	/* base.ss.ss_ScanTupleSlot is TTSOpsVirtual for abi,
	 * this slot is TTSOpsVecTuple for imported batch. */
	TupleTableSlot *vscanslot;

	GArrowSchema *schema;
} VecForeignScanState;

typedef struct VecSequenceState
{
	SequenceState base;	
	VecExecuteState estate;
} VecSequenceState;

/* ----------------
 * VecResultState information
 * ----------------
 */
typedef struct VecResultState
{
	ResultState base;
	VecExecuteState estate;
	void *hash_projector;
} VecResultState;

typedef struct VecSubqueryScanState
{
	SubqueryScanState base;
	VecExecuteState estate;
} VecSubqueryScanState;

typedef struct VecMotionState
{
	MotionState base;	
	TupleDesc 	transTupDesc;
	GList *unsorted_batches;
	GArrowSortOptions *sort_options;
	GArrowSchema *schema;
	GArrowTable *unsorted_table;
	GArrowTable *sorted_table;
	GArrowTableBatchReader *reader;

	/* FIXME: we need support vec hash in the future. */
	/* vectorization */
	//struct CdbHashVec *cdbhashvec;	/* vec hash api object */
	bool return_tup;

	int *offset;
	char *memtups;

	TupleTableSlot *outer_vecslot;
	TupleTableSlot *rowslot;
	TupleTableSlot *vecslot;
	bool islast;

	bool is_vec_hash;

	void ***segments;  /* nth item in segments is a batch of of builders used to
						* make the slot sent to nth segment.
						*/

	/* FIXME: express will be support in the future */
	bool *hash_cols;   /* proj col for hash */
	void *hash_projector;
	List *hashExprsGandivaNodes;
	GArrowUInt32Array* random_const_array_template;

	/*
	 * Sonic Motion Direct-Send fast-path: cached column index of the
	 * hidden target-segment column in the upstream batch. -1 means "not
	 * yet searched" (lazy init); -2 means "absent, run slow path".
	 *
	 * Two-layer invalidation in hashAndSendVec_vechash:
	 *   1. Column count mismatch (sonic_seg_col_n_cols) -> rescan.
	 *   2. If a positive idx is cached, verify the column at that idx
	 *      still has sonic_seg_col_name; otherwise rescan. Catches
	 *      same-width reorderings (multi-input fan-in, rescan-with-
	 *      replan) that would slip past the column-count gate.
	 *
	 * sonic_seg_col_name is set at ExecInit time from the registered
	 * VecMotionDirectSendHint and remains stable for the plan's lifetime.
	 * NULL means no hint was registered for this Motion (slow path
	 * unconditionally).
	 */
	int sonic_seg_col_idx;
	int sonic_seg_col_n_cols;
	const char *sonic_seg_col_name;
} VecMotionState;



typedef struct VecSortState
{
	SortState 	base;
	VecExecuteState estate;

	bool		skip; /* true if upper node is groupagg, sort done in groupagg */

	/*
	 * Set by build_topk_node to the K value (count + offset) when this Sort's
	 * Arrow plan was emitted as a TopKNode instead of an OrderByNode.
	 * Zero means the Sort emits an OrderByNode (either the Limit+Sort TopK
	 * optimization did not apply, or it's not a Limit's child).  Read by
	 * show_sort_info at EXPLAIN time to display "Vec Sort Method: TopK K: N".
	 */
	int64		topk_bound;
} VecSortState;

typedef struct VecAppendState
{
	AppendState base;
	VecExecuteState estate;
	GArrowSchema *schema;
} VecAppendState;


/*
 * Which Arrow aggregate engine BuildAggregatation routed this HashAgg to.
 * Set during ExecInit (by build_aggregatation_options); read at EXPLAIN
 * time by show_hashagg_info to surface the choice.  Only meaningful for
 * AGG_HASHED plans -- other strategies leave it at _UNSET.
 */
typedef enum VecAggMethod
{
	VEC_AGG_METHOD_UNSET = 0,	/* default / non-AGG_HASHED / init failed */
	VEC_AGG_METHOD_SONIC,		/* SonicGroupByNode (compute/sonic/exec_node.cc) */
	VEC_AGG_METHOD_NORMAL,		/* legacy Arrow GroupByNode (sonic-incompatible) */
	VEC_AGG_METHOD_LIMIT_FUSION	/* legacy GroupByNode + Limit+HashAgg fusion */
} VecAggMethod;

typedef struct VecAggState
{
	AggState base;

	VecExecuteState estate;

	/* Arrow plan control params */
	GArrowExecutePlan			*plan;
	GArrowAggregationNodeState	*state; 
	List						*rbs; 	/* n batches of data */

	/* source schema */
	GArrowSchema *source_schema;

	/* outplan slot */
	TupleTableSlot *outplan_slot;

	/* for streaming loop */
	bool init_arrow;

	/* agg proj node expression */
	void *proj;

	/* these fields are used in AGG_HASHED and initial pass */
	uint32		*hashkeys;

	/* these fields are used in AGG_HASHED and agg_retrieve_hash_table_vec */
	void		**entries;
	TupleTableSlot *outer_vecslot;

	ExprContext *veccontext;

	/* used by hashagg with arrow native */
	GArrowSchema *hashagg_input_schema;
	GArrowRecordBatch *hashagg_input_rb;
	GArrowDataType *decimal256_dt;
	GArrowDataType *int64_dt;

	char **aggfn_arg;
	char **aggfn_rs;
	char **aggfn_arg_count;
	char **aggfn_arg_sum;
	char **aggfn_rs_count;
	char **aggfn_rs_sum;
	char **grpkeys;

	/* used by slice recordbatch */
	TupleTableSlot *result_slot;
	GArrowRecordBatch *origin_rb;
	int offset;
	int rows;


	bool	sorted;
	bool	streaming;
	int		streamgroups; /* the number of the groups per batch */
	int		curgroups;  /* the number of the groups in GroupByNode currently */
	bool skip ;

	/*
	 * Set by build_aggregatation_options when the Limit+HashAgg fusion fires:
	 * the Arrow GroupByNode is built with AggregateNodeOptions::limit_count =
	 * offset + count so it stops tracking after the first N groups.  Zero
	 * means the fusion did not apply (no parent Limit, non-constant
	 * limit/offset, total > vector.limit_hashagg_max_total, or GUC disabled).
	 * Read by show_hashagg_info at EXPLAIN time to display the limit value
	 * alongside method == VEC_AGG_METHOD_LIMIT_FUSION.
	 */
	int64	limit_count;

	/*
	 * Which Arrow aggregate engine this HashAgg ended up using.  Set by
	 * build_aggregatation_options for AGG_HASHED plans (left at _UNSET
	 * otherwise).  Drives the "Vec HashAgg Method: ..." line in EXPLAIN.
	 */
	VecAggMethod	method;
} VecAggState;

typedef struct VecNestLoopState
{
	NestLoopState base;	
	VecExecuteState estate;
} VecNestLoopState;

typedef struct VecMaterialState
{
	MaterialState base;
	bool is_skip; /* can skip material, for example it combo with nestloop */
	VecExecuteState estate;
	bool cdb_strict;
}VecMaterialState;

typedef struct VecShareInputScanState
{
	ShareInputScanState base;
	VecExecuteState estate;
	VecDesc vecdesc;
	int gp_session_id;
	int gp_command_count;
	int share_id;
	int num_slices;
	int is_cross_slice;
	bool ready;
	bool is_producer;
	bool writer_ready_synced;
} VecShareInputScanState;

typedef struct VecAggStatePerAggData 
{
	AggStatePerAggData base;

	void *projector;

	TupleDesc	evaldesc;		/* descriptor of input tuples */
	/*
	 * Slots for holding the evaluated input arguments.  These are set up
	 * during ExecInitAgg() and then used for each input row.
	 */
	TupleTableSlot *evalslot;	/* current input tuple */

	/* these fields are used in aggfunc(distinct xxx) */
	Datum		*grp_keys_datums;
	bool		*grp_keys_isnull;
	GList		*order_slices;
} VecAggStatePerAggData;

typedef struct VecAggStatePerAggData *VecAggStatePerAgg;

typedef struct VecWindowAggState
{
	WindowAggState base;

	VecExecuteState estate;

	/* Arrow plan control params */
	GArrowExecutePlan			*plan;
	GArrowAggregationNodeState	*state; 
	List						*rbs; 	/* n batches of data */

	/* source schema */
	GArrowSchema *source_schema;

	/* outplan slot */
	TupleTableSlot *outplan_slot;

	/* for streaming loop */
	bool init_arrow;

	ExprContext *veccontext;

	/* used by slice recordbatch */
	TupleTableSlot *result_slot;
	GArrowRecordBatch *origin_rb;
	int offset;
	int rows;

} VecWindowAggState;

typedef struct WindowHashAggState
{
	ScanState	ss;				/* its first field is NodeTag */

	/* these fields are filled in by ExecInitExpr: */
	List	   *funcs;			/* all WindowFunc nodes in targetlist */
	int			numfuncs;		/* total number of window functions */
	int			numaggs;		/* number that are plain aggregates */

	WindowStatePerFunc perfunc; /* per-window-function information */
	WindowStatePerAgg peragg;	/* per-plain-aggregate information */
	ExprState  *partEqfunction; /* equality funcs for partition columns */
	ExprState  *ordEqfunction;	/* equality funcs for ordering columns */
	Tuplestorestate *buffer;	/* stores rows of current partition */
	int			current_ptr;	/* read pointer # for current row */
	int			framehead_ptr;	/* read pointer # for frame head, if used */
	int			frametail_ptr;	/* read pointer # for frame tail, if used */
	int			grouptail_ptr;	/* read pointer # for group tail, if used */
	int64		spooled_rows;	/* total # of rows in buffer */
	int64		currentpos;		/* position of current row in partition */
	int64		frameheadpos;	/* current frame head position */
	int64		frametailpos;	/* current frame tail position (frame end+1) */
	/* use struct pointer to avoid including windowapi.h here */
	struct WindowObjectData *agg_winobj;	/* winobj for aggregate fetches */
	int64		aggregatedbase; /* start row for current aggregates */
	int64		aggregatedupto; /* rows before this one are aggregated */

	int			frameOptions;	/* frame_clause options, see WindowDef */
	ExprState  *startOffset;	/* expression for starting bound offset */
	ExprState  *endOffset;		/* expression for ending bound offset */
	Datum		startOffsetValue;	/* result of startOffset evaluation */
	Datum		endOffsetValue; /* result of endOffset evaluation */

	/* these fields are used with RANGE offset PRECEDING/FOLLOWING: */
	FmgrInfo	startInRangeFunc;	/* in_range function for startOffset */
	FmgrInfo	endInRangeFunc; /* in_range function for endOffset */
	Oid			inRangeColl;	/* collation for in_range tests */
	bool		inRangeAsc;		/* use ASC sort order for in_range tests? */
	bool		inRangeNullsFirst;	/* nulls sort first for in_range tests? */
	/*
	 * In GPDB, we support RANGE/ROWS start/end expressions to contain
	 * variables. You lose on some optimizations in that case, so we use
	 * these flags to indicate if they don't contain any variables, to allow
	 * those optimizations in the usual case that they don't.
	 */
	bool		start_offset_var_free;
	bool		end_offset_var_free;

	bool		start_offset_valid;		/* is startOffsetValue valid for current row? */
	bool		end_offset_valid;		/* is endOffsetValue valid for current row? */

	/* these fields are used in GROUPS mode: */
	int64		currentgroup;	/* peer group # of current row in partition */
	int64		frameheadgroup; /* peer group # of frame head row */
	int64		frametailgroup; /* peer group # of frame tail row */
	int64		groupheadpos;	/* current row's peer group head position */
	int64		grouptailpos;	/* " " " " tail position (group end+1) */

	MemoryContext partcontext;	/* context for partition-lifespan data */
	MemoryContext aggcontext;	/* shared context for aggregate working data */
	MemoryContext curaggcontext;	/* current aggregate's working data */
	ExprContext *tmpcontext;	/* short-term evaluation context */

	bool		all_first;		/* true if the scan is starting */
	bool		all_done;		/* true if the scan is finished */
	bool		partition_spooled;	/* true if all tuples in current partition
									 * have been spooled into tuplestore */
	bool		more_partitions;	/* true if there's more partitions after
									 * this one */
	bool		framehead_valid;	/* true if frameheadpos is known up to
									 * date for current row */
	bool		frametail_valid;	/* true if frametailpos is known up to
									 * date for current row */
	bool		grouptail_valid;	/* true if grouptailpos is known up to
									 * date for current row */

	TupleTableSlot *first_part_slot;	/* first tuple of current or next
										 * partition */
	TupleTableSlot *framehead_slot; /* first tuple of current frame */
	TupleTableSlot *frametail_slot; /* first tuple after current frame */

	/* temporary slots for tuples fetched back from tuplestore */
	TupleTableSlot *agg_row_slot;
	TupleTableSlot *temp_slot_1;
	TupleTableSlot *temp_slot_2;
} WindowHashAggState;

typedef struct VecWindowHashAggState
{
	WindowHashAggState base;

	VecExecuteState estate;

	/* Arrow plan control params */
	GArrowExecutePlan			*plan;
	GArrowAggregationNodeState	*state; 
	List						*rbs; 	/* n batches of data */

	/* source schema */
	GArrowSchema *source_schema;

	/* outplan slot */
	TupleTableSlot *outplan_slot;

	/* for streaming loop */
	bool init_arrow;

	ExprContext *veccontext;

	/* used by slice recordbatch */
	TupleTableSlot *result_slot;
	GArrowRecordBatch *origin_rb;
	int offset;
	int rows;

} VecWindowHashAggState;

/*
 * This struct is the data actually passed to an fmgr-called function.
 */
typedef struct FunctionCallInfoData
{
	FmgrInfo   *flinfo;			/* ptr to lookup info used for this call */
	fmNodePtr	context;		/* pass info about context of call */
	fmNodePtr	resultinfo;		/* pass or return extra info about result */
	Oid			fncollation;	/* collation for function to use */
	bool		isnull;			/* function must set true if result is NULL */
	short		nargs;			/* # arguments actually passed */
	Datum		arg[FUNC_MAX_ARGS];		/* Arguments passed to function */
	bool		argnull[FUNC_MAX_ARGS]; /* T if arg[i] is actually NULL */
} FunctionCallInfoData;

/* ----------------
 *  FuncExprState node
 *  
 *  Although named for FuncExpr, this is also used for OpExpr, DistinctExpr,
 *  and NullIf nodes; be careful to check what xprstate.expr is actually
 *  pointing at!
 *  ----------------
 */
typedef struct FuncExprState
{
	ExprState	xprstate;
	List	   *args;			/* states of argument expressions */

	/*
  	 * Function manager's lookup info for the target function.  If func.fn_oid
  	 * is InvalidOid, we haven't initialized it yet (nor any of the following
  	 * fields).
  	 */
	FmgrInfo	func;

	/*
  	 * For a set-returning function (SRF) that returns a tuplestore, we keep
  	 * the tuplestore here and dole out the result rows one at a time. The
  	 * slot holds the row currently being returned.
  	 */
	Tuplestorestate *funcResultStore;
	TupleTableSlot *funcResultSlot;

	/*
         * In some cases we need to compute a tuple descriptor for the function's
         * output.  If so, it's stored here.
         */
	TupleDesc	funcResultDesc;
	bool		funcReturnsTuple;		/* valid when funcResultDesc isn't
										 * NULL */

	/*
  	 * setArgsValid is true when we are evaluating a set-returning function
  	 * that uses value-per-call mode and we are in the middle of a call
  	 * series; we want to pass the same argument values to the function again
  	 * (and again, until it returns ExprEndResult).  This indicates that
  	 * fcinfo_data already contains valid argument data.
  	 */
	bool		setArgsValid;

	/*
  	 * Flag to remember whether we found a set-valued argument to the
  	 * function. This causes the function result to be a set as well. Valid
  	 * only when setArgsValid is true or funcResultStore isn't NULL.
  	 */
	bool		setHasSetArg;	/* some argument returns a set */

	/*
  	 * Flag to remember whether we have registered a shutdown callback for
  	 * this FuncExprState.  We do so only if funcResultStore or setArgsValid
  	 * has been set at least once (since all the callback is for is to release
  	 * the tuplestore or clear setArgsValid).
  	 */
	bool		shutdown_reg;	/* a shutdown callback is registered */

	/*
  	 * Call parameter structure for the function.  This has been initialized
  	 * (by InitFunctionCallInfoData) if func.fn_oid is valid.  It also saves
  	 * argument values between calls, when setArgsValid is true.
  	 */
	FunctionCallInfoData fcinfo_data;

	/* Fast Path */
	ExprState  *fp_arg[2];
	Datum		fp_datum[2];
	bool		fp_null[2];
} FuncExprState;

/* ----------------
 *
 * ScalarArrayOpExprState node
 * 
 * This is a FuncExprState plus some additional data.
 *  ----------------
 */
typedef struct ScalarArrayOpExprState
{
	FuncExprState fxprstate;
	/* Cached info about array element type */
	Oid			element_type;
	int16		typlen;
	bool		typbyval;
	char		typalign;

	/* Fast path x in ('A', 'B', 'C') */
	int			fp_n;
	int		   *fp_len;
	Datum	   *fp_datum;
} ScalarArrayOpExprState;

/* ----------------
 *  GenericExprState node
 *  
 *  This is used for Expr node types that need no local run-time state,
 *  but have one child Expr node.
 *  ----------------
 */
typedef struct GenericExprState
{
	ExprState	xprstate;
	ExprState  *arg;			/* state of my child node */
} GenericExprState;

typedef struct VecAggrefExprState
{
	ExprState	xprstate;
	List	   *aggdirectargs;	/* states of direct-argument expressions */
	List	   *args;			/* states of aggregated-argument expressions */
	ExprState  *aggfilter;		/* state of FILTER expression, if any */
	int			aggno;			/* ID number for agg within its plan node */
	char        name[30];
} VecAggrefExprState;
typedef struct FuncExprState VecFuncExprState;

typedef struct CdbHashVec
{
        void                *hash;                      /* The result hash value                                                        */
        int          hash_size;
        int                     numsegs;                    /* number of segments in Greenplum Database used for
                                                                 * partitioning  */
        CdbHashReduce reducealg;            /* the algorithm used for reducing to buckets               */
        bool            is_legacy_hash;

        //uint32          numLogicParts;  /* consistent hash-ring index*/
        int                *segmapping;         /* mapping between logic part and physical part */
        void       **segmapping_scalar; /* scalar array for mapping between logic part and physical part */
        bool            useGPDBHash;

        int                     natts;
        FmgrInfo   *hashfuncs;

} CdbHashVec;

typedef enum VecHashJoinMethod
{
	VEC_HJ_METHOD_UNSET = 0,	/* not routed through BuildHashjoin */
	VEC_HJ_METHOD_SONIC,		/* Arrow Sonic join (sonic_join_node.cc) */
	VEC_HJ_METHOD_NORMAL		/* legacy Arrow hash join (hash_join_node.cc) */
} VecHashJoinMethod;

typedef struct VecHashJoinState
{
	HashJoinState base;
	VecExecuteState estate;
	/* fields for vector engine */
	ExprContext    *hashkeys_econtext;

	/* used by slice recordbatch */
	TupleTableSlot *result_slot;
	GArrowRecordBatch *origin_rb;
	int offset;
	int nrows;
	bool is_left; 

	/* for semi/anti join with joinqual */
	bool joinqual_pushdown; 
	GList *semi_anti_filter;
	int left_attr_in_joinqual;
	int right_attr_in_joinqual;
	bool skip ;

	/* Which Arrow hash-join implementation BuildHashjoin routed this node to;
	 * surfaced as "Vec Hash Join Method: Sonic|Normal" under EXPLAIN VERBOSE. */
	VecHashJoinMethod method;
} VecHashJoinState;

typedef struct VecHashState
{
	HashState base;
} VecHashState;

typedef struct VecAssertOpState
{
	AssertOpState base;
	VecExecuteState estate;
} VecAssertOpState;

typedef struct VecLimitState
{
	LimitState base;
	VecExecuteState estate;
} VecLimitState;

#endif							/* VEC_EXEC_NODES_H */
