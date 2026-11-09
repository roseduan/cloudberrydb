/*-------------------------------------------------------------------------
 *
 * cdbexplain_stats.h
 *    The per-node EXPLAIN ANALYZE statistics the dispatcher collects from
 *    its workers.
 *
 * These two structs describe the layout of what Instrumentation.cdbNodeSummary
 * points at.  They used to be defined privately inside explain_gp.c and copied
 * verbatim into the vectorized executor's own EXPLAIN implementation
 * (contrib/vectorization/src/backend/hook/explain.c), which reads the same
 * objects.  Nothing made the compiler check that the two copies agreed, and
 * when they drifted the vectorized side read every field at the wrong offset
 * and printed silently wrong statistics.  One definition, included by both.
 *
 * Portions Copyright (c) 2006-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 *
 * IDENTIFICATION
 *	    src/include/cdb/cdbexplain_stats.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBEXPLAIN_STATS_H
#define CDBEXPLAIN_STATS_H

#include "cdb/cdbexplain.h"		/* CdbExplain_Agg */
#include "executor/instrument.h"	/* instr_time, WalUsage, QueryMetricsStatus */
#include "nodes/execnodes.h"	/* HashInstrumentation, IncrementalSortGroupInfo */
#include "nodes/nodes.h"		/* NodeTag */
#include "utils/tuplesort.h"	/* TuplesortInstrumentation, NUM_SORT_* */

/* EXPLAIN ANALYZE statistics for one plan node of a slice */
typedef struct CdbExplain_StatInst
{
	NodeTag		pstype;			/* PlanState node type */

	/* fields from Instrumentation struct */
	instr_time	starttime;		/* Start time of current iteration of node */
	instr_time	counter;		/* Accumulated runtime for this node */
	double		firsttuple;		/* Time for first tuple of this cycle */
	double		startup;		/* Total startup time (in seconds) */
	double		total;			/* Total total time (in seconds) */
	double		ntuples;		/* Total tuples produced */
	double		ntuples2;
	double		nloops;			/* # of run cycles for this node */
	double		nfiltered1;
	double		nfiltered2;
	bool		prf_work;
	double		nfilteredPRF;
	double		execmemused;	/* executor memory used (bytes) */
	double		workmemused;	/* work_mem actually used (bytes) */
	double		workmemwanted;	/* work_mem to avoid workfile i/o (bytes) */
	bool		workfileCreated;	/* workfile created in this node */
	instr_time	firststart;		/* Start time of first iteration of node */
	int			numPartScanned; /* Number of part tables scanned */

	TuplesortInstrumentation sortstats; /* Sort stats, if this is a Sort node */
	HashInstrumentation hashstats; /* Hash stats, if this is a Hash node */
	IncrementalSortGroupInfo fullsortGroupInfo; /* Full sort group info for Incremental Sort node */
	IncrementalSortGroupInfo prefixsortGroupInfo; /* Prefix sort group info for Incremental Sort node */
	int			bnotes;			/* Offset to beginning of node's extra text */
	int			enotes;			/* Offset to end of node's extra text */
	int 		nworkers_launched; /* Number of workers launched for this node */
	WalUsage	walusage;		/* add WAL usage */

	/* fields from Instrumentation struct for one cycle of a node */
	double tuplecount;
	QueryMetricsStatus nodeStatus; /*CDB: stauts*/
} CdbExplain_StatInst;

/* One node's EXPLAIN ANALYZE statistics for all the workers of its segworker group */
typedef struct CdbExplain_NodeSummary
{
	/* Summary over all the node's workers */
	CdbExplain_Agg ntuples;
	CdbExplain_Agg runtime_tupleAgg; /* tuples of one loop, for runtime stat */
	CdbExplain_Agg nloops;
	CdbExplain_Agg execmemused;
	CdbExplain_Agg workmemused;
	CdbExplain_Agg workmemwanted;
	CdbExplain_Agg totalWorkfileCreated;
	/* Used for DynamicSeqScan, DynamicIndexScan, DynamicBitmapHeapScan, and DynamicForeignScan */
	CdbExplain_Agg totalPartTableScanned;
	/* Summary of space used by sort */
	CdbExplain_Agg sortSpaceUsed[NUM_SORT_SPACE_TYPE][NUM_SORT_METHOD];

	/* insts array info */
	int			segindex0;		/* segment id of insts[0] */
	int			ninst;			/* num of StatInst entries in inst array */

	/*
	 * Per-worker raw stats, kept only when gp_enable_explain_allstat is on and
	 * this node's slice runs more than one parallel worker per segment. Unlike
	 * insts[] (one slot per segment, overwritten by later workers), this keeps
	 * one entry per QE message so a segment's parallel workers stay distinct.
	 * The three arrays are parallel and separately palloc'd; workerinsts is
	 * NULL otherwise.
	 */
	int			nworkerinst;	/* num of entries in the worker arrays */
	CdbExplain_StatInst *workerinsts;	/* [0..nworkerinst-1] raw per-QE stat */
	int		   *workerinst_segindex;	/* segindex of each worker entry */
	int		   *workerinst_workidx;		/* worker index within its segment */
	double	   *workerinst_peakmem;		/* worker QE's peak memory (bytes) */

	/* Array [0..ninst-1] of StatInst entries is appended starting here */
	CdbExplain_StatInst insts[1];	/* variable size - must be last */
} CdbExplain_NodeSummary;

#endif							/* CDBEXPLAIN_STATS_H */
