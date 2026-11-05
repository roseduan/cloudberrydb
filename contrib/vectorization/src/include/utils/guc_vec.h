/*-------------------------------------------------------------------------
 * guc_vec.h
 *	  Vectorization GUCs
 *
 * Copyright (c) 2016-Present Hashdata, Inc. 
 *
 *
 * IDENTIFICATION
 *		src/include/utils/guc_vec.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef GUC_VEC_H
#define GUC_VEC_H

/* max vectorization count */
extern int max_batch_size;
/* deciding whether to enable vectorization */
extern bool enable_vectorization;
/* whether to force vectorization, and if neither orca nor non-ORCA 
supports vectors, use the optimizer originally set*/
extern bool force_vectorization;
/* optimize better plan for tpcds */
extern bool enable_vector_optimizer;
/* min concatenate rows */
extern int min_concatenate_rows;
/* min redistribute motion handle rows */
/* partition top k */
extern int partition_top_k;
extern int take_thread_num;
extern bool two_phase_take;
extern bool gather_motion_take;
/* enable execution resources */
extern int pool_threads;
extern bool print_fallback_log;
/* hash join spill memory budget in MB, 0 = disabled */
extern int hashjoin_spill_memory_mb;
/* memory budget (MB) for window aggregate spill; 0 means disabled (no spill) */
extern int winagg_spill_memory_mb;
/* memory budget (MB) for sonic hash-aggregate spill; 0 means disabled */
extern int sonicagg_spill_memory_mb;
/* memory budget (MB) for the input a vectorized Sort holds; 0 means no spill */
extern int sort_spill_memory_mb;
extern int sort_external_num_threads;
extern char *sort_external_temp_file_base;
/* topk bound threshold: max K value for TopKNode, larger K falls back to OrderByNode */
extern int topk_bound_threshold;
/* topk runtime filter: push threshold to PAX for group-level skip */
extern bool topk_runtime_filter;
/*
 * Limit+HashAgg fusion cap.  When GROUP BY feeds directly into LIMIT,
 * Arrow's GroupByNode tracks only the first N groups (N = LIMIT + OFFSET).
 * This GUC sets the upper bound on N; 0 disables the fusion entirely.
 *
 * The hard ceiling LIMIT_HASHAGG_MAX_TOTAL_MAX is empirical: beyond ~10k
 * the per-row group-tracking overhead in Arrow's GroupByNode dominates
 * the early-stop savings, so we refuse to expose larger settings even
 * if an operator asks for them.
 */
#define LIMIT_HASHAGG_MAX_TOTAL_MAX 10000
extern int limit_hashagg_max_total;
/* sonic motion direct send: emit segment-tagged batches from Sonic so that a
 * downstream Redistribute Motion can route each batch without re-hashing */
extern bool sonic_motion_direct_send;
/* sonic hash join: route a vectorized HashJoin to Arrow's Sonic join instead
 * of the normal Arrow hash join, when the join type / key / payload types are
 * supported. Off keeps all hash joins on the normal Arrow hash join path. */
extern bool enable_sonic_hashjoin;
/* backpressure for vectorized execution (push_pipeline / yield / SinkNode) */
extern bool enable_vec_pipeline;
extern int backpressure_memory_mb;
extern int query_memory_limit_mb;
extern bool report_memory_to_vmem;
/*
 * Allow ORCA to consider WindowHashAgg for vector plans.  Only window queries
 * that are not forced into Partition Top-K can get one: those with a window
 * lacking PARTITION BY, or all of them while enable_partition_topk is off.
 * The forced Partition Top-K shape always uses an ordered WindowAgg.  Off
 * keeps WindowHashAgg out of the plan altogether.
 */
extern bool enable_window_hash_agg;
/*
 * Force the Partition Top-K world for window queries that
 * should_force_partition_topk() picks.  Off lets ORCA choose, which is the
 * only way to get WindowHashAgg back for those queries short of turning
 * the optimizer or vectorization off.
 */
extern bool enable_partition_topk;

#endif   /* GUC_VEC_H */
