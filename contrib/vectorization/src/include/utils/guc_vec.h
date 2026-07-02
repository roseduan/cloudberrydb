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
extern int min_redistribute_handle_rows;
/* partition top k */
extern int partition_top_k;
extern int min_redistribute_handle_rows;
extern int control_memory_resource;
extern int control_global_memory_resource;
extern int take_thread_num;
extern bool two_phase_take;
extern bool gather_motion_take;
/* enable execution resources */
extern bool enable_vector_memory_resource;
extern int pool_threads;
extern bool print_fallback_log;
/* hash join spill memory budget in MB, 0 = disabled */
extern int hashjoin_spill_memory_mb;
/* memory budget (MB) for window aggregate spill; 0 means disabled (no spill) */
extern int winagg_spill_memory_mb;
/* memory budget (MB) for sonic hash-aggregate spill; 0 means disabled */
extern int sonicagg_spill_memory_mb;
extern bool sort_use_external_sort;
extern int sort_external_batch_size;
extern int sort_external_num_threads;
extern int sort_external_max_rows;
extern char *sort_external_temp_file_base;
/* topk bound threshold: max K value for TopKNode, larger K falls back to OrderByNode */
extern int topk_bound_threshold;
/* topk runtime filter: push threshold to PAX for group-level skip */
extern bool enable_topk_runtime_filter;
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
extern bool enable_sonic_motion_direct_send;
/*
 * Toggle for RIGHT_SEMI / RIGHT_ANTI hash join (Mark Join) plan. When on,
 * ORCA's cost model picks the right-flipped form per join based on relative
 * outer/inner cardinalities (small outer + large inner with duplicates). When
 * off, CostRightSemi/AntiHashJoin returns infinity so ORCA never selects the
 * new physical operators and the planner falls back to LEFT_SEMI/ANTI.
 * Default on; turn off as an emergency kill switch.
 */
extern bool enable_right_join_flip;
/* sonic hash join: route a vectorized HashJoin to Arrow's Sonic join instead
 * of the normal Arrow hash join, when the join type / key / payload types are
 * supported. Off forces all hash joins onto the normal Arrow hash join path. */
extern bool enable_sonic_hashjoin;

#endif   /* GUC_VEC_H */
