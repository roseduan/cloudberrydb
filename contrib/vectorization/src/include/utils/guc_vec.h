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
extern bool sort_use_external_sort;
extern int sort_external_batch_size;
extern int sort_external_num_threads;
extern int sort_external_max_rows;
extern char *sort_external_temp_file_base;
/* topk bound threshold: max K value for TopKNode, larger K falls back to OrderByNode */
extern int topk_bound_threshold;
/* topk runtime filter: push threshold to PAX for group-level skip */
extern bool enable_topk_runtime_filter;
/* limit+hashagg fusion: only track first N groups when GROUP BY feeds LIMIT */
extern bool enable_limit_hashagg;

#endif   /* GUC_VEC_H */
