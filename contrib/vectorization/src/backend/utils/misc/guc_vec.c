/*--------------------------------------------------------------------
 * guc_vec.h
 *	  Define Vectorization GUCs
 *
 *
 * Copyright (c) 2016-Present Hashdata, Inc. 
 *
 * IDENTIFICATION
 *	  src/backend/misc/guc_vec.c
 *
 *--------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/guc_vec.h"
#include "cdb/cdbvars.h"
#include "miscadmin.h"

/* max vectorization count */
int max_batch_size = 0;

/* deciding whether to enable vectorization */
bool enable_vectorization = false;

bool force_vectorization = false;

bool enable_vector_optimizer = false;

int min_concatenate_rows = 0;
int min_redistribute_handle_rows = 0;
int partition_top_k = 0;
int take_thread_num = 0;
bool two_phase_take = false;
bool sort_use_external_sort = false;
int sort_external_batch_size = 65536;
int sort_external_num_threads = 0;
int sort_external_max_rows = 65536;
char *sort_external_temp_file_base = NULL;
bool gather_motion_take = false;
int control_memory_resource = 5;
int control_global_memory_resource = 5;
bool enable_vector_memory_resource = false;
int pool_threads = 0;
bool print_fallback_log = false;
int hashjoin_spill_memory_mb = 4096;
int winagg_spill_memory_mb = 4096;
int sonicagg_spill_memory_mb = 4096;
int topk_bound_threshold = 2000;
bool enable_topk_runtime_filter = true;
int limit_hashagg_max_total = 1000;
bool enable_sonic_motion_direct_send = false;
bool enable_sonic_hashjoin = false;

/*
 * Drop ProjectNodes that emit their input unchanged.  Kept as a switch so a
 * plan can be captured with and without the elision for comparison.
 */
bool enable_vec_identity_project_elision = true;

/* backpressure for vectorized execution (push_pipeline / yield / SinkNode) */
/*
 * Keep yield mode away from plan shapes that deadlocked it (a ShareInputScan
 * anywhere, or a slice reading two or more Motions).  Kept as a switch so the
 * cost of the guard can be measured against the shapes it protects.
 */
bool enable_vec_yield_guard = true;

bool enable_vec_pipeline = false;
int backpressure_memory_mb = 256;

bool enable_window_hash_agg = true;
bool enable_partition_topk = true;
bool partition_topk_require_topk_qual = false;
