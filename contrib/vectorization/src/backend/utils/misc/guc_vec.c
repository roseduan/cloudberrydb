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
int max_batch_size = 16384;

/* deciding whether to enable vectorization */
bool enable_vectorization = false;

bool force_vectorization = false;

bool enable_vector_optimizer = false;

int min_concatenate_rows = 0;
int partition_top_k = 0;
int take_thread_num = 0;
bool two_phase_take = false;
int sort_external_num_threads = 0;
char *sort_external_temp_file_base = NULL;
bool gather_motion_take = false;
int pool_threads = 0;
bool print_fallback_log = false;
int hashjoin_spill_memory_mb = 4096;
int winagg_spill_memory_mb = 4096;
int sonicagg_spill_memory_mb = 4096;
int sort_spill_memory_mb = 4096;
int topk_bound_threshold = 2000;
bool topk_runtime_filter = true;
int limit_hashagg_max_total = 1000;
bool sonic_motion_direct_send = false;
bool enable_sonic_hashjoin = false;

/* backpressure for vectorized execution (push_pipeline / yield / SinkNode) */
bool enable_vec_pipeline = false;
int backpressure_memory_mb = 256;

/* Ceiling on arrow memory per query; 0 leaves it unconstrained. */
int query_memory_limit_mb = 0;

/* Whether arrow's memory is reported to the GPDB vmem tracker. */
bool report_memory_to_vmem = false;

bool enable_window_hash_agg = true;
bool enable_partition_topk = true;
