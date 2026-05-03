/*--------------------------------------------------------------------
 * main.c
 *
 * Copyright (c) 2016-Present Hashdata, Inc. 
 *
 * IDENTIFICATION
 *	  main.c
 *
 *--------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/stat.h>

#include "access/printtup_vec.h"
#include "cdb/cdbvars.h"
#include "optimizer/planner.h"
#include "postmaster/bgworker.h"
#include "postmaster/bgworker_internals.h"
#include "postmaster/interrupt.h"
#include "postmaster/postmaster.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "utils/guc.h"

#include "hook/hook.h"
#include "optimizer/planner_vec.h"
#include "tcop/tcopprot.h"
#include "utils/am_vec.h"
#include "utils/guc_vec.h"
#include "vecexecutor/executor.h"
#include "vecexecutor/nodeShareInputScan.h"
#include "vecnodes/nodes.h"

#include "catalog/pg_tablespace_d.h"
#include "transform.h"
PG_MODULE_MAGIC;

exec_simple_query_hook_type exec_simple_query_hook_prev = NULL;
static void exec_simple_query_vec(const char *query_string);
static bool CleanupWorker(bool is_init);
static bool rm_all_vec_path(const char *path);

void		_PG_init(void);

PG_FUNCTION_INFO_V1(clearfile);

Datum
clearfile(PG_FUNCTION_ARGS)
{
	bool flag = PG_GETARG_BOOL(0);
	PG_RETURN_BOOL(CleanupWorker(flag));
}

void
_PG_init(void)
{
    DefineCustomIntVariable("vector.max_batch_size",
                            "max vectorization executor row count handle in one batch",
                            NULL,
                            &max_batch_size,
                            16384,
                            0, 163840,
                            PGC_USERSET,
							GUC_GPDB_NEED_SYNC,
                            NULL, NULL, NULL);
    DefineCustomBoolVariable("vector.enable_vectorization",
                             "Enables the planner's use of vectorized plans."
                             "Notice: gp_interconnect_queue_depth and gp_interconnect_snd_queue_depth"
                             "will be set to 4096 when vectorization is enabled,"
                             "and restore to default value when disabled.",
                             NULL,
                             &enable_vectorization,
                             false,
                             PGC_USERSET,
							 GUC_GPDB_NEED_SYNC,
                             NULL, NULL, NULL);
    DefineCustomBoolVariable("vector.enable_vector_optimizer",
							 "This guc enables optimizing better plan for vectorization",
							 NULL,
							 &enable_vector_optimizer,
							 false,
							 PGC_USERSET,
							 GUC_GPDB_NO_SYNC,
							 NULL, NULL, NULL);
    DefineCustomBoolVariable("vector.force_vectorization",
                             "Force the planner's use of vectorized plans."
                             "If the plan produced by the current optimizer does not support vectorization, "
                             "it will switch to regenerate the plan using another optimizer. "
                             "If the plans produced by both optimizers cannot be vectorized, "
                             "the plan produced by the preset optimizer is used",
                             NULL,
                             &force_vectorization,
                             false,
                             PGC_USERSET,
							 GUC_GPDB_NEED_SYNC,
                             NULL, NULL, NULL);
    DefineCustomIntVariable("vector.min_concatenate_rows",
                            "Minimum number of rows to motion concatenate",
                            NULL,
                            &min_concatenate_rows,
                            max_batch_size,
                            0, 65536,
                            PGC_USERSET,
                            GUC_GPDB_NEED_SYNC,
                            NULL, NULL, NULL);
    DefineCustomIntVariable("vector.min_redistribute_handle_rows",
                            "Minimum number of rows to motion concatenate",
                            NULL,
                            &min_redistribute_handle_rows,
                            0,
                            0, 163840,
                            PGC_USERSET,
                            GUC_GPDB_NEED_SYNC,
                            NULL, NULL, NULL);
    DefineCustomIntVariable("vector.partition_top_k",
                            "Partition selecter for WindowAgg sort",
                            NULL,
                            &partition_top_k,
                            0,
                            0, 10000000,
                            PGC_USERSET,
                            GUC_GPDB_NEED_SYNC,
                            NULL, NULL, NULL);
    DefineCustomIntVariable("vector.take_thread_num",
                            "take thread for sort",
                            NULL,
                            &take_thread_num,
                            0,
                            0, 10000000,
                            PGC_USERSET,
                            GUC_GPDB_NEED_SYNC,
                            NULL, NULL, NULL);
    DefineCustomBoolVariable("vector.two_phase_take",
                             "take phase for take",
                             NULL,
                             &two_phase_take,
                             false,
                             PGC_USERSET,
                             GUC_GPDB_NEED_SYNC,
                             NULL, NULL, NULL);
    DefineCustomBoolVariable("vector.sort_use_external_sort",
                             "Enable Arrow file-based external sort in vectorized sort paths",
                             NULL,
                             &sort_use_external_sort,
                             false,
                             PGC_USERSET,
                             GUC_GPDB_NEED_SYNC,
                             NULL, NULL, NULL);
    DefineCustomIntVariable("vector.sort_external_batch_size",
                            "Rows per run when vectorized external sort is enabled",
                            NULL,
                            &sort_external_batch_size,
                            65536,
                            1, 100000000,
                            PGC_USERSET,
                            GUC_GPDB_NEED_SYNC,
                            NULL, NULL, NULL);
    DefineCustomIntVariable("vector.sort_external_num_threads",
                            "Worker count for vectorized external sort, 0 follows Arrow defaults",
                            NULL,
                            &sort_external_num_threads,
                            0,
                            0, 1024,
                            PGC_USERSET,
                            GUC_GPDB_NEED_SYNC,
                            NULL, NULL, NULL);
    DefineCustomIntVariable("vector.sort_external_max_rows",
                            "Maximum in-memory rows before vectorized external sort spills to files",
                            NULL,
                            &sort_external_max_rows,
                            65536,
                            1, 100000000,
                            PGC_USERSET,
                            GUC_GPDB_NEED_SYNC,
                            NULL, NULL, NULL);
    DefineCustomStringVariable("vector.sort_external_temp_file_base",
                               "Optional base path for vectorized external sort spill files",
                               NULL,
                               &sort_external_temp_file_base,
                               "",
                               PGC_SUSET,
                               GUC_GPDB_NEED_SYNC,
                               NULL, NULL, NULL);
    DefineCustomBoolVariable("vector.gather_motion_take",
                             "take for gather motion",
                             NULL,
                             &gather_motion_take,
                             false,
                             PGC_USERSET,
                             GUC_GPDB_NEED_SYNC,
                             NULL, NULL, NULL);
    DefineCustomIntVariable("vector.control_memory_resource",
                            "minimal execution resources",
                            NULL,
                            &control_memory_resource,
                            6,
                            1, 8,
                            PGC_USERSET,
                            GUC_GPDB_NEED_SYNC,
                            NULL, NULL, NULL);

    DefineCustomIntVariable("vector.control_global_memory_resource",
                            "global max execution resources",
                            NULL,
                            &control_global_memory_resource,
                            1,
                            1, 10,
                            PGC_USERSET,
                            GUC_GPDB_NEED_SYNC,
                            NULL, NULL, NULL);

    DefineCustomBoolVariable("vector.enable_vector_memory_resource",
                             "enable execution resources",
                             NULL,
                             &enable_vector_memory_resource,
                             false,
                             PGC_USERSET,
                             GUC_GPDB_NEED_SYNC,
                             NULL, NULL, NULL);

    DefineCustomIntVariable("vector.pool_threads",
                            "num threads in pool",
                            NULL,
                            &pool_threads,
                            0,
                            0, 64,
                            PGC_USERSET,
                            GUC_GPDB_NEED_SYNC,
                            NULL, NULL, NULL);

    DefineCustomBoolVariable("vector.print_fallback_log",
							 "This guc enables print fallback log",
							 NULL,
							 &print_fallback_log,
							 false,
							 PGC_USERSET,
							 GUC_GPDB_NO_SYNC,
							 NULL, NULL, NULL);

    DefineCustomIntVariable("vector.hashjoin_spill_memory_mb",
                            "Memory budget in MB for hash join build side. "
                            "When estimated hash table size exceeds this, data is spilled to disk. "
                            "0 means disabled (no spill).",
                            NULL,
                            &hashjoin_spill_memory_mb,
                            512,
                            0, INT_MAX,
                            PGC_USERSET,
                            GUC_GPDB_NEED_SYNC,
                            NULL, NULL, NULL);

    DefineCustomIntVariable("vector.winagg_spill_memory_mb",
                            "Memory budget in MB for window aggregate spill. "
                            "0 means disabled (no spill).",
                            NULL,
                            &winagg_spill_memory_mb,
                            512,
                            0, INT_MAX,
                            PGC_USERSET,
                            GUC_GPDB_NEED_SYNC,
                            NULL, NULL, NULL);

    DefineCustomIntVariable("vector.topk_bound_threshold",
                            "Max K value for TopKNode; larger K falls back to OrderByNode+SelectK",
                            NULL,
                            &topk_bound_threshold,
                            2000,
                            0, 10000000,
                            PGC_USERSET,
                            GUC_GPDB_NEED_SYNC,
                            NULL, NULL, NULL);

    DefineCustomBoolVariable("vector.topk_runtime_filter",
                             "Enable TopK runtime filter to skip PAX groups "
                             "whose min/max exceeds the TopK threshold.",
                             NULL,
                             &enable_topk_runtime_filter,
                             true,
                             PGC_USERSET,
                             GUC_GPDB_NEED_SYNC,
                             NULL, NULL, NULL);

    DefineCustomIntVariable("vector.limit_hashagg_max_total",
                            "Maximum value of (LIMIT + OFFSET) at which the "
                            "Limit+HashAgg fusion is applied; 0 disables "
                            "the fusion. When GROUP BY feeds directly into "
                            "LIMIT without ORDER BY, Arrow's GroupByNode "
                            "tracks only the first (LIMIT + OFFSET) groups. "
                            "Above this threshold the per-group tracking "
                            "overhead is judged to outweigh the early-stop "
                            "benefit.",
                            NULL,
                            &limit_hashagg_max_total,
                            1000,
                            0,
                            LIMIT_HASHAGG_MAX_TOTAL_MAX,
                            PGC_USERSET,
                            GUC_GPDB_NEED_SYNC,
                            NULL, NULL, NULL);

    exec_simple_query_hook_prev = exec_simple_query_hook;
    exec_simple_query_hook = exec_simple_query_vec;

    planner_prev = planner_hook; 
    planner_hook = planner_hook_wrapper;

    vec_explain_prev = ExplainOneQuery_hook;
    ExplainOneQuery_hook = VecExplainOneQuery;

    vec_exec_start_prev = ExecutorStart_hook;
    ExecutorStart_hook = ExecutorStartWrapper;

    vec_exec_run_prev = ExecutorRun_hook;
    ExecutorRun_hook = ExecutorRunWrapper;

    vec_exec_finish_prev = ExecutorFinish_hook;
    ExecutorFinish_hook = ExecutorFinishWrapper;

    vec_exec_end_prev = ExecutorEnd_hook;
    ExecutorEnd_hook = ExecutorEndWrapper;

    /* init vectorization am routine */
    InitAOCSVecHandler();

    RegisterXactCallback(dummy_schema_xact_cb, NULL);

    RegisterVectorExtensibleNode();

    CleanupWorker(true);
}

static void
exec_simple_query_vec(const char *query_string)
{
	if (enable_vector_optimizer && enable_vectorization && Gp_role == GP_ROLE_DISPATCH)
	{
		const char **query_string_ref = &query_string;
		try_transform_sql(query_string_ref);
		exec_simple_query(*query_string_ref);
		restore_gucs();
	}
	else if (exec_simple_query_hook_prev)
		exec_simple_query_hook_prev(query_string);
	else
		exec_simple_query(query_string);
}

static bool
rm_all_vec_path(const char *dir)
{
	char command[MAXPGPATH] = { "\0" };
	int ret = 0;

	snprintf(command, sizeof(command), "rm -rf %s", dir);
	ret = system(command);
	if (ret != 0)
	{
		ereport(WARNING,
				(errmsg("Deleting vectorization directory %s failed.",
				dir)));
		return false;
	}
	return true;
}

static bool
CleanupWorker(bool is_init)
{
	Oid tblspcOid = InvalidOid;
	char ts_path[MAXPGPATH] = { "\0" };
	char temp_path[MAXPGPATH] = { "\0" };
	char vec_path[MAXPGPATH] = { "\0" };
	bool ret = false;

	if (!process_shared_preload_libraries_in_progress)
		return ret;

	tblspcOid = MyDatabaseTableSpace ? MyDatabaseTableSpace : DEFAULTTABLESPACE_OID;
	TempTablespacePath(ts_path, tblspcOid);
	snprintf(temp_path, sizeof(temp_path), "%s", ts_path);
	snprintf(vec_path, sizeof(vec_path), "%s/%s*", ts_path, VECPATH);
	ret = cleanup_directory(temp_path, 0, 0, true, false);

	if (ret || is_init)
		return rm_all_vec_path(vec_path);

	return ret;
}

PG_FUNCTION_INFO_V1(vector_stddev_in);
PG_FUNCTION_INFO_V1(vector_stddev_out);


Datum
vector_stddev_in(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("cannot accept a value of a vector type")));

	PG_RETURN_VOID();                       /* keep compiler quiet */
}

Datum
vector_stddev_out(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("cannot display a value of a vector type")));

	PG_RETURN_VOID();                       /* keep compiler quiet */
}
