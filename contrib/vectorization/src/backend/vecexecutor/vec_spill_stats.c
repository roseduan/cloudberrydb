/*-------------------------------------------------------------------------
 *
 * vec_spill_stats.c
 *	  Per-query accounting of the spill done by the Arrow operators of the
 *	  vectorized executor.  See vec_spill_stats.h.
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 * IDENTIFICATION
 *		contrib/vectorization/src/backend/vecexecutor/vec_spill_stats.c
 *-------------------------------------------------------------------------
 */
#include "arrow-glib/compute.h"
#include "postgres.h"

#include <errno.h>

#include "executor/executor.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "storage/fd.h"
#include "utils/backend_status.h"
#include "utils/guc_vec.h"
#include "utils/metrics_utils.h"
#include "vecexecutor/vec_spill_stats.h"

#define SPILL_BYTES_PER_MB	(1024.0 * 1024.0)
#define SPILL_NS_PER_MS		1000000.0

typedef struct FindPlanStateContext
{
	int			plan_node_id;
	PlanState  *found;
} FindPlanStateContext;

/*
 * The directory every vectorized spill file goes to: $DataDir/base/pgsql_tmp,
 * where CleanupArrowSpillFiles and the post-crash RemovePgTempFiles sweep
 * reclaim them.  pgsql_tmp is only created by OpenTemporaryFile, so on a
 * cluster that has not used a temp file yet it may not exist; create it here,
 * so that a permission or disk-full problem gets a proper error rather than
 * an ENOENT from Arrow's open().
 */
void
VecSpillFileDir(char *buf, size_t bufsize)
{
	snprintf(buf, bufsize, "%s/base/%s", DataDir, PG_TEMP_FILES_DIR);
	if (MakePGDirectory(buf) < 0 && errno != EEXIST)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create temporary file directory \"%s\": %m",
						buf)));
}

/*
 * The name prefix of the spill files of plan node plan_node_id,
 * pgsql_tmp_vec_spill_<pid>_<plan_node_id>.  The operator appends its own
 * suffix per file.
 */
void
VecSpillFilePrefix(char *buf, size_t bufsize, int plan_node_id)
{
	snprintf(buf, bufsize, "%s_%s_%d_%d",
			 PG_TEMP_FILE_PREFIX, VEC_SPILL_FILE_PREFIX,
			 MyProcPid, plan_node_id);
}

/*
 * VecSpillFileDir() and VecSpillFilePrefix() joined into one path, for the
 * operators that take a single file base.
 */
void
VecSpillFileBase(char *buf, size_t bufsize, int plan_node_id)
{
	char		spill_dir[MAXPGPATH];
	char		prefix[MAXPGPATH];

	VecSpillFileDir(spill_dir, sizeof(spill_dir));
	VecSpillFilePrefix(prefix, sizeof(prefix), plan_node_id);
	snprintf(buf, bufsize, "%s/%s", spill_dir, prefix);
}

static bool
is_top_level_query(QueryDesc *queryDesc)
{
	return queryDesc->plannedstmt->metricsQueryType == TOP_LEVEL_QUERY;
}

/*
 * Forget the spill of earlier queries.  Called when a top-level query starts;
 * the spill of queries nested inside it is reported together with it.
 */
void
VecSpillStatsReset(QueryDesc *queryDesc)
{
	if (is_top_level_query(queryDesc))
		garrow_spill_stats_reset();
}

/*
 * The plan node id an operator's spill tag was built from: every tag this
 * executor hands out ends with "_<plan_node_id>" (VecSpillFilePrefix).
 * Returns -1 for a tag of another shape, e.g. a
 * vector.sort_external_temp_file_base set by the user.
 */
static int
spill_tag_plan_node_id(const char *tag)
{
	const char *p;
	char	   *end;
	long		id;

	if (strstr(tag, VEC_SPILL_FILE_PREFIX) == NULL)
		return -1;
	p = strrchr(tag, '_');
	if (p == NULL || p[1] == '\0')
		return -1;
	errno = 0;
	id = strtol(p + 1, &end, 10);
	if (errno != 0 || *end != '\0' || id < 0 || id > PG_INT32_MAX)
		return -1;
	return (int) id;
}

static bool
find_planstate_walker(PlanState *planstate, void *context)
{
	FindPlanStateContext *ctx = (FindPlanStateContext *) context;

	if (planstate->plan->plan_node_id == ctx->plan_node_id)
	{
		ctx->found = planstate;
		return true;
	}
	return planstate_tree_walker(planstate, find_planstate_walker, context);
}

static PlanState *
find_planstate(PlanState *root, int plan_node_id)
{
	FindPlanStateContext ctx;

	if (root == NULL || plan_node_id < 0)
		return NULL;
	ctx.plan_node_id = plan_node_id;
	ctx.found = NULL;
	if (!find_planstate_walker(root, &ctx))
		return NULL;
	return ctx.found;
}

/* The memory budget the operator spilled beyond, from its GUC. */
static double
spill_budget_bytes(gint32 type)
{
	int			mb;

	switch (type)
	{
		case GARROW_SPILL_OP_HASH_JOIN:
		case GARROW_SPILL_OP_SONIC_JOIN:
			mb = hashjoin_spill_memory_mb;
			break;
		case GARROW_SPILL_OP_SONIC_AGG:
			mb = sonicagg_spill_memory_mb;
			break;
		case GARROW_SPILL_OP_WINDOW_AGG:
			mb = winagg_spill_memory_mb;
			break;
		case GARROW_SPILL_OP_EXTERNAL_SORT:
			mb = sort_spill_memory_mb;
			break;
		default:
			mb = 0;
			break;
	}
	return (double) mb * 1024.0 * 1024.0;
}

/*
 * Add one operator's spill to the EXPLAIN ANALYZE notes of its plan node.
 *
 * The QD keeps the notes of one segment only, the one that stands out in
 * rows, memory used or work_mem wanted.  Nodes inside an Arrow plan report
 * none of these, so their notes would always be dropped; record the memory
 * the node would have needed to avoid spilling (its budget plus what it
 * spilled) as work_mem wanted, which makes the QD show the notes of the
 * segment that spilled most.
 */
static void
add_explain_note(PlanState *planstate, const GArrowSpillOpStat *stat)
{
	StringInfo	buf;

	planstate->instrument->workfileCreated = true;
	planstate->instrument->workmemwanted +=
		spill_budget_bytes(stat->type) + (double) stat->bytes_written;

	if (planstate->cdbexplainbuf == NULL)
		planstate->cdbexplainbuf = makeStringInfo();
	buf = planstate->cdbexplainbuf;
	if (buf->len > 0 && buf->data[buf->len - 1] != '\n')
		appendStringInfoChar(buf, '\n');

	appendStringInfo(buf,
					 "Vec spill (%s): %ld spills, %ld files, written %.3f MB, read %.3f MB, write time %.3f ms, read time %.3f ms",
					 garrow_spill_op_type_name(stat->type),
					 (long) stat->spill_count,
					 (long) stat->files,
					 stat->bytes_written / SPILL_BYTES_PER_MB,
					 stat->bytes_read / SPILL_BYTES_PER_MB,
					 stat->write_ns / SPILL_NS_PER_MS,
					 stat->read_ns / SPILL_NS_PER_MS);
	if (stat->repartitions > 0)
		appendStringInfo(buf, ", %ld repartitions", (long) stat->repartitions);
}

/*
 * Count the spill files in pg_stat_database.temp_files / temp_bytes, as PG
 * does for its own temporary files.  Only the total size is known, so it is
 * charged to the first file.
 */
static void
report_temp_files(const GArrowSpillOpStat *stat)
{
	int64		nfiles = Max(stat->files, 1);

	pgstat_report_tempfile((size_t) stat->bytes_written);
	for (int64 i = 1; i < nfiles; i++)
		pgstat_report_tempfile(0);
}

/*
 * Report the spill of the query that just finished executing in this
 * process.  Called from ExecutorFinish, so that the EXPLAIN ANALYZE notes are
 * in place before the QD prints the plan and before the QE sends its stats.
 */
void
VecSpillStatsReport(QueryDesc *queryDesc)
{
	GArrowSpillOpStat *stats;
	guint		n_stats;

	if (!is_top_level_query(queryDesc))
		return;

	stats = garrow_spill_stats_snapshot(&n_stats);
	for (guint i = 0; i < n_stats; i++)
	{
		const GArrowSpillOpStat *stat = &stats[i];
		int			plan_node_id = spill_tag_plan_node_id(stat->tag);
		PlanState  *planstate = find_planstate(queryDesc->planstate,
											   plan_node_id);

		if (planstate != NULL && planstate->instrument != NULL)
			add_explain_note(planstate, stat);
		report_temp_files(stat);
	}
	garrow_spill_stats_free(stats, n_stats);

	/* Don't report the same spill again if the executor is reused. */
	garrow_spill_stats_reset();
}
