/*-------------------------------------------------------------------------
 *
 * vec_spill_stats.h
 *	  Per-query accounting of the spill done by the Arrow operators of the
 *	  vectorized executor.
 *
 *	  Arrow writes its spill files with plain POSIX I/O, outside PG's VFD
 *	  layer, so neither the workfile manager nor pg_stat_database sees them.
 *	  Arrow counts the spill of every operator instead (spill_stats.h on the
 *	  Arrow side), keyed by the spill file base the executor hands the
 *	  operator; that base ends with the plan node id.  At the end of a
 *	  top-level query each process turns its counters into EXPLAIN ANALYZE
 *	  notes and temp file statistics.
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 * IDENTIFICATION
 *		contrib/vectorization/src/include/vecexecutor/vec_spill_stats.h
 *-------------------------------------------------------------------------
 */
#ifndef VEC_SPILL_STATS_H
#define VEC_SPILL_STATS_H

#include "postgres.h"
#include "executor/execdesc.h"

/*
 * Spill files are named $DataDir/base/pgsql_tmp/pgsql_tmp_vec_spill_{pid}_
 * {plan_node_id}..., so that CleanupArrowSpillFiles and the post-crash
 * RemovePgTempFiles sweep reclaim them.
 */
#define VEC_SPILL_FILE_PREFIX "vec_spill"

extern void VecSpillFileDir(char *buf, size_t bufsize);
extern void VecSpillFilePrefix(char *buf, size_t bufsize, int plan_node_id);
extern void VecSpillFileBase(char *buf, size_t bufsize, int plan_node_id);
extern void VecSpillStatsReset(QueryDesc *queryDesc);
extern void VecSpillStatsReport(QueryDesc *queryDesc);

#endif							/* VEC_SPILL_STATS_H */
