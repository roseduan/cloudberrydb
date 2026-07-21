/*-------------------------------------------------------------------------
 *
 * nodePartitionTopK.h
 *	  Vectorized PartitionTopK: per-partition rank() <= k pre-filter.
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 * IDENTIFICATION
 *	  contrib/vectorization/src/include/vecexecutor/nodePartitionTopK.h
 *-------------------------------------------------------------------------
 */
#ifndef VEC_NODE_PARTITION_TOPK_H
#define VEC_NODE_PARTITION_TOPK_H

#include "vecexecutor/execnodes.h"

extern PartitionTopKState *ExecInitVecPartitionTopK(PartitionTopK *node,
													EState *estate,
													int eflags);
extern void ExecEndVecPartitionTopK(PartitionTopKState *node);

#endif							/* VEC_NODE_PARTITION_TOPK_H */
