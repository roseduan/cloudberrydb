/*-------------------------------------------------------------------------
 *
 * nodeCustomscan.h
 *	  Vectorized execution wrapper for CustomScan nodes.
 *
 * Mirrors nodeForeignscan.h: a CustomScan provider (currently the datalake
 * iceberg AM's "Iceberg Scan") that returns virtual slots carrying an ABI
 * ArrowRecordBatch* is driven here and imported into the vector engine, the
 * same way ExecInitVecForeignScan drives an FDW.
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 *-------------------------------------------------------------------------
 */
#ifndef VEC_NODECUSTOMSCAN_H
#define VEC_NODECUSTOMSCAN_H

#include "nodes/execnodes.h"
#include "nodes/plannodes.h"

struct VecCustomScanState;

extern CustomScanState *ExecInitVecCustomScan(CustomScan *node, EState *estate, int eflags);
extern void ExecEndVecCustomScan(CustomScanState *node);
extern void ExecReScanVecCustomScan(CustomScanState *node);
extern void ExecSquelchVecCustomScan(CustomScanState *node);
extern struct VecCustomScanState *GetVecCustomScanState(CustomScanState *node);
extern TupleTableSlot *VecCustomNext(CustomScanState *node);
extern bool VecCustomRecheck(CustomScanState *node, TupleTableSlot *slot);

#endif							/* VEC_NODECUSTOMSCAN_H */
