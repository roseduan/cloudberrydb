/*-------------------------------------------------------------------------
 *
 * nodePartitionTopK.c
 *	  Vectorized PartitionTopK executor node.
 *
 * The heavy lifting happens in the Arrow "partition_topk" node built by
 * BuildVecPlan: per distinct partition-key combination it keeps every row
 * whose rank() by the sort keys is <= top_k (ties at rank k all kept),
 * mirroring the row executor's nodePartitionTopK.c semantics. This file
 * only wires the PG plan node onto the Arrow plan, like nodeLimit.c.
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 * IDENTIFICATION
 *	  contrib/vectorization/src/backend/vecexecutor/nodePartitionTopK.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "executor/executor.h"
#include "miscadmin.h"

#include "vecexecutor/nodePartitionTopK.h"
#include "vecexecutor/executor.h"
#include "vecexecutor/execnodes.h"
#include "vecexecutor/execAmi.h"
#include "vecexecutor/execslot.h"

static TupleTableSlot *
ExecVecPartitionTopK(PlanState *pstate)
{
	VecPartitionTopKState *vnode = (VecPartitionTopKState *) pstate;

	CHECK_FOR_INTERRUPTS();

	return ExecuteVecPlan(&vnode->estate);
}

PartitionTopKState *
ExecInitVecPartitionTopK(PartitionTopK *node, EState *estate, int eflags)
{
	VecPartitionTopKState *vstate;
	PartitionTopKState *state;

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	vstate = (VecPartitionTopKState *) palloc0(sizeof(VecPartitionTopKState));
	state = (PartitionTopKState *) vstate;
	NodeSetTag(state, T_PartitionTopKState);
	state->ps.plan = (Plan *) node;
	state->ps.state = estate;
	state->ps.ExecProcNode = ExecVecPartitionTopK;

	ExecAssignExprContext(estate, &state->ps);

	outerPlanState(state) = VecExecInitNode(outerPlan(node), estate, eflags);

	/*
	 * PartitionTopK is a pure pass-through filter: result type equals the
	 * outer plan's result type.
	 */
	ExecInitResultTupleSlotTL(&state->ps, &TTSOpsVecTuple);
	state->ps.resultopsset = true;
	state->ps.resultops = ExecGetResultSlotOps(outerPlanState(state),
											   &state->ps.resultopsfixed);
	state->ps.ps_ProjInfo = NULL;

	PostBuildVecPlan((PlanState *) vstate, &vstate->estate);

	return state;
}

void
ExecEndVecPartitionTopK(PartitionTopKState *node)
{
	VecPartitionTopKState *vnode = (VecPartitionTopKState *) node;

	if (node->ps.ps_ResultTupleSlot)
		ExecClearTuple(node->ps.ps_ResultTupleSlot);
	ExecFreeExprContext(&node->ps);

	FreeVecExecuteState(&vnode->estate);

	VecExecEndNode(outerPlanState(node));
}
