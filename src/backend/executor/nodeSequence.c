/*
 * nodeSequence.c
 *   Routines to handle Sequence node.
 *
 * Portions Copyright (c) 2012 - present, EMC/Greenplum
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 *
 *
 * IDENTIFICATION
 *	    src/backend/executor/nodeSequence.c
 *
 * Sequence node contains a list of subplans, which will be processed in the 
 * order of left-to-right. Result tuples from the last subplan will be outputted
 * as the results of the Sequence node.
 *
 * Sequence does not make use of its left and right subtrees, and instead it
 * maintains a list of subplans explicitly.
 */

#include "postgres.h"

#include "executor/nodeSequence.h"
#include "executor/executor.h"
#include "miscadmin.h"

#include "access/parallel.h"
#include "storage/barrier.h"
#include "utils/wait_event.h"

SequenceState *
ExecInitSequence(Sequence *node, EState *estate, int eflags)
{
	SequenceState *sequenceState;
	PlanState  *lastPlanState;

	/* Check for unsupported flags */
	Assert(!(eflags & EXEC_FLAG_MARK));

	/* Sequence should not contain 'qual'. */
	Assert(node->plan.qual == NIL);

	sequenceState = makeNode(SequenceState);
	sequenceState->ps.plan = (Plan *)node;
	sequenceState->ps.state = estate;
	sequenceState->ps.ExecProcNode = ExecSequence;

	int numSubplans = list_length(node->subplans);
	Assert(numSubplans >= 1);
	sequenceState->subplans = (PlanState **)palloc0(numSubplans * sizeof(PlanState *));
	sequenceState->numSubplans = numSubplans;
	
	/* Initialize subplans */
	ListCell *lc;
	int no = 0;
	foreach (lc, node->subplans)
	{
		Plan *subplan = (Plan *)lfirst(lc);
		Assert(subplan != NULL);
		Assert(no < numSubplans);
		
		sequenceState->subplans[no] = ExecInitNode(subplan, estate, eflags);
		no++;
	}

	sequenceState->initState = true;

	/* Initialize parallel state fields */
	sequenceState->pstate = NULL;
	sequenceState->pstate_len = 0;
	sequenceState->my_participant_id = -1;

	/* Sequence does not need projection. */
	sequenceState->ps.ps_ProjInfo = NULL;

	/*
	 * Initialize result type. We will pass through the last child slot.
	 */
	lastPlanState = sequenceState->subplans[numSubplans - 1];
	ExecInitResultTypeTL(&sequenceState->ps);
	sequenceState->ps.resultopsset = true;
	sequenceState->ps.resultops = ExecGetResultSlotOps(lastPlanState,
													   &lastPlanState->resultopsfixed);

	return sequenceState;
}

/*
 * completeSubplan
 *   Execute a given subplan to completion.
 *
 * The outputs from the given subplan will be discarded.
 */
static void
completeSubplan(PlanState *subplan)
{
	while (ExecProcNode(subplan) != NULL)
	{
	}
}

TupleTableSlot *
ExecSequence(PlanState *pstate)
{
	SequenceState *node = castNode(SequenceState, pstate);

	/*
	 * If no subplan has been executed yet, execute them here, except for
	 * the last subplan.
	 */
	if (node->initState)
	{
		if (node->pstate != NULL)
		{
			/*
			 * Parallel mode: only participant 0 executes producer subplans
			 * to completion. Other participants trigger initialization only
			 * (e.g., ShareInputScan with discard_output=true returns NULL).
			 * All synchronize via barrier before reading the last subplan.
			 */
			ParallelSequenceState *ps = node->pstate;

			for (int no = 0; no < node->numSubplans - 1; no++)
			{
				if (node->my_participant_id == 0)
					completeSubplan(node->subplans[no]);
				else
					ExecProcNode(node->subplans[no]);

				BarrierArriveAndWait(&ps->sync_barrier,
									 WAIT_EVENT_PARALLEL_FINISH);
				CHECK_FOR_INTERRUPTS();
			}
		}
		else
		{
			/* Non-parallel: original sequential execution */
			for (int no = 0; no < node->numSubplans - 1; no++)
			{
				completeSubplan(node->subplans[no]);
				CHECK_FOR_INTERRUPTS();
			}
		}

		node->initState = false;
	}

	Assert(!node->initState);

	PlanState *lastPlan = node->subplans[node->numSubplans - 1];
	TupleTableSlot *result = ExecProcNode(lastPlan);

	/*
	 * Return the tuple as returned by the subplan as-is. We do
	 * NOT make use of the result slot that was set up in
	 * ExecInitSequence, because there's no reason to.
	 */
	return result;
}

void
ExecEndSequence(SequenceState *node)
{
	/* shutdown subplans */
	for(int no = 0; no < node->numSubplans; no++)
	{
		Assert(node->subplans[no] != NULL);
		ExecEndNode(node->subplans[no]);
	}
}

void
ExecReScanSequence(SequenceState *node)
{
	for (int i = 0; i < node->numSubplans; i++)
	{
		PlanState  *subnode = node->subplans[i];

		/*
		 * ExecReScan doesn't know about my subplans, so I have to do
		 * changed-parameter signaling myself.
		 */
		if (node->ps.chgParam != NULL)
		{
			UpdateChangedParamSet(subnode, node->ps.chgParam);
		}

		/*
		 * Always rescan the inputs immediately, to ensure we can pass down
		 * any outer tuple that might be used in index quals.
		 */
		ExecReScan(subnode);
	}

	node->initState = true;
}

void
ExecSquelchSequence(SequenceState *node, bool force)
{
	node->ps.squelched = true;
	for (int i = 0; i < node->numSubplans; i++)
		ExecSquelchNode(node->subplans[i], force);
}

/* ----------------------------------------------------------------
 *		Parallel Sequence Support
 * ----------------------------------------------------------------
 */

/*
 * ExecSequenceEstimate
 *		Estimate the amount of space needed for parallel coordination info.
 */
void
ExecSequenceEstimate(SequenceState *node, ParallelContext *pcxt)
{
	node->pstate_len = sizeof(ParallelSequenceState);
	shm_toc_estimate_chunk(&pcxt->estimator, node->pstate_len);
	shm_toc_estimate_keys(&pcxt->estimator, 1);
}

/*
 * ExecSequenceInitializeDSM
 *		Initialize parallel coordination info in DSM.
 */
void
ExecSequenceInitializeDSM(SequenceState *node, ParallelContext *pcxt)
{
	ParallelSequenceState *pstate;

	pstate = shm_toc_allocate(pcxt->toc, node->pstate_len);
	memset(pstate, 0, node->pstate_len);

	pstate->nworkers = pcxt->nworkers;
	BarrierInit(&pstate->sync_barrier, pstate->nworkers);

	shm_toc_insert(pcxt->toc, node->ps.plan->plan_node_id, pstate);
	node->pstate = pstate;
	node->my_participant_id = 0;  /* Leader is participant 0 */
}

/*
 * ExecSequenceReInitializeDSM
 *		Re-initialize parallel coordination info for a fresh scan.
 */
void
ExecSequenceReInitializeDSM(SequenceState *node, ParallelContext *pcxt)
{
	if (node->pstate == NULL)
		return;

	BarrierInit(&node->pstate->sync_barrier, node->pstate->nworkers);
}

/*
 * ExecSequenceInitializeWorker
 *		Initialize parallel coordination info in a worker process.
 */
void
ExecSequenceInitializeWorker(SequenceState *node, ParallelWorkerContext *pwcxt)
{
	node->pstate = shm_toc_lookup(pwcxt->toc,
								  node->ps.plan->plan_node_id, false);
	node->my_participant_id = pwcxt->worker_id;
}
