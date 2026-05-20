/*-------------------------------------------------------------------------
 *
 * nodeSequence.h
 *    header file for nodeSequence.c.
 *
 * Portions Copyright (c) 2012 - present, EMC/Greenplum
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 *
 *
 * IDENTIFICATION
 *	    src/include/executor/nodeSequence.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODESEQUENCE_H
#define NODESEQUENCE_H

#include "executor/tuptable.h"
#include "nodes/execnodes.h"
#include "access/parallel.h"

extern SequenceState *ExecInitSequence(Sequence *node, EState *estate, int eflags);
extern TupleTableSlot *ExecSequence(PlanState *pstate);
extern void ExecReScanSequence(SequenceState *node);
extern void ExecEndSequence(SequenceState *node);
extern void ExecSquelchSequence(SequenceState *node, bool force);

/* parallel support */
extern void ExecSequenceEstimate(SequenceState *node, ParallelContext *pcxt);
extern void ExecSequenceInitializeDSM(SequenceState *node, ParallelContext *pcxt);
extern void ExecSequenceReInitializeDSM(SequenceState *node, ParallelContext *pcxt);
extern void ExecSequenceInitializeWorker(SequenceState *node, ParallelWorkerContext *pwcxt);

#endif
