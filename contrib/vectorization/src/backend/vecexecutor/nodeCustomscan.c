/*-------------------------------------------------------------------------
 *
 * nodeCustomscan.c
 *	  Routines to support vectorized scans of CustomScan providers.
 *
 * A CustomScan provider that is vectorization-aware returns, from its
 * ExecCustomScan callback, a *virtual* TupleTableSlot whose data is an ABI
 * ArrowRecordBatch* (the C Data Interface), exactly like a vectorized FDW's
 * IterateForeignScan.  This node drives that callback and imports each batch
 * into the vector engine via ExecVecScan/ExecImportABISlot -- mirroring
 * nodeForeignscan.c.
 *
 * The one structural difference from ForeignScan: a CustomScan's state is
 * allocated by the provider (CreateCustomScanState), so we cannot subclass it
 * into a "VecCustomScanState" the way ExecInitVecForeignScan subclasses
 * ForeignScanState.  Instead the engine keeps a VecCustomScanState alongside
 * the provider's CustomScanState and finds it through a small pointer-keyed
 * map.  This keeps the engine generic -- it never needs the provider's struct
 * layout.
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "executor/nodeCustom.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/rel.h"

#include "vecexecutor/executor.h"
#include "vecexecutor/execslot.h"
#include "vecexecutor/execnodes.h"
#include "vecexecutor/nodeCustomscan.h"

/*
 * CustomScanState* -> VecCustomScanState* map.
 *
 * Init and End of a given scan run in the same executor invocation, and the
 * provider's CustomScanState is freshly palloc'd per query, so entries never
 * outlive their key.  The table lives in TopMemoryContext; entries are removed
 * at End.
 */
static HTAB *vec_customscan_map = NULL;

typedef struct VecCSMapEntry
{
	CustomScanState *key;		/* hash key: the provider state pointer */
	VecCustomScanState *vcss;
} VecCSMapEntry;

static void
vec_cs_map_init(void)
{
	HASHCTL		ctl;

	if (vec_customscan_map != NULL)
		return;

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(CustomScanState *);
	ctl.entrysize = sizeof(VecCSMapEntry);
	ctl.hcxt = TopMemoryContext;
	vec_customscan_map = hash_create("vec customscan map", 32, &ctl,
									 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

static void
vec_cs_map_set(CustomScanState *css, VecCustomScanState *vcss)
{
	VecCSMapEntry *entry;
	bool		found;

	vec_cs_map_init();
	entry = (VecCSMapEntry *) hash_search(vec_customscan_map, &css,
										  HASH_ENTER, &found);
	entry->vcss = vcss;
}

static VecCustomScanState *
vec_cs_map_get(CustomScanState *css)
{
	VecCSMapEntry *entry;

	if (vec_customscan_map == NULL)
		return NULL;
	entry = (VecCSMapEntry *) hash_search(vec_customscan_map, &css,
										  HASH_FIND, NULL);
	return entry ? entry->vcss : NULL;
}

static void
vec_cs_map_del(CustomScanState *css)
{
	if (vec_customscan_map == NULL)
		return;
	hash_search(vec_customscan_map, &css, HASH_REMOVE, NULL);
}

/*
 * Public accessor: recover the vec state for a CustomScanState (e.g. the arrow
 * plan builder in execMain.c needs its input schema / column map).
 */
VecCustomScanState *
GetVecCustomScanState(CustomScanState *node)
{
	return vec_cs_map_get(node);
}

/* ----------------------------------------------------------------
 *		CustomNext
 *
 *		Fetch the next ABI-batch slot from the provider.  In vectorized
 *		mode the provider's ExecCustomScan returns a virtual slot whose
 *		data is a C-Data-Interface ArrowRecordBatch*, just like a
 *		vectorized FDW's IterateForeignScan.
 * ----------------------------------------------------------------
 */
TupleTableSlot *
VecCustomNext(CustomScanState *node)
{
	TupleTableSlot *slot;

	slot = node->methods->ExecCustomScan(node);
	if (slot == NULL || TupIsNull(slot))
		return NULL;

	Assert(TTS_IS_VIRTUAL(slot));
	return slot;
}

bool
VecCustomRecheck(CustomScanState *node, TupleTableSlot *slot)
{
	/* Vector recheck is not implemented; see nodeForeignscan.c. */
	return true;
}

static TupleTableSlot *
ExecVecCustomScan(PlanState *pstate)
{
	CustomScanState *node = castNode(CustomScanState, pstate);
	VecCustomScanState *vcss = vec_cs_map_get(node);

	Assert(vcss != NULL);

	return ExecVecScan(&node->ss,
					   &vcss->vestate,
					   vcss->vscanslot,
					   (ExecScanAccessMtd) VecCustomNext,
					   (ExecScanRecheckMtd) VecCustomRecheck);
}

/* ----------------------------------------------------------------
 *		ExecInitVecCustomScan
 *
 *		Replicates the parts of core ExecInitCustomScan we need, but wires
 *		the scan slot for ABI batches and the result slot as a vec slot,
 *		and hands EXEC_FLAG_VECTOR to the provider's BeginCustomScan so it
 *		produces RecordBatches instead of rows.
 * ----------------------------------------------------------------
 */
CustomScanState *
ExecInitVecCustomScan(CustomScan *cscan, EState *estate, int eflags)
{
	CustomScanState *css;
	VecCustomScanState *vcss;
	Relation	currentRelation = NULL;
	Index		scanrelid = cscan->scan.scanrelid;
	Index		tlistvarno;
	TupleDesc	scan_tupdesc;
	g_autoptr(GArrowSchema) schema = NULL;

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	/*
	 * The provider owns the state allocation (it may be a subclass carrying
	 * its own per-scan fields).
	 */
	css = castNode(CustomScanState, cscan->methods->CreateCustomScanState(cscan));
	css->flags = cscan->flags;
	css->ss.ps.plan = &cscan->scan.plan;
	css->ss.ps.state = estate;
	css->ss.ps.ExecProcNode = ExecVecCustomScan;

	/* create expression context for node */
	ExecAssignExprContext(estate, &css->ss.ps);

	/* open the scan relation, if any */
	if (scanrelid > 0)
	{
		currentRelation = ExecOpenScanRelation(estate, scanrelid, eflags);
		css->ss.ss_currentRelation = currentRelation;
	}

	vcss = (VecCustomScanState *) palloc0(sizeof(VecCustomScanState));
	vcss->csstate = css;

	/*
	 * Determine the scan tuple type.  If the provider supplied a scan
	 * targetlist use that (INDEX_VAR); else use the base relation's rowtype
	 * (scanrelid).  The iceberg custom scan leaves custom_scan_tlist NIL.
	 */
	if (cscan->custom_scan_tlist != NIL || currentRelation == NULL)
	{
		scan_tupdesc = ExecTypeFromTL(cscan->custom_scan_tlist);
		tlistvarno = INDEX_VAR;
	}
	else
	{
		scan_tupdesc = CreateTupleDescCopy(RelationGetDescr(currentRelation));
		tlistvarno = scanrelid;
	}

	vcss->columnmap = palloc0(scan_tupdesc->natts * sizeof(int));
	/* scan slot = TTSOpsVirtual (ABI batch); vscanslot = TTSOpsVecTuple. */
	ExecInitScanTupleSlotVec(estate, &css->ss, &vcss->vscanslot, scan_tupdesc,
							 vcss->columnmap);
	ExecInitResultTupleSlotTL(&css->ss.ps, &TTSOpsVecTuple);
	schema = TupDescToSchema(css->ss.ss_ScanTupleSlot->tts_tupleDescriptor);
	garrow_store_ptr(vcss->schema, schema);

	/* Don't know what the provider might return. */
	css->ss.ps.scanopsfixed = false;
	css->ss.ps.scanopsset = true;

	/* Initialize result slot, type and projection. */
	ExecAssignScanProjectionInfoWithVarno(&css->ss, tlistvarno);

	/* initialize child expressions */
	css->ss.ps.qual =
		ExecInitQual(cscan->scan.plan.qual, (PlanState *) css);

	/* Register before BeginCustomScan so any callback can look us up. */
	vec_cs_map_set(css, vcss);

	/*
	 * The provider applies its own initialization.  eflags carries
	 * EXEC_FLAG_VECTOR, which the provider must honour by producing ABI
	 * RecordBatches (see the iceberg AM's BeginCustomScan).
	 */
	css->methods->BeginCustomScan(css, estate, eflags);

	PostBuildVecPlan((PlanState *) css, &vcss->vestate);
	return css;
}

/* ----------------------------------------------------------------
 *		ExecEndVecCustomScan
 * ----------------------------------------------------------------
 */
void
ExecEndVecCustomScan(CustomScanState *node)
{
	VecCustomScanState *vcss = vec_cs_map_get(node);

	if (vcss != NULL && vcss->schema != NULL)
		ARROW_FREE(GArrowSchema, &vcss->schema);

	/* Let the provider shut down its reader. */
	node->methods->EndCustomScan(node);

	if (vcss != NULL)
	{
		FreeVecExecuteState(&vcss->vestate);
		if (vcss->columnmap != NULL)
			pfree(vcss->columnmap);
	}

	/* Free the exprcontext */
	ExecFreeExprContext(&node->ss.ps);

	/* clean out the tuple table */
	if (node->ss.ps.ps_ResultTupleSlot)
		ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
	if (node->ss.ss_ScanTupleSlot)
		ExecClearTuple(node->ss.ss_ScanTupleSlot);

	vec_cs_map_del(node);
}

/* ----------------------------------------------------------------
 *		ExecReScanVecCustomScan
 * ----------------------------------------------------------------
 */
void
ExecReScanVecCustomScan(CustomScanState *node)
{
	Assert(node->methods->ReScanCustomScan != NULL);
	node->methods->ReScanCustomScan(node);
	ExecScanReScan(&node->ss);
}

void
ExecSquelchVecCustomScan(CustomScanState *node)
{
	/* Nothing engine-specific to squelch; provider frees at End. */
}
