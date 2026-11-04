/*-------------------------------------------------------------------------
 *
 * execVecUtils.c
 *
 * Copyright (c) 2016-Present Hashdata, Inc. 
 *
 *
 * IDENTIFICATION
 *	  src/backend/vecexecutor/execVecUtils.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "executor/executor.h"
#include "nodes/nodeFuncs.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/tuplesort.h"

#include "utils/arrow.h"
#include "vecexecutor/executor.h"


void
ExecAssignScanProjectionInfoVec(ScanState *node)
{
    ExecAssignProjectionInfoVec(&node->ps,
                                node->ss_ScanTupleSlot->tts_tupleDescriptor);
}

/* FIXME:  expression needs refactoring  */
void
ExecAssignProjectionInfoVec(PlanState *planstate,
						 TupleDesc inputDesc)
{
}


/*
 * Print a vector slot.
 */
void
ExecVecSlotToLog(TupleTableSlot *slot, const char *label)
{
    VecTupleTableSlot *vslot = (VecTupleTableSlot *) slot;
	int i;
	GError *error = NULL;

	elog(INFO, "----------- %s start -------------", label);
	if (TupIsNull(slot))
	{
		elog(INFO, "Slot is empty.");
		return;
	}

	elog(INFO, "Slot flags: %d\n", slot->tts_flags);

	elog(INFO, "Schema: %s",
		garrow_schema_to_string(vslot->vec_schema.schema));

	for (i = 0; i < slot->tts_nvalid; i++)
	{
		int64 nrows;

		if (slot->tts_values[i])
		{
			gchar	   *str;

			nrows = garrow_array_get_length(
					(GArrowArray *)slot->tts_values[i]);
			str = garrow_array_to_string((GArrowArray *)slot->tts_values[i],
										 &error);
			elog(INFO, "Array[%d] of row %ld: %s\n", i, nrows,
				 str ? str : error->message);
			g_free(str);
			g_clear_error(&error);
		}
		else
			elog(INFO, "Array[%d]: NULL", i);
	}
	if (vslot->tts_recordbatch)
	{
		gchar	   *str;

		elog(INFO, "Batch Schema: %s",
			garrow_schema_to_string(garrow_record_batch_get_schema(
					GARROW_RECORD_BATCH(vslot->tts_recordbatch))));
		str = garrow_record_batch_to_string(
					GARROW_RECORD_BATCH(vslot->tts_recordbatch), &error);
		elog(INFO, "Batch: %s", str ? str : error->message);
		g_free(str);
		g_clear_error(&error);
		elog(INFO, "NumRows: %d", GetNumRows(slot));
	}
	else
		elog(INFO, "Batch: NULL");


	elog(INFO, "----------- %s end -------------", label);
}

struct ExtractVeccolumnContext
{
	bool	   *cols;
	AttrNumber	natts;
	bool		found;
	AttrNumber	extract_natts;
};

static bool
vec_extractcolumns_walker(Node *node, struct ExtractVeccolumnContext *ecCtx)
{
	if (node == NULL)
		return false;

	if (IsA(node, Var))
	{
		Var *var = (Var *)node;

		if (var->varattno > 0 && var->varattno <= ecCtx->natts)
		{
			if (ecCtx->cols[var->varattno -1])
				--ecCtx->extract_natts;
			else
				ecCtx->cols[var->varattno -1] = true;

			ecCtx->found = true;
			++ecCtx->extract_natts;
		}

		else if (var->varattno == 0)
		{
			for (AttrNumber attno = 0; attno < ecCtx->natts; attno++)
			{
				ecCtx->cols[attno] = true;
			}
			ecCtx->found = true;
			ecCtx->extract_natts = ecCtx->natts;
			return true;
		}

		return false;
	}

	return expression_tree_walker(node, vec_extractcolumns_walker, (void *)ecCtx);
}

static bool
vec_extractcolumns_from_node(Node *expr, bool *cols, AttrNumber natts, AttrNumber *extract_natts)
{
	struct ExtractVeccolumnContext	ecCtx;

	ecCtx.cols	= cols;
	ecCtx.natts = natts;
	ecCtx.found = false;
	ecCtx.extract_natts = 0;

	vec_extractcolumns_walker(expr, &ecCtx);
	(*extract_natts) += ecCtx.extract_natts;

	return  ecCtx.found;
}

/*
 *  BuildDescForScanExtract
 *
 *  Given a relation TupleDesc、targetlist、qual (list of nodes), build a extract TupleDesc.
 *
 */
TupleDesc
BuildDescForScanExtract(TupleDesc tupdesc, List *targetlist, List *qual, int *map)
{
	AttrNumber		natts = tupdesc->natts;
	AttrNumber		extract_natts = 0;
	TupleDesc		desc = NULL;
	bool		*cols;
	bool		found = false;
	int 		j = 0;

	/*
	 * A relation with no columns at all reaches here with natts == 0, and
	 * palloc0(0) hands back a chunk with no usable bytes.  The "nothing named
	 * a column, so scan the first one" fallback below then writes cols[0]
	 * straight past the end of it, which is the corruption reported in
	 * lightning issue #13:
	 *
	 *     WARNING:  problem in alloc set MessageContext:
	 *               detected write past chunk end
	 *
	 * It fired on every scan of such a relation, the ones that go on to be
	 * declined included -- the descriptor is built before anything looks at
	 * whether the plan can be vectorized.  So room for that one flag is
	 * reserved whatever natts says.
	 *
	 * What comes out is unchanged: a descriptor holding a single attribute
	 * that the loop below never fills, so its atttypid stays 0, and the
	 * callers' is_type_vectorable() declines it and the plan falls back to
	 * the row executor as it already did.
	 */
	cols = palloc0(Max(natts, 1) * sizeof(*cols));

	found |= vec_extractcolumns_from_node((Node *)targetlist, cols, natts, &extract_natts);
	found |= vec_extractcolumns_from_node((Node *)qual, cols, natts, &extract_natts);

	/*
	 * In some cases (for example, count(*)), targetlist and qual may be null,
	 * extractcolumns_walker will return immediately, so no columns are specified.
	 * We always scan the first column.
	 */
	if (!found)
	{
		cols[0] = true;
		extract_natts = 1;
	}
	desc = CreateTemplateTupleDesc(extract_natts);
	/* initialized */
	desc->tdtypeid = tupdesc->tdtypeid;
	desc->tdtypmod = tupdesc->tdtypmod;
	/*
	 * Deliberately do NOT copy tdrefcount from the source descriptor.  This
	 * descriptor is freshly palloc'd in the per-query context, so it must
	 * stay non-reference-counted (-1), exactly as CreateTemplateTupleDesc
	 * left it.  Copying the relcache descriptor's tdrefcount (1) used to make
	 * PinTupleDesc() register this transient descriptor with the current
	 * ResourceOwner: the scan slots built from it in
	 * ExecInitScanTupleSlotVec() then left entries in owner->tupdescarr.  On
	 * a normal end-of-query ExecResetTupleTable() unpinned them again, but on
	 * an abort AtAbort_Portals() destroys the portal's child contexts (and
	 * with them this descriptor) *before* ResourceOwnerRelease() walks
	 * tupdescarr, so DecrTupleDescRefCount() dereferenced freed memory and
	 * the backend crashed while cancelling a vectorized query.
	 */
	desc->constr = tupdesc->constr;
	for (AttrNumber i = 0; i < natts; i++)
	{
		if (cols[i])
		{
			TupleDescCopyEntry(desc, j+1, tupdesc, i+1);
			if (map)
				map[i] = j++;
			if (!found || j == desc->natts)
				break;
		}
	}
	return desc;
}

/* Initialize scan slot as virtual slot for abi RecordBatch.
 * And initialize another vector slot (vscanslot) for imported RecordBatch*/
void
ExecInitScanTupleSlotVec(EState *estate, ScanState *scanstate, TupleTableSlot **vscanslot,
		TupleDesc tupledesc, int *columnmap)
{
	TupleDesc origindesc, ctiddesc;

	origindesc = BuildDescForScanExtract(tupledesc,
										 scanstate->ps.plan->targetlist,
										 scanstate->ps.plan->qual, columnmap);

	if (has_ctid((Expr *)scanstate->ps.plan->targetlist, NULL))
		ctiddesc = CreateCtidTupleDesc(origindesc);
	else
		ctiddesc = origindesc;

	ExecInitScanTupleSlot(estate, scanstate,
						  ctiddesc,
						  &TTSOpsVirtual);
	*vscanslot = ExecAllocTableSlot(&estate->es_tupleTable, ctiddesc, &TTSOpsVecTuple);
}

/**
 *
 * Determine whether there is a ctid in the targetList.
 */
bool
has_ctid(Expr *expr, void *context)
{
 	if (expr == NULL)
 		return false;
	if (nodeTag(expr) == T_Var)
	{
		Var *var = (Var *) expr;
		if (var->varattno == SelfItemPointerAttributeNumber)
			return true;
	}
	return expression_tree_walker((Node *)expr, has_ctid, context);
}
