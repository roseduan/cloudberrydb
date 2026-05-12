/*-------------------------------------------------------------------------
 *
 * vec_motion_direct_send.c
 *	  Module-private hash table keyed by Agg Plan pointer, carrying the
 *	  Sonic Motion Direct-Send hint from Motion's ExecInit down to the
 *	  Agg's Arrow-plan build (BuildAggregatation).
 *
 *	  Design notes
 *	  ------------
 *	  Lifecycle is identical to vec_topk_bounds.c:
 *	    * The HTAB lives in the first-caller EState's es_query_cxt and
 *	      is referenced by a single module-level pointer.
 *	    * A MemoryContextResetCallback registered on that cxt nulls the
 *	      pointer when the cxt is freed, so the next query sees NULL
 *	      and lazily creates its own hash.
 *	    * Nested queries (e.g. SPI inside a vectorized query) reuse the
 *	      outer query's hash; entries are keyed by Plan pointer so they
 *	      do not collide across nesting levels.
 *
 *	  Rescan safety: callers must skip registration when EXEC_FLAG_REWIND
 *	  is set (see ExecInitVecMotion).  The Arrow plan is built only once
 *	  during init and cannot be rebuilt for a different hint on rescan.
 *
 *	  Why hash_grpcol_idx is deep-copied
 *	  ----------------------------------
 *	  The caller (ExecInitVecMotion) may palloc the index array in any
 *	  context — possibly one that is shorter-lived than es_query_cxt
 *	  (e.g. ExprContext, motion init scratch).  We copy it into the HTAB
 *	  context so BuildAggregatation can safely read it later, regardless
 *	  of which context the original allocation lived in.
 *
 *	  hashExprs and hashFuncs are NOT copied: they point into the Plan
 *	  tree which lives at least as long as es_query_cxt.
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 * IDENTIFICATION
 *		contrib/vectorization/src/backend/vecexecutor/vec_motion_direct_send.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/hsearch.h"
#include "utils/memutils.h"

#include "vecexecutor/vec_motion_direct_send.h"

typedef struct VecMotionDirectSendEntry
{
	Plan				   *key;	/* HASH_BLOBS on raw pointer bytes */
	VecMotionDirectSendHint	hint;
} VecMotionDirectSendEntry;

static HTAB *vec_motion_direct_send = NULL;

static void
vec_motion_direct_send_reset_cb(void *arg)
{
	/*
	 * The HTAB and any payload arrays we allocated are sub-allocations of
	 * the cxt that is about to be freed, so we only need to drop our
	 * module-level reference.  The next call to vec_motion_direct_send_set()
	 * will lazily recreate the hash.
	 */
	vec_motion_direct_send = NULL;
}

static HTAB *
ensure_htab(EState *estate)
{
	HASHCTL					 ctl;
	MemoryContextCallback	*cb;

	if (vec_motion_direct_send != NULL)
		return vec_motion_direct_send;

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Plan *);
	ctl.entrysize = sizeof(VecMotionDirectSendEntry);
	ctl.hcxt = estate->es_query_cxt;

	vec_motion_direct_send = hash_create("vec_motion_direct_send", 8, &ctl,
										 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	cb = (MemoryContextCallback *) MemoryContextAlloc(estate->es_query_cxt,
													  sizeof(*cb));
	cb->func = vec_motion_direct_send_reset_cb;
	cb->arg = NULL;
	MemoryContextRegisterResetCallback(estate->es_query_cxt, cb);

	return vec_motion_direct_send;
}

void
vec_motion_direct_send_set(EState *estate, Plan *agg_plan,
						   const VecMotionDirectSendHint *hint)
{
	VecMotionDirectSendEntry   *e;
	bool						found;
	MemoryContext				oldcxt;
	int						   *idx_copy;

	Assert(estate != NULL);
	Assert(agg_plan != NULL);
	Assert(hint != NULL);
	Assert(hint->numHashSegments > 0);
	Assert(hint->numHashCols > 0);
	Assert(hint->hash_grpcol_idx != NULL);

	e = (VecMotionDirectSendEntry *) hash_search(ensure_htab(estate),
												 &agg_plan, HASH_ENTER, &found);
	if (found)
	{
		/*
		 * Should not happen in practice — each Agg is at most one Motion's
		 * child.  Log at DEBUG1 and overwrite (last-writer-wins).  Free
		 * the previously-copied idx array first to avoid leaking it within
		 * the query context.
		 */
		ereport(DEBUG1,
				(errmsg("vec_motion_direct_send: duplicate hint for plan %p; overwriting",
						(void *) agg_plan)));
		if (e->hint.hash_grpcol_idx != NULL)
			pfree(e->hint.hash_grpcol_idx);
		if (e->hint.target_segment_col_name != NULL)
			pfree(e->hint.target_segment_col_name);
	}

	/*
	 * Deep-copy hash_grpcol_idx and target_segment_col_name into
	 * es_query_cxt.  hashExprs and hashFuncs are shared refs (no copy
	 * needed; Plan tree lifetime covers es_query_cxt).
	 */
	oldcxt = MemoryContextSwitchTo(estate->es_query_cxt);
	idx_copy = (int *) palloc(sizeof(int) * hint->numHashCols);
	memcpy(idx_copy, hint->hash_grpcol_idx, sizeof(int) * hint->numHashCols);
	char *name_copy = (hint->target_segment_col_name != NULL)
		? pstrdup(hint->target_segment_col_name) : NULL;
	MemoryContextSwitchTo(oldcxt);

	e->hint.numHashSegments         = hint->numHashSegments;
	e->hint.hashExprs               = hint->hashExprs;
	e->hint.hashFuncs               = hint->hashFuncs;
	e->hint.numHashCols             = hint->numHashCols;
	e->hint.reduce_alg              = hint->reduce_alg;
	e->hint.hash_grpcol_idx         = idx_copy;
	e->hint.target_segment_col_name = name_copy;
}

const VecMotionDirectSendHint *
vec_motion_direct_send_lookup(Plan *agg_plan)
{
	VecMotionDirectSendEntry *e;

	if (vec_motion_direct_send == NULL)
		return NULL;

	e = (VecMotionDirectSendEntry *) hash_search(vec_motion_direct_send,
												 &agg_plan, HASH_FIND, NULL);
	return e ? &e->hint : NULL;
}
