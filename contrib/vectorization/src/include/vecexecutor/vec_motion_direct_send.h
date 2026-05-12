/*-------------------------------------------------------------------------
 *
 * vec_motion_direct_send.h
 *	  Runtime-only channel for passing Sonic Motion Direct-Send hint from
 *	  a HASH Redistribute Motion node down to its child Agg node during
 *	  vectorized execution init.
 *
 *	  When the Motion's ExecInitVecMotion detects that:
 *	    * it is a MOTIONTYPE_HASH Motion;
 *	    * its child (modulo trivial Result/Project passthrough) is an Agg;
 *	    * Motion.hashExprs all resolve to group-by keys (not Aggrefs);
 *	    * all hash columns use a v1-supported PG type and reduce alg,
 *	  it registers a hint here keyed by the Agg's Plan pointer.  Later,
 *	  when BuildAggregatation constructs the Sonic Acero node, it looks
 *	  up the hint and passes the hash configuration down to Sonic so
 *	  Sonic can emit batches pre-bucketed by target segment.
 *
 *	  The hand-off is strictly depth-first: ExecInitVecMotion registers
 *	  the hint, then recurses into child ExecInit (which builds the
 *	  Arrow plan via BuildAggregatation), then returns.
 *
 *	  Modeled after vec_topk_bounds.h.  See sonic_motion_direct_send_design.md
 *	  for the full design.
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 * IDENTIFICATION
 *		contrib/vectorization/src/include/vecexecutor/vec_motion_direct_send.h
 *-------------------------------------------------------------------------
 */
#ifndef VEC_MOTION_DIRECT_SEND_H
#define VEC_MOTION_DIRECT_SEND_H

#include "postgres.h"
#include "nodes/execnodes.h"
#include "nodes/plannodes.h"

/*
 * Hint payload published by Motion ExecInit, consumed by BuildAggregatation.
 *
 * Pointer fields (hashExprs, hashFuncs) are shared references into the
 * source Motion plan node; they remain valid for the lifetime of the
 * EState (es_query_cxt).
 *
 * hash_grpcol_idx is owned by the HTAB — vec_motion_direct_send_set()
 * deep-copies the array into es_query_cxt so callers can pass stack
 * memory or transient allocations without worrying about lifetime.
 */
typedef struct VecMotionDirectSendHint
{
	int		numHashSegments;	/* Motion target segment count */
	List   *hashExprs;			/* shared ref to Motion->hashExprs (Plan-level) */
	Oid	   *hashFuncs;			/* shared ref to Motion->hashFuncs (hash function Oids) */
	int		numHashCols;		/* = list_length(hashExprs) */
	int		reduce_alg;			/* CdbHash.reducealg snapshot; v1 only JUMP_HASH */
	int	   *hash_grpcol_idx;	/* len = numHashCols.  hash_grpcol_idx[i] =
								 * the agg->grpColIdx[] index that Motion's
								 * i-th hash column maps to (also the Sonic
								 * key_field_ids_ index).  Order matches
								 * Motion.hashExprs order (cdbhash combine
								 * is not commutative). */
	char   *target_segment_col_name;
								/* Hidden int32 column that Sonic prepends
								 * to each emitted batch. Generated here
								 * (with random suffix) and validated for
								 * collision against the Agg's input/output
								 * schemas before registration. Deep-copied
								 * into es_query_cxt by _set(). */
} VecMotionDirectSendHint;

/*
 * Register a direct-send hint for an Agg plan node.  The hint payload is
 * shallow-copied; hash_grpcol_idx is deep-copied into es_query_cxt.
 *
 * Caller is responsible for having validated all gate conditions (Motion
 * type, child shape, type support, reduce_alg support, etc.) before
 * calling.  Registration here implies the optimization is safe to apply.
 *
 * Same-key (Plan*) double registration: the most recent set() wins
 * (HASH_ENTER semantics).  This should not happen in practice; flag at
 * DEBUG1 if encountered.
 */
extern void
vec_motion_direct_send_set(EState *estate, Plan *agg_plan,
						   const VecMotionDirectSendHint *hint);

/*
 * Look up a previously registered hint.  Returns a pointer to the in-HTAB
 * payload (caller MUST NOT free), or NULL if no entry exists.  The returned
 * pointer is valid for the remainder of the EState's es_query_cxt
 * lifetime.
 */
extern const VecMotionDirectSendHint *
vec_motion_direct_send_lookup(Plan *agg_plan);

/*
 * Gate: is `hashfunc_oid` one of the PG hash functions whose byte-level
 * semantics Sonic's cdbhash kernel reproduces? Returns true for the v1
 * supported set, false for everything else (including legacy hash, bpchar
 * hash, numeric/float hash, etc.).
 *
 * Supported (all map to bytewise hashint4 / hashint8 / hashtext on the
 * canonical row's raw value):
 *   hashint2       (449)  -> hashint4-equivalent  (int2)
 *   hashint4       (450)  -> hashint4             (int4, also date column)
 *   hashint8       (949)  -> hashint8             (int8)
 *   hashtext       (400)  -> hashtext             (text / varchar)
 *   timestamp_hash (2039) -> hashint8             (timestamp, timestamptz)
 *   time_hash      (1688) -> hashint8             (time)
 *
 * Used by nodeMotion.c::ExecInitVecMotion as the v1 type gate. Arrow no
 * longer needs the type information — it derives Sonic KeyType from the
 * input schema directly.
 *
 * ─────────────────────────────────────────────────────────────────────────
 * ⚠️  DO NOT add hashbpchar (1080) here without first implementing the
 *     corresponding Sonic-side bpchar handling.
 *
 *     PG bpchar semantics strip trailing blanks for BOTH hash AND equality:
 *       * hashbpchar (src/backend/utils/adt/varchar.c:981) calls bcTruelen()
 *         to scan backward past ' ' before hashing.
 *       * bpchareq / bpcharcmp do the same on both sides before memcmp.
 *
 *     Sonic's cdbhash kernel hashes raw payload bytes
 *     (cpp/src/arrow/compute/sonic/global_aggregator.cc::ComputeCdbHash
 *     STRING case), and Sonic's row HT compares string_t bytewise. With
 *     hashbpchar in this whitelist but Sonic unchanged, 'abc' and 'abc  '
 *     would hash to the same segment via the (PG-side) row engine but be
 *     placed in different HT entries inside Sonic → silent over-grouping
 *     of bpchar GROUP BY queries, NO crash, NO error.
 *
 *     The ClickBench `hits` schema's HitColor column is CHAR (= bpchar);
 *     this is not a hypothetical type. Any future commit that "expands
 *     string-type coverage" by adding 1080 here must also:
 *       (a) strip trailing blanks at Sonic ingest for bpchar key columns,
 *           OR
 *       (b) add a new KeyType variant with strip-then-hash + strip-then-
 *           compare semantics in row_layout / row_matcher / global_agg /
 *           local_agg switches.
 *     Same applies to other type-specific hash functions (hashnumeric,
 *     hashfloat4/8, network types, etc.).
 * ─────────────────────────────────────────────────────────────────────────
 */
static inline bool
vec_motion_direct_send_supports_hashfunc(Oid hashfunc_oid)
{
	switch (hashfunc_oid)
	{
		case 449:		/* hashint2 */
		case 450:		/* hashint4 — also covers date (date opclass uses hashint4) */
		case 949:		/* hashint8 */
		case 400:		/* hashtext — also covers varchar */
		case 2039:		/* timestamp_hash — timestamp + timestamptz share Oid */
		case 1688:		/* time_hash */
			return true;
		default:
			return false;
	}
}

#endif							/* VEC_MOTION_DIRECT_SEND_H */
