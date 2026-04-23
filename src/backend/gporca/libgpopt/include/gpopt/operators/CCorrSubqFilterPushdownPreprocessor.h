//---------------------------------------------------------------------------
//	HashData Lightning Database
//
//	@filename:
//		CCorrSubqFilterPushdownPreprocessor.h
//
//	@doc:
//		Preprocessor that rewrites Q17-shaped correlated scalar subqueries
//		with aggregate into window-function form (StarRocks WinMagic rule).
//
//		Target pattern in the scalar predicate of an NAryJoin:
//		  CScalarCmp(<, CScalarIdent(outer_expr_col),
//		             CScalarSubquery(
//		                 CLogicalProject(
//		                     CLogicalGbAgg(grouping=[],
//		                         CLogicalSelect(Get, correlation_pred),
//		                         agg_proj_list),
//		                     final_proj_list)))
//
//		Where correlation_pred is `inner_col = outer_col` (or commutative
//		equivalent) matching an outer NAryJoin join conjunct.
//
//		Rewrite to (WinMagic):
//		  CLogicalSelect(filter: outer_expr_col OP subquery_out_col,
//		    CLogicalSequenceProject(Window
//		      partition_by=outer_col,
//		      proj_list=rebuilt_from_agg_and_final_proj,
//		      NAryJoin(rels..., scalar_pred_without_subq_conjunct)))
//
//		Gated by GUC optimizer_enable_scalar_subq_filter_pushdown.
//
//	Portions Copyright (c) 2026, HashData Technology Limited.
//---------------------------------------------------------------------------
#ifndef GPOPT_CCorrSubqFilterPushdownPreprocessor_H
#define GPOPT_CCorrSubqFilterPushdownPreprocessor_H

#include "gpos/base.h"

#include "gpopt/base/CColRef.h"
#include "gpopt/operators/CExpression.h"
#include "naucrates/md/IMDId.h"

namespace gpopt
{
// Result of parsing a Q17-shaped subquery site inside an NAryJoin.
// Populated by FMatchAndParseQ17Site when the rewrite is applicable;
// consumed by the tree builder to construct the WinMagic plan.
struct SQ17ParseResult
{
	// The CScalarCmp conjunct in the NAryJoin scalar pred that holds
	// the subquery (e.g. `l_quantity < CScalarSubquery(...)`).  When
	// rewriting, this conjunct is removed from NAryJoin's pred and
	// repositioned above the new SequenceProject as a Select.
	CExpression *pexprOuterCmp = nullptr;

	// The CScalarSubquery node itself (child of pexprOuterCmp).
	CExpression *pexprSubquery = nullptr;

	// The body below CScalarSubquery:
	// CLogicalProject(CLogicalGbAgg(CLogicalSelect(Get, corr), agg_proj),
	//                 final_proj).
	CExpression *pexprSubqBody = nullptr;	   // the Project
	CExpression *pexprSubqGbAgg = nullptr;	   // the GbAgg
	CExpression *pexprSubqSelect = nullptr;	   // the Select over Get
	CExpression *pexprSubqCorrPred = nullptr;  // the correlation predicate
	CExpression *pexprSubqInnerGet = nullptr;  // the inner relational child
	CExpression *pexprAggProjList = nullptr;   // GbAgg's project list
	CExpression *pexprFinalProjList = nullptr; // Project's project list

	// CColRef produced by the scalar subquery (the final_proj output);
	// this is what the outer Cmp references and what the Window output
	// must map back to.
	const CColRef *pcrSubqOutput = nullptr;

	// Correlation: inner_col (bound inside subquery, from Get) and
	// outer_col (outer reference into NAryJoin's scope).  The outer
	// NAryJoin must contain `outer_col = some_col` as a join conjunct
	// where `some_col` can serve as the Window PARTITION BY key.
	const CColRef *pcrInnerCorr = nullptr;
	const CColRef *pcrOuterCorr = nullptr;

	// The column in the outer NAryJoin's scope that is equi-joined
	// to pcrOuterCorr by one of the NAryJoin conjuncts (so PARTITION
	// BY this column is equivalent to PARTITION BY pcrOuterCorr).
	// This is what WinMagic uses as the Window partition key.
	const CColRef *pcrPartitionBy = nullptr;

	// The outer relational child of NAryJoin that matches
	// pexprSubqInnerGet's underlying table (same MDId).  Used to build
	// a full inner-col → outer-col mapping so the agg function inside
	// the subquery can be rewritten to reference outer columns.
	CExpression *pexprOuterMatchingGet = nullptr;

	// inner_col_id → outer CColRef mapping, keyed by the CColRef::Id()
	// of inner Get's output columns.  Owned by caller (allocated in
	// PexprPreprocess memory pool); released at end of preprocessing.
	UlongToColRefMap *pcolmap = nullptr;
};

class CCorrSubqFilterPushdownPreprocessor
{
public:
	CCorrSubqFilterPushdownPreprocessor(
		const CCorrSubqFilterPushdownPreprocessor &) = delete;

	// Main driver.  Recursively descends and rewrites Q17-shaped NAryJoin
	// sites.  GUC-gated.
	static CExpression *PexprPreprocess(CMemoryPool *mp, CExpression *pexpr);

private:
	// Detect and parse a Q17-shaped subquery site inside pexprNAryJoin's
	// scalar predicate.  Returns true if a Q17 pattern is found *and*
	// the WinMagic preconditions (correlation pred matches an outer
	// join conjunct) are satisfied.  On success, fills *pResult with
	// the components needed to build the rewritten tree.
	static BOOL FMatchAndParseQ17Site(CMemoryPool *mp,
									  CExpression *pexprNAryJoin,
									  SQ17ParseResult *pResult);
};
}  // namespace gpopt

#endif	// !GPOPT_CCorrSubqFilterPushdownPreprocessor_H
