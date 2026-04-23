//---------------------------------------------------------------------------
//	HashData Lightning Database
//
//	@filename:
//		CCorrSubqFilterPushdownPreprocessor.cpp
//
//	@doc:
//		WinMagic-style preprocessor for correlated scalar-subquery-with-
//		aggregate.  See header for pattern and rewrite target.
//
//		Detects the Q17-shaped site, parses it into SQ17ParseResult, then
//		constructs a CLogicalSelect + CLogicalSequenceProject (window) tree
//		from the parse result and splices it back into the NAryJoin.
//
//	Portions Copyright (c) 2026, HashData Technology Limited.
//---------------------------------------------------------------------------

#include "gpopt/operators/CCorrSubqFilterPushdownPreprocessor.h"

#include "gpos/base.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CDistributionSpecHashed.h"
#include "gpopt/base/COrderSpec.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/base/CWindowFrame.h"
#include "gpopt/metadata/CColumnDescriptor.h"
#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/CLogicalGbAgg.h"
#include "gpopt/operators/CLogicalGet.h"
#include "gpopt/operators/CLogicalNAryJoin.h"
#include "gpopt/operators/CLogicalProject.h"
#include "gpopt/operators/CLogicalSelect.h"
#include "gpopt/operators/CLogicalSequenceProject.h"
#include "gpopt/operators/CScalarAggFunc.h"
#include "gpopt/operators/CScalarBoolOp.h"
#include "gpopt/operators/CScalarCmp.h"
#include "gpopt/operators/CScalarIdent.h"
#include "gpopt/operators/CScalarProjectList.h"
#include "gpopt/operators/CScalarSubquery.h"
#include "gpopt/operators/CScalarWindowFunc.h"
#include "naucrates/traceflags/traceflags.h"

using namespace gpopt;

namespace
{
// Extract conjuncts from a scalar expression tree rooted at a
// CScalarBoolOp(AND), or return [pexpr] if it is a single predicate.
// Returned vector does not take ownership.
static void
ExtractConjuncts(CExpression *pexpr, std::vector<CExpression *> &out)
{
	if (nullptr == pexpr) return;
	COperator *pop = pexpr->Pop();
	if (COperator::EopScalarBoolOp == pop->Eopid() &&
		CScalarBoolOp::PopConvert(pop)->Eboolop() == CScalarBoolOp::EboolopAnd)
	{
		const ULONG arity = pexpr->Arity();
		for (ULONG i = 0; i < arity; i++)
		{
			ExtractConjuncts((*pexpr)[i], out);
		}
	}
	else
	{
		out.push_back(pexpr);
	}
}

// Given a conjunct and a target outer CColRef, check if it is an
// equality predicate of the form `outer_col = X` (or commutative
// `X = outer_col`) and return X (any CColRef) if so, else nullptr.
static const CColRef *
PcrEqualPeer(CExpression *pexprConj, const CColRef *pcrOuter)
{
	if (nullptr == pexprConj || nullptr == pcrOuter) return nullptr;
	COperator *pop = pexprConj->Pop();
	if (COperator::EopScalarCmp != pop->Eopid()) return nullptr;
	CScalarCmp *popCmp = CScalarCmp::PopConvert(pop);
	if (IMDType::EcmptEq != popCmp->ParseCmpType()) return nullptr;
	if (2 != pexprConj->Arity()) return nullptr;

	CExpression *pexprL = (*pexprConj)[0];
	CExpression *pexprR = (*pexprConj)[1];
	if (COperator::EopScalarIdent != pexprL->Pop()->Eopid() ||
		COperator::EopScalarIdent != pexprR->Pop()->Eopid())
	{
		return nullptr;
	}
	const CColRef *pcrL = CScalarIdent::PopConvert(pexprL->Pop())->Pcr();
	const CColRef *pcrR = CScalarIdent::PopConvert(pexprR->Pop())->Pcr();
	if (pcrL == pcrOuter) return pcrR;
	if (pcrR == pcrOuter) return pcrL;
	return nullptr;
}

// Parse the correlation predicate inside a Q17-shaped subquery.
// Expected: single CScalarCmp(=, inner_ident, outer_ident) or
// commutative.  innerOutputCols contains the CColRef set output by
// the Get below Select.  Returns true + fills *ppcrInner, *ppcrOuter.
static BOOL
FParseCorrelationPred(CExpression *pexprCorrPred,
					  CColRefSet *pcrsInnerOutput,
					  const CColRef **ppcrInner,
					  const CColRef **ppcrOuter)
{
	if (nullptr == pexprCorrPred) return false;
	if (COperator::EopScalarCmp != pexprCorrPred->Pop()->Eopid())
	{
		return false;
	}
	CScalarCmp *popCmp = CScalarCmp::PopConvert(pexprCorrPred->Pop());
	if (IMDType::EcmptEq != popCmp->ParseCmpType()) return false;
	if (2 != pexprCorrPred->Arity()) return false;

	CExpression *pexprL = (*pexprCorrPred)[0];
	CExpression *pexprR = (*pexprCorrPred)[1];
	if (COperator::EopScalarIdent != pexprL->Pop()->Eopid() ||
		COperator::EopScalarIdent != pexprR->Pop()->Eopid())
	{
		return false;
	}
	const CColRef *pcrL = CScalarIdent::PopConvert(pexprL->Pop())->Pcr();
	const CColRef *pcrR = CScalarIdent::PopConvert(pexprR->Pop())->Pcr();

	// Inner column is in innerOutputCols; outer column is an outer ref.
	if (pcrsInnerOutput->FMember(pcrL) && !pcrsInnerOutput->FMember(pcrR))
	{
		*ppcrInner = pcrL;
		*ppcrOuter = pcrR;
		return true;
	}
	if (pcrsInnerOutput->FMember(pcrR) && !pcrsInnerOutput->FMember(pcrL))
	{
		*ppcrInner = pcrR;
		*ppcrOuter = pcrL;
		return true;
	}
	return false;
}

// Locate a CScalarCmp conjunct in scalar_pred whose one operand is a
// CScalarSubquery whose body matches the Q17 shape (Project over GbAgg
// over Select over Get, GbAgg empty-grouping).  Returns nullptr if no
// such conjunct exists.  Fills the body pointers in *pResult on success.
static CExpression *
PexprFindQ17Conjunct(CExpression *pexprScalarPred, SQ17ParseResult *pResult)
{
	std::vector<CExpression *> conjuncts;
	ExtractConjuncts(pexprScalarPred, conjuncts);

	for (CExpression *pexprConj : conjuncts)
	{
		if (nullptr == pexprConj) continue;
		if (COperator::EopScalarCmp != pexprConj->Pop()->Eopid()) continue;
		if (2 != pexprConj->Arity()) continue;

		// Find a CScalarSubquery child of this Cmp.
		CExpression *pexprSubq = nullptr;
		for (ULONG i = 0; i < 2; i++)
		{
			CExpression *pexprSide = (*pexprConj)[i];
			if (COperator::EopScalarSubquery == pexprSide->Pop()->Eopid() &&
				1 == pexprSide->Arity())
			{
				pexprSubq = pexprSide;
				break;
			}
		}
		if (nullptr == pexprSubq) continue;

		// Verify body shape: Project -> GbAgg(empty) -> Select -> Get.
		CExpression *pexprBody = (*pexprSubq)[0];
		if (nullptr == pexprBody ||
			COperator::EopLogicalProject != pexprBody->Pop()->Eopid() ||
			2 != pexprBody->Arity())
		{
			continue;
		}
		CExpression *pexprGbAgg = (*pexprBody)[0];
		CExpression *pexprFinalProj = (*pexprBody)[1];
		if (COperator::EopLogicalGbAgg != pexprGbAgg->Pop()->Eopid() ||
			2 != pexprGbAgg->Arity())
		{
			continue;
		}
		CLogicalGbAgg *popAgg = CLogicalGbAgg::PopConvert(pexprGbAgg->Pop());
		if (!popAgg->FGlobal() || 0 != popAgg->Pdrgpcr()->Size()) continue;

		CExpression *pexprAggChild = (*pexprGbAgg)[0];
		CExpression *pexprAggProj = (*pexprGbAgg)[1];
		if (nullptr == pexprAggChild ||
			COperator::EopLogicalSelect != pexprAggChild->Pop()->Eopid() ||
			2 != pexprAggChild->Arity())
		{
			continue;
		}
		// The rewritten-tree builder emits exactly one COrderSpec and one
		// CWindowFrame for the SequenceProject, so only a single-aggregate
		// subquery (Q17's one avg()) can be rewritten safely.  A GbAgg with
		// two or more aggregates would yield N window functions against a
		// single order/frame spec and abort debug builds; skip those.
		if (nullptr == pexprAggProj ||
			COperator::EopScalarProjectList != pexprAggProj->Pop()->Eopid() ||
			1 != pexprAggProj->Arity())
		{
			continue;
		}
		// WinMagic soundness: the rewritten window runs over the full join
		// result, where each inner-table row is duplicated once per matching
		// outer row (multiplicity M).  AVG/MIN/MAX are insensitive to this
		// duplication (the M factor cancels or is irrelevant), but SUM and
		// COUNT would be inflated M-fold and silently return wrong results
		// when the matched join is not multiplicity-preserving (e.g. a
		// non-unique correlation key).  Restrict the rewrite to the
		// duplication-insensitive aggregates.
		{
			CExpression *pexprAggPrjElem = (*pexprAggProj)[0];
			if (nullptr == pexprAggPrjElem || 1 != pexprAggPrjElem->Arity() ||
				COperator::EopScalarAggFunc !=
					(*pexprAggPrjElem)[0]->Pop()->Eopid())
			{
				continue;
			}
			CScalarAggFunc *popAggFunc =
				CScalarAggFunc::PopConvert((*pexprAggPrjElem)[0]->Pop());
			const CWStringConst *pstrAgg = popAggFunc->PstrAggFunc();
			CWStringConst strAvg(GPOS_WSZ_LIT("avg"));
			CWStringConst strMin(GPOS_WSZ_LIT("min"));
			CWStringConst strMax(GPOS_WSZ_LIT("max"));
			// DISTINCT aggregates must also be skipped: the builder copies
			// the distinct flag into the CScalarWindowFunc, and PostgreSQL
			// does not implement DISTINCT for window function calls, so the
			// rewritten plan could never execute (and avg(DISTINCT) over the
			// duplicated join partition is not equivalent anyway).
			if ((!pstrAgg->Equals(&strAvg) && !pstrAgg->Equals(&strMin) &&
				 !pstrAgg->Equals(&strMax)) ||
				popAggFunc->IsDistinct())
			{
				continue;
			}
		}
		if (!pexprAggChild->HasOuterRefs()) continue;

		CExpression *pexprInnerGet = (*pexprAggChild)[0];
		CExpression *pexprCorrPred = (*pexprAggChild)[1];

		// Accept the site.  Fill parse result (Phase 1).
		pResult->pexprOuterCmp = pexprConj;
		pResult->pexprSubquery = pexprSubq;
		pResult->pexprSubqBody = pexprBody;
		pResult->pexprSubqGbAgg = pexprGbAgg;
		pResult->pexprSubqSelect = pexprAggChild;
		pResult->pexprSubqCorrPred = pexprCorrPred;
		pResult->pexprSubqInnerGet = pexprInnerGet;
		pResult->pexprAggProjList = pexprAggProj;
		pResult->pexprFinalProjList = pexprFinalProj;
		pResult->pcrSubqOutput =
			CScalarSubquery::PopConvert(pexprSubq->Pop())->Pcr();
		return pexprConj;
	}
	return nullptr;
}
// Recursively search the relational children of pexprNAryJoin for a
// CLogicalGet whose underlying table MDId equals pexprInnerGet's.
// Returns nullptr if no match.  (Q17: subquery inner Get is "lineitem";
// outer NAryJoin contains a "lineitem" Get as one of its rel children.)
static CExpression *
PexprFindMatchingGet(CExpression *pexprNAryJoin, CExpression *pexprInnerGet)
{
	if (nullptr == pexprNAryJoin || nullptr == pexprInnerGet) return nullptr;
	if (COperator::EopLogicalGet != pexprInnerGet->Pop()->Eopid())
	{
		return nullptr;
	}
	CLogicalGet *popInnerGet = CLogicalGet::PopConvert(pexprInnerGet->Pop());
	IMDId *mdidInner = popInnerGet->Ptabdesc()->MDId();

	// NAryJoin's last child is the scalar pred; skip it.
	const ULONG arity = pexprNAryJoin->Arity();
	for (ULONG i = 0; i + 1 < arity; i++)
	{
		CExpression *pexprChild = (*pexprNAryJoin)[i];
		if (nullptr == pexprChild) continue;

		// Walk through wrappers (e.g., Select, Project) down to a Get.
		CExpression *pexprCursor = pexprChild;
		while (nullptr != pexprCursor &&
			   COperator::EopLogicalGet != pexprCursor->Pop()->Eopid())
		{
			if (pexprCursor->Arity() == 0) break;
			pexprCursor = (*pexprCursor)[0];
		}
		if (nullptr == pexprCursor ||
			COperator::EopLogicalGet != pexprCursor->Pop()->Eopid())
		{
			continue;
		}
		CLogicalGet *popOuterGet = CLogicalGet::PopConvert(pexprCursor->Pop());
		if (popOuterGet->Ptabdesc()->MDId()->Equals(mdidInner))
		{
			return pexprCursor;
		}
	}
	return nullptr;
}

// Build an inner_col_id → outer CColRef map by pairing the two Gets'
// output CColRefArrays on attribute number.  Returns map allocated in
// mp; caller owns and must Release().
static UlongToColRefMap *
BuildInnerToOuterMap(CMemoryPool *mp, CExpression *pexprInnerGet,
					 CExpression *pexprOuterGet)
{
	GPOS_ASSERT(nullptr != pexprInnerGet);
	GPOS_ASSERT(nullptr != pexprOuterGet);
	CLogicalGet *popInner = CLogicalGet::PopConvert(pexprInnerGet->Pop());
	CLogicalGet *popOuter = CLogicalGet::PopConvert(pexprOuterGet->Pop());
	CColRefArray *pdrgpcrInner = popInner->PdrgpcrOutput();
	CColRefArray *pdrgpcrOuter = popOuter->PdrgpcrOutput();
	CColumnDescriptorArray *pdrgpcoldescInner =
		popInner->Ptabdesc()->Pdrgpcoldesc();
	CColumnDescriptorArray *pdrgpcoldescOuter =
		popOuter->Ptabdesc()->Pdrgpcoldesc();

	UlongToColRefMap *pcolmap = GPOS_NEW(mp) UlongToColRefMap(mp);

	// Fast path: same-sized arrays with aligned attno — direct pairing.
	const ULONG inner_cnt = pdrgpcrInner->Size();
	const ULONG outer_cnt = pdrgpcrOuter->Size();
	for (ULONG i = 0; i < inner_cnt; i++)
	{
		CColRef *pcrInner = (*pdrgpcrInner)[i];
		INT attno_inner = (*pdrgpcoldescInner)[i]->AttrNum();

		CColRef *pcrOuterMatched = nullptr;
		for (ULONG j = 0; j < outer_cnt; j++)
		{
			if ((*pdrgpcoldescOuter)[j]->AttrNum() == attno_inner)
			{
				pcrOuterMatched = (*pdrgpcrOuter)[j];
				break;
			}
		}
		if (nullptr == pcrOuterMatched)
		{
			// No outer column with this attno — leave it unmapped.  The
			// caller (FMatchAndParseQ17Site step 6) verifies the agg
			// projection list references no unmapped column before
			// accepting the rewrite.
			continue;
		}
		ULONG *pulKey = GPOS_NEW(mp) ULONG(pcrInner->Id());
		pcolmap->Insert(pulKey, pcrOuterMatched);
	}
	return pcolmap;
}

// Convert a CScalarAggFunc expression into an equivalent CScalarWindowFunc
// expression.  The children (agg args) are copied as-is via AddRef.
// Returns a newly-allocated CExpression.
static CExpression *
PexprAggFuncToWindowFunc(CMemoryPool *mp, CExpression *pexprAggFunc)
{
	GPOS_ASSERT(nullptr != pexprAggFunc);
	GPOS_ASSERT(COperator::EopScalarAggFunc == pexprAggFunc->Pop()->Eopid());

	CScalarAggFunc *popAgg = CScalarAggFunc::PopConvert(pexprAggFunc->Pop());

	// Copy mdids (ref-counted) and function name string.
	IMDId *mdid_func = popAgg->MDId();
	mdid_func->AddRef();
	IMDId *mdid_return_type = popAgg->MdidType();
	mdid_return_type->AddRef();
	CWStringConst *pstrFunc =
		GPOS_NEW(mp) CWStringConst(mp, popAgg->PstrAggFunc()->GetBuffer());

	CScalarWindowFunc *popWin = GPOS_NEW(mp) CScalarWindowFunc(
		mp, mdid_func, mdid_return_type, pstrFunc,
		CScalarWindowFunc::EwsImmediate, popAgg->IsDistinct(),
		popAgg->IsAggStar(), /*is_simple_agg*/ true);

	// CScalarAggFunc wraps args in CScalarValuesList (4 slots: args,
	// distinct-args, filter-args, ordered-args).  CScalarWindowFunc
	// expects flat args as direct children, so extract the first
	// CScalarValuesList's children as the window function args.
	CExpressionArray *pdrgpexprChildren = GPOS_NEW(mp) CExpressionArray(mp);
	if (pexprAggFunc->Arity() > 0)
	{
		CExpression *pexprArgs = (*pexprAggFunc)[0];
		const ULONG n_args = pexprArgs->Arity();
		for (ULONG i = 0; i < n_args; i++)
		{
			CExpression *pexprArg = (*pexprArgs)[i];
			pexprArg->AddRef();
			pdrgpexprChildren->Append(pexprArg);
		}
	}
	return GPOS_NEW(mp) CExpression(mp, popWin, pdrgpexprChildren);
}

// Recursively walk a scalar expression; wherever a CScalarAggFunc is
// encountered, replace it with an equivalent CScalarWindowFunc.  All
// other operators are structurally copied (children recursed, op AddRef).
static CExpression *
PexprConvertAggToWindow(CMemoryPool *mp, CExpression *pexpr)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(nullptr != pexpr);

	if (COperator::EopScalarAggFunc == pexpr->Pop()->Eopid())
	{
		return PexprAggFuncToWindowFunc(mp, pexpr);
	}

	CExpressionArray *pdrgpexprChildren = GPOS_NEW(mp) CExpressionArray(mp);
	const ULONG arity = pexpr->Arity();
	for (ULONG i = 0; i < arity; i++)
	{
		pdrgpexprChildren->Append(
			PexprConvertAggToWindow(mp, (*pexpr)[i]));
	}
	COperator *pop = pexpr->Pop();
	pop->AddRef();
	return GPOS_NEW(mp) CExpression(mp, pop, pdrgpexprChildren);
}

// Build the SeqPrj project list: remap agg_proj_list inner→outer and
// convert AggFunc→WindowFunc.  Only window functions go in SeqPrj's
// proj list (ORCA / DXL restriction).  Returns a CScalarProjectList.
static CExpression *
PexprBuildWindowProjList(CMemoryPool *mp, const SQ17ParseResult &parse)
{
	// Remap agg_proj_list's inner col refs to outer via colmap.  must_exist
	// is false, but FMatchAndParseQ17Site has already verified every column
	// used here has a mapping, so no inner ref survives unremapped.
	CExpression *pexprAggProjRemapped =
		parse.pexprAggProjList->PexprCopyWithRemappedColumns(
			mp, parse.pcolmap, /*must_exist*/ false);
	// Convert AggFunc → WindowFunc inside the remapped proj list.
	CExpression *pexprAggProjAsWindow =
		PexprConvertAggToWindow(mp, pexprAggProjRemapped);
	pexprAggProjRemapped->Release();

	CExpressionArray *pdrgpexprElems = GPOS_NEW(mp) CExpressionArray(mp);
	const ULONG agg_cnt = pexprAggProjAsWindow->Arity();
	for (ULONG i = 0; i < agg_cnt; i++)
	{
		CExpression *pexprPrjElem = (*pexprAggProjAsWindow)[i];
		pexprPrjElem->AddRef();
		pdrgpexprElems->Append(pexprPrjElem);
	}
	pexprAggProjAsWindow->Release();

	return GPOS_NEW(mp)
		CExpression(mp, GPOS_NEW(mp) CScalarProjectList(mp),
					pdrgpexprElems);
}

// Build the NAryJoin scalar predicate with the subq-carrying conjunct
// removed.  If no conjuncts remain, returns a scalar constant TRUE;
// if one remains, returns that conjunct standalone; else rebuilds
// CScalarBoolOp(AND).  Every retained conjunct is AddRef'd.
static CExpression *
PexprScalarPredMinusConjunct(CMemoryPool *mp, CExpression *pexprScalarPred,
							 CExpression *pexprConjunctToRemove)
{
	std::vector<CExpression *> conjuncts;
	ExtractConjuncts(pexprScalarPred, conjuncts);

	CExpressionArray *pdrgpexprKeep = GPOS_NEW(mp) CExpressionArray(mp);
	for (CExpression *pexprConj : conjuncts)
	{
		if (pexprConj == pexprConjunctToRemove) continue;
		pexprConj->AddRef();
		pdrgpexprKeep->Append(pexprConj);
	}
	if (0 == pdrgpexprKeep->Size())
	{
		pdrgpexprKeep->Release();
		return CUtils::PexprScalarConstBool(mp, /*fVal*/ true);
	}
	if (1 == pdrgpexprKeep->Size())
	{
		CExpression *pexprOne = (*pdrgpexprKeep)[0];
		pexprOne->AddRef();
		pdrgpexprKeep->Release();
		return pexprOne;
	}
	return CUtils::PexprScalarBoolOp(mp, CScalarBoolOp::EboolopAnd,
									 pdrgpexprKeep);
}

// Replace every occurrence of CScalarIdent(pcrSubqOutput) inside a scalar
// expression with its copy unchanged, AND replace the CScalarSubquery
// node itself (which has the same output CColRef) with CScalarIdent
// pointing at pcrSubqOutput.  Used to rewrite the outer comparison so
// it references the window's computed column instead of the subquery.
static CExpression *
PexprReplaceSubqueryWithIdent(CMemoryPool *mp, CExpression *pexpr,
							  const CColRef *pcrSubqOutput)
{
	GPOS_CHECK_STACK_SIZE;
	if (COperator::EopScalarSubquery == pexpr->Pop()->Eopid())
	{
		return CUtils::PexprScalarIdent(mp, pcrSubqOutput);
	}
	CExpressionArray *pdrgpexprChildren = GPOS_NEW(mp) CExpressionArray(mp);
	const ULONG arity = pexpr->Arity();
	for (ULONG i = 0; i < arity; i++)
	{
		pdrgpexprChildren->Append(PexprReplaceSubqueryWithIdent(
			mp, (*pexpr)[i], pcrSubqOutput));
	}
	COperator *pop = pexpr->Pop();
	pop->AddRef();
	return GPOS_NEW(mp) CExpression(mp, pop, pdrgpexprChildren);
}

// Rebuild the NAryJoin with children unchanged (AddRef) but scalar pred
// swapped out.
static CExpression *
PexprNAryJoinWithNewScalarPred(CMemoryPool *mp, CExpression *pexprNAryJoin,
							   CExpression *pexprNewScalarPred)
{
	const ULONG arity = pexprNAryJoin->Arity();
	CExpressionArray *pdrgpexprChildren = GPOS_NEW(mp) CExpressionArray(mp);
	for (ULONG i = 0; i + 1 < arity; i++)
	{
		CExpression *pexprRel = (*pexprNAryJoin)[i];
		pexprRel->AddRef();
		pdrgpexprChildren->Append(pexprRel);
	}
	pdrgpexprChildren->Append(pexprNewScalarPred);	 // takes ownership
	COperator *pop = pexprNAryJoin->Pop();
	pop->AddRef();
	return GPOS_NEW(mp) CExpression(mp, pop, pdrgpexprChildren);
}

// Build the rewritten WinMagic tree from a successful parse:
//   CLogicalSelect(pexprNewFilter,
//     CLogicalSequenceProject(partition_by=pcrPartitionBy,
//                             NAryJoin(rels..., scalar_pred_minus_subq),
//                             win_proj_list))
static CExpression *
PexprBuildRewrittenTree(CMemoryPool *mp, CExpression *pexprNAryJoin,
						const SQ17ParseResult &parse)
{
	// 1. Drop the subq-carrying conjunct from NAryJoin's scalar pred.
	CExpression *pexprScalarPred =
		(*pexprNAryJoin)[pexprNAryJoin->Arity() - 1];
	CExpression *pexprNewScalarPred = PexprScalarPredMinusConjunct(
		mp, pexprScalarPred, parse.pexprOuterCmp);

	// 2. Rebuild NAryJoin with the trimmed scalar pred.
	CExpression *pexprNewNAryJoin =
		PexprNAryJoinWithNewScalarPred(mp, pexprNAryJoin, pexprNewScalarPred);

	// 3. Build the SeqPrj window project list (agg→window + final_proj).
	CExpression *pexprWinProjList = PexprBuildWindowProjList(mp, parse);

	// 4. Build CDistributionSpecHashed from pcrPartitionBy.
	CExpressionArray *pdrgpexprPartKeys = GPOS_NEW(mp) CExpressionArray(mp);
	pdrgpexprPartKeys->Append(
		CUtils::PexprScalarIdent(mp, parse.pcrPartitionBy));
	CDistributionSpecHashed *pds = GPOS_NEW(mp) CDistributionSpecHashed(
		pdrgpexprPartKeys, /*fNullsColocated*/ true);

	// 5. Empty order spec + empty window frame arrays (one entry each
	// per window function — Q17 has exactly one avg()).
	COrderSpecArray *pdrgpos = GPOS_NEW(mp) COrderSpecArray(mp);
	pdrgpos->Append(GPOS_NEW(mp) COrderSpec(mp));

	CWindowFrameArray *pdrgpwf = GPOS_NEW(mp) CWindowFrameArray(mp);
	// Use default-constructed frame: RANGE, UNBOUNDED PRECEDING .. CURRENT ROW.
	pdrgpwf->Append(GPOS_NEW(mp) CWindowFrame(
		mp, CWindowFrame::EfsRange, CWindowFrame::EfbUnboundedPreceding,
		CWindowFrame::EfbCurrentRow, /*pexprLeading*/ nullptr,
		/*pexprTrailing*/ nullptr, CWindowFrame::EfesNulls,
		/*start_in_range_func*/ 0, /*end_in_range_func*/ 0,
		/*in_range_coll*/ 0, /*in_range_asc*/ false,
		/*in_range_nulls_first*/ false));

	// 6. SequenceProject = [NAryJoin', win_proj_list (window funcs only)].
	CExpression *pexprSeqPrj = GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp) CLogicalSequenceProject(
			mp, COperator::EsptypeGlobalOneStep, pds, pdrgpos, pdrgpwf),
		pexprNewNAryJoin, pexprWinProjList);

	// 7. Stack the subquery's final_proj_list on top as CLogicalProject.
	// This is where col_54 = 0.2 * col_53 (window output) is computed.
	CExpressionArray *pdrgpexprFinalElems = GPOS_NEW(mp) CExpressionArray(mp);
	const ULONG final_cnt = parse.pexprFinalProjList->Arity();
	for (ULONG i = 0; i < final_cnt; i++)
	{
		CExpression *pexprElem = (*parse.pexprFinalProjList)[i];
		pexprElem->AddRef();
		pdrgpexprFinalElems->Append(pexprElem);
	}
	CExpression *pexprFinalProj = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CScalarProjectList(mp), pdrgpexprFinalElems);
	CExpression *pexprProjected = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CLogicalProject(mp), pexprSeqPrj, pexprFinalProj);

	// 8. New filter: replace CScalarSubquery in pexprOuterCmp with
	// CScalarIdent(pcrSubqOutput).
	CExpression *pexprNewFilter = PexprReplaceSubqueryWithIdent(
		mp, parse.pexprOuterCmp, parse.pcrSubqOutput);

	// 9. Wrap with Select(new_filter).
	return CUtils::PexprLogicalSelect(mp, pexprProjected, pexprNewFilter);
}

}  // namespace

BOOL
CCorrSubqFilterPushdownPreprocessor::FMatchAndParseQ17Site(
	CMemoryPool *mp, CExpression *pexprNAryJoin, SQ17ParseResult *pResult)
{
	GPOS_ASSERT(nullptr != pexprNAryJoin);
	GPOS_ASSERT(nullptr != pResult);
	GPOS_ASSERT(COperator::EopLogicalNAryJoin == pexprNAryJoin->Pop()->Eopid());

	const ULONG arity = pexprNAryJoin->Arity();
	if (2 > arity) return false;	 // need at least one rel + scalar pred
	CExpression *pexprScalarPred = (*pexprNAryJoin)[arity - 1];

	// Step 1: find a Q17-shaped scalar subquery conjunct.
	if (nullptr == PexprFindQ17Conjunct(pexprScalarPred, pResult))
	{
		return false;
	}

	// Step 2: parse correlation pred → (inner_col, outer_col).
	CColRefSet *pcrsInnerOutput =
		pResult->pexprSubqInnerGet->DeriveOutputColumns();
	if (!FParseCorrelationPred(pResult->pexprSubqCorrPred, pcrsInnerOutput,
							   &pResult->pcrInnerCorr,
							   &pResult->pcrOuterCorr))
	{
		return false;
	}

	// Step 3: verify an outer NAryJoin conjunct ties pcrOuterCorr to a
	// column in NAryJoin's scope — that column is the Window partition key.
	std::vector<CExpression *> outer_conjuncts;
	ExtractConjuncts(pexprScalarPred, outer_conjuncts);
	const CColRef *pcrPartitionBy = nullptr;
	for (CExpression *pexprConj : outer_conjuncts)
	{
		if (pexprConj == pResult->pexprOuterCmp) continue;	// skip subq-conj
		const CColRef *pcrPeer = PcrEqualPeer(pexprConj, pResult->pcrOuterCorr);
		if (nullptr != pcrPeer)
		{
			pcrPartitionBy = pcrPeer;
			break;
		}
	}
	if (nullptr == pcrPartitionBy)
	{
		// StarRocks condition 4: no matching outer join conjunct.
		return false;
	}
	pResult->pcrPartitionBy = pcrPartitionBy;

	// Step 4: locate outer relational child whose Get matches the
	// subquery's inner Get on table MDId.  This gives the physical
	// column correspondence for the agg function rewrite.
	pResult->pexprOuterMatchingGet =
		PexprFindMatchingGet(pexprNAryJoin, pResult->pexprSubqInnerGet);
	if (nullptr == pResult->pexprOuterMatchingGet)
	{
		// No matching table in outer — violates StarRocks condition 5.
		return false;
	}

	// Step 5: build inner_col → outer_col map keyed on CColRef::Id().
	pResult->pcolmap = BuildInnerToOuterMap(mp, pResult->pexprSubqInnerGet,
										   pResult->pexprOuterMatchingGet);

	// Step 6: every column referenced by the agg projection list must have an
	// inner→outer mapping.  If an inner column was pruned from the outer Get
	// (no matching attno) it has no entry, and remapping it with
	// must_exist=false would silently leave the inner CColRef in the window
	// project list as an out-of-scope reference, producing an invalid plan.
	// Decline the rewrite in that case so we fall back to the original
	// correlated plan.  (For Q17 itself the map is always complete.)
	CColRefSet *pcrsAggUsed = pResult->pexprAggProjList->DeriveUsedColumns();
	CColRefSetIter crsi(*pcrsAggUsed);
	while (crsi.Advance())
	{
		ULONG colid = crsi.Pcr()->Id();
		if (nullptr == pResult->pcolmap->Find(&colid))
		{
			// Declining the rewrite here; release the map the caller would
			// otherwise only free on the accepted (true) path.
			pResult->pcolmap->Release();
			pResult->pcolmap = nullptr;
			return false;
		}
	}

	// All Q17 preconditions satisfied; the caller builds the rewritten tree.
	return true;
}

CExpression *
CCorrSubqFilterPushdownPreprocessor::PexprPreprocess(CMemoryPool *mp,
													 CExpression *pexpr)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(nullptr != mp);
	GPOS_ASSERT(nullptr != pexpr);

	// GUC-gated: no-op when disabled.
	if (!GPOS_FTRACE(EopttraceEnableScalarSubq2FilteredAgg))
	{
		pexpr->AddRef();
		return pexpr;
	}

	// Detect a Q17-shaped site on this NAryJoin, then rewrite it.
	if (COperator::EopLogicalNAryJoin == pexpr->Pop()->Eopid() &&
		1 < pexpr->Arity())
	{
		SQ17ParseResult parse_result;
		if (FMatchAndParseQ17Site(mp, pexpr, &parse_result))
		{
			// Build the full rewritten tree and return it
			// in place of the original NAryJoin.
			CExpression *pexprRewritten =
				PexprBuildRewrittenTree(mp, pexpr, parse_result);

			if (nullptr != parse_result.pcolmap)
			{
				parse_result.pcolmap->Release();
			}
			return pexprRewritten;
		}
	}

	// Default: recursively preprocess children and rebuild.
	const ULONG arity = pexpr->Arity();
	CExpressionArray *pdrgpexprChildren = GPOS_NEW(mp) CExpressionArray(mp);
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprChild = PexprPreprocess(mp, (*pexpr)[ul]);
		pdrgpexprChildren->Append(pexprChild);
	}

	COperator *pop = pexpr->Pop();
	pop->AddRef();
	return GPOS_NEW(mp) CExpression(mp, pop, pdrgpexprChildren);
}

// EOF
