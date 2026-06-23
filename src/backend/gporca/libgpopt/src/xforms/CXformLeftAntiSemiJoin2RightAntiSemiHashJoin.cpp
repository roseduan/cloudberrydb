//---------------------------------------------------------------------------
//	Greenplum Database
//	Portions Copyright (c) 2023-2026, HashData Technology Limited.
//
//	@filename:
//		CXformLeftAntiSemiJoin2RightAntiSemiHashJoin.cpp
//
//	@doc:
//		Implementation of transform: logical left anti semi join ->
//		physical right anti semi hash join (build = outer/left).
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformLeftAntiSemiJoin2RightAntiSemiHashJoin.h"

#include "gpos/base.h"
#include "gpos/memory/CAutoMemoryPool.h"

#include "gpopt/operators/CLogicalLeftAntiSemiJoin.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/operators/CPhysicalRightAntiSemiHashJoin.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/xforms/CXformUtils.h"

using namespace gpopt;


CXformLeftAntiSemiJoin2RightAntiSemiHashJoin::
	CXformLeftAntiSemiJoin2RightAntiSemiHashJoin(CMemoryPool *mp)
	:  // pattern
	  CXformImplementation(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) CLogicalLeftAntiSemiJoin(mp),
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // left child
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // right child
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternTree(mp))  // predicate
		  ))
{
}


CXform::EXformPromise
CXformLeftAntiSemiJoin2RightAntiSemiHashJoin::Exfp(
	CExpressionHandle &exprhdl) const
{
	CXform::EXformPromise promise =
		CXformUtils::ExfpLogicalJoin2PhysicalJoin(exprhdl);
	if (CXform::ExfpNone == promise)
	{
		return CXform::ExfpNone;
	}

	// EXCEPT [ALL] is implemented in ORCA as a LeftAntiSemiJoin whose join
	// predicate uses NULL-aware "IS NOT DISTINCT FROM" (INDF) equalities so
	// that NULLs match.  The right-anti hash-join flip cannot honor that
	// NULL-matching hash condition, which makes ORCA throw and fall back to
	// the Postgres planner -- the EXCEPT query then loses vectorization
	// (degrades to HashSetOp).  Skip the RIGHT candidate when any join
	// conjunct is an INDF predicate; the LEFT anti-semi hash join handles
	// INDF correctly.
	CExpression *pexprScalar = exprhdl.PexprScalarExactChild(2);
	if (nullptr != pexprScalar)
	{
		CAutoMemoryPool amp;
		CExpressionArray *pdrgpexprConjuncts =
			CPredicateUtils::PdrgpexprConjuncts(amp.Pmp(), pexprScalar);
		const ULONG ulConjuncts = pdrgpexprConjuncts->Size();
		for (ULONG ul = 0; ul < ulConjuncts; ul++)
		{
			if (CPredicateUtils::FINDF((*pdrgpexprConjuncts)[ul]))
			{
				pdrgpexprConjuncts->Release();
				return CXform::ExfpNone;
			}
		}
		pdrgpexprConjuncts->Release();
	}

	return promise;
}


void
CXformLeftAntiSemiJoin2RightAntiSemiHashJoin::Transform(
	CXformContext *pxfctxt, CXformResult *pxfres, CExpression *pexpr) const
{
	GPOS_ASSERT(nullptr != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CXformUtils::ImplementHashJoin<CPhysicalRightAntiSemiHashJoin>(
		pxfctxt, pxfres, pexpr);
}


// EOF
