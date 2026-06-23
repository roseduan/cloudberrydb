//---------------------------------------------------------------------------
//	Greenplum Database
//	Portions Copyright (c) 2023-2026, HashData Technology Limited.
//
//	@filename:
//		CXformLeftSemiJoin2RightSemiHashJoin.cpp
//
//	@doc:
//		Implementation of transform: logical left semi join -> physical
//		right semi hash join (build = outer/left, probe = inner/right).
//		Generates the RIGHT candidate; cost model (M3) picks vs LEFT.
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformLeftSemiJoin2RightSemiHashJoin.h"

#include "gpos/base.h"

#include "gpopt/operators/CLogicalLeftSemiJoin.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/operators/CPhysicalRightSemiHashJoin.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/xforms/CXformUtils.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformLeftSemiJoin2RightSemiHashJoin::CXformLeftSemiJoin2RightSemiHashJoin
//
//	@doc:
//		ctor -- pattern identical to LeftSemi xform (same logical pattern)
//
//---------------------------------------------------------------------------
CXformLeftSemiJoin2RightSemiHashJoin::CXformLeftSemiJoin2RightSemiHashJoin(
	CMemoryPool *mp)
	:  // pattern
	  CXformImplementation(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) CLogicalLeftSemiJoin(mp),
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // left child
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // right child
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternTree(mp))  // predicate
		  ))
{
}


//---------------------------------------------------------------------------
//	@function:
//		CXformLeftSemiJoin2RightSemiHashJoin::Exfp
//
//	@doc:
//		Compute xform promise. Generate RIGHT_SEMI candidate whenever the
//		expression can be implemented as a hash join. Cost model decides
//		LEFT vs RIGHT -- no GUC, no heuristic in xform layer.
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformLeftSemiJoin2RightSemiHashJoin::Exfp(CExpressionHandle &exprhdl) const
{
	CXform::EXformPromise promise =
		CXformUtils::ExfpLogicalJoin2PhysicalJoin(exprhdl);
	if (CXform::ExfpNone == promise)
	{
		return CXform::ExfpNone;
	}

	// RIGHT_SEMI makes the outer (left) child the hash build side, which is
	// executed before the probe (inner) child.  If the outer child contains a
	// partitioned-table consumer eligible for dynamic partition elimination
	// (DPE), building on it would run the partitioned scan before the partition
	// selector on the probe side, defeating DPE (and emitting the runtime
	// warning "partition selector was not fully executed").  Skip the RIGHT
	// candidate in that case so the LEFT semi hash join -- which keeps the
	// partitioned table on the probe side and prunes correctly -- is chosen.
	if (exprhdl.DerivePartitionInfo(0)->UlConsumers() > 0)
	{
		return CXform::ExfpNone;
	}

	return promise;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformLeftSemiJoin2RightSemiHashJoin::Transform
//
//	@doc:
//		Actual transformation: instantiate CPhysicalRightSemiHashJoin with
//		children in original [outer, inner] order. The RIGHT operator's
//		Eopid encodes the build/probe flip.
//
//---------------------------------------------------------------------------
void
CXformLeftSemiJoin2RightSemiHashJoin::Transform(CXformContext *pxfctxt,
												CXformResult *pxfres,
												CExpression *pexpr) const
{
	GPOS_ASSERT(nullptr != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CXformUtils::ImplementHashJoin<CPhysicalRightSemiHashJoin>(pxfctxt, pxfres,
															   pexpr);
}


// EOF
