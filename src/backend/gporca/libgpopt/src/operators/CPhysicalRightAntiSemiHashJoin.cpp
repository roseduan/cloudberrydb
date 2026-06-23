//---------------------------------------------------------------------------
//	Greenplum Database
//	Portions Copyright (c) 2023-2026, HashData Technology Limited.
//
//	@filename:
//		CPhysicalRightAntiSemiHashJoin.cpp
//
//	@doc:
//		Implementation of right anti semi hash join operator.
//		Mirror of CPhysicalLeftAntiSemiHashJoin with build/probe roles flipped:
//		  - LeftAntiSemi:  build = inner (right child), probe = outer (left)
//		  - RightAntiSemi: build = outer (left child),  probe = inner (right)
//		Finalize emits left-side rows that have no match. Output column
//		semantics identical to LeftAntiSemi.
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalRightAntiSemiHashJoin.h"

#include "gpos/base.h"

#include "gpopt/base/CDistributionSpecHashed.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalRightAntiSemiHashJoin::CPhysicalRightAntiSemiHashJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalRightAntiSemiHashJoin::CPhysicalRightAntiSemiHashJoin(
	CMemoryPool *mp, CExpressionArray *pdrgpexprOuterKeys,
	CExpressionArray *pdrgpexprInnerKeys, IMdIdArray *hash_opfamilies,
	BOOL is_null_aware, CXform::EXformId origin_xform)
	: CPhysicalHashJoin(mp, pdrgpexprOuterKeys, pdrgpexprInnerKeys,
						hash_opfamilies, is_null_aware, origin_xform)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalRightAntiSemiHashJoin::~CPhysicalRightAntiSemiHashJoin
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalRightAntiSemiHashJoin::~CPhysicalRightAntiSemiHashJoin() = default;


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalRightAntiSemiHashJoin::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
BOOL
CPhysicalRightAntiSemiHashJoin::FProvidesReqdCols(CExpressionHandle &exprhdl,
												  CColRefSet *pcrsRequired,
												  ULONG	 // ulOptReq
) const
{
	// Right anti semi join semantically preserves left-side rows (same as
	// Left anti semi). Output columns come from the outer (left) child.
	return FOuterProvidesReqdCols(exprhdl, pcrsRequired);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalRightAntiSemiHashJoin::PppsRequired
//
//	@doc:
//		Compute required partition propagation spec.  Delegate to the
//		join-specific helper (same as CPhysicalRightSemiHashJoin) so that the
//		build=outer/probe=inner flip is handled correctly; the generic
//		CPhysicalHashJoin::PppsRequired assumes build=inner and would request
//		a partition selector on the wrong (already-built) side, tripping the
//		"rti > 0 && rti <= es_range_table_size" executor assertion.
//
//---------------------------------------------------------------------------
CPartitionPropagationSpec *
CPhysicalRightAntiSemiHashJoin::PppsRequired(
	CMemoryPool *mp, CExpressionHandle &exprhdl,
	CPartitionPropagationSpec *pppsRequired, ULONG child_index,
	CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq) const
{
	return PppsRequiredForJoins(mp, exprhdl, pppsRequired, child_index,
								pdrgpdpCtxt, ulOptReq);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalRightAntiSemiHashJoin::PppsDerive
//
//	@doc:
//		Derive partition propagation spec
//
//---------------------------------------------------------------------------
CPartitionPropagationSpec *
CPhysicalRightAntiSemiHashJoin::PppsDerive(CMemoryPool *mp,
										   CExpressionHandle &exprhdl) const
{
	return PppsDeriveForJoins(mp, exprhdl);
}

// EOF
