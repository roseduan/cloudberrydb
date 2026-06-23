//---------------------------------------------------------------------------
//	Greenplum Database
//	Portions Copyright (c) 2023-2026, HashData Technology Limited.
//
//	@filename:
//		CPhysicalRightSemiHashJoin.cpp
//
//	@doc:
//		Implementation of right semi hash join operator.
//		Mirror of CPhysicalLeftSemiHashJoin with build/probe roles flipped:
//		  - LeftSemi:  build = inner (right child), probe = outer (left child)
//		  - RightSemi: build = outer (left child),  probe = inner (right child)
//		Output schema and FProvidesReqdCols semantics are identical to LeftSemi
//		(both preserve left-side rows). The flip is encoded in the operator
//		EOperatorId, queried by M3 cost model and M4 DXL translation.
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalRightSemiHashJoin.h"

#include "gpos/base.h"

#include "gpopt/base/CDistributionSpecHashed.h"
#include "gpopt/base/CUtils.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalRightSemiHashJoin::CPhysicalRightSemiHashJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalRightSemiHashJoin::CPhysicalRightSemiHashJoin(
	CMemoryPool *mp, CExpressionArray *pdrgpexprOuterKeys,
	CExpressionArray *pdrgpexprInnerKeys, IMdIdArray *hash_opfamilies,
	BOOL is_null_aware, CXform::EXformId origin_xform)
	: CPhysicalHashJoin(mp, pdrgpexprOuterKeys, pdrgpexprInnerKeys,
						hash_opfamilies, is_null_aware, origin_xform)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalRightSemiHashJoin::~CPhysicalRightSemiHashJoin
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalRightSemiHashJoin::~CPhysicalRightSemiHashJoin() = default;


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalRightSemiHashJoin::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
BOOL
CPhysicalRightSemiHashJoin::FProvidesReqdCols(CExpressionHandle &exprhdl,
											  CColRefSet *pcrsRequired,
											  ULONG	 // ulOptReq
) const
{
	// Right semi join semantically preserves left-side rows (same as Left
	// semi). Physical build side is the left child, finalize phase scans the
	// build hash table and emits visited left rows -- so output columns come
	// from the outer (left) child.
	return FOuterProvidesReqdCols(exprhdl, pcrsRequired);
}


CPartitionPropagationSpec *
CPhysicalRightSemiHashJoin::PppsRequired(CMemoryPool *mp,
										 CExpressionHandle &exprhdl,
										 CPartitionPropagationSpec *pppsRequired,
										 ULONG child_index,
										 CDrvdPropArray *pdrgpdpCtxt,
										 ULONG ulOptReq) const
{
	return PppsRequiredForJoins(mp, exprhdl, pppsRequired, child_index,
								pdrgpdpCtxt, ulOptReq);
}

CPartitionPropagationSpec *
CPhysicalRightSemiHashJoin::PppsDerive(CMemoryPool *mp,
									   CExpressionHandle &exprhdl) const
{
	return PppsDeriveForJoins(mp, exprhdl);
}
// EOF
