//---------------------------------------------------------------------------
//	Greenplum Database
//	Portions Copyright (c) 2023-2026, HashData Technology Limited.
//
//	@filename:
//		CPhysicalRightAntiSemiHashJoin.h
//
//	@doc:
//		Right anti semi hash join operator (PG-style: build = outer/left,
//		probe = inner/right, finalize 输出 left 行 unvisited).
//		Mirror of CPhysicalLeftAntiSemiHashJoin with build/probe roles flipped.
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalRightAntiSemiHashJoin_H
#define GPOPT_CPhysicalRightAntiSemiHashJoin_H

#include "gpos/base.h"

#include "gpopt/operators/CPhysicalHashJoin.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CPhysicalRightAntiSemiHashJoin
//
//	@doc:
//		Right anti semi hash join operator
//
//---------------------------------------------------------------------------
class CPhysicalRightAntiSemiHashJoin : public CPhysicalHashJoin
{
private:
public:
	CPhysicalRightAntiSemiHashJoin(const CPhysicalRightAntiSemiHashJoin &) =
		delete;

	// ctor
	CPhysicalRightAntiSemiHashJoin(
		CMemoryPool *mp, CExpressionArray *pdrgpexprOuterKeys,
		CExpressionArray *pdrgpexprInnerKeys, IMdIdArray *hash_opfamilies,
		BOOL is_null_aware = true,
		CXform::EXformId origin_xform = CXform::ExfSentinel);

	// dtor
	~CPhysicalRightAntiSemiHashJoin() override;

	// ident accessors
	EOperatorId
	Eopid() const override
	{
		return EopPhysicalRightAntiSemiHashJoin;
	}

	// return a string for operator name
	const CHAR *
	SzId() const override
	{
		return "CPhysicalRightAntiSemiHashJoin";
	}

	// check if required columns are included in output columns
	BOOL FProvidesReqdCols(CExpressionHandle &exprhdl, CColRefSet *pcrsRequired,
						   ULONG ulOptReq) const override;

	// compute required partition propagation spec; mirror the right semi
	// hash join so the join-specific helper (which skips DPE for the
	// build=outer flip) is used instead of the generic base implementation
	CPartitionPropagationSpec *PppsRequired(
		CMemoryPool *mp, CExpressionHandle &exprhdl,
		CPartitionPropagationSpec *pppsRequired, ULONG child_index,
		CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq) const override;

	// derive partition propagation spec
	CPartitionPropagationSpec *PppsDerive(
		CMemoryPool *mp, CExpressionHandle &exprhdl) const override;

	// conversion function
	static CPhysicalRightAntiSemiHashJoin *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(EopPhysicalRightAntiSemiHashJoin == pop->Eopid());

		return dynamic_cast<CPhysicalRightAntiSemiHashJoin *>(pop);
	}


};	// class CPhysicalRightAntiSemiHashJoin

}  // namespace gpopt

#endif	// !GPOPT_CPhysicalRightAntiSemiHashJoin_H

// EOF
