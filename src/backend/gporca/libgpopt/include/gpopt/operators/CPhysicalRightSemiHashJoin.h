//---------------------------------------------------------------------------
//	Greenplum Database
//	Portions Copyright (c) 2023-2026, HashData Technology Limited.
//
//	@filename:
//		CPhysicalRightSemiHashJoin.h
//
//	@doc:
//		Right semi hash join operator (PG-style: build = outer/left,
//		probe = inner/right, finalize 输出 left 行 visited).
//		Mirror of CPhysicalLeftSemiHashJoin with build/probe roles flipped.
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalRightSemiHashJoin_H
#define GPOPT_CPhysicalRightSemiHashJoin_H

#include "gpos/base.h"

#include "gpopt/operators/CPhysicalHashJoin.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CPhysicalRightSemiHashJoin
//
//	@doc:
//		Right semi hash join operator
//
//---------------------------------------------------------------------------
class CPhysicalRightSemiHashJoin : public CPhysicalHashJoin
{
private:
public:
	CPhysicalRightSemiHashJoin(const CPhysicalRightSemiHashJoin &) = delete;

	// ctor
	CPhysicalRightSemiHashJoin(
		CMemoryPool *mp, CExpressionArray *pdrgpexprOuterKeys,
		CExpressionArray *pdrgpexprInnerKeys, IMdIdArray *hash_opfamilies,
		BOOL is_null_aware = true,
		CXform::EXformId origin_xform = CXform::ExfSentinel);

	// dtor
	~CPhysicalRightSemiHashJoin() override;

	// ident accessors
	EOperatorId
	Eopid() const override
	{
		return EopPhysicalRightSemiHashJoin;
	}

	// return a string for operator name
	const CHAR *
	SzId() const override
	{
		return "CPhysicalRightSemiHashJoin";
	}

	CPartitionPropagationSpec *PppsRequired(
		CMemoryPool *mp, CExpressionHandle &exprhdl,
		CPartitionPropagationSpec *pppsRequired, ULONG child_index,
		CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq) const override;

	CPartitionPropagationSpec *PppsDerive(
		CMemoryPool *mp, CExpressionHandle &exprhdl) const override;

	// check if required columns are included in output columns.
	// Right semi join semantically preserves left-side rows (same as Left
	// semi join), so output columns come from the outer (left) child.
	BOOL FProvidesReqdCols(CExpressionHandle &exprhdl, CColRefSet *pcrsRequired,
						   ULONG ulOptReq) const override;

	// conversion function
	static CPhysicalRightSemiHashJoin *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(EopPhysicalRightSemiHashJoin == pop->Eopid());

		return dynamic_cast<CPhysicalRightSemiHashJoin *>(pop);
	}


};	// class CPhysicalRightSemiHashJoin

}  // namespace gpopt

#endif	// !GPOPT_CPhysicalRightSemiHashJoin_H

// EOF
