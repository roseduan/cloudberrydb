//---------------------------------------------------------------------------
//	Greenplum Database
//	Portions Copyright (c) 2023-2026, HashData Technology Limited.
//
//	@filename:
//		CXformLeftAntiSemiJoin2RightAntiSemiHashJoin.h
//
//	@doc:
//		Transform logical left anti semi join to physical right anti semi
//		hash join. Symmetric to CXformLeftAntiSemiJoin2HashJoin.
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformLeftAntiSemiJoin2RightAntiSemiHashJoin_H
#define GPOPT_CXformLeftAntiSemiJoin2RightAntiSemiHashJoin_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformImplementation.h"

namespace gpopt
{
using namespace gpos;

class CXformLeftAntiSemiJoin2RightAntiSemiHashJoin : public CXformImplementation
{
private:
public:
	CXformLeftAntiSemiJoin2RightAntiSemiHashJoin(
		const CXformLeftAntiSemiJoin2RightAntiSemiHashJoin &) = delete;

	// ctor
	explicit CXformLeftAntiSemiJoin2RightAntiSemiHashJoin(CMemoryPool *mp);

	// dtor
	~CXformLeftAntiSemiJoin2RightAntiSemiHashJoin() override = default;

	// ident accessors
	EXformId
	Exfid() const override
	{
		return ExfLeftAntiSemiJoin2RightAntiSemiHashJoin;
	}

	// return a string for xform name
	const CHAR *
	SzId() const override
	{
		return "CXformLeftAntiSemiJoin2RightAntiSemiHashJoin";
	}

	// compute xform promise for a given expression handle
	EXformPromise Exfp(CExpressionHandle &exprhdl) const override;

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const override;

};	// class CXformLeftAntiSemiJoin2RightAntiSemiHashJoin

}  // namespace gpopt

#endif	// !GPOPT_CXformLeftAntiSemiJoin2RightAntiSemiHashJoin_H

// EOF
