//---------------------------------------------------------------------------
//	Greenplum Database
//	Portions Copyright (c) 2023-2026, HashData Technology Limited.
//
//	@filename:
//		CXformLeftSemiJoin2RightSemiHashJoin.h
//
//	@doc:
//		Transform logical left semi join to physical right semi hash join.
//		Symmetric to CXformLeftSemiJoin2HashJoin but generates the RIGHT
//		(build = outer) physical candidate. Memo will hold both Left and
//		Right candidates; cost model picks the cheaper one (M3).
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformLeftSemiJoin2RightSemiHashJoin_H
#define GPOPT_CXformLeftSemiJoin2RightSemiHashJoin_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformImplementation.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformLeftSemiJoin2RightSemiHashJoin
//
//	@doc:
//		Transform left semi join to right semi hash join (build = outer)
//
//---------------------------------------------------------------------------
class CXformLeftSemiJoin2RightSemiHashJoin : public CXformImplementation
{
private:
public:
	CXformLeftSemiJoin2RightSemiHashJoin(
		const CXformLeftSemiJoin2RightSemiHashJoin &) = delete;

	// ctor
	explicit CXformLeftSemiJoin2RightSemiHashJoin(CMemoryPool *mp);

	// dtor
	~CXformLeftSemiJoin2RightSemiHashJoin() override = default;

	// ident accessors
	EXformId
	Exfid() const override
	{
		return ExfLeftSemiJoin2RightSemiHashJoin;
	}

	// return a string for xform name
	const CHAR *
	SzId() const override
	{
		return "CXformLeftSemiJoin2RightSemiHashJoin";
	}

	// compute xform promise for a given expression handle
	EXformPromise Exfp(CExpressionHandle &exprhdl) const override;

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const override;

};	// class CXformLeftSemiJoin2RightSemiHashJoin

}  // namespace gpopt

#endif	// !GPOPT_CXformLeftSemiJoin2RightSemiHashJoin_H

// EOF
