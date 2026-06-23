/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

//---------------------------------------------------------------------------
//	@filename:
//		CPhysicalParallelRightAntiSemiHashJoin.h
//
//	@doc:
//		Parallel right anti semi hash join operator (build = outer/left).
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalParallelRightAntiSemiHashJoin_H
#define GPOPT_CPhysicalParallelRightAntiSemiHashJoin_H

#include "gpos/base.h"

#include "gpopt/operators/CPhysicalParallelHashJoin.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CPhysicalParallelRightAntiSemiHashJoin
//
//	@doc:
//		Parallel right anti semi hash join operator
//
//---------------------------------------------------------------------------
class CPhysicalParallelRightAntiSemiHashJoin : public CPhysicalParallelHashJoin
{
public:
	CPhysicalParallelRightAntiSemiHashJoin(
		const CPhysicalParallelRightAntiSemiHashJoin &) = delete;

	// ctor
	CPhysicalParallelRightAntiSemiHashJoin(CMemoryPool *mp,
										   CExpressionArray *pdrgpexprOuterKeys,
										   CExpressionArray *pdrgpexprInnerKeys,
										   IMdIdArray *hash_opfamilies,
										   BOOL is_null_aware = true,
										   CXform::EXformId origin_xform =
											   CXform::ExfSentinel);

	// dtor
	~CPhysicalParallelRightAntiSemiHashJoin() override;

	// ident accessors
	EOperatorId
	Eopid() const override
	{
		return EopPhysicalParallelRightAntiSemiHashJoin;
	}

	// return a string for operator name
	const CHAR *
	SzId() const override
	{
		return "CPhysicalParallelRightAntiSemiHashJoin";
	}

	// partition propagation
	CPartitionPropagationSpec *PppsRequired(
		CMemoryPool *mp, CExpressionHandle &exprhdl,
		CPartitionPropagationSpec *pppsRequired, ULONG child_index,
		CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq) const override;

	CPartitionPropagationSpec *PppsDerive(
		CMemoryPool *mp, CExpressionHandle &exprhdl) const override;

	// check if required columns are included in output columns
	// Right anti semi join preserves left-side rows -> output from outer (left)
	BOOL FProvidesReqdCols(CExpressionHandle &exprhdl,
						   CColRefSet *pcrsRequired,
						   ULONG ulOptReq) const override;

	// conversion function
	static CPhysicalParallelRightAntiSemiHashJoin *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(nullptr != pop);
		GPOS_ASSERT(EopPhysicalParallelRightAntiSemiHashJoin == pop->Eopid());

		return dynamic_cast<CPhysicalParallelRightAntiSemiHashJoin *>(pop);
	}

};	// class CPhysicalParallelRightAntiSemiHashJoin

}  // namespace gpopt

#endif	// !GPOPT_CPhysicalParallelRightAntiSemiHashJoin_H

// EOF
