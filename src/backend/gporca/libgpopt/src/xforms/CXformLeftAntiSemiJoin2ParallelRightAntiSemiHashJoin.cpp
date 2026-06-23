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
//		CXformLeftAntiSemiJoin2ParallelRightAntiSemiHashJoin.cpp
//
//	@doc:
//		Transform left anti semi join to parallel right anti semi hash join
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformLeftAntiSemiJoin2ParallelRightAntiSemiHashJoin.h"

#include "gpos/base.h"
#include "gpos/memory/CAutoMemoryPool.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CLogicalLeftAntiSemiJoin.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/operators/CPhysicalParallelRightAntiSemiHashJoin.h"
#include "gpopt/operators/CPhysicalParallelTableScan.h"
#include "gpopt/xforms/CXformUtils.h"
#include "gpopt/search/CGroupProxy.h"
#include "gpopt/search/CMemo.h"

// Forward declarations for gpdbwrappers functions
namespace gpdb {
	bool IsParallelModeOK(void);
}

using namespace gpopt;


CXformLeftAntiSemiJoin2ParallelRightAntiSemiHashJoin::
	CXformLeftAntiSemiJoin2ParallelRightAntiSemiHashJoin(CMemoryPool *mp)
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
CXformLeftAntiSemiJoin2ParallelRightAntiSemiHashJoin::Exfp(
	CExpressionHandle &exprhdl) const
{
	if (!gpdb::IsParallelModeOK())
	{
		return CXform::ExfpNone;
	}

	if (COptCtxt::PoctxtFromTLS()->HasReplicatedTables())
	{
		return CXform::ExfpNone;
	}

	if (!COptCtxt::PoctxtFromTLS()->HasParallelOperators())
	{
		return CXform::ExfpNone;
	}

	CXform::EXformPromise promise =
		CXformUtils::ExfpLogicalJoin2PhysicalJoin(exprhdl);
	if (CXform::ExfpNone == promise)
	{
		return CXform::ExfpNone;
	}

	// EXCEPT is implemented as a LeftAntiSemiJoin with NULL-aware
	// "IS NOT DISTINCT FROM" (INDF) equalities; the right-anti flip cannot
	// honor that NULL-matching hash condition and forces an ORCA fallback.
	// Skip the RIGHT candidate for INDF predicates (see the non-parallel
	// CXformLeftAntiSemiJoin2RightAntiSemiHashJoin for details).
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
CXformLeftAntiSemiJoin2ParallelRightAntiSemiHashJoin::Transform(
	CXformContext *pxfctxt, CXformResult *pxfres, CExpression *pexpr) const
{
	GPOS_ASSERT(nullptr != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	if (!GPOS_FTRACE(EopttraceDisableParallelHashJoin))
	{
		CXformUtils::ImplementHashJoin<CPhysicalParallelRightAntiSemiHashJoin>(
			pxfctxt, pxfres, pexpr);
	}
}

// EOF
