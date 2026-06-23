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
//		CXformLeftSemiJoin2ParallelRightSemiHashJoin.cpp
//
//	@doc:
//		Transform left semi join to parallel right semi hash join
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformLeftSemiJoin2ParallelRightSemiHashJoin.h"

#include "gpos/base.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CLogicalLeftSemiJoin.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/operators/CPhysicalParallelRightSemiHashJoin.h"
#include "gpopt/operators/CPhysicalParallelTableScan.h"
#include "gpopt/xforms/CXformUtils.h"
#include "gpopt/search/CGroupProxy.h"
#include "gpopt/search/CMemo.h"

// Forward declarations for gpdbwrappers functions
namespace gpdb {
	bool IsParallelModeOK(void);
}

using namespace gpopt;


CXformLeftSemiJoin2ParallelRightSemiHashJoin::
	CXformLeftSemiJoin2ParallelRightSemiHashJoin(CMemoryPool *mp)
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


CXform::EXformPromise
CXformLeftSemiJoin2ParallelRightSemiHashJoin::Exfp(
	CExpressionHandle &exprhdl) const
{
	// Same gating as parallel left semi: parallel mode enabled + has parallel
	// operators + no replicated tables
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

	// RIGHT_SEMI builds on the outer (left) child, executed before the probe
	// (inner) child.  If the outer child has a partitioned-table consumer
	// eligible for dynamic partition elimination, building on it would defeat
	// DPE (the partition selector on the probe side runs too late).  Skip the
	// RIGHT candidate so the LEFT semi hash join, which prunes correctly, wins.
	if (exprhdl.DerivePartitionInfo(0)->UlConsumers() > 0)
	{
		return CXform::ExfpNone;
	}

	return promise;
}


void
CXformLeftSemiJoin2ParallelRightSemiHashJoin::Transform(
	CXformContext *pxfctxt, CXformResult *pxfres, CExpression *pexpr) const
{
	GPOS_ASSERT(nullptr != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	if (!GPOS_FTRACE(EopttraceDisableParallelHashJoin))
	{
		CXformUtils::ImplementHashJoin<CPhysicalParallelRightSemiHashJoin>(
			pxfctxt, pxfres, pexpr);
	}
}

// EOF
