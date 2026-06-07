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
//		CXformLeftAntiSemiJoin2ParallelHashJoin.cpp
//
//	@doc:
//		Transform left anti semi join to parallel hash join
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformLeftAntiSemiJoin2ParallelHashJoin.h"

#include "gpos/base.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CLogicalLeftAntiSemiJoin.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/operators/CPhysicalParallelLeftAntiSemiHashJoin.h"
#include "gpopt/operators/CPhysicalParallelTableScan.h"
#include "gpopt/xforms/CXformUtils.h"
#include "gpopt/search/CGroupProxy.h"
#include "gpopt/search/CMemo.h"

// Forward declarations for gpdbwrappers functions
namespace gpdb {
	bool IsParallelModeOK(void);
}

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformLeftAntiSemiJoin2ParallelHashJoin::CXformLeftAntiSemiJoin2ParallelHashJoin
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CXformLeftAntiSemiJoin2ParallelHashJoin::CXformLeftAntiSemiJoin2ParallelHashJoin(
	CMemoryPool *mp)
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


//---------------------------------------------------------------------------
//	@function:
//		CXformLeftAntiSemiJoin2ParallelHashJoin::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformLeftAntiSemiJoin2ParallelHashJoin::Exfp(CExpressionHandle &exprhdl) const
{
	// Check if parallel execution is enabled
	// Uses gpdb::IsParallelModeOK() which checks max_parallel_workers_per_gather > 0
	if (!gpdb::IsParallelModeOK())
	{
		return CXform::ExfpNone;
	}

	// Check if the query has any parallel operators
	// Parallel hash join is only beneficial when parallel table scans exist
	if (!COptCtxt::PoctxtFromTLS()->HasParallelOperators())
	{
		return CXform::ExfpNone;
	}

	// Forbid the parallel hash join when the probe (outer) child is a
	// universal/general relation, i.e. it references no distributed table
	// (e.g. a full join of two universal generate_series/unnest views).  Such
	// a child holds a full copy on every worker; ORCA only deduplicates it at
	// the segment level (a gp_execution_segment() hash filter), which is
	// worker-blind.  As the probe side of a parallel hash join, every worker
	// on the surviving segment re-emits the whole probe, doubling the result.
	// The build (inner) side is unaffected -- a broadcast/universal build is
	// built once per segment -- so we only check the outer child here; the
	// commuted orientation (universal as the build side) and the non-parallel
	// hash join remain available and are correct.
	if (0 == exprhdl.DeriveTableDescriptor(0)->Size())
	{
		return CXform::ExfpNone;
	}

	// Use the same logic as regular hash join transformation
	return CXformUtils::ExfpLogicalJoin2PhysicalJoin(exprhdl);
}


//---------------------------------------------------------------------------
//	@function:
//		CXformLeftAntiSemiJoin2ParallelHashJoin::Transform
//
//	@doc:
//		actual transformation
//
//---------------------------------------------------------------------------
void
CXformLeftAntiSemiJoin2ParallelHashJoin::Transform(CXformContext *pxfctxt,
													CXformResult *pxfres,
													CExpression *pexpr) const
{
	GPOS_ASSERT(nullptr != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	// Only generate parallel hash join if not explicitly disabled
	if (!GPOS_FTRACE(EopttraceDisableParallelHashJoin))
	{
		CXformUtils::ImplementHashJoin<CPhysicalParallelLeftAntiSemiHashJoin>(
			pxfctxt, pxfres, pexpr);
	}
}

// EOF
