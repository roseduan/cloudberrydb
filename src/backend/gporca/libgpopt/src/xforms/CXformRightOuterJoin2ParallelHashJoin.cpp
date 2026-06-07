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
//		CXformRightOuterJoin2ParallelHashJoin.cpp
//
//	@doc:
//		Transform right outer join to parallel hash join
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformRightOuterJoin2ParallelHashJoin.h"

#include "gpos/base.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/hints/CPlanHint.h"
#include "gpopt/operators/CLogicalRightOuterJoin.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/operators/CPhysicalParallelRightOuterHashJoin.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/optimizer/COptimizerConfig.h"
#include "gpopt/xforms/CXformUtils.h"

// Forward declarations for gpdbwrappers functions
namespace gpdb {
	bool IsParallelModeOK(void);
}

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformRightOuterJoin2ParallelHashJoin::CXformRightOuterJoin2ParallelHashJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformRightOuterJoin2ParallelHashJoin::CXformRightOuterJoin2ParallelHashJoin(
	CMemoryPool *mp)
	:  // pattern
	  CXformImplementation(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) CLogicalRightOuterJoin(mp),
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
//		CXformRightOuterJoin2ParallelHashJoin::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformRightOuterJoin2ParallelHashJoin::Exfp(CExpressionHandle &exprhdl) const
{
	// Check if parallel execution is enabled
	if (!gpdb::IsParallelModeOK())
	{
		return CXform::ExfpNone;
	}

	if (COptCtxt::PoctxtFromTLS()->HasReplicatedTables())
	{
		return CXform::ExfpNone;
	}

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
//		CXformRightOuterJoin2ParallelHashJoin::Transform
//
//	@doc:
//		Actual transformation
//
//		Mirrors CXformRightOuterJoin2HashJoin::Transform with its
//		stats-based heuristic to skip ROJ when inner >> outer.
//
//---------------------------------------------------------------------------
void
CXformRightOuterJoin2ParallelHashJoin::Transform(CXformContext *pxfctxt,
												CXformResult *pxfres,
												CExpression *pexpr) const
{
	GPOS_ASSERT(nullptr != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	// Only generate parallel hash join if not explicitly disabled
	if (GPOS_FTRACE(EopttraceDisableParallelHashJoin))
	{
		return;
	}

	// If the ROJ is being considered because of a join order hint, then add
	// the right outer hash join alternative regardless of the stats.
	CPlanHint *planhint =
		COptCtxt::PoctxtFromTLS()->GetOptimizerConfig()->GetPlanHint();
	if (nullptr != planhint && planhint->WasCreatedViaDirectedHint(pexpr))
	{
		CXformUtils::ImplementHashJoin<CPhysicalParallelRightOuterHashJoin>(
			pxfctxt, pxfres, pexpr);
		return;
	}

	const IStatistics *outerStats = (*pexpr)[0]->Pstats();
	const IStatistics *innerStats = (*pexpr)[1]->Pstats();

	if (nullptr == outerStats || nullptr == innerStats)
	{
		return;
	}

	// If the inner row estimate is an arbitary factor larger than the outer,
	// don't generate a ROJ alternative (same heuristic as non-parallel ROJ).
	CDouble outerRows = outerStats->Rows();
	CDouble outerWidth = outerStats->Width();
	CDouble innerRows = innerStats->Rows();
	CDouble innerWidth = innerStats->Width();

	CDouble confidenceFactor = 2 * (*pexpr)[1]->DeriveJoinDepth();
	if (innerRows * innerWidth * confidenceFactor > outerRows * outerWidth)
	{
		return;
	}

	CXformUtils::ImplementHashJoin<CPhysicalParallelRightOuterHashJoin>(
		pxfctxt, pxfres, pexpr);
}

// EOF
