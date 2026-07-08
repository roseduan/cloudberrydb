/*-------------------------------------------------------------------------
 *
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
 *
 * CXformImplementParallelSequence.cpp
 *
 * IDENTIFICATION
 *	  src/backend/gporca/libgpopt/src/xforms/CXformImplementParallelSequence.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "gpopt/xforms/CXformImplementParallelSequence.h"

#include "gpos/base.h"

#include "gpopt/operators/CLogicalSequence.h"
#include "gpopt/operators/CPatternMultiLeaf.h"
#include "gpopt/operators/CPhysicalParallelSequence.h"
#include "gpopt/xforms/CXformUtils.h"

using namespace gpopt;

namespace gpdb {
bool IsParallelModeOK(void);
}

// Use gpdbwrappers for parallel checks
extern int max_parallel_workers_per_gather;

//---------------------------------------------------------------------------
//	@function:
//		CXformImplementParallelSequence::CXformImplementParallelSequence
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformImplementParallelSequence::CXformImplementParallelSequence(CMemoryPool *mp)
	: CXformImplementation(
	// pattern
	GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CLogicalSequence(mp),
		GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternMultiLeaf(mp))))
{
}


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementParallelSequence::Exfp
//
//	@doc:
//		Compute promise of xform
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformImplementParallelSequence::Exfp(CExpressionHandle &exprhdl) const
{
	if (!gpdb::IsParallelModeOK())
	{
		return CXform::ExfpNone;
	}

	/* In a DML query no parallel scan will be generated, so a parallel
	 * Sequence would survive with a non-parallel CTE producer as child 0
	 * and be rejected in FValidContext. Avoid that by not producing the
	 * parallel Sequence at all. */
	if (COptCtxt::PoctxtFromTLS()->FDMLQuery())
	{
		return CXform::ExfpNone;
	}

	/* Subquery-level LIMIT lowers into a segment-local Limit, which is
	 * incompatible with worker-level parallel distribution. */
	if (COptCtxt::PoctxtFromTLS()->FHasSubqueryLimit())
	{
		return CXform::ExfpNone;
	}

	// Ordered-set aggregates (percentile_cont etc.) must not run under
	// worker-level parallelism; see COrderedAggPreprocessor::PexprPreprocess().
	if (COptCtxt::PoctxtFromTLS()->FHasOrderedAgg())
	{
		return CXform::ExfpNone;
	}

	/*
	 * Skip parallel sequence when any child contains a Foreign Scan or a
	 * replicated table.  Both force the corresponding CTE producer to run
	 * serially, so a parallel Sequence would survive with a non-parallel
	 * child and be rejected in FValidContext.
	 */
	const ULONG arity = exprhdl.Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CTableDescriptorHashSet *ptabdescset =
			exprhdl.DeriveTableDescriptor(ul);
		if (CXformUtils::FContainsForeignTable(ptabdescset) ||
			CXformUtils::FContainsReplicatedTable(ptabdescset))
		{
			return CXform::ExfpNone;
		}
	}

	return CXform::ExfpHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformImplementParallelSequence::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformImplementParallelSequence::Transform(CXformContext *pxfctxt, CXformResult *pxfres,
										   CExpression *pexpr) const
{
	GPOS_ASSERT(nullptr != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();

	ULONG ulParallelWorkers = 2;

	CExpressionArray *pdrgpexpr = pexpr->PdrgPexpr();
	pdrgpexpr->AddRef();

	if (max_parallel_workers_per_gather > 0)
		ulParallelWorkers = (ULONG)max_parallel_workers_per_gather;

	// create alternative expression
	CExpression *pexprAlt = GPOS_NEW(mp)
		CExpression(mp, GPOS_NEW(mp) CPhysicalParallelSequence(mp, ulParallelWorkers), pdrgpexpr);
	// add alternative to transformation result
	pxfres->Add(pexprAlt);
}


//	EOF
