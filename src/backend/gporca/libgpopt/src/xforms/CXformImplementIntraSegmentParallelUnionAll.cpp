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

#include "gpopt/xforms/CXformImplementIntraSegmentParallelUnionAll.h"

#include "gpos/base.h"

#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalUnionAll.h"
#include "gpopt/operators/CPatternMultiLeaf.h"
#include "gpopt/operators/CPhysicalParallelUnionAll.h"
#include "gpopt/xforms/CXformUtils.h"

extern int max_parallel_workers_per_gather;
namespace gpdb
{
bool IsParallelModeOK(void);
}

using namespace gpopt;

CXformImplementIntraSegmentParallelUnionAll::
	CXformImplementIntraSegmentParallelUnionAll(CMemoryPool *mp)
	: CXformImplementation(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) CLogicalUnionAll(mp),
		  GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternMultiLeaf(mp))))
{
}

CXform::EXformPromise
CXformImplementIntraSegmentParallelUnionAll::Exfp(
	CExpressionHandle &  // exprhdl
) const
{
	// Check 1: Is parallel mode enabled and OK?
	if (!gpdb::IsParallelModeOK())
	{
		return CXform::ExfpNone;
	}

	if (GPOS_FTRACE(EopttraceEnableParallelAppend))
	{
		return CXform::ExfpNone;
	}

	// Subquery-level LIMIT lowers into a segment-local Limit, which is
	// incompatible with worker-level parallel distribution.
	if (COptCtxt::PoctxtFromTLS()->FHasSubqueryLimit())
	{
		return CXform::ExfpNone;
	}

	return CXform::ExfpHigh;
}

void
CXformImplementIntraSegmentParallelUnionAll::Transform(
	CXformContext *pxfctxt, CXformResult *pxfres, CExpression *pexpr) const
{
	GPOS_ASSERT(nullptr != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();

	CLogicalUnionAll *popUnionAll = CLogicalUnionAll::PopConvert(pexpr->Pop());

	CColRefArray *pdrgpcrOutput = popUnionAll->PdrgpcrOutput();
	CColRef2dArray *pdrgpdrgpcrInput = popUnionAll->PdrgpdrgpcrInput();

	// Check datatype compatibility
	if (!CXformUtils::FSameDatatype(pdrgpdrgpcrInput))
	{
		return;
	}

	// Copy children expressions
	CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
	const ULONG arity = pexpr->Arity();

	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprChild = (*pexpr)[ul];
		pexprChild->AddRef();
		pdrgpexpr->Append(pexprChild);
	}

	// Get parallel worker count
	ULONG ulParallelWorkers = (ULONG) max_parallel_workers_per_gather;

	// Mark that we have parallel operators
	COptCtxt::PoctxtFromTLS()->SetHasParallelOperators();

	// Create physical operator with parallel workers
	pdrgpcrOutput->AddRef();
	pdrgpdrgpcrInput->AddRef();

	CPhysicalParallelUnionAll *popPhysical =
		GPOS_NEW(mp) CPhysicalParallelUnionAll(mp, pdrgpcrOutput,
											   pdrgpdrgpcrInput, ulParallelWorkers);

	CExpression *pexprResult =
		GPOS_NEW(mp) CExpression(mp, popPhysical, pdrgpexpr);

	pxfres->Add(pexprResult);
}
