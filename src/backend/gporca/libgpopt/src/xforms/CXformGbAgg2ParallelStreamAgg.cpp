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
 * CXformGbAgg2ParallelStreamAgg.cpp
 *
 * IDENTIFICATION
 *	  src/backend/gporca/libgpopt/src/xforms/CXformGbAgg2ParallelStreamAgg.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "gpopt/xforms/CXformGbAgg2ParallelStreamAgg.h"

#include "gpos/base.h"

#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CLogicalGbAgg.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/operators/CPhysicalParallelStreamAgg.h"
#include "gpopt/operators/CPhysicalStreamAgg.h"
#include "gpopt/xforms/CXformUtils.h"
#include "naucrates/md/IMDRelation.h"

// Use gpdbwrappers for parallel checks
extern int max_parallel_workers_per_gather;

// Forward declarations for gpdbwrappers functions
namespace gpdb
{
bool IsParallelModeOK(void);
}

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CXformGbAgg2ParallelStreamAgg::CXformGbAgg2ParallelStreamAgg
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformGbAgg2ParallelStreamAgg::CXformGbAgg2ParallelStreamAgg(CMemoryPool *mp)
	: CXformImplementation(
		  // pattern
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CLogicalGbAgg(mp),
			  GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)),
			  GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp))))
{
}

//---------------------------------------------------------------------------
//	@function:
//		CXformGbAgg2ParallelStreamAgg::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle
//		Only promise if parallel mode is enabled and grouping columns are sortable
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformGbAgg2ParallelStreamAgg::Exfp(CExpressionHandle &exprhdl) const
{
	// Check if parallel plans are enabled
	if (!gpdb::IsParallelModeOK())
	{
		return CXform::ExfpNone;
	}

	// Check if parallel group aggregation is disabled via trace flag
	if (GPOS_FTRACE(EopttraceDisableParallelGroupAgg))
	{
		return CXform::ExfpNone;
	}

	if (!COptCtxt::PoctxtFromTLS()->HasParallelOperators())
	{
		return CXform::ExfpNone;
	}

	// Replicated tables are now handled correctly via ReplicatedWorkers
	// distribution in PdsDerive and FValidContext, no need to disable here.

	CLogicalGbAgg *popAgg = CLogicalGbAgg::PopConvert(exprhdl.Pop());
	CColRefArray *colref_array = popAgg->Pdrgpcr();

	// Must have grouping columns for stream aggregation
	// and grouping columns must be sortable
	if (0 == colref_array->Size() ||
		!CUtils::FComparisonPossible(colref_array, IMDType::EcmptL) ||
		exprhdl.DeriveHasSubquery(1))
	{
		// no grouping columns, or no sort operators are available for grouping columns, or
		// agg functions use subquery arguments
		return CXform::ExfpNone;
	}

	// High promise for parallel stream aggregation
	return CXform::ExfpHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformGbAgg2ParallelStreamAgg::Transform
//
//	@doc:
//		Actual transformation
//		Creates a CPhysicalParallelStreamAgg with explicit worker count
//
//---------------------------------------------------------------------------
void
CXformGbAgg2ParallelStreamAgg::Transform(CXformContext *pxfctxt,
										  CXformResult *pxfres,
										  CExpression *pexpr) const
{
	GPOS_ASSERT(nullptr != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();
	CLogicalGbAgg *popAgg = CLogicalGbAgg::PopConvert(pexpr->Pop());

	// Skip single-stage Global parallel agg for replicated tables.
	// Each segment holds the full data; a single-stage parallel agg would
	// have every worker scan the full table, producing duplicate results
	// in Gather. Only two-stage (Local + Global) is correct.
	if (popAgg->Egbaggtype() == COperator::EgbaggtypeGlobal &&
		!CXformUtils::FMultiStageAgg(pexpr))
	{
		CExpression *pexprRel = (*pexpr)[0];
		CTableDescriptorHashSet *ptds = pexprRel->DeriveTableDescriptor();
		if (ptds)
		{
			CTableDescriptorHashSetIter iter(ptds);
			while (iter.Advance())
			{
				const CTableDescriptor *ptd = iter.Get();
				if (IMDRelation::EreldistrReplicated ==
					ptd->GetRelDistribution())
				{
					return;
				}
			}
		}
	}

	CColRefArray *colref_array = popAgg->Pdrgpcr();
	colref_array->AddRef();

	// Extract components
	CExpression *pexprRel = (*pexpr)[0];
	CExpression *pexprScalar = (*pexpr)[1];

	// AddRef children
	pexprRel->AddRef();
	pexprScalar->AddRef();

	CColRefArray *pdrgpcrArgDQA = popAgg->PdrgpcrArgDQA();
	if (pdrgpcrArgDQA != nullptr && 0 != pdrgpcrArgDQA->Size())
	{
		pdrgpcrArgDQA->AddRef();
	}

	// Determine parallel workers degree from GUC
	ULONG ulParallelWorkers = 2;  // default
	if (max_parallel_workers_per_gather > 0)
	{
		ulParallelWorkers = (ULONG) max_parallel_workers_per_gather;
	}

	// Create alternative expression with explicit parallel worker count
	CExpression *pexprAlt = GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp) CPhysicalParallelStreamAgg(
			mp, colref_array, popAgg->PdrgpcrMinimal(), popAgg->Egbaggtype(),
			popAgg->FGeneratesDuplicates(), pdrgpcrArgDQA,
			CXformUtils::FMultiStageAgg(pexpr),
			CXformUtils::FAggGenBySplitDQAXform(pexpr), popAgg->AggStage(),
			popAgg->FAggPushdown(),
			!CXformUtils::FLocalAggCreatedByEagerAggXform(pexpr),
			ulParallelWorkers),
		pexprRel, pexprScalar);

	// Add alternative to transformation result
	pxfres->Add(pexprAlt);
}

// EOF
