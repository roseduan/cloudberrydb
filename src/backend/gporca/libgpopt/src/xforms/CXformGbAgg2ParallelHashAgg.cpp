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
 * CXformGbAgg2ParallelHashAgg.cpp
 *
 * IDENTIFICATION
 *	  src/backend/gporca/libgpopt/src/xforms/CXformGbAgg2ParallelHashAgg.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "gpopt/xforms/CXformGbAgg2ParallelHashAgg.h"

#include "gpos/base.h"

#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CLogicalGbAgg.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/operators/CPhysicalHashAgg.h"
#include "gpopt/operators/CPhysicalParallelHashAgg.h"
#include "gpopt/xforms/CXformUtils.h"
#include "naucrates/md/IMDAggregate.h"
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
//		CXformGbAgg2ParallelHashAgg::CXformGbAgg2ParallelHashAgg
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformGbAgg2ParallelHashAgg::CXformGbAgg2ParallelHashAgg(CMemoryPool *mp)
	: CXformImplementation(
		  // pattern
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CLogicalGbAgg(mp),
			  GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)),
			  // we need to extract deep tree in the project list to check
			  // for existence of distinct agg functions
			  GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternTree(mp))))
{
}

//---------------------------------------------------------------------------
//	@function:
//		CXformGbAgg2ParallelHashAgg::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle
//		Only promise if parallel mode is enabled and grouping columns are hashable
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformGbAgg2ParallelHashAgg::Exfp(CExpressionHandle &exprhdl) const
{
	// Check if parallel plans are enabled
	if (!gpdb::IsParallelModeOK())
	{
		return CXform::ExfpNone;
	}

	// Check if parallel hash aggregation is disabled via trace flag
	if (GPOS_FTRACE(EopttraceDisableParallelHashAgg))
	{
		return CXform::ExfpNone;
	}

	if (!COptCtxt::PoctxtFromTLS()->HasParallelOperators())
	{
		return CXform::ExfpNone;
	}

	CLogicalGbAgg *popAgg = CLogicalGbAgg::PopConvert(exprhdl.Pop());
	CColRefArray *colref_array = popAgg->Pdrgpcr();

	// Must have grouping columns for hash aggregation
	if (0 == colref_array->Size())
	{
		return CXform::ExfpNone;
	}

	// Check if subquery in agg functions
	if (exprhdl.DeriveHasSubquery(1))
	{
		return CXform::ExfpNone;
	}

	// Check if grouping columns are hashable and comparable
	if (!CUtils::FComparisonPossible(colref_array, IMDType::EcmptEq) ||
		!CUtils::IsHashable(colref_array))
	{
		return CXform::ExfpNone;
	}

	// High promise for parallel hash aggregation.
	// Replicated tables are now handled correctly via ReplicatedWorkers
	// distribution: each segment's workers each hold a distinct subset of the
	// replicated data so there is no intra-segment duplication, and
	// CPhysicalAgg::PdsDerive propagates StrictReplicated upward so that the
	// Gather motion only collects from one segment.
	return CXform::ExfpHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformGbAgg2ParallelHashAgg::FApplicable
//
//	@doc:
//		Check if the transformation is applicable
//		Hash agg is not used with distinct agg functions
//		Hash agg is not used if agg function does not have prelim func
//		Hash agg is not used for ordered aggregate
//
//---------------------------------------------------------------------------
BOOL
CXformGbAgg2ParallelHashAgg::FApplicable(CExpression *pexpr)
{
	CExpression *pexprPrjList = (*pexpr)[1];
	ULONG arity = pexprPrjList->Arity();
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();

	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprPrjEl = (*pexprPrjList)[ul];
		CExpression *pexprAggFunc = (*pexprPrjEl)[0];
		CScalarAggFunc *popScAggFunc =
			CScalarAggFunc::PopConvert(pexprAggFunc->Pop());

		if (popScAggFunc->IsDistinct() ||
			!md_accessor->RetrieveAgg(popScAggFunc->MDId())->IsHashAggCapable())
		{
			return false;
		}
	}

	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformGbAgg2ParallelHashAgg::Transform
//
//	@doc:
//		Actual transformation
//		Creates a CPhysicalHashAgg that will extract worker count from child
//		during optimization (via FValidContext)
//
//---------------------------------------------------------------------------
void
CXformGbAgg2ParallelHashAgg::Transform(CXformContext *pxfctxt,
										CXformResult *pxfres,
										CExpression *pexpr) const
{
	GPOS_ASSERT(nullptr != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	// Hash agg is not used with distinct agg functions
	// Hash agg is not used if agg function does not have prelim func
	// Hash agg is not used for ordered aggregate
	// Evaluating these conditions needs a deep tree in the project list
	if (!FApplicable(pexpr))
	{
		return;
	}

	CMemoryPool *mp = pxfctxt->Pmp();
	CLogicalGbAgg *popAgg = CLogicalGbAgg::PopConvert(pexpr->Pop());

	// Skip parallel agg entirely when child subtree contains a replicated table.
	// Replicated tables hold full data on every segment. The Local agg's
	// FValidContext requires worker-level distribution from its child, but the
	// optimizer may pick CPhysicalTableScan (segment-level Replicated) as best
	// child — leaving no fallback and crashing PexprExtractPlan. Falling back
	// to non-parallel HashAgg/StreamAgg is correct and safe for replicated tables.
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
		GPOS_ASSERT(nullptr != pdrgpcrArgDQA);
		pdrgpcrArgDQA->AddRef();
	}

	// Determine parallel workers degree from GUC
	ULONG ulParallelWorkers = 2;  // default
	if (max_parallel_workers_per_gather > 0)
	{
		ulParallelWorkers = (ULONG) max_parallel_workers_per_gather;
	}

	// Create alternative expression with explicit parallel worker count
	// CPhysicalParallelHashAgg stores worker count directly in member variable
	CExpression *pexprAlt = GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp) CPhysicalParallelHashAgg(
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
