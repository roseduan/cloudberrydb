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
//		CPhysicalParallelStreamAgg.cpp
//
//	@doc:
//		Implementation of parallel stream aggregation operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalParallelStreamAgg.h"

#include "gpos/base.h"

#include "gpopt/base/CDistributionSpecAny.h"
#include "gpopt/base/CDistributionSpecHashed.h"
#include "gpopt/base/CDistributionSpecHashedWorker.h"
#include "gpopt/base/CDistributionSpecSingleton.h"
#include "gpopt/base/CDistributionSpecWorkerRandom.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "naucrates/md/IMDFunction.h"
#include "naucrates/md/IMDRelation.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelStreamAgg::CPhysicalParallelStreamAgg
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalParallelStreamAgg::CPhysicalParallelStreamAgg(
	CMemoryPool *mp, CColRefArray *colref_array, CColRefArray *pdrgpcrMinimal,
	COperator::EGbAggType egbaggtype, BOOL fGeneratesDuplicates,
	CColRefArray *pdrgpcrArgDQA, BOOL fMultiStage, BOOL isAggFromSplitDQA,
	CLogicalGbAgg::EAggStage aggStage, BOOL isAggPushdown,
	BOOL should_enforce_distribution, ULONG ulParallelWorkers)
	: CPhysicalStreamAgg(mp, colref_array, pdrgpcrMinimal, egbaggtype,
						 fGeneratesDuplicates, pdrgpcrArgDQA, fMultiStage,
						 isAggFromSplitDQA, aggStage, isAggPushdown,
						 should_enforce_distribution)
{
	GPOS_ASSERT(ulParallelWorkers > 0 &&
				"CPhysicalParallelStreamAgg requires workers > 0");
	m_ulParallelWorkers = ulParallelWorkers;

	if (COperator::EgbaggtypeGlobal == egbaggtype)
	{
		SetDistrRequests(1);  // Global: single distribution request
	}
	else if (COperator::EgbaggtypeLocal == egbaggtype)
	{
		SetDistrRequests(1);  // Local: single distribution request (WorkerRandom only)
	}
	else
	{
		SetDistrRequests(0);  // Intermediate: not supported yet
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelStreamAgg::~CPhysicalParallelStreamAgg
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalParallelStreamAgg::~CPhysicalParallelStreamAgg() = default;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelStreamAgg::Matches
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL
CPhysicalParallelStreamAgg::Matches(COperator *pop) const
{
	if (pop->Eopid() != Eopid())
	{
		return false;
	}

	CPhysicalParallelStreamAgg *popParallelStreamAgg =
		CPhysicalParallelStreamAgg::PopConvert(pop);

	// Match parallel worker count
	if (m_ulParallelWorkers != popParallelStreamAgg->UlParallelWorkers())
	{
		return false;
	}

	// Match base aggregate properties via parent class
	return CPhysicalStreamAgg::Matches(pop);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelStreamAgg::HashValue
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG
CPhysicalParallelStreamAgg::HashValue() const
{
	// Combine base hash with parallel worker count
	ULONG ulHash = CPhysicalStreamAgg::HashValue();
	ulHash = gpos::CombineHashes(ulHash, gpos::HashValue(&m_ulParallelWorkers));

	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelStreamAgg::PdsRequired
//
//	@doc:
//		Compute required distribution of the n-th child
//		Parallel stream aggregate only supports Global aggregate
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalParallelStreamAgg::PdsRequired(CMemoryPool *mp,
										CExpressionHandle &exprhdl,
										CDistributionSpec *pdsRequired,
										ULONG child_index,
										CDrvdPropArray *pdrgpdpCtxt GPOS_UNUSED,
										ULONG ulOptReq) const
{
	GPOS_ASSERT(0 == child_index);
	// Handle Global aggregate type
	if (FGlobal())
	{
		return PdsRequiredForGlobal(mp, exprhdl, pdsRequired, child_index,
									ulOptReq);
	}

	if (exprhdl.NeedsSingletonExecution())
	{
		return PdsRequireSingleton(mp, exprhdl, pdsRequired, child_index);
	}

	// Handle Local aggregate type
	if (FLocal())
	{
		return PdsRequiredForLocal(mp, exprhdl, pdsRequired, child_index,
								   ulOptReq);
	}

	return GPOS_NEW(mp) CDistributionSpecRandom();

}

CDistributionSpec *
CPhysicalParallelStreamAgg::PdsRequiredForLocal(
	CMemoryPool *mp, CExpressionHandle &exprhdl GPOS_UNUSED,
	CDistributionSpec *pdsRequired GPOS_UNUSED, ULONG child_index GPOS_UNUSED,
	ULONG ulOptReq GPOS_ASSERTS_ONLY) const
{
	GPOS_ASSERT(FLocal());
	GPOS_ASSERT(0 == ulOptReq && "Local aggregate only supports single distribution request");

	if (m_pdrgpcrArgDQA != nullptr && 0 != m_pdrgpcrArgDQA->Size())
	{
		return PdsMaximalHashed(mp, m_pdrgpcrArgDQA);
	}

	/*
	 * When the query contains replicated tables, request an explicit
	 * worker-level distribution instead of ANY.  See the matching comment
	 * in CPhysicalParallelHashAgg::PdsRequiredForLocal.
	 */
	if (COptCtxt::PoctxtFromTLS()->HasReplicatedTables())
	{
		return CDistributionSpecWorkerRandom::PdsCreateWorkerRandom(
			mp, UlParallelWorkers());
	}

	// WorkerRandom distribution: each worker independently processes local data fragments
	// This allows maximum parallelism for partial aggregation
	return GPOS_NEW(mp) CDistributionSpecAny(this->Eopid());
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelStreamAgg::PdsRequiredForGlobal
//
//	@doc:
//		Compute required distribution for Global aggregate type
//		Global aggregates create HashedWorker distribution on grouping columns
//		for parallel final aggregation
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalParallelStreamAgg::PdsRequiredForGlobal(
	CMemoryPool *mp, CExpressionHandle &exprhdl GPOS_UNUSED,
	CDistributionSpec *pdsRequired GPOS_UNUSED, ULONG child_index GPOS_UNUSED,
	ULONG ulOptReq GPOS_UNUSED) const
{
	GPOS_ASSERT(FGlobal());
	GPOS_ASSERT(0 == ulOptReq &&
				"Global aggregate only supports single distribution request");

	if (exprhdl.HasOuterRefs())
	{
		return PdsPassThru(mp, exprhdl, pdsRequired, child_index);
	}

	if (0 == m_pdrgpcrMinimal->Size())
	{
		if (CDistributionSpec::EdtSingleton == pdsRequired->Edt())
		{
			pdsRequired->AddRef();
			return pdsRequired;
		}

		return GPOS_NEW(mp) CDistributionSpecSingleton();
	}

	if (0 == ulOptReq && (IMDFunction::EfsVolatile ==
						  exprhdl.DeriveFunctionProperties(0)->Efs()))
	{
		return GPOS_NEW(mp) CDistributionSpecSingleton();
	}

	ULONG ulWorkers = m_ulParallelWorkers;

	// Create hashed distribution on minimal grouping columns
	CDistributionSpec *pdsSpec = PdsMaximalHashed(mp, m_pdrgpcrMinimal);
	if (pdsSpec->Edt() == CDistributionSpec::EdtHashed)
	{
		CDistributionSpecHashed *pdsHashed =
			CDistributionSpecHashed::PdsConvert(pdsSpec);

		// Extract hash expressions and properties
		CExpressionArray *pdrgpexpr = pdsHashed->Pdrgpexpr();
		pdrgpexpr->AddRef();
		BOOL fNullsColocated = pdsHashed->FNullsColocated();
		IMdIdArray *opfamilies = pdsHashed->Opfamilies();
		if (nullptr != opfamilies)
		{
			opfamilies->AddRef();
		}

		// Release the temporary hashed distribution
		pdsHashed->Release();

		// Create HashedWorker distribution to require partial aggregation results
		return GPOS_NEW(mp) CDistributionSpecHashedWorker(
			pdrgpexpr, fNullsColocated, ulWorkers, opfamilies);
	}
	pdsSpec->Release();

	// Fallback: if there are grouping columns, require a hash distribution explicitly
	return PdsMaximalHashed(mp, m_pdrgpcrMinimal);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelStreamAgg::FValidContext
//
//	@doc:
//		Check if optimization context is valid;
//		Reject if parent requires REWINDABLE (e.g., for NL Join inner child)
//		because ParallelStreamAgg derives NONE (not rewindable)
//
//---------------------------------------------------------------------------
BOOL
CPhysicalParallelStreamAgg::FValidContext(
	CMemoryPool *mp GPOS_UNUSED, COptimizationContext *poc,
	COptimizationContextArray *pdrgpocChild) const
{
	GPOS_ASSERT(nullptr != poc);
	GPOS_ASSERT(nullptr != pdrgpocChild);
	GPOS_ASSERT(1 == pdrgpocChild->Size());

	CReqdPropPlan *prpp = poc->Prpp();
	CRewindabilitySpec *prsRequired = prpp->Per()->PrsRequired();

	// If parent requires REWINDABLE or higher, reject
	// ParallelHashAgg can only provide ErtNone
	if (prsRequired->IsOriginNLJoin())
	{
		// Parent requires rewindability (e.g., NL Join inner child)
		// but ParallelHashAgg cannot provide it
		// Reject this plan to avoid issues with parallel worker state
		return false;
	}

	// For Local aggregates, validate that child provides worker-level distribution
	if (FLocal())
	{
		COptimizationContext *pocChild = (*pdrgpocChild)[0];
		CGroupExpression *pgexprChild = pocChild->PgexprBest();
		if (pgexprChild)
		{
			CCostContext *pccBest = pocChild->PccBest();
			GPOS_ASSERT(nullptr != pccBest);
			CDrvdPropPlan *pdpplanChild = pccBest->Pdpplan();
			CDistributionSpec *pdsChild = pdpplanChild->Pds();

			// ReplicatedWorkers is also a valid worker-level distribution
			if (CDistributionSpec::EdtWorkerRandom != pdsChild->Edt() &&
				CDistributionSpec::EdtHashedWorker != pdsChild->Edt() &&
				CDistributionSpec::EdtReplicatedWorkers != pdsChild->Edt())
			{
				return false;
			}
		}
	}

	return true;
}

// EOF
