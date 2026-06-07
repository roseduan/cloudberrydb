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
//		CPhysicalParallelHashJoin.cpp
//
//	@doc:
//		Implementation of parallel hash join operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalParallelHashJoin.h"

#include "gpos/base.h"

#include "gpopt/base/CDistributionSpecAny.h"
#include "gpopt/base/CDistributionSpecHashed.h"
#include "gpopt/base/CDistributionSpecRandom.h"
#include "gpopt/base/CDistributionSpecReplicated.h"
#include "gpopt/base/CDistributionSpecSingleton.h"
#include "gpopt/base/CDistributionSpecNonSingleton.h"
#include "gpopt/base/CDistributionSpecNonReplicated.h"
#include "gpopt/base/CDistributionSpecReplicatedWorkers.h"
#include "gpopt/base/CDistributionSpecWorkerRandom.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPhysicalParallelTableScan.h"
#include "gpopt/search/CGroupProxy.h"
#include "gpopt/operators/CPhysicalParallelAppendTableScan.h"
#include "gpopt/operators/CPhysicalParallelCTEConsumer.h"

using namespace gpopt;

// GUC variable from PostgreSQL
extern int max_parallel_workers_per_gather;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelHashJoin::CPhysicalParallelHashJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalParallelHashJoin::CPhysicalParallelHashJoin(
	CMemoryPool *mp, CExpressionArray *pdrgpexprOuterKeys,
	CExpressionArray *pdrgpexprInnerKeys, IMdIdArray *hash_opfamilies,
	BOOL is_null_aware, CXform::EXformId origin_xform)
	: CPhysicalHashJoin(mp, pdrgpexprOuterKeys, pdrgpexprInnerKeys,
						hash_opfamilies, is_null_aware, origin_xform),
	  m_ulProbeWorkers(0),
	  m_ulBuildWorkers(0)
{
	// m_ulProbeWorkers and m_ulBuildWorkers will be extracted in FValidContext()
	// from child groups when optimization contexts are available

	// Parallel hash join uses one redistribute request plus the base hash
	// distribute requests; the base class' GPOPT_NON_HASH_DIST_REQUESTS
	// (hashed/broadcast, non-singleton/broadcast, singleton/singleton)
	// variants do not apply to the parallel variant.
	SetDistrRequests(1 + NumDistrReq());

	// Parallel hash join may force a Motion on either child (HashDistribute /
	// BroadcastWorkers), and a Motion is a hard barrier for partition
	// propagation.  With only the DPE request (ulOptReq=0 in
	// PppsRequiredForJoins) the child contexts always carry a consumer or
	// propagator requirement that cannot be satisfied across a worker Motion,
	// causing the optimizer to fall back to the Postgres planner.  Generate
	// the non-DPE alternative as well so a parallel plan without partition
	// propagation can be considered.
	SetPartPropagateRequests(2);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelHashJoin::~CPhysicalParallelHashJoin
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalParallelHashJoin::~CPhysicalParallelHashJoin()
{
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelHashJoin::UlExtractRequestedWorkers
//
//	@doc:
//		Extract requested worker count for distribution requirement.
//		Priority:
//		1. Child group's parallel operators (table scans, nested joins)
//		2. GUC max_parallel_workers_per_gather
//		3. Default fallback (2 workers)
//
//		This is used when requesting WorkerRandom distribution for the first child,
//		before it has been optimized.
//
//---------------------------------------------------------------------------
ULONG
CPhysicalParallelHashJoin::UlExtractRequestedWorkers(
	CExpressionHandle &exprhdl, ULONG child_index) const
{
	ULONG ulWorkers = 0;
	// Try to extract from child group if available
	CGroupExpression *pgexpr = exprhdl.Pgexpr();
	if (nullptr != pgexpr && child_index < pgexpr->Arity())
	{
		CGroup *pgroupChild = (*pgexpr)[child_index];
		if (nullptr != pgroupChild)
		{
			ulWorkers = CUtils::UlExtractWorkersFromGroup(pgroupChild, true /*fStopAtMotion*/);
			if (ulWorkers > 0)
			{
				return ulWorkers;
			}
		}
	}

	// Fall back to GUC setting if no parallel operators found in child
	if (max_parallel_workers_per_gather > 0)
	{
		return (ULONG)max_parallel_workers_per_gather;
	}

	// Default fallback
	return 2;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelHashJoin::ExtractWorkersIfNeeded
//
//	@doc:
//		Extract probe/build workers from child distributions
//		This is a legacy function kept for compatibility but no longer used
//		Worker extraction now happens in FValidContext()
//
//---------------------------------------------------------------------------
void
CPhysicalParallelHashJoin::ExtractWorkersIfNeeded(
	CExpressionHandle &exprhdl) const
{
	// Extract workers by scanning child groups for parallel operators
	GPOS_ASSERT(nullptr != exprhdl.Pgexpr() &&
		"ExtractWorkersIfNeeded requires group expression to access child groups");

	CGroup *pgroupOuter = exprhdl.Pgexpr()->Pdrgpgroup()->operator[](0);
	m_ulProbeWorkers = CUtils::UlExtractWorkersFromGroup(pgroupOuter, true /*fStopAtMotion*/);

	CGroup *pgroupInner = exprhdl.Pgexpr()->Pdrgpgroup()->operator[](1);
	m_ulBuildWorkers = CUtils::UlExtractWorkersFromGroup(pgroupInner, true /*fStopAtMotion*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelHashJoin::PdsRequiredRedistribute
//
//	@doc:
//		Compute required redistribute distribution spec for the n-th child
//		Wraps base class redistribution logic with WorkerRandom to preserve
//		worker-level parallelism
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalParallelHashJoin::PdsRequiredRedistributeParallel(
	CMemoryPool *mp, CExpressionHandle &exprhdl, CDistributionSpec *pdsInput GPOS_UNUSED,
	ULONG child_index, CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq) const
{
	if (FFirstChildToOptimize(child_index))
	{
		// require first child to provide a hashed distribution,
		return PdshashedRequired(mp, child_index, ulOptReq);
	}
	// Second child: match the first child's base distribution
	GPOS_ASSERT(nullptr != pdrgpdpCtxt && pdrgpdpCtxt->Size() > 0);

	CDistributionSpec *pdsFirst =
		CDrvdPropPlan::Pdpplan((*pdrgpdpCtxt)[0])->Pds();

	CDistributionSpec *pdsBaseFirst = pdsFirst;
	CDistributionSpec *pdsInputForMatch = nullptr;

	if (CDistributionSpec::EdtWorkerRandom == pdsFirst->Edt())
	{
		CDistributionSpecWorkerRandom *pdsWorkerFirst =
			CDistributionSpecWorkerRandom::PdsConvert(pdsFirst);
		pdsBaseFirst = pdsWorkerFirst->PdsSegmentBase();

		// If first child is WorkerRandom with hashed base, use PdsMatch directly
		if (nullptr != pdsBaseFirst &&
			CDistributionSpec::EdtHashed == pdsBaseFirst->Edt())
		{
			CDistributionSpecHashed *pdsHashedBase =
				CDistributionSpecHashed::PdsConvert(pdsBaseFirst);

			CDistributionSpecHashed *pdsHashed = pdsHashedBase->Copy(mp);
			pdsHashed->ComputeEquivHashExprs(mp, exprhdl);
			pdsInputForMatch = pdsHashed;
		}
	}
	else
	{
		pdsInputForMatch = pdsBaseFirst;
	}
	// find the index of the first child
	ULONG ulFirstChild = 0;
	if (EceoRightToLeft == Eceo())
	{
		ulFirstChild = 1;
	}

	// return a matching distribution request for the second child
	CDistributionSpec *pdsMatch = PdsMatch(mp, pdsInputForMatch, ulFirstChild);
	if (pdsBaseFirst->Edt() == CDistributionSpec::EdtHashed)
	{
		// if the input spec was created as a copy, release it
		pdsInputForMatch->Release();
	}
	return pdsMatch;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelHashJoin::PdsRequiredReplicateWorkers
//
//	@doc:
//		Create (hashed/non-singleton, broadcast workers) optimization request
//		Similar to PdsRequiredReplicate but uses ReplicatedWorkers distribution
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalParallelHashJoin::PdsRequiredReplicateWorkers(
	CMemoryPool *mp, CExpressionHandle &exprhdl, CDistributionSpec *pdsInput,
	ULONG child_index, CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq) const
{
	EChildExecOrder eceo GPOS_ASSERTS_ONLY = Eceo();
	GPOS_ASSERT(EceoRightToLeft == eceo);  // ParallelHashJoin always RightToLeft

	if (1 == child_index)
	{
		// Require inner child (build side) to be replicated to workers
		// Extract worker count from GUC or child group
		ULONG ulWorkers = UlExtractRequestedWorkers(exprhdl, child_index);
		return CDistributionSpecReplicatedWorkers::PdsCreate(
			mp, ulWorkers, false /*ignore_broadcast_threshold*/);
	}

	GPOS_ASSERT(0 == child_index);

	// Outer child (probe side): require a matching distribution
	CDistributionSpec *pdsInner =
		CDrvdPropPlan::Pdpplan((*pdrgpdpCtxt)[0])->Pds();
	GPOS_ASSERT(nullptr != pdsInner);

	if (CDistributionSpec::EdtUniversal == pdsInner->Edt())
	{
		return GPOS_NEW(mp) CDistributionSpecNonReplicated();
	}
	// Inner child must be ReplicatedWorkers for BroadcastWorkers requests
	GPOS_ASSERT(CDistributionSpec::EdtReplicatedWorkers == pdsInner->Edt() &&
				"Inner must be ReplicatedWorkers for BroadcastWorkers requests");

	CDistributionSpecReplicatedWorkers *pdsReplicatedInner =
		CDistributionSpecReplicatedWorkers::PdsConvert(pdsInner);
	ULONG ulWorkers = pdsReplicatedInner->UlWorkers();

	// First BroadcastWorkers request (H): Try to pass through hashed if available
	if (ulOptReq == NumDistrReq() &&
		CDistributionSpec::EdtHashed == pdsInput->Edt())
	{
		// Try to pass through hashed requirement to outer child
		CDistributionSpecHashed *pdshashedOuter = PdshashedPassThru(
			mp, exprhdl, CDistributionSpecHashed::PdsConvert(pdsInput),
			child_index, pdrgpdpCtxt, ulOptReq);
		if (nullptr != pdshashedOuter)
		{
			// Wrap in WorkerRandom to preserve parallel execution
			return CDistributionSpecWorkerRandom::PdsCreateWorkerRandom(
				mp, ulWorkers, pdshashedOuter);
		}
	}

	// Fallback: request worker-level non-singleton so a partitioned probe
	// (HashedWorker or WorkerRandom) can satisfy it without forcing a
	// redistribute motion.  fAllowReplicated is false so a broadcast probe --
	// whether segment-level Replicated or worker-level ReplicatedWorkers --
	// cannot satisfy this request: the outer stays partitioned while only the
	// inner (build) side is broadcast.
	return GPOS_NEW(mp) CDistributionSpecNonSingleton(false /*fAllowReplicated*/,
													 true /*fAllowWorker*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelHashJoin::Ped
//
//	@doc:
//		Compute enforced distribution property for the n-th child.
//		Overridden to preserve WorkerRandom distributions and avoid unnecessary
//		redistribute motion nodes that would collapse worker-level parallelism.
//
//		Request structure (similar to CPhysicalHashJoin::Ped):
//		- Req(1 to N): (redistribute, redistribute) wrapped in WorkerRandom
//		- Req(N+1, N+2): (hashed/non-singleton, replicate) - base class handles
//		- Req(N+3): (singleton, singleton) - base class handles
//
//---------------------------------------------------------------------------
CEnfdDistribution *
CPhysicalParallelHashJoin::Ped(CMemoryPool *mp, CExpressionHandle &exprhdl,
							   CReqdPropPlan *prppInput, ULONG child_index,
							   CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq)
{
	GPOS_ASSERT(2 > child_index);
	GPOS_ASSERT(ulOptReq < UlDistrRequests());

	CEnfdDistribution::EDistributionMatching dmatch =
		Edm(prppInput, child_index, pdrgpdpCtxt, ulOptReq);
	CDistributionSpec *const pdsInput = prppInput->Ped()->PdsRequired();

	const ULONG ulHashDistributeRequests = NumDistrReq();

	if (ulOptReq < ulHashDistributeRequests)
	{
		// requests 1 .. N are (redistribute, redistribute)
		// ParallelHashJoin wraps these in WorkerRandom to preserve parallelism
		CDistributionSpec *pds = nullptr;

		if (FFirstChildToOptimize(child_index))
		{
			// First child: request hashed distribution on join keys
			pds = PdsRequiredRedistribute(
				mp, exprhdl, pdsInput, child_index, pdrgpdpCtxt, ulOptReq);

			if (CDistributionSpec::EdtHashed == pds->Edt())
			{
				CDistributionSpecHashed::PdsConvert(pds)->ComputeEquivHashExprs(mp, exprhdl);
			}

			// Extract worker count dynamically from child group
			ULONG ulWorkers = UlExtractRequestedWorkers(exprhdl, child_index);

			// Wrap hashed distribution in WorkerRandom
			return GPOS_NEW(mp) CEnfdDistribution(
				CDistributionSpecWorkerRandom::PdsCreateWorkerRandom(mp, ulWorkers, pds), dmatch);
		}

		// Second child: match first child's distribution
		// If first child is WorkerRandom, preserve it by matching its base
		if (nullptr != pdrgpdpCtxt && pdrgpdpCtxt->Size() > 0)
		{
			CDistributionSpec *pdsFirst = CDrvdPropPlan::Pdpplan((*pdrgpdpCtxt)[0])->Pds();

			if (CDistributionSpec::EdtWorkerRandom == pdsFirst->Edt())
			{
				CDistributionSpecWorkerRandom *pdsWorkerFirst =
					CDistributionSpecWorkerRandom::PdsConvert(pdsFirst);
				CDistributionSpec *pdsSegmentBase = pdsWorkerFirst->PdsSegmentBase();
				ULONG ulWorkers = pdsWorkerFirst->UlWorkers();

				if (nullptr != pdsSegmentBase &&
					CDistributionSpec::EdtHashed == pdsSegmentBase->Edt())
				{
					CDistributionSpecHashed *pdsHashedBase =
						CDistributionSpecHashed::PdsConvert(pdsSegmentBase);

					// Check if first child's base is covered by its join keys
					const CExpressionArray *pdrgpexprFirstKeys =
						(EceoRightToLeft == Eceo()) ? PdrgpexprInnerKeys() : PdrgpexprOuterKeys();

					if (!pdsHashedBase->IsCoveredBy(pdrgpexprFirstKeys))
					{
						// Base not covered - use PdsRequiredRedistributeParallel
						pds = PdsRequiredRedistributeParallel(
							mp, exprhdl, pdsInput, child_index, pdrgpdpCtxt, ulOptReq);

						if (CDistributionSpec::EdtHashed == pds->Edt())
						{
							CDistributionSpecHashed::PdsConvert(pds)->ComputeEquivHashExprs(mp, exprhdl);
						}
						return GPOS_NEW(mp) CEnfdDistribution(pds, dmatch);
					}

					// Base is covered - create matching hashed distribution
					ULONG ulFirstChild = (EceoRightToLeft == Eceo()) ? 1 : 0;
					CDistributionSpecHashed *pdsMatch =
						PdshashedMatching(mp, pdsHashedBase, ulFirstChild);
					pdsMatch->ComputeEquivHashExprs(mp, exprhdl);

					// Wrap in WorkerRandom to match first child
					return GPOS_NEW(mp) CEnfdDistribution(
						CDistributionSpecWorkerRandom::PdsCreateWorkerRandom(
							mp, ulWorkers, pdsMatch), dmatch);
				}
			}
		}

		// Default: use base class logic for non-WorkerRandom first child
		pds = PdsRequiredRedistribute(
			mp, exprhdl, pdsInput, child_index, pdrgpdpCtxt, ulOptReq);

		if (CDistributionSpec::EdtHashed == pds->Edt())
		{
			CDistributionSpecHashed::PdsConvert(pds)->ComputeEquivHashExprs(mp, exprhdl);
		}
		return GPOS_NEW(mp) CEnfdDistribution(pds, dmatch);
	}

	if (ulOptReq == ulHashDistributeRequests ||
		ulOptReq == ulHashDistributeRequests + 1)
	{
		// requests N, N+1 are BroadcastWorkers requests
		// Use PdsRequiredReplicateWorkers to generate EdtReplicatedWorkers (worker-level)
		// instead of EdtReplicated (segment-level) from base class
		CDistributionSpec *pds =
			PdsRequiredReplicateWorkers(mp, exprhdl, pdsInput, child_index,
										pdrgpdpCtxt, ulOptReq);

		if (CDistributionSpec::EdtHashed == pds->Edt())
		{
			CDistributionSpecHashed::PdsConvert(pds)->ComputeEquivHashExprs(mp, exprhdl);
		}

		return GPOS_NEW(mp) CEnfdDistribution(pds, dmatch);
	}

	GPOS_ASSERT(ulOptReq == ulHashDistributeRequests + 2);

	// request N+3 is (singleton, singleton)
	// Singleton execution is incompatible with parallelism
	return CPhysicalHashJoin::Ped(mp, exprhdl, prppInput, child_index,
								  pdrgpdpCtxt, ulOptReq);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelHashJoin::PdsRequired
//
//	@doc:
//		Compute required distribution of the n-th child
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalParallelHashJoin::PdsRequired(CMemoryPool *mp GPOS_UNUSED,
									   CExpressionHandle &exprhdl GPOS_UNUSED,
									   CDistributionSpec *pdsRequired GPOS_UNUSED,
									   ULONG child_index GPOS_UNUSED,
									   CDrvdPropArray *pdrgpdpCtxt GPOS_UNUSED,
									   ULONG ulOptReq GPOS_UNUSED) const
{
	GPOS_RAISE(
		CException::ExmaInvalid, CException::ExmiInvalid,
		GPOS_WSZ_LIT("PdsRequired should not be called for CPhysicalParallelHashJoin"));
	return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelHashJoin::PrsRequired
//
//	@doc:
//		Compute required rewindability of the n-th child
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysicalParallelHashJoin::PrsRequired(CMemoryPool *mp,
									   CExpressionHandle &exprhdl,
									   CRewindabilitySpec *prsRequired,
									   ULONG child_index,
									   CDrvdPropArray *pdrgpdpCtxt,
									   ULONG ulOptReq) const
{
	GPOS_ASSERT(child_index < 2);

	// Parallel hash join requires same rewindability as traditional hash join
	// Inner child is materialized by Hash node, outer child passes through
	return CPhysicalHashJoin::PrsRequired(mp, exprhdl, prsRequired,
										  child_index, pdrgpdpCtxt, ulOptReq);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelHashJoin::PdsDerive
//
//	@doc:
//		Derive distribution for parallel hash join
//
//		Strategy (two-tier priority):
//		1. Handle WorkerRandom distributions (parallel-specific)
//		2. Fall back to traditional hash join logic (from CPhysicalHashJoin)
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalParallelHashJoin::PdsDerive(CMemoryPool *mp,
									 CExpressionHandle &exprhdl) const
{
	// Get distributions from children
	CDistributionSpec *pdsOuter = exprhdl.Pdpplan(0)->Pds();
	CDistributionSpec *pdsInner = exprhdl.Pdpplan(1)->Pds();

	// ========== Priority 1: Handle Parallel-specific Distributions ==========

	// Case 1: Inner child is ReplicatedWorkers (BroadcastWorkers scenario)
	// This means the inner table is small and replicated to all workers
	// Join output distribution follows the outer child's distribution
	if (CDistributionSpec::EdtReplicatedWorkers == pdsInner->Edt())
	{
		// Inner is replicated to workers, outer's distribution is preserved
		// Join result follows outer's distribution (similar to segment-level broadcast)
		pdsOuter->AddRef();
		return pdsOuter;
	}

	// Case 2: Both children are WorkerRandom (HashDistributeWorkers scenario)
	if (CDistributionSpec::EdtWorkerRandom == pdsOuter->Edt() &&
		CDistributionSpec::EdtWorkerRandom == pdsInner->Edt())
	{
		CDistributionSpecWorkerRandom *pdsWorkerOuter =
			CDistributionSpecWorkerRandom::PdsConvert(pdsOuter);
		CDistributionSpecWorkerRandom *pdsWorkerInner =
			CDistributionSpecWorkerRandom::PdsConvert(pdsInner);

		// Verify worker count matches
		//if (pdsWorkerOuter->UlWorkers() == pdsWorkerInner->UlWorkers())
		{
			// Verify segment-level base distributions are compatible
			CDistributionSpec *pdsBaseOuter = pdsWorkerOuter->PdsSegmentBase();
			CDistributionSpec *pdsBaseInner = pdsWorkerInner->PdsSegmentBase();

			if (nullptr != pdsBaseOuter && nullptr != pdsBaseInner)
			{
				// If both bases are hashed, check if they cover join keys
				if (CDistributionSpec::EdtHashed == pdsBaseOuter->Edt() &&
					CDistributionSpec::EdtHashed == pdsBaseInner->Edt())
				{
					CDistributionSpecHashed *pdsHashedOuter =
						CDistributionSpecHashed::PdsConvert(pdsBaseOuter);
					CDistributionSpecHashed *pdsHashedInner =
						CDistributionSpecHashed::PdsConvert(pdsBaseInner);

					// Check segment-level co-location
					if (pdsHashedOuter->IsCoveredBy(PdrgpexprOuterKeys()) &&
						pdsHashedInner->IsCoveredBy(PdrgpexprInnerKeys()))
					{
						// Perfect match! Return outer's WorkerRandom distribution
						pdsOuter->AddRef();
						return pdsOuter;
					}
				}
				// If both bases are Random, they're compatible
				else if (CDistributionSpec::EdtRandom == pdsBaseOuter->Edt() &&
						 CDistributionSpec::EdtRandom == pdsBaseInner->Edt())
				{
					pdsOuter->AddRef();
					return pdsOuter;
				}
			}
		}

		// Worker count mismatch or base incompatible - fall back to outer
		// Enforcement will add Motion if needed
		pdsOuter->AddRef();
		return pdsOuter;
	}

	// Case 2: Only outer is WorkerRandom - check if base matches join keys
	if (CDistributionSpec::EdtWorkerRandom == pdsOuter->Edt())
	{
		// Return outer's distribution
		// Inner will be redistributed to match (handled by PdsRequired)
		pdsOuter->AddRef();
		return pdsOuter;
	}

	// ========== Priority 2: Traditional Distributions ==========
	// For non-WorkerRandom cases (e.g., from Motion nodes), use parent class logic
	// which correctly handles:
	// 1. Replicated/Universal outer → return inner distribution
	// 2. Cleanup of incomplete Hashed distribution specs
	// 3. Right outer join distribution swap
	return CPhysicalJoin::PdsDerive(mp, exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelHashJoin::PrsDerive
//
//	@doc:
//		Derive rewindability
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysicalParallelHashJoin::PrsDerive(CMemoryPool *mp,
									 CExpressionHandle &exprhdl
) const
{
	return CPhysicalJoin::PrsDerive(mp, exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelHashJoin::EpetDistribution
//
//	@doc:
//		Return the enforcing type for distribution property based on this
//		operator
//
//		Key scenarios handled:
//		1. Direct match: Required WorkerRandom[N] == Derived WorkerRandom[N]
//		2. Segment base match: Required Hashed(a) matches our WorkerRandom's
//		   segment base Hashed(a) - critical for being used as inner child
//		   of another hash join (see doc section 8.2)
//		3. Mismatch: Motion required to enforce required distribution
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalParallelHashJoin::EpetDistribution(CExpressionHandle &exprhdl,
											 const CEnfdDistribution *ped) const
{
	GPOS_ASSERT(nullptr != ped);

	// Get our derived distribution (returned by PdsDerive)
	CDistributionSpec *pdsDerived = CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Pds();

	// Case 1: Our derived distribution directly satisfies the requirement
	// Examples:
	//   - Required: WorkerRandom[2] base:Hashed(a)
	//     Derived:  WorkerRandom[2] base:Hashed(a) → Match!
	if (ped->FCompatible(pdsDerived))
	{
		return CEnfdProp::EpetUnnecessary;
	}

	// Case 2: No distribution satisfies the requirement
	// Motion enforcement will be needed on the output
	// Examples:
	//   - Required: WorkerRandom[4], Derived: WorkerRandom[2] (worker mismatch)
	//   - Required: Hashed(c), Derived: WorkerRandom[2] base:Hashed(a) (key mismatch)
	return CEnfdProp::EpetRequired;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelHashJoin::FValidContext
//
//	@doc:
//		Check if optimization context is valid;
//		Reject if parent requires REWINDABLE (e.g., for NL Join inner child)
//		because ParallelHashJoin derives NONE (not rewindable)
//
//---------------------------------------------------------------------------
BOOL
CPhysicalParallelHashJoin::FValidContext(
	CMemoryPool *,  // mp
	COptimizationContext *poc,
	COptimizationContextArray *pdrgpocChild) const
{
	GPOS_ASSERT(nullptr != poc);

	// Check if parent requires rewindability
	CReqdPropPlan *prpp = poc->Prpp();
	CRewindabilitySpec *prsRequired = prpp->Per()->PrsRequired();

	// Parallel hash join is non-rewindable (derives ErtNone)
	// If parent requires REWINDABLE (e.g., NL join inner child), reject early.
	if (prsRequired->IsOriginNLJoin())
	{
		// Parent requires rewindability but ParallelHashJoin cannot provide it.
		// Reject this plan so the optimizer can choose alternatives or add Spool.
		return false;
	}

	// Extract workers from child groups if not already extracted
	if (0 == m_ulProbeWorkers || 0 == m_ulBuildWorkers)
	{
		if (nullptr != pdrgpocChild && pdrgpocChild->Size() >= 2)
		{
			COptimizationContext *pocOuter = (*pdrgpocChild)[0];
			COptimizationContext *pocInner = (*pdrgpocChild)[1];

			if (nullptr != pocOuter && nullptr != pocInner)
			{
				CGroup *pgroupOuter = pocOuter->Pgroup();
				CGroup *pgroupInner = pocInner->Pgroup();

				if (nullptr != pgroupOuter && nullptr != pgroupInner)
				{
					m_ulProbeWorkers = CUtils::UlExtractWorkersFromGroup(pgroupOuter, true /*fStopAtMotion*/);
					m_ulBuildWorkers = CUtils::UlExtractWorkersFromGroup(pgroupInner, true /*fStopAtMotion*/);
				}
			}
		}

		// Validate extracted worker counts
		if (0 == m_ulProbeWorkers || 0 == m_ulBuildWorkers)
		{
			// Invalid worker counts extracted - reject this optimization context
			return false;
		}
	}

	/* Reject when a child's best plan delivers singleton distribution.
	 * A parallel hash join needs both children to run in a parallel context
	 * with workers.  If a child (e.g. full hash join of two universal
	 * relations) delivers SINGLETON, the parallel context has 0 workers and
	 * BroadcastWorkers motion targets would be 0, causing "cannot execute
	 * inactive Motion" errors at runtime. */
	if (nullptr != pdrgpocChild && pdrgpocChild->Size() >= 2)
	{
		for (ULONG ul = 0; ul < 2; ul++)
		{
			COptimizationContext *pocChild = (*pdrgpocChild)[ul];
			if (nullptr != pocChild && nullptr != pocChild->PccBest())
			{
				CDrvdPropPlan *pdpplan = pocChild->PccBest()->Pdpplan();
				if (nullptr != pdpplan)
				{
					CDistributionSpec *pds = pdpplan->Pds();
					if (pds->FSingletonOrStrictSingleton() ||
						CDistributionSpec::EdtUniversal == pds->Edt())
					{
						return false;
					}
				}
			}
		}
	}

	/* Probe (outer) side must actually run worker-parallel in THIS context.
	 *
	 * m_ulProbeWorkers extracted above via UlExtractWorkersFromGroup() is
	 * group-level: it returns a positive worker count whenever ANY expression
	 * in the outer group is a worker-level parallel source (e.g. the item
	 * ParallelTableScan), even when the best plan chosen for this context is a
	 * non-parallel subtree -- a plain Hash Join whose store_sales scan carries
	 * no worker motion.  Such an outer derives a segment-level (non-worker)
	 * distribution, so the parallel hash join above it executes once per worker
	 * per segment and doubles the join output.
	 *
	 * Reject the context unless the outer's best plan actually derives a
	 * worker-partitioned distribution (HashedWorker or WorkerRandom), i.e. a
	 * probe parallel degree >= 2.  Only the probe side is constrained here; the
	 * build side may stay segment-level Hashed with a worker-shared hash table.
	 */
	if (nullptr != pdrgpocChild && pdrgpocChild->Size() >= 2)
	{
		COptimizationContext *pocOuter = (*pdrgpocChild)[0];
		if (nullptr != pocOuter && nullptr != pocOuter->PccBest())
		{
			CDrvdPropPlan *pdpplanOuter = pocOuter->PccBest()->Pdpplan();
			if (nullptr != pdpplanOuter)
			{
				const CDistributionSpec::EDistributionType dtOuterDrvd =
					pdpplanOuter->Pds()->Edt();
				if (CDistributionSpec::EdtHashedWorker != dtOuterDrvd &&
					CDistributionSpec::EdtWorkerRandom != dtOuterDrvd)
				{
					return false;
				}
			}
		}
	}

	// Lightweight pruning based on children required distributions (when available).
	// Do NOT attempt to inspect derived child properties here. We only reject
	// obviously incompatible contexts to reduce search:
	//   - Disallow singleton/replicated requirements for either child (parallel hash
	//     join expects segment-distributed inputs).
	//   - The outer/probe side should allow parallelism: its required distribution
	//     must be Either Any or WorkerRandom. If it is explicitly constrained to a
	//     non-parallel distribution, prune this context.
	if (nullptr != pdrgpocChild && pdrgpocChild->Size() >= 2)
	{
		CEnfdDistribution *pedOuter = (*pdrgpocChild)[0]->Prpp()->Ped();
		CEnfdDistribution *pedInner = (*pdrgpocChild)[1]->Prpp()->Ped();
		CDistributionSpec *pdsOuterReq = pedOuter->PdsRequired();
		CDistributionSpec *pdsInnerReq = pedInner->PdsRequired();

		const CDistributionSpec::EDistributionType dtOuter = pdsOuterReq->Edt();
		const CDistributionSpec::EDistributionType dtInner = pdsInnerReq->Edt();

		// Reject singleton or replicated requirements on either child
		if (dtOuter == CDistributionSpec::EdtSingleton ||
			dtOuter == CDistributionSpec::EdtStrictSingleton ||
			dtOuter == CDistributionSpec::EdtReplicated ||
			dtOuter == CDistributionSpec::EdtStrictReplicated ||
			dtInner == CDistributionSpec::EdtSingleton ||
			dtInner == CDistributionSpec::EdtStrictSingleton ||
			dtInner == CDistributionSpec::EdtReplicated ||
			dtInner == CDistributionSpec::EdtStrictReplicated)
		{
			return false;
		}

		// Outer/probe side must allow parallelism: require Any or WorkerRandom.
		// A NonSingleton requirement that allows worker-level distribution is
		// also acceptable: it is the outer requirement produced by a
		// BroadcastWorkers request (see PdsRequiredReplicateWorkers), and a
		// worker-parallel outer plan satisfies it without forcing a redistribute
		// motion.  Without this, the BroadcastWorkers alternative is always
		// rejected here and a small inner can never be broadcast in parallel.
		BOOL fOuterNonSingletonAllowsWorker =
			CDistributionSpec::EdtNonSingleton == dtOuter &&
			CDistributionSpecNonSingleton::PdsConvert(pdsOuterReq)->FAllowWorker();
		if (dtOuter != CDistributionSpec::EdtAny &&
			dtOuter != CDistributionSpec::EdtWorkerRandom &&
			dtOuter != CDistributionSpec::EdtHashedWorker &&
			!fOuterNonSingletonAllowsWorker)
		{
			return false;
		}

		// Inner/build side validation:
		// - We allow Any, WorkerRandom, or Hashed distributions
		// - Hashed is allowed because parallel hash join can work when:
		//   1. Both children have compatible segment-level hashed distributions
		//   2. Workers share the hash table built from segment-local data
		// - We only reject obviously incompatible distributions
		//
		// Note: We do NOT require inner to be WorkerRandom. The EpetDistribution()
		// logic (lines 541-593) handles cases where inner has HASHED distribution
		// and our derived WorkerRandom's base distribution satisfies parent requirements.
		if (dtInner != CDistributionSpec::EdtAny &&
			dtInner != CDistributionSpec::EdtWorkerRandom &&
			dtInner != CDistributionSpec::EdtReplicatedWorkers &&
			dtInner != CDistributionSpec::EdtHashed &&
			dtInner != CDistributionSpec::EdtHashedWorker)
		{
			// Inner child requires a distribution incompatible with parallel hash join
			// (e.g., coordinator-only distribution, universal distribution, etc.)
			return false;
		}
	}

	return true;
}

// EOF
