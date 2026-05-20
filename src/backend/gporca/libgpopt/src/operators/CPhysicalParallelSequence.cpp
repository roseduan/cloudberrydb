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
 * CPhysicalParallelSequence.cpp
 *
 * IDENTIFICATION
 *	  src/backend/gporca/libgpopt/src/operators/CPhysicalParallelSequence.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "gpopt/operators/CPhysicalParallelSequence.h"

#include "gpos/base.h"

#include "gpopt/base/CCTEReq.h"
#include "gpopt/base/CDistributionSpecAny.h"
#include "gpopt/base/CDistributionSpecNonSingleton.h"
#include "gpopt/base/CDistributionSpecReplicated.h"
#include "gpopt/base/CDistributionSpecSingleton.h"
#include "gpopt/base/CDistributionSpecWorkerRandom.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CExpressionHandle.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelSequence::CPhysicalParallelSequence
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalParallelSequence::CPhysicalParallelSequence(CMemoryPool *mp, ULONG ulParallelWorkers)
	: CPhysical(mp), m_pcrsEmpty(nullptr), m_ulParallelWorkers(ulParallelWorkers)
{
	GPOS_ASSERT(ulParallelWorkers > 0);

	SetDistrRequests(1);

	m_pcrsEmpty = GPOS_NEW(mp) CColRefSet(mp);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelSequence::~CPhysicalSequence
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalParallelSequence::~CPhysicalParallelSequence()
{
	m_pcrsEmpty->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelSequence::Matches
//
//	@doc:
//		Match operators
//
//---------------------------------------------------------------------------
BOOL
CPhysicalParallelSequence::Matches(COperator *pop) const
{
	return Eopid() == pop->Eopid();
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelSequence::PcrsRequired
//
//	@doc:
//		Compute required output columns of n-th child
//
//---------------------------------------------------------------------------
CColRefSet *
CPhysicalParallelSequence::PcrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
								CColRefSet *pcrsRequired, ULONG child_index,
								CDrvdPropArray *,  // pdrgpdpCtxt
								ULONG			   // ulOptReq
)
{
	const ULONG arity = exprhdl.Arity();
	if (child_index == arity - 1)
	{
		// request required columns from the last child of the sequence
		return PcrsChildReqd(mp, exprhdl, pcrsRequired, child_index,
							 gpos::ulong_max);
	}

	m_pcrsEmpty->AddRef();
	GPOS_ASSERT(0 == m_pcrsEmpty->Size());

	return m_pcrsEmpty;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelSequence::PcteRequired
//
//	@doc:
//		Compute required CTE map of the n-th child
//
//---------------------------------------------------------------------------
CCTEReq *
CPhysicalParallelSequence::PcteRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
								CCTEReq *pcter, ULONG child_index,
								CDrvdPropArray *pdrgpdpCtxt,
								ULONG  //ulOptReq
) const
{
	GPOS_ASSERT(nullptr != pcter);
	if (child_index < exprhdl.Arity() - 1)
	{
		return pcter->PcterAllOptional(mp);
	}

	// derived CTE maps from previous children
	CCTEMap *pcmCombined = PcmCombine(mp, pdrgpdpCtxt);

	// pass the remaining requirements that have not been resolved
	CCTEReq *pcterUnresolved =
		pcter->PcterUnresolvedSequence(mp, pcmCombined, pdrgpdpCtxt);
	pcmCombined->Release();

	return pcterUnresolved;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelSequence::FProvidesReqdCols
//
//	@doc:
//		Helper for checking if required columns are included in output columns
//
//---------------------------------------------------------------------------
BOOL
CPhysicalParallelSequence::FProvidesReqdCols(CExpressionHandle &exprhdl,
									 CColRefSet *pcrsRequired,
									 ULONG	// ulOptReq
) const
{
	GPOS_ASSERT(nullptr != pcrsRequired);

	// last child must provide required columns
	ULONG arity = exprhdl.Arity();
	GPOS_ASSERT(0 < arity);

	CColRefSet *pcrsChild = exprhdl.DeriveOutputColumns(arity - 1);

	return pcrsChild->ContainsAll(pcrsRequired);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelSequence::PdsRequired
//
//	@doc:
//		Compute required distribution of the n-th child
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalParallelSequence::PdsRequired(CMemoryPool *mp,
							   CExpressionHandle &
#ifdef GPOS_DEBUG
	exprhdl
#endif	// GPOS_DEBUG
	,
							   CDistributionSpec *,
							   ULONG child_index, CDrvdPropArray *pdrgpdpCtxt,
							   ULONG
#ifdef GPOS_DEBUG
								   ulOptReq
#endif	// GPOS_DEBUG
							   ) const
{
	GPOS_ASSERT(child_index < exprhdl.Arity());
	GPOS_ASSERT(ulOptReq < UlDistrRequests());

	if (0 == child_index)
	{
		// Request Any on the first child
		return GPOS_NEW(mp) CDistributionSpecAny(this->Eopid());
	}

	// Get derived plan properties of first child
	CDrvdPropPlan *pdpplan = CDrvdPropPlan::Pdpplan((*pdrgpdpCtxt)[0]);
	// Because we request Any on the first child, no motion could
	// have been enforced. The derived spec of the first child is
	// what it delivers in the first place.
	CDistributionSpec *pds = pdpplan->Pds();

	// If the first child is singleton, request singleton on the
	// second child. This request is identical to the 1st request
	// (ulOptReq = 0), where we request singleton on ALL the
	// children, and would eventually be deduplicated.
	if (pds->FSingletonOrStrictSingleton())
	{
		CDistributionSpecSingleton *pdss =
			CDistributionSpecSingleton::PdssConvert(pds);
		return GPOS_NEW(mp) CDistributionSpecSingleton(pdss->Est());
	}

	// If the first child is universal, request Any on the second
	// child. This is because if the producer is universally
	// available, no matter what the locality of the consumer
	// is, the consumer would be able to access the shared data
	if (CDistributionSpec::EdtUniversal == pds->Edt())
	{
		return GPOS_NEW(mp) CDistributionSpecAny(this->Eopid());
	}

	// If the first child is replicated, request replicated on the
	// second child. This way, the producer is only executed on one
	// segment, and so is the consumer. The plan would look like:
	// Gather Motion 1:1  (slice1; segments: 1)
	//   ->  Sequence
	//         ->  Shared Scan (share slice:id 1:1)
	//               ->  Seq Scan on rep
	//         ->  Broadcast Motion 3:1  (slice2; segments: 3)
	//             ...
	//             ...
	//             ->  Gather Motion 1:1  (sliceN; segments: 1)
	//                   ->  Shared Scan (share slice:id N:1)
	if (CDistributionSpec::EdtStrictReplicated == pds->Edt() ||
		CDistributionSpec::EdtTaintedReplicated == pds->Edt())
	{
		return GPOS_NEW(mp)
			CDistributionSpecReplicated(CDistributionSpec::EdtReplicated);
	}

	// Request non-singleton (excluding replicated) on the second
	// child. Worker-level distributions are allowed because the
	// parallel sequence executes inside a worker slice.
	return GPOS_NEW(mp) CDistributionSpecNonSingleton(
		false /* fAllowReplicated */, true /* fAllowWorker */);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelSequence::PosRequired
//
//	@doc:
//		Compute required sort order of the n-th child
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalParallelSequence::PosRequired(CMemoryPool *mp,
							   CExpressionHandle &,	 // exprhdl,
							   COrderSpec *,		 // posRequired,
							   ULONG,				 // child_index,
							   CDrvdPropArray *,	 // pdrgpdpCtxt
							   ULONG				 // ulOptReq
) const
{
	// no order requirement on the children
	return GPOS_NEW(mp) COrderSpec(mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelSequence::PrsRequired
//
//	@doc:
//		Compute required rewindability order of the n-th child
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysicalParallelSequence::PrsRequired(CMemoryPool *,		 // mp,
							   CExpressionHandle &,	 // exprhdl,
							   CRewindabilitySpec *prsRequired,
							   ULONG,			  // child_index,
							   CDrvdPropArray *,  // pdrgpdpCtxt
							   ULONG			  // ulOptReq
) const
{
	// TODO: shardikar; Handle outer refs in the subtree correctly, by passing
	// "Rescannable' Also, maybe it should pass through the prsRequired, since it
	// doesn't materialize any results? It's important to consider performance
	// consequences of that also.
	return GPOS_NEW(m_mp)
		CRewindabilitySpec(CRewindabilitySpec::ErtNone, prsRequired->Emht());
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelSequence::PosDerive
//
//	@doc:
//		Derive sort order
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalParallelSequence::PosDerive(CMemoryPool *,	 // mp,
							 CExpressionHandle &exprhdl) const
{
	// pass through sort order from last child
	const ULONG arity = exprhdl.Arity();

	GPOS_ASSERT(1 <= arity);

	COrderSpec *pos = exprhdl.Pdpplan(arity - 1 /*child_index*/)->Pos();
	pos->AddRef();

	return pos;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelSequence::PdsDerive
//
//	@doc:
//		Derive distribution
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalParallelSequence::PdsDerive(CMemoryPool *,	 // mp,
							 CExpressionHandle &exprhdl) const
{
	// pass through distribution from last child
	const ULONG arity = exprhdl.Arity();

	GPOS_ASSERT(1 <= arity);

	CDistributionSpec *pds = exprhdl.Pdpplan(arity - 1 /*child_index*/)->Pds();
	pds->AddRef();

	return pds;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelSequence::PrsDerive
//
//	@doc:
//		Derive rewindability
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysicalParallelSequence::PrsDerive(CMemoryPool *,	 //mp
							 CExpressionHandle &exprhdl) const
{
	const ULONG arity = exprhdl.Arity();
	GPOS_ASSERT(1 <= arity);

	CRewindabilitySpec::EMotionHazardType motion_hazard =
		CRewindabilitySpec::EmhtNoMotion;
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CRewindabilitySpec *prs = exprhdl.Pdpplan(ul)->Prs();
		if (prs->HasMotionHazard())
		{
			motion_hazard = CRewindabilitySpec::EmhtMotion;
			break;
		}
	}

	// TODO: shardikar; Fix this implementation. Although CPhysicalSequence is
	// not rewindable, all its children might be rewindable. This implementation
	// ignores the rewindability of the op's children
	return GPOS_NEW(m_mp)
		CRewindabilitySpec(CRewindabilitySpec::ErtNone, motion_hazard);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelSequence::EpetOrder
//
//	@doc:
//		Return the enforcing type for the order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalParallelSequence::EpetOrder(CExpressionHandle &exprhdl,
							 const CEnfdOrder *peo) const
{
	GPOS_ASSERT(nullptr != peo);

	// get order delivered by the sequence node
	COrderSpec *pos = CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Pos();

	if (peo->FCompatible(pos))
	{
		// required order will be established by the sequence operator
		return CEnfdProp::EpetUnnecessary;
	}

	// required distribution will be enforced on sequence's output
	return CEnfdProp::EpetRequired;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelSequence::EpetRewindability
//
//	@doc:
//		Return the enforcing type for rewindability property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalParallelSequence::EpetRewindability(CExpressionHandle &,		 // exprhdl
									 const CEnfdRewindability *	 // per
) const
{
	// rewindability must be enforced on operator's output
	return CEnfdProp::EpetRequired;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelSequence::FValidContext
//
//	@doc:
//		Check if optimization contexts is valid;
//		Reject if first child distribution is not worker
//
//---------------------------------------------------------------------------
BOOL
CPhysicalParallelSequence::FValidContext(CMemoryPool *,
										 COptimizationContext *poc,
										 COptimizationContextArray *pdrgpocChild) const
{
	GPOS_ASSERT(nullptr != poc);

	CReqdPropPlan *prpp = poc->Prpp();
	CRewindabilitySpec *prsRequired = prpp->Per()->PrsRequired();

	// If parent requires REWINDABLE or higher, reject
	// ParallelTableScan can only provide ErtNone
	if (prsRequired->IsOriginNLJoin())
	{
		// Parent requires rewindability (e.g., NL Join inner child)
		// but ParallelTableScan cannot provide it
		// Reject this plan to avoid the assertion failure later
		return false;
	}

	if (nullptr != pdrgpocChild)
	{
		COperator *pop = (*pdrgpocChild)[0]->PccBest()->Pgexpr()->Pop();
		if (COperator::EopPhysicalParallelCTEProducer != pop->Eopid())
			return false;
	}

	return true;
}

// EOF