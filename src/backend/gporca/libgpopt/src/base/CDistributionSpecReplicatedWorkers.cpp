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

#include "gpopt/base/CDistributionSpecReplicatedWorkers.h"

#include "gpopt/base/CDistributionSpecNonSingleton.h"
#include "gpopt/base/CDistributionSpecReplicated.h"
#include "gpopt/base/CDistributionSpecWorkerRandom.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPhysicalMotionBroadcastWorkers.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecReplicatedWorkers::CDistributionSpecReplicatedWorkers
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDistributionSpecReplicatedWorkers::CDistributionSpecReplicatedWorkers(
	ULONG ulWorkers, BOOL ignore_broadcast_threshold,
	CDistributionSpec *pdsSegmentBase)
	: m_ulWorkers(ulWorkers),
	  m_ignore_broadcast_threshold(ignore_broadcast_threshold),
	  m_pdsSegmentBase(pdsSegmentBase)
{
	GPOS_ASSERT(ulWorkers > 0);
	if (nullptr != m_pdsSegmentBase)
	{
		m_pdsSegmentBase->AddRef();
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecReplicatedWorkers::~CDistributionSpecReplicatedWorkers
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDistributionSpecReplicatedWorkers::~CDistributionSpecReplicatedWorkers()
{
	CRefCount::SafeRelease(m_pdsSegmentBase);
}

//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecReplicatedWorkers::PdsCreate
//
//	@doc:
//		Factory method
//
//---------------------------------------------------------------------------
CDistributionSpecReplicatedWorkers *
CDistributionSpecReplicatedWorkers::PdsCreate(CMemoryPool *mp, ULONG ulWorkers,
											  BOOL ignore_broadcast_threshold,
											  CDistributionSpec *pdsSegmentBase)
{
	GPOS_ASSERT(nullptr != mp);
	GPOS_ASSERT(ulWorkers > 0);

	return GPOS_NEW(mp)
		CDistributionSpecReplicatedWorkers(ulWorkers, ignore_broadcast_threshold, pdsSegmentBase);
}

//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecReplicatedWorkers::Matches
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL
CDistributionSpecReplicatedWorkers::Matches(const CDistributionSpec *pds) const
{
	if (EdtReplicatedWorkers != pds->Edt())
	{
		return false;
	}

	const CDistributionSpecReplicatedWorkers *pdsReplicatedWorkers =
		CDistributionSpecReplicatedWorkers::PdsConvert(pds);

	return m_ulWorkers == pdsReplicatedWorkers->m_ulWorkers &&
		   ((nullptr == m_pdsSegmentBase && nullptr == pdsReplicatedWorkers->m_pdsSegmentBase) ||
			(nullptr != m_pdsSegmentBase && nullptr != pdsReplicatedWorkers->m_pdsSegmentBase &&
			 m_pdsSegmentBase->Matches(pdsReplicatedWorkers->m_pdsSegmentBase)));
}

//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecReplicatedWorkers::FSatisfies
//
//	@doc:
//		Check if this distribution satisfies the given requirement
//
//---------------------------------------------------------------------------
BOOL
CDistributionSpecReplicatedWorkers::FSatisfies(const CDistributionSpec *pds) const
{
	if (EdtAny == pds->Edt())
	{
		return true;
	}

	// ReplicatedWorkers satisfies ReplicatedWorkers if worker count matches,
	// regardless of base distribution. E.g., a replicated table scan derives
	// ReplicatedWorkers(N, base=StrictReplicated) which satisfies a join's
	// requirement of ReplicatedWorkers(N, base=nullptr).
	if (EdtReplicatedWorkers == pds->Edt())
	{
		const CDistributionSpecReplicatedWorkers *pdsReq =
			CDistributionSpecReplicatedWorkers::PdsConvert(pds);
		return m_ulWorkers == pdsReq->UlWorkers();
	}
	// ReplicatedWorkers is both worker-level parallel and a replicated
	// distribution (data duplicated on every segment).  A NonSingleton
	// requirement is satisfied only if it allows BOTH dimensions:
	//   - FAllowWorker:     parent accepts worker-parallel inputs
	//     (mirrors HashedWorker / WorkerRandom).
	//   - FAllowReplicated: parent accepts replicated inputs
	//     (mirrors CDistributionSpecReplicated; required so that
	//     Parallel UNION ALL with FAllowReplicated=false can still
	//     force a motion to deduplicate replicated children).
	if (EdtNonSingleton == pds->Edt())
	{
		const CDistributionSpecNonSingleton *pdsNS =
			CDistributionSpecNonSingleton::PdsConvert(pds);
		return pdsNS->FAllowWorker() && pdsNS->FAllowReplicated();
	}

	// ReplicatedWorkers(N) satisfies WorkerRandom[N, X]:
	// - Segment level: replicated data is a superset of any distribution
	// - Worker level: N workers collectively hold all segment data
	// Parallel hash join uses shared hash tables, so workers cooperatively
	// build one hash table from their portions — no broadcast needed.
	if (EdtWorkerRandom == pds->Edt())
	{
		const CDistributionSpecWorkerRandom *pdsWorkerRandom =
			CDistributionSpecWorkerRandom::PdsConvert(pds);
		return m_ulWorkers == pdsWorkerRandom->UlWorkers();
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecReplicatedWorkers::AppendEnforcers
//
//	@doc:
//		Add required motion enforcers to dynamic array
//
//---------------------------------------------------------------------------
void
CDistributionSpecReplicatedWorkers::AppendEnforcers(
	CMemoryPool *mp, CExpressionHandle &
	/*exprhdl*/
	,
	CReqdPropPlan *prpp GPOS_ASSERTS_ONLY, CExpressionArray *pdrgpexpr, CExpression *pexpr)
{
	GPOS_ASSERT(nullptr != mp);
	GPOS_ASSERT(nullptr != prpp);
	GPOS_ASSERT(nullptr != pdrgpexpr);
	GPOS_ASSERT(nullptr != pexpr);
	GPOS_ASSERT(!GPOS_FTRACE(EopttraceDisableMotions));
	GPOS_ASSERT(this == prpp->Ped()->PdsRequired() &&
				"required plan properties don't match enforced distribution spec");

	if (GPOS_FTRACE(EopttraceDisableMotionBroadcastWorkers))
	{
		// broadcast-workers Motion is disabled
		return;
	}

	// Add reference to input expression
	pexpr->AddRef();

	// Create Broadcast Workers Motion
	CDistributionSpecReplicatedWorkers *pdsReplicatedWorkers =
		PdsCreate(mp, m_ulWorkers, m_ignore_broadcast_threshold);

	CExpression *pexprMotion = GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp) CPhysicalMotionBroadcastWorkers(
			mp, pdsReplicatedWorkers, m_ignore_broadcast_threshold),
		pexpr);

	pdrgpexpr->Append(pexprMotion);
}

//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecReplicatedWorkers::OsPrint
//
//	@doc:
//		Print function
//
//---------------------------------------------------------------------------
IOstream &
CDistributionSpecReplicatedWorkers::OsPrint(IOstream &os) const
{
	os << "REPLICATED_WORKERS(workers=" << m_ulWorkers << ")";
	if (nullptr != m_pdsSegmentBase)
	{
		os << " base:";
		m_pdsSegmentBase->OsPrint(os);
	}
	return os;
}

// EOF
