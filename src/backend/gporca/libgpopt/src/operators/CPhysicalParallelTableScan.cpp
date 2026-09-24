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
 * CPhysicalParallelTableScan.cpp
 *
 * IDENTIFICATION
 *	  src/backend/gporca/libgpopt/src/operators/CPhysicalParallelTableScan.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "gpopt/operators/CPhysicalParallelTableScan.h"

#include "gpos/base.h"

#include "gpopt/base/CDistributionSpec.h"
#include "gpopt/base/CDistributionSpecHashed.h"
#include "gpopt/base/CDistributionSpecRandom.h"
#include "gpopt/base/CDistributionSpecReplicatedWorkers.h"
#include "gpopt/base/CDistributionSpecWorkerRandom.h"
#include "gpopt/base/CDistributionSpecSingleton.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/base/CEnfdDistribution.h"
#include "gpopt/base/CEnfdRewindability.h"
#include "gpopt/base/COptimizationContext.h"
#include "gpopt/base/CRewindabilitySpec.h"
#include "gpopt/base/CDrvdPropPlan.h"
#include "gpopt/metadata/CName.h"
#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/CExpressionHandle.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelTableScan::CPhysicalParallelTableScan
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CPhysicalParallelTableScan::CPhysicalParallelTableScan(CMemoryPool *mp)
	: CPhysicalTableScan(mp, GPOS_NEW(mp) CName(GPOS_NEW(mp) CWStringConst(GPOS_WSZ_LIT("parallel_table"))), nullptr, nullptr),
	  m_ulParallelWorkers(1),
	  m_pdsWorkerDistribution(nullptr)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelTableScan::CPhysicalParallelTableScan
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CPhysicalParallelTableScan::CPhysicalParallelTableScan(CMemoryPool *mp,
													   const CName *pnameAlias,
													   CTableDescriptor *ptabdesc,
													   CColRefArray *pdrgpcrOutput,
													   ULONG ulParallelWorkers)
	: CPhysicalTableScan(mp, pnameAlias, ptabdesc, pdrgpcrOutput),
	  m_ulParallelWorkers(ulParallelWorkers),
	  m_pdsWorkerDistribution(nullptr)
{
	GPOS_ASSERT(ulParallelWorkers > 0);
	GPOS_ASSERT(nullptr != m_pds);
	// Create worker-level distribution based on table's segment distribution
	if (ulParallelWorkers > 0 && nullptr != m_pds)
	{
		if (CDistributionSpec::EdtStrictReplicated == m_pds->Edt() ||
			CDistributionSpec::EdtTaintedReplicated == m_pds->Edt())
		{
			// Replicated table: each worker scans a portion of the segment's full data.
			// ReplicatedWorkers triggers FDuplicateHazardDistributionSpec -> true,
			// which restricts Gather to 1 segment (avoiding duplicate results).
			// Pass m_pds as base to distinguish from BroadcastWorkers origin.
			m_pdsWorkerDistribution = CDistributionSpecReplicatedWorkers::PdsCreate(
				mp, ulParallelWorkers, false /*ignore_broadcast_threshold*/, m_pds);
		}
		else
		{
			// Non-replicated table: create WorkerRandom with table's distribution as base
			m_pdsWorkerDistribution = CDistributionSpecWorkerRandom::PdsCreateWorkerRandom(mp, ulParallelWorkers, m_pds);
		}
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelTableScan::~CPhysicalParallelTableScan
//
//	@doc:
//		dtor
//
//---------------------------------------------------------------------------
CPhysicalParallelTableScan::~CPhysicalParallelTableScan()
{
	CRefCount::SafeRelease(m_pdsWorkerDistribution);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelTableScan::HashValue
//
//	@doc:
//		Combine pointer for table descriptor, parallel workers and Eop
//
//---------------------------------------------------------------------------
ULONG
CPhysicalParallelTableScan::HashValue() const
{
	ULONG ulHash = gpos::CombineHashes(CPhysicalTableScan::HashValue(),
									   gpos::HashValue<ULONG>(&m_ulParallelWorkers));
	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelTableScan::Matches
//
//	@doc:
//		match operator
//
//---------------------------------------------------------------------------
BOOL
CPhysicalParallelTableScan::Matches(COperator *pop) const
{
	if (Eopid() != pop->Eopid())
	{
		return false;
	}

	CPhysicalParallelTableScan *popParallelTableScan = 
		CPhysicalParallelTableScan::PopConvert(pop);
	
	return CPhysicalTableScan::Matches(pop) && 
		   m_ulParallelWorkers == popParallelTableScan->UlParallelWorkers();
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelTableScan::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CPhysicalParallelTableScan::OsPrint(IOstream &os) const
{
	os << SzId() << " ";

	// alias of table as referenced in the query
	m_pnameAlias->OsPrint(os);

	// actual name of table in catalog and columns
	os << " (";
	m_ptabdesc->Name().OsPrint(os);
	os << "), Columns: [";

	CUtils::OsPrintDrgPcr(os, m_pdrgpcrOutput);
	os << "], Workers: " << m_ulParallelWorkers;

	return os;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelTableScan::PdsDerive
//
//	@doc:
//		Derive distribution for parallel table scan
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalParallelTableScan::PdsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl) const
{
	// If we have a pre-computed worker distribution, use it
	if (nullptr != m_pdsWorkerDistribution)
	{
		m_pdsWorkerDistribution->AddRef();
		return m_pdsWorkerDistribution;
	}

	// Otherwise, derive from the base physical scan
	// This uses the m_pds member from CPhysicalScan
	return CPhysicalScan::PdsDerive(mp, exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelTableScan::EpetDistribution
//
//	@doc:
//		Return the enforcing type for distribution property based on this
//		operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalParallelTableScan::EpetDistribution(CExpressionHandle & /*exprhdl*/,
											  const CEnfdDistribution *ped) const
{
	GPOS_ASSERT(nullptr != ped);

	// First check if worker-level distribution can satisfy the requirement
	// This is the primary distribution for parallel scans
	if (nullptr != m_pdsWorkerDistribution && ped->FCompatible(m_pdsWorkerDistribution))
	{
		return CEnfdProp::EpetUnnecessary;
	}

	// For replicated table parallel scans: prohibit ALL segment-level distribution
	// requirements. Only worker-level requests (from parallel-aware operators like
	// ParallelHashJoin) should use this scan. Segment-level motions (HashDistribute,
	// Broadcast, etc.) would make the ParallelTableScan's plan the cheapest in the
	// optimization context, but the parent's FValidContext would then reject it.
	// Since ORCA keeps only ONE best plan per context, the valid regular TableScan
	// plan (which is more expensive) becomes unreachable. Prohibiting here ensures
	// the regular TableScan wins in all segment-level contexts.
	if (nullptr != m_pdsWorkerDistribution &&
		CDistributionSpec::EdtReplicatedWorkers == m_pdsWorkerDistribution->Edt())
	{
		const CDistributionSpecReplicatedWorkers *pdsRW =
			CDistributionSpecReplicatedWorkers::PdsConvert(m_pdsWorkerDistribution);
		if (pdsRW->FFromReplicatedTable())
		{
			CDistributionSpec::EDistributionType edtReq = ped->PdsRequired()->Edt();
			if (CDistributionSpec::EdtWorkerRandom != edtReq &&
				CDistributionSpec::EdtReplicatedWorkers != edtReq &&
				CDistributionSpec::EdtHashedWorker != edtReq &&
				CDistributionSpec::EdtAny != edtReq)
			{
				return CEnfdProp::EpetProhibited;
			}
		}
	}

	// Worker-level motion enforcement needed (e.g., HashDistributeWorkers)
	return CEnfdProp::EpetRequired;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelTableScan::EpetRewindability
//
//	@doc:
//		Return rewindability property enforcing type for this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalParallelTableScan::EpetRewindability(CExpressionHandle &exprhdl,
											  const CEnfdRewindability *per) const
{
	GPOS_ASSERT(nullptr != per);

	// Get derived rewindability from this operator
	CRewindabilitySpec *prs = CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Prs();

	// Check if our derived rewindability satisfies the requirement
	if (per->FCompatible(prs))
	{
		// Our derived rewindability (ErtNone) satisfies the requirement
		return CEnfdProp::EpetUnnecessary;
	}

	// Cannot satisfy the rewindability requirement
	// Check if requirement originates from NL Join
	if (per->PrsRequired()->IsOriginNLJoin())
	{
		// Prohibit enforcement - NL Join cannot work efficiently with parallel scan
		// even with a Spool enforcer, so reject this plan combination
		return CEnfdProp::EpetProhibited;
	}

	// For other contexts, allow enforcement with Spool
	return CEnfdProp::EpetRequired;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelTableScan::FValidContext
//
//	@doc:
//		Check if optimization contexts is valid;
//		Reject if parent requires REWINDABLE (e.g., for NL Join inner child)
//		because ParallelTableScan derives NONE (not rewindable)
//
//---------------------------------------------------------------------------
BOOL
CPhysicalParallelTableScan::FValidContext(CMemoryPool *,
										  COptimizationContext *poc,
										  COptimizationContextArray *) const
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

	/*
	 * A parallel scan of a replicated table must not win a generic ANY
	 * context.  ANY contexts are shared with consumers that depend on the
	 * in-place replicated property: e.g. the inner side of a co-located
	 * hash join reaches this scan through its child's ANY context and
	 * needs StrictReplicated.  Since ORCA keeps one best plan per context,
	 * letting the cheaper (cost / workers) parallel scan win ANY makes the
	 * serial TableScan plan unreachable, its ReplicatedWorkers output can
	 * only be consumed safely by a Gather, and the co-located join
	 * collapses to a gather-to-singleton plan.  Segment-level requirements
	 * are already prohibited in EpetDistribution; ANY cannot be handled
	 * there because the engine skips the distribution enforcement check
	 * for ANY requirements (CEngine::FCheckEnfdProps, fDistributionReqd).
	 * Worker-level contexts requested by parallel joins/aggs stay valid.
	 */
	if (nullptr != m_pdsWorkerDistribution &&
		CDistributionSpec::EdtReplicatedWorkers ==
			m_pdsWorkerDistribution->Edt() &&
		CDistributionSpec::EdtAny == prpp->Ped()->PdsRequired()->Edt())
	{
		const CDistributionSpecReplicatedWorkers *pdsRW =
			CDistributionSpecReplicatedWorkers::PdsConvert(
				m_pdsWorkerDistribution);
		if (pdsRW->FFromReplicatedTable())
		{
			return false;
		}
	}

	return true;
}

// EOF