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
 * CPhysicalParallelIndexScan.cpp
 *
 * IDENTIFICATION
 *	  src/backend/gporca/libgpopt/src/operators/CPhysicalParallelIndexScan.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "gpopt/operators/CPhysicalParallelIndexScan.h"

#include "gpos/base.h"

#include "gpopt/base/CDistributionSpecWorkerRandom.h"
#include "gpopt/base/CDistributionSpecReplicatedWorkers.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CExpressionHandle.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelIndexScan::CPhysicalParallelIndexScan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalParallelIndexScan::CPhysicalParallelIndexScan(
	CMemoryPool *mp, CIndexDescriptor *pindexdesc, CTableDescriptor *ptabdesc,
	ULONG ulOriginOpId, const CName *pnameAlias, CColRefArray *pdrgpcrOutput,
	COrderSpec *pos, ULONG ulUnindexedPredColCount,
	EIndexScanDirection scan_direction, ULONG ulParallelWorkers)
	: CPhysicalIndexScan(mp, pindexdesc, ptabdesc, ulOriginOpId, pnameAlias,
						pdrgpcrOutput, pos, ulUnindexedPredColCount,
						scan_direction),
	  m_ulParallelWorkers(ulParallelWorkers)
{
	GPOS_ASSERT(ulParallelWorkers > 0);
	GPOS_ASSERT(nullptr != m_pds);
	// Create worker-level distribution
	if (ulParallelWorkers > 0 && m_pds)
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
			// Create worker-level random distribution using the table's distribution as base
			// The base CPhysicalScan already sets up m_pds from the table descriptor
			m_pdsWorkerDistribution = CDistributionSpecWorkerRandom::PdsCreateWorkerRandom(mp, ulParallelWorkers, m_pds);
		}
		
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelIndexScan::~CPhysicalParallelIndexScan
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalParallelIndexScan::~CPhysicalParallelIndexScan()
{
	CRefCount::SafeRelease(m_pdsWorkerDistribution);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelIndexScan::HashValue
//
//	@doc:
//		Combine pointers for table descriptor, index descriptor,
//		Eop and parallel workers
//
//---------------------------------------------------------------------------
ULONG
CPhysicalParallelIndexScan::HashValue() const
{
	ULONG ulHash = gpos::CombineHashes(CPhysicalIndexScan::HashValue(),
									   gpos::HashValue<ULONG>(&m_ulParallelWorkers));
	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelIndexScan::Matches
//
//	@doc:
//		match operator
//
//---------------------------------------------------------------------------
BOOL
CPhysicalParallelIndexScan::Matches(COperator *pop) const
{
	if (Eopid() != pop->Eopid())
	{
		return false;
	}

	CPhysicalParallelIndexScan *popParallelIndexScan = 
		CPhysicalParallelIndexScan::PopConvert(pop);
	
	return CPhysicalIndexScan::Matches(pop) && 
		   m_ulParallelWorkers == popParallelIndexScan->UlParallelWorkers();
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelIndexScan::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CPhysicalParallelIndexScan::OsPrint(IOstream &os) const
{
	if (m_fPattern)
	{
		return COperator::OsPrint(os);
	}

	CIndexDescriptor *pindexdesc = Pindexdesc();
	EIndexScanDirection scan_direction = IndexScanDirection();

	os << SzId() << " ";
	// index name
	os << "  Index Name: (";
	pindexdesc->Name().OsPrint(os);
	// table name
	os << ")";
	os << ", Table Name: (";
	m_ptabdesc->Name().OsPrint(os);
	os << ")";
	os << ", Columns: [";
	CUtils::OsPrintDrgPcr(os, m_pdrgpcrOutput);
	os << "]";
	if (scan_direction == EBackwardScan)
	{
		os << ", Backward Scan";
	}
	os << ", Workers: " << m_ulParallelWorkers;

	return os;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelIndexScan::PdsDerive
//
//	@doc:
//		Derive distribution for parallel index scan
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalParallelIndexScan::PdsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl) const
{
	// If we have a pre-computed worker distribution, use it
	if (nullptr != m_pdsWorkerDistribution)
	{
		m_pdsWorkerDistribution->AddRef();
		return m_pdsWorkerDistribution;
	}

	return CPhysicalIndexScan::PdsDerive(mp, exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelIndexScan::EpetDistribution
//
//	@doc:
//		Return the enforcing type for distribution property based on this
//		operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalParallelIndexScan::EpetDistribution(CExpressionHandle & /*exprhdl*/,
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
	// Broadcast, etc.) would make the ParallelIndexScan's plan the cheapest in the
	// optimization context, but the parent's FValidContext would then reject it.
	// Since ORCA keeps only ONE best plan per context, the valid regular IndexScan
	// plan (which is more expensive) becomes unreachable. Prohibiting here ensures
	// the regular IndexScan wins in all segment-level contexts.
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

	// Neither distribution satisfies the requirement
	// Motion enforcement will be needed on the output
	return CEnfdProp::EpetRequired;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelIndexScan::EpetRewindability
//
//	@doc:
//		Return rewindability property enforcing type for this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalParallelIndexScan::EpetRewindability(CExpressionHandle &exprhdl,
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
//		CPhysicalParallelIndexScan::FValidContext
//
//	@doc:
//		Check if optimization contexts is valid;
//		Reject if parent requires REWINDABLE (e.g., for NL Join inner child)
//		because ParallelIndexScan derives NONE (not rewindable)
//
//---------------------------------------------------------------------------
BOOL
CPhysicalParallelIndexScan::FValidContext(CMemoryPool *,
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
		// but ParallelIndexScan cannot provide it
		// Reject this plan to avoid the assertion failure later
		return false;
	}

	return true;
}

// EOF
