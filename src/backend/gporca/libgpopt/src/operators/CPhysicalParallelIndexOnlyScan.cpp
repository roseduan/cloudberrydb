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
 * CPhysicalParallelIndexOnlyScan.cpp
 *
 * IDENTIFICATION
 *	  src/backend/gporca/libgpopt/src/operators/CPhysicalParallelIndexOnlyScan.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "gpopt/operators/CPhysicalParallelIndexOnlyScan.h"

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
//		CPhysicalParallelIndexOnlyScan::CPhysicalParallelIndexOnlyScan
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CPhysicalParallelIndexOnlyScan::CPhysicalParallelIndexOnlyScan(
	CMemoryPool *mp, CIndexDescriptor *pindexdesc, CTableDescriptor *ptabdesc,
	ULONG ulOriginOpId, const CName *pnameAlias, CColRefArray *colref_array,
	COrderSpec *pos, EIndexScanDirection scan_direction,
	ULONG ulParallelWorkers)
	: CPhysicalIndexOnlyScan(mp, pindexdesc, ptabdesc, ulOriginOpId,
							 pnameAlias, colref_array, pos, scan_direction),
	  m_ulParallelWorkers(ulParallelWorkers),
	  m_pdsWorkerDistribution(nullptr)
{
	GPOS_ASSERT(ulParallelWorkers > 0);
	GPOS_ASSERT(nullptr != m_pds);

	if (ulParallelWorkers > 0 && m_pds)
	{
		if (CDistributionSpec::EdtStrictReplicated == m_pds->Edt() ||
			CDistributionSpec::EdtTaintedReplicated == m_pds->Edt())
		{
			m_pdsWorkerDistribution = CDistributionSpecReplicatedWorkers::PdsCreate(
				mp, ulParallelWorkers, false /*ignore_broadcast_threshold*/, m_pds);
		}
		else
		{
			m_pdsWorkerDistribution =
				CDistributionSpecWorkerRandom::PdsCreateWorkerRandom(mp,
																	 ulParallelWorkers, m_pds);
		}
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelIndexOnlyScan::~CPhysicalParallelIndexOnlyScan
//
//	@doc:
//		dtor
//
//---------------------------------------------------------------------------
CPhysicalParallelIndexOnlyScan::~CPhysicalParallelIndexOnlyScan()
{
	CRefCount::SafeRelease(m_pdsWorkerDistribution);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelIndexOnlyScan::HashValue
//
//	@doc:
//		Combine pointer for table descriptor, index descriptor, parallel
//		workers and Eop
//
//---------------------------------------------------------------------------
ULONG
CPhysicalParallelIndexOnlyScan::HashValue() const
{
	ULONG ulHash =
		gpos::CombineHashes(CPhysicalIndexOnlyScan::HashValue(),
							gpos::HashValue<ULONG>(&m_ulParallelWorkers));
	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelIndexOnlyScan::Matches
//
//	@doc:
//		match operator
//
//---------------------------------------------------------------------------
BOOL
CPhysicalParallelIndexOnlyScan::Matches(COperator *pop) const
{
	if (Eopid() != pop->Eopid())
	{
		return false;
	}

	CPhysicalParallelIndexOnlyScan *popParallel =
		CPhysicalParallelIndexOnlyScan::PopConvert(pop);

	return CUtils::FMatchIndex(this, pop) &&
		   m_ulParallelWorkers == popParallel->UlParallelWorkers();
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelIndexOnlyScan::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CPhysicalParallelIndexOnlyScan::OsPrint(IOstream &os) const
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
//		CPhysicalParallelIndexOnlyScan::PdsDerive
//
//	@doc:
//		Derive distribution for parallel index only scan
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalParallelIndexOnlyScan::PdsDerive(CMemoryPool *mp,
										  CExpressionHandle &exprhdl) const
{
	if (nullptr != m_pdsWorkerDistribution)
	{
		m_pdsWorkerDistribution->AddRef();
		return m_pdsWorkerDistribution;
	}

	return CPhysicalScan::PdsDerive(mp, exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelIndexOnlyScan::EpetDistribution
//
//	@doc:
//		Return the enforcing type for distribution property
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalParallelIndexOnlyScan::EpetDistribution(
	CExpressionHandle & /*exprhdl*/, const CEnfdDistribution *ped) const
{
	GPOS_ASSERT(nullptr != ped);

	if (nullptr != m_pdsWorkerDistribution &&
		ped->FCompatible(m_pdsWorkerDistribution))
	{
		return CEnfdProp::EpetUnnecessary;
	}

	// For replicated table parallel scans: prohibit ALL segment-level distribution
	// requirements. Only worker-level requests (from parallel-aware operators like
	// ParallelHashJoin) should use this scan. Segment-level motions (HashDistribute,
	// Broadcast, etc.) would make the ParallelIndexOnlyScan's plan the cheapest in
	// the optimization context, but the parent's FValidContext would then reject it.
	// Since ORCA keeps only ONE best plan per context, the valid regular
	// IndexOnlyScan plan (which is more expensive) becomes unreachable. Prohibiting
	// here ensures the regular IndexOnlyScan wins in all segment-level contexts.
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

	return CEnfdProp::EpetRequired;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelIndexOnlyScan::EpetRewindability
//
//	@doc:
//		Return rewindability property enforcing type
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalParallelIndexOnlyScan::EpetRewindability(
	CExpressionHandle &exprhdl, const CEnfdRewindability *per) const
{
	GPOS_ASSERT(nullptr != per);

	CRewindabilitySpec *prs = CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Prs();

	if (per->FCompatible(prs))
	{
		return CEnfdProp::EpetUnnecessary;
	}

	if (per->PrsRequired()->IsOriginNLJoin())
	{
		return CEnfdProp::EpetProhibited;
	}

	return CEnfdProp::EpetRequired;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelIndexOnlyScan::FValidContext
//
//	@doc:
//		Check if optimization context is valid;
//		Reject if parent requires REWINDABLE (e.g., for NL Join inner child)
//
//---------------------------------------------------------------------------
BOOL
CPhysicalParallelIndexOnlyScan::FValidContext(CMemoryPool *,
											  COptimizationContext *poc,
											  COptimizationContextArray *) const
{
	GPOS_ASSERT(nullptr != poc);

	CReqdPropPlan *prpp = poc->Prpp();
	CRewindabilitySpec *prsRequired = prpp->Per()->PrsRequired();

	if (prsRequired->IsOriginNLJoin())
	{
		return false;
	}

	return true;
}

// EOF
