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
 * CPhysicalParallelSequenceProject.cpp
 *
 * IDENTIFICATION
 *	  src/backend/gporca/libgpopt/src/operators/CPhysicalParallelSequenceProject.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "gpopt/operators/CPhysicalParallelSequenceProject.h"

#include "gpos/base.h"

#include "gpopt/base/CDistributionSpecNonSingleton.h"
#include "gpopt/base/CDistributionSpecHashedWorker.h"
#include "gpopt/base/CDistributionSpecHashed.h"
#include "gpopt/base/CDistributionSpecSingleton.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CCostContext.h"
#include "gpopt/base/CDrvdPropPlan.h"
#include "gpopt/operators/CExpressionHandle.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelSequenceProject::CPhysicalParallelSequenceProject
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalParallelSequenceProject::CPhysicalParallelSequenceProject(
	CMemoryPool *mp, ESPType sptype, CDistributionSpec *pds,
	COrderSpecArray *pdrgpos, CWindowFrameArray *pdrgpwf,
	ULONG ulParallelWorkers)
	: CPhysicalSequenceProject(mp, sptype, pds, pdrgpos, pdrgpwf),
	  m_ulParallelWorkers(ulParallelWorkers)
{
	GPOS_ASSERT(ulParallelWorkers > 0);
	GPOS_ASSERT(COperator::EsptypeGlobalOneStep == sptype ||
				COperator::EsptypeGlobalTwoStep == sptype ||
				COperator::EsptypeLocal == sptype);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelSequenceProject::Matches
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL
CPhysicalParallelSequenceProject::Matches(COperator *pop) const
{
	if (Eopid() != pop->Eopid())
	{
		return false;
	}

	CPhysicalParallelSequenceProject *popParallel =
		CPhysicalParallelSequenceProject::PopConvert(pop);

	if (m_ulParallelWorkers != popParallel->UlParallelWorkers())
	{
		return false;
	}

	return CPhysicalSequenceProject::Matches(pop);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelSequenceProject::HashValue
//
//	@doc:
//		Hashing function
//
//---------------------------------------------------------------------------
ULONG
CPhysicalParallelSequenceProject::HashValue() const
{
	ULONG ulHash = CPhysicalSequenceProject::HashValue();
	ulHash = gpos::CombineHashes(ulHash,
								 gpos::HashValue<ULONG>(&m_ulParallelWorkers));
	return ulHash;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelSequenceProject::PdsRequired
//
//	@doc:
//		Compute required distribution of child.
//		- Local phase: any distribution (each worker processes independently)
//		- Global phases: EdtHashedWorker on PARTITION BY columns
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalParallelSequenceProject::PdsRequired(
	CMemoryPool *mp, CExpressionHandle &exprhdl,
	CDistributionSpec *pdsRequired, ULONG child_index,
	CDrvdPropArray *pdrgpdpCtxt GPOS_UNUSED,
	ULONG ulOptReq GPOS_ASSERTS_ONLY) const
{
	GPOS_ASSERT(0 == child_index);
	GPOS_ASSERT(0 == ulOptReq);
	GPOS_ASSERT(!exprhdl.HasOuterRefs());

	// Defensive: singleton execution
	if (exprhdl.NeedsSingletonExecution())
	{
		return PdsRequireSingleton(mp, exprhdl, pdsRequired, child_index);
	}

	// Local phase: accept any distribution, each worker processes independently
	if (COperator::EsptypeLocal == m_sptype)
	{
		return GPOS_NEW(mp) CDistributionSpecNonSingleton(
			false /* fAllowReplicated */, true /* fAllowWorker */);
	}

	// Global phases (OneStep/TwoStep): construct EdtHashedWorker from PARTITION BY columns
	GPOS_ASSERT(CDistributionSpec::EdtHashed == m_pds->Edt());
	CDistributionSpecHashed *pdshashed =
		CDistributionSpecHashed::PdsConvert(m_pds);
	CExpressionArray *pdrgpexpr = pdshashed->Pdrgpexpr();
	pdrgpexpr->AddRef();
	IMdIdArray *opfamilies = pdshashed->Opfamilies();
	if (nullptr != opfamilies)
	{
		opfamilies->AddRef();
	}

	return GPOS_NEW(mp) CDistributionSpecHashedWorker(
		pdrgpexpr, pdshashed->FNullsColocated(), m_ulParallelWorkers,
		opfamilies);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelSequenceProject::FValidContext
//
//	@doc:
//		Check if optimization context is valid.
//		Reject NL Join inner child (needs rewindability).
//		Verify child provides worker-level distribution.
//
//---------------------------------------------------------------------------
BOOL
CPhysicalParallelSequenceProject::FValidContext(
	CMemoryPool *mp GPOS_UNUSED, COptimizationContext *poc,
	COptimizationContextArray *pdrgpocChild) const
{
	GPOS_ASSERT(nullptr != poc);
	GPOS_ASSERT(nullptr != pdrgpocChild);
	GPOS_ASSERT(1 == pdrgpocChild->Size());

	// Reject NL Join inner child (needs rewindability)
	if (poc->Prpp()->Per()->PrsRequired()->IsOriginNLJoin())
	{
		return false;
	}

	// Verify child provides worker-level distribution
	COptimizationContext *pocChild = (*pdrgpocChild)[0];
	CGroupExpression *pgexprChild = pocChild->PgexprBest();
	if (pgexprChild && CUtils::FNLJoin(pgexprChild->Pop()))
	{
		return false;
	}

	CCostContext *pccBest = pocChild->PccBest();
	if (nullptr == pccBest)
	{
		return false;
	}

	CDistributionSpec *pdsChild = pccBest->Pdpplan()->Pds();

	// Must be worker-level distribution
	if (CDistributionSpec::EdtWorkerRandom != pdsChild->Edt() &&
		CDistributionSpec::EdtHashedWorker != pdsChild->Edt())
	{
		return false;
	}

	return true;
}


// EOF
