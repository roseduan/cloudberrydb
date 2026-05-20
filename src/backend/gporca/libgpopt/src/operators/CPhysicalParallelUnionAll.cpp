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

#include "gpopt/operators/CPhysicalParallelUnionAll.h"

#include "gpopt/base/CCostContext.h"
#include "gpopt/base/CDistributionSpecNonSingleton.h"
#include "gpopt/base/CDistributionSpecRandom.h"
#include "gpopt/base/CDistributionSpecWorkerRandom.h"
#include "gpopt/base/COptimizationContext.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPhysicalMotion.h"

namespace gpopt
{

CPhysicalParallelUnionAll::CPhysicalParallelUnionAll(
	CMemoryPool *mp, CColRefArray *pdrgpcrOutput,
	CColRef2dArray *pdrgpdrgpcrInput, ULONG ulParallelWorkers)
	: CPhysicalUnionAll(mp, pdrgpcrOutput, pdrgpdrgpcrInput),
	  m_ulParallelWorkers(ulParallelWorkers)
{
	GPOS_ASSERT(ulParallelWorkers > 0);
	SetDistrRequests(1 /*ulDistrReq*/);
	GPOS_ASSERT(0 < UlDistrRequests());
}

COperator::EOperatorId
CPhysicalParallelUnionAll::Eopid() const
{
	return EopPhysicalParallelUnionAll;
}

const CHAR *
CPhysicalParallelUnionAll::SzId() const
{
	return "CPhysicalParallelUnionAll";
}

CDistributionSpec *
CPhysicalParallelUnionAll::PdsRequired(CMemoryPool *mp, CExpressionHandle &,
									   CDistributionSpec *,
									   ULONG child_index GPOS_UNUSED,
									   CDrvdPropArray *,
									   ULONG ulOptReq GPOS_UNUSED) const
{
	GPOS_ASSERT(nullptr != PdrgpdrgpcrInput());
	GPOS_ASSERT(child_index < PdrgpdrgpcrInput()->Size());
	GPOS_ASSERT(1 > ulOptReq);

	return GPOS_NEW(mp) CDistributionSpecNonSingleton(
		false /*fAllowReplicated*/, true /*fAllowWorker*/);
}

CEnfdDistribution::EDistributionMatching
CPhysicalParallelUnionAll::Edm(CReqdPropPlan *,	   // prppInput
							   ULONG,			   // child_index
							   CDrvdPropArray *,   // pdrgpdpCtxt
							   ULONG			   // ulOptReq
)
{
	return CEnfdDistribution::EdmSatisfy;
}

CDistributionSpec *
CPhysicalParallelUnionAll::PdsDerive(CMemoryPool *mp,
									 CExpressionHandle &exprhdl) const
{
	// If every non-scalar child derives a worker-level random distribution
	// with the same worker count, expose that worker-level distribution to
	// the parent.  This lets a downstream operator (e.g. Parallel Hash Join)
	// that requires WORKER_RANDOM[base:NonSingleton] satisfy its outer
	// directly from the Parallel UnionAll without inserting a Motion
	// enforcer.  A Motion would erase partition propagation (Motion is a
	// hard barrier for Ppps), preventing Parallel Partition Selectors from
	// being plumbed through a UNION ALL that combines partition tables.
	const ULONG arity = exprhdl.Arity();
	BOOL fAllWorkerRandom = (arity > 0);
	ULONG ulWorkers = 0;
	for (ULONG ul = 0; fAllWorkerRandom && ul < arity; ul++)
	{
		if (exprhdl.FScalarChild(ul))
		{
			continue;
		}
		CDistributionSpec *pdsChild = exprhdl.Pdpplan(ul)->Pds();
		if (CDistributionSpec::EdtWorkerRandom != pdsChild->Edt())
		{
			fAllWorkerRandom = false;
			break;
		}
		CDistributionSpecWorkerRandom *pdsWR =
			CDistributionSpecWorkerRandom::PdsConvert(pdsChild);
		if (0 == ulWorkers)
		{
			ulWorkers = pdsWR->UlWorkers();
		}
		else if (ulWorkers != pdsWR->UlWorkers())
		{
			fAllWorkerRandom = false;
			break;
		}
	}
	if (fAllWorkerRandom && ulWorkers > 0)
	{
		return CDistributionSpecWorkerRandom::PdsCreateWorkerRandom(
			mp, ulWorkers, GPOS_NEW(mp) CDistributionSpecRandom());
	}

	return CPhysicalUnionAll::PdsDerive(mp, exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		FHasIncompatibleOps
//
//	@doc:
//		Recursively check if a physical plan contains operators incompatible
//		with Parallel Union All:
//		- Motion operators (except duplicate-sensitive random motions)
//		- Parallel hash joins (require barrier sync, can cause deadlock)
//		- CTE consumers (parallel or serial): multiple CTE consumers of the
//		  same CTE share a scan_barrier in the executor; Parallel Append
//		  splits workers across children, so different workers arrive at the
//		  barrier for different consumers simultaneously, causing deadlock
//
//---------------------------------------------------------------------------
static BOOL
FHasIncompatibleOps(CCostContext *pcc)
{
	GPOS_CHECK_STACK_SIZE;

	if (nullptr == pcc)
	{
		return false;
	}

	CGroupExpression *pgexpr = pcc->Pgexpr();
	if (nullptr == pgexpr)
	{
		return false;
	}

	COperator *pop = pgexpr->Pop();

	// Check for motion operators
	if (CUtils::FPhysicalMotion(pop))
	{
		// Allow duplicate-sensitive random motions (used for UNION ALL with constants)
		CDistributionSpec::EDistributionType edt =
			CPhysicalMotion::PopConvert(pop)->Pds()->Edt();
		if (CDistributionSpec::EdtRandom == edt ||
			CDistributionSpec::EdtStrictRandom == edt)
		{
			const CDistributionSpecRandom *pdsRandom =
				dynamic_cast<const CDistributionSpecRandom *>(
					CPhysicalMotion::PopConvert(pop)->Pds());
			if (nullptr == pdsRandom || !pdsRandom->IsDuplicateSensitive())
			{
				return true;
			}
			// Duplicate-sensitive random motion is safe, continue checking children
		}
		else
		{
			return true;
		}
	}

	// Check for parallel hash join (barrier sync can deadlock under Parallel Append)
	if (CUtils::FParallelHashJoin(pop))
	{
		return true;
	}

	// Check for CTE consumers (ShareInputScan in executor).  Multiple CTE
	// consumers sharing the same CTE use a common scan_barrier that requires
	// ALL workers to arrive.  Under Parallel Append, workers are split across
	// children, preventing the barrier from completing -> deadlock.
	if (COperator::EopPhysicalCTEConsumer == pop->Eopid() ||
		COperator::EopPhysicalParallelCTEConsumer == pop->Eopid())
	{
		return true;
	}

	// Recursively check children
	if (pop->FPhysical())
	{
		COptimizationContextArray *pdrgpoc = pcc->Pdrgpoc();
		if (nullptr != pdrgpoc)
		{
			const ULONG arity = pdrgpoc->Size();
			for (ULONG ul = 0; ul < arity; ul++)
			{
				COptimizationContext *pocChild = (*pdrgpoc)[ul];
				if (nullptr != pocChild &&
					FHasIncompatibleOps(pocChild->PccBest()))
				{
					return true;
				}
			}
		}
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelUnionAll::FValidContext
//
//	@doc:
//		Check if the optimization context is valid for Parallel Union All.
//		Rejects plans containing incompatible operators in child subtrees.
//
//---------------------------------------------------------------------------
BOOL
CPhysicalParallelUnionAll::FValidContext(CMemoryPool *,
										 COptimizationContext *,
										 COptimizationContextArray *pdrgpocChild) const
{
	const ULONG arity = pdrgpocChild->Size();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		COptimizationContext *pocChild = (*pdrgpocChild)[ul];
		CCostContext *pccChildBest = pocChild->PccBest();

		if (nullptr != pccChildBest && FHasIncompatibleOps(pccChildBest))
		{
			return false;
		}
	}

	return true;
}

CPhysicalParallelUnionAll *
CPhysicalParallelUnionAll::PopConvert(COperator *pop)
{
	GPOS_ASSERT(nullptr != pop);
	GPOS_ASSERT(EopPhysicalParallelUnionAll == pop->Eopid());

	return dynamic_cast<CPhysicalParallelUnionAll *>(pop);
}

}  // namespace gpopt
