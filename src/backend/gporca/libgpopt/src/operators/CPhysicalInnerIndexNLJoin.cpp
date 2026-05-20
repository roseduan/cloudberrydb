//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 Greenplum, Inc.
//
//	@filename:
//		CPhysicalInnerIndexNLJoin.cpp
//
//	@doc:
//		Implementation of index inner nested-loops join operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalInnerIndexNLJoin.h"

#include "gpos/base.h"

#include "gpopt/base/CDistributionSpecAny.h"
#include "gpopt/base/CDistributionSpecHashed.h"
#include "gpopt/base/CDistributionSpecNonSingleton.h"
#include "gpopt/base/CDistributionSpecReplicated.h"
#include "gpopt/base/CDistributionSpecReplicatedWorkers.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/base/COptCtxt.h"
#include "gpos/common/CBitSet.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPhysicalParallelCTEConsumer.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/search/CGroupProxy.h"



using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalInnerIndexNLJoin::CPhysicalInnerIndexNLJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalInnerIndexNLJoin::CPhysicalInnerIndexNLJoin(CMemoryPool *mp,
													 CColRefArray *colref_array,
													 CExpression *origJoinPred)
	: CPhysicalInnerNLJoin(mp),
	  m_pdrgpcrOuterRefs(colref_array),
	  m_origJoinPred(origJoinPred)
{
	GPOS_ASSERT(nullptr != colref_array);
	if (nullptr != origJoinPred)
	{
		origJoinPred->AddRef();
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalInnerIndexNLJoin::~CPhysicalInnerIndexNLJoin
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalInnerIndexNLJoin::~CPhysicalInnerIndexNLJoin()
{
	m_pdrgpcrOuterRefs->Release();
	CRefCount::SafeRelease(m_origJoinPred);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalInnerIndexNLJoin::Matches
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL
CPhysicalInnerIndexNLJoin::Matches(COperator *pop) const
{
	if (pop->Eopid() == Eopid())
	{
		return m_pdrgpcrOuterRefs->Equals(
			CPhysicalInnerIndexNLJoin::PopConvert(pop)->PdrgPcrOuterRefs());
	}

	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalInnerIndexNLJoin::PdsRequired
//
//	@doc:
//		Compute required distribution of the n-th child;
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalInnerIndexNLJoin::PdsRequired(CMemoryPool *mp GPOS_UNUSED,
									   CExpressionHandle &exprhdl GPOS_UNUSED,
									   CDistributionSpec *,	 //pdsRequired,
									   ULONG child_index GPOS_UNUSED,
									   CDrvdPropArray *pdrgpdpCtxt GPOS_UNUSED,
									   ULONG  // ulOptReq
) const
{
	GPOS_RAISE(
		CException::ExmaInvalid, CException::ExmiInvalid,
		GPOS_WSZ_LIT(
			"PdsRequired should not be called for CPhysicalInnerIndexNLJoin"));
	return nullptr;
}

CEnfdDistribution *
CPhysicalInnerIndexNLJoin::Ped(CMemoryPool *mp, CExpressionHandle &exprhdl,
							   CReqdPropPlan *prppInput, ULONG child_index,
							   CDrvdPropArray *pdrgpdpCtxt, ULONG ulDistrReq)
{
	GPOS_ASSERT(2 > child_index);

	CEnfdDistribution::EDistributionMatching dmatch =
		Edm(prppInput, child_index, pdrgpdpCtxt, ulDistrReq);

	if (1 == child_index)
	{
		// inner (index-scan side) is requested for Any distribution,
		// we allow outer references on the inner child of the join since it needs
		// to refer to columns in join's outer child
		return GPOS_NEW(mp) CEnfdDistribution(
			GPOS_NEW(mp)
				CDistributionSpecAny(this->Eopid(), true /*fAllowOuterRefs*/),
			dmatch);
	}

	// we need to match distribution of inner
	CDistributionSpec *pdsInner =
		CDrvdPropPlan::Pdpplan((*pdrgpdpCtxt)[0])->Pds();
	CDistributionSpec::EDistributionType edtInner = pdsInner->Edt();
	if (CDistributionSpec::EdtSingleton == edtInner ||
		CDistributionSpec::EdtStrictSingleton == edtInner ||
		CDistributionSpec::EdtUniversal == edtInner)
	{
		// enforce executing on a single host
		return GPOS_NEW(mp) CEnfdDistribution(
			GPOS_NEW(mp) CDistributionSpecSingleton(), dmatch);
	}

	if (CDistributionSpec::EdtHashed == edtInner)
	{
		// check if we could create an equivalent hashed distribution request to the inner child
		CDistributionSpecHashed *pdshashed =
			CDistributionSpecHashed::PdsConvert(pdsInner);
		CDistributionSpecHashed *pdshashedEquiv = pdshashed->PdshashedEquiv();

		// If the inner child is a IndexScan on a multi-key distributed index, it
		// may derive an incomplete equiv spec (see CPhysicalScan::PdsDerive()).
		// However, there is no point to using that here since there will be no
		// operator above this that can complete it.
		if (pdshashed->HasCompleteEquivSpec(mp))
		{
			// request hashed distribution from outer
			pdshashedEquiv->Pdrgpexpr()->AddRef();
			CDistributionSpecHashed *pdsHashedRequired = GPOS_NEW(mp)
				CDistributionSpecHashed(pdshashedEquiv->Pdrgpexpr(),
										pdshashedEquiv->FNullsColocated());
			pdsHashedRequired->ComputeEquivHashExprs(mp, exprhdl);

			return GPOS_NEW(mp) CEnfdDistribution(pdsHashedRequired, dmatch);
		}
	}

	// otherwise, require outer child to be replicated.
	//
	// When the outer subtree contains a CPhysicalParallelCTEConsumer, the
	// consumer delivers a worker-partitioned stream (WORKER_RANDOM) that
	// still runs inside the worker-level gang established by the enclosing
	// parallel Sequence.  A plain EdtReplicated request there gets
	// satisfied by CPhysicalMotionBroadcast, which at execution time
	// delivers one copy per receiver in the target gang.  Under a
	// worker-level gang (nworkers * nsegments receivers) that duplicates
	// each outer tuple to every worker on every segment; the non-parallel
	// inner IndexScan then runs once per worker per segment and doubles
	// join output.  Upgrade the requirement to EdtReplicatedWorkers so
	// the enforcer picks CPhysicalMotionBroadcastWorkers, which at
	// execution time sends each tuple to exactly one worker per segment
	// (see nodeMotion.c: MOTIONTYPE_BROADCAST_WORKERS).  That preserves
	// the single-worker-per-segment execution the non-parallel inner
	// IndexScan assumes.
	//
	// This is restricted to the CTE-consumer shape on purpose: a bare
	// CPhysicalParallel{Seq,Index,...}Scan as outer is already handled
	// correctly by segment-level Broadcast because the non-CTE parallel
	// scan path does not inject a redistribute-then-broadcast chain whose
	// receivers are worker-level.
	//
	// The match type for this request has to be "Satisfy" since EdtReplicated
	// is required only property. Since a Broadcast motion will always
	// derive a EdtStrictReplicated distribution spec, it will never "Match"
	// the required distribution spec and hence will not be optimized.
	// Only safe when the parent lets this join live in a worker-level gang.
	// If the parent requires a concrete segment-level distribution
	// (Hashed / StrictHashed / Random / StrictRandom / Singleton /
	// (Strict)Replicated), the join's slice will be segment-level; under
	// that shape BroadcastWorkers degenerates to "N senders -> 1 receiver"
	// and the non-parallel inner IndexScan sees only one segment's data,
	// dropping rows.  Keep plain Replicated in those cases.
	if (0 == child_index && FParentAllowsWorkerLevelGang(prppInput))
	{
		CGroupExpression *pgexpr = exprhdl.Pgexpr();
		if (nullptr != pgexpr && child_index < pgexpr->Arity())
		{
			CGroup *pgroupOuter = (*pgexpr)[child_index];
			ULONG ulWorkers = UlExtractParallelCTEConsumerWorkers(pgroupOuter);
			if (0 < ulWorkers)
			{
				return GPOS_NEW(mp) CEnfdDistribution(
					CDistributionSpecReplicatedWorkers::PdsCreate(
						mp, ulWorkers, false /*ignore_broadcast_threshold*/),
					CEnfdDistribution::EdmSatisfy);
			}
		}
	}

	return GPOS_NEW(mp) CEnfdDistribution(
		GPOS_NEW(mp)
			CDistributionSpecReplicated(CDistributionSpec::EdtReplicated),
		CEnfdDistribution::EdmSatisfy);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalInnerIndexNLJoin::FParentAllowsWorkerLevelGang
//
//	@doc:
//		Return true when the parent's required distribution does not pin
//		this join to a segment-level slice.  Flexible (Any / NonSingleton)
//		or already worker-aware requirements (WorkerRandom / HashedWorker /
//		ReplicatedWorkers) all allow the join to run inside a worker-level
//		gang; segment-level Hashed/Random/Singleton/Replicated requirements
//		do not.
//
//---------------------------------------------------------------------------
BOOL
CPhysicalInnerIndexNLJoin::FParentAllowsWorkerLevelGang(
	CReqdPropPlan *prppInput)
{
	if (nullptr == prppInput || nullptr == prppInput->Ped())
	{
		return false;
	}
	CDistributionSpec *pdsRequired = prppInput->Ped()->PdsRequired();
	if (nullptr == pdsRequired)
	{
		return false;
	}
	switch (pdsRequired->Edt())
	{
		case CDistributionSpec::EdtAny:
		case CDistributionSpec::EdtNonSingleton:
		case CDistributionSpec::EdtWorkerRandom:
		case CDistributionSpec::EdtHashedWorker:
		case CDistributionSpec::EdtReplicatedWorkers:
			return true;
		default:
			return false;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		UlExtractParallelCTEConsumerWorkersInternal
//
//	@doc:
//		Recursive worker for UlExtractParallelCTEConsumerWorkers.  Uses a
//		visited-group bitset so cycles in the memo DAG (groups referencing
//		themselves via exploration alternatives) do not cause unbounded
//		recursion.
//
//---------------------------------------------------------------------------
static ULONG
UlExtractParallelCTEConsumerWorkersInternal(CGroup *pgroup, CBitSet *visited)
{
	if (nullptr == pgroup || visited->Get(pgroup->Id()))
	{
		return 0;
	}
	visited->ExchangeSet(pgroup->Id());

	CGroupProxy gp(pgroup);
	CGroupExpression *pgexpr = gp.PgexprFirst();
	while (nullptr != pgexpr)
	{
		COperator *pop = pgexpr->Pop();
		if (COperator::EopPhysicalParallelCTEConsumer == pop->Eopid())
		{
			return CPhysicalParallelCTEConsumer::PopConvert(pop)
				->UlParallelWorkers();
		}

		// Stop recursion at Motion boundaries: a Motion delineates a
		// separate slice, so a parallel CTE consumer below a Motion does
		// not determine the current IndexApply's gang layout.
		if (CUtils::FPhysicalMotion(pop))
		{
			pgexpr = gp.PgexprNext(pgexpr);
			continue;
		}

		const ULONG arity = pgexpr->Arity();
		for (ULONG ul = 0; ul < arity; ul++)
		{
			ULONG ulWorkers = UlExtractParallelCTEConsumerWorkersInternal(
				(*pgexpr)[ul], visited);
			if (0 < ulWorkers)
			{
				return ulWorkers;
			}
		}
		pgexpr = gp.PgexprNext(pgexpr);
	}
	return 0;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalInnerIndexNLJoin::UlExtractParallelCTEConsumerWorkers
//
//	@doc:
//		Walk the outer group subtree (stopping at Motion boundaries) and
//		return the worker count of the first CPhysicalParallelCTEConsumer
//		found; return 0 if none exists.  Used to decide whether the
//		IndexApply outer lives inside a worker-level parallel gang driven
//		by a parallel CTE.
//
//---------------------------------------------------------------------------
ULONG
CPhysicalInnerIndexNLJoin::UlExtractParallelCTEConsumerWorkers(CGroup *pgroup)
{
	if (nullptr == pgroup)
	{
		return 0;
	}

	CMemoryPool *mp = COptCtxt::PoctxtFromTLS()->Pmp();
	CBitSet *visited = GPOS_NEW(mp) CBitSet(mp);
	ULONG ulWorkers = UlExtractParallelCTEConsumerWorkersInternal(pgroup, visited);
	visited->Release();
	return ulWorkers;
}


// EOF
