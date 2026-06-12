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
 * CXformIndexOnlyGet2ParallelIndexOnlyScan.cpp
 *
 * IDENTIFICATION
 *	  src/backend/gporca/libgpopt/src/xforms/CXformIndexOnlyGet2ParallelIndexOnlyScan.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "gpopt/xforms/CXformIndexOnlyGet2ParallelIndexOnlyScan.h"

#include <cwchar>

#include "gpos/base.h"

#include "gpopt/base/COptCtxt.h"
#include "gpopt/hints/CHintUtils.h"
#include "gpopt/metadata/CIndexDescriptor.h"
#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalIndexOnlyGet.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/operators/CPhysicalParallelIndexOnlyScan.h"
#include "gpopt/optimizer/COptimizerConfig.h"
#include "gpopt/xforms/CXformUtils.h"
#include "naucrates/md/CMDIndexGPDB.h"
#include "naucrates/md/IMDRelation.h"

extern int max_parallel_workers_per_gather;

namespace gpdb
{
	bool IsParallelModeOK(void);
}

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CXformIndexOnlyGet2ParallelIndexOnlyScan::CXformIndexOnlyGet2ParallelIndexOnlyScan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformIndexOnlyGet2ParallelIndexOnlyScan::
	CXformIndexOnlyGet2ParallelIndexOnlyScan(CMemoryPool *mp)
	: CXformImplementation(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) CLogicalIndexOnlyGet(mp),
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp))))
{
}

//---------------------------------------------------------------------------
//	@function:
//		CXformIndexOnlyGet2ParallelIndexOnlyScan::Exfp
//
//	@doc:
//		Compute promise of xform
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformIndexOnlyGet2ParallelIndexOnlyScan::Exfp(
	CExpressionHandle &exprhdl) const
{
	if (!gpdb::IsParallelModeOK())
	{
		return CXform::ExfpNone;
	}

	// Subquery-level LIMIT lowers into a segment-local Limit, which is
	// incompatible with worker-level parallel distribution.
	if (COptCtxt::PoctxtFromTLS()->FHasSubqueryLimit())
	{
		return CXform::ExfpNone;
	}

	CLogicalIndexOnlyGet *popGet =
		CLogicalIndexOnlyGet::PopConvert(exprhdl.Pop());
	CTableDescriptor *ptabdesc = popGet->Ptabdesc();
	CIndexDescriptor *pindexdesc = popGet->Pindexdesc();
	BOOL possible_ao_table = ptabdesc->IsNonBlockTable() ||
							 ptabdesc->RetrieveRelStorageType() ==
								 IMDRelation::ErelstorageMixedPartitioned;

	if (ptabdesc->GetRelDistribution() == IMDRelation::EreldistrReplicated ||
		ptabdesc->GetRelDistribution() == IMDRelation::EreldistrMasterOnly ||
		ptabdesc->RetrieveRelStorageType() == IMDRelation::ErelstorageIceberg ||
		COptCtxt::PoctxtFromTLS()->HasReplicatedTables())
	{
		return CXform::ExfpNone;
	}

	if (pindexdesc->IndexType() != IMDIndex::EmdindBitmap && possible_ao_table)
	{
		// we only support index scan type bitmap on AO tables
		return CXform::ExfpNone;
	}

	if (exprhdl.DeriveHasSubquery(0))
	{
		return CXform::ExfpNone;
	}

	// only support btree index right now
	if (pindexdesc->IndexType() != IMDIndex::EmdindBtree)
	{
		return CXform::ExfpNone;
	}

	return CXform::ExfpHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformIndexOnlyGet2ParallelIndexOnlyScan::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformIndexOnlyGet2ParallelIndexOnlyScan::Transform(CXformContext *pxfctxt,
													 CXformResult *pxfres,
													 CExpression *pexpr) const
{
	GPOS_ASSERT(nullptr != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CLogicalIndexOnlyGet *pop =
		CLogicalIndexOnlyGet::PopConvert(pexpr->Pop());
	CMemoryPool *mp = pxfctxt->Pmp();
	CIndexDescriptor *pindexdesc = pop->Pindexdesc();
	CTableDescriptor *ptabdesc = pop->Ptabdesc();
	CColRefArray *pdrgpcrOutput = pop->PdrgpcrOutput();

	CExpression *pexprIndexCond = (*pexpr)[0];
	if (pexprIndexCond->DeriveHasSubquery() ||
		!CXformUtils::FCoverIndex(mp, pindexdesc, ptabdesc, pdrgpcrOutput))
	{
		return;
	}

	if (!CHintUtils::SatisfiesPlanHints(
			pop,
			COptCtxt::PoctxtFromTLS()->GetOptimizerConfig()->GetPlanHint()))
	{
		return;
	}

	// Determine parallel workers
	ULONG ulParallelWorkers = 2;

	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDRelation *pmdrel = md_accessor->RetrieveRel(ptabdesc->MDId());
	INT table_parallel_workers = pmdrel->ParallelWorkers();

	if (table_parallel_workers > 0)
	{
		ulParallelWorkers = (ULONG) table_parallel_workers;
	}
	else if (max_parallel_workers_per_gather > 0)
	{
		ulParallelWorkers = (ULONG) max_parallel_workers_per_gather;
	}

	pindexdesc->AddRef();
	ptabdesc->AddRef();

	COrderSpec *pos = pop->Pos();
	GPOS_ASSERT(nullptr != pos);
	pos->AddRef();

	pexprIndexCond->AddRef();

	COptCtxt::PoctxtFromTLS()->SetHasParallelOperators();

	CExpression *pexprAlt = GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp) CPhysicalParallelIndexOnlyScan(
			mp, pindexdesc, ptabdesc, pexpr->Pop()->UlOpId(),
			GPOS_NEW(mp) CName(mp, pop->NameAlias()), pdrgpcrOutput, pos,
			pop->ScanDirection(), ulParallelWorkers),
		pexprIndexCond);
	pxfres->Add(pexprAlt);
}

// EOF
