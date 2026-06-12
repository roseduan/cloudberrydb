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
 * CXformIndexGet2ParallelIndexScan.cpp
 *
 * IDENTIFICATION
 *	  src/backend/gporca/libgpopt/src/xforms/CXformIndexGet2ParallelIndexScan.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "gpopt/xforms/CXformIndexGet2ParallelIndexScan.h"

#include "gpos/base.h"

#include "gpopt/metadata/CIndexDescriptor.h"
#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalIndexGet.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/operators/CPhysicalParallelIndexScan.h"
#include "gpopt/xforms/CXformUtils.h"

// Use gpdbwrappers for parallel checks
extern int max_parallel_workers_per_gather;

namespace gpdb {
	bool IsParallelModeOK(void);
}

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformIndexGet2ParallelIndexScan::CXformIndexGet2ParallelIndexScan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformIndexGet2ParallelIndexScan::CXformIndexGet2ParallelIndexScan(CMemoryPool *mp)
	:  // pattern
	  CXformImplementation(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) CLogicalIndexGet(mp),
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CPatternLeaf(mp))  // index lookup predicate
		  ))
{
}

CXform::EXformPromise
CXformIndexGet2ParallelIndexScan::Exfp(CExpressionHandle &exprhdl) const
{
	// Check if parallel plans are enabled in context and parallel processing is safe
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

	CLogicalIndexGet *popGet = CLogicalIndexGet::PopConvert(exprhdl.Pop());

	CTableDescriptor *ptabdesc = popGet->Ptabdesc();
	CIndexDescriptor *pindexdesc = popGet->Pindexdesc();
	BOOL possible_ao_table = ptabdesc->IsNonBlockTable() ||
							 ptabdesc->RetrieveRelStorageType() ==
								 IMDRelation::ErelstorageMixedPartitioned;

	// Don't use parallel scan for replicated tables
	if (ptabdesc->GetRelDistribution() == IMDRelation::EreldistrReplicated ||
		ptabdesc->GetRelDistribution() == IMDRelation::EreldistrMasterOnly ||
		ptabdesc->RetrieveRelStorageType() == IMDRelation::ErelstorageIceberg ||
		COptCtxt::PoctxtFromTLS()->HasReplicatedTables())
	{
		//FIXME: Should we consider replicated tables.
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
//		CXformIndexGet2ParallelIndexScan::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformIndexGet2ParallelIndexScan::Transform(CXformContext *pxfctxt,
									CXformResult *pxfres,
									CExpression *pexpr) const
{
	GPOS_ASSERT(nullptr != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CLogicalIndexGet *pop = CLogicalIndexGet::PopConvert(pexpr->Pop());
	CMemoryPool *mp = pxfctxt->Pmp();
	CIndexDescriptor *pindexdesc = pop->Pindexdesc();
	CTableDescriptor *ptabdesc = pop->Ptabdesc();

	// extract components
	CExpression *pexprIndexCond = (*pexpr)[0];
	if (pexprIndexCond->DeriveHasSubquery())
	{
		return;
	}

	pindexdesc->AddRef();
	ptabdesc->AddRef();

	CColRefArray *pdrgpcrOutput = pop->PdrgpcrOutput();
	GPOS_ASSERT(nullptr != pdrgpcrOutput);
	pdrgpcrOutput->AddRef();

	COrderSpec *pos = pop->Pos();
	GPOS_ASSERT(nullptr != pos);
	pos->AddRef();

	// addref all children
	pexprIndexCond->AddRef();

	// Determine parallel workers degree
	// Priority: table-level parallel_workers setting > GUC max_parallel_workers_per_gather > default
	ULONG ulParallelWorkers = 2;  // default

	// Check if table has a specific parallel_workers setting
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDRelation *pmdrel = md_accessor->RetrieveRel(ptabdesc->MDId());
	INT table_parallel_workers = pmdrel->ParallelWorkers();

	if (table_parallel_workers > 0)
	{
		// Use table-level setting if explicitly configured
		ulParallelWorkers = (ULONG)table_parallel_workers;
	}
	else if (max_parallel_workers_per_gather > 0)
	{
		// Fall back to GUC setting
		ulParallelWorkers = (ULONG)max_parallel_workers_per_gather;
	}

	// Mark that we have parallel operators in the query
	COptCtxt::PoctxtFromTLS()->SetHasParallelOperators();

	CExpression *pexprAlt = GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp) CPhysicalParallelIndexScan(
			mp, pindexdesc, ptabdesc, pexpr->Pop()->UlOpId(),
			GPOS_NEW(mp) CName(mp, pop->NameAlias()), pdrgpcrOutput, pos,
			pop->ResidualPredicateSize(), pop->ScanDirection(),
			ulParallelWorkers),
		pexprIndexCond);
	pxfres->Add(pexprAlt);
}

// EOF
