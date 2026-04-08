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
 * CXformBitmapTableGet2ParallelBitmapTableScan.cpp
 *
 * IDENTIFICATION
 *	  src/backend/gporca/libgpopt/src/xforms/CXformBitmapTableGet2ParallelBitmapTableScan.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "gpopt/xforms/CXformBitmapTableGet2ParallelBitmapTableScan.h"

#include "gpos/base.h"

#include "gpopt/base/COptCtxt.h"
#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalBitmapTableGet.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/operators/CPhysicalParallelBitmapTableScan.h"
#include "gpopt/optimizer/COptimizerConfig.h"
#include "gpopt/xforms/CXformUtils.h"
#include "naucrates/md/IMDIndex.h"
#include "naucrates/md/IMDRelation.h"

extern int max_parallel_workers_per_gather;

namespace gpdb {
	bool IsParallelModeOK(void);
}

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CXformBitmapTableGet2ParallelBitmapTableScan::CXformBitmapTableGet2ParallelBitmapTableScan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformBitmapTableGet2ParallelBitmapTableScan::CXformBitmapTableGet2ParallelBitmapTableScan(CMemoryPool *mp)
	: CXformImplementation(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) CLogicalBitmapTableGet(mp),
		  GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)),
		  GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp))))
{
}

//---------------------------------------------------------------------------
//	@function:
//		CXformBitmapTableGet2ParallelBitmapTableScan::Exfp
//
//	@doc:
//		Compute promise of xform
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformBitmapTableGet2ParallelBitmapTableScan::Exfp(CExpressionHandle &exprhdl) const
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

	CLogicalBitmapTableGet *popGet = CLogicalBitmapTableGet::PopConvert(exprhdl.Pop());
	CTableDescriptor *ptabdesc = popGet->Ptabdesc();

	// AO/AOCS tables do not support parallel bitmap scan: the executor's
	// parallel bitmap path (tbm_shared_iterate + multiple workers) is
	// incompatible with appendonly_scan_bitmap_next_tuple which maintains
	// per-scan fetch state not designed for concurrent access.
	IMDRelation::Erelstoragetype storage_type =
		ptabdesc->RetrieveRelStorageType();
	if (storage_type == IMDRelation::ErelstorageAppendOnlyRows ||
		storage_type == IMDRelation::ErelstorageAppendOnlyCols ||
		storage_type == IMDRelation::ErelstoragePAX ||
		storage_type == IMDRelation::ErelstorageMixedPartitioned)
	{
		return CXform::ExfpNone;
	}

	if (ptabdesc->GetRelDistribution() == IMDRelation::EreldistrReplicated ||
		ptabdesc->GetRelDistribution() == IMDRelation::EreldistrMasterOnly ||
		COptCtxt::PoctxtFromTLS()->HasReplicatedTables())
	{
		return CXform::ExfpNone;
	}

	if (exprhdl.DeriveHasSubquery(0) || exprhdl.DeriveHasSubquery(1))
	{
		return CXform::ExfpNone;
	}

	// Bitmap AM indexes produce StreamBitmap which is incompatible with
	// parallel bitmap heap scan (requires TIDBitmap in DSA shared memory).
	// Only allow parallel bitmap scan when all indexes on the table are btree.
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDRelation *pmdrel = md_accessor->RetrieveRel(ptabdesc->MDId());
	for (ULONG ul = 0; ul < pmdrel->IndexCount(); ul++)
	{
		IMDId *pmdidIndex = pmdrel->IndexMDidAt(ul);
		const IMDIndex *pmdindex = md_accessor->RetrieveIndex(pmdidIndex);
		if (pmdindex->IndexType() == IMDIndex::EmdindBitmap)
		{
			return CXform::ExfpNone;
		}
	}

	return CXform::ExfpHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformBitmapTableGet2ParallelBitmapTableScan::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformBitmapTableGet2ParallelBitmapTableScan::Transform(CXformContext *pxfctxt,
														 CXformResult *pxfres,
														 CExpression *pexpr) const
{
	GPOS_ASSERT(nullptr != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();
	CLogicalBitmapTableGet *popLogical =
		CLogicalBitmapTableGet::PopConvert(pexpr->Pop());

	CTableDescriptor *ptabdesc = popLogical->Ptabdesc();
	ptabdesc->AddRef();

	CColRefArray *pdrgpcrOutput = popLogical->PdrgpcrOutput();
	pdrgpcrOutput->AddRef();

	// Determine parallel workers degree
	ULONG ulParallelWorkers = 2;  // default

	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDRelation *pmdrel = md_accessor->RetrieveRel(ptabdesc->MDId());
	INT table_parallel_workers = pmdrel->ParallelWorkers();

	if (table_parallel_workers > 0)
	{
		ulParallelWorkers = (ULONG)table_parallel_workers;
	}
	else if (max_parallel_workers_per_gather > 0)
	{
		ulParallelWorkers = (ULONG)max_parallel_workers_per_gather;
	}

	CPhysicalParallelBitmapTableScan *popPhysical = GPOS_NEW(mp)
		CPhysicalParallelBitmapTableScan(mp, ptabdesc, pexpr->Pop()->UlOpId(),
										  GPOS_NEW(mp) CName(mp, *popLogical->PnameTableAlias()),
										  pdrgpcrOutput, ulParallelWorkers);

	CExpression *pexprCondition = (*pexpr)[0];
	CExpression *pexprIndexPath = (*pexpr)[1];
	pexprCondition->AddRef();
	pexprIndexPath->AddRef();

	COptCtxt::PoctxtFromTLS()->SetHasParallelOperators();

	CExpression *pexprPhysical = GPOS_NEW(mp)
		CExpression(mp, popPhysical, pexprCondition, pexprIndexPath);
	pxfres->Add(pexprPhysical);
}

// EOF
