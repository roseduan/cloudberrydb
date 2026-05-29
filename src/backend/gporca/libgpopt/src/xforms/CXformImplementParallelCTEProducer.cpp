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
 * CXformImplementParallelCTEProducer.cpp
 *
 * IDENTIFICATION
 *	  src/backend/gporca/libgpopt/src/xforms/CXformImplementParallelCTEProducer.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "gpopt/xforms/CXformImplementParallelCTEProducer.h"

#include "gpos/base.h"

#include "gpopt/operators/CLogicalCTEProducer.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/operators/CPhysicalParallelCTEProducer.h"
#include "gpopt/xforms/CXformUtils.h"

using namespace gpopt;

namespace gpdb {
bool IsParallelModeOK(void);
}

// Use gpdbwrappers for parallel checks
extern int max_parallel_workers_per_gather;

//---------------------------------------------------------------------------
//	@function:
//		CXformImplementParallelCTEProducer::CXformImplementParallelCTEProducer
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformImplementParallelCTEProducer::CXformImplementParallelCTEProducer(CMemoryPool *mp)
	: CXformImplementation(
	// pattern
	GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CLogicalCTEProducer(mp),
		GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp))))
{
}

//---------------------------------------------------------------------------
//	@function:
//		CXformImplementParallelCTEProducer::Exfp
//
//	@doc:
//		Compute promise of xform
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformImplementParallelCTEProducer::Exfp(CExpressionHandle &exprhdl) const
{
	if (!gpdb::IsParallelModeOK())
	{
		return CXform::ExfpNone;
	}

	// Check for parallel-incompatible operations that would conflict with parallel scans
	if (CXformUtils::FHasParallelIncompatibleOps(exprhdl))
	{
		return CXform::ExfpNone;
	}

	CTableDescriptorHashSet *ptabdescset =
		exprhdl.DeriveTableDescriptor(0 /* child_index */);

	/*
	 * Skip parallel CTE when the child subtree contains a Foreign Scan.
	 * Foreign Scans are not parallel-aware, so each parallel worker
	 * would independently scan the full external table, causing
	 * duplicate data in the SharedTuplestore.
	 */
	if (CXformUtils::FContainsForeignTable(ptabdescset))
	{
		return CXform::ExfpNone;
	}

	/*
	 * Skip parallel CTE when the child subtree contains a replicated
	 * table.  A replicated table already has a full copy on every
	 * segment, so a parallel shared scan over it would have each worker
	 * redundantly populate the SharedTuplestore, producing duplicate
	 * rows in the CTE.
	 */
	if (CXformUtils::FContainsReplicatedTable(ptabdescset))
	{
		return CXform::ExfpNone;
	}

	return CXform::ExfpHigh;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementParallelCTEProducer::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformImplementParallelCTEProducer::Transform(CXformContext *pxfctxt,
									  CXformResult *pxfres,
									  CExpression *pexpr) const
{
	GPOS_ASSERT(nullptr != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CLogicalCTEProducer *popCTEProducer =
		CLogicalCTEProducer::PopConvert(pexpr->Pop());
	CMemoryPool *mp = pxfctxt->Pmp();

	CExpression *pexprChild = (*pexpr)[0];
	ULONG ulParallelWorkers = 2;

	// extract components for alternative
	ULONG id = popCTEProducer->UlCTEId();

	CColRefArray *colref_array = popCTEProducer->Pdrgpcr();
	colref_array->AddRef();

	pexprChild->AddRef();

	if (max_parallel_workers_per_gather > 0)
		ulParallelWorkers = (ULONG)max_parallel_workers_per_gather;

	// create physical CTE Producer
	CExpression *pexprAlt = GPOS_NEW(mp)
		CExpression(mp, GPOS_NEW(mp) CPhysicalParallelCTEProducer(mp, id, colref_array, nullptr, ulParallelWorkers),
					pexprChild);

	// add alternative to transformation result
	pxfres->Add(pexprAlt);
}

// EOF