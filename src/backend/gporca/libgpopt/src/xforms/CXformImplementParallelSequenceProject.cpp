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
 * CXformImplementParallelSequenceProject.cpp
 *
 * IDENTIFICATION
 *	  src/backend/gporca/libgpopt/src/xforms/CXformImplementParallelSequenceProject.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "gpopt/xforms/CXformImplementParallelSequenceProject.h"

#include "gpos/base.h"

#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CDistributionSpec.h"
#include "gpopt/base/CDistributionSpecHashed.h"
#include "gpopt/operators/CLogicalSequenceProject.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/operators/CPhysicalParallelSequenceProject.h"
#include "gpopt/operators/CScalarIdent.h"
#include "gpopt/xforms/CXformUtils.h"
#include "naucrates/md/IMDType.h"

// Use gpdbwrappers for parallel checks
extern int max_parallel_workers_per_gather;

// Forward declarations for gpdbwrappers functions
namespace gpdb
{
bool IsParallelModeOK(void);
}

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementParallelSequenceProject::CXformImplementParallelSequenceProject
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformImplementParallelSequenceProject::
	CXformImplementParallelSequenceProject(CMemoryPool *mp)
	:  // pattern
	  CXformImplementation(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) CLogicalSequenceProject(mp),
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // relational child
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp))  // scalar child
		  ))
{
}


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementParallelSequenceProject::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle.
//		Only applicable when:
//		1. Parallel mode is enabled
//		2. Trace flag not disabled
//		3. Parallel operators exist (e.g. Parallel Scan generated)
//		4. No subqueries in scalar child
//		5. No outer refs
//		6. GlobalOneStep, GlobalTwoStep, or Local type
//		7. For Global phases: has PARTITION BY columns (EdtHashed) that are hashable
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformImplementParallelSequenceProject::Exfp(
	CExpressionHandle &exprhdl) const
{
	if (!gpdb::IsParallelModeOK())
	{
		return CXform::ExfpNone;
	}

	if (GPOS_FTRACE(EopttraceDisableParallelWindow))
	{
		return CXform::ExfpNone;
	}

	if (!COptCtxt::PoctxtFromTLS()->HasParallelOperators())
	{
		return CXform::ExfpNone;
	}

	if (exprhdl.DeriveHasSubquery(1))
	{
		return CXform::ExfpNone;
	}

	if (exprhdl.HasOuterRefs())
	{
		return CXform::ExfpNone;
	}

	/*
	 * Skip parallel window when the relational child subtree contains a
	 * replicated table.  A replicated table already holds a full copy on
	 * every segment, so spreading its scan across parallel workers would
	 * have each worker see only a partial copy, producing wrong window
	 * results.  Keep the sequence project serial in that case.
	 */
	if (CXformUtils::FContainsReplicatedTable(exprhdl.DeriveTableDescriptor(0)))
	{
		return CXform::ExfpNone;
	}

	CLogicalSequenceProject *popSeqPrj =
		CLogicalSequenceProject::PopConvert(exprhdl.Pop());
	COperator::ESPType sptype = popSeqPrj->Pspt();

	// Reject unsupported types
	if (COperator::EsptypeGlobalOneStep != sptype &&
		COperator::EsptypeGlobalTwoStep != sptype &&
		COperator::EsptypeLocal != sptype)
	{
		return CXform::ExfpNone;
	}

	// Local phase: no PARTITION BY hashability checks needed
	// (uses CDistributionSpecAny, each worker processes independently)
	if (COperator::EsptypeLocal == sptype)
	{
		return CXform::ExfpHigh;
	}

	// Global phases: must have PARTITION BY (EdtHashed) with hashable columns
	CDistributionSpec *pds = popSeqPrj->Pds();
	if (CDistributionSpec::EdtHashed != pds->Edt())
	{
		return CXform::ExfpNone;
	}

	// PARTITION BY columns must be hashable
	CDistributionSpecHashed *pdshashed =
		CDistributionSpecHashed::PdsConvert(pds);
	CExpressionArray *pdrgpexpr = pdshashed->Pdrgpexpr();
	for (ULONG ul = 0; ul < pdrgpexpr->Size(); ul++)
	{
		CExpression *pexpr = (*pdrgpexpr)[ul];
		if (COperator::EopScalarIdent != pexpr->Pop()->Eopid())
		{
			return CXform::ExfpNone;
		}
		CScalarIdent *popScId = CScalarIdent::PopConvert(pexpr->Pop());
		const CColRef *pcr = popScId->Pcr();
		const IMDType *pmdtype =
			COptCtxt::PoctxtFromTLS()->Pmda()->RetrieveType(
				pcr->RetrieveType()->MDId());
		if (!pmdtype->IsHashable())
		{
			return CXform::ExfpNone;
		}
	}

	return CXform::ExfpHigh;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementParallelSequenceProject::Transform
//
//	@doc:
//		Actual transformation: create CPhysicalParallelSequenceProject
//
//---------------------------------------------------------------------------
void
CXformImplementParallelSequenceProject::Transform(CXformContext *pxfctxt,
												  CXformResult *pxfres,
												  CExpression *pexpr) const
{
	GPOS_ASSERT(nullptr != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();

	CLogicalSequenceProject *popLogical =
		CLogicalSequenceProject::PopConvert(pexpr->Pop());

	// extract components
	CExpression *pexprRelational = (*pexpr)[0];
	CExpression *pexprScalar = (*pexpr)[1];

	// addref children
	pexprRelational->AddRef();
	pexprScalar->AddRef();

	// extract logical operator properties
	COperator::ESPType sptype = popLogical->Pspt();
	CDistributionSpec *pds = popLogical->Pds();
	COrderSpecArray *pdrgpos = popLogical->Pdrgpos();
	CWindowFrameArray *pdrgpwf = popLogical->Pdrgpwf();
	pds->AddRef();
	pdrgpos->AddRef();
	pdrgpwf->AddRef();

	// Determine parallel workers from GUC
	ULONG ulParallelWorkers = 2;  // default
	if (max_parallel_workers_per_gather > 0)
	{
		ulParallelWorkers = (ULONG) max_parallel_workers_per_gather;
	}

	// Create parallel physical operator
	CExpression *pexprResult = GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp) CPhysicalParallelSequenceProject(
			mp, sptype, pds, pdrgpos, pdrgpwf, ulParallelWorkers),
		pexprRelational, pexprScalar);

	pxfres->Add(pexprResult);
}


// EOF
