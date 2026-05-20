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

//---------------------------------------------------------------------------
//	@filename:
//		CDistributionSpecHashedWorker.h
//
//	@doc:
//		Distribution spec for worker-level hashed distribution from partial aggregation
//
//		Represents data that has been hash-partitioned among parallel workers
//		with partial aggregation already performed. This is distinct from
//		WorkerRandom which represents fragmented scan data.
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CDistributionSpecHashedWorker_H
#define GPOPT_CDistributionSpecHashedWorker_H

#include "gpos/base.h"

#include "gpopt/base/CDistributionSpec.h"
#include "gpopt/base/CDistributionSpecHashed.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CDistributionSpecHashedWorker
//
//	@doc:
//		Distribution spec for hash-partitioned workers with partial aggregation
//
//---------------------------------------------------------------------------
class CDistributionSpecHashedWorker : public CDistributionSpecHashed
{
private:
	// number of parallel workers
	ULONG m_ulWorkers;

public:
	CDistributionSpecHashedWorker(const CDistributionSpecHashedWorker &) =
		delete;

	// ctor
	CDistributionSpecHashedWorker(CExpressionArray *pdrgpexpr,
								  BOOL fNullsColocated, ULONG ulWorkers,
								  IMdIdArray *opfamilies = nullptr);

	// dtor
	~CDistributionSpecHashedWorker() override = default;

	// accessor
	EDistributionType
	Edt() const override
	{
		return EdtHashedWorker;
	}

	const CHAR *
	SzId() const override
	{
		return "HASHED WORKER";
	}

	ULONG
	UlWorkers() const
	{
		return m_ulWorkers;
	}

	// does this distribution match the given one
	BOOL Matches(const CDistributionSpec *pds) const override;

	// does this distribution satisfy the given one
	BOOL FSatisfies(const CDistributionSpec *pds) const override;

	// append enforcers to dynamic array for the given plan properties
	void AppendEnforcers(CMemoryPool *mp, CExpressionHandle &exprhdl,
						 CReqdPropPlan *prpp, CExpressionArray *pdrgpexpr,
						 CExpression *pexpr) override;

	// return a copy of the distribution spec with remapped columns,
	// preserving HashedWorker type and worker count
	CDistributionSpec *PdsCopyWithRemappedColumns(
		CMemoryPool *mp, UlongToColRefMap *colref_mapping,
		BOOL must_exist) override;

	// return a copy stripping equivalent columns,
	// preserving HashedWorker type and worker count
	CDistributionSpec *StripEquivColumns(CMemoryPool *mp) override;

	// hash function
	ULONG HashValue() const override;

	// print
	IOstream &OsPrint(IOstream &os) const override;

	// conversion function
	static CDistributionSpecHashedWorker *
	PdsConvert(CDistributionSpec *pds)
	{
		GPOS_ASSERT(nullptr != pds);
		GPOS_ASSERT(EdtHashedWorker == pds->Edt());
		return dynamic_cast<CDistributionSpecHashedWorker *>(pds);
	}

	static CDistributionSpecHashedWorker *
	PdsConvert(const CDistributionSpec *pds)
	{
		GPOS_ASSERT(nullptr != pds);
		GPOS_ASSERT(EdtHashedWorker == pds->Edt());
		return dynamic_cast<CDistributionSpecHashedWorker *>(
			const_cast<CDistributionSpec *>(pds));
	}
};

}  // namespace gpopt

#endif	// !GPOPT_CDistributionSpecHashedWorker_H

// EOF
