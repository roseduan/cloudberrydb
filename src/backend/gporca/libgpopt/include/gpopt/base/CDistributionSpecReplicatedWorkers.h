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

#ifndef GPOPT_CDistributionSpecReplicatedWorkers_H
#define GPOPT_CDistributionSpecReplicatedWorkers_H

#include "gpos/base.h"
#include "gpopt/base/CDistributionSpec.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CDistributionSpecReplicatedWorkers
//
//	@doc:
//		Distribution specification for replicated table data split among workers
//
//		Semantics:
//		- Segment-level: data is replicated across all segments (each segment has full data)
//		- Worker-level: each worker has a portion of the segment's data
//		- Does NOT cross segment boundaries
//
//		Example with 3 segments, 2 workers per segment (table has rows 0-100):
//		  Seg0.W0 has rows 0-50, Seg0.W1 has rows 51-100
//		  Seg1.W0 has rows 0-50, Seg1.W1 has rows 51-100
//		  Seg2.W0 has rows 0-50, Seg2.W1 has rows 51-100
//		  (Each segment has all rows 0-100, split among its workers)
//
//---------------------------------------------------------------------------
class CDistributionSpecReplicatedWorkers : public CDistributionSpec
{
private:
	// Number of parallel workers per segment
	ULONG m_ulWorkers;
	// Ignore broadcast threshold when costing (similar to Replicated)
	BOOL m_ignore_broadcast_threshold;
	// Base segment-level distribution (e.g., Replicated for table scan, nullptr for broadcast/requirement)
	CDistributionSpec *m_pdsSegmentBase;

	// Private copy ctor
	CDistributionSpecReplicatedWorkers(const CDistributionSpecReplicatedWorkers &);

public:
	// Ctor
	CDistributionSpecReplicatedWorkers(ULONG ulWorkers,
									   BOOL ignore_broadcast_threshold = false,
									   CDistributionSpec *pdsSegmentBase = nullptr);

	// Dtor
	~CDistributionSpecReplicatedWorkers() override;

	// Accessor: distribution type
	EDistributionType
	Edt() const override
	{
		return CDistributionSpec::EdtReplicatedWorkers;
	}

	// Accessor: number of workers
	ULONG
	UlWorkers() const
	{
		return m_ulWorkers;
	}

	BOOL
	FIgnoreBroadcastThreshold() const
	{
		return m_ignore_broadcast_threshold;
	}

	// Accessor: base segment-level distribution
	CDistributionSpec *
	PdsSegmentBase() const
	{
		return m_pdsSegmentBase;
	}

	// Check if this distribution originated from a replicated table scan
	BOOL
	FFromReplicatedTable() const
	{
		return nullptr != m_pdsSegmentBase &&
			   (CDistributionSpec::EdtStrictReplicated == m_pdsSegmentBase->Edt() ||
				CDistributionSpec::EdtTaintedReplicated == m_pdsSegmentBase->Edt());
	}

	// Does this distribution match the given one?
	BOOL Matches(const CDistributionSpec *pds) const override;

	// Does this distribution satisfy the given one?
	BOOL FSatisfies(const CDistributionSpec *pds) const override;

	// Append enforcers to dynamic array for the given plan properties
	void AppendEnforcers(CMemoryPool *mp, CExpressionHandle &exprhdl,
						 CReqdPropPlan *prpp, CExpressionArray *pdrgpexpr,
						 CExpression *pexpr) override;

	// Return distribution partitioning type
	EDistributionPartitioningType
	Edpt() const override
	{
		return EdptNonPartitioned;
	}

	// Print function
	IOstream &OsPrint(IOstream &os) const override;

	// Factory method
	static CDistributionSpecReplicatedWorkers *PdsCreate(CMemoryPool *mp,
														  ULONG ulWorkers,
														  BOOL ignore_broadcast_threshold = false,
														  CDistributionSpec *pdsSegmentBase = nullptr);

	// Conversion function
	static CDistributionSpecReplicatedWorkers *
	PdsConvert(CDistributionSpec *pds)
	{
		GPOS_ASSERT(nullptr != pds);
		GPOS_ASSERT(EdtReplicatedWorkers == pds->Edt());
		return dynamic_cast<CDistributionSpecReplicatedWorkers *>(pds);
	}

	// Conversion function: const argument
	static const CDistributionSpecReplicatedWorkers *
	PdsConvert(const CDistributionSpec *pds)
	{
		GPOS_ASSERT(nullptr != pds);
		GPOS_ASSERT(EdtReplicatedWorkers == pds->Edt());
		return dynamic_cast<const CDistributionSpecReplicatedWorkers *>(pds);
	}

};	// class CDistributionSpecReplicatedWorkers

}  // namespace gpopt

#endif	// !GPOPT_CDistributionSpecReplicatedWorkers_H

// EOF
