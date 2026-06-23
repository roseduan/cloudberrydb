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
//		CXformLeftAntiSemiJoin2ParallelRightAntiSemiHashJoin.h
//
//	@doc:
//		Transform left anti semi join to parallel right anti semi hash join
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformLeftAntiSemiJoin2ParallelRightAntiSemiHashJoin_H
#define GPOPT_CXformLeftAntiSemiJoin2ParallelRightAntiSemiHashJoin_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformImplementation.h"

namespace gpopt
{
using namespace gpos;

class CXformLeftAntiSemiJoin2ParallelRightAntiSemiHashJoin
	: public CXformImplementation
{
public:
	CXformLeftAntiSemiJoin2ParallelRightAntiSemiHashJoin(
		const CXformLeftAntiSemiJoin2ParallelRightAntiSemiHashJoin &) = delete;

	// ctor
	explicit CXformLeftAntiSemiJoin2ParallelRightAntiSemiHashJoin(
		CMemoryPool *mp);

	// dtor
	~CXformLeftAntiSemiJoin2ParallelRightAntiSemiHashJoin() override = default;

	// ident accessors
	EXformId
	Exfid() const override
	{
		return ExfLeftAntiSemiJoin2ParallelRightAntiSemiHashJoin;
	}

	// return a string for xform name
	const CHAR *
	SzId() const override
	{
		return "CXformLeftAntiSemiJoin2ParallelRightAntiSemiHashJoin";
	}

	// compute xform promise for a given expression handle
	EXformPromise Exfp(CExpressionHandle &exprhdl) const override;

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const override;

};	// class CXformLeftAntiSemiJoin2ParallelRightAntiSemiHashJoin

}  // namespace gpopt

#endif	// !GPOPT_CXformLeftAntiSemiJoin2ParallelRightAntiSemiHashJoin_H

// EOF
