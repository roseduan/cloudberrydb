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
//		CXformLeftSemiJoin2ParallelRightSemiHashJoin.h
//
//	@doc:
//		Transform left semi join to parallel right semi hash join
//		(build = outer/left).
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformLeftSemiJoin2ParallelRightSemiHashJoin_H
#define GPOPT_CXformLeftSemiJoin2ParallelRightSemiHashJoin_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformImplementation.h"

namespace gpopt
{
using namespace gpos;

class CXformLeftSemiJoin2ParallelRightSemiHashJoin
	: public CXformImplementation
{
public:
	CXformLeftSemiJoin2ParallelRightSemiHashJoin(
		const CXformLeftSemiJoin2ParallelRightSemiHashJoin &) = delete;

	// ctor
	explicit CXformLeftSemiJoin2ParallelRightSemiHashJoin(CMemoryPool *mp);

	// dtor
	~CXformLeftSemiJoin2ParallelRightSemiHashJoin() override = default;

	// ident accessors
	EXformId
	Exfid() const override
	{
		return ExfLeftSemiJoin2ParallelRightSemiHashJoin;
	}

	// return a string for xform name
	const CHAR *
	SzId() const override
	{
		return "CXformLeftSemiJoin2ParallelRightSemiHashJoin";
	}

	// compute xform promise for a given expression handle
	EXformPromise Exfp(CExpressionHandle &exprhdl) const override;

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const override;

};	// class CXformLeftSemiJoin2ParallelRightSemiHashJoin

}  // namespace gpopt

#endif	// !GPOPT_CXformLeftSemiJoin2ParallelRightSemiHashJoin_H

// EOF
