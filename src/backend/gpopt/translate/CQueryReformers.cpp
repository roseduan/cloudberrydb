extern "C" {
#include "postgres.h"

#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "optimizer/walkers.h"
#include "utils/rel.h"
}

#include "gpopt/base/CUtils.h"
#include "gpopt/gpdbwrappers.h"
#include "gpopt/mdcache/CMDAccessor.h"
#include "gpopt/mdcache/CMDAccessorUtils.h"
#include "gpopt/translate/CQueryReformers.h"
#include "gpopt/translate/CTranslatorDXLToPlStmt.h"
#include "naucrates/exception.h"
#include "naucrates/md/IMDAggregate.h"
#include "naucrates/md/IMDScalarOp.h"
#include "naucrates/md/IMDTypeBool.h"

using namespace gpdxl;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CQueryReformers::ReformSubQueries
//
//	@doc:
//		ORGINAL QUERY:
//			SELECT * from (select * from foo, boo where foo.a = boo.a and a > 1) s1,
//						  (select * from foo, boo where foo.a = boo.a and a > 2) s2;
// 		NEW QUERY:
//			with cte(
//				SELECT * from foo, boo where foo.a = boo.a
//			)
//			SELECT * from (select * from cte where a > 1) s1,
//						  (select * from cte where a > 2) s2;
//---------------------------------------------------------------------------

Query *
CQueryReformers::ReformSubQueries(CMemoryPool *mp, CMDAccessor *md_accessor,
								  Query *query, ULONG query_level, BOOL *free_old)
{
	(*free_old) = false;

	// each subquery is identified by the rangetblref
	List *subqrefs = GetSubQueryCandidates(query, query_level);

	if (gpdb::ListLength(subqrefs) < 2)
		return query;

	// grouped same relid subqueries
	List *sq_relids = NIL;

	List *newsubqrefs = GetSubqueryRelids(query, subqrefs, &sq_relids);
	// replace old subquery refs with new ones
	gpdb::ListFree(subqrefs); 
	subqrefs = newsubqrefs;

	List *comm_subq_refs = GroupedSameRelidsSubquery(subqrefs, sq_relids);

	gpdb::ListFreeDeep(sq_relids);
	gpdb::ListFree(subqrefs);

	if (gpdb::ListLength(comm_subq_refs) == 0)
		return query;

	Query *new_query = (Query *) gpdb::CopyObject(query);
	(*free_old) = true;

	ConjunctiveSubQueryJoinQual(new_query, comm_subq_refs);

	// filter out matched subqueries
	List *matched_diffquals;
	List *matched_subq_refs =
		GroupedMatchSubquery(new_query, comm_subq_refs, &matched_diffquals);
	gpdb::ListFreeDeep(comm_subq_refs);

	if (gpdb::ListLength(matched_subq_refs) == 0)
		return new_query;

	ListCell *lcsubq, *lcdiffquals;

	int counter = 0;
	ForBoth(lcsubq, matched_subq_refs, lcdiffquals, matched_diffquals)
	{
		new_query =
			ExtractCteQuery(mp, md_accessor, new_query, (List *) lfirst(lcsubq),
							(Bitmapset *) lfirst(lcdiffquals), counter);
		counter ++;
	}

	return new_query;
}

Query *
CQueryReformers::ExtractCteQuery(CMemoryPool *mp, CMDAccessor *md_accessor,
								 Query *query, List *subqueryrefs,
								 Bitmapset *diffquals, int id)
{
	RangeTblEntry *rte = RTRefLC2RTE(query, gpdb::ListHead(subqueryrefs));
	Query *cte_query =
		(Query *) gpdb::CopyObject(const_cast<Query *>(rte->subquery));

	// mutate cte query tree
	{
		ListCell *lc;
		List *uppers = StripCteQueryTlist(cte_query);
		uppers =
			gpdb::ListConcat(uppers, StripCteQueryQual(cte_query, diffquals));

		CollectVarContext ctx;
		ctx.vars = NIL;
		ctx.lowerquery = cte_query;

		gpdb::WalkExpressionTree((Node *) uppers, (ExprWalkerFn) CollectExprVar,
								 &ctx);

		ForEach(lc, ctx.vars)
		{
			ULONG arity = gpdb::ListLength(cte_query->targetList) + 1;
			CWStringConst str_unnamed_col(GPOS_WSZ_LIT("?column?"));
			TargetEntry *new_target_entry = gpdb::MakeTargetEntry(
				(Expr *) lfirst(lc), (AttrNumber) arity,
				CTranslatorUtils::CreateMultiByteCharStringFromWCString(
					str_unnamed_col.GetBuffer()),
				false /*resjunk */);

			cte_query->targetList =
				gpdb::LAppend(cte_query->targetList, new_target_entry);
		}

		cte_query->hasAggs = false;

		// ammend quals if only one qual left
		if (cte_query->jointree->quals &&
			IsA(cte_query->jointree->quals, BoolExpr))
		{
			BoolExpr *bexpr = castNode(BoolExpr, cte_query->jointree->quals);
			if (gpdb::ListLength(bexpr->args) == 1)
			{
				cte_query->jointree->quals = (Node *)gpdb::ListNth(bexpr->args, 0);
			}
		}

		// add a dummy target entry if all target entries are stripped
		if (cte_query->targetList == NIL)
		{
			CWStringConst str_unnamed_col(GPOS_WSZ_LIT("?column?"));
			TargetEntry *new_target_entry = gpdb::MakeTargetEntry(
				(Expr *) gpdb::MakeBoolConst(true, false /* isnull */),
				(AttrNumber) 1,
				CTranslatorUtils::CreateMultiByteCharStringFromWCString(
					str_unnamed_col.GetBuffer()),
				false /*resjunk */);

			cte_query->targetList =
				gpdb::LAppend(cte_query->targetList, new_target_entry);
		}
	}

	// generate cte expr
	WCHAR wszBuf[64];
	CWStringStatic str(wszBuf, GPOS_ARRAY_SIZE(wszBuf));
	str.AppendFormat(GPOS_WSZ_LIT("GCTE_%d"), id);

	ListCell *lctlst;
	CommonTableExpr *cte_expr = makeNode(CommonTableExpr);
	cte_expr->ctename = CTranslatorUtils::CreateMultiByteCharStringFromWCString(
		str.GetBuffer());
	cte_expr->ctequery = (Node *) cte_query;
	cte_expr->location = -1;
	foreach (lctlst, cte_query->targetList)
	{
		TargetEntry *te = (TargetEntry *) lfirst(lctlst);

		cte_expr->ctecolnames = gpdb::LAppend(cte_expr->ctecolnames, makeString(pstrdup(te->resname)));
		cte_expr->ctecoltypes = gpdb::LAppendOid(cte_expr->ctecoltypes, exprType((Node *) te->expr));
		cte_expr->ctecoltypmods = gpdb::LAppendInt(cte_expr->ctecoltypmods, exprTypmod((Node *) te->expr));
		cte_expr->ctecolcollations = gpdb::LAppendOid(cte_expr->ctecolcollations, exprCollation((Node *) te->expr));
	}

	query->cteList = gpdb::LAppend(query->cteList, cte_expr);

	// fix subquery rte
	ListCell *lcref;
	foreach (lcref, subqueryrefs)
	{
		RangeTblEntry *rte = RTRefLC2RTE(query, lcref);
		Query *subquery = rte->subquery;
		List *upperquals = StripCteQueryQual(subquery, diffquals);

		RangeTblEntry *new_subrte = MakeNode(RangeTblEntry);

		new_subrte->rtekind = RTE_CTE;
		new_subrte->ctename =
			CTranslatorUtils::CreateMultiByteCharStringFromWCString(
				str.GetBuffer());
		new_subrte->ctelevelsup = 1;
		new_subrte->coltypes = (List *) gpdb::CopyObject(cte_expr->ctecoltypes);
		new_subrte->coltypmods = (List *) gpdb::CopyObject(cte_expr->ctecoltypmods);
		new_subrte->colcollations = (List *) gpdb::CopyObject(cte_expr->ctecolcollations);
		new_subrte->eref =
			gpdb::MakeAlias(new_subrte->ctename, cte_expr->ctecolnames);
		new_subrte->inFromCl = true;

		FromExpr *fromexpr = MakeNode(FromExpr);
		RangeTblRef *rtref = MakeNode(RangeTblRef);
		fromexpr->fromlist = gpdb::LAppend(fromexpr->fromlist, rtref);
		rtref->rtindex = 1;

		upperquals = (List *) gpdb::MutateExpressionTree(
			(Node *) upperquals, (MutatorWalkerFn) FixUpperExprByTlist,
			cte_query->targetList);
		subquery->targetList = (List *) gpdb::MutateExpressionTree(
			(Node *) subquery->targetList,
			(MutatorWalkerFn) FixUpperExprByTlist, cte_query->targetList);

		if (gpdb::ListLength(upperquals) == 0)
		{
			fromexpr->quals = NULL;
		}
		else if (gpdb::ListLength(upperquals) == 1)
		{
			fromexpr->quals = (Node *) gpdb::ListNth(upperquals, 0);
		}
		else
		{
			BoolExpr *bExpr = MakeNode(BoolExpr);
			bExpr->boolop = AND_EXPR;
			bExpr->location = -1;
			bExpr->args = upperquals;
			fromexpr->quals = (Node *) bExpr;
		}

		subquery->jointree = fromexpr;
		subquery->rtable = gpdb::LAppend(NIL, new_subrte);
		cte_expr->cterefcount++;
	}

	return query;
}

List *
CQueryReformers::StripCteQueryTlist(Query *query)
{
	ListCell *lc;
	List *newtlist = NIL;
	List *uppertlist = NIL;

	foreach (lc, query->targetList)
	{
		TargetEntry *tentry = lfirst_node(TargetEntry, lc);

		if (IsA(tentry->expr, Var))
			newtlist = lappend(newtlist, tentry);
		else
			uppertlist = lappend(uppertlist, tentry);
	}

	query->targetList = newtlist;

	return uppertlist;
}

List *
CQueryReformers::StripCteQueryQual(Query *query, Bitmapset *diffquals)
{
	ListCell *lc;
	List *newquals = NIL;
	List *upperquals = NIL;
	if (query->jointree->quals && IsA(query->jointree->quals, BoolExpr))
	{
		BoolExpr *joinquals = castNode(BoolExpr, query->jointree->quals);
		int counter;

		ForEachWithCount(lc, joinquals->args, counter)
		{
			if (gpdb::BmsIsMember(counter, diffquals))
				upperquals = gpdb::LAppend(upperquals, lfirst(lc));
			else
				newquals = gpdb::LAppend(newquals, lfirst(lc));
		}

		/*
		 * Normalize the residual BoolExpr: an AND/OR with 0 or 1 args is
		 * invalid for ORCA's scalar translator (asserts in
		 * TranslateBoolExprToDXL). Drop the BoolExpr wrapper entirely when
		 * empty, or unwrap to the single remaining arg.
		 */
		int n = gpdb::ListLength(newquals);
		if (n == 0)
			query->jointree->quals = NULL;
		else if (n == 1)
			query->jointree->quals = (Node *) gpdb::ListNth(newquals, 0);
		else
			joinquals->args = newquals;
	}
	else
	{
		if (gpdb::BmsIsMember(SINGLE_QUAL_ID, diffquals))
		{
			upperquals = gpdb::LAppend(upperquals, query->jointree->quals);
			query->jointree->quals = NULL;
		}
	}

	return upperquals;
}

Query *
CQueryReformers::ReformQuery(CMemoryPool *mp, CMDAccessor *md_accessor,
							 Query *query, ULONG query_level)
{
	BOOL free_old = false;

	Query *new_query = CQueryReformers::ReformSubQueries(
		mp, md_accessor, query, query_level, &free_old);

	if (free_old)
		gpdb::GPDBFree(query);

	return new_query;
}

List *
CQueryReformers::GetSubQueryCandidates(Query *query, ULONG query_level)
{
	ListCell *lc;
	List *subq_refs = NIL;

	// do not normalize if rangetblentry smaller than 2
	// do not normalize subquery
	if (2 > gpdb::ListLength(query->jointree->fromlist) || query_level != 0)
	{
		return subq_refs;
	}

	// pick the simple subquery rangetblentry as the candidate
	ForEach(lc, query->jointree->fromlist)
	{
		ListCell *lc_t;

		if (!IsA(lfirst(lc), RangeTblRef))
			continue;

		RangeTblEntry *rte = RTRefLC2RTE(query, lc);

		if (rte->rtekind == RTE_SUBQUERY)
		{
			ListCell *lc_query;
			Query *subquery = rte->subquery;
			bool iscandidate = !subquery->hasWindowFuncs &&
							   !subquery->hasTargetSRFs &&
							   !subquery->hasSubLinks &&
							   !subquery->hasDynamicFunctions &&
							   !subquery->hasFuncsWithExecRestrictions &&
							   !subquery->hasDistinctOn &&
							   !subquery->hasRecursive &&
							   !subquery->hasModifyingCTE &&
							   !subquery->hasForUpdate &&
							   !subquery->hasRowSecurity &&
							   (subquery->cteList == NIL);

			iscandidate = iscandidate &&
						subquery->groupClause == NIL &&
						subquery->groupingSets == NIL &&
						subquery->havingQual == NULL &&
						subquery->windowClause == NIL &&
						subquery->distinctClause == NIL &&
						subquery->sortClause == NIL &&
						subquery->scatterClause == NIL &&
						subquery->limitCount == NULL &&
						subquery->limitOffset == NULL &&
						subquery->rowMarks == NIL &&
						subquery->setOperations == NULL &&
						subquery->constraintDeps == NIL &&
						subquery->withCheckOptions == NIL &&
						subquery->intoPolicy == NULL &&
						subquery->parentStmtType == PARENTSTMTTYPE_NONE &&
						subquery->expandMatViews == false;

			iscandidate = iscandidate &&
						  !gpdb::ContainVarsOfLevelOrAbove((Node *) subquery, 1);

			ForEach(lc_t, subquery->targetList)
			{
				TargetEntry *tentry = (TargetEntry *) lfirst(lc_t);

				// FIXME: more Expr need to support
				if (!IsA(tentry->expr, Aggref) && !IsA(tentry->expr, Var) &&
					!IsA(tentry->expr, Const))
					iscandidate = false;
			}

			ForEach(lc_query, subquery->jointree->fromlist)
			{
				//FIXME: do not handle other Expr yet
				iscandidate = iscandidate &&
							  (IsA(lfirst(lc_query), RangeTblRef) &&
							   (RTRefLC2RTE(subquery, lc_query)->rtekind == RTE_RELATION));
			}

			if (iscandidate)
				subq_refs = gpdb::LAppend(subq_refs, lfirst(lc));
		}
	}

	return subq_refs;
}

RangeTblEntry *
CQueryReformers::RTRefLC2RTE(Query *query, ListCell *lc)
{
	RangeTblRef *ref = (RangeTblRef *) lfirst(lc);
	return (RangeTblEntry *) gpdb::ListNth(query->rtable, ref->rtindex - 1);
}

List *
CQueryReformers::GetSubqueryRelids(Query *query, List *refs, List **pRelids)
{
	List *new_refs = NIL;
	List *sq_relids = NIL;
	ListCell *lcref;

	ForEach(lcref, refs)
	{
		List *relids = NIL;
		ListCell *lc;
		RangeTblEntry *rte = RTRefLC2RTE(query, lcref);
		Query *subq = rte->subquery;
		ForEach(lc, subq->jointree->fromlist)
		{
			RangeTblEntry *subrte = RTRefLC2RTE(subq, lc);
			relids = gpdb::LAppendOid(relids, subrte->relid);
		}

		if (relids == NIL)
			continue;

		sq_relids = gpdb::LAppend(sq_relids, relids);
		new_refs = gpdb::LAppend(new_refs, lfirst(lcref));
	}
	// FIXME:: although Reformer only considers inner join, so far all matched
	// queries the relids need to keep same order, otherwise, varno can not be
	// mapped correctly.

	*pRelids = sq_relids;

	return new_refs;
}

List *
CQueryReformers::GroupedSameRelidsSubquery(List *subqrefs, List *sq_relids)
{
	List *grouped_subqrefs = NIL;
	List *newsubqrefs = NIL;
	List *newsq_relids = NIL;
	ListCell *lcref, *lcrelids;

	while (subqrefs)
	{
		List *fst_relids = NIL;
		List *same_relid_refs = NIL;

		ForBoth(lcref, subqrefs, lcrelids, sq_relids)
		{
			if (same_relid_refs == NIL)
			{
				same_relid_refs = gpdb::LAppend(same_relid_refs, lfirst(lcref));
				fst_relids = (List *) lfirst(lcrelids);
				continue;
			}

			if (gpdb::Equals(fst_relids, lfirst(lcrelids)))
			{
				// relids matched, group together
				same_relid_refs = gpdb::LAppend(same_relid_refs, lfirst(lcref));
			}
			else
			{
				// not matched, waiting for next loop
				newsubqrefs = gpdb::LAppend(newsubqrefs, lfirst(lcref));
				newsq_relids = gpdb::LAppend(newsq_relids, lfirst(lcrelids));
			}
		}

		if (gpdb::ListLength(same_relid_refs) > 1)
			grouped_subqrefs = gpdb::LAppend(grouped_subqrefs, same_relid_refs);

		// handle the rest
		subqrefs = newsubqrefs;
		sq_relids = newsq_relids;
		newsubqrefs = NIL;
		newsq_relids = NIL;
	}

	return grouped_subqrefs;
}

List *
CQueryReformers::GroupedMatchSubquery(Query *query, List *comm_subq_refs, List **pDiffQuals)
{
	ListCell *lc;
	List *match_subq_refs = NIL;
	*pDiffQuals = NIL;
	ForEach(lc, comm_subq_refs)
	{
		int counter;
		ListCell *lcref;
		ListCell *lcfst;
		List *subqrefs = lfirst_node(List, lc);
		List *newsubqrefs = NIL;
		Query *fstquery;
		Node *fstquals;

		ForEachWithCount(lcref, subqrefs, counter)
		{
			bool match;
			if (counter == 0)
			{
				fstquery = RTRefLC2RTE(query, lcref)->subquery;
				fstquals = fstquery->jointree->quals;
				lcfst = lcref;
				continue;
			}

			ListCell *lc1, *lc2;
			Query *curquery = RTRefLC2RTE(query, lcref)->subquery;
			Node *curqual = curquery->jointree->quals;

			// both tlist requires equal
			ForBoth(lc1, fstquery->targetList, lc2, curquery->targetList)
			{
				TargetEntry *tentry = (TargetEntry *) lfirst(lc1);
				TargetEntry *centry = (TargetEntry *) lfirst(lc2);

				match = (tentry->expr->type == centry->expr->type) &&
						(tentry->resno == centry->resno) &&
						//resname does not match is ok
						(tentry->ressortgroupref == centry->ressortgroupref) &&
						(tentry->resorigtbl == centry->resorigtbl) &&
						(tentry->resorigcol == centry->resorigcol) &&
						(tentry->resjunk == centry->resjunk);

				match = match && ExprSimilar((Node *) tentry->expr, (Node *) centry->expr);
			}

			match = match && ExprSimilar(fstquals, curqual);

			if (match)
			{
				if (newsubqrefs == NIL)
					newsubqrefs = gpdb::LAppend(newsubqrefs, lfirst(lcfst));

				newsubqrefs = gpdb::LAppend(newsubqrefs, lfirst(lcref));
			}
		}

		if (newsubqrefs)
		{
			// gather matched subqueries diff quals
			Bitmapset *diffquals = nullptr;
			ForEachWithCount(lcref, newsubqrefs, counter)
			{
				if (counter == 0)
				{
					fstquery = RTRefLC2RTE(query, lcref)->subquery;
					fstquals = fstquery->jointree->quals;
					continue;
				}
				Query *curquery = RTRefLC2RTE(query, lcref)->subquery;
				Node *curqual = curquery->jointree->quals;

				if (fstquals && IsA(fstquals, BoolExpr))
				{
					BoolExpr *bExpr1 = castNode(BoolExpr, fstquals);
					BoolExpr *bExpr2 = castNode(BoolExpr, curqual);
					int qualid = 0;
					ListCell *lc1, *lc2;
					ForBoth(lc1, bExpr1->args, lc2, bExpr2->args)
					{
						if (!gpdb::Equals(lfirst(lc1), lfirst(lc2)))
							diffquals = gpdb::BmsAddMember(diffquals, qualid);

						qualid++;
					}
				}
				else
				{
					if (!gpdb::Equals(fstquals, curqual))
						diffquals = gpdb::BmsAddMember(diffquals, SINGLE_QUAL_ID);
				}
			}
			*pDiffQuals = gpdb::LAppend(*pDiffQuals, diffquals);

			// gather matched subqueries
			match_subq_refs = gpdb::LAppend(match_subq_refs, newsubqrefs);
		}
	}

	return match_subq_refs;
}

bool
CQueryReformers::ExprSimilar(Node *lhs, Node *rhs)
{
	ListCell *lc1, *lc2;
	if (lhs == NULL || rhs == NULL)
		return (lhs == NULL) && (rhs == NULL);

	if (lhs->type != rhs->type)
		return false;

	if (IsA(lhs, Const))
		return true;
	else if (IsA(lhs, Var))
		return gpdb::Equals(lhs, rhs);
	else if (IsA(lhs, Aggref))
	{
		Aggref *agg1 = castNode(Aggref, lhs);
		Aggref *agg2 = castNode(Aggref, rhs);

		// aggfnoid can be different
		if (agg1->inputcollid == agg2->inputcollid &&
			gpdb::Equals(agg1->aggargtypes, agg2->aggargtypes) &&
			gpdb::ListLength(agg1->aggdirectargs) == gpdb::ListLength(agg2->aggdirectargs) &&
			gpdb::ListLength(agg1->args) == gpdb::ListLength(agg2->args))
		{

			ForBoth(lc1, agg1->aggdirectargs, lc2, agg2->aggdirectargs)
			{
				if (!ExprSimilar((Node *) lfirst(lc1),
									 (Node *) lfirst(lc2)))
					return false;
			}

			ForBoth(lc1, agg1->args, lc2, agg2->args)
			{
				if (!ExprSimilar((Node *) lfirst(lc1),
									 (Node *) lfirst(lc2)))
					return false;
			}

			if (ExprSimilar((Node *) agg1->aggfilter, (Node *) agg2->aggfilter))
				return true;
		}

		return false;
	}
	else if (IsA(lhs, FuncExpr))
	{
		FuncExpr *f1 = castNode(FuncExpr, lhs);
		FuncExpr *f2 = castNode(FuncExpr, rhs);

		// funcid can be different
		if (f1->funcresulttype == f2->funcresulttype &&
			f1->funcretset == f2->funcretset &&
			f1->funcvariadic == f2->funcvariadic &&
			f1->funcformat == f2->funcformat &&
			f1->funccollid == f2->funccollid &&
			f1->inputcollid == f2->inputcollid &&
			f1->is_tablefunc == f2->is_tablefunc &&
			gpdb::ListLength(f1->args) == gpdb::ListLength(f2->args))
		{
			ForBoth(lc1, f1->args, lc2, f2->args)
			{
				if (!ExprSimilar((Node *) lfirst(lc1),
									 (Node *) lfirst(lc2)))
					return false;
			}
			return true;
		}

		return false;
	}
	else if (IsA(lhs, OpExpr))
	{
		OpExpr *op1 = castNode(OpExpr, lhs);
		OpExpr *op2 = castNode(OpExpr, rhs);

		// opno can be different
		// opfuncid can be different
		if (op1->opresulttype == op2->opresulttype &&
			op1->opretset == op2->opretset &&
			op1->opcollid == op2->opcollid &&
			op1->inputcollid == op2->inputcollid &&
			gpdb::ListLength(op1->args) == gpdb::ListLength(op2->args))
		{
			ForBoth(lc1, op1->args, lc2, op2->args)
			{
				if (!ExprSimilar((Node *) lfirst(lc1),
									 (Node *) lfirst(lc2)))
					return false;
			}
			return true;
		}

		return false;
	}
	else if (IsA(lhs, BoolExpr))
	{
		BoolExpr *bExpr1 = castNode(BoolExpr, lhs);
		BoolExpr *bExpr2 = castNode(BoolExpr, rhs);

		if (bExpr1->boolop == bExpr2->boolop &&
			gpdb::ListLength(bExpr1->args) == gpdb::ListLength(bExpr2->args))
		{
			ForBoth(lc1, bExpr1->args, lc2, bExpr2->args)
			{
				if (!ExprSimilar((Node *) lfirst(lc1),
									 (Node *) lfirst(lc2)))
					return false;
			}
			return true;
		}

		return false;
	}
	else
		return gpdb::Equals(lhs, rhs);

	return true;
}

void
CQueryReformers::ConjunctiveSubQueryJoinQual(Query *query, List *comm_subq_refs)
{
	ListCell *lc;
	ForEach(lc, comm_subq_refs)
	{
		ListCell *lcref;
		List *subqrefs = lfirst_node(List, lc);

		ForEach(lcref, subqrefs)
		{
			Query *subq = RTRefLC2RTE(query, lcref)->subquery;
			if (subq->jointree->quals)
			{
				subq->jointree->quals = (Node *) gpdb::CanonicalizeQual(
					(Expr *) subq->jointree->quals, false);
			}
		}
	}
}

BOOL
CQueryReformers::CollectExprVar(Node *node, CollectVarContext *ctx)
{
	if (node == NULL)
		return false;

	if (IsA(node, Var))
	{
		ListCell *lc;
		Var *v = castNode(Var, node);
		uint32 counter;

		ForEachWithCount(lc, ctx->lowerquery->targetList, counter)
		{
			TargetEntry *tentry = lfirst_node(TargetEntry, lc);
			if (gpdb::Equals(tentry->expr, node))
				break;
		}

		if (counter == gpdb::ListLength(ctx->lowerquery->targetList))
		{
			ctx->vars = gpdb::LAppend(ctx->vars, v);
		}

		return false;
	}

	return gpdb::WalkExpressionTree(node, (ExprWalkerFn) CollectExprVar, ctx);
}

Node *
CQueryReformers::FixUpperExprByTlist(Node *node, List *tlist)
{
	if (node == NULL)
		return node;

	if (IsA(node, Var))
	{
		ListCell *lc;
		uint32 counter;
		Var *v = castNode(Var, node);
		ForEachWithCount(lc, tlist, counter)
		{
			TargetEntry *tentry = (TargetEntry *) lfirst(lc);
			if (gpdb::Equals(tentry->expr, v))
			{
				v->varno = 1;
				v->varattno = tentry->resno;
				break;
			}
		}

		if (counter == gpdb::ListLength(tlist))
			GPOS_RAISE(CException::ExmaInvalid, CException::ExmiInvalid,
					   GPOS_WSZ_LIT("Can not find related TargetEntry by Upper Expr"));

		return (Node *)v;
	}
	else if (IsA(node, Const))
	{
		return node;
	}

	return gpdb::MutateExpressionTree(
		node, (MutatorWalkerFn) FixUpperExprByTlist, tlist);
}
