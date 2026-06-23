/*-------------------------------------------------------------------------
 *
 * planner_vec.c
 *	  The query vectorized optimizer external interface.
 *
 * Copyright (c) 2016-Present Hashdata, Inc. 
 *
 *
 * IDENTIFICATION
 *		src/backend/optimizer/planner_vec.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "optimizer/planner.h"
#include "cdb/cdbplan.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbmutate.h"
#include "commands/dbcommands.h"
#include "commands/extension.h"
#include "commands/defrem.h"
#include "optimizer/cost.h"
#include "optimizer/planshare.h"
#include "optimizer/planmain.h"
#include "miscadmin.h"
#include "catalog/pg_attribute_encoding.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "parser/parsetree.h"
#include "parser/scansup.h"
#include "nodes/parsenodes.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "access/heapam.h"
#include "nodes/nodes.h"
#include "cdb/cdbllize.h"
#include "optimizer/walkers.h"
#include "utils/guc.h"
#include "portability/instr_time.h"
#include "utils/fmgroids.h"
#include "port.h"
#include "utils/pg_locale.h"
#include "utils/syscache.h"

#include "optimizer/planner_vec.h"
#include "vecexecutor/executor.h"
#include "vecnodes/nodes.h"
#include "utils/fmgr_vec.h"
#include "utils/guc_vec.h"
#include "utils/wrapper.h"
#include "utils/fmgr_vec.h"
#include "transform.h"

#include <sys/stat.h>
#include <sys/file.h>
#include <regex.h>

#define FALLBACK_LOG(fmt, ...) do { \
	if (print_fallback_log) \
		elog(LOG, "Fallback to non-vectorization: " fmt, ##__VA_ARGS__); \
} while (0)

planner_hook_type           planner_prev = NULL;

static void arrow_aggref_adapter(List *targetlist);
static void match_replace_tl(List *dst, List* src);
static Node *agg_trans_mutator(Node *node, void *context);
static Node *vectorize_plan_mutator(Node *node, void *context);
static bool is_plan_vectorable(Plan* plan, List *rtable);
static bool is_expr_vectorable(Expr* expr, void *context);
static bool is_hash_expr_vectorable(Expr* expr, void *context);
static bool is_relation_vectorable(SeqScan* seqscan, List *rtable, bool isForeign);
static bool is_sort_collation_vectorable(Sort *sort);
static bool fallback_distinct_junk(Plan *plan);
static bool fallback_nested_loop_jointype(Plan *plan);
static void demote_right_semi_anti_to_left(Plan *plan);
static bool joinclauses_type_different(Expr *node, void *context);
static bool processFunction(int pid, int queryid);
static bool GloablprocessFunction(int pid);
typedef struct PreNodeContext
{
	Node *plannedStmt;
	Node *parent_node;
	List *hashkeys;
}PreNodeContext;

static bool leftjoin_pull_antijoin(Node *node, PreNodeContext *context);
static bool is_foreign_scan_vectorable(ForeignScan *foreignscan);
static bool is_seventy_two_control(const char *query_string);

static char* vector_foreign_data_wapper_whitelist[] =
{
	"datalake_fdw"
};

typedef struct
{
	plan_tree_base_prefix base; /* Required prefix for plan_tree_walker/mutator */
	bool has_windowhashagg;         /* Flag to indicate if WindowAgg node is found */
} windowhashagg_check_context;

static bool windowhashagg_check_walker(Node *node, windowhashagg_check_context *context);
static bool windowhashagg_is_in_plan_tree(struct PlannedStmt *stmt);

static bool
is_type_vectorable(Oid typeOid)
{
	if (PGTypeToArrowID(typeOid) > 0)
		return true;
	FALLBACK_LOG("type %d not support", typeOid);
	return false;
}

static bool
is_func_vectorable(Oid funcOid, List* args)
{
	const FuncTable *fmgr = get_arrow_fmgr(funcOid);
	if (!fmgr)
	{
		FALLBACK_LOG("funcOid %d not support", funcOid);
		return false;
	}

	if (!fmgr->checkFunc(args))
	{
		FALLBACK_LOG("funcOid %d not extra check failed", funcOid);
		return false;
	}
	return true;
}

static bool
is_gandiva_func_support(Oid opno)
{
	char *name = get_opname(opno);
	if (!name)
		return false;
	if (0 == strcmp(name, "+"))
		return true;
	else if (0 == strcmp(name, "-"))
		return true;
	else if (0 == strcmp(name, "*"))
		return true;
	else if (0 == strcmp(name, "/"))
		return true;
	return false;
}

static bool
is_aggfn_vectorable(Oid aggfnOid)
{
	if (get_arrow_agg_fmgr(aggfnOid) != NULL)
		return true;
	FALLBACK_LOG("aggfnoid %d not support", aggfnOid);
	return false;
}

/*
 * Fixme: These types may appear in targetlist as intermediate type, but we
 * don't support the scan of this types. So we work around.
 * Remove this after all scanning type supported.
 */
static bool
is_scan_type_vectorable(Form_pg_attribute attr)
{
	Oid typeOid = attr->atttypid;
	if (typeOid == TIDOID && attr->attnum != SelfItemPointerAttributeNumber)
	{
		FALLBACK_LOG("scan type user tid not support");
		return false;
	}
	if (typeOid == BYTEAOID || type_is_array(typeOid))
	{
		FALLBACK_LOG("scan type %d not support", typeOid);
		return false;
	}
	if (typeOid == NUMERICOID)
	{
		int32_t numeric_type, precision;
		if (attr->atttypmod == -1)
		{
			FALLBACK_LOG("numeric type precision and scale not specified");
			return false;
		}
		numeric_type = attr->atttypmod - VARHDRSZ;
		precision = (numeric_type >> 16) & 0xffff;
		if (precision > VEC_NUMERIC_MAX_PRECISION)
		{
			FALLBACK_LOG("numeric type precision %d not support", precision);
			return false;
		}
	}
	return is_type_vectorable(typeOid);
}

typedef struct
{
	plan_tree_base_prefix base; /* Required prefix for
								 * plan_tree_walker/mutator */
	int			nextPlanId;
} assign_plannode_id_context;

/* cbdb remove this */
static bool assign_plannode_id_walker(Node *node, assign_plannode_id_context *ctxt);

static bool
assign_plannode_id_walker(Node *node, assign_plannode_id_context *ctxt)
{
	if (node == NULL)
		return false;

	if (is_plan_node(node))
		((Plan *) node)->plan_node_id = ctxt->nextPlanId++;

	if (IsA(node, SubPlan))
		return false;

	return plan_tree_walker(node, assign_plannode_id_walker, ctxt, true);
}

/*
 * assign_plannode_id - Assign an id for each plan node.
 * Used by gpmon and instrument.
 */
static void
assign_plannode_id(PlannedStmt *stmt)
{
	assign_plannode_id_context ctxt;
	ListCell   *lc;

	ctxt.base.node = (Node *) stmt->planTree;
	ctxt.nextPlanId = 0;

	assign_plannode_id_walker((Node *) stmt->planTree, &ctxt);

	foreach(lc, stmt->subplans)
	{
		Plan	   *subplan = lfirst(lc);

		Assert(subplan);

		assign_plannode_id_walker((Node *) subplan, &ctxt);
	}
}

static PlannedStmt *
generate_plan(Query *parse, const char *query_string, int cursorOptions, ParamListInfo boundParams, OptimizerOptions *optimizer_options)
{
	PlannedStmt *result;
	instr_time	starttime, endtime;
	extern planner_hook_type planner_prev;

	if (planner_prev)
	{
		if (gp_log_optimization_time)
			INSTR_TIME_SET_CURRENT(starttime);

		result = (*planner_prev) (parse, query_string, cursorOptions, boundParams, optimizer_options);

		if (gp_log_optimization_time)
		{
			INSTR_TIME_SET_CURRENT(endtime);
			INSTR_TIME_SUBTRACT(endtime, starttime);
			elog(LOG, "Planner Hook(s): %.3f ms", INSTR_TIME_GET_MILLISEC(endtime));
		}
	}
	else
	{
		result = standard_planner(parse, query_string, cursorOptions, boundParams, optimizer_options);
	}

	return result;
}

PlannedStmt *
planner_hook_wrapper(Query *parse, const char *query_string, int cursorOptions, ParamListInfo boundParams, OptimizerOptions *optimizer_options)
{
	PlannedStmt *result;
	bool try_result;
	bool optimizer_origin;
	extern bool force_vectorization;
	const int64 orca_spilling_mem_threshold = 1000LL * 1204 * 1024 * 1024;

	/* fallback for prepare and execute */
	/* fallback for cursor */
	if (!enable_vectorization || boundParams || cursorOptions != CURSOR_OPT_PARALLEL_OK)
	{
		return generate_plan(parse, query_string, cursorOptions, boundParams, optimizer_options);
	}

	init_vector_types();

	/* try bigger memory threshold for vector plan */
	if (enable_vector_optimizer)
		optimizer_spilling_mem_threshold = optimizer ? orca_spilling_mem_threshold : 0;

	/* try WindowHashAgg */
	OptimizerOptions *vec_optimizer_options = palloc(sizeof(OptimizerOptions));
	if (optimizer_options)
		memcpy(vec_optimizer_options, optimizer_options, sizeof(OptimizerOptions));
	else
		memset(vec_optimizer_options, 0, sizeof(OptimizerOptions));
	vec_optimizer_options->create_vectorization_plan = true;
	result = generate_plan(parse, query_string, cursorOptions, boundParams, vec_optimizer_options);
	try_result = try_vectorize_plan(result);
	if (try_result)
		return result;

	/* create plan again if WindowHashAgg is in plan tree */
	if (windowhashagg_is_in_plan_tree(result)) {
		result = generate_plan(parse, query_string, cursorOptions, boundParams, optimizer_options);
		try_result = try_vectorize_plan(result);
		if (try_result)
			return result;
	}

	/* try another optimizer */
	if (force_vectorization) {
		optimizer_origin = optimizer;
		PG_TRY();
		{
			optimizer = !optimizer;
			if (enable_vector_optimizer)
				optimizer_spilling_mem_threshold = optimizer ? orca_spilling_mem_threshold : 0;
			result = generate_plan(parse, query_string, cursorOptions, boundParams, optimizer_options);
		}
		PG_FINALLY();
		{
			optimizer = optimizer_origin;
			if(!try_vectorize_plan(result))
			{
				optimizer_spilling_mem_threshold = 0.0;
				result = generate_plan(parse, query_string, cursorOptions, boundParams, optimizer_options);
				FALLBACK_LOG("current plan cannot be vectorized");
			}
			else
			{
				elog(DEBUG2, "switch to another optimizer to generate vectorized plan");
			}
		}
		PG_END_TRY();
	}

	if (enable_vector_optimizer && enable_vector_memory_resource && is_seventy_two_control(query_string))
	{
		int loop = 0;
		while(++loop < 1500)
		{
			bool status = processFunction(getpid(), 172);
			if (status)
				break;
			else
				sleep(0.1);
		}
	}

	/*
	 * Safety net for JOIN_RIGHT_ANTI_NOTIN only.  PG 14 base now executes
	 * JOIN_RIGHT_SEMI / JOIN_RIGHT_ANTI natively (PG 16 16dc2703c54 +
	 * PG 18 4c87ddfb62a backports).  JOIN_RIGHT_ANTI_NOTIN remains
	 * vec-only (no PG upstream equivalent), so demote it when the plan
	 * is going to PG executor.
	 */
	if (!try_vectorize_plan(result))
		demote_right_semi_anti_to_left(result->planTree);

	return result;
}

static bool
is_seventy_two_control(const char *query_string)
{
	regex_t	 regex;
	int	     r;
	r = regcomp(&regex, seventy_two_control_partten, 0);
	if (r)
		return false;
	r = regexec(&regex, query_string, 0, NULL, 0);
	regfree(&regex);
	return REG_NOERROR == r;
}

/*
 * Try to generate a vectorized plan.
 */
bool
try_vectorize_plan(PlannedStmt *result)
{
	bool vectorable;
	int loop = 0;
	Plan *plan_copy;
	VectorExtensionContext *ext_ctx = NULL;
	PreNodeContext context;
	context.plannedStmt = (Node *)result;
	context.parent_node = NULL;
	context.hashkeys = NIL;

	plan_copy = copyObject(result->planTree);

	plan_copy = (Plan *) vectorize_plan_mutator((Node *) plan_copy, NULL);

	vectorable = is_plan_vectorable(plan_copy, result->rtable);
	if (!vectorable)
		return false;

	if (enable_vector_optimizer)
		leftjoin_pull_antijoin((Node *)plan_copy, &context);
	ext_ctx = (VectorExtensionContext *) palloc(sizeof(VectorExtensionContext));
	ext_ctx->base.type = T_ExtensibleNode;
	ext_ctx->base.extnodename = pstrdup(VECTOR_EXTENSION_CONTEXT);
	result->extensionContext = lappend(result->extensionContext, (ExtensibleNode *)ext_ctx);
	result->planTree = plan_copy;

	if (enable_vector_optimizer && enable_vector_memory_resource)
	{
		if (control_global_memory_resource > 1)
		{
			loop = 0;
			while(++loop < 1500)
			{
				bool status = GloablprocessFunction(getpid());
				if (status)
					break;
				else
					sleep(0.1);
			}
		}
	}
	/* after vectorization, should reassign plan node id */
	assign_plannode_id(result);

	return true;
}


Node *
vectorize_plan_mutator(Node *node, void *context)
{
	Plan *mutated;

	if (node == NULL)
		return NULL;

	mutated = (Plan *)plan_tree_mutator(
		node, vectorize_plan_mutator, context, false);

	if (IsA(mutated, Agg))
		arrow_aggref_adapter(mutated->targetlist);

	/* Match scale of Numeric Const with var type*/
	if (IsA(mutated, OpExpr))
	{
		OpExpr *op = (OpExpr *)mutated;

		if ((op->opfuncid == F_NUMERIC_EQ) ||
				(op->opfuncid == F_NUMERIC_NE) ||
				(op->opfuncid == F_NUMERIC_GT) ||
				(op->opfuncid == F_NUMERIC_GE) ||
				(op->opfuncid == F_NUMERIC_LT) ||
				(op->opfuncid == F_NUMERIC_LE) ||
				(op->opfuncid == F_NUMERIC_ADD) ||
				(op->opfuncid == F_NUMERIC_SUB))
		{
			Expr *arg1 = (Expr *)linitial(op->args);
			Expr *arg2 = (Expr *)lsecond(op->args);
			Const *con = NULL;
			int32 typmod = 0;

			if (IsA(arg1, Const))
			{
				con = (Const *)arg1;
				typmod = exprTypmod((Node *)arg2);
				con->consttypmod = typmod;
			}
			else if (IsA(arg2, Const))
			{
				con = (Const *)arg2;
				typmod = exprTypmod((Node *)arg1);
				con->consttypmod = typmod;
			}
		}
	}

	if (IsA(mutated, NestLoop))
	{
		/* remove material node in inner. */
		if (mutated->righttree && IsA(mutated->righttree, Material))
			mutated->righttree = mutated->righttree->lefttree;
	}

	/* Modify table may contains vars didn't match children's targetlist*/
	if (is_plan_node((Node *) mutated) && mutated->lefttree
			&& mutated->lefttree->targetlist)
	{
		/* Fixme: Support join mutated */
		if (!mutated->righttree)
			match_replace_tl(mutated->targetlist, mutated->lefttree->targetlist);
		/* Fixme: replace for qual should be supported */
	}

	return (Node *) mutated;
}

static inline bool
is_expr_fallback(Expr* expr, void *context)
{
	return !is_expr_vectorable(expr, context);
}

/*
 * true for vectorized plan.
 * false for fallback.
 *
 * This walker reverse normal expression walker. Return true to continue walker,
 * return false to stop.
 */
static bool
is_expr_vectorable(Expr* expr, void *context)
{
	if (expr == NULL)
		return true;

	/* Find the vector engine not support expression */
	switch (nodeTag(expr))
	{
		case T_Var:
			{
				Var *var = (Var *) expr;

				if (!is_type_vectorable(var->vartype))
					return false;
			}
			break;
		case T_Const:
			{
				Const* c = (Const *)expr;

				if (!is_type_vectorable(c->consttype))
					return false;
			}
			break;
		case T_OpExpr:
			{
				OpExpr	  *opexpr = (OpExpr *)expr;
				if (!is_func_vectorable(opexpr->opfuncid, opexpr->args) ||
						list_length(opexpr->args) == 1 ||
						!is_expr_vectorable((Expr*)opexpr->args, context) ||
						!is_type_vectorable(opexpr->opresulttype))
					return false;
			}
			break;
		case T_FuncExpr:
			{
				FuncExpr *func_expr = (FuncExpr *)expr;
				if (!is_func_vectorable(func_expr->funcid, func_expr->args) ||
						!is_expr_vectorable((Expr*)func_expr->args, context) ||
						!is_type_vectorable(func_expr->funcresulttype))
					return false;
			}
			break;
		case T_WindowFunc:
		case T_Aggref:
			{
				Oid typid  = InvalidOid;
				Oid funcid = InvalidOid;

				if (IsA(expr, Aggref))
				{
					Aggref *aggref = (Aggref *) expr;
					typid  = aggref->aggtype;
					funcid = aggref->aggfnoid;
					if(aggref->aggfilter != NULL)
					{
						FALLBACK_LOG("agg filter not support");
						return false;
					}
				}
				else /* WindowFunc */
				{
					WindowFunc *wfunc = (WindowFunc *) expr;
					typid  = wfunc->wintype;
					funcid = wfunc->winfnoid;
					if(wfunc->aggfilter != NULL)
					{
						FALLBACK_LOG("agg filter not support");
						return false;
					}
				}

				if (!is_type_vectorable(typid))
					return false;
				if (!is_aggfn_vectorable(funcid))
					return false;
			}
			break;
		case T_RelabelType:
			{
				RelabelType	  *relexpr = (RelabelType *)expr;

				if (!is_type_vectorable(relexpr->resulttype))
					return false;
			}
			break;
		case T_ScalarArrayOpExpr:
			{
				ScalarArrayOpExpr *arrayexpr = (ScalarArrayOpExpr *) expr;

				/* Fixme: Fully support of ScalarArrayOpExpr, only "IN" now. */
				if (strcmp(get_opname(arrayexpr->opno), "=")
						|| !IsA(lsecond(arrayexpr->args), Const)
						|| !arrayexpr->useOr)
				{
					FALLBACK_LOG("ScalarArrayOpExpr only support IN operator");
					return false;
				}

				/* IN operator doesn't support null::type[] as array, so const_expr must be not null */
				Const *const_expr = (Const *) lsecond(arrayexpr->args);
				if (const_expr->constisnull)
				{
					FALLBACK_LOG("ScalarArrayOpExpr only support IN operator");
					return false;
				}
			}
			break;
		case T_CaseExpr:
			{
				CaseExpr *caseexpr = (CaseExpr *) expr;

				if (!is_type_vectorable(caseexpr->casetype))
					return false;
				break;
			}
		case T_Param:
			{
				Param *param = (Param *) expr;
				if (param->paramkind != PARAM_EXEC)
				{
					FALLBACK_LOG("PARAM only support PARAM_EXEC");
					return false;
				}
				break;
			}
		case T_NullIfExpr:
		/* Expressions fully vectorable */
		case T_CaseTestExpr:
		case T_NullTest:
		case T_BoolExpr:
		case T_List:
		case T_SortGroupClause:
		case T_TargetEntry:
		case T_CoalesceExpr:
		case T_DistinctExpr:
			break;
		/* All other expression fallback */
		default:
			FALLBACK_LOG("Expression not support %s", nodeToString(expr));
			return false;
			break;
	}

	/* reaching here means current expression is vectorable, continue walker */
	return !expression_tree_walker((Node *) expr, is_expr_fallback, context);
}

/*
 * @Description: Check if it has unsupport expression in vector engine
 *
 * true for vectorized plan.
 * false for fallback.
 */
static bool
is_plan_vectorable(Plan* plan, List *rtable)
{
	if (plan == NULL)
		return true;

	if (!rtable)
	{
		FALLBACK_LOG("Query without table.");
		return false;
	}

	if (!is_plan_vectorable(plan->lefttree, rtable))
		return false;

	if (!is_plan_vectorable(plan->righttree, rtable))
		return false;

	if (plan->parallel_aware)
	{
		FALLBACK_LOG("Query with parallel.");
		return false;
	}

	switch (nodeTag(plan))
	{
		/* Check plan node to fallback unvectorable cases: */
		case T_SeqScan:
			{
				SeqScan *seqscan = (SeqScan*)plan;

				if (!is_relation_vectorable((Scan *)seqscan, rtable, false))
					return false;
			}
			break;
		case T_Sort:
			{
				Sort *sort = (Sort *)plan;

				if (!is_sort_collation_vectorable(sort))
					return false;
			}
			break;
		case T_Result:
			{
				if (!plan->lefttree)
				{
					FALLBACK_LOG("Result without table.");
					return false;
				}
			}
			break;
		case T_Agg:
			{
				Agg *agg = (Agg *)plan;

				if (agg->aggstrategy == AGG_MIXED || agg->groupingSets
						|| (fallback_distinct_junk(plan)))
				{
					FALLBACK_LOG("Unsupported Agg type.");
					return false;
				}
				for (int i = 0; i < agg->numCols; i++)
				{
					if (agg->grpOperators[i] == ARRAY_EQ_OP)
					{
						FALLBACK_LOG("ARRAY_EQ_OP in agg.");
						return false;
					}
				}
			}
			break;
		case T_WindowAgg:
			{
				WindowAgg *wagg = (WindowAgg *)plan;

				int frameOptions = wagg->frameOptions;
				if (wagg->startOffset || wagg->endOffset ||
				    (frameOptions != FRAMEOPTION_DEFAULTS &&
				     frameOptions != (FRAMEOPTION_DEFAULTS | FRAMEOPTION_NONDEFAULT) &&
				     frameOptions != (FRAMEOPTION_DEFAULTS | FRAMEOPTION_NONDEFAULT |
				                      FRAMEOPTION_BETWEEN) &&
				     frameOptions != (FRAMEOPTION_NONDEFAULT | FRAMEOPTION_ROWS |
				                      FRAMEOPTION_START_UNBOUNDED_PRECEDING |
				                      FRAMEOPTION_END_CURRENT_ROW) &&
				     frameOptions != (FRAMEOPTION_NONDEFAULT | FRAMEOPTION_ROWS |
				                      FRAMEOPTION_BETWEEN |
				                      FRAMEOPTION_START_UNBOUNDED_PRECEDING |
				                      FRAMEOPTION_END_CURRENT_ROW)
				    )
				   )
				{
					FALLBACK_LOG("frameOption: %d with "
					             "\"frame\" clause not supported.", frameOptions);
					return false;
				}

				if (wagg->ordNumCols > 0)
				{
					Plan *child = plan;
					while (child && IsA(child, WindowAgg))
						child = child->lefttree;

					if (IsA(child, Sort))
					{
						Sort *sort = (Sort *)child;
						if (sort->numCols < wagg->ordNumCols)
						{
							FALLBACK_LOG("the number of sort keys: %d in the child Sort "
										"is less than the number of \"order by\" keys: %d in WindowAgg.",
										sort->numCols, wagg->ordNumCols);
							return false;
						}
					}
					else if (IsA(child, Motion))
					{
						Motion *motion = (Motion *)child;
						if (!motion->sendSorted)
						{
							FALLBACK_LOG("the motion node is NOT sorted.");
							return false;
						}
					}
					else if (IsA(child, Agg))
					{
						Agg *agg = castNode(Agg, child);

						if (agg->aggstrategy != AGG_SORTED)
						{
							FALLBACK_LOG("the agg node is NOT sorted.");
							return false;
						}

						Plan *child_of_agg = child->lefttree;
						if (!IsA(child_of_agg, Motion) && !IsA(child_of_agg, Sort))
						{
							FALLBACK_LOG("the child node of GroupAgg is NOT Sort or sorted GatherMotion.");
							return false;
						}
					}
					else
					{
						FALLBACK_LOG("the child node of the WindowAgg is NOT Sort or sorted GatherMotion.");
						return false;
					}
				}

				for (int i = 0; i < wagg->partNumCols; i++)
				{
					if (wagg->partOperators[i] == ARRAY_EQ_OP)
					{
						FALLBACK_LOG("ARRAY_EQ_OP in window agg.");
						return false;
					}
				}

				break;
			}
		case T_WindowHashAgg:
			{
				WindowHashAgg *wagg = castNode(WindowHashAgg, plan);

				int frameOptions = wagg->frameOptions;
				if (wagg->startOffset || wagg->endOffset ||
				    (frameOptions != FRAMEOPTION_DEFAULTS &&
				     frameOptions != (FRAMEOPTION_DEFAULTS | FRAMEOPTION_NONDEFAULT) &&
				     frameOptions != (FRAMEOPTION_DEFAULTS | FRAMEOPTION_NONDEFAULT |
				                      FRAMEOPTION_BETWEEN) &&
				     frameOptions != (FRAMEOPTION_NONDEFAULT | FRAMEOPTION_ROWS |
				                      FRAMEOPTION_START_UNBOUNDED_PRECEDING |
				                      FRAMEOPTION_END_CURRENT_ROW) &&
				     frameOptions != (FRAMEOPTION_NONDEFAULT | FRAMEOPTION_ROWS |
				                      FRAMEOPTION_BETWEEN |
				                      FRAMEOPTION_START_UNBOUNDED_PRECEDING |
				                      FRAMEOPTION_END_CURRENT_ROW)
				    )
				   )
				{
					FALLBACK_LOG("frameOption: %d with "
					             "\"frame\" clause not supported.", frameOptions);
					return false;
				}

				for (int i = 0; i < wagg->partNumCols; i++)
				{
					if (wagg->partOperators[i] == ARRAY_EQ_OP)
					{
						FALLBACK_LOG("ARRAY_EQ_OP in window agg.");
						return false;
					}
				}
			}
			break;
		case T_HashJoin:
			{
				HashJoin* hash_expr = (HashJoin *)plan;
				if (!is_expr_vectorable((Expr *)hash_expr->join.joinqual, NULL))
				{
					FALLBACK_LOG("Unsupported join qual expr.");
					return false;
				}
				if (!is_expr_vectorable((Expr *)hash_expr->hashkeys, NULL) || joinclauses_type_different((Expr *)hash_expr->hashclauses, NULL))
				{
					FALLBACK_LOG("Unsupported hash key expr.");
					return false;
				}
				if (!is_expr_vectorable((Expr *)hash_expr->hashqualclauses, NULL) || !is_expr_vectorable((Expr *)hash_expr->hashclauses, NULL) || joinclauses_type_different((Expr *)hash_expr->hashclauses, NULL))
				{
					FALLBACK_LOG("Unsupported hash clauses expr.");
					return false;
				}
			}
			break;
		case T_NestLoop:	
			{
				NestLoop *tstlp = (NestLoop *) plan;
				if (fallback_nested_loop_jointype(plan) || joinclauses_type_different((Expr *) tstlp->join.joinqual, NULL))
				{
					FALLBACK_LOG("Unsupported nested join type.");
					return false;
				}
				if (!is_expr_vectorable((Expr *)tstlp->join.joinqual, NULL))
				{
					FALLBACK_LOG("Unsupported nested join qual expr.");
					return false;
				}
			}
			break;
		case T_Motion:
			{
				Motion *motion = (Motion *)plan;

				if (!is_hash_expr_vectorable((Expr *)motion->hashExprs, NULL))
				{
					FALLBACK_LOG("hash expr support.");
					return false;
				}
			}
			break;
		case T_Limit:
			{
				Limit *node = (Limit *)plan;

				if (node->limitOption == LIMIT_OPTION_WITH_TIES)
				{
					FALLBACK_LOG("LIMIT_OPTION_WITH_TIES is not support.");
					return false;
				}
				if (node->limitCount && !IsA(node->limitCount, Const))
				{
					FALLBACK_LOG("LIMIT only support const expr");
					return false;
				}
				if (node->limitOffset && !IsA(node->limitOffset, Const))
				{
					FALLBACK_LOG("LIMIT only support const expr");
					return false;
				}
			}
			break;
		case T_SubqueryScan:
			{
				SubqueryScan *subquery = (SubqueryScan *)plan;
				if (!is_plan_vectorable(subquery->subplan, rtable))
						return false;
			}
			break;
		case T_Append:
			{
				ListCell   *lc;
				Append *append = (Append*) plan;
				foreach (lc, append->appendplans)
				{
					Plan *initNode = (Plan *)lfirst(lc);
					if (!is_plan_vectorable(initNode, rtable))
					{
						FALLBACK_LOG("Append subnode.");	
						return false;
					}
				}
			}
			break;
		case T_Sequence:
			{
				ListCell   *lc;
				Sequence *sequence = (Sequence *) plan;
				foreach (lc, sequence->subplans)
				{
					Plan *initNode = (Plan *)lfirst(lc);
					if (!is_plan_vectorable(initNode, rtable))
					{
						FALLBACK_LOG("Sequence subplan.");	
						return false;
					}
				}
			}
			break;
		case T_ForeignScan:
			{
				ForeignScan *foreignscan = (ForeignScan *) plan;

				if (!is_foreign_scan_vectorable(foreignscan))
					return false;
				if (!is_relation_vectorable((Scan *)foreignscan, rtable, true))
					return false;
			}
			break;

		/* Plans fully vectorable: */
		case T_AssertOp:
		case T_Hash:
		case T_Material:
		case T_ShareInputScan:
			break;
		/* All other plan node fallback */
		default:
			{
				FALLBACK_LOG("node is not supported, tag is : %d", nodeTag(plan));	
				return false;
			}
	}

	return is_expr_vectorable((Expr *)plan->targetlist, NULL)
			&& is_expr_vectorable((Expr *)plan->qual, NULL);
}

static bool
is_foreign_scan_vectorable(ForeignScan *foreignscan)
{
	int i;
	ForeignServer *server;
	ForeignDataWrapper *fdw;

	server = GetForeignServer(foreignscan->fs_server);
	fdw = GetForeignDataWrapper(server->fdwid);

	for (i = 0; i < sizeof(vector_foreign_data_wapper_whitelist)/ sizeof(char *); i++)
		if (strcmp(vector_foreign_data_wapper_whitelist[i], fdw->fdwname) == 0)
			return true;
	FALLBACK_LOG("unsupported foreign data wrapper: %s", fdw->fdwname);
	return false;
}

static Node *
agg_trans_mutator(Node *node, void *context)
{
	if (node == NULL)
		return NULL;

	switch (nodeTag(node))
	{
		case T_Aggref:
		{
			Aggref *aggref = (Aggref *) node;
			if (aggref->aggsplit != AGGSPLIT_INITIAL_SERIAL)
				return node;
			switch (aggref->aggfnoid)
			{
				/* F_SUM_INT8: Arrow avg_trans outputs struct<sum:int64,count:int64>
				 * but GP's sum(bigint) needs numeric precision. Until Arrow's
				 * avg_trans supports numeric128 accumulation for int64 input,
				 * keep the old path that outputs numeric directly. */
				case F_SUM_INT8:
				{
					aggref->aggtype = NUMERICOID;
					break;
				}
				case F_SUM_NUMERIC:
				case F_AVG_INT8:
				case F_AVG_NUMERIC:
				{
					/* change result bytea type to avgintbytea */
					aggref->aggtype = AvgIntByteAOid;
					break;
				}
			    case F_STDDEV_SAMP_INT4:
				{
					aggref->aggtype = STDDEVOID;
					break;
				}
			}
			return node;
		}
		default:
			break;
	}
	return expression_tree_mutator(node, agg_trans_mutator, context);
}


/*
 * sum(int2):
 *    trans: input var type is 21(INT2OID), result type(aggtype) is 20(INT8OID).
 *    final: input var type is 20(INT8OID), result type(aggtype) is 20(INT8OID).
 * 
 * avg(int2):
 *    trans: input var type is 21(INT2OID), result type(aggtype) is 1016(INT8ARRAYOID).
 *    final: input var type is 1016(INT8ARRAYOID), result type(aggtype) is 1700(NUMERICOID).
 *
 * sum(int4):
 *    trans: input var type is 23(INT4OID), result type(aggtype) is 20(INT8OID).
 *    final: input var type is 20(INT8OID), result type(aggtype) is 20(INT8OID).
 * 
 * avg(int4):
 *    trans: input var type is 23(INT4OID), result type(aggtype) is 1016(INT8ARRAYOID).
 *    final: input var type is 1016(INT8ARRAYOID), result type(aggtype) is 1700(NUMERICOID).
 * 
 * sum(int8):
 *    trans: input var type is 20(INT8OID), result type(aggtype) is 17(BYTEAOID).
 *    final: input var type is 17(BYTEAOID), result type(aggtype) is 1700(NUMERICOID).
 * 
 * avg(int8):
 *    trans: input var type is 20(INT8OID), result type(aggtype) is 17(BYTEAOID).
 *    final: input var type is 17(BYTEAOID), result type(aggtype) is 1700(NUMERICOID).
 * 
 */
static void
arrow_aggref_adapter(List *targetlist)
{
	ListCell *lc;
	foreach(lc, targetlist)
	{
		TargetEntry *tle = lfirst(lc);
		Node *node = (Node *) tle->expr;
		agg_trans_mutator(node, NULL);
	}
}

static bool
is_relation_vectorable(Scan* seqscan, List *rtable, bool isForeign)
{
	TupleDesc tupdesc;
	AttrNumber	attnum;
	bool unsupport;
	StdRdOptions **opts;
	char *compresstype;
	unsupport = false;
	Plan *plan = (Plan *) seqscan;
	int *columnmap;
	RangeTblEntry *rte;
	Relation rel;
	TupleDesc reldesc;

	rte = rt_fetch(seqscan->scanrelid, rtable);
	/* Invalid table fallback */
	if ((rte == NULL) || (rte->rtekind != RTE_RELATION))
	{
		FALLBACK_LOG("Invalid relation");
		return false;
	}

	rel = relation_open(rte->relid, AccessShareLock);
	/* Fixme : Support more table, only aocs now. */
	if (isForeign)
	{
		ForeignTable *table;
		ListCell   *lc;

		table = GetForeignTable(rte->relid);
		foreach(lc, table->options)
		{
			DefElem    *def = (DefElem *) lfirst(lc);

			if (strcmp(def->defname, "transactional") == 0 && defGetBoolean(def))
			{
				FALLBACK_LOG("foreign table relation is transactional.");
				return false;
			}
			if (strcmp(def->defname, "format") == 0 && strcmp(defGetString(def), "orc") != 0)
			{
				FALLBACK_LOG("foreign table relation is not orc.");
				return false;
			}
		}
	}
	else if (!(table_scan_flags(rel) & SCAN_SUPPORT_VECTORIZATION))
	{
		FALLBACK_LOG("relation does not support vectorization.");
		relation_close(rel, AccessShareLock);
		return false;
	}

	reldesc = RelationGetDescr(rel);
	columnmap = palloc0((reldesc->natts + 1) * sizeof(int));
	tupdesc = BuildDescForScanExtract(reldesc,
					    plan->targetlist,
					    plan->qual, columnmap);


	/*
	 * RelationGetAttributeOptions() parses attoptions using AO's reloption
	 * kind, which only recognizes AO/AOCS compression types.  For AOCS tables
	 * we need to check whether rle_type is used (unsupported by vectorization).
	 * PAX and FDW tables manage their own column encoding, so skip this check.
	 */
	if (RelationIsAoCols(rel))
	{
		opts = RelationGetAttributeOptions(rel);
		relation_close(rel, AccessShareLock);
		if (!opts)
			return false;
		for (attnum = 1; attnum <= tupdesc->natts; attnum++)
		{
			Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);
			if (!is_scan_type_vectorable(attr))
				return false;

			if(!opts[columnmap[attnum - 1]])
				continue;
			compresstype = opts[columnmap[attnum - 1]]->compresstype;

			/* FIXME: fix this bug to support rle_type */
			if (strcmp(compresstype, "rle_type") == 0)
			{
				FALLBACK_LOG("relation is rel_type compressed.");
				return false;
			}
		}
	}
	else
	{
		relation_close(rel, AccessShareLock);
		for (attnum = 1; attnum <= tupdesc->natts; attnum++)
		{
			Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);
			if (!is_scan_type_vectorable(attr))
				return false;
		}
	}

	pfree(columnmap);
	return true;
}

static bool
is_sort_collation_vectorable(Sort *sort)
{
	if (sort->collations)
	{
		Oid *sortCollations = sort->collations;
		for (int i = 0; i < sort->numCols; i++)
		{
			Oid collation = sortCollations[i];
			HeapTuple  tp;
			Form_pg_collation collform;
			const char *collcollate;
			const char *localeptr = NULL;

			if (!OidIsValid(collation))
				continue;

			tp = SearchSysCache1(COLLOID, ObjectIdGetDatum(collation));
			if (!HeapTupleIsValid(tp))
				elog(ERROR, "cache lookup failed for collation %u", collation);

			collform = (Form_pg_collation) GETSTRUCT(tp);
			collcollate = NameStr(collform->collcollate);

			if (strcasecmp(collcollate, "en_US.UTF-8") == 0
					|| strcasecmp(collcollate, "en_US.utf8") == 0
					|| strcasecmp(collcollate, "C.UTF-8") == 0
					|| strcasecmp(collcollate, "C.utf8") == 0
					|| (collform->oid == C_COLLATION_OID))
			{
				ReleaseSysCache(tp);
				continue;
			}
			if (collform->oid == DEFAULT_COLLATION_OID)
			{
				localeptr = setlocale(LC_COLLATE, NULL);
				if (!localeptr)
					elog(ERROR, "invalid LC_COLLATE setting");
				if (strcmp(localeptr, "C") == 0
						|| strcasecmp(localeptr, "C.utf8") == 0
						|| strcasecmp(localeptr, "C.UTF-8") == 0
						|| strcasecmp(localeptr, "en_US.utf8") == 0
						|| strcasecmp(localeptr, "en_US.UTF-8") == 0)
				{
					ReleaseSysCache(tp);
					continue;
				}
			}

			FALLBACK_LOG("Unsupported collation %d : %s. ",
					collform->oid, localeptr);
			ReleaseSysCache(tp);
			return false;
		}
	}
	return true;
}

static bool
fallback_nested_loop_jointype(Plan *plan) 
{
	NestLoop *node = (NestLoop *) plan;
	if (node->join.jointype == JOIN_LASJ_NOTIN)
		return true;
	return false;
}

static Node *
match_replace_mutator(Node *node, void *context)
{
	if (node == NULL)
		return NULL;
	TupleDesc tupdesc = (TupleDesc) context;
	switch (nodeTag(node))
	{
		case T_Var:
		{
			Var *var = (Var *) node;
			Form_pg_attribute attr;

			/* no need to match system columns */
			if (var->varattno < 0 || var->varattno > tupdesc->natts)
				break;
			/* Fixme: var->varattno zero represents all attributes.
			 */
			if (var->varattno == 0)
				break;

			attr = TupleDescAttr(tupdesc, var->varattno - 1);
			var->vartype = attr->atttypid;
			break;
		}
		/* Fixme: match for other expression type. */
		default:
			elog(DEBUG2, "[match replace tl] node type: %d not support", nodeTag(node));
	}
	return expression_tree_mutator(node, match_replace_mutator, context);
}

/*
 * The var of the current node is replaced with the var of the downstream node.
 */
static void
match_replace_tl(List *dst, List* src) 
{
	ListCell *lc;
	TupleDesc tupdesc;

	tupdesc = ExecTypeFromTL(src);
	foreach (lc, dst) 
	{
		TargetEntry *tle = lfirst(lc);
		Node *node = (Node *) tle->expr;
		match_replace_mutator(node, tupdesc);
	}
	FreeTupleDesc(tupdesc);
}

static inline bool
is_hash_expr_fallback(Expr* expr, void *context)
{
	return !is_hash_expr_vectorable(expr, context);
}

static bool
is_hash_expr_vectorable(Expr *expr, void *context)
{
	if (expr == NULL)
		return true;

	switch (nodeTag(expr))
	{
		/* motion expression white list*/
		case T_Var:
			{
				Var *var = (Var *) expr;

				if (!is_type_vectorable(var->vartype))
					return false;
			}
			break;
		case T_Const:
			{
				Const  *con = (Const *) expr;

				/* support types */
				if ((con->consttype == INT4OID)
						|| (con->consttype == INT8OID)
						|| (con->consttype == DATEOID)
						|| (con->consttype == TEXTOID))
					break;
				FALLBACK_LOG("Hash expression const type not support %d",
						con->consttype);
				return false;
			}
			break;
		case T_BoolExpr:
			{
				BoolExpr *bexpr = (BoolExpr *)expr;

				if (bexpr->boolop == AND_EXPR || bexpr->boolop == OR_EXPR)
					break;
				FALLBACK_LOG("Hash expression bool expr type not support %d",
					 bexpr->boolop);
				return false;
			}
			break;
		/* Expressions fully vectorable */
		case T_List:
		case T_TargetEntry:
			break;
		case T_OpExpr:
			{
				OpExpr *opexpr = (OpExpr *) expr;
				if (!is_gandiva_func_support(opexpr->opno))
				{
					FALLBACK_LOG("converting invalid opexpr to gandiva node, func id : %d",
						(int) opexpr->opno);
					return false;
				}
			}
			break;
		case T_FuncExpr:
			{
				FuncExpr *opexpr = (FuncExpr *)expr;
				if (opexpr->funcid != F_SUBSTR_TEXT_INT4_INT4 && opexpr->funcid != F_UPPER_TEXT)
				{
					FALLBACK_LOG("converting invalid funcexpr to gandiva node, func id : %d",
						(int) opexpr->funcid);
					return false;
				}
			}
			break;
		default:
			FALLBACK_LOG("Hash expression not support %s", nodeToString(expr));
			return false;
	}

	/* reaching here means current expression is vectorable, continue walker */
	return !expression_tree_walker((Node *) expr, is_hash_expr_fallback, NULL);
}

/*
 * Walk the plan tree and rewrite any HashJoin with jointype
 * JOIN_RIGHT_SEMI / JOIN_RIGHT_ANTI back to JOIN_SEMI / JOIN_ANTI. ORCA's
 * RIGHT_SEMI/ANTI xform left the plan tree's lefttree (= outer = LHS) and
 * righttree (= Hash-wrapped inner = RHS) in their natural positions, with
 * hashjoin->hashkeys = outer side keys and hash->hashkeys = inner side keys.
 * That layout is identical to a standard LEFT_SEMI / LEFT_ANTI plan, so just
 * flipping the jointype is sufficient for PG executor to run it correctly --
 * we only lose the small-build memory advantage that vec engine's RIGHT_SEMI
 * path provided.
 */
static void
demote_right_semi_anti_to_left(Plan *plan)
{
	if (plan == NULL)
		return;

	if (IsA(plan, HashJoin))
	{
		HashJoin *hj = (HashJoin *) plan;
		/* PG 14 now natively supports JOIN_RIGHT_SEMI / JOIN_RIGHT_ANTI
		 * after PG 16/18 backports.  Only RIGHT_ANTI_NOTIN (GPDB-only,
		 * no upstream equivalent) still needs demotion when the plan
		 * goes to the PG executor. */
		if (hj->join.jointype == JOIN_RIGHT_ANTI_NOTIN)
			hj->join.jointype = JOIN_LASJ_NOTIN;
	}

	demote_right_semi_anti_to_left(plan->lefttree);
	demote_right_semi_anti_to_left(plan->righttree);

	/* SubPlans (e.g. CTEs) live on plan->initPlan / plan->subPlan, but those
	 * are PG SubPlan nodes that already recurse into a Plan tree owned by
	 * PlannedStmt->subplans -- we leave traversal of those to higher-level
	 * walkers if needed. For simple queries the lefttree/righttree recursion
	 * above is sufficient. */
}

static bool
fallback_distinct_junk(Plan *plan)
{
	Agg *agg = (Agg *) plan;
	int grpkey = agg->numCols;
	List *tl = plan->targetlist;
	ListCell *l;
	bool has_aggref = false;
	bool has_junk_column = false;
	foreach (l, tl)
	{
		TargetEntry *tle = (TargetEntry *)lfirst(l);
		if (tle->resjunk)
			has_junk_column = true;
		Expr *node = tle->expr;
		switch (nodeTag(node))
		{
			case T_Aggref:
			{
				has_aggref = true;
				break;
			}
			default:
				break;
		}
	}
	return grpkey > 0 && !has_aggref && has_junk_column;
}

/*
 * true means different type
 * false means same type
 */
static bool
joinclauses_type_different(Expr *node, void *context)
{
	if (node == NULL)
		return false;

	ListCell *lc;
	Oid prev = InvalidOid;

	switch (nodeTag(node))
	{
	case T_List:
		{
			List *l = (List *)node;
			int length = list_length(l);
			foreach (lc, l)
			{
				Expr *expr = (Expr *)lfirst(lc);
				if (length == 1)
					return joinclauses_type_different(expr, context);
				else
				{
					Oid curr_oid = exprType((Node *)expr);
					if (prev != InvalidOid && prev != curr_oid)
						return true;
					prev = curr_oid;
				}
			}
			return false;
		}
		break;
	case T_OpExpr:
		{
			OpExpr *opexpr = (OpExpr *) node;
			return joinclauses_type_different((Expr *) opexpr->args, context);
		}
	default:
		break;
	}
	return expression_tree_walker((Node *) node, joinclauses_type_different, context);
}

static bool
is_attribute_nonnullable(Oid relationOid, AttrNumber attrNumber)
{
	HeapTuple	attributeTuple;
	Form_pg_attribute attribute;
	bool		result = true;

	attributeTuple = SearchSysCache2(ATTNUM,
									 ObjectIdGetDatum(relationOid),
									 Int16GetDatum(attrNumber));
	if (!HeapTupleIsValid(attributeTuple))
		return false;

	attribute = (Form_pg_attribute) GETSTRUCT(attributeTuple);

	if (attribute->attisdropped)
		result = false;

	if (!attribute->attnotnull)
		result = false;

	ReleaseSysCache(attributeTuple);

	return result;
}

static bool
traverse_is_join_key(Node *node, int varno)
{
	if (node == NULL)
		return NULL;

	switch (nodeTag(node))
	{
		case T_Var:
			{
				Var *var = (Var *) node;
				if (var->varattno == varno)
					return true;
			}
			break;
		default:
			return false;
			break;
	}

	return false;
}

static bool
loop_targetlist(List *targetList, int varattno, PreNodeContext *context)
{
	ListCell *cell = NULL;
	TargetEntry *targetEntry = NULL;
	Var *var = NULL;
	bool status = false;
	if (!targetList)
		return status;

	foreach(cell, targetList)
	{
		targetEntry = (TargetEntry *) lfirst(cell);

		if (IsA(targetEntry->expr, Var))
		{
			var = (Var *)targetEntry->expr;

			if (var->varno == INNER_VAR || var->varno == OUTER_VAR)
			{
				return status;
			}
			else if (var->varattno == varattno)
			{
				status = is_attribute_nonnullable(targetEntry->resorigtbl, varattno);

				if (status && context->hashkeys)
				{
					ListCell	*l;
					foreach(l, context->hashkeys)
					{
						status = traverse_is_join_key(lfirst(l), targetEntry->resno);
						if (status)
						{
							return status;
						}
					}
				}
				else
					return status;
			}
		}
	}

	return status;
}

static bool
check_targetlist(Node *node, int varattno, PreNodeContext *context)
{
	switch (nodeTag(node))
	{
		case T_SeqScan:
			{
				SeqScan *seqscan = (SeqScan *)node;
				return loop_targetlist(seqscan->plan.targetlist, varattno, context);
			}
			break;
		case T_Motion:
			{
				Motion *motion = (Motion *)node;
				return loop_targetlist(motion->plan.targetlist, varattno, context);
			}
			break;
		case T_HashJoin:
			{
				HashJoin *join = (HashJoin *)node;
				return loop_targetlist(join->join.plan.targetlist, varattno, context);
			}
		case T_Hash:
			{
				Hash *hash = (Hash *)node;
				context->hashkeys = hash->hashkeys;
				return loop_targetlist(hash->plan.targetlist, varattno, context);
			}
		default:
			break;
	}
	return false;
}

static bool isProcessExists(pid_t pid)
{
	char command[128] = {"\0"};
	sprintf(command, "ps -ef | grep %d | grep -v grep", pid);

	FILE* fp = popen(command, "r");
	if (fp == NULL)
		return false;

	char buffer[128]= {"\0"};
	bool exists = false;

	while (fgets(buffer, sizeof(buffer), fp) != NULL)
	{
		exists = true;
		break;
	}

	pclose(fp);

	return exists;
}

static bool processFunction(int pid, int queryid)
{
	char dirPath[100] = "/tmp/cloudberry";
	int max_muti_process = control_memory_resource < 8 ? control_memory_resource : 1;
	char lockFilePath[100] = {"\0"};
	int process_id ;
	int lockFile;
	char* line = NULL;
	size_t lineLen = 0;
	ssize_t bytesRead;
	int existing_processes = 0;
	int not_existing_processes = 0;
	long currPos = 0;
	int processBuffer[20] = {0};
	bool status = false;

	snprintf(lockFilePath, sizeof(lockFilePath), "%s/%d.lock", dirPath, queryid);

	if (access(dirPath, F_OK) != 0)
	{
	#ifdef DEBUG
	elog(INFO,"Create path %s",dirPath);
	#endif
	mkdir(dirPath, 0777);
	}

	lockFile = open(lockFilePath, O_RDWR | O_CREAT, 0666);
	if (lockFile == -1)
	{
	#ifdef DEBUG
	elog(INFO,"Open file error :%d pid:%d",lockFile,pid);
	#endif
		return status;
	}

	if (flock(lockFile, LOCK_EX | LOCK_NB) == -1)
	{
	#ifdef DEBUG
	elog(INFO,"Open file lock  error pid %d",pid);
	#endif
	return status;
	}
	#ifdef DEBUG
	elog(INFO,"max_muti_process is %d",max_muti_process);
	#endif

	while ((bytesRead = getline(&line, &lineLen, fdopen(lockFile, "r"))) != -1)
	{
		process_id = atoi(line);

		char procPath[100] = {"\0"};
		snprintf(procPath, sizeof(procPath), "/proc/%d", process_id);
		if (access(procPath, F_OK) == 0 /*|| isProcessExists(process_id)*/)
		{
			if (process_id == pid)
				status =  true;
			else
			{
				processBuffer[existing_processes] =  process_id;
				++existing_processes;
				#ifdef DEBUG
				elog(INFO,"existing_processes ++");
				#endif
			}
		}
		else
		{
			++not_existing_processes;
			#ifdef DEBUG
			elog(INFO,"Not exist process++");
			#endif
		}

		currPos = lseek(lockFile, 0, SEEK_CUR);
		free(line);
	}

	if (not_existing_processes > 0)
	{
		if(ftruncate(lockFile, 0) == -1)
			elog(INFO,"Ftruncate is error");
		lseek(lockFile, 0, SEEK_SET);
		for (int i = 0; i < existing_processes; i++)
		{
			char buffer[100] = {"\0"};
			int size = snprintf(buffer, sizeof(buffer), "%d\n",processBuffer[i]);
			#ifdef DEBUG
			elog(INFO,"Write process :%s",buffer);
			#endif
			write(lockFile, buffer, size);
		}
		#ifdef DEBUG
		elog(INFO,"All exsit process in stask is %d",existing_processes);
		#endif
	}
	else
	{
		existing_processes = 0;
		int file = open(lockFilePath, O_RDONLY);
		if (file == -1)
		{
			return -1;
		}

		char buffer[256] = {"\0"};

		while (read(file, buffer, sizeof(buffer)) > 0)
		{
			char* token = strtok(buffer, "\n");
			while (token != NULL)
			{
				int process_id = atoi(token);
				if(0 != process_id && isProcessExists(process_id))
				{
					processBuffer[existing_processes] =  process_id;
					++existing_processes;
				}
				#ifdef DEBUG
				elog(INFO,"Line %d\n", existing_processes);
				#endif
					token = strtok(NULL, "\n");
			}
		}
	}

	if (existing_processes < max_muti_process)
	{
		status = true;
		char buffer[100] = {"\0"};
		int size = snprintf(buffer, sizeof(buffer), "%d\n", pid);
		#ifdef DEBUG
		elog(INFO,"Write self process :%s",buffer);
		#endif
		write(lockFile, buffer, size);
	}
	flock(lockFile, LOCK_UN);

	close(lockFile);
	return status;
}

static bool GloablprocessFunction(int pid)
{
	int queryid = 200;
	char dirPath[100] = "/tmp/cloudberry";
	int max_muti_process = control_global_memory_resource <= 10 ? control_global_memory_resource
																: 4;
	char lockFilePath[100] = {"\0"};
	int process_id ;
	int lockFile;
	char* line = NULL;
	size_t lineLen = 0;
	ssize_t bytesRead;
	int existing_processes = 0;
	int not_existing_processes = 0;
	long currPos = 0;
	int processBuffer[20] = {0};
	bool status = false;

	snprintf(lockFilePath, sizeof(lockFilePath), "%s/%d.lock", dirPath, queryid);

	if (access(dirPath, F_OK) != 0)
	{
#ifdef DEBUG
	elog(INFO,"Create path %s",dirPath);
#endif
	mkdir(dirPath, 0777);
	}

	lockFile = open(lockFilePath, O_RDWR | O_CREAT, 0666);
	if (lockFile == -1)
	{
	#ifdef DEBUG
	elog(INFO,"Open file error :%d pid:%d",lockFile,pid);
	#endif
		return status;
	}

	if (flock(lockFile, LOCK_EX | LOCK_NB) == -1)
	{
	#ifdef DEBUG
	elog(INFO,"Open file lock  error pid %d",pid);
	#endif
	return status;
	}
	#ifdef DEBUG
	elog(INFO,"max_muti_process is %d",max_muti_process);
	#endif

	while ((bytesRead = getline(&line, &lineLen, fdopen(lockFile, "r"))) != -1)
	{
		process_id = atoi(line);

		char procPath[100] = {"\0"};
		snprintf(procPath, sizeof(procPath), "/proc/%d", process_id);
		if (access(procPath, F_OK) == 0 /*|| isProcessExists(process_id)*/)
		{
			if (process_id == pid)
				status =  true;
			else
			{
				processBuffer[existing_processes] =  process_id;
				++existing_processes;
				#ifdef DEBUG
				elog(INFO,"existing_processes ++");
				#endif
			}
		}
		else
		{
			++not_existing_processes;
			#ifdef DEBUG
			elog(INFO,"Not exist process++");
			#endif
		}

		currPos = lseek(lockFile, 0, SEEK_CUR);
		free(line);
	}

	if (not_existing_processes > 0)
	{
		if(ftruncate(lockFile, 0) == -1)
			elog(INFO,"Ftruncate is error");
		lseek(lockFile, 0, SEEK_SET);
		for (int i = 0; i < existing_processes; i++)
		{
			char buffer[100] = {"\0"};
			int size = snprintf(buffer, sizeof(buffer), "%d\n",processBuffer[i]);
			#ifdef DEBUG
			elog(INFO,"Write process :%s",buffer);
			#endif
			write(lockFile, buffer, size);
		}
		#ifdef DEBUG
		elog(INFO,"All exsit process in stask is %d",existing_processes);
		#endif
	}
	else
	{
		existing_processes = 0;
		int file = open(lockFilePath, O_RDONLY);
		if (file == -1)
		{
			return -1;
		}

		char buffer[256] = {"\0"};

		while (read(file, buffer, sizeof(buffer)) > 0)
		{
			char* token = strtok(buffer, "\n");
			while (token != NULL)
			{
				int process_id = atoi(token);
				if(0 != process_id && isProcessExists(process_id))
				{
					processBuffer[existing_processes] =  process_id;
					++existing_processes;
				}
				#ifdef DEBUG
				elog(INFO,"Line %d\n", existing_processes);
				#endif
					token = strtok(NULL, "\n");
			}
		}
	
	}
	
	if (existing_processes < max_muti_process)
	{
		status = true;
		char buffer[100] = {"\0"};
		int size = snprintf(buffer, sizeof(buffer), "%d\n", pid);
		#ifdef DEBUG
		elog(INFO,"Write self process :%s",buffer);
		#endif
		write(lockFile, buffer, size);
	}
	flock(lockFile, LOCK_UN);

	close(lockFile);
	return status;
}


static bool
traverse_is_attribute_nonnullable(Node *node, int varattno, PreNodeContext *context)
{
	bool status = false;

	if (node == NULL)
	{
		return status;
	}

	if (outerPlan(node) != NULL)
	{
		if (IsA(node, HashJoin))
			status = check_targetlist((Node *)innerPlan(node), varattno, context);
		else
			status = check_targetlist((Node *)outerPlan(node), varattno, context);

		if (status)
			return status;
	}
	else
	{
		status = check_targetlist(node, varattno, context);

		return status;
	}

	if (IsA(node, HashJoin))
		return traverse_is_attribute_nonnullable((Node *)innerPlan(node), varattno, context);
	else
		return traverse_is_attribute_nonnullable((Node *)outerPlan(node), varattno, context);
}

static bool
leftjoin_pull_antijoin(Node *node, PreNodeContext *context)
{
	bool NeedPull = false;

	if (node == NULL)
		return false;

	if (IsA(node, Result))
	{
		int find_varattnol = 0;
		List *targetList = NULL;
		ListCell *cell = NULL;
		TargetEntry *targetEntry = NULL;
		Result *result = (Result *)node;
		List *qualList = result->plan.qual;
		HashJoin *join = NULL;
		Expr * expr = NULL;
		NullTest  *nt = NULL;
		Var * var = NULL;

		if (qualList && list_length(qualList) == 1 && IsA(outerPlan(node), HashJoin))
		{
			join = (HashJoin *)outerPlan(node);
			expr = (Expr *)linitial(qualList);

			if (IsA(expr, NullTest))
			{
				nt = (NullTest *) expr;
				if (nt->nulltesttype == IS_NULL && join->join.jointype == JOIN_LEFT)
				{
					var = (Var *)nt->arg;
					find_varattnol = var->varattno;
					if (traverse_is_attribute_nonnullable(node, var->varattnosyn, context))
						NeedPull = true;
				}
			}
		}

		if (NeedPull && context->parent_node)
		{
			targetList = join->join.plan.targetlist;
			outerPlan(context->parent_node) = outerPlan(node);

			foreach(cell, targetList)
			{
				targetEntry = (TargetEntry *) lfirst(cell);

				if (targetEntry->resno == find_varattnol)
				{
					if (IsA(targetEntry->expr, Var))
					{
						var = (Var *)targetEntry->expr;

						if (var->varno == INNER_VAR)
						{
							targetList = list_delete_ptr(targetList, targetEntry);
						}
					}
				}
			}

			join->join.jointype = JOIN_ANTI;
		}
	}

	if ((IsA(node, HashJoin) || IsA(node, Motion)) && IsA(outerPlan(node), Result))
		context->parent_node = node;

	return plan_tree_walker(node, leftjoin_pull_antijoin, context, true);
}

static bool
windowhashagg_check_walker(Node *node, windowhashagg_check_context *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, WindowHashAgg))
	{
		context->has_windowhashagg = true;
		return true;
	}

	return plan_tree_walker(node, windowhashagg_check_walker, context, true);
}

static bool
windowhashagg_is_in_plan_tree(struct PlannedStmt *stmt)
{
	windowhashagg_check_context context;

	exec_init_plan_tree_base(&context.base, stmt);
	context.has_windowhashagg = false;

	windowhashagg_check_walker((Node *) stmt->planTree, &context);

	return context.has_windowhashagg;
}
