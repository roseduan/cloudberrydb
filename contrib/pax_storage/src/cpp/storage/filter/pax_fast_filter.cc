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
 * pax_fast_filter.cc
 *
 *   Predicate classification and construction for fast filter path.
 *   Contains helpers that analyze SQL predicates to determine whether
 *   they can be evaluated via native C++ comparison (fast filter) and
 *   build the corresponding FastFilterDesc / FastFilterContext.
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/filter/pax_fast_filter.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/filter/pax_fast_filter.h"

#include <algorithm>
#include <atomic>
#include <set>

#include "comm/cbdb_wrappers.h"
#include "comm/guc.h"
#include "storage/oper/pax_stats.h"

namespace {

bool IsFloatType(Oid typid) {
  return typid == FLOAT4OID || typid == FLOAT8OID;
}

bool IsIntegerType(Oid typid) {
  return typid == INT2OID || typid == INT4OID || typid == INT8OID;
}

// Check if vartype and consttype are compatible integer types that can
// be cross-compared (e.g. int2 col vs int4 constant).
bool AreCompatibleIntTypes(Oid vartype, Oid consttype) {
  return IsIntegerType(vartype) && IsIntegerType(consttype);
}

// Cast a Datum constant from consttype to the column's storage width (attlen).
// Returns the cast Datum. Caller must ensure types are compatible integers.
Datum CastIntConstToAttlen(Datum value, Oid consttype, int32 attlen) {
  // First widen to int64 regardless of source type.
  int64 v;
  switch (consttype) {
    case INT2OID: v = DatumGetInt16(value); break;
    case INT4OID: v = DatumGetInt32(value); break;
    case INT8OID: v = DatumGetInt64(value); break;
    default:
      CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
  }
  // Then narrow to the target attlen.
  switch (attlen) {
    case 2: return Int16GetDatum((int16)v);
    case 4: return Int32GetDatum((int32)v);
    case 8: return Int64GetDatum(v);
    default:
      CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
  }
  return value;  // unreachable; silence compiler warning
}

bool StrategyToFastFilterOp(StrategyNumber strategy,
                                    const char *op_name,
                                    pax::FastFilterOp *out) {
  switch (strategy) {
    case BTEqualStrategyNumber:
      *out = pax::FastFilterOp::kEqual; return true;
    case BTLessStrategyNumber:
      *out = pax::FastFilterOp::kLess; return true;
    case BTLessEqualStrategyNumber:
      *out = pax::FastFilterOp::kLessEqual; return true;
    case BTGreaterEqualStrategyNumber:
      *out = pax::FastFilterOp::kGreaterEqual; return true;
    case BTGreaterStrategyNumber:
      *out = pax::FastFilterOp::kGreater; return true;
    default:
      break;
  }
  if (strcmp(op_name, "<>") == 0) {
    *out = pax::FastFilterOp::kNotEqual;
    return true;
  }
  return false;
}

pax::FastFilterOp InvertFastFilterOp(pax::FastFilterOp op) {
  switch (op) {
    case pax::FastFilterOp::kEqual:        return pax::FastFilterOp::kEqual;
    case pax::FastFilterOp::kNotEqual:     return pax::FastFilterOp::kNotEqual;
    case pax::FastFilterOp::kLess:         return pax::FastFilterOp::kGreater;
    case pax::FastFilterOp::kLessEqual:    return pax::FastFilterOp::kGreaterEqual;
    case pax::FastFilterOp::kGreater:      return pax::FastFilterOp::kLess;
    case pax::FastFilterOp::kGreaterEqual: return pax::FastFilterOp::kLessEqual;
    default:
      CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
  }
  return pax::FastFilterOp::kEqual;  // unreachable; silence compiler warning
}

// Strip RelabelType wrapper to get the underlying Var or Const node.
Expr *StripRelabelType(Expr *expr) {
  while (expr && IsA(expr, RelabelType)) {
    expr = ((RelabelType *)expr)->arg;
  }
  return expr;
}

// Sanity-check a Var's varattno against the relation TupleDesc.  Returns
// false (do not fast-filter) on out-of-range, defending against malformed
// quals reaching this layer.  Asserts in debug builds.
static inline bool VarAttnoInRange(const Var *var, const TupleDesc desc) {
  Assert(var->varattno >= 1 && var->varattno <= desc->natts);
  return var->varattno >= 1 && var->varattno <= desc->natts;
}

// Try to build a string FastFilterDesc from an OpExpr.
// When out is nullptr, acts as a pure classification check (no construction).
// Supports: col <> '', col LIKE '%xxxx%', col NOT LIKE '%xxxx%'
bool TryBuildStringFastFilter(OpExpr *opexpr, const char *op_name,
                                     TupleDesc desc,
                                     pax::FastFilterDesc *out) {
  Expr *leftop = StripRelabelType((Expr *)get_leftop(opexpr));
  Expr *rightop = StripRelabelType((Expr *)get_rightop(opexpr));
  if (!leftop || !rightop) return false;

  Var *var = nullptr;
  Const *cnst = nullptr;
  bool inverted = false;

  if (IsA(leftop, Var) && IsA(rightop, Const)) {
    var = (Var *)leftop;
    cnst = (Const *)rightop;
  } else if (IsA(leftop, Const) && IsA(rightop, Var)) {
    cnst = (Const *)leftop;
    var = (Var *)rightop;
    inverted = true;
  } else {
    return false;
  }

  if (cnst->constisnull) return false;
  if (!VarAttnoInRange(var, desc)) return false;

  int32 type_len = desc->attrs[var->varattno - 1].attlen;
  if (type_len != -1) return false;

  Oid typid = var->vartype;
  if (typid != TEXTOID && typid != VARCHAROID) return false;

  // Case 1: col <> '' (or '' <> col)
  if (strcmp(op_name, "<>") == 0) {
    text *t = DatumGetTextPP(cnst->constvalue);
    if (VARSIZE_ANY_EXHDR(t) != 0) return false;

    if (!out) return true;

    out->attno = var->varattno;
    out->op = pax::FastFilterOp::kStrNotEmpty;
    out->type_len = -1;
    out->const_value = (Datum)0;
    out->str_const = nullptr;
    out->str_const_len = 0;
    out->in_values = nullptr;
    out->in_count = 0;
    return true;
  }

  // Case 2: col LIKE '%xxxx%' / col NOT LIKE '%xxxx%'
  bool is_like = (strcmp(op_name, "~~") == 0);
  bool is_not_like = (strcmp(op_name, "!~~") == 0);
  if ((is_like || is_not_like) && !inverted) {
    text *pattern = DatumGetTextPP(cnst->constvalue);
    const char *p = VARDATA_ANY(pattern);
    int plen = VARSIZE_ANY_EXHDR(pattern);

    if (plen < 3) return false;
    if (p[0] != '%' || p[plen - 1] != '%') return false;

    for (int i = 1; i < plen - 1; i++) {
      if (p[i] == '%' || p[i] == '_' || p[i] == '\\')
        return false;
    }

    if (!out) return true;

    int needle_len = plen - 2;
    char *needle = (char *)palloc(needle_len);
    memcpy(needle, p + 1, needle_len);

    out->attno = var->varattno;
    out->op = is_like ? pax::FastFilterOp::kStrContains
                      : pax::FastFilterOp::kStrNotContains;
    out->type_len = -1;
    out->const_value = (Datum)0;
    out->str_const = needle;
    out->str_const_len = needle_len;
    out->in_values = nullptr;
    out->in_count = 0;
    return true;
  }

  return false;
}

// Try to build a FastFilterDesc from a single predicate.
// When out is nullptr, acts as a pure classification check (no construction).
// Returns true on success (fills *out if non-null), false if this predicate
// cannot use fast filter path.
bool TryBuildFastFilter(Node *qual, TupleDesc desc,
                                pax::FastFilterDesc *out) {
  if (!IsA(qual, OpExpr)) return false;
  OpExpr *opexpr = (OpExpr *)qual;

  NameData op_name;
  Oid op_lefttype, op_righttype;
  if (!cbdb::PGGetOperatorNo(opexpr->opno, &op_name,
                              &op_lefttype, &op_righttype, NULL))
    return false;

  // Try string fast filter first
  if (TryBuildStringFastFilter(opexpr, NameStr(op_name), desc, out))
    return true;

  StrategyNumber strategy = OpernameToStrategy(NameStr(op_name));
  pax::FastFilterOp op;
  if (!StrategyToFastFilterOp(strategy, NameStr(op_name), &op))
    return false;

  Expr *leftop = StripRelabelType((Expr *)get_leftop(opexpr));
  Expr *rightop = StripRelabelType((Expr *)get_rightop(opexpr));
  if (!leftop || !rightop) return false;

  Var *var = nullptr;
  Const *cnst = nullptr;

  if (IsA(leftop, Var) && IsA(rightop, Const)) {
    var = (Var *)leftop;
    cnst = (Const *)rightop;
  } else if (IsA(leftop, Const) && IsA(rightop, Var)) {
    cnst = (Const *)leftop;
    var = (Var *)rightop;
    op = InvertFastFilterOp(op);
  } else {
    return false;
  }

  if (cnst->constisnull) return false;
  if (!VarAttnoInRange(var, desc)) return false;
  if (IsFloatType(var->vartype)) return false;
  bool cross_int = false;
  if (var->vartype != cnst->consttype) {
    if (!AreCompatibleIntTypes(var->vartype, cnst->consttype))
      return false;
    cross_int = true;
  }

  int32 type_len = desc->attrs[var->varattno - 1].attlen;
  if (type_len != 1 && type_len != 2 && type_len != 4 && type_len != 8)
    return false;

  if (!out) return true;

  out->attno = var->varattno;
  out->op = op;
  out->type_len = type_len;
  out->const_value = cross_int
      ? CastIntConstToAttlen(cnst->constvalue, cnst->consttype, type_len)
      : cnst->constvalue;
  out->str_const = nullptr;
  out->str_const_len = 0;
  out->in_values = nullptr;
  out->in_count = 0;

  return true;
}

// Try to build a kInValues FastFilterDesc from a ScalarArrayOpExpr (col IN (...)).
// When fast_ctx is nullptr, acts as a pure classification check (no construction).
// Returns the number of filters added to fast_ctx (0 or 1).
int TryBuildInFastFilter(Node *qual, TupleDesc desc,
                                pax::FastFilterContext *fast_ctx) {
  if (!IsA(qual, ScalarArrayOpExpr)) return 0;
  auto *saop = (ScalarArrayOpExpr *)qual;

  // Only handle OR semantics (IN), not AND (NOT IN / ALL)
  if (!saop->useOr) return 0;

  // Operator must be '='
  NameData op_name;
  if (!cbdb::PGGetOperatorNo(saop->opno, &op_name, NULL, NULL, NULL))
    return 0;
  if (strcmp(NameStr(op_name), "=") != 0) return 0;

  // Left operand: Var (column reference)
  Expr *leftop = StripRelabelType((Expr *)linitial(saop->args));
  if (!leftop || !IsA(leftop, Var)) return 0;
  auto *var = (Var *)leftop;

  // Right operand: Const (array literal)
  Expr *rightop = StripRelabelType((Expr *)lsecond(saop->args));
  if (!rightop || !IsA(rightop, Const)) return 0;
  auto *cnst = (Const *)rightop;
  if (cnst->constisnull) return 0;
  if (!VarAttnoInRange(var, desc)) return 0;

  // Column must be fixed-length integer (1/2/4/8), exclude float
  if (IsFloatType(var->vartype)) return 0;
  int32 type_len = desc->attrs[var->varattno - 1].attlen;
  if (type_len != 1 && type_len != 2 && type_len != 4 && type_len != 8)
    return 0;

  // Deconstruct the array constant
  ArrayType *arrayval = DatumGetArrayTypeP(cnst->constvalue);
  int16 elmlen;
  bool elmbyval;
  char elmalign;
  get_typlenbyvalalign(ARR_ELEMTYPE(arrayval), &elmlen, &elmbyval, &elmalign);

  Datum *elem_values;
  bool *elem_nulls;
  int num_elems;
  deconstruct_array(arrayval, ARR_ELEMTYPE(arrayval),
                    elmlen, elmbyval, elmalign,
                    &elem_values, &elem_nulls, &num_elems);
  if (num_elems <= 0) return 0;

  // Count non-NULL elements to verify at least one exists.
  int32 non_null_cnt = 0;
  for (int i = 0; i < num_elems; i++) {
    if (!elem_nulls[i]) non_null_cnt++;
  }
  if (non_null_cnt == 0) return 0;

  // Classification-only mode: all checks passed, no construction needed.
  if (!fast_ctx) return 1;

  // Check if array element type differs from column type (cross-type IN).
  Oid elem_type = ARR_ELEMTYPE(arrayval);
  bool cross_int_in = (elem_type != var->vartype) &&
                       AreCompatibleIntTypes(var->vartype, elem_type);

  // Collect non-NULL values, casting if cross-type.
  Datum *in_vals = (Datum *)palloc(sizeof(Datum) * num_elems);
  int32 in_cnt = 0;
  for (int i = 0; i < num_elems; i++) {
    if (!elem_nulls[i]) {
      in_vals[in_cnt] = cross_int_in
          ? CastIntConstToAttlen(elem_values[i], elem_type, type_len)
          : elem_values[i];
      in_cnt++;
    }
  }
  Assert(in_cnt > 0);

  pax::FastFilterDesc fd{};
  fd.attno = var->varattno;
  fd.op = pax::FastFilterOp::kInValues;
  fd.type_len = type_len;
  fd.const_value = (Datum)0;
  fd.str_const = nullptr;
  fd.str_const_len = 0;
  fd.in_values = in_vals;
  fd.in_count = in_cnt;

  fast_ctx->filters.push_back(fd);
  return 1;
}

// Check if a single qual can be converted to a fast filter.
// Pure classification — no side effects, no FastFilterDesc construction.
bool CanFastFilterSingleQual(Node *qual, TupleDesc desc) {
  if (!pax::pax_enable_fast_filter)
    return false;

  // Must reference exactly one column
  int natts = desc->natts;
  bool *proj = (bool *)palloc0(sizeof(bool) * natts);
  extractcolumns_from_node(qual, proj, natts);

  int ncols = 0;
  for (int i = 0; i < natts; i++) {
    if (proj[i]) ncols++;
  }
  pfree(proj);

  if (ncols != 1)
    return false;

  if (TryBuildFastFilter(qual, desc, nullptr))
    return true;
  if (TryBuildInFastFilter(qual, desc, nullptr) > 0)
    return true;
  return false;
}

}  // anonymous namespace

namespace pax {

bool BuildFastFilter(Relation rel, List *qual,
                     FastFilterContext *fast_ctx,
                     std::vector<AttrNumber> *fast_filter_attnos) {
  Assert(pax_enable_fast_filter);
  if (!qual || !IsA(qual, List)) return false;

  List *flat_qual = qual;
  if (list_length(flat_qual) == 1 && IsA(linitial(flat_qual), BoolExpr)) {
    auto boolexpr = (BoolExpr *)linitial(flat_qual);
    if (boolexpr->boolop != AND_EXPR) return false;
    flat_qual = boolexpr->args;
  }
  Assert(IsA(flat_qual, List));

  TupleDesc desc = RelationGetDescr(rel);
  int natts = RelationGetNumberOfAttributes(rel);
  bool *proj = (bool *)palloc(sizeof(bool) * natts);

  ListCell *lc;
  foreach (lc, flat_qual) {
    Node *subqual = (Node *)lfirst(lc);

    // Only consider single-column predicates
    memset(proj, 0, sizeof(bool) * natts);
    extractcolumns_from_node(subqual, proj, natts);
    int num_qual_atts = 0;
    for (int i = 0; i < natts; i++) {
      if (proj[i]) num_qual_atts++;
    }
    if (num_qual_atts != 1) continue;

    FastFilterDesc fast_desc{};
    if (TryBuildInFastFilter(subqual, desc, fast_ctx) > 0) {
      // IN filter added to fast_ctx
    } else if (TryBuildFastFilter(subqual, desc, &fast_desc)) {
      fast_ctx->filters.push_back(fast_desc);
    }
  }

  pfree(proj);

  // Partition filters: integer filters before string filters, and within
  // each partition group all filters of the same column together.  Without
  // the secondary attno sort, a qual like (col1 > 5 AND col2 = 1 AND col1
  // < 10) would leave the two col1 filters non-adjacent; InitColumnGroups
  // (which merges only consecutive same-attno descriptors) would then
  // create two separate ColumnGroupDesc entries for col1, causing the
  // interleaved phase to read col1 twice.
  std::stable_sort(fast_ctx->filters.begin(), fast_ctx->filters.end(),
                   [](const FastFilterDesc &a, const FastFilterDesc &b) {
                     if (a.IsStringFilter() != b.IsStringFilter())
                       return !a.IsStringFilter();
                     return a.attno < b.attno;
                   });

  // Collect unique fast filter column numbers
  if (fast_filter_attnos) {
    std::set<AttrNumber> seen;
    for (const auto &f : fast_ctx->filters) {
      if (seen.insert(f.attno).second) {
        fast_filter_attnos->push_back(f.attno);
      }
    }
  }

  return !fast_ctx->filters.empty();
}

void BuildProjectionMasks(int natts, const std::vector<bool> &projection,
                          const std::vector<AttrNumber> &all_filter_attnos,
                          const std::vector<AttrNumber> &fast_filter_attnos,
                          std::vector<bool> *fast_filter_proj,
                          std::vector<bool> *remaining_proj,
                          bool *has_remaining_columns) {
  // Convert projection to full_proj bitmap (empty -> all true)
  auto proj_len = projection.size();
  std::vector<bool> full_proj(natts, false);
  if (proj_len > 0) {
    for (size_t i = 0; i < proj_len && i < static_cast<size_t>(natts); i++)
      full_proj[i] = projection[i];
  } else {
    std::fill(full_proj.begin(), full_proj.end(), true);
  }

  // fast_filter_proj = only fast filter columns
  fast_filter_proj->assign(natts, false);
  for (auto attno : fast_filter_attnos) {
    Assert(attno > 0 && attno <= natts);
    (*fast_filter_proj)[attno - 1] = true;
  }

  // remaining_proj = full projection - all filter columns
  remaining_proj->assign(natts, false);
  *has_remaining_columns = false;
  for (int i = 0; i < natts; i++) {
    if (full_proj[i]) {
      (*remaining_proj)[i] = true;
    }
  }
  for (auto attno : all_filter_attnos) {
    Assert(attno > 0 && attno <= natts);
    (*remaining_proj)[attno - 1] = false;
  }
  for (int i = 0; i < natts; i++) {
    if ((*remaining_proj)[i]) {
      *has_remaining_columns = true;
      break;
    }
  }
}

// Process-wide monotonic id sequence used to stamp every successfully
// initialized PaxFastFilter.  Read by tl_filter_state caching in
// pax_vec_reader.cc; see FastFilterContext::id comment for ABA rationale.
static std::atomic<uint64_t> g_fast_filter_id_seq{1};

bool PaxFastFilter::Initialize(Relation rel, List *qual,
                               const std::vector<bool> &projection) {
  bool ok = false;

  CBDB_WRAP_START;
  {
    ok = BuildFastFilter(rel, qual, &fast_ctx_, &fast_filter_attnos_);
  }
  CBDB_WRAP_END;

  if (ok) {
    int natts = RelationGetNumberOfAttributes(rel);
    BuildProjectionMasks(natts, projection, fast_filter_attnos_,
                         fast_filter_attnos_, &fast_filter_proj_,
                         &remaining_proj_, &has_remaining_columns_);
    fast_ctx_.id = g_fast_filter_id_seq.fetch_add(1, std::memory_order_relaxed);
  }

  return ok;
}

}  // namespace pax

namespace paxc {

bool PaxCanFastFilter(Node *qual, TupleDesc desc) {
  return CanFastFilterSingleQual(qual, desc);
}

}  // namespace paxc

extern "C" bool PaxCanFastFilterC(Node *qual, TupleDesc desc) {
  return paxc::PaxCanFastFilter(qual, desc);
}
