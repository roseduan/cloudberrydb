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
#include <utility>
#include <vector>

#include "comm/cbdb_wrappers.h"
#include "comm/guc.h"
#include "comm/vec_numeric.h"
#include "storage/oper/pax_stats.h"

extern "C" {
#include "utils/fmgroids.h"
}

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

// ---------------------------------------------------------------------------
// Range sets: an AND / OR tree of comparisons that all test the same column
// against constants, e.g. q13's
//   (ss_sales_price >= 100.00 AND ss_sales_price <= 150.00) OR
//   (ss_sales_price >= 50.00 AND ss_sales_price <= 100.00) OR ...
// For a non-NULL value every comparison is a set of values, AND and OR are
// intersection and union, so the tree is the union of a few closed ranges.
// A NULL value makes every comparison NULL and so the whole tree NULL, which
// rejects the row exactly like the evaluation of the scan qual would.
// ---------------------------------------------------------------------------

using Range = std::pair<int64, int64>;  // closed [first, second]
using RangeVec = std::vector<Range>;

// More ranges than this are left to the executor.
constexpr size_t kMaxRanges = 16;

// The column a range set is built for and the key domain of its values.
struct RangeColumn {
  AttrNumber attno = InvalidAttrNumber;
  bool is_numeric = false;
  int scale = 0;     // numeric columns
  int32 attlen = 0;  // integer columns
};

RangeVec IntersectRanges(const RangeVec &a, const RangeVec &b) {
  RangeVec out;
  size_t i = 0, j = 0;
  while (i < a.size() && j < b.size()) {
    int64 lo = std::max(a[i].first, b[j].first);
    int64 hi = std::min(a[i].second, b[j].second);
    if (lo <= hi) out.emplace_back(lo, hi);
    if (a[i].second < b[j].second)
      i++;
    else
      j++;
  }
  return out;
}

RangeVec UnionRanges(const RangeVec &a, const RangeVec &b) {
  RangeVec all(a);
  all.insert(all.end(), b.begin(), b.end());
  std::sort(all.begin(), all.end());
  RangeVec out;
  for (const auto &r : all) {
    // Merge overlapping and adjacent ranges; the second test avoids
    // overflowing back.second + 1.
    if (!out.empty() && (r.first <= out.back().second ||
                         r.first - 1 == out.back().second)) {
      out.back().second = std::max(out.back().second, r.second);
    } else {
      out.push_back(r);
    }
  }
  return out;
}

// Ranges of the values that compare `strategy` (or <>) to `c` within
// [min_key, max_key], for a domain where c - 1 and c + 1 are the neighbours
// of c.  The caller guarantees min_key < c < max_key.
RangeVec ComparisonRanges(pax::FastFilterOp op, int64 c, int64 min_key,
                          int64 max_key) {
  switch (op) {
    case pax::FastFilterOp::kEqual:
      return {{c, c}};
    case pax::FastFilterOp::kNotEqual:
      return {{min_key, c - 1}, {c + 1, max_key}};
    case pax::FastFilterOp::kLess:
      return {{min_key, c - 1}};
    case pax::FastFilterOp::kLessEqual:
      return {{min_key, c}};
    case pax::FastFilterOp::kGreater:
      return {{c + 1, max_key}};
    case pax::FastFilterOp::kGreaterEqual:
      return {{c, max_key}};
    default:
      CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
  }
  return {};  // unreachable
}

// Comparison operator of a numeric = / <> / < / <= / > / >= OpExpr, by its
// implementing function, so a user-defined operator of the same name does
// not pass for one.
bool NumericComparisonOp(Oid opfuncid, pax::FastFilterOp *op) {
  switch (opfuncid) {
    case F_NUMERIC_EQ: *op = pax::FastFilterOp::kEqual; return true;
    case F_NUMERIC_NE: *op = pax::FastFilterOp::kNotEqual; return true;
    case F_NUMERIC_LT: *op = pax::FastFilterOp::kLess; return true;
    case F_NUMERIC_LE: *op = pax::FastFilterOp::kLessEqual; return true;
    case F_NUMERIC_GT: *op = pax::FastFilterOp::kGreater; return true;
    case F_NUMERIC_GE: *op = pax::FastFilterOp::kGreaterEqual; return true;
    default: return false;
  }
}

// Comparison operator of an integer OpExpr (int2/int4/int8, cross-type
// included), recognised the same way TryBuildFastFilter recognises it.
bool IntegerComparisonOp(Oid opno, pax::FastFilterOp *op) {
  NameData op_name;
  Oid op_lefttype, op_righttype;
  if (!cbdb::PGGetOperatorNo(opno, &op_name, &op_lefttype, &op_righttype,
                             NULL))
    return false;
  if (!AreCompatibleIntTypes(op_lefttype, op_righttype)) return false;
  return StrategyToFastFilterOp(OpernameToStrategy(NameStr(op_name)),
                                NameStr(op_name), op);
}

// Scale of a numeric typmod with 0 <= scale <= precision <= 18, i.e. a column
// whose values all have numeric keys; -1 otherwise.
int NumericColumnScale(int32 typmod) {
  if (typmod < (int32)VARHDRSZ) return -1;
  int32 tm = typmod - VARHDRSZ;
  int precision = (tm >> 16) & 0xffff;
  int scale = ((tm & 0x7ff) ^ 1024) - 1024;
  if (precision < 1 || precision > 18 || scale < 0 || scale > precision)
    return -1;
  return scale;
}

// Ranges of one leaf comparison `col op const` on `col`.
bool ComparisonToRanges(OpExpr *opexpr, TupleDesc desc, RangeColumn *col,
                        RangeVec *out) {
  if (list_length(opexpr->args) != 2) return false;
  Expr *leftop = StripRelabelType((Expr *)linitial(opexpr->args));
  Expr *rightop = StripRelabelType((Expr *)lsecond(opexpr->args));
  if (!leftop || !rightop) return false;

  Var *var;
  Const *cnst;
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
  if (var->varlevelsup != 0 || cnst->constisnull) return false;
  if (!VarAttnoInRange(var, desc)) return false;
  if (col->attno != InvalidAttrNumber && col->attno != var->varattno)
    return false;

  const FormData_pg_attribute *attr = &desc->attrs[var->varattno - 1];
  pax::FastFilterOp op;
  int64 c;
  RangeColumn this_col;
  this_col.attno = var->varattno;

  if (var->vartype == NUMERICOID) {
    if (attr->atttypid != NUMERICOID || cnst->consttype != NUMERICOID)
      return false;
    if (!NumericComparisonOp(opexpr->opfuncid, &op)) return false;
    int scale = NumericColumnScale(attr->atttypmod);
    if (scale < 0) return false;
    Numeric num = DatumGetNumeric(cnst->constvalue);
    c = pax::NumericKeyFromPgNumeric(VARDATA(num), VARSIZE(num) - VARHDRSZ,
                                     scale);
    // Only constants exact at the column scale (even keys), and no NaN,
    // infinities or values beyond the column precision.
    if ((c & 1) != 0 || c > pax::numeric_key::kMaxConst ||
        c < -pax::numeric_key::kMaxConst)
      return false;
    this_col.is_numeric = true;
    this_col.scale = scale;
  } else {
    if (!IsIntegerType(var->vartype) || !IsIntegerType(cnst->consttype))
      return false;
    if (!IntegerComparisonOp(opexpr->opno, &op)) return false;
    switch (cnst->consttype) {
      case INT2OID: c = DatumGetInt16(cnst->constvalue); break;
      case INT4OID: c = DatumGetInt32(cnst->constvalue); break;
      default: c = DatumGetInt64(cnst->constvalue); break;
    }
    this_col.attlen = attr->attlen;
  }

  if (col->attno == InvalidAttrNumber)
    *col = this_col;
  else if (col->is_numeric != this_col.is_numeric)
    return false;

  if (inverted && op != pax::FastFilterOp::kEqual &&
      op != pax::FastFilterOp::kNotEqual)
    op = InvertFastFilterOp(op);

  if (col->is_numeric) {
    // Numeric keys of constants stay within +-kMaxConst, so c +- 1 does not
    // reach the special-value keys at either end.
    *out = ComparisonRanges(op, c, pax::numeric_key::kNegInf,
                            pax::numeric_key::kNaN);
    return true;
  }

  // Integer column: c - 1 / c + 1 overflow at the ends of int64.
  if (c == INT64_MIN || c == INT64_MAX) {
    bool at_min = (c == INT64_MIN);
    switch (op) {
      case pax::FastFilterOp::kEqual: *out = {{c, c}}; break;
      case pax::FastFilterOp::kNotEqual:
        *out = at_min ? RangeVec{{INT64_MIN + 1, INT64_MAX}}
                      : RangeVec{{INT64_MIN, INT64_MAX - 1}};
        break;
      case pax::FastFilterOp::kLess:
        *out = at_min ? RangeVec{} : RangeVec{{INT64_MIN, INT64_MAX - 1}};
        break;
      case pax::FastFilterOp::kLessEqual:
        *out = at_min ? RangeVec{{c, c}} : RangeVec{{INT64_MIN, c}};
        break;
      case pax::FastFilterOp::kGreater:
        *out = at_min ? RangeVec{{INT64_MIN + 1, INT64_MAX}} : RangeVec{};
        break;
      case pax::FastFilterOp::kGreaterEqual:
        *out = at_min ? RangeVec{{INT64_MIN, INT64_MAX}} : RangeVec{{c, c}};
        break;
      default:
        return false;
    }
    return true;
  }
  *out = ComparisonRanges(op, c, INT64_MIN, INT64_MAX);
  return true;
}

// Ranges of an AND / OR tree of comparisons on one column.
bool QualToRanges(Node *node, TupleDesc desc, RangeColumn *col,
                  RangeVec *out) {
  if (IsA(node, OpExpr))
    return ComparisonToRanges((OpExpr *)node, desc, col, out);
  if (!IsA(node, BoolExpr)) return false;

  auto *boolexpr = (BoolExpr *)node;
  if (boolexpr->boolop != AND_EXPR && boolexpr->boolop != OR_EXPR)
    return false;
  bool first = true;
  ListCell *lc;
  foreach (lc, boolexpr->args) {
    RangeVec arg;
    if (!QualToRanges((Node *)lfirst(lc), desc, col, &arg)) return false;
    if (first)
      *out = std::move(arg);
    else if (boolexpr->boolop == AND_EXPR)
      *out = IntersectRanges(*out, arg);
    else
      *out = UnionRanges(*out, arg);
    first = false;
    // Intermediate results are capped too, so a wide OR cannot grow them.
    if (out->size() > kMaxRanges) return false;
  }
  return !first;
}

// Try to build a kRangeSet FastFilterDesc from a single-column qual that the
// other builders do not take: a comparison on a numeric column, or an AND /
// OR tree of comparisons on one numeric or integer column.
// When out is nullptr, acts as a pure classification check (no construction).
bool TryBuildRangeSetFastFilter(Node *qual, TupleDesc desc,
                                pax::FastFilterDesc *out) {
  if (!IsA(qual, OpExpr) && !IsA(qual, BoolExpr)) return false;

  RangeColumn col;
  RangeVec ranges;
  if (!QualToRanges(qual, desc, &col, &ranges)) return false;
  // A lone integer comparison is the plain integer fast filter's job.
  if (!col.is_numeric && IsA(qual, OpExpr)) return false;
  if (!col.is_numeric && col.attlen != 2 && col.attlen != 4 &&
      col.attlen != 8)
    return false;

  if (!out) return true;

  int64 *bounds =
      (int64 *)palloc(sizeof(int64) * 2 * std::max<size_t>(ranges.size(), 1));
  for (size_t r = 0; r < ranges.size(); r++) {
    bounds[2 * r] = ranges[r].first;
    bounds[2 * r + 1] = ranges[r].second;
  }

  out->attno = col.attno;
  out->op = pax::FastFilterOp::kRangeSet;
  out->type_len = col.is_numeric ? pax::kFastFilterNumericTypeLen : col.attlen;
  out->const_value = (Datum)0;
  out->str_const = nullptr;
  out->str_const_len = 0;
  out->in_values = nullptr;
  out->in_count = 0;
  out->ranges = bounds;
  out->range_count = static_cast<int32>(ranges.size());
  out->numeric_scale = col.scale;
  return true;
}

// Check if a single qual can be converted to a fast filter.
// Pure classification — no side effects, no FastFilterDesc construction.
bool CanFastFilterSingleQual(Node *qual, TupleDesc desc) {
  // The vectorized scan drops every qual accepted here from its own filter,
  // so accept only what the scan will really evaluate: the same condition
  // under which PaxDatasetInterface builds the fast filter.
  if (!gp_enable_predicate_pushdown || !pax::pax_enable_fast_filter)
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
  if (TryBuildRangeSetFastFilter(qual, desc, nullptr))
    return true;
  return false;
}

}  // anonymous namespace

namespace pax {

namespace {

// |value| * 10^scale at or above this saturates: 2 * M + 1 still fits int64
// below kPosHuge.
constexpr uint64 kNumericKeyHugeMagnitude = 4000000000000000000ULL;

// Key from the magnitude M, the fraction flag and the sign (see numeric_key).
inline int64 MakeNumericKey(unsigned __int128 magnitude, bool has_fraction,
                            bool negative) {
  if (magnitude >= kNumericKeyHugeMagnitude)
    return negative ? pax::numeric_key::kNegHuge : pax::numeric_key::kPosHuge;
  int64 key = 2 * static_cast<int64>(magnitude) + (has_fraction ? 1 : 0);
  return negative ? -key : key;
}

// 10^0 .. 10^38 as unsigned __int128.
struct Pow10U128 {
  unsigned __int128 v[39];
};
constexpr Pow10U128 kPow10U128 = [] {
  Pow10U128 t{};
  t.v[0] = 1;
  for (int k = 1; k < 39; k++) t.v[k] = t.v[k - 1] * 10;
  return t;
}();

}  // namespace

int64 NumericKeyFromPgNumeric(const char *data, size_t len, int scale) {
  Assert(scale >= 0 && scale <= 18);
  uint16 header;
  if (len < sizeof(header))
    CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError,
               fmt("truncated numeric datum [len=%lu]", len));
  memcpy(&header, data, sizeof(header));

  // Special values: NaN, +Infinity, -Infinity.
  if ((header & NUMERIC_SIGN_MASK) == NUMERIC_SPECIAL) {
    switch (header & NUMERIC_EXT_SIGN_MASK) {
      case NUMERIC_PINF: return pax::numeric_key::kPosInf;
      case NUMERIC_NINF: return pax::numeric_key::kNegInf;
      default: return pax::numeric_key::kNaN;
    }
  }

  bool negative;
  int weight;
  size_t digits_offset;
  if ((header & NUMERIC_SIGN_MASK) == NUMERIC_SHORT) {
    negative = (header & NUMERIC_SHORT_SIGN_MASK) != 0;
    // As NUMERIC_WEIGHT() does for the short format.
    weight =
        ((header & NUMERIC_SHORT_WEIGHT_SIGN_MASK) ? ~NUMERIC_SHORT_WEIGHT_MASK
                                                   : 0) |
        (header & NUMERIC_SHORT_WEIGHT_MASK);
    digits_offset = sizeof(uint16);
  } else {
    // Long format: n_sign_dscale, then n_weight.
    int16 w;
    if (len < sizeof(uint16) + sizeof(int16))
      CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError,
                 fmt("truncated numeric datum [len=%lu]", len));
    memcpy(&w, data + sizeof(uint16), sizeof(w));
    negative = (header & NUMERIC_SIGN_MASK) == NUMERIC_NEG;
    weight = w;
    digits_offset = sizeof(uint16) + sizeof(int16);
  }
  size_t ndigits = (len - digits_offset) / sizeof(NumericDigit);

  // Digit i is worth digit * NBASE^(weight - i), i.e. digit * 10^e with
  // e = DEC_DIGITS * (weight - i) + scale after scaling by 10^scale.
  unsigned __int128 magnitude = 0;
  bool has_fraction = false;
  for (size_t i = 0; i < ndigits; i++) {
    NumericDigit digit;
    memcpy(&digit, data + digits_offset + i * sizeof(NumericDigit),
           sizeof(digit));
    if (digit == 0) continue;
    int e = DEC_DIGITS * (weight - static_cast<int>(i)) + scale;
    if (e >= 0) {
      // digit < 10^4, so digit * 10^34 still fits; anything larger is far
      // beyond the saturation point anyway.
      if (e > 34)
        return MakeNumericKey(kNumericKeyHugeMagnitude, false, negative);
      magnitude += static_cast<unsigned __int128>(digit) * kPow10U128.v[e];
      if (magnitude >= kNumericKeyHugeMagnitude)
        return MakeNumericKey(magnitude, false, negative);
    } else if (e <= -DEC_DIGITS) {
      has_fraction = true;  // the whole digit is below 10^-scale
    } else {
      // Constant divisors, so no division instruction.
      int kept;
      switch (-e) {
        case 1: kept = digit / 10; has_fraction |= (digit % 10) != 0; break;
        case 2: kept = digit / 100; has_fraction |= (digit % 100) != 0; break;
        default:
          kept = digit / 1000;
          has_fraction |= (digit % 1000) != 0;
          break;
      }
      magnitude += kept;
    }
  }
  if (magnitude == 0 && !has_fraction) return 0;  // also -0
  return MakeNumericKey(magnitude, has_fraction, negative);
}

int64 NumericKeyFromVecNumeric(int64 low, int64 high, int scale) {
  Assert(scale >= 0 && scale <= 18);
  // Top two bits tag special values (see vec_numeric_special()).
  switch (static_cast<uint64>(high) >> 62) {
    case 1: return pax::numeric_key::kPosInf;
    case 2: return pax::numeric_key::kNaN;
    case 3: return pax::numeric_key::kNegInf;
    default: break;
  }
  constexpr int kDataWidth = VEC_SHORT_NUMERIC_HIGH_DATA_WIDTH;
  constexpr int kHeaderWidth = 64 - kDataWidth;
  int value_scale = static_cast<int>(
      (static_cast<uint64>(high) >> kDataWidth) &
      ((1ULL << VEC_SHORT_NUMERIC_SCALE_WIDTH) - 1));
  // Sign-extend the 120-bit two's complement value.
  int64 high_value =
      static_cast<int64>(static_cast<uint64>(high) << kHeaderWidth) >>
      kHeaderWidth;
  __int128 value = (static_cast<__int128>(high_value) << 64) |
                   static_cast<__int128>(static_cast<uint64>(low));
  bool negative = value < 0;
  unsigned __int128 magnitude =
      negative ? -static_cast<unsigned __int128>(value)
               : static_cast<unsigned __int128>(value);
  bool has_fraction = false;
  if (value_scale < scale) {
    int k = scale - value_scale;
    // magnitude < 2^119; past 10^19 it saturates whatever k is.
    if (magnitude >= kPow10U128.v[19])
      return MakeNumericKey(magnitude, false, negative);
    magnitude *= kPow10U128.v[k];
  } else if (value_scale > scale) {
    // Never for values stored under the column's typmod; exact anyway.
    unsigned __int128 div = kPow10U128.v[value_scale - scale];
    has_fraction = (magnitude % div) != 0;
    magnitude /= div;
  }
  if (magnitude == 0 && !has_fraction) return 0;
  return MakeNumericKey(magnitude, has_fraction, negative);
}

namespace {

template <typename T>
bool FilterPassesInt(T value, const FastFilterDesc &fd) {
  switch (fd.op) {
    case FastFilterOp::kInValues:
      for (int32 i = 0; i < fd.in_count; i++) {
        if (value == static_cast<T>(fd.in_values[i])) return true;
      }
      return false;
    case FastFilterOp::kRangeSet:
      return KeyInRanges(static_cast<int64>(value), fd.ranges, fd.range_count);
    default:
      return CompareValue(value, fd.op, static_cast<T>(fd.const_value));
  }
}

// Whether the non-NULL datum `value` of the filter's column passes the
// filter: the same test the scan applies to stored values.
bool FilterPassesDatum(const FastFilterDesc &fd, Datum value) {
  if (fd.IsNumericFilter()) {
    Numeric num = DatumGetNumeric(value);
    return KeyInRanges(NumericKeyFromPgNumeric(VARDATA(num),
                                               VARSIZE(num) - VARHDRSZ,
                                               fd.numeric_scale),
                       fd.ranges, fd.range_count);
  }
  if (fd.IsStringFilter()) {
    text *t = DatumGetTextPP(value);
    const char *s = VARDATA_ANY(t);
    size_t len = VARSIZE_ANY_EXHDR(t);
    switch (fd.op) {
      case FastFilterOp::kStrNotEmpty:
        return len > 0;
      case FastFilterOp::kStrContains:
      case FastFilterOp::kStrNotContains: {
        bool found = memmem(s, len, fd.str_const, fd.str_const_len) != nullptr;
        return (fd.op == FastFilterOp::kStrContains) ? found : !found;
      }
      default:
        return false;
    }
  }
  switch (fd.type_len) {
    case 1: return FilterPassesInt<int8>(DatumGetChar(value), fd);
    case 2: return FilterPassesInt<int16>(DatumGetInt16(value), fd);
    case 4: return FilterPassesInt<int32>(DatumGetInt32(value), fd);
    default: return FilterPassesInt<int64>(DatumGetInt64(value), fd);
  }
}

}  // namespace

bool BuildFastFilter(Relation rel, List *qual,
                     FastFilterContext *fast_ctx,
                     std::vector<AttrNumber> *fast_filter_attnos) {
  Assert(pax_enable_fast_filter);
  if (!qual || !IsA(qual, List)) return false;

  // Split the quals the way the vectorized scan does before it asks
  // PaxCanFastFilterC about each one (BuildVecPlan in execMain.c): a single
  // AND is unwrapped, anything else (an OR, say) is one qual of its own.
  List *flat_qual = qual;
  if (list_length(flat_qual) == 1 && IsA(linitial(flat_qual), BoolExpr)) {
    auto boolexpr = (BoolExpr *)linitial(flat_qual);
    if (boolexpr->boolop == AND_EXPR) flat_qual = boolexpr->args;
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

    // The range set goes last, as in CanFastFilterSingleQual.
    FastFilterDesc fast_desc{};
    if (TryBuildInFastFilter(subqual, desc, fast_ctx) > 0) {
      // IN filter added to fast_ctx
    } else if (TryBuildFastFilter(subqual, desc, &fast_desc)) {
      fast_ctx->filters.push_back(fast_desc);
    } else if (TryBuildRangeSetFastFilter(subqual, desc, &fast_desc)) {
      fast_ctx->filters.push_back(fast_desc);
    }
  }

  pfree(proj);

  // Files written before ALTER TABLE ADD COLUMN lack the column; the vec
  // adapter reads every row of them as the column's missing value
  // (VecAdapter::FillMissColumn), so the filter has to judge that value.
  AttrMissing *missing = desc->constr ? desc->constr->missing : nullptr;
  for (auto &fd : fast_ctx->filters) {
    const AttrMissing *am = missing ? &missing[fd.attno - 1] : nullptr;
    fd.missing_passes =
        am && am->am_present && FilterPassesDatum(fd, am->am_value);
  }

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
