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
 * pax_fast_filter.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/filter/pax_fast_filter.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "comm/cbdb_api.h"

#include <cstring>
#include <vector>

#include "comm/bitmap.h"

namespace pax {

enum class FastFilterOp : uint8_t {
  kEqual = 0,
  kLess,
  kLessEqual,
  kGreater,
  kGreaterEqual,
  kNotEqual,
  // IN expression
  kInValues,       // col IN (v1, v2, ...)
  // AND / OR of comparisons on one column, as a union of closed ranges
  kRangeSet,       // (col >= a AND col <= b) OR col = c ...
  // String operations
  kStrNotEmpty,       // col <> ''
  kStrContains,       // col LIKE '%xxxx%'
  kStrNotContains,    // col NOT LIKE '%xxxx%'
};

inline const char *FastFilterOpName(FastFilterOp op) {
  switch (op) {
    case FastFilterOp::kEqual:          return "=";
    case FastFilterOp::kLess:           return "<";
    case FastFilterOp::kLessEqual:      return "<=";
    case FastFilterOp::kGreater:        return ">";
    case FastFilterOp::kGreaterEqual:   return ">=";
    case FastFilterOp::kNotEqual:       return "<>";
    case FastFilterOp::kInValues:       return "IN";
    case FastFilterOp::kRangeSet:       return "RANGES";
    case FastFilterOp::kStrNotEmpty:    return "<>''";
    case FastFilterOp::kStrContains:    return "LIKE";
    case FastFilterOp::kStrNotContains: return "NOT LIKE";
  }
  return "?";
}

// type_len of a filter on a numeric column.  The column stores varlena (PORC)
// or 16-byte vec numerics (PORC_VEC); the filter compares numeric keys (see
// numeric_key below).
constexpr int32 kFastFilterNumericTypeLen = -2;

struct FastFilterDesc {
  AttrNumber attno;       // column number (1-based)
  FastFilterOp op;        // comparison operator
  int32 type_len;         // type width in bytes (1/2/4/8); string: -1;
                          // numeric: kFastFilterNumericTypeLen
  Datum const_value;      // constant value (by-value Datum, for fixed-length)
  const char *str_const;  // search substring pointer (palloc'd, for kStrContains)
  int32 str_const_len;    // search substring length
  Datum *in_values;       // palloc'd array of non-NULL enum values (for kInValues)
  int32 in_count;         // number of enum values (0 = not an IN filter)
  // kRangeSet: palloc'd [lo0, hi0, lo1, hi1, ...], closed ranges, sorted and
  // disjoint.  Bounds are int64 values on integer columns and numeric keys on
  // numeric columns.
  const int64 *ranges;
  int32 range_count;      // number of ranges (not of array elements)
  int32 numeric_scale;    // numeric columns: scale of the column's typmod
  // Whether the column's missing value passes this filter.  Files written
  // before ALTER TABLE ADD COLUMN do not have the column, and every row of
  // them reads as that value (the default, or NULL).
  bool missing_passes;

  bool IsStringFilter() const { return type_len == -1; }
  bool IsNumericFilter() const { return type_len == kFastFilterNumericTypeLen; }
};

// Keys that order numeric values the way numeric_cmp() does, for a column of
// scale `scale` whose values have at most 18 digits.
//
// A finite value v maps to sign(v) * (2 * M + R), where M is the integer part
// of |v| * 10^scale and R is 1 when |v| * 10^scale has a fractional part.  A
// value exact at `scale` thus maps to an even key, and a value strictly
// between two such values to the odd key between theirs, so a value compares
// to a constant exact at `scale` exactly as its key compares to the
// constant's key.  (Two values inside the same gap share a key; filters only
// compare against constants, which are always exact.)  Column values carry
// at most `scale` fractional digits (the typmod enforces it), so R is 0 in
// practice; it keeps the comparison exact even if one does not.
//
// Special values sort as in numeric_cmp(): NaN above everything, then
// +Infinity, then the finite values, and -Infinity below them all.  Finite
// values too large for a key saturate to kPosHuge / kNegHuge.  A constant is
// only accepted while |key| <= kMaxConst, so it never equals one of these.
namespace numeric_key {
constexpr int64 kNaN = INT64_MAX;
constexpr int64 kPosInf = INT64_MAX - 1;
constexpr int64 kPosHuge = INT64_MAX - 2;
constexpr int64 kNegHuge = INT64_MIN + 1;
constexpr int64 kNegInf = INT64_MIN;
// 2 * 10^18 >= the key of any finite value of at most 18 digits.
constexpr int64 kMaxConst = 2000000000000000000LL;
}  // namespace numeric_key

// Numeric key (see above) of a PostgreSQL numeric datum: `data` points at the
// bytes after the varlena header and `len` is their count.  `data` needs no
// alignment.  Allocation-free, so it is safe on scan threads.
int64 NumericKeyFromPgNumeric(const char *data, size_t len, int scale);

// Numeric key of a 16-byte vec short numeric (PORC_VEC storage).
int64 NumericKeyFromVecNumeric(int64 low, int64 high, int scale);

// True if `key` lies in one of the `range_count` sorted closed ranges.
static inline bool KeyInRanges(int64 key, const int64 *ranges,
                               int32 range_count) {
  for (int32 r = 0; r < range_count; r++) {
    if (key < ranges[2 * r]) return false;
    if (key <= ranges[2 * r + 1]) return true;
  }
  return false;
}

struct FastFilterContext {
  // Predicates that can use fast native comparison
  std::vector<FastFilterDesc> filters;

  // Predicates that cannot use fast path, falling back to ExecQual.
  // These point into ExecutionFilterContext::estates/attnos arrays.
  ExprState **remaining_estates = nullptr;
  AttrNumber *remaining_attnos = nullptr;
  int nremaining = 0;

  // Unique, process-monotonic identity assigned at PaxFastFilter::Initialize.
  // Thread-local fast-path caches (e.g. tl_filter_state in pax_vec_reader.cc)
  // key off this id rather than the FastFilterContext address to avoid
  // ABA: when a PaxFastFilter is destroyed and a new one is heap-allocated
  // at the same address with different quals, the address would alias the
  // stale cache entry; the id will not.  0 means "unassigned".
  uint64_t id = 0;
};

template <typename T>
static inline bool CompareValue(T value, FastFilterOp op, T constant) {
  switch (op) {
    case FastFilterOp::kEqual:
      return value == constant;
    case FastFilterOp::kNotEqual:
      return value != constant;
    case FastFilterOp::kLess:
      return value < constant;
    case FastFilterOp::kLessEqual:
      return value <= constant;
    case FastFilterOp::kGreater:
      return value > constant;
    case FastFilterOp::kGreaterEqual:
      return value >= constant;
    default:
      break;
  }
  return false;
}

// Build fast filter descriptors from a qual list.
// Fills fast_ctx with filters and fast_filter_attnos with unique column numbers.
bool BuildFastFilter(Relation rel, List *qual,
                     FastFilterContext *fast_ctx,
                     std::vector<AttrNumber> *fast_filter_attnos);

// Build two-phase projection masks from projection and filter column info.
// fast_filter_proj: only fast filter columns (for Phase 1 read).
// remaining_proj: output columns minus all filter columns (for Phase 2 read).
void BuildProjectionMasks(int natts, const std::vector<bool> &projection,
                          const std::vector<AttrNumber> &all_filter_attnos,
                          const std::vector<AttrNumber> &fast_filter_attnos,
                          std::vector<bool> *fast_filter_proj,
                          std::vector<bool> *remaining_proj,
                          bool *has_remaining_columns);

class PaxFastFilter final {
 public:
  PaxFastFilter() = default;

  bool Initialize(Relation rel, List *qual,
                  const std::vector<bool> &projection);

  inline bool HasFastFilter() const {
    return !fast_ctx_.filters.empty();
  }

  inline FastFilterContext *GetFastFilterContext() {
    return &fast_ctx_;
  }

  inline const FastFilterContext *GetFastFilterContext() const {
    return &fast_ctx_;
  }

  // Projection containing only fast filter columns (for Phase 1 read).
  inline const std::vector<bool> &GetFastFilterProjection() const {
    return fast_filter_proj_;
  }

  inline const std::vector<bool> &GetRemainingProjection() const {
    return remaining_proj_;
  }

  inline bool HasRemainingColumns() const {
    return has_remaining_columns_;
  }

 private:
  FastFilterContext fast_ctx_;
  std::vector<AttrNumber> fast_filter_attnos_;

  // Two-phase projections (vec path only)
  std::vector<bool> remaining_proj_;   // pure output columns
  std::vector<bool> fast_filter_proj_; // fast filter columns only
  bool has_remaining_columns_ = false;
};

}  // namespace pax

namespace paxc {

// Determine if a single qual can be converted to a fast filter.
// Pure classification — no side effects.
bool PaxCanFastFilter(Node *qual, TupleDesc desc);

}  // namespace paxc

// C-linkage wrapper for PaxCanFastFilter, callable from C code.
#ifdef __cplusplus
extern "C" {
#endif
bool PaxCanFastFilterC(Node *qual, TupleDesc desc);
#ifdef __cplusplus
}
#endif
