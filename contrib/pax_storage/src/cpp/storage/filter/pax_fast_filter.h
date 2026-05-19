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
    case FastFilterOp::kStrNotEmpty:    return "<>''";
    case FastFilterOp::kStrContains:    return "LIKE";
    case FastFilterOp::kStrNotContains: return "NOT LIKE";
  }
  return "?";
}

struct FastFilterDesc {
  AttrNumber attno;       // column number (1-based)
  FastFilterOp op;        // comparison operator
  int32 type_len;         // type width in bytes (1/2/4/8); string: -1
  Datum const_value;      // constant value (by-value Datum, for fixed-length)
  const char *str_const;  // search substring pointer (palloc'd, for kStrContains)
  int32 str_const_len;    // search substring length
  Datum *in_values;       // palloc'd array of non-NULL enum values (for kInValues)
  int32 in_count;         // number of enum values (0 = not an IN filter)

  bool IsStringFilter() const { return type_len == -1; }
};

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
