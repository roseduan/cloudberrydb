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
 * pax_vec_reader.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/vec/pax_vec_reader.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/vec/pax_vec_reader.h"

#include <algorithm>
#include <string>

#include "comm/guc.h"
#include "comm/log.h"
#include "comm/pax_memory.h"
#include "storage/columns/pax_columns.h"
#include "storage/columns/pax_vec_column.h"
#include "storage/filter/pax_fast_filter.h"
#include "storage/filter/pax_filter.h"
#include "storage/filter/pax_sparse_filter.h"
#include "storage/micro_partition_stats.h"
#include "storage/oper/pax_stats.h"
#include "storage/pax_itemptr.h"
#include "storage/toast/pax_toast.h"
#include "storage/vec/pax_vec_adapter.h"

#include <arrow/scalar.h>
#include <arrow/compute/exec/topk_threshold_state.h>

#ifdef __x86_64__
#include <cpuid.h>
#include <immintrin.h>
#endif

#ifdef VEC_BUILD

namespace pax {

// ---------------------------------------------------------------------------
// Column-at-a-time fast filter infrastructure.
//
// Filters are grouped by column (attno).  Each group is evaluated over
// ALL rows of that column before moving to the next column.  Per-column
// bitmaps are AND-ed together to produce the final pass/fail bitmap.
// ---------------------------------------------------------------------------

// Filters grouped by column.  One column may have multiple filter predicates.
struct ColumnGroupDesc {
  AttrNumber attno;      // column number (1-based)
  int filter_start;      // start index into FastFilterContext::filters
  int filter_count;      // number of filters on this column
  bool is_string;        // true if type_len == -1
};

struct ThreadFilterState {
  // Identity of the FastFilterContext whose state is currently cached in this
  // thread-local.  Keyed by FastFilterContext::id (process-monotonic, stamped
  // by PaxFastFilter::Initialize) rather than by the context's address so a
  // new PaxFastFilter heap-allocated where an old one lived does not silently
  // reuse the old col_groups / eval_order.  0 means "no context cached".
  uint64_t ctx_id = 0;

  // Column groups, partitioned: integer groups first, then string groups.
  std::vector<ColumnGroupDesc> col_groups;
  int int_group_count = 0;  // [0..int_group_count) = integer, rest = string

  // Evaluation order: indices into col_groups[].
  std::vector<int> eval_order;

  // Per-column-group rejection counts (accumulated across sampling groups).
  std::vector<size_t> reject_counts;

  // Sampling control.  During the first kSamplingGroups micro-partitions,
  // all filter columns are read at once and evaluated with full scan to
  // collect per-column rejection counts for eval_order sorting.
  int group_count = 0;
  static constexpr int kSamplingGroups = 3;

  // If combined rejection rate >= this threshold, use selective scan.
  static constexpr double kSelectiveThreshold = 0.75;

  // Thread-local bitmap buffers (grow-only).
  // combined_bm: bit=1 = row passes all columns evaluated so far.
  // col_bm:      temporary bitmap for one column group's full-scan result.
  // filt_bm:     temporary bitmap for continuous-scan substring match result.
  std::vector<uint8_t> combined_bm;
  std::vector<uint8_t> col_bm;
  std::vector<uint8_t> filt_bm;
  size_t bm_capacity_rows = 0;

  void EnsureBitmapCapacity(size_t nrows) {
    if (nrows <= bm_capacity_rows) return;
    size_t bytes = (nrows + 7) / 8;
    bytes = std::max(bytes, static_cast<size_t>(1250));  // min 10000 rows
    combined_bm.resize(bytes);
    col_bm.resize(bytes);
    filt_bm.resize(bytes);
    bm_capacity_rows = bytes * 8;
  }

  // Toast pack — detoasted contiguous buffer of all toast rows in a batch,
  // plus offsets and reverse mapping (pack element index -> absolute row
  // index in the batch).  Grow-only across batches.
  std::vector<uint8_t> toast_buf;
  std::vector<int32_t> toast_offsets;      // size = toast_count + 1
  std::vector<int32_t> toast_elem_to_row;  // size = toast_count
  std::vector<int32_t> toast_elem_idx;     // size = toast_count; PORC only
  size_t toast_buf_capacity = 0;

  void EnsureToastBuf(size_t bytes) {
    if (bytes > toast_buf_capacity) {
      toast_buf.resize(bytes);
      toast_buf_capacity = bytes;
    }
  }
  void EnsureToastIndex(size_t count) {
    if (toast_offsets.size() < count + 1) toast_offsets.resize(count + 1);
    if (toast_elem_to_row.size() < count) toast_elem_to_row.resize(count);
  }
  void EnsureToastElemIdx(size_t count) {
    if (toast_elem_idx.size() < count) toast_elem_idx.resize(count);
  }
};

static thread_local ThreadFilterState tl_filter_state;

// Build column groups from the flat filter list.
static void InitColumnGroups(ThreadFilterState *tl,
                             const FastFilterContext *fast_ctx) {
  tl->col_groups.clear();
  tl->int_group_count = 0;

  int nfast = static_cast<int>(fast_ctx->filters.size());
  int i = 0;
  while (i < nfast) {
    ColumnGroupDesc cg;
    cg.attno = fast_ctx->filters[i].attno;
    cg.filter_start = i;
    cg.is_string = fast_ctx->filters[i].IsStringFilter();
    int j = i + 1;
    while (j < nfast && fast_ctx->filters[j].attno == cg.attno) j++;
    cg.filter_count = j - i;
    tl->col_groups.push_back(cg);
    if (!cg.is_string) tl->int_group_count++;
    i = j;
  }

  int ncg = static_cast<int>(tl->col_groups.size());
  tl->eval_order.resize(ncg);
  for (int k = 0; k < ncg; k++) tl->eval_order[k] = k;
  tl->reject_counts.assign(ncg, 0);
  tl->group_count = 0;
}

// Build a single-column projection vector (only one attno set to true).
static std::vector<bool> BuildSingleColumnProj(int natts, AttrNumber attno) {
  std::vector<bool> proj(natts, false);
  proj[attno - 1] = true;
  return proj;
}

// Count set bits in a raw uint8 bitmap for the first nrows bits.
static size_t PopcountBitmap(const uint8_t *bm, size_t nrows) {
  size_t nwords = (nrows + 7) / 8;
  size_t count = 0;
  size_t i = 0;
  for (; i + 8 <= nwords; i += 8) {
    uint64_t qword;
    memcpy(&qword, bm + i, 8);
    count += __builtin_popcountll(qword);
  }
  for (; i < nwords; i++) {
    count += __builtin_popcount(bm[i]);
  }
  // Mask off extra bits beyond nrows in the last byte.
  size_t extra = nrows & 7;
  if (extra > 0 && nwords > 0) {
    uint8_t mask = static_cast<uint8_t>((1U << extra) - 1);
    count -= __builtin_popcount(bm[nwords - 1] & ~mask);
  }
  return count;
}

// ---------------------------------------------------------------------------
// Column evaluation: integer columns
// ---------------------------------------------------------------------------

// Helper: extract typed Datum value.
template <typename T>
static inline T GetDatumAs(Datum d);
template <> inline int8 GetDatumAs<int8>(Datum d) {
  return static_cast<int8>(DatumGetChar(d));
}
template <> inline int16 GetDatumAs<int16>(Datum d) { return DatumGetInt16(d); }
template <> inline int32 GetDatumAs<int32>(Datum d) { return DatumGetInt32(d); }
template <> inline int64 GetDatumAs<int64>(Datum d) { return DatumGetInt64(d); }

// Evaluate all filters on a single integer value.  Returns true if row passes.
template <typename T>
static inline bool EvalIntFilters(T value, const FastFilterDesc *filters,
                                  int filter_count) {
  for (int f = 0; f < filter_count; f++) {
    const auto &fd = filters[f];
    if (fd.op == FastFilterOp::kInValues) {
      bool found = false;
      for (int32 iv = 0; iv < fd.in_count; iv++) {
        if (value == GetDatumAs<T>(fd.in_values[iv])) {
          found = true;
          break;
        }
      }
      if (!found) return false;
    } else {
      if (!CompareValue(value, fd.op, GetDatumAs<T>(fd.const_value)))
        return false;
    }
  }
  return true;
}

// Full scan: evaluate all rows, write results to col_bm (bit=1 = pass).
//
// `is_vec` selects buffer indexing: VEC format (PORC_VEC) keeps a placeholder
// slot for every row including nulls, so data[row] is correct; non-VEC (PORC)
// stores only non-null values densely, so we must index by the running
// non-null count instead.
template <typename T>
static void EvalColumnIntFullScan(const T *data, const uint8_t *null_bm,
                                  const FastFilterDesc *filters,
                                  int filter_count, bool is_vec, size_t nrows,
                                  uint8_t *col_bm) {
  size_t nwords = (nrows + 7) / 8;
  memset(col_bm, 0, nwords);

  size_t non_null_count = 0;
  for (size_t row = 0; row < nrows; row++) {
    size_t w = row >> 3;
    uint8_t bit = static_cast<uint8_t>(1U << (row & 7));

    bool row_is_null = (null_bm && !(null_bm[w] & bit));
    size_t data_idx = is_vec ? row : non_null_count;
    if (!row_is_null) non_null_count++;

    if (row_is_null) continue;

    if (EvalIntFilters(data[data_idx], filters, filter_count))
      col_bm[w] |= bit;
  }
}

// Selective scan: evaluate only rows still passing in combined_bm (in-place).
//
// See EvalColumnIntFullScan for the meaning of `is_vec`.  In the non-VEC
// branch we must walk every row (not just set bits) so that non_null_count
// stays consistent with the underlying dense buffer.
template <typename T>
static void EvalColumnIntSelective(const T *data, const uint8_t *null_bm,
                                   const FastFilterDesc *filters,
                                   int filter_count, bool is_vec, size_t nrows,
                                   uint8_t *combined_bm) {
  size_t nwords = (nrows + 7) / 8;

  if (is_vec) {
    for (size_t w = 0; w < nwords; w++) {
      uint8_t word = combined_bm[w];
      if (word == 0) continue;  // all 8 rows already rejected

      uint8_t new_word = word;
      size_t base_row = w * 8;

      for (uint8_t b = 0; b < 8 && (base_row + b) < nrows; b++) {
        uint8_t bit = static_cast<uint8_t>(1U << b);
        if (!(word & bit)) continue;  // already rejected

        size_t row = base_row + b;

        // Null → reject
        if (null_bm && !(null_bm[w] & bit)) {
          new_word &= ~bit;
          continue;
        }

        if (!EvalIntFilters(data[row], filters, filter_count))
          new_word &= ~bit;
      }

      combined_bm[w] = new_word;
    }
    return;
  }

  // Non-VEC: dense buffer, must iterate every row to maintain non_null_count.
  size_t non_null_count = 0;
  for (size_t row = 0; row < nrows; row++) {
    size_t w = row >> 3;
    uint8_t bit = static_cast<uint8_t>(1U << (row & 7));

    bool row_is_null = (null_bm && !(null_bm[w] & bit));
    size_t data_idx = non_null_count;
    if (!row_is_null) non_null_count++;

    if (!(combined_bm[w] & bit)) continue;  // already rejected

    if (row_is_null) {
      combined_bm[w] &= ~bit;
      continue;
    }

    if (!EvalIntFilters(data[data_idx], filters, filter_count))
      combined_bm[w] &= ~bit;
  }
}

// ---------------------------------------------------------------------------
// Fast substring search helpers (adapted from Apache Arrow scalar_string.cc).
// Uses a Boyer-Moore-like shift table for needles of length >= 4.
// ---------------------------------------------------------------------------

#define pax_hash2(p) \
  (((size_t)(p)[0] - ((size_t)(p)[-1] << 3)) % sizeof(pax_shift))

static thread_local uint8_t pax_shift[256];
static thread_local size_t pax_shift1;

static void pax_init_shift(const char *needle, int32_t ne_len) {
  const unsigned char *ne = (const unsigned char *)needle;
  size_t m1 = ne_len - 1;
  memset(pax_shift, 0, sizeof(pax_shift));
  for (size_t i = 1; i < m1; i++)
    pax_shift[pax_hash2(ne + i)] = i;
  pax_shift1 = m1 - pax_shift[pax_hash2(ne + m1)];
  pax_shift[pax_hash2(ne + m1)] = m1;
}

// Search for needle in haystack.  Returns pointer to first match, or NULL.
// All code paths are bounded by data_len — no null-terminator assumption.
static char *pax_is_substr(const char *src_data, int32_t data_len,
                           const char *substr, int32_t substr_len) {
  if (substr_len == 0) return (char *)src_data;
  if (data_len < substr_len) return NULL;

  const unsigned char *data = (const unsigned char *)src_data;
  const unsigned char *ne = (const unsigned char *)substr;

  // Length-1: memchr is optimal and bounded.
  if (substr_len == 1)
    return (char *)memchr(src_data, ne[0], data_len);

  // Length 2-3: bounded memchr + byte comparison (Arrow's strstr2/strstr3 use
  // c!=0 loop termination which is unsafe for non-null-terminated buffers).
  if (substr_len <= 3) {
    int32_t remaining = data_len - substr_len + 1;
    const unsigned char *p = data;
    while (remaining > 0) {
      const unsigned char *hit =
          (const unsigned char *)memchr(p, ne[0], remaining);
      if (!hit) return NULL;
      if (hit[1] == ne[1] && (substr_len == 2 || hit[2] == ne[2]))
        return (char *)hit;
      remaining -= (hit - p + 1);
      p = hit + 1;
    }
    return NULL;
  }

  // Length >= 4: shift-table algorithm (pax_init_shift must have been called).
  const unsigned char *hs =
      (const unsigned char *)memchr(src_data, ne[0], data_len);
  if (!hs) return NULL;

  size_t ne_len = substr_len;
  size_t cur_len = data + data_len - hs;
  size_t hs_len = cur_len < (ne_len | 512) ? cur_len : (ne_len | 512);
  if (hs_len < ne_len) return NULL;
  if (memcmp(hs, ne, ne_len) == 0) return (char *)hs;
  if (ne_len > 256) return NULL;

  const unsigned char *end = hs + hs_len - ne_len;
  size_t tmp, m1 = ne_len - 1, offset = 0;

  while (1) {
    if (hs > end) {
      if (end + 2048 < data + data_len - m1 - 1)
        end += 2048;
      else
        end = data + data_len - m1 - 1;
      if (hs > end) return NULL;
    }
    do {
      hs += m1;
      tmp = pax_shift[pax_hash2(hs)];
    } while (tmp == 0 && hs <= end);
    hs -= tmp;
    if (tmp < m1) continue;
    if (m1 < 15 || memcmp(hs + offset, ne + offset, 8) == 0) {
      if (memcmp(hs, ne, m1) == 0) return (char *)hs;
      offset = (offset >= 8 ? offset : m1) - 8;
    }
    hs += pax_shift1;
  }
}

// ---------------------------------------------------------------------------
// Core buffer-scan substring search.
//
// Scans the contiguous buffer [data_buf + offsets[0], data_buf + offsets[num_elems])
// for occurrences of needle, mapping each match to the element whose offset range
// contains it.  Sets corresponding bits in match_bm.
//
// Template parameter HasVarlena:
//   true  – elements include varlena headers (PORC format).  Matches are
//           validated against the actual string-data range within the varlena
//           to eliminate false positives from header/padding bytes.
//   false – elements are pure string data (PORCVEC / dict format).  A simple
//           boundary check suffices.
//
// If elem_to_idx is non-null, bit positions in match_bm use elem_to_idx[elem]
// instead of elem directly.  This supports PORC's elem→row mapping.
//
// Caller must: zero match_bm, call pax_init_shift() if needle_len >= 4.
// ---------------------------------------------------------------------------

// Validate a candidate match at `found` for element `elem`.
// If the match falls within the element's actual string data, marks the
// corresponding bit in match_bm and returns true.
//
// For HasVarlena=true: parses the varlena header to find the real string range
//   and sets *out_str_start (if non-null) for the caller's retry logic.
// For HasVarlena=false: simple boundary check against offsets.
template <bool HasVarlena>
static inline bool CheckAndMarkMatch(const char *data_buf,
                                     const int32_t *offsets, size_t elem,
                                     const char *found, int32_t needle_len,
                                     uint8_t *match_bm,
                                     const int32_t *elem_to_idx,
                                     const char **out_str_start) {
  if constexpr (HasVarlena) {
    const char *varlena_ptr = data_buf + offsets[elem];
    const char *str_start = VARDATA_ANY(varlena_ptr);
    size_t str_len = VARSIZE_ANY_EXHDR(varlena_ptr);
    if (out_str_start) *out_str_start = str_start;

    if (found >= str_start &&
        found + needle_len <= str_start + str_len) {
      size_t idx = elem_to_idx ? elem_to_idx[elem] : elem;
      match_bm[idx >> 3] |= static_cast<uint8_t>(1U << (idx & 7));
      return true;
    }
    return false;
  } else {
    (void)out_str_start;
    int32_t fo = static_cast<int32_t>(found - data_buf);
    if (fo >= offsets[elem] && fo + needle_len <= offsets[elem + 1]) {
      size_t idx = elem_to_idx ? elem_to_idx[elem] : elem;
      match_bm[idx >> 3] |= static_cast<uint8_t>(1U << (idx & 7));
      return true;
    }
    return false;
  }
}

template <bool HasVarlena>
static void BufferScanSubstr(const char *data_buf, const int32_t *offsets,
                             size_t num_elems, const char *needle,
                             int32_t needle_len, uint8_t *match_bm,
                             const int32_t *elem_to_idx) {
  const char *end = data_buf + offsets[num_elems];
  const char *current = data_buf + offsets[0];
  size_t elem = 0;

  while (elem < num_elems && current < end) {
    char *found = pax_is_substr(current, static_cast<int32_t>(end - current),
                                needle, needle_len);
    if (!found) break;

    int32_t found_off = static_cast<int32_t>(found - data_buf);

    // Advance past elements that end before the match.
    while (elem < num_elems && offsets[elem + 1] <= found_off) elem++;
    if (elem >= num_elems) break;

    const char *str_start = nullptr;
    bool matched = CheckAndMarkMatch<HasVarlena>(
        data_buf, offsets, elem, found, needle_len, match_bm, elem_to_idx,
        &str_start);

    if constexpr (HasVarlena) {
      if (matched) {
        // Case 1: real match — advance to next element.
        current = data_buf + offsets[++elem];
      } else if (found < str_start) {
        // Case 2: false positive in varlena header — retry from string data.
        current = str_start;
      } else {
        // Case 3: match extends past string data — skip to next element.
        current = data_buf + offsets[++elem];
      }
    } else {
      current = data_buf + offsets[++elem];
    }
  }
}

// ---------------------------------------------------------------------------
// Column evaluation: string columns
// ---------------------------------------------------------------------------

// Evaluate all string filters on one row.  Returns true if row passes.
static inline bool EvalStrFilters(const char *str_data, size_t str_len,
                                  const FastFilterDesc *filters,
                                  int filter_count) {
  for (int f = 0; f < filter_count; f++) {
    const auto &fd = filters[f];
    bool ok;
    switch (fd.op) {
      case FastFilterOp::kStrNotEmpty:
        ok = str_len > 0;
        break;
      case FastFilterOp::kStrContains:
      case FastFilterOp::kStrNotContains: {
        size_t needle_len = static_cast<size_t>(fd.str_const_len);
        bool found = false;
        if (str_len >= needle_len) {
          size_t limit = str_len - needle_len;
          for (size_t i = 0; i <= limit; i++) {
            if (memcmp(str_data + i, fd.str_const, needle_len) == 0) {
              found = true;
              break;
            }
          }
        }
        ok = (fd.op == FastFilterOp::kStrContains) ? found : !found;
        break;
      }
      default:
        ok = false;
        break;
    }
    if (!ok) return false;
  }
  return true;
}

// ---------------------------------------------------------------------------
// Toast handling for string fast filters.
//
// In the absence of fast filter, the existing PAX/Arrow path detoasts every
// varlena element before evaluating predicates (pax_porc_*_adpater.cc).  Fast
// filter operates directly on the column buffer to skip that cost; but the
// raw buffer of a toast row contains only the toast pointer header, which is
// not the real string.  Comparing the needle against those header bytes
// produces both false positives and false negatives.
//
// Strategy:
// (1) For per-row evaluation paths (selective scan, >256-byte needle fallback),
//     detoast each toast row inline into tl->toast_buf and run EvalStrFilters
//     on the detoasted bytes.
// (2) For buffer-scan paths (FullScan / AVX-512), pack ALL toast rows in the
//     batch into a single contiguous detoasted buffer (tl->toast_buf) once,
//     then for each filter:
//       a. Run BufferScanSubstr* over the raw column buffer (Pass 1) — this
//          may mistag toast rows since it scans header bytes.
//       b. Clear all toast bits in filt_bm, then run BufferScanSubstr* over
//          the toast pack with elem_to_idx = toast_elem_to_row (Pass 2).  The
//          Pass-2 bits override Pass-1 for toast rows.
// ---------------------------------------------------------------------------

// Detoast a single toast row into tl->toast_buf and evaluate filters on the
// detoasted bytes.  Returns true if the row passes all filters.
// Caller must have already checked column->IsToast(buf_pos) == true.
static bool DetoastAndEvalRow(ThreadFilterState *tl, PaxColumn *column,
                              size_t buf_pos, const FastFilterDesc *filters,
                              int filter_count) {
  auto [ptr, _len] = column->GetBuffer(buf_pos);
  if (!ptr) return false;
  size_t raw_size = pax_toast_raw_size(PointerGetDatum(ptr));
  if (raw_size == 0) {
    // Inconsistent IsToast flag — treat as non-matching to be safe.
    return EvalStrFilters(ptr, 0, filters, filter_count);
  }
  tl->EnsureToastBuf(raw_size);
  auto et = column->GetExternalToastDataBuffer();
  size_t decoded = pax_detoast_raw(
      PointerGetDatum(ptr),
      reinterpret_cast<char *>(tl->toast_buf.data()), raw_size,
      et ? et->Start() : nullptr, et ? et->Used() : 0);
  return EvalStrFilters(reinterpret_cast<const char *>(tl->toast_buf.data()),
                        decoded, filters, filter_count);
}

// Build a packed detoasted buffer for all toast rows in a PORC_VEC column.
// In PORC_VEC the buffer element index equals the row index.  Fills
// tl->toast_offsets[0..count], tl->toast_elem_to_row[0..count), and
// tl->toast_buf[0..total].  Returns toast_count (0 if no toast rows).
static size_t BuildToastPackPORCVEC(ThreadFilterState *tl, PaxColumn *column,
                                    const char *data_buf,
                                    const int32_t *offsets, size_t nrows) {
  if (column->ToastCounts() == 0) return 0;

  size_t toast_count = 0;
  for (size_t row = 0; row < nrows; row++) {
    if (column->IsToast(row)) toast_count++;
  }
  if (toast_count == 0) return 0;

  tl->EnsureToastIndex(toast_count);

  // Pass A: collect per-row sizes and total length.
  // toast_offsets[] is int32_t (matching PORC offset format).  Guard the
  // running sum after each accumulation; the alternative — postponing the
  // check to the sentinel offset — would let an earlier
  // toast_offsets[k] = (int32_t)total truncate silently.
  size_t total = 0, k = 0;
  for (size_t row = 0; row < nrows; row++) {
    if (!column->IsToast(row)) continue;
    tl->toast_elem_to_row[k] = static_cast<int32_t>(row);
    tl->toast_offsets[k] = static_cast<int32_t>(total);
    size_t sz = pax_toast_raw_size(
        PointerGetDatum(data_buf + offsets[row]));
    Assert(sz > 0);
    total += sz;
    CBDB_CHECK(total <= static_cast<size_t>(INT32_MAX),
               cbdb::CException::ExType::kExTypeOutOfRange,
               fmt("BuildToastPackPORCVEC: detoasted toast pack exceeds "
                   "INT32_MAX [total=%lu, nrows=%lu]",
                   total, nrows));
    k++;
  }
  tl->toast_offsets[toast_count] = static_cast<int32_t>(total);
  tl->EnsureToastBuf(total);

  // Pass B: detoast into the pack.
  auto et = column->GetExternalToastDataBuffer();
  char *out = reinterpret_cast<char *>(tl->toast_buf.data());
  for (size_t i = 0; i < toast_count; i++) {
    int32_t row = tl->toast_elem_to_row[i];
    size_t cap = static_cast<size_t>(tl->toast_offsets[i + 1] -
                                     tl->toast_offsets[i]);
    pax_detoast_raw(PointerGetDatum(data_buf + offsets[row]),
                    out + tl->toast_offsets[i], cap,
                    et ? et->Start() : nullptr, et ? et->Used() : 0);
  }
  return toast_count;
}

// Build a packed detoasted buffer for all toast rows in a PORC column.
// In PORC nulls are excluded from the data buffer; toast indexes are by
// elem (non-null position), but filt_bm uses absolute row index, so we
// keep two mappings: toast_elem_idx (-> elem) and toast_elem_to_row (-> row).
// elem_to_row maps non-null elem -> absolute row, supplied by caller.
static size_t BuildToastPackPORC(ThreadFilterState *tl, PaxColumn *column,
                                 const char *data_buf,
                                 const int32_t *offsets,
                                 const int32_t *elem_to_row,
                                 size_t non_null_count) {
  if (column->ToastCounts() == 0) return 0;

  size_t toast_count = 0;
  for (size_t ei = 0; ei < non_null_count; ei++) {
    if (column->IsToast(ei)) toast_count++;
  }
  if (toast_count == 0) return 0;

  tl->EnsureToastIndex(toast_count);
  tl->EnsureToastElemIdx(toast_count);

  // See BuildToastPackPORCVEC for the int32_t overflow rationale.
  size_t total = 0, k = 0;
  for (size_t ei = 0; ei < non_null_count; ei++) {
    if (!column->IsToast(ei)) continue;
    tl->toast_elem_idx[k] = static_cast<int32_t>(ei);
    tl->toast_elem_to_row[k] = elem_to_row[ei];
    tl->toast_offsets[k] = static_cast<int32_t>(total);
    size_t sz = pax_toast_raw_size(PointerGetDatum(data_buf + offsets[ei]));
    Assert(sz > 0);
    total += sz;
    CBDB_CHECK(total <= static_cast<size_t>(INT32_MAX),
               cbdb::CException::ExType::kExTypeOutOfRange,
               fmt("BuildToastPackPORC: detoasted toast pack exceeds "
                   "INT32_MAX [total=%lu, non_null_count=%lu]",
                   total, non_null_count));
    k++;
  }
  tl->toast_offsets[toast_count] = static_cast<int32_t>(total);
  tl->EnsureToastBuf(total);

  auto et = column->GetExternalToastDataBuffer();
  char *out = reinterpret_cast<char *>(tl->toast_buf.data());
  for (size_t i = 0; i < toast_count; i++) {
    int32_t ei = tl->toast_elem_idx[i];
    size_t cap = static_cast<size_t>(tl->toast_offsets[i + 1] -
                                     tl->toast_offsets[i]);
    pax_detoast_raw(PointerGetDatum(data_buf + offsets[ei]),
                    out + tl->toast_offsets[i], cap,
                    et ? et->Start() : nullptr, et ? et->Used() : 0);
  }
  return toast_count;
}

// Pass-2 substring scan over the toast pack (scalar / shift-table version).
// Clears every toast row's bit in filt_bm, then scans tl->toast_buf for
// matches using BufferScanSubstr<false> (pack is raw bytes, no varlena
// header) and writes matches back into filt_bm via toast_elem_to_row.
static void MergeToastPackScalar(ThreadFilterState *tl, size_t toast_count,
                                 const char *needle, int32_t needle_len,
                                 uint8_t *filt_bm) {
  if (toast_count == 0) return;
  for (size_t i = 0; i < toast_count; i++) {
    int32_t row = tl->toast_elem_to_row[i];
    filt_bm[row >> 3] &= ~static_cast<uint8_t>(1U << (row & 7));
  }
  if (needle_len >= 4) pax_init_shift(needle, needle_len);
  BufferScanSubstr<false>(
      reinterpret_cast<const char *>(tl->toast_buf.data()),
      tl->toast_offsets.data(), toast_count, needle, needle_len, filt_bm,
      tl->toast_elem_to_row.data());
}

// Full scan for string column (per-element memcmp).  Writes col_bm (bit=1 = pass).
static void EvalColumnStrFullScanMemcmp(PaxColumn *column,
                                        const FastFilterDesc *filters,
                                        int filter_count, size_t nrows,
                                        bool is_vec, uint8_t *col_bm) {
  size_t nwords = (nrows + 7) / 8;
  memset(col_bm, 0, nwords);

  const uint8_t *null_bm = nullptr;
  if (column->HasNull()) {
    auto bm = column->GetBitmap().get();
    if (bm) null_bm = bm->Raw().bitmap;
  }

  ThreadFilterState *tl = &tl_filter_state;
  size_t non_null_count = 0;

  for (size_t row = 0; row < nrows; row++) {
    size_t w = row >> 3;
    uint8_t bit = static_cast<uint8_t>(1U << (row & 7));

    bool row_is_null = (null_bm && !(null_bm[w] & bit));

    size_t buf_pos;
    if (is_vec) {
      buf_pos = row;
    } else {
      buf_pos = non_null_count;
      if (!row_is_null) non_null_count++;
    }

    if (row_is_null) continue;  // null → fail, bit stays 0

    // TOAST: detoast inline and run real comparison.
    if (column->IsToast(buf_pos)) {
      if (DetoastAndEvalRow(tl, column, buf_pos, filters, filter_count))
        col_bm[w] |= bit;
      continue;
    }

    auto [ptr, len] = column->GetBuffer(buf_pos);
    if (!ptr) continue;

    const char *str_data;
    size_t str_len;
    if (is_vec) {
      str_data = ptr;
      str_len = len;
    } else {
      str_data = VARDATA_ANY(ptr);
      str_len = VARSIZE_ANY_EXHDR(ptr);
    }

    if (EvalStrFilters(str_data, str_len, filters, filter_count))
      col_bm[w] |= bit;
  }
}

// ---------------------------------------------------------------------------
// Continuous-buffer substring scan for PORC_VEC format.
//
// PORC_VEC stores pure string data without varlena headers ("vec format will
// remove the val header"), so the data buffer + int32 offsets array matches
// Arrow StringArray layout exactly.  We scan the entire contiguous buffer
// with pax_is_substr() and only map matches back to row indices via offsets.
// ---------------------------------------------------------------------------
static void EvalColumnStrFullScan_PORCVEC(PaxColumn *column,
                                         const FastFilterDesc *filters,
                                         int filter_count, size_t nrows,
                                         uint8_t *col_bm) {
  size_t nwords = (nrows + 7) / 8;

  // Null bitmap: bit=1 means NOT null.
  const uint8_t *null_bm = nullptr;
  if (column->HasNull()) {
    auto bm = column->GetBitmap().get();
    if (bm) null_bm = bm->Raw().bitmap;
  }

  // Initialize col_bm: all non-null rows pass (bit=1).
  memset(col_bm, 0xFF, nwords);
  if (nrows & 7)
    col_bm[nwords - 1] = static_cast<uint8_t>((1U << (nrows & 7)) - 1);
  if (null_bm) {
    for (size_t w = 0; w < nwords; w++)
      col_bm[w] &= null_bm[w];
  }

  // Contiguous data buffer and offsets (nrows+1 int32 entries).
  auto [data_buf, data_size] = column->GetBuffer();
  auto *nf_col = static_cast<PaxVecNonFixedColumn *>(column);
  auto [off_buf, off_size] = nf_col->GetOffsetBuffer(false);
  const int32_t *offsets = reinterpret_cast<const int32_t *>(off_buf);

  // Build toast pack once for this column; reused by every LIKE filter.
  ThreadFilterState *tl = &tl_filter_state;
  size_t toast_count =
      BuildToastPackPORCVEC(tl, column, data_buf, offsets, nrows);

  for (int f = 0; f < filter_count; f++) {
    const auto &fd = filters[f];

    // kStrNotEmpty: simple offset-length check.  Toast values are never
    // empty by construction (toast triggers on large varlenas), so the
    // conservative-pass for toast rows is correct.
    if (fd.op == FastFilterOp::kStrNotEmpty) {
      for (size_t row = 0; row < nrows; row++) {
        size_t w = row >> 3;
        uint8_t bit = static_cast<uint8_t>(1U << (row & 7));
        if (!(col_bm[w] & bit)) continue;
        if (column->IsToast(row)) continue;
        if (offsets[row + 1] == offsets[row])
          col_bm[w] &= ~bit;
      }
      continue;
    }

    const char *needle = fd.str_const;
    int32_t needle_len = fd.str_const_len;
    bool is_contains = (fd.op == FastFilterOp::kStrContains);

    // Empty needle: defensive dead path (fast filter classifier requires
    // plen >= 3 in '%...%' patterns, so needle_len >= 1).
    if (needle_len == 0) {
      if (!is_contains) {
        for (size_t row = 0; row < nrows; row++) {
          size_t w = row >> 3;
          uint8_t bit = static_cast<uint8_t>(1U << (row & 7));
          if ((col_bm[w] & bit) && !column->IsToast(row))
            col_bm[w] &= ~bit;
        }
      }
      continue;
    }

    if (needle_len >= 4)
      pax_init_shift(needle, needle_len);

    // filt_bm: bit=1 means "row contains needle" (elem == row for PORCVEC).
    tl->EnsureBitmapCapacity(nrows);
    uint8_t *filt_bm = tl->filt_bm.data();
    memset(filt_bm, 0, nwords);

    // Pass 1: scan raw buffer.  Toast rows' bits here are unreliable (they
    // reflect matches against toast pointer header bytes, not real data).
    BufferScanSubstr<false>(data_buf, offsets, nrows, needle, needle_len,
                            filt_bm, nullptr);

    // Pass 2: clear toast bits in filt_bm and rescan the detoasted pack.
    MergeToastPackScalar(tl, toast_count, needle, needle_len, filt_bm);

    // Apply: kStrContains → AND, kStrNotContains → NAND.
    if (is_contains) {
      for (size_t w = 0; w < nwords; w++)
        col_bm[w] &= filt_bm[w];
    } else {
      for (size_t w = 0; w < nwords; w++)
        col_bm[w] &= ~filt_bm[w];
    }
  }
}

// ---------------------------------------------------------------------------
// Continuous-buffer substring scan for PORC format.
//
// PORC stores each string as [varlena_header][string_data][optional_padding].
// Null rows are excluded from the buffer entirely.  We scan the whole buffer
// with pax_is_substr() and validate each candidate match against the actual
// string-data range within the varlena to eliminate false positives caused by
// matches landing in headers or padding.
// ---------------------------------------------------------------------------
static void EvalColumnStrFullScan_PORC(PaxColumn *column,
                                      const FastFilterDesc *filters,
                                      int filter_count, size_t nrows,
                                      uint8_t *col_bm) {
  size_t nwords = (nrows + 7) / 8;

  // Null bitmap.
  const uint8_t *null_bm = nullptr;
  if (column->HasNull()) {
    auto bm = column->GetBitmap().get();
    if (bm) null_bm = bm->Raw().bitmap;
  }

  // Build elem_to_row mapping (buffer element index → actual row index).
  size_t non_null_count = column->GetNonNullRows();
  std::vector<int32_t> elem_to_row;
  elem_to_row.reserve(non_null_count);
  for (size_t row = 0; row < nrows; row++) {
    size_t w = row >> 3;
    uint8_t bit = static_cast<uint8_t>(1U << (row & 7));
    if (!null_bm || (null_bm[w] & bit))
      elem_to_row.push_back(static_cast<int32_t>(row));
  }

  // Initialize col_bm: all non-null rows pass.
  memset(col_bm, 0xFF, nwords);
  if (nrows & 7)
    col_bm[nwords - 1] = static_cast<uint8_t>((1U << (nrows & 7)) - 1);
  if (null_bm) {
    for (size_t w = 0; w < nwords; w++)
      col_bm[w] &= null_bm[w];
  }

  // Contiguous data buffer and offsets (non_null_count+1 int32 entries).
  auto [data_buf, data_size] = column->GetBuffer();
  auto *nf_col = static_cast<PaxNonFixedColumn *>(column);
  auto [off_buf, off_size] = nf_col->GetOffsetBuffer(false);
  const int32_t *offsets = reinterpret_cast<const int32_t *>(off_buf);

  // Build toast pack once for this column; reused by every LIKE filter.
  ThreadFilterState *tl = &tl_filter_state;
  size_t toast_count = BuildToastPackPORC(tl, column, data_buf, offsets,
                                          elem_to_row.data(), non_null_count);

  for (int f = 0; f < filter_count; f++) {
    const auto &fd = filters[f];

    // kStrNotEmpty: per-element check via varlena.  Toast values are never
    // empty by construction, so conservative-pass for toast is correct.
    if (fd.op == FastFilterOp::kStrNotEmpty) {
      for (size_t ei = 0; ei < non_null_count; ei++) {
        int32_t row = elem_to_row[ei];
        size_t w = row >> 3;
        uint8_t bit = static_cast<uint8_t>(1U << (row & 7));
        if (!(col_bm[w] & bit)) continue;
        if (column->IsToast(ei)) continue;
        const char *ptr = data_buf + offsets[ei];
        if (VARSIZE_ANY_EXHDR(ptr) == 0)
          col_bm[w] &= ~bit;
      }
      continue;
    }

    const char *needle = fd.str_const;
    int32_t needle_len = fd.str_const_len;
    bool is_contains = (fd.op == FastFilterOp::kStrContains);

    if (needle_len == 0) {
      if (!is_contains) {
        for (size_t ei = 0; ei < non_null_count; ei++) {
          int32_t row = elem_to_row[ei];
          size_t w = row >> 3;
          uint8_t bit = static_cast<uint8_t>(1U << (row & 7));
          if ((col_bm[w] & bit) && !column->IsToast(ei))
            col_bm[w] &= ~bit;
        }
      }
      continue;
    }

    if (needle_len >= 4)
      pax_init_shift(needle, needle_len);

    // filt_bm: bit=1 at ROW index means "element contains needle".
    tl->EnsureBitmapCapacity(nrows);
    uint8_t *filt_bm = tl->filt_bm.data();
    memset(filt_bm, 0, nwords);

    // Pass 1: scan raw varlena buffer.  Toast row bits are unreliable.
    BufferScanSubstr<true>(data_buf, offsets, non_null_count, needle,
                           needle_len, filt_bm, elem_to_row.data());

    // Pass 2: clear toast bits and rescan the detoasted pack.
    MergeToastPackScalar(tl, toast_count, needle, needle_len, filt_bm);

    // Apply: kStrContains → AND, kStrNotContains → NAND.
    if (is_contains) {
      for (size_t w = 0; w < nwords; w++)
        col_bm[w] &= filt_bm[w];
    } else {
      for (size_t w = 0; w < nwords; w++)
        col_bm[w] &= ~filt_bm[w];
    }
  }
}

// ---------------------------------------------------------------------------
// AVX-512 SIMD substring scan for PORC_VEC format.
//
// First-last byte filter: broadcast needle[0] and needle[len-1] into 512-bit
// vectors, compare 64 positions at once, AND the two masks, then verify each
// candidate with memcmp on the middle bytes.  No shift-table setup required,
// so there is no needle-length limitation.
// ---------------------------------------------------------------------------
#ifdef __x86_64__
static bool pax_has_avx512() {
  unsigned int eax, ebx, ecx, edx;
  if (__get_cpuid_count(7, 0, &eax, &ebx, &ecx, &edx)) {
    // AVX-512F = EBX bit 16, AVX-512BW = EBX bit 30
    return (ebx & (1u << 16)) && (ebx & (1u << 30));
  }
  return false;
}

// ---------------------------------------------------------------------------
// Core AVX-512 SIMD substring scan.
//
// First-last byte filter: broadcast needle[0] and needle[len-1] into 512-bit
// vectors, compare 64 positions at once, AND the two masks, then verify each
// candidate with memcmp on the middle bytes.
//
// Same template parameter and elem_to_idx semantics as BufferScanSubstr.
// Caller must zero match_bm.  No needle-length limitation.
// ---------------------------------------------------------------------------
template <bool HasVarlena>
__attribute__((target("avx512f,avx512bw")))
static void BufferScanSubstrAVX512(const char *data_buf,
                                   const int32_t *offsets,
                                   size_t num_elems, const char *needle,
                                   int32_t needle_len, uint8_t *match_bm,
                                   const int32_t *elem_to_idx) {
  const int32_t buf_start = offsets[0];
  const int32_t buf_end = offsets[num_elems];
  const int32_t buf_len = buf_end - buf_start;

  if (buf_len < needle_len) return;

  const __m512i first_vec = _mm512_set1_epi8(needle[0]);
  const __m512i last_vec = _mm512_set1_epi8(needle[needle_len - 1]);
  const int32_t scan_len = buf_len - needle_len + 1;
  size_t elem = 0;
  bool scan_done = false;

  // Process a verified match at match_pos.  Returns true when scan is done.
  auto handle_match = [&](int32_t match_pos) -> bool {
    while (elem < num_elems && offsets[elem + 1] <= match_pos) elem++;
    if (elem >= num_elems) return true;

    CheckAndMarkMatch<HasVarlena>(data_buf, offsets, elem,
                                  data_buf + match_pos, needle_len,
                                  match_bm, elem_to_idx, nullptr);
    return false;
  };

  // Main 64-byte blocks.
  int32_t pos = buf_start;
  const int32_t main_end =
      buf_start + (scan_len >= 64 ? scan_len - 63 : 0);

  for (; pos < main_end && !scan_done; pos += 64) {
    __m512i block_first = _mm512_loadu_si512(data_buf + pos);
    __m512i block_last =
        _mm512_loadu_si512(data_buf + pos + needle_len - 1);

    __mmask64 eq_first = _mm512_cmpeq_epi8_mask(block_first, first_vec);
    __mmask64 eq_last = _mm512_cmpeq_epi8_mask(block_last, last_vec);
    __mmask64 candidates = eq_first & eq_last;

    while (candidates) {
      int bit_pos = __builtin_ctzll(candidates);
      candidates &= candidates - 1;
      int32_t match_pos = pos + bit_pos;

      if (needle_len <= 2 ||
          memcmp(data_buf + match_pos + 1, needle + 1,
                 needle_len - 2) == 0) {
        if (handle_match(match_pos)) { scan_done = true; break; }
      }
    }
  }

  // Tail with masked SIMD load.
  // Mask-suppressed; safe across page boundary (Intel SDM Vol 2C, AVX-512
  // masking) — unlike AVX2 vmaskmov*, masked-off bytes do not generate #PF.
  if (!scan_done && pos < buf_start + scan_len) {
    int32_t remaining = buf_start + scan_len - pos;
    __mmask64 tail_mask =
        (remaining >= 64) ? ~0ULL : (1ULL << remaining) - 1;

    __m512i block_first =
        _mm512_maskz_loadu_epi8(tail_mask, data_buf + pos);
    __m512i block_last =
        _mm512_maskz_loadu_epi8(tail_mask,
                                data_buf + pos + needle_len - 1);

    __mmask64 eq_first = _mm512_cmpeq_epi8_mask(block_first, first_vec);
    __mmask64 eq_last = _mm512_cmpeq_epi8_mask(block_last, last_vec);
    __mmask64 candidates = eq_first & eq_last & tail_mask;

    while (candidates) {
      int bit_pos = __builtin_ctzll(candidates);
      candidates &= candidates - 1;

      int32_t match_pos = pos + bit_pos;

      if (needle_len <= 2 ||
          memcmp(data_buf + match_pos + 1, needle + 1,
                 needle_len - 2) == 0) {
        if (handle_match(match_pos)) break;
      }
    }
  }
}

// AVX-512 variant of MergeToastPackScalar.  See comments there.
__attribute__((target("avx512f,avx512bw")))
static void MergeToastPackAVX512(ThreadFilterState *tl, size_t toast_count,
                                 const char *needle, int32_t needle_len,
                                 uint8_t *filt_bm) {
  if (toast_count == 0) return;
  for (size_t i = 0; i < toast_count; i++) {
    int32_t row = tl->toast_elem_to_row[i];
    filt_bm[row >> 3] &= ~static_cast<uint8_t>(1U << (row & 7));
  }
  BufferScanSubstrAVX512<false>(
      reinterpret_cast<const char *>(tl->toast_buf.data()),
      tl->toast_offsets.data(), toast_count, needle, needle_len, filt_bm,
      tl->toast_elem_to_row.data());
}

__attribute__((target("avx512f,avx512bw")))
static void EvalColumnStrFullScan_PORCVEC_AVX512(PaxColumn *column,
                                         const FastFilterDesc *filters,
                                         int filter_count, size_t nrows,
                                         uint8_t *col_bm) {
  size_t nwords = (nrows + 7) / 8;

  // Null bitmap: bit=1 means NOT null.
  const uint8_t *null_bm = nullptr;
  if (column->HasNull()) {
    auto bm = column->GetBitmap().get();
    if (bm) null_bm = bm->Raw().bitmap;
  }

  // Initialize col_bm: all non-null rows pass (bit=1).
  memset(col_bm, 0xFF, nwords);
  if (nrows & 7)
    col_bm[nwords - 1] = static_cast<uint8_t>((1U << (nrows & 7)) - 1);
  if (null_bm) {
    for (size_t w = 0; w < nwords; w++)
      col_bm[w] &= null_bm[w];
  }

  // Contiguous data buffer and offsets (nrows+1 int32 entries).
  auto [data_buf, data_size] = column->GetBuffer();
  auto *nf_col = static_cast<PaxVecNonFixedColumn *>(column);
  auto [off_buf, off_size] = nf_col->GetOffsetBuffer(false);
  const int32_t *offsets = reinterpret_cast<const int32_t *>(off_buf);

  // Build toast pack once for this column; reused by every LIKE filter.
  ThreadFilterState *tl = &tl_filter_state;
  size_t toast_count =
      BuildToastPackPORCVEC(tl, column, data_buf, offsets, nrows);

  for (int f = 0; f < filter_count; f++) {
    const auto &fd = filters[f];

    // kStrNotEmpty: toast values are never empty by construction.
    if (fd.op == FastFilterOp::kStrNotEmpty) {
      for (size_t row = 0; row < nrows; row++) {
        size_t w = row >> 3;
        uint8_t bit = static_cast<uint8_t>(1U << (row & 7));
        if (!(col_bm[w] & bit)) continue;
        if (column->IsToast(row)) continue;
        if (offsets[row + 1] == offsets[row])
          col_bm[w] &= ~bit;
      }
      continue;
    }

    const char *needle = fd.str_const;
    int32_t needle_len = fd.str_const_len;
    bool is_contains = (fd.op == FastFilterOp::kStrContains);

    // Empty needle: defensive dead path.
    if (needle_len == 0) {
      if (!is_contains) {
        for (size_t row = 0; row < nrows; row++) {
          size_t w = row >> 3;
          uint8_t bit = static_cast<uint8_t>(1U << (row & 7));
          if ((col_bm[w] & bit) && !column->IsToast(row))
            col_bm[w] &= ~bit;
        }
      }
      continue;
    }

    // filt_bm: bit=1 means "row contains needle" (elem == row for PORCVEC).
    tl->EnsureBitmapCapacity(nrows);
    uint8_t *filt_bm = tl->filt_bm.data();
    memset(filt_bm, 0, nwords);

    // Pass 1: scan raw buffer.  Toast row bits are unreliable.
    BufferScanSubstrAVX512<false>(data_buf, offsets, nrows, needle,
                                  needle_len, filt_bm, nullptr);

    // Pass 2: clear toast bits and rescan the detoasted pack.
    MergeToastPackAVX512(tl, toast_count, needle, needle_len, filt_bm);

    // Apply: kStrContains → AND, kStrNotContains → NAND.
    if (is_contains) {
      for (size_t w = 0; w < nwords; w++)
        col_bm[w] &= filt_bm[w];
    } else {
      for (size_t w = 0; w < nwords; w++)
        col_bm[w] &= ~filt_bm[w];
    }
  }
}

// ---------------------------------------------------------------------------
// AVX-512 SIMD substring scan for PORC format.
//
// Same first-last byte SIMD filter as the PORCVEC variant, but with PORC's
// false-positive elimination: each SIMD-verified match is checked against
// the actual string-data range inside the varlena (skipping headers/padding).
// Null rows are excluded from the buffer; elem_to_row maps buffer elements
// back to actual row indices.
// ---------------------------------------------------------------------------
__attribute__((target("avx512f,avx512bw")))
static void EvalColumnStrFullScan_PORC_AVX512(PaxColumn *column,
                                               const FastFilterDesc *filters,
                                               int filter_count, size_t nrows,
                                               uint8_t *col_bm) {
  size_t nwords = (nrows + 7) / 8;

  // Null bitmap.
  const uint8_t *null_bm = nullptr;
  if (column->HasNull()) {
    auto bm = column->GetBitmap().get();
    if (bm) null_bm = bm->Raw().bitmap;
  }

  // Build elem_to_row mapping (buffer element index → actual row index).
  size_t non_null_count = column->GetNonNullRows();
  std::vector<int32_t> elem_to_row;
  elem_to_row.reserve(non_null_count);
  for (size_t row = 0; row < nrows; row++) {
    size_t w = row >> 3;
    uint8_t bit = static_cast<uint8_t>(1U << (row & 7));
    if (!null_bm || (null_bm[w] & bit))
      elem_to_row.push_back(static_cast<int32_t>(row));
  }

  // Initialize col_bm: all non-null rows pass.
  memset(col_bm, 0xFF, nwords);
  if (nrows & 7)
    col_bm[nwords - 1] = static_cast<uint8_t>((1U << (nrows & 7)) - 1);
  if (null_bm) {
    for (size_t w = 0; w < nwords; w++)
      col_bm[w] &= null_bm[w];
  }

  // Contiguous data buffer and offsets (non_null_count+1 int32 entries).
  auto [data_buf, data_size] = column->GetBuffer();
  auto *nf_col = static_cast<PaxNonFixedColumn *>(column);
  auto [off_buf, off_size] = nf_col->GetOffsetBuffer(false);
  const int32_t *offsets = reinterpret_cast<const int32_t *>(off_buf);

  // Build toast pack once for this column; reused by every LIKE filter.
  ThreadFilterState *tl = &tl_filter_state;
  size_t toast_count = BuildToastPackPORC(tl, column, data_buf, offsets,
                                          elem_to_row.data(), non_null_count);

  for (int f = 0; f < filter_count; f++) {
    const auto &fd = filters[f];

    // kStrNotEmpty: per-element check via varlena.  Toast values are never
    // empty by construction.
    if (fd.op == FastFilterOp::kStrNotEmpty) {
      for (size_t ei = 0; ei < non_null_count; ei++) {
        int32_t row = elem_to_row[ei];
        size_t w = row >> 3;
        uint8_t bit = static_cast<uint8_t>(1U << (row & 7));
        if (!(col_bm[w] & bit)) continue;
        if (column->IsToast(ei)) continue;
        const char *ptr = data_buf + offsets[ei];
        if (VARSIZE_ANY_EXHDR(ptr) == 0)
          col_bm[w] &= ~bit;
      }
      continue;
    }

    const char *needle = fd.str_const;
    int32_t needle_len = fd.str_const_len;
    bool is_contains = (fd.op == FastFilterOp::kStrContains);

    if (needle_len == 0) {
      if (!is_contains) {
        for (size_t ei = 0; ei < non_null_count; ei++) {
          int32_t row = elem_to_row[ei];
          size_t w = row >> 3;
          uint8_t bit = static_cast<uint8_t>(1U << (row & 7));
          if ((col_bm[w] & bit) && !column->IsToast(ei))
            col_bm[w] &= ~bit;
        }
      }
      continue;
    }

    // filt_bm: bit=1 at ROW index means "element contains needle".
    tl->EnsureBitmapCapacity(nrows);
    uint8_t *filt_bm = tl->filt_bm.data();
    memset(filt_bm, 0, nwords);

    // Pass 1: SIMD scan over the raw varlena buffer.  Toast row bits are
    // unreliable.
    BufferScanSubstrAVX512<true>(data_buf, offsets, non_null_count, needle,
                                 needle_len, filt_bm, elem_to_row.data());

    // Pass 2: clear toast bits and rescan the detoasted pack.
    MergeToastPackAVX512(tl, toast_count, needle, needle_len, filt_bm);

    // Apply: kStrContains → AND, kStrNotContains → NAND.
    if (is_contains) {
      for (size_t w = 0; w < nwords; w++)
        col_bm[w] &= filt_bm[w];
    } else {
      for (size_t w = 0; w < nwords; w++)
        col_bm[w] &= ~filt_bm[w];
    }
  }
}
#endif  // __x86_64__

// ---------------------------------------------------------------------------
// Dispatcher: choose the best FullScan strategy for string columns.
// ---------------------------------------------------------------------------
static void EvalColumnStrFullScan(PaxColumn *column,
                                  const FastFilterDesc *filters,
                                  int filter_count, size_t nrows,
                                  bool is_vec, uint8_t *col_bm) {
#ifdef __x86_64__
  // AVX-512 path: no needle-length limitation, handles all ops.
  static const bool has_avx512 = pax_has_avx512();
  if (has_avx512) {
    if (is_vec)
      EvalColumnStrFullScan_PORCVEC_AVX512(column, filters, filter_count,
                                           nrows, col_bm);
    else
      EvalColumnStrFullScan_PORC_AVX512(column, filters, filter_count,
                                        nrows, col_bm);
    return;
  }
#endif

  // Fall back to per-element Memcmp for needles > 256 bytes
  // (pax_is_substr shift-table limitation).
  for (int f = 0; f < filter_count; f++) {
    if ((filters[f].op == FastFilterOp::kStrContains ||
         filters[f].op == FastFilterOp::kStrNotContains) &&
        filters[f].str_const_len > 256) {
      EvalColumnStrFullScanMemcmp(column, filters, filter_count, nrows,
                                  is_vec, col_bm);
      return;
    }
  }

  if (is_vec)
    EvalColumnStrFullScan_PORCVEC(column, filters, filter_count, nrows,
                                  col_bm);
  else
    EvalColumnStrFullScan_PORC(column, filters, filter_count, nrows, col_bm);
}

// Selective scan for string column.  Modifies combined_bm in-place.
// PORC format: must iterate all rows to maintain non_null_count.
static void EvalColumnStrSelective(PaxColumn *column,
                                   const FastFilterDesc *filters,
                                   int filter_count, size_t nrows,
                                   bool is_vec, uint8_t *combined_bm) {
  const uint8_t *null_bm = nullptr;
  if (column->HasNull()) {
    auto bm = column->GetBitmap().get();
    if (bm) null_bm = bm->Raw().bitmap;
  }

  ThreadFilterState *tl = &tl_filter_state;

  if (is_vec) {
    // VEC format: buf_pos == row_idx, can skip entire zero bytes.
    size_t nwords = (nrows + 7) / 8;
    for (size_t w = 0; w < nwords; w++) {
      uint8_t word = combined_bm[w];
      if (word == 0) continue;

      uint8_t new_word = word;
      size_t base_row = w * 8;

      for (uint8_t b = 0; b < 8 && (base_row + b) < nrows; b++) {
        uint8_t bit = static_cast<uint8_t>(1U << b);
        if (!(word & bit)) continue;

        size_t row = base_row + b;

        if (null_bm && !(null_bm[w] & bit)) {
          new_word &= ~bit;
          continue;
        }

        if (column->IsToast(row)) {
          if (!DetoastAndEvalRow(tl, column, row, filters, filter_count))
            new_word &= ~bit;
          continue;
        }

        auto [ptr, len] = column->GetBuffer(row);
        if (!ptr) { new_word &= ~bit; continue; }

        if (!EvalStrFilters(ptr, len, filters, filter_count))
          new_word &= ~bit;
      }

      combined_bm[w] = new_word;
    }
  } else {
    // PORC format: must iterate all rows sequentially for non_null_count.
    size_t non_null_count = 0;

    for (size_t row = 0; row < nrows; row++) {
      size_t w = row >> 3;
      uint8_t bit = static_cast<uint8_t>(1U << (row & 7));

      bool row_is_null = (null_bm && !(null_bm[w] & bit));
      size_t buf_pos = non_null_count;
      if (!row_is_null) non_null_count++;

      // Skip evaluation for already-rejected rows (but non_null_count updated above).
      if (!(combined_bm[w] & bit)) continue;

      if (row_is_null) {
        combined_bm[w] &= ~bit;
        continue;
      }

      if (column->IsToast(buf_pos)) {
        if (!DetoastAndEvalRow(tl, column, buf_pos, filters, filter_count))
          combined_bm[w] &= ~bit;
        continue;
      }

      auto [ptr, len] = column->GetBuffer(buf_pos);
      if (!ptr) { combined_bm[w] &= ~bit; continue; }

      const char *str_data = VARDATA_ANY(ptr);
      size_t str_len = VARSIZE_ANY_EXHDR(ptr);

      if (!EvalStrFilters(str_data, str_len, filters, filter_count))
        combined_bm[w] &= ~bit;
    }
  }
}

// ---------------------------------------------------------------------------
// Type dispatch wrappers
// ---------------------------------------------------------------------------

static void EvalColumnGroupFullScan(PaxColumn *column,
                                    const FastFilterDesc *filters,
                                    int filter_count, bool is_string,
                                    bool is_vec, size_t nrows,
                                    uint8_t *col_bm) {
  if (is_string) {
    EvalColumnStrFullScan(column, filters, filter_count, nrows, is_vec,
                          col_bm);
    return;
  }

  auto [buffer, buf_size] = column->GetBuffer();
  const uint8_t *null_bm = nullptr;
  if (column->HasNull()) {
    auto bm = column->GetBitmap().get();
    if (bm) null_bm = bm->Raw().bitmap;
  }

  switch (filters[0].type_len) {
    case 8:
      EvalColumnIntFullScan(reinterpret_cast<int64 *>(buffer), null_bm,
                            filters, filter_count, is_vec, nrows, col_bm);
      break;
    case 4:
      EvalColumnIntFullScan(reinterpret_cast<int32 *>(buffer), null_bm,
                            filters, filter_count, is_vec, nrows, col_bm);
      break;
    case 2:
      EvalColumnIntFullScan(reinterpret_cast<int16 *>(buffer), null_bm,
                            filters, filter_count, is_vec, nrows, col_bm);
      break;
    case 1:
      EvalColumnIntFullScan(reinterpret_cast<int8 *>(buffer), null_bm,
                            filters, filter_count, is_vec, nrows, col_bm);
      break;
  }
}

static void EvalColumnGroupSelective(PaxColumn *column,
                                     const FastFilterDesc *filters,
                                     int filter_count, bool is_string,
                                     bool is_vec, size_t nrows,
                                     uint8_t *combined_bm) {
  if (is_string) {
    EvalColumnStrSelective(column, filters, filter_count, nrows, is_vec,
                           combined_bm);
    return;
  }

  auto [buffer, buf_size] = column->GetBuffer();
  const uint8_t *null_bm = nullptr;
  if (column->HasNull()) {
    auto bm = column->GetBitmap().get();
    if (bm) null_bm = bm->Raw().bitmap;
  }

  switch (filters[0].type_len) {
    case 8:
      EvalColumnIntSelective(reinterpret_cast<int64 *>(buffer), null_bm,
                             filters, filter_count, is_vec, nrows, combined_bm);
      break;
    case 4:
      EvalColumnIntSelective(reinterpret_cast<int32 *>(buffer), null_bm,
                             filters, filter_count, is_vec, nrows, combined_bm);
      break;
    case 2:
      EvalColumnIntSelective(reinterpret_cast<int16 *>(buffer), null_bm,
                             filters, filter_count, is_vec, nrows, combined_bm);
      break;
    case 1:
      EvalColumnIntSelective(reinterpret_cast<int8 *>(buffer), null_bm,
                             filters, filter_count, is_vec, nrows, combined_bm);
      break;
  }
}

PaxVecReader::PaxVecReader(std::unique_ptr<MicroPartitionReader> &&reader,
                           std::shared_ptr<VecAdapter> adapter,
                           std::shared_ptr<PaxFilter> filter)
    : adapter_(std::move(adapter)),
      current_group_index_(0),
      ctid_offset_(0),
      filter_(std::move(filter)) {
  Assert(reader && adapter_);
  SetReader(std::move(reader));
}

void PaxVecReader::Open(const ReaderOptions &options) {
  auto visimap = options.visibility_bitmap;
  micro_partition_visibility_bitmap_ = visimap;

  // Two-stage requires disabling reused_buffer (Phase 1 and Phase 2 data
  // cannot share buffer). Current vec path already sets reused_buffer=nullptr,
  // but this is defensive.
  auto *fast_filter = filter_ ? filter_->GetFastFilter().get() : nullptr;
  bool has_fast_filter = fast_filter && fast_filter->HasFastFilter();
  if (has_fast_filter && options.reused_buffer) {
    ReaderOptions modified = options;
    modified.reused_buffer = nullptr;
    reader_->Open(modified);
  } else {
    reader_->Open(options);
  }

  if (visimap) {
    adapter_->SetVisibitilyMapInfo(visimap);
  }
}

void PaxVecReader::Close() {
  reader_->Close();
}

PaxVecReader::~PaxVecReader() {}

std::shared_ptr<arrow::RecordBatch> PaxVecReader::ReadBatch(
    PaxFragmentInterface *frag) {
  auto desc = adapter_->GetRelationTupleDesc();
  std::shared_ptr<arrow::RecordBatch> result;
  size_t flush_nums_of_rows = 0;

  auto *fast_filter = filter_ ? filter_->GetFastFilter().get() : nullptr;
  bool has_fast_filter = fast_filter && fast_filter->HasFastFilter();

retry_next_group:
  if (!working_group_) {
    if (current_group_index_ >= reader_->GetGroupNums()) {
      return nullptr;
    }
    auto group_index = current_group_index_++;
    auto info = reader_->GetGroupStatsInfo(group_index);
    if (filter_ && !filter_->ExecSparseFilter(
                       *info, desc, PaxSparseFilter::StatisticsKind::kGroup)) {
      goto retry_next_group;
    }

    // TopK Runtime Filter: skip group if all rows are worse than threshold.
    // Cheaper than fast filter (stats-only), so evaluated first.
    if (topk_threshold_ && topk_threshold_->IsSet()) {
      if (EvalTopKThresholdSkip(*info, desc)) {
        goto retry_next_group;
      }
    }

    // Two-phase fast filter: Phase 1 reads filter columns and evaluates
    // predicates; Phase 2 reads only remaining output columns if needed.
    if (has_fast_filter) {
      auto ff_result = FastFilterGroup(group_index);

      // Merge filter bitmap into visibility bitmap.
      if (ff_result.bitmap) {
        if (!micro_partition_visibility_bitmap_) {
          micro_partition_visibility_bitmap_ =
              std::shared_ptr<Bitmap8>(ff_result.bitmap.release());
          adapter_->SetVisibitilyMapInfo(micro_partition_visibility_bitmap_);
        } else {
          const auto &filt_raw = ff_result.bitmap->Raw();
          for (size_t w = 0; w < filt_raw.size; w++) {
            if (filt_raw.bitmap[w] == 0) continue;
            for (uint8 bit = 0; bit < 8; bit++) {
              if (filt_raw.bitmap[w] & (1U << bit))
                micro_partition_visibility_bitmap_->Set(
                    static_cast<uint32>(w * 8 + bit));
            }
          }
        }
      }

      // All visible rows filtered out — skip Phase 2 entirely.
      if (ff_result.all_filtered) {
        goto retry_next_group;
      }

      // Phase 2: read only output-only columns and merge with Phase 1.
      if (fast_filter->HasRemainingColumns()) {
        const auto &remaining_proj = fast_filter->GetRemainingProjection();
        auto remaining_group = reader_->ReadGroup(group_index, remaining_proj);
        // Merge Phase 1 filter columns into Phase 2 result.
        remaining_group->GetAllColumns()->MergeColumnsFrom(
            ff_result.filter_group->GetAllColumns().get());
        working_group_ = std::move(remaining_group);
      } else {
        // All scan columns are fast filter columns — no Phase 2 needed.
        working_group_ = std::move(ff_result.filter_group);
      }
    } else {
      // No fast filter — read all columns as before.
      working_group_ = reader_->ReadGroup(group_index);
    }

    adapter_->SetDataSource(working_group_->GetAllColumns().get(),
                            working_group_->GetRowOffset());
  }

  if (!adapter_->AppendToVecBuffer()) {
    working_group_ = nullptr;
    goto retry_next_group;
  }

  result = adapter_->FlushVecBuffer(ctid_offset_, frag, flush_nums_of_rows);
  ctid_offset_ += flush_nums_of_rows;
  if (!result) {
    working_group_ = nullptr;
    goto retry_next_group;
  }

  Assert(flush_nums_of_rows > 0);
  return result;
}

// Initialize `combined_bm` to "all visible rows pass" for a batch of `nrows`
// starting at absolute index `row_offset`.  Rows marked invisible in the
// micro-partition visibility bitmap are pre-cleared so the downstream eval
// loops naturally skip them (no detoast / memcmp work spent on deleted rows).
static void InitCombinedBmWithVisibility(uint8_t *combined_bm, size_t nrows,
                                         const Bitmap8 *visibility,
                                         size_t row_offset) {
  size_t nwords = (nrows + 7) / 8;
  memset(combined_bm, 0xFF, nwords);
  size_t extra_bits = nrows & 7;
  if (extra_bits > 0 && nwords > 0)
    combined_bm[nwords - 1] = static_cast<uint8_t>((1U << extra_bits) - 1);

  if (!visibility) return;

  // Visibility bit=1 means "invisible (deleted)".  Clear those bits.
  for (size_t row = 0; row < nrows; row++) {
    if (visibility->Test(static_cast<uint32>(row_offset + row))) {
      combined_bm[row >> 3] &= ~static_cast<uint8_t>(1U << (row & 7));
    }
  }
}

// ---------------------------------------------------------------------------
// Sampling phase: evaluate all filter columns (already batch-read) with full
// scan, collect per-column rejection counts, and sort eval_order.
// ---------------------------------------------------------------------------
static void FastFilterSampling(ThreadFilterState *tl,
                               const FastFilterContext *fast_ctx,
                               PaxColumns *all_columns, bool is_vec,
                               size_t nrows, int ncg,
                               const Bitmap8 *visibility,
                               size_t row_offset) {
  size_t nwords = (nrows + 7) / 8;

  InitCombinedBmWithVisibility(tl->combined_bm.data(), nrows, visibility,
                               row_offset);

  for (int ci = 0; ci < ncg; ci++) {
    int g = tl->eval_order[ci];
    auto &cg = tl->col_groups[g];
    PaxColumn *column = (*all_columns)[cg.attno - 1].get();
    if (!column) continue;

    const FastFilterDesc *filters = &fast_ctx->filters[cg.filter_start];
    int filter_count = cg.filter_count;

    // Always full scan during sampling to collect independent rejection counts.
    EvalColumnGroupFullScan(column, filters, filter_count, cg.is_string,
                            is_vec, nrows, tl->col_bm.data());
    for (size_t w = 0; w < nwords; w++)
      tl->combined_bm[w] &= tl->col_bm[w];

    // Record per-column rejection count.
    size_t col_pass = PopcountBitmap(tl->col_bm.data(), nrows);
    tl->reject_counts[g] += (nrows - col_pass);

    // Early exit if all rows rejected.
    bool all_rejected = true;
    for (size_t w = 0; w < nwords; w++) {
      if (tl->combined_bm[w] != 0) { all_rejected = false; break; }
    }
    if (all_rejected) break;
  }

  // Sort evaluation order by descending rejection count within int/str groups.
  int ic = tl->int_group_count;
  if (ic > 1) {
    std::sort(tl->eval_order.begin(), tl->eval_order.begin() + ic,
              [tl](int a, int b) {
                return tl->reject_counts[a] > tl->reject_counts[b];
              });
  }
  if (ncg - ic > 1) {
    std::sort(tl->eval_order.begin() + ic, tl->eval_order.end(),
              [tl](int a, int b) {
                return tl->reject_counts[a] > tl->reject_counts[b];
              });
  }
  tl->group_count++;
}

// ---------------------------------------------------------------------------
// Interleaved phase: read one column at a time, evaluate filter, merge into
// accumulated filter_group, and check for early exit after each column.
// Returns the accumulated filter_group with all read columns merged.
// ---------------------------------------------------------------------------
static std::unique_ptr<MicroPartitionReader::Group> FastFilterInterleaved(
    ThreadFilterState *tl, const FastFilterContext *fast_ctx,
    MicroPartitionReader *reader, size_t group_index, int natts,
    bool *out_is_vec, size_t *out_nrows, size_t *out_row_offset, int ncg,
    const Bitmap8 *visibility) {
  std::unique_ptr<MicroPartitionReader::Group> filter_group;
  size_t nrows = 0;
  size_t row_offset = 0;
  bool is_vec = true;

  for (int ci = 0; ci < ncg; ci++) {
    int g = tl->eval_order[ci];
    auto &cg = tl->col_groups[g];

    // Read single column.
    auto single_proj = BuildSingleColumnProj(natts, cg.attno);
    auto col_group = reader->ReadGroup(group_index, single_proj);

    if (ci == 0) {
      nrows = col_group->GetRows();
      row_offset = col_group->GetRowOffset();

      // Detect storage format from first column.
      PaxColumn *first_col =
          (*col_group->GetAllColumns())[cg.attno - 1].get();
      if (first_col) is_vec = COLUMN_STORAGE_FORMAT_IS_VEC(first_col);

      // Setup bitmaps.
      tl->EnsureBitmapCapacity(nrows);
      InitCombinedBmWithVisibility(tl->combined_bm.data(), nrows,
                                   visibility, row_offset);
    }

    size_t nwords = (nrows + 7) / 8;
    PaxColumn *column = (*col_group->GetAllColumns())[cg.attno - 1].get();

    if (column) {
      const FastFilterDesc *filters = &fast_ctx->filters[cg.filter_start];
      int filter_count = cg.filter_count;

      // Decide full scan vs selective.
      bool use_selective = false;
      if (ci > 0) {
        if (cg.is_string && tl->int_group_count > 0) {
          // String after integer columns: always selective.
          use_selective = true;
        } else {
          size_t pass_count =
              PopcountBitmap(tl->combined_bm.data(), nrows);
          double rejection_rate =
              1.0 - static_cast<double>(pass_count) /
                         static_cast<double>(nrows);
          use_selective =
              (rejection_rate >= ThreadFilterState::kSelectiveThreshold);
        }
      }

      if (use_selective) {
        EvalColumnGroupSelective(column, filters, filter_count, cg.is_string,
                                 is_vec, nrows, tl->combined_bm.data());
      } else {
        EvalColumnGroupFullScan(column, filters, filter_count, cg.is_string,
                                is_vec, nrows, tl->col_bm.data());
        for (size_t w = 0; w < nwords; w++)
          tl->combined_bm[w] &= tl->col_bm[w];
      }
    }

    // Merge into accumulated filter_group.
    if (!filter_group) {
      filter_group = std::move(col_group);
    } else {
      filter_group->GetAllColumns()->MergeColumnsFrom(
          col_group->GetAllColumns().get());
    }

    // Early exit if all rows rejected.
    bool all_rejected = true;
    for (size_t w = 0; w < nwords; w++) {
      if (tl->combined_bm[w] != 0) { all_rejected = false; break; }
    }
    if (all_rejected) break;
  }

  tl->group_count++;
  *out_is_vec = is_vec;
  *out_nrows = nrows;
  *out_row_offset = row_offset;
  return filter_group;
}

PaxVecReader::FastFilterResult PaxVecReader::FastFilterGroup(
    size_t group_index) {
  auto *fast_filter = filter_->GetFastFilter().get();
  auto *fast_ctx = fast_filter->GetFastFilterContext();
  Assert(!fast_ctx->filters.empty());

  // --- Thread-local state initialization ---
  auto *tl = &tl_filter_state;
  Assert(fast_ctx->id != 0);
  if (tl->ctx_id != fast_ctx->id) {
    tl->ctx_id = fast_ctx->id;
    InitColumnGroups(tl, fast_ctx);
  }

  int ncg = static_cast<int>(tl->col_groups.size());
  int natts = static_cast<int>(fast_filter->GetFastFilterProjection().size());

  // Sampling during first kSamplingGroups micro-partitions (when ncg > 1,
  // i.e. there is something to re-order).
  bool is_sampling =
      (ncg > 1 && tl->group_count < ThreadFilterState::kSamplingGroups);

  std::unique_ptr<MicroPartitionReader::Group> filter_group;
  size_t nrows;
  size_t row_offset;
  bool is_vec;

  if (is_sampling) {
    // Sampling phase: read ALL filter columns at once, evaluate with full
    // scan, collect per-column rejection counts, and sort eval_order.
    const auto &fast_proj = fast_filter->GetFastFilterProjection();
    filter_group = reader_->ReadGroup(group_index, fast_proj);
    nrows = filter_group->GetRows();
    row_offset = filter_group->GetRowOffset();

    // Detect storage format.
    is_vec = true;
    for (int g = 0; g < ncg; g++) {
      PaxColumn *col =
          (*filter_group->GetAllColumns())[tl->col_groups[g].attno - 1].get();
      if (col) {
        is_vec = COLUMN_STORAGE_FORMAT_IS_VEC(col);
        break;
      }
    }

    tl->EnsureBitmapCapacity(nrows);
    FastFilterSampling(tl, fast_ctx, filter_group->GetAllColumns().get(),
                       is_vec, nrows, ncg,
                       micro_partition_visibility_bitmap_.get(), row_offset);
  } else {
    // Interleaved phase: read columns one at a time, filter, early exit.
    filter_group = FastFilterInterleaved(
        tl, fast_ctx, reader_.get(), group_index, natts,
        &is_vec, &nrows, &row_offset, ncg,
        micro_partition_visibility_bitmap_.get());
  }

  // --- Convert internal bitmap to output Bitmap8 ---
  // Internal: bit=1 = pass.  Output: bit=1 = filtered OUT.
  size_t filtered_count = 0;
  bool any_visible_passed = false;

  // Bitmap8 indexes by uint32.  Guard against silent truncation if a future
  // change ever feeds groups larger than ~4 billion rows.
  size_t max_bit_sz = row_offset + nrows;
  CBDB_CHECK(max_bit_sz <= static_cast<size_t>(UINT32_MAX),
             cbdb::CException::ExType::kExTypeOutOfRange,
             fmt("FastFilterGroup row offset+count exceeds uint32 "
                 "[row_offset=%lu, nrows=%lu]",
                 row_offset, nrows));
  auto bitmap = std::make_unique<Bitmap8>(static_cast<uint32>(max_bit_sz), 0);

  // combined_bm already has invisible rows pre-cleared by
  // InitCombinedBmWithVisibility, so they appear as "not passed" here and
  // get folded into the visibility output bitmap, which is correct (the
  // visibility bitmap downstream is the union of pre-existing invisibility
  // and rows rejected by fast filter).
  for (size_t row_idx = 0; row_idx < nrows; row_idx++) {
    bool passed =
        (tl->combined_bm[row_idx >> 3] & (1U << (row_idx & 7))) != 0;

    if (!passed) {
      bitmap->Set(row_offset + row_idx);
      filtered_count++;
    } else {
      any_visible_passed = true;
    }
  }

  FastFilterResult result;
  result.filter_group = std::move(filter_group);
  result.all_filtered = !any_visible_passed;

  if (filtered_count == 0) {
    result.bitmap = nullptr;
  } else {
    result.bitmap = std::move(bitmap);
  }

  return result;
}

bool PaxVecReader::ReadTuple(TupleTableSlot *slot) {
  auto desc = adapter_->GetRelationTupleDesc();
retry_read_group:
  if (!working_group_) {
    if (current_group_index_ >= reader_->GetGroupNums()) {
      return false;
    }
    auto group_index = current_group_index_++;
    auto info = reader_->GetGroupStatsInfo(group_index);
    if (filter_ && !filter_->ExecSparseFilter(
                       *info, desc, PaxSparseFilter::StatisticsKind::kGroup)) {
      goto retry_read_group;
    }

    working_group_ = reader_->ReadGroup(group_index);

    adapter_->SetDataSource(working_group_->GetAllColumns().get(),
                            working_group_->GetRowOffset());
  }

  auto flush_nums_of_rows = adapter_->AppendToVecBuffer();
  if (flush_nums_of_rows == -1) {
    working_group_ = nullptr;
    goto retry_read_group;
  }

  if (flush_nums_of_rows == 0) {
    goto retry_read_group;
  }

  adapter_->FlushVecBuffer(slot);

  return true;
}

int PaxVecReader::GetTuple(TupleTableSlot *slot, size_t row_index) {
  CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
}

size_t PaxVecReader::GetGroupNums() {
  CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
}

size_t PaxVecReader::GetTupleCountsInGroup(size_t group_index) {
  CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
}

std::unique_ptr<ColumnStatsProvider> PaxVecReader::GetGroupStatsInfo(
    size_t group_index) {
  CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
}

std::unique_ptr<MicroPartitionReader::Group> PaxVecReader::ReadGroup(
    size_t index) {
  CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
}

// ---------------------------------------------------------------------------
// TopK Runtime Filter: EvalTopKThresholdSkip
// ---------------------------------------------------------------------------

// Build a varlena (text/varchar/bpchar) using PAX_ALLOC (malloc) instead of
// PG palloc, so it is safe to call from Arrow worker threads where the PG
// CurrentMemoryContext is not available.  The returned pointer must be freed
// with PAX_FREE, NOT pfree.  The format is the standard 4-byte-header
// varlena, which is correctly parsed by VARDATA_ANY/VARSIZE_ANY_EXHDR used
// in the PAX TextCmp/VarstrCmp comparators (pax_oper.cc).
//
// Note: we intentionally do not honor atttypmod (no truncation/padding) since
// thresholds are obtained from already-stored column values that are already
// normalized; comparison only inspects the actual byte payload.
static inline void *MakeVarlenaPaxAlloc(const char *s, size_t len) {
  void *p = ::pax::PAX_ALLOC(len + VARHDRSZ);
  SET_VARSIZE(p, len + VARHDRSZ);
  memcpy(VARDATA(p), s, len);
  return p;
}

// Convert Arrow physical-type Scalar to PG Datum for comparison with
// PAX group min/max statistics. Handles the common ClickBench types.
//
// IMPORTANT: This function is called from Arrow worker threads via
// PaxVecReader::ReadBatch -> EvalTopKThresholdSkip.  PG palloc/pfree are
// NOT thread-safe, so for variable-length string types we construct the
// varlena buffer with PAX_ALLOC (malloc).  The caller in EvalTopKThresholdSkip
// frees the returned datum with PAX_FREE.
static std::pair<Datum, bool> ThresholdScalarToDatum(
    const std::shared_ptr<arrow::Scalar> &scalar, Form_pg_attribute attr) {
  if (!scalar || !scalar->is_valid) return {0, false};

  switch (scalar->type->id()) {
    case arrow::Type::BOOL: {
      auto v = static_cast<const arrow::BooleanScalar*>(scalar.get())->value;
      return {BoolGetDatum(v), true};
    }
    case arrow::Type::INT8: {
      auto v = static_cast<const arrow::Int8Scalar*>(scalar.get())->value;
      return {Int8GetDatum(v), true};
    }
    case arrow::Type::INT16: {
      auto v = static_cast<const arrow::Int16Scalar*>(scalar.get())->value;
      return {Int16GetDatum(v), true};
    }
    case arrow::Type::INT32: {
      auto v = static_cast<const arrow::Int32Scalar*>(scalar.get())->value;
      return {Int32GetDatum(v), true};
    }
    case arrow::Type::INT64: {
      auto v = static_cast<const arrow::Int64Scalar*>(scalar.get())->value;
      return {Int64GetDatum(v), true};
    }
    case arrow::Type::FLOAT: {
      auto v = static_cast<const arrow::FloatScalar*>(scalar.get())->value;
      return {Float4GetDatum(v), true};
    }
    case arrow::Type::DOUBLE: {
      auto v = static_cast<const arrow::DoubleScalar*>(scalar.get())->value;
      return {Float8GetDatum(v), true};
    }
    case arrow::Type::BINARY: {
      // Physical type for STRING columns (StringType::PhysicalType = BinaryType)
      auto *bs = static_cast<const arrow::BinaryScalar*>(scalar.get());
      const char *s = reinterpret_cast<const char *>(bs->value->data());
      auto len = static_cast<size_t>(bs->value->size());
      switch (attr->atttypid) {
        case TEXTOID:
        case VARCHAROID:
        case BPCHAROID:
          return {PointerGetDatum(MakeVarlenaPaxAlloc(s, len)), true};
        default:
          break;
      }
      return {0, false};
    }
    case arrow::Type::STRING: {
      auto *ss = static_cast<const arrow::StringScalar*>(scalar.get());
      const char *s = reinterpret_cast<const char *>(ss->value->data());
      auto len = static_cast<size_t>(ss->value->size());
      switch (attr->atttypid) {
        case TEXTOID:
        case VARCHAROID:
        case BPCHAROID:
          return {PointerGetDatum(MakeVarlenaPaxAlloc(s, len)), true};
        default:
          break;
      }
      return {0, false};
    }
    default:
      break;
  }
  return {0, false};
}

bool PaxVecReader::EvalTopKThresholdSkip(
    const ColumnStatsProvider& stats, TupleDesc desc) {
  // 1. Get current threshold
  auto threshold_scalar = topk_threshold_->Get();
  if (!threshold_scalar || !threshold_scalar->is_valid) return false;

  // 2. Get sort column info from TopKThresholdState
  int col = topk_threshold_->sort_column_index();
  auto order = topk_threshold_->sort_order();
  Oid collation = static_cast<Oid>(topk_threshold_->collation());

  // 3. Check sort column statistics availability
  if (col < 0 || col >= stats.ColumnSize()) return false;
  const auto& data_stats = stats.DataStats(col);
  if (!data_stats.has_minimal() || !data_stats.has_maximum()) return false;

  // 4. Arrow Scalar → Datum
  Form_pg_attribute attr = TupleDescAttr(desc, col);
  auto [threshold_datum, ok] = ThresholdScalarToDatum(threshold_scalar, attr);
  if (!ok) return false;

  // 5. Group min/max → Datum
  Datum group_min = pax::MicroPartitionStats::FromValue(
      data_stats.minimal(), attr->attlen, attr->attbyval, col);
  Datum group_max = pax::MicroPartitionStats::FromValue(
      data_stats.maximum(), attr->attlen, attr->attbyval, col);

  // 6. Compare using PAX's OperMinMaxFunc infrastructure
  OperMinMaxFunc cmp_func;
  bool skip = false;
  if (order == arrow::compute::SortOrder::Ascending) {
    // ASC: threshold is ceiling. If group_min > threshold → skip
    if (pax::MinMaxGetStrategyProcinfo(attr->atttypid, attr->atttypid,
                                       collation, cmp_func,
                                       BTGreaterStrategyNumber))
      skip = cmp_func(&group_min, &threshold_datum, collation);
  } else {
    // DESC: threshold is floor. If group_max < threshold → skip
    if (pax::MinMaxGetStrategyProcinfo(attr->atttypid, attr->atttypid,
                                       collation, cmp_func,
                                       BTLessStrategyNumber))
      skip = cmp_func(&group_max, &threshold_datum, collation);
  }

  // ThresholdScalarToDatum allocates a fresh varlena for non-byval types
  // (TEXT/VARCHAR/BPCHAR) using PAX_ALLOC (malloc) so it is safe to call
  // from Arrow worker threads.  Free with PAX_FREE here, NOT pfree.
  // group_min/group_max are pointers into the protobuf stats message and
  // must not be freed.
  if (!attr->attbyval && DatumGetPointer(threshold_datum) != nullptr)
    ::pax::PAX_FREE(DatumGetPointer(threshold_datum));

  return skip;
}

// ---------------------------------------------------------------------------
// Test-only hooks.  Exposed under RUN_GTEST so unit tests in
// pax_vec_reader_test.cc can drive the file-local scan / toast helpers
// without pulling them into the production header.
// ---------------------------------------------------------------------------
#ifdef RUN_GTEST
namespace test_hooks {

// pax_is_substr — Boyer-Moore-ish substring search primitive.
char *PaxIsSubstr(const char *data, int32_t data_len, const char *substr,
                  int32_t substr_len) {
  if (substr_len >= 4) pax_init_shift(substr, substr_len);
  return pax_is_substr(data, data_len, substr, substr_len);
}

// BufferScanSubstr<false> for PORC_VEC-like packed buffers.
void BufferScanSubstrVec(const char *data_buf, const int32_t *offsets,
                         size_t num_elems, const char *needle,
                         int32_t needle_len, uint8_t *match_bm,
                         const int32_t *elem_to_idx) {
  if (needle_len >= 4) pax_init_shift(needle, needle_len);
  BufferScanSubstr<false>(data_buf, offsets, num_elems, needle, needle_len,
                          match_bm, elem_to_idx);
}

// BufferScanSubstr<true> for PORC varlena buffers.
void BufferScanSubstrVarlena(const char *data_buf, const int32_t *offsets,
                             size_t num_elems, const char *needle,
                             int32_t needle_len, uint8_t *match_bm,
                             const int32_t *elem_to_idx) {
  if (needle_len >= 4) pax_init_shift(needle, needle_len);
  BufferScanSubstr<true>(data_buf, offsets, num_elems, needle, needle_len,
                         match_bm, elem_to_idx);
}

// EvalStrFilters — single-row predicate evaluation entry.
bool EvalStrFiltersHook(const char *s, size_t len,
                        const FastFilterDesc *filters, int n) {
  return EvalStrFilters(s, len, filters, n);
}

// BuildToastPack* / MergeToastPack* — toast pipeline.  Tests pass in a
// fresh ThreadFilterState through a small wrapper so they can inspect the
// resulting pack independently from the singleton tl_filter_state.
size_t BuildToastPackPORCVECHook(ThreadFilterState *tl, PaxColumn *column,
                                 const char *data_buf, const int32_t *offsets,
                                 size_t nrows) {
  return BuildToastPackPORCVEC(tl, column, data_buf, offsets, nrows);
}

size_t BuildToastPackPORCHook(ThreadFilterState *tl, PaxColumn *column,
                              const char *data_buf, const int32_t *offsets,
                              const int32_t *elem_to_row,
                              size_t non_null_count) {
  return BuildToastPackPORC(tl, column, data_buf, offsets, elem_to_row,
                            non_null_count);
}

void MergeToastPackScalarHook(ThreadFilterState *tl, size_t toast_count,
                              const char *needle, int32_t needle_len,
                              uint8_t *filt_bm) {
  MergeToastPackScalar(tl, toast_count, needle, needle_len, filt_bm);
}

bool DetoastAndEvalRowHook(ThreadFilterState *tl, PaxColumn *column,
                           size_t buf_pos, const FastFilterDesc *filters,
                           int n) {
  return DetoastAndEvalRow(tl, column, buf_pos, filters, n);
}

// Access the singleton thread-local state used by production paths.
ThreadFilterState *GetThreadFilterState() { return &tl_filter_state; }

// Populate the toast-pack region of a ThreadFilterState directly from caller
// arrays.  Used by tests that want to drive MergeToastPackScalar without
// going through a real PaxColumn / BuildToastPack pipeline.
void SetupToastPackForTest(ThreadFilterState *tl, const uint8_t *buf,
                           size_t buf_size, const int32_t *offsets,
                           const int32_t *elem_to_row, size_t toast_count) {
  tl->EnsureToastBuf(buf_size);
  tl->EnsureToastIndex(toast_count);
  if (buf_size > 0) std::memcpy(tl->toast_buf.data(), buf, buf_size);
  for (size_t i = 0; i < toast_count + 1; i++) tl->toast_offsets[i] = offsets[i];
  for (size_t i = 0; i < toast_count; i++)
    tl->toast_elem_to_row[i] = elem_to_row[i];
}

// Drive the production FullScan dispatcher (matches what fast filter calls
// from the eval engine).  Picks the right scalar/AVX-512 + PORC/PORCVEC
// variant internally based on runtime CPUID and the column's storage format.
void EvalColumnStrFullScanHook(PaxColumn *column, const FastFilterDesc *filters,
                               int filter_count, size_t nrows, bool is_vec,
                               uint8_t *col_bm) {
  EvalColumnStrFullScan(column, filters, filter_count, nrows, is_vec, col_bm);
}

void EvalColumnStrSelectiveHook(PaxColumn *column, const FastFilterDesc *filters,
                                int filter_count, size_t nrows, bool is_vec,
                                uint8_t *combined_bm) {
  EvalColumnStrSelective(column, filters, filter_count, nrows, is_vec,
                         combined_bm);
}

// Reset the thread-local cache to "no context known" so a test can observe
// the next cache-init from a clean slate.
void ResetThreadFilterCache() {
  tl_filter_state.ctx_id = 0;
  tl_filter_state.col_groups.clear();
  tl_filter_state.eval_order.clear();
  tl_filter_state.reject_counts.clear();
  tl_filter_state.group_count = 0;
  tl_filter_state.int_group_count = 0;
}

// Drive the cache-key check that FastFilterGroup runs on entry, returning
// true when the cache was (re)initialized for `fast_ctx`.  Used by tests to
// verify that a new PaxFastFilter — even one allocated at the same memory
// address as a recently-destroyed one — invalidates the stale cache.
bool MaybeInitCacheForCtx(FastFilterContext *fast_ctx) {
  auto *tl = &tl_filter_state;
  if (tl->ctx_id != fast_ctx->id) {
    tl->ctx_id = fast_ctx->id;
    InitColumnGroups(tl, fast_ctx);
    return true;
  }
  return false;
}

// Read-only access to cached col_groups for verification.
size_t GetCachedColGroupCount() { return tl_filter_state.col_groups.size(); }
AttrNumber GetCachedColGroupAttno(size_t i) {
  return tl_filter_state.col_groups[i].attno;
}

// Integer-column eval — type-dispatched wrappers around the templated
// EvalColumnIntFullScan / EvalColumnIntSelective in this file.  Width must
// be 1, 2, 4, or 8 to match int8/16/32/64.
void EvalColumnIntFullScanHook(int width_bytes, const void *data,
                               const uint8_t *null_bm,
                               const FastFilterDesc *filters, int filter_count,
                               bool is_vec, size_t nrows, uint8_t *col_bm) {
  switch (width_bytes) {
    case 1:
      EvalColumnIntFullScan(static_cast<const int8 *>(data), null_bm, filters,
                            filter_count, is_vec, nrows, col_bm);
      break;
    case 2:
      EvalColumnIntFullScan(static_cast<const int16 *>(data), null_bm, filters,
                            filter_count, is_vec, nrows, col_bm);
      break;
    case 4:
      EvalColumnIntFullScan(static_cast<const int32 *>(data), null_bm, filters,
                            filter_count, is_vec, nrows, col_bm);
      break;
    case 8:
      EvalColumnIntFullScan(static_cast<const int64 *>(data), null_bm, filters,
                            filter_count, is_vec, nrows, col_bm);
      break;
  }
}

void EvalColumnIntSelectiveHook(int width_bytes, const void *data,
                                const uint8_t *null_bm,
                                const FastFilterDesc *filters,
                                int filter_count, bool is_vec, size_t nrows,
                                uint8_t *combined_bm) {
  switch (width_bytes) {
    case 1:
      EvalColumnIntSelective(static_cast<const int8 *>(data), null_bm, filters,
                             filter_count, is_vec, nrows, combined_bm);
      break;
    case 2:
      EvalColumnIntSelective(static_cast<const int16 *>(data), null_bm,
                             filters, filter_count, is_vec, nrows, combined_bm);
      break;
    case 4:
      EvalColumnIntSelective(static_cast<const int32 *>(data), null_bm,
                             filters, filter_count, is_vec, nrows, combined_bm);
      break;
    case 8:
      EvalColumnIntSelective(static_cast<const int64 *>(data), null_bm,
                             filters, filter_count, is_vec, nrows, combined_bm);
      break;
  }
}

}  // namespace test_hooks
#endif  // RUN_GTEST

}  // namespace pax

#endif  // VEC_BUILD
