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
 * pax_vec_comm.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/vec/pax_vec_comm.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#ifdef VEC_BUILD

#include "comm/bitmap.h"
#include "storage/columns/pax_column.h"
#include "storage/pax_buffer.h"
#include "storage/pax_defined.h"

namespace pax {

void CopyBitmap(const Bitmap8 *bitmap, size_t range_begin, size_t range_lens,
                DataBuffer<char> *null_bits_buffer);

void VarlenaToRawBuffer(char *buffer, size_t buffer_len, char **out_data,
                        size_t *out_len);

static_assert(__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__,
              "LoadBits64 reads bitmap bytes as a little-endian word");

// The 64 bits of `bitmap` from bit `index` on: bit 0 of the result is bit
// `index`.  Bits past the end of the bitmap read as 0, the same as
// Bitmap8::Test() reports them.
static inline uint64 LoadBits64(const Bitmap8 &bitmap, size_t index) {
  const auto &raw = bitmap.Raw();
  size_t byte = index >> 3;
  if (byte >= raw.size) return 0;

  size_t avail = raw.size - byte;
  uint64 word = 0;
  if (avail >= 8) {
    memcpy(&word, raw.bitmap + byte, 8);
  } else {
    memcpy(&word, raw.bitmap + byte, avail);
  }
  size_t shift = index & 7;
  if (shift != 0) {
    word >>= shift;
    if (avail > 8)
      word |= static_cast<uint64>(raw.bitmap[byte + 8]) << (64 - shift);
  }
  return word;
}

// Writes to `out` the offsets, relative to `begin`, of the rows in
// [begin, begin + count) whose bit in `visibility` is clear (the visible
// rows), in increasing order, and returns how many there are.  `out` needs
// room for `count` entries.
static inline size_t VisibleRowOffsets(const Bitmap8 &visibility, size_t begin,
                                       size_t count, uint32 *out) {
  size_t n = 0;
  for (size_t base = 0; base < count; base += 64) {
    uint64 word = ~LoadBits64(visibility, begin + base);
    if (count - base < 64) word &= (1ULL << (count - base)) - 1;
    while (word != 0) {
      out[n++] = static_cast<uint32>(base + __builtin_ctzll(word));
      word &= word - 1;
    }
  }
  return n;
}

// Counts the set bits of a bitmap from a start bit up to a row that only
// moves forward, so walking the visible rows of a range costs one popcount
// per 64 rows passed.  On a column's null bitmap (bit set = not null) the
// count is the row's position among the non-null values that PORC stores.
class SetBitCounter final {
 public:
  SetBitCounter(const Bitmap8 &bitmap, size_t start)
      : bitmap_(bitmap), pos_(start) {}

  // The number of set bits in [start, row).  `row` must not be less than in
  // the previous call.
  inline size_t CountTo(size_t row) {
    Assert(row >= pos_);
    while (row - pos_ >= 64) {
      count_ += __builtin_popcountll(LoadBits64(bitmap_, pos_));
      pos_ += 64;
    }
    if (row > pos_) {
      uint64 mask = (1ULL << (row - pos_)) - 1;
      count_ += __builtin_popcountll(LoadBits64(bitmap_, pos_) & mask);
      pos_ = row;
    }
    return count_;
  }

 private:
  const Bitmap8 &bitmap_;
  size_t pos_;
  size_t count_ = 0;
};

// Copies the fixed-width values at row offsets `sel` of `src` to `dst`, back
// to back.
template <size_t kTypeLen>
static inline void GatherFixedValues(const char *src, const uint32 *sel,
                                     size_t sel_count, char *dst) {
  for (size_t k = 0; k < sel_count; k++) {
    memcpy(dst + k * kTypeLen, src + static_cast<size_t>(sel[k]) * kTypeLen,
           kTypeLen);
  }
}

static inline void GatherFixedValues(const char *src, size_t type_len,
                                     const uint32 *sel, size_t sel_count,
                                     char *dst) {
  switch (type_len) {
    case 1:
      GatherFixedValues<1>(src, sel, sel_count, dst);
      break;
    case 2:
      GatherFixedValues<2>(src, sel, sel_count, dst);
      break;
    case 4:
      GatherFixedValues<4>(src, sel, sel_count, dst);
      break;
    case 8:
      GatherFixedValues<8>(src, sel, sel_count, dst);
      break;
    case 16:
      GatherFixedValues<16>(src, sel, sel_count, dst);
      break;
    default:
      for (size_t k = 0; k < sel_count; k++) {
        memcpy(dst + k * type_len,
               src + static_cast<size_t>(sel[k]) * type_len, type_len);
      }
      break;
  }
}

// Null bitmap of the visible rows `sel` (offsets from `range_begin`) of
// `column`, for a column that has nulls.
void CopySelectedBitmapBuffer(PaxColumn *column, size_t range_begin,
                              const uint32 *sel, size_t sel_count,
                              DataBuffer<char> *null_bits_buffer,
                              size_t *out_visible_null_counts);

// Null bitmap of rows [range_begin, range_begin + range_lens) of `column`,
// `data_range_lens` of which are not null.
void CopyBitmapBuffer(PaxColumn *column, size_t range_begin, size_t range_lens,
                      size_t data_range_lens,
                      DataBuffer<char> *null_bits_buffer,
                      size_t *out_null_counts);

}  // namespace pax
#endif  // VEC_BUILD
