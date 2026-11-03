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
 * pax_porc_vec_adpater.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/vec/pax_porc_vec_adpater.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/vec/pax_vec_adapter.h"

#ifdef VEC_BUILD

#include "comm/vec_numeric.h"
#include "storage/columns/pax_column_traits.h"
#include "storage/orc/orc_type.h"
#include "storage/toast/pax_toast.h"
#include "storage/vec/pax_vec_comm.h"

namespace pax {

template <typename T>
static std::pair<bool, size_t> ColumnTransMemory(PaxColumn *column) {
  Assert(column->GetStorageFormat() == PaxStorageFormat::kTypeStoragePorcVec);

  auto vec_column = static_cast<T*>(column);
  auto data_buffer = vec_column->GetDataBuffer();
  auto data_cap = data_buffer->Capacity();

  if (data_buffer->IsMemTakeOver() && data_cap != 0) {
    Assert(data_cap % MEMORY_ALIGN_SIZE == 0);

    data_buffer->SetMemTakeOver(false);
    return {true, data_cap};
  }

  return {false, 0};
}

// The copy functions below take the rows to copy as offsets `sel` (in
// increasing order); a null `sel` means every one of the `count` rows.
// PORC_VEC keeps a slot for every row, nulls included, so row `r` is value
// `r` of the column.

static void CopyNonFixedBuffer(PaxVecNonFixedColumn *column, const uint32 *sel,
                               size_t count,
                               DataBuffer<int32> *out_offset_buffer,
                               DataBuffer<char> *out_data_buffer) {
  char *buffer;
  size_t buffer_len;
  char *offset_buffer_raw = nullptr;
  size_t offset_buffer_len = 0;
  std::tie(buffer, buffer_len) = column->GetBuffer();
  std::tie(offset_buffer_raw, offset_buffer_len) = column->GetOffsetBuffer(false);

  auto const typlen = sizeof(int32);
  auto offset_array = reinterpret_cast<int32 *>(offset_buffer_raw);

  bool has_toast = column->ToastCounts() > 0;
  auto et_buffer = has_toast ? column->GetExternalToastDataBuffer() : nullptr;

  // Pass 1: Build offset buffer and calculate total data size.
  // For toast rows, use pax_toast_raw_size to get the detoasted size.
  auto visible_len = TYPEALIGN(MEMORY_ALIGN_SIZE, typlen * (count + 1));
  out_offset_buffer->Set(BlockBuffer::Alloc<char>(visible_len), visible_len);

  int32 adjust_offset = 0;
  for (size_t k = 0; k < count; k++) {
    size_t i = sel ? sel[k] : k;
    out_offset_buffer->Write(&adjust_offset, typlen);
    out_offset_buffer->Brush(typlen);

    auto value_size = offset_array[i + 1] - offset_array[i];
    if (value_size > 0 && has_toast && column->IsToast(i)) {
      adjust_offset += pax_toast_raw_size(
          PointerGetDatum(&buffer[offset_array[i]]));
    } else {
      adjust_offset += value_size;
    }
  }
  out_offset_buffer->Write(&adjust_offset, typlen);
  out_offset_buffer->Brush(typlen);

  // Pass 2: Copy data buffer, detoasting toast rows.
  auto data_len = TYPEALIGN(MEMORY_ALIGN_SIZE, adjust_offset);
  out_data_buffer->Set(BlockBuffer::Alloc<char>(data_len), data_len);

  for (size_t k = 0; k < count; k++) {
    size_t i = sel ? sel[k] : k;
    auto value_size = offset_array[i + 1] - offset_array[i];
    if (value_size > 0) {
      if (has_toast && column->IsToast(i)) {
        auto decompress_size = pax_detoast_raw(
            PointerGetDatum(&buffer[offset_array[i]]),
            out_data_buffer->GetAvailableBuffer(),
            out_data_buffer->Available(),
            et_buffer ? et_buffer->Start() : nullptr,
            et_buffer ? et_buffer->Used() : 0);
        out_data_buffer->Brush(decompress_size);
      } else {
        out_data_buffer->Write(&buffer[offset_array[i]], value_size);
        out_data_buffer->Brush(value_size);
      }
    }
  }
}

static void CopySelectedFixedBuffer(PaxColumn *column, const uint32 *sel,
                                    size_t sel_count,
                                    DataBuffer<char> *out_data_buffer) {
  char *buffer;
  size_t pg_attribute_unused() buffer_len;
  std::tie(buffer, buffer_len) = column->GetBuffer();

  const size_t typlen = column->GetTypeLength();
  Assert(buffer_len >= (sel_count ? (sel[sel_count - 1] + 1) * typlen : 0));
  auto visible_len = TYPEALIGN(MEMORY_ALIGN_SIZE, sel_count * typlen);
  out_data_buffer->Set(BlockBuffer::Alloc<char>(visible_len), visible_len);

  GatherFixedValues(buffer, typlen, sel, sel_count,
                    out_data_buffer->GetAvailableBuffer());
  out_data_buffer->Brush(sel_count * typlen);
}

// Bool columns keep one bit per row.
static void CopySelectedBitPackedBuffer(PaxColumn *column, const uint32 *sel,
                                        size_t sel_count,
                                        DataBuffer<char> *out_data_buffer) {
  char *buffer;
  size_t pg_attribute_unused() buffer_len;
  std::tie(buffer, buffer_len) = column->GetBuffer();
  Assert(buffer_len * 8 >= (sel_count ? sel[sel_count - 1] + 1 : 0));

  auto in = reinterpret_cast<const uint8 *>(buffer);
  auto visible_len = TYPEALIGN(MEMORY_ALIGN_SIZE, BITS_TO_BYTES(sel_count));
  out_data_buffer->Set(BlockBuffer::Alloc0<char>(visible_len), visible_len);

  auto out = reinterpret_cast<uint8 *>(out_data_buffer->GetBuffer());
  for (size_t k = 0; k < sel_count; k++) {
    uint32 r = sel[k];
    if (in[r >> 3] & (1U << (r & 7))) out[k >> 3] |= 1U << (k & 7);
  }
  out_data_buffer->Brush(BITS_TO_BYTES(sel_count));
}

std::pair<size_t, size_t> VecAdapter::AppendPorcVecFormat(PaxColumns *columns) {
  size_t total_rows = columns->GetRows();
  size_t visible_rows = total_rows;
  // visible rows as offsets in the group; nullptr when all are visible, in
  // which case the column buffers can be handed over whole
  const uint32 *sel = nullptr;

  if (micro_partition_visibility_bitmap_) {
    visible_rows_.resize(total_rows);
    visible_rows = VisibleRowOffsets(*micro_partition_visibility_bitmap_,
                                     group_base_offset_, total_rows,
                                     visible_rows_.data());
    if (visible_rows < total_rows) sel = visible_rows_.data();
  }

  if (visible_rows == 0) {
    return {0, total_rows};
  }

  for (size_t index = 0; index < columns->GetColumns(); index++) {
    auto column = (*columns)[index].get();
    if (column == nullptr) {
      continue;
    }

    DataBuffer<char> *vec_buffer = &(vec_cache_buffer_[index].vec_buffer);
    DataBuffer<int32> *offset_buffer =
        &(vec_cache_buffer_[index].offset_buffer);

    Assert(index < (size_t)vec_cache_buffer_lens_ && vec_cache_buffer_);

    char *buffer = nullptr;
    size_t buffer_len = 0;
    bool trans_succ = false;
    size_t cap_len = 0;

    vec_cache_buffer_[index].null_counts = 0;
    if (sel == nullptr) {
      CopyBitmapBuffer(column, 0, total_rows,
                       column->GetRangeNonNullRows(0, total_rows),
                       &(vec_cache_buffer_[index].null_bits_buffer),
                       &(vec_cache_buffer_[index].null_counts));
    } else if (column->HasNull()) {
      CopySelectedBitmapBuffer(column, 0, sel, visible_rows,
                               &(vec_cache_buffer_[index].null_bits_buffer),
                               &(vec_cache_buffer_[index].null_counts));
    }

    switch (column->GetPaxColumnTypeInMem()) {
      case PaxColumnTypeInMem::kTypeVecBpChar:
      case PaxColumnTypeInMem::kTypeVecNoHeader:
      case PaxColumnTypeInMem::kTypeNonFixed: {
        Assert(!vec_buffer->GetBuffer());
        Assert(!offset_buffer->GetBuffer());

        // When there are invisible rows or toast data, we must do a
        // per-row copy.  Toast entries are small pointers that expand
        // into full-size data after detoasting, so zero-copy is not
        // possible when toast is present.
        if (sel != nullptr || column->ToastCounts() > 0) {
          CopyNonFixedBuffer(dynamic_cast<PaxVecNonFixedColumn*>(column), sel,
                             visible_rows, offset_buffer, vec_buffer);

          break;
        }

        std::tie(buffer, buffer_len) = column->GetBuffer();
        std::tie(trans_succ, cap_len) =
            ColumnTransMemory<PaxVecNonFixedColumn>(column);

        if (trans_succ) {
          vec_buffer->Set(buffer, cap_len);
          vec_buffer->BrushAll();
        } else {
          vec_buffer->Set(BlockBuffer::Alloc<char>(
                              TYPEALIGN(MEMORY_ALIGN_SIZE, buffer_len)),
                          TYPEALIGN(MEMORY_ALIGN_SIZE, buffer_len));
          vec_buffer->Write(buffer, buffer_len);
          vec_buffer->BrushAll();
        }

        std::tie(buffer, buffer_len) =
            static_cast<PaxVecNonFixedColumn*>(column)->GetOffsetBuffer(false);
        // TODO(jiaqizho): this buffer can also be transferred
        offset_buffer->Set(BlockBuffer::Alloc<char>(
                               TYPEALIGN(MEMORY_ALIGN_SIZE, buffer_len)),
                           TYPEALIGN(MEMORY_ALIGN_SIZE, buffer_len));
        offset_buffer->Write((int *)buffer, buffer_len);
        offset_buffer->BrushAll();
        break;
      }
      case PaxColumnTypeInMem::kTypeVecDecimal: {
        Assert(!vec_buffer->GetBuffer());
        if (sel != nullptr) {
          CopySelectedFixedBuffer(column, sel, visible_rows, vec_buffer);
          break;
        }

        std::tie(buffer, buffer_len) = column->GetBuffer();
        std::tie(trans_succ, cap_len) =
            ColumnTransMemory<PaxShortNumericColumn>(column);

        if (trans_succ) {
          vec_buffer->Set(buffer, cap_len);
          vec_buffer->BrushAll();
        } else {
          vec_buffer->Set(
              (char *)BlockBuffer::Alloc0(TYPEALIGN(MEMORY_ALIGN_SIZE, buffer_len)),
              TYPEALIGN(MEMORY_ALIGN_SIZE, buffer_len));
          vec_buffer->Write(buffer, buffer_len);
          vec_buffer->BrushAll();
        }
        break;
      }
      case PaxColumnTypeInMem::kTypeVecBitPacked:
      case PaxColumnTypeInMem::kTypeFixed: {
        Assert(!vec_buffer->GetBuffer());

        if (sel != nullptr) {
          if (column->GetPaxColumnTypeInMem() ==
              PaxColumnTypeInMem::kTypeVecBitPacked) {
            CopySelectedBitPackedBuffer(column, sel, visible_rows, vec_buffer);
          } else {
            CopySelectedFixedBuffer(column, sel, visible_rows, vec_buffer);
          }
          break;
        }

        std::tie(buffer, buffer_len) = column->GetBuffer();

        switch (column->GetTypeLength()) {
          case 1:
            std::tie(trans_succ, cap_len) =
                ColumnTransMemory<PaxVecCommColumn<int8>>(column);
            break;
          case 2:
            std::tie(trans_succ, cap_len) =
                ColumnTransMemory<PaxVecCommColumn<int16>>(column);
            break;
          case 4:
            std::tie(trans_succ, cap_len) =
                ColumnTransMemory<PaxVecCommColumn<int32>>(column);
            break;
          case 8:
            std::tie(trans_succ, cap_len) =
                ColumnTransMemory<PaxVecCommColumn<int64>>(column);
            break;
          default:
            Assert(false);
        }

        if (trans_succ) {
          vec_buffer->Set(buffer, cap_len);
          vec_buffer->BrushAll();
        } else {
          auto align_size = TYPEALIGN(MEMORY_ALIGN_SIZE, buffer_len);

          vec_buffer->Set(BlockBuffer::Alloc<char>(align_size), align_size);
          if (column->GetPaxColumnTypeInMem() ==
              PaxColumnTypeInMem::kTypeVecBitPacked) {
            memset(vec_buffer->Start(), 0, align_size);
          }
          vec_buffer->Write(buffer, buffer_len);
          vec_buffer->BrushAll();
        }
        break;
      }
      default: {
        CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError,
                   fmt("Invalid column [type=%d], PORC_VEC format won't create "
                       "this type of column.",
                       column->GetPaxColumnTypeInMem()));
      }
    }
  }

  return std::make_pair(visible_rows, total_rows);
}

}  // namespace pax

#endif  // VEC_BUILD
