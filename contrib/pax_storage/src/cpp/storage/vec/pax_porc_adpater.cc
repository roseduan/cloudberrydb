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
 * pax_porc_adpater.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/vec/pax_porc_adpater.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/vec/pax_vec_adapter.h"

#ifdef VEC_BUILD

#include <stdlib.h>

#include "comm/vec_numeric.h"
#include "storage/columns/pax_column_traits.h"
#include "storage/toast/pax_toast.h"
#include "storage/vec/pax_vec_comm.h"
#ifdef BUILD_RB_RET_DICT
#include "storage/columns/pax_dict_encoding.h"  // GetRawDictionary
#endif
namespace pax {

static inline struct varlena *VarlenaShortTo4B(struct varlena *attr) {
  Assert(attr != nullptr);
  Assert(VARATT_IS_SHORT(attr));
  Size data_size = VARSIZE_SHORT(attr) - VARHDRSZ_SHORT;
  Size new_size = data_size + VARHDRSZ;

  struct varlena *new_attr =
      reinterpret_cast<struct varlena *>(malloc(new_size));

  Assert(new_attr != nullptr);

  SET_VARSIZE(new_attr, new_size);
  memcpy(VARDATA(new_attr), VARDATA_SHORT(attr), data_size);
  return new_attr;
}

// The copy functions below come in two forms: one copies every row of the
// range, the other ("Selected") only the visible rows `sel`, given as offsets
// from `range_begin` in increasing order.  The visible rows are worked out
// once per batch (VisibleRowOffsets), so a filter that keeps few rows costs
// each column a pass over those rows rather than over the whole range.
//
// PORC stores only the non-null values of a column, so the value of row `row`
// is the n-th one, n being the number of non-null rows in [range_begin, row);
// the selected forms get n from a SetBitCounter on the null bitmap.

static void CopyFixedRawBufferWithNull(PaxColumn *column, size_t range_begin,
                                       size_t range_lens,
                                       size_t data_index_begin,
                                       size_t data_range_lens,
                                       DataBuffer<char> *out_data_buffer) {
  char *buffer;
  size_t buffer_len;

  std::tie(buffer, buffer_len) =
      column->GetRangeBuffer(data_index_begin, data_range_lens);

  const auto &null_bitmap = column->GetBitmap();
  size_t non_null_offset = 0;
  size_t type_len = column->GetTypeLength();
  for (size_t i = range_begin; i < (range_begin + range_lens); i++) {
    if (null_bitmap->Test(i)) {
      out_data_buffer->Write(buffer + non_null_offset, type_len);
      non_null_offset += type_len;
    }
    out_data_buffer->Brush(type_len);
  }
}

static inline void CopyFixedRawBuffer(char *buffer, size_t len,
                                      DataBuffer<char> *data_buffer) {
  data_buffer->Write(buffer, len);
  data_buffer->Brush(len);
}

static void CopyFixedBuffer(PaxColumn *column, size_t range_begin,
                            size_t range_lens, size_t data_index_begin,
                            size_t data_range_lens,
                            DataBuffer<char> *out_data_buffer) {
  if (column->HasNull()) {
    CopyFixedRawBufferWithNull(column, range_begin, range_lens,
                               data_index_begin, data_range_lens,
                               out_data_buffer);
  } else {
    char *buffer;
    size_t buffer_len;
    std::tie(buffer, buffer_len) =
        column->GetRangeBuffer(data_index_begin, data_range_lens);
    CopyFixedRawBuffer(buffer, buffer_len, out_data_buffer);
  }
}

static void CopySelectedFixedBuffer(PaxColumn *column, size_t range_begin,
                                    const uint32 *sel, size_t sel_count,
                                    size_t data_index_begin,
                                    size_t data_range_lens,
                                    DataBuffer<char> *out_data_buffer) {
  char *buffer;
  size_t buffer_len;
  std::tie(buffer, buffer_len) =
      column->GetRangeBuffer(data_index_begin, data_range_lens);

  size_t type_len = column->GetTypeLength();
  Assert(out_data_buffer->Available() >= sel_count * type_len);
  char *dst = out_data_buffer->GetAvailableBuffer();

  if (!column->HasNull()) {
    GatherFixedValues(buffer, type_len, sel, sel_count, dst);
  } else {
    // A null row keeps an unset slot, as in CopyFixedRawBufferWithNull.
    const auto &null_bitmap = column->GetBitmap();
    SetBitCounter non_nulls(*null_bitmap, range_begin);
    for (size_t k = 0; k < sel_count; k++) {
      size_t row = range_begin + sel[k];
      if (null_bitmap->Test(row)) {
        size_t n = non_nulls.CountTo(row);
        Assert(n < data_range_lens);
        memcpy(dst + k * type_len, buffer + n * type_len, type_len);
      }
    }
  }
  out_data_buffer->Brush(sel_count * type_len);
}

// Appends the non-null value `data_index` of `column`, which is row `row`.
static void AppendNonFixedValue(PaxColumn *column, size_t row,
                                size_t data_index,
                                DataBuffer<int32> *offset_buffer,
                                DataBuffer<char> *out_data_buffer,
                                bool is_bpchar, size_t *dst_offset) {
  char *buffer;
  size_t buffer_len;
  std::tie(buffer, buffer_len) = column->GetBuffer(data_index);

  // deal toast
  if (column->IsToast(row)) {
    auto et_buffer = column->GetExternalToastDataBuffer();

#ifdef USE_ASSERT_CHECKING
    auto toast_raw_size = pax_toast_raw_size(PointerGetDatum(buffer));
    Assert(toast_raw_size <= out_data_buffer->Available());
#endif

    auto decompress_size = pax_detoast_raw(
        PointerGetDatum(buffer), out_data_buffer->GetAvailableBuffer(),
        out_data_buffer->Available(), et_buffer ? et_buffer->Start() : nullptr,
        et_buffer ? et_buffer->Used() : 0);
    out_data_buffer->Brush(decompress_size);

    offset_buffer->Write(*dst_offset);
    offset_buffer->Brush(sizeof(int32));

    *dst_offset += decompress_size;
  } else {
    size_t read_len = 0;
    char *read_data;
    VarlenaToRawBuffer(buffer, buffer_len, &read_data, &read_len);

    // In vec, bpchar not allow store empty char after the actual characters
    if (is_bpchar) {
      read_len = bpchartruelen(read_data, read_len);
    }

    out_data_buffer->Write(read_data, read_len);
    out_data_buffer->Brush(read_len);

    offset_buffer->Write(*dst_offset);
    offset_buffer->Brush(sizeof(int32));

    *dst_offset += read_len;
  }
}

static void CopyNonFixedBuffer(PaxColumn *column, size_t range_begin,
                               size_t range_lens, size_t data_index_begin,
                               size_t data_range_lens,
                               DataBuffer<int32> *offset_buffer,
                               DataBuffer<char> *out_data_buffer,
                               bool is_bpchar) {
  size_t dst_offset = out_data_buffer->Used();
  size_t non_null_offset = 0;

  // null_bitmap: 0 represents null, 1 represents non-null
  const auto &null_bitmap = column->GetBitmap();

  for (size_t i = range_begin; i < (range_begin + range_lens); i++) {
    if (null_bitmap && !null_bitmap->Test(i)) {
      offset_buffer->Write(dst_offset);
      offset_buffer->Brush(sizeof(int32));
    } else {
      AppendNonFixedValue(column, i, data_index_begin + non_null_offset,
                          offset_buffer, out_data_buffer, is_bpchar,
                          &dst_offset);
      non_null_offset++;
    }
  }

  offset_buffer->Write(dst_offset);
  offset_buffer->Brush(sizeof(int32));

  CBDB_CHECK(non_null_offset == data_range_lens,
             cbdb::CException::ExType::kExTypeOutOfRange,
             fmt("The data range len may invalid or logic error here [end "
                 "read offset=%lu, range len=%lu]",
                 non_null_offset, data_range_lens));
}

static void CopySelectedNonFixedBuffer(
    PaxColumn *column, size_t range_begin, const uint32 *sel, size_t sel_count,
    size_t data_index_begin, size_t data_range_lens,
    DataBuffer<int32> *offset_buffer, DataBuffer<char> *out_data_buffer,
    bool is_bpchar) {
  size_t dst_offset = out_data_buffer->Used();

  const auto &null_bitmap = column->GetBitmap();
  if (null_bitmap) {
    SetBitCounter non_nulls(*null_bitmap, range_begin);
    for (size_t k = 0; k < sel_count; k++) {
      size_t row = range_begin + sel[k];
      if (!null_bitmap->Test(row)) {
        offset_buffer->Write(dst_offset);
        offset_buffer->Brush(sizeof(int32));
        continue;
      }
      size_t n = non_nulls.CountTo(row);
      Assert(n < data_range_lens);
      AppendNonFixedValue(column, row, data_index_begin + n, offset_buffer,
                          out_data_buffer, is_bpchar, &dst_offset);
    }
  } else {
    for (size_t k = 0; k < sel_count; k++) {
      Assert(sel[k] < data_range_lens);
      AppendNonFixedValue(column, range_begin + sel[k],
                          data_index_begin + sel[k], offset_buffer,
                          out_data_buffer, is_bpchar, &dst_offset);
    }
  }

  offset_buffer->Write(dst_offset);
  offset_buffer->Brush(sizeof(int32));
}

// Converts the non-null numeric value `data_index` of `column` into the
// 16-byte vec numeric at `dest_buff`.
static void ConvertDecimalValue(PaxColumn *column, size_t data_index,
                                char *dest_buff) {
  Numeric numeric;
  bool should_free = false;
  size_t num_len = 0;
  char *buffer;
  size_t buffer_len;
  std::tie(buffer, buffer_len) = column->GetBuffer(data_index);

  auto vl = (struct varlena *)DatumGetPointer(buffer);
  Assert(!(VARATT_IS_EXTERNAL(vl) || VARATT_IS_COMPRESSED(vl)));
  num_len = VARSIZE_ANY_EXHDR(vl);
  // it has been detoasted in OrcWriter::PrepareWriteTuple, except numeric
  // type with short header should be detoasted to 4B header
  if (unlikely(VARATT_IS_SHORT(vl))) {
    numeric = (Numeric)VarlenaShortTo4B(vl);
    should_free = true;
  } else {  // direct cast
    numeric = (Numeric)(buffer);
  }

  pg_short_numeric_to_vec_short_numeric(numeric, num_len, (int64 *)dest_buff,
                                        (int64 *)(dest_buff + sizeof(int64)));

  if (should_free) {
    free(numeric);
  }
}

static void CopyDecimalBuffer(PaxColumn *column, size_t range_begin,
                              size_t range_lens, size_t data_index_begin,
                              size_t data_range_lens,
                              DataBuffer<char> *out_data_buffer) {
  size_t non_null_offset = 0;
  int32 type_len = VEC_SHORT_NUMERIC_STORE_BYTES;

  const auto &null_bitmap = column->GetBitmap();

  for (size_t i = range_begin; i < (range_begin + range_lens); i++) {
    if (null_bitmap && !null_bitmap->Test(i)) {
      out_data_buffer->Brush(type_len);
    } else {
      Assert(out_data_buffer->Available() >= (size_t)type_len);
      ConvertDecimalValue(column, data_index_begin + non_null_offset,
                          out_data_buffer->GetAvailableBuffer());
      out_data_buffer->Brush(type_len);
      non_null_offset++;
    }
  }

  CBDB_CHECK(non_null_offset == data_range_lens,
             cbdb::CException::ExType::kExTypeOutOfRange,
             fmt("The data range len may invalid or logic error here [end "
                 "read offset=%lu, range len=%lu]",
                 non_null_offset, data_range_lens));
}

static void CopySelectedDecimalBuffer(PaxColumn *column, size_t range_begin,
                                      const uint32 *sel, size_t sel_count,
                                      size_t data_index_begin,
                                      size_t data_range_lens,
                                      DataBuffer<char> *out_data_buffer) {
  const size_t type_len = VEC_SHORT_NUMERIC_STORE_BYTES;
  Assert(out_data_buffer->Available() >= sel_count * type_len);
  char *dst = out_data_buffer->GetAvailableBuffer();

  const auto &null_bitmap = column->GetBitmap();
  if (null_bitmap) {
    SetBitCounter non_nulls(*null_bitmap, range_begin);
    for (size_t k = 0; k < sel_count; k++) {
      size_t row = range_begin + sel[k];
      if (!null_bitmap->Test(row)) continue;
      size_t n = non_nulls.CountTo(row);
      Assert(n < data_range_lens);
      ConvertDecimalValue(column, data_index_begin + n, dst + k * type_len);
    }
  } else {
    for (size_t k = 0; k < sel_count; k++) {
      Assert(sel[k] < data_range_lens);
      ConvertDecimalValue(column, data_index_begin + sel[k],
                          dst + k * type_len);
    }
  }
  out_data_buffer->Brush(sel_count * type_len);
}

static void CopyBitPackedBuffer(PaxColumn *column, size_t range_begin,
                                size_t range_lens, size_t data_index_begin,
                                size_t data_range_lens,
                                Bitmap8 *out_data_buffer) {
  char *buffer;
  size_t buffer_len;
  std::tie(buffer, buffer_len) =
      column->GetRangeBuffer(data_index_begin, data_range_lens);

  size_t non_null_offset = 0;
  size_t type_len = column->GetTypeLength();
  const auto &null_bitmap = column->GetBitmap();
  bool has_null = column->HasNull();
  AssertImply(has_null, null_bitmap != nullptr);

  for (size_t i = 0; i < range_lens; i++) {
    bool is_null = has_null && !null_bitmap->Test(range_begin + i);
    if (!is_null) {
      bool value = *(bool *)(buffer + non_null_offset);
      if (value) {
        out_data_buffer->Set(i);
      }
      non_null_offset += type_len;
    }
  }
}

static void CopySelectedBitPackedBuffer(PaxColumn *column, size_t range_begin,
                                        const uint32 *sel, size_t sel_count,
                                        size_t data_index_begin,
                                        size_t data_range_lens,
                                        Bitmap8 *out_data_buffer) {
  char *buffer;
  size_t buffer_len;
  std::tie(buffer, buffer_len) =
      column->GetRangeBuffer(data_index_begin, data_range_lens);

  size_t type_len = column->GetTypeLength();
  const auto &null_bitmap = column->GetBitmap();
  bool has_null = column->HasNull();
  AssertImply(has_null, null_bitmap != nullptr);

  if (has_null) {
    SetBitCounter non_nulls(*null_bitmap, range_begin);
    for (size_t k = 0; k < sel_count; k++) {
      size_t row = range_begin + sel[k];
      if (!null_bitmap->Test(row)) continue;
      size_t n = non_nulls.CountTo(row);
      Assert(n < data_range_lens);
      if (*(bool *)(buffer + n * type_len)) out_data_buffer->Set(k);
    }
  } else {
    for (size_t k = 0; k < sel_count; k++) {
      if (*(bool *)(buffer + sel[k] * type_len)) out_data_buffer->Set(k);
    }
  }
}

static size_t CalcRecordBatchDataBufferSize(PaxColumn *column,
                                            size_t range_buffer_len,
                                            size_t num_of_not_nulls) {
  size_t toast_counts;
  size_t raw_data_size;
  Assert(column);

  toast_counts = column->ToastCounts();
  if (toast_counts > 0) {
    char *toast_buff;
    size_t toast_buff_size;
    auto toast_indexes = column->GetToastIndexes();
    int64 toast_total_size = 0;
    for (size_t i = 0; i < toast_indexes->GetSize(); i++) {
      auto toast_index = (*toast_indexes)[i];
      std::tie(toast_buff, toast_buff_size) =
          column->GetBuffer(column->GetRangeNonNullRows(0, toast_index));
      toast_total_size += pax_toast_raw_size(PointerGetDatum(toast_buff));
      toast_total_size -= pax_toast_hdr_size(PointerGetDatum(toast_buff));
    }

    raw_data_size = range_buffer_len -
                    ((num_of_not_nulls - toast_counts) * VARHDRSZ_SHORT) +
                    toast_total_size;
  } else {
    raw_data_size = range_buffer_len - (num_of_not_nulls * VARHDRSZ_SHORT);
  }
  return TYPEALIGN(MEMORY_ALIGN_SIZE, raw_data_size);
}

std::pair<size_t, size_t> VecAdapter::AppendPorcFormat(PaxColumns *columns,
                                                       size_t range_begin,
                                                       size_t range_lens) {
  size_t out_range_lens;
  // visible rows as offsets from `range_begin`; nullptr when all are visible
  const uint32 *sel = nullptr;

  // recompute `range_lens`, if remain data LT `max_batch_size_`
  // then should reduce `range_lens`
  if ((range_begin + range_lens) > columns->GetRows()) {
    range_lens = columns->GetRows() - range_begin;
  }

  if (micro_partition_visibility_bitmap_) {
    visible_rows_.resize(range_lens);
    out_range_lens = VisibleRowOffsets(*micro_partition_visibility_bitmap_,
                                       group_base_offset_ + range_begin,
                                       range_lens, visible_rows_.data());
    if (out_range_lens < range_lens) sel = visible_rows_.data();
  } else {
    out_range_lens = range_lens;
  }

  if (out_range_lens == 0) {
    return std::make_pair(0, range_begin + range_lens);
  }

  for (size_t index = 0; index < columns->GetColumns(); index++) {
    size_t data_index_begin = 0;
    size_t num_of_not_nulls = 0;
    DataBuffer<char> *vec_buffer = nullptr;
    DataBuffer<int32> *offset_buffer = nullptr;

    char *raw_buffer = nullptr;
    size_t buffer_len = 0;
    PaxColumnTypeInMem column_type;

    auto column = (*columns)[index].get();
    if (column == nullptr) {
      continue;
    }

    Assert(index < (size_t)vec_cache_buffer_lens_ && vec_cache_buffer_);

    data_index_begin = column->GetRangeNonNullRows(0, range_begin);
    num_of_not_nulls = column->GetRangeNonNullRows(range_begin, range_lens);

    // data buffer holder
    vec_buffer = &(vec_cache_buffer_[index].vec_buffer);
    offset_buffer = &(vec_cache_buffer_[index].offset_buffer);

    // copy null bitmap
    vec_cache_buffer_[index].null_counts = 0;
    if (sel == nullptr) {
      CopyBitmapBuffer(column, range_begin, range_lens, num_of_not_nulls,
                       &(vec_cache_buffer_[index].null_bits_buffer),
                       &(vec_cache_buffer_[index].null_counts));
    } else if (column->HasNull()) {
      CopySelectedBitmapBuffer(column, range_begin, sel, out_range_lens,
                               &(vec_cache_buffer_[index].null_bits_buffer),
                               &(vec_cache_buffer_[index].null_counts));
    }

    // copy data
    std::tie(raw_buffer, buffer_len) =
        column->GetRangeBuffer(data_index_begin, num_of_not_nulls);
    column_type = column->GetPaxColumnTypeInMem();

#ifdef BUILD_RB_RET_DICT
    if (column->GetEncodingType() ==
        ColumnEncoding_Kind::ColumnEncoding_Kind_DICTIONARY) {
      CBDB_CHECK(micro_partition_visibility_bitmap_ == nullptr,
                 cbdb::CException::ExType::kExTypeInvalid,
                 fmt("Invalid column [index=%lu], Can't enable RB_RET_DICT "
                     "with visibility bitmap update",
                     index));

      CBDB_CHECK(range_lens == columns->GetRows(),
                 cbdb::CException::ExType::kExTypeInvalid,
                 fmt("Invalid column [index=%lu], When enable RB_RET_DICT "
                     "should make vector.max_batch_size LE [group size=%lu]",
                     index, columns->GetRows()));

      std::shared_ptr<DataBuffer<int32>> index_buffer;
      std::shared_ptr<DataBuffer<char>> entry_buffer;
      std::shared_ptr<DataBuffer<int32>> desc_buffer;
      bool expect_hdr = false;

      char *undecoded_buffer;
      size_t undecoded_buffer_len;
      auto undecoded_data_buffer =
          std::dynamic_pointer_cast<PaxNonFixedEncodingColumn>(column)
              ->GetUndecodedBuffer();
      auto out_data_buffer_len =
          TYPEALIGN(MEMORY_ALIGN_SIZE, (range_lens * sizeof(int32)));
      std::tie(index_buffer, entry_buffer, desc_buffer) =
          PaxDictDecoder::GetRawDictionary(undecoded_data_buffer);
      undecoded_buffer = (char *)index_buffer->GetBuffer();
      undecoded_buffer_len = index_buffer->Used();
      vec_buffer->Set(BlockBuffer::Alloc<char>(out_data_buffer_len),
                      out_data_buffer_len);

      vec_cache_buffer_[index].is_dict = true;
      if (column->HasNull()) {
        const auto &null_bitmap = column->GetBitmap();
        size_t non_null_offset = 0;

        for (size_t i = 0; i < range_lens; i++) {
          if (null_bitmap->Test(i)) {
            vec_buffer->Write(undecoded_buffer + non_null_offset,
                              sizeof(int32));
            non_null_offset += sizeof(int32);
          }
          vec_buffer->Brush(sizeof(int32));
        }
      } else {
        CopyFixedRawBuffer(undecoded_buffer, undecoded_buffer_len, vec_buffer);
      }

      expect_hdr = rel_tuple_desc_->attrs[index].attlen == -1 &&
                   rel_tuple_desc_->attrs[index].attbyval == false;

#ifdef RUN_GTEST
      expect_hdr = false;
#endif

      // the dict part
      auto desc_offset_buffer = &(vec_cache_buffer_[index].dict_offset_buffer);
      auto desc_entry_buffer = &(vec_cache_buffer_[index].dict_entry_buffer);
      {
        auto desc_offset_buffer_len =
            TYPEALIGN(MEMORY_ALIGN_SIZE, desc_buffer->Used() + sizeof(int32));
        desc_offset_buffer->Set(
            BlockBuffer::Alloc<char>(desc_offset_buffer_len),
            desc_offset_buffer_len);

        if (!expect_hdr) {
          size_t dst_offset = 0;
          desc_offset_buffer->Write(dst_offset);
          desc_offset_buffer->Brush(sizeof(int32));

          for (size_t l = 0; l < desc_buffer->GetSize(); l++) {
            dst_offset += (*desc_buffer)[l];
            desc_offset_buffer->Write(dst_offset);
            desc_offset_buffer->Brush(sizeof(int32));
          }

          auto entry_buffer_len =
              TYPEALIGN(MEMORY_ALIGN_SIZE, entry_buffer->Used());
          desc_entry_buffer->Set(BlockBuffer::Alloc<char>(entry_buffer_len),
                                 entry_buffer_len);

          desc_entry_buffer->Write(entry_buffer->GetBuffer(),
                                   entry_buffer->Used());
          desc_entry_buffer->Brush(entry_buffer->Used());
        } else {
          auto entry_buffer_len =
              TYPEALIGN(MEMORY_ALIGN_SIZE, entry_buffer->Used());
          desc_entry_buffer->Set(BlockBuffer::Alloc<char>(entry_buffer_len),
                                 entry_buffer_len);

          size_t dst_offset = 0;
          size_t raw_dst_offset = 0;
          for (size_t l = 0; l < desc_buffer->GetSize(); l++) {
            char *raw_buffer = NULL;
            size_t raw_buffer_len = 0;

            VarlenaToRawBuffer(entry_buffer->GetBuffer() + dst_offset,
                               (*desc_buffer)[l], &raw_buffer, &raw_buffer_len);
            desc_entry_buffer->Write(raw_buffer, raw_buffer_len);
            desc_entry_buffer->Brush(raw_buffer_len);
            desc_offset_buffer->Write(raw_dst_offset);
            desc_offset_buffer->Brush(sizeof(int32));

            dst_offset += (*desc_buffer)[l];
            raw_dst_offset += raw_buffer_len;
          }

          desc_offset_buffer->Write(raw_dst_offset);
          desc_offset_buffer->Brush(sizeof(int32));
        }
      }

      continue;
    }
#endif

    switch (column_type) {
      case PaxColumnTypeInMem::kTypeDecimal: {
        // notice that: current arrow require the 16 width numeric return
        auto align_size =
            TYPEALIGN(MEMORY_ALIGN_SIZE,
                      (out_range_lens * VEC_SHORT_NUMERIC_STORE_BYTES));
        Assert(!vec_buffer->GetBuffer());

        vec_buffer->Set(BlockBuffer::Alloc<char>(align_size), align_size);

        if (sel == nullptr) {
          CopyDecimalBuffer(column, range_begin, range_lens, data_index_begin,
                            num_of_not_nulls, vec_buffer);
        } else {
          CopySelectedDecimalBuffer(column, range_begin, sel, out_range_lens,
                                    data_index_begin, num_of_not_nulls,
                                    vec_buffer);
        }
        break;
      }
      case PaxColumnTypeInMem::kTypeBpChar:
      case PaxColumnTypeInMem::kTypeNonFixed: {
        auto data_align_size =
            CalcRecordBatchDataBufferSize(column, buffer_len, num_of_not_nulls);
        auto offset_align_bytes =
            TYPEALIGN(MEMORY_ALIGN_SIZE, (out_range_lens + 1) * sizeof(int32));

        Assert(!vec_buffer->GetBuffer() && !offset_buffer->GetBuffer());
        vec_buffer->Set(BlockBuffer::Alloc<char>(data_align_size),
                        data_align_size);
        offset_buffer->Set(BlockBuffer::Alloc<char>(offset_align_bytes),
                           offset_align_bytes);

        bool is_bpchar = column_type == PaxColumnTypeInMem::kTypeBpChar;
        if (sel == nullptr) {
          CopyNonFixedBuffer(column, range_begin, range_lens, data_index_begin,
                             num_of_not_nulls, offset_buffer, vec_buffer,
                             is_bpchar);
        } else {
          CopySelectedNonFixedBuffer(column, range_begin, sel, out_range_lens,
                                     data_index_begin, num_of_not_nulls,
                                     offset_buffer, vec_buffer, is_bpchar);
        }

        break;
      }
      case PaxColumnTypeInMem::kTypeFixed: {
        Assert(column->GetTypeLength() > 0);
        auto align_size = TYPEALIGN(MEMORY_ALIGN_SIZE,
                                    (out_range_lens * column->GetTypeLength()));
        Assert(!vec_buffer->GetBuffer());

        vec_buffer->Set(BlockBuffer::Alloc<char>(align_size), align_size);
        if (sel == nullptr) {
          CopyFixedBuffer(column, range_begin, range_lens, data_index_begin,
                          num_of_not_nulls, vec_buffer);
        } else {
          CopySelectedFixedBuffer(column, range_begin, sel, out_range_lens,
                                  data_index_begin, num_of_not_nulls,
                                  vec_buffer);
        }

        break;
      }
      case PaxColumnTypeInMem::kTypeBitPacked: {
        auto align_size =
            TYPEALIGN(MEMORY_ALIGN_SIZE, BITS_TO_BYTES(out_range_lens));
        Assert(!vec_buffer->GetBuffer());
        // the boolean_buffer is bitpacked-layout, we must use Alloc0 to fill it
        // with zeros. then we can only set the bit according to the index of
        // true value.
        auto boolean_buffer = BlockBuffer::Alloc0<char>(align_size);
        vec_buffer->Set(boolean_buffer, align_size);

        Bitmap8 vec_bool_bitmap(
            BitmapRaw<uint8>((uint8 *)(boolean_buffer), align_size));

        if (sel == nullptr) {
          CopyBitPackedBuffer(column, range_begin, range_lens,
                              data_index_begin, num_of_not_nulls,
                              &vec_bool_bitmap);
        } else {
          CopySelectedBitPackedBuffer(column, range_begin, sel, out_range_lens,
                                      data_index_begin, num_of_not_nulls,
                                      &vec_bool_bitmap);
        }
        break;
      }
      default: {
        CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError,
                   fmt("Invalid column [type=%d], PORC format won't create "
                       "this type of column.",
                       column_type));
      }
    }  // switch column type
  }

  return std::make_pair(out_range_lens, range_begin + range_lens);
}

}  // namespace pax

#endif  // VEC_BUILD
