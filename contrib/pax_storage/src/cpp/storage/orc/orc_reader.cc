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
 * orc_reader.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/orc/orc_reader.cc
 *
 *-------------------------------------------------------------------------
 */

#include "comm/cbdb_api.h"

#include <string>
#include <utility>
#include <vector>

#include "comm/cbdb_wrappers.h"
#include "exceptions/CException.h"
#include "storage/filter/pax_filter.h"
#include "storage/filter/pax_sparse_filter.h"
#include "storage/orc/orc_defined.h"
#include "storage/orc/orc_group.h"
#include "storage/orc/porc.h"
#include "storage/pax_itemptr.h"

namespace pax {

class OrcGroupStatsProvider final : public ColumnStatsProvider {
 public:
  OrcGroupStatsProvider(const OrcFormatReader &format_reader,
                        size_t group_index)
      : format_reader_(format_reader), group_index_(group_index) {
    Assert(group_index < format_reader.GetStripeNums());
  }
  int ColumnSize() const override {
    return static_cast<int>(format_reader_.file_footer_.colinfo_size());
  }
  bool AllNull(int column_index) const override {
    return format_reader_.file_footer_.stripes(group_index_)
        .colstats(column_index)
        .allnull();
  }
  bool HasNull(int column_index) const override {
    return format_reader_.file_footer_.stripes(group_index_)
        .colstats(column_index)
        .hasnull();
  }
  uint64 NonNullRows(int column_index) const override {
    return format_reader_.file_footer_.stripes(group_index_)
        .colstats(column_index)
        .nonnullrows();
  }
  const ::pax::stats::ColumnBasicInfo &ColumnInfo(
      int column_index) const override {
    return format_reader_.file_footer_.colinfo(column_index);
  }
  const ::pax::stats::ColumnDataStats &DataStats(
      int column_index) const override {
    return format_reader_.file_footer_.stripes(group_index_)
        .colstats(column_index)
        .coldatastats();
  }

  bool HasBloomFilter(int column_index) const override {
    return format_reader_.file_footer_.stripes(group_index_)
        .colstats(column_index)
        .has_columnbfstats();
  }

  const ::pax::stats::BloomFilterBasicInfo &BloomFilterBasicInfo(
      int column_index) const override {
    return format_reader_.file_footer_.stripes(group_index_)
        .colstats(column_index)
        .bloomfilterinfo();
  }

  std::string GetBloomFilter(int column_index) const override {
    return format_reader_.file_footer_.stripes(group_index_)
        .colstats(column_index)
        .columnbfstats();
  }

 private:
  const OrcFormatReader &format_reader_;
  size_t group_index_;
};

OrcReader::OrcReader(std::unique_ptr<File> file,
                     std::unique_ptr<File> toast_file)
    : working_group_(nullptr),
      cached_group_(nullptr),
      current_group_index_(0),
      format_reader_(std::move(file), std::move(toast_file)),
      is_closed_(true) {}

std::unique_ptr<ColumnStatsProvider> OrcReader::GetGroupStatsInfo(
    size_t group_index) {
  return std::make_unique<OrcGroupStatsProvider>(format_reader_, group_index);
}

std::unique_ptr<MicroPartitionReader::Group> OrcReader::ReadGroup(
    size_t group_index) {
  std::unique_ptr<PaxColumns> pax_columns;

  Assert(group_index < GetGroupNums());

  if (use_prefetch_) {
    size_t prefetch_index = group_index + 1;
    if (prefetch_group_.group_index == static_cast<int>(group_index)) {
      pax_columns = get_prefetched_group(group_index);
    }
    // else fallthrough to fetch the current group in sync mode

    if (prefetch_index < GetGroupNums()) {
        PrefetchGroup(prefetch_index,
                      filter_ ? filter_->GetColumnProjection()
                              : std::vector<bool>{});
    }
  }
  if (!pax_columns) {
    pax_columns = format_reader_.ReadStripe(
        group_index,
        filter_ ? filter_->GetColumnProjection() : std::vector<bool>{});
  }

#ifdef ENABLE_DEBUG
  for (size_t i = 0; i < pax_columns->GetColumns(); i++) {
    auto column = (*pax_columns)[i].get();
    if (column && !column->GetBuffer().first) {
      const auto &bm = column->GetBitmap();
      // Assert(bm);
      if (bm) {
        for (size_t n = 0; n < column->GetRows(); n++) {
          Assert(!bm->Test(n));
        }
      }
    }
  }
#endif  // ENABLE_DEBUG

  std::unique_ptr<MicroPartitionReader::Group> group;
  size_t group_offset = format_reader_.GetStripeOffset(group_index);
  const std::vector<int> *proj_column_index =
      filter_ ? &filter_->GetColumnProjectionIndex() : nullptr;
  if (COLUMN_STORAGE_FORMAT_IS_VEC(pax_columns))
    group = std::make_unique<OrcVecGroup>(std::move(pax_columns), group_offset,
                                          proj_column_index);
  else
    group = std::make_unique<OrcGroup>(std::move(pax_columns), group_offset,
                                       proj_column_index);

  group->SetVisibilityMap(visibility_bitmap_);
  return group;
}

std::unique_ptr<MicroPartitionReader::Group> OrcReader::ReadGroup(
    size_t group_index, const std::vector<bool> &proj_cols) {
  Assert(group_index < GetGroupNums());

  // Read stripe with the caller-specified projection (no prefetch).
  auto pax_columns = format_reader_.ReadStripe(group_index, proj_cols);

  std::unique_ptr<MicroPartitionReader::Group> group;
  size_t group_offset = format_reader_.GetStripeOffset(group_index);

  // Pass nullptr for proj_col_index: GetColumnValue does not use it,
  // and it avoids lifetime issues with a locally-built index vector.
  if (COLUMN_STORAGE_FORMAT_IS_VEC(pax_columns))
    group = std::make_unique<OrcVecGroup>(std::move(pax_columns), group_offset,
                                          nullptr);
  else
    group = std::make_unique<OrcGroup>(std::move(pax_columns), group_offset,
                                       nullptr);

  group->SetVisibilityMap(visibility_bitmap_);
  return group;
}

size_t OrcReader::GetGroupNums() { return format_reader_.GetStripeNums(); }

size_t OrcReader::GetTupleCountsInGroup(size_t group_index) {
  return format_reader_.GetStripeNumberOfRows(group_index);
}

void OrcReader::Open(const ReaderOptions &options) {
  // Must not open twice.
  Assert(is_closed_);

  use_prefetch_ = options.use_prefetch;
  // prefetch group must not reuse buffer
  if (!use_prefetch_ && options.reused_buffer) {
    CBDB_CHECK(options.reused_buffer->IsMemTakeOver(),
               cbdb::CException::ExType::kExTypeLogicError,
               "Invalid memory owner in resued buffer");
    options.reused_buffer->BrushBackAll();
    format_reader_.SetReusedBuffer(options.reused_buffer);
  }

  filter_ = options.filter;

  format_reader_.Open();
  is_closed_ = false;

  // only referenced, owner by caller who constructed ReadOptions
  visibility_bitmap_ = options.visibility_bitmap;
}

void OrcReader::ResetCurrentReading() {
  working_group_ = nullptr;

  cached_group_ = nullptr;

  current_group_index_ = 0;
}

void OrcReader::Close() {
  if (is_closed_) {
    return;
  }

  ResetCurrentReading();
  format_reader_.Close();
  is_closed_ = true;
}

bool OrcReader::ReadTuple(TupleTableSlot *slot) {
retry_read_group:
  if (!working_group_) {
    if (current_group_index_ >= GetGroupNums()) {
      return false;
    }

    auto group_index = current_group_index_++;
    if (filter_ && filter_->SparseFilterEnabled()) {
      auto info = GetGroupStatsInfo(group_index);
      TupleDesc tupleDesc = slot->tts_tupleDescriptor;
      if (!filter_->ExecSparseFilter(*info, tupleDesc,
                     pax::PaxSparseFilter::StatisticsKind::kGroup)) {
        goto retry_read_group;
      }
    }
    working_group_ = ReadGroup(group_index);
    auto columns = working_group_->GetAllColumns().get();

    // The column number in Pax file meta could be smaller than the column
    // number in TupleSlot in case after alter table add column DDL operation
    // was done.
    CBDB_CHECK(columns->GetColumns() <=
                   static_cast<size_t>(slot->tts_tupleDescriptor->natts),
               cbdb::CException::ExType::kExTypeSchemaNotMatch,
               fmt("There are more number of column in the current file than "
                   "in TupleDesc. [in file=%lu, in desc=%d]",
                   columns->GetColumns(), slot->tts_tupleDescriptor->natts));
  }

  bool ok = false;
  size_t group_row_offset = 0;

  std::tie(ok, group_row_offset) = working_group_->ReadTuple(slot);
  if (!ok) {
    working_group_ = nullptr;
    goto retry_read_group;
  }

  SetTupleOffset(&slot->tts_tid,
                 working_group_->GetRowOffset() + group_row_offset);
  return true;
}

int OrcReader::GetTuple(TupleTableSlot *slot, size_t row_index) {
  // Fast path: the row lives in the group we already have decoded. Bitmap and
  // index scans fetch TIDs in ascending order, so consecutive rows almost
  // always fall into the same group. Check this before touching any stripe
  // metadata so the common case is a couple of comparisons per tuple.
  if (!cached_group_ || row_index < cached_group_->GetRowOffset() ||
      row_index >= (cached_group_->GetRowOffset() + cached_group_->GetRows())) {
    // Locate the containing group via the cached row-offset array. This avoids
    // the per-tuple protobuf field access that GetStripeNumberOfRows() would
    // otherwise perform on every step of the search.
    size_t group_index = format_reader_.FindStripeByRow(row_index);
    if (group_index >= format_reader_.GetStripeNums()) {
      return -1;
    }
    cached_group_ = ReadGroup(group_index);
  }

  int rc =
      cached_group_->GetTuple(slot, row_index - cached_group_->GetRowOffset());
  SetTupleOffset(&slot->tts_tid, row_index);

  Assert(rc >= 0);
  return rc;
}

void OrcReader::PrefetchGroup(size_t group_index,
                              const std::vector<bool> &proj_cols) {
  Assert(use_prefetch_);
  if (static_cast<size_t>(prefetch_group_.group_index) == group_index) return;

  prefetch_group_.future_columns = std::async(
      std::launch::async, &OrcFormatReader::ReadStripe, &format_reader_,
      group_index, proj_cols);
  prefetch_group_.group_index = static_cast<int>(group_index);
}

std::unique_ptr<PaxColumns> OrcReader::get_prefetched_group(
    size_t group_index) {
  Assert(use_prefetch_);
  if (static_cast<size_t>(prefetch_group_.group_index) != group_index) return nullptr;

  auto pax_columns = prefetch_group_.future_columns.get();
  prefetch_group_.group_index = -1;
  return pax_columns;
}

void OrcReader::clear_prefetch_status() {
  prefetch_group_.group_index = -1;
  if (!prefetch_group_.future_columns.valid()) return;
  // When the async thread starts, the caller may have no chance to get the future result,
  // so consume the future result and ignore any exception.
  // Calling this function means the prefetched result is no longer needed.
  try {
    (void)prefetch_group_.future_columns.get();
  } catch (...) {
    // ignore all exceptions
  }
}

OrcReader::~OrcReader() {
  clear_prefetch_status();
}

}  // namespace pax
