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
 * pax_scanner.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/access/pax_scanner.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "comm/cbdb_api.h"

#include <unordered_set>

#include "comm/pax_memory.h"
#include "storage/filter/pax_filter.h"
#include "storage/pax.h"
#ifdef VEC_BUILD
#include "storage/vec/pax_vec_adapter.h"
#endif

namespace paxc {
bool IndexUniqueCheck(Relation rel, ItemPointer tid, Snapshot snapshot,
                      bool *all_dead);
}

namespace pax {
class PaxIndexScanDesc final {
 public:
  explicit PaxIndexScanDesc(
      Relation rel, std::shared_ptr<PaxFilter> filter = nullptr,
      std::shared_ptr<PaxFilter> filter_recheck = nullptr);
  ~PaxIndexScanDesc();
  // -1: EOF the offset is out of range
  // 0: invisible
  // 1: visible
  int FetchTuple(ItemPointer tid, Snapshot snapshot, TupleTableSlot *slot,
                 bool *call_again, bool *all_dead);

  // Select which projection the following fetches use: exact bitmap pages
  // (recheck == false) decode only the no-recheck columns; lossy pages
  // (recheck == true) additionally decode the bitmapqualorig columns needed
  // by the recheck qual. The two projections are served by two independent
  // readers so a lossy and an exact page of the same micro-partition never
  // share a reader opened with the wrong projection.
  inline void SetRecheck(bool recheck) { which_ = recheck ? 1 : 0; }

  // release internal reader
  void Release();
  inline IndexFetchTableData *ToBase() { return &base_; }
  inline Relation GetRelation() { return base_.rel; }
  static inline PaxIndexScanDesc *FromBase(IndexFetchTableData *base) {
    return reinterpret_cast<PaxIndexScanDesc *>(base);
  }

 private:
  bool OpenMicroPartition(BlockNumber block, Snapshot snapshot);

  // Two independent readers, one per projection: [0] = no-recheck (exact
  // pages), [1] = recheck (lossy pages). Each keeps its own open
  // micro-partition and cached group, so switching per page is free and the
  // recheck column is guaranteed decoded on the reader that needs it.
  IndexFetchTableData base_;
  std::unique_ptr<MicroPartitionReader> reader_[2];
  std::string rel_path_;
  BlockNumber current_block_[2] = {InvalidBlockNumber, InvalidBlockNumber};
  // Column projections; null means read all columns. filter_[0] is the
  // no-recheck projection (targetlist u qual), filter_[1] additionally
  // covers bitmapqualorig. Shared with the owning PaxScanDesc.
  std::shared_ptr<PaxFilter> filter_[2];
  // Which projection/reader the current block uses (0 = no-recheck).
  int which_ = 0;
};

class PaxScanDesc {
 public:
  PaxScanDesc() = default;
  TableScanDesc BeginScan(Relation relation, Snapshot snapshot, int nkeys,
                          struct ScanKeyData *key, ParallelTableScanDesc pscan,
                          uint32 flags, std::shared_ptr<PaxFilter> &&pax_filter,
                          bool build_bitmap);

  TableScanDesc BeginScanExtractColumnsBM(Relation rel, Snapshot snapshot,
                                          List *targetlist, List *qual,
                                          List *bitmapqualorig, uint32 flags);

  TableScanDesc BeginScanExtractColumns(Relation rel, Snapshot snapshot,
                                        int nkeys, struct ScanKeyData *key,
                                        ParallelTableScanDesc parallel_scan,
                                        struct PlanState *ps, uint32 flags);

  void EndScan();
  void ReScan(ScanKey key, bool set_params, bool allow_strat, bool allow_sync,
              bool allow_pagemode);

  bool GetNextSlot(TupleTableSlot *slot);

  bool ScanAnalyzeNextBlock(BlockNumber blockno,
                            BufferAccessStrategy bstrategy);
  bool ScanAnalyzeNextTuple(TransactionId oldest_xmin, double *liverows,
                            double *deadrows, TupleTableSlot *slot);

  bool ScanSampleNextBlock(SampleScanState *scanstate);

  bool ScanSampleNextTuple(SampleScanState *scanstate, TupleTableSlot *slot);

  bool BitmapNextBlock(struct TBMIterateResult *tbmres);
  bool BitmapNextTuple(struct TBMIterateResult *tbmres, TupleTableSlot *slot);

  ~PaxScanDesc();

  static inline PaxScanDesc *ToDesc(TableScanDesc scan) {
    auto desc = reinterpret_cast<PaxScanDesc *>(scan);
    return desc;
  }

  inline Relation GetRelation() { return rs_base_.rs_rd; }

 private:
  TableScanDescData rs_base_{};

  std::unique_ptr<TableReader> reader_;

  std::shared_ptr<DataBuffer<char>> reused_buffer_;

  MemoryContext memory_context_ = nullptr;

  // Only used by `scan analyze` and `scan sample`
  uint64 next_tuple_id_ = 0;
  // Only used by `scan analyze`
  uint64 prev_target_tuple_id_ = 0;
  // Only used by `scan analyze`
  uint64 target_tuple_id_ = 0;
  // Only used by `scan sample`
  uint64 fetch_tuple_id_ = 0;
  uint64 total_tuples_ = 0;

  // filter used to do column projection (no-recheck: targetlist u qual)
  std::shared_ptr<PaxFilter> filter_ = nullptr;
  // recheck projection for bitmap scans: filter_ plus the bitmapqualorig
  // columns, used on lossy pages that re-evaluate the recheck qual.
  std::shared_ptr<PaxFilter> filter_recheck_ = nullptr;
#ifdef VEC_BUILD
  std::unique_ptr<VecAdapter> vec_adapter_;
#endif

  // used only by bitmap index scan
  std::unique_ptr<PaxIndexScanDesc> index_desc_;
  int cindex_ = 0;
};  // class PaxScanDesc

}  // namespace pax
