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
 * pax_vec_reader_e2e_test.cc
 *
 *   End-to-end test for the fast filter toast-pack pipeline:
 *   write a vec-format PAX file containing both toast and non-toast rows
 *   in a text column, read it back as a PaxColumn, and verify that
 *   BuildToastPackPORCVEC + MergeToastPackScalar produce the correct
 *   row-level result.  This is the read-side complement of the per-row
 *   detoast already verified in pax_vec_reader_test.cc.
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/vec/pax_vec_reader_e2e_test.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/filter/pax_fast_filter.h"

#include "comm/guc.h"
#include "comm/singleton.h"
#include "storage/columns/pax_columns.h"
#include "storage/columns/pax_vec_column.h"
#include "storage/local_file_system.h"
#include "storage/orc/orc_defined.h"
#include "storage/orc/porc.h"

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "comm/gtest_wrappers.h"
#include "pax_gtest_helper.h"

#ifdef VEC_BUILD

namespace pax {

struct ThreadFilterState;

namespace test_hooks {

size_t BuildToastPackPORCVECHook(ThreadFilterState *tl, PaxColumn *column,
                                 const char *data_buf, const int32_t *offsets,
                                 size_t nrows);

size_t BuildToastPackPORCHook(ThreadFilterState *tl, PaxColumn *column,
                              const char *data_buf, const int32_t *offsets,
                              const int32_t *elem_to_row,
                              size_t non_null_count);

void MergeToastPackScalarHook(ThreadFilterState *tl, size_t toast_count,
                              const char *needle, int32_t needle_len,
                              uint8_t *filt_bm);

void EvalColumnStrFullScanHook(PaxColumn *column,
                               const FastFilterDesc *filters,
                               int filter_count, size_t nrows, bool is_vec,
                               uint8_t *col_bm);

void EvalColumnStrSelectiveHook(PaxColumn *column,
                                const FastFilterDesc *filters,
                                int filter_count, size_t nrows, bool is_vec,
                                uint8_t *combined_bm);

ThreadFilterState *GetThreadFilterState();

}  // namespace test_hooks

}  // namespace pax

namespace pax::tests {

class PaxFastFilterE2ETest : public ::testing::Test {
 public:
  void SetUp() override {
    Singleton<LocalFileSystem>::GetInstance()->Delete(file_name_);
    CreateMemoryContext();
    CreateTestResourceOwner();

    // Snapshot original GUC values and configure toast thresholds.
    saved_min_compress_ = pax_min_size_of_compress_toast;
    saved_min_external_ = pax_min_size_of_external_toast;
    saved_enable_      = pax_enable_toast;
    pax_min_size_of_compress_toast = 64;
    pax_min_size_of_external_toast = 1 << 30;  // disable external
    pax_enable_toast = true;
  }
  void TearDown() override {
    pax_min_size_of_compress_toast = saved_min_compress_;
    pax_min_size_of_external_toast = saved_min_external_;
    pax_enable_toast               = saved_enable_;
    Singleton<LocalFileSystem>::GetInstance()->Delete(file_name_);
    ReleaseTestResourceOwner();
  }

 protected:
  std::string file_name_ = "./test_fast_filter_e2e.file";
  int saved_min_compress_ = 0;
  int saved_min_external_ = 0;
  bool saved_enable_ = false;

  // Build a 1-column text TupleDesc with attstorage=EXTENDED so PAX will
  // toast large values.
  static TupleDesc Build1TextDesc() {
    auto td = reinterpret_cast<TupleDescData *>(cbdb::Palloc0(
        sizeof(TupleDescData) + sizeof(FormData_pg_attribute)));
    td->natts = 1;
    auto *a = &td->attrs[0];
    memset(a, 0, sizeof(*a));
    a->atttypid = TEXTOID;
    a->attlen = -1;
    a->attbyval = false;
    a->attalign = TYPALIGN_INT;
    a->attstorage = TYPSTORAGE_EXTENDED;
    a->attcollation = DEFAULT_COLLATION_OID;
    return td;
  }

  // Make a varlena buffer holding `s` with a 4-byte header.
  static text *MakeText(const std::string &s) {
    size_t total = s.size() + VARHDRSZ;
    auto *t = reinterpret_cast<text *>(cbdb::Palloc0(total));
    SET_VARSIZE(t, total);
    memcpy(VARDATA(t), s.data(), s.size());
    return t;
  }

  // Write a 1-text-column file in the given storage format, then read it
  // back and return its single group + column.  Caller owns `reader` and
  // must delete it before fixture TearDown.
  void WriteAndRead(const std::vector<std::string> &values,
                    PaxStorageFormat storage_format,
                    OrcReader **out_reader,
                    std::unique_ptr<MicroPartitionReader::Group> *out_group,
                    PaxColumn **out_col) {
    TupleDesc desc = Build1TextDesc();
    TupleTableSlot *slot = MakeTupleTableSlot(desc, &TTSOpsVirtual);
    auto local_fs = Singleton<LocalFileSystem>::GetInstance();

    {
      auto file = local_fs->Open(file_name_, fs::kWriteMode);
      ASSERT_NE(file.get(), nullptr);
      std::vector<pax::porc::proto::Type_Kind> types;
      types.emplace_back(pax::porc::proto::Type_Kind::Type_Kind_STRING);
      OrcWriter::WriterOptions wo;
      wo.rel_tuple_desc = desc;
      wo.storage_format = storage_format;
      auto writer = OrcWriter::CreateWriter(wo, types, std::move(file));
      for (const auto &v : values) {
        slot->tts_values[0] = PointerGetDatum(MakeText(v));
        slot->tts_isnull[0] = false;
        writer->WriteTuple(slot);
      }
      writer->Close();
    }
    ExecDropSingleTupleTableSlot(slot);

    auto rfile = local_fs->Open(file_name_, fs::kReadMode);
    ASSERT_NE(rfile.get(), nullptr);
    auto reader = new OrcReader(std::move(rfile));
    MicroPartitionReader::ReaderOptions ro;
    reader->Open(ro);
    ASSERT_EQ(reader->GetGroupNums(), 1U);
    auto group = reader->ReadGroup(0);
    auto &cols = group->GetAllColumns();
    ASSERT_EQ(cols->GetColumns(), 1U);
    *out_col = (*cols)[0].get();
    ASSERT_NE(*out_col, nullptr);
    *out_group = std::move(group);
    *out_reader = reader;
  }

  static bool Bit(const uint8_t *bm, size_t row) {
    return (bm[row >> 3] & static_cast<uint8_t>(1U << (row & 7))) != 0;
  }
};

TEST_F(PaxFastFilterE2ETest, BuildToastPackFromVecColumn) {
  // Write 5 rows; rows 1 and 3 are toast (size > 64 bytes), rest are short.
  TupleDesc desc = Build1TextDesc();
  TupleTableSlot *slot = MakeTupleTableSlot(desc, &TTSOpsVirtual);

  const std::vector<std::string> values = {
      "short_a",                                            // row 0  - plain
      std::string(200, 'A') + "FOO" + std::string(50, 'B'), // row 1  - toast
      "short_b",                                            // row 2  - plain
      std::string(150, 'X') + "BAR" + std::string(150, 'Y'),// row 3  - toast
      "short_c_FOO",                                        // row 4  - plain, contains "FOO"
  };

  auto local_fs = Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(local_fs, nullptr);

  // Write phase.
  {
    auto file = local_fs->Open(file_name_, fs::kWriteMode);
    ASSERT_NE(file.get(), nullptr);

    std::vector<pax::porc::proto::Type_Kind> types;
    types.emplace_back(pax::porc::proto::Type_Kind::Type_Kind_STRING);

    OrcWriter::WriterOptions wo;
    wo.rel_tuple_desc = desc;
    wo.storage_format = PaxStorageFormat::kTypeStoragePorcVec;

    auto writer = OrcWriter::CreateWriter(wo, types, std::move(file));

    for (const auto &v : values) {
      slot->tts_values[0] = PointerGetDatum(MakeText(v));
      slot->tts_isnull[0] = false;
      writer->WriteTuple(slot);
    }
    writer->Close();
  }

  // Read phase.
  auto file = local_fs->Open(file_name_, fs::kReadMode);
  ASSERT_NE(file.get(), nullptr);

  auto reader = new OrcReader(std::move(file));
  MicroPartitionReader::ReaderOptions ro;
  reader->Open(ro);

  ASSERT_EQ(reader->GetGroupNums(), 1U);
  auto group = reader->ReadGroup(0);
  auto &columns = group->GetAllColumns();
  ASSERT_EQ(columns->GetColumns(), 1U);

  auto *col = (*columns)[0].get();
  ASSERT_NE(col, nullptr);

  // Sanity: rows 1 and 3 must be flagged as toast; rows 0,2,4 must not.
  ASSERT_TRUE (col->IsToast(1));
  ASSERT_TRUE (col->IsToast(3));
  ASSERT_FALSE(col->IsToast(0));
  ASSERT_FALSE(col->IsToast(2));
  ASSERT_FALSE(col->IsToast(4));
  ASSERT_EQ(col->ToastCounts(), 2U);

  // Build the toast pack.
  auto *vc = static_cast<PaxVecNonFixedColumn *>(col);
  auto [data_buf, _data_size] = col->GetBuffer();
  auto [off_buf, _off_size] = vc->GetOffsetBuffer(false);
  const int32_t *offsets = reinterpret_cast<const int32_t *>(off_buf);

  auto *tl = test_hooks::GetThreadFilterState();
  size_t toast_count = test_hooks::BuildToastPackPORCVECHook(
      tl, col, data_buf, offsets, values.size());
  ASSERT_EQ(toast_count, 2U);

  // Pass-1 filt_bm: assume the scan over the raw column buffer produced
  // garbage for toast rows.  Start with all 5 bits set; emulate that row 4
  // (which contains "FOO" in the body) is correctly marked, and rows 1 / 3
  // are wrongly marked by Pass 1 because the toast pointer header bytes may
  // have spuriously matched.  We start everything set so the test focuses on
  // whether Merge correctly overrides the toast bits with the truth.
  uint8_t filt_bm[1] = {0x1F};  // bits 0..4 set

  test_hooks::MergeToastPackScalarHook(tl, toast_count, "FOO", 3, filt_bm);

  // Expected:
  //   row 0 ("short_a")       : non-toast, untouched → 1  (Pass 1 set it; this
  //                              is just the simulated start state; not under
  //                              test here)
  //   row 1 (toast contains FOO): cleared then re-set by Pass 2 → 1
  //   row 2 ("short_b")       : non-toast, untouched → 1
  //   row 3 (toast no FOO)    : cleared by Pass 2 (BAR), not re-set → 0
  //   row 4 ("short_c_FOO")   : non-toast, untouched → 1
  auto Bit = [&](size_t r) {
    return (filt_bm[r >> 3] & static_cast<uint8_t>(1U << (r & 7))) != 0;
  };
  EXPECT_TRUE (Bit(0));
  EXPECT_TRUE (Bit(1));
  EXPECT_TRUE (Bit(2));
  EXPECT_FALSE(Bit(3));
  EXPECT_TRUE (Bit(4));

  delete reader;
  ExecDropSingleTupleTableSlot(slot);
}

// ---------------------------------------------------------------------------
// LIKE / NOT LIKE driven through the production FullScan dispatcher.
//
// These two tests cover the toast-fix integration point: the dispatcher
// (EvalColumnStrFullScan) picks between scalar/AVX-512 and PORC/PORCVEC
// variants, each of which now calls BuildToastPackPORCVEC + MergeToastPack*
// to handle toast rows correctly.  We feed a mixed toast/non-toast column
// and verify col_bm matches what `LIKE '%FOO%'` should produce on the real
// (detoasted) string values.
// ---------------------------------------------------------------------------

TEST_F(PaxFastFilterE2ETest, LikeContainsOnToastVecColumn) {
  const std::vector<std::string> values = {
      "short_a",                                              // row 0
      std::string(200, 'A') + "FOO" + std::string(50, 'B'),   // row 1 (toast +)
      "short_b",                                              // row 2
      std::string(150, 'X') + "BAR" + std::string(150, 'Y'),  // row 3 (toast -)
      "short_c_FOO",                                          // row 4
  };

  OrcReader *reader = nullptr;
  std::unique_ptr<MicroPartitionReader::Group> group;
  PaxColumn *col = nullptr;
  WriteAndRead(values, PaxStorageFormat::kTypeStoragePorcVec, &reader, &group,
               &col);

  FastFilterDesc fd{};
  fd.attno = 1;
  fd.op = FastFilterOp::kStrContains;
  fd.type_len = -1;
  char needle[] = "FOO";
  fd.str_const = needle;
  fd.str_const_len = 3;

  uint8_t col_bm[1] = {0};
  test_hooks::EvalColumnStrFullScanHook(col, &fd, 1, values.size(),
                                        /*is_vec=*/true, col_bm);

  // Expected: rows that REALLY contain "FOO" → bit=1; others → bit=0.
  // Row 0 "short_a" -> no; row 1 toast contains "FOO" -> yes; row 2 -> no;
  // row 3 toast does NOT contain "FOO" -> no; row 4 "short_c_FOO" -> yes.
  EXPECT_FALSE(Bit(col_bm, 0));
  EXPECT_TRUE (Bit(col_bm, 1));
  EXPECT_FALSE(Bit(col_bm, 2));
  EXPECT_FALSE(Bit(col_bm, 3));
  EXPECT_TRUE (Bit(col_bm, 4));

  delete reader;
}

TEST_F(PaxFastFilterE2ETest, NotLikeOnToastVecColumn) {
  const std::vector<std::string> values = {
      "short_a",                                              // row 0
      std::string(200, 'A') + "FOO" + std::string(50, 'B'),   // row 1 (toast +)
      "short_b",                                              // row 2
      std::string(150, 'X') + "BAR" + std::string(150, 'Y'),  // row 3 (toast -)
      "short_c_FOO",                                          // row 4
  };

  OrcReader *reader = nullptr;
  std::unique_ptr<MicroPartitionReader::Group> group;
  PaxColumn *col = nullptr;
  WriteAndRead(values, PaxStorageFormat::kTypeStoragePorcVec, &reader, &group,
               &col);

  FastFilterDesc fd{};
  fd.attno = 1;
  fd.op = FastFilterOp::kStrNotContains;
  fd.type_len = -1;
  char needle[] = "FOO";
  fd.str_const = needle;
  fd.str_const_len = 3;

  uint8_t col_bm[1] = {0};
  test_hooks::EvalColumnStrFullScanHook(col, &fd, 1, values.size(),
                                        /*is_vec=*/true, col_bm);

  // Expected: NOT-contains semantics — exactly the inverse of LIKE.
  EXPECT_TRUE (Bit(col_bm, 0));
  EXPECT_FALSE(Bit(col_bm, 1));
  EXPECT_TRUE (Bit(col_bm, 2));
  EXPECT_TRUE (Bit(col_bm, 3));
  EXPECT_FALSE(Bit(col_bm, 4));

  delete reader;
}

// ---------------------------------------------------------------------------
// PORC (non-vec) variant — covers BuildToastPackPORC, which has a different
// indexing scheme: IsToast(elem_index) instead of IsToast(row), with an
// explicit elem_to_row mapping that skips nulls.
// ---------------------------------------------------------------------------

TEST_F(PaxFastFilterE2ETest, BuildToastPackFromPorcColumn) {
  const std::vector<std::string> values = {
      "short_a",                                              // row 0
      std::string(200, 'A') + "FOO" + std::string(50, 'B'),   // row 1 (toast +)
      "short_b",                                              // row 2
      std::string(150, 'X') + "BAR" + std::string(150, 'Y'),  // row 3 (toast -)
      "short_c",                                              // row 4
  };

  OrcReader *reader = nullptr;
  std::unique_ptr<MicroPartitionReader::Group> group;
  PaxColumn *col = nullptr;
  WriteAndRead(values, PaxStorageFormat::kTypeStoragePorcNonVec, &reader,
               &group, &col);

  // For PORC (non-vec), 5 rows with no nulls → elem_to_row is identity.
  std::vector<int32_t> elem_to_row{0, 1, 2, 3, 4};

  auto [data_buf, _data_size] = col->GetBuffer();
  auto *nf_col = static_cast<PaxNonFixedColumn *>(col);
  auto [off_buf, _off_size] = nf_col->GetOffsetBuffer(false);
  const int32_t *offsets = reinterpret_cast<const int32_t *>(off_buf);

  auto *tl = test_hooks::GetThreadFilterState();
  size_t toast_count = test_hooks::BuildToastPackPORCHook(
      tl, col, data_buf, offsets, elem_to_row.data(), values.size());
  ASSERT_EQ(toast_count, 2U);

  // Pre-load filt_bm with all rows passing (simulating Pass-1 over the raw
  // varlena buffer).  Pass-2 should reset toast rows to their correct value.
  uint8_t filt_bm[1] = {0x1F};
  test_hooks::MergeToastPackScalarHook(tl, toast_count, "FOO", 3, filt_bm);

  EXPECT_TRUE (Bit(filt_bm, 0));  // non-toast, untouched
  EXPECT_TRUE (Bit(filt_bm, 1));  // toast with FOO, Pass-2 re-set
  EXPECT_TRUE (Bit(filt_bm, 2));
  EXPECT_FALSE(Bit(filt_bm, 3));  // toast without FOO, Pass-2 cleared
  EXPECT_TRUE (Bit(filt_bm, 4));

  delete reader;
}

// ---------------------------------------------------------------------------
// Selective scan path — covers DetoastAndEvalRow.  In the production
// interleaved phase, fast filter calls EvalColumnStrSelective when rejection
// rate is high; it walks combined_bm and clears bits whose row fails the
// filter.  Toast rows previously stayed in (conservatively-pass bug); the
// fix now runs DetoastAndEvalRow on them.
// ---------------------------------------------------------------------------

TEST_F(PaxFastFilterE2ETest, SelectiveToastPath) {
  const std::vector<std::string> values = {
      "short_a",                                              // row 0 (-)
      std::string(200, 'A') + "FOO" + std::string(50, 'B'),   // row 1 (toast +)
      "short_b_FOO",                                          // row 2 (+)
      std::string(150, 'X') + "BAR" + std::string(150, 'Y'),  // row 3 (toast -)
      "short_c",                                              // row 4 (-)
  };

  OrcReader *reader = nullptr;
  std::unique_ptr<MicroPartitionReader::Group> group;
  PaxColumn *col = nullptr;
  WriteAndRead(values, PaxStorageFormat::kTypeStoragePorcVec, &reader, &group,
               &col);

  FastFilterDesc fd{};
  fd.attno = 1;
  fd.op = FastFilterOp::kStrContains;
  fd.type_len = -1;
  char needle[] = "FOO";
  fd.str_const = needle;
  fd.str_const_len = 3;

  // Pre-set combined_bm: all 5 rows marked as Pass-1 survivors.  Selective
  // must KEEP only those whose value really contains "FOO".
  uint8_t combined_bm[1] = {0x1F};
  test_hooks::EvalColumnStrSelectiveHook(col, &fd, 1, values.size(),
                                         /*is_vec=*/true, combined_bm);

  EXPECT_FALSE(Bit(combined_bm, 0));
  EXPECT_TRUE (Bit(combined_bm, 1));  // toast row, real content has FOO
  EXPECT_TRUE (Bit(combined_bm, 2));
  EXPECT_FALSE(Bit(combined_bm, 3));  // toast row, real content has no FOO
  EXPECT_FALSE(Bit(combined_bm, 4));

  delete reader;
}

}  // namespace pax::tests

#endif  // VEC_BUILD
