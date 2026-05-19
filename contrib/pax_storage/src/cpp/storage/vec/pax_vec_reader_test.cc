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
 * pax_vec_reader_test.cc
 *
 *   Unit tests for substring-scan primitives and the toast-pack pipeline
 *   used by the PAX fast filter string evaluation path.  Production helpers
 *   live as file-local statics inside pax_vec_reader.cc and are exposed
 *   here via the pax::test_hooks namespace (compiled in only with
 *   -DRUN_GTEST).
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/vec/pax_vec_reader_test.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/filter/pax_fast_filter.h"

#include <cstdint>
#include <cstring>
#include <vector>

#include "comm/gtest_wrappers.h"
#include "pax_gtest_helper.h"

#ifdef VEC_BUILD

namespace pax {

// Forward declaration of the file-local ThreadFilterState in pax_vec_reader.cc;
// tests only hold a pointer to it returned by test_hooks::GetThreadFilterState.
struct ThreadFilterState;

namespace test_hooks {

// Mirrors the declarations at the bottom of pax_vec_reader.cc under RUN_GTEST.
char *PaxIsSubstr(const char *data, int32_t data_len, const char *substr,
                  int32_t substr_len);

void BufferScanSubstrVec(const char *data_buf, const int32_t *offsets,
                         size_t num_elems, const char *needle,
                         int32_t needle_len, uint8_t *match_bm,
                         const int32_t *elem_to_idx);

void BufferScanSubstrVarlena(const char *data_buf, const int32_t *offsets,
                             size_t num_elems, const char *needle,
                             int32_t needle_len, uint8_t *match_bm,
                             const int32_t *elem_to_idx);

bool EvalStrFiltersHook(const char *s, size_t len,
                        const FastFilterDesc *filters, int n);

void MergeToastPackScalarHook(ThreadFilterState *tl, size_t toast_count,
                              const char *needle, int32_t needle_len,
                              uint8_t *filt_bm);

ThreadFilterState *GetThreadFilterState();

void SetupToastPackForTest(ThreadFilterState *tl, const uint8_t *buf,
                           size_t buf_size, const int32_t *offsets,
                           const int32_t *elem_to_row, size_t toast_count);

void ResetThreadFilterCache();
bool MaybeInitCacheForCtx(FastFilterContext *fast_ctx);
size_t GetCachedColGroupCount();
AttrNumber GetCachedColGroupAttno(size_t i);

void EvalColumnIntFullScanHook(int width_bytes, const void *data,
                               const uint8_t *null_bm,
                               const FastFilterDesc *filters, int filter_count,
                               bool is_vec, size_t nrows, uint8_t *col_bm);
void EvalColumnIntSelectiveHook(int width_bytes, const void *data,
                                const uint8_t *null_bm,
                                const FastFilterDesc *filters,
                                int filter_count, bool is_vec, size_t nrows,
                                uint8_t *combined_bm);

}  // namespace test_hooks

}  // namespace pax

namespace pax::tests {

class PaxVecReaderTest : public ::testing::Test {
 protected:
  void SetUp() override {
    CreateMemoryContext();
    CreateTestResourceOwner();
  }
  void TearDown() override { ReleaseTestResourceOwner(); }

  static bool Bit(const uint8_t *bm, size_t row) {
    return (bm[row >> 3] & static_cast<uint8_t>(1U << (row & 7))) != 0;
  }
  static void SetBit(uint8_t *bm, size_t row) {
    bm[row >> 3] |= static_cast<uint8_t>(1U << (row & 7));
  }
};

// ---------------------------------------------------------------------------
// pax_is_substr — substring search primitive.
// ---------------------------------------------------------------------------

TEST_F(PaxVecReaderTest, IsSubstrFindsMiddleMatch) {
  const char data[] = "hello world foo bar";
  char *m = test_hooks::PaxIsSubstr(data, sizeof(data) - 1, "world", 5);
  ASSERT_NE(m, nullptr);
  EXPECT_EQ(m, data + 6);
}

TEST_F(PaxVecReaderTest, IsSubstrNoMatchReturnsNull) {
  const char data[] = "hello world";
  char *m = test_hooks::PaxIsSubstr(data, sizeof(data) - 1, "zzzz", 4);
  EXPECT_EQ(m, nullptr);
}

TEST_F(PaxVecReaderTest, IsSubstrSingleByteNeedle) {
  const char data[] = "abcdefg";
  char *m = test_hooks::PaxIsSubstr(data, sizeof(data) - 1, "e", 1);
  ASSERT_NE(m, nullptr);
  EXPECT_EQ(*m, 'e');
}

TEST_F(PaxVecReaderTest, IsSubstrTwoByteNeedle) {
  const char data[] = "aabbccd";
  char *m = test_hooks::PaxIsSubstr(data, sizeof(data) - 1, "cc", 2);
  ASSERT_NE(m, nullptr);
  EXPECT_EQ(m, data + 4);
}

TEST_F(PaxVecReaderTest, IsSubstrThreeByteNeedle) {
  const char data[] = "xxabcdxxx";
  char *m = test_hooks::PaxIsSubstr(data, sizeof(data) - 1, "abc", 3);
  ASSERT_NE(m, nullptr);
  EXPECT_EQ(m, data + 2);
}

TEST_F(PaxVecReaderTest, IsSubstrShortDataReturnsNull) {
  const char data[] = "abc";
  char *m = test_hooks::PaxIsSubstr(data, 3, "abcdef", 6);
  EXPECT_EQ(m, nullptr);
}

TEST_F(PaxVecReaderTest, IsSubstrEmptyNeedleReturnsData) {
  const char data[] = "abc";
  char *m = test_hooks::PaxIsSubstr(data, 3, "", 0);
  EXPECT_EQ(m, data);
}

// ---------------------------------------------------------------------------
// BufferScanSubstr<false> — PORC_VEC packed (no varlena header).
// ---------------------------------------------------------------------------

// Build a packed PORC_VEC-like buffer + offsets array from a list of strings.
struct PackedStrings {
  std::vector<char> data;
  std::vector<int32_t> offsets;
  size_t count() const { return offsets.size() - 1; }
};

static PackedStrings Pack(std::initializer_list<const char *> strs) {
  PackedStrings p;
  p.offsets.push_back(0);
  for (const char *s : strs) {
    size_t n = std::strlen(s);
    p.data.insert(p.data.end(), s, s + n);
    p.offsets.push_back(static_cast<int32_t>(p.data.size()));
  }
  return p;
}

TEST_F(PaxVecReaderTest, BufferScanVecMarksMatchingElems) {
  // Elements: "alpha", "beta", "gamma", "delta", "epsilon"
  // Needle:   "et"  → matches "beta" (offset 3) and not the others.
  // Wait — "beta" contains "et" at offset 1. None of the others.
  PackedStrings p = Pack({"alpha", "beta", "gamma", "delta", "epsilon"});
  uint8_t match_bm[1] = {0};
  test_hooks::BufferScanSubstrVec(p.data.data(), p.offsets.data(),
                                   p.count(), "et", 2, match_bm, nullptr);
  EXPECT_FALSE(Bit(match_bm, 0));  // alpha
  EXPECT_TRUE (Bit(match_bm, 1));  // beta
  EXPECT_FALSE(Bit(match_bm, 2));  // gamma
  EXPECT_FALSE(Bit(match_bm, 3));  // delta
  EXPECT_FALSE(Bit(match_bm, 4));  // epsilon
}

TEST_F(PaxVecReaderTest, BufferScanVecLongerNeedle) {
  PackedStrings p = Pack({"abcdef", "12345foobar", "barbaz", "foo"});
  uint8_t match_bm[1] = {0};
  test_hooks::BufferScanSubstrVec(p.data.data(), p.offsets.data(),
                                   p.count(), "foobar", 6, match_bm, nullptr);
  EXPECT_FALSE(Bit(match_bm, 0));
  EXPECT_TRUE (Bit(match_bm, 1));
  EXPECT_FALSE(Bit(match_bm, 2));
  EXPECT_FALSE(Bit(match_bm, 3));
}

TEST_F(PaxVecReaderTest, BufferScanVecDoesNotMatchAcrossBoundary) {
  // Adjacent elements "abc" + "def" form "abcdef" in buffer; needle "cd"
  // would match across the seam but CheckAndMarkMatch must reject it.
  PackedStrings p = Pack({"abc", "def"});
  uint8_t match_bm[1] = {0};
  test_hooks::BufferScanSubstrVec(p.data.data(), p.offsets.data(),
                                   p.count(), "cd", 2, match_bm, nullptr);
  EXPECT_FALSE(Bit(match_bm, 0));
  EXPECT_FALSE(Bit(match_bm, 1));
}

TEST_F(PaxVecReaderTest, BufferScanVecRespectsElemToIdx) {
  // 3 elements: "xx", "yfoo", "zz".  Without elem_to_idx, "yfoo" would set
  // bit 1.  With elem_to_idx mapping [10, 20, 30], it must set bit 20.
  PackedStrings p = Pack({"xx", "yfoo", "zz"});
  int32_t map[3] = {10, 20, 30};
  uint8_t match_bm[8] = {0};  // big enough
  test_hooks::BufferScanSubstrVec(p.data.data(), p.offsets.data(),
                                   p.count(), "foo", 3, match_bm, map);
  EXPECT_FALSE(Bit(match_bm, 0));
  EXPECT_FALSE(Bit(match_bm, 1));
  EXPECT_FALSE(Bit(match_bm, 10));
  EXPECT_TRUE (Bit(match_bm, 20));
  EXPECT_FALSE(Bit(match_bm, 30));
}

// ---------------------------------------------------------------------------
// BufferScanSubstr<true> — PORC varlena (each element has 4B header).
// ---------------------------------------------------------------------------

// Pack strings as raw 4-byte-header varlena values.
static PackedStrings PackVarlena(std::initializer_list<const char *> strs) {
  PackedStrings p;
  p.offsets.push_back(0);
  for (const char *s : strs) {
    size_t n = std::strlen(s);
    size_t total = n + VARHDRSZ;
    size_t before = p.data.size();
    p.data.resize(before + total);
    char *vl = p.data.data() + before;
    SET_VARSIZE(vl, total);
    memcpy(vl + VARHDRSZ, s, n);
    p.offsets.push_back(static_cast<int32_t>(p.data.size()));
  }
  return p;
}

TEST_F(PaxVecReaderTest, BufferScanVarlenaIgnoresHeaderBytes) {
  // 4-byte varlena header bytes are in the buffer.  CheckAndMarkMatch<true>
  // must skip any "match" that falls inside the header region.  We construct
  // strings where the body genuinely contains the needle.
  PackedStrings p = PackVarlena({"hello", "worldfoobar", "buzz"});
  uint8_t match_bm[1] = {0};
  test_hooks::BufferScanSubstrVarlena(p.data.data(), p.offsets.data(),
                                       p.count(), "foobar", 6, match_bm,
                                       nullptr);
  EXPECT_FALSE(Bit(match_bm, 0));
  EXPECT_TRUE (Bit(match_bm, 1));
  EXPECT_FALSE(Bit(match_bm, 2));
}

// ---------------------------------------------------------------------------
// EvalStrFilters — per-row predicate evaluation.
// ---------------------------------------------------------------------------

TEST_F(PaxVecReaderTest, EvalStrContainsHit) {
  char needle[] = "foo";
  FastFilterDesc fd{};
  fd.attno = 1; fd.op = FastFilterOp::kStrContains; fd.type_len = -1;
  fd.str_const = needle; fd.str_const_len = 3;
  const char *s = "hello foo bar";
  EXPECT_TRUE(test_hooks::EvalStrFiltersHook(s, std::strlen(s), &fd, 1));
}

TEST_F(PaxVecReaderTest, EvalStrContainsMiss) {
  char needle[] = "xyz";
  FastFilterDesc fd{};
  fd.attno = 1; fd.op = FastFilterOp::kStrContains; fd.type_len = -1;
  fd.str_const = needle; fd.str_const_len = 3;
  const char *s = "hello foo bar";
  EXPECT_FALSE(test_hooks::EvalStrFiltersHook(s, std::strlen(s), &fd, 1));
}

TEST_F(PaxVecReaderTest, EvalStrNotContainsHit) {
  char needle[] = "foo";
  FastFilterDesc fd{};
  fd.attno = 1; fd.op = FastFilterOp::kStrNotContains; fd.type_len = -1;
  fd.str_const = needle; fd.str_const_len = 3;
  const char *s = "hello bar";  // does NOT contain "foo"
  EXPECT_TRUE(test_hooks::EvalStrFiltersHook(s, std::strlen(s), &fd, 1));
}

TEST_F(PaxVecReaderTest, EvalStrNotEmpty) {
  FastFilterDesc fd{};
  fd.attno = 1; fd.op = FastFilterOp::kStrNotEmpty; fd.type_len = -1;
  EXPECT_TRUE (test_hooks::EvalStrFiltersHook("a", 1, &fd, 1));
  EXPECT_FALSE(test_hooks::EvalStrFiltersHook("",  0, &fd, 1));
}

TEST_F(PaxVecReaderTest, EvalStrAllFiltersMustPass) {
  char n1[] = "foo";
  char n2[] = "bar";
  FastFilterDesc fds[2]{};
  fds[0].attno = 1; fds[0].op = FastFilterOp::kStrContains;
  fds[0].type_len = -1;
  fds[0].str_const = n1; fds[0].str_const_len = 3;
  fds[1].attno = 1; fds[1].op = FastFilterOp::kStrNotContains;
  fds[1].type_len = -1;
  fds[1].str_const = n2; fds[1].str_const_len = 3;

  // Contains "foo" AND does not contain "bar".
  EXPECT_TRUE (test_hooks::EvalStrFiltersHook("foozz", 5, fds, 2));
  EXPECT_FALSE(test_hooks::EvalStrFiltersHook("foobar", 6, fds, 2));
  EXPECT_FALSE(test_hooks::EvalStrFiltersHook("zz", 2, fds, 2));
}

// ---------------------------------------------------------------------------
// MergeToastPackScalar — clears toast bits, then scans the pack.
// ---------------------------------------------------------------------------

TEST_F(PaxVecReaderTest, MergeToastPackClearsThenMarks) {
  // Suppose nrows = 10; rows 3, 5, 7 are toast.  We pre-populate the pack
  // such that the detoasted text for those rows is:
  //   row 3 → "alpha foo"            (contains needle "foo")
  //   row 5 → "no match"             (does NOT contain "foo")
  //   row 7 → "xfoobaz"              (contains "foo")
  // Before calling Merge, filt_bm has random bits set including stale ones
  // for rows 3,5,7.  After Merge, filt_bm bits for rows 3 and 7 are set;
  // row 5 is cleared.  Non-toast row bits are untouched.

  const char *row3 = "alpha foo";
  const char *row5 = "no match";
  const char *row7 = "xfoobaz";

  size_t l3 = std::strlen(row3), l5 = std::strlen(row5), l7 = std::strlen(row7);
  std::vector<uint8_t> buf;
  buf.insert(buf.end(), row3, row3 + l3);
  buf.insert(buf.end(), row5, row5 + l5);
  buf.insert(buf.end(), row7, row7 + l7);
  int32_t offsets[4] = {0,
                        (int32_t)l3,
                        (int32_t)(l3 + l5),
                        (int32_t)(l3 + l5 + l7)};
  int32_t elem_to_row[3] = {3, 5, 7};

  auto *tl = test_hooks::GetThreadFilterState();
  test_hooks::SetupToastPackForTest(tl, buf.data(), buf.size(), offsets,
                                    elem_to_row, /*toast_count=*/3);

  // filt_bm starts with all 10 rows marked "matches" — Pass 1 over a raw
  // toast-pointer buffer would have done that wrongly for the 3 toast rows.
  uint8_t filt_bm[2] = {0xFF, 0x03};  // bits 0..9 all set
  // Sanity for non-toast rows (0,1,2,4,6,8,9) — start as 1.
  ASSERT_TRUE(Bit(filt_bm, 0));
  ASSERT_TRUE(Bit(filt_bm, 9));

  test_hooks::MergeToastPackScalarHook(tl, /*toast_count=*/3, "foo", 3,
                                        filt_bm);

  // Toast rows: 3 and 7 set, 5 cleared.
  EXPECT_TRUE (Bit(filt_bm, 3));
  EXPECT_FALSE(Bit(filt_bm, 5));
  EXPECT_TRUE (Bit(filt_bm, 7));
  // Non-toast rows: untouched (still set).
  EXPECT_TRUE(Bit(filt_bm, 0));
  EXPECT_TRUE(Bit(filt_bm, 1));
  EXPECT_TRUE(Bit(filt_bm, 2));
  EXPECT_TRUE(Bit(filt_bm, 4));
  EXPECT_TRUE(Bit(filt_bm, 6));
  EXPECT_TRUE(Bit(filt_bm, 8));
  EXPECT_TRUE(Bit(filt_bm, 9));
}

TEST_F(PaxVecReaderTest, MergeToastPackZeroToastCountIsNoOp) {
  auto *tl = test_hooks::GetThreadFilterState();
  uint8_t filt_bm[1] = {0xAA};
  uint8_t expected[1] = {0xAA};
  test_hooks::MergeToastPackScalarHook(tl, /*toast_count=*/0, "foo", 3,
                                        filt_bm);
  EXPECT_EQ(filt_bm[0], expected[0]);
}

// ---------------------------------------------------------------------------
// Review #2: ThreadFilterState cache invalidation must be id-keyed, not
// address-keyed.  The tests below directly exercise the cache-key check
// that FastFilterGroup performs on entry.
// ---------------------------------------------------------------------------

TEST_F(PaxVecReaderTest, CacheReinitOnFirstUse) {
  test_hooks::ResetThreadFilterCache();
  FastFilterContext ctx;
  ctx.id = 100;
  FastFilterDesc fd{};
  fd.attno = 1; fd.op = FastFilterOp::kEqual; fd.type_len = 4;
  ctx.filters.push_back(fd);

  EXPECT_TRUE(test_hooks::MaybeInitCacheForCtx(&ctx));  // first time
  EXPECT_EQ(test_hooks::GetCachedColGroupCount(), 1U);
  EXPECT_EQ(test_hooks::GetCachedColGroupAttno(0), 1);
}

TEST_F(PaxVecReaderTest, CacheHitsOnSameId) {
  test_hooks::ResetThreadFilterCache();
  FastFilterContext ctx;
  ctx.id = 200;
  FastFilterDesc fd{};
  fd.attno = 1; fd.op = FastFilterOp::kEqual; fd.type_len = 4;
  ctx.filters.push_back(fd);

  EXPECT_TRUE (test_hooks::MaybeInitCacheForCtx(&ctx));   // first
  EXPECT_FALSE(test_hooks::MaybeInitCacheForCtx(&ctx));   // cached
  EXPECT_FALSE(test_hooks::MaybeInitCacheForCtx(&ctx));   // still cached
}

// ---------------------------------------------------------------------------
// Integer column eval — full-scan and selective paths across all four
// supported widths.
// ---------------------------------------------------------------------------

TEST_F(PaxVecReaderTest, IntFullScanEqual) {
  int32 data[] = {1, 2, 3, 2, 5};
  FastFilterDesc fd{};
  fd.attno = 1; fd.op = FastFilterOp::kEqual; fd.type_len = 4;
  fd.const_value = Int32GetDatum(2);
  uint8_t col_bm[1] = {0};
  test_hooks::EvalColumnIntFullScanHook(4, data, nullptr, &fd, 1, true, 5, col_bm);
  EXPECT_FALSE(Bit(col_bm, 0));
  EXPECT_TRUE (Bit(col_bm, 1));
  EXPECT_FALSE(Bit(col_bm, 2));
  EXPECT_TRUE (Bit(col_bm, 3));
  EXPECT_FALSE(Bit(col_bm, 4));
}

TEST_F(PaxVecReaderTest, IntFullScanLessAndGreater) {
  int64 data[] = {10, 20, 30, 40, 50};

  FastFilterDesc lt{};
  lt.attno = 1; lt.op = FastFilterOp::kLess; lt.type_len = 8;
  lt.const_value = Int64GetDatum(30);
  uint8_t col_bm[1] = {0};
  test_hooks::EvalColumnIntFullScanHook(8, data, nullptr, &lt, 1, true, 5, col_bm);
  EXPECT_TRUE (Bit(col_bm, 0));
  EXPECT_TRUE (Bit(col_bm, 1));
  EXPECT_FALSE(Bit(col_bm, 2));  // not strictly less
  EXPECT_FALSE(Bit(col_bm, 3));
  EXPECT_FALSE(Bit(col_bm, 4));

  FastFilterDesc ge{};
  ge.attno = 1; ge.op = FastFilterOp::kGreaterEqual; ge.type_len = 8;
  ge.const_value = Int64GetDatum(30);
  uint8_t col_bm2[1] = {0};
  test_hooks::EvalColumnIntFullScanHook(8, data, nullptr, &ge, 1, true, 5, col_bm2);
  EXPECT_FALSE(Bit(col_bm2, 0));
  EXPECT_FALSE(Bit(col_bm2, 1));
  EXPECT_TRUE (Bit(col_bm2, 2));
  EXPECT_TRUE (Bit(col_bm2, 3));
  EXPECT_TRUE (Bit(col_bm2, 4));
}

TEST_F(PaxVecReaderTest, IntFullScanInValues) {
  int16 data[] = {1, 5, 7, 5, 9};
  Datum in_vals[2] = {Int16GetDatum(5), Int16GetDatum(7)};
  FastFilterDesc fd{};
  fd.attno = 1; fd.op = FastFilterOp::kInValues; fd.type_len = 2;
  fd.in_values = in_vals;
  fd.in_count = 2;
  uint8_t col_bm[1] = {0};
  test_hooks::EvalColumnIntFullScanHook(2, data, nullptr, &fd, 1, true, 5, col_bm);
  EXPECT_FALSE(Bit(col_bm, 0));
  EXPECT_TRUE (Bit(col_bm, 1));
  EXPECT_TRUE (Bit(col_bm, 2));
  EXPECT_TRUE (Bit(col_bm, 3));
  EXPECT_FALSE(Bit(col_bm, 4));
}

TEST_F(PaxVecReaderTest, IntFullScanInt8AllOps) {
  // Verify the smallest int width path.
  int8 data[] = {-128, -1, 0, 1, 127};
  FastFilterDesc gt{};
  gt.attno = 1; gt.op = FastFilterOp::kGreater; gt.type_len = 1;
  gt.const_value = Int8GetDatum(0);
  uint8_t col_bm[1] = {0};
  test_hooks::EvalColumnIntFullScanHook(1, data, nullptr, &gt, 1, true, 5, col_bm);
  EXPECT_FALSE(Bit(col_bm, 0));
  EXPECT_FALSE(Bit(col_bm, 1));
  EXPECT_FALSE(Bit(col_bm, 2));
  EXPECT_TRUE (Bit(col_bm, 3));
  EXPECT_TRUE (Bit(col_bm, 4));
}

TEST_F(PaxVecReaderTest, IntFullScanRespectsNullBitmap) {
  // Bit=1 in null_bm means NOT null; bit=0 means null → must NOT pass.
  int32 data[] = {1, 2, 3, 4, 5};
  uint8_t null_bm[1] = {0x15};  // 0b00010101: rows 0, 2, 4 not-null
  FastFilterDesc fd{};
  fd.attno = 1; fd.op = FastFilterOp::kGreater; fd.type_len = 4;
  fd.const_value = Int32GetDatum(0);
  uint8_t col_bm[1] = {0};
  test_hooks::EvalColumnIntFullScanHook(4, data, null_bm, &fd, 1, true, 5, col_bm);
  EXPECT_TRUE (Bit(col_bm, 0));
  EXPECT_FALSE(Bit(col_bm, 1));   // null
  EXPECT_TRUE (Bit(col_bm, 2));
  EXPECT_FALSE(Bit(col_bm, 3));   // null
  EXPECT_TRUE (Bit(col_bm, 4));
}

// PORC (non-VEC) layout: null rows are squeezed out of the data buffer.
// Logical rows: NULL, 2, 3, 4, 5  →  dense buffer: [2, 3, 4, 5] (4 elements).
// IN (2,3) must pass logical rows 1 and 2.  This is the regression case
// from filter.sql `b in (2, 3)` with one NULL row preceding the matches.
TEST_F(PaxVecReaderTest, IntFullScanDenseBufferWithLeadingNull) {
  int64 data[] = {2, 3, 4, 5};               // dense, NULL slot skipped
  uint8_t null_bm[1] = {0x1E};               // 0b00011110: row 0 NULL
  Datum in_vals[2] = {Int64GetDatum(2), Int64GetDatum(3)};
  FastFilterDesc fd{};
  fd.attno = 1; fd.op = FastFilterOp::kInValues; fd.type_len = 8;
  fd.in_values = in_vals; fd.in_count = 2;
  uint8_t col_bm[1] = {0};
  test_hooks::EvalColumnIntFullScanHook(8, data, null_bm, &fd, 1,
                                        /*is_vec=*/false, 5, col_bm);
  EXPECT_FALSE(Bit(col_bm, 0));   // NULL
  EXPECT_TRUE (Bit(col_bm, 1));   // logical b=2
  EXPECT_TRUE (Bit(col_bm, 2));   // logical b=3
  EXPECT_FALSE(Bit(col_bm, 3));
  EXPECT_FALSE(Bit(col_bm, 4));
}

// Same case but for the Selective path.  Combined bitmap starts all-pass.
TEST_F(PaxVecReaderTest, IntSelectiveDenseBufferWithLeadingNull) {
  int64 data[] = {2, 3, 4, 5};
  uint8_t null_bm[1] = {0x1E};
  Datum in_vals[2] = {Int64GetDatum(2), Int64GetDatum(3)};
  FastFilterDesc fd{};
  fd.attno = 1; fd.op = FastFilterOp::kInValues; fd.type_len = 8;
  fd.in_values = in_vals; fd.in_count = 2;
  uint8_t combined_bm[1] = {0x1F};  // all 5 rows initially passing
  test_hooks::EvalColumnIntSelectiveHook(8, data, null_bm, &fd, 1,
                                         /*is_vec=*/false, 5, combined_bm);
  EXPECT_FALSE(Bit(combined_bm, 0));  // NULL
  EXPECT_TRUE (Bit(combined_bm, 1));
  EXPECT_TRUE (Bit(combined_bm, 2));
  EXPECT_FALSE(Bit(combined_bm, 3));
  EXPECT_FALSE(Bit(combined_bm, 4));
}

// Dense buffer with an interior NULL — verifies index advance after the
// null skip stays in sync with the buffer.
TEST_F(PaxVecReaderTest, IntFullScanDenseBufferWithInteriorNull) {
  // Logical rows: 10, NULL, 20, 30  →  dense: [10, 20, 30]
  int32 data[] = {10, 20, 30};
  uint8_t null_bm[1] = {0x0D};               // 0b00001101: row 1 NULL
  FastFilterDesc fd{};
  fd.attno = 1; fd.op = FastFilterOp::kEqual; fd.type_len = 4;
  fd.const_value = Int32GetDatum(20);
  uint8_t col_bm[1] = {0};
  test_hooks::EvalColumnIntFullScanHook(4, data, null_bm, &fd, 1,
                                        /*is_vec=*/false, 4, col_bm);
  EXPECT_FALSE(Bit(col_bm, 0));
  EXPECT_FALSE(Bit(col_bm, 1));   // NULL
  EXPECT_TRUE (Bit(col_bm, 2));   // logical 20
  EXPECT_FALSE(Bit(col_bm, 3));
}

TEST_F(PaxVecReaderTest, IntFullScanMultipleFiltersAllMustPass) {
  // (>10) AND (<50)
  int32 data[] = {5, 15, 25, 60, 100};
  FastFilterDesc fds[2]{};
  fds[0].attno = 1; fds[0].op = FastFilterOp::kGreater; fds[0].type_len = 4;
  fds[0].const_value = Int32GetDatum(10);
  fds[1].attno = 1; fds[1].op = FastFilterOp::kLess;    fds[1].type_len = 4;
  fds[1].const_value = Int32GetDatum(50);
  uint8_t col_bm[1] = {0};
  test_hooks::EvalColumnIntFullScanHook(4, data, nullptr, fds, 2, true, 5, col_bm);
  EXPECT_FALSE(Bit(col_bm, 0));
  EXPECT_TRUE (Bit(col_bm, 1));
  EXPECT_TRUE (Bit(col_bm, 2));
  EXPECT_FALSE(Bit(col_bm, 3));
  EXPECT_FALSE(Bit(col_bm, 4));
}

TEST_F(PaxVecReaderTest, IntSelectiveOnlyTouchesPassingRows) {
  // Pre-set combined_bm: rows 0 and 4 already rejected; selective should
  // not flip those bits even if the data would pass the filter.
  int32 data[] = {100, 100, 100, 100, 100};
  FastFilterDesc fd{};
  fd.attno = 1; fd.op = FastFilterOp::kGreater; fd.type_len = 4;
  fd.const_value = Int32GetDatum(0);  // every row would pass
  uint8_t combined_bm[1] = {0x0E};    // 0b00001110: rows 1,2,3 passing
  test_hooks::EvalColumnIntSelectiveHook(4, data, nullptr, &fd, 1, true, 5,
                                          combined_bm);
  EXPECT_FALSE(Bit(combined_bm, 0));
  EXPECT_TRUE (Bit(combined_bm, 1));
  EXPECT_TRUE (Bit(combined_bm, 2));
  EXPECT_TRUE (Bit(combined_bm, 3));
  EXPECT_FALSE(Bit(combined_bm, 4));
}

TEST_F(PaxVecReaderTest, IntSelectiveClearsFailingPassingRows) {
  // All rows initially passing; selective clears those that fail filter.
  int32 data[] = {1, 2, 3, 4, 5};
  FastFilterDesc fd{};
  fd.attno = 1; fd.op = FastFilterOp::kGreater; fd.type_len = 4;
  fd.const_value = Int32GetDatum(3);
  uint8_t combined_bm[1] = {0x1F};  // 0b00011111
  test_hooks::EvalColumnIntSelectiveHook(4, data, nullptr, &fd, 1, true, 5,
                                          combined_bm);
  EXPECT_FALSE(Bit(combined_bm, 0));
  EXPECT_FALSE(Bit(combined_bm, 1));
  EXPECT_FALSE(Bit(combined_bm, 2));
  EXPECT_TRUE (Bit(combined_bm, 3));
  EXPECT_TRUE (Bit(combined_bm, 4));
}

TEST_F(PaxVecReaderTest, CacheInvalidatesOnAbaScenario) {
  // Simulate ABA: re-use the *same* FastFilterContext memory but stamp it
  // with a new id (as PaxFastFilter::Initialize would do for a fresh
  // instance), with different filters inside.  The cache must invalidate.
  test_hooks::ResetThreadFilterCache();

  FastFilterContext ctx;  // single stack object; address never changes
  ctx.id = 300;
  FastFilterDesc fd_a{};
  fd_a.attno = 1; fd_a.op = FastFilterOp::kEqual; fd_a.type_len = 4;
  ctx.filters.push_back(fd_a);

  ASSERT_TRUE(test_hooks::MaybeInitCacheForCtx(&ctx));
  ASSERT_EQ(test_hooks::GetCachedColGroupCount(), 1U);
  ASSERT_EQ(test_hooks::GetCachedColGroupAttno(0), 1);

  // Now "destroy" the old PaxFastFilter and reincarnate at the same address
  // with new filters + new id.
  ctx.filters.clear();
  ctx.id = 301;
  FastFilterDesc fd_b1{}, fd_b2{};
  fd_b1.attno = 3; fd_b1.op = FastFilterOp::kEqual; fd_b1.type_len = 4;
  fd_b2.attno = 5; fd_b2.op = FastFilterOp::kLess;  fd_b2.type_len = 4;
  ctx.filters.push_back(fd_b1);
  ctx.filters.push_back(fd_b2);

  EXPECT_TRUE(test_hooks::MaybeInitCacheForCtx(&ctx))
      << "Address-only cache key would silently reuse stale col_groups";
  EXPECT_EQ(test_hooks::GetCachedColGroupCount(), 2U);
  EXPECT_EQ(test_hooks::GetCachedColGroupAttno(0), 3);
  EXPECT_EQ(test_hooks::GetCachedColGroupAttno(1), 5);
}

}  // namespace pax::tests

#endif  // VEC_BUILD
