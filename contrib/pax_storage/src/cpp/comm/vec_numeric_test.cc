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
 * vec_numeric_test.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/comm/vec_numeric_test.cc
 *
 *-------------------------------------------------------------------------
 */

#include "comm/vec_numeric.h"

#include <random>
#include <string>

#include "comm/cbdb_wrappers.h"
#include "comm/gtest_wrappers.h"

#ifdef HAVE_INT128
namespace pax::tests {

class VecNumericTest : public ::testing::Test {
 public:
  void SetUp() override {
    MemoryContext ctx = AllocSetContextCreate(
        (MemoryContext)NULL, "VecNumericTestMemoryContext", 1 * 1024 * 1024,
        1 * 1024 * 1024, 1 * 1024 * 1024);
    MemoryContextSwitchTo(ctx);
  }
};

// Convert `text` with pg_short_numeric_to_vec_short_numeric and check the
// result against the unscaled integer read straight from the text, which
// must carry exactly `scale` fractional digits.
static void CheckConversion(const std::string &text, int scale) {
  Datum d = DirectFunctionCall3(numeric_in, CStringGetDatum(text.c_str()),
                                ObjectIdGetDatum(InvalidOid),
                                Int32GetDatum(-1));
  Numeric num = DatumGetNumeric(d);
  ASSERT_EQ(NUMERIC_DSCALE(num), scale) << text;

  int128 expected = 0;
  bool neg = false;
  for (char c : text) {
    if (c == '-')
      neg = true;
    else if (c >= '0' && c <= '9')
      expected = expected * 10 + (c - '0');
  }
  if (neg) expected = -expected;

  int64 low, high;
  pg_short_numeric_to_vec_short_numeric(
      num, VARSIZE_ANY_EXHDR((struct varlena *)num), &low, &high);

  const int64 data_mask = (1LL << VEC_SHORT_NUMERIC_HIGH_DATA_WIDTH) - 1;
  EXPECT_EQ((uint64)low, (uint64)expected) << text;
  EXPECT_EQ(high & data_mask, (int64)(expected >> 64) & data_mask) << text;
  EXPECT_EQ((uint64)high >> VEC_SHORT_NUMERIC_HIGH_DATA_WIDTH, (uint64)scale)
      << text;
}

TEST_F(VecNumericTest, ShortNumericToVecFixedCases) {
  // scale % 4 != 0 makes the last fractional group run past the scale.
  CheckConversion("0.00", 2);
  CheckConversion("12.34", 2);
  CheckConversion("-12.34", 2);
  CheckConversion("99999.99", 2);
  CheckConversion("0.05", 2);
  CheckConversion("0.5", 1);
  CheckConversion("1.000", 3);
  CheckConversion("0.001", 3);
  CheckConversion("0.1234", 4);
  CheckConversion("0.12345", 5);
  CheckConversion("12345678.9", 1);
  // Leading zero groups after the point (negative weight).
  CheckConversion("0.00001", 5);
  CheckConversion("0.000000012", 9);
  CheckConversion("-0.0000000000123", 13);
  // Trailing zeros PG drops from the digits but keeps in the dscale.
  CheckConversion("1.50000", 5);
  CheckConversion("100.0000000", 7);
  // Values that need the high 64 bits.
  CheckConversion("12345678901234567890123.456789", 6);
  CheckConversion("-1234567890123456789012345678.9012345", 7);
  CheckConversion("0.12345678901234567890123456789012345", 35);
  CheckConversion("-9999999999999999999999999999999999.9", 1);
}

TEST_F(VecNumericTest, ShortNumericToVecRandom) {
  std::mt19937_64 rng(20261020);
  for (int scale = 0; scale <= VEC_SHORT_NUMERIC_MAX_SCALE; scale++) {
    for (int iter = 0; iter < 2000; iter++) {
      int int_digits = rng() % (VEC_SHORT_NUMERIC_MAX_PRECISION - scale + 1);
      std::string text = (rng() & 1) ? "-" : "";
      if (int_digits == 0) {
        text += "0";
      } else {
        text += char('1' + rng() % 9);
        for (int i = 1; i < int_digits; i++) text += char('0' + rng() % 10);
      }
      if (scale > 0) {
        text += ".";
        // Bias towards zeros so leading / trailing zero groups show up.
        for (int i = 0; i < scale; i++)
          text += (rng() % 3 == 0) ? '0' : char('0' + rng() % 10);
      }
      CheckConversion(text, scale);
      if (HasFatalFailure() || HasFailure()) return;
    }
  }
}

}  // namespace pax::tests
#endif
