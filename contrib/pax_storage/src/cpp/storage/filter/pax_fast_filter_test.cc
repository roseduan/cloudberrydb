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
 * pax_fast_filter_test.cc
 *
 *   Unit tests for predicate classification, FastFilterDesc construction,
 *   and projection-mask building in the PAX fast filter path.
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/filter/pax_fast_filter_test.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/filter/pax_fast_filter.h"

#include <cstring>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "comm/cbdb_wrappers.h"
#include "comm/guc.h"
#include "comm/gtest_wrappers.h"
#include "comm/vec_numeric.h"
#include "cpp-stub/src/stub.h"
#include "pax_gtest_helper.h"

extern "C" {
#include "utils/fmgroids.h"
}

namespace pax::tests {

// ---------------------------------------------------------------------------
// Mock OID table for stubbed cbdb::PGGetOperatorNo.
//
// Tests reference these "operator OIDs" symbolically; the stub maps each to a
// canonical operator name (so OpernameToStrategy can do its lookup) and to
// left/right types (so cross-type detection works).  These OIDs are local to
// the test file and do not need to match any real catalog entry.
// ---------------------------------------------------------------------------
constexpr Oid kOpInt4Eq    = 1001;
constexpr Oid kOpInt4Lt    = 1002;
constexpr Oid kOpInt4Le    = 1003;
constexpr Oid kOpInt4Gt    = 1004;
constexpr Oid kOpInt4Ge    = 1005;
constexpr Oid kOpInt4Ne    = 1006;
constexpr Oid kOpInt8Eq    = 1007;
constexpr Oid kOpInt2Eq    = 1008;
constexpr Oid kOpInt48Eq   = 1009;  // int4 = int8 (cross type)
constexpr Oid kOpFloat4Eq  = 1010;
constexpr Oid kOpTextEq    = 1020;
constexpr Oid kOpTextNe    = 1021;
constexpr Oid kOpTextLike  = 1022;  // text ~~ text
constexpr Oid kOpTextNlike = 1023;  // text !~~ text

struct MockOpEntry {
  Oid opno;
  const char *name;
  Oid lefttype;
  Oid righttype;
};

static const MockOpEntry kMockOps[] = {
    {kOpInt4Eq,    "=",  INT4OID,  INT4OID},
    {kOpInt4Lt,    "<",  INT4OID,  INT4OID},
    {kOpInt4Le,    "<=", INT4OID,  INT4OID},
    {kOpInt4Gt,    ">",  INT4OID,  INT4OID},
    {kOpInt4Ge,    ">=", INT4OID,  INT4OID},
    {kOpInt4Ne,    "<>", INT4OID,  INT4OID},
    {kOpInt8Eq,    "=",  INT8OID,  INT8OID},
    {kOpInt2Eq,    "=",  INT2OID,  INT2OID},
    {kOpInt48Eq,   "=",  INT4OID,  INT8OID},
    {kOpFloat4Eq,  "=",  FLOAT4OID, FLOAT4OID},
    {kOpTextEq,    "=",  TEXTOID,  TEXTOID},
    {kOpTextNe,    "<>", TEXTOID,  TEXTOID},
    {kOpTextLike,  "~~", TEXTOID,  TEXTOID},
    {kOpTextNlike, "!~~", TEXTOID, TEXTOID},
};

// Stub for PG's get_typlenbyvalalign — replies for the small set of types
// referenced by our IN-list tests; ereport-FATAL otherwise to make
// uncovered-type usage visible.
static void MockGetTyplenbyvalalign(Oid typid, int16 *typlen, bool *typbyval,
                                    char *typalign) {
  switch (typid) {
    case INT2OID:
      *typlen = 2; *typbyval = true; *typalign = TYPALIGN_SHORT; break;
    case INT4OID:
      *typlen = 4; *typbyval = true; *typalign = TYPALIGN_INT; break;
    case INT8OID:
      *typlen = 8; *typbyval = true; *typalign = TYPALIGN_DOUBLE; break;
    default:
      *typlen = 0; *typbyval = false; *typalign = TYPALIGN_CHAR; break;
  }
}

static bool MockPGGetOperatorNo(Oid opno, NameData *oprname, Oid *oprleft,
                                Oid *oprright, FmgrInfo *finfo) {
  (void)finfo;
  for (const auto &e : kMockOps) {
    if (e.opno != opno) continue;
    if (oprname) {
      memset(oprname, 0, sizeof(*oprname));
      std::strncpy(NameStr(*oprname), e.name, NAMEDATALEN - 1);
    }
    if (oprleft)  *oprleft  = e.lefttype;
    if (oprright) *oprright = e.righttype;
    return true;
  }
  return false;
}

// ---------------------------------------------------------------------------
// Test fixture
// ---------------------------------------------------------------------------
class PaxFastFilterTest : public ::testing::Test {
 protected:
  Stub *stub_ = nullptr;

  void SetUp() override {
    CreateMemoryContext();
    CreateTestResourceOwner();
    stub_ = new Stub();
    stub_->set(cbdb::PGGetOperatorNo, MockPGGetOperatorNo);
    stub_->set(get_typlenbyvalalign, MockGetTyplenbyvalalign);
    // Ensure the GUCs are enabled (default values, but be explicit; the
    // core GUC is not initialised in this binary).
    pax::pax_enable_fast_filter = true;
    gp_enable_predicate_pushdown = true;
  }

  void TearDown() override {
    delete stub_;
    stub_ = nullptr;
    ReleaseTestResourceOwner();
  }

  // Build a TupleDesc with the given attribute init callbacks.  attlen and
  // attbyval are set per type.  We expose a small set of column kinds
  // sufficient for the fast filter classification tests.
  enum AttKind {
    kAttInt2, kAttInt4, kAttInt8, kAttFloat4, kAttText,
    kAttNumeric72,    // numeric(7,2)
    kAttNumeric186,   // numeric(18,6)
    kAttNumeric192,   // numeric(19,2): too wide for numeric keys
    kAttNumericFree,  // numeric without typmod
  };

  static TupleDesc BuildTupleDesc(std::initializer_list<AttKind> kinds) {
    int n = static_cast<int>(kinds.size());
    auto td = reinterpret_cast<TupleDescData *>(cbdb::Palloc0(
        sizeof(TupleDescData) + sizeof(FormData_pg_attribute) * n));
    td->natts = n;
    int i = 0;
    for (auto k : kinds) {
      auto *a = &td->attrs[i++];
      memset(a, 0, sizeof(*a));
      a->attstorage = TYPSTORAGE_PLAIN;
      a->attcollation = InvalidOid;
      switch (k) {
        case kAttInt2:
          a->atttypid = INT2OID; a->attlen = 2; a->attbyval = true;
          a->attalign = TYPALIGN_SHORT; break;
        case kAttInt4:
          a->atttypid = INT4OID; a->attlen = 4; a->attbyval = true;
          a->attalign = TYPALIGN_INT; break;
        case kAttInt8:
          a->atttypid = INT8OID; a->attlen = 8; a->attbyval = true;
          a->attalign = TYPALIGN_DOUBLE; break;
        case kAttFloat4:
          a->atttypid = FLOAT4OID; a->attlen = 4; a->attbyval = true;
          a->attalign = TYPALIGN_INT; break;
        case kAttText:
          a->atttypid = TEXTOID; a->attlen = -1; a->attbyval = false;
          a->attalign = TYPALIGN_INT;
          a->attcollation = DEFAULT_COLLATION_OID;
          break;
        case kAttNumeric72:
        case kAttNumeric186:
        case kAttNumeric192:
        case kAttNumericFree:
          a->atttypid = NUMERICOID; a->attlen = -1; a->attbyval = false;
          a->attalign = TYPALIGN_INT;
          a->atttypmod = k == kAttNumeric72    ? NumericTypmod(7, 2)
                         : k == kAttNumeric186 ? NumericTypmod(18, 6)
                         : k == kAttNumeric192 ? NumericTypmod(19, 2)
                                               : -1;
          break;
      }
    }
    return td;
  }

  static int32 NumericTypmod(int precision, int scale) {
    return ((precision << 16) | scale) + VARHDRSZ;
  }

  // Minimal fake Relation.  BuildFastFilter touches rd_att (via
  // RelationGetDescr) and rd_rel->relnatts (via RelationGetNumberOfAttributes;
  // the macro reads pg_class.relnatts in cloudberry).
  static Relation MakeFakeRelation(TupleDesc desc) {
    auto *rel = reinterpret_cast<RelationData *>(
        cbdb::Palloc0(sizeof(RelationData)));
    rel->rd_att = desc;
    rel->rd_rel = reinterpret_cast<Form_pg_class>(
        cbdb::Palloc0(sizeof(*rel->rd_rel)));
    rel->rd_rel->relnatts = desc->natts;
    return rel;
  }

  // Construct a Var node for column `varattno` (1-based).
  static Var *MakeVar(TupleDesc desc, AttrNumber varattno) {
    Form_pg_attribute a = TupleDescAttr(desc, varattno - 1);
    return makeVar(/*varno=*/1, varattno, a->atttypid, /*vartypmod=*/-1,
                   a->attcollation, /*varlevelsup=*/0);
  }

  // Construct a Const node holding a by-value integer (int2/int4/int8).
  static Const *MakeIntConst(Oid typeoid, int64 value) {
    int16 typlen;
    bool typbyval = true;
    Datum d = 0;
    switch (typeoid) {
      case INT2OID: typlen = 2; d = Int16GetDatum((int16)value); break;
      case INT4OID: typlen = 4; d = Int32GetDatum((int32)value); break;
      case INT8OID: typlen = 8; d = Int64GetDatum(value); break;
      default: typlen = 4; d = Int32GetDatum((int32)value);
    }
    return makeConst(typeoid, /*consttypmod=*/-1, /*constcollid=*/InvalidOid,
                     typlen, d, /*constisnull=*/false, typbyval);
  }

  // Construct a Const holding a text value (palloc'd varlena).
  static Const *MakeTextConst(const char *s) {
    size_t n = std::strlen(s);
    text *t = reinterpret_cast<text *>(cbdb::Palloc0(n + VARHDRSZ));
    SET_VARSIZE(t, n + VARHDRSZ);
    memcpy(VARDATA(t), s, n);
    return makeConst(TEXTOID, /*consttypmod=*/-1, DEFAULT_COLLATION_OID,
                     /*constlen=*/-1, PointerGetDatum(t), false, false);
  }

  // OpExpr: var <op> const  (or const <op> var if `var_is_left == false`)
  static OpExpr *MakeOpExpr(Oid opno, Var *var, Const *cnst,
                            bool var_is_left = true,
                            Oid opresulttype = BOOLOID) {
    auto *e = makeNode(OpExpr);
    e->opno = opno;
    e->opfuncid = InvalidOid;
    e->opresulttype = opresulttype;
    e->opretset = false;
    e->opcollid = InvalidOid;
    e->inputcollid = var->varcollid;
    if (var_is_left) {
      e->args = list_make2(var, cnst);
    } else {
      e->args = list_make2(cnst, var);
    }
    return e;
  }

  static Const *MakeNumericConst(const char *text) {
    Datum d = DirectFunctionCall3(numeric_in, CStringGetDatum(text),
                                  ObjectIdGetDatum(InvalidOid),
                                  Int32GetDatum(-1));
    return makeConst(NUMERICOID, -1, InvalidOid, -1, d, false, false);
  }

  // numeric comparison: identified by its function, the opno does not matter.
  static OpExpr *MakeNumericOpExpr(Oid opfuncid, Var *var, Const *cnst,
                                   bool var_is_left = true) {
    auto *e = MakeOpExpr(InvalidOid, var, cnst, var_is_left);
    e->opfuncid = opfuncid;
    return e;
  }

  static Node *MakeAnd(std::initializer_list<void *> args) {
    List *l = NIL;
    for (void *a : args) l = lappend(l, a);
    return (Node *)makeBoolExpr(AND_EXPR, l, -1);
  }

  static Node *MakeOr(std::initializer_list<void *> args) {
    List *l = NIL;
    for (void *a : args) l = lappend(l, a);
    return (Node *)makeBoolExpr(OR_EXPR, l, -1);
  }

  // (var >= lo AND var <= hi) on a numeric column.
  static Node *NumericBetween(Var *var, const char *lo, const char *hi) {
    return MakeAnd(
        {MakeNumericOpExpr(F_NUMERIC_GE, var, MakeNumericConst(lo)),
         MakeNumericOpExpr(F_NUMERIC_LE, var, MakeNumericConst(hi))});
  }

  // Build the fast filter of `qual` (one qual) on `desc`; return its ranges
  // as pairs, or fail the test if no single kRangeSet filter came out.
  static std::vector<std::pair<int64, int64>> BuildRanges(TupleDesc desc,
                                                          Node *qual) {
    Relation rel = MakeFakeRelation(desc);
    FastFilterContext ctx;
    std::vector<AttrNumber> attnos;
    std::vector<std::pair<int64, int64>> out;
    EXPECT_TRUE(pax::BuildFastFilter(rel, list_make1(qual), &ctx, &attnos));
    EXPECT_EQ(ctx.filters.size(), 1U);
    if (ctx.filters.size() != 1) return out;
    EXPECT_EQ(ctx.filters[0].op, FastFilterOp::kRangeSet);
    for (int32 r = 0; r < ctx.filters[0].range_count; r++)
      out.emplace_back(ctx.filters[0].ranges[2 * r],
                       ctx.filters[0].ranges[2 * r + 1]);
    return out;
  }

  // ScalarArrayOpExpr for "var IN (v1, v2, ...)".  Elements are Int32 by
  // default.
  static ScalarArrayOpExpr *MakeIntInExpr(Oid opno, Var *var,
                                          std::initializer_list<int32> values,
                                          bool useOr = true) {
    int n = static_cast<int>(values.size());
    Datum *elems = reinterpret_cast<Datum *>(
        cbdb::Palloc0(sizeof(Datum) * n));
    bool *nulls = reinterpret_cast<bool *>(cbdb::Palloc0(sizeof(bool) * n));
    int i = 0;
    for (int v : values) { elems[i] = Int32GetDatum(v); nulls[i] = false; i++; }
    int dims[1] = {n};
    int lbs[1] = {1};
    ArrayType *arr = construct_md_array(elems, nulls, /*ndims=*/1, dims, lbs,
                                        INT4OID, /*elmlen=*/4,
                                        /*elmbyval=*/true,
                                        /*elmalign=*/TYPALIGN_INT);
    auto *cnst = makeConst(INT4ARRAYOID, /*typmod=*/-1, InvalidOid, -1,
                           PointerGetDatum(arr), false, false);
    auto *e = makeNode(ScalarArrayOpExpr);
    e->opno = opno;
    e->opfuncid = InvalidOid;
    e->useOr = useOr;
    e->inputcollid = var->varcollid;
    e->args = list_make2(var, cnst);
    return e;
  }
};

// ---------------------------------------------------------------------------
// PaxCanFastFilter — pure classification, no construction.
// ---------------------------------------------------------------------------

TEST_F(PaxFastFilterTest, ClassifyIntEqual) {
  TupleDesc desc = BuildTupleDesc({kAttInt4});
  auto *v = MakeVar(desc, 1);
  auto *c = MakeIntConst(INT4OID, 42);
  auto *op = MakeOpExpr(kOpInt4Eq, v, c);
  EXPECT_TRUE(paxc::PaxCanFastFilter((Node *)op, desc));
}

TEST_F(PaxFastFilterTest, ClassifyIntInequality) {
  TupleDesc desc = BuildTupleDesc({kAttInt4});
  auto *v = MakeVar(desc, 1);
  for (Oid opno : {kOpInt4Lt, kOpInt4Le, kOpInt4Gt, kOpInt4Ge, kOpInt4Ne}) {
    auto *c = MakeIntConst(INT4OID, 7);
    auto *op = MakeOpExpr(opno, v, c);
    EXPECT_TRUE(paxc::PaxCanFastFilter((Node *)op, desc)) << "opno=" << opno;
  }
}

TEST_F(PaxFastFilterTest, ClassifyCrossTypeIntEqual) {
  // int4 column compared against int8 constant.
  TupleDesc desc = BuildTupleDesc({kAttInt4});
  auto *v = MakeVar(desc, 1);
  auto *c = MakeIntConst(INT8OID, 100);
  auto *op = MakeOpExpr(kOpInt48Eq, v, c);
  EXPECT_TRUE(paxc::PaxCanFastFilter((Node *)op, desc));
}

TEST_F(PaxFastFilterTest, RejectFloatEqual) {
  TupleDesc desc = BuildTupleDesc({kAttFloat4});
  auto *v = MakeVar(desc, 1);
  // Float const value — encoded as int4 here for simplicity, since the
  // classifier rejects on var->vartype being float before looking at the
  // const at all.
  auto *c = MakeIntConst(INT4OID, 0);
  auto *op = MakeOpExpr(kOpFloat4Eq, v, c);
  EXPECT_FALSE(paxc::PaxCanFastFilter((Node *)op, desc));
}

TEST_F(PaxFastFilterTest, ClassifyTextEqualNotEmpty) {
  // text <> '' → kStrNotEmpty
  TupleDesc desc = BuildTupleDesc({kAttText});
  auto *v = MakeVar(desc, 1);
  auto *empty = MakeTextConst("");
  auto *op = MakeOpExpr(kOpTextNe, v, empty);
  EXPECT_TRUE(paxc::PaxCanFastFilter((Node *)op, desc));
}

TEST_F(PaxFastFilterTest, RejectTextEqualNonEmpty) {
  // text <> 'foo' is NOT fast-filter-handled (only <>'' is).
  TupleDesc desc = BuildTupleDesc({kAttText});
  auto *v = MakeVar(desc, 1);
  auto *foo = MakeTextConst("foo");
  auto *op = MakeOpExpr(kOpTextNe, v, foo);
  EXPECT_FALSE(paxc::PaxCanFastFilter((Node *)op, desc));
}

TEST_F(PaxFastFilterTest, ClassifyLikeContains) {
  TupleDesc desc = BuildTupleDesc({kAttText});
  auto *v = MakeVar(desc, 1);
  auto *pat = MakeTextConst("%foo%");
  auto *op = MakeOpExpr(kOpTextLike, v, pat);
  EXPECT_TRUE(paxc::PaxCanFastFilter((Node *)op, desc));
}

TEST_F(PaxFastFilterTest, ClassifyNotLikeContains) {
  TupleDesc desc = BuildTupleDesc({kAttText});
  auto *v = MakeVar(desc, 1);
  auto *pat = MakeTextConst("%bar%");
  auto *op = MakeOpExpr(kOpTextNlike, v, pat);
  EXPECT_TRUE(paxc::PaxCanFastFilter((Node *)op, desc));
}

TEST_F(PaxFastFilterTest, RejectLikePrefixOrSuffix) {
  TupleDesc desc = BuildTupleDesc({kAttText});
  auto *v = MakeVar(desc, 1);
  for (const char *pat_str : {"foo%", "%foo", "foo"}) {
    auto *pat = MakeTextConst(pat_str);
    auto *op = MakeOpExpr(kOpTextLike, v, pat);
    EXPECT_FALSE(paxc::PaxCanFastFilter((Node *)op, desc))
        << "pattern=" << pat_str;
  }
}

TEST_F(PaxFastFilterTest, RejectLikeWithSpecialChars) {
  TupleDesc desc = BuildTupleDesc({kAttText});
  auto *v = MakeVar(desc, 1);
  // Middle contains another %, _, or \: must be rejected (no real substring).
  for (const char *pat_str : {"%fo%o%", "%fo_o%", "%fo\\o%"}) {
    auto *pat = MakeTextConst(pat_str);
    auto *op = MakeOpExpr(kOpTextLike, v, pat);
    EXPECT_FALSE(paxc::PaxCanFastFilter((Node *)op, desc))
        << "pattern=" << pat_str;
  }
}

TEST_F(PaxFastFilterTest, ClassifyIntInList) {
  TupleDesc desc = BuildTupleDesc({kAttInt4});
  auto *v = MakeVar(desc, 1);
  auto *e = MakeIntInExpr(kOpInt4Eq, v, {1, 2, 3, 4});
  EXPECT_TRUE(paxc::PaxCanFastFilter((Node *)e, desc));
}

TEST_F(PaxFastFilterTest, RejectNotInList) {
  // NOT IN uses useOr=false (i.e. ALL semantics): classifier rejects.
  TupleDesc desc = BuildTupleDesc({kAttInt4});
  auto *v = MakeVar(desc, 1);
  auto *e = MakeIntInExpr(kOpInt4Eq, v, {1, 2}, /*useOr=*/false);
  EXPECT_FALSE(paxc::PaxCanFastFilter((Node *)e, desc));
}

TEST_F(PaxFastFilterTest, RejectGUCDisabled) {
  TupleDesc desc = BuildTupleDesc({kAttInt4});
  auto *v = MakeVar(desc, 1);
  auto *c = MakeIntConst(INT4OID, 42);
  auto *op = MakeOpExpr(kOpInt4Eq, v, c);
  pax::pax_enable_fast_filter = false;
  EXPECT_FALSE(paxc::PaxCanFastFilter((Node *)op, desc));
  pax::pax_enable_fast_filter = true;
}

TEST_F(PaxFastFilterTest, RejectGUCDisabledIN) {
  TupleDesc desc = BuildTupleDesc({kAttInt4});
  auto *v = MakeVar(desc, 1);
  auto *e = MakeIntInExpr(kOpInt4Eq, v, {1, 2, 3});
  pax::pax_enable_fast_filter = false;
  EXPECT_FALSE(paxc::PaxCanFastFilter((Node *)e, desc));
  pax::pax_enable_fast_filter = true;
}

// ---------------------------------------------------------------------------
// BuildFastFilter — full FastFilterDesc construction.
// ---------------------------------------------------------------------------

TEST_F(PaxFastFilterTest, BuildIntFilterDesc) {
  TupleDesc desc = BuildTupleDesc({kAttInt4});
  Relation rel = MakeFakeRelation(desc);

  auto *op = MakeOpExpr(kOpInt4Eq, MakeVar(desc, 1),
                        MakeIntConst(INT4OID, 42));
  List *qual = list_make1(op);

  FastFilterContext ctx;
  std::vector<AttrNumber> attnos;
  EXPECT_TRUE(pax::BuildFastFilter(rel, qual, &ctx, &attnos));
  ASSERT_EQ(ctx.filters.size(), 1U);
  EXPECT_EQ(ctx.filters[0].attno, 1);
  EXPECT_EQ(ctx.filters[0].op, FastFilterOp::kEqual);
  EXPECT_EQ(ctx.filters[0].type_len, 4);
  EXPECT_EQ(DatumGetInt32(ctx.filters[0].const_value), 42);
  ASSERT_EQ(attnos.size(), 1U);
  EXPECT_EQ(attnos[0], 1);
}

TEST_F(PaxFastFilterTest, BuildCrossTypeIntCastsConst) {
  // int4 column = int8 constant.  Construction must cast the const down to
  // int4 (the column's storage width).
  TupleDesc desc = BuildTupleDesc({kAttInt4});
  Relation rel = MakeFakeRelation(desc);

  auto *op = MakeOpExpr(kOpInt48Eq, MakeVar(desc, 1),
                        MakeIntConst(INT8OID, 0x100000007LL));
  List *qual = list_make1(op);

  FastFilterContext ctx;
  std::vector<AttrNumber> attnos;
  EXPECT_TRUE(pax::BuildFastFilter(rel, qual, &ctx, &attnos));
  ASSERT_EQ(ctx.filters.size(), 1U);
  EXPECT_EQ(ctx.filters[0].type_len, 4);
  // Cast to int4 truncates the high bits: 0x100000007 -> 0x7.
  EXPECT_EQ(DatumGetInt32(ctx.filters[0].const_value), 7);
}

TEST_F(PaxFastFilterTest, BuildLikeFilterDescNeedle) {
  TupleDesc desc = BuildTupleDesc({kAttText});
  Relation rel = MakeFakeRelation(desc);

  auto *op = MakeOpExpr(kOpTextLike, MakeVar(desc, 1),
                        MakeTextConst("%hello%"));
  List *qual = list_make1(op);

  FastFilterContext ctx;
  std::vector<AttrNumber> attnos;
  EXPECT_TRUE(pax::BuildFastFilter(rel, qual, &ctx, &attnos));
  ASSERT_EQ(ctx.filters.size(), 1U);
  EXPECT_EQ(ctx.filters[0].op, FastFilterOp::kStrContains);
  EXPECT_EQ(ctx.filters[0].type_len, -1);
  ASSERT_NE(ctx.filters[0].str_const, nullptr);
  EXPECT_EQ(ctx.filters[0].str_const_len, 5);
  EXPECT_EQ(std::memcmp(ctx.filters[0].str_const, "hello", 5), 0);
}

TEST_F(PaxFastFilterTest, BuildInListCollectsNonNullValues) {
  TupleDesc desc = BuildTupleDesc({kAttInt4});
  Relation rel = MakeFakeRelation(desc);

  auto *e = MakeIntInExpr(kOpInt4Eq, MakeVar(desc, 1), {10, 20, 30});
  List *qual = list_make1(e);

  FastFilterContext ctx;
  std::vector<AttrNumber> attnos;
  EXPECT_TRUE(pax::BuildFastFilter(rel, qual, &ctx, &attnos));
  ASSERT_EQ(ctx.filters.size(), 1U);
  const auto &f = ctx.filters[0];
  EXPECT_EQ(f.op, FastFilterOp::kInValues);
  ASSERT_EQ(f.in_count, 3);
  EXPECT_EQ(DatumGetInt32(f.in_values[0]), 10);
  EXPECT_EQ(DatumGetInt32(f.in_values[1]), 20);
  EXPECT_EQ(DatumGetInt32(f.in_values[2]), 30);
}

TEST_F(PaxFastFilterTest, BuildSortsAllFiltersOfSameAttnoTogether) {
  // Qual: col1 > 5  AND  col2 = 1  AND  col1 < 10
  // All three are integer filters, so the type-partition comparator returns
  // false on every pair and the SQL-original order would be [col1>5, col2=1,
  // col1<10] — leaving col1's two filters non-adjacent.  InitColumnGroups
  // only merges consecutive same-attno descriptors, so without a secondary
  // sort it would create THREE ColumnGroupDescs (col1, col2, col1 again).
  // With the attno secondary sort, the two col1 filters group together and
  // we get TWO descriptors (col1 with 2 filters, col2 with 1 filter).
  TupleDesc desc = BuildTupleDesc({kAttInt4, kAttInt4});
  Relation rel = MakeFakeRelation(desc);

  auto *q1 = MakeOpExpr(kOpInt4Gt, MakeVar(desc, 1),
                        MakeIntConst(INT4OID, 5));
  auto *q2 = MakeOpExpr(kOpInt4Eq, MakeVar(desc, 2),
                        MakeIntConst(INT4OID, 1));
  auto *q3 = MakeOpExpr(kOpInt4Lt, MakeVar(desc, 1),
                        MakeIntConst(INT4OID, 10));
  List *qual = list_make3(q1, q2, q3);

  FastFilterContext ctx;
  EXPECT_TRUE(pax::BuildFastFilter(rel, qual, &ctx, nullptr));
  ASSERT_EQ(ctx.filters.size(), 3U);
  // After sort: [col1>5, col1<10, col2=1].  The two col1 filters are
  // adjacent; the SQL order within col1's pair is preserved by stable_sort.
  EXPECT_EQ(ctx.filters[0].attno, 1);
  EXPECT_EQ(ctx.filters[0].op,    FastFilterOp::kGreater);
  EXPECT_EQ(ctx.filters[1].attno, 1);
  EXPECT_EQ(ctx.filters[1].op,    FastFilterOp::kLess);
  EXPECT_EQ(ctx.filters[2].attno, 2);
  EXPECT_EQ(ctx.filters[2].op,    FastFilterOp::kEqual);
}

TEST_F(PaxFastFilterTest, BuildSortsIntegersBeforeStrings) {
  // Order in qual list:  text LIKE '%a%' ,  int4 = 1 ,  text <> '' ,  int4 < 2
  // Expected eval order: int filters first, then string filters; within each
  // type group, attno-ascending (then SQL order on ties).
  TupleDesc desc = BuildTupleDesc({kAttInt4, kAttText});
  Relation rel = MakeFakeRelation(desc);

  auto *q1 = MakeOpExpr(kOpTextLike, MakeVar(desc, 2),
                        MakeTextConst("%a%"));
  auto *q2 = MakeOpExpr(kOpInt4Eq, MakeVar(desc, 1),
                        MakeIntConst(INT4OID, 1));
  auto *q3 = MakeOpExpr(kOpTextNe, MakeVar(desc, 2),
                        MakeTextConst(""));
  auto *q4 = MakeOpExpr(kOpInt4Lt, MakeVar(desc, 1),
                        MakeIntConst(INT4OID, 2));
  List *qual = list_make4(q1, q2, q3, q4);

  FastFilterContext ctx;
  std::vector<AttrNumber> attnos;
  EXPECT_TRUE(pax::BuildFastFilter(rel, qual, &ctx, &attnos));
  ASSERT_EQ(ctx.filters.size(), 4U);
  // [0,1] integers (in original SQL order: q2 then q4), [2,3] strings (q1 then q3)
  EXPECT_EQ(ctx.filters[0].op, FastFilterOp::kEqual);
  EXPECT_EQ(ctx.filters[1].op, FastFilterOp::kLess);
  EXPECT_TRUE(ctx.filters[2].IsStringFilter());
  EXPECT_TRUE(ctx.filters[3].IsStringFilter());
}

TEST_F(PaxFastFilterTest, BuildUnwrapsTopLevelAnd) {
  // qual is wrapped in BoolExpr(AND, [int=1, int<2]); BuildFastFilter must
  // flatten and produce two filters.
  TupleDesc desc = BuildTupleDesc({kAttInt4});
  Relation rel = MakeFakeRelation(desc);

  auto *q1 = MakeOpExpr(kOpInt4Eq, MakeVar(desc, 1),
                        MakeIntConst(INT4OID, 5));
  auto *q2 = MakeOpExpr(kOpInt4Lt, MakeVar(desc, 1),
                        MakeIntConst(INT4OID, 100));
  auto *and_node = makeNode(BoolExpr);
  and_node->boolop = AND_EXPR;
  and_node->args = list_make2(q1, q2);
  List *qual = list_make1(and_node);

  FastFilterContext ctx;
  EXPECT_TRUE(pax::BuildFastFilter(rel, qual, &ctx, nullptr));
  ASSERT_EQ(ctx.filters.size(), 2U);
}

TEST_F(PaxFastFilterTest, BuildTopLevelOr) {
  // An OR on one column is a range set; an OR across columns cannot be one
  // filter per column and is rejected.
  TupleDesc desc = BuildTupleDesc({kAttInt4, kAttInt4});
  Relation rel = MakeFakeRelation(desc);

  auto *q1 = MakeOpExpr(kOpInt4Eq, MakeVar(desc, 1),
                        MakeIntConst(INT4OID, 5));
  auto *q2 = MakeOpExpr(kOpInt4Eq, MakeVar(desc, 1),
                        MakeIntConst(INT4OID, 6));
  auto *or_node = makeNode(BoolExpr);
  or_node->boolop = OR_EXPR;
  or_node->args = list_make2(q1, q2);

  FastFilterContext ctx;
  EXPECT_TRUE(pax::BuildFastFilter(rel, list_make1(or_node), &ctx, nullptr));
  ASSERT_EQ(ctx.filters.size(), 1U);
  EXPECT_EQ(ctx.filters[0].op, FastFilterOp::kRangeSet);
  ASSERT_EQ(ctx.filters[0].range_count, 1);
  EXPECT_EQ(ctx.filters[0].ranges[0], 5);
  EXPECT_EQ(ctx.filters[0].ranges[1], 6);

  auto *q3 = MakeOpExpr(kOpInt4Eq, MakeVar(desc, 2),
                        MakeIntConst(INT4OID, 6));
  auto *or2 = makeNode(BoolExpr);
  or2->boolop = OR_EXPR;
  or2->args = list_make2(q1, q3);
  FastFilterContext ctx2;
  EXPECT_FALSE(pax::BuildFastFilter(rel, list_make1(or2), &ctx2, nullptr));
  EXPECT_TRUE(ctx2.filters.empty());
  EXPECT_FALSE(paxc::PaxCanFastFilter((Node *)or2, desc));
}

TEST_F(PaxFastFilterTest, BuildSkipsNonClassifiable) {
  // Mix: one fast-filterable (int=1), one float= which is rejected.  Only
  // the int filter ends up in ctx.
  TupleDesc desc = BuildTupleDesc({kAttInt4, kAttFloat4});
  Relation rel = MakeFakeRelation(desc);

  auto *q1 = MakeOpExpr(kOpInt4Eq, MakeVar(desc, 1),
                        MakeIntConst(INT4OID, 1));
  auto *q2 = MakeOpExpr(kOpFloat4Eq, MakeVar(desc, 2),
                        MakeIntConst(INT4OID, 0));
  List *qual = list_make2(q1, q2);

  FastFilterContext ctx;
  std::vector<AttrNumber> attnos;
  EXPECT_TRUE(pax::BuildFastFilter(rel, qual, &ctx, &attnos));
  ASSERT_EQ(ctx.filters.size(), 1U);
  EXPECT_EQ(ctx.filters[0].attno, 1);
  ASSERT_EQ(attnos.size(), 1U);
  EXPECT_EQ(attnos[0], 1);
}

// ---------------------------------------------------------------------------
// BuildProjectionMasks — input/output combinations.
// ---------------------------------------------------------------------------

TEST_F(PaxFastFilterTest, ProjMaskEmptyMeansReadAll) {
  // empty projection vector → treat as "read all", so remaining_proj has
  // every column except filter columns.
  std::vector<bool> proj;          // empty
  std::vector<AttrNumber> all_f{1};
  std::vector<AttrNumber> fast_f{1};

  std::vector<bool> ff_proj, rem_proj;
  bool has_rem = false;
  pax::BuildProjectionMasks(3, proj, all_f, fast_f, &ff_proj, &rem_proj,
                            &has_rem);

  ASSERT_EQ(ff_proj.size(), 3U);
  EXPECT_TRUE(ff_proj[0]);
  EXPECT_FALSE(ff_proj[1]);
  EXPECT_FALSE(ff_proj[2]);

  ASSERT_EQ(rem_proj.size(), 3U);
  EXPECT_FALSE(rem_proj[0]);  // filter col excluded
  EXPECT_TRUE(rem_proj[1]);
  EXPECT_TRUE(rem_proj[2]);

  EXPECT_TRUE(has_rem);
}

TEST_F(PaxFastFilterTest, ProjMaskNoRemainingColumns) {
  // projection = {col1}; filter = {col1}; remaining_proj should be all false.
  std::vector<bool> proj{true, false, false};
  std::vector<AttrNumber> all_f{1};
  std::vector<AttrNumber> fast_f{1};

  std::vector<bool> ff_proj, rem_proj;
  bool has_rem = false;
  pax::BuildProjectionMasks(3, proj, all_f, fast_f, &ff_proj, &rem_proj,
                            &has_rem);

  EXPECT_FALSE(rem_proj[0]);
  EXPECT_FALSE(rem_proj[1]);
  EXPECT_FALSE(rem_proj[2]);
  EXPECT_FALSE(has_rem);
}

TEST_F(PaxFastFilterTest, ProjMaskMixedColumns) {
  // 5 columns; project {1,3,5}; filter {1,3}.  Phase 2 (remaining) only reads
  // col 5; Phase 1 reads cols 1 and 3.
  std::vector<bool> proj{true, false, true, false, true};
  std::vector<AttrNumber> all_f{1, 3};
  std::vector<AttrNumber> fast_f{1, 3};

  std::vector<bool> ff_proj, rem_proj;
  bool has_rem = false;
  pax::BuildProjectionMasks(5, proj, all_f, fast_f, &ff_proj, &rem_proj,
                            &has_rem);

  EXPECT_TRUE (ff_proj[0]);
  EXPECT_FALSE(ff_proj[1]);
  EXPECT_TRUE (ff_proj[2]);
  EXPECT_FALSE(ff_proj[3]);
  EXPECT_FALSE(ff_proj[4]);

  EXPECT_FALSE(rem_proj[0]);
  EXPECT_FALSE(rem_proj[1]);
  EXPECT_FALSE(rem_proj[2]);
  EXPECT_FALSE(rem_proj[3]);
  EXPECT_TRUE (rem_proj[4]);

  EXPECT_TRUE(has_rem);
}

// ---------------------------------------------------------------------------
// PaxFastFilter::Initialize — top-level integration with Relation+projection.
// ---------------------------------------------------------------------------

TEST_F(PaxFastFilterTest, InitializeWiresEverythingTogether) {
  TupleDesc desc = BuildTupleDesc({kAttInt4, kAttText});
  Relation rel = MakeFakeRelation(desc);

  auto *q1 = MakeOpExpr(kOpInt4Eq, MakeVar(desc, 1),
                        MakeIntConst(INT4OID, 1));
  auto *q2 = MakeOpExpr(kOpTextLike, MakeVar(desc, 2),
                        MakeTextConst("%foo%"));
  List *qual = list_make2(q1, q2);

  std::vector<bool> proj{true, true};  // read both columns

  PaxFastFilter ff;
  EXPECT_TRUE(ff.Initialize(rel, qual, proj));
  EXPECT_TRUE(ff.HasFastFilter());
  EXPECT_EQ(ff.GetFastFilterContext()->filters.size(), 2U);
  // Both columns are filter columns; remaining_proj must be empty.
  EXPECT_FALSE(ff.HasRemainingColumns());
}

TEST_F(PaxFastFilterTest, InitializeReturnsFalseWhenNoFastQual) {
  // Single float= qual: not fast-filterable.  Initialize should return false.
  TupleDesc desc = BuildTupleDesc({kAttFloat4});
  Relation rel = MakeFakeRelation(desc);

  auto *q = MakeOpExpr(kOpFloat4Eq, MakeVar(desc, 1),
                       MakeIntConst(INT4OID, 0));
  List *qual = list_make1(q);

  PaxFastFilter ff;
  EXPECT_FALSE(ff.Initialize(rel, qual, /*proj=*/{}));
  EXPECT_FALSE(ff.HasFastFilter());
}

// ---------------------------------------------------------------------------
// Review #2: FastFilterContext::id and ABA-safe cache keying.
//
// Every successfully-initialized PaxFastFilter must receive a unique,
// process-monotonic id.  Thread-local caches in pax_vec_reader.cc key off
// this id to avoid silently reusing stale state when a new PaxFastFilter
// is allocated at the same heap address as a recently-destroyed one.
// ---------------------------------------------------------------------------

TEST_F(PaxFastFilterTest, InitializeAssignsNonZeroId) {
  TupleDesc desc = BuildTupleDesc({kAttInt4});
  Relation rel = MakeFakeRelation(desc);
  auto *q = MakeOpExpr(kOpInt4Eq, MakeVar(desc, 1),
                       MakeIntConst(INT4OID, 1));
  PaxFastFilter ff;
  ASSERT_TRUE(ff.Initialize(rel, list_make1(q), /*proj=*/{}));
  EXPECT_NE(ff.GetFastFilterContext()->id, 0U);
}

TEST_F(PaxFastFilterTest, InitializeAssignsDistinctIdsAcrossInstances) {
  TupleDesc desc = BuildTupleDesc({kAttInt4});
  Relation rel = MakeFakeRelation(desc);
  auto *q1 = MakeOpExpr(kOpInt4Eq, MakeVar(desc, 1),
                        MakeIntConst(INT4OID, 1));
  auto *q2 = MakeOpExpr(kOpInt4Eq, MakeVar(desc, 1),
                        MakeIntConst(INT4OID, 2));
  PaxFastFilter ff1, ff2;
  ASSERT_TRUE(ff1.Initialize(rel, list_make1(q1), /*proj=*/{}));
  ASSERT_TRUE(ff2.Initialize(rel, list_make1(q2), /*proj=*/{}));
  EXPECT_NE(ff1.GetFastFilterContext()->id,
            ff2.GetFastFilterContext()->id);
}

TEST_F(PaxFastFilterTest, InitializeFailureDoesNotConsumeId) {
  // A PaxFastFilter that fails Initialize (no fast-filterable qual) must
  // not increment the id counter, otherwise we burn through ids on every
  // unrelated scan.
  TupleDesc desc_f = BuildTupleDesc({kAttFloat4});
  Relation rel_f = MakeFakeRelation(desc_f);
  PaxFastFilter ff_bad;
  auto *q_bad = MakeOpExpr(kOpFloat4Eq, MakeVar(desc_f, 1),
                           MakeIntConst(INT4OID, 0));
  ASSERT_FALSE(ff_bad.Initialize(rel_f, list_make1(q_bad), /*proj=*/{}));

  // The next successful Initialize gets some id; if the prior failed one
  // had consumed an id we'd expect the gap to be > 1 — but we can't observe
  // that directly without two valid initializations bracketing the failed
  // one.  Take that route: id_before, then failed init, then id_after.
  TupleDesc desc = BuildTupleDesc({kAttInt4});
  Relation rel = MakeFakeRelation(desc);
  PaxFastFilter ff_before, ff_after;
  auto *q_before = MakeOpExpr(kOpInt4Eq, MakeVar(desc, 1),
                              MakeIntConst(INT4OID, 1));
  auto *q_after  = MakeOpExpr(kOpInt4Eq, MakeVar(desc, 1),
                              MakeIntConst(INT4OID, 2));
  ASSERT_TRUE(ff_before.Initialize(rel, list_make1(q_before), /*proj=*/{}));
  uint64_t id_before = ff_before.GetFastFilterContext()->id;

  PaxFastFilter ff_bad2;
  auto *q_bad2 = MakeOpExpr(kOpFloat4Eq, MakeVar(desc_f, 1),
                            MakeIntConst(INT4OID, 0));
  ASSERT_FALSE(ff_bad2.Initialize(rel_f, list_make1(q_bad2), /*proj=*/{}));

  ASSERT_TRUE(ff_after.Initialize(rel, list_make1(q_after), /*proj=*/{}));
  uint64_t id_after = ff_after.GetFastFilterContext()->id;

  EXPECT_EQ(id_after, id_before + 1)
      << "Failed Initialize must not consume an id";
}

// ---------------------------------------------------------------------------
// Numeric keys and range sets (kRangeSet).
// ---------------------------------------------------------------------------

static Numeric NumericFromText(const char *text) {
  return DatumGetNumeric(DirectFunctionCall3(numeric_in, CStringGetDatum(text),
                                             ObjectIdGetDatum(InvalidOid),
                                             Int32GetDatum(-1)));
}

static int64 KeyOf(const char *text, int scale) {
  Numeric n = NumericFromText(text);
  return pax::NumericKeyFromPgNumeric(VARDATA(n), VARSIZE(n) - VARHDRSZ, scale);
}

TEST_F(PaxFastFilterTest, NumericKeyValues) {
  EXPECT_EQ(KeyOf("0", 2), 0);
  EXPECT_EQ(KeyOf("-0.00", 2), 0);
  EXPECT_EQ(KeyOf("12.34", 2), 2 * 1234);
  EXPECT_EQ(KeyOf("-12.34", 2), -2 * 1234);
  EXPECT_EQ(KeyOf("100", 2), 2 * 10000);
  EXPECT_EQ(KeyOf("99999.99", 2), 2 * 9999999);
  EXPECT_EQ(KeyOf("0.000001", 6), 2);
  EXPECT_EQ(KeyOf("123456789012.345678", 6), 2 * 123456789012345678LL);
  // More digits than the scale: odd keys between the neighbours.
  EXPECT_EQ(KeyOf("1.005", 2), 2 * 100 + 1);
  EXPECT_EQ(KeyOf("-1.005", 2), -(2 * 100 + 1));
  EXPECT_EQ(KeyOf("0.001", 2), 1);
  EXPECT_EQ(KeyOf("0.00000001", 2), 1);
  // Special values and saturation.
  EXPECT_EQ(KeyOf("NaN", 2), numeric_key::kNaN);
  EXPECT_EQ(KeyOf("Infinity", 2), numeric_key::kPosInf);
  EXPECT_EQ(KeyOf("-Infinity", 2), numeric_key::kNegInf);
  EXPECT_EQ(KeyOf("1e30", 2), numeric_key::kPosHuge);
  EXPECT_EQ(KeyOf("-1e30", 2), numeric_key::kNegHuge);
  // Weight beyond the short header: long-format datum.
  EXPECT_EQ(KeyOf("1e300", 0), numeric_key::kPosHuge);
  EXPECT_EQ(KeyOf("-1e300", 0), numeric_key::kNegHuge);
}

// Keys must order exactly like numeric_cmp(), for every pair.
TEST_F(PaxFastFilterTest, NumericKeyOrderMatchesNumericCmp) {
  std::vector<std::string> values = {
      "NaN", "Infinity", "-Infinity", "0", "-0", "1e30", "-1e30", "1e19",
      "0.001", "-0.001", "0.005", "0.01", "-0.01", "1.004", "1.005", "1.01",
      "99999.99", "-99999.99", "99999.995", "12.34", "12.345", "12.3401"};
  std::mt19937_64 rng(42);
  for (int i = 0; i < 400; i++) {
    int64 v = static_cast<int64>(rng() % 20000001) - 10000000;
    int digits = static_cast<int>(rng() % 5);  // 0..4 fractional digits
    char buf[64];
    int64 p = 1;
    for (int k = 0; k < digits; k++) p *= 10;
    if (digits == 0)
      snprintf(buf, sizeof(buf), "%lld", (long long)v);
    else
      snprintf(buf, sizeof(buf), "%s%lld.%0*lld", v < 0 ? "-" : "",
               (long long)(std::llabs(v) / p), digits,
               (long long)(std::llabs(v) % p));
    values.emplace_back(buf);
  }
  for (int scale : {0, 2, 3, 6}) {
    std::vector<int64> keys;
    std::vector<Numeric> nums;
    for (const auto &t : values) {
      keys.push_back(KeyOf(t.c_str(), scale));
      nums.push_back(NumericFromText(t.c_str()));
    }
    for (size_t a = 0; a < values.size(); a++) {
      for (size_t b = 0; b < values.size(); b++) {
        int cmp = DatumGetInt32(DirectFunctionCall2(
            numeric_cmp, NumericGetDatum(nums[a]), NumericGetDatum(nums[b])));
        int kcmp = keys[a] < keys[b] ? -1 : (keys[a] > keys[b] ? 1 : 0);
        // Values inside the same gap between two values exact at the scale
        // (odd keys) share a key, as do saturated values; both only need to
        // order against the values around them.
        bool huge = keys[a] == numeric_key::kPosHuge ||
                    keys[a] == numeric_key::kNegHuge;
        if (keys[a] == keys[b] && ((keys[a] & 1) != 0 || huge)) continue;
        ASSERT_EQ(kcmp, cmp < 0 ? -1 : (cmp > 0 ? 1 : 0))
            << values[a] << " vs " << values[b] << " at scale " << scale;
      }
    }
  }
}

// The vec numeric decoder gives the same keys as the PostgreSQL one.
TEST_F(PaxFastFilterTest, NumericKeyFromVecNumericMatches) {
  std::mt19937_64 rng(7);
  for (int i = 0; i < 2000; i++) {
    int digits = static_cast<int>(rng() % 7);
    int64 v = static_cast<int64>(rng() % 2000000000001LL) - 1000000000000LL;
    int64 p = 1;
    for (int k = 0; k < digits; k++) p *= 10;
    char buf[64];
    if (digits == 0)
      snprintf(buf, sizeof(buf), "%lld", (long long)v);
    else
      snprintf(buf, sizeof(buf), "%s%lld.%0*lld", v < 0 ? "-" : "",
               (long long)(std::llabs(v) / p), digits,
               (long long)(std::llabs(v) % p));
    Numeric n = NumericFromText(buf);
    int64 low, high;
    pg_short_numeric_to_vec_short_numeric(n, VARSIZE(n) - VARHDRSZ, &low,
                                          &high);
    for (int scale : {0, 2, 6}) {
      ASSERT_EQ(pax::NumericKeyFromVecNumeric(low, high, scale),
                pax::NumericKeyFromPgNumeric(VARDATA(n), VARSIZE(n) - VARHDRSZ,
                                             scale))
          << buf << " at scale " << scale;
    }
  }
  for (const char *special : {"NaN", "Infinity", "-Infinity"}) {
    Numeric n = NumericFromText(special);
    int64 low, high;
    pg_short_numeric_to_vec_short_numeric(n, VARSIZE(n) - VARHDRSZ, &low,
                                          &high);
    EXPECT_EQ(pax::NumericKeyFromVecNumeric(low, high, 2), KeyOf(special, 2))
        << special;
  }
}

TEST_F(PaxFastFilterTest, KeyInRanges) {
  const int64 r[] = {-10, -5, 0, 0, 7, 9};
  EXPECT_FALSE(KeyInRanges(-11, r, 3));
  EXPECT_TRUE(KeyInRanges(-10, r, 3));
  EXPECT_TRUE(KeyInRanges(-5, r, 3));
  EXPECT_FALSE(KeyInRanges(-1, r, 3));
  EXPECT_TRUE(KeyInRanges(0, r, 3));
  EXPECT_FALSE(KeyInRanges(1, r, 3));
  EXPECT_TRUE(KeyInRanges(8, r, 3));
  EXPECT_FALSE(KeyInRanges(10, r, 3));
  EXPECT_FALSE(KeyInRanges(0, r, 0));  // empty set
}

// q13: an OR of three adjacent BETWEENs collapses into one range.
TEST_F(PaxFastFilterTest, BuildNumericQ13Ranges) {
  TupleDesc desc = BuildTupleDesc({kAttNumeric72});
  Var *a = MakeVar(desc, 1);
  Node *qual = MakeOr({NumericBetween(a, "100.00", "150.00"),
                       NumericBetween(a, "50.00", "100.00"),
                       NumericBetween(a, "150.00", "200.00")});
  EXPECT_TRUE(paxc::PaxCanFastFilter(qual, desc));
  auto ranges = BuildRanges(desc, qual);
  ASSERT_EQ(ranges.size(), 1U);
  EXPECT_EQ(ranges[0].first, 2 * 5000);
  EXPECT_EQ(ranges[0].second, 2 * 20000);
}

TEST_F(PaxFastFilterTest, BuildNumericComparisonRanges) {
  TupleDesc desc = BuildTupleDesc({kAttNumeric72});
  Var *a = MakeVar(desc, 1);
  using numeric_key::kNaN;
  using numeric_key::kNegInf;
  const int64 c = 2 * 1234;  // 12.34
  struct {
    Oid func;
    bool var_left;
    std::vector<std::pair<int64, int64>> expected;
  } cases[] = {
      {F_NUMERIC_EQ, true, {{c, c}}},
      {F_NUMERIC_NE, true, {{kNegInf, c - 1}, {c + 1, kNaN}}},
      {F_NUMERIC_LT, true, {{kNegInf, c - 1}}},
      {F_NUMERIC_LE, true, {{kNegInf, c}}},
      {F_NUMERIC_GT, true, {{c + 1, kNaN}}},
      {F_NUMERIC_GE, true, {{c, kNaN}}},
      // 12.34 < a  is  a > 12.34
      {F_NUMERIC_LT, false, {{c + 1, kNaN}}},
      {F_NUMERIC_GE, false, {{kNegInf, c}}},
  };
  for (const auto &t : cases) {
    Node *qual = (Node *)MakeNumericOpExpr(t.func, a, MakeNumericConst("12.34"),
                                           t.var_left);
    EXPECT_TRUE(paxc::PaxCanFastFilter(qual, desc)) << t.func;
    EXPECT_EQ(BuildRanges(desc, qual), t.expected) << t.func;
  }
}

TEST_F(PaxFastFilterTest, BuildNumericNestedRanges) {
  TupleDesc desc = BuildTupleDesc({kAttNumeric72});
  Var *a = MakeVar(desc, 1);
  // (((a > 0 AND a < 10) OR a > 100) AND a <> 150) OR a = -1; a top-level
  // AND would be split into one filter per conjunct, so wrap it in an OR.
  Node *qual = MakeOr(
      {MakeAnd({MakeOr({MakeAnd({MakeNumericOpExpr(F_NUMERIC_GT, a,
                                                   MakeNumericConst("0")),
                                 MakeNumericOpExpr(F_NUMERIC_LT, a,
                                                   MakeNumericConst("10"))}),
                        MakeNumericOpExpr(F_NUMERIC_GT, a,
                                          MakeNumericConst("100"))}),
                MakeNumericOpExpr(F_NUMERIC_NE, a, MakeNumericConst("150"))}),
       MakeNumericOpExpr(F_NUMERIC_EQ, a, MakeNumericConst("-1"))});
  std::vector<std::pair<int64, int64>> expected = {
      {-2 * 100, -2 * 100},
      {1, 2 * 1000 - 1},
      {2 * 10000 + 1, 2 * 15000 - 1},
      {2 * 15000 + 1, numeric_key::kNaN}};
  EXPECT_EQ(BuildRanges(desc, qual), expected);
  // (a < 10 AND a > 20) OR a = 50: the AND is empty.
  Node *qual2 = MakeOr(
      {MakeAnd({MakeNumericOpExpr(F_NUMERIC_LT, a, MakeNumericConst("10")),
                MakeNumericOpExpr(F_NUMERIC_GT, a, MakeNumericConst("20"))}),
       MakeNumericOpExpr(F_NUMERIC_EQ, a, MakeNumericConst("50"))});
  std::vector<std::pair<int64, int64>> expected2 = {{2 * 5000, 2 * 5000}};
  EXPECT_EQ(BuildRanges(desc, qual2), expected2);
}

TEST_F(PaxFastFilterTest, RejectNumericRangeSets) {
  TupleDesc desc = BuildTupleDesc({kAttNumeric72, kAttNumeric72, kAttNumeric192,
                                   kAttNumericFree, kAttInt4});
  Var *a = MakeVar(desc, 1);
  Var *b = MakeVar(desc, 2);
  // Constant not exact at the column scale, or beyond its precision.
  EXPECT_FALSE(paxc::PaxCanFastFilter(
      (Node *)MakeNumericOpExpr(F_NUMERIC_GT, a, MakeNumericConst("1.005")),
      desc));
  EXPECT_FALSE(paxc::PaxCanFastFilter(
      (Node *)MakeNumericOpExpr(F_NUMERIC_LT, a, MakeNumericConst("1e20")),
      desc));
  EXPECT_FALSE(paxc::PaxCanFastFilter(
      (Node *)MakeNumericOpExpr(F_NUMERIC_EQ, a, MakeNumericConst("NaN")),
      desc));
  // Two columns in one OR.
  EXPECT_FALSE(paxc::PaxCanFastFilter(
      MakeOr({MakeNumericOpExpr(F_NUMERIC_EQ, a, MakeNumericConst("1")),
              MakeNumericOpExpr(F_NUMERIC_EQ, b, MakeNumericConst("2"))}),
      desc));
  // NOT is not a range set.
  EXPECT_FALSE(paxc::PaxCanFastFilter(
      (Node *)makeBoolExpr(NOT_EXPR,
                           list_make1(MakeNumericOpExpr(F_NUMERIC_EQ, a,
                                                        MakeNumericConst("1"))),
                           -1),
      desc));
  // Precision above 18, or no typmod at all.
  EXPECT_FALSE(paxc::PaxCanFastFilter(
      (Node *)MakeNumericOpExpr(F_NUMERIC_EQ, MakeVar(desc, 3),
                                MakeNumericConst("1")),
      desc));
  EXPECT_FALSE(paxc::PaxCanFastFilter(
      (Node *)MakeNumericOpExpr(F_NUMERIC_EQ, MakeVar(desc, 4),
                                MakeNumericConst("1")),
      desc));
  // A function other than the numeric comparisons.
  EXPECT_FALSE(paxc::PaxCanFastFilter(
      (Node *)MakeNumericOpExpr(F_NUMERIC_ADD, a, MakeNumericConst("1")),
      desc));
}

TEST_F(PaxFastFilterTest, BuildIntegerOrRanges) {
  TupleDesc desc = BuildTupleDesc({kAttInt4, kAttInt8});
  Var *c = MakeVar(desc, 1);
  Var *g = MakeVar(desc, 2);
  // c = 1 OR c = 3
  Node *q1 = MakeOr({MakeOpExpr(kOpInt4Eq, c, MakeIntConst(INT4OID, 1)),
                     MakeOpExpr(kOpInt4Eq, c, MakeIntConst(INT4OID, 3))});
  EXPECT_TRUE(paxc::PaxCanFastFilter(q1, desc));
  std::vector<std::pair<int64, int64>> e1 = {{1, 1}, {3, 3}};
  EXPECT_EQ(BuildRanges(desc, q1), e1);
  // c < 2 OR c > 8 (and 2 > c, the constant on the left)
  Node *q2 = MakeOr({MakeOpExpr(kOpInt4Gt, c, MakeIntConst(INT4OID, 2), false),
                     MakeOpExpr(kOpInt4Gt, c, MakeIntConst(INT4OID, 8))});
  std::vector<std::pair<int64, int64>> e2 = {{INT64_MIN, 1}, {9, INT64_MAX}};
  EXPECT_EQ(BuildRanges(desc, q2), e2);
  // c = 4 OR c = 5 OR c = 6: adjacent points merge
  Node *q3 = MakeOr({MakeOpExpr(kOpInt4Eq, c, MakeIntConst(INT4OID, 4)),
                     MakeOpExpr(kOpInt4Eq, c, MakeIntConst(INT4OID, 5)),
                     MakeOpExpr(kOpInt4Eq, c, MakeIntConst(INT4OID, 6))});
  std::vector<std::pair<int64, int64>> e3 = {{4, 6}};
  EXPECT_EQ(BuildRanges(desc, q3), e3);
  // int8 at the ends of its range: no overflow at c - 1 / c + 1.
  Node *q4 =
      MakeOr({MakeOpExpr(kOpInt8Eq, g, MakeIntConst(INT8OID, INT64_MAX)),
              MakeOpExpr(kOpInt8Eq, g, MakeIntConst(INT8OID, INT64_MIN))});
  std::vector<std::pair<int64, int64>> e4 = {{INT64_MIN, INT64_MIN},
                                             {INT64_MAX, INT64_MAX}};
  EXPECT_EQ(BuildRanges(desc, q4), e4);
  // A lone integer comparison stays with the plain integer filter.
  Relation rel = MakeFakeRelation(desc);
  FastFilterContext ctx;
  std::vector<AttrNumber> attnos;
  EXPECT_TRUE(pax::BuildFastFilter(
      rel, list_make1(MakeOpExpr(kOpInt4Eq, c, MakeIntConst(INT4OID, 1))), &ctx,
      &attnos));
  ASSERT_EQ(ctx.filters.size(), 1U);
  EXPECT_EQ(ctx.filters[0].op, FastFilterOp::kEqual);
}

// The scan qual is one OR: BuildFastFilter must take it, since
// PaxCanFastFilter tells the vectorized scan to drop it.
TEST_F(PaxFastFilterTest, BuildSingleOrQual) {
  TupleDesc desc = BuildTupleDesc({kAttNumeric72});
  Var *a = MakeVar(desc, 1);
  Node *qual =
      MakeOr({NumericBetween(a, "-10", "-5"), NumericBetween(a, "5", "10")});
  EXPECT_TRUE(paxc::PaxCanFastFilter(qual, desc));
  auto ranges = BuildRanges(desc, qual);
  EXPECT_EQ(ranges.size(), 2U);
}

TEST_F(PaxFastFilterTest, RejectPredicatePushdownOff) {
  TupleDesc desc = BuildTupleDesc({kAttInt4, kAttNumeric72});
  auto *op = MakeOpExpr(kOpInt4Eq, MakeVar(desc, 1), MakeIntConst(INT4OID, 42));
  Node *num = (Node *)MakeNumericOpExpr(F_NUMERIC_EQ, MakeVar(desc, 2),
                                        MakeNumericConst("1"));
  gp_enable_predicate_pushdown = false;
  EXPECT_FALSE(paxc::PaxCanFastFilter((Node *)op, desc));
  EXPECT_FALSE(paxc::PaxCanFastFilter(num, desc));
  gp_enable_predicate_pushdown = true;
  EXPECT_TRUE(paxc::PaxCanFastFilter((Node *)op, desc));
  EXPECT_TRUE(paxc::PaxCanFastFilter(num, desc));
}

// Columns missing from older files read as their missing value: each filter
// records whether that value passes it.
TEST_F(PaxFastFilterTest, BuildRecordsWhetherMissingValuePasses) {
  TupleDesc desc = BuildTupleDesc({kAttInt4, kAttInt4, kAttText});
  // Column 1 has missing value 5, column 2 none, column 3 'abc'.
  auto *constr =
      reinterpret_cast<TupleConstr *>(cbdb::Palloc0(sizeof(TupleConstr)));
  auto *missing =
      reinterpret_cast<AttrMissing *>(cbdb::Palloc0(sizeof(AttrMissing) * 3));
  missing[0].am_present = true;
  missing[0].am_value = Int32GetDatum(5);
  missing[1].am_present = false;
  missing[2].am_present = true;
  missing[2].am_value = MakeTextConst("abc")->constvalue;
  constr->missing = missing;
  desc->constr = constr;
  Relation rel = MakeFakeRelation(desc);
  Var *k = MakeVar(desc, 1);
  Var *m = MakeVar(desc, 2);
  Var *t = MakeVar(desc, 3);

  struct {
    Node *qual;
    bool passes;
  } cases[] = {
      {(Node *)MakeOpExpr(kOpInt4Eq, k, MakeIntConst(INT4OID, 5)), true},
      {(Node *)MakeOpExpr(kOpInt4Eq, k, MakeIntConst(INT4OID, 7)), false},
      {(Node *)MakeOpExpr(kOpInt4Gt, k, MakeIntConst(INT4OID, 4)), true},
      {(Node *)MakeOpExpr(kOpInt4Lt, k, MakeIntConst(INT4OID, 5), false),
       false},
      {(Node *)MakeIntInExpr(kOpInt4Eq, k, {1, 2, 3}), false},
      {(Node *)MakeIntInExpr(kOpInt4Eq, k, {4, 5}), true},
      {(Node *)MakeOpExpr(kOpTextLike, t, MakeTextConst("%b%")), true},
      {(Node *)MakeOpExpr(kOpTextNlike, t, MakeTextConst("%b%")), false},
      {(Node *)MakeOpExpr(kOpTextNe, t, MakeTextConst("")), true},
      // No missing value: every old row is NULL and fails every filter.
      {(Node *)MakeOpExpr(kOpInt4Ne, m, MakeIntConst(INT4OID, 7)), false},
  };
  for (const auto &c : cases) {
    FastFilterContext ctx;
    ASSERT_TRUE(pax::BuildFastFilter(rel, list_make1(c.qual), &ctx, nullptr));
    ASSERT_EQ(ctx.filters.size(), 1U);
    EXPECT_EQ(ctx.filters[0].missing_passes, c.passes) << nodeToString(c.qual);
  }
}

// The same for numeric columns and range sets.
TEST_F(PaxFastFilterTest, BuildRecordsWhetherNumericMissingValuePasses) {
  TupleDesc desc = BuildTupleDesc({kAttInt4, kAttNumeric72, kAttInt4});
  // Column 1 has missing value 5, column 2 has 5.00, column 3 has none.
  auto *constr =
      reinterpret_cast<TupleConstr *>(cbdb::Palloc0(sizeof(TupleConstr)));
  auto *missing =
      reinterpret_cast<AttrMissing *>(cbdb::Palloc0(sizeof(AttrMissing) * 3));
  missing[0].am_present = true;
  missing[0].am_value = Int32GetDatum(5);
  missing[1].am_present = true;
  missing[1].am_value = MakeNumericConst("5.00")->constvalue;
  missing[2].am_present = false;
  constr->missing = missing;
  desc->constr = constr;
  Relation rel = MakeFakeRelation(desc);
  Var *k = MakeVar(desc, 1);
  Var *h = MakeVar(desc, 2);
  Var *m = MakeVar(desc, 3);

  struct {
    Node *qual;
    bool passes;
  } cases[] = {
      {(Node *)MakeOpExpr(kOpInt4Eq, k, MakeIntConst(INT4OID, 5)), true},
      {(Node *)MakeOpExpr(kOpInt4Eq, k, MakeIntConst(INT4OID, 7)), false},
      {MakeOr({MakeOpExpr(kOpInt4Eq, k, MakeIntConst(INT4OID, 7)),
               MakeOpExpr(kOpInt4Gt, k, MakeIntConst(INT4OID, 4))}),
       true},
      {(Node *)MakeIntInExpr(kOpInt4Eq, k, {1, 2, 3}), false},
      {(Node *)MakeIntInExpr(kOpInt4Eq, k, {4, 5}), true},
      {NumericBetween(h, "4", "6"), true},
      {MakeOr({NumericBetween(h, "6", "7"),
               MakeNumericOpExpr(F_NUMERIC_LT, h, MakeNumericConst("0"))}),
       false},
      // No missing value: every old row is NULL and fails every filter.
      {(Node *)MakeOpExpr(kOpInt4Ne, m, MakeIntConst(INT4OID, 7)), false},
  };
  for (const auto &t : cases) {
    FastFilterContext ctx;
    ASSERT_TRUE(pax::BuildFastFilter(rel, list_make1(t.qual), &ctx, nullptr));
    // A top-level AND (the BETWEEN) becomes one filter per conjunct.
    bool passes = true;
    for (const auto &fd : ctx.filters) passes &= fd.missing_passes;
    EXPECT_EQ(passes, t.passes) << nodeToString(t.qual);
  }
}

}  // namespace pax::tests
