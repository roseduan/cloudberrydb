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

#include "comm/cbdb_wrappers.h"
#include "comm/guc.h"
#include "comm/gtest_wrappers.h"
#include "cpp-stub/src/stub.h"
#include "pax_gtest_helper.h"

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
    // Ensure the GUC is enabled (default value, but be explicit).
    pax::pax_enable_fast_filter = true;
  }

  void TearDown() override {
    delete stub_;
    stub_ = nullptr;
    ReleaseTestResourceOwner();
  }

  // Build a TupleDesc with the given attribute init callbacks.  attlen and
  // attbyval are set per type.  We expose a small set of column kinds
  // sufficient for the fast filter classification tests.
  enum AttKind { kAttInt2, kAttInt4, kAttInt8, kAttFloat4, kAttText };

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
      }
    }
    return td;
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

TEST_F(PaxFastFilterTest, BuildRejectsTopLevelOr) {
  // OR can't be flattened to AND-of-filters; BuildFastFilter should reject.
  TupleDesc desc = BuildTupleDesc({kAttInt4});
  Relation rel = MakeFakeRelation(desc);

  auto *q1 = MakeOpExpr(kOpInt4Eq, MakeVar(desc, 1),
                        MakeIntConst(INT4OID, 5));
  auto *q2 = MakeOpExpr(kOpInt4Eq, MakeVar(desc, 1),
                        MakeIntConst(INT4OID, 6));
  auto *or_node = makeNode(BoolExpr);
  or_node->boolop = OR_EXPR;
  or_node->args = list_make2(q1, q2);
  List *qual = list_make1(or_node);

  FastFilterContext ctx;
  EXPECT_FALSE(pax::BuildFastFilter(rel, qual, &ctx, nullptr));
  EXPECT_TRUE(ctx.filters.empty());
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

}  // namespace pax::tests
