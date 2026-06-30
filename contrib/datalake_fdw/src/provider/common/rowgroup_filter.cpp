/*-------------------------------------------------------------------------
 *
 * rowgroup_filter.cpp
 *    Columnar min/max ("zone map") predicate pruning shared by the Parquet and
 *    ORC readers.  See rowgroup_filter.h.
 *
 *    The qual-walking and PG-side type/operator logic here is format
 *    independent; per-format statistics access is provided via IZoneStats.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/provider/common/rowgroup_filter.cpp
 *-------------------------------------------------------------------------
 */
#include <parquet/api/reader.h>
#include <string>
#include <cstring>
#include <cmath>

#include "rowgroup_filter.h"
#include "base_reader.h"		/* TIMEUNIT_* enum */
#include "common.h"				/* gpdbDirectFunctionCall2/3 */
#include "datalake_numeric.h"	/* FLBA_to_int64 */

extern "C"
{
#include "nodes/primnodes.h"
#include "access/stratnum.h"
#include "catalog/pg_type.h"
#include "utils/lsyscache.h"
#include "utils/date.h"
#include "datatype/timestamp.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgrprotos.h"
}

namespace {

/* ---- PG-side primitives (format independent) ------------------------- */

/* Const value as int64 in the column's PG domain; false unless the (non-null)
 * Const type matches the column's domain. */
static bool
pgConstInt64(Const *c, Oid colType, int64_t &out)
{
	if (c->constisnull)
		return false;
	switch (colType)
	{
		case INT2OID: case INT4OID: case INT8OID:
			switch (c->consttype)
			{
				case INT2OID: out = (int64_t) DatumGetInt16(c->constvalue); return true;
				case INT4OID: out = (int64_t) DatumGetInt32(c->constvalue); return true;
				case INT8OID: out = (int64_t) DatumGetInt64(c->constvalue); return true;
				default: return false;
			}
		case DATEOID:
			if (c->consttype != DATEOID) return false;
			out = (int64_t) DatumGetDateADT(c->constvalue);
			return true;
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			if (c->consttype != TIMESTAMPOID && c->consttype != TIMESTAMPTZOID) return false;
			out = (int64_t) DatumGetTimestamp(c->constvalue);
			return true;
		case BOOLOID:
			if (c->consttype != BOOLOID) return false;
			out = DatumGetBool(c->constvalue) ? 1 : 0;
			return true;
		default:
			return false;
	}
}

/* btree strategy (1=<,2=<=,3==,4=>=,5=>) for opno, or 0 if not a btree comparison. */
static int
pgOpBtreeStrategy(Oid opno)
{
	int			strat = 0;
	List	   *interp = get_op_btree_interpretation(opno);
	ListCell   *lc;

	foreach(lc, interp)
	{
		OpBtreeInterpretation *opi = (OpBtreeInterpretation *) lfirst(lc);
		if (opi->strategy >= BTLessStrategyNumber &&
			opi->strategy <= BTGreaterStrategyNumber)
		{
			strat = opi->strategy;
			break;
		}
	}
	list_free_deep(interp);
	return strat;
}

/* true iff integer range [mn,mx] provably has NO value v with (v strat c). */
static bool
intRangeExcluded(int strat, int64_t mn, int64_t mx, int64_t c)
{
	switch (strat)
	{
		case BTLessStrategyNumber:			return mn >= c;
		case BTLessEqualStrategyNumber:		return mn >  c;
		case BTEqualStrategyNumber:			return c < mn || c > mx;
		case BTGreaterEqualStrategyNumber:	return mx <  c;
		case BTGreaterStrategyNumber:		return mx <= c;
		default:							return false;
	}
}

/* A single Datum of the column type as int64 in PG's internal domain. */
static bool
datumToColInt64(Datum d, Oid colType, int64_t &out)
{
	switch (colType)
	{
		case INT2OID: out = (int64_t) DatumGetInt16(d); return true;
		case INT4OID: out = (int64_t) DatumGetInt32(d); return true;
		case INT8OID: out = (int64_t) DatumGetInt64(d); return true;
		case DATEOID: out = (int64_t) DatumGetDateADT(d); return true;
		case TIMESTAMPOID:
		case TIMESTAMPTZOID: out = (int64_t) DatumGetTimestamp(d); return true;
		case BOOLOID: out = DatumGetBool(d) ? 1 : 0; return true;
		default: return false;
	}
}

/* Const value as double; accepts float4/float8 consts (cross-type compares
 * between float4 and float8 share a btree opfamily). */
static bool
pgConstFloat(Const *c, double &out)
{
	if (c->constisnull)
		return false;
	switch (c->consttype)
	{
		case FLOAT4OID: out = (double) DatumGetFloat4(c->constvalue); return true;
		case FLOAT8OID: out = DatumGetFloat8(c->constvalue); return true;
		default: return false;
	}
}

/* true iff double range [mn,mx] provably has NO value v with (v strat c). */
static bool
doubleRangeExcluded(int strat, double mn, double mx, double c)
{
	if (std::isnan(c))			/* NaN comparisons: keep (don't prune) */
		return false;
	switch (strat)
	{
		case BTLessStrategyNumber:			return mn >= c;
		case BTLessEqualStrategyNumber:		return mn >  c;
		case BTEqualStrategyNumber:			return c < mn || c > mx;
		case BTGreaterEqualStrategyNumber:	return mx <  c;
		case BTGreaterStrategyNumber:		return mx <= c;
		default:							return false;
	}
}

/* A single array element Datum as double; false if not a float type. */
static bool
datumToColFloat(Datum d, Oid elmtype, double &out)
{
	switch (elmtype)
	{
		case FLOAT4OID: out = (double) DatumGetFloat4(d); return true;
		case FLOAT8OID: out = DatumGetFloat8(d); return true;
		default: return false;
	}
}

/* Strip trailing ASCII spaces (bpchar compares ignore them; the writer also
 * stores CHAR(N) trimmed, so the stats bytes are already space-free). */
static void
rtrimSpaces(std::string &s)
{
	size_t n = s.size();
	while (n > 0 && s[n - 1] == ' ')
		n--;
	s.resize(n);
}

/* A single array element Datum as a string; false if not a text-family type. */
static bool
datumToStr(Datum d, Oid elmtype, std::string &out)
{
	if (elmtype != TEXTOID && elmtype != VARCHAROID && elmtype != BPCHAROID)
		return false;
	text *t = DatumGetTextPP(d);
	out.assign(VARDATA_ANY(t), VARSIZE_ANY_EXHDR(t));
	return true;
}

static bool
pgConstStr(Const *c, Oid colType, std::string &out)
{
	if (c->constisnull)
		return false;
	if (colType != TEXTOID && colType != VARCHAROID && colType != BPCHAROID)
		return false;
	if (c->consttype != TEXTOID && c->consttype != VARCHAROID && c->consttype != BPCHAROID)
		return false;
	text *t = DatumGetTextPP(c->constvalue);
	out.assign(VARDATA_ANY(t), VARSIZE_ANY_EXHDR(t));
	return true;
}

static bool
byteLess(const std::string &a, const std::string &b)
{
	size_t n = (a.size() < b.size()) ? a.size() : b.size();
	int r = (n > 0) ? memcmp(a.data(), b.data(), n) : 0;
	if (r != 0)
		return r < 0;
	return a.size() < b.size();
}

/*
 * Byte-range pruning of a text equality is only valid when equality means
 * byte-identity, i.e. for a deterministic collation.  Under a non-deterministic
 * collation two byte-different strings can compare equal, so a matching value
 * could lie outside the byte-ordered [min,max] -- pruning would then drop a
 * matching row.  Treat an invalid/unknown collation as unsafe (keep).
 */
static bool
collationAllowsByteEqPrune(Oid collid)
{
	return OidIsValid(collid) && get_collation_isdeterministic(collid);
}

static bool
numericRangeExcluded(int strat, Datum minN, Datum maxN, Datum c)
{
	switch (strat)
	{
		case BTLessStrategyNumber:
			return DatumGetInt32(gpdbDirectFunctionCall2(numeric_cmp, minN, c)) >= 0;
		case BTLessEqualStrategyNumber:
			return DatumGetInt32(gpdbDirectFunctionCall2(numeric_cmp, minN, c)) > 0;
		case BTEqualStrategyNumber:
			return DatumGetInt32(gpdbDirectFunctionCall2(numeric_cmp, c, minN)) < 0 ||
				   DatumGetInt32(gpdbDirectFunctionCall2(numeric_cmp, c, maxN)) > 0;
		case BTGreaterEqualStrategyNumber:
			return DatumGetInt32(gpdbDirectFunctionCall2(numeric_cmp, maxN, c)) < 0;
		case BTGreaterStrategyNumber:
			return DatumGetInt32(gpdbDirectFunctionCall2(numeric_cmp, maxN, c)) <= 0;
		default:
			return false;
	}
}

/* Resolve a Var to its column meta; nullptr if out of range / no column stats. */
static const RowGroupColMeta *
colFor(Var *var, const std::vector<RowGroupColMeta> &cols)
{
	int attno = var->varattno;
	if (attno <= 0 || (size_t) attno > cols.size())
		return nullptr;
	const RowGroupColMeta *ci = &cols[attno - 1];
	if (ci->colIdx < 0)
		return nullptr;
	return ci;
}

/* ---- Generic qual evaluation (over IZoneStats) ----------------------- */

static bool
opExprExcludes(OpExpr *op, IZoneStats &stats,
			   const std::vector<RowGroupColMeta> &cols)
{
	if (list_length(op->args) != 2)
		return false;

	Node *larg = (Node *) linitial(op->args);
	Node *rarg = (Node *) lsecond(op->args);
	Var *var = NULL;
	Const *con = NULL;
	bool varOnLeft;

	if (IsA(larg, Var) && IsA(rarg, Const)) { var = (Var *) larg; con = (Const *) rarg; varOnLeft = true; }
	else if (IsA(larg, Const) && IsA(rarg, Var)) { var = (Var *) rarg; con = (Const *) larg; varOnLeft = false; }
	else return false;

	int strat = pgOpBtreeStrategy(op->opno);
	if (strat == 0)
		return false;
	if (!varOnLeft)
	{
		switch (strat)
		{
			case BTLessStrategyNumber:			strat = BTGreaterStrategyNumber; break;
			case BTLessEqualStrategyNumber:		strat = BTGreaterEqualStrategyNumber; break;
			case BTGreaterStrategyNumber:		strat = BTLessStrategyNumber; break;
			case BTGreaterEqualStrategyNumber:	strat = BTLessEqualStrategyNumber; break;
			default: break;
		}
	}

	const RowGroupColMeta *ci = colFor(var, cols);
	if (!ci)
		return false;

	/* Text/varchar/bpchar: equality only (byte-order vs collation).  For
	 * bpchar, trailing spaces are insignificant; the writer stores CHAR(N)
	 * already trimmed, so right-trimming the constant (and defensively the
	 * stats) makes byte-equality match bpchar semantics. */
	if (ci->pgType == TEXTOID || ci->pgType == VARCHAROID || ci->pgType == BPCHAROID)
	{
		if (strat != BTEqualStrategyNumber)
			return false;
		if (!collationAllowsByteEqPrune(op->inputcollid))
			return false;
		std::string cs, mns, mxs;
		if (!pgConstStr(con, ci->pgType, cs))
			return false;
		if (!stats.minMaxStr(*ci, mns, mxs))
			return false;
		if (ci->pgType == BPCHAROID)
		{
			rtrimSpaces(cs);
			rtrimSpaces(mns);
			rtrimSpaces(mxs);
		}
		return byteLess(cs, mns) || byteLess(mxs, cs);
	}

	/* float4/float8. */
	if (ci->pgType == FLOAT4OID || ci->pgType == FLOAT8OID)
	{
		double c;
		if (!pgConstFloat(con, c))
			return false;
		double mn, mx;
		if (!stats.minMaxFloat(*ci, mn, mx))
			return false;
		return doubleRangeExcluded(strat, mn, mx, c);
	}

	/* Numeric/decimal. */
	if (ci->pgType == NUMERICOID)
	{
		if (con->consttype != NUMERICOID || con->constisnull)
			return false;
		std::string mns, mxs;
		if (!stats.minMaxDecimalStr(*ci, mns, mxs))
			return false;
		Datum minN = gpdbDirectFunctionCall3(numeric_in, CStringGetDatum(mns.c_str()),
											 ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
		Datum maxN = gpdbDirectFunctionCall3(numeric_in, CStringGetDatum(mxs.c_str()),
											 ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
		return numericRangeExcluded(strat, minN, maxN, con->constvalue);
	}

	/* int / date / timestamp / bool. */
	int64_t c;
	if (!pgConstInt64(con, ci->pgType, c))
		return false;
	int64_t mn, mx;
	if (!stats.minMaxInt64(*ci, mn, mx))
		return false;
	return intRangeExcluded(strat, mn, mx, c);
}

static bool
nullTestExcludes(NullTest *nt, IZoneStats &stats,
				 const std::vector<RowGroupColMeta> &cols)
{
	if (nt->arg == NULL || !IsA(nt->arg, Var))
		return false;
	const RowGroupColMeta *ci = colFor((Var *) nt->arg, cols);
	if (!ci)
		return false;
	int64_t nullCount, numRows;
	if (!stats.nullCounts(*ci, nullCount, numRows))
		return false;
	if (nt->nulltesttype == IS_NULL)
		return nullCount == 0;
	else
		return nullCount == numRows;
}

static bool
saoExprExcludes(ScalarArrayOpExpr *sao, IZoneStats &stats,
				const std::vector<RowGroupColMeta> &cols)
{
	if (!sao->useOr || list_length(sao->args) != 2)
		return false;
	Node *larg = (Node *) linitial(sao->args);
	Node *rarg = (Node *) lsecond(sao->args);
	if (!IsA(larg, Var) || !IsA(rarg, Const))
		return false;
	if (pgOpBtreeStrategy(sao->opno) != BTEqualStrategyNumber)
		return false;

	const RowGroupColMeta *ci = colFor((Var *) larg, cols);
	if (!ci)
		return false;

	/* Fetch the zone for this column's type family up front; bail (keep) if
	 * stats are missing or the type is not handled. */
	bool		isText = (ci->pgType == TEXTOID || ci->pgType == VARCHAROID || ci->pgType == BPCHAROID);
	bool		isNumeric = (ci->pgType == NUMERICOID);
	bool		isFloat = (ci->pgType == FLOAT4OID || ci->pgType == FLOAT8OID);
	int64_t		mn = 0, mx = 0;
	double		fmn = 0, fmx = 0;
	std::string smn, smx;
	Datum		nMin = 0, nMax = 0;

	/* Text equality pruning needs byte-identity semantics (see opExprExcludes);
	 * a non-deterministic collation could treat byte-different strings as equal. */
	if (isText && !collationAllowsByteEqPrune(sao->inputcollid))
		return false;

	if (isText)
	{
		if (!stats.minMaxStr(*ci, smn, smx))
			return false;
		if (ci->pgType == BPCHAROID)
		{
			rtrimSpaces(smn);
			rtrimSpaces(smx);
		}
	}
	else if (isNumeric)
	{
		std::string mns, mxs;
		if (!stats.minMaxDecimalStr(*ci, mns, mxs))
			return false;
		nMin = gpdbDirectFunctionCall3(numeric_in, CStringGetDatum(mns.c_str()),
									   ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
		nMax = gpdbDirectFunctionCall3(numeric_in, CStringGetDatum(mxs.c_str()),
									   ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
	}
	else if (isFloat)
	{
		if (!stats.minMaxFloat(*ci, fmn, fmx))
			return false;
	}
	else if (!stats.minMaxInt64(*ci, mn, mx))
		return false;

	Const *arr = (Const *) rarg;
	if (arr->constisnull)
		return false;
	ArrayType *at = DatumGetArrayTypeP(arr->constvalue);
	Oid elmtype = ARR_ELEMTYPE(at);
	int16 elmlen;
	bool elmbyval;
	char elmalign;
	get_typlenbyvalalign(elmtype, &elmlen, &elmbyval, &elmalign);

	Datum *elems;
	bool *nulls;
	int nelems;
	deconstruct_array(at, elmtype, elmlen, elmbyval, elmalign, &elems, &nulls, &nelems);

	/* Prune iff EVERY array element is provably outside the zone (so the IN
	 * can match no row).  Any element we cannot interpret -> assume in range. */
	bool anyInRange = false;
	for (int i = 0; i < nelems && !anyInRange; i++)
	{
		if (nulls[i])
			continue;
		if (isText)
		{
			std::string es;
			if (!datumToStr(elems[i], elmtype, es))
			{
				anyInRange = true;
				break;
			}
			if (ci->pgType == BPCHAROID)
				rtrimSpaces(es);
			if (!(byteLess(es, smn) || byteLess(smx, es)))
				anyInRange = true;
		}
		else if (isNumeric)
		{
			if (elmtype != NUMERICOID)
			{
				anyInRange = true;
				break;
			}
			if (!numericRangeExcluded(BTEqualStrategyNumber, nMin, nMax, elems[i]))
				anyInRange = true;
		}
		else if (isFloat)
		{
			double v;
			if (!datumToColFloat(elems[i], elmtype, v))
			{
				anyInRange = true;
				break;
			}
			if (!doubleRangeExcluded(BTEqualStrategyNumber, fmn, fmx, v))
				anyInRange = true;
		}
		else
		{
			int64_t v;
			if (!datumToColInt64(elems[i], ci->pgType, v))
			{
				anyInRange = true;
				break;
			}
			if (!intRangeExcluded(BTEqualStrategyNumber, mn, mx, v))
				anyInRange = true;
		}
	}
	if (elems)
		pfree(elems);
	if (nulls)
		pfree(nulls);
	return !anyInRange;
}

static bool
boolVarExcludes(Var *var, IZoneStats &stats,
				const std::vector<RowGroupColMeta> &cols, bool wantTrue)
{
	const RowGroupColMeta *ci = colFor(var, cols);
	if (!ci || ci->pgType != BOOLOID)
		return false;
	int64_t mn, mx;
	if (!stats.minMaxInt64(*ci, mn, mx))
		return false;
	return wantTrue ? (mx == 0) : (mn == 1);
}

static bool
exprExcludes(Expr *expr, IZoneStats &stats,
			 const std::vector<RowGroupColMeta> &cols)
{
	if (expr == NULL)
		return false;

	switch (nodeTag(expr))
	{
		case T_OpExpr:
			return opExprExcludes((OpExpr *) expr, stats, cols);
		case T_ScalarArrayOpExpr:
			return saoExprExcludes((ScalarArrayOpExpr *) expr, stats, cols);
		case T_NullTest:
			return nullTestExcludes((NullTest *) expr, stats, cols);
		case T_Var:
			if (((Var *) expr)->vartype == BOOLOID)
				return boolVarExcludes((Var *) expr, stats, cols, true);
			return false;
		case T_BoolExpr:
		{
			BoolExpr *b = (BoolExpr *) expr;
			ListCell *lc;

			if (b->boolop == AND_EXPR)
			{
				foreach(lc, b->args)
					if (exprExcludes((Expr *) lfirst(lc), stats, cols))
						return true;
				return false;
			}
			if (b->boolop == OR_EXPR)
			{
				foreach(lc, b->args)
					if (!exprExcludes((Expr *) lfirst(lc), stats, cols))
						return false;
				return true;
			}
			if (b->boolop == NOT_EXPR && list_length(b->args) == 1)
			{
				Node *a = (Node *) linitial(b->args);
				if (IsA(a, Var) && ((Var *) a)->vartype == BOOLOID)
					return boolVarExcludes((Var *) a, stats, cols, false);
			}
			return false;
		}
		default:
			return false;
	}
}

/* ---- Parquet IZoneStats implementation ------------------------------- */

struct ParquetZoneStats : public IZoneStats
{
	parquet::RowGroupMetaData *rg_;
	explicit ParquetZoneStats(parquet::RowGroupMetaData *rg) : rg_(rg) {}

	bool minMaxInt64(const RowGroupColMeta &c, int64_t &mn, int64_t &mx) override
	{
		auto cc = rg_->ColumnChunk(c.colIdx);
		auto stats = cc->statistics();
		if (!stats || !stats->HasMinMax())
			return false;

		int64_t rawMin, rawMax;
		switch (cc->type())
		{
			case parquet::Type::INT32:
			{
				auto s = std::static_pointer_cast<parquet::Int32Statistics>(stats);
				rawMin = s->min(); rawMax = s->max(); break;
			}
			case parquet::Type::INT64:
			{
				auto s = std::static_pointer_cast<parquet::Int64Statistics>(stats);
				rawMin = s->min(); rawMax = s->max(); break;
			}
			case parquet::Type::BOOLEAN:
			{
				auto s = std::static_pointer_cast<parquet::BoolStatistics>(stats);
				rawMin = s->min() ? 1 : 0; rawMax = s->max() ? 1 : 0; break;
			}
			default:
				return false;
		}

		switch (c.pgType)
		{
			case INT2OID: case INT4OID: case INT8OID:
			case BOOLOID:		/* bool ordered false(0) < true(1) */
				mn = rawMin; mx = rawMax;
				return true;
			case DATEOID:
				mn = rawMin + (UNIX_EPOCH_JDATE - POSTGRES_EPOCH_JDATE);
				mx = rawMax + (UNIX_EPOCH_JDATE - POSTGRES_EPOCH_JDATE);
				return true;
			case TIMESTAMPOID:
			case TIMESTAMPTZOID:
			{
				const int64_t off = ((int64_t)(POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE))
									 * SECS_PER_DAY * USECS_PER_SEC;
				switch (c.timeUnit)
				{
					case TIMEUNIT_MILLIS:
						mn = rawMin * 1000 - off; mx = rawMax * 1000 - off; return true;
					case TIMEUNIT_MICROS:
						mn = rawMin - off; mx = rawMax - off; return true;
					default:
						return false;	/* NANOS truncation / unknown: keep */
				}
			}
			default:
				return false;
		}
	}

	bool minMaxFloat(const RowGroupColMeta &c, double &mn, double &mx) override
	{
		(void) c;
		auto cc = rg_->ColumnChunk(c.colIdx);
		auto stats = cc->statistics();
		if (!stats || !stats->HasMinMax())
			return false;
		switch (cc->type())
		{
			case parquet::Type::FLOAT:
			{
				auto s = std::static_pointer_cast<parquet::FloatStatistics>(stats);
				mn = (double) s->min(); mx = (double) s->max(); break;
			}
			case parquet::Type::DOUBLE:
			{
				auto s = std::static_pointer_cast<parquet::DoubleStatistics>(stats);
				mn = s->min(); mx = s->max(); break;
			}
			default:
				return false;
		}
		if (std::isnan(mn) || std::isnan(mx))
			return false;
		return true;
	}

	bool minMaxStr(const RowGroupColMeta &c, std::string &mn, std::string &mx) override
	{
		(void) c;
		auto cc = rg_->ColumnChunk(c.colIdx);
		if (cc->type() != parquet::Type::BYTE_ARRAY)
			return false;
		auto stats = cc->statistics();
		if (!stats || !stats->HasMinMax())
			return false;
		auto s = std::static_pointer_cast<parquet::ByteArrayStatistics>(stats);
		parquet::ByteArray lo = s->min();
		parquet::ByteArray hi = s->max();
		mn.assign((const char *) lo.ptr, lo.len);
		mx.assign((const char *) hi.ptr, hi.len);
		return true;
	}

	bool minMaxDecimalStr(const RowGroupColMeta &c, std::string &mn, std::string &mx) override
	{
		auto cc = rg_->ColumnChunk(c.colIdx);
		auto stats = cc->statistics();
		if (!stats || !stats->HasMinMax())
			return false;
		int64_t rawMin, rawMax;
		switch (cc->type())
		{
			case parquet::Type::INT32:
			{
				auto s = std::static_pointer_cast<parquet::Int32Statistics>(stats);
				rawMin = s->min(); rawMax = s->max(); break;
			}
			case parquet::Type::INT64:
			{
				auto s = std::static_pointer_cast<parquet::Int64Statistics>(stats);
				rawMin = s->min(); rawMax = s->max(); break;
			}
			case parquet::Type::FIXED_LEN_BYTE_ARRAY:
			{
				if (c.precision <= 0 || c.precision > 18)
					return false;
				auto s = std::static_pointer_cast<parquet::FLBAStatistics>(stats);
				parquet::FixedLenByteArray lo = s->min();
				parquet::FixedLenByteArray hi = s->max();
				rawMin = FLBA_to_int64(lo.ptr, c.typeLength);
				rawMax = FLBA_to_int64(hi.ptr, c.typeLength);
				break;
			}
			default:
				return false;
		}
		mn = decimalToStr(rawMin, c.scale);
		mx = decimalToStr(rawMax, c.scale);
		return true;
	}

	bool nullCounts(const RowGroupColMeta &c, int64_t &nullCount, int64_t &numRows) override
	{
		auto cc = rg_->ColumnChunk(c.colIdx);
		auto stats = cc->statistics();
		if (!stats || !stats->HasNullCount())
			return false;
		nullCount = stats->null_count();
		numRows = rg_->num_rows();
		return true;
	}

private:
	/* unscaled integer -> decimal text with `scale` fractional digits. */
	static std::string decimalToStr(int64_t unscaled, int scale)
	{
		if (scale <= 0)
			return std::to_string(unscaled);
		std::string s = std::to_string(unscaled);
		bool neg = (!s.empty() && s[0] == '-');
		std::string digits = neg ? s.substr(1) : s;
		if ((int) digits.size() <= scale)
			digits = std::string(scale - digits.size() + 1, '0') + digits;
		std::string out = digits.substr(0, digits.size() - scale) + "." +
						  digits.substr(digits.size() - scale);
		return neg ? ("-" + out) : out;
	}
};

} /* anonymous namespace */

bool
zoneExcludedByQuals(List *quals, const std::vector<RowGroupColMeta> &cols,
					IZoneStats &stats)
{
	ListCell *lc;

	if (quals == NIL)
		return false;

	foreach(lc, quals)
	{
		if (exprExcludes((Expr *) lfirst(lc), stats, cols))
			return true;
	}
	return false;
}

bool
rowGroupExcludedByQuals(List *quals, parquet::RowGroupMetaData *rg,
						const std::vector<RowGroupColMeta> &cols)
{
	ParquetZoneStats stats(rg);
	return zoneExcludedByQuals(quals, cols, stats);
}
