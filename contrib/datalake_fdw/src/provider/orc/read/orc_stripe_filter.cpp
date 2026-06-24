/*-------------------------------------------------------------------------
 *
 * orc_stripe_filter.cpp
 *    ORC IZoneStats implementation (see orc_stripe_filter.h).  Maps ORC
 *    typed column statistics to PG's internal domain for the shared zone-map
 *    evaluator.  Strictly conservative: any column/type whose statistics are
 *    missing or not interpretable falls through to "keep".
 *
 *    TIMESTAMP is intentionally not pruned here: ORC timestamp statistics are
 *    in UTC milliseconds and reconciling that with PG's TIMESTAMP (local) vs
 *    TIMESTAMPTZ (UTC) domains is ambiguous, so we keep (never risk dropping a
 *    matching row).  date/int/float/numeric/text/bpchar/bool are tz-independent
 *    and safe.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/provider/orc/read/orc_stripe_filter.cpp
 *-------------------------------------------------------------------------
 */
#include <orc/Statistics.hh>
#include <orc/Vector.hh>		/* orc::Decimal */
#include <cmath>
#include <string>

#include "orc_stripe_filter.h"

extern "C"
{
#include "catalog/pg_type.h"
#include "utils/date.h"
#include "datatype/timestamp.h"
}

namespace {

struct OrcZoneStats : public IZoneStats
{
	const orc::StripeStatistics *ss_;
	int64_t numRows_;

	OrcZoneStats(const orc::StripeStatistics *ss, int64_t numRows)
		: ss_(ss), numRows_(numRows) {}

	/* ORC column 0 is the struct root; table column i is ORC column id i+1. */
	const orc::ColumnStatistics *col(const RowGroupColMeta &c)
	{
		if (c.colIdx < 0)
			return nullptr;
		uint32_t id = (uint32_t) (c.colIdx + 1);
		if (id >= ss_->getNumberOfColumns())
			return nullptr;
		return ss_->getColumnStatistics(id);
	}

	bool minMaxInt64(const RowGroupColMeta &c, int64_t &mn, int64_t &mx) override
	{
		const orc::ColumnStatistics *cs = col(c);
		if (cs == nullptr)
			return false;

		switch (c.pgType)
		{
			case INT2OID: case INT4OID: case INT8OID:
			{
				auto *s = dynamic_cast<const orc::IntegerColumnStatistics *>(cs);
				if (!s || !s->hasMinimum() || !s->hasMaximum())
					return false;
				mn = s->getMinimum();
				mx = s->getMaximum();
				return true;
			}
			case DATEOID:
			{
				auto *s = dynamic_cast<const orc::DateColumnStatistics *>(cs);
				if (!s || !s->hasMinimum() || !s->hasMaximum())
					return false;
				mn = (int64_t) s->getMinimum() + (UNIX_EPOCH_JDATE - POSTGRES_EPOCH_JDATE);
				mx = (int64_t) s->getMaximum() + (UNIX_EPOCH_JDATE - POSTGRES_EPOCH_JDATE);
				return true;
			}
			case BOOLOID:
			{
				auto *s = dynamic_cast<const orc::BooleanColumnStatistics *>(cs);
				if (!s || !s->hasCount())
					return false;
				uint64_t t = s->getTrueCount();
				uint64_t f = s->getFalseCount();
				if (t == 0 && f == 0)
					return false;	/* no non-null values: keep */
				mn = (f > 0) ? 0 : 1;	/* false(0) < true(1) */
				mx = (t > 0) ? 1 : 0;
				return true;
			}
			default:
				return false;		/* timestamp & others: keep */
		}
	}

	bool minMaxFloat(const RowGroupColMeta &c, double &mn, double &mx) override
	{
		const orc::ColumnStatistics *cs = col(c);
		if (cs == nullptr)
			return false;
		auto *s = dynamic_cast<const orc::DoubleColumnStatistics *>(cs);
		if (!s || !s->hasMinimum() || !s->hasMaximum())
			return false;
		mn = s->getMinimum();
		mx = s->getMaximum();
		if (std::isnan(mn) || std::isnan(mx))
			return false;
		return true;
	}

	bool minMaxStr(const RowGroupColMeta &c, std::string &mn, std::string &mx) override
	{
		const orc::ColumnStatistics *cs = col(c);
		if (cs == nullptr)
			return false;
		auto *s = dynamic_cast<const orc::StringColumnStatistics *>(cs);
		if (!s || !s->hasMinimum() || !s->hasMaximum())
			return false;
		mn = s->getMinimum();
		mx = s->getMaximum();
		return true;
	}

	bool minMaxDecimalStr(const RowGroupColMeta &c, std::string &mn, std::string &mx) override
	{
		const orc::ColumnStatistics *cs = col(c);
		if (cs == nullptr)
			return false;
		auto *s = dynamic_cast<const orc::DecimalColumnStatistics *>(cs);
		if (!s || !s->hasMinimum() || !s->hasMaximum())
			return false;
		/* orc::Decimal::toString() yields the scaled decimal text (e.g. 12.34),
		 * directly parseable by numeric_in in the shared evaluator. */
		mn = s->getMinimum().toString();
		mx = s->getMaximum().toString();
		return true;
	}

	bool nullCounts(const RowGroupColMeta &c, int64_t &nullCount, int64_t &numRows) override
	{
		const orc::ColumnStatistics *cs = col(c);
		if (cs == nullptr)
			return false;
		int64_t numValues = (int64_t) cs->getNumberOfValues();	/* non-null count */
		nullCount = numRows_ - numValues;
		numRows = numRows_;
		return true;
	}
};

} /* anonymous namespace */

bool
stripeExcludedByQuals(List *quals, const orc::StripeStatistics *stripeStats,
					  int64_t numRows, const std::vector<RowGroupColMeta> &cols)
{
	if (stripeStats == nullptr)
		return false;
	OrcZoneStats stats(stripeStats, numRows);
	return zoneExcludedByQuals(quals, cols, stats);
}
