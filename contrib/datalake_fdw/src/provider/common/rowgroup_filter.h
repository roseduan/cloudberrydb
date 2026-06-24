/*-------------------------------------------------------------------------
 *
 * rowgroup_filter.h
 *    Columnar min/max ("zone map") predicate pruning, shared by the Parquet
 *    readers (provider/common/parquet_reader, provider/parquet/read/parquetRead)
 *    and the ORC reader (provider/orc/read/orcRead).
 *
 *    Given the WHERE-clause quals (raw Expr) and a block's per-column
 *    statistics, decide whether the block (Parquet row group / ORC stripe) can
 *    be skipped.  Strictly conservative: a block is excluded only when the
 *    quals PROVE no row in [min,max] can match; anything unhandled keeps the
 *    block (the executor still applies the real qual), so results are never
 *    wrong.
 *
 *    The qual-walking and PG-side type/operator logic is format-independent and
 *    lives here.  Each format provides an IZoneStats that fetches a column's
 *    min/max/null statistics already converted to PG's internal domain.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/provider/common/rowgroup_filter.h
 *-------------------------------------------------------------------------
 */
#ifndef ROWGROUP_FILTER_H
#define ROWGROUP_FILTER_H

#include <vector>
#include <string>
#include <parquet/metadata.h>

extern "C"
{
#include "postgres.h"
#include "nodes/pg_list.h"
}

/*
 * Per-column metadata needed to evaluate quals against a block, indexed by
 * (table attno - 1).  colIdx is the format column index (-1 if the column has
 * no counterpart / no stats); timeUnit is the reader's TIMEUNIT enum value
 * (relevant only for timestamps).  scale/precision/typeLength describe decimals.
 */
struct RowGroupColMeta
{
	Oid		pgType;
	int		colIdx;
	int		scale;
	int		precision;
	int		typeLength;
	int		timeUnit;
};

/*
 * Abstract per-block statistics source.  Each method returns false when the
 * statistic is missing or not usable for that column (the evaluator then keeps
 * the block).  All min/max values are returned already converted to PG's
 * internal on-disk domain for the column's type (e.g. dates as day counts from
 * the PG epoch, timestamps as microseconds, decimals as their textual form).
 */
struct IZoneStats
{
	virtual ~IZoneStats() {}
	virtual bool minMaxInt64(const RowGroupColMeta &c, int64_t &mn, int64_t &mx) = 0;
	virtual bool minMaxFloat(const RowGroupColMeta &c, double &mn, double &mx) = 0;
	virtual bool minMaxStr(const RowGroupColMeta &c, std::string &mn, std::string &mx) = 0;
	virtual bool minMaxDecimalStr(const RowGroupColMeta &c, std::string &mn, std::string &mx) = 0;
	/* nullCount/numRows for the column in this block (for IS [NOT] NULL). */
	virtual bool nullCounts(const RowGroupColMeta &c, int64_t &nullCount, int64_t &numRows) = 0;
};

/*
 * Generic evaluator: returns true iff `quals` prove the block described by
 * `stats` contains no matching rows.  `cols` is indexed by (Var attno - 1).
 */
bool zoneExcludedByQuals(List *quals, const std::vector<RowGroupColMeta> &cols,
						 IZoneStats &stats);

/*
 * Parquet convenience wrapper: build an IZoneStats over `rg` and evaluate.
 */
bool rowGroupExcludedByQuals(List *quals, parquet::RowGroupMetaData *rg,
							 const std::vector<RowGroupColMeta> &cols);

#endif							/* ROWGROUP_FILTER_H */
