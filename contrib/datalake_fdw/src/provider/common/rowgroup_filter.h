/*-------------------------------------------------------------------------
 *
 * rowgroup_filter.h
 *    Parquet row-group min/max ("zone map") predicate pruning, shared by the
 *    Iceberg/Hudi/Delta reader (provider/common/parquet_reader) and the generic
 *    parquet reader (provider/parquet/read/parquetRead).
 *
 *    Given the WHERE-clause quals (raw Expr) and a Parquet row group's per-column
 *    statistics, decide whether the row group can be skipped.  Strictly
 *    conservative: a row group is excluded only when the quals PROVE no row in
 *    [min,max] can match; anything unhandled keeps the row group (the executor
 *    still applies the real qual), so results are never wrong.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/provider/common/rowgroup_filter.h
 *-------------------------------------------------------------------------
 */
#ifndef ROWGROUP_FILTER_H
#define ROWGROUP_FILTER_H

#include <vector>
#include <parquet/metadata.h>

extern "C"
{
#include "postgres.h"
#include "nodes/pg_list.h"
}

/*
 * Per-column metadata needed to evaluate quals against a row group, indexed by
 * (table attno - 1).  colIdx is the Parquet column index (-1 if the column has
 * no Parquet counterpart / no stats); timeUnit is the reader's TIMEUNIT enum
 * value (relevant only for timestamps).
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
 * Returns true iff `quals` prove that row group `rg` contains no matching rows.
 * `cols` is indexed by (Var attno - 1).
 */
bool rowGroupExcludedByQuals(List *quals, parquet::RowGroupMetaData *rg,
							 const std::vector<RowGroupColMeta> &cols);

#endif							/* ROWGROUP_FILTER_H */
