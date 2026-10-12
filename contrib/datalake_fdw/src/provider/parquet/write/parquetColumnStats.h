/*-------------------------------------------------------------------------
 *
 * parquetColumnStats.h
 *    Read per-column statistics out of a just-closed parquet file's own
 *    in-memory footer and render them as the wire-format v1 JSON the dlagent
 *    turns into Iceberg manifest Metrics (issue #442).
 *
 * The agent used to reopen every committed data file on object storage just to
 * read the footer it needs for manifest statistics -- O(files) serial remote
 * reads at commit time, which is what makes a high-cardinality partitioned
 * INSERT time out.  The writer already has that footer in memory when it
 * closes the file, and parquet's encoded min/max bytes are byte-identical to
 * Iceberg's bound encoding for every type this writer emits, so the bytes can
 * simply ride along with the file metadata.
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 * IDENTIFICATION
 *        contrib/datalake_fdw/src/provider/parquet/write/parquetColumnStats.h
 *-------------------------------------------------------------------------
 */
#ifndef DATALAKE_PARQUETCOLUMNSTATS_H
#define DATALAKE_PARQUETCOLUMNSTATS_H

#include <string>
#include <parquet/metadata.h>

namespace Datalake {
namespace Internal {

/*
 * Per-bound ceiling, in bytes.  A bound longer than this makes the whole
 * column drop its bounds rather than travel: the request body is O(files x
 * columns x 2 x this), and parquet's own default statistics ceiling (4096 B)
 * would put a 6144-file commit past both the QD StringInfo limit and the
 * agent heap.  128 covers the worst case of Iceberg's default truncate(16)
 * -- 16 code points of 4-byte UTF-8 -- with a factor of two to spare, so the
 * ceiling only ever costs pruning on values the agent would have truncated
 * anyway.  Dropping a bound is always safe: it widens the file's apparent
 * range, so a scan reads more, never less.
 */
#define ICEBERG_COLUMN_BOUND_MAX_BYTES 128

/*
 * Render the column statistics of a closed parquet file as wire-format v1.
 *
 * Returns an empty string when no usable statistics could be read; the caller
 * then sends no column_stats for the file and the agent falls back to reading
 * the footer itself.  Never throws: statistics are an optimization, and a
 * failure here must not fail the INSERT that produced the data.
 */
std::string buildIcebergColumnStatsJson(const parquet::FileMetaData *metadata);

}
}

#endif							/* DATALAKE_PARQUETCOLUMNSTATS_H */
