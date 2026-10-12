/*-------------------------------------------------------------------------
 *
 * parquetColumnStats.cpp
 *    Wire-format v1 column statistics collected from a closed parquet file.
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 * IDENTIFICATION
 *        contrib/datalake_fdw/src/provider/parquet/write/parquetColumnStats.cpp
 *-------------------------------------------------------------------------
 */
#include "parquetColumnStats.h"

#include <memory>
#include <vector>
#include <parquet/schema.h>
#include <parquet/statistics.h>
#include <parquet/types.h>

extern "C" {
#include "common/base64.h"
}

namespace Datalake {
namespace Internal {

namespace {

/* One row group's bound pair for one column, still parquet-encoded. */
struct ColumnBound
{
	std::string	lower;
	std::string	upper;
};

struct ColumnStat
{
	int32_t		fieldId;
	std::string	typeTag;
	int64_t		valueCount;		/* includes nulls -- see valueCount note */
	int64_t		nullCount;
	int64_t		columnSize;
	bool		hasNullCount;
	bool		boundsUsable;
	std::vector<ColumnBound> bounds;
};

/*
 * Map the column's PARQUET FILE type to the tag the agent interprets its
 * bound bytes with.
 *
 * Deliberately derived from the file schema, not from the PostgreSQL column
 * type: the agent's existing footer-reading path derives the Iceberg type the
 * same way, and for three columns the two disagree.  "char", time and interval
 * are written as BYTE_ARRAY with no converted type, so the file says binary
 * while the table schema says string -- truncating them as strings would not
 * match what the old path produced.
 *
 * Returns false for a type this writer never emits (INT96, plain fixed-length
 * byte arrays); the caller then sends no statistics for that column.
 */
bool
columnTypeTag(const parquet::ColumnDescriptor *descr, std::string &tag)
{
	const std::shared_ptr<const parquet::LogicalType> &logical =
		descr->logical_type();

	switch (descr->physical_type())
	{
		case parquet::Type::BOOLEAN:
			tag = "bool";
			return true;
		case parquet::Type::INT32:
			if (logical && logical->is_date())
				tag = "date";
			else
				tag = "i32";
			return true;
		case parquet::Type::INT64:
			if (logical && logical->is_timestamp())
			{
				const auto &ts =
					static_cast<const parquet::TimestampLogicalType &>(*logical);

				tag = ts.is_adjusted_to_utc() ? "tstz" : "ts";
			}
			else
				tag = "i64";
			return true;
		case parquet::Type::FLOAT:
			tag = "f32";
			return true;
		case parquet::Type::DOUBLE:
			tag = "f64";
			return true;
		case parquet::Type::BYTE_ARRAY:
			if ((logical && logical->is_string()) ||
				descr->converted_type() == parquet::ConvertedType::UTF8)
				tag = "str";
			else
				tag = "bin";
			return true;
		case parquet::Type::FIXED_LEN_BYTE_ARRAY:
			if ((logical && logical->is_decimal()) ||
				descr->converted_type() == parquet::ConvertedType::DECIMAL)
			{
				/*
				 * Precision and scale travel with the column: the agent needs
				 * them to renormalize the fixed-width big-endian bytes this
				 * writer emits into Iceberg's minimum-width encoding.
				 */
				tag = "dec(" + std::to_string(descr->type_precision()) + "," +
					std::to_string(descr->type_scale()) + ")";
				return true;
			}
			return false;
		default:
			return false;
	}
}

/*
 * Byte width a bound must have for a fixed-width physical type, or -1 for the
 * variable-width one (BYTE_ARRAY).
 */
int
fixedBoundWidth(const parquet::ColumnDescriptor *descr)
{
	switch (descr->physical_type())
	{
		case parquet::Type::BOOLEAN:
			return 1;
		case parquet::Type::INT32:
		case parquet::Type::FLOAT:
			return 4;
		case parquet::Type::INT64:
		case parquet::Type::DOUBLE:
			return 8;
		case parquet::Type::FIXED_LEN_BYTE_ARRAY:
			return descr->type_length();
		default:
			return -1;
	}
}

/*
 * Decide whether one row group contributes a usable bound pair for a column.
 *
 * Three outcomes, and the difference between the last two is the whole point:
 *
 *   contribute   -- both bounds present and within the ceiling.
 *   skip         -- the chunk holds no non-null value, so it constrains
 *                   nothing and leaving it out keeps the merged bound honest.
 *   unusable     -- the chunk holds values whose range we cannot see, so the
 *                   column must lose its bounds entirely.  Merging the other
 *                   row groups instead would produce a bound that excludes
 *                   real values, and a too-tight bound makes a scan skip a
 *                   file that has matching rows.
 *
 * The unusable case is not hypothetical.  parquet's writer drops an oversized
 * min and an oversized max independently (max_statistics_size, 4096 B by
 * default), while the metadata reader reports HasMinMax() if EITHER survived
 * -- so a single-sided drop reads back as "has statistics" with one side an
 * empty string.  For a fixed-width type any length but the type's own is that
 * anomaly.  For BYTE_ARRAY the two sides are not symmetric:
 *
 *   an empty LOWER bound is always safe to believe.  Either it is the empty
 *   string, which really is the smallest value, or the min was dropped for
 *   size -- and "" is still below whatever that huge value was.  A lower
 *   bound can only err by being too low, which costs pruning, not rows.
 *
 *   an empty UPPER bound is not.  It means either that every value in the
 *   chunk is the empty string, or that an oversized max was dropped -- and
 *   believing the second would put the file's ceiling below rows it actually
 *   contains, so a range scan would skip it.  The two are indistinguishable
 *   here, so the chunk is refused.  The only thing that costs is a bound on a
 *   column of nothing but empty strings; a dropped max was over 4096 B and
 *   would have failed the ceiling below in any case.
 */
enum ChunkVerdict
{
	CHUNK_CONTRIBUTE,
	CHUNK_SKIP,
	CHUNK_UNUSABLE
};

ChunkVerdict
chunkBound(const parquet::ColumnDescriptor *descr,
		   const parquet::ColumnChunkMetaData *chunk,
		   const parquet::Statistics *stats,
		   ColumnBound &out)
{
	int			width;

	/* Nothing but nulls here: no values, so no constraint to report. */
	if (chunk->num_values() - stats->null_count() <= 0)
		return CHUNK_SKIP;

	if (!stats->HasMinMax())
		return CHUNK_UNUSABLE;

	out.lower = stats->EncodeMin();
	out.upper = stats->EncodeMax();

	width = fixedBoundWidth(descr);
	if (width >= 0)
	{
		if ((int) out.lower.size() != width || (int) out.upper.size() != width)
			return CHUNK_UNUSABLE;
	}
	else if (out.upper.empty())
		return CHUNK_UNUSABLE;

	if (out.lower.size() > ICEBERG_COLUMN_BOUND_MAX_BYTES ||
		out.upper.size() > ICEBERG_COLUMN_BOUND_MAX_BYTES)
		return CHUNK_UNUSABLE;

	return CHUNK_CONTRIBUTE;
}

/* Base64-encode raw bound bytes for the JSON payload. */
std::string
encodeBound(const std::string &raw)
{
	int			dstlen = pg_b64_enc_len((int) raw.size());
	std::vector<char> dst(dstlen > 0 ? dstlen : 1);
	int			written = pg_b64_encode(raw.data(), (int) raw.size(),
									   dst.data(), dstlen);

	if (written < 0)
		return std::string();

	return std::string(dst.data(), (size_t) written);
}

}							/* anonymous namespace */

std::string
buildIcebergColumnStatsJson(const parquet::FileMetaData *metadata)
{
	std::vector<ColumnStat> stats;
	std::string	json;

	if (metadata == nullptr)
		return std::string();

	try
	{
		const parquet::SchemaDescriptor *schema = metadata->schema();
		int			numColumns = metadata->num_columns();
		int			numRowGroups = metadata->num_row_groups();

		for (int col = 0; col < numColumns; col++)
		{
			const parquet::ColumnDescriptor *descr = schema->Column(col);
			ColumnStat	cs;

			cs.fieldId = descr->schema_node()->field_id();
			cs.valueCount = 0;
			cs.nullCount = 0;
			cs.columnSize = 0;
			cs.hasNullCount = true;
			cs.boundsUsable = true;

			/*
			 * A column with no field id cannot be placed in the manifest at
			 * all; leave it out and let the agent see one fewer entry, exactly
			 * as its own footer reader would.
			 */
			if (cs.fieldId < 0)
				continue;
			if (!columnTypeTag(descr, cs.typeTag))
				continue;

			for (int rg = 0; rg < numRowGroups; rg++)
			{
				std::unique_ptr<parquet::RowGroupMetaData> rowGroup =
					metadata->RowGroup(rg);
				std::unique_ptr<parquet::ColumnChunkMetaData> chunk =
					rowGroup->ColumnChunk(col);
				std::shared_ptr<parquet::Statistics> chunkStats;
				ColumnBound	bound;

				/*
				 * value_count must include nulls -- it is the manifest's
				 * valueCounts, which Iceberg pairs with nullValueCounts.
				 * ColumnChunkMetaData::num_values() counts every value;
				 * Statistics::num_values() counts only the non-null ones and
				 * would silently understate it.
				 */
				cs.valueCount += chunk->num_values();
				cs.columnSize += chunk->total_compressed_size();

				if (!chunk->is_stats_set())
				{
					cs.hasNullCount = false;
					cs.boundsUsable = false;
					continue;
				}

				chunkStats = chunk->statistics();
				if (chunkStats == nullptr)
				{
					cs.hasNullCount = false;
					cs.boundsUsable = false;
					continue;
				}

				if (chunkStats->HasNullCount())
					cs.nullCount += chunkStats->null_count();
				else
					cs.hasNullCount = false;

				if (!cs.boundsUsable)
					continue;

				switch (chunkBound(descr, chunk.get(), chunkStats.get(), bound))
				{
					case CHUNK_CONTRIBUTE:
						cs.bounds.push_back(bound);
						break;
					case CHUNK_SKIP:
						break;
					case CHUNK_UNUSABLE:
						cs.boundsUsable = false;
						cs.bounds.clear();
						break;
				}
			}

			stats.push_back(cs);
		}

		if (stats.empty())
			return std::string();

		json = "{\"v\":1,\"cols\":[";
		for (size_t i = 0; i < stats.size(); i++)
		{
			const ColumnStat &cs = stats[i];

			if (i > 0)
				json += ',';
			json += "{\"id\":" + std::to_string(cs.fieldId);
			json += ",\"t\":\"" + cs.typeTag + "\"";
			json += ",\"vc\":" + std::to_string(cs.valueCount);
			if (cs.hasNullCount)
				json += ",\"nc\":" + std::to_string(cs.nullCount);
			json += ",\"sz\":" + std::to_string(cs.columnSize);

			/*
			 * No "rg" means no bound for this column, which the agent reads as
			 * "unknown range" -- never as "empty range".
			 */
			if (cs.boundsUsable && !cs.bounds.empty())
			{
				json += ",\"rg\":[";
				for (size_t b = 0; b < cs.bounds.size(); b++)
				{
					if (b > 0)
						json += ',';
					json += "{\"lo\":\"" + encodeBound(cs.bounds[b].lower) +
						"\",\"hi\":\"" + encodeBound(cs.bounds[b].upper) + "\"}";
				}
				json += ']';
			}
			json += '}';
		}
		json += "]}";
	}
	catch (...)
	{
		/*
		 * Statistics are an optimization.  Whatever the footer did that we did
		 * not expect, the file itself is written and committed; report nothing
		 * and let the agent fall back to reading it.
		 */
		return std::string();
	}

	return json;
}

}
}
