/*-------------------------------------------------------------------------
 *
 * parquet_vec_batch_util.h
 *    Shared helpers for building engine-Arrow RecordBatches out of
 *    libparquet-19 (parquet_arrow) columns: per-type normalization,
 *    constant / NULL column builders, numeric128 encoding, and the C-ABI
 *    "D128" re-tag.
 *
 *    Extracted verbatim from parquetReadRecordBatch (issue #424) so the
 *    FDW-iceberg vectorized reader shares the exact same conversion rules.
 *
 *    Like parquetReadRecordBatch.h, this header pulls in NO parquet-arrow
 *    headers: parquet_arrow types appear only by shared_ptr / pointer behind
 *    forward declarations, so a translation unit that also includes the engine
 *    Arrow ABI does not hit the "redefinition of struct ArrowArray" clash.
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 *-------------------------------------------------------------------------
 */
#ifndef DATALAKE_PARQUET_VEC_BATCH_UTIL_H
#define DATALAKE_PARQUET_VEC_BATCH_UTIL_H

#include <memory>
#include <vector>
#include <string>
#include <cstdint>
#include <cstring>

extern "C" {
#include "postgres.h"
#include "access/tupdesc.h"
}

/* Forward declarations of the libparquet-19 (parquet_arrow) types referenced
 * only by pointer / shared_ptr -- no parquet-arrow header is included here. */
namespace parquet_arrow {
	class MemoryPool;
	class RecordBatch;
	class Array;
	class Field;
	class DataType;
	class FixedSizeBinaryBuilder;
}
/* Low-level libparquet-19 type referenced only by pointer (raw FLBA decimal
 * values read via TypedColumnReader<FLBAType>, bypassing the decimal128 decode). */
namespace parquet { struct FixedLenByteArray; }
struct ArrowSchema;			/* C Data Interface schema (shared ABI tag) */

/* Raise a PG ERROR from a parquet_arrow Status/Result failure. */
#define PARQUET_VEC_CHECK(st, what)											\
	do {																	\
		auto _vs = (st);													\
		if (!_vs.ok())														\
			elog(ERROR, "datalake parquet vectorized read: %s failed: %s",	\
				 (what), _vs.ToString().c_str());							\
	} while (0)

namespace Datalake {
namespace Internal {

/*
 * Rows per Arrow batch the vectorized parquet readers emit (ArrowReaderProperties
 * batch_size; libparquet's default is 64K).  Batch COUNT -- not just row count --
 * is a real downstream cost: a Redistribute Motion hash-splits every input batch
 * into one send per target segment (32-way = 32 chunks per batch, measured
 * exactly), and the sonic aggregator then pays fixed per-batch costs on every
 * chunk (MaybeSpill's full memory walk, RowTable block churn).  PAX's Vec Seq
 * Scan emits ~112K-row batches; at the 64K default an iceberg/parquet scan of the
 * same data emitted 1.74x more batches and burned ~1.5x the aggregate CPU on an
 * identical plan.  128K keeps the parquet scan's batch count at or below PAX's.
 * (Batches still never cross a row-group or file boundary -- this is only an
 * upper bound -- so the FLBA decimal row-group cursor alignment is unaffected.)
 */
#define PARQUET_VEC_BATCH_ROWS 131072

/*
 * The engine's numeric128 (arrow fork's Numeric128Type) is a FixedSizeBinary(16)
 * whose 16 little-endian bytes are [low_bits_ (8)][high_bits_ (8)], where
 *   high_bits_ = (scale << 56) | (high64 & ((1<<56)-1))
 * i.e. the top 8 bits of the high word hold the scale.  parquet::arrow (arrow-19)
 * reads a DECIMAL column back as a plain decimal128 whose unscaled int128 value
 * is (value * 10^scale); re-pack that (low64, high64, scale) into the engine's
 * numeric128 byte layout.  Only precision <= 35 reaches here (the planner guard),
 * so the value fits in the 120 signed bits left after the scale header and
 * masking never clobbers the sign bit (bit 55).
 */
static inline void
encodeNumeric128(uint64_t low64, int64_t high64, int32_t scale, uint8_t out[16])
{
	static const uint64_t kHighValueMask = (1ULL << 56) - 1;
	uint64_t enc_high = ((uint64_t) scale << 56) | ((uint64_t) high64 & kHighValueMask);

	memcpy(out, &low64, sizeof(low64));
	memcpy(out + sizeof(low64), &enc_high, sizeof(enc_high));
}

/*
 * Normalize one raw parquet_arrow column (whose type comes from the FILE) into
 * the engine's expected Arrow array/type for PG column type `typeOid`.  ERRORs
 * clearly on a type mismatch instead of over-reading a bad cast.
 */
void normalizeParquetVecColumn(parquet_arrow::MemoryPool *pool, Oid typeOid,
			const std::shared_ptr<parquet_arrow::Array> &in,
			std::shared_ptr<parquet_arrow::Array> &out_arr,
			std::shared_ptr<parquet_arrow::DataType> &out_type);

/*
 * Append one ReadBatch chunk of raw parquet FLBA decimal values to an engine
 * numeric128 builder (fixed_size_binary(16), retagged D128 at export): `vals`
 * holds this chunk's dense (non-null) big-endian values, `defs` its `levels`
 * def levels (one per row, NULL when the column has no nulls / maxDef==0).
 * Uses the fast precision-branched decode (FLBA_to_int64 for precision<=18,
 * else FLBA_to_int128), skipping the library's always-128-bit FromBigEndian.
 *
 * MUST be called on each chunk BEFORE the next ReadBatch: a FixedLenByteArray's
 * `ptr` points into the column reader's current data page buffer, which is
 * released as soon as the reader advances to the next page, so retaining the
 * FLBAs across ReadBatch calls and decoding them in one pass dereferences freed
 * memory (crashed TPC-DS q14 on nullable decimal(7,2) at 128K batches).
 */
void appendNumeric128FromFLBA(parquet_arrow::FixedSizeBinaryBuilder &b,
			const parquet::FixedLenByteArray *vals,
			const int16_t *defs, int16_t maxDef, int64_t levels,
			int typeLength, int precision, int scale);

/* Append `column` to `out`, creating the RecordBatch on the first column. */
void parquetVecAddColOrCreate(std::shared_ptr<parquet_arrow::RecordBatch> &out,
			const std::shared_ptr<parquet_arrow::Field> &field,
			const std::shared_ptr<parquet_arrow::Array> &column,
			int64_t nrows, int batch_index);

/*
 * Constant column carrying a partition-key Datum for PG column `mpp_index`.
 * A NUMERIC value's output index is recorded in numericColIndices for re-tag.
 */
void parquetVecAddConstColumn(parquet_arrow::MemoryPool *pool,
			std::shared_ptr<parquet_arrow::RecordBatch> &out,
			std::vector<int> &numericColIndices, TupleDesc tupdesc,
			int mpp_index, int batch_index, const std::string &name,
			Datum value, int64_t nrows);

/* All-NULL column for PG column `mpp_index` (absent from the file: schema
 * evolution / trailing added column). */
void parquetVecAddNullColumn(parquet_arrow::MemoryPool *pool,
			std::shared_ptr<parquet_arrow::RecordBatch> &out,
			std::vector<int> &numericColIndices, TupleDesc tupdesc,
			int mpp_index, int batch_index, const std::string &name, int64_t nrows);

/*
 * Re-tag the exported C-ABI schema children listed in numericColIndices from
 * arrow-19's "w:16" (fixed_size_binary(16)) to the engine's private "D128"
 * numeric128 format, so the engine's bridge reconstructs numeric128().
 */
void parquetVecRetagNumeric128(struct ArrowSchema *schema,
			const std::vector<int> &numericColIndices);

}
}

#endif /* DATALAKE_PARQUET_VEC_BATCH_UTIL_H */
