/*-------------------------------------------------------------------------
 *
 * parquetReadRecordBatch.h
 *    Vectorized reader for the generic datalake_fdw Parquet path (issue #424).
 *
 *    Reuses parquetRead's file / row-group iteration but, instead of the
 *    row-wise scanners, drives libparquet-19's parquet::arrow::FileReader to
 *    produce Arrow RecordBatches and hands them to the vectorization engine
 *    across the Arrow C Data Interface ("Scheme A" -- see
 *    .router/plans/424/DESIGN.md).
 *
 *    IMPORTANT: this header pulls in NO parquet-arrow headers.  libparquet-19
 *    ships its own copy of the Arrow C ABI (parquet/internal/arrow/c/abi.h)
 *    with the same `struct ArrowArray` / `ArrowSchema` tags as the engine's
 *    fork (arrow/c/abi.h).  A translation unit that includes both (e.g.
 *    provider.cpp, which also pulls the engine ABI via orcReadRecordBatch.h)
 *    fails to compile with "redefinition of struct ArrowArray".  So we hold the
 *    parquet_arrow types only by pointer / shared_ptr behind forward
 *    declarations, keep every real parquet-arrow include inside the .cpp, and
 *    expose construction through a factory so callers never need the class's
 *    (parquet-arrow-typed) internals.
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 *-------------------------------------------------------------------------
 */
#ifndef DATALAKE_PARQUETREADRECORDBATCH_H
#define DATALAKE_PARQUETREADRECORDBATCH_H

#include "parquetRead.h"

#include <memory>
#include <vector>
#include <string>

/* Forward declarations of the libparquet-19 (parquet_arrow) types we reference
 * only by pointer / shared_ptr -- no parquet-arrow header is included here. */
namespace parquet_arrow {
	class MemoryPool;
	class RecordBatch;
	class RecordBatchReader;
	class Array;
	class Field;
	class DataType;
}
namespace parquet { namespace arrow { class FileReader; } }
namespace parquet { class ColumnReader; }

namespace Datalake {
namespace Internal {

/* Defined in the .cpp; holds the parquet-arrow ArrowArray / ArrowSchema. */
struct ParquetCBatch;

class parquetReadRecordBatch : public parquetRead
{
public:
	parquetReadRecordBatch();
	virtual ~parquetReadRecordBatch();

	virtual void createHandler(void *sstate);
	virtual int64_t read(void **recordBatch);
	virtual void destroyHandler();

private:
	/* readLogical hook: build the Arrow RecordBatchReader over the current
	 * file's already-selected row groups (tempRowGroupNums). */
	virtual bool getNextGroup();

	void restart();
	bool readNextRecordBatch();
	bool getRecordBatch();
	void buildColumnIndices(std::vector<int> &colIndices);
	void normalizeBatch(const std::shared_ptr<::parquet_arrow::RecordBatch> &raw);
	void normalizeColumn(Oid typeOid, int32 typeMod,
						 const std::shared_ptr<::parquet_arrow::Array> &in,
						 std::shared_ptr<::parquet_arrow::Array> &out_arr,
						 std::shared_ptr<::parquet_arrow::DataType> &out_type);
	void addColOrCreateRecordBatch(const std::shared_ptr<::parquet_arrow::Field> &field_id,
								   const std::shared_ptr<::parquet_arrow::Array> &column,
								   int64_t nrows, int batch_index);
	void recordBatchAddColumn(int mpp_index, int batch_index,
							  const std::string &partitionkey, Datum mpp_datum,
							  int64_t nrows);
	void recordBatchAddNullColumn(int mpp_index, int batch_index,
								  const std::string &field_name, int64_t nrows);

	::parquet_arrow::MemoryPool				   *pool;
	std::unique_ptr<::parquet::arrow::FileReader>			arrowReader;
	std::shared_ptr<::parquet_arrow::RecordBatchReader>		rbr;
	std::shared_ptr<::parquet_arrow::RecordBatch>			out;
	std::vector<int>							readColIndices;
	/* Output-column indices in `out` whose values were re-encoded into the
	 * engine numeric128 layout (carried as fixed_size_binary(16)); read() re-tags
	 * exactly these on export.  Rebuilt per batch. */
	std::vector<int>							numericColIndices;
	std::unique_ptr<ParquetCBatch>				c_batch;

	/*
	 * Numeric(decimal) columns stored as FIXED_LEN_BYTE_ARRAY are excluded from
	 * the Arrow RecordBatchReader and read low-level as raw big-endian FLBA, then
	 * decoded straight to numeric128 (buildNumeric128FromFLBA) -- skipping
	 * libparquet's always-128-bit FromBigEndian decode.  readRawPos[k] gives the
	 * raw-batch position of output column k, or -2 for a low-level decimal column
	 * (see decimalCols).  Aligned via a per-row-group cursor over selRowGroups
	 * (Arrow batches never cross a row-group boundary).  Same scheme as
	 * iceberg_read_record_batch. */
	struct DecimalFlbaCol { int outK; int physColIdx; int precision; int scale; int typeLength; };
	std::vector<DecimalFlbaCol>	decimalCols;
	std::vector<int>			readRawPos;			/* per output col: raw pos, or -2 decimal */
	std::vector<int>			selRowGroups;		/* file row-group index of each selected rg */
	std::vector<int64_t>		selNumRows;			/* rows in each selected rg */
	std::vector<std::shared_ptr<::parquet::ColumnReader>> decimalReaders;
	int							decimalRgCursor;
	int64_t						decimalRowsInRg;

	void openDecimalReadersForRg(int selRgPos);
	std::vector<std::shared_ptr<::parquet_arrow::Array>> readDecimalBatch(int64_t nrows);
};

/* Factory: constructs a parquetReadRecordBatch where the parquet-arrow types
 * are complete (i.e. inside the .cpp), so provider.cpp need not include this
 * class's parquet-arrow internals. */
std::shared_ptr<Provider> makeParquetReadRecordBatch();

}
}

#endif
