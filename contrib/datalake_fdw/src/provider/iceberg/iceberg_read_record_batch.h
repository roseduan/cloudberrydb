/*-------------------------------------------------------------------------
 *
 * iceberg_read_record_batch.h
 *    Vectorized reader for the FDW-iceberg path (CREATE FOREIGN TABLE ...
 *    OPTIONS(format 'iceberg')).  Reuses the iceberg task/metadata resolution
 *    (datalakeProtocolImportStart -> FileScanTask list) but, for each parquet
 *    data file, drives libparquet-19's parquet::arrow::FileReader to produce
 *    Arrow RecordBatches and hands them to the vectorization engine across the
 *    Arrow C Data Interface ("Scheme A"), sharing the column matching
 *    (matchParquetColumn, #401) and Arrow batch helpers
 *    (parquet_vec_batch_util) with the generic parquet vectorized reader (#424).
 *
 *    Scope: read-only, parquet data files only.  MOR position deletes are
 *    applied from a per-scan Roaring index; equality deletes are refused with
 *    an error so the query falls back to the row engine.  Columns are emitted in table
 *    attribute order (matching BuildDescForScanExtract) so the engine's
 *    columnmap aligns regardless of physical file column order / schema
 *    evolution.
 *
 *    IMPORTANT: like parquetReadRecordBatch.h, this header pulls in NO
 *    parquet-arrow C-ABI headers -- the parquet_arrow types appear only by
 *    pointer / shared_ptr behind forward declarations, and construction is
 *    exposed through a factory so provider.cpp (which also pulls the engine
 *    Arrow ABI) does not hit "redefinition of struct ArrowArray".
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 *-------------------------------------------------------------------------
 */
#ifndef DATALAKE_ICEBERG_READ_RECORD_BATCH_H
#define DATALAKE_ICEBERG_READ_RECORD_BATCH_H

/* Standard + parquet headers before the PG-pulling headers: PG's port.h
 * `#define snprintf pg_snprintf` breaks a later first-time libstdc++ `using
 * ::snprintf`.  (A .cpp that includes the parquet C-ABI must itself front-load
 * the parquet headers too -- see iceberg_read_record_batch.cpp.) */
#include <memory>
#include <vector>

#include "src/provider/parquet/read/parquetFileReader.h"
#include "src/common/rewrLogical.h"
#include "src/provider/provider.h"

extern "C" {
#include "src/provider/common/row_reader.h"
}

/* Forward declarations of the libparquet-19 (parquet_arrow) types referenced
 * only by pointer / shared_ptr -- no parquet-arrow header is included here. */
namespace parquet_arrow {
	class MemoryPool;
	class RecordBatch;
	class RecordBatchReader;
}
namespace parquet { namespace arrow { class FileReader; } }
namespace parquet { class ColumnReader; }

namespace Datalake {
namespace Internal {

/* Defined in the .cpp; holds the parquet-arrow ArrowArray / ArrowSchema. */
struct IcebergCBatch;

class icebergReadRecordBatch : public Provider, public readLogical
{
public:
	icebergReadRecordBatch();
	virtual ~icebergReadRecordBatch();

	virtual void createHandler(void *sstate);
	virtual int64_t read(void **recordBatch);
	virtual void destroyHandler();
	virtual void reScan();

private:
	/* Open the current FileScanTask's parquet data file: build the field-id
	 * column map, select row groups by the task byte range, and create the
	 * Arrow RecordBatchReader.  Returns false when the task list is exhausted. */
	bool openCurrentTask();
	void closeCurrentFile();
	/* Assemble one engine-Arrow batch (table-attribute order) from a raw
	 * parquet-arrow batch and export it into c_batch. */
	void assembleOut(const std::shared_ptr<::parquet_arrow::RecordBatch> &raw);
	/* (Re)open the low-level FLBA column readers for selected row group
	 * `selRgPos` (advances the decimal read cursor). */
	void openDecimalReadersForRg(int selRgPos);
	/* Read `nrows` of each low-level decimal column for the current batch and
	 * return the built numeric128 arrays (parallel to decimalCols). */
	std::vector<std::shared_ptr<::parquet_arrow::Array>> readDecimalBatch(int64_t nrows);

	DatalakeProtocolContext	   *protocolContext;
	List					   *fileScanTasks;		/* borrowed from the row reader */
	int							curTaskIndex;
	bool						fileOpen;

	parquetFileReader							fileReader;
	std::unique_ptr<::parquet::arrow::FileReader>		arrowReader;
	std::shared_ptr<::parquet_arrow::RecordBatchReader>	rbr;
	std::shared_ptr<::parquet_arrow::RecordBatch>		out;

	/* Emit plan for the current file, in table-attribute (ascending) order:
	 * emitAttr[k] = PG attnum index of the k-th emitted column;
	 * emitRawPos[k] = its position in the raw batch, or -1 for absent -> NULL,
	 * or EMIT_DECIMAL_FLBA (-2) for a numeric column read low-level (see below). */
	std::vector<int>			emitAttr;
	std::vector<int>			emitRawPos;

	/*
	 * Numeric(decimal) columns are excluded from the Arrow RecordBatchReader and
	 * read low-level as raw big-endian FLBA (TypedColumnReader<FLBAType>), then
	 * decoded straight to engine numeric128 via the fast precision-branched path
	 * (buildNumeric128FromFLBA) -- this skips libparquet's always-128-bit
	 * RawBytesToDecimalBytes/FromBigEndian decode and the intermediate
	 * Decimal128Array.  The low-level readers advance in lockstep with the Arrow
	 * batches: Arrow batches never cross a row-group boundary, so a per-row-group
	 * cursor (decimalRgCursor/decimalRowsInRg over selRowGroups) stays aligned.
	 */
	struct DecimalFlbaCol { int emitK; int physColIdx; int precision; int scale;
							int typeLength; int32 typeMod; };
	std::vector<DecimalFlbaCol>	decimalCols;
	std::vector<int>			selRowGroups;		/* file row-group index of each selected rg */
	std::vector<std::shared_ptr<::parquet::ColumnReader>> decimalReaders; /* for selRowGroups[decimalRgCursor] */
	int							decimalRgCursor;	/* selected-rg index the low-level readers are on */
	int64_t						decimalRowsInRg;	/* rows already consumed from that rg */

	/* Output-column indices re-encoded as numeric128 (re-tagged on export). */
	std::vector<int>			numericColIndices;

	/*
	 * MOR position-delete state.  deleteIndex maps data-file path -> Roaring
	 * bitmap of deleted absolute row positions (NULL if the scan has no
	 * position delete files); curBitmap is the current file's bitmap (NULL if
	 * this file has no deletes).  The per-file row-group table maps a row's
	 * index within the Arrow reader stream back to its absolute file position
	 * so the bitmap can be probed (selected row groups need not be contiguous).
	 */
	void					   *deleteIndex;	/* IcebergDeleteIndex * */
	void					   *deleteIndexOwner;	/* IcebergDeleteIndexOwner *, cleanup hook state */
	void					   *curBitmap;		/* Roaring bitmap for current file */
	std::vector<int64_t>		selAbsStart;	/* abs file start pos of each selected row group */
	std::vector<int64_t>		selNumRows;		/* rows in each selected row group */
	int							curSelRg;		/* cursor: selected row group index */
	int64_t						posInSelRg;		/* cursor: row offset within curSelRg */

	::parquet_arrow::MemoryPool	   *pool;
	std::unique_ptr<IcebergCBatch>	c_batch;
};

/* Factory: constructs an icebergReadRecordBatch where the parquet-arrow types
 * are complete (inside the .cpp), so provider.cpp need not include this class's
 * parquet-arrow internals. */
std::shared_ptr<Provider> makeIcebergReadRecordBatch();

}
}

#endif /* DATALAKE_ICEBERG_READ_RECORD_BATCH_H */
