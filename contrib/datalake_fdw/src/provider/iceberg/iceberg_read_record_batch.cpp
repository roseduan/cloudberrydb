/*-------------------------------------------------------------------------
 *
 * iceberg_read_record_batch.cpp
 *    Vectorized reader for the FDW-iceberg path.  See iceberg_read_record_batch.h.
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 *-------------------------------------------------------------------------
 */
/*
 * ALL C++ standard + parquet headers first, before any PostgreSQL header.
 * PG's port.h does `#define snprintf pg_snprintf`; if a libstdc++ header (e.g.
 * <cstdio>, pulled transitively by the parquet headers) is first included AFTER
 * that macro, its `namespace std { using ::snprintf; }` becomes `using
 * ::pg_snprintf` and fails to compile.  Front-loading them (subsequent
 * re-includes are guarded no-ops) keeps every such `using` bound to the real
 * ::snprintf.
 */
#include <cstdio>
#include <cstring>
#include <memory>
#include <vector>
#include <set>
#include <map>
#include <string>

#include <parquet/api/reader.h>
#include <parquet/arrow/reader.h>
#include <parquet/internal/arrow/api.h>
#include <parquet/internal/arrow/c/bridge.h>

#include "iceberg_read_record_batch.h"
#include "src/provider/parquet/read/parquet_vec_batch_util.h"
#include "src/provider/common/parquet_reader.h"		/* matchParquetColumn, parquetFileHasFieldIds */
#include "src/provider/common/common.h"				/* convertToGopherPath */

extern "C" {
#include "miscadmin.h"
#include "utils/builtins.h"
#include "src/dlproxy/datalake.h"					/* FileScanTask, FileFragment */
#include "src/provider/common/utils.h"
}

namespace Datalake {
namespace Internal {

/* C-ABI hand-off struct, layout-identical to the engine's ArrowRecordBatch. */
struct IcebergCBatch
{
	struct ArrowArray  batch;
	struct ArrowSchema schema;
};

std::shared_ptr<Provider>
makeIcebergReadRecordBatch()
{
	return std::make_shared<icebergReadRecordBatch>();
}

icebergReadRecordBatch::icebergReadRecordBatch()
	: protocolContext(nullptr), fileScanTasks(nullptr), curTaskIndex(0),
	  fileOpen(false), pool(nullptr), c_batch(new IcebergCBatch())
{}

icebergReadRecordBatch::~icebergReadRecordBatch() = default;

void
icebergReadRecordBatch::createHandler(void *sstate)
{
	initParameter(sstate);
	protocolContext = datalakeCreateContext(scanstate->options);
	datalakeProtocolImportStart(scanstate, protocolContext, includes_columns);
	fileScanTasks = protocolContext->file->reader->fileScanTasks;
	curTaskIndex = 0;
	fileOpen = false;
	pool = ::parquet_arrow::default_memory_pool();
}

/* Mirrors ParquetReader::invalidFileOffset (parquet_reader.cpp). */
static bool
icebergInvalidFileOffset(int64_t startIndex, int64_t preStartIndex, int64_t preCompressedSize)
{
	if (preStartIndex == 0 && startIndex != 4)
		return true;
	if (startIndex < preStartIndex + preCompressedSize)
		return true;
	return false;
}

bool
icebergReadRecordBatch::openCurrentTask()
{
	while (curTaskIndex < list_length(fileScanTasks))
	{
		FileScanTask *task = (FileScanTask *) list_nth(fileScanTasks, curTaskIndex);
		FileFragment *dataFile = task->dataFile;

		/* Iceberg data-file paths are full URIs (s3://bucket/...); strip the
		 * scheme+bucket to the gopher-relative path, as the row reader does
		 * (ParquetReader::open -> convertToGopherPath). */
		std::string fpath(dataFile->filePath);
		std::string gopherPath = convertToGopherPath(fpath);

		if (!fileReader.createParquetReader(protocolContext->file->fileStream,
											gopherPath, options))
		{
			curTaskIndex++;
			continue;
		}

		auto metadata = fileReader.getFileMetadata();
		const parquet::SchemaDescriptor *schema = metadata->schema();
		bool hasFieldIds = parquetFileHasFieldIds(schema);

		/*
		 * Build the emit plan in table-attribute (ascending) order -- the order
		 * BuildDescForScanExtract compacts referenced columns into, so the vec
		 * engine's columnmap aligns regardless of physical file column order.
		 */
		List *datafileDesc = protocolContext->file->reader->datafileDesc;
		emitAttr.clear();
		emitRawPos.clear();
		std::vector<int> emitPhysical;

		ListCell *lc;
		int i = 0;
		foreach_with_count(lc, datafileDesc, i)
		{
			if (i >= (int) ncolumns)
				break;
			if (!includes_columns[i])
				continue;
			DatalakeFieldDescription *fd = (DatalakeFieldDescription *) lfirst(lc);
			emitAttr.push_back(i);
			emitPhysical.push_back(matchParquetColumn(schema, fd, hasFieldIds));
		}
		/*
		 * count(*) / no referenced column: emit attr 0 (NULL-filled) so the
		 * batch still carries the row count the engine expects (see
		 * BuildDescForScanExtract, which forces col[0]).
		 */
		if (emitAttr.empty())
		{
			emitAttr.push_back(0);
			emitPhysical.push_back(-1);
		}

		/* Distinct physical columns, sorted ascending -> deterministic raw
		 * batch column order regardless of Arrow's own ordering. */
		std::set<int> physSet;
		for (int p : emitPhysical)
			if (p >= 0)
				physSet.insert(p);
		std::vector<int> colIndices(physSet.begin(), physSet.end());
		std::map<int, int> physToRaw;
		for (size_t j = 0; j < colIndices.size(); j++)
			physToRaw[colIndices[j]] = (int) j;

		emitRawPos.resize(emitPhysical.size());
		for (size_t k = 0; k < emitPhysical.size(); k++)
			emitRawPos[k] = (emitPhysical[k] >= 0) ? physToRaw[emitPhysical[k]] : -1;

		/*
		 * Select row groups by the task byte range (mirrors the row path's
		 * ParquetReader::filterRowGroupByOffset midpoint rule), so a data file
		 * split across tasks is read exactly once.
		 */
		std::vector<int> rowGroups;
		int64_t startOffset = task->start;
		int64_t endOffset = task->start + task->length;
		int64_t preStartIndex = 0, preCompressedSize = 0;
		for (int rg = 0; rg < metadata->num_row_groups(); rg++)
		{
			if (startOffset == -1)
			{
				rowGroups.push_back(rg);
				continue;
			}
			auto rgmd = metadata->RowGroup(rg);
			int64_t startIndex = rgmd->file_offset();
			if (icebergInvalidFileOffset(startIndex, preStartIndex, preCompressedSize))
				startIndex = (preStartIndex == 0) ? 4 : (preStartIndex + preCompressedSize);
			preStartIndex = startIndex;
			preCompressedSize = rgmd->total_compressed_size();
			int64_t totalSize = 0;
			for (int j = 0; j < rgmd->num_columns(); j++)
				totalSize += rgmd->ColumnChunk(j)->total_compressed_size();
			int64_t midPoint = startIndex + totalSize / 2;
			if (midPoint >= startOffset && midPoint < endOffset)
				rowGroups.push_back(rg);
		}

		if (rowGroups.empty())
		{
			fileReader.closeParquetReader();
			curTaskIndex++;
			continue;
		}

		/* Build the Arrow RecordBatchReader over the selected row groups. */
		::parquet::ArrowReaderProperties arrowProps;
		arrowProps.set_use_threads(false);
		arrowProps.set_pre_buffer(false);
		arrowProps.set_arrow_extensions_enabled(false);

		::parquet::arrow::FileReaderBuilder builder;
		PARQUET_VEC_CHECK(builder.Open(fileReader.getFileStream(),
									   ::parquet::default_reader_properties(),
									   metadata),
						  "open arrow reader");
		builder.memory_pool(pool);
		builder.properties(arrowProps);
		rbr.reset();
		arrowReader.reset();
		PARQUET_VEC_CHECK(builder.Build(&arrowReader), "build arrow reader");

		std::unique_ptr<::parquet_arrow::RecordBatchReader> reader;
		PARQUET_VEC_CHECK(arrowReader->GetRecordBatchReader(rowGroups, colIndices, &reader),
						  "get record batch reader");
		rbr = std::move(reader);

		fileOpen = true;
		return true;
	}
	return false;
}

void
icebergReadRecordBatch::closeCurrentFile()
{
	rbr.reset();
	arrowReader.reset();
	if (fileOpen)
		fileReader.closeParquetReader();
	fileOpen = false;
}

void
icebergReadRecordBatch::assembleOut(const std::shared_ptr<::parquet_arrow::RecordBatch> &raw)
{
	int64_t nrows = raw->num_rows();
	out.reset();
	numericColIndices.clear();

	for (size_t k = 0; k < emitAttr.size(); k++)
	{
		int attr = emitAttr[k];
		int rawPos = emitRawPos[k];
		Oid typeOid = tupdesc->attrs[attr].atttypid;
		std::string name(tupdesc->attrs[attr].attname.data);
		int batch_index = (int) k;

		if (rawPos >= 0)
		{
			std::shared_ptr<::parquet_arrow::Array> normArr;
			std::shared_ptr<::parquet_arrow::DataType> normType;
			normalizeParquetVecColumn(pool, typeOid, raw->column(rawPos), normArr, normType);
			parquetVecAddColOrCreate(out, ::parquet_arrow::field(name, normType),
									 normArr, nrows, batch_index);
			if (typeOid == NUMERICOID)
				numericColIndices.push_back(batch_index);
		}
		else
		{
			parquetVecAddNullColumn(pool, out, numericColIndices, tupdesc,
									attr, batch_index, name, nrows);
		}
	}

	PARQUET_VEC_CHECK(::parquet_arrow::ExportRecordBatch(*out.get(),
						&c_batch->batch, &c_batch->schema),
					  "export record batch");
	parquetVecRetagNumeric128(&c_batch->schema, numericColIndices);
}

int64_t
icebergReadRecordBatch::read(void **recordBatch)
{
	for (;;)
	{
		if (QueryFinishPending || QueryCancelPending)
			return 0;

		if (!fileOpen && !openCurrentTask())
			return 0;

		std::shared_ptr<::parquet_arrow::RecordBatch> raw;
		PARQUET_VEC_CHECK(rbr->ReadNext(&raw), "read next batch");
		if (raw == nullptr)
		{
			closeCurrentFile();
			curTaskIndex++;
			continue;
		}

		assembleOut(raw);
		*recordBatch = c_batch.get();
		return 1;
	}
}

void
icebergReadRecordBatch::destroyHandler()
{
	closeCurrentFile();
	out.reset();
	if (protocolContext)
	{
		protocolContext->record = NULL;
		datalakeCleanupContext(protocolContext);
		protocolContext = NULL;
	}
	releaseResources();
}

void
icebergReadRecordBatch::reScan()
{
	closeCurrentFile();
	curTaskIndex = 0;
}

}
}
