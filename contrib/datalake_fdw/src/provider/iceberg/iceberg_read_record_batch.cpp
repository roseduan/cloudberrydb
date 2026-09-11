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
#include <parquet/internal/arrow/compute/api.h>		/* parquet_arrow::compute::Filter */

#include "iceberg_read_record_batch.h"
#include "src/provider/parquet/read/parquet_vec_batch_util.h"
#include "src/provider/common/parquet_reader.h"		/* matchParquetColumn, parquetFileHasFieldIds */
#include "src/provider/common/common.h"				/* convertToGopherPath */

extern "C" {
#include "miscadmin.h"
#include "utils/builtins.h"
#include "src/dlproxy/datalake.h"					/* FileScanTask, FileFragment */
#include "src/provider/common/utils.h"
#include "src/provider/iceberg/iceberg_delete_index.h"	/* position-delete Roaring index */
#include "src/provider/common/delete_bitmap_c.h"		/* datalakeBitmapContains */
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
	  fileOpen(false), decimalRgCursor(0), decimalRowsInRg(0),
	  deleteIndex(nullptr), curBitmap(nullptr), curSelRg(0), posInSelRg(0),
	  pool(nullptr), c_batch(new IcebergCBatch())
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

	/*
	 * Build the global position-delete index once (data-file path -> Roaring
	 * bitmap of deleted positions); NULL when the scan has no position delete
	 * files.  Equality deletes are handled separately (not yet).
	 */
	deleteIndex = icebergBuildDeleteIndex(TopMemoryContext,
										  protocolContext->file->fileStream,
										  fileScanTasks);
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

		/*
		 * Equality deletes are not yet applied in the vectorized path (they can
		 * only be produced by external streaming writers -- the iceberg AM writes
		 * position deletes -- so there is no local way to exercise them).  Refuse
		 * clearly rather than silently return rows an equality delete removed.
		 */
		{
			ListCell *dc;
			foreach(dc, task->deletes)
			{
				FileFragment *df = (FileFragment *) lfirst(dc);
				if (df->content == EQUALITY_DELETES)
					elog(ERROR, "datalake vectorized FDW-iceberg read does not support "
						 "equality deletes yet; run with vectorization off");
			}
		}

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
		 * The row reader's datafileDesc IS the engine's compact scan descriptor:
		 * createFieldDescription builds it from scanstate->scan_tupdesc, which for
		 * a vec ForeignScan is the BuildDescForScanExtract projection of the
		 * referenced columns (targetlist + qual), in that compact order.  Emit
		 * exactly one output column per datafileDesc entry, in order, so the batch
		 * matches the engine's columnmap.  Do NOT re-filter by includes_columns:
		 * that array is indexed by full-table attno and would mis-map onto (or
		 * drop entries from) the compact list.
		 */
		List *datafileDesc = protocolContext->file->reader->datafileDesc;
		TupleDesc sd = scanstate->scan_tupdesc;
		emitAttr.clear();
		emitRawPos.clear();
		decimalCols.clear();
		std::vector<int> emitPhysical;

		ListCell *lc;
		foreach(lc, datafileDesc)
		{
			DatalakeFieldDescription *fd = (DatalakeFieldDescription *) lfirst(lc);
			/* index into scan_tupdesc (== position in datafileDesc) */
			emitAttr.push_back((int) emitAttr.size());
			emitPhysical.push_back(matchParquetColumn(schema, fd, hasFieldIds));
		}

		/*
		 * Route numeric(decimal) columns physically stored as FIXED_LEN_BYTE_ARRAY
		 * to the low-level FLBA path (buildNumeric128FromFLBA -- fast
		 * precision-branched decode), and exclude them from the Arrow reader so we
		 * never pay libparquet's always-128-bit FromBigEndian decode.  A decimal
		 * stored some other way (int32/int64) falls through to the Arrow +
		 * normalizeParquetVecColumn path.
		 */
		std::set<int> decimalPhys;
		for (size_t k = 0; k < emitPhysical.size(); k++)
		{
			int phys = emitPhysical[k];
			if (phys < 0 || sd->attrs[emitAttr[k]].atttypid != NUMERICOID)
				continue;
			const parquet::ColumnDescriptor *cd = schema->Column(phys);
			auto lt = cd->logical_type();
			if (cd->physical_type() != parquet::Type::FIXED_LEN_BYTE_ARRAY ||
				!lt || !lt->is_decimal())
				continue;
			const auto &dt = static_cast<const parquet::DecimalLogicalType &>(*lt);
			decimalCols.push_back({ (int) k, phys, dt.precision(), dt.scale(),
									cd->type_length() });
			decimalPhys.insert(phys);
		}

		/* Distinct physical columns for the Arrow reader (decimals excluded),
		 * sorted ascending -> deterministic raw batch column order. */
		std::set<int> physSet;
		for (int p : emitPhysical)
			if (p >= 0 && decimalPhys.find(p) == decimalPhys.end())
				physSet.insert(p);
		std::vector<int> colIndices(physSet.begin(), physSet.end());
		std::map<int, int> physToRaw;
		for (size_t j = 0; j < colIndices.size(); j++)
			physToRaw[colIndices[j]] = (int) j;

		emitRawPos.resize(emitPhysical.size());
		for (size_t k = 0; k < emitPhysical.size(); k++)
		{
			if (emitPhysical[k] < 0)
				emitRawPos[k] = -1;						/* absent -> NULL fill */
			else if (decimalPhys.find(emitPhysical[k]) != decimalPhys.end())
				emitRawPos[k] = -2;						/* EMIT_DECIMAL_FLBA (low-level) */
			else
				emitRawPos[k] = physToRaw[emitPhysical[k]];
		}

		/*
		 * Select row groups by the task byte range (mirrors the row path's
		 * ParquetReader::filterRowGroupByOffset midpoint rule), so a data file
		 * split across tasks is read exactly once.
		 */
		std::vector<int> rowGroups;
		selAbsStart.clear();
		selNumRows.clear();
		int64_t startOffset = task->start;
		int64_t endOffset = task->start + task->length;
		int64_t preStartIndex = 0, preCompressedSize = 0;
		int64_t curRowCount = 0;		/* absolute row position over ALL row groups */
		for (int rg = 0; rg < metadata->num_row_groups(); rg++)
		{
			auto rgmd = metadata->RowGroup(rg);
			int64_t rgRows = rgmd->num_rows();
			bool select;
			if (startOffset == -1)
			{
				select = true;
			}
			else
			{
				int64_t startIndex = rgmd->file_offset();
				if (icebergInvalidFileOffset(startIndex, preStartIndex, preCompressedSize))
					startIndex = (preStartIndex == 0) ? 4 : (preStartIndex + preCompressedSize);
				preStartIndex = startIndex;
				preCompressedSize = rgmd->total_compressed_size();
				int64_t totalSize = 0;
				for (int j = 0; j < rgmd->num_columns(); j++)
					totalSize += rgmd->ColumnChunk(j)->total_compressed_size();
				int64_t midPoint = startIndex + totalSize / 2;
				select = (midPoint >= startOffset && midPoint < endOffset);
			}
			if (select)
			{
				rowGroups.push_back(rg);
				selAbsStart.push_back(curRowCount);	/* abs file pos of this rg's first row */
				selNumRows.push_back(rgRows);
			}
			curRowCount += rgRows;
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

		/* Low-level FLBA decimal columns read the SAME selected row groups in
		 * lockstep with the Arrow batches (which never cross a row-group
		 * boundary), tracked by decimalRgCursor/decimalRowsInRg over selRowGroups. */
		selRowGroups = rowGroups;
		decimalRgCursor = 0;
		decimalRowsInRg = 0;
		decimalReaders.clear();
		if (!decimalCols.empty())
			openDecimalReadersForRg(0);

		/* MOR position deletes for this data file (NULL if none). */
		curBitmap = deleteIndex
			? icebergDeleteIndexLookup((IcebergDeleteIndex *) deleteIndex, dataFile->filePath)
			: NULL;
		curSelRg = 0;
		posInSelRg = 0;

		fileOpen = true;
		return true;
	}
	return false;
}

void
icebergReadRecordBatch::closeCurrentFile()
{
	/* Release the low-level FLBA readers before their owning ParquetFileReader. */
	decimalReaders.clear();
	rbr.reset();
	arrowReader.reset();
	if (fileOpen)
		fileReader.closeParquetReader();
	fileOpen = false;
}

void
icebergReadRecordBatch::openDecimalReadersForRg(int selRgPos)
{
	decimalRgCursor = selRgPos;
	decimalRowsInRg = 0;
	decimalReaders.clear();
	if (selRgPos >= (int) selRowGroups.size())
		return;
	std::shared_ptr<::parquet::RowGroupReader> rg =
		arrowReader->parquet_reader()->RowGroup(selRowGroups[selRgPos]);
	for (size_t j = 0; j < decimalCols.size(); j++)
		decimalReaders.push_back(rg->Column(decimalCols[j].physColIdx));
}

std::vector<std::shared_ptr<::parquet_arrow::Array>>
icebergReadRecordBatch::readDecimalBatch(int64_t nrows)
{
	/* Arrow batches never cross a row-group boundary, so once the current rg is
	 * drained the next batch belongs to the next selected rg. */
	if (decimalRowsInRg >= selNumRows[decimalRgCursor])
		openDecimalReadersForRg(decimalRgCursor + 1);

	std::vector<std::shared_ptr<::parquet_arrow::Array>> arrs(decimalCols.size());
	std::vector<::parquet::FixedLenByteArray> vals((size_t) nrows);
	std::vector<int16_t> defs((size_t) nrows);
	for (size_t j = 0; j < decimalCols.size(); j++)
	{
		auto *tr = static_cast<::parquet::FixedLenByteArrayReader *>(decimalReaders[j].get());
		int16_t maxDef = tr->descr()->max_definition_level();
		int16_t *defsPtr = (maxDef > 0) ? defs.data() : nullptr;
		/* ReadBatch stops at a data-page boundary and may return fewer rows than
		 * requested, so loop until this batch's whole nrows are read.  Dense
		 * (non-null) values accumulate contiguously in `vals`; def levels fill one
		 * slot per row in `defs`. */
		int64_t rowsGot = 0, valsGot = 0;
		while (rowsGot < nrows)
		{
			int64_t vr = 0;
			int64_t lv = tr->ReadBatch(nrows - rowsGot,
									   defsPtr ? defsPtr + rowsGot : nullptr, nullptr,
									   vals.data() + valsGot, &vr);
			if (lv <= 0)
				break;
			rowsGot += lv;
			valsGot += vr;
		}
		if (rowsGot != nrows)
			elog(ERROR, "datalake parquet vectorized read: decimal FLBA read %ld of %ld "
				 "rows (row-group misalignment)", (long) rowsGot, (long) nrows);
		arrs[j] = buildNumeric128FromFLBA(pool, vals.data(), defsPtr, maxDef, nrows,
										  decimalCols[j].typeLength,
										  decimalCols[j].precision, decimalCols[j].scale);
	}
	decimalRowsInRg += nrows;
	return arrs;
}

void
icebergReadRecordBatch::assembleOut(const std::shared_ptr<::parquet_arrow::RecordBatch> &raw)
{
	int64_t nrows = raw->num_rows();
	TupleDesc sd = scanstate->scan_tupdesc;		/* compact scan descriptor */
	out.reset();
	numericColIndices.clear();

	/* Numeric(decimal) FLBA columns: read this batch's rows low-level and decode
	 * straight to numeric128, in lockstep with the Arrow batch above. */
	std::vector<std::shared_ptr<::parquet_arrow::Array>> decArrs;
	std::vector<int> decForEmit;
	if (!decimalCols.empty())
	{
		decArrs = readDecimalBatch(nrows);
		decForEmit.assign(emitAttr.size(), -1);
		for (size_t j = 0; j < decimalCols.size(); j++)
			decForEmit[decimalCols[j].emitK] = (int) j;
	}

	for (size_t k = 0; k < emitAttr.size(); k++)
	{
		int attr = emitAttr[k];					/* index into sd (== k) */
		int rawPos = emitRawPos[k];
		Oid typeOid = sd->attrs[attr].atttypid;
		std::string name(sd->attrs[attr].attname.data);
		int batch_index = (int) k;

		if (rawPos == -2)						/* EMIT_DECIMAL_FLBA: low-level numeric128 */
		{
			parquetVecAddColOrCreate(out,
				::parquet_arrow::field(name, ::parquet_arrow::fixed_size_binary(16)),
				decArrs[decForEmit[k]], nrows, batch_index);
			numericColIndices.push_back(batch_index);
		}
		else if (rawPos >= 0)
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
			parquetVecAddNullColumn(pool, out, numericColIndices, sd,
									attr, batch_index, name, nrows);
		}
	}

	/*
	 * MOR position deletes: drop rows whose absolute file position is in the
	 * delete bitmap.  Map each batch row (read in row-group order) back to its
	 * absolute file position via the selected-row-group cursor, build a keep
	 * mask, and compact the batch with a single Arrow filter.  The mask does not
	 * disturb column positions, so numericColIndices stays valid for the re-tag.
	 */
	if (curBitmap != NULL)
	{
		::parquet_arrow::BooleanBuilder mb(pool);
		PARQUET_VEC_CHECK(mb.Reserve(nrows), "delete mask reserve");
		bool anyDeleted = false;
		for (int64_t i = 0; i < nrows; i++)
		{
			while (curSelRg < (int) selNumRows.size() && posInSelRg >= selNumRows[curSelRg])
			{
				curSelRg++;
				posInSelRg = 0;
			}
			int64_t absPos = (curSelRg < (int) selAbsStart.size())
				? selAbsStart[curSelRg] + posInSelRg : -1;
			posInSelRg++;
			bool deleted = (absPos >= 0) && datalakeBitmapContains(curBitmap, (uint64) absPos);
			anyDeleted = anyDeleted || deleted;
			mb.UnsafeAppend(!deleted);
		}
		if (anyDeleted)
		{
			std::shared_ptr<::parquet_arrow::Array> mask;
			PARQUET_VEC_CHECK(mb.Finish(&mask), "delete mask finish");
			auto res = ::parquet_arrow::compute::Filter(::parquet_arrow::Datum(out),
														::parquet_arrow::Datum(mask));
			PARQUET_VEC_CHECK(res.status(), "apply position deletes");
			out = res.ValueOrDie().record_batch();
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
	if (deleteIndex)
	{
		icebergDeleteIndexDestroy((IcebergDeleteIndex *) deleteIndex);
		deleteIndex = NULL;
		curBitmap = NULL;
	}
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
