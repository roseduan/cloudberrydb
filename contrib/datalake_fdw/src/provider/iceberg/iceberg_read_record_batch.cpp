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
#include "utils/memutils.h"						/* MemoryContextRegisterResetCallback */
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
	  deleteIndex(nullptr), deleteIndexOwner(nullptr),
	  curBitmap(nullptr), curSelRg(0), posInSelRg(0),
	  pool(nullptr), c_batch(new IcebergCBatch())
{}

icebergReadRecordBatch::~icebergReadRecordBatch() = default;

/*
 * Cleanup hook for the MOR position-delete index.
 *
 * The index's palloc'd memory lives in a child of the scan's initcontext, but
 * the Roaring bitmaps it owns are C++ objects allocated outside any PostgreSQL
 * memory context, so letting the context go away is not enough -- they have to
 * be destroyed explicitly.  destroyHandler does that on the normal path, but an
 * elog(ERROR) while scanning (a decimal decode failure, a gopher read error, a
 * query cancel) unwinds without ever reaching it, and the bitmaps would then
 * leak for the life of the session.  Registering this callback on initcontext
 * makes the destruction happen however the scan ends.
 *
 * initcontext is created per scan in fdwFunction.c and is never reset while the
 * scan runs (only destroyed with its parent query context), so the callback
 * cannot fire while the index is still in use.  destroyHandler clears
 * owner->index first, so the normal path releases the bitmaps promptly and this
 * hook becomes a no-op rather than a double free.
 */
typedef struct IcebergDeleteIndexOwner
{
	IcebergDeleteIndex *index;		/* NULL once destroyHandler has released it */
} IcebergDeleteIndexOwner;

static void
icebergDeleteIndexResetCallback(void *arg)
{
	IcebergDeleteIndexOwner *owner = (IcebergDeleteIndexOwner *) arg;

	if (owner != NULL && owner->index != NULL)
	{
		icebergDeleteIndexDestroy(owner->index);
		owner->index = NULL;
	}
}

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
	deleteIndex = icebergBuildDeleteIndex(scanstate->initcontext,
										  protocolContext->file->fileStream,
										  fileScanTasks);
	if (deleteIndex != nullptr)
	{
		IcebergDeleteIndexOwner *owner;
		MemoryContextCallback   *cb;

		owner = (IcebergDeleteIndexOwner *)
			MemoryContextAllocZero(scanstate->initcontext,
								   sizeof(IcebergDeleteIndexOwner));
		owner->index = (IcebergDeleteIndex *) deleteIndex;
		deleteIndexOwner = owner;

		cb = (MemoryContextCallback *)
			MemoryContextAllocZero(scanstate->initcontext,
								   sizeof(MemoryContextCallback));
		cb->func = icebergDeleteIndexResetCallback;
		cb->arg = owner;
		MemoryContextRegisterResetCallback(scanstate->initcontext, cb);
	}
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
			/*
			 * Same 120-bit budget the Arrow decimal128 path enforces in
			 * normalizeParquetVecColumn: precision <= 35
			 * (VEC_NUMERIC_MAX_PRECISION) and 0 <= scale <= precision.  The
			 * precision here is the FILE schema's, which the planner's
			 * PG-column guard never sees, so a DECIMAL(38,10) file read
			 * through a numeric(30,10) table would otherwise reach this fast
			 * path and have encodeNumeric128 silently drop the top bits of
			 * any value >= 2^120.  Skip it here and let the Arrow reader
			 * refuse it with a clear error.
			 */
			if (dt.precision() > 35 || dt.scale() < 0 || dt.scale() > dt.precision())
				continue;
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
		arrowProps.set_batch_size(PARQUET_VEC_BATCH_ROWS);	/* see parquet_vec_batch_util.h */

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
	/*
	 * An Arrow batch is not bounded by a row group: the reader fills it to its
	 * batch size across however many row groups that takes, so a batch of
	 * 10000 rows over row groups of 1000 arrives as one batch spanning ten of
	 * them.  The low-level FLBA readers, in contrast, are per row group.  Read
	 * the batch in one segment per row group and let each column's builder
	 * accumulate across the segments; the rows arrive in row group order on
	 * both sides, so the two stay in lockstep.
	 */
	std::vector<std::shared_ptr<::parquet_arrow::Array>> arrs(decimalCols.size());
	std::vector<::parquet::FixedLenByteArray> vals((size_t) nrows);
	std::vector<int16_t> defs((size_t) nrows);
	std::vector<std::unique_ptr<::parquet_arrow::FixedSizeBinaryBuilder>> builders;

	/*
	 * PARQUET_VEC_CHECK raises with elog(ERROR), which longjmps without
	 * unwinding C++ frames and would leave the builders constructed so far
	 * holding their Arrow buffers.  Release them first.
	 */
	auto checkOrRelease = [&builders](const ::parquet_arrow::Status &st, const char *what)
	{
		if (st.ok())
			return;

		std::string msg = st.ToString();

		builders.clear();
		elog(ERROR, "datalake parquet vectorized read: %s failed: %s", what, msg.c_str());
	};

	for (size_t j = 0; j < decimalCols.size(); j++)
	{
		builders.push_back(std::make_unique<::parquet_arrow::FixedSizeBinaryBuilder>(
							   ::parquet_arrow::fixed_size_binary(16), pool));
		checkOrRelease(builders[j]->Reserve(nrows), "numeric flba reserve");
	}

	for (int64_t remaining = nrows; remaining > 0;)
	{
		/* Move to the next selected row group once this one is drained. */
		while (decimalRgCursor < (int) selRowGroups.size() &&
			   decimalRowsInRg >= selNumRows[decimalRgCursor])
			openDecimalReadersForRg(decimalRgCursor + 1);
		if (decimalRgCursor >= (int) selRowGroups.size())
		{
			/* elog(ERROR) longjmps without unwinding C++ frames. */
			builders.clear();
			elog(ERROR, "datalake parquet vectorized read: decimal FLBA is short by "
				 "%ld of %ld rows: no selected row group left",
				 (long) remaining, (long) nrows);
		}

		int64_t		rgLeft = selNumRows[decimalRgCursor] - decimalRowsInRg;
		int64_t		take = (remaining < rgLeft) ? remaining : rgLeft;

		for (size_t j = 0; j < decimalCols.size(); j++)
		{
			auto *tr = static_cast<::parquet::FixedLenByteArrayReader *>(decimalReaders[j].get());
			int16_t maxDef = tr->descr()->max_definition_level();
			int16_t *defsPtr = (maxDef > 0) ? defs.data() : nullptr;
			/*
			 * ReadBatch stops at a data-page boundary and may return fewer rows
			 * than requested, so loop until this segment is read.  Each chunk
			 * MUST be decoded before the next ReadBatch: a FixedLenByteArray's
			 * ptr points into the reader's current data page buffer, which is
			 * freed once the reader advances, so collecting the FLBAs and
			 * decoding them at the end reads freed memory.
			 */
			int64_t		rowsGot = 0;

			while (rowsGot < take)
			{
				int64_t		vr = 0;
				int64_t		lv = tr->ReadBatch(take - rowsGot, defsPtr, nullptr,
											   vals.data(), &vr);

				if (lv <= 0)
					break;
				appendNumeric128FromFLBA(*builders[j], vals.data(), defsPtr, maxDef, lv,
										 decimalCols[j].typeLength,
										 decimalCols[j].precision, decimalCols[j].scale);
				rowsGot += lv;
			}
			if (rowsGot != take)
			{
				int			rg = selRowGroups[decimalRgCursor];

			/*
			 * elog(ERROR) longjmps without unwinding C++ frames, so release
			 * the builders' Arrow buffers before raising.
			 */
				builders.clear();
				elog(ERROR, "datalake parquet vectorized read: decimal FLBA read %ld of "
					 "%ld rows of row group %d", (long) rowsGot, (long) take, rg);
			}
		}
		decimalRowsInRg += take;
		remaining -= take;
	}

	for (size_t j = 0; j < decimalCols.size(); j++)
		checkOrRelease(builders[j]->Finish(&arrs[j]), "numeric flba finish");
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
		/* disarm the initcontext hook so it cannot double free */
		if (deleteIndexOwner)
			((IcebergDeleteIndexOwner *) deleteIndexOwner)->index = NULL;
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
