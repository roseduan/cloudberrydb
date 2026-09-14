/*-------------------------------------------------------------------------
 *
 * parquetReadRecordBatch.cpp
 *    Vectorized reader for the generic datalake_fdw Parquet path (issue #424).
 *
 *    See parquetReadRecordBatch.h and .router/plans/424/DESIGN.md.  Reuse
 *    parquetRead's file / row-group iteration, read the data with
 *    libparquet-19's parquet::arrow::FileReader, normalize each column to the
 *    Arrow type the engine expects, inject partition-key constant columns /
 *    trailing NULL columns exactly as the ORC vectorized reader does, and
 *    export the batch across the Arrow C Data Interface.
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 *-------------------------------------------------------------------------
 */

/*
 * The parquet-arrow headers MUST come first: they must be parsed before
 * PostgreSQL's c.h defines its function-like macros, which otherwise expand
 * inside the Arrow templates and break the compile.  (parquetFileReader.cpp
 * uses the same ordering.)
 */
#include <parquet/api/reader.h>				/* low-level ColumnReader / RowGroupReader */
#include <parquet/arrow/reader.h>
#include <parquet/internal/arrow/api.h>
#include <parquet/internal/arrow/c/bridge.h>

#include <set>
#include <map>

#include "parquetReadRecordBatch.h"
#include "parquet_vec_batch_util.h"

extern "C" {
#include "miscadmin.h"
#include "utils/date.h"
#include "utils/builtins.h"
#include "utils/fmgrprotos.h"
#include "src/common/random_segment.h"
#include "src/common/partition_selector.h"
#include "src/datalake_fragment.h"
}

namespace Datalake {
namespace Internal {

/*
 * C-ABI hand-off struct.  Built from libparquet-19's own ArrowArray /
 * ArrowSchema (pulled in above), it is layout-identical to the engine's
 * `struct ArrowRecordBatch` (arrow/c/abi.h); we pass &c_batch to the engine as
 * a void*, which imports it as its ArrowRecordBatch.
 */
struct ParquetCBatch
{
	struct ArrowArray  batch;
	struct ArrowSchema schema;
};

std::shared_ptr<Provider>
makeParquetReadRecordBatch()
{
	return std::make_shared<parquetReadRecordBatch>();
}

parquetReadRecordBatch::parquetReadRecordBatch()
	: pool(nullptr), c_batch(new ParquetCBatch()),
	  decimalRgCursor(0), decimalRowsInRg(0)
{
}

parquetReadRecordBatch::~parquetReadRecordBatch() = default;

void
parquetReadRecordBatch::createHandler(void *sstate)
{
	initParameter(sstate);
	datalakeExecSegment(selected_segments, segId, segnum, &exec,
						&dummy_segid, &dummy_segnums);
	if (!exec)
		return;
	blockSerial = dummy_segid;
	initializeColumnValue();
	createPolicy();
	initFileStream();
	pool = ::parquet_arrow::default_memory_pool();
	readNextGroup();
}

void
parquetReadRecordBatch::destroyHandler()
{
	/*
	 * Release the Arrow reader (which holds a parquet::arrow FileReader built on
	 * the base reader's gopher stream) BEFORE the base reader closes that
	 * stream in closeParquetReader().
	 */
	decimalReaders.clear();
	rbr.reset();
	arrowReader.reset();
	out.reset();
	fileReader.closeParquetReader();
	datalakeDestroyFileSystem(fileStream);
	fileStream = NULL;
	releaseResources();
}

/* Reset for the next partition (mirrors parquetRead::restart()). */
void
parquetReadRecordBatch::restart()
{
	decimalReaders.clear();
	rbr.reset();
	arrowReader.reset();
	out.reset();
	rowGroupNums.clear();
	tempRowGroupNums.clear();
	curRowGroupNum = 0;
	last = false;
	fileReader.closeParquetReader();
	blockPolicy.resetReadBlockPolicy();
	initializeColumnValue();
	createPolicy();
	readNextGroup();
}

void
parquetReadRecordBatch::buildColumnIndices(std::vector<int> &colIndices)
{
	int nColumnsInFile = fileReader.getFileColumns();
	for (int i = 0; i < nColumnsInFile; i++)
	{
		if (attrs_used.find(i) != attrs_used.end())
			colIndices.push_back(i);
	}
}

/*
 * readLogical::readNextGroup() calls this after readNextFile() populated
 * tempRowGroupNums for the current block.  Build a parquet::arrow reader over
 * the SAME gopher stream the base reader already opened -- reusing its file
 * metadata and NOT moving the base's ParquetFileReader, so
 * parquetFileReader::resetParquetReader()'s Close path stays valid
 * (issue #424 DESIGN R1#3).
 */
bool
parquetReadRecordBatch::getNextGroup()
{
	if (tempRowGroupNums.empty())
		return false;

	std::vector<int> colIndices;
	buildColumnIndices(colIndices);

	/*
	 * Route numeric(decimal) columns stored as FIXED_LEN_BYTE_ARRAY to the
	 * low-level fast decode and exclude them from the Arrow reader (same scheme as
	 * iceberg_read_record_batch).  readColIndices keeps the full output order;
	 * readRawPos[k] maps output column k to its raw-batch position, or -2 for a
	 * low-level decimal column.
	 */
	const parquet::SchemaDescriptor *pschema = fileReader.getFileMetadata()->schema();
	decimalCols.clear();
	std::set<int> decimalPhys;
	for (int c : colIndices)
	{
		if (tupdesc->attrs[c].atttypid != NUMERICOID)
			continue;
		const parquet::ColumnDescriptor *cd = pschema->Column(c);
		auto lt = cd->logical_type();
		if (cd->physical_type() != parquet::Type::FIXED_LEN_BYTE_ARRAY ||
			!lt || !lt->is_decimal())
			continue;
		const auto &dt = static_cast<const parquet::DecimalLogicalType &>(*lt);
		/*
		 * Apply the same 120-bit budget the Arrow decimal128 path enforces in
		 * normalizeParquetVecColumn: precision <= 35 (VEC_NUMERIC_MAX_PRECISION)
		 * and 0 <= scale <= precision.  The precision here comes from the FILE
		 * schema, which the planner's PG-column guard never sees -- a
		 * DECIMAL(38,10) file read through a numeric(30,10) foreign table passes
		 * the planner and would reach this fast path with precision 38, where
		 * encodeNumeric128 silently drops the top bits of any value >= 2^120.
		 * Leaving such a column out of the fast path sends it through the Arrow
		 * reader, which refuses it with a clear error instead of returning a
		 * wrong number.
		 */
		if (dt.precision() > 35 || dt.scale() < 0 || dt.scale() > dt.precision())
			continue;
		decimalCols.push_back({ -1, c, dt.precision(), dt.scale(), cd->type_length() });
		decimalPhys.insert(c);
	}

	std::vector<int> arrowCols;
	std::map<int, int> physToRaw;
	for (int c : colIndices)
		if (decimalPhys.find(c) == decimalPhys.end())
		{
			physToRaw[c] = (int) arrowCols.size();
			arrowCols.push_back(c);
		}
	readRawPos.assign(colIndices.size(), -1);
	for (size_t k = 0; k < colIndices.size(); k++)
		readRawPos[k] = (decimalPhys.find(colIndices[k]) != decimalPhys.end())
			? -2 : physToRaw[colIndices[k]];
	for (auto &dc : decimalCols)
		for (size_t k = 0; k < colIndices.size(); k++)
			if (colIndices[k] == dc.physColIdx) { dc.outK = (int) k; break; }

	::parquet::ArrowReaderProperties arrowProps;
	arrowProps.set_use_threads(false);
	arrowProps.set_pre_buffer(false);			/* keep gopher stream reads synchronous */
	arrowProps.set_arrow_extensions_enabled(false);
	arrowProps.set_batch_size(PARQUET_VEC_BATCH_ROWS);	/* see parquet_vec_batch_util.h */

	::parquet::arrow::FileReaderBuilder builder;
	PARQUET_VEC_CHECK(builder.Open(fileReader.getFileStream(),
								   ::parquet::default_reader_properties(),
								   fileReader.getFileMetadata()),
					  "open arrow reader");
	builder.memory_pool(pool);
	builder.properties(arrowProps);

	/*
	 * Release the previous group's RecordBatchReader BEFORE its FileReader:
	 * rbr is derived from the old arrowReader and its destructor may touch the
	 * FileReader's internals, so resetting arrowReader first would be a
	 * use-after-free when rbr is later reassigned (matches the rbr-then-reader
	 * ordering in restart()/destroyHandler()).
	 */
	decimalReaders.clear();		/* release before their owning FileReader */
	rbr.reset();
	arrowReader.reset();
	PARQUET_VEC_CHECK(builder.Build(&arrowReader), "build arrow reader");

	std::unique_ptr<::parquet_arrow::RecordBatchReader> reader;
	PARQUET_VEC_CHECK(arrowReader->GetRecordBatchReader(tempRowGroupNums, arrowCols, &reader),
					  "get record batch reader");
	rbr = std::move(reader);
	readColIndices = colIndices;

	/* Low-level FLBA decimal columns read the SAME selected row groups in
	 * lockstep with the Arrow batches (which never cross a row-group boundary). */
	selRowGroups = tempRowGroupNums;
	selNumRows.clear();
	{
		auto md = fileReader.getFileMetadata();
		for (int rg : selRowGroups)
			selNumRows.push_back(md->RowGroup(rg)->num_rows());
	}
	decimalRgCursor = 0;
	decimalRowsInRg = 0;
	if (!decimalCols.empty())
		openDecimalReadersForRg(0);

	/*
	 * Consumed: base readNextFile() refills tempRowGroupNums for the next
	 * block/file.  Using tempRowGroupNums (this block's row groups) rather than
	 * rowGroupNums (row groups deferred to later blocks) avoids duplicate /
	 * cross-segment reads (issue #424 DESIGN R1#4).
	 */
	tempRowGroupNums.clear();
	curRowGroupNum = 0;
	return true;
}

bool
parquetReadRecordBatch::readNextRecordBatch()
{
	while ((getFileState() == FILE_OPEN) && !last)
	{
		if (getRecordBatch())
			return true;
		if (!readNextGroup())
		{
			last = true;
			return false;
		}
	}
	return false;
}

bool
parquetReadRecordBatch::getRecordBatch()
{
	if (!rbr)
		return false;

	std::shared_ptr<::parquet_arrow::RecordBatch> raw;
	PARQUET_VEC_CHECK(rbr->ReadNext(&raw), "read next batch");
	if (raw == nullptr)
		return false;			/* row groups for this reader are exhausted */

	normalizeBatch(raw);
	tupleIndex = (int) raw->num_rows();
	return true;
}

void
parquetReadRecordBatch::normalizeBatch(const std::shared_ptr<::parquet_arrow::RecordBatch> &raw)
{
	::parquet_arrow::FieldVector fields;
	::parquet_arrow::ArrayVector arrays;
	int64_t nrows = raw->num_rows();

	/* Rebuilt for this batch; read() re-tags exactly these output columns. */
	numericColIndices.clear();

	/* Numeric(decimal) FLBA columns read low-level in lockstep with this batch. */
	std::vector<std::shared_ptr<::parquet_arrow::Array>> decArrs;
	std::vector<int> decForOut;
	if (!decimalCols.empty())
	{
		decArrs = readDecimalBatch(nrows);
		decForOut.assign(readColIndices.size(), -1);
		for (size_t j = 0; j < decimalCols.size(); j++)
			decForOut[decimalCols[j].outK] = (int) j;
	}

	for (size_t k = 0; k < readColIndices.size(); k++)
	{
		int t = readColIndices[k];
		Oid typeOid = tupdesc->attrs[t].atttypid;
		std::string name = tupdesc->attrs[t].attname.data;

		if (readRawPos[k] == -2)			/* low-level numeric128 (fast FLBA decode) */
		{
			numericColIndices.push_back((int) k);
			fields.push_back(::parquet_arrow::field(name,
						::parquet_arrow::fixed_size_binary(16)));
			arrays.push_back(decArrs[decForOut[k]]);
			continue;
		}

		std::shared_ptr<::parquet_arrow::Array>    normArr;
		std::shared_ptr<::parquet_arrow::DataType> normType;
		normalizeColumn(typeOid, raw->column(readRawPos[k]), normArr, normType);

		if (typeOid == NUMERICOID)
			numericColIndices.push_back((int) k);

		fields.push_back(::parquet_arrow::field(name, normType));
		arrays.push_back(normArr);
	}

	auto schema = std::make_shared<::parquet_arrow::Schema>(
					fields, std::make_shared<::parquet_arrow::KeyValueMetadata>());
	out = ::parquet_arrow::RecordBatch::Make(schema, nrows, arrays);
}

/*
 * parquet::arrow's output Arrow type comes from the FILE schema, not the PG
 * column type, so we normalize per column (issue #424 DESIGN R1#1/2/6):
 *   - date32: file value is days since 1970-01-01; the engine reads the raw
 *     date32 as a PG DateADT (days since 2000-01-01) -> subtract 10957.
 *   - int2: the writer stores it as an un-annotated INT32 -> narrow to int16.
 *   - text/varchar: un-annotated BYTE_ARRAY reads back as binary -> re-tag utf8.
 *   - numeric: reads back as decimal128; re-pack into the engine's numeric128
 *     16-byte layout, carried as fixed_size_binary(16) here and re-tagged "D128"
 *     at export time (see read()).
 * int4/int8/float4/float8 already match (schema compat validated at open time).
 */
void
parquetReadRecordBatch::normalizeColumn(Oid typeOid,
					const std::shared_ptr<::parquet_arrow::Array> &in,
					std::shared_ptr<::parquet_arrow::Array> &out_arr,
					std::shared_ptr<::parquet_arrow::DataType> &out_type)
{
	normalizeParquetVecColumn(pool, typeOid, in, out_arr, out_type);
}

void
parquetReadRecordBatch::openDecimalReadersForRg(int selRgPos)
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
parquetReadRecordBatch::readDecimalBatch(int64_t nrows)
{
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
		/* Decode each ReadBatch chunk before the next one: the FLBA ptrs point
		 * into the reader's current data page, freed when it advances (see
		 * appendNumeric128FromFLBA). */
		::parquet_arrow::FixedSizeBinaryBuilder b(::parquet_arrow::fixed_size_binary(16), pool);
		PARQUET_VEC_CHECK(b.Reserve(nrows), "numeric flba reserve");
		int64_t rowsGot = 0;
		while (rowsGot < nrows)
		{
			int64_t vr = 0;
			int64_t lv = tr->ReadBatch(nrows - rowsGot, defsPtr, nullptr,
									   vals.data(), &vr);
			if (lv <= 0)
				break;
			appendNumeric128FromFLBA(b, vals.data(), defsPtr, maxDef, lv,
									 decimalCols[j].typeLength,
									 decimalCols[j].precision, decimalCols[j].scale);
			rowsGot += lv;
		}
		if (rowsGot != nrows)
			elog(ERROR, "datalake parquet vectorized read: decimal FLBA read %ld of %ld "
				 "rows (row-group misalignment)", (long) rowsGot, (long) nrows);
		PARQUET_VEC_CHECK(b.Finish(&arrs[j]), "numeric flba finish");
	}
	decimalRowsInRg += nrows;
	return arrs;
}

void
parquetReadRecordBatch::addColOrCreateRecordBatch(
					const std::shared_ptr<::parquet_arrow::Field> &field_id,
					const std::shared_ptr<::parquet_arrow::Array> &column,
					int64_t nrows, int batch_index)
{
	parquetVecAddColOrCreate(out, field_id, column, nrows, batch_index);
}

/*
 * Constant column for a partition key (issue #424 DESIGN R1#7 adds the DATE /
 * VARCHAR branches the ORC helper lacked).  The partition Datum is already a PG
 * value, so a DATE constant is already a PG DateADT and must NOT be
 * epoch-shifted (unlike the file's data date columns).
 */
void
parquetReadRecordBatch::recordBatchAddColumn(int mpp_index, int batch_index,
					const std::string &partitionkey, Datum mpp_datum, int64_t nrows)
{
	parquetVecAddConstColumn(pool, out, numericColIndices, tupdesc, mpp_index, batch_index, partitionkey, mpp_datum, nrows);
}

/*
 * Trailing NULL column for a used table column absent from the file.  Only the
 * trailing (schema-evolution) case is in T1 scope (issue #424 DESIGN R1#5).
 */
void
parquetReadRecordBatch::recordBatchAddNullColumn(int mpp_index, int batch_index,
					const std::string &field_name, int64_t nrows)
{
	parquetVecAddNullColumn(pool, out, numericColIndices, tupdesc, mpp_index, batch_index, field_name, nrows);
}

int64_t
parquetReadRecordBatch::read(void **recordBatch)
{
	if (!exec)
		return 0;

nextPartition:

	if (readNextRecordBatch())
	{
		int nColumnsToRead = ncolumns - nPartitionKey;
		int nColumnsInFile = fileReader.getFileColumns();

		/* Trailing NULL columns for used table columns beyond the file. */
		for (int i = nColumnsInFile; i < nColumnsToRead; i++)
		{
			if (options.includes_columns[i])
				recordBatchAddNullColumn(i, out->num_columns(),
										 std::string(tupdesc->attrs[i].attname.data),
										 tupleIndex);
		}

		/* Partition-key constant columns. */
		if (scanstate->options->hiveOption->hivePartitionKey != NULL)
		{
			for (int i = 0; i < nDefaults; i++)
			{
				if (!options.includes_columns[defMap[i]])
					continue;

				std::string name(tupdesc->attrs[defMap[i]].attname.data);
				/*
				 * Use the isNull-aware evaluator: a NULL partition value (e.g.
				 * Hive's __HIVE_DEFAULT_PARTITION__) has constvalue 0, and feeding
				 * that 0 to numeric_out/textout/varcharout in recordBatchAddColumn
				 * would dereference a null pointer.  Emit an all-NULL column
				 * instead (datalakeExecEvalConst2 drops the null flag).
				 */
				bool isNull = false;
				Datum partitionvalue = datalakeExecEvalConst(defExprs[i], NULL, &isNull, NULL);
				if (isNull)
					recordBatchAddNullColumn(defMap[i], out->num_columns(),
											 name, tupleIndex);
				else
					recordBatchAddColumn(defMap[i], out->num_columns(),
										 name, partitionvalue, tupleIndex);
			}
		}

		PARQUET_VEC_CHECK(::parquet_arrow::ExportRecordBatch(*out.get(),
										&c_batch->batch, &c_batch->schema),
						  "export record batch");

		/*
		 * Two-arrows numeric bridge: numeric columns were re-encoded into the
		 * engine's numeric128 byte layout but carried as fixed_size_binary(16),
		 * which arrow-19 exports with the C-ABI format "w:16".  The engine's fork
		 * bridge (arrow/c/bridge.cc) tags numeric128 as "D128" and imports "D"
		 * back to numeric128() -- a private format char arrow-19 cannot emit.
		 * Re-tag exactly the columns we numeric-encoded (tracked in
		 * numericColIndices as they were added) so the engine reconstructs
		 * numeric128 (matching buffer layout: validity + 16-byte values).  We key
		 * off the recorded indices rather than the "w:16" format string so a
		 * future 16-byte fixed-binary T1 type can never be mistaken for numeric.
		 * The "D128" literal outlives the engine's synchronous import, and
		 * arrow-19's release frees its own private_data, not this borrowed
		 * format pointer.
		 */
		parquetVecRetagNumeric128(&c_batch->schema, numericColIndices);

		*recordBatch = c_batch.get();
		return 1;
	}

	/* Stop instead of opening the next partition on cancel/finish (matches the
	 * row path parquetRead::read()). */
	if (QueryFinishPending || QueryCancelPending)
		return 0;

	if (!datalakeIsLastPartition(scanstate))
	{
		restart();
		goto nextPartition;
	}
	return 0;
}

}
}
