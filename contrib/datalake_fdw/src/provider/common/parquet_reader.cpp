#include <parquet/internal/arrow/util/decimal.h>
#include <parquet/internal/arrow/result.h>
#include "parquet_reader.h"
#include "rowgroup_filter.h"
#include "common.h"
#include "datalake_random_file.h"
#include "datalake_numeric.h"

extern "C"
{
#include "postgres.h"
#include "access/tupdesc.h"
#include "datatype/timestamp.h"
#include "utils/memutils.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "access/stratnum.h"
#include "catalog/pg_type.h"
#include "utils/date.h"
#include "utils/array.h"
#include "utils/fmgrprotos.h"
#include "utils.h"
#include "src/datalake_def.h"

/* GUC kill-switch for batch column read/convert (defined in am_iceberg). */
extern bool pg_iceberg_enable_batch_read;
}

bool
ParquetReader::rowGroupMightMatch(int rgIdx)
{
	if (quals_ == NIL)
		return true;

	/* Map this reader's name-based column info to the shared evaluator's
	 * attno-indexed metadata. */
	std::vector<RowGroupColMeta> cols;
	cols.reserve(typeMap_.size());
	for (const auto &ti : typeMap_)
	{
		RowGroupColMeta m;
		m.pgType = ti.pgTypeId_;
		m.colIdx = ti.columnIndex_;
		m.scale = ti.scale_;
		m.precision = ti.precision_;
		m.typeLength = ti.typeLength_;
		m.timeUnit = (int) ti.timeUnit_;
		cols.push_back(m);
	}

	auto rg = metadata->RowGroup(rgIdx);
	return !rowGroupExcludedByQuals(quals_, rg.get(), cols);
}

ParquetReader::ParquetReader(MemoryContext rowContext, char *filePath, ossFileStream fileStream, dataBufferArray *buffer, List *quals)
	: BaseFileReader(rowContext), numColumns_(0), filePath_(filePath), fileStream_(fileStream), buffer_(buffer), quals_(quals)
{}

ParquetReader::~ParquetReader()
{}

TIMEUNIT
ParquetReader::getTimeUnit(const parquet::ColumnDescriptor *field)
{
	const auto &logicalType = field->logical_type();
	if (!logicalType->is_timestamp())
		return TIMEUNIT_UNKNOWN;

	const auto *timestampType = dynamic_cast<const parquet::TimestampLogicalType*>(logicalType.get());
	switch (timestampType->time_unit())
	{
		case parquet::LogicalType::TimeUnit::MILLIS:
			return TIMEUNIT_MILLIS;
		case parquet::LogicalType::TimeUnit::MICROS:
			return TIMEUNIT_MICROS;
		case parquet::LogicalType::TimeUnit::NANOS:
			return TIMEUNIT_NANOS;
		default:
			throw Error("parquet error: Unknown timestamp precision");
	}
}

void
ParquetReader::createMapping(List *columnDesc, bool *attrUsed)
{
	int       i;
	int       j;
	ListCell *lc;
	auto      schema = metadata->schema();

	numColumns_ = schema->num_columns();
	foreach_with_count(lc, columnDesc, i)
	{
		DatalakeFieldDescription *entry = (DatalakeFieldDescription *) lfirst(lc);
		TypeInfo typInfo = {entry->typeOid, entry->typeMod, InvalidOid, -1, TIMEUNIT_UNKNOWN, 0, 0, 0, nullptr};
		typeMap_.push_back(typInfo);

		if (!attrUsed[i])
			continue;

		for (j = 0; j < numColumns_; j++)
		{
			const parquet::ColumnDescriptor *field = schema->Column(j);
			auto fieldName = field->name();

			if (pg_strcasecmp(entry->name, fieldName.c_str()) == 0)
			{
				typeMap_[i].columnIndex_ = j;
				typeMap_[i].fileTypeId_ = mapParquetDataType(field->physical_type());
				typeMap_[i].timeUnit_ = getTimeUnit(field);
				typeMap_[i].scale_ = field->type_scale();
				typeMap_[i].precision_ = field->type_precision();
				typeMap_[i].typeLength_ = field->type_length();
				typeMap_[i].readFn_ = resolveReadFn(typeMap_[i]);
				break;
			}
		}
	}

	numBatch_.resize(numColumns_);
}

/*
 * Kill-switch for the batch decimal path.  Read the GUC directly at the
 * decision point rather than caching it at open(): on a QE the dispatched
 * SET may not be applied to the C variable until after open() runs, so a
 * cached value could be stale.  The GUC cannot change mid-query, so reading
 * it per row is safe (no risk of desynchronizing an in-flight batch buffer).
 */
bool
ParquetReader::supportsBatchPrimitive()
{
	return pg_iceberg_enable_batch_read;
}

bool
ParquetReader::readNextRowGroup()
{
	curGroup_++;

	if ((uint) curGroup_ >= rowGroups_.size())
		return false;

	auto rowGroupReader = reader_->RowGroup(rowGroups_[curGroup_]);
	size_t typeSize = typeMap_.size();

	curRow_ = 0;
	numRows_ = rowGroupReader->metadata()->num_rows();

	/*
	 * Use the full row group size as Scanner batch_size instead of the default 128.
	 * This makes Scanner::ReadBatch() decode the entire column in one call,
	 * so subsequent NextValue() calls are pure array accesses (similar to PAX's
	 * ReadStripe + GetDatum pattern). This reduces ReadBatch call count from
	 * ~(numRows/128) to 1 per column per row group.
	 */
	int64_t scannerBatchSize = numRows_ > 0 ? numRows_ : parquet::DEFAULT_SCANNER_BATCH_SIZE;

	/*
	 * Decide the batch-read mode once, at the first row group rather than at
	 * open(): on a QE a dispatched SET may not be applied to the C variable
	 * until after open() runs, but it is always applied by the time rows are
	 * pulled.  The decision is then fixed for the whole scan so a per-column
	 * serve function never sees a half-initialized state.
	 */
	if (!batchModeDecided_)
	{
		batchMode_ = pg_iceberg_enable_batch_read;
		batchModeDecided_ = true;
		if (batchMode_)
			setupBatchColumns();
	}

	scanners_.clear();
	scanners_.resize(numColumns_);
	for (size_t i = 0; i < typeSize; ++i)
	{
		TypeInfo &typInfo = typeMap_[i];

		if (typInfo.columnIndex_ < 0)
			continue;

		if (batchMode_ && colBatch_[typInfo.columnIndex_].kind != FILL_NONE)
		{
			ColumnBatch &b = colBatch_[typInfo.columnIndex_];

			/*
			 * For conversion-heavy kinds, ask for the dictionary to be
			 * exposed.  The reader only exposes it when the whole column
			 * chunk is dictionary-encoded (fallback chunks report
			 * NO_ENCODING and take the plain ReadBatch path below).
			 */
			if (fillKindWantsDict(b.kind))
			{
				b.reader = rowGroupReader->ColumnWithExposeEncoding(
					typInfo.columnIndex_, parquet::ExposedEncoding::DICTIONARY);
				b.dictExposed = (b.reader->GetExposedEncoding() ==
								 parquet::ExposedEncoding::DICTIONARY);
			}
			else
			{
				b.reader = rowGroupReader->Column(typInfo.columnIndex_);
				b.dictExposed = false;
			}
			b.dictConvertedFrom = nullptr;
			b.pos = 0;
			b.filled = 0;
		}
		else
		{
			scanners_[typInfo.columnIndex_] = parquet::Scanner::Make(
				rowGroupReader->Column(typInfo.columnIndex_), scannerBatchSize);
		}
	}

	/* Scanners were recreated for the new row group; invalidate any buffered
	 * batch so the next read refills from the new scanner. */
	for (auto &b : numBatch_)
	{
		b.pos = 0;
		b.filled = 0;
	}

	return true;
}

void
ParquetReader::open(List *columnDesc, bool *attrUsed, int64 startOffset, int64 endOffset)
{
	std::string filename = convertToGopherPath(filePath_);
	reader_ = parquet::ParquetFileReader::Open(std::make_shared<DatalakeRandomAccessFile> (fileStream_, filename));
	metadata = reader_->metadata();
	createMapping(columnDesc, attrUsed);
	filterRowGroupByOffset(startOffset, endOffset);

	/* Report row-group min/max pruning at DEBUG1 for diagnostics. */
	if (rowGroupsSkipped_ > 0)
		elog(DEBUG1,
			 "datalake parquet row-group skip: file=%s skipped=%d of %d row groups by min/max",
			 filePath_.c_str(), rowGroupsSkipped_, metadata->num_row_groups());
}

void
ParquetReader::close()
{
	scanners_.clear();
	reader_->Close();
}

bool
ParquetReader::invalidFileOffset(int64_t startIndex, int64_t preStartIndex, int64_t preCompressedSize)
{
	bool invalid = false;

	// checking the first rowGroup
	if (preStartIndex == 0 && startIndex != 4)
	{
		invalid = true;
		return invalid;
	}

	//calculate start index for other blocks
	int64_t minStartIndex = preStartIndex + preCompressedSize;
	if (startIndex < minStartIndex)
		invalid = true;

	return invalid;
}

void
ParquetReader::filterRowGroupByOffset(int64_t startOffset, int64_t endOffset)
{
	int64_t preStartIndex = 0;
	int64_t preCompressedSize = 0;
	int64_t curRowCount = 0;

	for (int i = 0; i < metadata->num_row_groups(); i++)
	{
		int64_t totalSize = 0;
		int64_t startIndex;

		if (startOffset == -1)
		{
			if (rowGroupMightMatch(i))
			{
				rowGroups_.push_back(i);
				rowPositions_.push_back(0);
			}
			else
				rowGroupsSkipped_++;
			continue;
		}

		auto rowGroup = metadata->RowGroup(i);
		startIndex = rowGroup->file_offset();

		if (invalidFileOffset(startIndex, preStartIndex, preCompressedSize))
		{
			if (preStartIndex == 0)
				startIndex = 4;
			else
				startIndex = preStartIndex + preCompressedSize;
		}

		preStartIndex = startIndex;
		preCompressedSize = rowGroup->total_compressed_size();

		for (int j = 0; j < rowGroup->num_columns(); j++)
		{
			auto col = rowGroup->ColumnChunk(j);
			totalSize += col->total_compressed_size();
		}

		int64_t midPoint = startIndex + totalSize / 2;
		if (midPoint >= startOffset && midPoint < endOffset)
		{
			if (rowGroupMightMatch(i))
			{
				rowGroups_.push_back(i);
				rowPositions_.push_back(curRowCount);
			}
			else
				rowGroupsSkipped_++;
		}

		curRowCount += rowGroup->num_rows();
	}
}

/*
 * Batch read entry point for NUMERIC columns.  Serves one value per call from
 * a per-column batch buffer, refilling in bulk (Parquet physical-type switch
 * hoisted out of the per-value loop) when the buffer drains.  Non-NUMERIC
 * types keep the per-value readPrimitive() path.  The whole batch path is
 * bypassed when the kill-switch GUC is off (supportsBatchPrimitive() == false).
 * The conversion per value is identical to readDecimal(), so results are
 * byte-for-byte the same as the per-value path.
 */
Datum
ParquetReader::readBatchPrimitive(const TypeInfo &typInfo, bool &isNull)
{
	if (typInfo.pgTypeId_ != NUMERICOID)
		return readPrimitive(typInfo, isNull);

	NumericColumnBatch &b = numBatch_[typInfo.columnIndex_];
	if (b.pos >= b.filled)
		fillNumericBatch(typInfo, b);
	if (b.pos >= b.filled)
	{
		/* Column exhausted for this row group.  next() advances row groups
		 * before calling populateRecord(), so this is defensive only. */
		isNull = true;
		PG_RETURN_DATUM(0);
	}

	isNull = b.isnull[b.pos];
	if (isNull)
	{
		b.pos++;
		PG_RETURN_DATUM(0);
	}

	Datum d = NumericGetDatum((Numeric) (b.buf.data() + (size_t) b.pos * b.stride_));
	b.pos++;
	return d;
}

void
ParquetReader::fillNumericBatch(const TypeInfo &typInfo, NumericColumnBatch &b)
{
	const int    N = NUMERIC_BATCH_SIZE;
	const int    scale = typInfo.scale_;
	const size_t numSz = NUMERIC_HDRSZ + IntDigitsTraits<__int128>::digits * sizeof(NumericDigit);
	parquet::Scanner *scanner = scanners_[typInfo.columnIndex_].get();

	b.pos = 0;
	b.filled = 0;
	b.stride_ = numSz;
	/*
	 * Allocate all NUMERIC_BATCH_SIZE fixed-size slots once and reuse across
	 * batches.  Each value is written at slot i (base + i*numSz), so the fill
	 * loop needs no per-value resize() nor an offset table.
	 */
	if (b.buf.size() < (size_t) N * numSz)
		b.buf.resize((size_t) N * numSz);
	if ((int) b.isnull.size() < N)
		b.isnull.resize(N);
	char *base = b.buf.data();

	switch (typInfo.fileTypeId_)
	{
		case INT4OID:
		{
			auto *sc = (parquet::TypedScanner<parquet::Int32Type> *) scanner;
			for (int i = 0; i < N; i++)
			{
				int32_t v;
				bool    vnull = false;
				if (!sc->NextValue(&v, &vnull))
					break;
				b.isnull[i] = vnull;
				if (!vnull)
					int_to_numeric_with_scale(v, scale, (Numeric) (base + (size_t) i * numSz));
				b.filled++;
			}
			break;
		}
		case INT8OID:
		{
			auto *sc = (parquet::TypedScanner<parquet::Int64Type> *) scanner;
			for (int i = 0; i < N; i++)
			{
				int64_t v;
				bool    vnull = false;
				if (!sc->NextValue(&v, &vnull))
					break;
				b.isnull[i] = vnull;
				if (!vnull)
					int_to_numeric_with_scale(v, scale, (Numeric) (base + (size_t) i * numSz));
				b.filled++;
			}
			break;
		}
		default:	/* DECIMAL stored as fixed-length byte array */
		{
			auto      *sc = (parquet::TypedScanner<parquet::FLBAType> *) scanner;
			const bool fits64 = (typInfo.precision_ > 0 && typInfo.precision_ <= 18);
			for (int i = 0; i < N; i++)
			{
				parquet::FixedLenByteArray v;
				bool                       vnull = false;
				if (!sc->NextValue(&v, &vnull))
					break;
				b.isnull[i] = vnull;
				if (!vnull)
				{
					char *out = base + (size_t) i * numSz;
					if (fits64)
						int_to_numeric_with_scale(FLBA_to_int64(v.ptr, typInfo.typeLength_), scale, (Numeric) out);
					else
						int_to_numeric_with_scale(FLBA_to_int128(v.ptr, typInfo.typeLength_), scale, (Numeric) out);
				}
				b.filled++;
			}
			break;
		}
	}
}

Datum
ParquetReader::readPrimitive(const TypeInfo &typInfo, bool &isNull)
{
	ReaderValue d;
	auto &scanner = scanners_[typInfo.columnIndex_];

	switch (typInfo.pgTypeId_)
	{
		case BOOLOID:
		{
			((parquet::TypedScanner<parquet::BooleanType> *)scanner.get())->NextValue(&d.boolValue, &isNull);
			return BoolGetDatum(d.boolValue);
		}
		case INT2OID:
		{
			/* Parquet stores SMALLINT as INT32; read and truncate */
			((parquet::TypedScanner<parquet::Int32Type> *)scanner.get())->NextValue(&d.int32Value, &isNull);
			return Int16GetDatum((int16) d.int32Value);
		}
		case INT4OID:
		{
			((parquet::TypedScanner<parquet::Int32Type> *)scanner.get())->NextValue(&d.int32Value, &isNull);
			return Int32GetDatum(d.int32Value);
		}
		case TIMEOID:
		case INT8OID:
		{
			((parquet::TypedScanner<parquet::Int64Type> *)scanner.get())->NextValue(&d.int64Value, &isNull);
			return Int64GetDatum(d.int64Value);
		}
		case FLOAT4OID:
		{
			((parquet::TypedScanner<parquet::FloatType> *)scanner.get())->NextValue(&d.floatValue, &isNull);
			return Float4GetDatum(d.floatValue);
		}
		case FLOAT8OID:
		{
			((parquet::TypedScanner<parquet::DoubleType> *)scanner.get())->NextValue(&d.doubleValue, &isNull);
			return Float8GetDatum(d.doubleValue);
		}
		case BYTEAOID:
		case TEXTOID:
		case VARCHAROID:
		{
			parquet::ByteArray value;
			((parquet::TypedScanner<parquet::ByteArrayType> *)scanner.get())->NextValue(&value, &isNull);
			if (isNull)
				PG_RETURN_DATUM(0);

			if (!buffer_)
			{
				bytea *result = (bytea *) gpdbPalloc(value.len + VARHDRSZ);
				SET_VARSIZE(result, value.len + VARHDRSZ);
				memcpy(VARDATA(result), value.ptr, value.len);
				return PointerGetDatum(result);
			}
			if (value.len + VARHDRSZ > static_cast<uint32>(buffer_->getDataBuffer(typInfo.columnIndex_)->length))
			{
				buffer_->resizeDataBuffer(typInfo.columnIndex_, value.len + VARHDRSZ);
			}
			dataBuff *colBuffer = buffer_->getDataBuffer(typInfo.columnIndex_);
			SET_VARSIZE(colBuffer->buffer, value.len + VARHDRSZ);
			memcpy(VARDATA(colBuffer->buffer), value.ptr, value.len);
			return PointerGetDatum(colBuffer->buffer);
		}
		case BPCHAROID:
		{
			/*
			 * BPCHAR on Iceberg is written as variable-length BYTE_ARRAY +
			 * UTF8 (Iceberg `string`; see commit 48bf8311b1a / issue #321),
			 * with trailing spaces stripped at write time so external engines
			 * (Spark/Trino) see clean strings.  To keep PG CHAR(N) semantics
			 * identical to a heap table, re-pad the value with blanks up to the
			 * declared length N on read, mirroring PG's bpchar() coercion
			 * (src/backend/utils/adt/varchar.c).  N comes from atttypmod
			 * (typeMod_ == N + VARHDRSZ); typeMod_ < VARHDRSZ means an
			 * unbounded `char` with no length, which is never padded.
			 */
			parquet::ByteArray value;
			((parquet::TypedScanner<parquet::ByteArrayType> *)scanner.get())->NextValue(&value, &isNull);
			if (isNull)
				PG_RETURN_DATUM(0);

			int pad = 0;
			if (typInfo.typeMod_ >= (int) VARHDRSZ)
			{
				int maxlen = typInfo.typeMod_ - VARHDRSZ;	/* declared char count N */
				/*
				 * Count UTF-8 code points directly (every byte that is not a
				 * 10xxxxxx continuation byte starts a character) rather than
				 * pg_mbstrlen_with_len(): the on-disk Iceberg `string` is always
				 * UTF-8, so the character count must not depend on the database
				 * encoding -- under a non-UTF-8 server encoding pg_mbstrlen_with_len
				 * would mis-count and pad to the wrong length.
				 */
				int charlen = 0;
				for (uint32 k = 0; k < value.len; k++)
					if (((unsigned char) value.ptr[k] & 0xC0) != 0x80)
						charlen++;
				if (charlen < maxlen)
					pad = maxlen - charlen;					/* blanks are single-byte */
			}
			uint32 totalLen = value.len + pad;

			if (!buffer_)
			{
				bytea *result = (bytea *) gpdbPalloc(totalLen + VARHDRSZ);
				SET_VARSIZE(result, totalLen + VARHDRSZ);
				memcpy(VARDATA(result), value.ptr, value.len);
				if (pad > 0)
					memset(VARDATA(result) + value.len, ' ', pad);
				return PointerGetDatum(result);
			}
			if (totalLen + VARHDRSZ > static_cast<uint32>(buffer_->getDataBuffer(typInfo.columnIndex_)->length))
			{
				buffer_->resizeDataBuffer(typInfo.columnIndex_, totalLen + VARHDRSZ);
			}
			dataBuff *colBuffer = buffer_->getDataBuffer(typInfo.columnIndex_);
			SET_VARSIZE(colBuffer->buffer, totalLen + VARHDRSZ);
			memcpy(VARDATA(colBuffer->buffer), value.ptr, value.len);
			if (pad > 0)
				memset(VARDATA(colBuffer->buffer) + value.len, ' ', pad);
			return PointerGetDatum(colBuffer->buffer);
		}
		case UUIDOID:
		{
			parquet::ByteArray value;
			((parquet::TypedScanner<parquet::ByteArrayType> *)scanner.get())->NextValue(&value, &isNull);
			if (isNull)
				PG_RETURN_DATUM(0);

			if (!buffer_)
			{
				bytea *result = (bytea *) gpdbPalloc(value.len);
				memcpy(VARDATA(result), value.ptr, 16);
				return PointerGetDatum(result);
			}	
			dataBuff *colBuffer = buffer_->getDataBuffer(typInfo.columnIndex_);
			memcpy(colBuffer->buffer, value.ptr, 16);
			return PointerGetDatum(colBuffer->buffer);
		}
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
		{
			/*
			 * Direct conversion from Parquet timestamp to PG timestamp.
			 *
			 * PG stores timestamps as microseconds since PG epoch (2000-01-01).
			 * Parquet stores timestamps since Unix epoch (1970-01-01) in
			 * millis/micros/nanos. The old code divided to seconds then
			 * multiplied back to microseconds, losing sub-second precision.
			 */
			static const int64 UNIX_TO_PG_EPOCH_USECS =
				((int64)(POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE)) * SECS_PER_DAY * USECS_PER_SEC;

			((parquet::TypedScanner<parquet::Int64Type> *)scanner.get())->NextValue(&d.int64Value, &isNull);
			if (isNull)
				PG_RETURN_DATUM(0);

			int64 pgTimestamp;
			switch (typInfo.timeUnit_)
			{
				case TIMEUNIT_MILLIS:
					pgTimestamp = d.int64Value * 1000 - UNIX_TO_PG_EPOCH_USECS;
					break;
				case TIMEUNIT_MICROS:
					pgTimestamp = d.int64Value - UNIX_TO_PG_EPOCH_USECS;
					break;
				case TIMEUNIT_NANOS:
					pgTimestamp = d.int64Value / 1000 - UNIX_TO_PG_EPOCH_USECS;
					break;
				default:
					/*
					 * Fallback for files without explicit timestamp LogicalType
					 * (e.g., legacy INT96 or INT64 without annotation).
					 * Default to microseconds — the Iceberg standard.
					 */
					pgTimestamp = d.int64Value - UNIX_TO_PG_EPOCH_USECS;
					break;
			}
			return TimestampGetDatum(pgTimestamp);
		}
		case DATEOID:
		{
			((parquet::TypedScanner<parquet::Int32Type> *)scanner.get())->NextValue(&d.int32Value, &isNull);
			return DateADTGetDatum(d.int32Value + (UNIX_EPOCH_JDATE - POSTGRES_EPOCH_JDATE));
		}
		case NUMERICOID:
			return readDecimal(scanner, typInfo, isNull);

		default:
			throw Error("unsupported column type oid: \"%u\"", typInfo.pgTypeId_);
	}

	PG_RETURN_DATUM(0);
}

Datum
ParquetReader::readDecimal(std::shared_ptr<parquet::Scanner> &scanner, const TypeInfo &typInfo, bool &isNull)
{
	ReaderValue d;
	parquet::FixedLenByteArray value;
	int scale = typInfo.scale_;
	char *out_buf = nullptr;

	if (!buffer_)
	{
		out_buf = (char *) gpdbPalloc(NUMERIC_HDRSZ + IntDigitsTraits<__int128>::digits * sizeof(NumericDigit));
	}
	else
	{
		dataBuff *res = buffer_->getDataBuffer(typInfo.columnIndex_);
		out_buf = (char *) res->buffer;
	}

	// iceberg only support int4, int8, fixed-len
	switch (typInfo.fileTypeId_)
	{
		case INT4OID:
		{
			((parquet::TypedScanner<parquet::Int32Type> *)scanner.get())->NextValue(&d.int32Value, &isNull);
			if (isNull)
				PG_RETURN_DATUM(0);
			int_to_numeric_with_scale(d.int32Value, scale, (Numeric) out_buf);
			return NumericGetDatum(out_buf);
		}
		case INT8OID:
		{
			((parquet::TypedScanner<parquet::Int64Type> *)scanner.get())->NextValue(&d.int64Value, &isNull);
			if (isNull)
				PG_RETURN_DATUM(0);
			int_to_numeric_with_scale(d.int64Value, scale, (Numeric) out_buf);
			return NumericGetDatum(out_buf);
		}
		default:
		{
			((parquet::TypedScanner<parquet::FLBAType> *)scanner.get())->NextValue(&value, &isNull);
			if (isNull)
				PG_RETURN_DATUM(0);
			/*
			 * For DECIMAL with precision <= 18, the value fits in int64.
			 * Use native 64-bit division instead of __int128 software division,
			 * which is ~5x faster on aarch64.
			 */
			if (typInfo.precision_ > 0 && typInfo.precision_ <= 18)
				int_to_numeric_with_scale(FLBA_to_int64(value.ptr, typInfo.typeLength_), scale, (Numeric) out_buf);
			else
				int_to_numeric_with_scale(FLBA_to_int128(value.ptr, typInfo.typeLength_), scale, (Numeric) out_buf);
			return NumericGetDatum(out_buf);
		}
	}

	PG_RETURN_DATUM(0);
}

void ParquetReader::decodeRecord() {}

/*
 * Per-type direct read functions.  Called via function pointer from
 * populateRecord(), bypassing virtual dispatch and the type switch.
 * Each covers the hot path for one physical Parquet type; complex types
 * (TEXT, BPCHAR, UUID, TIMESTAMP, NUMERIC) still go through readPrimitive
 * because they need buffer management or multi-step conversion.
 */

Datum
ParquetReader::readBoolColumn(BaseFileReader *r, int idx, bool &isNull)
{
	auto *self = static_cast<ParquetReader *>(r);
	bool value;
	((parquet::TypedScanner<parquet::BooleanType> *)self->scanners_[idx].get())->NextValue(&value, &isNull);
	return BoolGetDatum(value);
}

Datum
ParquetReader::readInt16Column(BaseFileReader *r, int idx, bool &isNull)
{
	auto *self = static_cast<ParquetReader *>(r);
	int32_t value;
	((parquet::TypedScanner<parquet::Int32Type> *)self->scanners_[idx].get())->NextValue(&value, &isNull);
	return Int16GetDatum((int16) value);
}

Datum
ParquetReader::readInt32Column(BaseFileReader *r, int idx, bool &isNull)
{
	auto *self = static_cast<ParquetReader *>(r);
	int32_t value;
	((parquet::TypedScanner<parquet::Int32Type> *)self->scanners_[idx].get())->NextValue(&value, &isNull);
	return Int32GetDatum(value);
}

Datum
ParquetReader::readInt64Column(BaseFileReader *r, int idx, bool &isNull)
{
	auto *self = static_cast<ParquetReader *>(r);
	int64_t value;
	((parquet::TypedScanner<parquet::Int64Type> *)self->scanners_[idx].get())->NextValue(&value, &isNull);
	return Int64GetDatum(value);
}

Datum
ParquetReader::readFloat4Column(BaseFileReader *r, int idx, bool &isNull)
{
	auto *self = static_cast<ParquetReader *>(r);
	float value;
	((parquet::TypedScanner<parquet::FloatType> *)self->scanners_[idx].get())->NextValue(&value, &isNull);
	return Float4GetDatum(value);
}

Datum
ParquetReader::readFloat8Column(BaseFileReader *r, int idx, bool &isNull)
{
	auto *self = static_cast<ParquetReader *>(r);
	double value;
	((parquet::TypedScanner<parquet::DoubleType> *)self->scanners_[idx].get())->NextValue(&value, &isNull);
	return Float8GetDatum(value);
}

Datum
ParquetReader::readDateColumn(BaseFileReader *r, int idx, bool &isNull)
{
	auto *self = static_cast<ParquetReader *>(r);
	int32_t value;
	((parquet::TypedScanner<parquet::Int32Type> *)self->scanners_[idx].get())->NextValue(&value, &isNull);
	return DateADTGetDatum(value + (UNIX_EPOCH_JDATE - POSTGRES_EPOCH_JDATE));
}

/*
 * Resolve which direct read function to use for a column.  Returns NULL
 * for complex types that need buffer management or multi-step conversion;
 * those fall back to readPrimitive() via virtual dispatch.
 */
ReadColumnFn
ParquetReader::resolveReadFn(const TypeInfo &typInfo)
{
	switch (typInfo.pgTypeId_)
	{
		case BOOLOID:
			return readBoolColumn;
		case INT2OID:
			return readInt16Column;
		case INT4OID:
			return readInt32Column;
		case TIMEOID:
		case INT8OID:
			return readInt64Column;
		case FLOAT4OID:
			return readFloat4Column;
		case FLOAT8OID:
			return readFloat8Column;
		case DATEOID:
			return readDateColumn;
		default:
			/*
			 * BPCHAR, TEXT, BYTEA, UUID, TIMESTAMP, NUMERIC — these need
			 * buffer_ access or multi-step logic, handled by readPrimitive.
			 */
			return nullptr;
	}
}

/*
 * ---------------------------------------------------------------------------
 * Whole-column batch read path
 * ---------------------------------------------------------------------------
 */

/*
 * Map a column to its batch fill kind.  FILL_NONE means the column stays on
 * the per-value Scanner path.  Kinds mirror the type legs of readPrimitive()/
 * readDecimal() exactly so results are byte-for-byte identical.
 */
ParquetReader::FillKind
ParquetReader::resolveFillKind(const TypeInfo &typInfo)
{
	switch (typInfo.pgTypeId_)
	{
		case BOOLOID:
			return FILL_BOOL;
		case INT2OID:
			return FILL_INT2;
		case INT4OID:
			return FILL_INT4;
		case TIMEOID:
		case INT8OID:
			return FILL_INT8;
		case FLOAT4OID:
			return FILL_FLOAT4;
		case FLOAT8OID:
			return FILL_FLOAT8;
		case DATEOID:
			return FILL_DATE;
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			/* INT96 timestamps are not read via the Int64 leg; keep old path */
			return typInfo.fileTypeId_ == INT8OID ? FILL_TIMESTAMP : FILL_NONE;
		case NUMERICOID:
			switch (typInfo.fileTypeId_)
			{
				case INT4OID:
					return FILL_NUMERIC_I32;
				case INT8OID:
					return FILL_NUMERIC_I64;
				default:
					return FILL_NUMERIC_FLBA;
			}
		case BYTEAOID:
		case TEXTOID:
		case VARCHAROID:
			return FILL_TEXT;
		case BPCHAROID:
			return FILL_BPCHAR;
		case UUIDOID:
			return FILL_UUID;
		default:
			return FILL_NONE;
	}
}

/*
 * One-time setup when batch mode turns on: pick the batchable columns,
 * repoint their readFn_ at the batch serve functions and size their
 * buffers.  Columns with repetition levels (repeated/nested) keep the
 * Scanner path -- the batch expansion below only handles flat def levels.
 */
void
ParquetReader::setupBatchColumns()
{
	auto schema = metadata->schema();

	colBatch_.resize(numColumns_);
	for (size_t attr = 0; attr < typeMap_.size(); attr++)
	{
		TypeInfo &typInfo = typeMap_[attr];
		int ci = typInfo.columnIndex_;

		if (ci < 0)
			continue;

		const parquet::ColumnDescriptor *cd = schema->Column(ci);
		if (cd->max_repetition_level() > 0)
			continue;

		FillKind kind = resolveFillKind(typInfo);
		if (kind == FILL_NONE)
			continue;

		ColumnBatch &b = colBatch_[ci];
		b.kind = kind;
		b.attr = (int) attr;
		b.maxDef = cd->max_definition_level();
		b.datums.resize(COLUMN_BATCH_SIZE);
		b.isnull.resize(COLUMN_BATCH_SIZE);
		if (b.maxDef > 0)
			b.defs.resize(COLUMN_BATCH_SIZE);

		switch (kind)
		{
			case FILL_TEXT:
			case FILL_BPCHAR:
			case FILL_UUID:
				b.offsets.resize(COLUMN_BATCH_SIZE);
				b.slab.reserve(COLUMN_BATCH_SIZE * 32);
				typInfo.readFn_ = serveBatchPtr;
				break;
			case FILL_NUMERIC_I32:
			case FILL_NUMERIC_I64:
			case FILL_NUMERIC_FLBA:
			{
				const size_t numSz = NUMERIC_HDRSZ +
					IntDigitsTraits<__int128>::digits * sizeof(NumericDigit);
				b.slab.resize(COLUMN_BATCH_SIZE * numSz);
				typInfo.readFn_ = serveBatchVal;
				break;
			}
			default:
				typInfo.readFn_ = serveBatchVal;
				break;
		}
	}

	/*
	 * Build the active-attribute list for the fused populateRecord loop,
	 * mirroring its branch order: batch-served columns, readFn_ columns and
	 * scanner-path columns are active; attributes with no file column (or an
	 * unmapped file type) are always NULL and are pre-filled once per record
	 * buffer instead of being re-tested for every row.
	 */
	activeAttrs_.clear();
	for (size_t attr = 0; attr < typeMap_.size(); attr++)
	{
		const TypeInfo &ti = typeMap_[attr];
		int ci = ti.columnIndex_;

		if ((ci >= 0 && colBatch_[ci].kind != FILL_NONE) ||
			ti.readFn_ != nullptr ||
			(ci >= 0 && ti.fileTypeId_ != InvalidOid))
			activeAttrs_.push_back((uint32_t) attr);
	}
	nullPrefilledRecord_ = nullptr;
}

Datum
ParquetReader::serveBatchVal(BaseFileReader *r, int idx, bool &isNull)
{
	auto *self = static_cast<ParquetReader *>(r);
	ColumnBatch &b = self->colBatch_[idx];

	if (b.pos >= b.filled)
		self->refillColumnBatch(idx);
	if (b.pos >= b.filled)
	{
		/* Column exhausted; defensive, next() advances row groups first. */
		isNull = true;
		return (Datum) 0;
	}
	isNull = b.isnull[b.pos];
	return b.datums[b.pos++];
}

Datum
ParquetReader::serveBatchPtr(BaseFileReader *r, int idx, bool &isNull)
{
	auto *self = static_cast<ParquetReader *>(r);
	ColumnBatch &b = self->colBatch_[idx];

	if (b.pos >= b.filled)
		self->refillColumnBatch(idx);
	if (b.pos >= b.filled)
	{
		isNull = true;
		return (Datum) 0;
	}
	isNull = b.isnull[b.pos];
	if (isNull)
	{
		b.pos++;
		return (Datum) 0;
	}
	/* Dictionary-exposed chunks store direct Datums (into dictSlab). */
	if (b.dictExposed)
		return b.datums[b.pos++];
	return PointerGetDatum(b.slab.data() + b.offsets[b.pos++]);
}

/*
 * Append one varlena/uuid payload to the batch arena, growing geometrically.
 * Offsets (not pointers) are stored so growth cannot invalidate served rows:
 * serveBatchPtr recomputes the pointer from slab.data() at serve time, and
 * the arena only grows during refill, never while rows of the current batch
 * are being served.
 */
static inline size_t
arenaAppend(std::vector<char> &slab, size_t &used, const void *src,
			uint32_t len, uint32_t extra)
{
	size_t need = used + len + extra;

	if (need > slab.size())
		slab.resize(std::max(need, slab.size() * 2));

	size_t off = used;

	if (len > 0)
		memcpy(slab.data() + used, src, len);
	used = need;
	return off;
}

/*
 * Refill one column's batch: bulk-decode up to COLUMN_BATCH_SIZE rows via
 * TypedColumnReader::ReadBatch and convert them to Datum-ready form in one
 * per-type loop.  ReadBatch returns dense (non-null only) values plus one
 * def level per row; EXPAND walks the levels and spaces the converted
 * values out to one slot per row.
 */
void
ParquetReader::refillColumnBatch(int idx)
{
	ColumnBatch &b = colBatch_[idx];
	const TypeInfo &ti = typeMap_[b.attr];
	const int N = COLUMN_BATCH_SIZE;
	int16_t *defs = b.maxDef > 0 ? b.defs.data() : nullptr;
	const int16_t maxDef = b.maxDef;
	int64_t valuesRead = 0;
	int64_t levels = 0;

	b.pos = 0;
	b.filled = 0;
	b.slabUsed = 0;

	if (b.dictExposed)
	{
		refillColumnBatchDict(b, ti);
		return;
	}

	/*
	 * Convert dense values + def levels to one Datum slot per row.  CONV is
	 * evaluated once per non-null value with `v` bound to the raw value.
	 */
#define EXPAND(vals, CONV) \
	do { \
		if (defs == nullptr) \
		{ \
			for (int64_t i = 0; i < levels; i++) \
			{ \
				auto v = (vals)[i]; \
				b.isnull[i] = 0; \
				b.datums[i] = (CONV); \
			} \
		} \
		else \
		{ \
			int64_t vi = 0; \
			for (int64_t i = 0; i < levels; i++) \
			{ \
				if (defs[i] < maxDef) \
				{ \
					b.isnull[i] = 1; \
					b.datums[i] = (Datum) 0; \
				} \
				else \
				{ \
					auto v = (vals)[vi++]; \
					b.isnull[i] = 0; \
					b.datums[i] = (CONV); \
				} \
			} \
		} \
	} while (0)

	switch (b.kind)
	{
		case FILL_BOOL:
		{
			auto *tr = static_cast<parquet::BoolReader *>(b.reader.get());
			b.raw.resize(N * sizeof(bool));
			bool *vals = (bool *) b.raw.data();

			levels = tr->ReadBatch(N, defs, nullptr, vals, &valuesRead);
			if (levels <= 0)
				return;
			EXPAND(vals, BoolGetDatum(v));
			break;
		}
		case FILL_INT2:
		case FILL_INT4:
		case FILL_DATE:
		{
			auto *tr = static_cast<parquet::Int32Reader *>(b.reader.get());
			b.raw.resize(N * sizeof(int32_t));
			int32_t *vals = (int32_t *) b.raw.data();

			levels = tr->ReadBatch(N, defs, nullptr, vals, &valuesRead);
			if (levels <= 0)
				return;
			if (b.kind == FILL_INT4)
				EXPAND(vals, Int32GetDatum(v));
			else if (b.kind == FILL_INT2)
				EXPAND(vals, Int16GetDatum((int16) v));
			else
				EXPAND(vals, DateADTGetDatum(v + (UNIX_EPOCH_JDATE - POSTGRES_EPOCH_JDATE)));
			break;
		}
		case FILL_INT8:
		{
			auto *tr = static_cast<parquet::Int64Reader *>(b.reader.get());
			b.raw.resize(N * sizeof(int64_t));
			int64_t *vals = (int64_t *) b.raw.data();

			levels = tr->ReadBatch(N, defs, nullptr, vals, &valuesRead);
			if (levels <= 0)
				return;
			EXPAND(vals, Int64GetDatum(v));
			break;
		}
		case FILL_FLOAT4:
		{
			auto *tr = static_cast<parquet::FloatReader *>(b.reader.get());
			b.raw.resize(N * sizeof(float));
			float *vals = (float *) b.raw.data();

			levels = tr->ReadBatch(N, defs, nullptr, vals, &valuesRead);
			if (levels <= 0)
				return;
			EXPAND(vals, Float4GetDatum(v));
			break;
		}
		case FILL_FLOAT8:
		{
			auto *tr = static_cast<parquet::DoubleReader *>(b.reader.get());
			b.raw.resize(N * sizeof(double));
			double *vals = (double *) b.raw.data();

			levels = tr->ReadBatch(N, defs, nullptr, vals, &valuesRead);
			if (levels <= 0)
				return;
			EXPAND(vals, Float8GetDatum(v));
			break;
		}
		case FILL_TIMESTAMP:
		{
			static const int64 UNIX_TO_PG_EPOCH_USECS =
				((int64)(POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE)) * SECS_PER_DAY * USECS_PER_SEC;
			auto *tr = static_cast<parquet::Int64Reader *>(b.reader.get());
			b.raw.resize(N * sizeof(int64_t));
			int64_t *vals = (int64_t *) b.raw.data();

			levels = tr->ReadBatch(N, defs, nullptr, vals, &valuesRead);
			if (levels <= 0)
				return;
			switch (ti.timeUnit_)
			{
				case TIMEUNIT_MILLIS:
					EXPAND(vals, TimestampGetDatum(v * 1000 - UNIX_TO_PG_EPOCH_USECS));
					break;
				case TIMEUNIT_NANOS:
					EXPAND(vals, TimestampGetDatum(v / 1000 - UNIX_TO_PG_EPOCH_USECS));
					break;
				case TIMEUNIT_MICROS:
				default:
					/* Micros is the Iceberg standard; also the untyped fallback */
					EXPAND(vals, TimestampGetDatum(v - UNIX_TO_PG_EPOCH_USECS));
					break;
			}
			break;
		}
		case FILL_NUMERIC_I32:
		case FILL_NUMERIC_I64:
		{
			const size_t numSz = NUMERIC_HDRSZ +
				IntDigitsTraits<__int128>::digits * sizeof(NumericDigit);
			char *base = b.slab.data();
			const int scale = ti.scale_;

			if (b.kind == FILL_NUMERIC_I32)
			{
				auto *tr = static_cast<parquet::Int32Reader *>(b.reader.get());
				b.raw.resize(N * sizeof(int32_t));
				int32_t *vals = (int32_t *) b.raw.data();

				levels = tr->ReadBatch(N, defs, nullptr, vals, &valuesRead);
				if (levels <= 0)
					return;
				EXPAND(vals,
					   (int_to_numeric_with_scale(v, scale, (Numeric)(base + (size_t) i * numSz)),
						NumericGetDatum(base + (size_t) i * numSz)));
			}
			else
			{
				auto *tr = static_cast<parquet::Int64Reader *>(b.reader.get());
				b.raw.resize(N * sizeof(int64_t));
				int64_t *vals = (int64_t *) b.raw.data();

				levels = tr->ReadBatch(N, defs, nullptr, vals, &valuesRead);
				if (levels <= 0)
					return;
				EXPAND(vals,
					   (int_to_numeric_with_scale(v, scale, (Numeric)(base + (size_t) i * numSz)),
						NumericGetDatum(base + (size_t) i * numSz)));
			}
			break;
		}
		case FILL_NUMERIC_FLBA:
		{
			const size_t numSz = NUMERIC_HDRSZ +
				IntDigitsTraits<__int128>::digits * sizeof(NumericDigit);
			char *base = b.slab.data();
			const int scale = ti.scale_;
			const int typeLen = ti.typeLength_;
			const bool fits64 = (ti.precision_ > 0 && ti.precision_ <= 18);
			auto *tr = static_cast<parquet::FixedLenByteArrayReader *>(b.reader.get());

			b.raw.resize(N * sizeof(parquet::FixedLenByteArray));
			parquet::FixedLenByteArray *vals = (parquet::FixedLenByteArray *) b.raw.data();

			levels = tr->ReadBatch(N, defs, nullptr, vals, &valuesRead);
			if (levels <= 0)
				return;
			if (fits64)
				EXPAND(vals,
					   (int_to_numeric_with_scale(FLBA_to_int64(v.ptr, typeLen), scale,
												  (Numeric)(base + (size_t) i * numSz)),
						NumericGetDatum(base + (size_t) i * numSz)));
			else
				EXPAND(vals,
					   (int_to_numeric_with_scale(FLBA_to_int128(v.ptr, typeLen), scale,
												  (Numeric)(base + (size_t) i * numSz)),
						NumericGetDatum(base + (size_t) i * numSz)));
			break;
		}
		case FILL_TEXT:
		{
			auto *tr = static_cast<parquet::ByteArrayReader *>(b.reader.get());

			b.raw.resize(N * sizeof(parquet::ByteArray));
			parquet::ByteArray *vals = (parquet::ByteArray *) b.raw.data();

			levels = tr->ReadBatch(N, defs, nullptr, vals, &valuesRead);
			if (levels <= 0)
				return;

			int64_t vi = 0;
			for (int64_t i = 0; i < levels; i++)
			{
				if (defs != nullptr && defs[i] < maxDef)
				{
					b.isnull[i] = 1;
					b.offsets[i] = 0;
					continue;
				}
				const parquet::ByteArray &v = vals[vi++];
				size_t off = arenaAppend(b.slab, b.slabUsed, nullptr, 0,
										   v.len + VARHDRSZ);
				char *dst = b.slab.data() + off;

				SET_VARSIZE(dst, v.len + VARHDRSZ);
				if (v.len > 0)
					memcpy(VARDATA(dst), v.ptr, v.len);
				b.isnull[i] = 0;
				b.offsets[i] = off;
			}
			break;
		}
		case FILL_BPCHAR:
		{
			auto *tr = static_cast<parquet::ByteArrayReader *>(b.reader.get());
			const int typeMod = ti.typeMod_;

			b.raw.resize(N * sizeof(parquet::ByteArray));
			parquet::ByteArray *vals = (parquet::ByteArray *) b.raw.data();

			levels = tr->ReadBatch(N, defs, nullptr, vals, &valuesRead);
			if (levels <= 0)
				return;

			int64_t vi = 0;
			for (int64_t i = 0; i < levels; i++)
			{
				if (defs != nullptr && defs[i] < maxDef)
				{
					b.isnull[i] = 1;
					b.offsets[i] = 0;
					continue;
				}
				const parquet::ByteArray &v = vals[vi++];

				/*
				 * Re-pad CHAR(N) with blanks up to the declared length, same
				 * UTF-8 codepoint counting as readPrimitive()'s BPCHAR leg.
				 */
				int pad = 0;
				if (typeMod >= (int) VARHDRSZ)
				{
					int maxlen = typeMod - VARHDRSZ;
					int charlen = 0;

					for (uint32_t k = 0; k < v.len; k++)
						if (((unsigned char) v.ptr[k] & 0xC0) != 0x80)
							charlen++;
					if (charlen < maxlen)
						pad = maxlen - charlen;
				}
				uint32_t totalLen = v.len + pad;
				size_t off = arenaAppend(b.slab, b.slabUsed, nullptr, 0,
										   totalLen + VARHDRSZ);
				char *dst = b.slab.data() + off;

				SET_VARSIZE(dst, totalLen + VARHDRSZ);
				if (v.len > 0)
					memcpy(VARDATA(dst), v.ptr, v.len);
				if (pad > 0)
					memset(VARDATA(dst) + v.len, ' ', pad);
				b.isnull[i] = 0;
				b.offsets[i] = off;
			}
			break;
		}
		case FILL_UUID:
		{
			auto *tr = static_cast<parquet::FixedLenByteArrayReader *>(b.reader.get());

			b.raw.resize(N * sizeof(parquet::FixedLenByteArray));
			parquet::FixedLenByteArray *vals = (parquet::FixedLenByteArray *) b.raw.data();

			levels = tr->ReadBatch(N, defs, nullptr, vals, &valuesRead);
			if (levels <= 0)
				return;

			int64_t vi = 0;
			for (int64_t i = 0; i < levels; i++)
			{
				if (defs != nullptr && defs[i] < maxDef)
				{
					b.isnull[i] = 1;
					b.offsets[i] = 0;
					continue;
				}
				/* Raw 16-byte pg uuid payload, matching readPrimitive() */
				b.offsets[i] = arenaAppend(b.slab, b.slabUsed,
										   vals[vi++].ptr, 16, 0);
				b.isnull[i] = 0;
			}
			break;
		}
		default:
			return;
	}
#undef EXPAND

	b.filled = (int) levels;
}

/*
 * Convert a column chunk's dictionary to Datum-ready form, once per chunk.
 * After this, each row costs one index lookup instead of one conversion.
 * dictSlab is only written here and never grows while rows reference it,
 * so dictDatums can hold direct pointers.
 */
void
ParquetReader::convertDict(ColumnBatch &b, const TypeInfo &ti,
						   const void *dict, int32_t dictLen)
{
	const size_t numSz = NUMERIC_HDRSZ +
		IntDigitsTraits<__int128>::digits * sizeof(NumericDigit);

	b.dictDatums.resize(dictLen);

	switch (b.kind)
	{
		case FILL_NUMERIC_I32:
		case FILL_NUMERIC_I64:
		{
			const int scale = ti.scale_;

			b.dictSlab.resize((size_t) dictLen * numSz);
			char *base = b.dictSlab.data();

			if (b.kind == FILL_NUMERIC_I32)
			{
				const int32_t *vals = (const int32_t *) dict;

				for (int32_t k = 0; k < dictLen; k++)
				{
					char *out = base + (size_t) k * numSz;

					int_to_numeric_with_scale(vals[k], scale, (Numeric) out);
					b.dictDatums[k] = NumericGetDatum(out);
				}
			}
			else
			{
				const int64_t *vals = (const int64_t *) dict;

				for (int32_t k = 0; k < dictLen; k++)
				{
					char *out = base + (size_t) k * numSz;

					int_to_numeric_with_scale(vals[k], scale, (Numeric) out);
					b.dictDatums[k] = NumericGetDatum(out);
				}
			}
			break;
		}
		case FILL_NUMERIC_FLBA:
		{
			const int scale = ti.scale_;
			const int typeLen = ti.typeLength_;
			const bool fits64 = (ti.precision_ > 0 && ti.precision_ <= 18);
			const parquet::FixedLenByteArray *vals = (const parquet::FixedLenByteArray *) dict;

			b.dictSlab.resize((size_t) dictLen * numSz);
			char *base = b.dictSlab.data();

			for (int32_t k = 0; k < dictLen; k++)
			{
				char *out = base + (size_t) k * numSz;

				if (fits64)
					int_to_numeric_with_scale(FLBA_to_int64(vals[k].ptr, typeLen), scale, (Numeric) out);
				else
					int_to_numeric_with_scale(FLBA_to_int128(vals[k].ptr, typeLen), scale, (Numeric) out);
				b.dictDatums[k] = NumericGetDatum(out);
			}
			break;
		}
		case FILL_TEXT:
		case FILL_BPCHAR:
		{
			const parquet::ByteArray *vals = (const parquet::ByteArray *) dict;
			const int typeMod = ti.typeMod_;
			std::vector<size_t> offs(dictLen);
			size_t used = 0;

			for (int32_t k = 0; k < dictLen; k++)
			{
				const parquet::ByteArray &v = vals[k];
				int pad = 0;

				if (b.kind == FILL_BPCHAR && typeMod >= (int) VARHDRSZ)
				{
					int maxlen = typeMod - VARHDRSZ;
					int charlen = 0;

					for (uint32_t j = 0; j < v.len; j++)
						if (((unsigned char) v.ptr[j] & 0xC0) != 0x80)
							charlen++;
					if (charlen < maxlen)
						pad = maxlen - charlen;
				}

				uint32_t totalLen = v.len + pad;
				size_t off = arenaAppend(b.dictSlab, used, nullptr, 0,
										   totalLen + VARHDRSZ);
				char *dst = b.dictSlab.data() + off;

				SET_VARSIZE(dst, totalLen + VARHDRSZ);
				if (v.len > 0)
					memcpy(VARDATA(dst), v.ptr, v.len);
				if (pad > 0)
					memset(VARDATA(dst) + v.len, ' ', pad);
				offs[k] = off;
			}
			/* Fix up pointers only after the arena stopped growing. */
			for (int32_t k = 0; k < dictLen; k++)
				b.dictDatums[k] = PointerGetDatum(b.dictSlab.data() + offs[k]);
			break;
		}
		default:
			throw Error("unexpected dictionary fill kind %d", (int) b.kind);
	}

	b.dictConvertedFrom = dict;
}

/*
 * Fused row fill: serve every batch column of the current row inline in one
 * loop (no per-column function-pointer hop), falling back to the base logic
 * per column for non-batch columns.  Overrides the base populateRecord; one
 * virtual call per row replaces one indirect call per column per row.
 */
void
ParquetReader::populateRecord(DatalakeInternalRecord *record)
{
	size_t nactive = activeAttrs_.size();

	if (!batchMode_)
	{
		BaseFileReader::populateRecord(record);
		return;
	}

	/*
	 * Always-NULL attributes (not in activeAttrs_) keep the same value for
	 * every row, so write them once per record buffer instead of per row.
	 */
	if ((void *) record != nullPrefilledRecord_)
	{
		size_t size = typeMap_.size();

		for (size_t attr = 0; attr < size; attr++)
		{
			record->nulls[attr] = true;
			record->values[attr] = (Datum) 0;
		}
		nullPrefilledRecord_ = (void *) record;
	}

	for (size_t k = 0; k < nactive; k++)
	{
		size_t attr = activeAttrs_[k];
		TypeInfo &typInfo = typeMap_[attr];
		int ci = typInfo.columnIndex_;

		if (ci >= 0 && colBatch_[ci].kind != FILL_NONE)
		{
			ColumnBatch &b = colBatch_[ci];

			if (b.pos >= b.filled)
				refillColumnBatch(ci);
			if (b.pos >= b.filled)
			{
				record->nulls[attr] = true;
				continue;
			}
			if (b.isnull[b.pos])
			{
				record->nulls[attr] = true;
				record->values[attr] = (Datum) 0;
				b.pos++;
				continue;
			}
			record->nulls[attr] = false;
			if (b.kind == FILL_TEXT || b.kind == FILL_BPCHAR || b.kind == FILL_UUID)
				record->values[attr] = b.dictExposed
					? b.datums[b.pos]
					: PointerGetDatum(b.slab.data() + b.offsets[b.pos]);
			else
				record->values[attr] = b.datums[b.pos];
			b.pos++;
		}
		else if (typInfo.readFn_)
		{
			bool isNull = false;

			record->values[attr] = typInfo.readFn_(this, ci, isNull);
			record->nulls[attr] = isNull;
		}
		else if (ci < 0 || typInfo.fileTypeId_ == InvalidOid)
		{
			record->nulls[attr] = true;
		}
		else
		{
			bool isNull = false;

			record->values[attr] = supportsBatchPrimitive()
				? readBatchPrimitive(typInfo, isNull)
				: readPrimitive(typInfo, isNull);
			record->nulls[attr] = isNull;
		}
	}

	record->position = rowPositions_[curGroup_] + curRow_;
}

/*
 * Dictionary-exposed refill: read indices instead of values, convert the
 * dictionary once per chunk, then serve rows as dictDatums[index].  All
 * dict kinds serve direct Datums, so serveBatchVal handles them.
 */
void
ParquetReader::refillColumnBatchDict(ColumnBatch &b, const TypeInfo &ti)
{
	const int N = COLUMN_BATCH_SIZE;
	int16_t *defs = b.maxDef > 0 ? b.defs.data() : nullptr;
	const int16_t maxDef = b.maxDef;
	int64_t indicesRead = 0;
	int64_t levels = 0;
	int32_t dictLen = 0;
	const void *dict = nullptr;

	b.raw.resize(N * sizeof(int32_t));
	int32_t *indices = (int32_t *) b.raw.data();

	switch (b.kind)
	{
		case FILL_NUMERIC_I32:
		{
			const int32_t *d = nullptr;

			levels = static_cast<parquet::Int32Reader *>(b.reader.get())->ReadBatchWithDictionary(
				N, defs, nullptr, indices, &indicesRead, &d, &dictLen);
			dict = d;
			break;
		}
		case FILL_NUMERIC_I64:
		{
			const int64_t *d = nullptr;

			levels = static_cast<parquet::Int64Reader *>(b.reader.get())->ReadBatchWithDictionary(
				N, defs, nullptr, indices, &indicesRead, &d, &dictLen);
			dict = d;
			break;
		}
		case FILL_NUMERIC_FLBA:
		{
			const parquet::FixedLenByteArray *d = nullptr;

			levels = static_cast<parquet::FixedLenByteArrayReader *>(b.reader.get())->ReadBatchWithDictionary(
				N, defs, nullptr, indices, &indicesRead, &d, &dictLen);
			dict = d;
			break;
		}
		case FILL_TEXT:
		case FILL_BPCHAR:
		{
			const parquet::ByteArray *d = nullptr;

			levels = static_cast<parquet::ByteArrayReader *>(b.reader.get())->ReadBatchWithDictionary(
				N, defs, nullptr, indices, &indicesRead, &d, &dictLen);
			dict = d;
			break;
		}
		default:
			throw Error("unexpected dictionary fill kind %d", (int) b.kind);
	}

	if (levels <= 0)
		return;

	if (indicesRead > 0 && dict == nullptr)
		throw Error("parquet dictionary not returned for dictionary-encoded chunk");

	if (dict != nullptr && dict != b.dictConvertedFrom)
		convertDict(b, ti, dict, dictLen);

	if (defs == nullptr)
	{
		for (int64_t i = 0; i < levels; i++)
		{
			b.isnull[i] = 0;
			b.datums[i] = b.dictDatums[indices[i]];
		}
	}
	else
	{
		int64_t vi = 0;

		for (int64_t i = 0; i < levels; i++)
		{
			if (defs[i] < maxDef)
			{
				b.isnull[i] = 1;
				b.datums[i] = (Datum) 0;
			}
			else
			{
				b.isnull[i] = 0;
				b.datums[i] = b.dictDatums[indices[vi++]];
			}
		}
	}

	b.filled = (int) levels;
}

/*
 * True when Datums for this record attribute are served from the batch path:
 * varlena/uuid values point into the column's slab (or dictionary slab) and
 * by-value/numeric datums into reader-owned buffers, all reused across rows.
 * FILL_NONE columns fall back to per-row readPrimitive() palloc, which the
 * caller owns and must free.
 */
bool
ParquetReader::datumOwnedByReader(int attIdx) const
{
	if (!batchMode_)
		return false;

	for (const ColumnBatch &cb : colBatch_)
	{
		if (cb.attr == attIdx)
			return cb.kind != FILL_NONE;
	}
	return false;
}
