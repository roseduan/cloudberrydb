#include <parquet/internal/arrow/util/decimal.h>
#include <parquet/internal/arrow/result.h>
#include "parquet_reader.h"
#include "common.h"
#include "gopher_random_file.h"
#include "datalake_numeric.h"

extern "C"
{
#include "postgres.h"
#include "access/tupdesc.h"
#include "datatype/timestamp.h"
#include "utils/memutils.h"
#include "nodes/pg_list.h"
#include "utils/builtins.h"
#include "utils.h"
#include "src/datalake_def.h"
}

ParquetReader::ParquetReader(MemoryContext rowContext, char *filePath, gopherFS gopherFilesystem, dataBufferArray *buffer)
	: BaseFileReader(rowContext), numColumns_(0), filePath_(filePath), gopherFilesystem_(gopherFilesystem), buffer_(buffer)
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

	scanners_.clear();
	scanners_.resize(numColumns_);
	for (size_t i = 0; i < typeSize; ++i)
	{
		TypeInfo &typInfo = typeMap_[i];

		if (typInfo.columnIndex_ >= 0)
		{
			scanners_[typInfo.columnIndex_] = parquet::Scanner::Make(
				rowGroupReader->Column(typInfo.columnIndex_), scannerBatchSize);
		}
	}

	return true;
}

void
ParquetReader::open(List *columnDesc, bool *attrUsed, int64 startOffset, int64 endOffset)
{
	std::string filename = convertToGopherPath(filePath_);
	reader_ = parquet::ParquetFileReader::Open(std::make_shared<GopherRandomAccessFile> (gopherFilesystem_, filename));
	metadata = reader_->metadata();
	createMapping(columnDesc, attrUsed);
	filterRowGroupByOffset(startOffset, endOffset);
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
			rowGroups_.push_back(i);
			rowPositions_.push_back(0);
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
			rowGroups_.push_back(i);
			rowPositions_.push_back(curRowCount);
		}

		curRowCount += rowGroup->num_rows();
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
		case BPCHAROID:
		{
			/*
			 * BPCHAR on Iceberg is written as variable-length BYTE_ARRAY +
			 * UTF8 (Iceberg `string`; see commit 48bf8311b1a / issue #321).
			 * Trailing-space padding is not preserved on disk, so read it
			 * back exactly like VARCHAR / TEXT, with no padding to N.
			 */
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
