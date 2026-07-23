#include <parquet/internal/arrow/util/decimal.h>
#include <parquet/internal/arrow/result.h>
#include "avro_block_reader.h"
#include "common.h"

extern "C"
{
#include "postgres.h"
#include "datatype/timestamp.h"
#include "utils/memutils.h"
#include "nodes/pg_list.h"
#include "utils/builtins.h"
#include "utils.h"
#include "jansson.h"
}

AvroBlockReader::AvroBlockReader(MemoryContext rowContext,
								 char *contentBuffer,
								 char *schemaBuffer)
	: BaseFileReader(rowContext),
	  contentBuffer_(contentBuffer),
	  schemaBuffer_(schemaBuffer),
	  contentBufferPos_(0),
	  lastRecordLength_(0),
	  dataSchema_(NULL),
	  schemaIface_(NULL),
	  reader_(NULL),
	  isRecordInited(false)
{}

AvroBlockReader::~AvroBlockReader() {}

TIMEUNIT
AvroBlockReader::getTimeUnit(const char *field)
{
	auto it = fieldType_.find(field);

	if (it == fieldType_.end())
		return TIMEUNIT_UNKNOWN;

	if (it->second == "timestamp-micros")
		return TIMEUNIT_MICROS;

	return TIMEUNIT_MILLIS;
}

void
AvroBlockReader::createMapping(List *columnDesc, bool *attrUsed)
{
	int       i;
	ListCell *lc;

	if (!is_avro_record(dataSchema_))
		throw Error("schema: \"%s\" is not a avro_record", schemaBuffer_);

	foreach_with_count(lc, columnDesc, i)
	{
		DatalakeFieldDescription *entry = (DatalakeFieldDescription *) lfirst(lc);
		TypeInfo typInfo = {entry->typeOid, entry->typeMod, InvalidOid, -1, TIMEUNIT_UNKNOWN, 0, 0, 0, nullptr};

		typeMap_.push_back(typInfo);

		if (!attrUsed[i])
			continue;

		typeMap_[i].columnIndex_ = avro_schema_record_field_get_index(dataSchema_, entry->name);
		typeMap_[i].fileTypeId_ = mapAvroDataType(avro_typeof(avro_schema_record_field_get(dataSchema_, entry->name)));
		typeMap_[i].timeUnit_ = getTimeUnit(entry->name);
	}
}

bool
AvroBlockReader::readNextRowGroup()
{
	int totalRecords;

	curGroup_++;

	if ((uint) curGroup_ >= rowGroups_.size())
		return false;

	contentBufferPos_ += 4;
	totalRecords = *((int *) (contentBuffer_ + contentBufferPos_));
#ifndef WORDS_BIGENDIAN
	totalRecords = datalakeReverse32(totalRecords);
#endif
	numRows_ = totalRecords;

	contentBufferPos_ += 4;
	curRow_ = 0;

	if (avro_skip(reader_, contentBufferPos_))
		throw Error("failed to skip %d bytes on avro data block: %s", contentBufferPos_, avro_strerror());

	return true;
}

void
AvroBlockReader::prepareRowGroup()
{
	rowGroups_.push_back(0);
	rowPositions_.push_back(0);
}

void
AvroBlockReader::parseAvroSchema(int64_t schemaBufferLength)
{
	json_error_t error;
	json_t *root = json_loadb(schemaBuffer_, schemaBufferLength, JSON_DECODE_ANY, &error);

	if (!root)
		throw Error("failed to parse avro schema: %s", error.text);

	json_t* fields = json_object_get(root, "fields");
	if (!json_is_array(fields))
		throw Error("failed to parse avro schema: Fields is not an array");

	size_t  index;
	json_t *field;
	json_array_foreach(fields, index, field)
	{
		json_t* nameObj = json_object_get(field, "name");
		json_t* typeObj = json_object_get(field, "type");

		if (!json_is_string(nameObj) || !json_is_array(typeObj))
			continue;

		std::string name = json_string_value(nameObj);
		if (json_array_size(typeObj) >= 2)
		{
			json_t* secondType = json_array_get(typeObj, 1);
			if (!json_is_object(secondType))
				continue;

			json_t* logicalTypeObj = json_object_get(secondType, "logicalType");
			if (json_is_string(logicalTypeObj))
			{
				std::string logicalType = json_string_value(logicalTypeObj);
				if (logicalType == "timestamp-micros" || logicalType == "timestamp-millis")
					fieldType_[name] = logicalType;
			}
		}
	}

    json_decref(root);
}

void
AvroBlockReader::open(List *columnDesc, bool *attrUsed, int64_t contentBufferLlength, int64_t schemaBufferLength)
{
	if (avro_schema_from_json_length(schemaBuffer_, schemaBufferLength, &dataSchema_))
		throw Error("failed to parse avro schema: %s, \"%s\", ", avro_strerror(), schemaBuffer_);

	parseAvroSchema(schemaBufferLength);

	createMapping(columnDesc, attrUsed);
	prepareRowGroup();

	schemaIface_  = avro_generic_class_from_schema(dataSchema_);
	if (avro_generic_value_new(schemaIface_, &record_))
		throw Error("failed to create record instance of avro %s", avro_strerror());

	isRecordInited = true;

	reader_ = avro_reader_memory(contentBuffer_, contentBufferLlength);
	if (reader_ == NULL)
		throw Error("out of memory");
}

void AvroBlockReader::close()
{
	if (isRecordInited)
		avro_value_decref(&record_);

	if (schemaIface_)
		avro_value_iface_decref(schemaIface_);

	if (dataSchema_)
		avro_schema_decref(dataSchema_);

	if (reader_)
		avro_reader_free(reader_);
}

void
AvroBlockReader::decodeRecord()
{
	int recordLength;

	avro_value_reset(&record_);

	contentBufferPos_ += lastRecordLength_;

	recordLength = *((int *) (contentBuffer_ + contentBufferPos_));

#ifndef WORDS_BIGENDIAN
	recordLength = datalakeReverse32(recordLength);
#endif

	contentBufferPos_ += 4;
	if (avro_skip(reader_, 4))
		throw Error("failed to skip 4 bytes on avro data block: %s", avro_strerror());

	if (avro_value_read(reader_, &record_))
		throw Error("failed to decode record on avro data block: offset %d: %s", contentBufferPos_, avro_strerror());

	lastRecordLength_ = recordLength;
}

#define formVarlena(address, size) \
	bytea *result = (bytea *) gpdbPalloc((size) + VARHDRSZ); \
	SET_VARSIZE(result, (size) + VARHDRSZ); \
	memcpy(VARDATA(result), (address), (size))

avro_value_t *
AvroBlockReader::extractField(avro_value_t *container, avro_value_t *field)
{
	avro_type_t   typeId;

	typeId = avro_value_get_type(container);
	if (typeId != AVRO_UNION)
		return container;

	avro_value_get_current_branch(container, field);
	return field;
}

int
AvroBlockReader::extractScalFromTypeMod(int32 typmod)
{
	// int precision;
	int scale;

	if (typmod < (int32) (VARHDRSZ))
		return 0;

	typmod -= VARHDRSZ;
	// precision = (typmod >> 16) & 0xffff;
	scale = typmod & 0xffff;

	return scale;
}

Datum
AvroBlockReader::readPrimitive(const TypeInfo &typInfo, bool &isNull)
{
	ReaderValue   data;
	avro_type_t   typeId;
	avro_value_t  container;
	avro_value_t  field;
	avro_value_t *pfield;
	const char   *buff;
	size_t        buffSize;

	avro_value_get_by_index(&record_, typInfo.columnIndex_, &container, NULL);
	pfield = extractField(&container, &field);
	typeId = avro_value_get_type(pfield);
	if (typeId == AVRO_NULL)
	{
		isNull = true;
		PG_RETURN_DATUM(0);
	}

	switch (typInfo.pgTypeId_)
	{
		case BOOLOID:
		{
			avro_value_get_boolean(pfield, &data.int32Value);
			return BoolGetDatum(data.int32Value);
		}
		case INT4OID:
		{
			avro_value_get_int(pfield, &data.int32Value);
			return Int32GetDatum(data.int32Value);
		}
		case TIMEOID:
		case INT8OID:
		{
			avro_value_get_long(pfield, &data.int64Value);
			return Int64GetDatum(data.int64Value);
		}
		case FLOAT4OID:
		{
			avro_value_get_float(pfield, &data.floatValue);
			return Float4GetDatum(data.floatValue);
		}
		case FLOAT8OID:
		{
			avro_value_get_double(pfield, &data.doubleValue);
			return Float8GetDatum(data.doubleValue);
		}
		case BYTEAOID:
		{
			avro_value_get_bytes(pfield, (const void **) &buff, &buffSize);
			formVarlena(buff, buffSize);
			return PointerGetDatum(result);
		}
		case TEXTOID:
		{
			avro_value_get_string(pfield, &buff, &buffSize);
			formVarlena(buff, buffSize - 1);
			return PointerGetDatum(result);
		}
		case UUIDOID:
		{
			avro_value_get_bytes(pfield, (const void **) &buff, &buffSize);
			unsigned char *result = (unsigned char *) gpdbPalloc(16);
			memcpy(result, buff, 16);
			return PointerGetDatum(result);
		}
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
		{
			static const int64 UNIX_TO_PG_EPOCH_USECS =
				((int64)(POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE)) * SECS_PER_DAY * USECS_PER_SEC;

			avro_value_get_long(pfield, &data.int64Value);

			int64 pgTimestamp;
			switch (typInfo.timeUnit_)
			{
				case TIMEUNIT_MILLIS:
					pgTimestamp = data.int64Value * 1000 - UNIX_TO_PG_EPOCH_USECS;
					break;
				case TIMEUNIT_MICROS:
					pgTimestamp = data.int64Value - UNIX_TO_PG_EPOCH_USECS;
					break;
				case TIMEUNIT_NANOS:
					pgTimestamp = data.int64Value / 1000 - UNIX_TO_PG_EPOCH_USECS;
					break;
				default:
					throw Error("Unknown timestamp precision");
			}
			return TimestampGetDatum(pgTimestamp);
		}
		case DATEOID:
		{
			avro_value_get_int(pfield, &data.int32Value);
			return DateADTGetDatum(data.int32Value + (UNIX_EPOCH_JDATE - POSTGRES_EPOCH_JDATE));
		}
		case NUMERICOID:
		{
			avro_value_get_fixed(pfield, (const void **) &buff, &buffSize);
			parquet_arrow::Result<parquet_arrow::Decimal128> result = parquet_arrow::Decimal128::
																		FromBigEndian((const uint8_t*) buff, buffSize);
			if (!result.ok())
				throw Error("decimal128: out of range");

			int scale = extractScalFromTypeMod(typInfo.typeMod_);
			parquet_arrow::Decimal128 decimal = result.ValueOrDie();
			return gpdbDirectFunctionCall3(numeric_in,
										   CStringGetDatum(decimal.ToString(scale).c_str()),
										   ObjectIdGetDatum(InvalidOid),
										   Int32GetDatum(typInfo.typeMod_));
		}

		default:
			throw Error("unsupported column type oid: \"%u\"", typInfo.pgTypeId_);
	}

	PG_RETURN_DATUM(0);
}
