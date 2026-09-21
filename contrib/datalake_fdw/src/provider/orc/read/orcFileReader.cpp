#include "arrow/adapters/orc/util.h"
#include "orcFileReader.h"
#include <list>
#include <cassert>
#include <sstream>
#include "src/provider/common/datalake_numeric.h"
extern "C"
{
#include "utils/date.h"
#include "utils/timestamp.h"
#include "utils/builtins.h"
#include "fmgr.h"
}

#define ORC_FIELD_STR_BUF_LEN (1024)

namespace Datalake {
namespace Internal {

bool orcFileReader::createORCReader(ossFileStream fileStream, std::string fileName, int64_t length, readOption options)
{
	this->options = options;
	state = FILE_OPEN;
	readInterface.setTransactionTable(options.transactionTable);
	return readInterface.createORCReader(fileStream, fileName, length,
		setStreamWhetherCache(options), options.allocBuffer);
}

void orcFileReader::resetORCReader()
{
	readInterface.batch.reset();
	readInterface.rowReader.reset();
	readInterface.reader.reset();
	if (state != FILE_UNDEF)
	{
		readInterface.deleteORCReader();
	}
	state = FILE_CLOSE;
}

void orcFileReader::closeORCReader()
{
	if (state == FILE_CLOSE)
    {
        return;
    }
	resetORCReader();
	state = FILE_CLOSE;
}

void orcFileReader::read(TupleDesc tupdesc, Datum *values, bool *nulls, int index, int columns)
{
	orc::StructVectorBatch *orcVector = readInterface.getORCStructVectorBatch();
	int fieldsLength = orcVector->fields.size();
	for (int i = 0, j = 0; i < columns; ++i)
	{
		if (!options.includes_columns[i])
		{
			nulls[i] = true;
			if (options.transactionTable)
            {
                /*
                transcation table format
                {"operation":0,
                "originalTransaction":1,
                "bucket":536870912,
                "rowId":0,
                "currentTransaction":1,
                "row":{"id":1,"name":"Jerry","salary":5000}}

                transcation table take the field of "row" save data in rowoption.
                So get all the columns of transcation table. Here is to skip the columns
                that select will not query.
                */
                ++j;
            }
            continue;
		}
		if (j >= fieldsLength || (orcVector->fields[j]->hasNulls && orcVector->fields[j]->notNull[index] == false))
        {
            nulls[i] = true;
            ++j;
            continue;
        }

        values[i] = readField(tupdesc, tupdesc->attrs[i].atttypid, index, tupdesc->attrs[i].attname.data, i, orcVector->fields[j]);
        nulls[i] = false;
        ++j;
	}
	return;
}

arrow::Status orcFileReader::readRecordBatch(arrow::MemoryPool* pool,
				std::shared_ptr<arrow::Schema>& schema,
				std::shared_ptr<arrow::RecordBatch>* out,
				std::vector<int32_t> &schemaColMap)
{
	std::unique_ptr<arrow::RecordBatchBuilder> builder;
	arrow::RecordBatchBuilder::Make(schema, pool, ORC_BATCH_SIZE, &builder);
	orc::StructVectorBatch *orcVector = readInterface.getORCStructVectorBatch();
	for (int i = 0; i < builder->num_fields(); i++)
	{
		RETURN_NOT_OK(arrow::adapters::orc::AppendBatchHdw(readInterface.type->getSubtype(schemaColMap[i]), orcVector->fields[i], 0,
								readInterface.batch->numElements, builder->GetField(i)));
	}
	RETURN_NOT_OK(builder->Flush(out));
	return arrow::Status::OK();
}

/*
 * Helper macro: dynamic_cast to the expected ORC vector batch type and
 * error out instead of dereferencing NULL when the actual ORC column
 * type doesn't match the PostgreSQL column type.  This turns a SIGSEGV
 * into a clean ereport(ERROR).
 */
#define ORC_SAFE_CAST(CppType, batch, columnName, typeOid) \
	({ \
		auto *_vec = dynamic_cast<CppType *>(batch); \
		if (_vec == NULL) \
			elog(ERROR, "Type mismatch reading ORC column \"%s\": " \
				 "expected %s-compatible ORC vector, got incompatible type", \
				 columnName, getColTypeName(typeOid).data()); \
		_vec; \
	})

Datum orcFileReader::readField(TupleDesc tupDesc, Oid typeOid, int rowIndex, const char *columnName, int columnIndex, orc::ColumnVectorBatch *batch)
{
    auto colType = readInterface.type->getSubtype(columnIndex);

    switch (typeOid)
    {
		case BOOLOID:
		{
			orc::LongVectorBatch *vec = ORC_SAFE_CAST(orc::LongVectorBatch, batch, columnName, typeOid);
			return BoolGetDatum(vec->data[rowIndex] != 0L);
		}
		case INT8OID:
		{
			orc::LongVectorBatch *vec = ORC_SAFE_CAST(orc::LongVectorBatch, batch, columnName, typeOid);
			return Int64GetDatum(vec->data[rowIndex]);
		}
		case INT4OID:
		{
			orc::LongVectorBatch *vec = ORC_SAFE_CAST(orc::LongVectorBatch, batch, columnName, typeOid);
			return Int32GetDatum((int32_t)vec->data[rowIndex]);
		}
		case INT2OID:
		{
			orc::LongVectorBatch *vec = ORC_SAFE_CAST(orc::LongVectorBatch, batch, columnName, typeOid);
			return Int16GetDatum((int16_t)vec->data[rowIndex]);
		}
		case FLOAT4OID:
		{
			orc::DoubleVectorBatch *vec = ORC_SAFE_CAST(orc::DoubleVectorBatch, batch, columnName, typeOid);
			return Float4GetDatum((float)vec->data[rowIndex]);
		}
		case FLOAT8OID:
		{
			orc::DoubleVectorBatch *vec = ORC_SAFE_CAST(orc::DoubleVectorBatch, batch, columnName, typeOid);
			return Float8GetDatum(vec->data[rowIndex]);
		}
		case INTERVALOID:
		{
			// No corresponding data type in orc, needs the source to be string
			orc::StringVectorBatch *vec = ORC_SAFE_CAST(orc::StringVectorBatch, batch, columnName, typeOid);
			size_t size = vec->length[rowIndex];
			if (size > ORC_FIELD_STR_BUF_LEN)
			{
				readInterface.deleteORCReader();
				elog(ERROR, "Field %d length too large.", rowIndex);
			}
			char readBuf[ORC_FIELD_STR_BUF_LEN];
			memset(readBuf, 0, ORC_FIELD_STR_BUF_LEN);
			memcpy(readBuf, vec->data[rowIndex], size);
			return DirectFunctionCall1(interval_in, CStringGetDatum(readBuf));
		}
		case DATEOID:
		{
			orc::LongVectorBatch *vec = ORC_SAFE_CAST(orc::LongVectorBatch, batch, columnName, typeOid);
			int32_t date = vec->data[rowIndex] + (UNIX_EPOCH_JDATE - POSTGRES_EPOCH_JDATE);
			return Int32GetDatum(date);

		}
		case TIMEOID:
		{
			// No corresponding data type in orc, needs the source to be string
			orc::StringVectorBatch *vec = ORC_SAFE_CAST(orc::StringVectorBatch, batch, columnName, typeOid);
			size_t size = vec->length[rowIndex];
			if (size > ORC_FIELD_STR_BUF_LEN)
			{
				readInterface.deleteORCReader();
				elog(ERROR, "Field %d length too large.", rowIndex);
			}
			char readBuf[ORC_FIELD_STR_BUF_LEN];
			memset(readBuf, 0, ORC_FIELD_STR_BUF_LEN);
			memcpy(readBuf, vec->data[rowIndex], size);
			return DirectFunctionCall1(time_in, CStringGetDatum(readBuf));
		}
		case TIMESTAMPOID:
		{
			orc::TimestampVectorBatch *vec = ORC_SAFE_CAST(orc::TimestampVectorBatch, batch, columnName, typeOid);
			int64_t second = (int64_t)vec->data[rowIndex];
			int64_t nanosec = (int64_t)vec->nanoseconds[rowIndex];
			int64_t timestamp = 0;
			int64_t micsecs = (second + (UNIX_EPOCH_JDATE - POSTGRES_EPOCH_JDATE) *60*60*24) * 1000000;
			int64_t nano = nanosec/1000;
			timestamp = micsecs + nano;
			return Int64GetDatum(timestamp);

		}
		case NUMERICOID:
		{
			int scale = 0;
			dataBuffer *ptr = options.buffer.getDataBuffer(columnIndex);
			if (colType->getPrecision() <= 18)
			{
				orc::Decimal64VectorBatch *d64vec = ORC_SAFE_CAST(orc::Decimal64VectorBatch, batch, columnName, typeOid);
				int64_t *values = d64vec->values.data();
				scale = d64vec->scale;
				int64_t value = values[rowIndex];
				int_to_numeric_with_scale(value, scale, (Numeric)ptr->buffer);
			}
			else if (colType->getPrecision() <= 38)
			{
				orc::Decimal128VectorBatch *d128vec = ORC_SAFE_CAST(orc::Decimal128VectorBatch, batch, columnName, typeOid);
				scale = d128vec->scale;
				orc::Int128 *values = d128vec->values.data();
				orc::Int128 value = values[rowIndex];
				__int128 i128 = ((__int128)value.getHighBits() << 64) + value.getLowBits();
				int_to_numeric_with_scale(i128, scale, (Numeric) ptr->buffer);
			}
			else
			{
				elog(ERROR, "ORC Decimal precision more than 38 is not support");
			}
			return NumericGetDatum((Numeric) ptr->buffer);
		}
		case CHAROID:
		{
			orc::StringVectorBatch *vec = ORC_SAFE_CAST(orc::StringVectorBatch, batch, columnName, typeOid);
			return CharGetDatum(vec->data[rowIndex][0]);
		}
		case BYTEAOID:
		case TEXTOID:
		case BPCHAROID:
		case VARCHAROID:
		{
			orc::StringVectorBatch *vec = ORC_SAFE_CAST(orc::StringVectorBatch, batch, columnName, typeOid);
			int64_t size = vec->length[rowIndex];
			if (size > BUFFER_LENGTH)
			{
				bytea *result = (bytea*)palloc(size + VARHDRSZ);
				memcpy(VARDATA(result), vec->data[rowIndex], size);
				SET_VARSIZE(result, size + VARHDRSZ);
				return PointerGetDatum(result);
			}
			else
			{
				dataBuffer *ptr = options.buffer.getDataBuffer(columnIndex);
				SET_VARSIZE(ptr->buffer, size + VARHDRSZ);
				memcpy(VARDATA(ptr->buffer), vec->data[rowIndex], size);
				return PointerGetDatum(ptr->buffer);
			}
		}
		default:
			return 0;
    }

    return 0;
}

bool orcFileReader::compareToDeleteMap(orcReadDeltaFile &compact, int index)
{
	orc::StructVectorBatch *batchVector = dynamic_cast<orc::StructVectorBatch *>(readInterface.batch.get());
    if (compact.compareMapIsEmpty())
    {
        return false;
    }
    transcationStruct data;
    orc::ColumnVectorBatch *batch = batchVector->fields[0];
    orc::LongVectorBatch *vec = dynamic_cast<orc::LongVectorBatch *>(batch);
    data.operation = vec->data[index];

    batch = batchVector->fields[1];
    vec = dynamic_cast<orc::LongVectorBatch *>(batch);
    data.originalTransaction = vec->data[index];

    batch = batchVector->fields[2];
    vec = dynamic_cast<orc::LongVectorBatch *>(batch);
    data.bucketId = vec->data[index];

    batch = batchVector->fields[3];
    vec = dynamic_cast<orc::LongVectorBatch *>(batch);
    data.rowId = vec->data[index];

    batch = batchVector->fields[4];
    vec = dynamic_cast<orc::LongVectorBatch *>(batch);
    data.currentTransaction = vec->data[index];

    if (compact.compareTo(data) == ROW_EQUAL_OTHER)
    {
        return true;
    }
    return false;
}

void orcFileReader::checkFileSchemaCompatibility(TupleDesc tupdesc, int columns)
{
    for (int i = 0; i < columns; ++i)
    {
        auto colType = readInterface.type->getSubtype(i);
        auto orcColType = colType->getKind();
        auto columnName = tupdesc->attrs[i].attname.data;
        auto typeOid = tupdesc->attrs[i].atttypid;
        if (!checkDataTypeCompatible(typeOid, orcColType))
        {
            std::string column_type = colType->toString().data();
            readInterface.deleteORCReader();
             elog(ERROR,
                "Type Mismatch: data in '%s' is defined as '%s' in ORC file, but in datalake foreign table "
                "as '%s'. %s",
                columnName, column_type.c_str(), getColTypeName(typeOid).data(), getTypeMappingSupported().data());
        }
    }
}
}
}
