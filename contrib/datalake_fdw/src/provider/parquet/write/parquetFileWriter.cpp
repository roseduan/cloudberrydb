#include <parquet/internal/arrow/util/decimal.h>
#include <parquet/schema.h>
#include <parquet/column_writer.h>
#include <parquet/internal/arrow/util/bit_util.h>
#include <parquet/arrow/writer.h>

#include "parquetFileWriter.h"
#include "src/provider/common/datalake_numeric.h"

extern "C"
{
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/timestamp.h"
#include "utils/builtins.h"
#include "fmgr.h"
}

#define BATCH_WRITE_SIZE (1024)
#define DECIMAL_FIXBUFFER_SIZE (16)
#define GP_NUMERIC_MAX_SIZE (1000)

parquetFileWriter::parquetFileWriter()
{
    openState = false;
}

bool parquetFileWriter::createParquetWriter(ossFileStream ossFile, const std::string &fileName)
{
    name = fileName;
    out_file = std::make_shared<gopherWriteFileSystem>(ossFile);
    out_file->OpenFile(name.c_str());
    file_writer = parquet::ParquetFileWriter::Open(out_file, schema, props);
    batchNum = 0;
    dataBufferOffset = 0;
    dataBuffer.resize(1024*1024);
    rg_writer = file_writer->AppendBufferedRowGroup();
    openState = true;
    return true;
}

void parquetFileWriter::createColumnBatch()
{
    byteArray = (parquet::ByteArray*)palloc(sizeof(parquet::ByteArray) * BATCH_WRITE_SIZE);
    fixByteArray = (parquet::FixedLenByteArray*)palloc(sizeof(parquet::FixedLenByteArray) * BATCH_WRITE_SIZE);
    definition_level = (int16_t*)palloc(sizeof(int16_t) * BATCH_WRITE_SIZE);
    int96Array = (parquet::Int96*)palloc(sizeof(parquet::Int96) * BATCH_WRITE_SIZE);
    int64Array = (int64_t*)palloc(sizeof(int64_t) * BATCH_WRITE_SIZE);

    for (int i = 0; i < ncolumns; i++)
    {
        Oid typeId = tupdesc->attrs[i].atttypid;
        std::string columnName = tupdesc->attrs[i].attname.data;
        if (tupdesc->attrs[i].attisdropped)
        {
            batchField.push_back(nullptr);
            continue;
        }
        switch (typeId)
        {
            case BOOLOID: {
                columnBatch<bool> *val = (columnBatch<bool>*)palloc(sizeof(columnBatch<bool>));
                val->buffer = (bool*)palloc(sizeof(bool) * BATCH_WRITE_SIZE);
                val->length = (int64_t*)palloc0(sizeof(int64_t) * BATCH_WRITE_SIZE);
                val->notNull = (bool*)palloc0(sizeof(bool) * BATCH_WRITE_SIZE);
                batchField.push_back(val);
                break;

            }
            case INT2OID:
            case INT4OID: {
                columnBatch<int32_t> *val = (columnBatch<int32_t>*)palloc(sizeof(columnBatch<int32_t>));
                val->buffer = (int32_t*)palloc(sizeof(int32_t) * BATCH_WRITE_SIZE);
                val->length = (int64_t*)palloc0(sizeof(int64_t) * BATCH_WRITE_SIZE);
                val->notNull = (bool*)palloc0(sizeof(bool) * BATCH_WRITE_SIZE);
                batchField.push_back(val);
                break;
            }
            case INT8OID: {
                columnBatch<int64_t> *val = (columnBatch<int64_t>*)palloc(sizeof(columnBatch<int64_t>));
                val->buffer = (int64_t*)palloc(sizeof(int64_t) * BATCH_WRITE_SIZE);
                val->length = (int64_t*)palloc0(sizeof(int64_t) * BATCH_WRITE_SIZE);
                val->notNull = (bool*)palloc0(sizeof(bool) * BATCH_WRITE_SIZE);
                batchField.push_back(val);
                break;
            }
            case FLOAT4OID: {
                columnBatch<float> *val = (columnBatch<float>*)palloc(sizeof(columnBatch<float>));
                val->buffer = (float*)palloc(sizeof(float) * BATCH_WRITE_SIZE);
                val->length = (int64_t*)palloc0(sizeof(int64_t) * BATCH_WRITE_SIZE);
                val->notNull = (bool*)palloc0(sizeof(bool) * BATCH_WRITE_SIZE);
                batchField.push_back(val);
                break;
            }
            case FLOAT8OID: {
                columnBatch<double> *val = (columnBatch<double>*)palloc(sizeof(columnBatch<double>));
                val->buffer = (double*)palloc(sizeof(double) * BATCH_WRITE_SIZE);
                val->length = (int64_t*)palloc0(sizeof(int64_t) * BATCH_WRITE_SIZE);
                val->notNull = (bool*)palloc0(sizeof(bool) * BATCH_WRITE_SIZE);
                batchField.push_back(val);
                break;
            }
            case DATEOID: {
                columnBatch<int32_t> *val = (columnBatch<int32_t>*)palloc(sizeof(columnBatch<int32_t>));
                val->buffer = (int32_t*)palloc(sizeof(int32_t) * BATCH_WRITE_SIZE);
                val->length = (int64_t*)palloc0(sizeof(int64_t) * BATCH_WRITE_SIZE);
                val->notNull = (bool*)palloc0(sizeof(bool) * BATCH_WRITE_SIZE);
                batchField.push_back(val);
                break;
            }
            case TIMESTAMPOID:
            case TIMESTAMPTZOID: {
                columnBatch<int64_t> *val = (columnBatch<int64_t>*)palloc(sizeof(columnBatch<int64_t>));
                val->buffer = (int64_t*)palloc(sizeof(int64_t) * BATCH_WRITE_SIZE);
                val->length = (int64_t*)palloc0(sizeof(int64_t) * BATCH_WRITE_SIZE);
                val->notNull = (bool*)palloc0(sizeof(bool) * BATCH_WRITE_SIZE);
                batchField.push_back(val);
                break;
            }
            case NUMERICOID: {
                StringVectorBatch *val = (StringVectorBatch*)palloc(sizeof(StringVectorBatch));
                val->buffer = (char**)palloc(sizeof(char*) * BATCH_WRITE_SIZE);
                val->length = (int64_t*)palloc0(sizeof(int64_t) * BATCH_WRITE_SIZE);
                val->precision = (int64_t*)palloc0(sizeof(int64_t) * BATCH_WRITE_SIZE);
                val->scale = (int64_t*)palloc0(sizeof(int64_t) * BATCH_WRITE_SIZE);
                val->notNull = (bool*)palloc0(sizeof(bool) * BATCH_WRITE_SIZE);
                batchField.push_back(val);
                break;
            }
            case CHAROID:
            case BPCHAROID:
            case VARCHAROID:
            case BYTEAOID:
            case TEXTOID:
            case CSTRINGOID:
            case INTERVALOID:
            case TIMEOID: {
                StringVectorBatch *val = (StringVectorBatch*)palloc(sizeof(StringVectorBatch));
                val->buffer = (char**)palloc(sizeof(char*) * BATCH_WRITE_SIZE);
                val->length = (int64_t*)palloc0(sizeof(int64_t) * BATCH_WRITE_SIZE);
                val->notNull = (bool*)palloc0(sizeof(bool) * BATCH_WRITE_SIZE);
                batchField.push_back(val);
                break;
            }
            default:
                elog(ERROR, "Datalake foreign table Type Mismatch: MPP type %s not define in parquet type %s. type mapping %s",
                    tupdesc->attrs[i].attname.data,
                    getColTypeName(typeId).data(),
                    getTypeMappingSupported().data());
                break;
        }
    }
}

std::shared_ptr<parquet::schema::GroupNode> parquetFileWriter::setupSchema()
{
    parquet::schema::NodeVector fields;
    for (int i = 0; i < ncolumns; i++)
    {
        Oid typeId = tupdesc->attrs[i].atttypid;
        std::string columnName = tupdesc->attrs[i].attname.data;
        if (tupdesc->attrs[i].attisdropped)
            continue;
        switch (typeId)
        {
            case BOOLOID: {
                fields.push_back(::parquet::schema::PrimitiveNode::Make(columnName.c_str(),
                    ::parquet::Repetition::OPTIONAL, ::parquet::Type::BOOLEAN, ::parquet::ConvertedType::NONE, -1, -1, -1, i + 1));
                break;
            }
            case INT2OID:
            case INT4OID: {
                fields.push_back(::parquet::schema::PrimitiveNode::Make(columnName.c_str(),
                    ::parquet::Repetition::OPTIONAL, ::parquet::Type::INT32, ::parquet::ConvertedType::NONE, -1, -1, -1, i + 1));
                break;
            }
            case INT8OID: {
                fields.push_back(::parquet::schema::PrimitiveNode::Make(columnName.c_str(),
                    ::parquet::Repetition::OPTIONAL, ::parquet::Type::INT64, ::parquet::ConvertedType::NONE, -1, -1, -1, i + 1));
                break;
            }
            case FLOAT4OID: {
                fields.push_back(::parquet::schema::PrimitiveNode::Make(columnName.c_str(),
                    ::parquet::Repetition::OPTIONAL, ::parquet::Type::FLOAT, ::parquet::ConvertedType::NONE, -1, -1, -1, i + 1));
                break;
            }
            case FLOAT8OID: {
                fields.push_back(::parquet::schema::PrimitiveNode::Make(columnName.c_str(),
                    ::parquet::Repetition::OPTIONAL, ::parquet::Type::DOUBLE, ::parquet::ConvertedType::NONE, -1, -1, -1, i + 1));
                break;
            }
            case DATEOID: {
                fields.push_back(::parquet::schema::PrimitiveNode::Make(columnName.c_str(),
                    ::parquet::Repetition::OPTIONAL, ::parquet::LogicalType::Date(), ::parquet::Type::INT32, -1, i + 1));
                break;
            }
            case TIMESTAMPOID:
            case TIMESTAMPTZOID: {
                /*
                 * Iceberg spec requires INT64 microseconds since Unix epoch.
                 * Use adjustedToUTC=true for Iceberg compatibility with
                 * Spark/Trino/Flink.
                 */
                fields.push_back(::parquet::schema::PrimitiveNode::Make(columnName.c_str(),
                    ::parquet::Repetition::OPTIONAL,
                    ::parquet::LogicalType::Timestamp(true, ::parquet::LogicalType::TimeUnit::MICROS),
                    ::parquet::Type::INT64, -1, i + 1));
                break;
            }
            case NUMERICOID: {
                int32_t precision = 0;
                int32_t scale = 0;
                if (tupdesc->attrs[i].atttypmod < 0)
                {
                    ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("The precision of numeric in foreign tables with parquet format should be specified explicitly.")));
                }
                else
                {
                    precision = ((tupdesc->attrs[i].atttypmod - VARHDRSZ) >> 16) & 0xffff;
                    scale = (tupdesc->attrs[i].atttypmod - VARHDRSZ) & 0xffff;
                }
                fields.push_back(::parquet::schema::PrimitiveNode::Make(columnName.c_str(), ::parquet::Repetition::OPTIONAL,
                    ::parquet::Type::FIXED_LEN_BYTE_ARRAY, ::parquet::ConvertedType::DECIMAL, ::parquet_arrow::DecimalType::DecimalSize(precision), precision, scale, i + 1));
                break;
            }
            case CHAROID: {
                fields.push_back(::parquet::schema::PrimitiveNode::Make(columnName.c_str(),
                    ::parquet::Repetition::OPTIONAL, ::parquet::Type::BYTE_ARRAY, ::parquet::ConvertedType::NONE, -1, -1, -1, i + 1));
                break;
            }
            case BPCHAROID: {
                /*
                 * CHAR(N) is mapped to Iceberg string (= Parquet variable-length
                 * BYTE_ARRAY with UTF8 logical type), aligning with VARCHAR /
                 * TEXT and matching Snowflake / Spark / Trino / PrestoDB
                 * semantics.  PG CHAR trailing-space padding is not preserved
                 * on disk.
                 */
                fields.push_back(::parquet::schema::PrimitiveNode::Make(columnName.c_str(),
                    ::parquet::Repetition::OPTIONAL, ::parquet::Type::BYTE_ARRAY, ::parquet::ConvertedType::UTF8, -1, -1, -1, i + 1));
                break;
            }
            case VARCHAROID: {
                fields.push_back(::parquet::schema::PrimitiveNode::Make(columnName.c_str(),
                    ::parquet::Repetition::OPTIONAL, ::parquet::Type::BYTE_ARRAY, ::parquet::ConvertedType::UTF8, -1, -1, -1, i + 1));
                break;
            }
            case BYTEAOID: {
                fields.push_back(::parquet::schema::PrimitiveNode::Make(columnName.c_str(),
                    ::parquet::Repetition::OPTIONAL, ::parquet::Type::BYTE_ARRAY, ::parquet::ConvertedType::NONE, -1, -1, -1, i + 1));
                break;
            }
            case TEXTOID: {
                fields.push_back(::parquet::schema::PrimitiveNode::Make(columnName.c_str(),
                    ::parquet::Repetition::OPTIONAL, ::parquet::Type::BYTE_ARRAY, ::parquet::ConvertedType::UTF8, -1, -1, -1, i + 1));
                break;
            }
            case INTERVALOID: {
                fields.push_back(::parquet::schema::PrimitiveNode::Make(columnName.c_str(),
                    ::parquet::Repetition::OPTIONAL, ::parquet::Type::BYTE_ARRAY, ::parquet::ConvertedType::NONE, -1, -1, -1, i + 1));
                break;
            }
            case TIMEOID: {
                fields.push_back(::parquet::schema::PrimitiveNode::Make(columnName.c_str(),
                    ::parquet::Repetition::OPTIONAL, ::parquet::Type::BYTE_ARRAY, ::parquet::ConvertedType::NONE, -1, -1, -1, i + 1));
                break;
            }
            default:
                elog(ERROR, "Datalake foreign table Type Mismatch: MPP type %s not define in parquet type %s. type mapping %s",
                    tupdesc->attrs[i].attname.data,
                    getColTypeName(typeId).data(),
                    getTypeMappingSupported().data());
                break;
        }
    }
    return std::static_pointer_cast<::parquet::schema::GroupNode>(
        ::parquet::schema::GroupNode::Make("schema", ::parquet::Repetition::OPTIONAL, fields));
}

void parquetFileWriter::writeProperties()
{
    parquet::WriterProperties::Builder builder;
    parquet_arrow::Compression::type codec_type = parquet_arrow::Compression::UNCOMPRESSED;
    switch (option.compression) {
        case UNCOMPRESS:{
            codec_type = parquet_arrow::Compression::UNCOMPRESSED;
            break;
        }
        case SNAPPY: {
            codec_type = parquet_arrow::Compression::SNAPPY;
            break;
        }
        case GZIP: {
            codec_type = parquet_arrow::Compression::GZIP;
            break;
        }
        case BROTLI: {
            codec_type = parquet_arrow::Compression::BROTLI;
            break;
        }
        case ZSTD: {
            codec_type = parquet_arrow::Compression::ZSTD;
            break;
        }
        case LZ4: {
            codec_type = parquet_arrow::Compression::LZ4;
            break;
        }
        default:
            codec_type = parquet_arrow::Compression::UNCOMPRESSED;
            break;
    }

    builder.compression(codec_type);
    builder.enable_statistics();
    builder.created_by("Hashdata");
    builder.data_pagesize(1024*1024);
    builder.enable_dictionary();
    props = builder.build();
}

void parquetFileWriter::resetParquetWriter()
{
    if (batchNum > 0)
    {
        writeToBatch(batchNum);
        batchNum = 0;
        estimated_bytes = 0;
    }
    rg_writer->Close();
    file_writer->Close();
    out_file->Close();
    dataBufferOffset = 0;
    openState = false;
}

void parquetFileWriter::closeParquetWriter()
{
    resetParquetWriter();
}


void parquetFileWriter::resizeDataBuff(int count, std::vector<char>& buffer, int64_t datalen, int offset)
{
    char* oldBufferAddress = buffer.data();
    bool isResize = false;
    int64_t oldSize = buffer.size();
    int64_t newSize = oldSize;
    while (newSize - offset < datalen) {
        newSize = newSize * 2;
        isResize = true;
    }
    if (isResize)
    {
        buffer.resize(newSize);
        char* newBufferAddress = buffer.data();
        reDistributedDataBuffer(count, oldBufferAddress, newBufferAddress);
    }
}

bool parquetFileWriter::columnBelongStringType(int attColumn)
{
    switch (attColumn)
    {
        case CHAROID:
        case BPCHAROID:
        case VARCHAROID:
        case BYTEAOID:
        case TEXTOID:
        case INTERVALOID:
        case TIMEOID:
        case NUMERICOID:
        {
            return true;
        }
        default:
            break;
    }
    return false;
}

void parquetFileWriter::reDistributedDataBuffer(int count, char* oldBufferAddress, char* newBufferAddress)
{
    for (int row = 0; row < count + 1; row++)
    {
        for (int i = 0; i < ncolumns; i++)
        {
            if (columnBelongStringType(tupdesc->attrs[i].atttypid))
            {
                StringVectorBatch* val = reinterpret_cast<StringVectorBatch*>(batchField[i]);
                bool notNull = val->notNull[row];
                if (notNull)
                {
                    val->buffer[row] = val->buffer[row] - oldBufferAddress + newBufferAddress;
                }
            }
        }
    }
}

int64_t parquetFileWriter::write(const void* buf, size_t length)
{
    writeToField(batchNum, buf);
    batchNum++;
    if (batchNum >= BATCH_WRITE_SIZE)
    {
        writeToBatch(batchNum);
        batchNum = 0;
        dataBufferOffset = 0;
        estimated_bytes = 0;
    }
    return length;
}

void parquetFileWriter::init(void *sstate, writeOption option)
{
    dataLakeFdwScanState *ss = (dataLakeFdwScanState*)sstate;

    Relation relation = ss->rel;
    if (relation == NULL) {
        elog(ERROR, "Parquet get Relation failed\n");
    }
    estimated_bytes = 0;
    ncolumns = relation->rd_att->natts;
    tupdesc = relation->rd_att;
    this->option = option;
    createColumnBatch();
    schema = setupSchema();
    writeProperties();
}

void parquetFileWriter::init(TupleDesc tupdesc, std::shared_ptr<parquet::schema::GroupNode> schema, writeOption option)
{
    if (tupdesc == NULL) {
        elog(ERROR, "Parquet get Relation failed\n");
    }
    estimated_bytes = 0;
    ncolumns = tupdesc->natts;
    this->tupdesc = tupdesc;
    this->option = option;
    createColumnBatch();
    this->schema = schema;
    writeProperties();
}

void parquetFileWriter::destroy()
{
    schema.reset();
    props.reset();
    if (byteArray != NULL)
    {
        pfree(byteArray);
        byteArray = NULL;
    }
    if (fixByteArray != NULL)
    {
        pfree(fixByteArray);
        fixByteArray = NULL;
    }
    if (definition_level != NULL)
    {
        pfree(definition_level);
        definition_level = NULL;
    }
    if (int96Array != NULL)
    {
        pfree(int96Array);
        int96Array = NULL;
    }
}

bool parquetFileWriter::isOpen()
{
	return openState;
}

int64_t parquetFileWriter::getWrittenBytes()
{
    return out_file->getBytesWritten();
}

void parquetFileWriter::writeToField(int index, const void* data)
{
	TupleTableSlot* slot = (TupleTableSlot*)data;
    for (int i = 0; i < ncolumns; i++)
    {
        Oid typeID = tupdesc->attrs[i].atttypid;
		Datum tts_values = slot->tts_values[i];
		bool isNULL = slot->tts_isnull[i];

        if (tupdesc->attrs[i].attisdropped)
            continue;
        switch (tupdesc->attrs[i].atttypid) {
            case BOOLOID: {
                columnBatch<bool> * val = reinterpret_cast<columnBatch<bool>*>(batchField[i]);
                if (!isNULL)
                {
                    val->buffer[index] = DatumGetBool(tts_values);
                    val->notNull[index] = true;
                    val->num = index;
                    estimated_bytes += sizeof(bool);
                }
                else
                {
                    val->notNull[index] = false;
                }
                break;
            }
            case INT2OID:
            case INT4OID: {
                columnBatch<int32_t> * val = reinterpret_cast<columnBatch<int32_t>*>(batchField[i]);
                if (!isNULL)
                {
                    val->buffer[index] = DatumGetInt32(tts_values);
                    val->notNull[index] = true;
                    val->num = index;
                    estimated_bytes += sizeof(int32_t);
                }
                else
                {
                    val->notNull[index] = false;
                }
                break;
            }
            case INT8OID: {
                columnBatch<int64_t> * val = reinterpret_cast<columnBatch<int64_t>*>(batchField[i]);
                if (!isNULL)
                {
                    val->buffer[index] = DatumGetInt64(tts_values);
                    val->notNull[index] = true;
                    val->num = index;
                    estimated_bytes += sizeof(int64_t);
                }
                else
                {
                    val->notNull[index] = false;
                }
                break;
            }
            case FLOAT4OID: {
                columnBatch<float> * val = reinterpret_cast<columnBatch<float>*>(batchField[i]);
                if (!isNULL)
                {
                    val->buffer[index] = DatumGetFloat4(tts_values);
                    val->notNull[index] = true;
                    val->num = index;
                    estimated_bytes += sizeof(float);
                }
                else
                {
                    val->notNull[index] = false;
                }
                break;
            }
            case FLOAT8OID: {
                columnBatch<double> * val = reinterpret_cast<columnBatch<double>*>(batchField[i]);
                if (!isNULL)
                {
                    val->buffer[index] = DatumGetFloat8(tts_values);
                    val->notNull[index] = true;
                    val->num = index;
                    estimated_bytes += sizeof(double);
                }
                else
                {
                    val->notNull[index] = false;
                }
                break;
            }
            case DATEOID: {
                columnBatch<int32_t> * val = reinterpret_cast<columnBatch<int32_t>*>(batchField[i]);
                if (!isNULL)
                {
                    val->buffer[index] = DatumGetDateADT(tts_values);
                    val->notNull[index] = true;
                    val->num = index;
                    estimated_bytes += sizeof(int32_t);
                }
                else
                {
                    val->notNull[index] = false;
                }
                break;
            }
            case TIMESTAMPOID:
            case TIMESTAMPTZOID: {
                columnBatch<int64_t> * val = reinterpret_cast<columnBatch<int64_t>*>(batchField[i]);
                if (!isNULL)
                {
                    val->buffer[index] = DatumGetTimestamp(tts_values);
                    val->notNull[index] = true;
                    val->num = index;
                    estimated_bytes += sizeof(int64_t);
                }
                else
                {
                    val->notNull[index] = false;
                }
                break;
            }
            case NUMERICOID: {
                StringVectorBatch* val = reinterpret_cast<StringVectorBatch*>(batchField[i]);
                if (!isNULL)
                {
                    int32_t precision = ((tupdesc->attrs[i].atttypmod - VARHDRSZ) >> 16) & 0xffff;
                    int32_t scale = (tupdesc->attrs[i].atttypmod - VARHDRSZ) & 0xffff;
                    char data[16];
                    numeric_to_FLBA(DatumGetNumeric(tts_values), data);
                    int32_t datalen = ::parquet_arrow::DecimalType::DecimalSize(precision);
                    resizeDataBuff(index, dataBuffer, datalen, dataBufferOffset);
                    memcpy(dataBuffer.data() + dataBufferOffset, data + 16 - datalen, datalen);
                    val->buffer[index] = dataBuffer.data() + dataBufferOffset;
                    val->length[index] = datalen;
                    val->notNull[index] = true;
                    val->precision[index] = precision;
                    val->scale[index] = scale;
                    val->num = index;
                    dataBufferOffset += datalen;
                    estimated_bytes += datalen;
                }
                else
                {
                    val->notNull[index] = false;
                }
                break;
            }
            case CHAROID:
            {
                StringVectorBatch* val = reinterpret_cast<StringVectorBatch*>(batchField[i]);
                if (!isNULL)
                {
                    char *data = DatumGetCString(DirectFunctionCall1(bpcharout, tts_values));
                    int64_t textlen = static_cast<int64_t> (strlen(data));
                    int64_t datalen = 1;
                    resizeDataBuff(index, dataBuffer, datalen, dataBufferOffset);
                    memset(dataBuffer.data() + dataBufferOffset, ' ', datalen);
                    memcpy(dataBuffer.data() + dataBufferOffset, data, textlen);
                    val->buffer[index] = dataBuffer.data() + dataBufferOffset;
                    val->length[index] = datalen;
                    val->notNull[index] = true;
                    val->num = index;
                    dataBufferOffset += datalen;
                    if (data != NULL)
                    {
                        pfree(data);
                    }
                    estimated_bytes += datalen;
                }
                else
                {
                    val->notNull[index] = false;
                }
                break;
            }
            case BPCHAROID:
            {
                /*
                 * Write CHAR(N) as variable-length UTF-8 (Iceberg string
                 * semantics).  bpcharout returns the full padded storage,
                 * so we explicitly strip trailing spaces before writing —
                 * matching Snowflake / Spark / Trino / PrestoDB so external
                 * engines see real string values, not space-padded bytes.
                 */
                StringVectorBatch* val = reinterpret_cast<StringVectorBatch*>(batchField[i]);
                if (!isNULL)
                {
                    char *data = DatumGetCString(DirectFunctionCall1(bpcharout, tts_values));
                    int64_t datalen = static_cast<int64_t> (strlen(data));
                    while (datalen > 0 && data[datalen - 1] == ' ')
                        datalen--;
                    resizeDataBuff(index, dataBuffer, datalen, dataBufferOffset);
                    memcpy(dataBuffer.data() + dataBufferOffset, data, datalen);
                    val->buffer[index] = dataBuffer.data() + dataBufferOffset;
                    val->length[index] = datalen;
                    val->notNull[index] = true;
                    val->num = index;
                    dataBufferOffset += datalen;
                    if (data != NULL)
                    {
                        pfree(data);
                    }
                    estimated_bytes += datalen;
                }
                else
                {
                    val->notNull[index] = false;
                }
                break;
            }
            case VARCHAROID:
            case TEXTOID: {
                StringVectorBatch* val = reinterpret_cast<StringVectorBatch*>(batchField[i]);
                if (!isNULL)
                {
                    char *data = DatumGetCString(DirectFunctionCall1(textout, tts_values));
                    int64_t datalen = static_cast<int64_t> (strlen(data));
                    resizeDataBuff(index, dataBuffer, datalen, dataBufferOffset);
                    memcpy(dataBuffer.data() + dataBufferOffset, data, datalen);

                    val->buffer[index] = dataBuffer.data() + dataBufferOffset;
                    val->length[index] = datalen;
                    val->notNull[index] = true;
                    val->num = index;
                    dataBufferOffset += datalen;
                    if (data != NULL)
                    {
                        pfree(data);
                    }
                    estimated_bytes += datalen;
                }
                else
                {
                    val->notNull[index] = false;
                }
                break;
            }
            case BYTEAOID: {
                /*
                 * bytea must be written as raw bytes, including embedded NULs.
                 * The earlier shared path with text/varchar called byteaout via
                 * textout and then strlen() on the result, which truncated at
                 * the first 0x00 byte (issue #328).  Detoast and copy
                 * length-prefixed bytes directly out of the varlena.
                 */
                StringVectorBatch* val = reinterpret_cast<StringVectorBatch*>(batchField[i]);
                if (!isNULL)
                {
                    bytea   *b      = DatumGetByteaPP(tts_values);
                    char    *data   = VARDATA_ANY(b);
                    int64_t  datalen = (int64_t) VARSIZE_ANY_EXHDR(b);
                    resizeDataBuff(index, dataBuffer, datalen, dataBufferOffset);
                    memcpy(dataBuffer.data() + dataBufferOffset, data, datalen);

                    val->buffer[index] = dataBuffer.data() + dataBufferOffset;
                    val->length[index] = datalen;
                    val->notNull[index] = true;
                    val->num = index;
                    dataBufferOffset += datalen;
                    estimated_bytes += datalen;
                }
                else
                {
                    val->notNull[index] = false;
                }
                break;
            }
            case CSTRINGOID: {
                StringVectorBatch* val = reinterpret_cast<StringVectorBatch*>(batchField[i]);
                if (!isNULL)
                {
                    const char *data = DatumGetCString(tts_values);
                    int64_t datalen = static_cast<int64_t> (strlen(data));
                    resizeDataBuff(index, dataBuffer, datalen, dataBufferOffset);
                    memcpy(dataBuffer.data() + dataBufferOffset, data, datalen);

                    val->buffer[index] = dataBuffer.data() + dataBufferOffset;
                    val->length[index] = datalen;
                    val->notNull[index] = true;
                    val->num = index;
                    dataBufferOffset += datalen;
                    estimated_bytes += datalen;
                }
                else
                {
                    val->notNull[index] = false;
                }
                break;
            }
            case INTERVALOID: {
                StringVectorBatch* val = reinterpret_cast<StringVectorBatch*>(batchField[i]);
                if (!isNULL)
                {
                    char *data = DatumGetCString(DirectFunctionCall1(interval_out, tts_values));
                    int64_t datalen = static_cast<int64_t> (strlen(data));
                    resizeDataBuff(index, dataBuffer, datalen, dataBufferOffset);
                    memcpy(dataBuffer.data() + dataBufferOffset, data, datalen);

                    val->buffer[index] = dataBuffer.data() + dataBufferOffset;
                    val->length[index] = datalen;
                    val->notNull[index] = true;
                    val->num = index;
                    dataBufferOffset += datalen;
                    if (data != NULL)
                    {
                        pfree(data);
                    }
                    estimated_bytes += datalen;
                }
                else
                {
                    val->notNull[index] = false;
                }
                break;
            }
            case TIMEOID: {
                StringVectorBatch* val = reinterpret_cast<StringVectorBatch*>(batchField[i]);
                if (!isNULL)
                {
                    char *data = DatumGetCString(DirectFunctionCall1(time_out, DatumGetTimeADT(tts_values)));
                    int64_t datalen = static_cast<int64_t> (strlen(data));
                    resizeDataBuff(index, dataBuffer, datalen, dataBufferOffset);
                    memcpy(dataBuffer.data() + dataBufferOffset, data, datalen);

                    val->buffer[index] = dataBuffer.data() + dataBufferOffset;
                    val->length[index] = datalen;
                    val->notNull[index] = true;
                    val->num = index;
                    dataBufferOffset += datalen;
                    if (data != NULL)
                    {
                        pfree(data);
                    }
                    estimated_bytes += datalen;
                }
                else
                {
                    val->notNull[index] = false;
                }
                break;
            }
            default:
                std::string columnName = tupdesc->attrs[i].attname.data;
                elog(ERROR,
                    "Type Mismatch: data in %s is as define %s in datalake foreign table, but in parquet not define %s",
                    columnName.c_str(),
                    getColTypeName(typeID).data(),
                    getTypeMappingSupported().data());
                break;
        }
    }
}

void parquetFileWriter::writeToBatch(int rows)
{
    if (rg_writer && rg_writer->num_rows() >= props->max_row_group_length())
    {
        rg_writer->Close();
        rg_writer = file_writer->AppendBufferedRowGroup();
    }
    int parquetCol = 0;
    for (int i = 0; i < ncolumns; i++)
    {
        Oid typeID = tupdesc->attrs[i].atttypid;
        if (tupdesc->attrs[i].attisdropped)
            continue;
        int col = parquetCol++;
        switch (tupdesc->attrs[i].atttypid) {
            case BOOLOID: {
                columnBatch<bool> * val = reinterpret_cast<columnBatch<bool>*>(batchField[i]);
                parquet::BoolWriter* writer = static_cast<parquet::BoolWriter*>(rg_writer->column(col));
                std::vector<uint8_t> valid_bits(parquet_arrow::bit_util::BytesForBits(BATCH_WRITE_SIZE), 255);
                for (int row = 0; row < rows; row++)
                {
                    bool notNull = val->notNull[row];
                    if (notNull)
                    {
                        definition_level[row] = 1;
                    }
                    else
                    {
                        definition_level[row] = 0;
                        parquet_arrow::bit_util::ClearBit(valid_bits.data(), row);
                    }
                }
                writer->WriteBatchSpaced(rows, definition_level, nullptr, valid_bits.data(), 0, val->buffer);
                break;
            }
            case INT2OID:
            case INT4OID: {
                columnBatch<int32_t> * val = reinterpret_cast<columnBatch<int32_t>*>(batchField[i]);
                parquet::Int32Writer* writer = static_cast<parquet::Int32Writer*>(rg_writer->column(col));
                std::vector<uint8_t> valid_bits(parquet_arrow::bit_util::BytesForBits(BATCH_WRITE_SIZE), 255);
                for (int row = 0; row < rows; row++)
                {
                    bool notNull = val->notNull[row];
                    if (notNull)
                    {
                        definition_level[row] = 1;
                    }
                    else
                    {
                        definition_level[row] = 0;
                        parquet_arrow::bit_util::ClearBit(valid_bits.data(), row);
                    }
                }
                writer->WriteBatchSpaced(rows, definition_level, nullptr, valid_bits.data(), 0, val->buffer);
                break;
            }
            case INT8OID: {
                columnBatch<int64_t> * val = reinterpret_cast<columnBatch<int64_t>*>(batchField[i]);
                parquet::Int64Writer* writer = static_cast<parquet::Int64Writer*>(rg_writer->column(col));
                std::vector<uint8_t> valid_bits(parquet_arrow::bit_util::BytesForBits(BATCH_WRITE_SIZE), 255);
                for (int row = 0; row < rows; row++)
                {
                    bool notNull = val->notNull[row];
                    if (notNull)
                    {
                        definition_level[row] = 1;
                    }
                    else
                    {
                        definition_level[row] = 0;
                        parquet_arrow::bit_util::ClearBit(valid_bits.data(), row);
                    }
                }
                writer->WriteBatchSpaced(rows, definition_level, nullptr, valid_bits.data(), 0, val->buffer);
                break;
            }
            case FLOAT4OID: {
                columnBatch<float> * val = reinterpret_cast<columnBatch<float>*>(batchField[i]);
                parquet::FloatWriter* writer = static_cast<parquet::FloatWriter*>(rg_writer->column(col));
                std::vector<uint8_t> valid_bits(parquet_arrow::bit_util::BytesForBits(BATCH_WRITE_SIZE), 255);
                for (int row = 0; row < rows; row++)
                {
                    bool notNull = val->notNull[row];
                    if (notNull)
                    {
                        definition_level[row] = 1;
                    }
                    else
                    {
                        definition_level[row] = 0;
                        parquet_arrow::bit_util::ClearBit(valid_bits.data(), row);
                    }
                }
                writer->WriteBatchSpaced(rows, definition_level, nullptr, valid_bits.data(), 0, val->buffer);
                break;
            }
            case FLOAT8OID: {
                columnBatch<double> * val = reinterpret_cast<columnBatch<double>*>(batchField[i]);
                parquet::DoubleWriter* writer = static_cast<parquet::DoubleWriter*>(rg_writer->column(col));
                std::vector<uint8_t> valid_bits(parquet_arrow::bit_util::BytesForBits(BATCH_WRITE_SIZE), 255);
                for (int row = 0; row < rows; row++)
                {
                    bool notNull = val->notNull[row];
                    if (notNull)
                    {
                        definition_level[row] = 1;
                    }
                    else
                    {
                        definition_level[row] = 0;
                        parquet_arrow::bit_util::ClearBit(valid_bits.data(), row);
                    }
                }
                writer->WriteBatchSpaced(rows, definition_level, nullptr, valid_bits.data(), 0, val->buffer);
                break;
            }
            case DATEOID: {
                columnBatch<int32_t> * val = reinterpret_cast<columnBatch<int32_t>*>(batchField[i]);
                parquet::Int32Writer* writer = static_cast<parquet::Int32Writer*>(rg_writer->column(col));
                std::vector<uint8_t> valid_bits(parquet_arrow::bit_util::BytesForBits(BATCH_WRITE_SIZE), 255);
                for (int row = 0; row < rows; row++)
                {
                    bool notNull = val->notNull[row];
                    if (notNull)
                    {
                        int32_t value = val->buffer[row] + (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE);
                        val->buffer[row] = value;
                        definition_level[row] = 1;
                    }
                    else
                    {
                        definition_level[row] = 0;
                        parquet_arrow::bit_util::ClearBit(valid_bits.data(), row);
                    }
                }
                writer->WriteBatchSpaced(rows, definition_level, nullptr, valid_bits.data(), 0, val->buffer);
                break;
            }
            case TIMESTAMPOID:
            case TIMESTAMPTZOID: {
                /*
                 * Write timestamp/timestamptz as INT64 microseconds since Unix epoch.
                 * PG stores both as microseconds since PG epoch (2000-01-01); for
                 * timestamptz the stored value is already UTC, which matches the
                 * Iceberg Timestamp(adjustedToUTC=true) logical type set up in the
                 * Parquet schema.
                 */
                static const int64_t UNIX_TO_PG_EPOCH_USECS =
                    ((int64_t)(POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE)) * SECS_PER_DAY * USECS_PER_SEC;

                columnBatch<int64_t> * val = reinterpret_cast<columnBatch<int64_t>*>(batchField[i]);
                parquet::Int64Writer* writer = static_cast<parquet::Int64Writer*>(rg_writer->column(col));
                std::vector<uint8_t> valid_bits(parquet_arrow::bit_util::BytesForBits(BATCH_WRITE_SIZE), 255);
                for (int row = 0; row < rows; row++)
                {
                    bool notNull = val->notNull[row];
                    if (notNull)
                    {
                        int64_t pgTimestamp = val->buffer[row];
                        int64Array[row] = pgTimestamp + UNIX_TO_PG_EPOCH_USECS;
                        definition_level[row] = 1;
                    }
                    else
                    {
                        definition_level[row] = 0;
                        parquet_arrow::bit_util::ClearBit(valid_bits.data(), row);
                    }
                }
                writer->WriteBatchSpaced(rows, definition_level, nullptr, valid_bits.data(), 0, int64Array);
                break;
            }
            case NUMERICOID: {
                StringVectorBatch* val = reinterpret_cast<StringVectorBatch*>(batchField[i]);
                parquet::FixedLenByteArrayWriter* writer = static_cast<parquet::FixedLenByteArrayWriter*>(rg_writer->column(col));
                std::vector<uint8_t> valid_bits(parquet_arrow::bit_util::BytesForBits(BATCH_WRITE_SIZE), 255);
                for (int row = 0; row < rows; row++)
                {
                    bool notNull = val->notNull[row];
                    if (notNull)
                    {
                        fixByteArray[row].ptr = reinterpret_cast<const uint8_t*>(val->buffer[row]);
                        definition_level[row] = 1;
                    }
                    else
                    {
                        definition_level[row] = 0;
                        parquet_arrow::bit_util::ClearBit(valid_bits.data(), row);
                    }
                }
                writer->WriteBatchSpaced(rows, definition_level, nullptr, valid_bits.data(), 0, fixByteArray);
                break;
            }
            case CHAROID: {
                StringVectorBatch* val = reinterpret_cast<StringVectorBatch*>(batchField[i]);
                parquet::FixedLenByteArrayWriter* writer = static_cast<parquet::FixedLenByteArrayWriter*>(rg_writer->column(col));
                std::vector<uint8_t> valid_bits(parquet_arrow::bit_util::BytesForBits(BATCH_WRITE_SIZE), 255);
                for (int row = 0; row < rows; row++)
                {
                    bool notNull = val->notNull[row];
                    if (notNull)
                    {
                        fixByteArray[row].ptr = reinterpret_cast<const uint8_t*>(val->buffer[row]);
                        definition_level[row] = 1;
                    }
                    else
                    {
                        definition_level[row] = 0;
                        parquet_arrow::bit_util::ClearBit(valid_bits.data(), row);
                    }
                }
                writer->WriteBatchSpaced(rows, definition_level, nullptr, valid_bits.data(), 0, fixByteArray);
                break;
            }
            case BPCHAROID:
            case VARCHAROID:
            case BYTEAOID:
            case CSTRINGOID:
            case TEXTOID: {
                StringVectorBatch* val = reinterpret_cast<StringVectorBatch*>(batchField[i]);
                parquet::ByteArrayWriter* writer = static_cast<parquet::ByteArrayWriter*>(rg_writer->column(col));
                std::vector<uint8_t> valid_bits(parquet_arrow::bit_util::BytesForBits(BATCH_WRITE_SIZE), 255);
                for (int row = 0; row < rows; row++)
                {
                    bool notNull = val->notNull[row];
                    if (notNull)
                    {
                        byteArray[row].ptr = reinterpret_cast<const uint8_t*>(val->buffer[row]);
                        byteArray[row].len = val->length[row];
                        definition_level[row] = 1;
                    }
                    else
                    {
                        definition_level[row] = 0;
                        parquet_arrow::bit_util::ClearBit(valid_bits.data(), row);
                    }
                }
                writer->WriteBatchSpaced(rows, definition_level, nullptr, valid_bits.data(), 0, byteArray);
                break;
            }
            case INTERVALOID: {
                StringVectorBatch* val = reinterpret_cast<StringVectorBatch*>(batchField[i]);
                parquet::ByteArrayWriter* writer = static_cast<parquet::ByteArrayWriter*>(rg_writer->column(col));
                std::vector<uint8_t> valid_bits(parquet_arrow::bit_util::BytesForBits(BATCH_WRITE_SIZE), 255);
                for (int row = 0; row < rows; row++)
                {
                    bool notNull = val->notNull[row];
                    if (notNull)
                    {
                        byteArray[row].ptr = reinterpret_cast<const uint8_t*>(val->buffer[row]);
                        byteArray[row].len = val->length[row];
                        definition_level[row] = 1;
                    }
                    else
                    {
                        definition_level[row] = 0;
                        parquet_arrow::bit_util::ClearBit(valid_bits.data(), row);
                    }
                }
                writer->WriteBatchSpaced(rows, definition_level, nullptr, valid_bits.data(), 0, byteArray);
                break;
            }
            case TIMEOID: {
                StringVectorBatch* val = reinterpret_cast<StringVectorBatch*>(batchField[i]);
                parquet::ByteArrayWriter* writer = static_cast<parquet::ByteArrayWriter*>(rg_writer->column(col));
                std::vector<uint8_t> valid_bits(parquet_arrow::bit_util::BytesForBits(BATCH_WRITE_SIZE), 255);
                for (int row = 0; row < rows; row++)
                {
                    bool notNull = val->notNull[row];
                    if (notNull)
                    {
                        byteArray[row].ptr = reinterpret_cast<const uint8_t*>(val->buffer[row]);
                        byteArray[row].len = val->length[row];
                        definition_level[row] = 1;
                    }
                    else
                    {
                        definition_level[row] = 0;
                        parquet_arrow::bit_util::ClearBit(valid_bits.data(), row);
                    }
                }
                writer->WriteBatchSpaced(rows, definition_level, nullptr, valid_bits.data(), 0, byteArray);
                break;
            }
            default:
                std::string columnName = tupdesc->attrs[i].attname.data;
                elog(ERROR,
                    "Type Mismatch: data in %s is as define %s in datalake foreign table, but in parquet not define %s",
                    columnName.c_str(),
                    getColTypeName(typeID).data(),
                    getTypeMappingSupported().data());
                break;
        }
    }
}
