#include <parquet/internal/arrow/util/decimal.h>
#include <parquet/internal/arrow/result.h>
#include "parquetFileReader.h"
#include "parquetInputStream.h"
#include "src/provider/common/datalake_numeric.h"
#include "src/provider/common/common.h"
#include "src/provider/common/parquet_scanner_guard.h"


extern "C"
{
#include "utils/date.h"
#include "utils/timestamp.h"
#include "utils/builtins.h"
#include "fmgr.h"
}

namespace Datalake {
namespace Internal {

parquetFileReader::parquetFileReader()
{
    row_count = 0;
    row_offset = 0;
    name = "";
    num_row_groups = 0;
    num_columns = 0;
    parquet_reader.reset();
    row_group_reader.reset();
    scanners.clear();
    state = FILE_UNDEF;
}

// void parquetFileReader::keepScannerForPartitonTable()
// {
// 	/* Keep a scanner get data. If foreign table only select partition key.
// 	 * example:
// 	 * 1. create foreign table test(a text, b text, c text);
// 	 *    'c' is hive partition key.
// 	 * 2. If only select * from test where c='xx';
// 	 * 3. parquet will be a dead cycle. so we keep a scanner in scanners If
// 	 *    partition table.
// 	 */

// 	if (options.nPartitionKey <= 0)
// 	{
// 		/* not partiton table */
// 		return;
// 	}
// 	bool table_columns_all_null = true;
// 	int ncolumns = options.includes_columns.size() - options.nPartitionKey;
//     for (int i = 0; i < ncolumns; i++)
//     {
// 		if (options.includes_columns[i])
// 		{
// 			table_columns_all_null = false;
// 			break;
// 		}
// 	}

// 	if (table_columns_all_null)
// 	{

// 	}
// }

bool parquetFileReader::createParquetReader(ossFileStream ossFile, std::string fileName, readOption options)
{

    this->options = options;
    if (!createInternalReader(ossFile, fileName))
    {
        return false;
    }
    int ncolumns = options.includes_columns.size() - options.nPartitionKey;
    scanners.resize(ncolumns);
    return true;
}

void parquetFileReader::resetParquetReader()
{
    row_count = 0;
    row_offset = 0;
    if (state != FILE_UNDEF)
    {
        parquet_reader->Close();
		auto status = fileStream->Close();
		fileStream.reset();
    }
    resetScanners();
    file_metadata.reset();
    parquet_reader.reset();
    num_row_groups = 0;
    num_columns = 0;
}

bool parquetFileReader::createInternalReader(ossFileStream ossFile, std::string fileName)
{
	fileStream = std::make_shared<gopherReadFileSystem>(ossFile, fileName, setStreamWhetherCache(options));
    auto s = fileStream->Open();
    if (!fileStream->checkFileIsParquet())
    {
        return false;
    }
    parquet_reader = parquet::ParquetFileReader::Open(fileStream);
    file_metadata = parquet_reader->metadata();

    /*
     * Cold-path backfill, placed here for the same two reasons as the ORC
     * one: only now is the metadata length known, and by now the trailing
     * block has already been materialized by Gopher, so reading the
     * footer back is a local pread that generates no remote traffic.
     *
     * An encrypted footer is deliberately never cached. Its layout
     * differs (it ends in "PARE", which file_reader.cc checks for), and
     * stashing plaintext metadata for an encrypted file in a shared cache
     * is a security decision nobody asked us to make.
     */
    if (!fileStream->isMetaOk())
    {
        if (file_metadata->is_encryption_algorithm_set())
            elog(DEBUG1, "datalake file-meta-cache: %s has an encrypted footer, "
                         "not caching", fileName.c_str());
        else
            fileStream->backfillFooter(file_metadata->size());
    }
    num_row_groups = file_metadata->num_row_groups();
    num_columns = file_metadata->num_columns();
    name = fileName;
    int mpp_columns = options.includes_columns.size() - options.nPartitionKey;
    if (mpp_columns < num_columns)
    {
        elog(ERROR, "Datalake foreign table Error, mpp columns %d is less than parquet %s columns %d.",
            mpp_columns, fileName.c_str(), num_columns);
    }
    state = FILE_OPEN;
    return true;
}

void parquetFileReader::setRowGroup(int row_group_num)
{
    row_offset = 0;
    row_group_reader = parquet_reader->RowGroup(row_group_num);
    row_count = row_group_reader->metadata()->num_rows();
}

int64_t parquetFileReader::rowGroupOffset(int row_group_num)
{
    if (file_metadata->RowGroup(row_group_num)->ColumnChunk(0)->has_dictionary_page())
    {
        return file_metadata->RowGroup(row_group_num)->ColumnChunk(0)->dictionary_page_offset();
    }
    else
    {
        return file_metadata->RowGroup(row_group_num)->ColumnChunk(0)->data_page_offset();
    }
}

void parquetFileReader::createScanners()
{
    int index = 0;
    auto schema = file_metadata->schema();
    columnDescriptors.resize(scanners.size(), nullptr);
    for (int i = 0; i < num_columns; i++)
    {
        if (options.nPartitionKey <= 0 && !options.includes_columns[i])
        {
            // not select columns continue
            index++;
            continue;
        }
        std::shared_ptr<parquet::ColumnReader> col_reader = row_group_reader->Column(i);
        scanners[index] = ::parquet::Scanner::Make(col_reader, options.batch_size);
        columnDescriptors[index] = schema->Column(i);
        index++;
    }
}

void parquetFileReader::resetScanners() {
    scanners.clear();
    columnDescriptors.clear();
}


Datum parquetFileReader::read(Oid typeOid, int column_index, bool &isNull, int &state)
{
    const parquet::ColumnDescriptor* des = columnDescriptors[column_index];

    auto &scanner = scanners[column_index];
    switch(typeOid)
    {
        case BOOLOID:
        {
            parquet::TypedScanner<parquet::BooleanType>* scanner_ = dlCheckedScanner<parquet::BooleanType>(scanner, des, typeOid, name.c_str());
            bool values = false;
            bool hasRow = scanner_->NextValue(&values, &isNull);
            if (!hasRow)
            {
                //no row to read
                state = -1;
                return 0;
            }
            if (isNull)
            {
                return 0;
            }
            return BoolGetDatum(values);
        }
        case INT8OID:
        {
            parquet::TypedScanner<parquet::Int64Type>* scanner_ = dlCheckedScanner<parquet::Int64Type>(scanner, des, typeOid, name.c_str());
            int64_t values = 0;
            bool hasRow = scanner_->NextValue(&values, &isNull);
            if (!hasRow)
            {
                //no row to read
                state = -1;
                return 0;
            }
            if (isNull)
            {
                return 0;
            }
            return Int64GetDatum(values);
        }
        case INT4OID:
        {
            parquet::TypedScanner<parquet::Int32Type>* scanner_ = dlCheckedScanner<parquet::Int32Type>(scanner, des, typeOid, name.c_str());
            int32_t values = 0;
            bool hasRow = scanner_->NextValue(&values, &isNull);
            if (!hasRow)
            {
                //no row to read
                state = -1;
                return 0;
            }
            if (isNull)
            {
                return 0;
            }
            return Int32GetDatum(values);
        }
        case INT2OID:
        {
            parquet::TypedScanner<parquet::Int32Type>* scanner_ = dlCheckedScanner<parquet::Int32Type>(scanner, des, typeOid, name.c_str());
            int32_t values = 0;
            bool hasRow = scanner_->NextValue(&values, &isNull);
            if (!hasRow)
            {
                //no row to read
                state = -1;
                return 0;
            }
            if (isNull)
            {
                return 0;
            }
            return Int16GetDatum(values);
        }
        case FLOAT4OID:
        {
            parquet::TypedScanner<parquet::FloatType>* scanner_ = dlCheckedScanner<parquet::FloatType>(scanner, des, typeOid, name.c_str());
            float values = 0;
            bool hasRow = scanner_->NextValue(&values, &isNull);
            if (!hasRow)
            {
                //no row to read
                state = -1;
                return 0;
            }
            if (isNull)
            {
                return 0;
            }
            return Float4GetDatum(values);
        }
        case FLOAT8OID:
        {
            parquet::TypedScanner<parquet::DoubleType>* scanner_ = dlCheckedScanner<parquet::DoubleType>(scanner, des, typeOid, name.c_str());
            double values = 0;
            bool hasRow = scanner_->NextValue(&values, &isNull);
            if (!hasRow)
            {
                //no row to read
                state = -1;
                return 0;
            }
            if (isNull)
            {
                return 0;
            }
            return Float8GetDatum(values);
        }
        case INTERVALOID:
        {
            parquet::TypedScanner<parquet::ByteArrayType>* scanner_ = dlCheckedScanner<parquet::ByteArrayType>(scanner, des, typeOid, name.c_str());
            parquet::ByteArray values;
            bool hasRow = scanner_->NextValue(&values, &isNull);
            if (!hasRow)
            {
                //no row to read
                state = -1;
                return 0;
            }
            if (isNull)
            {
                return 0;
            }
            char readBuf[1024] = {0};
            memcpy(readBuf, values.ptr, values.len);
            return DirectFunctionCall1(interval_in, CStringGetDatum(readBuf));
        }
        case DATEOID:
        {
            parquet::TypedScanner<parquet::Int32Type>* scanner_ = dlCheckedScanner<parquet::Int32Type>(scanner, des, typeOid, name.c_str());
            int32_t values = 0;
            bool hasRow = scanner_->NextValue(&values, &isNull);
            if (!hasRow)
            {
                //no row to read
                state = -1;
                return 0;
            }
            if (isNull)
            {
                return 0;
            }
            return Int32GetDatum(values  + (UNIX_EPOCH_JDATE - POSTGRES_EPOCH_JDATE));
        }
        case TIMEOID:
        {
            parquet::TypedScanner<parquet::ByteArrayType>* scanner_ = dlCheckedScanner<parquet::ByteArrayType>(scanner, des, typeOid, name.c_str());
            parquet::ByteArray values;
            bool hasRow = scanner_->NextValue(&values, &isNull);
            if (!hasRow)
            {
                //no row to read
                state = -1;
                return 0;
            }
            if (isNull)
            {
                return 0;
            }
            char readBuf[1024] = {0};
            memcpy(readBuf, values.ptr, values.len);
            return DirectFunctionCall1(time_in, CStringGetDatum(readBuf));
        }
        case TIMESTAMPOID:
        case TIMESTAMPTZOID:
        {
            /*
             * Handle both INT64 (new: microseconds since Unix epoch) and
             * INT96 (legacy: Julian day + nanoseconds) timestamp formats.
             */
            if (des->physical_type() == ::parquet::Type::INT64)
            {
                /* INT64 MICROS: microseconds since Unix epoch */
                static const int64_t UNIX_TO_PG_EPOCH_USECS =
                    ((int64_t)(POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE)) * SECS_PER_DAY * USECS_PER_SEC;
                parquet::TypedScanner<parquet::Int64Type>* scanner_ = dlCheckedScanner<parquet::Int64Type>(scanner, des, typeOid, name.c_str());
                int64_t values = 0;
                bool hasRow = scanner_->NextValue(&values, &isNull);
                if (!hasRow) { state = -1; return 0; }
                if (isNull) return 0;
                return Int64GetDatum(values - UNIX_TO_PG_EPOCH_USECS);
            }
            else
            {
                /* INT96 legacy: Julian day + nanoseconds within day */
                parquet::TypedScanner<parquet::Int96Type>* scanner_ = dlCheckedScanner<parquet::Int96Type>(scanner, des, typeOid, name.c_str());
                parquet::Int96 values;
                bool hasRow = scanner_->NextValue(&values, &isNull);
                if (!hasRow) { state = -1; return 0; }
                if (isNull) return 0;
                int64_t second = parquet::Int96GetSeconds(values);
                int64_t micsecs = (second + (UNIX_EPOCH_JDATE - POSTGRES_EPOCH_JDATE) *60*60*24) * 1000000;
                int64_t diff_micsecs = parquet::Int96GetMicroSeconds(values) - second * 1000000;
                int64_t timestamp = micsecs + diff_micsecs;
                return Int64GetDatum(timestamp);
            }
        }
        case NUMERICOID:
        {
            switch(des->physical_type())
            {
                case ::parquet::Type::INT32: {
                    parquet::TypedScanner<parquet::Int32Type>* scanner_ = dlCheckedScanner<parquet::Int32Type>(scanner, des, typeOid, name.c_str());
                    int32_t values = 0;
                    uint scale = des->type_scale();
                    bool hasRow = scanner_->NextValue(&values, &isNull);
                    if (!hasRow)
                    {
                        //no row to read
                        state = -1;
                        return 0;
                    }
                    if (isNull)
                    {
                        return 0;
                    }
                    dataBuff *res = options.buffer.getDataBuffer(column_index);
                    int_to_numeric_with_scale(values, scale, (Numeric) res->buffer);
			        return NumericGetDatum((Numeric) res->buffer);
                }
                case ::parquet::Type::INT64: {
                    parquet::TypedScanner<parquet::Int64Type>* scanner_ = dlCheckedScanner<parquet::Int64Type>(scanner, des, typeOid, name.c_str());
                    int64_t values = 0;
                    uint scale = des->type_scale();
                    bool hasRow = scanner_->NextValue(&values, &isNull);
                    if (!hasRow)
                    {
                        //no row to read
                        state = -1;
                        return 0;
                    }
                    if (isNull)
                    {
                        return 0;
                    }
                    dataBuff *res = options.buffer.getDataBuffer(column_index);
                    int_to_numeric_with_scale(values, scale, (Numeric) res->buffer);
			        return NumericGetDatum((Numeric) res->buffer);
                }
                case ::parquet::Type::BYTE_ARRAY: {
                    int precision = des->type_precision();
                    int scale = des->type_scale();
                    if (precision > 0)
                    {
                        //TODO
                    }
                    if (scale > 0)
                    {
                        //TODO
                    }
                    parquet::TypedScanner<parquet::ByteArrayType>* scanner_ = dlCheckedScanner<parquet::ByteArrayType>(scanner, des, typeOid, name.c_str());
                    parquet::ByteArray values;
                    bool hasRow = scanner_->NextValue(&values, &isNull);
                    if (!hasRow)
                    {
                        //no row to read
                        state = -1;
                        return 0;
                    }
                    if (isNull)
                    {
                        return 0;
                    }
                    dataBuff *res = options.buffer.getDataBuffer(column_index);
                    int_to_numeric_with_scale(FLBA_to_int128(values.ptr, values.len), scale, (Numeric) res->buffer);
			        return NumericGetDatum((Numeric) res->buffer);
                }
                case ::parquet::Type::FIXED_LEN_BYTE_ARRAY: {
                    int precision = des->type_precision();
                    int scale = des->type_scale();
                    if (precision > 38)
                    {
                        elog(ERROR, "Parquet Decimal128 precision more than 38 is not support");
                    }
                    if (scale > 0)
                    {
                        //TODO
                    }
                    auto type_len = des->type_length();
                    parquet::TypedScanner<parquet::FLBAType>* scanner_ = dlCheckedScanner<parquet::FLBAType>(scanner, des, typeOid, name.c_str());
                    parquet::FixedLenByteArray values;
                    bool hasRow = scanner_->NextValue(&values, &isNull);
                    if (!hasRow)
                    {
                        //no row to read
                        state = -1;
                        return 0;
                    }
                    if (isNull)
                    {
                        return 0;
                    }
                    dataBuff *res = options.buffer.getDataBuffer(column_index);
                    int_to_numeric_with_scale(FLBA_to_int128(values.ptr, type_len), scale, (Numeric) res->buffer);
			        return NumericGetDatum((Numeric) res->buffer);
                }
                default:
                    elog(ERROR, "Datalake foreign table Error, Column index %d Parquet format "
                    "Physical type for decimal128 must be int32, int64, byte array, "
                    "or fixed length binary. physical_type %d.", typeOid, des->physical_type());
            }
            break;
        }
        case CHAROID:
        {
            parquet::TypedScanner<parquet::ByteArrayType>* scanner_ = dlCheckedScanner<parquet::ByteArrayType>(scanner, des, typeOid, name.c_str());
            parquet::ByteArray values;
            bool hasRow = scanner_->NextValue(&values, &isNull);
            if (!hasRow)
            {
                //no row to read
                state = -1;
                return 0;
            }
            if (isNull)
            {
                return 0;
            }
            char result[5] = {0};
            memcpy(result, values.ptr, values.len);
            return CharGetDatum(result[0]);
        }
        case BYTEAOID:
        case TEXTOID:
        case BPCHAROID:
        case VARCHAROID:
        {
            switch(des->physical_type())
            {
                case ::parquet::Type::BYTE_ARRAY: {
                    parquet::TypedScanner<parquet::ByteArrayType>* scanner_ = dlCheckedScanner<parquet::ByteArrayType>(scanner, des, typeOid, name.c_str());
                    parquet::ByteArray values;
                    bool hasRow = scanner_->NextValue(&values, &isNull);
                    if (!hasRow)
                    {
                        //no row to read
                        state = -1;
                        return 0;
                    }
                    if (isNull)
                    {
                        return 0;
                    }
                    /*
                     * A Parquet string is UTF-8 by the format's definition and
                     * a PG text datum must be valid in the database encoding,
                     * so validate (and convert where the two differ) instead of
                     * copying the bytes as they lie.  bytea is exempt: it is
                     * bytes, not text.
                     */
                    const char *bytes = (const char *) values.ptr;
                    int         nbytes = (int) values.len;
                    char       *conv = NULL;

                    if (typeOid != BYTEAOID)
                    {
                        conv = gpdbAnyToServer(bytes, nbytes, PG_UTF8, &nbytes);
                        bytes = conv;
                    }

                    /*
                     * The shared column buffer is BUFFER_LENGTH bytes and
                     * the else leg writes VARHDRSZ + nbytes into it, so the
                     * header has to be part of the test: at nbytes exactly
                     * BUFFER_LENGTH the old form overran it by VARHDRSZ.
                     */
                    if (nbytes + (int) VARHDRSZ > BUFFER_LENGTH)
                    {
                        bytea *result = (bytea*)palloc(nbytes + VARHDRSZ);
                        memcpy(VARDATA(result), bytes, nbytes);
                        SET_VARSIZE(result, nbytes + VARHDRSZ);
                        if (conv != NULL && conv != (const char *) values.ptr)
                            pfree(conv);
                        return PointerGetDatum(result);
                    }
                    else
                    {
                        dataBuffer* ptr = options.buffer.getDataBuffer(column_index);
                        memcpy(VARDATA(ptr->buffer), bytes, nbytes);
                        SET_VARSIZE(ptr->buffer, nbytes + VARHDRSZ);
                        if (conv != NULL && conv != (const char *) values.ptr)
                            pfree(conv);
                        return PointerGetDatum(ptr->buffer);
                    }
                }
                case ::parquet::Type::FIXED_LEN_BYTE_ARRAY: {
                    /*
                     * This branch is written for FLBA -- it asks the
                     * descriptor for type_length() -- but it decoded through a
                     * ByteArrayType scanner, so values.len was read out of a
                     * buffer that holds bare pointers.  It then sized the
                     * allocation and the varlena header from that garbage
                     * length while copying type_len bytes, or the reverse.
                     * A fixed-length column carries its length in the schema;
                     * take it from there and use the one number throughout.
                     */
                    int type_len = des->type_length();
                    parquet::TypedScanner<parquet::FLBAType>* scanner_ = dlCheckedScanner<parquet::FLBAType>(scanner, des, typeOid, name.c_str());
                    parquet::FixedLenByteArray values;
                    bool hasRow = scanner_->NextValue(&values, &isNull);
                    if (!hasRow)
                    {
                        //no row to read
                        state = -1;
                        return 0;
                    }
                    if (isNull)
                    {
                        return 0;
                    }
                    if (type_len > BUFFER_LENGTH)
                    {
                        bytea *result = (bytea*)palloc(type_len + VARHDRSZ);
                        memcpy(VARDATA(result), values.ptr, type_len);
                        SET_VARSIZE(result, type_len + VARHDRSZ);
                        return PointerGetDatum(result);
                    }
                    else
                    {
                        dataBuffer* ptr = options.buffer.getDataBuffer(column_index);
                        memcpy(VARDATA(ptr->buffer), values.ptr, type_len);
                        SET_VARSIZE(ptr->buffer, type_len + VARHDRSZ);
                        return PointerGetDatum(ptr->buffer);
                    }
                }
                default:
                    elog(ERROR, "Datalake foreign table Error, Column index %d. "
                     "MPP type BYTEAOID/TEXTOID/BPCHAROID/VARCHAROID Mapping "
                     "parquet BYTE_ARRAY/FIXED_LEN_BYTE_ARRAY. but column %d.", 
                     typeOid, des->physical_type());
            }
            break;
        }
        default:
            return 0;

    }
    return 0;
}

void parquetFileReader::closeParquetReader()
{
    if (state == FILE_CLOSE)
    {
        return;
    }
    resetParquetReader();
    state = FILE_CLOSE;
}

}
}