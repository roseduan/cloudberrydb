#include <sys/time.h>
#include <sstream>
#include <uuid/uuid.h>
#include "src/provider/orc/read/orcReadRecordBatch.h"
#include "src/provider/parquet/read/parquetRead.h"
#include "src/provider/parquet/write/parquetWrite.h"
#include "src/provider/orc/read/orcRead.h"
#include "src/provider/orc/write/orcWriter.h"
#include "src/provider/archive/read/archiveRead.h"
#include "src/provider/archive/read/textFileRead.h"
#include "src/provider/archive/write/archiveWrite.h"
#include "src/provider/avro/read/avroRead.h"
#include "src/provider/avro/write/avroWrite.h"
#include "src/provider/iceberg/iceberg_read.h"
#include "src/provider/iceberg/iceberg_write.h"




extern "C" {
#include "utils/builtins.h"
}
#include "src/provider/iceberg/iceberg_posdel_write.h"
#include "src/provider/hudi/hudi_read.h"
#include "provider.h"
extern "C" {
#include "src/common/util.h"
}

using Datalake::Internal::orcRead;
using Datalake::Internal::parquetRead;
using Datalake::Internal::avroRead;
using Datalake::Internal::archiveRead;
using Datalake::Internal::archiveWrite;
using Datalake::Internal::orcReadRecordBatch;
using Datalake::Internal::icebergRead;
using Datalake::Internal::icebergWrite;
using Datalake::Internal::icebergPosDeleteWrite;
using Datalake::Internal::hudiRead;
using Datalake::Internal::textFileRead;


std::shared_ptr<Provider> getProvider(DLTblFmt type, DLCmdType cmd, bool vectorization)
{
	switch (cmd)
	{
		case DL_OP_READ:
			if (FORMAT_IS_TEXT(type) || FORMAT_IS_CSV(type) || FORMAT_IS_CUSTOM(type))
			{
				if (external_table_new_text)
					return std::make_shared<textFileRead>();
				else
					return std::make_shared<archiveRead>();
			}
			else if (FORMAT_IS_ORC(type))
			{
				if (vectorization)
				{
					return std::make_shared<orcReadRecordBatch>();
				}
				else
				{
					return std::make_shared<orcRead>();
				}
			}
			else if (FORMAT_IS_PARQUET(type))
			{
				return std::make_shared<parquetRead>();
			}
			else if (FORMAT_IS_AVRO(type))
			{
				return std::make_shared<avroRead>();
			}
			else if (FORMAT_IS_ICEBERG(type))
			{
				return std::make_shared<icebergRead>();
			}
			else if (FORMAT_IS_HUDI(type))
			{
				return std::make_shared<hudiRead>();
			}
			else
			{
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						errmsg("unknow format. "
						"datalake_fdw support read format text|csv|custom|orc|parquet|avro|hudi|iceberg.")));
			}
			break;
		case DL_OP_WRITE:
			if (FORMAT_IS_TEXT(type) || FORMAT_IS_CSV(type) || FORMAT_IS_CUSTOM(type))
			{
				return std::make_shared<archiveWrite>();
			}
			else if (FORMAT_IS_ORC(type))
			{
				return std::make_shared<orcWrite>();
			}
			else if (FORMAT_IS_PARQUET(type))
			{
				return std::make_shared<parquetWrite>();
			}
			else if (FORMAT_IS_AVRO(type))
			{
				return std::make_shared<avroWrite>();
			}
			else if (FORMAT_IS_ICEBERG(type))
			{
				return std::make_shared<icebergWrite>();
			}
			else
			{
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						errmsg("unknow format. "
						"datalake_fdw support write format text|csv|custom|orc|parquet|avro|iceberg.")));
			}
			break;
		case DL_OP_DELETE:
			if (FORMAT_IS_ICEBERG(type))
			{
				return std::make_shared<icebergPosDeleteWrite>();
			}
			else
			{
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						errmsg("datalake_fdw support delete format iceberg.")));
			}
			break;
	}
	return nullptr;
}

void Provider::createHandler(void* sstate)
{
	return;
}

int64_t Provider::read(void *values, void *nulls)
{
	return 0;
}

int64_t Provider::read(void *values, void *nulls, void *tid)
{
	return 0;
}

int64_t Provider::read(void **recordBatch)
{
	return 0;
}

int64_t Provider::readWithBuffer(void* buffer, int64_t length)
{
	return 0;
}

int64_t Provider::write(const void* buf, int64_t length)
{
	return 0;
}

void Provider::destroyHandler()
{
	return;
}

void Provider::reScan()
{
	return;
}

void Provider::setPartitionValue(void* values, void* nulls) {
	return;
}

const char* Provider::getReadFileName() {
	return NULL;
}


CompressType Provider::getCompressType(char* type)
{
	CompressType compresstype = UNCOMPRESS;
	if (type == NULL)
	{
		return compresstype;
	}

	if (strcmp(strConvertLow(type), "uncompress") == 0 ||
		strcmp(strConvertLow(type), "none") == 0)
	{
		compresstype = UNCOMPRESS;
	}
	else if (strcmp(strConvertLow(type), "zlib") == 0)
	{
		compresstype = ZLIB;
	}
	else if (strcmp(strConvertLow(type), "gzip") == 0)
	{
		compresstype = GZIP;
	}
	else if (strcmp(strConvertLow(type), "snappy") == 0)
	{
		compresstype = SNAPPY;
	}
	else if (strcmp(strConvertLow(type), "zstd") == 0)
	{
		compresstype = ZSTD;
	}
	else if (strcmp(strConvertLow(type), "lz4") == 0)
	{
		compresstype = LZ4;
	}
	else if (strcmp(strConvertLow(type), "brotli") == 0)
	{
		compresstype = BROTLI;
	}
	else if (strcmp(strConvertLow(type), "zip") == 0)
	{
		compresstype = ZIP;
	}
	else
	{
		elog(ERROR, "datalake unkonw compress type %s", type);
	}

	return compresstype;
}

std::string Provider::generateWriteFileName(const std::string &writePrefix, const std::string &compress, const std::string &suffix)
{
	// Generate UUID using PostgreSQL built-in function (version 4, random)
	Datum uuid_d = OidFunctionCall0(F_GEN_RANDOM_UUID);
	char *uuid = DatumGetCString(OidFunctionCall1(F_UUID_OUT, uuid_d));

	std::stringstream fileName;
	if (!writePrefix.empty())
	{
		fileName << writePrefix;
		if (fileName.str().back() != '/')
		{
			fileName << "/";
		}
	}

	// Use UUID as filename (UUID is already unique)
	fileName << uuid;
	if (!compress.empty())
	{
		fileName << "." << compress;
	}
	if (!suffix.empty())
	{
		fileName << "." << suffix;
	}

	std::string result = fileName.str();
	pfree(uuid);
	return result;
}

std::string Provider::generateIcebergUuidFileName(const std::string &writePrefix, const std::string &suffix)
{
	std::stringstream fileName;
	if (!writePrefix.empty())
	{
		fileName << writePrefix;
		if (fileName.str().back() != '/')
		{
			fileName << "/";
		}
	}
	// Generate UUID-based filename
	char uuid_str[37] = {0};
	uuid_t uuid;
	uuid_generate_time(uuid);
	uuid_unparse(uuid, uuid_str);
	fileName << uuid_str;
	if (!suffix.empty())
	{
		fileName << "." << suffix;
	}
	return fileName.str();
}
