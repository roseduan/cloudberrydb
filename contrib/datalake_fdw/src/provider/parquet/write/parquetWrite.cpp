#include "parquetWrite.h"
#include "src/common/fileSystemWrapper.h"

extern "C"
{
#include "src/common/util.h"
#include "src/datalake_option.h"
}

#define DATALAKE_EXPORT_NAME ("datalake")


void parquetWrite::createHandler(void *sstate)
{
	/* parquet init */ 
	ss = (dataLakeFdwScanState*)sstate;
	gopherConfig* conf = datalakeCreateGopherConfig((void*)(ss->options->gopher));
	fileStream = datalakeCreateFileSystem(conf);
	datalakeFreeGopherConfig(conf);
	prefix = (char*)lfirst(list_head(ss->fragments)); 
	setOption(ss->options);
	sliceIdx= 0;
	file_writer.init(sstate, option);
}

void parquetWrite::setOption(dataLakeOptions *options)
{
    option.compression = options->compress;
    option.compressionLevel = options->compressLevel;
    option.writeFileSize = options->fileSizeLimit;
}

std::string parquetWrite::generateParquetFileName(const std::string &filePath, uint32 fileSliceIndex)
{
	return generateWriteFileName(filePath, std::string(datalakeGetCompressionName(option.compression)), PARQUET_WRITE_SUFFIX);
}

int64_t parquetWrite::write(const void *buf, int64_t length)
{
	if (file_writer.isOpen() && option.writeFileSize > 0 && file_writer.getWrittenBytes() + length > option.writeFileSize)
	{
		file_writer.closeParquetWriter();
		sliceIdx += 1;
	}

	if (!file_writer.isOpen())
	{
		fileName = generateParquetFileName(prefix, sliceIdx);
		file_writer.createParquetWriter(fileStream, fileName);
	}
	int64_t len = file_writer.write(buf, length);
	return len;
}

void parquetWrite::destroyHandler()
{
	if (file_writer.isOpen())
	{
		file_writer.closeParquetWriter();
	}
	datalakeDestroyFileSystem(fileStream);
	fileStream = NULL;
}