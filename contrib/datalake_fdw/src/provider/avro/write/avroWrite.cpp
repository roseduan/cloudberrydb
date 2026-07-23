
#include "avroWrite.h"
#include "src/common/fileSystemWrapper.h"

extern "C"
{
#include "src/datalake_option.h"
}

void avroWrite::createHandler(void* sstate)
{
    dataLakeFdwScanState *ss = (dataLakeFdwScanState*)sstate;
    fileStream = datalakeCreateFileSystem((void*)(ss->options->gopher));
    std::string prefix = (char*)lfirst(list_head(ss->fragments)); 
    setOption(ss->options->compress);
    generateAvroFileName(prefix);
    file_writer=std::make_unique<avroWriter>(fileStream, file_name, sstate, option);
}

int64_t avroWrite::write(const void* buf, int64_t length)
{
    int64_t rownum = file_writer->write(buf, length);
    return rownum;
}

std::string& avroWrite::generateAvroFileName(const std::string &filePath)
{
    file_name = generateWriteFileName(filePath, datalakeGetCompressionName(option.compression), AVRO_WRITE_SUFFIX);
    return file_name;
}

void avroWrite::destroyHandler()
{
    file_writer->close();
    datalakeDestroyFileSystem(fileStream);
    fileStream = NULL;
}

void avroWrite::setOption(CompressType compressType)
{
    option.compression = compressType;
}
