#include "archiveWrite.h"

namespace Datalake {
namespace Internal {



void archiveWrite::createHandler(void *sstate)
{
	dataLakeFdwScanState *ss = (dataLakeFdwScanState*)sstate;
	fileStream = datalakeCreateFileSystem((void*)(ss->options->gopher));
	setOption(ss->options);
	sliceIndex = 0;
	currentWriteSize = 0;
	prefix = (char*)lfirst(list_head(ss->fragments));

	if (option.compression == GZIP)
	{
		suffix = "gz";
	}
	else if (option.compression == ZIP)
	{
		suffix = "zip";
	}
	else
	{
		if (FORMAT_IS_TEXT(ss->options->format))
		{
			suffix = TEXT_WRITE_SUFFIX;
		} else if (FORMAT_IS_CSV(ss->options->format))
		{
			suffix = CSV_WRITE_SUFFIX;
		}
	}
}

int64_t archiveWrite::write(const void *buf, int64_t length)
{
	if (option.writeFileSize > 0 && currentWriteSize + length > option.writeFileSize && currentWriteSize > 0)
	{
		fileWrite.close();
		currentWriteSize = 0;
		sliceIndex += 1;
	}

	if (!fileWrite.isOpen())
	{
		fileName = generateArchiveWriteFileName(prefix, suffix, sliceIndex);
		fileWrite.open(fileStream, fileName, option);
	}
	int64_t len = fileWrite.write(buf, length);
	currentWriteSize += len;
	return len;
}

void archiveWrite::destroyHandler()
{
	fileWrite.close();
}


std::string archiveWrite::generateArchiveWriteFileName(const std::string &filePath, const std::string &suffix, uint32 fileSliceIdx)
{
	std::string exportName = generateWriteFileName(filePath, "", suffix);
	return exportName;
}

void archiveWrite::setOption(dataLakeOptions *options)
{
	option.compression = options->compress;
	option.writeFileSize = options->fileSizeLimit;
}

}
}