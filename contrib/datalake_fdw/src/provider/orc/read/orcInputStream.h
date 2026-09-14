#ifndef DATALAKE_ORCINPUTSTREAM_H
#define DATALAKE_ORCINPUTSTREAM_H

#include "orc/OrcFile.hh"
#include "orc/Reader.hh"
#include "orc/Type.hh"
#include "orc/Vector.hh"
#include "src/common/fileSystemWrapper.h"
#include "src/common/fileMetaRangeCache.h"
#include <iostream>

namespace Datalake {
namespace Internal {

class OssInputStream : public orc::InputStream
{
public:
	OssInputStream(ossFileStream stream, std::string filename, uint64_t length, bool enableCache, char* allocBuffer);

    virtual uint64_t getLength() const {
		return length;
	}

    virtual uint64_t getNaturalReadSize() const {
		return length;
	}

    virtual void read(void *buf, uint64_t length, uint64_t offset);

    virtual const std::string &getName() const {
		return filename;
	}

    virtual ~OssInputStream();

    void range(uint64_t offset, uint64_t length);

	bool checkORCFile();

	bool isMetaOk() const { return metaOk; }
	void fillAndPushTail(int64_t off, int32_t len);

private:
	ossFileStream stream;
	std::string filename;
	uint64_t length;
	bool enableCache;
	char* allocBuffer;

	FileMetaRangeCache metaCache;
	/* metaOk = hit AND it is an ORC tail AND the shape checks out; distinct from metaCache.hit() */
	bool metaOk;
};

}
}
#endif