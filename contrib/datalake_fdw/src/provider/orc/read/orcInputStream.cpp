#include <list>
#include <cassert>
#include <sstream>
#include "orcInputStream.h"

extern "C" {
#include "utils/elog.h"
#include "src/datalake_def.h"
}

namespace Datalake {
namespace Internal {

OssInputStream::OssInputStream(ossFileStream fileStream, std::string filename, uint64_t length, bool enableCache, char* allocBuffer)
	: stream(fileStream), filename(filename), length(length), enableCache(enableCache), allocBuffer(allocBuffer), metaOk(false)
{
	int flag = O_RDONLY | O_RDTHR;
	if (enableCache)
	{
		flag = O_RDONLY;
	}
	datalakeOpenFile(stream, filename.c_str(), flag);

	metaCache.load(stream, DATALAKE_PRODUCER_ID, (int64_t) length);
	/*
	 * ORC-specific EOF-anchor validation: the tail is, by definition,
	 * flush against the end of the file. This is a second line of
	 * defense independent of Gopher's own fileLength/CRC check -- if it
	 * does not hold, the blob does not match the current file, and we
	 * would rather treat it as a miss. (Same for Parquet; Avro instead
	 * anchors at off == 0, which is why this check cannot live in the
	 * shared FileMetaRangeCache component.)
	 */
	metaOk = metaCache.hit()
	         && metaCache.type() == DATALAKE_META_ORC_TAIL
	         && metaCache.offset() + (int64_t) metaCache.length() == (int64_t) length;

	/*
	 * Task 16 (benefit verification) needs to be able to tell "the cache
	 * did not kick in" apart from "this scenario just did not have much
	 * to save" -- the two look identical in both the query result and
	 * the ordinary log. This DEBUG1 line is the only place that records
	 * the metaOk verdict PER FILE OPEN; running the acceptance cases
	 * relies on it to count hits/misses, rather than guessing.
	 */
	elog(DEBUG1, "datalake file-meta-cache: %s for %s (hit=%d tombstone=%d type=%d)",
		 metaOk ? "hit" : "miss", filename.c_str(),
		 (int) metaCache.hit(), (int) metaCache.tombstone(), metaCache.type());
}

OssInputStream::~OssInputStream()
{
	datalakeCloseFile(stream);
}

void OssInputStream::range(uint64_t offset, uint64_t length)
{
	/* If the orc is not very efficient. Maybe can do a memory load in the range */
	//TODO
}

void OssInputStream::read(void *buf, uint64_t length, uint64_t offset)
{
	/* Only served from memory when the range is fully contained; any partial overlap always falls back to the source. Better one extra I/O than ever stitching data together. */
	if (metaOk && metaCache.serve(buf, length, offset))
		return;

	datalakeSeekFile(stream, offset);
	datalakeReadFile(stream, buf, length);
}

bool OssInputStream::checkORCFile()
{
    // skip hive orc acid version
    int pos = filename.find("orc_acid_version");
    if (pos >= 0)
    {
        return false;
    }

	/*
	 * Skip the magic-number check when the cache hits. Two reasons:
	 * 1) the tail has already passed Gopher's own CRC check and is
	 *    length-consistent, which is stronger proof of being a real ORC
	 *    file than the 3-byte magic number at offset 0;
	 * 2) MORE IMPORTANTLY, that 3-byte read would force Gopher to
	 *    materialize the whole of block 0 (8MB by default), and that 8MB
	 *    of raw data goes completely unused whenever every stripe ends
	 *    up pruned -- this is the single biggest win this feature
	 *    delivers.
	 */
	if (metaOk)
		return true;

    // Check ORC Magic
	datalakeSeekFile(stream, 0);
	char magic[4] = {0};
	datalakeReadFile(stream, magic, 3);
    if (strcmp(magic, "ORC") != 0)
    {
        elog(LOG, "%s is not orc file.", filename.c_str());
		return false;
    }
	datalakeSeekFile(stream, 0);
    return true;
}

void OssInputStream::fillAndPushTail(int64_t off, int32_t len)
{
	metaCache.push(stream, DATALAKE_PRODUCER_ID, DATALAKE_META_ORC_TAIL, off, len);
}

}
}