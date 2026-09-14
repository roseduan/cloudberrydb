
#ifndef DATALAKE_PARQUETINPUTSTREAM_H
#define DATALAKE_PARQUETINPUTSTREAM_H
#include "src/common/fileSystemWrapper.h"
#include "src/common/fileMetaRangeCache.h"
#include <parquet/internal/arrow/io/interfaces.h>
#include <parquet/internal/arrow/result.h>

namespace Datalake {
namespace Internal {
class gopherReadFileSystem : public ::parquet_arrow::io::RandomAccessFile {

public:
    gopherReadFileSystem(ossFileStream stream, std::string filePath, bool enableCache) : stream_(stream), filePath_(filePath), enableCache(enableCache) {
        checkMagic = true;
        closed_ = true;
    };

    ::parquet_arrow::Status Open();

    ::parquet_arrow::Result<int64_t> GetSize();

    ::parquet_arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out);

    ::parquet_arrow::Result<std::shared_ptr<::parquet_arrow::Buffer>> ReadAt(int64_t position, int64_t nbytes);

    ::parquet_arrow::Result<std::string_view> Peek(int64_t nbytes) {
        return ::parquet_arrow::Status::NotImplemented("Peek not implemented");
    }

    ::parquet_arrow::Result<std::shared_ptr<::parquet_arrow::Buffer>> Read(int64_t nbytes);

    ::parquet_arrow::Result<int64_t> Read(int64_t nbytes, void* out);

    ::parquet_arrow::Result<int64_t> Tell() const {
        return pos_;
    }

    ::parquet_arrow::Status Seek(int64_t position);

    ::parquet_arrow::Status Close();

    bool closed() const {
        return closed_;
    }

    bool checkFileIsParquet();

    /*
     * metaOk = the blob hit AND it is a Parquet footer AND it is anchored
     * at EOF. Deliberately distinct from metaCache.hit(): the anchor check
     * cannot live inside the shared FileMetaRangeCache, because Avro's
     * metadata sits at offset 0 instead.
     */
    bool isMetaOk() const { return metaOk; }

    /*
     * Backfill the footer, given the serialized FileMetaData length that
     * parquet reports. Owns the range arithmetic, including the 64 KB
     * floor, because that floor is a fact about how arrow reads rather
     * than anything the caller should have to know.
     */
    void backfillFooter(uint32_t metadataLen);

private:
    /*
     * Serve a pread from the cache and keep BOTH cursors in step.
     *
     * This is the one place Parquet differs from ORC in a way that
     * matters. OssInputStream::read() can serve from the cache and return
     * without touching the file cursor, because every ORC read seeks
     * first. Here, Read(nbytes) and Read(nbytes, out) are SEQUENTIAL --
     * they never seek and rely on pos_ tracking the real cursor by
     * convention. Serving a ReadAt from memory without advancing the real
     * cursor would silently desynchronise the two, and the next
     * sequential read would return data from the wrong offset with no
     * error at all. So a cache hit still issues the seek; it just skips
     * the read.
     *
     * If that seek fails, the call reports a MISS rather than a hit, so
     * the caller falls back to its own explicit seek and the invariant
     * holds either way. Returning a hit on a failed seek would reinstate
     * exactly the desynchronisation described above.
     */
    bool serveFromMeta(void *out, int64_t position, int64_t nbytes);

    ossFileStream stream_;
    int64_t fileSize_;
    int64_t pos_;
    bool closed_;
    std::string filePath_;
    bool checkMagic;
	bool enableCache;

    FileMetaRangeCache metaCache;
    bool metaOk = false;
};

}
}

#endif
