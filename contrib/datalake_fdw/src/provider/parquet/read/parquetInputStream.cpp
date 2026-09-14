#include "parquetInputStream.h"
#include <parquet/platform.h>

/*
 * For elog/DEBUG1. parquetFileReader.cpp already pulls the PostgreSQL
 * headers in this way; this file did not need them until the file-meta
 * cache added its hit/miss log line.
 */
extern "C" {
#include "postgres.h"
#include "utils/elog.h"
}

namespace Datalake {
namespace Internal {

::parquet_arrow::Status gopherReadFileSystem::Open()
{
    closed_ = false;
	int flag = O_RDONLY|O_RDTHR;
	if (enableCache)
	{
		flag = O_RDONLY;
	}
    datalakeOpenFile(stream_, filePath_.c_str(), flag);
    /*
     * datalakeOpenFile leaves a fresh handle at offset 0, and Open() below
     * either leaves it there or never moves it. pos_ has to start out
     * agreeing with that: the sequential Read() overloads advance pos_
     * without seeking, so an uninitialised pos_ would make Tell() and any
     * cache accounting garbage until the first ReadAt or Seek.
     */
    pos_ = 0;
    datalakeFileInfo* info = datalakeGetFileInfo(stream_, filePath_.c_str());
    fileSize_ = info->length;
    datalakeFreeFileInfo(info, 1);
    if (fileSize_ == 0)
    {
        checkMagic = false;
        return ::parquet_arrow::Status::OK();
    }

    /*
     * Pick the cached footer out of the openFile reply. Zero RPC -- it
     * rode along with the response we already paid for.
     */
    metaCache.load(stream_, DATALAKE_PRODUCER_ID, fileSize_);
    metaOk = metaCache.hit()
             && metaCache.type() == DATALAKE_META_PARQUET_FOOTER
             && metaCache.offset() + (int64_t) metaCache.length() == fileSize_;

    elog(DEBUG1, "datalake file-meta-cache: %s for %s (hit=%d tombstone=%d type=%d)",
         metaOk ? "hit" : "miss", filePath_.c_str(),
         (int) metaCache.hit(), (int) metaCache.tombstone(), metaCache.type());

    const char* magic = "PAR1";
    if (metaOk)
    {
        /*
         * Validate from the cached footer instead of reading the head.
         *
         * The point is not the four bytes, it is that reading offset 0
         * makes Gopher materialize the whole of block 0 -- 8 MB by
         * default -- which is then thrown away whenever every row group
         * gets pruned. Avoiding that is a bigger win than the footer read
         * this feature was built for.
         *
         * A Parquet file ends with the same "PAR1" it starts with, so the
         * cached range's last four bytes carry the check. Be precise
         * about what this is, though: it is NOT the head check. It is the
         * tail magic plus provenance -- a blob only exists because an
         * earlier open passed the head check at this exact file length
         * and then successfully parsed the footer. What it inherits is
         * the invalidation hole this whole feature already has (same
         * path, same length, different content); it does not create a new
         * one. An encrypted footer ends in "PARE" and is never cached at
         * all, so it cannot reach here.
         */
        char tailMagic[4] = {0};
        if (metaCache.serve(tailMagic, 4, fileSize_ - 4)
            && strncmp(tailMagic, magic, 4) == 0)
        {
            return ::parquet_arrow::Status::OK();
        }
        /* Cached bytes do not look like a footer after all: fall through. */
        metaOk = false;
    }

    char fileMagic[5] = {0};
    datalakeReadFile(stream_, fileMagic, 4);
    if (strncmp(fileMagic, magic, 4) != 0)
    {
        checkMagic = false;
    }
    datalakeSeekFile(stream_, 0);
    pos_ = 0;
    return ::parquet_arrow::Status::OK();
}

bool gopherReadFileSystem::serveFromMeta(void *out, int64_t position, int64_t nbytes)
{
    if (!metaOk || !metaCache.serve(out, nbytes, position))
        return false;

    /*
     * Advance the REAL cursor too -- see the contract in the header. The
     * seek costs no I/O; skipping the read is where the saving is.
     *
     * A failed seek must not be swallowed. pos_ would advance while the real
     * cursor stayed where it was, and the next sequential Read() -- which does
     * not seek -- would hand back bytes from the wrong offset, silently.
     * Report a miss instead: both ReadAt() overloads seek explicitly before
     * falling back, so a transient failure costs one extra seek, and a real
     * one surfaces through the normal read path rather than as corrupt data.
     */
    if (datalakeSeekFile(stream_, position + nbytes) != 0)
        return false;

    pos_ = position + nbytes;
    return true;
}

void gopherReadFileSystem::backfillFooter(uint32_t metadataLen)
{
    /*
     * THE CACHED RANGE MUST BE AT LEAST THE LAST 64 KB, not the exact
     * footer. This is the single most important decision in Parquet
     * support, and getting it wrong fails silently.
     *
     * arrow cannot know metadata_len before reading it, so its first
     * footer request is unconditionally the last min(fileSize, 64 KB)
     * (kDefaultFooterReadSize in parquet/file_reader.cc). Meanwhile
     * FileMetaRangeCache::serve() only answers a request that falls
     * ENTIRELY inside the cached range -- a partial overlap is a miss, on
     * purpose, so that short data is never stitched together. Cache just
     * the precise footer (typically 2-8 KB) and arrow's 64 KB request
     * overlaps it partially every single time: the cache would never once
     * be used, with no error and a hit rate pinned at zero.
     *
     * So the range is max(footer, 64 KB), clamped to the file:
     *   - footer + 8 < 64 KB  -> cache 64 KB, arrow's first read matches
     *     exactly;
     *   - footer + 8 > 64 KB  -> cache the footer, which contains arrow's
     *     64 KB request and also its follow-up read of the full metadata
     *     (file_reader.cc only issues that second read in this case);
     *   - file < 64 KB        -> cache the whole file, and arrow asks for
     *     min(size, 64 KB) = the whole file.
     * The cost is 64 KB per file instead of a few KB. It is not
     * optimizable away; it is the price of the interface.
     */
    const int64_t kArrowFooterReadSize = 64 * 1024;

    int64_t footerLen = (int64_t) metadataLen + 8;   /* thrift + 4B len + 4B "PAR1" */
    int64_t cacheLen = footerLen > kArrowFooterReadSize ? footerLen : kArrowFooterReadSize;
    if (cacheLen > fileSize_)
        cacheLen = fileSize_;

    if (cacheLen <= 0 || cacheLen > (int64_t) INT32_MAX)
    {
        /*
         * Same reasoning as the ORC adapter's guard: push() takes an
         * int32 length, and a truncated negative would be dropped by its
         * own len <= 0 check -- a file that never gets cached with nothing
         * in the log to say why.
         */
        elog(DEBUG1, "datalake: Parquet footer range %ld bytes cannot be "
                     "expressed as an int32 blob length, skipping backfill",
             (long) cacheLen);
        return;
    }

    metaCache.push(stream_, DATALAKE_PRODUCER_ID, DATALAKE_META_PARQUET_FOOTER,
                   fileSize_ - cacheLen, (int32_t) cacheLen);
}

::parquet_arrow::Result<int64_t> gopherReadFileSystem::GetSize()
{
    return fileSize_;
}

::parquet_arrow::Result<int64_t> gopherReadFileSystem::ReadAt(int64_t position, int64_t nbytes, void* out)
{
    if (serveFromMeta(out, position, nbytes))
        return nbytes;

    datalakeSeekFile(stream_, position);
    pos_ = position;
    int64_t bytes = datalakeReadFile(stream_, out, nbytes);
    pos_ += bytes;
    return bytes;
}

::parquet_arrow::Result<std::shared_ptr<::parquet_arrow::Buffer>> gopherReadFileSystem::ReadAt(int64_t position, int64_t nbytes)
{
    std::shared_ptr<parquet_arrow::ResizableBuffer> buffer = parquet::AllocateBuffer(NULL, nbytes);
    if (serveFromMeta((void*)(buffer->data()), position, nbytes))
        return std::move(buffer);

    datalakeSeekFile(stream_, position);
    pos_ = position;
    int64_t bytes = datalakeReadFile(stream_, (void*)(buffer->data()), nbytes);
    pos_ += bytes;
    return std::move(buffer);
}

::parquet_arrow::Result<std::shared_ptr<::parquet_arrow::Buffer>> gopherReadFileSystem::Read(int64_t nbytes) {
    std::shared_ptr<parquet::ResizableBuffer> buffer = parquet::AllocateBuffer(NULL, nbytes);
    int64_t bytes = datalakeReadFile(stream_, (void*)(buffer->data()), nbytes);
    pos_ += bytes;
    return std::move(buffer);
}

::parquet_arrow::Result<int64_t> gopherReadFileSystem::Read(int64_t nbytes, void* out) {
    int64_t bytes = datalakeReadFile(stream_, out, nbytes);
    pos_ += bytes;
    return bytes;
}

::parquet_arrow::Status gopherReadFileSystem::Seek(int64_t position) {
    datalakeSeekFile(stream_, position);
    pos_ = position;
    return ::parquet_arrow::Status::OK();
}

::parquet_arrow::Status gopherReadFileSystem::Close() {
    if (closed_)
    {
        // file maybe already closed
        return ::parquet_arrow::Status::OK();
    }
    closed_ = true;
    datalakeCloseFile(stream_);
    return ::parquet_arrow::Status::OK();
}

bool gopherReadFileSystem::checkFileIsParquet() {
    return checkMagic;
}

}
}
