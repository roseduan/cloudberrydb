/*-------------------------------------------------------------------------
 *
 * fileMetaRangeCache.h
 *    Format-agnostic range cache over a Gopher file-metadata blob.
 *
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 *
 * IDENTIFICATION
 *        contrib/datalake_fdw/src/common/fileMetaRangeCache.h
 *-------------------------------------------------------------------------
 */
#ifndef DATALAKE_FILEMETARANGECACHE_H
#define DATALAKE_FILEMETARANGECACHE_H

#include "fileSystemWrapper.h"
#include <cstdint>
#include <vector>

namespace Datalake {
namespace Internal {

/*
 * Range cache over a file-metadata blob. FORMAT-AGNOSTIC: it only knows
 * about "some byte range within a file" -- it does not care whether that
 * range is an ORC tail, a Parquet footer, or an Avro header.
 *
 * The interface is unified around pread semantics -- the three formats'
 * native read semantics differ (ORC read(buf,len,off) / Parquet
 * ReadAt(pos,n,out) + Read(n,out) / Avro fill() using byteCount_), so
 * each adapter converts its own position into an absolute offset before
 * calling serve().
 * push() backfills by doing a seek+read on the caller's own stream, so it
 * necessarily moves the file cursor -- and therefore it SAVES THE CURSOR
 * (datalakeTellFile) BEFORE THE READ AND SEEKS BACK TO IT AFTERWARDS,
 * leaving the stream where the caller left it. Callers do not have to
 * know that push() reads at all.
 *
 * That restore is what makes push() usable from an adapter that does not
 * seek before every logical read:
 *   ORC never needed it -- OssInputStream::read() in orcInputStream.cpp
 *   unconditionally does datalakeSeekFile then datalakeReadFile;
 *   parquetInputStream.cpp's Read(nbytes[,out]) is purely sequential and
 *   never seeks, relying on an internal pos_ staying in sync with the
 *   real underlying cursor by convention;
 *   avroOssInputStream.h's next()/fill() are likewise sequential reads,
 *   and only the skip()/seek() branches seek explicitly.
 * Without the restore, a backfill landing "right after a seek, in the
 * middle of the run of seek-free sequential reads that follows it" would
 * move the real cursor without that internal bookkeeping knowing about
 * it, and the next sequential read would fetch data from the wrong
 * offset -- **producing silently misaligned data**, quite possibly with
 * no error at all on small files.
 *
 * The restore is best-effort: a failing seek-back is ignored, which can
 * only happen on a stream that is already unusable for the real read
 * that triggered the backfill.
 *
 * push() also enforces datalake.file_meta_max_size, refusing to send a
 * blob larger than the configured limit. That check lives here rather
 * than in each format's adapter because this is the single place a
 * backfill can originate, so a format added later cannot forget it. A
 * refusal is logged at DEBUG1: an oversized blob and a backfill that
 * never ran look identical from the outside.
 *
 * LOAD() MUST BE CALLED BEFORE PUSH() -- push()'s bounds check depends
 * on the fileLength_ that load() fills in; calling push() without load()
 * first will silently fail every time because fileLength_ == 0 (safe,
 * but it fails quietly with no error).
 *
 * NO EOF-ANCHOR VALIDATION IS DONE HERE -- Avro's metadata sits at the
 * start of the file (off == 0). Anchor validation belongs to each
 * format's adapter layer.
 *
 * SCOPE: BARE EXTERNAL TABLES ONLY. datalake_fdw has TWO independent
 * Parquet reader stacks, and only one of them is wired into this cache:
 *
 *   provider/parquet/read/  gopherReadFileSystem   external tables  CACHED
 *   provider/common/        DatalakeRandomAccessFile  Iceberg/Hudi  NOT CACHED
 *
 * The Iceberg and Hudi path is datalakeRowReaderNext ->
 * icebergTaskReaderNext -> fileReaderNext (see row_reader.h), which
 * reaches parquet_reader.cpp and its own arrow RandomAccessFile in
 * datalake_random_file.cpp. That class has the same shape as
 * gopherReadFileSystem -- Open/ReadAt/Read/Seek/Tell over the same
 * datalake* wrappers -- but shares no code with it, so it does not call
 * load() or push() and gets no benefit from this cache at all.
 *
 * That is a deliberate scope decision, not an oversight. Wiring the
 * second stack in is a separate piece of work: it would need the same
 * three pieces (load in Open, serve in ReadAt, backfill after
 * ParquetFileReader::Open) AND the same cursor-restore care, because
 * DatalakeRandomAccessFile::Read is sequential too.
 *
 * ORC is not affected by the split: it has only one reader stack, and
 * file_reader.c's orcReader entry is all NULL function pointers, so the
 * Iceberg/Hudi path cannot read ORC in the first place. Every ORC read
 * therefore goes through the cached adapter.
 */
class FileMetaRangeCache
{
public:
	/* Pull the blob out of datalakeOpenFile's response. Zero RPC. */
	void load(ossFileStream stream, uint32_t producerId, int64_t fileLength);

	bool    hit()       const { return hit_; }
	bool    tombstone() const { return tombstone_; }   /* this file cannot be cached, skip backfill */
	int32_t type()      const { return type_; }
	int64_t offset()    const { return off_; }
	int32_t length()    const { return len_; }

	/* Only memcpy and return true when the range is FULLY CONTAINED; any partial overlap returns false (fall back to the source). */
	bool serve(void *buf, uint64_t len, uint64_t offset) const;

	/* Backfill: reads [offset, offset+len) itself and pushes it. Any failure is silently ignored. */
	void push(ossFileStream stream, uint32_t producerId, int32_t type,
			  int64_t offset, int32_t len);

private:
	bool    hit_        = false;
	bool    tombstone_  = false;
	int32_t type_       = 0;

	/*
	 * data_ owns a private copy of the cached bytes.
	 *
	 * datalakeGetFileMeta() hands back a pointer into a buffer that lives
	 * inside the Gopher file handle -- it is only valid until the stream is
	 * closed via datalakeCloseFile().  A FileMetaRangeCache is expected to
	 * outlive that (it typically backs reads for the whole lifetime of a
	 * format reader), so load() copies the bytes out immediately instead of
	 * holding a dangling view into the underlying stream's buffer.
	 */
	std::vector<char> data_;

	int64_t off_        = 0;
	int32_t len_        = 0;
	int64_t fileLength_ = 0;
};

}
}
#endif /* DATALAKE_FILEMETARANGECACHE_H */
