/*-------------------------------------------------------------------------
 *
 * fileMetaRangeCache.cpp
 *    Format-agnostic range cache over a Gopher file-metadata blob.
 *
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 *
 * IDENTIFICATION
 *        contrib/datalake_fdw/src/common/fileMetaRangeCache.cpp
 *-------------------------------------------------------------------------
 */
#include "fileMetaRangeCache.h"
#include <cstring>

extern "C" {
#include "postgres.h"
#include "utils/elog.h"
}

/*
 * Host variable for the GUC datalake.enable_file_meta_cache (defined in
 * datalake_fdw.c, registered in Task 15). The same variable also has an
 * extern declaration in rewrLogical.h; it is declared again separately
 * here so that this lightweight shared component does not have to drag
 * in the whole pile of unrelated types in rewrLogical.h
 * (readOption/readLogical/...). The two declarations must agree on type
 * (bool), otherwise the build fails at compile time -- there is no risk
 * of a silent drift between them.
 */
extern bool datalake_enable_file_meta_cache;

/*
 * Companion size limit in kilobytes (datalake.file_meta_max_size). Checked
 * here rather than in each format's adapter so that a format added later
 * inherits it -- there is exactly one place a backfill can originate.
 */
extern int datalake_file_meta_max_size;

namespace Datalake {
namespace Internal {

void FileMetaRangeCache::load(ossFileStream stream, uint32_t producerId, int64_t fileLength)
{
	/*
	 * Clear all state up front so that load() is re-entrant.
	 *
	 * This component is designed to be REUSED ACROSS MULTIPLE FILES (a
	 * single scan walking several ORC/Parquet files under a partition
	 * typically reuses the same reader object). If an early-return branch
	 * left the previous file's hit_/data_/off_/len_ in place, hit_ would
	 * still be true even when the next file's load() misses, and serve()
	 * only looks at hit_ and range containment -- so it would memcpy
	 * bytes from THE PREVIOUS FILE into this file's read result, with no
	 * way for the caller to notice. The relative offset of the trailing
	 * metadata tends to be very close across files of the same kind, so
	 * landing in the same range is anything but rare.
	 */
	hit_       = false;
	tombstone_ = false;
	type_      = 0;
	off_       = 0;
	len_       = 0;
	data_.clear();

	fileLength_ = fileLength;
	if (!datalake_enable_file_meta_cache || stream == NULL)
		return;

	uint32_t    pid;
	int32_t     type;
	int64_t     off;
	const void *buf;
	int32_t     len;

	if (datalakeGetFileMeta(stream, &pid, &type, &off, &buf, &len) != 0)
		return;                     /* miss / backend does not support it */
	if (pid != producerId)
		return;                     /* blob written by a different producer, not ours */
	if (len == 0)
	{
		/*
		 * Tombstone: this file cannot be cached (e.g. too large / format
		 * not eligible), and Gopher is telling us explicitly not to try
		 * backfilling it again. This DOES COUNT AS A HIT in the sense
		 * that hit() still returns false (there is no data to serve()),
		 * but tombstone() returning true makes push() skip outright, so
		 * we never retry backfilling the same pathological file on every
		 * open.
		 */
		tombstone_ = true;
		return;
	}
	/*
	 * Generic validation: the range must lie entirely within the file.
	 * Compare via subtraction rather than addition to avoid overflowing
	 * int64_t when off + len would be computed with a very large off
	 * (real files never get that big, but avoiding the hazard here costs
	 * nothing). Deliberately does NOT validate EOF anchoring -- that is
	 * the job of each format's adapter layer (Avro's metadata sits at the
	 * start of the file).
	 */
	if (off < 0 || len <= 0 || off > fileLength_ - (int64_t) len)
		return;

	/*
	 * Check for null even when the backend reports a hit:
	 * assign(nullptr, nullptr + len) is undefined behavior, and in
	 * practice segfaults -- turning "cache failure degrades silently to a
	 * miss" into a crash of the whole process, which is far worse than
	 * the query failure this feature exists to avoid. The current
	 * implementation just passes the C struct fields through as-is and
	 * does not guarantee this on its own.
	 */
	if (buf == NULL)
		return;

	/*
	 * Copy the bytes out immediately, decoupling them from the stream's
	 * lifetime (see the comment on data_ in the header).
	 */
	data_.assign((const char *) buf, (const char *) buf + len);

	hit_  = true;
	type_ = type;
	off_  = off;
	len_  = len;
}

bool FileMetaRangeCache::serve(void *buf, uint64_t len, uint64_t offset) const
{
	if (!hit_)
		return false;

	uint64_t rangeOff = (uint64_t) off_;
	uint64_t rangeEnd = rangeOff + (uint64_t) len_;   /* off_+len_ <= fileLength_ is already validated by load(), so this cannot overflow */

	if (offset < rangeOff || offset > rangeEnd || len > rangeEnd - offset)
		return false;                                 /* any partial overlap falls back to the source; never stitched together */

	memcpy(buf, data_.data() + (offset - rangeOff), len);
	return true;
}

void FileMetaRangeCache::push(ossFileStream stream, uint32_t producerId, int32_t type,
							   int64_t offset, int32_t len)
{
	if (stream == NULL || hit_ || tombstone_ || !datalake_enable_file_meta_cache)
		return;
	if (offset < 0 || len <= 0 || offset > fileLength_ - (int64_t) len)
		return;

	/*
	 * Client-side size policy: refuse to even send an oversized blob.
	 * int64 arithmetic because MAX_KILOBYTES * 1024 overflows int.
	 *
	 * DEBUG1 rather than silence: "the blob was too big" and "the backfill
	 * never ran" are indistinguishable from the outside, and a hit rate
	 * pinned at zero with no explanation is the hardest failure mode this
	 * feature has.
	 */
	if ((int64_t) len > (int64_t) datalake_file_meta_max_size * 1024)
	{
		elog(DEBUG1, "datalake file-meta-cache: blob %d bytes exceeds "
					 "datalake.file_meta_max_size (%d kB), skipping backfill "
					 "(producer=%u type=%d)",
			 len, datalake_file_meta_max_size, producerId, type);
		return;
	}

	std::vector<char> tmp(len);
	bool readOk = false;
	MemoryContext mcxt = CurrentMemoryContext;

	/*
	 * The backfill read below moves the caller's cursor; remember where it
	 * was so it can be put back (see the contract on push() in the
	 * header). -1 only comes back for a NULL stream, which is already
	 * excluded above.
	 */
	int64_t savedPos = datalakeTellFile(stream);

	/*
	 * datalakeSeekFile() reports failure via a plain -1 return (safe to
	 * check directly), but datalakeReadFile() is written for the primary
	 * read path and elog(ERROR)s on failure -- see fileSystemWrapper.cpp.
	 * Here the read exists only to backfill the cache, so any failure,
	 * PostgreSQL ERROR included, must be swallowed: it must never unwind
	 * past this function and abort the real read that triggered the
	 * backfill.
	 */
	PG_TRY();
	{
		if (datalakeSeekFile(stream, offset) == 0 &&
			datalakeReadFile(stream, tmp.data(), len) == len)
			readOk = true;
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(mcxt);
		FlushErrorState();
	}
	PG_END_TRY();

	/*
	 * Put the cursor back before doing anything else, including on the
	 * failure paths -- an aborted backfill read may well have left it
	 * somewhere in the middle of the range. datalakeSeekFile() reports
	 * failure with -1 and never elog(ERROR)s, so this needs no PG_TRY of
	 * its own, and a failure here is not worth reporting: it can only
	 * happen on a stream the caller's own next read would fail on anyway.
	 */
	if (savedPos >= 0)
		(void) datalakeSeekFile(stream, savedPos);

	if (!readOk)
		return;

	/* datalakePutFileMeta() itself never elog(ERROR)s, so a failure can simply be discarded as-is. */
	int putRc = datalakePutFileMeta(stream, producerId, type, offset, tmp.data(), len);

	/*
	 * Task 16 observability point: whether the backfill actually wrote
	 * the blob. The backfill read itself is a local pread (see the
	 * comment at its call site in orcReadInterface.cpp) and generates no
	 * remote traffic, but whether this PutFileMeta step succeeds is an
	 * entirely separate question -- only an extra DEBUG1 line lets us
	 * tell "read it but failed to store it" apart from "never managed to
	 * read it at all".
	 */
	elog(DEBUG1, "datalake file-meta-cache: backfill %s (producer=%u type=%d offset=%ld len=%d)",
		 putRc == 0 ? "ok" : "failed", producerId, type, (long) offset, len);
}

}
}
