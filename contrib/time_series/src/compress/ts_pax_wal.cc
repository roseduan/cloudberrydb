/*-------------------------------------------------------------------------
 *
 * ts_pax_wal.cc
 *    pax::File wrapper that emits ts_rmgr WAL after every PWriteN.
 *    See ts_pax_wal.h for the design rationale.
 *
 * Copyright (c) 2026 HashData Inc.
 * Licensed under Apache License 2.0
 *
 * IDENTIFICATION
 *    contrib/time_series/src/compress/ts_pax_wal.cc
 *
 *-------------------------------------------------------------------------
 */
#include "../include/compress/ts_pax_wal.h"

extern "C" {
#include "postgres.h"
#include "../include/time_series.h"
#include "../include/access/ts_wal.h"
}

#include <utility>

namespace ts {

TsWalRecordingFile::TsWalRecordingFile(std::unique_ptr<pax::File> base,
									   Oid dbid, Oid relid,
									   std::string filename)
	: base_(std::move(base)),
	  dbid_(dbid),
	  relid_(relid),
	  filename_(std::move(filename)),
	  append_offset_(0)
{
	Assert(base_);
	Assert(!filename_.empty());
}

void
TsWalRecordingFile::PWriteN(const void *buf, size_t count, off_t offset)
{
	/* Data first (matches PAX's XLogPaxInsert ordering in orc_writer.cc). */
	base_->PWriteN(buf, count, offset);

	if (count > 0)
	{
		::ts_wal_pax_write(dbid_, relid_, filename_.c_str(),
						   (int64) offset, buf, count);
	}
}

ssize_t
TsWalRecordingFile::PWrite(const void *buf, size_t count, off_t offset)
{
	ssize_t n = base_->PWrite(buf, count, offset);
	if (n > 0)
	{
		::ts_wal_pax_write(dbid_, relid_, filename_.c_str(),
						   (int64) offset, buf, (size_t) n);
	}
	return n;
}

void
TsWalRecordingFile::WriteN(const void *ptr, size_t n)
{
	off_t start = append_offset_;
	base_->WriteN(ptr, n);
	append_offset_ += (off_t) n;

	if (n > 0)
	{
		::ts_wal_pax_write(dbid_, relid_, filename_.c_str(),
						   (int64) start, ptr, n);
	}
}

ssize_t
TsWalRecordingFile::Write(const void *ptr, size_t n)
{
	off_t start = append_offset_;
	ssize_t written = base_->Write(ptr, n);
	if (written > 0)
	{
		append_offset_ += (off_t) written;
		::ts_wal_pax_write(dbid_, relid_, filename_.c_str(),
						   (int64) start, ptr, (size_t) written);
	}
	return written;
}

}  // namespace ts
