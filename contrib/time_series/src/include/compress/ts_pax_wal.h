/*-------------------------------------------------------------------------
 *
 * ts_pax_wal.h
 *    pax::File wrapper that emits ts_rmgr WAL after every PWriteN.
 *
 *    time_series drives PAX compression through paxformat.so with
 *    options.need_wal=false — PAX's own XLogPaxInsert calls (rmgr id
 *    199) are compiled in but never fire.  This wrapper intercepts
 *    the underlying pax::File and emits an equivalent XLOG_TS_PAX_WRITE
 *    record under the extension's own rmgr (id 200) so mirror
 *    replicas / crash recovery can rebuild the PAX file.
 *
 * Copyright (c) 2026 HashData Inc.
 * Licensed under Apache License 2.0
 *
 * IDENTIFICATION
 *    contrib/time_series/src/include/compress/ts_pax_wal.h
 *
 *-------------------------------------------------------------------------
 */
#pragma once

#include "storage/file_system.h"

extern "C" {
#include "postgres.h"
#include "storage/relfilenode.h"
}

#include <memory>
#include <string>

namespace ts {

/*
 * Forward every read to the wrapped file; forward every write to the
 * wrapped file first, then emit an XLOG_TS_PAX_WRITE record covering
 * the bytes just written.  Matches PAX's own "data first, WAL second"
 * ordering (see orc_writer.cc XLogPaxInsert call sites).
 *
 * `filename` is the basename passed to XLog — redo re-derives the
 * parent directory from dbid + relid.
 */
class TsWalRecordingFile final : public pax::File
{
public:
	TsWalRecordingFile(std::unique_ptr<pax::File> base,
					   Oid dbid, Oid relid, std::string filename);
	~TsWalRecordingFile() override = default;

	/* Reads: pure delegation. */
	ssize_t Read(void *ptr, size_t n) const override
		{ return base_->Read(ptr, n); }
	ssize_t PRead(void *buf, size_t count, off_t offset) const override
		{ return base_->PRead(buf, count, offset); }
	void ReadN(void *ptr, size_t n) const override
		{ base_->ReadN(ptr, n); }
	void PReadN(void *buf, size_t count, off_t offset) const override
		{ base_->PReadN(buf, count, offset); }
	void ReadBatch(const std::vector<pax::IORequest> &requests) const override
		{ base_->ReadBatch(requests); }

	/*
	 * Writes: delegate then WAL.
	 * All four write APIs record what got written and at what offset.
	 */
	ssize_t Write(const void *ptr, size_t n) override;
	ssize_t PWrite(const void *buf, size_t count, off_t offset) override;
	void WriteN(const void *ptr, size_t n) override;
	void PWriteN(const void *buf, size_t count, off_t offset) override;

	/* Lifecycle: pure delegation. */
	void Flush() override { base_->Flush(); }
	void Delete() override { base_->Delete(); }
	void Close() override { base_->Close(); }
	size_t FileLength() const override { return base_->FileLength(); }
	std::string GetPath() const override { return base_->GetPath(); }
	std::string DebugString() const override { return base_->DebugString(); }

private:
	std::unique_ptr<pax::File>	base_;
	Oid							dbid_;
	Oid							relid_;
	std::string					filename_;
	off_t						append_offset_;		/* tracks non-positional Write() offset */
};

}  // namespace ts
