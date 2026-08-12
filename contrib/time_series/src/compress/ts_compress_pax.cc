/*-------------------------------------------------------------------------
 *
 * ts_compress_pax.cc
 *    PAX storage bridge for time_series chunk compression.
 *
 *    Uses the PAX MicroPartitionWriter/Reader API directly (linked
 *    against pax.so) to write and read columnar ORC files.
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 * IDENTIFICATION
 *    contrib/time_series/src/compress/ts_compress_pax.cc
 *
 *-------------------------------------------------------------------------
 */

extern "C" {
#include "postgres.h"
#include "access/htup_details.h"
#include "catalog/pg_type_d.h"
#include "executor/tuptable.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "utils/rel.h"

#include <sys/stat.h>
}

#include <memory>
#include <string>
#include <vector>

#include "comm/guc.h"
#include "exceptions/CException.h"
#include "storage/local_file_system.h"
#include "storage/micro_partition.h"
#include "storage/micro_partition_file_factory.h"
#include "storage/columns/pax_compress.h"
#include "storage/filter/pax_filter.h"

#include "../include/compress/ts_pax_wal.h"

extern "C" {
#include "../include/compress/ts_compress.h"
#include "../include/time_series.h"		/* ts_wal_pax_create_dir etc. */
#include "../include/access/ts_wal.h"
}

using pax::ColumnEncoding_Kind;
using pax::LocalFileSystem;
using pax::MicroPartitionFileFactory;
using pax::MicroPartitionReader;
using pax::MicroPartitionWriter;
using pax::Singleton;

/*
 * C++ static destructor workaround + bgworker exit-code transparency.
 *
 * libpaxformat.so is linked and loaded at postmaster startup (via
 * shared_preload_libraries).  Its C++ global statics (e.g.
 * pax::min_max_opers) register atexit destructors.  When backend
 * processes call exit(), these destructors try to free memory already
 * cleaned up by PostgreSQL's proc_exit, causing double-free crashes.
 *
 * Naive fix: register an atexit() handler that calls _exit() to skip
 * C++ atexit destructors entirely.  Safe because proc_exit already
 * completed all PG cleanup before reaching exit().
 *
 * BUT a literal `_exit(0)` silently demotes every bgworker's
 * proc_exit(non-zero) to "clean shutdown".  PG's postmaster decides
 * whether to auto-restart a bgworker based on its exit code, so this
 * meant `pg_terminate_backend(launcher_pid)` left the launcher dead
 * forever — fatal for the CAGG scheduler infrastructure.
 *
 * Full fix (this file): two-step capture.
 *   1. on_proc_exit callback (pax_capture_exit_code) stashes the int
 *      passed to proc_exit() into pax_saved_exit_code.
 *   2. atexit callback (pax_atexit_cb) reads that variable and calls
 *      _exit(pax_saved_exit_code) — preserves the original code while
 *      still bypassing C++ destructors.
 *
 * atexit is registered once in postmaster (inherited across fork by
 * libc) but on_proc_exit registrations are NOT inherited, so each
 * bgworker entry point must call
 * ts_pax_register_per_backend_exit_capture() to install its own copy
 * of the capture callback.
 */
static bool pax_exit_registered = false;

/*
 * Cached exit code, captured by on_proc_exit during proc_exit_prepare
 * and read by pax_atexit_cb at the actual exit() boundary.
 *
 * Without this two-step capture the atexit callback would have no way
 * to recover the int passed to proc_exit() — atexit handlers receive
 * no arguments.  PG bgworkers rely on non-zero exit codes to signal
 * "crash → please restart" to the postmaster; if we hard-coded
 * _exit(0) here, every bgworker (launcher + per-DB scheduler) would
 * be silently demoted to "clean shutdown, do not restart" after any
 * pg_terminate_backend(), breaking bgw_launcher TEST-03/04/14 and
 * any production reliance on automatic respawn.
 */
static int pax_saved_exit_code = 0;

/*
 * pax_capture_exit_code — on_proc_exit callback.  PG calls this from
 * proc_exit_prepare with the int that was passed to proc_exit(); we
 * stash it so the atexit handler below can use it.
 */
static void
pax_capture_exit_code(int code, Datum arg)
{
	pax_saved_exit_code = code;
}

/*
 * pax_atexit_cb — registered via atexit(), runs before C++ destructors.
 * Calls _exit(saved_code) to skip C++ atexit destructors entirely while
 * preserving the exit-code semantics callers expected from proc_exit().
 *
 * We use atexit() (in addition to on_proc_exit above) because:
 * - on_proc_exit is per-process and not inherited across fork()
 * - _PG_init runs in postmaster, but startup / aux processes are forked
 *   from postmaster and inherit the loaded .so but not on_proc_exit
 *   callbacks
 * - atexit() callbacks ARE inherited across fork(), so this hook fires
 *   even in workers that never run our _PG_init body
 *
 * The default `pax_saved_exit_code = 0` is used when a worker exits
 * outside the proc_exit path (e.g. abort()/SIGKILL) — those bypass
 * atexit entirely, so 0 here only matters for the rare "clean exit
 * but proc_exit was never called" case, which mirrors exit(0).
 */
static void
pax_atexit_cb(void)
{
	_exit(pax_saved_exit_code);
}

/*
 * ts_pax_register_exit_hook — public entry point for _PG_init.
 * Must be called once during shared_preload_libraries loading.
 */
void
ts_pax_register_exit_hook(void)
{
	if (pax_exit_registered)
		return;
	on_proc_exit(pax_capture_exit_code, (Datum) 0);
	atexit(pax_atexit_cb);
	pax_exit_registered = true;
}

/*
 * ts_pax_register_per_backend_exit_capture — call from each bgworker's
 * entry function (bgw_launcher_main, bgw_scheduler_main, ...) so the
 * worker's own proc_exit path runs pax_capture_exit_code and updates
 * pax_saved_exit_code before atexit fires.
 *
 * Needed because on_proc_exit registrations made inside postmaster's
 * _PG_init are NOT inherited by forked workers (each backend has its
 * own on_proc_exit list, initialised empty).  The pax_atexit_cb itself
 * IS inherited because C's atexit() is shared across fork(), but it
 * fires with pax_saved_exit_code = 0 unless the worker also registers
 * the capture callback in its own process.
 */
extern "C" void
ts_pax_register_per_backend_exit_capture(void)
{
	on_proc_exit(pax_capture_exit_code, (Datum) 0);
}

/* ----------------------------------------------------------------
 *		Per-column encoding picker.
 *
 *		Default mapping by attribute type:
 *
 *		    INT2/4/8, DATE, TIMESTAMP, TIMESTAMPTZ → DELTA_DELTA
 *		    FLOAT4, FLOAT8                         → GORILLA
 *		    BOOL                                   → BOOL_COMPRESS
 *		    text-like / other varlena              → DICTIONARY
 *		    everything else                        → ZSTD
 *
 *		PAX architecture note (pax_encoding_column.cc:84-103): a column
 *		uses streaming encoder OR block compressor, never both — the
 *		picked kind is the only layer.  Streaming encoders that produce
 *		already-entropy-coded output (DELTA_DELTA, GORILLA, BOOL) skip
 *		the optional block compressor; ones that emit raw byte sequences
 *		(DICTIONARY) can be paired with a block compressor only after
 *		PAX gains "encoder + compressor" chaining.
 *
 *		Bench note (bench/run_ts.sh, 21.6M rows of regular weather data):
 *		this picker scores ≈2.75x compression vs ≈4.11x for ZSTD-everywhere
 *		on this specific dataset.  ZSTD wins here because timestamps are
 *		perfectly equidistant and ZSTD's dictionary catches the binary
 *		redundancy that DELTA_DELTA + Simple8b-RLE leaves behind on a
 *		single layer.  Real-world data with irregular timestamp jitter
 *		closes the gap; high-cardinality varlena columns benefit most
 *		from a future two-layer (DICTIONARY + block compressor) path.
 * ----------------------------------------------------------------
 */
static ColumnEncoding_Kind
pick_column_encoding(Form_pg_attribute attr)
{
	switch (attr->atttypid)
	{
		/*
		 * Fixed-width integer-like (1/2/4/8 bytes) → DELTA_DELTA.
		 * Decoder is instantiated for int8/16/32/64
		 * (pax_deltadelta_encoding.cc:295) so all four widths decode.
		 */
		case INT2OID:
		case INT4OID:
		case INT8OID:
		case DATEOID:
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
		case OIDOID:
			return ColumnEncoding_Kind::ColumnEncoding_Kind_DELTA_DELTA;

		/*
		 * Fixed-width floats → GORILLA (XOR-based).  Decoder
		 * instantiated for int32/int64; float bytes share the same
		 * decoder under bitwise-identical layout.
		 */
		case FLOAT4OID:
		case FLOAT8OID:
			return ColumnEncoding_Kind::ColumnEncoding_Kind_GORILLA;

		/* Boolean → bit-packed encoder. */
		case BOOLOID:
			return ColumnEncoding_Kind::ColumnEncoding_Kind_BOOL_COMPRESS;

		/*
		 * NUMERIC → ZSTD.  PAX has a built-in NUMERIC column type
		 * (PaxDecimalColumn in orc_writer.cc:211-217) that lays out
		 * offsets-stream + data-stream + null bitmap; picking
		 * COMPRESS_ZSTD here applies a block compressor on the data
		 * stream, which is the closest single-layer analogue we have
		 * until PAX gains encoder + compressor chaining.
		 */
		case NUMERICOID:
			return ColumnEncoding_Kind::ColumnEncoding_Kind_COMPRESS_ZSTD;

		/*
		 * Binary / structured varlena → ZSTD.  These types rarely share
		 * exact bytes across rows even when logically equivalent, so
		 * DICTIONARY's hash-based matching has nothing to collapse;
		 * ZSTD block compression on the data stream catches the
		 * structural redundancy instead.  10 GB bench (§9.4) shows
		 * jsonb adds only ~6 MB to the compressed footprint.
		 */
		case JSONBOID:
		case BYTEAOID:
		case JSONOID:
		case XMLOID:
			return ColumnEncoding_Kind::ColumnEncoding_Kind_COMPRESS_ZSTD;

		default:
		{
			/*
			 * Other varlena (text, varchar, bpchar, citext, domains
			 * over varlena, ...) → DICTIONARY.  In time-series
			 * workloads these are typically low-cardinality tags
			 * (host / region / severity) where DICTIONARY's index
			 * stream is much smaller than the underlying bytes.
			 *
			 * Non-varlena fallback (MONEY, INTERVAL, fixed
			 * composites): ZSTD on the data stream.
			 */
			if (!attr->attbyval && attr->attlen == -1)
				return ColumnEncoding_Kind::ColumnEncoding_Kind_DICTIONARY;
			return ColumnEncoding_Kind::ColumnEncoding_Kind_COMPRESS_ZSTD;
		}
	}
}

/* ----------------------------------------------------------------
 *		Writer
 * ----------------------------------------------------------------
 */

struct TSPaxWriterImpl
{
	std::unique_ptr<MicroPartitionWriter>	writer;
	TupleDesc								tupdesc;
	int64									ntuples;
	int32									ngroups;
	std::string								filepath;
	std::string								toast_filepath;
};

extern "C" {

TSPaxWriter
ts_pax_writer_open(const char *filepath, const char *toast_filepath,
				   TupleDesc tupdesc,
				   Oid dbid, Oid relid,
				   const int *minmax_col_idxs, int n_minmax)
{
	TSPaxWriterImpl *w = nullptr;

	/*
	 * Declared here, above PG_TRY, rather than as locals inside it.
	 * elog(ERROR) unwinds via siglongjmp, which -- unlike a real C++
	 * exception -- does not run the destructors of C++ locals declared
	 * inside the PG_TRY block.  A file/toast_file assigned INSIDE
	 * PG_TRY would leak its open fd if any later step in this function
	 * raises before CreateMicroPartitionWriter takes ownership of
	 * them.  Declaring them here means PG_CATCH (an ordinary code path
	 * in this same C++ function, not a stack unwind) can call
	 * .reset() on them as a normal function call, which correctly and
	 * safely releases whatever they still hold -- a no-op if the
	 * factory call below already moved them out.
	 */
	std::unique_ptr<pax::File> file;
	std::unique_ptr<pax::File> toast_file;

	PG_TRY();
	{
	/*
	 * fs->Open / fs->CreateDirectory / CreateMicroPartitionWriter can
	 * all raise a real C++ exception (cbdb::CException, e.g.
	 * LocalFileSystem::Open's CBDB_CHECK on a failed open(2) --
	 * permission denied, ENOSPC, fd exhaustion) rather than going
	 * through elog/ereport.  PG_TRY/PG_CATCH is sigsetjmp/siglongjmp
	 * based and does not intercept a C++ throw; left unwrapped, such
	 * an exception would cross the extern "C" boundary into
	 * ts_compress.c with no handler, and the C++ runtime would call
	 * std::terminate() -- aborting the whole backend process instead
	 * of raising a clean, recoverable ERROR.  CBDB_TRY/CBDB_CATCH_DEFAULT
	 * is PAX's own convention for exactly this boundary (see the
	 * "only used by the outer c++ code called by C" comment on
	 * CBDB_CATCH_DEFAULT in exceptions/CException.h): a real C++
	 * catch, so stack unwinding runs normally, that re-raises via
	 * ereport(ERROR) -- which PG_TRY/PG_CATCH (the enclosing block
	 * here) can then correctly catch.
	 */
	CBDB_TRY();
	{
		std::string path(filepath);

		/*
		 * Extract the basename — this is what XLOG_TS_PAX_WRITE stores as
		 * the filename field.  Redo derives the parent dir from (dbid,
		 * relid), so the WAL is cluster-move-friendly.
		 *
		 * Record the name we ACTUALLY write to, ".new" suffix and all.
		 * This code used to strip ".new" so that redo would materialise
		 * the final name directly and standbys could skip the two-phase
		 * dance -- which quietly turned crash recovery into a data
		 * destroyer.  Redo is physical and commit-blind: it replays every
		 * flushed record whether or not the owning transaction committed.
		 * With the final name in the record, a recompress killed
		 * mid-write had its half-finished bytes replayed straight onto
		 * the live file, whose offset-0 record opens it O_TRUNC -- so
		 * recovery truncated data the PREVIOUS, successful compress had
		 * already committed, while that compress's ts_compressed_chunk
		 * row kept pointing there.  Not an orphan: a catalog-referenced
		 * ruin that every later scan walked into.
		 *
		 * Keeping ".new" confines redo to the temporary file.  The
		 * promotion to the live name travels as its own
		 * XLOG_TS_PAX_RENAME record, emitted by ts_compress.c only after
		 * the writer closed successfully, so an interrupted recompress
		 * leaves nothing but an orphan ".new" while the live file keeps
		 * serving the data it was committed with.
		 *
		 * Those orphans are currently never reclaimed -- the compress
		 * path registers no pending removal and unlinks nothing on error,
		 * so a ".new" (and its ".toast.new" sibling) survives until the
		 * next compress of the same chunk truncates it.  Harmless but
		 * untidy; worth a follow-up, and NOT something redo should paper
		 * over by writing to the live name instead.
		 */
		std::string basename;
		{
			auto slash_pos = path.rfind('/');
			basename = (slash_pos == std::string::npos)
				? path
				: path.substr(slash_pos + 1);
		}

		/* Ensure parent directory exists — and WAL its creation. */
		{
			auto slash_pos = path.rfind('/');
			if (slash_pos != std::string::npos)
			{
				std::string dir = path.substr(0, slash_pos);
				auto *fs = Singleton<LocalFileSystem>::GetInstance();
				fs->CreateDirectory(dir);
				/*
				 * Emit CREATE_DIR unconditionally: CreateDirectory is
				 * idempotent on the primary (mkdir + EEXIST tolerated),
				 * and the redo handler mirrors that (mkdir + EEXIST
				 * tolerated).  Multiple records per xact for the same
				 * relid are harmless.
				 */
				::ts_wal_pax_create_dir(dbid, relid);
			}
		}

		/* Open file for writing — wrap it so every PWriteN emits WAL. */
		auto *fs = Singleton<LocalFileSystem>::GetInstance();
		auto raw_file = fs->Open(path, pax::fs::kWriteWithTruncMode);
		file = std::make_unique<ts::TsWalRecordingFile>(
			std::move(raw_file), dbid, relid, basename);

		/*
		 * Toast sidecar — PAX writes external-toasted varlena values to
		 * a "<path>.toast" sibling whenever pax_enable_toast (default
		 * on, PGC_USERSET) is set and a column's attstorage isn't
		 * PLAIN: nothing here rejects varlena columns, and
		 * pick_column_encoding above assigns real encodings to text/
		 * jsonb/bytea, so this is NOT the empty-by-construction file an
		 * earlier version of this comment assumed.  OrcWriter also
		 * Assert(toast_file_) unconditionally whenever the schema has
		 * any varlena column, whether or not a row actually externalises
		 * a value this run, so we must provide a handle regardless.
		 *
		 * toast_filepath is supplied by the caller (ts_compress.c), not
		 * derived here by string surgery on `path` — this file already
		 * has one cautionary tale (the removed ".new"-stripping logic
		 * above) about reconstructing a live/temp name from another
		 * name's suffix.  Caller keeps it simple: toast_filepath is
		 * always "<live main file>.toast.new", so its own basename
		 * follows the same "<final-name>.new" convention as the main
		 * file, and ts_wal_pax_rename (see ts_compress.c) can promote
		 * it with the exact same "<name>.new" -> "<name>" logic, now
		 * carried in the SAME record as the main file's promotion —
		 * see ts_wal.h's comment on ts_wal_pax_rename for why splitting
		 * that into two independent records would be unsafe.
		 *
		 * Wrap the toast file too so its bytes ride the same
		 * XLOG_TS_PAX_WRITE stream.
		 */
		std::string toast_path(toast_filepath);
		std::string toast_basename;
		{
			auto slash_pos = toast_path.rfind('/');
			toast_basename = (slash_pos == std::string::npos)
				? toast_path
				: toast_path.substr(slash_pos + 1);
		}
		auto raw_toast_file = fs->Open(toast_path, pax::fs::kWriteWithTruncMode);
		toast_file = std::make_unique<ts::TsWalRecordingFile>(
			std::move(raw_toast_file), dbid, relid, toast_basename);

		/* Build writer options */
		MicroPartitionWriter::WriterOptions options;
		options.rel_tuple_desc = CreateTupleDescCopy(tupdesc);
		options.rel_oid = relid;
		options.group_limit = TS_MAX_TUPLES_PER_GROUP;

		/*
		 * Per-column encoding chosen by attribute type — see
		 * pick_column_encoding() above for the full mapping.
		 * compress_level only meaningful for ZSTD/ZLIB/LZ4; the
		 * specialised streaming encoders reject non-zero levels
		 * (paxc_rel_options.cc:142-156).
		 */
		for (int i = 0; i < tupdesc->natts; i++)
		{
			Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
			ColumnEncoding_Kind kind = pick_column_encoding(attr);
			int level = (kind == ColumnEncoding_Kind::ColumnEncoding_Kind_COMPRESS_ZSTD ||
						 kind == ColumnEncoding_Kind::ColumnEncoding_Kind_COMPRESS_ZLIB ||
						 kind == ColumnEncoding_Kind::ColumnEncoding_Kind_COMPRESS_LZ4)
				? 1 : 0;
			options.encoding_opts.emplace_back(kind, level);
		}

		/*
		 * Per-group min/max: only for caller-selected columns (usually
		 * segmentby + orderby).  Tracking every column adds per-row op
		 * lookups + datum copies that dominate compression time.
		 */
		for (int i = 0; i < n_minmax; i++)
			options.enable_min_max_col_idxs.push_back(minmax_col_idxs[i]);

		/* Create writer */
		auto writer = MicroPartitionFileFactory::CreateMicroPartitionWriter(
			options, std::move(file), std::move(toast_file));

		w = new TSPaxWriterImpl();
		w->writer = std::move(writer);
		w->tupdesc = tupdesc;
		w->ntuples = 0;
		w->ngroups = 0;
		w->filepath = path;
		w->toast_filepath = toast_path;
	}
	CBDB_CATCH_DEFAULT();
	CBDB_END_TRY();
	}
	PG_CATCH();
	{
		/*
		 * Ordinary function calls here, not an implicit stack unwind —
		 * safe even though we just arrived via siglongjmp.  No-ops if
		 * the factory call already moved file/toast_file out.
		 */
		file.reset();
		toast_file.reset();
		if (w)
		{
			delete w;
			w = nullptr;
		}
		PG_RE_THROW();
	}
	PG_END_TRY();

	return (TSPaxWriter) w;
}

void
ts_pax_writer_write_tuple(TSPaxWriter handle, TupleTableSlot *slot)
{
	TSPaxWriterImpl *w = (TSPaxWriterImpl *) handle;

	if (!TTS_IS_VIRTUAL(slot))
		slot_getallattrs(slot);

	/*
	 * WriteTuple can raise a cbdb::CException (encoding error, OOM,
	 * ...); convert it to ereport(ERROR) here rather than let it cross
	 * the extern "C" boundary uncaught (see the longer comment on this
	 * pattern in ts_pax_writer_open).  This is called once per row, so
	 * a PG_TRY here too would be too costly -- the caller
	 * (do_compress_one_chunk) wraps its whole scan-and-write loop in
	 * one PG_TRY and calls ts_pax_writer_abort on catch, which is
	 * where the resulting ereport(ERROR)'s siglongjmp actually lands.
	 */
	CBDB_TRY();
	{
		w->writer->WriteTuple(slot);
		w->ntuples++;
	}
	CBDB_CATCH_DEFAULT();
	CBDB_END_TRY();
}

void
ts_pax_writer_flush(TSPaxWriter handle)
{
	TSPaxWriterImpl *w = (TSPaxWriterImpl *) handle;

	CBDB_TRY();
	{
		w->writer->Flush();
		w->ngroups++;
	}
	CBDB_CATCH_DEFAULT();
	CBDB_END_TRY();
}

int64
ts_pax_writer_close(TSPaxWriter handle, int64 *out_ntuples, int32 *out_ngroups)
{
	TSPaxWriterImpl *w = (TSPaxWriterImpl *) handle;
	int64			file_size = 0;
	struct stat		st;

	/*
	 * Close() finalises the ORC stripe/footer and can raise a
	 * cbdb::CException (disk full, encoding error flushing the last
	 * group, ...).  Convert to ereport(ERROR) rather than let it cross
	 * the extern "C" boundary uncaught -- see ts_pax_writer_open's
	 * comment.  If this raises, everything below (including `delete
	 * w`) is skipped; the caller's own PG_CATCH is expected to call
	 * ts_pax_writer_abort(handle) to release w in that case (see
	 * do_compress_one_chunk).
	 */
	CBDB_TRY();
	{
		w->writer->Close();
	}
	CBDB_CATCH_DEFAULT();
	CBDB_END_TRY();

	/*
	 * Real on-disk size, not PhysicalSize() — the latter is PAX's running
	 * estimate (accumulates *raw* uncompressed bytes for varlena columns)
	 * used internally to trigger micro-partition rotation, not the byte
	 * count after the ORC stripe footer + per-column block compressor.
	 * Reporting that estimate as compressed_size made jsonb/text chunks
	 * appear 100-300x larger than the actual file.
	 */
	if (stat(w->filepath.c_str(), &st) == 0)
		file_size = (int64) st.st_size;

	/* PAX writes a sidecar `.toast` file when any varlena value gets
	 * external-toasted; include it so compressed_size matches du. */
	if (stat(w->toast_filepath.c_str(), &st) == 0)
		file_size += (int64) st.st_size;

	if (out_ntuples)
		*out_ntuples = w->ntuples;
	if (out_ngroups)
		*out_ngroups = w->ngroups;

	delete w;
	return file_size;
}

void
ts_pax_writer_abort(TSPaxWriter handle)
{
	TSPaxWriterImpl *w = (TSPaxWriterImpl *) handle;

	if (w == nullptr)
		return;

	/*
	 * Deliberately skip writer->Close(): that finalises the ORC
	 * stripe/footer and assumes a consistent, fully-written file --
	 * not safe to attempt from an error path where the last
	 * WriteTuple/Flush may have raised partway through.  The
	 * unique_ptr destructor chain (writer -> its wrapped main/toast
	 * TsWalRecordingFile) still runs normally here (this is a plain
	 * function call, not a stack unwind) and releases the underlying
	 * fds.  Caller (do_compress_one_chunk) is responsible for calling
	 * this from its own PG_CATCH -- see the header comment on
	 * ts_pax_writer_abort for why ts_pax_writer_write_tuple /
	 * ts_pax_writer_flush aren't individually PG_TRY-protected.
	 */
	delete w;
}

/* ----------------------------------------------------------------
 *		Reader
 * ----------------------------------------------------------------
 */

struct TSPaxReaderImpl
{
	std::unique_ptr<MicroPartitionReader>	reader;
	TupleDesc								tupdesc;
};

TSPaxReader
ts_pax_reader_open(const char *filepath, TupleDesc tupdesc)
{
	return ts_pax_reader_open_with_proj(filepath, tupdesc, nullptr);
}

TSPaxReader
ts_pax_reader_open_with_proj(const char *filepath, TupleDesc tupdesc,
							 const bool *proj_bitmap)
{
	return ts_pax_reader_open_filtered(filepath, nullptr, proj_bitmap, nullptr);
}

TSPaxReader
ts_pax_reader_open_filtered(const char *filepath, Relation rel,
							const bool *proj_bitmap, List *quals)
{
	TSPaxReaderImpl *r = nullptr;
	TupleDesc		 tupdesc = rel ? RelationGetDescr(rel) : nullptr;

	/*
	 * See the comment on the same pattern in ts_pax_writer_open: these
	 * must live above PG_TRY so PG_CATCH can release them explicitly
	 * (siglongjmp skips destructors of PG_TRY-local C++ objects).
	 */
	std::unique_ptr<pax::File> file;
	std::unique_ptr<pax::File> toast_file;
	std::shared_ptr<pax::PaxFilter> filter;

	PG_TRY();
	{
	/* See the comment in ts_pax_writer_open on why this CBDB_TRY layer
	 * is required: fs->Open / CreateMicroPartitionReader can raise a
	 * real cbdb::CException, which plain PG_TRY/PG_CATCH cannot catch. */
	CBDB_TRY();
	{
		std::string path(filepath);
		auto *fs = Singleton<LocalFileSystem>::GetInstance();
		file = fs->Open(path, pax::fs::kReadMode);

		/*
		 * Sibling toast file, if do_compress_one_chunk ever promoted one
		 * for this segment (see ts_wal_pax_rename's toast_filename arg).
		 * Absent in the common case -- no row in this chunk needed PAX's
		 * own external-toast storage -- and that is not an error: a
		 * missing toast file just means every stripe's toastlength()
		 * will read back as 0, so OrcFormatReader never dereferences
		 * toast_file_.  Only open it when present; passing a valid but
		 * pointless handle for every read would leave one more fd open
		 * per scan for no benefit.
		 */
		{
			std::string toast_path = path + ".toast";
			struct stat st;

			if (stat(toast_path.c_str(), &st) == 0)
				toast_file = fs->Open(toast_path, pax::fs::kReadMode);
		}

		MicroPartitionReader::ReaderOptions options;
		options.tuple_desc = tupdesc;

		/*
		 * Build a PaxFilter when we need column projection or sparse filter.
		 * Both are optional; when neither is requested, skip the filter
		 * object so the reader takes the no-filter fast path.
		 */
		bool want_sparse = (rel != nullptr && quals != nullptr &&
							pax::pax_enable_sparse_filter);

		if (proj_bitmap != nullptr || want_sparse)
		{
			filter = std::make_shared<pax::PaxFilter>();

			if (proj_bitmap != nullptr)
			{
				std::vector<bool> proj_vec(tupdesc->natts);
				for (int i = 0; i < tupdesc->natts; i++)
					proj_vec[i] = proj_bitmap[i];
				filter->SetColumnProjection(std::move(proj_vec));
			}

			/*
			 * Sparse filter: parse WHERE clauses into PFTNode AST.  At group
			 * read time, PAX compares per-column min/max with the predicate
			 * and skips groups that cannot match.
			 */
			if (want_sparse)
				filter->InitSparseFilter(rel, quals, nullptr, 0);

			options.filter = filter;
		}

		auto reader = MicroPartitionFileFactory::CreateMicroPartitionReader(
			options, pax::FLAGS_EMPTY, std::move(file), std::move(toast_file));

		r = new TSPaxReaderImpl();
		r->reader = std::move(reader);
		r->tupdesc = tupdesc;
	}
	CBDB_CATCH_DEFAULT();
	CBDB_END_TRY();
	}
	PG_CATCH();
	{
		file.reset();
		toast_file.reset();
		filter.reset();
		if (r)
		{
			delete r;
			r = nullptr;
		}
		PG_RE_THROW();
	}
	PG_END_TRY();

	return (TSPaxReader) r;
}

bool
ts_pax_reader_next(TSPaxReader handle, TupleTableSlot *slot)
{
	TSPaxReaderImpl *r = (TSPaxReaderImpl *) handle;
	bool			found;

	ExecClearTuple(slot);

	/*
	 * ReadTuple can raise a cbdb::CException (corrupt group, decode
	 * error, ...); convert to ereport(ERROR) here -- see the comment
	 * on this pattern in ts_pax_writer_open.  Called once per row, so
	 * (like the writer side) no PG_TRY here; callers that need cleanup
	 * on error wrap their own read loop and call ts_pax_reader_abort.
	 */
	CBDB_TRY();
	{
		found = r->reader->ReadTuple(slot);
	}
	CBDB_CATCH_DEFAULT();
	CBDB_END_TRY();

	if (found)
		ExecStoreVirtualTuple(slot);

	return found;
}

void
ts_pax_reader_close(TSPaxReader handle)
{
	TSPaxReaderImpl *r = (TSPaxReaderImpl *) handle;

	/*
	 * Close() can raise; convert to ereport(ERROR) -- see
	 * ts_pax_writer_open's comment.  If this raises, `delete r` below
	 * is skipped; the caller is expected to have its own PG_CATCH
	 * calling ts_pax_reader_abort(handle) if it needs that covered
	 * (mirrors ts_pax_writer_close's contract).
	 */
	CBDB_TRY();
	{
		r->reader->Close();
	}
	CBDB_CATCH_DEFAULT();
	CBDB_END_TRY();

	delete r;
}

void
ts_pax_reader_abort(TSPaxReader handle)
{
	TSPaxReaderImpl *r = (TSPaxReaderImpl *) handle;

	if (r == nullptr)
		return;

	/* See ts_pax_writer_abort's comment; same rationale. */
	delete r;
}

}  /* extern "C" */
