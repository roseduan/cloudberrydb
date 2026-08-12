/*-------------------------------------------------------------------------
 *
 * ts_wal.c
 *    Custom WAL resource manager for the time_series extension.
 *
 *    Records carry chunk_number in the rmgr body so the WAL block-reference
 *    header (whose forknum field is 4 bits) can stay untouched.  Full-page
 *    images, when needed, are registered via XLogRegisterBlock under a
 *    MAIN_FORKNUM placeholder — that routes the page through PG's standard
 *    hole-elimination + wal_compression machinery, and redo restores it
 *    via RestoreBlockImage.  Incremental records skip the block reference
 *    entirely and carry just the tuple bytes.
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 * IDENTIFICATION
 *    contrib/time_series/src/access/ts_wal.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/heapam_xlog.h"
#include "access/htup_details.h"
#include "access/xloginsert.h"
#include "access/xlogreader.h"
#include "access/xlog_internal.h"
#include "access/xlogutils.h"
#include "catalog/catalog.h"
#include "commands/tablespace.h"
#include "storage/bufmgr.h"
#include "storage/fd.h"
#include "utils/rel.h"
#include "utils/wait_event.h"

#include "../include/time_series.h"
#include "../include/access/ts_wal.h"

/*
 * PAX sidecar directory layout — must match the format used by
 * compress/ts_compress.c (TS_PAX_RELDIR_FMT) so redo lands on the
 * same file paths the primary wrote.
 */
#define TS_PAX_RELDIR_FMT_LOCAL		"base/%u/ts_compressed/%u"

/* Custom Resource Manager ID (128-255 range for extensions) */
#define TS_RMGR_ID				200

/*
 * Op codes occupy the lower bits of the high nibble (mask 0x70); the high
 * bit (0x80) is reserved for the page-reinit flag.  Same convention as
 * heap (see XLOG_HEAP_INIT_PAGE).
 */
#define XLOG_TS_INSERT			0x00
#define XLOG_TS_MULTI_INSERT	0x10
#define XLOG_TS_FORK_TRUNCATE	0x20
#define XLOG_TS_PAX_CREATE_DIR	0x30	/* mkdir ts_compressed/<relid>/ */
#define XLOG_TS_PAX_WRITE		0x40	/* pwrite bytes into a PAX file  */
#define XLOG_TS_PAX_REMOVE_DIR	0x50	/* rmtree ts_compressed/<relid>/ */
#define XLOG_TS_PAX_RENAME		0x60	/* rename <file>.new -> <file>   */
#define XLOG_TS_OPMASK			0x70
#define XLOG_TS_INIT_PAGE		0x80	/* redo re-inits the page from scratch */

/*
 * WAL record structure for single time_series INSERT.
 *
 * chunk_number lives in the rmgr payload rather than the WAL block-reference
 * header — the upstream header's forknum field is only 4 bits, while our
 * chunks occupy ForkNumber values up to 65535.  When an FPI is needed we
 * still let PG own the image: XLogRegisterBlock(block_id=0) under a
 * MAIN_FORKNUM placeholder routes the page through XLogRecordAssemble,
 * which applies hole elimination and wal_compression for us; redo pulls
 * the (decompressed, hole-restored) image back via RestoreBlockImage.
 *
 * Body layout (only when no FPI rides under block_id 0):
 *   xl_ts_insert
 *   xl_heap_header                       (5 bytes — infomask2, infomask, hoff)
 *   tuple data MINUS SizeofHeapTupleHeader bytes (bitmap [+ pad] [+ oid] + data)
 *
 * Redo reconstructs the full HeapTupleHeader from the WAL record's xid
 * (xmin), FirstCommandId (cmin), and (blkno, offnum) (self ctid).  Matches
 * heap_insert / heap_xlog_insert.
 *
 * The INIT_PAGE flag lives in the rmgr info byte (XLOG_TS_INIT_PAGE), not
 * here, matching heap.  FPI presence is detected via XLogRecHasBlockImage().
 */
typedef struct xl_ts_insert
{
	RelFileNode  rnode;			/* relation that owns the chunk page */
	uint32		chunk_number;	/* fork number = chunk identity (in body) */
	BlockNumber blkno;			/* block within the chunk fork */
	OffsetNumber offnum;		/* offset within page */
} xl_ts_insert;

#define SizeOfTSInsert	(offsetof(xl_ts_insert, offnum) + sizeof(OffsetNumber))

/*
 * WAL record structure for multi-INSERT (multiple tuples in one page).
 *
 * Body layout when no FPI:
 *   xl_ts_multi_insert
 *   OffsetNumber offsets[ntuples]      (only when !INIT_PAGE; init lays
 *                                       tuples out at FirstOffsetNumber..)
 *   for each tuple:  xl_ts_multi_tuple + (datalen) bytes of body
 *
 * Body layout when FPI:
 *   xl_ts_multi_insert
 *   (page image lives under block_id 0)
 */
typedef struct xl_ts_multi_insert
{
	RelFileNode  rnode;
	uint32		chunk_number;
	BlockNumber blkno;
	uint16		ntuples;
} xl_ts_multi_insert;

#define SizeOfTSMultiInsert		(offsetof(xl_ts_multi_insert, ntuples) + sizeof(uint16))

/*
 * Per-tuple header inside a multi-insert.  Mirrors xl_multi_insert_tuple
 * (heap), minus the SERVERLESS t_cid field; aligned to 2 bytes so
 * SHORTALIGN of the scratch pointer is enough.
 */
typedef struct xl_ts_multi_tuple
{
	uint16		datalen;		/* body length = full t_len - SizeofHeapTupleHeader */
	uint16		t_infomask2;
	uint16		t_infomask;
	uint8		t_hoff;
	/* body data (datalen bytes) follows */
} xl_ts_multi_tuple;

#define SizeOfTSMultiTuple	(offsetof(xl_ts_multi_tuple, t_hoff) + sizeof(uint8))

/*
 * WAL record structure for an extension-fork truncate.
 *
 * Standard heap relies on smgr's xl_smgr_truncate, whose flag field
 * (SMGR_TRUNCATE_HEAP / FSM / VM / ALL) cannot express extension forks.
 * We emit our own record so a crash, replica replay, or PITR rewinds
 * the chunk fork file to the same nblocks the master saw.
 */
typedef struct xl_ts_fork_truncate
{
	RelFileNode	rnode;
	int32		forknum;	/* the extension fork being truncated */
	BlockNumber	nblocks;	/* target length, typically 0 */
} xl_ts_fork_truncate;

#define SizeOfTSForkTruncate	sizeof(xl_ts_fork_truncate)

/*
 * WAL record: create the PAX sidecar directory for a relation.
 *
 * Emitted once in ts_pax_writer_open the first time a compressed chunk
 * gets a PAX file.  Redo runs MakePGDirectory("base/<db>/ts_compressed/
 * <relid>") — idempotent, EEXIST OK.
 *
 * XLOG_TS_PAX_REMOVE_DIR reuses this struct: same key fields (dbid,
 * relid), different op code.
 */
typedef struct xl_ts_pax_dir_op
{
	Oid			dbid;
	Oid			relid;
} xl_ts_pax_dir_op;

#define SizeOfTSPaxDirOp	sizeof(xl_ts_pax_dir_op)

/*
 * WAL record: write a slice of a PAX file.
 *
 * Emitted by TsWalRecordingFile::PWriteN after every PAX write (Flush,
 * MergePaxColumns, MergeGroup, Close all funnel through pax::File's
 * PWriteN).  Redo re-opens the file (O_CREAT + O_TRUNC iff offset==0)
 * and FileWrite(buf, len, offset).
 *
 * On-wire layout:
 *   xl_ts_pax_write
 *   filename (filename_len bytes, no NUL)
 *   payload  (rest of record)
 *
 * filename is the basename (e.g. "chunk_4.pax.seg0"), not the full
 * path — redo re-derives base/<db>/ts_compressed/<relid>/ from dbid +
 * relid so the record stays cluster-move-friendly.
 */
typedef struct xl_ts_pax_write
{
	Oid			dbid;
	Oid			relid;
	int64		offset;
	uint16		filename_len;
	/* followed by: filename[filename_len] + payload[record_len - header - filename_len] */
} xl_ts_pax_write;

#define SizeOfTSPaxWrite	(offsetof(xl_ts_pax_write, filename_len) + sizeof(uint16))

/*
 * WAL record: promote a freshly written PAX file to its live name,
 * i.e. rename "<filename>.new" -> "<filename>".
 *
 * Emitted by do_compress_one_chunk right before its own rename(2), once
 * the writer has closed and the file is structurally complete.  Redo
 * performs the same rename so mirrors and crash recovery converge on the
 * primary's layout.
 *
 * This record is what keeps XLOG_TS_PAX_WRITE honest.  Those records
 * name the ".new" file, so replaying them can never damage live data;
 * the single moment a live file changes identity is here, and this
 * record only exists if the compress got far enough to close its
 * writer.  An interrupted recompress therefore replays writes into an
 * orphan ".new" and leaves the live file -- and the committed
 * ts_compressed_chunk row describing it -- untouched.
 *
 * On-wire layout:
 *   xl_ts_pax_rename
 *   filename (filename_len bytes, no NUL) -- the FINAL basename, e.g.
 *   "chunk_4.pax.seg0"; redo appends ".new" to derive the source.
 *   toast_filename (toast_filename_len bytes, no NUL), present only when
 *   toast_filename_len > 0 -- the FINAL basename of PAX's own external-
 *   toast sidecar, e.g. "chunk_4.pax.seg0.toast".  Same source-derivation
 *   rule as filename.  Zero length means this compress wrote no toast
 *   content (the common case): nothing to promote, and any live toast
 *   file from a PREVIOUS compress is left as-is (harmless orphan -- the
 *   new main file's stripes all report toastlength()==0, so nothing
 *   references it).  Packed into the same record as filename rather than
 *   emitted as a second XLOG_TS_PAX_RENAME so the two renames can never
 *   be split by a crash landing between them -- see the comment on
 *   ts_wal_pax_rename's declaration in ts_wal.h.
 */
typedef struct xl_ts_pax_rename
{
	Oid			dbid;
	Oid			relid;
	uint16		filename_len;
	uint16		toast_filename_len;
	/* followed by: filename[filename_len], then toast_filename[toast_filename_len] */
} xl_ts_pax_rename;

#define SizeOfTSPaxRename	(offsetof(xl_ts_pax_rename, toast_filename_len) + sizeof(uint16))

/*
 * ts_wal_insert
 *		Emit an incremental WAL record for a tuple already placed on
 *		the chunk-fork page by ts_relation_put_heap_tuple.
 *
 *		Preconditions: buf is exclusively locked, caller is inside a
 *		START_CRIT_SECTION / END_CRIT_SECTION pair, and the tuple is
 *		already on the page at offnum (with the same payload as tup).
 *
 *		init_page is the caller's decision (typically derived from
 *		PageGetMaxOffsetNumber before any PageAddItem on this insert);
 *		when true the redo path re-inits the page instead of reading
 *		it from disk.
 */
void
ts_wal_insert(Relation rel, Buffer buf, HeapTuple tup,
			  OffsetNumber offnum, bool init_page)
{
	Page			page = BufferGetPage(buf);
	xl_ts_insert	xlrec;
	xl_heap_header	xlhdr;
	ForkNumber		real_forkno;
	XLogRecPtr		recptr;
	uint8			info = XLOG_TS_INSERT;
	bool			need_fpi;

	BufferGetTag(buf, &xlrec.rnode, &real_forkno, &xlrec.blkno);
	xlrec.chunk_number = (uint32) real_forkno;
	xlrec.offnum = offnum;

	if (init_page)
		info |= XLOG_TS_INIT_PAGE;

	/*
	 * FPI decision.  INIT_PAGE records re-init the page on redo from
	 * scratch, so no image is needed.  Otherwise consult PG's standard
	 * "is this buffer older than the last checkpoint" check.
	 */
	need_fpi = !init_page && XLogCheckBufferNeedsBackup(buf);

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, SizeOfTSInsert);

	if (need_fpi)
	{
		/*
		 * Route the page through PG's standard FPI path: hole elimination
		 * and wal_compression run inside XLogRecordAssemble; redo pulls
		 * the image back via RestoreBlockImage(record, 0, page).  The
		 * tuple data is dropped from the body — it's already in the page
		 * image.
		 */
		XLogRegisterBlock(0, &xlrec.rnode, MAIN_FORKNUM,
						  xlrec.blkno, page,
						  REGBUF_FORCE_IMAGE | REGBUF_STANDARD);
	}
	else
	{
		/*
		 * Emit only the bytes redo can't reconstruct: infomask2/infomask/
		 * hoff and the bitmap/data payload.  xmin (= record xid), cmin
		 * (FirstCommandId), and t_ctid (self) are recovered on replay.
		 * Same trick heap_insert uses.
		 */
		xlhdr.t_infomask2 = tup->t_data->t_infomask2;
		xlhdr.t_infomask  = tup->t_data->t_infomask;
		xlhdr.t_hoff      = tup->t_data->t_hoff;

		XLogRegisterData((char *) &xlhdr, SizeOfHeapHeader);
		XLogRegisterData((char *) tup->t_data + SizeofHeapTupleHeader,
						 tup->t_len - SizeofHeapTupleHeader);
	}

	recptr = XLogInsert(TS_RMGR_ID, info);
	PageSetLSN(page, recptr);
}

/*
 * ts_wal_multi_insert
 *		Emit one WAL record covering ntuples tuples already placed on
 *		the chunk-fork page by ts_relation_put_heap_tuple.
 *
 *		Preconditions: buf is exclusively locked, caller is inside a
 *		START_CRIT_SECTION / END_CRIT_SECTION pair, all tuples are on
 *		the page at offnums[i], and init_page reflects whether the
 *		page was empty before the batch (so redo can re-init instead
 *		of reading from disk).
 *
 *		The WAL payload is built into a single scratch buffer to keep
 *		XLogRegisterData calls down to one, matching the upstream
 *		heap_multi_insert pattern.
 */
void
ts_wal_multi_insert(Relation rel, Buffer buf, HeapTuple *tuples,
					int ntuples, OffsetNumber *offnums, bool init_page)
{
	Page			page = BufferGetPage(buf);
	xl_ts_multi_insert xlrec;
	ForkNumber		real_forkno;
	int				i;
	char		   *scratch;
	char		   *scratchptr;
	int				totallen;
	Size			scratch_size;
	XLogRecPtr		recptr;
	uint8			info = XLOG_TS_MULTI_INSERT;
	bool			need_fpi;

	BufferGetTag(buf, &xlrec.rnode, &real_forkno, &xlrec.blkno);
	xlrec.chunk_number = (uint32) real_forkno;
	xlrec.ntuples = (uint16) ntuples;

	if (init_page)
		info |= XLOG_TS_INIT_PAGE;

	need_fpi = !init_page && XLogCheckBufferNeedsBackup(buf);

	/*
	 * Scratch buffer holds:
	 *   xlrec header
	 *   + when no FPI: optional offsets[] (only when !init_page)
	 *                  + per-tuple {xl_ts_multi_tuple + body}
	 *   + when FPI: nothing — the page rides under block_id 0
	 *
	 * Worst-case sizing (sizeof(xl_ts_multi_tuple) covers alignment slack
	 * vs SizeOfTSMultiTuple).
	 */
	scratch_size = SizeOfTSMultiInsert;
	if (!need_fpi)
	{
		if (!init_page)
			scratch_size += sizeof(OffsetNumber) * ntuples;
		for (i = 0; i < ntuples; i++)
			scratch_size += sizeof(xl_ts_multi_tuple)
				+ (tuples[i]->t_len - SizeofHeapTupleHeader);
	}
	scratch = (char *) palloc(scratch_size);

	scratchptr = scratch;
	memcpy(scratchptr, &xlrec, SizeOfTSMultiInsert);
	scratchptr += SizeOfTSMultiInsert;

	if (!need_fpi)
	{
		if (!init_page)
		{
			memcpy(scratchptr, offnums, sizeof(OffsetNumber) * ntuples);
			scratchptr += sizeof(OffsetNumber) * ntuples;
		}
		for (i = 0; i < ntuples; i++)
		{
			HeapTuple			tup = tuples[i];
			xl_ts_multi_tuple  *thdr;
			uint16				datalen;

			/* per-tuple header needs 2-byte alignment */
			scratchptr = (char *) SHORTALIGN(scratchptr);
			thdr = (xl_ts_multi_tuple *) scratchptr;
			datalen = (uint16) (tup->t_len - SizeofHeapTupleHeader);

			thdr->datalen     = datalen;
			thdr->t_infomask2 = tup->t_data->t_infomask2;
			thdr->t_infomask  = tup->t_data->t_infomask;
			thdr->t_hoff      = tup->t_data->t_hoff;
			scratchptr += SizeOfTSMultiTuple;

			memcpy(scratchptr,
				   (char *) tup->t_data + SizeofHeapTupleHeader,
				   datalen);
			scratchptr += datalen;
		}
	}

	totallen = scratchptr - scratch;

	XLogBeginInsert();
	XLogRegisterData(scratch, totallen);

	if (need_fpi)
	{
		XLogRegisterBlock(0, &xlrec.rnode, MAIN_FORKNUM,
						  xlrec.blkno, page,
						  REGBUF_FORCE_IMAGE | REGBUF_STANDARD);
	}

	recptr = XLogInsert(TS_RMGR_ID, info);
	PageSetLSN(page, recptr);

	pfree(scratch);
}

/*
 * ts_wal_fork_truncate
 *		Emit a WAL record that, on replay, truncates `forknum` of `rel`
 *		to zero blocks.  Must be called by the caller of smgrtruncate
 *		BEFORE smgrtruncate, while the relation extension is serialised
 *		by LockRelationForExtension (matches the ordering in
 *		RelationTruncate at storage.c:369-396).
 *
 *		Flushes the WAL record before returning: the on-disk truncate
 *		is non-transactional, and if the file shrinks before the WAL
 *		record reaches stable storage a crash would leave a master
 *		whose fork is empty but whose WAL has no truncate record —
 *		standbys would then carry stale fork contents indefinitely.
 */
void
ts_wal_fork_truncate(Relation rel, ForkNumber forknum, BlockNumber nblocks)
{
	xl_ts_fork_truncate	xlrec;
	XLogRecPtr			recptr;

	xlrec.rnode = rel->rd_node;
	xlrec.forknum = (int32) forknum;
	xlrec.nblocks = nblocks;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, SizeOfTSForkTruncate);

	recptr = XLogInsert(TS_RMGR_ID,
						XLOG_TS_FORK_TRUNCATE | XLR_SPECIAL_REL_UPDATE);
	XLogFlush(recptr);
}

/*
 * ts_wal_pax_create_dir
 *		Emit a WAL record so the standby creates ts_compressed/<relid>/
 *		before the first XLOG_TS_PAX_WRITE arrives.  Called from
 *		ts_pax_writer_open right after fs->CreateDirectory().
 *
 *		Does NOT XLogFlush — the record rides along with the enclosing
 *		xact.  If the xact aborts, the directory on the primary is
 *		cleaned by ts_pax_register_pending_removal(...).
 */
void
ts_wal_pax_create_dir(Oid dbid, Oid relid)
{
	xl_ts_pax_dir_op	xlrec;

	xlrec.dbid = dbid;
	xlrec.relid = relid;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, SizeOfTSPaxDirOp);
	(void) XLogInsert(TS_RMGR_ID, XLOG_TS_PAX_CREATE_DIR);
}

/*
 * ts_wal_pax_write
 *		Emit a WAL record covering a slice of a PAX file just written
 *		by the primary's pax::File::PWriteN.  Called from
 *		TsWalRecordingFile after the base file write succeeds.
 *
 *		Does NOT XLogFlush — matches PAX's own XLogPaxInsert ordering
 *		(data first, then WAL, no flush).  Correctness comes from xact
 *		commit: PG ensures the xact's commit record — and everything
 *		before it, including this WAL — hits stable storage together.
 *
 *		filename is a basename (e.g. "chunk_4.pax.seg0"), not a full
 *		path; redo derives the parent directory from dbid + relid.
 */
void
ts_wal_pax_write(Oid dbid, Oid relid, const char *filename,
				 int64 offset, const void *payload, size_t payload_len)
{
	xl_ts_pax_write	xlrec;
	int				name_len = (int) strlen(filename);

	if (name_len <= 0 || name_len > UINT16_MAX)
		ereport(ERROR,
				(errmsg("ts_wal_pax_write: filename length %d out of range",
						name_len)));

	xlrec.dbid = dbid;
	xlrec.relid = relid;
	xlrec.offset = offset;
	xlrec.filename_len = (uint16) name_len;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, SizeOfTSPaxWrite);
	XLogRegisterData((char *) filename, name_len);
	if (payload_len > 0)
		XLogRegisterData((char *) payload, (uint32) payload_len);

	(void) XLogInsert(TS_RMGR_ID, XLOG_TS_PAX_WRITE);
}

/*
 * ts_wal_pax_rename
 *		Emit a WAL record promoting "<filename>.new" to "<filename>",
 *		and -- when toast_filename is non-NULL -- "<toast_filename>.new"
 *		to "<toast_filename>" too, from this SAME record.
 *
 *		Call this immediately before the primary's own rename(2) calls,
 *		after the PAX writer has closed successfully -- never earlier.
 *		The record is the only thing in the PAX WAL stream that touches
 *		a live filename, so emitting it before the file(s) are complete
 *		would reintroduce the very hazard the ".new" naming exists to
 *		prevent.
 *
 *		filename is the FINAL basename ("chunk_<N>.pax.seg<M>");
 *		toast_filename, if given, is the FINAL basename of the toast
 *		sidecar ("chunk_<N>.pax.seg<M>.toast").  Redo appends ".new" to
 *		each to derive its source and derives the directory from
 *		dbid + relid, matching XLOG_TS_PAX_WRITE.  Pass NULL when this
 *		compress wrote no toast content -- the common case -- so redo
 *		does not attempt a second rename with nothing to promote.
 *
 *		Residual window, deliberately accepted for now: like the write
 *		records, this one is replayed if it reached disk, even when the
 *		compress transaction later aborts.  A crash in the narrow gap
 *		between this record being flushed and the transaction committing
 *		therefore promotes a COMPLETE but uncommitted merge.  That is a
 *		self-correcting inconsistency (the next successful compress
 *		rewrites the file(s) and reconciles the catalog), not the
 *		permanent destruction of committed data that the ".new" naming
 *		removes.  Closing it entirely needs the rename to be carried by
 *		the commit record itself, which is not reachable from an
 *		extension.
 */
void
ts_wal_pax_rename(Oid dbid, Oid relid, const char *filename,
				  const char *toast_filename)
{
	xl_ts_pax_rename	xlrec;
	int					name_len = (int) strlen(filename);
	int					toast_name_len = toast_filename ? (int) strlen(toast_filename) : 0;

	if (name_len <= 0 || name_len > UINT16_MAX)
		ereport(ERROR,
				(errmsg("ts_wal_pax_rename: filename length %d out of range",
						name_len)));

	if (toast_filename && (toast_name_len <= 0 || toast_name_len > UINT16_MAX))
		ereport(ERROR,
				(errmsg("ts_wal_pax_rename: toast filename length %d out of range",
						toast_name_len)));

	xlrec.dbid = dbid;
	xlrec.relid = relid;
	xlrec.filename_len = (uint16) name_len;
	xlrec.toast_filename_len = (uint16) toast_name_len;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, SizeOfTSPaxRename);
	XLogRegisterData((char *) filename, name_len);
	if (toast_name_len > 0)
		XLogRegisterData((char *) toast_filename, toast_name_len);

	(void) XLogInsert(TS_RMGR_ID, XLOG_TS_PAX_RENAME);
}

/*
 * ts_wal_pax_remove_dir
 *		Emit a WAL record so standbys rmtree ts_compressed/<relid>/
 *		in sync with the primary.  Called from ts_pax_remove_reldir
 *		BEFORE the local rmtree.
 *
 *		DOES XLogFlush — the rmtree is non-transactional and
 *		unrecoverable, so the WAL must reach stable storage before
 *		the primary's rmtree returns.  Mirrors ts_wal_fork_truncate's
 *		XLogFlush pattern for the same reason.
 */
void
ts_wal_pax_remove_dir(Oid dbid, Oid relid)
{
	xl_ts_pax_dir_op	xlrec;
	XLogRecPtr			recptr;

	xlrec.dbid = dbid;
	xlrec.relid = relid;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, SizeOfTSPaxDirOp);
	recptr = XLogInsert(TS_RMGR_ID,
						XLOG_TS_PAX_REMOVE_DIR | XLR_SPECIAL_REL_UPDATE);
	XLogFlush(recptr);
}

/*
 * ts_wal_redo_multi_insert
 *		Replay a time_series MULTI_INSERT record.
 */
static void
ts_wal_redo_multi_insert(XLogReaderState *record)
{
	xl_ts_multi_insert *xlrec = (xl_ts_multi_insert *) XLogRecGetData(record);
	char		   *data = XLogRecGetData(record) + SizeOfTSMultiInsert;
	Buffer			buf;
	Page			page;
	int				ntuples = xlrec->ntuples;
	bool			init_page = (XLogRecGetInfo(record) & XLOG_TS_INIT_PAGE) != 0;
	bool			has_fpi   = XLogRecHasBlockImage(record, 0);
	ForkNumber		forkno = (ForkNumber) xlrec->chunk_number;
	OffsetNumber   *offnums = NULL;
	TransactionId	xid = XLogRecGetXid(record);
	union
	{
		HeapTupleHeaderData hdr;
		char		data[MaxHeapTupleSize];
	}				tbuf;
	int				i;

	/*
	 * Locate the chunk page using the real forkno from the rmgr body —
	 * the WAL block-reference header, when present, only carries a
	 * MAIN_FORKNUM placeholder (see ts_wal_insert for why).
	 */
	buf = XLogReadBufferExtended(xlrec->rnode, forkno, xlrec->blkno,
								 (init_page || has_fpi)
								 ? RBM_ZERO_AND_LOCK : RBM_NORMAL);
	if (!BufferIsValid(buf))
	{
		if (init_page || has_fpi)
			elog(PANIC, "ts_wal_redo_multi_insert: cannot init buffer "
				 "(rel=%u fork=%u blk=%u)",
				 xlrec->rnode.relNode, forkno, xlrec->blkno);
		return;					/* fork truncated past blk — nothing to do */
	}
	if (!(init_page || has_fpi))
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	page = BufferGetPage(buf);

	if (has_fpi)
	{
		if (!RestoreBlockImage(record, 0, (char *) page))
			elog(PANIC, "ts_wal_redo_multi_insert: RestoreBlockImage failed "
				 "(rel=%u fork=%u blk=%u)",
				 xlrec->rnode.relNode, forkno, xlrec->blkno);
		PageSetLSN(page, record->EndRecPtr);
		MarkBufferDirty(buf);
		UnlockReleaseBuffer(buf);
		return;
	}

	if (init_page)
	{
		PageInit(page, BufferGetPageSize(buf), 0);
	}
	else if (PageGetLSN(page) >= record->EndRecPtr)
	{
		/* already applied */
		UnlockReleaseBuffer(buf);
		return;
	}

	if (!init_page)
	{
		offnums = (OffsetNumber *) data;
		data += sizeof(OffsetNumber) * ntuples;
	}

	for (i = 0; i < ntuples; i++)
	{
		xl_ts_multi_tuple  *thdr;
		HeapTupleHeader		htup;
		OffsetNumber		off;
		uint32				newlen;

		data = (char *) SHORTALIGN(data);
		thdr = (xl_ts_multi_tuple *) data;
		data += SizeOfTSMultiTuple;

		off = init_page ? FirstOffsetNumber + i : offnums[i];

		/* Defensive: catch corrupted WAL early (heap_xlog_insert pattern). */
		if (PageGetMaxOffsetNumber(page) + 1 < off)
			elog(PANIC, "ts_wal_redo_multi_insert: invalid max offset %lu < %u",
				 (unsigned long) PageGetMaxOffsetNumber(page), off);

		htup = &tbuf.hdr;
		MemSet((char *) htup, 0, SizeofHeapTupleHeader);
		memcpy((char *) htup + SizeofHeapTupleHeader, data, thdr->datalen);
		newlen = SizeofHeapTupleHeader + thdr->datalen;

		htup->t_infomask2 = thdr->t_infomask2;
		htup->t_infomask  = thdr->t_infomask;
		htup->t_hoff      = thdr->t_hoff;
		HeapTupleHeaderSetXmin(htup, xid);
		HeapTupleHeaderSetCmin(htup, FirstCommandId);
		ItemPointerSet(&htup->t_ctid, xlrec->blkno, off);

		if (PageAddItem(page, (Item) htup, newlen, off, true, true)
			== InvalidOffsetNumber)
			elog(PANIC, "ts_wal_redo_multi_insert: failed at tuple %d offset %u",
				 i, off);

		data += thdr->datalen;
	}

	PageSetLSN(page, record->EndRecPtr);
	MarkBufferDirty(buf);
	UnlockReleaseBuffer(buf);
}

/*
 * ts_wal_redo_insert
 *		Replay a time_series INSERT record.
 */
static void
ts_wal_redo_insert(XLogReaderState *record)
{
	xl_ts_insert   *xlrec = (xl_ts_insert *) XLogRecGetData(record);
	char		   *payload = XLogRecGetData(record) + SizeOfTSInsert;
	Buffer			buf;
	Page			page;
	bool			init_page = (XLogRecGetInfo(record) & XLOG_TS_INIT_PAGE) != 0;
	bool			has_fpi   = XLogRecHasBlockImage(record, 0);
	ForkNumber		forkno = (ForkNumber) xlrec->chunk_number;
	TransactionId	xid = XLogRecGetXid(record);

	/*
	 * Three modes:
	 *   INIT_PAGE  → RBM_ZERO_AND_LOCK + PageInit + apply tuple
	 *   HAS_FPI    → RBM_ZERO_AND_LOCK + RestoreBlockImage from block_id 0
	 *   normal     → RBM_NORMAL + LSN check + apply tuple
	 */
	buf = XLogReadBufferExtended(xlrec->rnode, forkno, xlrec->blkno,
								 (init_page || has_fpi)
								 ? RBM_ZERO_AND_LOCK : RBM_NORMAL);
	if (!BufferIsValid(buf))
	{
		if (init_page || has_fpi)
			elog(PANIC, "ts_wal_redo_insert: cannot init buffer "
				 "(rel=%u fork=%u blk=%u)",
				 xlrec->rnode.relNode, forkno, xlrec->blkno);
		return;					/* fork truncated past blk — nothing to do */
	}
	if (!(init_page || has_fpi))
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	page = BufferGetPage(buf);

	if (has_fpi)
	{
		if (!RestoreBlockImage(record, 0, (char *) page))
			elog(PANIC, "ts_wal_redo_insert: RestoreBlockImage failed "
				 "(rel=%u fork=%u blk=%u)",
				 xlrec->rnode.relNode, forkno, xlrec->blkno);
	}
	else
	{
		xl_heap_header	xlhdr;
		union
		{
			HeapTupleHeaderData hdr;
			char		data[MaxHeapTupleSize];
		}				tbuf;
		HeapTupleHeader	htup;
		Size			datalen;
		uint32			newlen;

		if (init_page)
		{
			PageInit(page, BufferGetPageSize(buf), 0);
		}
		else if (!XLogRecPtrIsInvalid(PageGetLSN(page)) &&
				 PageGetLSN(page) >= record->EndRecPtr)
		{
			/* already applied */
			UnlockReleaseBuffer(buf);
			return;
		}

		/* Defensive bounds check (mirrors heap_xlog_insert). */
		if (PageGetMaxOffsetNumber(page) + 1 < xlrec->offnum)
			elog(PANIC, "ts_wal_redo_insert: invalid max offset %lu < %u",
				 (unsigned long) PageGetMaxOffsetNumber(page), xlrec->offnum);

		/* xl_heap_header + body in main rmgr data; reconstruct HeapTupleHeader. */
		memcpy(&xlhdr, payload, SizeOfHeapHeader);
		datalen = XLogRecGetDataLen(record) - SizeOfTSInsert - SizeOfHeapHeader;
		Assert(datalen <= MaxHeapTupleSize - SizeofHeapTupleHeader);

		htup = &tbuf.hdr;
		MemSet((char *) htup, 0, SizeofHeapTupleHeader);
		memcpy((char *) htup + SizeofHeapTupleHeader,
			   payload + SizeOfHeapHeader, datalen);
		newlen = SizeofHeapTupleHeader + datalen;

		htup->t_infomask2 = xlhdr.t_infomask2;
		htup->t_infomask  = xlhdr.t_infomask;
		htup->t_hoff      = xlhdr.t_hoff;
		HeapTupleHeaderSetXmin(htup, xid);
		HeapTupleHeaderSetCmin(htup, FirstCommandId);
		ItemPointerSet(&htup->t_ctid, xlrec->blkno, xlrec->offnum);

		if (PageAddItem(page, (Item) htup, newlen,
						xlrec->offnum, true, true) == InvalidOffsetNumber)
			elog(PANIC, "ts_wal_redo_insert: PageAddItem failed @off=%u",
				 xlrec->offnum);
	}

	PageSetLSN(page, record->EndRecPtr);
	MarkBufferDirty(buf);
	UnlockReleaseBuffer(buf);
}

/*
 * ts_wal_redo_fork_truncate
 *		Apply a truncate of one extension fork on the standby (or during
 *		crash recovery on the primary).  Mirrors smgr_redo's truncate
 *		path: drop any cached buffers for the about-to-vanish pages,
 *		then call smgrtruncate.
 */
static void
ts_wal_redo_fork_truncate(XLogReaderState *record)
{
	xl_ts_fork_truncate	   *xlrec;
	SMgrRelation			smgr;
	ForkNumber				forks[1];
	BlockNumber				blocks[1];

	xlrec = (xl_ts_fork_truncate *) XLogRecGetData(record);

	/*
	 * CB14 smgropen takes (rnode, backend, SMgrImpl, Relation).  For
	 * recovery we have no Relation handle; pass NULL + SMGR_INVALID so
	 * smgr_get_impl_hook picks the default magnetic-disk implementation
	 * (matches smgr_redo's call in smgr.c).
	 */
	smgr = smgropen(xlrec->rnode, InvalidBackendId, SMGR_INVALID, NULL);
	forks[0] = (ForkNumber) xlrec->forknum;
	blocks[0] = xlrec->nblocks;

	/*
	 * Drop any shared-buffer pages for the to-be-removed range so the
	 * subsequent smgrtruncate doesn't leave dirty buffers pointing at
	 * blocks that no longer exist on disk.
	 */
	DropRelFileNodeBuffers(smgr, forks, 1, blocks);

	if (smgrexists(smgr, forks[0]))
		smgrtruncate(smgr, forks, 1, blocks);
}

/*
 * Ensure base/<db>/ts_compressed/<relid>/ exists.  MakePGDirectory
 * is single-level (mkdir(2)), so create the parent "ts_compressed"
 * link first.  EEXIST at either level is ignored — both may already
 * exist on the standby from a previous relation's redo.
 */
static void
ts_pax_mkdir_relidir(Oid dbid, Oid relid)
{
	char	parent[MAXPGPATH];
	char	dir[MAXPGPATH];

	snprintf(parent, sizeof(parent), "base/%u/ts_compressed", dbid);
	if (MakePGDirectory(parent) != 0 && errno != EEXIST)
	{
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("ts_pax_mkdir_relidir: could not create \"%s\": %m",
						parent)));
		return;
	}

	snprintf(dir, sizeof(dir), TS_PAX_RELDIR_FMT_LOCAL, dbid, relid);
	if (MakePGDirectory(dir) != 0 && errno != EEXIST)
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("ts_pax_mkdir_relidir: could not create \"%s\": %m",
						dir)));
}

/*
 * ts_wal_redo_pax_create_dir
 *		Replay: MakePGDirectory("base/<db>/ts_compressed/<relid>").
 *		EEXIST is fine — a prior record for the same relid may have
 *		created it, or the primary may have created it before this
 *		record was even emitted (in which case the redo runs after
 *		the file already exists on the mirror via basebackup).
 */
static void
ts_wal_redo_pax_create_dir(XLogReaderState *record)
{
	xl_ts_pax_dir_op   *xlrec = (xl_ts_pax_dir_op *) XLogRecGetData(record);

	ts_pax_mkdir_relidir(xlrec->dbid, xlrec->relid);
}

/*
 * ts_wal_redo_pax_write
 *		Replay: pwrite the recorded byte range into the PAX file.
 *
 *		File may not exist yet on the mirror (first write to it) —
 *		O_CREAT handles that.  offset==0 additionally means O_TRUNC:
 *		the primary opened this file with kWriteWithTruncMode, so
 *		any pre-existing bytes on the mirror (from a failed prior
 *		attempt) must be wiped before we lay down the new stripe.
 *		Matches PAX's own XLogRedoPaxInsert semantics (paxc_wal.cc:338).
 */
static void
ts_wal_redo_pax_write(XLogReaderState *record)
{
	xl_ts_pax_write	   *xlrec = (xl_ts_pax_write *) XLogRecGetData(record);
	char			   *rec = XLogRecGetData(record);
	int32				rec_len = XLogRecGetDataLen(record);
	char				dir[MAXPGPATH];
	char				path[MAXPGPATH];
	char				fname[MAXPGPATH];
	const char		   *payload;
	int32				payload_len;
	int					flags;
	File				vfd;

	if (xlrec->filename_len == 0 ||
		xlrec->filename_len >= sizeof(fname))
		ereport(PANIC,
				(errmsg("ts_wal_redo_pax_write: bad filename_len %u",
						xlrec->filename_len)));

	memcpy(fname, rec + SizeOfTSPaxWrite, xlrec->filename_len);
	fname[xlrec->filename_len] = '\0';

	payload = rec + SizeOfTSPaxWrite + xlrec->filename_len;
	payload_len = rec_len - SizeOfTSPaxWrite - xlrec->filename_len;
	if (payload_len < 0)
		ereport(PANIC,
				(errmsg("ts_wal_redo_pax_write: negative payload_len")));

	snprintf(dir,  sizeof(dir),  TS_PAX_RELDIR_FMT_LOCAL, xlrec->dbid, xlrec->relid);
	snprintf(path, sizeof(path), "%s/%s", dir, fname);

	/*
	 * Ensure the parent dir hierarchy exists — a lost
	 * XLOG_TS_PAX_CREATE_DIR (e.g. mirror joined mid-stream) or a
	 * missing "ts_compressed" intermediate should not stop us;
	 * primary's ts_pax_writer_open always created it before writing.
	 */
	ts_pax_mkdir_relidir(xlrec->dbid, xlrec->relid);

	flags = O_RDWR | PG_BINARY | O_CREAT;
	if (xlrec->offset == 0)
		flags |= O_TRUNC;

	vfd = PathNameOpenFile(path, flags);
	if (vfd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("ts_wal_redo_pax_write: could not open \"%s\": %m", path)));

	if (payload_len > 0)
	{
		int	written = FileWrite(vfd, (char *) payload, payload_len,
								xlrec->offset, WAIT_EVENT_COPY_FILE_WRITE);
		if (written != payload_len)
		{
			FileClose(vfd);
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("ts_wal_redo_pax_write: short write to \"%s\": wrote %d of %d at offset %ld",
							path, written, payload_len, (long) xlrec->offset)));
		}
	}
	FileClose(vfd);
}

/*
 * ts_wal_redo_pax_rename_one
 *		Shared body for a single "<name>.new" -> "<name>" promotion,
 *		given the already-resolved relation directory.  Used twice by
 *		ts_wal_redo_pax_rename below: once for the main file, once
 *		(conditionally) for the toast sidecar.
 *
 *		Idempotent on purpose.  Recovery can replay this record when the
 *		rename already happened before the crash (the primary renames
 *		first, then writes the catalog row and commits), in which case
 *		the source is gone and the destination is already correct --
 *		a missing source is therefore success, not an error.  Anything
 *		else is a genuine filesystem problem and must stop recovery.
 */
static void
ts_wal_redo_pax_rename_one(const char *dir, const char *fname)
{
	char	src[MAXPGPATH];
	char	dst[MAXPGPATH];

	snprintf(dst, sizeof(dst), "%s/%s", dir, fname);
	snprintf(src, sizeof(src), "%s.new", dst);

	if (rename(src, dst) < 0)
	{
		if (errno == ENOENT)
		{
			elog(DEBUG1,
				 "ts_wal_redo_pax_rename: \"%s\" already promoted, skipping",
				 dst);
			return;
		}
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("ts_wal_redo_pax_rename: could not rename \"%s\" to \"%s\": %m",
						src, dst)));
	}
}

/*
 * ts_wal_redo_pax_rename
 *		Replay of XLOG_TS_PAX_RENAME: rename "<file>.new" -> "<file>",
 *		and -- when the record carries a toast_filename -- rename
 *		"<toast_file>.new" -> "<toast_file>" too, mirroring
 *		do_compress_one_chunk's own rename(2) calls.
 */
static void
ts_wal_redo_pax_rename(XLogReaderState *record)
{
	xl_ts_pax_rename   *xlrec = (xl_ts_pax_rename *) XLogRecGetData(record);
	char			   *rec = XLogRecGetData(record);
	char				dir[MAXPGPATH];
	char				fname[MAXPGPATH];
	char				toast_fname[MAXPGPATH];

	if (xlrec->filename_len == 0 ||
		xlrec->filename_len >= sizeof(fname))
		ereport(PANIC,
				(errmsg("ts_wal_redo_pax_rename: bad filename_len %u",
						xlrec->filename_len)));

	if (xlrec->toast_filename_len >= sizeof(toast_fname))
		ereport(PANIC,
				(errmsg("ts_wal_redo_pax_rename: bad toast_filename_len %u",
						xlrec->toast_filename_len)));

	/*
	 * Cross-check the length fields against the record it came in: a
	 * truncated or corrupt record would otherwise have us read past its
	 * end.  Trusting an unvalidated length is exactly how a damaged PAX
	 * file turns a clean error into a segfault elsewhere in this stack.
	 */
	if (XLogRecGetDataLen(record) !=
		(uint32) SizeOfTSPaxRename + xlrec->filename_len + xlrec->toast_filename_len)
		ereport(PANIC,
				(errmsg("ts_wal_redo_pax_rename: record length %u does not match filename_len %u + toast_filename_len %u",
						XLogRecGetDataLen(record), xlrec->filename_len,
						xlrec->toast_filename_len)));

	memcpy(fname, rec + SizeOfTSPaxRename, xlrec->filename_len);
	fname[xlrec->filename_len] = '\0';

	if (xlrec->toast_filename_len > 0)
	{
		memcpy(toast_fname, rec + SizeOfTSPaxRename + xlrec->filename_len,
			   xlrec->toast_filename_len);
		toast_fname[xlrec->toast_filename_len] = '\0';
	}

	snprintf(dir, sizeof(dir), TS_PAX_RELDIR_FMT_LOCAL, xlrec->dbid, xlrec->relid);

	/* A lost CREATE_DIR must not stop us -- same reasoning as pax_write. */
	ts_pax_mkdir_relidir(xlrec->dbid, xlrec->relid);

	ts_wal_redo_pax_rename_one(dir, fname);
	if (xlrec->toast_filename_len > 0)
		ts_wal_redo_pax_rename_one(dir, toast_fname);
}

/*
 * ts_wal_redo_pax_remove_dir
 *		Replay: rmtree base/<db>/ts_compressed/<relid>/.
 *		Missing dir is fine (already removed by an earlier attempt
 *		or never created on this standby); rmtree returns success
 *		when nothing to do.
 */
static void
ts_wal_redo_pax_remove_dir(XLogReaderState *record)
{
	xl_ts_pax_dir_op   *xlrec = (xl_ts_pax_dir_op *) XLogRecGetData(record);
	char				dir[MAXPGPATH];
	struct stat			st;

	snprintf(dir, sizeof(dir), TS_PAX_RELDIR_FMT_LOCAL, xlrec->dbid, xlrec->relid);
	if (stat(dir, &st) == 0)
	{
		if (!rmtree(dir, true))
			ereport(WARNING,
					(errmsg("ts_wal_redo_pax_remove_dir: could not remove \"%s\": %m",
							dir)));
	}
}

/*
 * ts_wal_redo
 *		Main redo dispatch for the time_series resource manager.
 */
static void
ts_wal_redo(XLogReaderState *record)
{
	uint8	info = XLogRecGetInfo(record) & XLR_RMGR_INFO_MASK;

	/* XLOG_TS_INIT_PAGE (0x80) is a flag bit, mask it off for dispatch. */
	switch (info & XLOG_TS_OPMASK)
	{
		case XLOG_TS_INSERT:
			ts_wal_redo_insert(record);
			break;
		case XLOG_TS_MULTI_INSERT:
			ts_wal_redo_multi_insert(record);
			break;
		case XLOG_TS_FORK_TRUNCATE:
			ts_wal_redo_fork_truncate(record);
			break;
		case XLOG_TS_PAX_CREATE_DIR:
			ts_wal_redo_pax_create_dir(record);
			break;
		case XLOG_TS_PAX_WRITE:
			ts_wal_redo_pax_write(record);
			break;
		case XLOG_TS_PAX_REMOVE_DIR:
			ts_wal_redo_pax_remove_dir(record);
			break;
		case XLOG_TS_PAX_RENAME:
			ts_wal_redo_pax_rename(record);
			break;
		default:
			elog(PANIC, "ts_wal_redo: unknown op code %u", info);
			break;
	}
}

/*
 *		WAL desc/identify (for pg_waldump)
 */
static void
ts_wal_desc(StringInfo buf, XLogReaderState *record)
{
	uint8	info = XLogRecGetInfo(record) & XLR_RMGR_INFO_MASK;
	bool	init_page = (info & XLOG_TS_INIT_PAGE) != 0;

	switch (info & XLOG_TS_OPMASK)
	{
		case XLOG_TS_INSERT:
		{
			xl_ts_insert *xlrec = (xl_ts_insert *) XLogRecGetData(record);

			appendStringInfo(buf, "insert: rel=%u/%u/%u chunk=%u blk=%u off=%u%s",
							 xlrec->rnode.spcNode, xlrec->rnode.dbNode,
							 xlrec->rnode.relNode, xlrec->chunk_number,
							 xlrec->blkno, xlrec->offnum,
							 init_page ? " INIT_PAGE" : "");
			break;
		}
		case XLOG_TS_MULTI_INSERT:
		{
			xl_ts_multi_insert *xlrec = (xl_ts_multi_insert *) XLogRecGetData(record);

			appendStringInfo(buf, "multi-insert: rel=%u/%u/%u chunk=%u blk=%u ntuples=%u%s",
							 xlrec->rnode.spcNode, xlrec->rnode.dbNode,
							 xlrec->rnode.relNode, xlrec->chunk_number,
							 xlrec->blkno, xlrec->ntuples,
							 init_page ? " INIT_PAGE" : "");
			break;
		}
		case XLOG_TS_FORK_TRUNCATE:
		{
			xl_ts_fork_truncate *xlrec = (xl_ts_fork_truncate *) XLogRecGetData(record);

			appendStringInfo(buf, "fork-truncate: fork %d nblocks %u",
							 xlrec->forknum, xlrec->nblocks);
			break;
		}
		case XLOG_TS_PAX_CREATE_DIR:
		{
			xl_ts_pax_dir_op *xlrec = (xl_ts_pax_dir_op *) XLogRecGetData(record);

			appendStringInfo(buf, "pax-create-dir: dbid=%u relid=%u",
							 xlrec->dbid, xlrec->relid);
			break;
		}
		case XLOG_TS_PAX_WRITE:
		{
			xl_ts_pax_write *xlrec = (xl_ts_pax_write *) XLogRecGetData(record);
			int32 payload_len = XLogRecGetDataLen(record)
								- SizeOfTSPaxWrite - xlrec->filename_len;

			appendStringInfo(buf, "pax-write: dbid=%u relid=%u offset=%ld "
							 "filename_len=%u payload_len=%d",
							 xlrec->dbid, xlrec->relid,
							 (long) xlrec->offset,
							 xlrec->filename_len, payload_len);
			break;
		}
		case XLOG_TS_PAX_REMOVE_DIR:
		{
			xl_ts_pax_dir_op *xlrec = (xl_ts_pax_dir_op *) XLogRecGetData(record);

			appendStringInfo(buf, "pax-remove-dir: dbid=%u relid=%u",
							 xlrec->dbid, xlrec->relid);
			break;
		}
		case XLOG_TS_PAX_RENAME:
		{
			xl_ts_pax_rename *xlrec = (xl_ts_pax_rename *) XLogRecGetData(record);

			appendStringInfo(buf, "pax-rename: dbid=%u relid=%u filename_len=%u toast_filename_len=%u",
							 xlrec->dbid, xlrec->relid, xlrec->filename_len,
							 xlrec->toast_filename_len);
			break;
		}
	}
}

static const char *
ts_wal_identify(uint8 info)
{
	switch (info & XLOG_TS_OPMASK)
	{
		case XLOG_TS_INSERT:
			return (info & XLOG_TS_INIT_PAGE) ? "INSERT+INIT" : "INSERT";
		case XLOG_TS_MULTI_INSERT:
			return (info & XLOG_TS_INIT_PAGE) ? "MULTI_INSERT+INIT" : "MULTI_INSERT";
		case XLOG_TS_FORK_TRUNCATE:
			return "FORK_TRUNCATE";
		case XLOG_TS_PAX_CREATE_DIR:
			return "PAX_CREATE_DIR";
		case XLOG_TS_PAX_WRITE:
			return "PAX_WRITE";
		case XLOG_TS_PAX_REMOVE_DIR:
			return "PAX_REMOVE_DIR";
		case XLOG_TS_PAX_RENAME:
			return "PAX_RENAME";
		default:
			return NULL;
	}
}

/*
 *		Registration (called from _PG_init)
 */
static RmgrData ts_rmgr_data = {
	.rm_name = "time_series",
	.rm_redo = ts_wal_redo,
	.rm_desc = ts_wal_desc,
	.rm_identify = ts_wal_identify,
	.rm_startup = NULL,
	.rm_cleanup = NULL,
	.rm_mask = NULL,
	.rm_decode = NULL,
};

void
ts_wal_init(void)
{
	RegisterCustomRmgr(TS_RMGR_ID, &ts_rmgr_data);
}
