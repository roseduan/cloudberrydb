/*-------------------------------------------------------------------------
 *
 * ts_smgr.c
 *    Storage manager wrapper for the time_series extension.
 *
 *    Standard forks (0..MAX_FORKNUM) live in the kernel's SMgrRelation
 *    arrays and are served by md.c.  Chunk forks (TS_FIRST_CHUNKNUM ..
 *    TS_MAX_CHUNK_FORKNUM) cannot fit in those fixed-size arrays, so we
 *    keep parallel md-like state in a TsSmgrPrivate sidecar held in an
 *    extension-owned HTAB keyed by RelFileNodeBackend (mirrors the kernel
 *    SMgrRelationHash key), and clone md.c's per-segment open / read /
 *    write / truncate / fsync logic against it.  The sidecar is released
 *    on the smgr_close_hook right before the SMgrRelation leaves the
 *    kernel hash, keeping the two tables in lock-step.
 *
 *    The relpath_hook (ts_fork_name.c) maps chunk forks to ..._ext<N>
 *    filenames, so the checkpointer's kernel mdsyncfiletag path-fallback
 *    branch finds them without needing a dedicated SyncRequestHandler.
 *
 * Copyright (c) 2026 HashData Inc.
 * Licensed under Apache License 2.0
 *
 * IDENTIFICATION
 *    contrib/time_series/src/storage/ts_smgr.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <unistd.h>

#include "access/xlog.h"
#include "access/xlogutils.h"
#include "catalog/catalog.h"
#include "commands/tablespace.h"
#include "common/relpath.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "storage/fd.h"
#include "storage/md.h"
#include "storage/relfilenode.h"
#include "storage/smgr.h"
#include "storage/sync.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"

#include "../include/time_series.h"
#include "../include/access/ts_tableam.h"
#include "../include/storage/ts_smgr.h"

/*
 * Mirror of md.c's private MdfdVec.  We can't use the kernel typedef
 * because it's declared static inside md.c.
 */
typedef struct TsMdfdVec
{
	File		mdfd_vfd;
	BlockNumber mdfd_segno;
} TsMdfdVec;

/*
 * Sidecar state for chunk forks.  One instance per SMgrRelation, held
 * inside TsSmgrPrivateHash below.
 *
 * The three parallel arrays are indexed by the raw chunk forknum (so
 * cells [0..MAX_FORKNUM] are never populated; the lazy-alloc helper
 * starts the slot at TS_FIRST_CHUNKNUM).  min_used_fork / max_used_fork
 * bound the populated range so teardown skips the empty prefix.
 */
typedef struct TsSmgrPrivate
{
	MemoryContext cxt;				/* parent of all allocations below */
	int			alloc_forks;		/* number of slots in the arrays */
	int			min_used_fork;		/* lowest populated chunk forknum */
	int			max_used_fork;		/* highest populated chunk forknum */
	BlockNumber *cached_nblocks;	/* parallels kernel smgr_cached_nblocks */
	int		   *num_open_segs;		/* parallels kernel md_num_open_segs */
	TsMdfdVec **seg_fds;			/* parallels kernel md_seg_fds */
} TsSmgrPrivate;

/*
 * Hash entry: RelFileNodeBackend key (matches kernel SMgrRelationHash key
 * exactly so a lookup on either side lands on the same relation) plus the
 * inline sidecar payload.  Stable pointers into the entry are handed back
 * to the rest of ts_smgr.c, matching how PG's dynahash treats HASH_BLOBS
 * entries.
 */
typedef struct TsSmgrHashEntry
{
	RelFileNodeBackend key;			/* MUST be first for HASH_BLOBS */
	TsSmgrPrivate priv;
} TsSmgrHashEntry;

/*
 * Extension-owned analogue of kernel SMgrRelationHash.  Populated lazily on
 * first chunk-fork touch, entry removed synchronously from the smgr_close
 * callback so the two hashes stay in lock-step.
 */
static HTAB *TsSmgrPrivateHash = NULL;

/* Bit flags for chunk-fork _ts_mdfd_getseg(); same semantics as md.c. */
#define TS_EXT_FAIL				(1 << 0)
#define TS_EXT_RETURN_NULL		(1 << 1)
#define TS_EXT_CREATE			(1 << 2)
#define TS_EXT_CREATE_RECOVERY	(1 << 3)

/*
 * Populate a FileTag for a chunk-fork dirty segment.  Equivalent to
 * md.c's static INIT_MD_FILETAG macro.
 */
#define TS_INIT_MD_FILETAG(a, xx_rnode, xx_forknum, xx_segno) \
	( \
		memset(&(a), 0, sizeof(FileTag)), \
		(a).handler = SYNC_HANDLER_MD, \
		(a).rnode = (xx_rnode), \
		(a).forknum = (xx_forknum), \
		(a).segno = (xx_segno) \
	)

/* Previous hooks, if any, for chaining. */
static smgr_hook_type			prev_smgr_hook = NULL;
static file_close_hook_type		prev_file_close_hook = NULL;
static file_unlink_hook_type	prev_file_unlink_hook = NULL;

/* Forward declarations of chunk-fork helpers. */
static TsSmgrPrivate *smgr_private_get(SMgrRelation reln);
static TsSmgrPrivate *smgr_private_lookup(RelFileNodeBackend rnode);
static void smgr_private_ensure_capacity(SMgrRelation reln, TsSmgrPrivate *e,
									 ForkNumber forknum);
static void smgr_private_note_used(TsSmgrPrivate *e, ForkNumber forknum);
static void ts_file_close(RelFileNodeBackend rnode);

static void _ts_fdvec_resize(TsSmgrPrivate *e, ForkNumber forknum, int nseg);
static char *_ts_mdfd_segpath(SMgrRelation reln, ForkNumber forknum,
							  BlockNumber segno);
static TsMdfdVec *_ts_mdfd_openseg(SMgrRelation reln, ForkNumber forknum,
								   BlockNumber segno, int oflags);
static TsMdfdVec *_ts_mdfd_getseg(SMgrRelation reln, ForkNumber forknum,
								  BlockNumber blkno, bool skipFsync,
								  int behavior);
static BlockNumber _ts_mdnblocks(File vfd);
static TsMdfdVec *smgr_private_openfork(SMgrRelation reln, ForkNumber forknum,
									int behavior);
static void smgr_private_register_dirty(SMgrRelation reln, ForkNumber forknum,
									TsMdfdVec *seg);

/*
 * smgr_private_hash_init
 *		Lazily create the extension-owned HTAB.  Called on first sidecar
 *		access; matches PG's own SMgrRelationHash lazy-init pattern in
 *		smgropen().  Not driven from ts_smgr_init because _PG_init runs
 *		before per-backend caches exist and we don't want to serialise
 *		hash allocation with shared_preload_libraries loading.
 */
static void
smgr_private_hash_init(void)
{
	HASHCTL		ctl;

	if (TsSmgrPrivateHash != NULL)
		return;

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(RelFileNodeBackend);
	ctl.entrysize = sizeof(TsSmgrHashEntry);
	ctl.hcxt = TopMemoryContext;
	TsSmgrPrivateHash = hash_create("time_series smgr sidecar table",
									64, &ctl,
									HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

/*
 * smgr_private_lookup
 *		Return the TsSmgrPrivate for reln if one exists, else NULL.  Used
 *		by paths that must not create a sidecar (per-fork close, cache
 *		invalidation, the smgr_close callback itself).
 */
static TsSmgrPrivate *
smgr_private_lookup(RelFileNodeBackend rnode)
{
	TsSmgrHashEntry *entry;

	if (TsSmgrPrivateHash == NULL)
		return NULL;
	entry = (TsSmgrHashEntry *) hash_search(TsSmgrPrivateHash,
											(void *) &rnode,
											HASH_FIND, NULL);
	return entry ? &entry->priv : NULL;
}

/*
 * smgr_private_get
 *		Return the TsSmgrPrivate for reln, allocating and inserting it
 *		lazily on first touch.  The entry lives in TsSmgrPrivateHash;
 *		its private memory context and pointer arrays are allocated
 *		inside the entry's cxt so ts_file_close can free the whole
 *		graph with one MemoryContextDelete.
 */
static TsSmgrPrivate *
smgr_private_get(SMgrRelation reln)
{
	TsSmgrHashEntry *entry;
	TsSmgrPrivate *e;
	bool		found;
	MemoryContext cxt;

	if (TsSmgrPrivateHash == NULL)
		smgr_private_hash_init();

	entry = (TsSmgrHashEntry *) hash_search(TsSmgrPrivateHash,
											(void *) &reln->smgr_rnode,
											HASH_ENTER, &found);
	e = &entry->priv;

	if (found)
		return e;

	cxt = AllocSetContextCreate(TopMemoryContext,
								"TsSmgrPrivate",
								ALLOCSET_SMALL_SIZES);
	e->cxt = cxt;
	e->alloc_forks = 0;
	e->min_used_fork = INT_MAX;
	e->max_used_fork = -1;
	e->cached_nblocks = NULL;
	e->num_open_segs = NULL;
	e->seg_fds = NULL;
	return e;
}

/*
 * smgr_private_ensure_capacity
 *		Grow the sidecar arrays so that 'forknum' is a valid index.  Newly
 *		grown slots are zero-initialised (cached_nblocks = InvalidBlockNumber).
 */
static void
smgr_private_ensure_capacity(SMgrRelation reln, TsSmgrPrivate *e, ForkNumber forknum)
{
	int			needed = forknum + 1;
	int			oldsize = e->alloc_forks;
	MemoryContext oldcxt;
	int			i;

	/*
	 * Guard the palloc so an upstream bug that walks off the chunk-forknum
	 * range cannot silently blow the sidecar into a gigantic allocation.
	 * A chunk_idx=14000 request would ask us for a 3×14001 pointer/int
	 * array; a bad chunk_idx of 60000 would ask for 3×60001 = ~700 KiB
	 * per relation *and* leave every subsequent index into the dense
	 * arrays walking that big region.  Instead fail loud so the corruption
	 * source (usually a WAL replay reading a stale/oversized chunk_idx or
	 * a caller that did chunk_num arithmetic without checking overflow)
	 * surfaces immediately.  See project_ts_smgr_sidecar_alloc_forks_bug
	 * for the historical incident this guard prevents from recurring.
	 */
	if (forknum > TS_MAX_CHUNK_FORKNUM || forknum < TS_FIRST_CHUNKNUM)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("time_series: chunk forknum %d out of range [%d, %d] "
						"for relation %u",
						(int) forknum,
						(int) TS_FIRST_CHUNKNUM,
						(int) TS_MAX_CHUNK_FORKNUM,
						reln->smgr_rnode.node.relNode)));

	if (needed <= oldsize)
		return;

	oldcxt = MemoryContextSwitchTo(e->cxt);

	if (oldsize == 0)
	{
		e->cached_nblocks = palloc(sizeof(BlockNumber) * needed);
		e->num_open_segs = palloc0(sizeof(int) * needed);
		e->seg_fds = palloc0(sizeof(TsMdfdVec *) * needed);
	}
	else
	{
		e->cached_nblocks = repalloc(e->cached_nblocks,
									 sizeof(BlockNumber) * needed);
		e->num_open_segs = repalloc(e->num_open_segs, sizeof(int) * needed);
		e->seg_fds = repalloc(e->seg_fds, sizeof(TsMdfdVec *) * needed);
		/* Zero the newly grown tail of num_open_segs and seg_fds. */
		memset(&e->num_open_segs[oldsize], 0,
			   sizeof(int) * (needed - oldsize));
		memset(&e->seg_fds[oldsize], 0,
			   sizeof(TsMdfdVec *) * (needed - oldsize));
	}

	for (i = oldsize; i < needed; i++)
		e->cached_nblocks[i] = InvalidBlockNumber;

	e->alloc_forks = needed;

	MemoryContextSwitchTo(oldcxt);
}

/*
 * smgr_private_note_used
 *		Update min/max_used_fork after a chunk fork was first opened.
 */
static void
smgr_private_note_used(TsSmgrPrivate *e, ForkNumber forknum)
{
	if (forknum < e->min_used_fork)
		e->min_used_fork = forknum;
	if (forknum > e->max_used_fork)
		e->max_used_fork = forknum;
}

/*
 * ts_file_close
 *		file_close_hook callback.  Runs from smgrclose() after the kernel
 *		SMgrRelation is already removed from SMgrRelationHash; the passed
 *		rnode is a stack-local snapshot, so we key everything off it and
 *		never touch the (now-freed) SMgrRelation.  Releases every VFD
 *		the sidecar opened, deletes the per-relation memory context
 *		(which frees the three parallel arrays), and drops the sidecar
 *		entry from TsSmgrPrivateHash so it stays in lock-step with the
 *		kernel hash.
 */
static void
ts_file_close(RelFileNodeBackend rnode)
{
	TsSmgrPrivate *e;

	if (prev_file_close_hook)
		(*prev_file_close_hook) (rnode);

	e = smgr_private_lookup(rnode);

	if (e != NULL)
	{
		int			i,
					s;

		for (i = e->min_used_fork; i <= e->max_used_fork; i++)
		{
			if (i < 0 || i >= e->alloc_forks)
				continue;
			if (e->seg_fds[i] == NULL)
				continue;
			for (s = 0; s < e->num_open_segs[i]; s++)
				FileClose(e->seg_fds[i][s].mdfd_vfd);
		}
		MemoryContextDelete(e->cxt);

		/*
		 * HASH_REMOVE below invalidates our sidecar entry (dynahash puts
		 * it back on the freelist).  Do this last so we never touch `e`
		 * after removal.
		 */
		if (hash_search(TsSmgrPrivateHash,
						(void *) &rnode,
						HASH_REMOVE, NULL) == NULL)
			elog(ERROR, "time_series smgr sidecar hash lookup failed");
	}
}

/*
 * _ts_fdvec_resize
 *		Sidecar equivalent of md.c's _fdvec_resize.
 */
static void
_ts_fdvec_resize(TsSmgrPrivate *e, ForkNumber forknum, int nseg)
{
	MemoryContext oldcxt;

	if (nseg == 0)
	{
		if (e->num_open_segs[forknum] > 0)
		{
			pfree(e->seg_fds[forknum]);
			e->seg_fds[forknum] = NULL;
		}
	}
	else if (e->num_open_segs[forknum] == 0)
	{
		oldcxt = MemoryContextSwitchTo(e->cxt);
		e->seg_fds[forknum] = palloc(sizeof(TsMdfdVec) * nseg);
		MemoryContextSwitchTo(oldcxt);
	}
	else
	{
		oldcxt = MemoryContextSwitchTo(e->cxt);
		e->seg_fds[forknum] = repalloc(e->seg_fds[forknum],
									   sizeof(TsMdfdVec) * nseg);
		MemoryContextSwitchTo(oldcxt);
	}

	e->num_open_segs[forknum] = nseg;
}

/*
 * _ts_mdfd_segpath
 *		Build the on-disk path for (forknum, segno).  For chunk forks the
 *		registered relpath_hook converts forknum to the ..._ext<N> form.
 */
static char *
_ts_mdfd_segpath(SMgrRelation reln, ForkNumber forknum, BlockNumber segno)
{
	char	   *path;
	char	   *fullpath;

	path = relpath(reln->smgr_rnode, forknum);

	if (segno > 0)
	{
		fullpath = psprintf("%s.%u", path, segno);
		pfree(path);
	}
	else
		fullpath = path;

	return fullpath;
}

/*
 * _ts_mdnblocks
 *		Single-segment block count.
 */
static BlockNumber
_ts_mdnblocks(File vfd)
{
	off_t		len;

	len = FileSize(vfd);
	if (len < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not seek to end of file \"%s\": %m",
						FilePathName(vfd))));
	return (BlockNumber) (len / BLCKSZ);
}

/*
 * _ts_mdfd_openseg
 *		Open one segment file and add it to the sidecar's seg_fds array.
 */
static TsMdfdVec *
_ts_mdfd_openseg(SMgrRelation reln, ForkNumber forknum, BlockNumber segno,
				 int oflags)
{
	TsSmgrPrivate *e = smgr_private_get(reln);
	TsMdfdVec  *v;
	File		fd;
	char	   *fullpath;

	smgr_private_ensure_capacity(reln, e, forknum);

	fullpath = _ts_mdfd_segpath(reln, forknum, segno);
	fd = PathNameOpenFile(fullpath, O_RDWR | PG_BINARY | oflags);
	pfree(fullpath);

	if (fd < 0)
		return NULL;

	Assert(segno == (BlockNumber) e->num_open_segs[forknum]);

	_ts_fdvec_resize(e, forknum, segno + 1);

	v = &e->seg_fds[forknum][segno];
	v->mdfd_vfd = fd;
	v->mdfd_segno = segno;

	Assert(_ts_mdnblocks(fd) <= ((BlockNumber) RELSEG_SIZE));

	smgr_private_note_used(e, forknum);
	return v;
}

/*
 * smgr_private_openfork
 *		Equivalent of md.c's mdopenfork for chunk forks: open segment 0,
 *		return its TsMdfdVec or NULL/ereport per behavior.
 */
static TsMdfdVec *
smgr_private_openfork(SMgrRelation reln, ForkNumber forknum, int behavior)
{
	TsSmgrPrivate *e = smgr_private_get(reln);
	TsMdfdVec  *v;
	File		fd;
	char	   *path;

	smgr_private_ensure_capacity(reln, e, forknum);

	if (e->num_open_segs[forknum] > 0)
		return &e->seg_fds[forknum][0];

	path = relpath(reln->smgr_rnode, forknum);
	fd = PathNameOpenFile(path, O_RDWR | PG_BINARY);

	if (fd < 0)
	{
		if ((behavior & TS_EXT_RETURN_NULL) && FILE_POSSIBLY_DELETED(errno))
		{
			pfree(path);
			return NULL;
		}
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", path)));
	}
	pfree(path);

	_ts_fdvec_resize(e, forknum, 1);
	v = &e->seg_fds[forknum][0];
	v->mdfd_vfd = fd;
	v->mdfd_segno = 0;

	Assert(_ts_mdnblocks(fd) <= ((BlockNumber) RELSEG_SIZE));

	smgr_private_note_used(e, forknum);
	return v;
}

/*
 * _ts_mdfd_getseg
 *		Find or create the segment of the chunk fork holding 'blkno'.
 *		Direct port of md.c's _mdfd_getseg.
 */
static TsMdfdVec *
_ts_mdfd_getseg(SMgrRelation reln, ForkNumber forknum, BlockNumber blkno,
				bool skipFsync, int behavior)
{
	TsSmgrPrivate *e = smgr_private_get(reln);
	TsMdfdVec  *v;
	BlockNumber targetseg;
	BlockNumber nextsegno;

	Assert(behavior & (TS_EXT_FAIL | TS_EXT_CREATE | TS_EXT_RETURN_NULL));

	smgr_private_ensure_capacity(reln, e, forknum);

	targetseg = blkno / ((BlockNumber) RELSEG_SIZE);

	if (targetseg < (BlockNumber) e->num_open_segs[forknum])
		return &e->seg_fds[forknum][targetseg];

	if (e->num_open_segs[forknum] > 0)
		v = &e->seg_fds[forknum][e->num_open_segs[forknum] - 1];
	else
	{
		v = smgr_private_openfork(reln, forknum, behavior);
		if (v == NULL)
			return NULL;
	}

	for (nextsegno = e->num_open_segs[forknum];
		 nextsegno <= targetseg;
		 nextsegno++)
	{
		BlockNumber nblocks = _ts_mdnblocks(v->mdfd_vfd);
		int			flags = 0;

		Assert(nextsegno == v->mdfd_segno + 1);

		if (nblocks > ((BlockNumber) RELSEG_SIZE))
			elog(FATAL, "segment too big");

		if ((behavior & TS_EXT_CREATE) ||
			(InRecovery && (behavior & TS_EXT_CREATE_RECOVERY)))
		{
			if (nblocks < ((BlockNumber) RELSEG_SIZE))
			{
				char	   *zerobuf = palloc0(BLCKSZ);

				/* Recurse through the dispatcher so the extend path runs. */
				smgrextend(reln, forknum,
						   nextsegno * ((BlockNumber) RELSEG_SIZE) - 1,
						   zerobuf, skipFsync);
				pfree(zerobuf);
			}
			flags = O_CREAT;
		}
		else if (nblocks < ((BlockNumber) RELSEG_SIZE))
		{
			if (behavior & TS_EXT_RETURN_NULL)
			{
				errno = ENOENT;
				return NULL;
			}
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open file \"%s\" (target block %u): previous segment is only %u blocks",
							_ts_mdfd_segpath(reln, forknum, nextsegno),
							blkno, nblocks)));
		}

		v = _ts_mdfd_openseg(reln, forknum, nextsegno, flags);

		if (v == NULL)
		{
			if ((behavior & TS_EXT_RETURN_NULL) && FILE_POSSIBLY_DELETED(errno))
				return NULL;
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open file \"%s\" (target block %u): %m",
							_ts_mdfd_segpath(reln, forknum, nextsegno),
							blkno)));
		}
	}

	return v;
}

/*
 * smgr_private_register_dirty
 *		Queue an fsync request for a chunk-fork segment.  Rides
 *		SYNC_HANDLER_MD; mdsyncfiletag's path-fallback opens chunk files
 *		by their relpath_hook-supplied filename.
 */
static void
smgr_private_register_dirty(SMgrRelation reln, ForkNumber forknum, TsMdfdVec *seg)
{
	FileTag		tag;

	TS_INIT_MD_FILETAG(tag, reln->smgr_rnode.node, forknum, seg->mdfd_segno);

	Assert(!SmgrIsTemp(reln));

	if (!RegisterSyncRequest(&tag, SYNC_REQUEST, false /* retryOnError */ ))
	{
		ereport(DEBUG1,
				(errmsg_internal("could not forward fsync request because request queue is full")));

		if (FileSync(seg->mdfd_vfd, WAIT_EVENT_DATA_FILE_SYNC) < 0)
			ereport(data_sync_elevel(ERROR),
					(errcode_for_file_access(),
					 errmsg("could not fsync file \"%s\": %m",
							FilePathName(seg->mdfd_vfd))));
	}
}

/* ----------------------------------------------------------------
 *	Chunk-fork op implementations (mirror md.c on TsSmgrPrivate).
 * ----------------------------------------------------------------
 */

static void
smgr_private_create(SMgrRelation reln, ForkNumber forknum, bool isRedo)
{
	TsSmgrPrivate *e = smgr_private_get(reln);
	TsMdfdVec  *mdfd;
	char	   *path;
	File		fd;

	smgr_private_ensure_capacity(reln, e, forknum);

	if (isRedo && e->num_open_segs[forknum] > 0)
		return;

	Assert(e->num_open_segs[forknum] == 0);

	TablespaceCreateDbspace(reln->smgr_rnode.node.spcNode,
							reln->smgr_rnode.node.dbNode,
							isRedo);

	path = relpath(reln->smgr_rnode, forknum);
	fd = PathNameOpenFile(path, O_RDWR | O_CREAT | O_EXCL | PG_BINARY);
	if (fd < 0)
	{
		int			save_errno = errno;

		if (isRedo)
			fd = PathNameOpenFile(path, O_RDWR | PG_BINARY);
		if (fd < 0)
		{
			errno = save_errno;
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not create file \"%s\": %m", path)));
		}
	}
	pfree(path);

	_ts_fdvec_resize(e, forknum, 1);
	mdfd = &e->seg_fds[forknum][0];
	mdfd->mdfd_vfd = fd;
	mdfd->mdfd_segno = 0;

	smgr_private_note_used(e, forknum);
}

static bool
smgr_private_exists(SMgrRelation reln, ForkNumber forknum)
{
	TsSmgrPrivate *e = smgr_private_get(reln);

	smgr_private_ensure_capacity(reln, e, forknum);

	/* Close first to notice unlinks since the last open. */
	{
		int			nopensegs = e->num_open_segs[forknum];

		while (nopensegs > 0)
		{
			TsMdfdVec  *v = &e->seg_fds[forknum][nopensegs - 1];

			FileClose(v->mdfd_vfd);
			_ts_fdvec_resize(e, forknum, nopensegs - 1);
			nopensegs--;
		}
	}

	return (smgr_private_openfork(reln, forknum, TS_EXT_RETURN_NULL) != NULL);
}

static void
smgr_private_close(SMgrRelation reln, ForkNumber forknum)
{
	TsSmgrPrivate *e = smgr_private_lookup(reln->smgr_rnode);
	int			nopensegs;

	if (e == NULL || forknum >= e->alloc_forks)
		return;

	nopensegs = e->num_open_segs[forknum];
	if (nopensegs == 0)
		return;

	while (nopensegs > 0)
	{
		TsMdfdVec  *v = &e->seg_fds[forknum][nopensegs - 1];

		FileClose(v->mdfd_vfd);
		_ts_fdvec_resize(e, forknum, nopensegs - 1);
		nopensegs--;
	}
}

static void
smgr_private_extend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
				char *buffer, bool skipFsync)
{
	TsSmgrPrivate *e = smgr_private_get(reln);
	off_t		seekpos;
	int			nbytes;
	TsMdfdVec  *v;

	if (blocknum == InvalidBlockNumber)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("cannot extend file \"%s\" beyond %u blocks",
						relpath(reln->smgr_rnode, forknum),
						InvalidBlockNumber)));

	v = _ts_mdfd_getseg(reln, forknum, blocknum, skipFsync, TS_EXT_CREATE);

	seekpos = (off_t) BLCKSZ * (blocknum % ((BlockNumber) RELSEG_SIZE));
	Assert(seekpos < (off_t) BLCKSZ * RELSEG_SIZE);

	nbytes = FileWrite(v->mdfd_vfd, buffer, BLCKSZ, seekpos,
					   WAIT_EVENT_DATA_FILE_EXTEND);
	if (nbytes != BLCKSZ)
	{
		if (nbytes < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not extend file \"%s\": %m",
							FilePathName(v->mdfd_vfd)),
					 errhint("Check free disk space.")));
		ereport(ERROR,
				(errcode(ERRCODE_DISK_FULL),
				 errmsg("could not extend file \"%s\": wrote only %d of %d bytes at block %u",
						FilePathName(v->mdfd_vfd),
						nbytes, BLCKSZ, blocknum),
				 errhint("Check free disk space.")));
	}

	if (!skipFsync && !SmgrIsTemp(reln))
		smgr_private_register_dirty(reln, forknum, v);

	/* Update our shadow cache, mirroring smgrextend's kernel fast-path. */
	if (e->cached_nblocks[forknum] == blocknum)
		e->cached_nblocks[forknum] = blocknum + 1;
	else
		e->cached_nblocks[forknum] = InvalidBlockNumber;

	Assert(_ts_mdnblocks(v->mdfd_vfd) <= ((BlockNumber) RELSEG_SIZE));
}

static bool
smgr_private_prefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum)
{
#ifdef USE_PREFETCH
	off_t		seekpos;
	TsMdfdVec  *v;

	v = _ts_mdfd_getseg(reln, forknum, blocknum, false,
						InRecovery ? TS_EXT_RETURN_NULL : TS_EXT_FAIL);
	if (v == NULL)
		return false;

	seekpos = (off_t) BLCKSZ * (blocknum % ((BlockNumber) RELSEG_SIZE));
	Assert(seekpos < (off_t) BLCKSZ * RELSEG_SIZE);

	(void) FilePrefetch(v->mdfd_vfd, seekpos, BLCKSZ,
						WAIT_EVENT_DATA_FILE_PREFETCH);
#endif
	return true;
}

static void
smgr_private_read(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
			  char *buffer)
{
	off_t		seekpos;
	int			nbytes;
	TsMdfdVec  *v;

	v = _ts_mdfd_getseg(reln, forknum, blocknum, false,
						TS_EXT_FAIL | TS_EXT_CREATE_RECOVERY);

	seekpos = (off_t) BLCKSZ * (blocknum % ((BlockNumber) RELSEG_SIZE));
	Assert(seekpos < (off_t) BLCKSZ * RELSEG_SIZE);

	nbytes = FileRead(v->mdfd_vfd, buffer, BLCKSZ, seekpos,
					  WAIT_EVENT_DATA_FILE_READ);
	if (nbytes != BLCKSZ)
	{
		if (nbytes < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read block %u in file \"%s\": %m",
							blocknum, FilePathName(v->mdfd_vfd))));

		if (zero_damaged_pages || InRecovery)
			MemSet(buffer, 0, BLCKSZ);
		else
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("could not read block %u in file \"%s\": read only %d of %d bytes",
							blocknum, FilePathName(v->mdfd_vfd),
							nbytes, BLCKSZ)));
	}
}

static void
smgr_private_write(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
			   char *buffer, bool skipFsync)
{
	off_t		seekpos;
	int			nbytes;
	TsMdfdVec  *v;

	v = _ts_mdfd_getseg(reln, forknum, blocknum, skipFsync,
						TS_EXT_FAIL | TS_EXT_CREATE_RECOVERY);

	seekpos = (off_t) BLCKSZ * (blocknum % ((BlockNumber) RELSEG_SIZE));
	Assert(seekpos < (off_t) BLCKSZ * RELSEG_SIZE);

	nbytes = FileWrite(v->mdfd_vfd, buffer, BLCKSZ, seekpos,
					   WAIT_EVENT_DATA_FILE_WRITE);
	if (nbytes != BLCKSZ)
	{
		if (nbytes < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write block %u in file \"%s\": %m",
							blocknum, FilePathName(v->mdfd_vfd))));
		ereport(ERROR,
				(errcode(ERRCODE_DISK_FULL),
				 errmsg("could not write block %u in file \"%s\": wrote only %d of %d bytes",
						blocknum, FilePathName(v->mdfd_vfd),
						nbytes, BLCKSZ),
				 errhint("Check free disk space.")));
	}

	if (!skipFsync && !SmgrIsTemp(reln))
		smgr_private_register_dirty(reln, forknum, v);
}

static void
smgr_private_writeback(SMgrRelation reln, ForkNumber forknum,
				   BlockNumber blocknum, BlockNumber nblocks)
{
	while (nblocks > 0)
	{
		BlockNumber nflush = nblocks;
		off_t		seekpos;
		TsMdfdVec  *v;
		int			segnum_start,
					segnum_end;

		v = _ts_mdfd_getseg(reln, forknum, blocknum, true,
							TS_EXT_RETURN_NULL);
		if (!v)
			return;

		segnum_start = blocknum / RELSEG_SIZE;
		segnum_end = (blocknum + nblocks - 1) / RELSEG_SIZE;
		if (segnum_start != segnum_end)
			nflush = RELSEG_SIZE - (blocknum % ((BlockNumber) RELSEG_SIZE));

		Assert(nflush >= 1);
		Assert(nflush <= nblocks);

		seekpos = (off_t) BLCKSZ * (blocknum % ((BlockNumber) RELSEG_SIZE));
		FileWriteback(v->mdfd_vfd, seekpos, (off_t) BLCKSZ * nflush,
					  WAIT_EVENT_DATA_FILE_FLUSH);

		nblocks -= nflush;
		blocknum += nflush;
	}
}

static BlockNumber
smgr_private_nblocks(SMgrRelation reln, ForkNumber forknum)
{
	TsSmgrPrivate *e = smgr_private_get(reln);
	TsMdfdVec  *v;
	BlockNumber nblocks;
	BlockNumber segno;

	smgr_private_openfork(reln, forknum, TS_EXT_FAIL);
	Assert(e->num_open_segs[forknum] > 0);

	segno = e->num_open_segs[forknum] - 1;
	v = &e->seg_fds[forknum][segno];

	for (;;)
	{
		nblocks = _ts_mdnblocks(v->mdfd_vfd);
		if (nblocks > ((BlockNumber) RELSEG_SIZE))
			elog(FATAL, "segment too big");
		if (nblocks < ((BlockNumber) RELSEG_SIZE))
			return (segno * ((BlockNumber) RELSEG_SIZE)) + nblocks;

		segno++;
		v = _ts_mdfd_openseg(reln, forknum, segno, 0);
		if (v == NULL)
			return segno * ((BlockNumber) RELSEG_SIZE);
	}
}

static void
smgr_private_truncate(SMgrRelation reln, ForkNumber forknum, BlockNumber nblocks)
{
	TsSmgrPrivate *e = smgr_private_get(reln);
	BlockNumber curnblk;
	BlockNumber priorblocks;
	int			curopensegs;

	curnblk = smgr_private_nblocks(reln, forknum);
	if (nblocks > curnblk)
	{
		if (InRecovery)
			return;
		ereport(ERROR,
				(errmsg("could not truncate file \"%s\" to %u blocks: it's only %u blocks now",
						relpath(reln->smgr_rnode, forknum),
						nblocks, curnblk)));
	}

	if (nblocks == curnblk && forknum != MAIN_FORKNUM)
		return;

	curopensegs = e->num_open_segs[forknum];
	while (curopensegs > 0)
	{
		TsMdfdVec  *v;

		priorblocks = (curopensegs - 1) * RELSEG_SIZE;
		v = &e->seg_fds[forknum][curopensegs - 1];

		if (priorblocks > nblocks)
		{
			if (FileTruncate(v->mdfd_vfd, 0,
							 WAIT_EVENT_DATA_FILE_TRUNCATE) < 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not truncate file \"%s\": %m",
								FilePathName(v->mdfd_vfd))));

			if (!SmgrIsTemp(reln))
				smgr_private_register_dirty(reln, forknum, v);

			Assert(v != &e->seg_fds[forknum][0]);

			FileClose(v->mdfd_vfd);
			_ts_fdvec_resize(e, forknum, curopensegs - 1);
		}
		else if (priorblocks + ((BlockNumber) RELSEG_SIZE) > nblocks)
		{
			BlockNumber lastsegblocks = nblocks - priorblocks;

			if (FileTruncate(v->mdfd_vfd,
							 (off_t) lastsegblocks * BLCKSZ,
							 WAIT_EVENT_DATA_FILE_TRUNCATE) < 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not truncate file \"%s\" to %u blocks: %m",
								FilePathName(v->mdfd_vfd), nblocks)));
			if (!SmgrIsTemp(reln))
				smgr_private_register_dirty(reln, forknum, v);
		}
		else
			break;

		curopensegs--;
	}

	if (forknum < e->alloc_forks)
		e->cached_nblocks[forknum] = InvalidBlockNumber;
}

static void
smgr_private_immedsync(SMgrRelation reln, ForkNumber forknum)
{
	TsSmgrPrivate *e = smgr_private_get(reln);
	int			segno;
	int			min_inactive_seg;

	smgr_private_nblocks(reln, forknum);

	min_inactive_seg = segno = e->num_open_segs[forknum];

	while (_ts_mdfd_openseg(reln, forknum, segno, 0) != NULL)
		segno++;

	while (segno > 0)
	{
		TsMdfdVec  *v = &e->seg_fds[forknum][segno - 1];

		if (FileSync(v->mdfd_vfd, WAIT_EVENT_DATA_FILE_IMMEDIATE_SYNC) < 0)
			ereport(data_sync_elevel(ERROR),
					(errcode_for_file_access(),
					 errmsg("could not fsync file \"%s\": %m",
							FilePathName(v->mdfd_vfd))));

		if (segno > min_inactive_seg)
		{
			FileClose(v->mdfd_vfd);
			_ts_fdvec_resize(e, forknum, segno - 1);
		}
		segno--;
	}
}

/* ----------------------------------------------------------------
 *	Per-callback routers: route on forknum.
 * ----------------------------------------------------------------
 */

static void
ts_mdcreate(SMgrRelation reln, ForkNumber forknum, bool isRedo)
{
	if (forknum <= MAX_FORKNUM)
	{
		mdcreate(reln, forknum, isRedo);
		return;
	}
	smgr_private_create(reln, forknum, isRedo);
}

static bool
ts_mdexists(SMgrRelation reln, ForkNumber forknum)
{
	if (forknum <= MAX_FORKNUM)
		return mdexists(reln, forknum);
	return smgr_private_exists(reln, forknum);
}

static void
ts_mdclose(SMgrRelation reln, ForkNumber forknum)
{
	if (forknum <= MAX_FORKNUM)
	{
		mdclose(reln, forknum);
		return;
	}
	smgr_private_close(reln, forknum);
}

static void
ts_mdextend(SMgrRelation reln, ForkNumber forknum,
			BlockNumber blocknum, char *buffer, bool skipFsync)
{
	if (forknum <= MAX_FORKNUM)
	{
		mdextend(reln, forknum, blocknum, buffer, skipFsync);
		return;
	}
	smgr_private_extend(reln, forknum, blocknum, buffer, skipFsync);
}

static bool
ts_mdprefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum)
{
	if (forknum <= MAX_FORKNUM)
		return mdprefetch(reln, forknum, blocknum);
	return smgr_private_prefetch(reln, forknum, blocknum);
}

static void
ts_mdread(SMgrRelation reln, ForkNumber forknum,
		  BlockNumber blocknum, char *buffer)
{
	if (forknum <= MAX_FORKNUM)
	{
		mdread(reln, forknum, blocknum, buffer);
		return;
	}
	smgr_private_read(reln, forknum, blocknum, buffer);
}

static void
ts_mdwrite(SMgrRelation reln, ForkNumber forknum,
		   BlockNumber blocknum, char *buffer, bool skipFsync)
{
	if (forknum <= MAX_FORKNUM)
	{
		mdwrite(reln, forknum, blocknum, buffer, skipFsync);
		return;
	}
	smgr_private_write(reln, forknum, blocknum, buffer, skipFsync);
}

static void
ts_mdwriteback(SMgrRelation reln, ForkNumber forknum,
			   BlockNumber blocknum, BlockNumber nblocks)
{
	if (forknum <= MAX_FORKNUM)
	{
		mdwriteback(reln, forknum, blocknum, nblocks);
		return;
	}
	smgr_private_writeback(reln, forknum, blocknum, nblocks);
}

static BlockNumber
ts_mdnblocks(SMgrRelation reln, ForkNumber forknum)
{
	if (forknum <= MAX_FORKNUM)
		return mdnblocks(reln, forknum);
	return smgr_private_nblocks(reln, forknum);
}

static void
ts_mdtruncate(SMgrRelation reln, ForkNumber forknum, BlockNumber nblocks)
{
	if (forknum <= MAX_FORKNUM)
	{
		mdtruncate(reln, forknum, nblocks);
		return;
	}
	smgr_private_truncate(reln, forknum, nblocks);
}

static void
ts_mdimmedsync(SMgrRelation reln, ForkNumber forknum)
{
	if (forknum <= MAX_FORKNUM)
	{
		mdimmedsync(reln, forknum);
		return;
	}
	smgr_private_immedsync(reln, forknum);
}

/*
 * Custom f_smgr.  smgr_init / smgr_open / smgr_unlink delegate to md
 * unchanged — they operate on the kernel's static per-fork arrays only.
 * The rest route on forknum.
 */
static const f_smgr ts_smgr_impl = {
	.smgr_name = "time_series",
	.smgr_init = mdinit,
	.smgr_shutdown = NULL,
	.smgr_open = mdopen,
	.smgr_close = ts_mdclose,
	.smgr_create = ts_mdcreate,
	.smgr_exists = ts_mdexists,
	.smgr_unlink = mdunlink,
	.smgr_extend = ts_mdextend,
	.smgr_prefetch = ts_mdprefetch,
	.smgr_read = ts_mdread,
	.smgr_write = ts_mdwrite,
	.smgr_writeback = ts_mdwriteback,
	.smgr_nblocks = ts_mdnblocks,
	.smgr_truncate = ts_mdtruncate,
	.smgr_immedsync = ts_mdimmedsync,
};

/*
 * ts_smgr_hook
 *		Replace reln->smgr with our wrapper for every relation.  This
 *		ensures chunk forks are routed correctly even during WAL replay
 *		where SMGR_MD is hardcoded.
 */
static void
ts_smgr_hook(SMgrRelation reln, BackendId backend,
			 SMgrImpl which, Relation rel)
{
	if (prev_smgr_hook)
		prev_smgr_hook(reln, backend, which, rel);

	reln->smgr = &ts_smgr_impl;
}

/*
 * ts_smgr_invalidate_nblocks
 *		Invalidate the cached fork length, whether it lives in the kernel
 *		static cache (standard fork) or the sidecar (chunk fork).
 *		Replacement for direct writes to reln->smgr_cached_nblocks[forknum]
 *		from extension code paths.
 */
void
ts_smgr_invalidate_nblocks(SMgrRelation reln, ForkNumber forknum)
{
	if (forknum <= MAX_FORKNUM)
	{
		reln->smgr_cached_nblocks[forknum] = InvalidBlockNumber;
		return;
	}

	{
		TsSmgrPrivate *e = smgr_private_lookup(reln->smgr_rnode);

		if (e != NULL && forknum < e->alloc_forks)
			e->cached_nblocks[forknum] = InvalidBlockNumber;
	}
}

/*
 * ts_file_unlink_hook
 *		Per-relation cleanup for chunk forks at drop / truncate time.
 *
 *		Standard smgrdounlinkall() only iterates forknum 0..MAX_FORKNUM,
 *		so chunk forks (TS_FIRST_CHUNKNUM..TS_MAX_CHUNK_FORKNUM, mapped
 *		on disk to "<relNode>_ts_<N>" by ts_fork_name.c's relpath_hook)
 *		are orphaned on DROP TABLE / TRUNCATE / ALTER-rewrites.
 *
 *		The kernel calls file_unlink_hook for every relfilenode being
 *		dropped (commit, abort, and WAL-replay paths all funnel through
 *		smgrdounlinkall, so this single hook covers them all).  We
 *		readdir() the relation's database directory, match the
 *		"<relNode>_ts_<N>" prefix, and call mdunlink for each matching
 *		chunk fork.  mdunlink itself handles any .1/.2/... continuation
 *		segments once it's told the forknum.
 *
 *		Chained: if a prior hook (e.g. diskquota's
 *		active_table_hook_smgrunlink) was registered, call it first so
 *		it sees the same per-relation event the kernel reports.
 */
static void
ts_file_unlink_hook(RelFileNodeBackend rnode)
{
	char	   *dir;
	char		prefix[32];
	size_t		plen;
	DIR		   *dh;
	struct dirent *de;

	if (prev_file_unlink_hook)
		(*prev_file_unlink_hook)(rnode);

	dir = GetDatabasePath(rnode.node.dbNode, rnode.node.spcNode);
	plen = snprintf(prefix, sizeof(prefix), "%u_ts_", rnode.node.relNode);

	dh = AllocateDir(dir);
	while ((de = ReadDir(dh, dir)) != NULL)
	{
		const char *suf;
		long		n;
		char	   *endptr;

		if (strncmp(de->d_name, prefix, plen) != 0)
			continue;

		suf = de->d_name + plen;

		/*
		 * Skip multi-segment continuation files ("..._ts_<N>.1", ".2",
		 * ...).  When we call mdunlink with the parsed forknum, md.c
		 * walks every .seg under the hood, so we'd double-unlink and
		 * log spurious WARNINGs if we drove each suffix ourselves.
		 */
		if (strchr(suf, '.') != NULL)
			continue;

		errno = 0;
		n = strtol(suf, &endptr, 10);
		if (errno != 0 || *endptr != '\0' || n < 0 ||
			n > (TS_MAX_CHUNK_FORKNUM - TS_FIRST_CHUNKNUM))
			continue;

		/*
		 * isRedo=false: this hook may run during WAL replay too, but
		 * we only reach mdunlink for files readdir just enumerated,
		 * so the "missing file" WARNING branch in mdunlink can't fire.
		 */
		mdunlink(rnode, (ForkNumber) (TS_FIRST_CHUNKNUM + n), false);
	}
	FreeDir(dh);
	pfree(dir);
}

/*
 * ts_smgr_init
 *		Install hooks.  Called from _PG_init.
 */
void
ts_smgr_init(void)
{
	prev_smgr_hook = smgr_hook;
	smgr_hook = ts_smgr_hook;

	prev_file_close_hook = file_close_hook;
	file_close_hook = ts_file_close;

	prev_file_unlink_hook = file_unlink_hook;
	file_unlink_hook = ts_file_unlink_hook;
}
