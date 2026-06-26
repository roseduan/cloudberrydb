/*-------------------------------------------------------------------------
 *
 * walredoproc.c
 *	  Entry point for WAL redo helper
 *
 *
 * This file contains an alternative main() function for the 'postgres'
 * binary. In the special mode, we go into a special mode that's similar
 * to the single user mode. We don't launch postmaster or any auxiliary
 * processes. Instead, we wait for command from 'stdin', and respond to
 * 'stdout'.
 *
 * The protocol through stdin/stdout is loosely based on the libpq protocol.
 * The process accepts messages through stdin, and each message has the format:
 *
 * char   msgtype;
 * int32  length; // length of message including 'length' but excluding
 *                // 'msgtype', in network byte order
 * <payload>
 *
 * There are three message types:
 *
 * BeginRedoForBlock ('B'): Prepare for WAL replay for given block
 * PushPage ('P'): Copy a page image (in the payload) to buffer cache
 * ApplyRecord ('A'): Apply a WAL record (in the payload)
 * GetPage ('G'): Return a page image from buffer cache.
 *
 * Currently, you only get a response to GetPage requests; the response is
 * simply a 8k page, without any headers. Errors are logged to stderr.
 *
 * FIXME:
 * - this currently requires a valid PGDATA, and creates a lock file there
 *   like a normal postmaster. There's no fundamental reason for that, though.
 * - should have EndRedoForBlock, and flush page cache, to allow using this
 *   mechanism for more than one block without restarting the process.
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <fcntl.h>
#include <limits.h>
#include <signal.h>
#include <unistd.h>
#include <sys/socket.h>
#include "access/distributedlog.h"
#include "cdb/cdbendpoint.h"
#include "cdb/cdbvars.h"
#include "executor/nodeShareInputScan.h"
#include "postmaster/backoff.h"
#include "replication/gp_replication.h"
#include "storage/checksum.h"
#include "utils/backend_cancel.h"
#include "utils/faultinjector.h"
#include "utils/gpexpand.h"
#include "utils/resource_manager.h"
#include "utils/sharedsnapshot.h"
#include "utils/workfile_mgr.h"
#ifdef HAVE_SYS_SELECT_H
#include <sys/select.h>
#endif
#ifdef HAVE_SYS_RESOURCE_H
#include <sys/time.h>
#include <sys/resource.h>
#endif

#if defined(HAVE_LIBSECCOMP) && defined(__GLIBC__)
#define MALLOC_NO_MMAP
#include <malloc.h>
#endif

#ifndef HAVE_GETRUSAGE
#include "rusagestub.h"
#endif

#include "access/clog.h"
#include "access/commit_ts.h"
#include "access/heapam.h"
#include "access/multixact.h"
#include "access/nbtree.h"
#include "access/subtrans.h"
#include "access/syncscan.h"
#include "access/twophase.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#if PG_VERSION_NUM >= 150000
#include "access/xlogrecovery.h"
#endif
#include "access/xlogutils.h"
#include "catalog/pg_class.h"
#include "commands/async.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/autovacuum.h"
#include "postmaster/bgworker_internals.h"
#include "postmaster/bgwriter.h"
#include "postmaster/postmaster.h"
#include "replication/logicallauncher.h"
#include "replication/origin.h"
#include "replication/slot.h"
#include "replication/walreceiver.h"
#include "replication/walsender.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/dsm.h"
#include "storage/ipc.h"
#include "storage/pg_shmem.h"
#include "storage/pmsignal.h"
#include "storage/predicate.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/procsignal.h"
#include "storage/sinvaladt.h"
#include "storage/smgr.h"
#include "storage/spin.h"
#include "tcop/tcopprot.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/snapmgr.h"

#include "inmem_smgr.h"

#ifdef HAVE_LIBSECCOMP
#include "unionstore_seccomp.h"
#endif

PG_MODULE_MAGIC;

static int	ReadRedoCommand(StringInfo inBuf);
static void BeginRedoForBlock(StringInfo input_message);
static void PushPage(StringInfo input_message);
static void ApplyRecord(StringInfo input_message);
static void apply_error_callback(void *arg);
static bool redo_block_filter(XLogReaderState *record, uint8 block_id);
static void GetPage(StringInfo input_message);
static ssize_t buffered_read(void *buf, size_t count);
static void CreateFakeSharedMemoryAndSemaphores(void);
static Buffer NeonReadBuffer_common(SMgrRelation smgr, char relpersistence, ForkNumber forkNum,
                                      BlockNumber blockNum, ReadBufferMode mode,
                                      BufferAccessStrategy strategy, bool *hit);
void WalRedoMain(int argc, char *argv[]);

static BufferTag target_redo_tag;

static XLogReaderState *reader_state;

#define TRACE DEBUG5

#ifdef HAVE_LIBSECCOMP
static void
enter_seccomp_mode(void)
{
	PgSeccompRule syscalls[] =
	{
		/* Hard requirements */
		PG_SCMP_ALLOW(exit_group),
		PG_SCMP_ALLOW(pselect6),
		PG_SCMP_ALLOW(read),
		PG_SCMP_ALLOW(select),
		PG_SCMP_ALLOW(write),

		/* Memory allocation */
		PG_SCMP_ALLOW(brk),
#ifndef MALLOC_NO_MMAP
		/* TODO: musl doesn't have mallopt */
		PG_SCMP_ALLOW(mmap),
		PG_SCMP_ALLOW(munmap),
#endif
		/*
		 * getpid() is called on assertion failure, in ExceptionalCondition.
		 * It's not really needed, but seems pointless to hide it either. The
		 * system call unlikely to expose a kernel vulnerability, and the PID
		 * is stored in MyProcPid anyway.
		 */
		PG_SCMP_ALLOW(getpid),

		/* Enable those for a proper shutdown.
		PG_SCMP_ALLOW(munmap),
		PG_SCMP_ALLOW(shmctl),
		PG_SCMP_ALLOW(shmdt),
		PG_SCMP_ALLOW(unlink), // shm_unlink
	 */
	};

#ifdef MALLOC_NO_MMAP
	/* Ask glibc not to use mmap() */
	mallopt(M_MMAP_MAX, 0);
#endif

	seccomp_load_rules(syscalls, lengthof(syscalls));
}
#endif /* HAVE_LIBSECCOMP */

/*
 * Buffer with target WAL redo page.
 * We must not evict this page from the buffer pool, but we cannot just keep it pinned because
 * some WAL redo functions expect the page to not be pinned. So we have a special check in
 * localbuf.c to prevent this buffer from being evicted.
 */
Buffer		wal_redo_buffer;

/*
 * Entry point for the WAL redo process.
 *
 * Performs similar initialization as PostgresMain does for normal
 * backend processes. Some initialization was done in CallExtMain
 * already.
 */
void
WalRedoMain(int argc, char *argv[])
{
	int			firstchar;
	StringInfoData input_message;
#ifdef HAVE_LIBSECCOMP
	bool		enable_seccomp;
#endif

	am_wal_redo_postgres = true;

	/*
	 * WAL redo does not need a large number of buffers. And speed of
	 * DropRelFileNodeAllLocalBuffers() is proportional to the number of
	 * buffers. So let's keep it small (default value is 1024)
	 */
	num_temp_buffers = 4;
	NBuffers = 4;

	/*
	 * install the simple in-memory smgr
	 */
	inmen_prev_smgr_hook = smgr_hook;
	smgr_hook = inmem_smgr_init;
	smgr_init_hook = smgr_init_inmem;
	ReadBuffer_hook = (ReadBuffer_hook_type) NeonReadBuffer_common;


	/* Initialize MaxBackends (if under postmaster, was done already) */
	MaxConnections = 1;
	max_worker_processes = 0;
	max_parallel_workers = 0;
	max_wal_senders = 0;
	InitializeMaxBackends();

#if PG_VERSION_NUM >= 150000
	process_shmem_requests();
	InitializeShmemGUCs();

	/*
	 * This will try to access data directory which we do not set.
	 * Seems to be pretty safe to disable.
	 */
	/* InitializeWalConsistencyChecking(); */
#endif

	/*
	 * We have our own version of CreateSharedMemoryAndSemaphores() that
	 * sets up local memory instead of shared one.
	 */
	CreateFakeSharedMemoryAndSemaphores();

	/*
	 * Remember stand-alone backend startup time,roughly at the same point
	 * during startup that postmaster does so.
	 */
	PgStartTime = GetCurrentTimestamp();

	/*
	 * Create a per-backend PGPROC struct in shared memory. We must do
	 * this before we can use LWLocks.
	 */
	InitAuxiliaryProcess();

	SetProcessingMode(NormalProcessing);

	/* Redo routines won't work if we're not "in recovery" */
	InRecovery = true;

	/*
	 * Create the memory context we will use in the main loop.
	 *
	 * MessageContext is reset once per iteration of the main loop, ie, upon
	 * completion of processing of each command message from the client.
	 */
	MessageContext = AllocSetContextCreate(TopMemoryContext,
										   "MessageContext",
										   ALLOCSET_DEFAULT_SIZES);

	/* we need a ResourceOwner to hold buffer pins */
	Assert(CurrentResourceOwner == NULL);
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "wal redo");

	/* Initialize resource managers */
	for (int rmid = 0; rmid <= RM_MAX_ID; rmid++)
	{
		if (RmgrTable[rmid].rm_startup != NULL)
			RmgrTable[rmid].rm_startup();
	}
	reader_state = XLogReaderAllocate(wal_segment_size, NULL, XL_ROUTINE(), NULL);

#ifdef HAVE_LIBSECCOMP
	/* We prefer opt-out to opt-in for greater security */
	enable_seccomp = true;
	for (int i = 1; i < argc; i++)
		if (strcmp(argv[i], "--disable-seccomp") == 0)
			enable_seccomp = false;

	/*
	 * We deliberately delay the transition to the seccomp mode
	 * until it's time to enter the main processing loop;
	 * else we'd have to add a lot more syscalls to the allowlist.
	 */
	if (enable_seccomp)
		enter_seccomp_mode();
#endif /* HAVE_LIBSECCOMP */

	/*
	 * Main processing loop
	 */
	MemoryContextSwitchTo(MessageContext);
	initStringInfo(&input_message);

	for (;;)
	{
		/* Release memory left over from prior query cycle. */
		resetStringInfo(&input_message);

		set_ps_display("idle");

		/*
		 * (3) read a command (loop blocks here)
		 */
		firstchar = ReadRedoCommand(&input_message);
		switch (firstchar)
		{
			case 'B':			/* BeginRedoForBlock */
				BeginRedoForBlock(&input_message);
				break;

			case 'P':			/* PushPage */
				PushPage(&input_message);
				break;

			case 'A':			/* ApplyRecord */
				ApplyRecord(&input_message);
				break;

			case 'G':			/* GetPage */
				GetPage(&input_message);
				break;

				/*
				 * EOF means we're done. Perform normal shutdown.
				 */
			case EOF:
				ereport(LOG,
						(errmsg("received EOF on stdin, shutting down")));

#ifdef HAVE_LIBSECCOMP
				/*
				 * Skip the shutdown sequence, leaving some garbage behind.
				 * Hopefully, postgres will clean it up in the next run.
				 * This way we don't have to enable extra syscalls, which is nice.
				 * See enter_seccomp_mode() above.
				 */
				if (enable_seccomp)
					_exit(0);
#endif /* HAVE_LIBSECCOMP */
				/*
				 * NOTE: if you are tempted to add more code here, DON'T!
				 * Whatever you had in mind to do should be set up as an
				 * on_proc_exit or on_shmem_exit callback, instead. Otherwise
				 * it will fail to be called during other backend-shutdown
				 * scenarios.
				 */
				proc_exit(0);

			default:
				ereport(FATAL,
						(errcode(ERRCODE_PROTOCOL_VIOLATION),
						 errmsg("invalid frontend message type %d",
								firstchar)));
		}
	}							/* end of input-reading loop */
}


/*
 * Initialize dummy shmem.
 *
 * This code follows CreateSharedMemoryAndSemaphores() but manually sets up
 * the shmem header and skips few initialization steps that are not needed for
 * WAL redo.
 *
 * I've also tried removing most of initialization functions that request some
 * memory (like ApplyLauncherShmemInit and friends) but in reality it haven't had
 * any sizeable effect on RSS, so probably such clean up not worth the risk of having
 * half-initialized postgres.
 */
static void
CreateFakeSharedMemoryAndSemaphores(void)
{
	PGShmemHeader *shim = NULL;
	PGShmemHeader *hdr;
	Size		size;
	int			numSemas;
	char		cwd[MAXPGPATH];

	/* Compute number of semaphores we'll need */
	numSemas = ProcGlobalSemas();
	numSemas += SpinlockSemas();

	elog(DEBUG3,"reserving %d semaphores",numSemas);
	/*
	 * Size of the Postgres shared-memory block is estimated via
	 * moderately-accurate estimates for the big hogs, plus 100K for the
	 * stuff that's too small to bother with estimating.
	 *
	 * We take some care during this phase to ensure that the total size
	 * request doesn't overflow size_t.  If this gets through, we don't
	 * need to be so careful during the actual allocation phase.
	 */
	size = 150000;
	size = add_size(size, PGSemaphoreShmemSize(numSemas));
	size = add_size(size, SpinlockSemaSize());
	size = add_size(size, hash_estimate_size(SHMEM_INDEX_SIZE,
											 sizeof(ShmemIndexEnt)));
	size = add_size(size, dsm_estimate_size());
	size = add_size(size, BufferShmemSize());
	size = add_size(size, LockShmemSize());
	size = add_size(size, PredicateLockShmemSize());

	if (IsResQueueEnabled() && Gp_role == GP_ROLE_DISPATCH)
	{
		size = add_size(size, ResSchedulerShmemSize());
		size = add_size(size, ResPortalIncrementShmemSize());
	}
	else if (IsResGroupEnabled())
		size = add_size(size, ResGroupShmemSize());
	size = add_size(size, SharedSnapshotShmemSize());

	size = add_size(size, ProcGlobalShmemSize());
	size = add_size(size, XLOGShmemSize());
	size = add_size(size, DistributedLog_ShmemSize());
	size = add_size(size, CLOGShmemSize());
	size = add_size(size, CommitTsShmemSize());
	size = add_size(size, SUBTRANSShmemSize());
	size = add_size(size, TwoPhaseShmemSize());
	size = add_size(size, BackgroundWorkerShmemSize());
	size = add_size(size, MultiXactShmemSize());
	size = add_size(size, LWLockShmemSize());
	size = add_size(size, ProcArrayShmemSize());
	size = add_size(size, BackendStatusShmemSize());
	size = add_size(size, SInvalShmemSize());
	size = add_size(size, PMSignalShmemSize());
	size = add_size(size, ProcSignalShmemSize());
	size = add_size(size, CheckpointerShmemSize());
	size = add_size(size, AutoVacuumShmemSize());
	size = add_size(size, ReplicationSlotsShmemSize());
	size = add_size(size, ReplicationOriginShmemSize());
	size = add_size(size, WalSndShmemSize());
	size = add_size(size, WalRcvShmemSize());
	size = add_size(size, PgArchShmemSize());
	size = add_size(size, ApplyLauncherShmemSize());
	size = add_size(size, FTSReplicationStatusShmemSize());
	size = add_size(size, SnapMgrShmemSize());
	size = add_size(size, BTreeShmemSize());
	size = add_size(size, SyncScanShmemSize());
	/* size = add_size(size, AsyncShmemSize()); */
#ifdef EXEC_BACKEND
	size = add_size(size, ShmemBackendArraySize());
#endif

	size = add_size(size, tmShmemSize());
	size = add_size(size, CheckpointerShmemSize());
	size = add_size(size, CancelBackendMsgShmemSize());
	size = add_size(size, WorkFileShmemSize());
	size = add_size(size, ShareInputShmemSize());

#ifdef FAULT_INJECTOR
	size = add_size(size, FaultInjector_ShmemSize());
#endif

	/* This elog happens before we know the name of the log file we are supposed to use */
	elog(DEBUG1, "Size not including the buffer pool %lu",
		 (unsigned long) size);

	/* freeze the addin request size and include it */
	/*
	 * addin_request_allowed = false;
	 * size = add_size(size, total_addin_request);
	 */

	/* might as well round it off to a multiple of a typical page size */
	size = add_size(size, BLCKSZ - (size % BLCKSZ));

	/* Consider the size of the SessionState array */
	size = add_size(size, SessionState_ShmemSize());

	/* size of Instrumentation slots */
	size = add_size(size, InstrShmemSize());

	/* size of expand version */
	size = add_size(size, GpExpandVersionShmemSize());

	/* size of token and endpoint shared memory */
	size = add_size(size, EndpointShmemSize());

	elog(DEBUG3, "invoking IpcMemoryCreate(size=%zu)", size);

	/* Dummy implementation of PGSharedMemoryCreate() */
	{
		hdr = (void *) malloc(size + PG_CACHE_LINE_SIZE);
		if (!hdr)
			ereport(FATAL,
					(errcode(ERRCODE_OUT_OF_MEMORY),
							errmsg("[neon-wal-redo] can not allocate (pseudo-) shared memory")));

		hdr = (PGShmemHeader*)CACHELINEALIGN(hdr);
		hdr->creatorPID = getpid();
		hdr->magic = PGShmemMagic;
		hdr->dsm_control = 0;
		hdr->device = 42; /* not relevant for non-shared memory */
		hdr->inode = 43; /* not relevant for non-shared memory */
		hdr->totalsize = size;
		hdr->freeoffset = MAXALIGN(sizeof(PGShmemHeader));

		shim = hdr;
		UsedShmemSegAddr = hdr;
		UsedShmemSegID = (unsigned long) 42; /* not relevant for non-shared memory */
	}

	InitShmemAccess(hdr);

	/*
	 * Reserve semaphores uses dir name as a source of entropy. Set it to cwd(). Rest
	 * of the code does not need DataDir access so nullify DataDir after
	 * PGReserveSemaphores() to error out if something will try to access it.
	 */
	if (!getcwd(cwd, MAXPGPATH))
		ereport(FATAL,
				(errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("[neon-wal-redo] can not read current directory name")));
	DataDir = cwd;
	PGReserveSemaphores(numSemas);
	DataDir = NULL;

	/*
	 * If spinlocks are disabled, initialize emulation layer (which
	 * depends on semaphores, so the order is important here).
	 */
#ifndef HAVE_SPINLOCKS
	SpinlockSemaInit();
#endif

	/*
	 * Set up shared memory allocation mechanism
	 */
	if (!IsUnderPostmaster)
		InitShmemAllocation();

	/*
	 * Now initialize LWLocks, which do shared memory allocation and are
	 * needed for InitShmemIndex.
	 */
	CreateLWLocks();

	/*
	 * Set up shmem.c index hashtable
	 */
	InitShmemIndex();

	dsm_shmem_init();

	/*
	 * Set up xlog, clog, and buffers
	 */
	XLOGShmemInit();
	CLOGShmemInit();
	DistributedLog_ShmemInit();
	CommitTsShmemInit();
	SUBTRANSShmemInit();
	MultiXactShmemInit();
	tmShmemInit();
	InitBufferPool();

	/*
	 * Set up lock manager
	 */
	InitLocks();

	/*
	 * Set up predicate lock manager
	 */
	InitPredicateLocks();

	/*
	 * Set up resource manager
	 */
	ResManagerShmemInit();

	/*
	 * Set up process table
	 */
	if (!IsUnderPostmaster)
		InitProcGlobal();

	/* Initialize SessionState shared memory array */
	SessionState_ShmemInit();
	/* Initialize vmem protection */
	GPMemoryProtect_ShmemInit();

	CreateSharedProcArray();
	CreateSharedBackendStatus();

	/*
	 * Set up Shared snapshot slots
	 *
	 * TODO: only need to do this if we aren't the QD. for now we are just
	 *		 doing it all the time and wasting shemem on the QD.  This is
	 *		 because this happens at postmaster startup time when we don't
	 *		 know who we are.
	 */
	CreateSharedSnapshotArray();
	TwoPhaseShmemInit();
	BackgroundWorkerShmemInit();

	/*
	 * Set up shared-inval messaging
	 */
	CreateSharedInvalidationState();

	/*
	 * Set up interprocess signaling mechanisms
	 */
	PMSignalShmemInit();
	ProcSignalShmemInit();
	CheckpointerShmemInit();
	AutoVacuumShmemInit();
	ReplicationSlotsShmemInit();
	ReplicationOriginShmemInit();
	WalSndShmemInit();
	WalRcvShmemInit();
	PgArchShmemInit();
	ApplyLauncherShmemInit();
	FTSReplicationStatusShmemInit();

#ifdef FAULT_INJECTOR
	FaultInjector_ShmemInit();
#endif

	/*
	 * Set up other modules that need some shared memory space
	 */
	SnapMgrInit();
	BTreeShmemInit();
	SyncScanShmemInit();
	/* Skip due to the 'pg_notify' directory check */
	/* AsyncShmemInit(); */
	BackendCancelShmemInit();
	WorkFileShmemInit();
	ShareInputShmemInit();

	/*
	 * Set up Instrumentation free list
	 */
	if (!IsUnderPostmaster)
		InstrShmemInit();

	GpExpandVersionShmemInit();

#ifdef EXEC_BACKEND

	/*
	 * Alloc the win32 shared backend array
	 */
	if (!IsUnderPostmaster)
		ShmemBackendArrayAllocation();
#endif

	if (gp_enable_resqueue_priority)
		BackoffStateInit();

	/* Initialize dynamic shared memory facilities. */
	if (!IsUnderPostmaster)
		dsm_postmaster_startup(shim);

	/* Initialize shared memory for parallel retrieve cursor */
	if (!IsUnderPostmaster)
		EndpointShmemInit();

	/*
	 * Now give loadable modules a chance to set up their shmem allocations
	 */
	if (shmem_startup_hook)
		shmem_startup_hook();
}

#define LocalBufHdrGetBlock(bufHdr) \
                        LocalBufferBlockPointers[-((bufHdr)->buf_id + 2)]

/*
 * This is the version of ReadBuffer_common for local buffer only.
 * Remove all statistics info tracking.
 */
static Buffer
NeonReadBuffer_common(SMgrRelation smgr, char relpersistence, ForkNumber forkNum,
                      BlockNumber blockNum, ReadBufferMode mode,
                      BufferAccessStrategy strategy, bool *hit)
{
    BufferDesc *bufHdr;
    Block		bufBlock;
    bool		found;
    bool		isExtend;

    *hit = false;

    Assert(smgr != NULL);

    /* Make sure we will have room to remember the buffer pin */
    ResourceOwnerEnlargeBuffers(CurrentResourceOwner);

    isExtend = (blockNum == P_NEW);

    /* Substitute proper block number if caller asked for P_NEW */
    if (isExtend)
    {
        blockNum = smgrnblocks(smgr, forkNum);
        /* Fail if relation is already at maximum possible length */
        if (blockNum == P_NEW)
            ereport(ERROR,
                    (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                            errmsg("cannot extend relation %s beyond %u blocks",
                                   relpath(smgr->smgr_rnode, forkNum),
                                   P_NEW)));
    }

    {
	/*
	 * Wal Redo process always use local buffers.
	 */
        bufHdr = LocalBufferAlloc(smgr, forkNum, blockNum, &found, wal_redo_buffer);
    }


    /* At this point we do NOT hold any locks. */

    /* if it was already in the buffer pool, we're done */
    if (found)
    {
        if (!isExtend)
        {
            /* Just need to update stats before we exit */
            *hit = true;

            return BufferDescriptorGetBuffer(bufHdr);
        }

        /*
         * We get here only in the corner case where we are trying to extend
         * the relation but we found a pre-existing buffer marked BM_VALID.
         * This can happen because mdread doesn't complain about reads beyond
         * EOF (when zero_damaged_pages is ON) and so a previous attempt to
         * read a block beyond EOF could have left a "valid" zero-filled
         * buffer.  Unfortunately, we have also seen this case occurring
         * because of buggy Linux kernels that sometimes return an
         * lseek(SEEK_END) result that doesn't account for a recent write. In
         * that situation, the pre-existing buffer would contain valid data
         * that we don't want to overwrite.  Since the legitimate case should
         * always have left a zero-filled buffer, complain if not PageIsNew.
         */
        bufBlock = LocalBufHdrGetBlock(bufHdr);
        if (!PageIsNew((Page) bufBlock))
            ereport(ERROR,
                    (errmsg("unexpected data beyond EOF in block %u of relation %s",
                            blockNum, relpath(smgr->smgr_rnode, forkNum)),
                                    errhint("This has been seen to occur with buggy kernels; consider updating your system.")));

        /*
         * We *must* do smgrextend before succeeding, else the page will not
         * be reserved by the kernel, and the next P_NEW call will decide to
         * return the same page.  Clear the BM_VALID bit, do the StartBufferIO
         * call that BufferAlloc didn't, and proceed.
         */
        {
            /* Only need to adjust flags */
            uint32		buf_state = pg_atomic_read_u32(&bufHdr->state);

            Assert(buf_state & BM_VALID);
            buf_state &= ~BM_VALID;
            pg_atomic_unlocked_write_u32(&bufHdr->state, buf_state);
        }
    }

    /*
     * if we have gotten to this point, we have allocated a buffer for the
     * page but its contents are not yet valid.  IO_IN_PROGRESS is set for it,
     * if it's a shared buffer.
     *
     * Note: if smgrextend fails, we will end up with a buffer that is
     * allocated but not marked BM_VALID.  P_NEW will still select the same
     * block number (because the relation didn't get any longer on disk) and
     * so future attempts to extend the relation will find the same buffer (if
     * it's not been recycled) but come right back here to try smgrextend
     * again.
     */
    Assert(!(pg_atomic_read_u32(&bufHdr->state) & BM_VALID));	/* spinlock not needed */

    bufBlock = LocalBufHdrGetBlock(bufHdr);

    if (isExtend)
    {
        /* new buffers are zero-filled */
        MemSet((char *) bufBlock, 0, BLCKSZ);
        /* don't set checksum for all-zero page */
        smgrextend(smgr, forkNum, blockNum, (char *) bufBlock, false);

        /*
         * NB: we're *not* doing a ScheduleBufferTagForWriteback here;
         * although we're essentially performing a write. At least on linux
         * doing so defeats the 'delayed allocation' mechanism, leading to
         * increased file fragmentation.
         */
    }
    else
    {
        /*
         * Read in the page, unless the caller intends to overwrite it and
         * just wants us to allocate a buffer.
         */
        if (mode == RBM_ZERO_AND_LOCK || mode == RBM_ZERO_AND_CLEANUP_LOCK)
            MemSet((char *) bufBlock, 0, BLCKSZ);
        else
        {
            smgrread(smgr, forkNum, blockNum, (char *) bufBlock);

            /* check for garbage data */
            if (!PageIsVerifiedExtended((Page) bufBlock, forkNum, blockNum,
                                        smgr->smgr_rnode.node.spcNode,
                                        PIV_LOG_WARNING | PIV_REPORT_STAT))
            {
                if (mode == RBM_ZERO_ON_ERROR || zero_damaged_pages)
                {
                    ereport(WARNING,
                            (errcode(ERRCODE_DATA_CORRUPTED),
                                    errmsg("invalid page in block %u of relation %s; zeroing out page",
                                           blockNum,
                                           relpath(smgr->smgr_rnode, forkNum))));
                    MemSet((char *) bufBlock, 0, BLCKSZ);
                }
                else
                    ereport(ERROR,
                            (errcode(ERRCODE_DATA_CORRUPTED),
                                    errmsg("invalid page in block %u of relation %s",
                                           blockNum,
                                           relpath(smgr->smgr_rnode, forkNum))));
            }
        }
    }

    {
        /* Only need to adjust flags */
        uint32		buf_state = pg_atomic_read_u32(&bufHdr->state);

        buf_state |= BM_VALID;
        pg_atomic_unlocked_write_u32(&bufHdr->state, buf_state);
    }

    return BufferDescriptorGetBuffer(bufHdr);
}

/* Version compatility wrapper for ReadBufferWithoutRelcache */
static inline Buffer
NeonRedoReadBuffer(RelFileNode rnode,
                    ForkNumber forkNum, BlockNumber blockNum,
                    ReadBufferMode mode)
{
#if PG_VERSION_NUM >= 150000
	return ReadBufferWithoutRelcache(rnode, forkNum, blockNum, mode,
									 NULL, /* no strategy */
									 true); /* WAL redo is only performed on permanent rels */
#else
    return ReadBufferWithoutRelcache(rnode, forkNum, blockNum, mode,
                                         NULL); /* no strategy */
#endif
}


/*
 * Some debug function that may be handy for now.
 */
pg_attribute_unused()
static char *
pprint_buffer(char *data, int len)
{
	StringInfoData s;

	initStringInfo(&s);
	appendStringInfo(&s, "\n");
	for (int i = 0; i < len; i++) {

		appendStringInfo(&s, "%02x ", (*(((char *) data) + i) & 0xff) );
		if (i % 32 == 31) {
			appendStringInfo(&s, "\n");
		}
	}
	appendStringInfo(&s, "\n");

	return s.data;
}

/* ----------------------------------------------------------------
 *		routines to obtain user input
 * ----------------------------------------------------------------
 */

/*
 * Read next command from the client.
 *
 *	the string entered by the user is placed in its parameter inBuf,
 *	and we act like a Q message was received.
 *
 *	EOF is returned if end-of-file input is seen; time to shut down.
 * ----------------
 */
static int
ReadRedoCommand(StringInfo inBuf)
{
	ssize_t		ret;
	char		hdr[1 + sizeof(int32)];
	int			qtype;
	int32		len;

	/* Read message type and message length */
	ret = buffered_read(hdr, sizeof(hdr));
	if (ret != sizeof(hdr))
	{
		if (ret == 0)
			return EOF;
		else if (ret < 0)
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("could not read message header: %m")));
		else
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("unexpected EOF")));
	}

	qtype = hdr[0];
	memcpy(&len, &hdr[1], sizeof(int32));
	len = pg_ntoh32(len);

	if (len < 4)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("invalid message length")));

	len -= 4;					/* discount length itself */

	/* Read the message payload */
	enlargeStringInfo(inBuf, len);
	ret = buffered_read(inBuf->data, len);
	if (ret != len)
	{
		if (ret < 0)
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("could not read message: %m")));
		else
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("unexpected EOF")));
	}
	inBuf->len = len;
	inBuf->data[len] = '\0';

	return qtype;
}

/*
 * Prepare for WAL replay on given block
 */
static void
BeginRedoForBlock(StringInfo input_message)
{
	RelFileNode rnode;
	ForkNumber forknum;
	BlockNumber blknum;
	SMgrRelation reln;

	/*
	 * message format:
	 *
	 * spcNode
	 * dbNode
	 * relNode
	 * ForkNumber
	 * BlockNumber
	 */
	forknum = pq_getmsgbyte(input_message);
	rnode.spcNode = pq_getmsgint(input_message, 4);
	rnode.dbNode = pq_getmsgint(input_message, 4);
	rnode.relNode = pq_getmsgint(input_message, 4);
	blknum = pq_getmsgint(input_message, 4);
	wal_redo_buffer = InvalidBuffer;

	INIT_BUFFERTAG(target_redo_tag, rnode, forknum, blknum);

	elog(TRACE, "BeginRedoForBlock %u/%u/%u.%d blk %u",
		 target_redo_tag.rnode.spcNode,
		 target_redo_tag.rnode.dbNode,
		 target_redo_tag.rnode.relNode,
		 target_redo_tag.forkNum,
		 target_redo_tag.blockNum);

	reln = smgropen(rnode, InvalidBackendId, 0, NULL);
	if (reln->smgr_cached_nblocks[forknum] == InvalidBlockNumber ||
		reln->smgr_cached_nblocks[forknum] < blknum + 1)
	{
		reln->smgr_cached_nblocks[forknum] = blknum + 1;
	}
}

/*
 * Receive a page given by the client, and put it into buffer cache.
 */
static void
PushPage(StringInfo input_message)
{
	RelFileNode rnode;
	ForkNumber forknum;
	BlockNumber blknum;
	const char *content;
	Buffer		buf;
	Page		page;

	/*
	 * message format:
	 *
	 * spcNode
	 * dbNode
	 * relNode
	 * ForkNumber
	 * BlockNumber
	 * 8k page content
	 */
	forknum = pq_getmsgbyte(input_message);
	rnode.spcNode = pq_getmsgint(input_message, 4);
	rnode.dbNode = pq_getmsgint(input_message, 4);
	rnode.relNode = pq_getmsgint(input_message, 4);
	blknum = pq_getmsgint(input_message, 4);
	content = pq_getmsgbytes(input_message, BLCKSZ);

	buf = NeonRedoReadBuffer(rnode, forknum, blknum, RBM_ZERO_AND_LOCK);
	wal_redo_buffer = buf;
	page = BufferGetPage(buf);
	memcpy(page, content, BLCKSZ);
	MarkBufferDirty(buf); /* pro forma */
	UnlockReleaseBuffer(buf);
}

/*
 * Receive a WAL record, and apply it.
 *
 * All the pages should be loaded into the buffer cache by PushPage calls already.
 */
static void
ApplyRecord(StringInfo input_message)
{
	char	   *errormsg;
	XLogRecPtr	lsn;
	XLogRecord *record;
	int			nleft;
	ErrorContextCallback errcallback;
#if PG_VERSION_NUM >= 150000
	DecodedXLogRecord *decoded;
#endif

	/*
	 * message format:
	 *
	 * LSN (the *end* of the record)
	 * record
	 */
	lsn = pq_getmsgint64(input_message);

	smgr_init_inmem();			/* reset inmem smgr state */

	/* note: the input must be aligned here */
	record = (XLogRecord *) pq_getmsgbytes(input_message, sizeof(XLogRecord));

	nleft = input_message->len - input_message->cursor;
	if (record->xl_tot_len != sizeof(XLogRecord) + nleft)
		elog(ERROR, "mismatch between record (%d) and message size (%d)",
			 record->xl_tot_len, (int) sizeof(XLogRecord) + nleft);

	/* Setup error traceback support for ereport() */
	errcallback.callback = apply_error_callback;
	errcallback.arg = (void *) reader_state;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;

	XLogBeginRead(reader_state, lsn);

#if PG_VERSION_NUM >= 150000
	decoded = (DecodedXLogRecord *) XLogReadRecordAlloc(reader_state, record->xl_tot_len, true);

	if (!DecodeXLogRecord(reader_state, decoded, record, lsn, &errormsg))
		elog(ERROR, "failed to decode WAL record: %s", errormsg);
	else
	{
		/* Record the location of the next record. */
		decoded->next_lsn = reader_state->NextRecPtr;

		/*
		 * If it's in the decode buffer, mark the decode buffer space as
		 * occupied.
		 */
		if (!decoded->oversized)
		{
			/* The new decode buffer head must be MAXALIGNed. */
			Assert(decoded->size == MAXALIGN(decoded->size));
			if ((char *) decoded == reader_state->decode_buffer)
				reader_state->decode_buffer_tail = reader_state->decode_buffer + decoded->size;
			else
				reader_state->decode_buffer_tail += decoded->size;
		}

		/* Insert it into the queue of decoded records. */
		Assert(reader_state->decode_queue_tail != decoded);
		if (reader_state->decode_queue_tail)
			reader_state->decode_queue_tail->next = decoded;
		reader_state->decode_queue_tail = decoded;
		if (!reader_state->decode_queue_head)
			reader_state->decode_queue_head = decoded;

		/*
		 * Update the pointers to the beginning and one-past-the-end of this
		 * record, again for the benefit of historical code that expected the
		 * decoder to track this rather than accessing these fields of the record
		 * itself.
		 */
		reader_state->record = reader_state->decode_queue_head;
		reader_state->ReadRecPtr = reader_state->record->lsn;
		reader_state->EndRecPtr = reader_state->record->next_lsn;
	}
#else
	/*
	 * In lieu of calling XLogReadRecord, store the record 'decoded_record'
	 * buffer directly.
	 */
	reader_state->ReadRecPtr = lsn;
	reader_state->decoded_record = record;
	if (!DecodeXLogRecord(reader_state, record, &errormsg))
		elog(ERROR, "failed to decode WAL record: %s", errormsg);
#endif

	/* Ignore any other blocks than the ones the caller is interested in */
	redo_read_buffer_filter = redo_block_filter;

	RmgrTable[record->xl_rmid].rm_redo(reader_state);

	/*
	 * If no base image of the page was provided by PushPage, initialize
	 * wal_redo_buffer here. The first WAL record must initialize the page
	 * in that case.
	 */
	if (BufferIsInvalid(wal_redo_buffer))
	{
		wal_redo_buffer = NeonRedoReadBuffer(target_redo_tag.rnode,
											 target_redo_tag.forkNum,
											 target_redo_tag.blockNum,
											 RBM_NORMAL);
		Assert(!BufferIsInvalid(wal_redo_buffer));
		ReleaseBuffer(wal_redo_buffer);
	}

	redo_read_buffer_filter = NULL;

	/* Pop the error context stack */
	error_context_stack = errcallback.previous;

	elog(TRACE, "applied WAL record with LSN %X/%X",
		 (uint32) (lsn >> 32), (uint32) lsn);
#if PG_VERSION_NUM >= 150000
	if (decoded && decoded->oversized)
		pfree(decoded);
#endif
}

static void
wal_outdesc(StringInfo buf, XLogReaderState *record)
{
    RmgrId		rmid = XLogRecGetRmid(record);
    uint8		info = XLogRecGetInfo(record);
    const char *id;

    appendStringInfoString(buf, RmgrTable[rmid].rm_name);
    appendStringInfoChar(buf, '/');

    id = RmgrTable[rmid].rm_identify(info);
    if (id == NULL)
        appendStringInfo(buf, "UNKNOWN (%X): ", info & ~XLR_INFO_MASK);
    else
        appendStringInfo(buf, "%s: ", id);

    RmgrTable[rmid].rm_desc(buf, record);
}

/*
 * Error context callback for errors occurring during ApplyRecord
 */
static void
apply_error_callback(void *arg)
{
	XLogReaderState *record = (XLogReaderState *) arg;
	StringInfoData buf;

	initStringInfo(&buf);
	wal_outdesc(&buf, record);

	/* translator: %s is a WAL record description */
	errcontext("WAL redo at %X/%X for %s",
			   LSN_FORMAT_ARGS(record->ReadRecPtr),
			   buf.data);

	pfree(buf.data);
}



static bool
redo_block_filter(XLogReaderState *record, uint8 block_id)
{
	BufferTag	target_tag;

#if PG_VERSION_NUM >= 150000
	XLogRecGetBlockTag(record, block_id,
					   &target_tag.rnode, &target_tag.forkNum, &target_tag.blockNum);
#else
	if (!XLogRecGetBlockTag(record, block_id,
							&target_tag.rnode, &target_tag.forkNum, &target_tag.blockNum))
	{
		/* Caller specified a bogus block_id */
		elog(PANIC, "failed to locate backup block with ID %d", block_id);
	}
#endif

	/*
	 * Can a WAL redo function ever access a relation other than the one that
	 * it modifies? I don't see why it would.
	 */
	if (!RelFileNodeEquals(target_tag.rnode, target_redo_tag.rnode))
		elog(WARNING, "REDO accessing unexpected page: %u/%u/%u.%u blk %u",
			 target_tag.rnode.spcNode, target_tag.rnode.dbNode, target_tag.rnode.relNode, target_tag.forkNum, target_tag.blockNum);

	/*
	 * If this block isn't one we are currently restoring, then return 'true'
	 * so that this gets ignored
	 */
	return !BUFFERTAGS_EQUAL(target_tag, target_redo_tag);
}

/*
 * Get a page image back from buffer cache.
 *
 * After applying some records.
 */
static void
GetPage(StringInfo input_message)
{
	RelFileNode rnode;
	ForkNumber forknum;
	BlockNumber blknum;
	Buffer		buf;
	Page		page;
	int			tot_written;

	/*
	 * message format:
	 *
	 * spcNode
	 * dbNode
	 * relNode
	 * ForkNumber
	 * BlockNumber
	 */
	forknum = pq_getmsgbyte(input_message);
	rnode.spcNode = pq_getmsgint(input_message, 4);
	rnode.dbNode = pq_getmsgint(input_message, 4);
	rnode.relNode = pq_getmsgint(input_message, 4);
	blknum = pq_getmsgint(input_message, 4);

	/* FIXME: check that we got a BeginRedoForBlock message or this earlier */

	buf = NeonRedoReadBuffer(rnode, forknum, blknum, RBM_NORMAL);
	Assert(buf == wal_redo_buffer);
	page = BufferGetPage(buf);
	/* single thread, so don't bother locking the page */

	/*
	 * Before send back the page, we should calculate the checksum.
	 * 	if (!PageIsNew(page))
	 *      ((PageHeader) page)->pd_checksum = pg_checksum_page((char *) page, blknum);
	 */

	/* Response: Page content */
	tot_written = 0;
	do {
		ssize_t		rc;

		rc = write(STDOUT_FILENO, &page[tot_written], BLCKSZ - tot_written);
		if (rc < 0) {
			/* If interrupted by signal, just retry */
			if (errno == EINTR)
				continue;
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write to stdout: %m")));
		}
		tot_written += rc;
	} while (tot_written < BLCKSZ);

	ReleaseBuffer(buf);
	DropRelFileNodeAllLocalBuffers(rnode);
	wal_redo_buffer = InvalidBuffer;

	elog(TRACE, "Page sent back for block %u", blknum);
}


/* Buffer used by buffered_read() */
static char stdin_buf[16 * 1024];
static size_t stdin_len = 0;	/* # of bytes in buffer */
static size_t stdin_ptr = 0;	/* # of bytes already consumed */

/*
 * Like read() on stdin, but buffered.
 *
 * We cannot use libc's buffered fread(), because it uses syscalls that we
 * have disabled with seccomp(). Depending on the platform, it can call
 * 'fstat' or 'newfstatat'. 'fstat' is probably harmless, but 'newfstatat'
 * seems problematic because it allows interrogating files by path name.
 *
 * The return value is the number of bytes read. On error, -1 is returned, and
 * errno is set appropriately. Unlike read(), this fills the buffer completely
 * unless an error happens or EOF is reached.
 */
static ssize_t
buffered_read(void *buf, size_t count)
{
	char	   *dst = buf;

	while (count > 0)
	{
		size_t		nthis;

		if (stdin_ptr == stdin_len)
		{
			ssize_t		ret;

			ret = read(STDIN_FILENO, stdin_buf, sizeof(stdin_buf));
			if (ret < 0)
			{
				/* don't do anything here that could set 'errno' */
				return ret;
			}
			if (ret == 0)
			{
				/* EOF */
				break;
			}
			stdin_len = (size_t) ret;
			stdin_ptr = 0;
		}
		nthis = Min(stdin_len - stdin_ptr, count);

		memcpy(dst, &stdin_buf[stdin_ptr], nthis);

		stdin_ptr += nthis;
		count -= nthis;
		dst += nthis;
	}

	return (dst - (char *) buf);
}
