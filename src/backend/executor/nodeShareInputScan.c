/*-------------------------------------------------------------------------
 *
 * nodeShareInputScan.c
 *
 * A Share Input Scan node is used to share the result of an operation in
 * two different branches in the plan tree.
 *
 * These come in two variants: local, and cross-slice.
 *
 * Local shares
 * ------------
 *
 * In local mode, all the consumers are in the same slice as the producer.
 * In that case, there's no need to communicate across processes, so we
 * rely entirely on data structures in backend-private memory to track the
 * state.
 *
 * In local mode, there is no difference between producer and consumer
 * nodes. In ExecInitShareInputScan(), the producer node stores the
 * PlanState of the shared child node where all the nodes can find it.
 * The first ExecShareInputScan() call initializes the store.
 *
 * A local-mode ShareInputScan is quite similar to PostgreSQL's CteScan,
 * but there are some implementation differences. CteScan uses a special
 * PARAM_EXEC entry to hold the shared state, while ShareInputScan uses
 * an entry in es_sharenode instead.
 *
 * Cross-slice shares
 * ------------------
 *
 * A cross-slice share works basically the same as a local one, except
 * that the producing slice makes the underlying tuplestore available to
 * other processes, by forcing it to be written to a file on disk. The
 * first ExecShareInputScan() call in the producing slice materializes
 * the whole tuplestore, and advertises that it's ready in shared memory.
 * Consumer slices wait for that before trying to read the store.
 *
 * The producer and the consumers communicate the status of the scan using
 * shared memory. There's a hash table in shared memory, containing a
 * 'shareinput_Xslice_state' struct for each shared scan. The producer uses
 * a condition variable to wake up consumers, when the tuplestore is fully
 * materialized, and the consumers use the same condition variable to inform
 * the producer when they're done reading it. The producer slice keeps the
 * underlying tuplestore open, until all the consumers have finished.
 *
 *
 * Portions Copyright (c) 2007-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	    src/backend/executor/nodeShareInputScan.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/xact.h"
#include "cdb/cdbvars.h"
#include "executor/executor.h"
#include "executor/nodeShareInputScan.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/condition_variable.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/faultinjector.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/tuplestore.h"
#include "port/atomics.h"
#include "access/parallel.h"
#include "storage/dsm.h"
#include "utils/sharedtuplestore.h"

/*
 * In a cross-slice ShareinputScan, the producer and consumer processes
 * communicate using shared memory. There's a hash table containing one
 * 'shareinput_share_state' for each in-progress Share Input Scan.
 *
 * The hash table itself,, and the fields within every entry, are protected
 * by ShareInputScanLock. (Although some operations get away without the
 * lock, when the field is atomic and/or there's only one possible writer.)
 *
 * All producers and consumers that participate in a shared scan hold
 * a reference to the 'shareinput_Xslice_state' entry of the scan, for
 * the whole lifecycle of the node from ExecInitShareInputScan() to
 * ExecEndShareInputScan() (although it can be released early by
 * ExecSquelchShareInputScan(). The entry in the hash table is created by
 * the first participant that initializes, which is not necessarily the
 * producer! When the last participant releases the entry, it is removed
 * from the hash table.
 */
typedef struct shareinput_tag
{
	int32		session_id;
	int32		command_count;
	int32		share_id;
} shareinput_tag;

typedef struct shareinput_Xslice_state
{
	shareinput_tag tag;			/* hash key */

	int			refcount;		/* reference count of this entry */
	pg_atomic_uint32	ready;	/* is the input fully materialized and ready to be read? */
	pg_atomic_uint32	ndone;	/* # of consumers that have finished the scan */
	pg_atomic_uint32	producer_failed; /* producer slice hit ERROR before ready */

	/*
	 * ready_done_cv is used for signaling when the scan becomes "ready", and
	 * when it becomes "done". The producer wakes up everyone waiting on this
	 * condition variable when it sets ready = true. Also, when the last
	 * consumer finishes the scan (ndone reaches nconsumers), it wakes up the
	 * producer using this same condition variable.
	 */
	ConditionVariable ready_done_cv;

	/*
	 * For cross-slice parallel mode: the producer stores the DSM handle of
	 * the SharedTuplestore here so consumer slices can attach.
	 */
	dsm_handle	sts_dsm_handle;		/* DSM handle for SharedTuplestore, or DSM_HANDLE_INVALID */
	int			nparticipants;		/* number of STS participants (P == C) */
} shareinput_Xslice_state;

/* shared memory hash table holding 'shareinput_Xslice_state' entries */
HTAB *shareinput_Xslice_hash = NULL;

/*
 * The tuplestore files for all share input scans are held in one SharedFileSet.
 * The SharedFileSet is attached to a single DSM segment that persists until
 * postmaster shutdown. When the reference count of the SharedFileSet reaches
 * zero, the SharedFileSet is automatically destroyed, but it is re-initialized
 * the next time it's needed.
 *
 * The SharedFileSet deletes any remaining files when the reference count
 * reaches zero, but we don't rely on that mechanism. All the files are
 * held in the same SharedFileSet, so it cannot be recycled until all
 * ShareInputScans in the system have finished, which might never happen if
 * new queries are started continuously. The shareinput_Xslice_state entries
 * are reference counted separately, and we clean up the files backing each
 * individual ShareInputScan whenever its reference count reaches zero.
 */
dsm_handle *shareinput_Xslice_dsm_handle_ptr;
static SharedFileSet *shareinput_Xslice_fileset;

/*
 * 'shareinput_reference' represents a reference or "lease" to an entry
 * in the shared memory hash table. It is used for garbage collection of
 * the entries, on transaction abort.
 *
 * These are allocated in TopMemoryContext, and held in the
 * 'shareinput_Xslice_refs' list.
 */
typedef struct shareinput_Xslice_reference
{
	int			share_id;
	shareinput_Xslice_state *xslice_state;

	ResourceOwner owner;

	dlist_node	node;
} shareinput_Xslice_reference;

static dlist_head shareinput_Xslice_refs = DLIST_STATIC_INIT(shareinput_Xslice_refs);
static bool shareinput_resowner_callback_registered = false;

/*
 * For local (i.e. intra-slice) variants, we use a 'shareinput_local_state'
 * to track the status. It is analogous to 'shareinput_share_state' used for
 * cross-slice scans, but we don't need to keep it in shared memory. These
 * are held in estate->es_sharenode, indexed by share_id.
 */
typedef struct shareinput_local_state
{
	bool		ready;
	bool		closed;
	int			ndone;
	int			nsharers;

	/*
	 * This points to the child node that's being shared. Set by
	 * ExecInitShareInputScan() of the instance that has the child.
	 */
	PlanState  *childState;

	/* Tuplestore that holds the result */
	Tuplestorestate *ts_state;

	/* Parallel state set by producer, used by consumers via share_id */
	ParallelShareInputState *parallel_state;

	/* DSM segment created by producer, reused by consumers in same process */
	dsm_segment *parallel_seg;
} shareinput_local_state;

static shareinput_Xslice_reference *get_shareinput_reference(int share_id);
static void release_shareinput_reference(shareinput_Xslice_reference *ref, bool reader_squelching);
static void shareinput_release_callback(ResourceReleasePhase phase,
										bool isCommit,
										bool isTopLevel,
										void *arg);

static void shareinput_writer_notifyready(shareinput_Xslice_reference *ref);
static void shareinput_reader_waitready(shareinput_Xslice_reference *ref);
static void shareinput_reader_notifydone(shareinput_Xslice_reference *ref, int nconsumers);
static void shareinput_writer_waitdone(shareinput_Xslice_reference *ref, int nconsumers);

static void ExecShareInputScanExplainEnd(PlanState *planstate, struct StringInfoData *buf);

/*
 * Raise ERROR if the in-slice producer/leader of a parallel ShareInputScan has
 * flagged failure.  Called from wait loops so waiters don't hang forever when
 * their broadcaster died before signaling ready.
 */
static void
check_parallel_producer_failed(ParallelShareInputState *pstate)
{
	if (pg_atomic_read_u32(&pstate->producer_failed))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("parallel ShareInputScan producer failed before signaling ready")));
}

/*
 * Cross-slice variant: raise ERROR if the producer slice flagged failure.
 */
static void
check_xslice_producer_failed(shareinput_Xslice_state *state)
{
	if (pg_atomic_read_u32(&state->producer_failed))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("cross-slice ShareInputScan producer slice failed before signaling ready")));
}


/*
 * init_tuplestore_state
 *    Initialize the tuplestore state for the Shared node if the state
 *    is not initialized.
 */
static void
init_tuplestore_state(ShareInputScanState *node)
{
	EState	   *estate = node->ss.ps.state;
	ShareInputScan *sisc = (ShareInputScan *) node->ss.ps.plan;
	shareinput_local_state *local_state = node->local_state;
	Tuplestorestate *ts = NULL;
	int			tsptrno;
	TupleTableSlot *outerslot;

	Assert(!node->isready);
	Assert(node->ts_state == NULL);
	Assert(node->ts_pos == -1);

	if (sisc->cross_slice)
	{
		if (!node->ref)
			elog(ERROR, "cannot execute ShareInputScan that was not initialized");
	}

	if (!local_state->ready)
	{
		/* parallel */
		if (node->parallel_state != NULL && sisc->scan.plan.parallel_aware)
		{
			/* cross slice */
			if (sisc->cross_slice)
			{
				/* producer */
				if (currentSliceId == sisc->producer_slice_id ||
					estate->es_plannedstmt->numSlices == 1)
				{
					ParallelShareInputState *pstate = node->parallel_state;
					shareinput_Xslice_state *xslice_state = node->ref->xslice_state;
					char		sts_name[64];
					dsm_segment *seg;
					SharedTuplestore *shared_ts;

					PG_TRY();
					{
						shareinput_create_bufname_prefix(sts_name, sizeof(sts_name),
														 sisc->share_id);

						if (ParallelWorkerNumberOfSlice == 0)
						{
							/* Leader: create pinned DSM + SharedTuplestore */
							Size	sz = sts_estimate(TotalParallelWorkerNumberOfSlice);

							seg = dsm_create(sz, 0);
							dsm_pin_segment(seg);
							dsm_pin_mapping(seg);
							pstate->sts_handle = dsm_segment_handle(seg);
							shared_ts = dsm_segment_address(seg);

							node->sts_seg = seg;
							node->sts_accessor = sts_initialize(shared_ts,
																TotalParallelWorkerNumberOfSlice,
																ParallelWorkerNumberOfSlice, 0, 0,
																get_shareinput_fileset(),
																sts_name);

							/* Publish to cross-slice shared state */
							xslice_state->sts_dsm_handle = dsm_segment_handle(seg);
							xslice_state->nparticipants = TotalParallelWorkerNumberOfSlice;

							/* Signal intra-slice workers */
							pg_atomic_write_u32(&pstate->ready, 1);
							ConditionVariableBroadcast(&pstate->ready_cv);
						}
						else
						{
							/* Worker: wait for leader to create STS, then attach */
							while (pg_atomic_read_u32(&pstate->ready) == 0)
							{
								check_parallel_producer_failed(pstate);
								ConditionVariableSleep(&pstate->ready_cv,
													   WAIT_EVENT_SHAREINPUT_SCAN);
							}
							ConditionVariableCancelSleep();
							check_parallel_producer_failed(pstate);

							seg = dsm_attach(pstate->sts_handle);
							if (seg == NULL)
								ereport(ERROR,
										(errmsg("could not attach to cross-slice SharedTuplestore DSM for share %d",
												sisc->share_id)));
							dsm_pin_mapping(seg);

							shared_ts = dsm_segment_address(seg);
							node->sts_seg = seg;
							node->sts_accessor = sts_attach(shared_ts,
															ParallelWorkerNumberOfSlice,
															get_shareinput_fileset());
						}

						/* All workers materialize in parallel */
						for (;;)
						{
							bool		shouldFree;
							MinimalTuple tuple;

							outerslot = ExecProcNode(local_state->childState);
							if (TupIsNull(outerslot))
								break;
							tuple = ExecFetchSlotMinimalTuple(outerslot, &shouldFree);
							sts_puttuple(node->sts_accessor, NULL, tuple);
							if (shouldFree)
								heap_free_minimal_tuple(tuple);
						}

						sts_end_write(node->sts_accessor);

						/* Wait for all writers to finish */
						BarrierArriveAndWait(&pstate->write_barrier,
											 WAIT_EVENT_SHAREINPUT_SCAN);

						/*
						 * All workers signal consumers idempotently.  Doing this on
						 * every worker (rather than only the leader) ensures the
						 * 'ready' flag is set even if the leader is interrupted
						 * after the barrier but before reaching the signal -- e.g.,
						 * by a cancel raised inside BarrierArriveAndWait's CFI on
						 * the consumer side.  Atomic writes and broadcasts are
						 * idempotent so multiple writers cause no harm.
						 */
						pg_atomic_write_u32(&xslice_state->ready, 1);
						ConditionVariableBroadcast(&xslice_state->ready_done_cv);
						pg_atomic_write_u32(&pstate->ready, TotalParallelWorkerNumberOfSlice);
						ConditionVariableBroadcast(&pstate->ready_cv);
					}
					PG_CATCH();
					{
						/*
						 * Unblock any in-slice workers waiting on pstate->ready
						 * and any cross-slice consumers waiting on
						 * xslice_state->ready, so they ERROR out instead of
						 * hanging on a producer that died before signaling.
						 */
						pg_atomic_write_u32(&pstate->producer_failed, 1);
						ConditionVariableBroadcast(&pstate->ready_cv);
						pg_atomic_write_u32(&xslice_state->producer_failed, 1);
						ConditionVariableBroadcast(&xslice_state->ready_done_cv);
						PG_RE_THROW();
					}
					PG_END_TRY();

					/* Store DSM segment for within-slice consumers in same process */
					local_state->parallel_seg = node->sts_seg;
					local_state->ready = true;
					node->ts_state = NULL;
					node->ts_pos = -1;
					node->isready = true;
					return;
				}
				/* consumer */
				else
				{
					/*
					 * Cross-slice parallel consumer: each consumer slice creates its
					 * own private copy of the SharedTuplestore metadata so that the
					 * slice has independent read_page tracking.  Workers within the
					 * consumer slice share the copy and do parallel scan.  Different
					 * consumer slices each read the full dataset independently.
					 *
					 * The slice leader creates the copy and publishes the DSM handle
					 * via ParallelShareInputState; other workers wait and attach.
					 */
					ParallelShareInputState *pstate = node->parallel_state;
					shareinput_Xslice_state *xslice_state = node->ref->xslice_state;

					PG_TRY();
					{
						/* Wait for producer to finish materializing */
						shareinput_reader_waitready(node->ref);

						if (ParallelWorkerNumberOfSlice == 0)
						{
							/* Leader: create a private STS copy for this consumer slice */
							dsm_segment *producer_seg;
							dsm_segment *my_seg;
							SharedTuplestore *producer_sts;
							SharedTuplestore *my_sts;
							int			nparticipants = xslice_state->nparticipants;
							Size		sz = sts_estimate(nparticipants);

							producer_seg = dsm_attach(xslice_state->sts_dsm_handle);
							if (producer_seg == NULL)
								ereport(ERROR,
										(errmsg("could not attach to cross-slice SharedTuplestore DSM for share %d",
												sisc->share_id)));
							producer_sts = dsm_segment_address(producer_seg);

							my_seg = dsm_create(sz, 0);
							dsm_pin_segment(my_seg);
							dsm_pin_mapping(my_seg);
							my_sts = dsm_segment_address(my_seg);
							memcpy(my_sts, producer_sts, sz);
							sts_reinit_locks(my_sts, nparticipants);

							dsm_detach(producer_seg);

							node->sts_seg = my_seg;
							node->sts_accessor = sts_attach(my_sts,
															ParallelWorkerNumberOfSlice,
															get_shareinput_fileset());
							sts_reinitialize(node->sts_accessor);

							/* Publish handle for other workers in this consumer slice */
							pstate->sts_handle = dsm_segment_handle(my_seg);
							pg_atomic_write_u32(&pstate->ready, 1);
							ConditionVariableBroadcast(&pstate->ready_cv);

							local_state->parallel_seg = my_seg;
						}
						else
						{
							/* Worker: wait for leader to create the STS copy */
							dsm_segment *seg;
							SharedTuplestore *my_sts;

							while (pg_atomic_read_u32(&pstate->ready) == 0)
							{
								check_parallel_producer_failed(pstate);
								ConditionVariableSleep(&pstate->ready_cv,
													   WAIT_EVENT_SHAREINPUT_SCAN);
							}
							ConditionVariableCancelSleep();
							check_parallel_producer_failed(pstate);

							seg = dsm_attach(pstate->sts_handle);
							if (seg == NULL)
								ereport(ERROR,
										(errmsg("could not attach to consumer STS copy DSM for share %d",
												sisc->share_id)));
							dsm_pin_mapping(seg);

							my_sts = dsm_segment_address(seg);
							node->sts_seg = seg;
							node->sts_accessor = sts_attach(my_sts,
															ParallelWorkerNumberOfSlice,
															get_shareinput_fileset());

							local_state->parallel_seg = seg;
						}

						sts_begin_parallel_scan(node->sts_accessor);
					}
					PG_CATCH();
					{
						/*
						 * Unblock the rest of this consumer slice's workers
						 * waiting on pstate->ready: if the leader (or any
						 * worker) hits ERROR here, others must not hang.
						 */
						pg_atomic_write_u32(&pstate->producer_failed, 1);
						ConditionVariableBroadcast(&pstate->ready_cv);
						PG_RE_THROW();
					}
					PG_END_TRY();

					node->parallel_scan_started = true;

					local_state->ready = true;
					node->ts_state = NULL;
					node->ts_pos = -1;
					node->isready = true;
					return;
				}
			}
			/* Intra slice */
			else
			{
				/* producer */
				if (currentSliceId == sisc->producer_slice_id ||
					estate->es_plannedstmt->numSlices == 1)
				{
					ParallelShareInputState *pstate = node->parallel_state;
					char		sts_name[64];
					dsm_segment *seg;
					SharedTuplestore *shared_ts;

					PG_TRY();
					{
						shareinput_create_bufname_prefix(sts_name, sizeof(sts_name),
														 sisc->share_id);

						if (ParallelWorkerNumberOfSlice == 0)
						{
							/* Leader: create SharedTuplestore */
							Size	sz = sts_estimate(TotalParallelWorkerNumberOfSlice);

							seg = dsm_create(sz, 0);
							dsm_pin_mapping(seg);
							pstate->sts_handle = dsm_segment_handle(seg);
							shared_ts = dsm_segment_address(seg);

							node->sts_seg = seg;
							node->sts_accessor = sts_initialize(shared_ts,
																TotalParallelWorkerNumberOfSlice,
																ParallelWorkerNumberOfSlice, 0, 0,
																get_shareinput_fileset(),
																sts_name);

							/* Signal other workers that STS is created */
							pg_atomic_write_u32(&pstate->ready, 1);
							ConditionVariableBroadcast(&pstate->ready_cv);
						}
						else
						{
							/* Worker: wait for leader to create STS, then attach */
							while (pg_atomic_read_u32(&pstate->ready) == 0)
							{
								check_parallel_producer_failed(pstate);
								ConditionVariableSleep(&pstate->ready_cv,
													   WAIT_EVENT_SHAREINPUT_SCAN);
							}
							ConditionVariableCancelSleep();
							check_parallel_producer_failed(pstate);

							seg = dsm_attach(pstate->sts_handle);
							if (seg == NULL)
								ereport(ERROR,
										(errmsg("could not attach to SharedTuplestore DSM for share %d",
												sisc->share_id)));
							dsm_pin_mapping(seg);

							shared_ts = dsm_segment_address(seg);
							node->sts_seg = seg;
							node->sts_accessor = sts_attach(shared_ts,
															ParallelWorkerNumberOfSlice,
															get_shareinput_fileset());
						}

						/* All workers materialize tuples in parallel */
						for (;;)
						{
							bool		shouldFree;
							MinimalTuple tuple;

							outerslot = ExecProcNode(local_state->childState);
							if (TupIsNull(outerslot))
								break;
							tuple = ExecFetchSlotMinimalTuple(outerslot, &shouldFree);
							sts_puttuple(node->sts_accessor, NULL, tuple);
							if (shouldFree)
								heap_free_minimal_tuple(tuple);
						}

						sts_end_write(node->sts_accessor);

						/* Wait for all writers to finish */
						BarrierArriveAndWait(&pstate->write_barrier,
											 WAIT_EVENT_SHAREINPUT_SCAN);

						/*
						 * Signal consumers idempotently from every worker, not
						 * only the leader, so the ready flag is still set if the
						 * leader is interrupted between the barrier and the
						 * signal.  See the cross-slice path for details.
						 */
						pg_atomic_write_u32(&pstate->ready, TotalParallelWorkerNumberOfSlice);
						ConditionVariableBroadcast(&pstate->ready_cv);

						sts_begin_parallel_scan(node->sts_accessor);
					}
					PG_CATCH();
					{
						pg_atomic_write_u32(&pstate->producer_failed, 1);
						ConditionVariableBroadcast(&pstate->ready_cv);
						PG_RE_THROW();
					}
					PG_END_TRY();

					node->parallel_scan_started = true;

					/* Store DSM segment for consumers in same process */
					local_state->parallel_seg = node->sts_seg;
					local_state->ready = true;
					node->ts_state = NULL;
					node->ts_pos = -1;
					node->isready = true;
					return;
				}
				/* There is no consumer in intra-slice when local_state->ready is false */
			}
		}
		/* non parallel */
		else
		{
			/* producer */
			if (currentSliceId == sisc->producer_slice_id || estate->es_plannedstmt->numSlices == 1)
			{
				/* We are the producer */
				if (sisc->cross_slice)
				{
					char		rwfile_prefix[100];

					elog((Debug_shareinput_xslice ? LOG : DEBUG1), "SISC WRITER (shareid=%d, slice=%d): No tuplestore yet, creating tuplestore",
						 sisc->share_id, currentSliceId);

					ts = tuplestore_begin_heap(true, /* randomAccess */
											   false, /* interXact */
											   10); /* maxKBytes FIXME */

					shareinput_create_bufname_prefix(rwfile_prefix, sizeof(rwfile_prefix), sisc->share_id);
					tuplestore_make_shared(ts,
										   get_shareinput_fileset(),
										   rwfile_prefix);
#ifdef FAULT_INJECTOR
					if (SIMPLE_FAULT_INJECTOR("sisc_xslice_temp_files") == FaultInjectorTypeSkip)
				{
					const char *filename = tuplestore_get_buffilename(ts);
					if (!filename)
						ereport(NOTICE, (errmsg("sisc_xslice: buffilename is null")));
					else if (strstr(filename, "base/" PG_TEMP_FILES_DIR) == filename)
						ereport(NOTICE, (errmsg("sisc_xslice: Use default tablespace")));
					else if (strstr(filename, "pg_tblspc/") == filename)
						ereport(NOTICE, (errmsg("sisc_xslice: Use temp tablespace")));
					else
						ereport(NOTICE, (errmsg("sisc_xslice: Unexpected prefix of the tablespace path")));
				}
#endif
				}
				else
				{
					/* intra-slice */
					ts = tuplestore_begin_heap(true, /* randomAccess */
											   false, /* interXact */
											   PlanStateOperatorMemKB((PlanState *) node));

					/*
					 * Offer extra memory usage info for EXPLAIN ANALYZE.
					 *
					 * If this is a cross-slice share, the tuplestore uses very
					 * little memory, because it has to materialize the result on
					 * a file anyway, so that it can be shared across processes.
					 * In that case, reporting memory usage doesn't make much
					 * sense. The "work_mem wanted" value would particularly
					 * non-sensical, as we we would write to a file regardless of
					 * work_mem. So only track memory usage in the non-cross-slice
					 * case.
					 */
					if (node->ss.ps.instrument && node->ss.ps.instrument->need_cdb)
					{
						/* Let the tuplestore share our Instrumentation object. */
						tuplestore_set_instrument(ts, node->ss.ps.instrument);

						/* Request a callback at end of query. */
						node->ss.ps.cdbexplainfun = ExecShareInputScanExplainEnd;
					}
				}

				for (;;)
				{
					outerslot = ExecProcNode(local_state->childState);
					if (TupIsNull(outerslot))
						break;
					tuplestore_puttupleslot(ts, outerslot);
				}

				if (sisc->cross_slice)
				{
					tuplestore_freeze(ts);
					shareinput_writer_notifyready(node->ref);
				}

				tuplestore_rescan(ts);
			}
			/* consumer */
			else
			{
				/*
				 * We are a consumer slice. Wait for the producer to create the
				 * tuplestore.
				 */
				char		rwfile_prefix[100];

				Assert(sisc->cross_slice);

				shareinput_reader_waitready(node->ref);

				shareinput_create_bufname_prefix(rwfile_prefix, sizeof(rwfile_prefix), sisc->share_id);
				ts = tuplestore_open_shared(get_shareinput_fileset(), rwfile_prefix);
			}
		}

		local_state->ts_state = ts;
		local_state->ready = true;
		tsptrno = 0;
	}
	else if (node->parallel_state != NULL && sisc->scan.plan.parallel_aware)
	{
		if (!sisc->cross_slice || (sisc->cross_slice && currentSliceId == sisc->producer_slice_id))
		{
			/*
			 * Consumer in parallel mode: producer already materialized into
			 * SharedTuplestore. Attach and begin parallel scan.
			 */
			ParallelShareInputState *pstate = node->parallel_state;
			dsm_segment *seg;
			SharedTuplestore *shared_ts;

			/* Wait for all producers to finish materialization */
			while (pg_atomic_read_u32(&pstate->ready) < TotalParallelWorkerNumberOfSlice)
			{
				check_parallel_producer_failed(pstate);
				ConditionVariableSleep(&pstate->ready_cv,
									   WAIT_EVENT_SHAREINPUT_SCAN);
			}
			ConditionVariableCancelSleep();
			check_parallel_producer_failed(pstate);

			/*
			 * Reuse the DSM segment from local_state if available (same process
			 * as producer), otherwise attach.
			 */
			if (local_state->parallel_seg != NULL)
			{
				seg = local_state->parallel_seg;
			}
			else
			{
				seg = dsm_attach(pstate->sts_handle);
				if (seg == NULL)
					ereport(ERROR,
							(errmsg("could not attach to SharedTuplestore DSM for share %d",
									sisc->share_id)));
				dsm_pin_mapping(seg);
			}

			shared_ts = dsm_segment_address(seg);
			node->sts_seg = seg;
			node->sts_accessor = sts_attach(shared_ts, ParallelWorkerNumberOfSlice,
											get_shareinput_fileset());

			/*
			 * Synchronize all workers before starting a new parallel scan.
			 * This ensures that any prior consumer group's scan is fully
			 * complete across all workers before we reset the shared read
			 * state.  Without this, a fast worker could start scanning while
			 * a slower worker resets read_page, causing data loss or duplication.
			 */
			BarrierArriveAndWait(&pstate->scan_barrier,
								 WAIT_EVENT_SHAREINPUT_SCAN);

			/* One worker reinitializes shared read counters */
			if (ParallelWorkerNumberOfSlice == 0)
				sts_reinitialize(node->sts_accessor);

			/* Wait for reinitialize to complete before anyone starts scanning */
			BarrierArriveAndWait(&pstate->scan_barrier,
								 WAIT_EVENT_SHAREINPUT_SCAN);

			sts_begin_parallel_scan(node->sts_accessor);
			node->parallel_scan_started = true;

			node->ts_state = NULL;
			node->ts_pos = -1;
			node->isready = true;
			return;
		}
		else if (sisc->cross_slice && currentSliceId != sisc->producer_slice_id)
		{
			/*
			 * Cross-slice parallel consumer, second+ reader in the same slice.
			 * The first consumer already created a private STS copy for this
			 * consumer slice; reuse it.  Each ShareInputScan node has its own
			 * ParallelShareInputState (and therefore its own scan_barrier), so
			 * workers synchronize independently per node.
			 *
			 * Because the hash join builds the inner side fully before probing
			 * the outer side, the prior consumer's parallel scan is guaranteed
			 * to be complete by this point.
			 */
			ParallelShareInputState *pstate = node->parallel_state;
			dsm_segment *seg;
			SharedTuplestore *shared_ts;

			Assert(local_state->parallel_seg != NULL);

			seg = local_state->parallel_seg;
			shared_ts = dsm_segment_address(seg);
			node->sts_seg = seg;
			node->sts_accessor = sts_attach(shared_ts,
											ParallelWorkerNumberOfSlice,
											get_shareinput_fileset());

			/*
			 * Synchronize all workers before reinitializing shared read state,
			 * so the prior consumer's scan is fully done across all workers.
			 */
			BarrierArriveAndWait(&pstate->scan_barrier,
								 WAIT_EVENT_SHAREINPUT_SCAN);

			/* One worker resets the shared read counters */
			if (ParallelWorkerNumberOfSlice == 0)
				sts_reinitialize(node->sts_accessor);

			/* Wait for reinitialize to complete before scanning */
			BarrierArriveAndWait(&pstate->scan_barrier,
								 WAIT_EVENT_SHAREINPUT_SCAN);

			sts_begin_parallel_scan(node->sts_accessor);
			node->parallel_scan_started = true;

			node->ts_state = NULL;
			node->ts_pos = -1;
			node->isready = true;
			return;
		}
	}
	else
	{
		/* Another local reader */
		ts = local_state->ts_state;
		tsptrno = tuplestore_alloc_read_pointer(ts, (EXEC_FLAG_BACKWARD | EXEC_FLAG_REWIND));

		tuplestore_select_read_pointer(ts, tsptrno);
		tuplestore_rescan(ts);
	}

	node->ts_state = ts;
	node->ts_pos = tsptrno;

	node->isready = true;
}


/* ------------------------------------------------------------------
 * 	ExecShareInputScan
 * 	Retrieve a tuple from the ShareInputScan
 * ------------------------------------------------------------------
 */
static TupleTableSlot *
ExecShareInputScan(PlanState *pstate)
{
	ShareInputScanState *node = castNode(ShareInputScanState, pstate);
	ShareInputScan *sisc = (ShareInputScan *) pstate->plan;
	EState	   *estate;
	ScanDirection dir;
	bool		forward;
	TupleTableSlot *slot;

	/*
	 * get state info from node
	 */
	estate = pstate->state;
	dir = estate->es_direction;
	forward = ScanDirectionIsForward(dir);

	if (sisc->this_slice_id != currentSliceId && estate->es_plannedstmt->numSlices != 1)
		elog(ERROR, "cannot execute alien Share Input Scan");

	/* if first time call, need to initialize the tuplestore state.  */
	if (!node->isready)
		init_tuplestore_state(node);
	
	/*
	 * Return NULL when necessary.
	 * This could help improve performance, especially when tuplestore is huge, because ShareInputScan 
	 * do not need to read tuple from tuplestore when discard_output is true, which means current 
	 * ShareInputScan is one but not the last one of Sequence's subplans.
	 */
	if (sisc->discard_output)
		return NULL;

	/* Parallel scan mode: use SharedTuplestore for parallel reading */
	if (node->parallel_scan_started && node->sts_accessor != NULL)
	{
		MinimalTuple tuple;

		slot = node->ss.ps.ps_ResultTupleSlot;
		tuple = sts_parallel_scan_next(node->sts_accessor, NULL);
		if (tuple == NULL)
			return ExecClearTuple(slot);

		SIMPLE_FAULT_INJECTOR("execshare_input_next");

		return ExecStoreMinimalTuple(tuple, slot, false);
	}

	slot = node->ss.ps.ps_ResultTupleSlot;

	Assert(!node->local_state->closed);

	tuplestore_select_read_pointer(node->ts_state, node->ts_pos);
	while(1)
	{
		bool		gotOK;

		gotOK = tuplestore_gettupleslot(node->ts_state, forward, false, slot);

		if (!gotOK)
			return NULL;

		SIMPLE_FAULT_INJECTOR("execshare_input_next");

		return slot;
	}

	Assert(!"should not be here");
	return NULL;
}

/*  ------------------------------------------------------------------
 * 	ExecInitShareInputScan
 * ------------------------------------------------------------------
 */
ShareInputScanState *
ExecInitShareInputScan(ShareInputScan *node, EState *estate, int eflags)
{
	ShareInputScanState *sisstate;
	Plan	   *outerPlan;
	PlanState  *childState;

	Assert(innerPlan(node) == NULL);

	/* create state data structure */
	sisstate = makeNode(ShareInputScanState);
	sisstate->ss.ps.plan = (Plan *) node;
	sisstate->ss.ps.state = estate;
	sisstate->ss.ps.ExecProcNode = ExecShareInputScan;

	sisstate->ts_state = NULL;
	sisstate->ts_pos = -1;

	/*
	 * init child node.
	 * if outerPlan is NULL, this is no-op (so that the ShareInput node will be
	 * only init-ed once).
	 */

	/*
	 * initialize child nodes
	 *
	 * Like a Material node, we shield the child node from the need to support
	 * BACKWARD, or MARK/RESTORE.
	 */
	eflags &= ~(EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK);

	outerPlan = outerPlan(node);
	childState = ExecInitNode(outerPlan, estate, eflags);
	outerPlanState(sisstate) = childState;

	Assert(node->scan.plan.qual == NULL);
	sisstate->ss.ps.qual = NULL;

	/* Misc initialization 
	 * 
	 * Create expression context 
	 */
	ExecAssignExprContext(estate, &sisstate->ss.ps);

	/* 
	 * Initialize result slot and type.
	 */
	ExecInitResultTupleSlotTL(&sisstate->ss.ps, &TTSOpsMinimalTuple);

	sisstate->ss.ps.ps_ProjInfo = NULL;

	/* Initialize parallel state fields */
	sisstate->parallel_state = NULL;
	sisstate->parallel_state_len = 0;
	sisstate->sts_accessor = NULL;
	sisstate->sts_seg = NULL;
	sisstate->parallel_scan_started = false;

	/*
	 * When doing EXPLAIN only, we won't actually execute anything, so don't
	 * bother initializing the state. This isn't merely an optimization:
	 * closing a cross-slice ShareInputScan waits for the consumers to finish,
	 * but if we don't execute anything, it will hang forever.
	 *
	 * We could also exit here immediately if this is an "alien" node, i.e.
	 * a node that doesn't execute in this slice, but we can't easily
	 * detect that here.
	 */
	if ((eflags & EXEC_FLAG_EXPLAIN_ONLY) != 0)
		return sisstate;

	shareinput_local_state *local_state;

	/* expand the list if necessary */
	while (list_length(estate->es_sharenode) <= node->share_id)
	{
		local_state = palloc0(sizeof(shareinput_local_state));
		local_state->ready = false;

		estate->es_sharenode = lappend(estate->es_sharenode, local_state);
	}

	local_state = list_nth(estate->es_sharenode, node->share_id);

	/*
	 * To accumulate the number of CTE consumers executed in this slice.
	 * This variable will be used by the last finishing CTE consumer
	 * in current slice, to wake the corresponding CTE producer up for
	 * cleaning the materialized tuplestore, during squelching.
	 */
	if (currentSliceId == node->this_slice_id &&
		currentSliceId != node->producer_slice_id)
		local_state->nsharers++;

	if (childState)
		local_state->childState = childState;
	sisstate->local_state = local_state;

	/* Get a lease on the shared state */
	if (node->cross_slice)
	{
#ifdef FAULT_INJECTOR
		if (node->producer_slice_id == currentSliceId)
		{
			FaultInjector_InjectFaultIfSet("get_shareinput_reference_delay_writer",
										   DDLNotSpecified,
										   "",  // databaseName
										   ""); // tableName
		}
#endif
		sisstate->ref = get_shareinput_reference(node->share_id);
	}
	else
		sisstate->ref = NULL;

	return sisstate;
}

/*
 * ExecShareInputScanExplainEnd
 *      Called before ExecutorEnd to finish EXPLAIN ANALYZE reporting.
 *
 * Some of the cleanup that ordinarily would occur during ExecEndShareInputScan()
 * needs to be done earlier in order to report statistics to EXPLAIN ANALYZE.
 * Note that ExecEndShareInputScan() will still be during ExecutorEnd().
 */
static void
ExecShareInputScanExplainEnd(PlanState *planstate, struct StringInfoData *buf)
{
	ShareInputScan *sisc = (ShareInputScan *) planstate->plan;
	shareinput_local_state *local_state = ((ShareInputScanState *) planstate)->local_state;

	/*
	 * Release tuplestore resources
	 */
	if (!sisc->cross_slice && local_state && local_state->ts_state)
	{
		tuplestore_end(local_state->ts_state);
		local_state->ts_state = NULL;
	}
}

/* ------------------------------------------------------------------
 * 	ExecEndShareInputScan
 * ------------------------------------------------------------------
 */
void
ExecEndShareInputScan(ShareInputScanState *node)
{
	EState	   *estate = node->ss.ps.state;
	ShareInputScan *sisc = (ShareInputScan *) node->ss.ps.plan;
	shareinput_local_state *local_state = node->local_state;

	/* clean up tuple table */
	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);

	if (node->ref)
	{
		bool		is_producer = (outerPlanState(node) != NULL);

		if (sisc->this_slice_id == currentSliceId || estate->es_plannedstmt->numSlices == 1)
		{
			/*
			 * The producer needs to wait for all the consumers to finish.
			 * Consumers signal the producer that they're done reading,
			 * but are free to exit immediately after that.
			 *
			 * Use outerPlanState to distinguish the actual producer from a
			 * within-slice consumer that happens to be in the producer's
			 * slice (both have currentSliceId == producer_slice_id).
			 */
			if (is_producer)
			{
				if (!local_state->ready)
					init_tuplestore_state(node);

				/*
				 * Defensive: ensure the cross-slice 'ready' flag is set
				 * before waiting on consumers.  If init_tuplestore_state ran
				 * on a non-leader worker via the squelch path while the
				 * leader was interrupted after its barrier arrival, the
				 * post-barrier signal could have been skipped.  Writing it
				 * here is safe (the data is materialized; local_state->ready
				 * implies the STS is complete) and idempotent.
				 */
				if (sisc->scan.plan.parallel_aware && sisc->cross_slice &&
					node->ref->xslice_state != NULL &&
					!pg_atomic_read_u32(&node->ref->xslice_state->ready))
				{
					pg_atomic_write_u32(&node->ref->xslice_state->ready, 1);
					ConditionVariableBroadcast(&node->ref->xslice_state->ready_done_cv);
				}

				shareinput_writer_waitdone(node->ref, sisc->nconsumers);
			}
			else if (currentSliceId != sisc->producer_slice_id)
			{
				if (!local_state->closed)
				{
					/*
					 * In parallel mode, each worker is a separate process
					 * with its own local_state.  Only worker 0 (the main
					 * QE) sends notifydone — it runs ExecEnd after all
					 * launched workers have exited, so one notification
					 * per consumer slice is guaranteed.
					 *
					 * We cannot access node->parallel_state here because
					 * the ParallelContext DSM may already be freed.
					 */
					if (ParallelWorkerNumberOfSlice <= 0)
					{
						shareinput_reader_notifydone(node->ref, sisc->nconsumers);
						local_state->closed = true;
					}
				}
			}
			/* else: within-slice consumer in producer's slice — no cross-slice
			 * notification needed; cleanup handled via local_state. */
		}
		release_shareinput_reference(node->ref, false);
		node->ref = NULL;
	}

	/* Clean up parallel STS resources (intra- and cross-slice) */
	if (sisc->scan.plan.parallel_aware && node->sts_seg)
	{
		bool	is_producer = (outerPlanState(node) != NULL);

		if (sisc->cross_slice)
		{
			bool	is_xslice_consumer = (!is_producer &&
										  currentSliceId != sisc->producer_slice_id);

			/*
			 * Only detach for producer and cross-slice consumers.
			 * Within-slice consumers reuse the producer's DSM via
			 * local_state->parallel_seg and must not detach it.
			 */
			if (is_producer || is_xslice_consumer)
			{
				bool	should_detach = true;

				/*
				 * Multiple cross-slice consumer nodes in the same slice share
				 * a single DSM segment via local_state->parallel_seg.  Use it
				 * as a guard so we only detach once: the first node to clean
				 * up clears the pointer and performs the detach; subsequent
				 * nodes skip it.
				 */
				if (is_xslice_consumer && local_state)
				{
					if (local_state->parallel_seg == NULL)
						should_detach = false;
					else
						local_state->parallel_seg = NULL;
				}

				if (should_detach)
				{
					/*
					 * Only worker 0 (main QE) unpins the DSM segment.
					 * For producer: worker 0 created and pinned the STS DSM.
					 * For cross-slice consumer: worker 0 created and pinned
					 * the private STS copy.  The pin keeps the segment alive
					 * even after other workers detach; worker 0 runs ExecEnd
					 * last.
					 */
					if (ParallelWorkerNumberOfSlice == 0)
						dsm_unpin_segment(dsm_segment_handle(node->sts_seg));
					dsm_detach(node->sts_seg);
				}
			}
		}
		else
		{
			/*
			 * Intra-slice parallel: the producer owns the DSM mapping (from
			 * dsm_create in the leader or its own dsm_attach in workers).
			 * Within-slice consumer nodes that reused the producer's mapping
			 * via local_state->parallel_seg share the producer's pointer and
			 * must not double-detach.  Consumers that did their own
			 * dsm_attach (producer ran in another worker) own an independent
			 * mapping and must detach it.
			 */
			bool	should_detach = true;

			if (!is_producer && local_state &&
				local_state->parallel_seg == node->sts_seg)
				should_detach = false;

			if (should_detach)
				dsm_detach(node->sts_seg);
		}

		if (node->parallel_scan_started && node->sts_accessor)
			sts_end_parallel_scan(node->sts_accessor);
		node->sts_seg = NULL;
		node->sts_accessor = NULL;
		node->parallel_scan_started = false;
	}

	if (local_state && local_state->ts_state)
	{
		tuplestore_end(local_state->ts_state);
		local_state->ts_state = NULL;
	}

	/*
	 * shutdown subplan.  First scanner of underlying share input will
	 * do the shutdown, all other scanners are no-op because outerPlanState
	 * is NULL
	 */
	ExecEndNode(outerPlanState(node));
}

/* ------------------------------------------------------------------
 * 	ExecReScanShareInputScan
 * ------------------------------------------------------------------
 */
void
ExecReScanShareInputScan(ShareInputScanState *node)
{
	/* On first call, initialize the tuplestore state */
	if (!node->isready)
		init_tuplestore_state(node);

	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
	Assert(node->ts_pos != -1);

	tuplestore_select_read_pointer(node->ts_state, node->ts_pos);
	tuplestore_rescan(node->ts_state);
}

/*
 * This is called when the node above us has finished and will not need any more
 * rows from us.
 */
void
ExecSquelchShareInputScan(ShareInputScanState *node, bool force)
{
	ShareInputScan *sisc = (ShareInputScan *) node->ss.ps.plan;
	shareinput_local_state *local_state = node->local_state;

	if (node->ss.ps.squelched)
		return;

	/* clean up tuple table */
	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);

	/*
	 * If this ShareInputScan is shared within the same slice then its
	 * subtree may still need to be executed and the motions in the subtree
	 * cannot yet be stopped. Thus, don't recurse in this case.
	 *
	 * In squelching a cross-slice ShareInputScan writer, we need to ensure
	 * we don't block any reader on other slices as a result of not
	 * materializing the shared plan.
	 *
	 * Note that we emphatically can't "fake" an empty tuple store and just
	 * go ahead waking up the readers because that can lead to wrong results.
	 */
	if (sisc->cross_slice && node->ref)
	{
		bool		is_producer = (outerPlanState(node) != NULL);

		if (is_producer)
		{
			/*
			 * We are the producer. If we haven't materialized the tuplestore
			 * yet, we need to do it now, even though we won't need the data
			 * for anything. There might be other consumers that need it, and
			 * they will hang waiting for us forever otherwise.
			 */
			if (!local_state->ready)
			{
				elog((Debug_shareinput_xslice ? LOG : DEBUG1), "SISC WRITER (shareid=%d, slice=%d): initializing because squelched",
					 sisc->share_id, currentSliceId);
				init_tuplestore_state(node);
			}
		}
		else if (currentSliceId != sisc->producer_slice_id)
		{
			/* We are a consumer. Let the producer know that we're done. */
			Assert(!local_state->closed);

			local_state->ndone++;

			if (local_state->ndone == local_state->nsharers)
			{
				/*
				 * In parallel mode, only worker 0 (main QE) sends
				 * notifydone.  The dsm_pin_segment on the STS copy
				 * keeps it alive for other workers; worker 0 unpins
				 * and notifies when all workers are squelched.
				 */
				if (ParallelWorkerNumberOfSlice <= 0)
				{
					shareinput_reader_notifydone(node->ref, sisc->nconsumers);
					local_state->closed = true;
				}
			}
			release_shareinput_reference(node->ref, true);
			node->ref = NULL;

			/*
			 * Defer all DSM/STS cleanup to ExecEndShareInputScan.  Multiple
			 * consumer nodes in the same slice share a single DSM segment
			 * via local_state->parallel_seg; detaching here in one node's
			 * squelch path would invalidate that pointer and cause the
			 * next consumer node to fail its parallel_seg lookup.
			 */
		}
	}
	node->ss.ps.squelched = true;
}


/*************************************************************************
 * IPC, for cross-slice variants.
 **************************************************************************/

/*
 * When creating a tuplestore file that will be accessed by
 * multiple processes, shareinput_create_bufname_prefix() is used to
 * construct the name for it.
 */
void
shareinput_create_bufname_prefix(char* p, int size, int share_id)
{
	snprintf(p, size, "SIRW_%d_%d_%d",
			 gp_session_id, gp_command_count, share_id);
}

/*
 * Initialization of the shared hash table for cross-slice communication.
 *
 * XXX: Use MaxBackends to size it, on the assumption that max_connections
 * will scale accordingly to query complexity. This is quite fuzzy, you could
 * create a query with tons of cross-slice ShareInputScans but only a few
 * slice, but that ought to be rare enough in practice. This isn't a hard
 * limit anyway, the hash table will use up any "slop" in shared memory if
 * needed.
 */
#define N_SHAREINPUT_SLOTS() (MaxBackends * 5)

Size
ShareInputShmemSize(void)
{
	Size		size;

	size = hash_estimate_size(N_SHAREINPUT_SLOTS(), sizeof(shareinput_Xslice_state));

	return size;
}

void
ShareInputShmemInit(void)
{
	bool		found;

	shareinput_Xslice_dsm_handle_ptr =
		ShmemInitStruct("ShareInputScan DSM handle", sizeof(dsm_handle), &found);
	if (!found)
	{
		HASHCTL		info;

		info.keysize = sizeof(shareinput_tag);
		info.entrysize = sizeof(shareinput_Xslice_state);

		shareinput_Xslice_hash = ShmemInitHash("ShareInputScan notifications",
											   N_SHAREINPUT_SLOTS(),
											   N_SHAREINPUT_SLOTS(),
											   &info,
											   HASH_ELEM | HASH_BLOBS);
	}
}

/*
 * Get reference to the SharedFileSet used to hold all the tuplestore files.
 *
 * This is exported so that it can also be used by the INITPLAN function
 * tuplestores.
 */
SharedFileSet *
get_shareinput_fileset(void)
{
	dsm_handle		handle;

	if (shareinput_Xslice_fileset == NULL)
	{
		dsm_segment *seg;

		LWLockAcquire(ShareInputScanLock, LW_EXCLUSIVE);

		handle = *shareinput_Xslice_dsm_handle_ptr;

		if (handle)
		{
			seg = dsm_attach(handle);
			if (seg == NULL)
				elog(ERROR, "could not attach to ShareInputScan DSM segment");
			dsm_pin_mapping(seg);

			shareinput_Xslice_fileset = dsm_segment_address(seg);
		}
		else
		{
			seg = dsm_create(sizeof(SharedFileSet), 0);
			dsm_pin_segment(seg);
			*shareinput_Xslice_dsm_handle_ptr = dsm_segment_handle(seg);
			dsm_pin_mapping(seg);

			shareinput_Xslice_fileset = dsm_segment_address(seg);
		}

		if (shareinput_Xslice_fileset->refcnt == 0)
			SharedFileSetInit(shareinput_Xslice_fileset, seg);
		else
			SharedFileSetAttach(shareinput_Xslice_fileset, seg);

		LWLockRelease(ShareInputScanLock);
	}

	return shareinput_Xslice_fileset;
}

/*
 * Get a reference to slot in shared memory for this shared scan.
 *
 * If the slot doesn't exist yet, it is created and initialized into
 * "not ready" state.
 *
 * The reference is tracked by the current ResourceOwner, and will be
 * automatically released on abort.
 */
static shareinput_Xslice_reference *
get_shareinput_reference(int share_id)
{
	shareinput_tag tag;
	shareinput_Xslice_state *xslice_state;
	bool		found;
	shareinput_Xslice_reference *ref;

	/* Register our resource owner callback to clean up on first call. */
	if (!shareinput_resowner_callback_registered)
	{
		RegisterResourceReleaseCallback(shareinput_release_callback, NULL);
		shareinput_resowner_callback_registered = true;
	}

	ref = MemoryContextAllocZero(TopMemoryContext,
								 sizeof(shareinput_Xslice_reference));

	LWLockAcquire(ShareInputScanLock, LW_EXCLUSIVE);

	tag.session_id = gp_session_id;
	tag.command_count = gp_command_count;
	tag.share_id = share_id;
	xslice_state = hash_search(shareinput_Xslice_hash,
							   &tag,
							   HASH_ENTER_NULL,
							   &found);
	if (!found)
	{
		if (xslice_state == NULL)
		{
			pfree(ref);
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of cross-slice ShareInputScan slots")));
		}

		xslice_state->refcount = 0;
		pg_atomic_init_u32(&xslice_state->ready, 0);
		pg_atomic_init_u32(&xslice_state->ndone, 0);
		pg_atomic_init_u32(&xslice_state->producer_failed, 0);

		ConditionVariableInit(&xslice_state->ready_done_cv);
		xslice_state->sts_dsm_handle = DSM_HANDLE_INVALID;
		xslice_state->nparticipants = 0;
		elog((Debug_shareinput_xslice ? LOG : DEBUG1), "SISC (shareid=%d, slice=%d): initialized xslice state",
			 share_id, currentSliceId);
	}

	xslice_state->refcount++;

	ref->share_id = share_id;
	ref->xslice_state = xslice_state;
	ref->owner = CurrentResourceOwner;
	dlist_push_head(&shareinput_Xslice_refs, &ref->node);

	LWLockRelease(ShareInputScanLock);

	return ref;
}

/*
 * Release reference to a shared scan.
 *
 * The reference count in the shared memory slot is decreased, and if
 * it reaches zero, it is destroyed if not in reader squelching.
 * The reference is also removed from the list of references tracked by
 * the current ResourceOwner.
 *
 * NB: We don't want to destroy the shared state if in reader squelching,
 * because there might be other readers or writers that are yet to reference
 * it. So leave the work to the producer.
 */
static void
release_shareinput_reference(shareinput_Xslice_reference *ref, bool reader_squelching)
{
	shareinput_Xslice_state *state = ref->xslice_state;

	LWLockAcquire(ShareInputScanLock, LW_EXCLUSIVE);
	state->refcount--;
	elog((Debug_shareinput_xslice ? LOG : DEBUG1), "SISC (shareid=%d, slice=%d): decreased xslice state refcount to %d",
		 state->tag.share_id, currentSliceId, state->refcount);

	if (!reader_squelching && state->refcount == 0)
	{
		bool		found;

		(void) hash_search(shareinput_Xslice_hash,
						   &state->tag,
						   HASH_REMOVE,
						   &found);
		Assert(found);
		elog((Debug_shareinput_xslice ? LOG : DEBUG1), "SISC (shareid=%d, slice=%d): removed xslice state",
			 state->tag.share_id, currentSliceId);
	}
	else if (state->refcount == 0)
		SIMPLE_FAULT_INJECTOR("get_shareinput_reference_done");

	dlist_delete(&ref->node);

	LWLockRelease(ShareInputScanLock);

	pfree(ref);
}

/*
 * Callback to release references on transaction abort.
 */
static void
shareinput_release_callback(ResourceReleasePhase phase,
							bool isCommit,
							bool isTopLevel,
							void *arg)
{
	dlist_mutable_iter miter;

	if (phase != RESOURCE_RELEASE_BEFORE_LOCKS)
		return;

	dlist_foreach_modify(miter, &shareinput_Xslice_refs)
	{
		shareinput_Xslice_reference *ref =
			dlist_container(shareinput_Xslice_reference,
							node,
							miter.cur);

		if (ref->owner == CurrentResourceOwner)
		{
			if (isCommit)
				elog(WARNING, "shareinput lease reference leak: lease %p still referenced", ref);
			release_shareinput_reference(ref, false);
		}
	}
}

/*
 * shareinput_reader_waitready
 *
 *  Called by the reader (consumer) to wait for the writer (producer) to produce
 *  all the tuples and write them to disk.
 *
 *  This is a blocking operation.
 */
static void
shareinput_reader_waitready(shareinput_Xslice_reference *ref)
{
	shareinput_Xslice_state *state = ref->xslice_state;

	elog((Debug_shareinput_xslice ? LOG : DEBUG1), "SISC READER (shareid=%d, slice=%d): Waiting for producer",
		 ref->share_id, currentSliceId);

	/*
	 * Wait until the the producer sets 'ready' to true. The producer will
	 * use the condition variable to wake us up.
	 */
	for (;;)
	{
		/*
		 * set state->ready via pg_atomic_exchange_u32() in shareinput_writer_notifyready()
		 * it acts as a memory barrier, so always get the latest value here
		 */
		int ready = pg_atomic_read_u32(&state->ready);
		if (ready)
			break;
		check_xslice_producer_failed(state);

		ConditionVariableSleep(&state->ready_done_cv, WAIT_EVENT_SHAREINPUT_SCAN);
	}
	ConditionVariableCancelSleep();
	check_xslice_producer_failed(state);

	/* it's ready now */
	elog((Debug_shareinput_xslice ? LOG : DEBUG1), "SISC READER (shareid=%d, slice=%d): Wait ready got writer's handshake",
		 ref->share_id, currentSliceId);
}

/*
 * shareinput_writer_notifyready
 *
 *  Called by the writer (producer) once it is done producing all tuples and
 *  writing them to disk. It notifies all the readers (consumers) that tuples
 *  are ready to be read from disk.
 */
static void
shareinput_writer_notifyready(shareinput_Xslice_reference *ref)
{
	shareinput_Xslice_state *state = ref->xslice_state;

	uint32 old_ready PG_USED_FOR_ASSERTS_ONLY = pg_atomic_exchange_u32(&state->ready, 1);
	if (old_ready)
		elog(ERROR, "shareinput_writer_notifyready() called create the tuplestore twice.");

#ifdef FAULT_INJECTOR
	SIMPLE_FAULT_INJECTOR("shareinput_writer_notifyready");
#endif

	ConditionVariableBroadcast(&state->ready_done_cv);

	elog((Debug_shareinput_xslice ? LOG : DEBUG1), "SISC WRITER (shareid=%d, slice=%d): wrote notify_ready",
		 ref->share_id, currentSliceId);
}

/*
 * shareinput_reader_notifydone
 *
 *  Called by the reader (consumer) to notify the writer (producer) that
 *  it is done reading tuples from disk.
 *
 *  This is a non-blocking operation.
 */
static void
shareinput_reader_notifydone(shareinput_Xslice_reference *ref, int nconsumers)
{
	shareinput_Xslice_state *state = ref->xslice_state;
	int ndone = pg_atomic_add_fetch_u32(&state->ndone, 1);

	/* If we were the last consumer, wake up the producer. */
	if (ndone >= nconsumers)
		ConditionVariableBroadcast(&state->ready_done_cv);

	elog((Debug_shareinput_xslice ? LOG : DEBUG1), "SISC READER (shareid=%d, slice=%d): wrote notify_done",
		 ref->share_id, currentSliceId);
}

/*
 * shareinput_writer_waitdone
 *
 *  Called by the writer (producer) to wait for the "done" notification from
 *  all readers (consumers).
 *
 *  This is a blocking operation.
 */
static void
shareinput_writer_waitdone(shareinput_Xslice_reference *ref, int nconsumers)
{
	shareinput_Xslice_state *state = ref->xslice_state;

	int ready = pg_atomic_read_u32(&state->ready);
	if (!ready)
		elog(ERROR, "shareinput_writer_waitdone() called without creating the tuplestore");

	ConditionVariablePrepareToSleep(&state->ready_done_cv);
	for (;;)
	{
		/*
		 * set state->ndone via pg_atomic_add_fetch_u32() in shareinput_reader_notifydone()
		 * it acts as a memory barrier, so always get the latest value here
		 */
		int	ndone = pg_atomic_read_u32(&state->ndone);
		if (ndone < nconsumers)
		{
			elog((Debug_shareinput_xslice ? LOG : DEBUG1), "SISC WRITER (shareid=%d, slice=%d): waiting for DONE message from %d / %d readers",
				 ref->share_id, currentSliceId, nconsumers - ndone, nconsumers);

			ConditionVariableSleep(&state->ready_done_cv, WAIT_EVENT_SHAREINPUT_SCAN);

			continue;
		}
		ConditionVariableCancelSleep();
		/*
		 * In parallel mode, each consumer slice may have multiple workers
		 * that each call shareinput_reader_notifydone, so ndone can exceed
		 * nconsumers.  Only warn in non-parallel mode.
		 */
		if (ndone > nconsumers && state->nparticipants == 0)
			elog(WARNING, "%d sharers of ShareInputScan reported to be done, but only %d were expected",
				 ndone, nconsumers);
		break;
	}

	elog((Debug_shareinput_xslice ? LOG : DEBUG1), "SISC WRITER (shareid=%d, slice=%d): got DONE message from %d readers",
		 ref->share_id, currentSliceId, nconsumers);

	/* it's all done now */
}

/* ----------------------------------------------------------------
 *		Parallel ShareInputScan Support
 * ----------------------------------------------------------------
 */

/*
 * ExecShareInputScanEstimate
 *		Estimate space needed for parallel coordination info.
 */
void
ExecShareInputScanEstimate(ShareInputScanState *node, ParallelContext *pcxt)
{
	ShareInputScan *sisc = (ShareInputScan *) node->ss.ps.plan;

	if (!sisc->scan.plan.parallel_aware)
		return;

	/*
	 * Producer (has child node) allocates shared state.
	 * Cross-slice consumer (different slice from producer) also needs its
	 * own ParallelShareInputState for intra-slice coordination, so that
	 * the leader can publish the private STS copy's DSM handle.
	 */
	if (outerPlan(sisc) == NULL &&
		!(sisc->cross_slice && sisc->this_slice_id != sisc->producer_slice_id))
		return;

	node->parallel_state_len = sizeof(ParallelShareInputState);
	shm_toc_estimate_chunk(&pcxt->estimator, node->parallel_state_len);
	shm_toc_estimate_keys(&pcxt->estimator, 1);
}

/*
 * ExecShareInputScanInitializeDSM
 *		Initialize parallel coordination info in DSM.
 */
void
ExecShareInputScanInitializeDSM(ShareInputScanState *node, ParallelContext *pcxt)
{
	ShareInputScan *sisc = (ShareInputScan *) node->ss.ps.plan;
	ParallelShareInputState *pstate;
	bool	is_xslice_consumer;

	if (!sisc->scan.plan.parallel_aware)
		return;

	is_xslice_consumer = (outerPlan(sisc) == NULL &&
						   sisc->cross_slice &&
						   sisc->this_slice_id != sisc->producer_slice_id);

	/*
	 * Within-slice consumer: get parallel_state from local_state
	 * (set by the producer in the same slice).
	 */
	if (outerPlan(sisc) == NULL && !is_xslice_consumer)
	{
		node->parallel_state = node->local_state->parallel_state;
		return;
	}

	/*
	 * Producer or cross-slice consumer: allocate and initialize
	 * parallel state in DSM.
	 */
	pstate = shm_toc_allocate(pcxt->toc, node->parallel_state_len);
	memset(pstate, 0, node->parallel_state_len);

	pstate->sts_handle = DSM_HANDLE_INVALID;
	pg_atomic_init_u32(&pstate->ready, 0);
	pg_atomic_init_u32(&pstate->ndone, 0);
	pg_atomic_init_u32(&pstate->producer_failed, 0);
	ConditionVariableInit(&pstate->ready_cv);
	BarrierInit(&pstate->write_barrier, TotalParallelWorkerNumberOfSlice);
	BarrierInit(&pstate->scan_barrier, TotalParallelWorkerNumberOfSlice);

	shm_toc_insert(pcxt->toc, node->ss.ps.plan->plan_node_id, pstate);

	node->parallel_state = pstate;
	node->local_state->parallel_state = pstate;
}

/*
 * ExecShareInputScanReInitializeDSM
 *		Re-initialize parallel coordination info for a fresh scan.
 */
void
ExecShareInputScanReInitializeDSM(ShareInputScanState *node, ParallelContext *pcxt)
{
	ParallelShareInputState *pstate = node->parallel_state;

	if (pstate == NULL)
		return;

	pstate->sts_handle = DSM_HANDLE_INVALID;
	pg_atomic_write_u32(&pstate->ready, 0);
	pg_atomic_write_u32(&pstate->ndone, 0);
	pg_atomic_write_u32(&pstate->producer_failed, 0);
	BarrierInit(&pstate->write_barrier, TotalParallelWorkerNumberOfSlice);
	BarrierInit(&pstate->scan_barrier, TotalParallelWorkerNumberOfSlice);

	if (node->parallel_scan_started && node->sts_accessor)
		sts_end_parallel_scan(node->sts_accessor);
	node->parallel_scan_started = false;
}

/*
 * ExecShareInputScanInitializeWorker
 *		Initialize parallel coordination info in a worker process.
 */
void
ExecShareInputScanInitializeWorker(ShareInputScanState *node,
									ParallelWorkerContext *pwcxt)
{
	ShareInputScan *sisc = (ShareInputScan *) node->ss.ps.plan;

	if (!sisc->scan.plan.parallel_aware)
		return;

	/*
	 * Try to find our pstate in the TOC.  This succeeds for the producer
	 * and for cross-slice consumers (both registered their own pstate
	 * under their plan_node_id).  It returns NULL for within-slice
	 * consumers that share the producer's pstate via local_state.
	 */
	node->parallel_state = shm_toc_lookup(pwcxt->toc,
										  node->ss.ps.plan->plan_node_id,
										  true);  /* missing_ok */

	if (node->parallel_state != NULL)
		node->local_state->parallel_state = node->parallel_state;
	else
		/* Within-slice consumer: get from local_state (set by producer) */
		node->parallel_state = node->local_state->parallel_state;
}
