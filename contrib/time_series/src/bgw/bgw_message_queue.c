/*-------------------------------------------------------------------------
 *
 * bgw_message_queue.c
 *    Shared-memory ring buffer used by SQL backends to send commands
 *    (STOP / START / RESTART <db_oid>) to the per-cluster launcher.
 *    Senders enqueue a BgwMessage, then synchronously wait for an ack
 *    on a per-call DSM segment.
 *
 * Adapted from TimescaleDB src/loader/bgw_message_queue.c (Apache 2.0).
 * Localisation:
 *   - Renamed ts_* symbols left intact; module-internal constants
 *     reworded to "time_series" naming.
 *   - Dropped 9.6 compat shims; CBDB is PG 14, so shm_mq_send is
 *     called directly.
 *   - Error messages reworded.
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 * IDENTIFICATION
 *    contrib/time_series/src/bgw/bgw_message_queue.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/shm_mq.h"
#include "storage/shmem.h"
#include "storage/spin.h"

#include "bgw_message_queue.h"

#define BGW_MQ_MAX_MESSAGES 16
#define BGW_MQ_NAME "time_series_bgw_message_queue"
#define BGW_MQ_TRANCHE_NAME "time_series_bgw_mq_tranche"

#define BGW_MQ_NUM_WAITS 100
#define BGW_MQ_WAIT_INTERVAL 1000L	/* ms -- WaitLatch expects long */

#define BGW_ACK_RETRIES 20
#define BGW_ACK_WAIT_INTERVAL 100L	/* ms */
#define BGW_ACK_QUEUE_SIZE (MAXALIGN(shm_mq_minimum_size + sizeof(int)))

/*
 * Circular queue with one reader (the launcher) and many writers
 * (regular backends executing SQL).
 */
typedef struct MessageQueue
{
	pid_t		reader_pid;		/* InvalidPid when no launcher is reading */
	slock_t		mutex;			/* protects reader_pid */
	LWLock	   *lock;			/* protects (read_upto, num_elements, buffer) */
	uint8		read_upto;
	uint8		num_elements;
	BgwMessage	buffer[BGW_MQ_MAX_MESSAGES];
}			MessageQueue;

typedef enum QueueResponseType
{
	MESSAGE_SENT = 0,
	QUEUE_FULL,
	READER_DETACHED
}			QueueResponseType;

static MessageQueue * mq = NULL;

static void
queue_init(void)
{
	bool		found;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	mq = ShmemInitStruct(BGW_MQ_NAME, sizeof(MessageQueue), &found);
	if (!found)
	{
		memset(mq, 0, sizeof(MessageQueue));
		mq->reader_pid = InvalidPid;
		SpinLockInit(&mq->mutex);
		mq->lock = &(GetNamedLWLockTranche(BGW_MQ_TRANCHE_NAME))->lock;
	}
	LWLockRelease(AddinShmemInitLock);
}

void
ts_bgw_message_queue_shmem_startup(void)
{
	queue_init();
}

void
ts_bgw_message_queue_shmem_request(void)
{
	RequestAddinShmemSpace(sizeof(MessageQueue));
	RequestNamedLWLockTranche(BGW_MQ_TRANCHE_NAME, 1);
}

static pid_t
queue_get_reader(MessageQueue * queue)
{
	pid_t		reader;
	volatile	MessageQueue *vq = queue;

	SpinLockAcquire(&vq->mutex);
	reader = vq->reader_pid;
	SpinLockRelease(&vq->mutex);
	return reader;
}

static void
queue_set_reader(MessageQueue * queue)
{
	volatile	MessageQueue *vq = queue;
	pid_t		reader_pid;

	SpinLockAcquire(&vq->mutex);
	if (vq->reader_pid == InvalidPid)
		vq->reader_pid = MyProcPid;
	reader_pid = vq->reader_pid;
	SpinLockRelease(&vq->mutex);

	if (reader_pid != MyProcPid)
		ereport(ERROR,
				(errmsg("only one reader allowed for time_series bgw message queue"),
				 errhint("current reader pid is %d", reader_pid)));
}

static void
queue_reset_reader(MessageQueue * queue)
{
	volatile	MessageQueue *vq = queue;
	bool		reset = false;

	SpinLockAcquire(&vq->mutex);
	if (vq->reader_pid == MyProcPid)
	{
		reset = true;
		vq->reader_pid = InvalidPid;
	}
	SpinLockRelease(&vq->mutex);

	if (!reset)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("multiple time_series launchers started; only one allowed")));
}

static QueueResponseType
queue_add(MessageQueue * queue, BgwMessage * message)
{
	QueueResponseType message_result = QUEUE_FULL;

	LWLockAcquire(queue->lock, LW_EXCLUSIVE);
	if (queue->num_elements < BGW_MQ_MAX_MESSAGES)
	{
		int			slot = (queue->read_upto + queue->num_elements) %
		BGW_MQ_MAX_MESSAGES;

		memcpy(&queue->buffer[slot], message, sizeof(BgwMessage));
		queue->num_elements++;
		message_result = MESSAGE_SENT;
	}
	LWLockRelease(queue->lock);

	if (queue_get_reader(queue) != InvalidPid)
		SetLatch(&BackendPidGetProc(queue_get_reader(queue))->procLatch);
	else
		message_result = READER_DETACHED;
	return message_result;
}

static BgwMessage *
queue_remove(MessageQueue * queue)
{
	BgwMessage *message = NULL;

	LWLockAcquire(queue->lock, LW_EXCLUSIVE);
	if (queue_get_reader(queue) != MyProcPid)
		ereport(ERROR,
				(errmsg("only the registered reader may receive bgw messages")));

	if (queue->num_elements > 0)
	{
		message = palloc(sizeof(BgwMessage));
		memcpy(message, &queue->buffer[queue->read_upto], sizeof(BgwMessage));
		queue->read_upto = (queue->read_upto + 1) % BGW_MQ_MAX_MESSAGES;
		queue->num_elements--;
	}
	LWLockRelease(queue->lock);
	return message;
}

static BgwMessage *
bgw_message_create(BgwMessageType message_type, Oid db_oid)
{
	BgwMessage *message = palloc(sizeof(BgwMessage));
	dsm_segment *seg;

	seg = dsm_create(BGW_ACK_QUEUE_SIZE, 0);

	*message = (BgwMessage)
	{
		.message_type = message_type,
			.sender_pid = MyProcPid,
			.db_oid = db_oid,
			.ack_dsm_handle = dsm_segment_handle(seg)
	};
	return message;
}

/*
 * Wait (with bounded retries) for the launcher to attach to our ack
 * DSM as the sender.  Without a bound we'd hang forever if the
 * launcher died after we enqueued.
 */
static shm_mq_result
ts_shm_mq_wait_for_attach(MessageQueue * queue,
						  shm_mq_handle *ack_queue_handle)
{
	int			n;
	PGPROC	   *reader_proc;

	for (n = 1; n <= BGW_MQ_NUM_WAITS; n++)
	{
		reader_proc = shm_mq_get_sender(shm_mq_get_queue(ack_queue_handle));
		if (reader_proc != NULL)
			return SHM_MQ_SUCCESS;
		else if (queue_get_reader(queue) == InvalidPid)
			return SHM_MQ_DETACHED;
		WaitLatch(MyLatch,
				  WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
				  BGW_MQ_WAIT_INTERVAL,
				  WAIT_EVENT_MQ_INTERNAL);
		ResetLatch(MyLatch);
		CHECK_FOR_INTERRUPTS();
	}
	return SHM_MQ_DETACHED;
}

static bool
enqueue_message_wait_for_ack(MessageQueue * queue, BgwMessage * message,
							 shm_mq_handle *ack_queue_handle)
{
	Size		bytes_received = 0;
	QueueResponseType send_result;
	bool	   *data = NULL;
	shm_mq_result mq_res;
	bool		ack_received = false;
	int			n;

	send_result = queue_add(queue, message);
	if (send_result != MESSAGE_SENT)
		return false;

	mq_res = ts_shm_mq_wait_for_attach(queue, ack_queue_handle);
	if (mq_res != SHM_MQ_SUCCESS)
		return false;

	for (n = 1; n <= BGW_ACK_RETRIES; n++)
	{
		mq_res = shm_mq_receive(ack_queue_handle, &bytes_received,
								(void **) &data, true);
		if (mq_res != SHM_MQ_WOULD_BLOCK)
			break;
		ereport(DEBUG1, (errmsg("time_series bgw ack receive failure, retrying")));
		WaitLatch(MyLatch,
				  WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
				  BGW_ACK_WAIT_INTERVAL,
				  WAIT_EVENT_MQ_INTERNAL);
		ResetLatch(MyLatch);
		CHECK_FOR_INTERRUPTS();
	}

	if (mq_res != SHM_MQ_SUCCESS)
		return false;

	ack_received = (bytes_received != 0) && *data;
	return ack_received;
}

bool
ts_bgw_message_send_and_wait(BgwMessageType message_type, Oid db_oid)
{
	shm_mq	   *ack_queue;
	dsm_segment *seg;
	shm_mq_handle *ack_queue_handle;
	BgwMessage *message;
	bool		ack_received = false;

	message = bgw_message_create(message_type, db_oid);

	seg = dsm_find_mapping(message->ack_dsm_handle);
	if (seg == NULL)
		ereport(ERROR,
				(errmsg("time_series bgw dsm segment not mapped")));
	ack_queue = shm_mq_create(dsm_segment_address(seg), BGW_ACK_QUEUE_SIZE);
	shm_mq_set_receiver(ack_queue, MyProc);
	ack_queue_handle = shm_mq_attach(ack_queue, seg, NULL);
	if (ack_queue_handle != NULL)
		ack_received = enqueue_message_wait_for_ack(mq, message, ack_queue_handle);
	dsm_detach(seg);
	pfree(message);
	return ack_received;
}

BgwMessage *
ts_bgw_message_receive(void)
{
	return queue_remove(mq);
}

void
ts_bgw_message_queue_set_reader(void)
{
	queue_set_reader(mq);
}

typedef enum MessageAckSent
{
	ACK_SENT = 0,
	DSM_SEGMENT_UNAVAILABLE,
	QUEUE_NOT_ATTACHED,
	SEND_FAILURE
}			MessageAckSent;

static const char *message_ack_sent_err[] = {
	[ACK_SENT] = "ack sent",
	[DSM_SEGMENT_UNAVAILABLE] = "dsm segment unavailable",
	[QUEUE_NOT_ATTACHED] = "ack queue unable to attach",
	[SEND_FAILURE] = "unable to send ack on queue"
};

static MessageAckSent
send_ack(dsm_segment *seg, bool success)
{
	shm_mq	   *ack_queue;
	shm_mq_handle *ack_queue_handle;
	shm_mq_result ack_res = SHM_MQ_SUCCESS;
	int			n;

	ack_queue = dsm_segment_address(seg);
	if (ack_queue == NULL)
		return DSM_SEGMENT_UNAVAILABLE;

	shm_mq_set_sender(ack_queue, MyProc);
	ack_queue_handle = shm_mq_attach(ack_queue, seg, NULL);
	if (ack_queue_handle == NULL)
		return QUEUE_NOT_ATTACHED;

	for (n = 1; n <= BGW_ACK_RETRIES; n++)
	{
		ack_res = shm_mq_send(ack_queue_handle, sizeof(bool), &success, true);
		if (ack_res != SHM_MQ_WOULD_BLOCK)
			break;
		ereport(DEBUG1, (errmsg("time_series bgw ack send failure, retrying")));
		WaitLatch(MyLatch,
				  WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
				  BGW_ACK_WAIT_INTERVAL,
				  WAIT_EVENT_MQ_INTERNAL);
		ResetLatch(MyLatch);
		CHECK_FOR_INTERRUPTS();
	}

	pfree(ack_queue_handle);
	if (ack_res != SHM_MQ_SUCCESS)
		return SEND_FAILURE;
	return ACK_SENT;
}

void
ts_bgw_message_send_ack(BgwMessage * message, bool success)
{
	dsm_segment *seg;

	seg = dsm_attach(message->ack_dsm_handle);
	if (seg != NULL)
	{
		MessageAckSent ack_res = send_ack(seg, success);

		if (ack_res != ACK_SENT)
			ereport(DEBUG1,
					(errmsg("time_series launcher unable to ack pid %d",
							message->sender_pid),
					 errhint("reason: %s", message_ack_sent_err[ack_res])));
		dsm_detach(seg);
	}
	pfree(message);
}

void
ts_bgw_message_queue_shmem_cleanup(void)
{
	queue_reset_reader(mq);
}
