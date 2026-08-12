/*-------------------------------------------------------------------------
 *
 * bgw_message_queue.h
 *    Shared-memory message queue for SQL → launcher IPC.
 *
 * Adapted from TimescaleDB src/loader/bgw_message_queue.h (Apache 2.0).
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 * IDENTIFICATION
 *    contrib/time_series/src/bgw/bgw_message_queue.h
 *-------------------------------------------------------------------------
 */
#ifndef TIME_SERIES_BGW_MESSAGE_QUEUE_H
#define TIME_SERIES_BGW_MESSAGE_QUEUE_H

#include "postgres.h"
#include "storage/dsm.h"

typedef enum BgwMessageType
{
	BGW_MSG_STOP = 0,
	BGW_MSG_START,
	BGW_MSG_RESTART
} BgwMessageType;

typedef struct BgwMessage
{
	BgwMessageType	message_type;
	pid_t			sender_pid;
	Oid				db_oid;
	dsm_handle		ack_dsm_handle;
} BgwMessage;

/* SQL backend side: send a message and synchronously wait for ack. */
extern bool ts_bgw_message_send_and_wait(BgwMessageType message_type, Oid db_oid);

/* Launcher side: receive next message, send ack back. */
extern void        ts_bgw_message_queue_set_reader(void);
extern BgwMessage *ts_bgw_message_receive(void);
extern void        ts_bgw_message_send_ack(BgwMessage *message, bool success);

/* shared_preload_libraries-time setup. */
extern void ts_bgw_message_queue_shmem_request(void);
extern void ts_bgw_message_queue_shmem_startup(void);
extern void ts_bgw_message_queue_shmem_cleanup(void);

#endif /* TIME_SERIES_BGW_MESSAGE_QUEUE_H */
