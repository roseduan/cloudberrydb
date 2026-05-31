/*-------------------------------------------------------------------------
 *
 * pg_iceberg_av_consumer.h
 *    Autovacuum-driven consumer for pg_ext_aux.pg_iceberg_deletion_queue.
 *
 * Registered as PG core's AutoVacWorkerPostHook from datalake_fdw's _PG_init.
 * The hook fires once per autovacuum worker lifecycle (after do_autovacuum
 * returns and before proc_exit), iterates a bounded batch of pending queue
 * entries, reconstructs fileIOConfig from the stored volume/server/owner
 * tuple, and calls dlagent's /v1/files/cleanup-from-metadata endpoint.
 *
 * IDENTIFICATION
 *    contrib/datalake_fdw/src/am_iceberg/include/pg_iceberg_av_consumer.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef __PG_ICEBERG_AV_CONSUMER_H__
#define __PG_ICEBERG_AV_CONSUMER_H__

/*
 * Register the AutoVacWorkerPostHook and define the four GUCs that govern
 * the consumer's behaviour.  Idempotent; safe to call once from _PG_init.
 */
extern void pg_iceberg_av_consumer_init(void);

#endif /* __PG_ICEBERG_AV_CONSUMER_H__ */
