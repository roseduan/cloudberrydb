/*-------------------------------------------------------------------------
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * pg_task_run_history.c
 *	  save all tasks run histories of pg cron scheduler.
 *
 * IDENTIFICATION
 *		src/backend/catalog/pg_task_run_history.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "access/genam.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/indexing.h"
#include "catalog/pg_task.h"
#include "catalog/pg_task_run_history.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/timestamp.h"

/*
 * TaskRunHistoryCreate
 *		Create an new task run history in pg_task_run_history.
 */
void
TaskRunHistoryCreate(Oid runid, int64 *jobid, const char *database, const char *username,
					 const char *command, const char *status)
{
    Relation pg_task_run_history;
    HeapTuple tup;
    Datum values[Natts_pg_task_run_history];
    bool nulls[Natts_pg_task_run_history];

    memset(values, 0, sizeof(values));
    memset(nulls, false, sizeof(nulls));

    /*
     * Serialize this job's run-history writes against a concurrent DROP TASK.
     * The table lock cannot do it: table_close releases it right here, while
     * the conflict window runs from the reader's snapshot to its commit, so a
     * DROP that read this job's history can still lose commit-time arbitration
     * to an insert that lands in between.  An object lock is held to end of
     * transaction, which is the whole window.
     *
     * It must be LockSharedObject, not LockDatabaseObject: pg_task and
     * pg_task_run_history are shared catalogs, and this job's writers sit in
     * different databases -- the worker runs in the job's database, DROP TASK
     * in whichever database the user issued it from, and MarkRunningTaskAsFailed
     * in the launcher's ("postgres").  A database-scoped tag would hand those
     * writers different locks and exclude nothing.
     *
     * The run-history half of the key, not the definition half: an ALTER TASK
     * rewrites only the pg_task row, and making the launcher wait for it stalls
     * every job the launcher serves, not just this one.
     */
    LockSharedObject(TaskRelationId, (Oid) *jobid, TASK_LOCK_RUN_HISTORY,
                     AccessExclusiveLock);

    /*
     * Only now is the job's existence stable, so only now is it worth asking.
     * Waiting for that lock is what makes the question necessary: the writer we
     * waited for may have been the DROP TASK that removed this job, and it
     * deleted the history it could see, which cannot include a row we have not
     * written yet.  Inserting anyway leaves run history for a job that no longer
     * exists, and nothing ever deletes it -- RemoveTaskRunHistoryByJobId only
     * runs from a DROP, which has already been and gone.
     */
    if (!TaskExists((Oid) *jobid))
    {
        elog(DEBUG1, "skipping run history for dropped job %u", (Oid) *jobid);
        return;
    }

    pg_task_run_history = table_open(TaskRunHistoryRelationId, RowExclusiveLock);

    values[Anum_pg_task_run_history_runid - 1] = ObjectIdGetDatum(runid);
    values[Anum_pg_task_run_history_jobid - 1] = ObjectIdGetDatum((Oid) *jobid);
    values[Anum_pg_task_run_history_database - 1] = CStringGetTextDatum(database);
    values[Anum_pg_task_run_history_username - 1] = CStringGetTextDatum(username);
    values[Anum_pg_task_run_history_command - 1] = CStringGetTextDatum(command);
    values[Anum_pg_task_run_history_status - 1] = CStringGetTextDatum(status);

    nulls[Anum_pg_task_run_history_return_message - 1] = true;
    nulls[Anum_pg_task_run_history_start_time - 1] = true;
    nulls[Anum_pg_task_run_history_end_time - 1] = true;

    tup = heap_form_tuple(RelationGetDescr(pg_task_run_history), values, nulls);
    CatalogTupleInsert(pg_task_run_history, tup);
    heap_freetuple(tup);

    table_close(pg_task_run_history, RowExclusiveLock);
}

/*
 * TaskRunHistoryUpdate
 *		Update an existing task run history in pg_task_run_history.
 */
void
TaskRunHistoryUpdate(Oid runid, int32 *job_pid, const char *status,
                     const char *return_message, TimestampTz *start_time, TimestampTz *end_time)
{
    Relation    pg_task_run_history;
    HeapTuple   tup;
    SysScanDesc scanDescriptor = NULL;
    ScanKeyData scanKey[1];
    Datum       values[Natts_pg_task_run_history];
    bool        nulls[Natts_pg_task_run_history];
    bool        doreplace[Natts_pg_task_run_history];

    memset(values, 0, sizeof(values));
    memset(nulls, false, sizeof(nulls));
    memset(doreplace, false, sizeof(doreplace));

    /*
     * Take this run's job lock before reading the row we are about to update,
     * for the same reason as TaskRunHistoryCreate: the table lock below is
     * self-compatible and table_close drops it anyway, so it cannot cover the
     * window from this statement's snapshot to commit -- which is where a
     * concurrent DROP TASK loses arbitration to the status update staged
     * here.  A run's jobid never changes, so resolving it under
     * AccessShareLock first and locking the job afterwards is safe.
     *
     * Finding nothing here is a shortcut, not the decision: the row can still
     * disappear while we wait for the lock, since waiting for it means waiting
     * for a writer that may be the DROP TASK removing this job.  The decision
     * belongs under the lock, below.
     */
    {
        Relation    probe;
        SysScanDesc probescan;
        ScanKeyData probekey[1];
        HeapTuple   probetup;
        Oid         jobid = InvalidOid;

        probe = table_open(TaskRunHistoryRelationId, AccessShareLock);
        ScanKeyInit(&probekey[0], Anum_pg_task_run_history_runid,
                    BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(runid));
        probescan = systable_beginscan(probe, TaskRunHistoryRunIdIndexId,
                                       true, NULL, 1, probekey);
        probetup = systable_getnext(probescan);
        if (HeapTupleIsValid(probetup))
            jobid = ((Form_pg_task_run_history) GETSTRUCT(probetup))->jobid;
        systable_endscan(probescan);
        table_close(probe, AccessShareLock);

        if (!OidIsValid(jobid))
            return;

        LockSharedObject(TaskRelationId, jobid, TASK_LOCK_RUN_HISTORY,
                         AccessExclusiveLock);
    }

    pg_task_run_history = table_open(TaskRunHistoryRelationId, RowExclusiveLock);

    ScanKeyInit(&scanKey[0],
                Anum_pg_task_run_history_runid,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(runid));
    
    scanDescriptor = systable_beginscan(pg_task_run_history,
                                        TaskRunHistoryRunIdIndexId,
                                        true, NULL, 1, scanKey);
    
    tup = systable_getnext(scanDescriptor);
    if (!HeapTupleIsValid(tup))
    {
        /*
         * The row went away while we waited for the job lock, which means the
         * job was dropped: RemoveTaskRunHistoryByJobId is the only thing that
         * deletes run history, and it only runs from a DROP.  A worker still
         * reporting status on a dropped job has nothing left to update, and this
         * is reached from the cron launcher, where raising an error kills the
         * process that serves every job in the cluster.
         */
        elog(DEBUG1, "run history for runid %u is gone; job was dropped", runid);
        systable_endscan(scanDescriptor);
        table_close(pg_task_run_history, RowExclusiveLock);
        return;
    }

    if (job_pid != NULL)
    {
        values[Anum_pg_task_run_history_job_pid - 1] = Int32GetDatum(*job_pid);
        doreplace[Anum_pg_task_run_history_job_pid - 1] = true;
    }
    if (status != NULL)
    {
        values[Anum_pg_task_run_history_status - 1] = CStringGetTextDatum(status);
        doreplace[Anum_pg_task_run_history_status - 1] = true;
    }
    if (return_message != NULL)
    {
        values[Anum_pg_task_run_history_return_message - 1] = CStringGetTextDatum(return_message);
        doreplace[Anum_pg_task_run_history_return_message - 1] = true;
    }
    if (start_time != NULL)
    {
        values[Anum_pg_task_run_history_start_time - 1] = TimestampTzGetDatum(*start_time);
        doreplace[Anum_pg_task_run_history_start_time - 1] = true;
    }
    if (end_time != NULL)
    {
        values[Anum_pg_task_run_history_end_time - 1] = TimestampTzGetDatum(*end_time);
        doreplace[Anum_pg_task_run_history_end_time - 1] = true;
    }

    tup = heap_modify_tuple(tup, RelationGetDescr(pg_task_run_history), values, nulls, doreplace);
    CatalogTupleUpdate(pg_task_run_history, &tup->t_self, tup);
    heap_freetuple(tup);

    systable_endscan(scanDescriptor);
    table_close(pg_task_run_history, RowExclusiveLock);
}

/*
 * MarkRunningTaskAsFailed
 *		Mark all the running tasks as failed.
 */
void
MarkRunningTaskAsFailed(void)
{
    Relation    pg_task_run_history;
    HeapTuple   tup;
    SysScanDesc scanDescriptor = NULL;
    ScanKeyData scanKey[1];
    List       *runids = NIL;
    ListCell   *lc;

    /*
     * Collect first, update afterwards.  Updating inside the scan would hold
     * the run-history table lock across each row's job lock, the reverse of
     * the order every other writer uses (job lock, then table); no writer
     * waits at the table level today, but an inverted order is one stronger
     * table lock away from deadlocking.  Two phases keep one order everywhere;
     * phase two delegates to TaskRunHistoryUpdate, which already locks the job
     * and tolerates a row that vanished meanwhile.
     */
    pg_task_run_history = table_open(TaskRunHistoryRelationId, AccessShareLock);

    ScanKeyInit(&scanKey[0], Anum_pg_task_run_history_status, BTEqualStrategyNumber,
                F_TEXTEQ, CStringGetTextDatum("running"));

    scanDescriptor = systable_beginscan(pg_task_run_history, InvalidOid,
                                        false, NULL, 1, scanKey);

    while (HeapTupleIsValid(tup = systable_getnext(scanDescriptor)))
    {
        Oid runid = ((Form_pg_task_run_history) GETSTRUCT(tup))->runid;

        runids = lappend_oid(runids, runid);
    }

    systable_endscan(scanDescriptor);
    table_close(pg_task_run_history, AccessShareLock);

    foreach(lc, runids)
    {
        TaskRunHistoryUpdate(lfirst_oid(lc), NULL, "failed",
                             "server restarted", NULL, NULL);
    }

    list_free(runids);
    CommandCounterIncrement();
}

/*
 * RemoveTaskRunHistoryByJobId
 *        Remove all the task run history records for the given jobid.
 */
void
RemoveTaskRunHistoryByJobId(Oid jobid)
{
    Relation    pg_task_run_history;
    HeapTuple   tup;
    SysScanDesc scanDescriptor = NULL;
    ScanKeyData scanKey[1];

    /* See TaskRunHistoryCreate: the job lock, not the table lock, is what
     * covers the snapshot-to-commit window this scan-then-delete opens.  The
     * caller took the definition half first, if it deletes that too. */
    LockSharedObject(TaskRelationId, jobid, TASK_LOCK_RUN_HISTORY,
                     AccessExclusiveLock);

    pg_task_run_history = table_open(TaskRunHistoryRelationId, RowExclusiveLock);

    ScanKeyInit(&scanKey[0], Anum_pg_task_run_history_jobid, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(jobid));

    scanDescriptor = systable_beginscan(pg_task_run_history, TaskRunHistoryJobIdIndexId,
                                        true, NULL, 1, scanKey);

    while (HeapTupleIsValid(tup = systable_getnext(scanDescriptor)))
    {
        CatalogTupleDelete(pg_task_run_history, &tup->t_self);
    }

    systable_endscan(scanDescriptor);
    table_close(pg_task_run_history, RowExclusiveLock);
}
