-- Test various scenarios with respect to backend termination.

0:CREATE RESOURCE QUEUE rq_terminate WITH (active_statements = 1);
0:CREATE ROLE role_terminate RESOURCE QUEUE rq_terminate;

-- Helper: after pg_terminate_backend() only sends SIGTERM, the terminated
-- backend releases its resource-queue slot asynchronously in proc_exit ->
-- ProcKill -> AtExitCleanup_ResPortals, which runs *after* the FATAL message
-- reaches the client. The sanity checks below must therefore poll until the
-- counter settles instead of reading it once (otherwise they race the cleanup
-- and flake on a loaded machine). On a genuine leak the poll times out and
-- returns the stale value, so the diff still catches it.
0:CREATE FUNCTION rq_wait_status(p_rsqname text, p_expected float4)
    RETURNS TABLE(rsqcountlimit float4, rsqcountvalue float4) AS $$
DECLARE /* in func */
    v_lim float4; /* in func */
    v_val float4; /* in func */
BEGIN /* in func */
    FOR i IN 1..300 LOOP /* in func */
        SELECT s.rsqcountlimit, s.rsqcountvalue INTO v_lim, v_val
            FROM pg_resqueue_status s WHERE s.rsqname = p_rsqname; /* in func */
        EXIT WHEN v_val = p_expected; /* in func */
        PERFORM pg_sleep(0.1); /* in func */
    END LOOP; /* in func */
    rsqcountlimit := v_lim; /* in func */
    rsqcountvalue := v_val; /* in func */
    RETURN NEXT; /* in func */
END; /* in func */
$$
LANGUAGE plpgsql;

--
-- Scenario 1: Terminate a backend with a regular open cursor
--
1:SET ROLE role_terminate;
1:BEGIN;
1:DECLARE cs1 CURSOR FOR SELECT 0;

0:SELECT pg_terminate_backend(pid) FROM pg_stat_activity
  WHERE query='DECLARE cs1 CURSOR FOR SELECT 0;';

1<:
-- Sanity check: Ensure that the resource queue is now empty.
0:SELECT rsqcountlimit, rsqcountvalue FROM rq_wait_status('rq_terminate', 0);

--
-- Scenario 2: Terminate a backend with a holdable open cursor that has been
-- persisted.
--
2:SET ROLE role_terminate;
2:DECLARE cs2 CURSOR WITH HOLD FOR SELECT 0;

0:SELECT pg_terminate_backend(pid) FROM pg_stat_activity
  WHERE query='DECLARE cs2 CURSOR WITH HOLD FOR SELECT 0;';

2<:
-- Sanity check: Ensure that the resource queue is now empty.
0:SELECT rsqcountlimit, rsqcountvalue FROM rq_wait_status('rq_terminate', 0);

--
-- Scenario 3: Terminate a backend with a waiting statement
--
3:SET ROLE role_terminate;
3:BEGIN;
3:DECLARE cs3 CURSOR FOR SELECT 0;
4:SET ROLE role_terminate;
4&:SELECT 331763;

0:SELECT pg_terminate_backend(pid) FROM pg_stat_activity
  WHERE query='SELECT 331763;';

4<:
3:END;
-- Sanity check: Ensure that the resource queue is now empty.
0:SELECT rsqcountlimit, rsqcountvalue FROM rq_wait_status('rq_terminate', 0);

--
-- Scenario 4: Terminate a backend with a waiting holdable cursor
--
5:SET ROLE role_terminate;
5:BEGIN;
5:DECLARE cs4 CURSOR FOR SELECT 0;
6:SET ROLE role_terminate;
6&:DECLARE cs5 CURSOR WITH HOLD FOR SELECT 0;

0:SELECT pg_terminate_backend(pid) FROM pg_stat_activity
  WHERE query='DECLARE cs5 CURSOR WITH HOLD FOR SELECT 0;';

6<:
5:END;
-- Sanity check: Ensure that the resource queue is now empty.
0:SELECT rsqcountlimit, rsqcountvalue FROM rq_wait_status('rq_terminate', 0);

--
-- Scenario 5: Race during termination of session having a waiting portal with
-- another session waking up the same one. This can happen if the waiter during
-- termination, hasn't yet removed itself from the wait queue in
-- AbortOutOfAnyTransaction() -> .. -> ResLockWaitCancel(), and another session
-- sees it on the wait queue, does an external grant and wakeup. This causes a
-- leak, as the external grant is never cleaned up. In an asserts build we see:
-- FailedAssertion(""!(SHMQueueEmpty(&(MyProc->myProcLocks[i])))"", File: ""proc.c", Line: 1031
--
7:SET ROLE role_terminate;
7:BEGIN;
7:DECLARE cs6 CURSOR FOR SELECT 0;
8:SET ROLE role_terminate;
8&:SELECT 331765;

0:SELECT gp_inject_fault('res_lock_wait_cancel_before_partition_lock', 'suspend', dbid) FROM
    gp_segment_configuration WHERE content = -1 AND role = 'p';

-- Fire the termination first and it will be stuck in the middle of aborting.
0:SELECT pg_terminate_backend(pid) FROM pg_stat_activity
  WHERE query='SELECT 331765;';
0:SELECT gp_wait_until_triggered_fault('res_lock_wait_cancel_before_partition_lock', 1, dbid) FROM
    gp_segment_configuration WHERE content = -1 AND role = 'p';

-- Now perform the external grant (as a part of relinquishing a spot in the queue)
7:CLOSE cs6;

-- Sanity check: Ensure that the resource queue now has 1 active statement (from
-- the external grant).
0:SELECT rsqcountlimit, rsqcountvalue FROM rq_wait_status('rq_terminate', 1);

0:SELECT gp_inject_fault('res_lock_wait_cancel_before_partition_lock', 'reset', dbid) FROM
    gp_segment_configuration WHERE content = -1 AND role = 'p';

8<:
7:END;

-- Sanity check: Ensure that the resource queue is now empty.
0:SELECT rsqcountlimit, rsqcountvalue FROM rq_wait_status('rq_terminate', 0);

--
-- Scenario 6: Same as 5, except the statement being terminated is a holdable cursor.
--
9:SET ROLE role_terminate;
9:BEGIN;
9:DECLARE cs7 CURSOR FOR SELECT 0;
10:SET ROLE role_terminate;
10&:DECLARE cs8 CURSOR WITH HOLD FOR SELECT 0;

0:SELECT gp_inject_fault('res_lock_wait_cancel_before_partition_lock', 'suspend', dbid) FROM
    gp_segment_configuration WHERE content = -1 AND role = 'p';

-- Fire the termination first and it will be stuck in the middle of aborting.
0:SELECT pg_terminate_backend(pid) FROM pg_stat_activity
  WHERE query='DECLARE cs8 CURSOR WITH HOLD FOR SELECT 0;';
0:SELECT gp_wait_until_triggered_fault('res_lock_wait_cancel_before_partition_lock', 1, dbid) FROM
    gp_segment_configuration WHERE content = -1 AND role = 'p';

-- Now perform the external grant (as a part of relinquishing a spot in the queue)
9:CLOSE cs6;

-- Sanity check: Ensure that the resource queue now has 1 active statement (from
-- the external grant).
0:SELECT rsqcountlimit, rsqcountvalue FROM rq_wait_status('rq_terminate', 1);

0:SELECT gp_inject_fault('res_lock_wait_cancel_before_partition_lock', 'reset', dbid) FROM
    gp_segment_configuration WHERE content = -1 AND role = 'p';

10<:
9:END;

-- Sanity check: Ensure that the resource queue is now empty.
0:SELECT rsqcountlimit, rsqcountvalue FROM rq_wait_status('rq_terminate', 0);

-- Cleanup
0:DROP FUNCTION rq_wait_status(text, float4);
0:DROP ROLE role_terminate;
0:DROP RESOURCE QUEUE rq_terminate;
