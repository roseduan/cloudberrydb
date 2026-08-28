--
-- Parallel-aware LASJ_NOTIN (NOT IN) with a NULL hash key on the inner side.
--
-- ExecHashJoinImpl has a CBDB-only early return: for JOIN_LASJ_NOTIN, as soon
-- as a NULL hash key is found on the inner side the whole join is abandoned,
-- because NOT IN against a NULL yields no rows.  That early return leaves the
-- parallel build_barrier short of PHJ_BUILD_RUNNING, so it has to fast-forward
-- the barrier the same way upstream's empty-inner case does; otherwise
-- ExecHashTableDetach() cannot negotiate who frees the shared batch state.
--
-- A mistake here shows up as a hang in the build barrier rather than a wrong
-- answer, so the join is run repeatedly.  Every execution must return zero rows.
--
set optimizer = off;   -- ORCA parallel is not supported here
create schema test_phj_lasj;
set search_path to test_phj_lasj;

create table phj_notin_o(a int) with (parallel_workers=2) distributed by (a);
create table phj_notin_i(b int) with (parallel_workers=2) distributed by (b);
insert into phj_notin_o select i from generate_series(1, 100) i;
insert into phj_notin_i select i from generate_series(1, 100) i;
-- The NULL is what triggers the early return.
insert into phj_notin_i values (NULL);
analyze phj_notin_o;
analyze phj_notin_i;

set enable_parallel = on;
set min_parallel_table_scan_size = 0;
set parallel_setup_cost = 0;
set parallel_tuple_cost = 0;
set enable_mergejoin = off;
set enable_nestloop = off;

-- Should plan as a parallel-aware Hash Left Anti Semi (Not-In) Join.
explain (costs off)
select count(*) from phj_notin_o where a not in (select b from phj_notin_i);

-- NOT IN over an inner side containing NULL yields no rows.  Repeat to exercise
-- the build/abandon/teardown cycle more than once.
select count(*) as must_be_zero from phj_notin_o
where a not in (select b from phj_notin_i);
select count(*) as must_be_zero from phj_notin_o
where a not in (select b from phj_notin_i);
select count(*) as must_be_zero from phj_notin_o
where a not in (select b from phj_notin_i);

-- Without the NULL the same shape must still produce correct results, so the
-- early return isn't masking a broken join.
delete from phj_notin_i where b is null;
delete from phj_notin_i where b > 50;
analyze phj_notin_i;
select count(*) as must_be_fifty from phj_notin_o
where a not in (select b from phj_notin_i);

-- Same early return with the leader excluded from the scan.  Upstream's report
-- was specific to parallel_leader_participation = off, because it changes which
-- participant is last to detach from the build barrier and therefore which one
-- frees pstate->batches.
insert into phj_notin_i values (NULL);
analyze phj_notin_i;
set parallel_leader_participation = off;
select count(*) as must_be_zero from phj_notin_o
where a not in (select b from phj_notin_i);
select count(*) as must_be_zero from phj_notin_o
where a not in (select b from phj_notin_i);
reset parallel_leader_participation;

-- A larger inner side, so the parallel scan is really split up and the NULL is
-- not in the first chunk of blocks that everybody reads.  A participant that
-- does not hash the NULL row itself learns about it from the shared
-- pstate->phs_lasj_has_null flag instead (MultiExecParallelHash), and it has to
-- join the same fast-forward wait: if any participant skipped that wait, the
-- barrier would never reach PHJ_BUILD_RUNNING and the query would hang.
-- work_mem is squeezed so the build has gone multi-batch before it is
-- abandoned.
create table phj_notin_big_o(a int) with (parallel_workers=2) distributed by (a);
create table phj_notin_big_i(b int) with (parallel_workers=2) distributed by (b);
insert into phj_notin_big_o select i from generate_series(1, 50000) i;
insert into phj_notin_big_i select i from generate_series(1, 50000) i;
analyze phj_notin_big_o;
analyze phj_notin_big_i;
-- Appended last, so it sits in the final block rather than the first one.
insert into phj_notin_big_i values (NULL);

set work_mem = '64kB';
explain (costs off)
select count(*) from phj_notin_big_o
where a not in (select b from phj_notin_big_i);
select count(*) as must_be_zero from phj_notin_big_o
where a not in (select b from phj_notin_big_i);
select count(*) as must_be_zero from phj_notin_big_o
where a not in (select b from phj_notin_big_i);
set parallel_leader_participation = off;
select count(*) as must_be_zero from phj_notin_big_o
where a not in (select b from phj_notin_big_i);
reset parallel_leader_participation;

-- And again without the NULL, to show the multi-batch build still joins.
delete from phj_notin_big_i where b is null;
delete from phj_notin_big_i where b > 20000;
analyze phj_notin_big_i;
select count(*) as must_be_30000 from phj_notin_big_o
where a not in (select b from phj_notin_big_i);
reset work_mem;

reset enable_parallel;
reset min_parallel_table_scan_size;
reset parallel_setup_cost;
reset parallel_tuple_cost;
reset enable_mergejoin;
reset enable_nestloop;
set search_path to public;
drop schema test_phj_lasj cascade;
