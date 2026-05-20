create schema orca_parallel;
set search_path=orca_parallel, public;
set statement_mem = '256MB';

create table t1(a int, b int) with(parallel_workers=2) distributed by (a);
create table t2(c int, d int ) with(parallel_workers=3) distributed by (c);
insert into t1 select i, i+1 from generate_series(1, 1000)i;
insert into t2 select i, i+2 from generate_series(1, 20000)i;
analyze t1;
analyze t2;

set parallel_setup_cost=0;
set max_parallel_workers_per_gather=4;
set enable_parallel = on;

explain (verbose, costs off) select * from t1  join t2  on t1.a = t2.c;
explain (verbose, costs off) select * from t1  join t2  on t1.a = t2.d;
explain (verbose, costs off) select * from t1  join t2  on t1.b = t2.c;
explain (verbose, costs off) select * from t1  join t2  on t1.b = t2.d;

-- Redistribute Motion
alter table t2 set (parallel_workers=2);
set max_parallel_workers_per_gather=2;
explain (verbose, costs off) select * from t1  join t2  on t1.a = t2.c;
explain (verbose, costs off) select * from t1  join t2  on t1.a = t2.d;
explain (verbose, costs off) select * from t1  join t2  on t1.b = t2.c;
explain (verbose, costs off) select * from t1  join t2  on t1.b = t2.d;

-- Left Join
explain (verbose, costs off) select * from t1 left join t2  on t1.a = t2.c;
-- Right Join
explain (verbose, costs off) select * from t1 right join t2 on t1.a = t2.c;
-- Full Join
explain (verbose, costs off) select * from t1 full join t2  on t1.a = t2.c;
-- Semi Join
explain (verbose, costs off) select *  from t1 where exists (select 1 from t2 where t2.c = t1.a);

create table t3_null(c2 int) distributed randomly;
insert into t3_null values (1), (2), (3), (null), (5), (null), (10), (20);
analyze t3_null;
-- Anti Semi Join (not-in)
explain (costs off) select * from t1 where a not in (select c2 from t3_null);

-- Broadcast Motion
drop table if exists t1;
drop table if exists t2;
create table t1(a int, b int) with(parallel_workers=2) distributed by (a);
create table t2(c int, d int ) with(parallel_workers=2) distributed by (c);
insert into t1 select i, i+1 from generate_series(1, 100)i;
insert into t2 select i, i+2 from generate_series(1, 30000)i;
analyze t1;
analyze t2;
explain (verbose, costs off) select * from t1  join t2  on t1.a = t2.c;
explain (verbose, costs off) select * from t1  join t2  on t1.a = t2.d;
explain (verbose, costs off) select * from t1  join t2  on t1.b = t2.c;
explain (verbose, costs off) select * from t1  join t2  on t1.b = t2.d;

-- Hash Agg
drop table if exists t0;

create table t0 (
    a int,
    b int
) distributed by (b);

insert into t0
select i % 10000, i 
from generate_series(1, 5000000) i;

analyze t0;

explain (costs off) select a, count(*) from t0 group by a;
explain (costs off) select b, count(*) from t0 group by b;

-- Group Agg
set optimizer_enable_hashagg = off;
explain (costs off) select a, count(*) from t0 group by a;
explain (costs off) select b, count(*) from t0 group by b;
reset optimizer_enable_hashagg;

-- DQA
explain (costs off) select b, count(distinct a) from t1 group by b;

-- No-Group-by Agg
explain (costs off) select count(*), sum(b) from t0;


explain (costs off)
select * from t1 where a in (
    select c from t2 limit 10
);

-- Subquery with LIMIT and ORDER BY should NOT use parallel scan
explain (costs off)
select * from t1 where a in (
    select c from t2 order by c limit 10
);

-- Top-level LIMIT should still allow parallel scan (query_level = 0)
explain (costs off) select * from t2 limit 100;

-- Nested subquery with LIMIT - inner limit should prevent parallel scan
explain (costs off)
select * from t1 where a in (
    select c from t2 where d in (
        select a from t1 limit 5
    )
);

-- =============================================================
-- Test: Non-parallel operators should NOT sit above Parallel Append
-- (Parallel Union All). When a UNION ALL subquery is used as input
-- to joins, aggregations, or window functions, the parent operator
-- must be the parallel variant (e.g., Parallel Hash Join, not Hash Join).
-- =============================================================

-- Setup: two tables with parallel_workers enabled
drop table if exists pu_t1;
drop table if exists pu_t2;
drop table if exists pu_t3;

create table pu_t1(a int, b int) with(parallel_workers=2) distributed by (a);
create table pu_t2(c int, d int) with(parallel_workers=2) distributed by (c);
create table pu_t3(e int, f text) with(parallel_workers=2) distributed by (e);

insert into pu_t1 select i, i+1 from generate_series(1, 10000) i;
insert into pu_t2 select i, i+2 from generate_series(1, 10000) i;
insert into pu_t3 select i, 'val_' || i from generate_series(1, 10000) i;
analyze pu_t1;
analyze pu_t2;
analyze pu_t3;

-- Test 1: Hash Join over UNION ALL
-- The join above the union all must be Parallel Hash Join, NOT Hash Join.
explain (costs off)
select *
from (select a as k, b as v from pu_t1
      union all
      select c as k, d as v from pu_t2) u
join pu_t3 on u.k = pu_t3.e;

-- Test 2: Aggregation over UNION ALL
-- The aggregate over the union all result should use parallel aggregation.
explain (costs off)
select k, count(*), sum(v)
from (select a as k, b as v from pu_t1
      union all
      select c as k, d as v from pu_t2) u
group by k;

-- Test 3: Window function over UNION ALL
-- The window function should not be a non-parallel SequenceProject above
-- Parallel Append.
explain (costs off)
select k, v, row_number() over (partition by k order by v) as rn
from (select a as k, b as v from pu_t1
      union all
      select c as k, d as v from pu_t2) u;

-- Test 4: Hash Join with UNION ALL on both sides
-- Both sides of the join contain union all; both should be under
-- parallel operators.
explain (costs off)
select *
from (select a as k, b as v from pu_t1
      union all
      select c as k, d as v from pu_t2) u1
join (select a as k, b as v from pu_t1
      union all
      select e as k, e as v from pu_t3) u2
on u1.k = u2.k;

-- Test 5: Multi-level: Agg over Join over UNION ALL
-- Both join and agg should use parallel variants.
explain (costs off)
select u.k, count(*)
from (select a as k, b as v from pu_t1
      union all
      select c as k, d as v from pu_t2) u
join pu_t3 on u.k = pu_t3.e
group by u.k;

-- Test 6: Correctness check - verify results match serial execution
-- Run the actual query and compare counts to ensure correct results.
set optimizer_enable_parallel_append = off;
select count(*) as serial_count
from (select a as k, b as v from pu_t1
      union all
      select c as k, d as v from pu_t2) u
join pu_t3 on u.k = pu_t3.e;

set optimizer_enable_parallel_append = on;
select count(*) as parallel_count
from (select a as k, b as v from pu_t1
      union all
      select c as k, d as v from pu_t2) u
join pu_t3 on u.k = pu_t3.e;

-- Test 7: Correctness check - aggregation results
set optimizer_enable_parallel_append = off;
select sum(total) as serial_sum from (
    select k, count(*) as total
    from (select a as k from pu_t1
          union all
          select c as k from pu_t2) u
    group by k
) t;

set optimizer_enable_parallel_append = on;
select sum(total) as parallel_sum from (
    select k, count(*) as total
    from (select a as k from pu_t1
          union all
          select c as k from pu_t2) u
    group by k
) t;

reset enable_parallel;
reset max_parallel_workers_per_gather;
reset parallel_setup_cost;
reset statement_mem;

-- start_ignore
drop schema orca_parallel cascade;
-- end_ignore
