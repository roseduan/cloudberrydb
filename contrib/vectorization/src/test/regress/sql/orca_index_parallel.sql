-- Tests for ORCA Parallel Index Scan and Parallel Index Only Scan
create schema orca_index_parallel;
set search_path=orca_index_parallel, public;
set statement_mem = '256MB';
set parallel_setup_cost=0;
set max_parallel_workers_per_gather=4;
set enable_parallel = on;

-- parallel index scan test
create table pidx1(a int, b int) with (parallel_workers=2) distributed by (a);
create table pidx2(c int, d int) with (parallel_workers=2) distributed by (c);
create table pidx3(e int, f int) with (parallel_workers=2) distributed by (e);

create index pidx1_a_idx on pidx1(a);
create index pidx1_b_idx on pidx1(b);
create index pidx2_c_idx on pidx2(c);
create index pidx2_d_idx on pidx2(d);
create index pidx3_e_idx on pidx3(e);
create index pidx3_f_idx on pidx3(f);

insert into pidx1 select i, i from generate_series(1, 10000) i;
insert into pidx2 select i, i from generate_series(1, 200000) i;
insert into pidx3 select i, i from generate_series(1, 300) i;

analyze pidx1;
analyze pidx2;
analyze pidx3;

-- parallel index scan(simple)
explain (verbose, costs off) select * from pidx1 where a < 1000;
select count(*) from pidx1 where a < 1000;

explain (verbose, costs off) select * from pidx1 where b < 1000;
select count(*) from pidx1 where b < 1000;

-- parallel index scan(no redistribute motion)
explain (verbose, costs off) select * from pidx1 left join pidx2 on pidx1.a = pidx2.c where pidx1.a < 1000;
select count(*) from pidx1 left join pidx2 on pidx1.a = pidx2.c where pidx1.a < 1000;

-- parallel index scan(redistribute motion)
explain (verbose, costs off) select * from pidx1 left join pidx2 on pidx1.b = pidx2.c where pidx1.b < 1000;
select count(*) from pidx1 left join pidx2 on pidx1.b = pidx2.c where pidx1.b < 1000;

-- parallel index scan(broadcast motion)
explain (verbose, costs off) select * from pidx1 join pidx3 on pidx1.a = pidx3.f where pidx1.a < 5000;
select count(*) from pidx1 join pidx3 on pidx1.a = pidx3.f where pidx1.a < 5000;

-- parallel index scan(with limit)
explain (verbose, costs off) select * from pidx1 where a < 1000 limit 10;
select count(*) from pidx1 where a < 1000 limit 10;

-- parallel index scan(with order and limit)
explain (verbose, costs off) select * from pidx1 where a < 100000 order by a limit 10;
select * from pidx1 where a < 100000 order by a limit 10;

-- parallel index scan inside IN subquery
set enable_hashagg = off;
explain (verbose, costs off) select * from pidx1 where a in (select c from pidx2 where c < 500);
select count(*) from pidx1 where a in (select c from pidx2 where c < 500);
reset enable_hashagg;

-- Subquery with LIMIT and ORDER BY should NOT use parallel scan
explain (verbose, costs off) select * from pidx1 where a in (select c from pidx2 order by c limit 10);
select * from pidx1 where a in (select c from pidx2 order by c limit 10) order by a;

-- left join
explain (verbose, costs off) select * from pidx1 left join pidx2 on pidx1.a = pidx2.c where pidx1.a < 1000;
select count(*) from pidx1 left join pidx2 on pidx1.a = pidx2.c where pidx1.a < 1000;

-- right join
explain (verbose, costs off) select * from pidx1 right join pidx2 on pidx1.a = pidx2.c where pidx2.c < 1000;
select count(*) from pidx1 right join pidx2 on pidx1.a = pidx2.c where pidx2.c < 1000;

-- semi join with parallel index scan
explain (verbose, costs off) select * from pidx1 where exists (select 1 from pidx2 where pidx2.c = pidx1.a);
select count(*) from pidx1 where exists (select 1 from pidx2 where pidx2.c = pidx1.a);

-- anti semi join with parallel index scan
explain (verbose, costs off) select * from pidx1 where a not in (select c from pidx2 where c < 1000);
select count(*) from pidx1 where a not in (select c from pidx2 where c < 1000);

-- parallel index scan with hash aggregation
explain (verbose, costs off) select a, count(*) from pidx1 where a < 1000 group by a;
select count(*) from (select a, count(*) from pidx1 where a < 1000 group by a) s;

-- Nested subquery with LIMIT - inner limit should prevent parallel index scan
explain (costs off)
select * from pidx1 where a in (
    select c from pidx2 where c in (
        select a from pidx1 limit 5
    )
);

-- GUC to control plan
set optimizer_enable_indexscan to off;
explain (verbose, costs off) select * from pidx1 where a < 1000;

reset optimizer_enable_indexscan;

-- parallel index only scan test
-- (modeled after parallel index scan tests from debug_parallel_index_scan branch)
-- Tables have an extra text column c/f/i so that "select *" cannot use index only scan.
create table pios1(a int, b int, c text) with (parallel_workers=2) distributed by (a);
create table pios2(d int, e int, f text) with (parallel_workers=2) distributed by (d);
create table pios3(g int, h int, i text) with (parallel_workers=2) distributed by (g);

create index pios1_ab_idx on pios1(a, b);
create index pios1_b_idx on pios1(b);
create index pios2_de_idx on pios2(d, e);
create index pios2_e_idx on pios2(e);
create index pios3_gh_idx on pios3(g, h);
create index pios3_h_idx on pios3(h);

insert into pios1 select i, i, 'text_'||i from generate_series(1, 10000) i;
insert into pios2 select i, i, 'text_'||i from generate_series(1, 200000) i;
insert into pios3 select i, i, 'text_'||i from generate_series(1, 300) i;

analyze pios1;
analyze pios2;
analyze pios3;

-- VACUUM is needed to populate the visibility map (relallvisible).
-- Without it, index-only scan cost equals index scan cost (dPartialVisFrac=1),
-- causing the optimizer to prefer parallel index scan over parallel index only scan.
vacuum pios1;
vacuum pios2;
vacuum pios3;

set enable_bitmapscan = off;
set enable_seqscan = off;
set enable_indexscan = on;

-- parallel index only scan (simple, on dist key)
explain (verbose, costs off) select a, b from pios1 where a < 1000;
select count(*) from (select a, b from pios1 where a < 1000) s;

-- parallel index only scan (simple, on non-dist key)
explain (verbose, costs off) select b from pios1 where b < 1000;
select count(*) from (select b from pios1 where b < 1000) s;

-- should NOT use index only scan (column c not in index)
explain (verbose, costs off) select a, b, c from pios1 where a < 1000;

-- parallel index only scan (no redistribute motion, join on dist key)
explain (verbose, costs off) select pios1.a, pios1.b from pios1 left join pios2 on pios1.a = pios2.d where pios1.a < 1000;
select count(*) from (select pios1.a, pios1.b from pios1 left join pios2 on pios1.a = pios2.d where pios1.a < 1000) s;

-- parallel index only scan (redistribute motion, join on non-dist key)
explain (verbose, costs off) select pios1.a, pios1.b from pios1 left join pios2 on pios1.b = pios2.d where pios1.b < 1000;
select count(*) from (select pios1.a, pios1.b from pios1 left join pios2 on pios1.b = pios2.d where pios1.b < 1000) s;

-- parallel index only scan (broadcast motion, join with small table)
explain (verbose, costs off) select pios1.a, pios1.b from pios1 join pios3 on pios1.a = pios3.h where pios1.a < 5000;
select count(*) from (select pios1.a, pios1.b from pios1 join pios3 on pios1.a = pios3.h where pios1.a < 5000) s;

-- parallel index only scan (with limit)
explain (verbose, costs off) select a, b from pios1 where a < 1000 limit 10;
select count(*) from (select a, b from pios1 where a < 1000 limit 10) s;

-- parallel index only scan (with order and limit)
explain (verbose, costs off) select a, b from pios1 where a < 100000 order by a limit 10;
select a, b from pios1 where a < 100000 order by a limit 10;

-- Subquery with LIMIT and ORDER BY should NOT use parallel scan
explain (verbose, costs off) select a, b from pios1 where a in (select d from pios2 order by d limit 10);
select * from (select a, b from pios1 where a in (select d from pios2 order by d limit 10)) s order by a;

-- left join
explain (verbose, costs off) select pios1.a, pios1.b from pios1 left join pios2 on pios1.a = pios2.d where pios1.a < 1000;
select count(*) from (select pios1.a, pios1.b from pios1 left join pios2 on pios1.a = pios2.d where pios1.a < 1000) s;

-- right join
explain (verbose, costs off) select pios2.d, pios2.e from pios1 right join pios2 on pios1.a = pios2.d where pios2.d < 1000;
select count(*) from (select pios2.d, pios2.e from pios1 right join pios2 on pios1.a = pios2.d where pios2.d < 1000) s;

-- semi join with parallel index only scan
explain (verbose, costs off) select a, b from pios1 where exists (select 1 from pios2 where pios2.d = pios1.a);
select count(*) from (select a, b from pios1 where exists (select 1 from pios2 where pios2.d = pios1.a)) s;

-- anti semi join with parallel index only scan
explain (verbose, costs off) select a, b from pios1 where a not in (select d from pios2 where d < 1000);
select count(*) from (select a, b from pios1 where a not in (select d from pios2 where d < 1000)) s;

-- parallel index only scan with hash aggregation
explain (verbose, costs off) select a, count(*) from pios1 where a < 1000 group by a;
select count(*) from (select a, count(*) from pios1 where a < 1000 group by a) s;

-- Nested subquery with LIMIT - inner limit should prevent parallel index only scan
explain (costs off)
select a, b from pios1 where a in (
    select d from pios2 where d in (
        select a from pios1 limit 5
    )
);

-- Verify result correctness: parallel vs non-parallel should match
set enable_parallel = off;
select count(*), sum(a), sum(b) from pios1 where a < 100;
set enable_parallel = on;
select count(*), sum(a), sum(b) from pios1 where a < 100;

-- GUC to control plan
set optimizer_enable_indexonlyscan = off;
explain (verbose, costs off) select a, b from pios1 where a < 1000;
reset optimizer_enable_indexonlyscan;

reset enable_bitmapscan;
reset enable_seqscan;
reset enable_indexscan;

-- parallel bitmap heap scan test
create table pbms1(a int, b int, c text) with (parallel_workers=2) distributed by (a);
create table pbms2(d int, e int, f text) with (parallel_workers=2) distributed by (d);
create table pbms3(g int, h int, i text) with (parallel_workers=2) distributed by (g);

create index pbms1_a_idx on pbms1(a);
create index pbms1_b_idx on pbms1(b);
create index pbms2_d_idx on pbms2(d);
create index pbms2_e_idx on pbms2(e);
create index pbms3_g_idx on pbms3(g);
create index pbms3_h_idx on pbms3(h);

insert into pbms1 select i, i % 500, 'val_'||i from generate_series(1, 10000) i;
insert into pbms2 select i, i % 500, 'val_'||i from generate_series(1, 200000) i;
insert into pbms3 select i, i % 500, 'val_'||i from generate_series(1, 300) i;

analyze pbms1;
analyze pbms2;
analyze pbms3;

set optimizer_enable_bitmapscan to on;
set optimizer_enable_tablescan to off;
set optimizer_enable_indexscan to off;
set enable_bitmapscan to on;
set enable_seqscan to off;
set enable_indexscan to off;

-- parallel bitmap heap scan (simple, OR predicate on dist key)
explain (verbose, costs off) select * from pbms1 where a = 100 or a = 200;
select count(*) from pbms1 where a = 100 or a = 200;

-- parallel bitmap heap scan (simple, OR predicate on non-dist key)
explain (verbose, costs off) select * from pbms1 where b = 10 or b = 20;
select count(*) from pbms1 where b = 10 or b = 20;

-- parallel bitmap heap scan (multiple OR conditions)
explain (verbose, costs off) select * from pbms1 where b = 1 or b = 2 or b = 3;
select count(*) from pbms1 where b = 1 or b = 2 or b = 3;

-- parallel bitmap heap scan (IN list)
explain (verbose, costs off) select * from pbms2 where e in (10, 20, 30, 40, 50);
select count(*) from pbms2 where e in (10, 20, 30, 40, 50);

-- parallel bitmap heap scan (no redistribute motion, join on dist key)
explain (verbose, costs off) select * from pbms1 left join pbms2 on pbms1.a = pbms2.d where pbms1.a = 100 or pbms1.a = 200;
select count(*) from pbms1 left join pbms2 on pbms1.a = pbms2.d where pbms1.a = 100 or pbms1.a = 200;

-- parallel bitmap heap scan (redistribute motion, join on non-dist key)
explain (verbose, costs off) select * from pbms1 left join pbms2 on pbms1.b = pbms2.d where pbms1.b = 10 or pbms1.b = 20;
select count(*) from pbms1 left join pbms2 on pbms1.b = pbms2.d where pbms1.b = 10 or pbms1.b = 20;

-- parallel bitmap heap scan (broadcast motion, join with small table)
explain (verbose, costs off) select * from pbms1 join pbms3 on pbms1.a = pbms3.h where pbms1.b = 10 or pbms1.b = 20;
select count(*) from pbms1 join pbms3 on pbms1.a = pbms3.h where pbms1.b = 10 or pbms1.b = 20;

-- parallel bitmap heap scan (with limit)
explain (verbose, costs off) select * from pbms1 where b = 10 or b = 20 limit 10;
select count(*) from (select * from pbms1 where b = 10 or b = 20 limit 10) s;

-- parallel bitmap heap scan (with order and limit)
explain (verbose, costs off) select * from pbms1 where b = 10 or b = 20 order by a limit 10;
select * from pbms1 where b = 10 or b = 20 order by a limit 10;

-- Subquery with LIMIT and ORDER BY should NOT use parallel scan
explain (verbose, costs off) select * from pbms1 where a in (select d from pbms2 order by d limit 10);
select * from (select * from pbms1 where a in (select d from pbms2 order by d limit 10)) s order by a;

-- left join
explain (verbose, costs off) select * from pbms1 left join pbms2 on pbms1.a = pbms2.d where pbms1.b = 10 or pbms1.b = 20;
select count(*) from pbms1 left join pbms2 on pbms1.a = pbms2.d where pbms1.b = 10 or pbms1.b = 20;

-- right join
explain (verbose, costs off) select * from pbms1 right join pbms2 on pbms1.a = pbms2.d where pbms2.e = 10 or pbms2.e = 20;
select count(*) from pbms1 right join pbms2 on pbms1.a = pbms2.d where pbms2.e = 10 or pbms2.e = 20;

-- anti semi join with parallel bitmap heap scan
explain (verbose, costs off) select * from pbms1 where a not in (select d from pbms2 where d < 1000) and (b = 10 or b = 20);
select count(*) from pbms1 where a not in (select d from pbms2 where d < 1000) and (b = 10 or b = 20);

-- parallel bitmap heap scan with hash aggregation
explain (verbose, costs off) select b, count(*) from pbms1 where b = 10 or b = 20 group by b;
select b, count(*) from pbms1 where b = 10 or b = 20 group by b;

-- Nested subquery with LIMIT - inner limit should prevent parallel bitmap scan
explain (costs off)
select * from pbms1 where a in (
    select d from pbms2 where d in (
        select a from pbms1 limit 5
    )
) and (b = 10 or b = 20);

-- BitmapAnd: conditions on different indexes
explain (verbose, costs off) select * from pbms2 where d < 1000 and e < 50;
select count(*) from pbms2 where d < 1000 and e < 50;

-- Parallel bitmap scan execution correctness (isshared fix)
-- Verifies that BitmapOr/BitmapAnd/BitmapIndexScan nodes have isshared=true
-- so the TID bitmap is allocated in DSA shared memory for parallel workers.
-- Without the fix, these queries hit: FailedAssertion("tbm->dsa != NULL")
-- BitmapOr: two OR conditions on the same index
select * from pbms1 where a = 100 or a = 200;
-- BitmapOr: three OR conditions (nested BitmapOr tree)
select * from pbms1 where b = 1 or b = 2 or b = 3 order by a;
-- parallel bitmap scan with join (exercises isshared across motion nodes)
select count(*) from pbms1 join pbms2 on pbms1.a = pbms2.d where pbms1.b = 10 or pbms1.b = 20;

-- Verify result correctness: parallel vs non-parallel should match
set enable_parallel = off;
select count(*), sum(a), sum(b) from pbms1 where b = 10 or b = 20;
set enable_parallel = on;
select count(*), sum(a), sum(b) from pbms1 where b = 10 or b = 20;

-- GUC to control plan
set optimizer_enable_bitmapscan = off;
explain (verbose, costs off) select * from pbms1 where b = 10 or b = 20;
reset optimizer_enable_bitmapscan;
reset optimizer_enable_tablescan;
reset optimizer_enable_indexscan;
reset enable_bitmapscan;
reset enable_seqscan;
reset enable_indexscan;

-- semi join with parallel bitmap heap scan
-- Drop pbms2_d_idx so ORCA prefers Hash Semi Join over correlated NL + Index probe
drop index pbms2_d_idx;
explain (verbose, costs off) select * from pbms1 where exists (select 1 from pbms2 where pbms2.d = pbms1.a) and (b = 10 or b = 20);
select count(*) from pbms1 where exists (select 1 from pbms2 where pbms2.d = pbms1.a) and (b = 10 or b = 20);

reset enable_parallel;
reset max_parallel_workers_per_gather;
reset parallel_setup_cost;
reset statement_mem;

-- start_ignore
drop schema orca_index_parallel cascade;
-- end_ignore
