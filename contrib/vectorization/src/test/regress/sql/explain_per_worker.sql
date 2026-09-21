-- Test per-worker EXPLAIN ANALYZE stats: the existing gp_enable_explain_allstat
-- GUC, combined with parallel execution, now drives a per-worker breakdown.
--
-- When gp_enable_explain_allstat is on AND a node's slice runs more than one
-- parallel worker per segment, the per-segment "allstat:" dump is replaced by a
-- per-worker "worker stats:" block, with one "segN wM:" line per parallel worker
-- (rows/time and the two per-slice memory figures), so skew across a segment's
-- parallel workers is visible.  Non-parallel slices keep the classic allstat
-- dump.  (Assumes the standard 3-segment demo cluster, so a parallel node
-- reports 6 worker lines.)
--
-- The per-worker numbers (rows/time/memory) and the standard ANALYZE figures
-- are inherently non-deterministic, so they are masked below.
--
-- start_matchsubs
-- m/ rows=\d+ first=\S+ ms total=\S+ ms exec=\d+K bytes work=\d+K bytes/
-- s/ rows=\d+ first=\S+ ms total=\S+ ms exec=\d+K bytes work=\d+K bytes/ rows=# first=# total=# exec=#K work=#K/
-- m/\(actual time=[^)]*\)/
-- s/\(actual time=[^)]*\)/(actual time=...)/
-- m/\(actual rows=[^)]*\)/
-- s/\(actual rows=[^)]*\)/(actual rows=...)/
-- m/Planning Time: .*/
-- s/Planning Time: .*/Planning Time: ### ms/
-- m/Execution Time: .*/
-- s/Execution Time: .*/Execution Time: ### ms/
-- m/Executor memory: .*/
-- s/Executor memory: .*/Executor memory: ###/
-- m/Memory used: .*/
-- s/Memory used: .*/Memory used: ### kB/
-- m/Memory wanted: .*/
-- s/Memory wanted: .*/Memory wanted: ### kB/
-- end_matchsubs

set enable_parallel = on;
set min_parallel_table_scan_size = 0;

create table epw_t (a int, b int) with (parallel_workers = 2) distributed by (a);
insert into epw_t select i, i % 100 from generate_series(1, 200000) i;
analyze epw_t;

-- The GUC is USERSET and visible.
show gp_enable_explain_allstat;

-- 1. GUC off (default): EXPLAIN ANALYZE has no per-worker "worker stats:" block.
set gp_enable_explain_allstat = off;
explain (analyze, costs off) select count(*) from epw_t;

-- 2. GUC on with a parallel slice (parallel_workers > 1): each parallel node
--    gets a "worker stats:" block with one line per worker (seg0 w0, seg0 w1,
--    seg1 w0, ...), reporting rows/time and the two per-slice memory figures
--    (exec = Executor memory, work = Work_mem).
set gp_enable_explain_allstat = on;
explain (analyze, costs off) select count(*) from epw_t;

set gp_enable_explain_allstat = off;
drop table epw_t;
reset min_parallel_table_scan_size;
reset enable_parallel;
