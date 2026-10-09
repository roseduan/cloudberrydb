-- AO / AOCO / PAX: a reader gang must inherit the writer's subtransaction state.
--
-- Companion to xmax_reader_subxact.sql, which covers plain heap tables.  The
-- same defect damages append-optimized and PAX tables, even though their data
-- files carry no per-row xmin/xmax at all: their metadata lives in ordinary
-- heap AUXILIARY tables, and the scan path hands the *query* snapshot straight
-- to the metadata reads --
--
--   appendonly_beginscan()   appendonlyam.c   appendOnlyMetaDataSnapshot = snapshot
--   aocs_beginscan()         aocsam.c         same
--   PaxScanDesc::BeginScan() pax_scanner.cc   aux_snapshot = snapshot
--
-- so a reader gang carrying the defective snapshot stamps the very same
-- permanent HEAP_XMAX_INVALID / HEAP_XMIN_INVALID on aux tuples.  The blast
-- radius is larger than for heap: one aoseg tuple stands for a whole segment
-- file and one PAX aux tuple for a whole micro-partition list, so losing or
-- resurrecting a single aux row moves many user rows at once.
--
-- Which heap relations of an AO/PAX table are exposed:
--
--   pg_aoseg, pg_aocsseg   YES  rewritten by every insert; read by every scan
--   pg_aovisimap           YES  rewritten by every delete/update; read by
--                               every scan
--   pg_aoblkdir            YES  rewritten as rows are appended; read by index
--                               and bitmap scans only
--   PAX pg_manifest_* /
--       pg_pax_blocks_*    YES  rewritten by every DML; read by every scan
--   pg_toast_*             no   detoasting uses SnapshotToast;
--                               HeapTupleSatisfiesToast never consults a
--                               snapshot and sets no hint bits outside the
--                               dead pre-9.0 HEAP_MOVED branches
--   pg_bitmapindex.pg_bm_* no   LOV tuples are inserted with
--                               FrozenTransactionId on purpose, so their
--                               visibility never asks the snapshot anything
--   gp_fastsequence,
--   pg_pax_fastsequence    no   heap_inplace_update -- no version chain at
--                               all -- and only writers ever read them
--
-- Every scenario needs the same three ingredients as the heap test:
--   a. the reader gang is warmed up while NO subtransaction exists anywhere,
--      so the reader QE's process-static snapshot keeps subxcnt = 0;
--   b. the DML runs in a subtransaction, so its xid can only reach a snapshot
--      through subxip[];
--   c. another transaction commits afterwards, so the subxid falls strictly
--      inside (snap_xmin, snap_xmax).
--
-- Every listing carries the table name as its first column, and each scenario
-- uses its own pad prefix.  That is not decoration: regression.diffs is a
-- "diff -U3", so the "NN: select ... from <table>" line often falls outside the
-- hunk, and several scenarios would otherwise produce rows that look exactly
-- alike -- which is how a PAX failure once got read as an AO one.
--
-- Each scenario lists the whole table before the trigger and again after it.
-- The tables deliberately hold a handful of rows: the damage is meant to be
-- read straight off the two listings -- rows back from the dead, rows gone
-- missing -- not inferred from a verdict function.  Where the aux relation
-- itself is the point the scenario also prints it, through the gp_toolkit
-- views.  No helper functions and no pageinspect: per-tuple forensics belong
-- in the repair tooling, not in a regression test.
--
-- Running this by hand (three psql terminals instead of this driver)
-- additionally needs "set gp_vmem_idle_resource_timeout = 0" in the reader
-- session, otherwise the warmed-up reader gang is recycled before the trigger
-- step reuses it.

-- ----------------------------------------------------------------------------
-- 1. AO_ROW, delete side: the visimap entry for the deleted row is lost, so the
--    deleted row stays visible next to the row that replaced it.
--
--    This is exactly the choreography people run by hand in three terminals,
--    with the table switched from heap to appendonly.
-- ----------------------------------------------------------------------------
create table xmax_ao_del (id int, pad text) with (appendonly=true, orientation=row) distributed by (id);
create table xmax_ao_del_bump (id int) distributed by (id);
insert into xmax_ao_del select i, 'orig-' || i from generate_series(1, 8) i;

-- before: eight rows, id 1 reads 'orig-1'
select 'ao_del' as t, id, pad from xmax_ao_del order by id, pad;

-- warm the reader gang up while no subtransaction exists
1: begin;
1: declare c0 cursor for select id from xmax_ao_del order by id;
1: fetch 3 from c0;
1: close c0;
1: commit;

-- delete inside a subtransaction, re-insert the key at the top level, hold open
2: begin;
2: savepoint s1;
2: delete from xmax_ao_del where id = 1;
2: release savepoint s1;
2: insert into xmax_ao_del values (1, 'new-1');

-- commit a write on every segment so the subxid lands inside the range
3: insert into xmax_ao_del_bump select generate_series(1, 30);

-- the trigger: same reader gang reads the aux with the defective snapshot
1: begin;
1: declare c1 cursor for select id, pad from xmax_ao_del order by id;
1: fetch 3 from c1;
1: close c1;
1: commit;

2: commit;

-- after: still eight rows, and id 1 now reads 'new-1'.  A broken build brings
-- 'orig-1' back from the dead next to 'new-1'.
1: select 'ao_del' as t, id, pad from xmax_ao_del order by id, pad;

1q:
2q:
3q:

-- ----------------------------------------------------------------------------
-- 2. AO_ROW, insert side: the new pg_aoseg row is stamped HEAP_XMIN_INVALID and
--    the old one resurrected, so eof rolls back to its pre-insert value and the
--    committed rows become invisible.  Nothing is duplicated here -- the data is
--    simply gone, and the next insert into that segment file would append from
--    the stale eof and overwrite it.
-- ----------------------------------------------------------------------------
create table xmax_ao_ins (id int, pad text) with (appendonly=true, orientation=row) distributed by (id);
create table xmax_ao_ins_bump (id int) distributed by (id);
insert into xmax_ao_ins select i, 'orig-' || i from generate_series(1, 8) i;

-- before: eight rows
select 'ao_ins' as t, id, pad from xmax_ao_ins order by id, pad;

4: begin;
4: declare c0 cursor for select id from xmax_ao_ins order by id;
4: fetch 3 from c0;
4: close c0;
4: commit;

5: begin;
5: savepoint s1;
5: insert into xmax_ao_ins select i, 'sub-' || i from generate_series(101, 103) i;
5: release savepoint s1;

6: insert into xmax_ao_ins_bump select generate_series(1, 30);

4: begin;
4: declare c1 cursor for select id, pad from xmax_ao_ins order by id;
4: fetch 3 from c1;
4: close c1;
4: commit;

5: commit;

-- after: eleven rows.  A broken build is back to the original eight -- the
-- three committed 'sub-' rows are invisible although the transaction committed.
4: select 'ao_ins' as t, id, pad from xmax_ao_ins order by id, pad;

4q:
5q:
6q:

-- ----------------------------------------------------------------------------
-- 3. AO_ROW, duplicate segno: one statement inside the subtransaction and one
--    at the top level.  The top-level xid travels in xip[] and is judged
--    correctly, so only the middle version is discarded -- the pre-insert row
--    is resurrected next to the final one and pg_aoseg holds two rows for the
--    same segno.  From then on INSERT fails outright:
--        ERROR: found two entries in "pg_aoseg_NNNN" with segno 1
--    and VACUUM fails with "file size smaller than logical eof".
-- ----------------------------------------------------------------------------
create table xmax_ao_dup (id int, pad text) with (appendonly=true, orientation=row) distributed by (id);
create table xmax_ao_dup_bump (id int) distributed by (id);
insert into xmax_ao_dup select i, 'orig-' || i from generate_series(1, 8) i;

-- before: eight rows, and exactly one pg_aoseg row per (segment, segno)
select 'ao_dup' as t, id, pad from xmax_ao_dup order by id, pad;
select segment_id, segno, tupcount from gp_toolkit.__gp_aoseg('xmax_ao_dup') order by 1, 2, 3;

7: begin;
7: declare c0 cursor for select id from xmax_ao_dup order by id;
7: fetch 3 from c0;
7: close c0;
7: commit;

8: begin;
8: savepoint s1;
8: insert into xmax_ao_dup select i, 'sub-' || i from generate_series(101, 103) i;
8: release savepoint s1;
8: insert into xmax_ao_dup select i, 'top-' || i from generate_series(201, 203) i;

9: insert into xmax_ao_dup_bump select generate_series(1, 30);

7: begin;
7: declare c1 cursor for select id, pad from xmax_ao_dup order by id;
7: fetch 3 from c1;
7: close c1;
7: commit;

8: commit;

-- after: fourteen rows, and still one pg_aoseg row per (segment, segno).
-- A broken build shows the 'orig-' rows twice and two pg_aoseg rows for the
-- same segno, the second one with the stale tupcount.
7: select 'ao_dup' as t, id, pad from xmax_ao_dup order by id, pad;
7: select segment_id, segno, tupcount from gp_toolkit.__gp_aoseg('xmax_ao_dup') order by 1, 2, 3;
-- the table must still accept writes: this is what a duplicate segno breaks.
-- Ten rows, so the probe reaches every segment -- only the segment holding the
-- duplicate raises, and a single-row probe could miss it.
7: insert into xmax_ao_dup select i, 'probe' from generate_series(9001, 9010) i;
7: delete from xmax_ao_dup where pad = 'probe';

7q:
8q:
9q:

-- ----------------------------------------------------------------------------
-- 4. AO_COLUMN: same mechanism, pg_aocsseg instead of pg_aoseg
-- ----------------------------------------------------------------------------
create table xmax_aoco_ins (id int, pad text) with (appendonly=true, orientation=column) distributed by (id);
create table xmax_aoco_ins_bump (id int) distributed by (id);
insert into xmax_aoco_ins select i, 'orig-' || i from generate_series(1, 8) i;

select 'aoco_ins' as t, id, pad from xmax_aoco_ins order by id, pad;

10: begin;
10: declare c0 cursor for select id from xmax_aoco_ins order by id;
10: fetch 3 from c0;
10: close c0;
10: commit;

11: begin;
11: savepoint s1;
11: insert into xmax_aoco_ins select i, 'sub-' || i from generate_series(101, 103) i;
11: release savepoint s1;

12: insert into xmax_aoco_ins_bump select generate_series(1, 30);

10: begin;
10: declare c1 cursor for select id, pad from xmax_aoco_ins order by id;
10: fetch 3 from c1;
10: close c1;
10: commit;

11: commit;

-- after: eleven rows; a broken build is back to eight
10: select 'aoco_ins' as t, id, pad from xmax_aoco_ins order by id, pad;

10q:
11q:
12q:

-- ----------------------------------------------------------------------------
-- 5. PAX, insert side.  PAX keeps its metadata in pg_ext_aux, and which
--    relation that is depends on how the module was built:
--
--      USE_MANIFEST_API=ON    pg_manifest_<oid>    one row per segment, naming
--                             the manifest file currently in force; every DML
--                             rewrites it
--      USE_PAX_CATALOG only   pg_pax_blocks_<oid>  one row per micro-partition,
--                             keyed by ptblockname; every DML inserts or
--                             rewrites one
--
--    Both are ordinary heap relations read with the query snapshot, so both
--    take the damage -- only the shape differs.  Under the manifest layout the
--    row is UPDATEd, so the pre-insert version is resurrected and the new one
--    discarded; under the catalog layout the new block's row is a fresh INSERT
--    that gets stamped HEAP_XMIN_INVALID.  Either way the rows written by the
--    subtransaction become invisible although it committed.
--
--    The checks below are plain row listings on purpose: they read the same in
--    both builds, so one expected file covers both.
-- ----------------------------------------------------------------------------
create table xmax_pax_ins (id int, pad text) using pax distributed by (id);
create table xmax_pax_ins_bump (id int) distributed by (id);
insert into xmax_pax_ins select i, 'orig-' || i from generate_series(1, 8) i;

select 'pax_ins' as t, id, pad from xmax_pax_ins order by id, pad;

13: begin;
13: declare c0 cursor for select id from xmax_pax_ins order by id;
13: fetch 3 from c0;
13: close c0;
13: commit;

14: begin;
14: savepoint s1;
14: insert into xmax_pax_ins select i, 'sub-' || i from generate_series(101, 103) i;
14: release savepoint s1;

15: insert into xmax_pax_ins_bump select generate_series(1, 30);

13: begin;
13: declare c1 cursor for select id, pad from xmax_pax_ins order by id;
13: fetch 3 from c1;
13: close c1;
13: commit;

14: commit;

-- after: eleven rows; a broken build is back to eight
13: select 'pax_ins' as t, id, pad from xmax_pax_ins order by id, pad;

13q:
14q:
15q:

-- ----------------------------------------------------------------------------
-- 5b. PAX, delete side.  A DELETE writes a visibility map file and rewrites the
--     aux row that names it, so the aux row is touched by deletes just as it is
--     by inserts -- and the reader undoes a committed DELETE instead of hiding
--     a committed INSERT.
--
--     Worth having as its own scenario rather than trusting 5 to cover PAX: the
--     two sides go through different aux code paths, and which one a given PAX
--     build damages first is not something the test should have to know.
-- ----------------------------------------------------------------------------
create table xmax_pax_del (id int, pad text) using pax distributed by (id);
create table xmax_pax_del_bump (id int) distributed by (id);
insert into xmax_pax_del select i, 'pxd-' || i from generate_series(1, 8) i;

-- before: eight rows
select 'pax_del' as t, id, pad from xmax_pax_del order by id;

25: begin;
25: declare c0 cursor for select id from xmax_pax_del order by id;
25: fetch 3 from c0;
25: close c0;
25: commit;

26: begin;
26: savepoint s1;
26: delete from xmax_pax_del where id = 5;
26: release savepoint s1;

27: insert into xmax_pax_del_bump select generate_series(1, 30);

25: begin;
25: declare c1 cursor for select 'pax_del' as t, id, pad from xmax_pax_del order by id;
25: fetch 3 from c1;
25: close c1;
25: commit;

26: commit;

-- after: seven rows, id 5 gone for good.  A broken build has it back from the
-- dead -- a committed DELETE undone by a reader that never wrote anything.
25: select 'pax_del' as t, id, pad from xmax_pax_del order by id;

25q:
26q:
27q:

-- ----------------------------------------------------------------------------
-- 6. pg_aovisimap: a committed DELETE silently comes undone.
--
--    The visimap row is rewritten by every delete.  The reader resurrects the
--    pre-delete version, so the row that was deleted is visible again -- and
--    because the update is HOT (only the bitmap column changes, and the index
--    is on segno/first_row_no) the two versions live in one HOT chain.
-- ----------------------------------------------------------------------------
create table xmax_ao_vm (id int, pad text) with (appendonly=true, orientation=row) distributed by (id);
create table xmax_ao_vm_bump (id int) distributed by (id);
insert into xmax_ao_vm select i, 'vm-' || i from generate_series(1, 8) i;
-- a committed delete first, so the visimap row already exists and the
-- subtransaction below UPDATEs it instead of inserting a fresh one
delete from xmax_ao_vm where id = 3;

-- before: seven rows, id 3 already gone, one row hidden by the visimap
select 'ao_vm' as t, id, pad from xmax_ao_vm order by id;
select sum(hidden_tupcount) as hidden, sum(total_tupcount) as total from gp_toolkit.__gp_aovisimap_hidden_info('xmax_ao_vm');

16: begin;
16: declare c0 cursor for select id from xmax_ao_vm order by id;
16: fetch 3 from c0;
16: close c0;
16: commit;

17: begin;
17: savepoint s1;
17: delete from xmax_ao_vm where id = 5;
17: release savepoint s1;

18: insert into xmax_ao_vm_bump select generate_series(1, 30);

16: begin;
16: declare c1 cursor for select 'ao_vm' as t, id, pad from xmax_ao_vm order by id;
16: fetch 3 from c1;
16: close c1;
16: commit;

17: commit;

-- after: six rows, neither 3 nor 5, and two rows hidden.  A broken build has
-- id 5 back from the dead and only one hidden row -- the committed DELETE was
-- undone by a reader that never wrote anything.
16: select 'ao_vm' as t, id, pad from xmax_ao_vm order by id;
16: select sum(hidden_tupcount) as hidden, sum(total_tupcount) as total from gp_toolkit.__gp_aovisimap_hidden_info('xmax_ao_vm');

16q:
17q:
18q:

-- ----------------------------------------------------------------------------
-- 7. pg_aoblkdir: only an index scan reads it, so the reader must take the
--    index path.  Damage there means the index and a sequential scan disagree
--    about the same rows.
--
--    The blkdir minipage row is rewritten as rows are appended, and that update
--    is HOT as well (the key is segno/columngroup_no/first_row_no, only the
--    minipage column changes).  Note the aoseg row is damaged in the same
--    breath -- every AO insert touches it -- so both listings below move.
-- ----------------------------------------------------------------------------
create table xmax_ao_bd (id int, pad text) with (appendonly=true, orientation=row) distributed by (id);
create table xmax_ao_bd_bump (id int) distributed by (id);
insert into xmax_ao_bd select i, 'bd-' || i from generate_series(1, 8) i;
create index xmax_ao_bd_idx on xmax_ao_bd(id);

select 'ao_bd' as t, id, pad from xmax_ao_bd order by id;

19: set optimizer = off;
19: set enable_seqscan = off;
19: begin;
19: declare c0 cursor for select id from xmax_ao_bd where id between 1 and 5 order by id;
19: fetch 3 from c0;
19: close c0;
19: commit;

20: begin;
20: savepoint s1;
20: insert into xmax_ao_bd select i, 'sub-' || i from generate_series(101, 103) i;
20: release savepoint s1;

21: insert into xmax_ao_bd_bump select generate_series(1, 30);

19: begin;
19: declare c1 cursor for select 'ao_bd' as t, id, pad from xmax_ao_bd where id between 101 and 103 order by id;
19: fetch 3 from c1;
19: close c1;
19: commit;

20: commit;

-- the three paths must agree, row for row: index scan, sequential scan, and
-- the unrestricted listing.  A broken build loses the three 'sub-' rows from
-- all of them, and on a half-damaged blkdir the first two disagree.
19: select 'ao_bd' as t, id, pad from xmax_ao_bd where id between 101 and 103 order by id;
19: set enable_seqscan = on;
19: set enable_indexscan = off;
19: set enable_bitmapscan = off;
19: set enable_indexonlyscan = off;
19: select 'ao_bd' as t, id, pad from xmax_ao_bd where id between 101 and 103 order by id;
19: select 'ao_bd' as t, id, pad from xmax_ao_bd order by id;

19q:
20q:
21q:

-- ----------------------------------------------------------------------------
-- 8. The same run also exercises the two heap relations that are NOT exposed,
--    so the claim is measured rather than argued:
--
--    pg_toast_*        detoasting uses SnapshotToast; HeapTupleSatisfiesToast
--                      never consults a snapshot and never sets a hint bit
--                      outside the dead pre-9.0 HEAP_MOVED branches
--                      (heapam_visibility.c), so no reader can mislabel a
--                      toast chunk -- every row that is visible at all must
--                      still detoast to exactly the bytes that were written
--    pg_bm_* (LOV)     bitmap index LOV tuples are inserted with
--                      FrozenTransactionId on purpose -- "so that all tuples
--                      are always visible to any transactions"
--                      (bitmapinsert.c, create_lovitem path) -- so the bitmap
--                      index and a sequential scan must return the same rows
--
--    "big" is declared EXTERNAL on purpose: a compressible value never leaves
--    the main relation, and a toast relation that stays empty would make the
--    check above vacuous.
-- ----------------------------------------------------------------------------
create table xmax_ao_neg (id int, cat text, big text) with (appendonly=true, orientation=row) distributed by (id);
alter table xmax_ao_neg alter column big set storage external;
create table xmax_ao_neg_bump (id int) distributed by (id);
insert into xmax_ao_neg select i, 'old', repeat(md5(i::text), 400) from generate_series(1, 8) i;
create index xmax_ao_neg_bmi on xmax_ao_neg using bitmap (cat);

-- the toast relation must really be in use, otherwise the check below proves
-- nothing
select pg_relation_size(reltoastrelid) > 0 as toast_in_use from pg_class where oid = 'xmax_ao_neg'::regclass;
select 'ao_neg' as t, id, cat, length(big), md5(big) = md5(repeat(md5(id::text), 400)) as big_intact from xmax_ao_neg order by id;

22: set optimizer = off;
22: set enable_seqscan = off;
22: begin;
22: declare c0 cursor for select id, length(big) from xmax_ao_neg where cat = 'old' order by id;
22: fetch 3 from c0;
22: close c0;
22: commit;

23: begin;
23: savepoint s1;
23: insert into xmax_ao_neg select i, 'new', repeat(md5(i::text), 400) from generate_series(101, 103) i;
23: release savepoint s1;

24: insert into xmax_ao_neg_bump select generate_series(1, 30);

22: begin;
22: declare c1 cursor for select 'ao_neg' as t, id, cat, length(big) from xmax_ao_neg where cat = 'new' order by id;
22: fetch 3 from c1;
22: close c1;
22: commit;

23: commit;

-- the exposed relation moved: a broken build loses the three 'new' rows here
22: select 'ao_neg' as t, id, cat, length(big) from xmax_ao_neg where cat = 'new' order by id;
22: set enable_seqscan = on;
22: set enable_indexscan = off;
22: set enable_bitmapscan = off;
22: set enable_indexonlyscan = off;
22: select 'ao_neg' as t, id, cat, length(big) from xmax_ao_neg where cat = 'new' order by id;
-- ... but every row that is visible at all still detoasts to the exact bytes
-- that were written: big_intact is t on every row, broken build included
22: select 'ao_neg' as t, id, cat, length(big), md5(big) = md5(repeat(md5(id::text), 400)) as big_intact from xmax_ao_neg order by id;

22q:
23q:
24q:
