-- Acceptance test for GitLab issue #432 (Gopher file-meta cache).
--
-- Read this before touching the file:
--
--  * THE FEATURE IS OFF BY DEFAULT (`datalake.enable_file_meta_cache`
--    boots to false), so every group below turns it on explicitly and
--    none of them depends on the default.  Two reasons it ships off, both
--    spelled out next to the boot value in datalake_fdw.c: a datalake_fdw
--    upgraded ahead of gopher_store hangs a backend permanently on the
--    first file open (the old server never answers the new message and
--    the client's receive has no timeout), and nothing enforces that
--    order; and the saving is still unmeasured -- the cache demonstrably
--    hits, but a blob's lifetime is tied to its File and eviction walks
--    Files, so no run yet shows fewer object-storage requests with it on.
--    UPGRADE ORDER, if you do turn it on: gopher_store FIRST.
--
--  * `enableCache` (a per-FOREIGN-TABLE option) and
--    `datalake.enable_file_meta_cache` (a GUC) are two INDEPENDENT
--    switches. `enableCache` toggles Gopher's *pre-existing* data-block
--    cache (O_RDONLY vs O_RDTHR); the GUC toggles *this* feature (the
--    file metadata cache added by #432 -- ORC tails and Parquet footers).
--    Every other ORC and Parquet case in this directory passes
--    `enableCache 'false'`, so none of them has ever exercised this
--    feature's code path -- this file is the first to run with
--    `datalake.enable_file_meta_cache` on *and* `enableCache 'true'`.
--    (Verified empirically: with `enableCache 'false'` the tail-cache
--    miss/backfill still fires, because it is gated solely by the GUC.)
--
--  * Groups A and "full-prune" cover ORC; Group P covers Parquet. Parquet
--    is not a copy of the ORC groups: the cached range is computed
--    differently there (at least the last 64 KB rather than the exact
--    tail), so Group P exercises both size branches on purpose. See its
--    own header.
--
--  * SCOPE: every table here is a BARE EXTERNAL TABLE (filePath +
--    format). That is the whole of what this feature covers, and the
--    coverage gap is deliberate rather than accidental: Iceberg and Hudi
--    reach Parquet through a SECOND, independent reader stack
--    (provider/common/parquet_reader.cpp over DatalakeRandomAccessFile,
--    via datalakeRowReaderNext -> icebergTaskReaderNext), which is not
--    wired into the cache. Adding an Iceberg case here would pass while
--    proving nothing: the assertions are correctness-only, and an
--    Iceberg scan with the cache doing nothing returns exactly the same
--    rows. If the second stack is ever wired in, the case to add is a
--    DEBUG1 hit/miss check, not another row count.
--    ORC has no such split -- file_reader.c's orcReader is all NULL
--    function pointers, so Iceberg/Hudi cannot read ORC at all, and
--    every ORC read goes through the adapter these groups exercise.
--
--  * This file only asserts *correctness* -- identical row counts across
--    the GUC's on/off/cold/warm states, and that full-stripe pruning still
--    returns the right (empty) answer once the tail is cached. That is
--    what pg_regress's exact-diff model is good at, and it is exactly the
--    "no-regression" half of Task 16.
--
--    It does NOT assert hit rate or remote-traffic savings ("benefit"): those
--    are quantitative and environment-dependent (HDFS DataNode byte
--    counters, Gopher's own block-cache state), so they were verified by
--    hand for this task with a dedicated multi-GB dataset and a lowered
--    `pg_gophermeta.gopher_local_capacity_mb` -- see task-16-report.md for
--    the actual before/after numbers and the DEBUG1 hit/miss/backfill
--    evidence. Embedding that experiment here would mean a GB-scale
--    fixture and a cluster-wide cache-capacity mutation on every routine
--    CI run, for a shared dev cluster other suites also use -- not a
--    trade worth making for a benefit that is fundamentally a
--    measurement, not a pass/fail. The manual reproduction recipe is
--    inlined as a comment at the end of this file.
--
--  * Known limitation: datalake_fdw's writable HDFS/ORC external tables
--    have no TRUNCATE/overwrite support (verified: re-running the writer
--    INSERTs against an already-populated path appends a new, distinctly
--    -named file rather than replacing it, silently doubling every row
--    count below). A fresh REGRESS/CI environment never hits this since
--    the target path does not exist yet; re-running this file by hand
--    against a long-lived dev cluster requires clearing
--    /filemeta-432-regress/ on the HDFS side first. (This is also why the
--    file does not lead with its own `CREATE EXTENSION IF NOT EXISTS
--    datalake_fdw` -- matching the rest of this directory's convention of
--    relying on setup.sql having already done it, so this file's expected
--    output does not have to special-case "first ever run" vs. "run after
--    setup".)

-- Deliberately named to avoid colliding with any other server in this
-- REGRESS run or in a shared ad hoc dev cluster.
CREATE SERVER file_meta_cache_hdfs_server
        FOREIGN DATA WRAPPER datalake_fdw
        OPTIONS (protocol 'hdfs', hdfs_namenodes 'hadoop', hdfs_port '8020',
                 hdfs_auth_method 'simple', hadoop_rpc_protection 'authentication');
CREATE USER MAPPING FOR gpadmin SERVER file_meta_cache_hdfs_server OPTIONS (user 'gpadmin');

-- ============================================================
-- Group A: a table that comfortably fits any realistic block-cache
-- capacity. Self-seeded so the test does not depend on a pre-loaded CI
-- fixture.
-- ============================================================
DROP FOREIGN TABLE IF EXISTS filemeta_small_w;
CREATE FOREIGN TABLE filemeta_small_w (id int, pad text)
        SERVER file_meta_cache_hdfs_server
        OPTIONS (filePath '/filemeta-432-regress/small/', format 'orc', compression 'none');
INSERT INTO filemeta_small_w SELECT i, lpad(i::text, 200, '0') FROM generate_series(1, 20000) i;

DROP FOREIGN TABLE IF EXISTS filemeta_small_r;
CREATE FOREIGN TABLE filemeta_small_r (id int, pad text)
        SERVER file_meta_cache_hdfs_server
        OPTIONS (filePath '/filemeta-432-regress/small/', format 'orc', enableCache 'true');

-- 1) Feature fully off: correctness must hold even when the metadata cache
--    is never consulted.
SET datalake.enable_file_meta_cache = off;
SELECT count(*) FROM filemeta_small_r;

-- 2) Feature on, first touch (cold: a tail-cache miss + backfill happens
--    under the hood).
SET datalake.enable_file_meta_cache = on;
SELECT count(*) FROM filemeta_small_r;

-- 3) Feature on, second touch (warm: should hit the cache). The answer
--    must be identical to the cold read -- the cache must never change it.
SELECT count(*) FROM filemeta_small_r;

-- Toggling the GUC back off mid-session must not corrupt anything already
-- cached, nor break the next read.
SET datalake.enable_file_meta_cache = off;
SELECT count(*) FROM filemeta_small_r;
SET datalake.enable_file_meta_cache = on;

-- ============================================================
-- Full-prune scenario: every stripe is excluded by min/max stats, so a
-- correct answer is empty regardless of whether the tail is cached. This
-- table needs more than one stripe to be a meaningful check (the default
-- ORC stripe size is 64MB), hence the bigger row count -- still small
-- enough to be a routine regression case.
-- ============================================================
DROP FOREIGN TABLE IF EXISTS filemeta_pruned_w;
CREATE FOREIGN TABLE filemeta_pruned_w (id int, pad text)
        SERVER file_meta_cache_hdfs_server
        OPTIONS (filePath '/filemeta-432-regress/pruned/', format 'orc', compression 'none');
INSERT INTO filemeta_pruned_w SELECT i, lpad(i::text, 1000, '0') FROM generate_series(1, 210000) i;

DROP FOREIGN TABLE IF EXISTS filemeta_pruned_r;
CREATE FOREIGN TABLE filemeta_pruned_r (id int, pad text)
        SERVER file_meta_cache_hdfs_server
        OPTIONS (filePath '/filemeta-432-regress/pruned/', format 'orc', enableCache 'true');

-- Cold: every stripe pruned by "id < -1" -- nothing to return.
SELECT count(*) FROM filemeta_pruned_r WHERE id < -1;
-- Warm: same predicate, must still return the same (empty) answer -- the
-- tail-cache hit must not change the pruning decision.
SELECT count(*) FROM filemeta_pruned_r WHERE id < -1;
-- Sanity: an unfiltered scan must still return every row -- pruning and
-- caching together must not silently drop data.
SELECT count(*) FROM filemeta_pruned_r;

-- ============================================================
-- Group P: Parquet.
--
-- Kept separate from the ORC groups because the cached RANGE is computed
-- differently, and that is where Parquet support can fail silently. ORC
-- caches its exact tail; Parquet must cache at least the last 64 KB,
-- because arrow always reads a footer as the last min(fileSize, 64 KB)
-- (it cannot know metadata_len before reading it) and the range cache
-- only answers a request that falls entirely inside the cached range.
-- Cache the precise footer instead and every request is a partial
-- overlap, i.e. a permanent miss with no error anywhere. Both size
-- branches below are deliberately exercised.
--
-- As with the ORC groups, this asserts CORRECTNESS ONLY: identical
-- answers across off/cold/warm. Hit rate is not assertable here; the
-- DEBUG1 "datalake file-meta-cache:" line is what to grep by hand.
-- ============================================================

-- P1: a file comfortably larger than 64 KB -- the ordinary branch, where
-- the cached range is exactly the 64 KB arrow asks for. 20000 rows of a
-- 200-byte pad with compression 'none' is ~4 MB, far above the floor.
DROP FOREIGN TABLE IF EXISTS filemeta_pq_w;
CREATE FOREIGN TABLE filemeta_pq_w (id int, pad text)
        SERVER file_meta_cache_hdfs_server
        OPTIONS (filePath '/filemeta-432-regress/parquet/', format 'parquet', compression 'none');
INSERT INTO filemeta_pq_w SELECT i, lpad(i::text, 200, '0') FROM generate_series(1, 20000) i;

DROP FOREIGN TABLE IF EXISTS filemeta_pq_r;
CREATE FOREIGN TABLE filemeta_pq_r (id int, pad text)
        SERVER file_meta_cache_hdfs_server
        OPTIONS (filePath '/filemeta-432-regress/parquet/', format 'parquet', enableCache 'true');

-- Feature off: correctness must hold when the footer cache is never consulted.
SET datalake.enable_file_meta_cache = off;
SELECT count(*) FROM filemeta_pq_r;

-- Cold: a footer-cache miss plus a backfill happens under the hood.
SET datalake.enable_file_meta_cache = on;
SELECT count(*) FROM filemeta_pq_r;

-- Warm: the footer is served from the blob AND the head "PAR1" read is
-- skipped, so this open takes a genuinely different path through
-- gopherReadFileSystem::Open than the cold one did. The answer must not
-- move.
SELECT count(*) FROM filemeta_pq_r;

-- Warm again, but forcing a real per-column decode rather than a
-- metadata-only answer: count(*) alone can be satisfied from footer
-- statistics, which would leave the data path untested on a warm open.
SELECT sum(id), max(length(pad)) FROM filemeta_pq_r;

-- P2: a file SMALLER than 64 KB -- the clamp branch. Here cacheLen is the
-- whole file, so the blob starts at offset 0 and the EOF anchor
-- (offset + length == fileSize) holds only because of the clamp. Get this
-- wrong and the file either never caches or the range runs past EOF.
DROP FOREIGN TABLE IF EXISTS filemeta_pq_tiny_w;
CREATE FOREIGN TABLE filemeta_pq_tiny_w (id int)
        SERVER file_meta_cache_hdfs_server
        OPTIONS (filePath '/filemeta-432-regress/parquet-tiny/', format 'parquet', compression 'none');
INSERT INTO filemeta_pq_tiny_w SELECT i FROM generate_series(1, 10) i;

DROP FOREIGN TABLE IF EXISTS filemeta_pq_tiny_r;
CREATE FOREIGN TABLE filemeta_pq_tiny_r (id int)
        SERVER file_meta_cache_hdfs_server
        OPTIONS (filePath '/filemeta-432-regress/parquet-tiny/', format 'parquet', enableCache 'true');

-- Cold, then warm. sum(id) is asserted as well as the count so that a
-- range served from the wrong offset shows up as a wrong VALUE, not just
-- a wrong row count.
SELECT count(*), sum(id) FROM filemeta_pq_tiny_r;
SELECT count(*), sum(id) FROM filemeta_pq_tiny_r;

-- P3: the size limit at its floor. 64 kB is the enforced minimum
-- precisely because arrow needs it, so at the floor a Parquet range is
-- exactly allowed while a larger ORC tail can be refused. Either way the
-- answers must not change -- a refused backfill degrades to reading from
-- the source, never to a wrong result.
SET datalake.file_meta_max_size = 64;
SELECT count(*) FROM filemeta_pq_r;
SELECT count(*) FROM filemeta_small_r;
RESET datalake.file_meta_max_size;

-- And below the floor must be rejected outright, not silently accepted:
-- a limit under 64 kB would disable Parquet caching with no symptom
-- except a hit rate of zero.
SET datalake.file_meta_max_size = 32;

-- ============================================================
-- Group Q: Parquet row-group pruning.
--
-- The ORC groups above have a full-prune case; Parquet had none, and the
-- asymmetry mattered: what this feature caches IS the metadata that
-- pruning decides from. A cache that subtly corrupted the footer could
-- keep row counts right on an unfiltered scan and still make the wrong
-- pruning decision, so a correct-but-empty answer under a predicate that
-- excludes everything is the check with teeth.
-- ============================================================
DROP FOREIGN TABLE IF EXISTS filemeta_pqprune_w;
CREATE FOREIGN TABLE filemeta_pqprune_w (id int, pad text)
        SERVER file_meta_cache_hdfs_server
        OPTIONS (filePath '/filemeta-432-regress/pqpruned/', format 'parquet', compression 'none');
INSERT INTO filemeta_pqprune_w SELECT i, lpad(i::text, 1000, '0') FROM generate_series(1, 210000) i;

DROP FOREIGN TABLE IF EXISTS filemeta_pqprune_r;
CREATE FOREIGN TABLE filemeta_pqprune_r (id int, pad text)
        SERVER file_meta_cache_hdfs_server
        OPTIONS (filePath '/filemeta-432-regress/pqpruned/', format 'parquet', enableCache 'true');

-- Cold: every row group excluded by "id < -1".
SELECT count(*) FROM filemeta_pqprune_r WHERE id < -1;
-- Warm: the footer now comes from the cache; the pruning decision must
-- not move.
SELECT count(*) FROM filemeta_pqprune_r WHERE id < -1;
-- Sanity: an unfiltered scan must still see every row, so that "empty"
-- above means "correctly pruned" and not "silently lost the data".
SELECT count(*) FROM filemeta_pqprune_r;

-- ============================================================
-- Group S: OBJECT STORAGE, not HDFS.
--
-- Everything above runs on HDFS, which is not what this feature is for.
-- The saving it exists to deliver is a ranged GET that never leaves the
-- database -- an object-storage shape. HDFS reads go through a DataNode
-- and have a different cost profile, and the two backends only converge
-- below GopherFileSystem, so an HDFS-only test leaves the actual target
-- scenario unproven end to end.
--
-- PREREQUISITE: the bucket filemeta-432-regress must exist. A bucket
-- cannot be created through a foreign table, and the sibling
-- oss_read_*.sql cases have the same kind of dependency on
-- ossext-ci-test. The S3 endpoint and credentials come from
-- hive_s3_server, defined once in setup.sql; this file declares no
-- server and no credentials of its own, like every sibling case.
-- ============================================================

-- S1: ORC on object storage.
DROP FOREIGN TABLE IF EXISTS filemeta_s3orc_w;
CREATE FOREIGN TABLE filemeta_s3orc_w (id int, pad text)
        SERVER hive_s3_server
        OPTIONS (filePath '/filemeta-432-regress/orc/', format 'orc', compression 'none');
INSERT INTO filemeta_s3orc_w SELECT i, lpad(i::text, 200, '0') FROM generate_series(1, 20000) i;

DROP FOREIGN TABLE IF EXISTS filemeta_s3orc_r;
CREATE FOREIGN TABLE filemeta_s3orc_r (id int, pad text)
        SERVER hive_s3_server
        OPTIONS (filePath '/filemeta-432-regress/orc/', format 'orc', enableCache 'true');

SET datalake.enable_file_meta_cache = off;
SELECT count(*) FROM filemeta_s3orc_r;
SET datalake.enable_file_meta_cache = on;
SELECT count(*) FROM filemeta_s3orc_r;
SELECT count(*) FROM filemeta_s3orc_r;
SELECT sum(id), max(length(pad)) FROM filemeta_s3orc_r;

-- S2: Parquet on object storage.
DROP FOREIGN TABLE IF EXISTS filemeta_s3pq_w;
CREATE FOREIGN TABLE filemeta_s3pq_w (id int, pad text)
        SERVER hive_s3_server
        OPTIONS (filePath '/filemeta-432-regress/parquet/', format 'parquet', compression 'none');
INSERT INTO filemeta_s3pq_w SELECT i, lpad(i::text, 200, '0') FROM generate_series(1, 20000) i;

DROP FOREIGN TABLE IF EXISTS filemeta_s3pq_r;
CREATE FOREIGN TABLE filemeta_s3pq_r (id int, pad text)
        SERVER hive_s3_server
        OPTIONS (filePath '/filemeta-432-regress/parquet/', format 'parquet', enableCache 'true');

SET datalake.enable_file_meta_cache = off;
SELECT count(*) FROM filemeta_s3pq_r;
SET datalake.enable_file_meta_cache = on;
SELECT count(*) FROM filemeta_s3pq_r;
SELECT count(*) FROM filemeta_s3pq_r;
SELECT sum(id), max(length(pad)) FROM filemeta_s3pq_r;

-- ============================================================
-- Cleanup: this test is self-seeded, so it must also be self-cleaning.
-- ============================================================
DROP FOREIGN TABLE filemeta_s3pq_r;
DROP FOREIGN TABLE filemeta_s3pq_w;
DROP FOREIGN TABLE filemeta_s3orc_r;
DROP FOREIGN TABLE filemeta_s3orc_w;
DROP FOREIGN TABLE filemeta_pqprune_r;
DROP FOREIGN TABLE filemeta_pqprune_w;
DROP FOREIGN TABLE filemeta_pq_r;
DROP FOREIGN TABLE filemeta_pq_w;
DROP FOREIGN TABLE filemeta_pq_tiny_r;
DROP FOREIGN TABLE filemeta_pq_tiny_w;
DROP FOREIGN TABLE filemeta_small_r;
DROP FOREIGN TABLE filemeta_small_w;
DROP FOREIGN TABLE filemeta_pruned_r;
DROP FOREIGN TABLE filemeta_pruned_w;
DROP USER MAPPING FOR gpadmin SERVER file_meta_cache_hdfs_server;
DROP SERVER file_meta_cache_hdfs_server;

-- ============================================================
-- Manual-only benefit verification (NOT part of this automated file --
-- run by hand; see task-16-report.md for the exact numbers this produced).
--
-- The design doc predicts that a table far bigger than the block-cache
-- capacity ("Group B") should make the SECOND scan miss again, because
-- the tail blob's lifetime is tied to its File object, and that File
-- should get evicted by the table's own later, much larger reads.
-- Empirically, on this build, it does NOT: a ~2.3GB/segment single ORC
-- file scanned twice under a 1GB-per-segment cache (the GUC's documented
-- minimum) shows exactly one "miss" (first open) and then "hit" on every
-- later open -- including a heavier scan that forces a real per-row
-- column decode (`max(length(pad))`, ruling out an all-metadata fast
-- path) and drives confirmed multi-GB of genuine remote reads in between.
-- `gopherosscmd ... state` corroborates this: the block-level cache
-- churns hard under the 1GB budget (CACHED FILE dropped from ~19000 to
-- 1, nearly all of it evicting *other*, older files on the shared dev
-- cluster), yet the tail blob for our own file survives regardless.
--
-- Do NOT read this as "byte pressure never evicts blobs". It does. An
-- earlier draft of this comment claimed the design assumption was wrong;
-- that claim was itself wrong and is corrected here.
--
-- What the code actually does: MetaService calls the three-argument
-- EvictionPolicy::releaseCache, whose first two branches are byte-driven
-- (SumSize > mMaxCapacity, and mCapacity < SumSize < mMaxCapacity). Both
-- call LRUCache::releaseCache, which evicts *Files* from the tail of the
-- LRU until enough bytes are freed -- and a File's tail blob dies with it.
-- Only when neither byte branch fires does it fall through to the
-- file-count threshold. So byte capacity and file count are both real
-- thresholds, and the byte one is what fires first under memory pressure.
--
-- Why this experiment saw the blob survive anyway: it reads a single hot
-- file. Every open promotes it to the head of the LRU, and eviction walks
-- from the tail, so what gets reclaimed is always someone else's older
-- file -- exactly what the numbers above show. Group B needs *many*
-- files, so that files touched early in the scan go cold and reach the
-- tail while the scan is still running. This run did not construct that
-- condition, which is a limitation of the experiment, not a finding about
-- the implementation.
--
-- Practical upshot: for a table much larger than the block cache, expect
-- the repeated cold-start cost the design doc describes. The saving is
-- real for repeated scans of the same files that stay resident.
--
-- Reproduction recipe:
--   1. ALTER SYSTEM SET pg_gophermeta.gopher_local_capacity_mb = 1024;
--      then `gpstop -u` alone is NOT enough -- empirically this GUC only
--      takes effect after a full `gpstop -ra`/`gpstart` cycle, despite
--      being marked sighup.
--   2. Write a single-file ORC table whose per-segment share is roughly
--      2.5x that capacity (needs compression 'none' and a wide, unique
--      `pad` column such as lpad(i::text, 1000, '0') to defeat ORC's own
--      dictionary encoding -- otherwise the file is dictionary-compressed
--      down to a few MB regardless of row count. ~7M rows of that shape
--      lands around 6.7GB total / ~2.2-2.3GB per segment across 3 QEs.)
--   3. `SET client_min_messages = debug1;` and scan it (`SELECT count(*),
--      max(length(pad))` -- plain `count(*)` alone is answered from ORC
--      footer statistics and never touches the bulk column data, which
--      makes it useless for exercising this scenario) at least twice;
--      grep the output for "datalake file-meta-cache:".
--   4. `ALTER SYSTEM SET pg_gophermeta.gopher_local_capacity_mb = 1024000;`
--      and a full restart to restore the default afterward.
-- ============================================================
