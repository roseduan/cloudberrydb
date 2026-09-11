-- Regression for the block-boundary defect in the new text reader
-- (datalake.external_table_new_text).
--
-- WHAT BROKE: readPolicy.end became EXCLUSIVE in 3b84ce1a9e5 ("Datalake: Fix
-- blocks not being assignment"), which updated readNextFile()'s loop guard to
-- `serial >= end` but left two `finalBlock = (serial == end)` tests behind.
-- Under a half-open range the last serial a segment reads is end - 1, so
-- finalBlock was never true, and with it the `extra_fetch_line_num = 2`
-- completion in customNext() never ran.  That completion is one half of a
-- pair: a segment's last block reads TWO lines past its end, and the next
-- block's `skipNum = 2` drops exactly those two.  With the pair broken, every
-- segment boundary cost one truncated row (the straddling line handed up
-- half-read) plus one row lost outright.  2.0.x is unaffected --
-- lineRecordReader.cpp is byte-identical there; the defect is purely the
-- missed half of that range-semantics change.
--
-- WHY THE BLOCK SIZE IS LOWERED HERE: blocks are cut at
-- datalake.external_table_text_block_size, default 32 MB.  Reaching even one
-- boundary at the default needs a 32 MB+ fixture, which is exactly why this
-- lived through every existing text case -- all of them are single-block
-- files.  At 64 kB a ~800 kB fixture spans several blocks per segment and the
-- case runs in seconds.
--
-- The assertions are the aggregates a mis-split moves: the row count (a lost
-- row), sum(id) (WHICH row was lost), and a count of pad values that are not
-- the exact width written (a truncated row).  Every one of them is identical
-- to what the pre-existing reader returns, which is the property that
-- matters: turning the GUC on must not change answers.
-- RE-RUNNING BY HAND: datalake_fdw's writable external tables append, they do
-- not truncate -- a second INSERT into an already-populated path adds another
-- distinctly-named file rather than replacing it, so every count below
-- doubles.  A fresh CI environment never hits this (the path does not exist
-- yet); re-running against a long-lived dev cluster needs
-- `hdfs dfs -rm -r -f /text-block-boundary` first.
CREATE SERVER text_block_hdfs_server
        FOREIGN DATA WRAPPER datalake_fdw
        OPTIONS (protocol 'hdfs', hdfs_namenodes 'hadoop', hdfs_port '8020',
                 hdfs_auth_method 'simple', hadoop_rpc_protection 'authentication');
CREATE USER MAPPING FOR gpadmin SERVER text_block_hdfs_server OPTIONS (user 'gpadmin');

SET datalake.external_table_text_block_size = 64;

-- Self-seeded: 20000 rows of a fixed 40-byte record, ~800 kB total, so each
-- segment's file spans several 64 kB blocks.
DROP FOREIGN TABLE IF EXISTS textblk_w;
CREATE FOREIGN TABLE textblk_w (id int, pad text)
        SERVER text_block_hdfs_server
        OPTIONS (filePath '/text-block-boundary/csv/', format 'csv');
INSERT INTO textblk_w SELECT i, lpad(i::text, 34, '0') FROM generate_series(1, 20000) i;

DROP FOREIGN TABLE IF EXISTS textblk_r;
CREATE FOREIGN TABLE textblk_r (id int, pad text)
        SERVER text_block_hdfs_server
        OPTIONS (filePath '/text-block-boundary/csv/', format 'csv');

-- Reference: the reader this GUC replaces.
SET datalake.external_table_new_text = off;
SELECT count(*) AS rows, sum(id) AS sum_id,
       count(*) FILTER (WHERE length(pad) <> 34) AS truncated,
       min(id) AS min_id, max(id) AS max_id, count(DISTINCT id) AS distinct_id
FROM textblk_r;

-- The reader under test: every number above must come back unchanged.
SET datalake.external_table_new_text = on;
SELECT count(*) AS rows, sum(id) AS sum_id,
       count(*) FILTER (WHERE length(pad) <> 34) AS truncated,
       min(id) AS min_id, max(id) AS max_id, count(DISTINCT id) AS distinct_id
FROM textblk_r;

-- A second scan must agree with the first: the block walk is stateful
-- (serial, skipNum, the lineRecordReader's pos/end), so a reset that missed a
-- field would show up as a different answer on re-read rather than on the
-- first read.
SELECT count(*) AS rows, sum(id) AS sum_id FROM textblk_r;

-- A predicate that excludes everything: still correct, still empty.  This is
-- read through the same block walk, so a mis-split shows up here as rows
-- appearing where none should.
SELECT count(*) FROM textblk_r WHERE id < 0;

-- The block-size knob is a size, and below one page it cannot hold a record.
SET datalake.external_table_text_block_size = 32;

RESET datalake.external_table_text_block_size;
RESET datalake.external_table_new_text;
DROP FOREIGN TABLE textblk_r;
DROP FOREIGN TABLE textblk_w;
DROP SERVER text_block_hdfs_server CASCADE;
