--
-- relfrozenxid_zero.sql — regression guard for the PAX relfrozenxid fix.
--
-- PAX stores visibility metadata in aux relations
-- (pg_ext_aux.pg_pax_blocks_<oid> or pg_manifest_<oid>); it has no
-- per-tuple xmin/xmax.  Consequently, pg_class.relfrozenxid for a PAX
-- relation is supposed to always be InvalidTransactionId (0), and the
-- vac_update_datfrozenxid() computation skips PAX rels.
--
-- Two independent code paths historically could leak a non-zero value
-- into pg_class.relfrozenxid for PAX rels.  Both were patched:
--   (a) cluster.c::swap_relation_files() AO-only override whitelist
--       missed PAX — VACUUM FULL / CLUSTER / ALTER-rebuild routes that
--       go through finish_heap_swap left the caller's RecentXmin in
--       relfrozenxid.  Fix: extend whitelist with IsAccessMethodPAX
--       (same place AO/AOCS is overridden back to Invalid).
--   (b) PAX RelationVacuum was a pure no-op; any historical stale value
--       (from older PAX or a previous (a)-bug occurrence) stayed
--       forever and pinned pg_database.datfrozenxid.  Fix: defensive
--       direct catalog UPDATE in RelationVacuum that resets relfrozenxid
--       and relminmxid back to Invalid when found non-zero.
--
-- This test exercises every command path that could potentially write to
-- pg_class.relfrozenxid for a PAX relation (table, matview, partition
-- root, partition leaf) and asserts that the value remains 0.  Any
-- regression makes show_dirty_pax_rels() emit one or more rows, which
-- diffs against the empty expected output.

-- start_ignore
DROP SCHEMA IF EXISTS pax_relfrozenxid_test CASCADE;
-- end_ignore

CREATE SCHEMA pax_relfrozenxid_test;
SET search_path = pax_relfrozenxid_test, public;
SET default_table_access_method = pax;

-- Helper: returns ONLY the PAX rels in this schema whose relfrozenxid is
-- non-zero.  In the green case this should emit zero rows after every
-- step; any output is a regression.
-- Helper: returns ONLY the PAX rels in this schema with a non-Invalid
-- relfrozenxid OR relminmxid.  Both fields must stay 0 for PAX rels.
CREATE FUNCTION show_dirty_pax_rels()
RETURNS TABLE(relname text, relkind "char",
              relfrozenxid text, relminmxid text) AS $$
    SELECT c.relname::text, c.relkind,
           c.relfrozenxid::text, c.relminmxid::text
    FROM pg_class c
    JOIN pg_am am ON am.oid = c.relam
    WHERE c.relnamespace = 'pax_relfrozenxid_test'::regnamespace
      AND am.amname = 'pax'
      AND (c.relfrozenxid::text <> '0' OR c.relminmxid::text <> '0')
    ORDER BY c.relname;
$$ LANGUAGE SQL;

-- ============================================================
-- 1. CREATE TABLE — initial state
-- ============================================================
CREATE TABLE t1 (a int, b text) USING pax DISTRIBUTED BY (a);
SELECT * FROM show_dirty_pax_rels();

-- ============================================================
-- 2. INSERT / UPDATE / DELETE / COPY  (don't touch relfrozenxid)
-- ============================================================
INSERT INTO t1 SELECT g, 'x'||g FROM generate_series(1, 100) g;
UPDATE t1 SET b = upper(b) WHERE a % 3 = 0;
DELETE FROM t1 WHERE a < 10;
COPY t1 FROM stdin (FORMAT csv);
101,a
102,b
103,c
\.
SELECT * FROM show_dirty_pax_rels();

-- ============================================================
-- 3. TRUNCATE  (RelationSetNewRelfilenode → PAX AM hook)
-- ============================================================
TRUNCATE t1;
SELECT * FROM show_dirty_pax_rels();
INSERT INTO t1 SELECT g, 'r'||g FROM generate_series(1, 100) g;

-- ============================================================
-- 4. VACUUM / VACUUM FREEZE  (PAX RelationVacuum)
-- ============================================================
VACUUM t1;
SELECT * FROM show_dirty_pax_rels();

VACUUM FREEZE t1;
SELECT * FROM show_dirty_pax_rels();

-- ============================================================
-- 5. VACUUM FULL  (cluster_rel → swap_relation_files; patches (a)+(b))
-- ============================================================
VACUUM FULL t1;
SELECT * FROM show_dirty_pax_rels();

-- ============================================================
-- 6. CLUSTER USING <btree-idx>  (same swap path)
-- ============================================================
CREATE INDEX t1_a_idx ON t1 (a);
CLUSTER t1 USING t1_a_idx;
SELECT * FROM show_dirty_pax_rels();

-- ============================================================
-- 7. ALTER TABLE SET TABLESPACE  (RelationSetNewRelfilenode path)
-- ============================================================
ALTER TABLE t1 SET TABLESPACE pg_default;
SELECT * FROM show_dirty_pax_rels();

-- ============================================================
-- 8. ALTER TABLE ADD COLUMN  (some variants trigger heap rewrite)
-- ============================================================
ALTER TABLE t1 ADD COLUMN c int;
ALTER TABLE t1 ADD COLUMN d int DEFAULT 1;
ALTER TABLE t1 ADD COLUMN e int DEFAULT 1 NOT NULL;  -- forces rewrite
SELECT * FROM show_dirty_pax_rels();

-- ============================================================
-- 9. ALTER TABLE ALTER COLUMN TYPE  (rewrite when storage layout changes)
-- ============================================================
ALTER TABLE t1 ALTER COLUMN b TYPE varchar(50);
SELECT * FROM show_dirty_pax_rels();

-- ============================================================
-- 10. ALTER TABLE SET / RESET reloptions
-- ============================================================
ALTER TABLE t1 SET (compresstype = 'zstd', compresslevel = 3);
SELECT * FROM show_dirty_pax_rels();
ALTER TABLE t1 RESET (compresstype, compresslevel);
SELECT * FROM show_dirty_pax_rels();

-- ============================================================
-- 11. REINDEX
-- ============================================================
REINDEX INDEX t1_a_idx;
SELECT * FROM show_dirty_pax_rels();
REINDEX TABLE t1;
SELECT * FROM show_dirty_pax_rels();

-- ============================================================
-- 12. ALTER TABLE SET ACCESS METHOD pax  (PAX rejects this today)
-- ============================================================
DO $$ BEGIN
    EXECUTE 'ALTER TABLE t1 SET ACCESS METHOD pax';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'set access method rejected as expected: %', SQLERRM;
END $$;
SELECT * FROM show_dirty_pax_rels();

-- ============================================================
-- 13. Materialized view USING pax
-- ============================================================
CREATE TABLE mv_base (a int, b text) USING pax DISTRIBUTED BY (a);
INSERT INTO mv_base SELECT g, 'mb'||g FROM generate_series(1, 50) g;

CREATE MATERIALIZED VIEW mv1 USING pax AS
    SELECT * FROM mv_base WITH NO DATA;
SELECT * FROM show_dirty_pax_rels();

REFRESH MATERIALIZED VIEW mv1;
SELECT * FROM show_dirty_pax_rels();

REFRESH MATERIALIZED VIEW mv1 WITH NO DATA;
SELECT * FROM show_dirty_pax_rels();

REFRESH MATERIALIZED VIEW mv1;   -- populate again
SELECT * FROM show_dirty_pax_rels();

VACUUM mv1;
SELECT * FROM show_dirty_pax_rels();

VACUUM FREEZE mv1;
SELECT * FROM show_dirty_pax_rels();

VACUUM FULL mv1;
SELECT * FROM show_dirty_pax_rels();

-- ============================================================
-- 14. Partitioned PAX  (root + leaves)
-- ============================================================
CREATE TABLE p_root (a int, b text) USING pax
  PARTITION BY RANGE (a);
CREATE TABLE p_leaf1 PARTITION OF p_root FOR VALUES FROM (1) TO (50);
CREATE TABLE p_leaf2 PARTITION OF p_root FOR VALUES FROM (50) TO (100);
INSERT INTO p_root SELECT g, 'p'||g FROM generate_series(1, 99) g;
SELECT * FROM show_dirty_pax_rels();

VACUUM p_root;
VACUUM FREEZE p_leaf1;
VACUUM FULL p_leaf2;
SELECT * FROM show_dirty_pax_rels();

-- ============================================================
-- 15. Final positive listing — every PAX rel in the schema, showing
--     relfrozenxid + relminmxid explicitly.  All must be 0.
-- ============================================================
SELECT c.relname, c.relkind,
       c.relfrozenxid::text AS relfrozenxid,
       c.relminmxid::text   AS relminmxid
FROM pg_class c
JOIN pg_am am ON am.oid = c.relam
WHERE c.relnamespace = 'pax_relfrozenxid_test'::regnamespace
  AND am.amname = 'pax'
ORDER BY c.relname;

-- Cleanup
DROP SCHEMA pax_relfrozenxid_test CASCADE;
RESET default_table_access_method;
RESET search_path;
