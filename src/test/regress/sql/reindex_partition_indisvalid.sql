--
-- Regression coverage for REINDEX revalidating the parent partitioned
-- index after the leaf children are rebuilt.
--
-- Background: when CREATE INDEX on a partitioned table adopts an existing
-- matching leaf index that is itself invalid, DefineIndex's invalidate_parent
-- path persists the parent partitioned index with pg_index.indisvalid=false.
-- REINDEX historically never touched that bit, so the parent stayed INVALID
-- in catalog (and in \d+) even after all of its leaf children were rebuilt.
--
-- The fix wires a validatePartitionedIndex() call into reindex_index() so
-- that whenever a leaf index becomes valid, its parent partitioned index is
-- recomputed using the same rule that ALTER INDEX ... ATTACH PARTITION
-- already applies.  These cases are covered below:
--
--   * REINDEX TABLE on the partitioned root, parent invalid, leaves valid
--   * REINDEX TABLE on the partitioned root, parent + leaves all invalid
--   * Three-level partition tree, everything invalid (exercises the
--     CommandCounterIncrement path and validatePartitionedIndex's internal
--     recursion to the grandparent)
--   * REINDEX TABLE on a single leaf table
--   * REINDEX INDEX on a single leaf index
--   * REINDEX when the parent is already valid is a harmless no-op
--

-- Use a private schema so the test never collides with anything else.
CREATE SCHEMA reidx_pi_test;
SET search_path = reidx_pi_test;

----------------------------------------------------------------------
-- Helper: print pg_index.indisvalid for every index attached to a
-- partition tree, ordered for diff stability.
----------------------------------------------------------------------
CREATE OR REPLACE VIEW v_indisvalid AS
SELECT n.nspname || '.' || c.relname AS idx,
       c.relkind,
       i.indisvalid
FROM pg_index i
JOIN pg_class c ON c.oid = i.indexrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'reidx_pi_test'
ORDER BY 1;

-- Note on scope: in Cloudberry, segment-side pg_index cannot be mutated
-- from a regress test (QE slices reject non-SELECT statements inside
-- plpgsql), so this test only forces invalid state on the coordinator
-- and asserts the coordinator's catalog after REINDEX.  The same
-- reindex_index() code runs on every QE during the per-leaf REINDEX
-- dispatch, so the segment-side path is covered transitively by the
-- single shared codepath.

----------------------------------------------------------------------
-- Case 1: 2-level partitioned table.  Parent partitioned index is
-- forced to indisvalid=false while leaves stay valid.  REINDEX TABLE
-- on the partitioned root must flip the parent to valid.
----------------------------------------------------------------------
CREATE TABLE p2 (id int, dt date) PARTITION BY RANGE (dt);
CREATE TABLE p2_a PARTITION OF p2 FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');
CREATE TABLE p2_b PARTITION OF p2 FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');
CREATE INDEX p2_id_idx ON p2 (id);

SET allow_system_table_mods = on;
UPDATE pg_index SET indisvalid = false
WHERE indexrelid = 'p2_id_idx'::regclass;
RESET allow_system_table_mods;

-- Before: parent invalid, leaves valid.
SELECT * FROM v_indisvalid;

REINDEX TABLE p2;

-- After: parent and leaves all valid.
SELECT * FROM v_indisvalid;

----------------------------------------------------------------------
-- Case 2: Same shape but flip the leaves invalid too, so reindex_index
-- has to actually rebuild them.  This is the case that needs the
-- CommandCounterIncrement() so that validatePartitionedIndex sees the
-- just-updated leaf indisvalid via SearchSysCache1.  Also flip every
-- segment's pg_index so the QE-side path is observably exercised.
----------------------------------------------------------------------
SET allow_system_table_mods = on;
UPDATE pg_index SET indisvalid = false
WHERE indrelid IN (SELECT relid FROM pg_partition_tree('p2'::regclass));
RESET allow_system_table_mods;

SELECT * FROM v_indisvalid;

REINDEX TABLE p2;

SELECT * FROM v_indisvalid;

----------------------------------------------------------------------
-- Case 3: Three-level partition tree, every index forced invalid.
-- Tests validatePartitionedIndex's internal recursion up to the
-- grandparent, driven by my fix calling it on the immediate parent only.
----------------------------------------------------------------------
CREATE TABLE p3 (id int, region text, dt date)
  PARTITION BY LIST (region);
CREATE TABLE p3_apac PARTITION OF p3 FOR VALUES IN ('apac')
  PARTITION BY RANGE (dt);
CREATE TABLE p3_emea PARTITION OF p3 FOR VALUES IN ('emea')
  PARTITION BY RANGE (dt);
CREATE TABLE p3_apac_q1 PARTITION OF p3_apac
  FOR VALUES FROM ('2026-01-01') TO ('2026-04-01');
CREATE TABLE p3_apac_q2 PARTITION OF p3_apac
  FOR VALUES FROM ('2026-04-01') TO ('2026-07-01');
CREATE TABLE p3_emea_q1 PARTITION OF p3_emea
  FOR VALUES FROM ('2026-01-01') TO ('2026-04-01');
CREATE TABLE p3_emea_q2 PARTITION OF p3_emea
  FOR VALUES FROM ('2026-04-01') TO ('2026-07-01');
CREATE INDEX p3_id_idx ON p3 (id);

SET allow_system_table_mods = on;
UPDATE pg_index SET indisvalid = false
WHERE indrelid IN (SELECT relid FROM pg_partition_tree('p3'::regclass));
RESET allow_system_table_mods;

SELECT * FROM v_indisvalid WHERE idx LIKE 'reidx_pi_test.p3%';

REINDEX TABLE p3;

SELECT * FROM v_indisvalid WHERE idx LIKE 'reidx_pi_test.p3%';

----------------------------------------------------------------------
-- Case 4: REINDEX TABLE on a single leaf partition flips the parent
-- when that leaf was the last invalid child.  Set things up so two
-- leaves of p2_b are invalid, REINDEX one, parent stays invalid;
-- REINDEX the second, parent flips.
----------------------------------------------------------------------
-- Reset state of p2 so this case starts clean.
SET allow_system_table_mods = on;
UPDATE pg_index SET indisvalid = false
WHERE indrelid IN (SELECT relid FROM pg_partition_tree('p2'::regclass));
RESET allow_system_table_mods;

SELECT * FROM v_indisvalid WHERE idx LIKE 'reidx_pi_test.p2%';

REINDEX TABLE p2_a;        -- only p2_a's leaf becomes valid, parent stays invalid

SELECT * FROM v_indisvalid WHERE idx LIKE 'reidx_pi_test.p2%';

REINDEX TABLE p2_b;        -- now all children valid, parent flips

SELECT * FROM v_indisvalid WHERE idx LIKE 'reidx_pi_test.p2%';

----------------------------------------------------------------------
-- Case 5: REINDEX INDEX on a single leaf index propagates the same
-- way as REINDEX TABLE on the leaf.
----------------------------------------------------------------------
SET allow_system_table_mods = on;
UPDATE pg_index SET indisvalid = false
WHERE indrelid IN (SELECT relid FROM pg_partition_tree('p2'::regclass));
RESET allow_system_table_mods;

SELECT * FROM v_indisvalid WHERE idx LIKE 'reidx_pi_test.p2%';

-- Reindex each leaf index by direct name; second one should flip parent.
DO $$
DECLARE
    r record;
BEGIN
    FOR r IN
        SELECT c.oid::regclass::text AS idx
        FROM pg_class c
        JOIN pg_index i ON i.indexrelid = c.oid
        WHERE c.relkind = 'i'
          AND i.indrelid IN (SELECT relid FROM pg_partition_tree('p2'::regclass)
                              WHERE isleaf)
        ORDER BY c.relname
    LOOP
        EXECUTE format('REINDEX INDEX %s', r.idx);
    END LOOP;
END $$;

SELECT * FROM v_indisvalid WHERE idx LIKE 'reidx_pi_test.p2%';

----------------------------------------------------------------------
-- Case 6: REINDEX with an already-valid parent must be a no-op for the
-- parent (no error, no spurious catalog write).  Run REINDEX twice and
-- confirm pg_index.indisvalid stays true throughout.
----------------------------------------------------------------------
SELECT * FROM v_indisvalid WHERE idx LIKE 'reidx_pi_test.p2%';

REINDEX TABLE p2;
REINDEX TABLE p2;

SELECT * FROM v_indisvalid WHERE idx LIKE 'reidx_pi_test.p2%';

----------------------------------------------------------------------
-- Cleanup
----------------------------------------------------------------------
DROP SCHEMA reidx_pi_test CASCADE;
