-- Iceberg AM time-travel basic regression (database-only, runs in CI).
--
-- Covers the iceberg_snapshot_scan() behaviors that do NOT need an externally
-- schema-evolved fixture (those live in iceberg_am_time_travel, which drives
-- Spark from the host and is skipped in the in-container runner):
--   * HEAD read (snapshot_id = 0) returns the live data;
--   * the HEAD read is planned ORCA-native as an Iceberg Custom Scan (the
--     main-path RTE rewrite), not a Function Scan;
--   * a non-existent snapshot id is rejected deterministically (never a silent
--     fall back to HEAD);
--   * SELECT privilege is enforced.
--
-- Snapshot ids are catalog-assigned and non-deterministic, so this test does
-- not read a specific historical snapshot (that is covered, with a Spark
-- fixture, by iceberg_am_time_travel).  Assertions emit stable booleans /
-- counts so the expected output does not depend on ids or plan-node counters.
\i ../../../lib/sql/common_setup.sql
SET client_min_messages = WARNING;

DROP SERVER IF EXISTS ttb_cat_srv CASCADE;
CREATE SERVER ttb_cat_srv FOREIGN DATA WRAPPER iceberg_catalog_fdw
    OPTIONS (type 's3');
CREATE USER MAPPING FOR current_user SERVER ttb_cat_srv;
CREATE FOREIGN CATALOG ttb_cat SERVER ttb_cat_srv
    OPTIONS (warehouse_location_prefix 's3://warehouse/iceberg_am_tt_basic/');
SET iceberg_default_catalog = 'ttb_cat';

DROP SERVER IF EXISTS ttb_vol_srv CASCADE;
CREATE SERVER ttb_vol_srv FOREIGN DATA WRAPPER iceberg_volume_fdw
    OPTIONS (type 's3', endpoint 'http://minio:9000', region 'us-east-1',
             bucket_name 'warehouse', path_style_access 'true');
CREATE USER MAPPING FOR current_user SERVER ttb_vol_srv
    OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME ttb_vol SERVER ttb_vol_srv
    OPTIONS (base_path '/iceberg_am_tt_basic/', allow_writes 'true');
SET iceberg_default_volume = 'ttb_vol';

DROP TABLE IF EXISTS tt_basic;
CREATE ICEBERG TABLE tt_basic (id int, v text);
INSERT INTO tt_basic SELECT g, 'r' || g FROM generate_series(1, 5) g;   -- snapshot 1
INSERT INTO tt_basic SELECT g, 'r' || g FROM generate_series(6, 8) g;   -- snapshot 2 (HEAD)

-- T1: HEAD read (snapshot_id = 0) sees all committed rows.  Two assertions,
-- deliberately both:
--   * the exact fixture count (5 + 3 INSERTed rows = 8; INSERT..SELECT row
--     counts do not vary with the segment layout) catches the HEAD read
--     returning too few or too many rows even if the ordinary scan is
--     equally wrong;
--   * equality with the ordinary relation scan catches the two paths
--     drifting apart.  On its own this half would be near-tautological
--     (the snapshot_id = 0 call is rewritten INTO a relation scan), which
--     is why the exact count above stays.
DO $$
DECLARE
    tt_rows   bigint;
    rel_rows  bigint;
BEGIN
    SELECT count(*) INTO tt_rows  FROM iceberg_snapshot_scan('tt_basic', 0);
    SELECT count(*) INTO rel_rows FROM tt_basic;
    RAISE WARNING 'head_rows_exact=% head_matches_relation_scan=%',
        (tt_rows = 8), (tt_rows = rel_rows);
END $$;

-- T2: iceberg_snapshot_list() runs standalone and its shape is sane: one row
-- per snapshot, exactly one is_current, ids/commit times all present, commit
-- times strictly ordered.  (Values are run-varying, so assert booleans.)
SELECT count(*) = 2                                   AS two_snapshots,
       count(*) FILTER (WHERE is_current) = 1         AS one_current,
       bool_and(snapshot_id IS NOT NULL)              AS ids_present,
       bool_and(committed_at IS NOT NULL)             AS times_present,
       (SELECT bool_and(ok) FROM (
          SELECT committed_at > lag(committed_at) OVER (ORDER BY committed_at)
                 IS DISTINCT FROM false AS ok
            FROM iceberg_snapshot_list('tt_basic')) o) AS times_ordered
  FROM iceberg_snapshot_list('tt_basic');

-- T3: iceberg_snapshot_list() lists the table's snapshots.  Counts and
-- booleans only -- ids and commit times are catalog-assigned.
DO $$
DECLARE
    n_snaps    int;
    n_current  int;
    all_ided   bool;
BEGIN
    SELECT count(*), count(*) FILTER (WHERE is_current),
           bool_and(snapshot_id IS NOT NULL AND committed_at IS NOT NULL)
      INTO n_snaps, n_current, all_ided
      FROM iceberg_snapshot_list('tt_basic');
    RAISE WARNING 'snapshots_listed=% one_current=% all_have_id_and_ts=%',
        (n_snaps = 2), (n_current = 1), all_ided;
END $$;

-- T4: the HEAD read is planned ORCA-native as an Iceberg Custom Scan (the
-- main-path relation rewrite), not a Function Scan.  Scan EXPLAIN text so the
-- assertion does not depend on the alias's per-session counter.
DO $$
DECLARE
    ln          text;
    has_cs      bool := false;
    has_orca    bool := false;
BEGIN
    FOR ln IN
        EXECUTE 'EXPLAIN (COSTS OFF) SELECT count(*) FROM iceberg_snapshot_scan(''tt_basic'', 0)'
    LOOP
        IF ln LIKE '%Custom Scan (Iceberg Scan)%' THEN has_cs := true; END IF;
        IF ln LIKE '%GPORCA%' THEN has_orca := true; END IF;
    END LOOP;
    RAISE WARNING 'head_customscan_orca=%', (has_cs AND has_orca);
END $$;

-- T5: a NULL snapshot id normalizes to HEAD and still plans ORCA-native as an
-- Iceberg Custom Scan (not left as a Function Scan).
--
-- The ::bigint cast is REQUIRED, do not drop it: iceberg_snapshot_scan has both
-- a bigint and a timestamptz overload, and a bare NULL is untyped, so it
-- matches neither uniquely ("function ... is not unique").  a later case locks that
-- resolution behavior in.
DO $$
DECLARE
    ln      text;
    has_cs  bool := false;
BEGIN
    FOR ln IN
        EXECUTE 'EXPLAIN (COSTS OFF) SELECT count(*) FROM iceberg_snapshot_scan(''tt_basic'', NULL::bigint)'
    LOOP
        IF ln LIKE '%Custom Scan (Iceberg Scan)%' THEN has_cs := true; END IF;
    END LOOP;
    RAISE WARNING 'null_snapshot_native=%', has_cs;
END $$;

-- T6: reading a snapshot AT its own commit timestamp returns the same rows as
-- reading it by id -- the timestamptz form resolves to "newest snapshot
-- committed at or before T", and a snapshot's own commit time selects itself.
DO $$
DECLARE
    sid     bigint;
    ts      timestamptz;
    by_id   bigint;
    by_ts   bigint;
BEGIN
    SELECT snapshot_id, committed_at INTO sid, ts
      FROM iceberg_snapshot_list('tt_basic') ORDER BY committed_at DESC LIMIT 1;
    EXECUTE format('SELECT count(*) FROM iceberg_snapshot_scan(''tt_basic'', %s::bigint)', sid)
       INTO by_id;
    EXECUTE format('SELECT count(*) FROM iceberg_snapshot_scan(''tt_basic'', %L::timestamptz)', ts)
       INTO by_ts;
    RAISE WARNING 'ts_read_matches_id_read=%', (by_id = by_ts);
END $$;

-- T7: a STABLE (time-dependent but statement-constant) selector is accepted.
-- now() is the natural way to ask for a historical read and Spark accepts the
-- equivalent; only VOLATILE expressions can disagree between the sites that
-- const-fold the argument.  now() selects the newest snapshot, i.e. HEAD.
DO $$
DECLARE
    n_now   bigint;
    n_head  bigint;
BEGIN
    SELECT count(*) INTO n_now  FROM iceberg_snapshot_scan('tt_basic', now());
    SELECT count(*) INTO n_head FROM tt_basic;
    RAISE WARNING 'now_accepted_and_is_head=%', (n_now = n_head);
END $$;

-- T8: a non-existent snapshot id is rejected (not a silent HEAD read).
DO $$
BEGIN
    PERFORM count(*) FROM iceberg_snapshot_scan('tt_basic', 999999999);
    RAISE WARNING 'missing_snapshot_rejected=f';
EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'missing_snapshot_rejected=%', (position('not found' in SQLERRM) > 0);
END $$;

-- T9: a negative snapshot id is rejected -- only 0/NULL mean HEAD, and an
-- id that cannot exist must never silently read current data.
DO $$
BEGIN
    PERFORM count(*) FROM iceberg_snapshot_scan('tt_basic', -1);
    RAISE WARNING 'negative_id_rejected=f';
EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'negative_id_rejected=%', (position('invalid snapshot id' in SQLERRM) > 0);
END $$;

-- T10: a volatile / non-constant snapshot id is rejected at parse time (the
-- id is const-folded independently at several sites, so it must be immutable).
DO $$
BEGIN
    PERFORM count(*) FROM iceberg_snapshot_scan('tt_basic', (random() * 10)::bigint);
    RAISE WARNING 'mutable_arg_rejected=f';
EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'mutable_arg_rejected=%', (position('constant' in SQLERRM) > 0);
END $$;

-- T11: a VOLATILE selector is still rejected -- clock_timestamp() really does
-- advance between the describe callback, the rewrite gate and the planner hook.
DO $$
BEGIN
    PERFORM count(*) FROM iceberg_snapshot_scan('tt_basic', clock_timestamp());
    RAISE WARNING 'volatile_ts_rejected=f';
EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'volatile_ts_rejected=%', (position('constant' in SQLERRM) > 0);
END $$;

-- T12: lock in the documented overload behavior.  With both a bigint and a
-- timestamptz second argument, an UNTYPED literal matches neither uniquely
-- (different type categories), so it must be written TIMESTAMPTZ '...'.  This
-- assertion exists so a future "cleanup" cannot quietly change the resolution.
DO $$
BEGIN
    PERFORM count(*) FROM iceberg_snapshot_scan('tt_basic', '2026-01-01 00:00:00');
    RAISE WARNING 'untyped_literal_ambiguous=f';
EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'untyped_literal_ambiguous=%', (position('is not unique' in SQLERRM) > 0);
END $$;

-- T13: a non-Iceberg relation is rejected.
CREATE TABLE tt_heap (id int);
DO $$
BEGIN
    PERFORM count(*) FROM iceberg_snapshot_scan('tt_heap', 0);
    RAISE WARNING 'non_iceberg_rejected=f';
EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'non_iceberg_rejected=%', (position('not an iceberg' in SQLERRM) > 0);
END $$;
DROP TABLE tt_heap;

-- T14: the snapshot's own row count reaches the planner (issue #413).
--
-- A rewritten time-travel scan is an ordinary relation scan, so the planner
-- would otherwise size it from pg_class.reltuples -- which describes HEAD --
-- and choose the join order and motion type for data the snapshot does not
-- hold.  pg_class has one slot per relid and a historical snapshot cannot be
-- ANALYZEd, so the count is carried out of band and applied through
-- get_relation_info_hook.  The fixture's first snapshot holds exactly 5 rows.
--
-- Read the estimate off the TOP plan node: that one is the whole-table figure,
-- while the scan node underneath shows the per-segment share and so depends on
-- the cluster's segment count.  This stage covers the PostgreSQL planner only
-- (optimizer = off); the ORCA path is wired separately.
DO $$
DECLARE
    sid         bigint;
    ln          text;
    est_hist    int := -1;
    est_head    int := -1;
    first       bool;
BEGIN
    SELECT snapshot_id INTO sid
      FROM iceberg_snapshot_list('tt_basic') ORDER BY committed_at LIMIT 1;

    SET LOCAL optimizer = off;

    first := true;
    FOR ln IN EXECUTE format(
        'EXPLAIN SELECT * FROM iceberg_snapshot_scan(''tt_basic'', %s::bigint)', sid)
    LOOP
        IF first THEN
            est_hist := substring(ln from 'rows=([0-9]+)')::int;
            first := false;
        END IF;
    END LOOP;

    first := true;
    FOR ln IN EXECUTE 'EXPLAIN SELECT * FROM tt_basic'
    LOOP
        IF first THEN
            est_head := substring(ln from 'rows=([0-9]+)')::int;
            first := false;
        END IF;
    END LOOP;

    RAISE WARNING 'snapshot_rows_estimated=% differs_from_head=%',
        (est_hist = 5), (est_hist <> est_head);
END $$;

-- T15: the override is keyed on (relid, alias), not on the alias alone.
--
-- The alias is user-writable text, so a plain relation spelled
-- AS "__icetts_<id>_<n>" reaches the same lookup.  It must keep its own
-- estimate: picking up an Iceberg snapshot's row count for an unrelated heap
-- table would mis-plan every query that happens to use that alias.
DO $$
DECLARE
    sid         bigint;
    ln          text;
    est_plain   int := -1;
    est_forged  int := -1;
    first       bool;
BEGIN
    SELECT snapshot_id INTO sid
      FROM iceberg_snapshot_list('tt_basic') ORDER BY committed_at LIMIT 1;

    CREATE TABLE tt_alias_heap AS SELECT g AS id FROM generate_series(1, 1000) g
        DISTRIBUTED BY (id);
    ANALYZE tt_alias_heap;

    SET LOCAL optimizer = off;

    first := true;
    FOR ln IN EXECUTE 'EXPLAIN SELECT * FROM tt_alias_heap'
    LOOP
        IF first THEN
            est_plain := substring(ln from 'rows=([0-9]+)')::int;
            first := false;
        END IF;
    END LOOP;

    first := true;
    FOR ln IN EXECUTE format(
        'EXPLAIN SELECT * FROM tt_alias_heap AS "__icetts_%s_1"', sid)
    LOOP
        IF first THEN
            est_forged := substring(ln from 'rows=([0-9]+)')::int;
            first := false;
        END IF;
    END LOOP;

    DROP TABLE tt_alias_heap;
    RAISE WARNING 'forged_alias_ignored=%', (est_plain = est_forged);
END $$;

-- T16: SELECT privilege is enforced for time travel.
DROP ROLE IF EXISTS ttb_reader;
CREATE ROLE ttb_reader LOGIN;
GRANT USAGE ON SCHEMA public TO ttb_reader;
SET ROLE ttb_reader;
DO $$
BEGIN
    PERFORM count(*) FROM iceberg_snapshot_scan('tt_basic', 0);
    RAISE WARNING 'acl_denied=f';
EXCEPTION WHEN insufficient_privilege THEN
    RAISE WARNING 'acl_denied=t';
END $$;
-- Snapshot history is table metadata, so the discovery function enforces the
-- same SELECT privilege as a read.
DO $$
BEGIN
    PERFORM * FROM iceberg_snapshot_list('tt_basic');
    RAISE WARNING 'snapshots_acl_denied=f';
EXCEPTION WHEN insufficient_privilege THEN
    RAISE WARNING 'snapshots_acl_denied=t';
END $$;
RESET ROLE;

-- cleanup
DROP OWNED BY ttb_reader;
DROP ROLE ttb_reader;
DROP TABLE tt_basic;
