-- Iceberg AM time-travel field-id regression.
--
-- iceberg_snapshot_scan(regclass, snapshot_id) must read a HISTORICAL snapshot
-- under THAT snapshot's own schema (resolved by field-id), not the table's
-- current schema.  So a column renamed or type-promoted after the snapshot
-- still reads back correctly from the old data files:
--   * a column renamed later shows its OLD name and data;
--   * a column type-promoted later (int -> bigint) reads with its OLD type
--     and un-corrupted values.
--
-- The multi-schema fixture (Spark table hc.public.tt_src: created as
-- (id,note,amt int), then RENAME note->memo + ALTER amt TYPE bigint + more
-- rows) is built by run.sh, which also captures the pre-rename snapshot id
-- (:snap_a) and the current metadata.json path (:head_meta) and passes them
-- in.  CBDB blocks in-place ALTER on Iceberg tables, so the evolution is done
-- externally (the supported path) and read back through a native AM table
-- pinned to the evolved metadata.
--
-- Deterministic output: values are emitted unaligned/tuples-only so the
-- expected file does not depend on the (Spark-assigned, run-varying) snapshot
-- id or on psql column widths.

\set QUIET on
SET client_min_messages = WARNING;
\i ../../../lib/sql/common_setup.sql
-- common_setup resets client_min_messages for its own NOTICE suppression, so
-- re-apply it after the include (the first SET silences the include itself).
SET client_min_messages = WARNING;

-- Catalog + volume over the shared MinIO warehouse, scoped to this test's
-- prefix.  The Spark side WRITES via Hadoop's S3A connector (see run.sh) --
-- s3a is a Hadoop requirement there; the CBDB side uses the standard s3
-- scheme (our recommended form).  The agent resolves every URI by its own
-- scheme, so the mixed fixture interoperates.
DROP SERVER IF EXISTS ttcat_srv CASCADE;
CREATE SERVER ttcat_srv FOREIGN DATA WRAPPER iceberg_catalog_fdw
    OPTIONS (type 's3');
CREATE USER MAPPING FOR current_user SERVER ttcat_srv;
CREATE FOREIGN CATALOG ttcat SERVER ttcat_srv
    OPTIONS (warehouse_location_prefix 's3://warehouse/iceberg_am_time_travel/');
SET iceberg_default_catalog = 'ttcat';

DROP SERVER IF EXISTS ttvol_srv CASCADE;
CREATE SERVER ttvol_srv FOREIGN DATA WRAPPER iceberg_volume_fdw
    OPTIONS (type 's3', endpoint 'http://minio:9000', region 'us-east-1',
             bucket_name 'warehouse', path_style_access 'true');
CREATE USER MAPPING FOR current_user SERVER ttvol_srv
    OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME ttvol SERVER ttvol_srv
    OPTIONS (base_path '/iceberg_am_time_travel/', allow_writes 'true');
SET iceberg_default_volume = 'ttvol';

-- Native AM table whose pg_class carries the CURRENT (post-evolution) schema
-- (id, memo, amt bigint), pinned to the Spark fixture's evolved metadata.
-- This is the realistic state: CBDB tracks HEAD, the user time-travels back.
DROP TABLE IF EXISTS tt_rename;
CREATE ICEBERG TABLE tt_rename (id int, memo text, amt bigint);
SET allow_system_table_mods = on;
UPDATE pg_ext_aux.pg_iceberg_metadata
   SET metadata_location = :'head_meta'
 WHERE relid = 'tt_rename'::regclass;
RESET allow_system_table_mods;

\pset format unaligned
\pset tuples_only on

-- Assertion 1: time travel to the pre-rename snapshot returns the OLD column
-- name "note" (referencing it at all proves the snapshot schema is used, not
-- the current "memo") with the pre-promotion int amounts intact.
SELECT CASE
         WHEN string_agg(id || '|' || note || '|' || amt, ',' ORDER BY id)
              = '1|note1|100,2|note2|200,3|note3|300'
         THEN 'ASSERT rename_type_travel: PASS'
         ELSE 'ASSERT rename_type_travel: FAIL got='
              || coalesce(string_agg(id || '|' || note || '|' || amt, ',' ORDER BY id), '<null>')
       END
  FROM iceberg_snapshot_scan('tt_rename', :snap_a);

-- Assertion 2: the historical snapshot has exactly its 3 pre-rename rows,
-- not the 5 rows visible at HEAD.
SELECT CASE
         WHEN count(*) = 3 THEN 'ASSERT snapshot_rowcount: PASS'
         ELSE 'ASSERT snapshot_rowcount: FAIL got=' || count(*)
       END
  FROM iceberg_snapshot_scan('tt_rename', :snap_a);

-- Assertion 3: the pre-promotion column reads back with its OLD Postgres type
-- (amt: integer, not the post-ALTER bigint; note: text).  Assertion 1
-- concatenates amt as text, where int and bigint render identically, so a
-- broken reverse type mapping (Iceberg "int" -> int8) would slip through it;
-- assert pg_typeof explicitly.  pg_typeof is constant across rows, so LIMIT 1
-- keeps this to a single assertion line.
SELECT CASE
         WHEN pg_typeof(amt) = 'integer'::regtype
              AND pg_typeof(note) = 'text'::regtype
         THEN 'ASSERT snapshot_coltypes: PASS'
         ELSE 'ASSERT snapshot_coltypes: FAIL got amt=' || pg_typeof(amt)
              || ' note=' || pg_typeof(note)
       END
  FROM iceberg_snapshot_scan('tt_rename', :snap_a)
 LIMIT 1;

-- Assertion 4: snapshot discovery on the Spark-evolved fixture -- both
-- snapshots listed, exactly one current, ids/commit times present.
SELECT CASE
         WHEN count(*) = 2
          AND count(*) FILTER (WHERE is_current) = 1
          AND bool_and(snapshot_id IS NOT NULL AND committed_at IS NOT NULL)
         THEN 'ASSERT snapshot_list: PASS'
         ELSE 'ASSERT snapshot_list: FAIL'
       END
  FROM iceberg_snapshot_list('tt_rename');

-- Assertion 5: the timestamptz overload resolves the pre-rename snapshot's own
-- commit instant to that snapshot (asOfTime semantics), returning the same
-- rows as the by-id read -- old column name, pre-promotion values.
DO $$
DECLARE
    ts     timestamptz;
    by_ts  text;
BEGIN
    SELECT committed_at INTO ts
      FROM iceberg_snapshot_list('tt_rename') ORDER BY committed_at LIMIT 1;
    EXECUTE format(
        'SELECT string_agg(id || ''|'' || note || ''|'' || amt, '','' ORDER BY id)
           FROM iceberg_snapshot_scan(''tt_rename'', %L::timestamptz)', ts)
       INTO by_ts;
    IF by_ts = '1|note1|100,2|note2|200,3|note3|300' THEN
        RAISE INFO 'ASSERT ts_travel: PASS';
    ELSE
        RAISE INFO 'ASSERT ts_travel: FAIL got=%', coalesce(by_ts, '<null>');
    END IF;
END $$;
