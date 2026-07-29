-- Iceberg AM time-travel schema-evolution regression (database-only, CI).
--
-- Builtin ALTER TABLE (#401) makes schema evolution constructible without
-- Spark, so the field-id correctness of historical reads is asserted here
-- with exact values.  What this locks in:
--
--   * a snapshot whose history holds a DROP COLUMN has HOLES in its field-id
--     space, so a column's position in the snapshot schema is NOT its id.
--     The fallback scan must match data-file columns by the snapshot's REAL
--     field-ids: matching by position once returned the dropped column's
--     physical data under the surviving column's name (garbage values);
--   * a column renamed after a snapshot reads back under its OLD name with
--     its values intact (ids are rename-stable);
--   * both discovery functions work on a PURE builtin catalog (no server
--     options) -- resolution there rides on buildInCatalog.tableExists +
--     metadata_location, which the time-travel agent calls once failed to set.
--
-- Values are asserted through CASE/string_agg so the expected file does not
-- depend on the run-varying snapshot ids.
\i ../../../lib/sql/common_setup.sql
SET client_min_messages = WARNING;

-- Pure builtin catalog: the server carries NO options on purpose (see above).
DROP SERVER IF EXISTS ttevo_cat_srv CASCADE;
CREATE SERVER ttevo_cat_srv FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER ttevo_cat_srv;
CREATE FOREIGN CATALOG ttevo_cat SERVER ttevo_cat_srv;
SET iceberg_default_catalog = 'ttevo_cat';

DROP SERVER IF EXISTS ttevo_vol_srv CASCADE;
CREATE SERVER ttevo_vol_srv FOREIGN DATA WRAPPER iceberg_volume_fdw
    OPTIONS (type 's3', endpoint 'http://minio:9000', region 'us-east-1',
             bucket_name 'warehouse', path_style_access 'true');
CREATE USER MAPPING FOR current_user SERVER ttevo_vol_srv
    OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME ttevo_vol SERVER ttevo_vol_srv
    OPTIONS (base_path '/iceberg_am_tt_evolution/', allow_writes 'true');
SET iceberg_default_volume = 'ttevo_vol';

-- Evolution fixture.  field-ids: id=1, note=2, amt=3, extra=4.
DROP TABLE IF EXISTS tt_evo_b;
CREATE ICEBERG TABLE tt_evo_b (id int, note text, amt int);
INSERT INTO tt_evo_b VALUES (1, 'n-one', 100), (2, 'n-two', 200); -- S1 {id,note,amt}
ALTER TABLE tt_evo_b DROP COLUMN note;
INSERT INTO tt_evo_b VALUES (3, 300);                             -- S2 {id,amt} <- hole at 2
ALTER TABLE tt_evo_b RENAME COLUMN amt TO amount;
INSERT INTO tt_evo_b VALUES (4, 400);                             -- S3 {id,amount}
ALTER TABLE tt_evo_b ADD COLUMN extra text;
INSERT INTO tt_evo_b VALUES (5, 500, 'x5');                       -- S4 = HEAD

-- Snapshot ids in commit order, without printing them.
SELECT snapshot_id AS s1 FROM iceberg_snapshot_list('tt_evo_b')
 ORDER BY committed_at OFFSET 0 LIMIT 1 \gset
SELECT snapshot_id AS s2 FROM iceberg_snapshot_list('tt_evo_b')
 ORDER BY committed_at OFFSET 1 LIMIT 1 \gset
SELECT snapshot_id AS s3 FROM iceberg_snapshot_list('tt_evo_b')
 ORDER BY committed_at OFFSET 2 LIMIT 1 \gset

\pset format unaligned
\pset tuples_only on

-- E1: HEAD sanity (also proves the pure-builtin discovery call works: the
-- \gset lookups above would have errored before this line otherwise).
SELECT CASE WHEN count(*) = 5 THEN 'ASSERT head_rows: PASS'
            ELSE 'ASSERT head_rows: FAIL got=' || count(*) END
  FROM tt_evo_b;

-- E2: S1 (dense ids) reads its own 3-column schema with exact values; the
-- column set referencing "note"/"amt" at all proves the snapshot schema wins
-- over the current one.
SELECT CASE
         WHEN string_agg(id || '|' || note || '|' || amt, ',' ORDER BY id)
              = '1|n-one|100,2|n-two|200'
         THEN 'ASSERT dense_snapshot: PASS'
         ELSE 'ASSERT dense_snapshot: FAIL got='
              || coalesce(string_agg(id || '|' || note || '|' || amt, ',' ORDER BY id), '<null>')
       END
  FROM iceberg_snapshot_scan('tt_evo_b', :s1);

-- E3: S2 -- THE field-id hole regression.  Schema {id:1, amt:3}; the files
-- from S1 still carry the dropped note's physical column at id 2.  A
-- positional match would return that column's data as "amt"; the real-id
-- match must return the integer amounts.
SELECT CASE
         WHEN string_agg(id || '|' || amt, ',' ORDER BY id)
              = '1|100,2|200,3|300'
         THEN 'ASSERT hole_snapshot: PASS'
         ELSE 'ASSERT hole_snapshot: FAIL got='
              || coalesce(string_agg(id || '|' || amt, ',' ORDER BY id), '<null>')
       END
  FROM iceberg_snapshot_scan('tt_evo_b', :s2);

-- E4: S3 -- rename + hole combined.  "amount" is field-id 3 regardless of the
-- name any given data file was written under.
SELECT CASE
         WHEN string_agg(id || '|' || amount, ',' ORDER BY id)
              = '1|100,2|200,3|300,4|400'
         THEN 'ASSERT renamed_snapshot: PASS'
         ELSE 'ASSERT renamed_snapshot: FAIL got='
              || coalesce(string_agg(id || '|' || amount, ',' ORDER BY id), '<null>')
       END
  FROM iceberg_snapshot_scan('tt_evo_b', :s3);

-- E5: the historical schemas surface the snapshot's own column NAMES; a
-- current-schema leak would make these references fail at parse time, so
-- reaching here with the counts right is the assertion.
SELECT CASE WHEN count(*) = 2 THEN 'ASSERT dense_rowcount: PASS'
            ELSE 'ASSERT dense_rowcount: FAIL got=' || count(*) END
  FROM iceberg_snapshot_scan('tt_evo_b', :s1);

-- E6: hand-writing a schema-CHANGED snapshot's alias on the Iceberg table
-- itself must be rejected by the CustomScan schema gate, never decoded under
-- the current tuple descriptor.  (A same-schema forged alias is by design a
-- legal historical read: table SELECT is the security boundary.)
DO $$
DECLARE
    sid   bigint;
BEGIN
    SELECT snapshot_id INTO sid
      FROM iceberg_snapshot_list('tt_evo_b') ORDER BY committed_at LIMIT 1;
    BEGIN
        EXECUTE format(
            'SELECT count(*) FROM tt_evo_b AS "__icetts_%s_1"', sid);
        RAISE WARNING 'forged_alias_rejected=f';
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'forged_alias_rejected=%',
            (position('differs from the current schema' in SQLERRM) > 0);
    END;
END $$;

-- E7: a plan prepared by a privileged role must re-check SELECT at every
-- execution of the fallback CustomScan (it replaced the SRF, whose body held
-- the runtime ACL check).
DROP ROLE IF EXISTS ttevo_reader;
CREATE ROLE ttevo_reader LOGIN;
GRANT USAGE ON SCHEMA public TO ttevo_reader;
-- A volume user mapping so the ONLY denial left is the relation ACL recheck.
CREATE USER MAPPING FOR ttevo_reader SERVER ttevo_vol_srv
    OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
-- The selector must be a parse-time constant, so interpolate the id (psql
-- variables survive from the \gset above; :s1 is the schema-changed snapshot
-- whose plan is the fallback CustomScan under review here).
PREPARE ttevo_p1 AS SELECT count(*) FROM iceberg_snapshot_scan('tt_evo_b', :s1);
SET ROLE ttevo_reader;
DO $$
BEGIN
    EXECUTE 'EXECUTE ttevo_p1';
    RAISE WARNING 'fallback_acl_recheck=f';
EXCEPTION WHEN insufficient_privilege THEN
    RAISE WARNING 'fallback_acl_recheck=t';
END $$;
RESET ROLE;
DEALLOCATE ttevo_p1;
DROP OWNED BY ttevo_reader;
DROP ROLE ttevo_reader;

\pset tuples_only off
\pset format aligned

-- cleanup
DROP TABLE tt_evo_b;
