-- Iceberg external-catalog metadata cache (issue #393)
--
-- Covers the per-table metadata_cache=N TTL on an external-catalog table:
-- which events stamp pg_ext_aux.pg_iceberg_metadata.metadata_cached_at, which
-- deliberately do not, and the read-only guard on persisting a refresh.
--
-- Expiry is forced by moving metadata_cached_at backwards rather than sleeping
-- out the TTL, so the case costs no wall-clock time.  Nothing prints a raw
-- timestamp -- it differs on every run.

-- Quiet the "already exists" notice: this file must produce identical output
-- whether it runs alone or after a sibling test that created the extension.
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
RESET client_min_messages;

-- ============================================================
-- Setup: Polaris REST catalog + S3 (MinIO) volume
-- ============================================================
CREATE SERVER metacache_vol_srv
FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (
    type 's3',
    endpoint 'http://minio:9000',
    region 'us-west-2',
    bucket_name 'warehouse',
    path_style_access 'true'
);
CREATE USER MAPPING FOR current_user
SERVER metacache_vol_srv
OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME metacache_vol SERVER metacache_vol_srv OPTIONS (base_path '/');
SET iceberg_default_volume = 'metacache_vol';

CREATE SERVER metacache_cat_srv
FOREIGN DATA WRAPPER iceberg_catalog_fdw
OPTIONS (type 'polaris', url 'http://polaris:8181/api/catalog');
CREATE USER MAPPING FOR current_user
SERVER metacache_cat_srv
OPTIONS (client_id 'root', client_secret 's3cr3t', scope 'PRINCIPAL_ROLE:ALL');
CREATE FOREIGN CATALOG metacache_cat
SERVER metacache_cat_srv
OPTIONS (catalog_name 'polaris_default_catalog');
SET iceberg_default_catalog = 'metacache_cat';

-- ============================================================
-- Test 1: only a read stamps the cache -- not CREATE, not DML
-- ============================================================
CREATE ICEBERG TABLE metacache_t (id bigint, v text)
OPTIONS (namespace 'public', table 'metacache_t', metadata_cache '5');

SELECT metadata_cached_at IS NULL AS stamp_null_after_create
FROM pg_ext_aux.pg_iceberg_metadata WHERE relid = 'metacache_t'::regclass;

-- DML resolves the head force-fresh and commits through the tracker's CAS,
-- which deliberately does not touch the cache stamp.
INSERT INTO metacache_t VALUES (1, 'a'), (2, 'b');

SELECT metadata_cached_at IS NULL AS stamp_null_after_dml
FROM pg_ext_aux.pg_iceberg_metadata WHERE relid = 'metacache_t'::regclass;

-- The first plain SELECT resolves the head and stamps it.
SELECT count(*) AS rows_seen FROM metacache_t;

SELECT metadata_cached_at IS NOT NULL AS stamp_seeded_by_read
FROM pg_ext_aux.pg_iceberg_metadata WHERE relid = 'metacache_t'::regclass;

-- ============================================================
-- Test 2: inside the TTL a read is served from the cache
--         (the agent echoes the head back and nothing is rewritten)
-- ============================================================
SELECT metadata_cached_at AS first_stamp
FROM pg_ext_aux.pg_iceberg_metadata WHERE relid = 'metacache_t'::regclass
\gset

SELECT count(*) AS rows_seen FROM metacache_t;

SELECT metadata_cached_at = :first_stamp AS stamp_unchanged_within_ttl
FROM pg_ext_aux.pg_iceberg_metadata WHERE relid = 'metacache_t'::regclass;

-- ============================================================
-- Test 3: past the TTL a writable read re-resolves and re-stamps
-- ============================================================
SET allow_system_table_mods = on;
UPDATE pg_ext_aux.pg_iceberg_metadata
   SET metadata_cached_at = metadata_cached_at - (6 * 60 * 1000)
 WHERE relid = 'metacache_t'::regclass;
RESET allow_system_table_mods;

SELECT metadata_cached_at AS aged_stamp
FROM pg_ext_aux.pg_iceberg_metadata WHERE relid = 'metacache_t'::regclass
\gset

SELECT count(*) AS rows_seen FROM metacache_t;

SELECT metadata_cached_at > :aged_stamp AS stamp_refreshed_after_ttl
FROM pg_ext_aux.pg_iceberg_metadata WHERE relid = 'metacache_t'::regclass;

-- ============================================================
-- Test 4: a read-only transaction resolves but cannot persist
-- ============================================================
SET allow_system_table_mods = on;
UPDATE pg_ext_aux.pg_iceberg_metadata
   SET metadata_cached_at = metadata_cached_at - (6 * 60 * 1000)
 WHERE relid = 'metacache_t'::regclass;
RESET allow_system_table_mods;

SELECT metadata_cached_at AS before_ro
FROM pg_ext_aux.pg_iceberg_metadata WHERE relid = 'metacache_t'::regclass
\gset

BEGIN;
SET TRANSACTION READ ONLY;
SELECT count(*) AS rows_seen_readonly FROM metacache_t;
COMMIT;

-- The read used a freshly resolved head, but the write-back is guarded by
-- !XactReadOnly, so the stamp stays where it was and the next read in such a
-- session pays the external-catalog round trip again.
SELECT metadata_cached_at = :before_ro AS stamp_unchanged_by_readonly_read
FROM pg_ext_aux.pg_iceberg_metadata WHERE relid = 'metacache_t'::regclass;

-- ============================================================
-- Test 5: metadata_cache = 0 (the default) never caches
-- ============================================================
CREATE ICEBERG TABLE metacache_off (id bigint)
OPTIONS (namespace 'public', table 'metacache_off');

INSERT INTO metacache_off VALUES (1);
SELECT count(*) AS rows_seen FROM metacache_off;

SELECT metadata_cached_at IS NULL AS stamp_null_when_ttl_zero
FROM pg_ext_aux.pg_iceberg_metadata WHERE relid = 'metacache_off'::regclass;

-- ============================================================
-- Test 6: internal tables have no external head to cache
-- ============================================================
CREATE ICEBERG TABLE metacache_internal (id bigint);
INSERT INTO metacache_internal VALUES (1);
SELECT count(*) AS rows_seen FROM metacache_internal;

SELECT is_internal, metadata_cached_at IS NULL AS stamp_null_when_internal
FROM pg_ext_aux.pg_iceberg_metadata WHERE relid = 'metacache_internal'::regclass;

-- ============================================================
-- Cleanup
-- ============================================================
DROP TABLE metacache_internal;
DROP TABLE metacache_off;
DROP TABLE metacache_t;
DROP CATALOG metacache_cat;
DROP VOLUME metacache_vol;
DROP SERVER metacache_cat_srv CASCADE;
DROP SERVER metacache_vol_srv CASCADE;
