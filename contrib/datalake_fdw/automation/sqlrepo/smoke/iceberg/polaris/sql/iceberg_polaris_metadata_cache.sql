-- Iceberg external-catalog metadata cache (issue #393)
--
-- Covers the per-table metadata_cache=N TTL on an external-catalog table: what
-- stamps pg_ext_aux.pg_iceberg_metadata.metadata_cached_at and what does not.
--
-- The rule this file pins down is that **a read never writes the mirror** --
-- not CREATE, not DML, not a plain SELECT, whether or not the transaction
-- could write.  The stamp moves only when something asks for a refresh
-- explicitly: iceberg_refresh_metadata_cache() on a schedule, or
-- iceberg_refresh_metadata() by hand.  A read past the TTL still resolves the
-- current head and returns correct rows; it just does not record it.
--
-- Expiry is forced by moving metadata_cached_at backwards rather than sleeping
-- out the TTL, so the case costs no wall-clock time.  Nothing prints a raw
-- timestamp -- it differs on every run.

-- Quiet the "already exists" notice: this file must produce identical output
-- whether it runs alone or after a sibling test that created the extension.
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
RESET client_min_messages;

-- Every external table with a TTL is a refresh candidate from the moment it is
-- created (a NULL stamp counts as infinitely stale), so an autovacuum worker
-- firing mid-file would stamp metacache_t and break the assertions below about
-- what does and does not move the stamp.  Restored at the start of cleanup.
ALTER SYSTEM SET datalake_fdw.metadata_refresh_enabled = off;
SELECT pg_reload_conf();

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
-- Test 1: nothing a reader does stamps the cache
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

-- A plain SELECT in an ordinary read-write transaction: resolves the head,
-- returns the rows, records nothing.
SELECT count(*) AS rows_seen FROM metacache_t;

SELECT metadata_cached_at IS NULL AS stamp_still_null_after_read
FROM pg_ext_aux.pg_iceberg_metadata WHERE relid = 'metacache_t'::regclass;

-- ============================================================
-- Test 2: the refresh is what stamps it
-- ============================================================
SELECT iceberg_refresh_metadata_cache() >= 1 AS refresh_stamped_something;

SELECT metadata_cached_at IS NOT NULL AS stamp_seeded_by_refresh
FROM pg_ext_aux.pg_iceberg_metadata WHERE relid = 'metacache_t'::regclass;

-- ============================================================
-- Test 3: reads leave the stamp alone, inside the TTL and past it
-- ============================================================
SELECT metadata_cached_at AS first_stamp
FROM pg_ext_aux.pg_iceberg_metadata WHERE relid = 'metacache_t'::regclass
\gset

-- Inside the TTL: the agent echoes the cached head back, no round trip.
SELECT count(*) AS rows_seen FROM metacache_t;

SELECT metadata_cached_at = :first_stamp AS stamp_unchanged_within_ttl
FROM pg_ext_aux.pg_iceberg_metadata WHERE relid = 'metacache_t'::regclass;

-- Past the TTL: the agent re-resolves, the query is served from the current
-- head -- and the stamp still does not move, so the next read pays the round
-- trip again until a refresh runs.
SET allow_system_table_mods = on;
UPDATE pg_ext_aux.pg_iceberg_metadata
   SET metadata_cached_at = metadata_cached_at - (6 * 60 * 1000)
 WHERE relid = 'metacache_t'::regclass;
RESET allow_system_table_mods;

SELECT metadata_cached_at AS aged_stamp
FROM pg_ext_aux.pg_iceberg_metadata WHERE relid = 'metacache_t'::regclass
\gset

SELECT count(*) AS rows_seen FROM metacache_t;

SELECT metadata_cached_at = :aged_stamp AS stamp_unchanged_past_ttl
FROM pg_ext_aux.pg_iceberg_metadata WHERE relid = 'metacache_t'::regclass;

-- ============================================================
-- Test 4: a read-only transaction behaves exactly like a read-write one
--         (it always did read correctly; now neither one writes)
-- ============================================================
BEGIN;
SET TRANSACTION READ ONLY;
SELECT count(*) AS rows_seen_readonly FROM metacache_t;
COMMIT;

SELECT metadata_cached_at = :aged_stamp AS stamp_unchanged_by_readonly_read
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

-- Before the DROPs, not after: this GUC is cluster-wide and outlives the
-- session, so nothing in teardown should get a chance to strand it off.
ALTER SYSTEM RESET datalake_fdw.metadata_refresh_enabled;
SELECT pg_reload_conf();

DROP TABLE metacache_internal;
DROP TABLE metacache_off;
DROP TABLE metacache_t;
DROP CATALOG metacache_cat;
DROP VOLUME metacache_vol;
DROP SERVER metacache_cat_srv CASCADE;
DROP SERVER metacache_vol_srv CASCADE;
