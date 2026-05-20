-- Iceberg Fragment Cache Test
-- Purpose: Exercise fragment caching, cache hit/miss/eviction paths
-- Target: dlproxy/iceberg_fragment_cache.c (+42), dlproxy/iceberg.c (+60),
--         dlproxy/icebergConfig.c (+112), hdw_gopher_cache.c (+76)

CREATE EXTENSION IF NOT EXISTS datalake_fdw;

CREATE SERVER fc_catalog_server FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER fc_catalog_server;
CREATE FOREIGN CATALOG fc_catalog SERVER fc_catalog_server;
SET iceberg_default_catalog = 'fc_catalog';

CREATE SERVER fc_volume_server FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (type 's3', endpoint 'http://minio:9000', region 'us-east-1',
         bucket_name 'warehouse', path_style_access 'true');
CREATE USER MAPPING FOR current_user SERVER fc_volume_server
OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME fc_volume SERVER fc_volume_server OPTIONS(base_path '/fc_volume/');
SET iceberg_default_volume = 'fc_volume';

-- ============================================================
-- Test 1: Basic fragment cache (enable cache, query twice)
-- ============================================================
SET datalake.enable_iceberg_fragment_cache = on;

CREATE ICEBERG TABLE fc_cache1 (id bigint, val text);
INSERT INTO fc_cache1 SELECT i, 'data_' || i FROM generate_series(1, 100) i;

-- First query: cache miss, stores fragments
SELECT COUNT(*) FROM fc_cache1;

-- Second query: cache hit
SELECT COUNT(*) FROM fc_cache1;

-- Third query with filter: still cache hit (fragments cached)
SELECT COUNT(*) FROM fc_cache1 WHERE id > 50;

-- ============================================================
-- Test 2: Cache invalidation (modify table -> cache miss)
-- ============================================================
INSERT INTO fc_cache1 SELECT i, 'new_' || i FROM generate_series(101, 200) i;

-- Should be cache miss (table changed)
SELECT COUNT(*) FROM fc_cache1;

-- ============================================================
-- Test 3: Cache eviction (query many tables to exceed cache capacity)
-- ============================================================
CREATE ICEBERG TABLE fc_evict1 (id bigint);
INSERT INTO fc_evict1 VALUES (1);
SELECT COUNT(*) FROM fc_evict1;

CREATE ICEBERG TABLE fc_evict2 (id bigint);
INSERT INTO fc_evict2 VALUES (1);
SELECT COUNT(*) FROM fc_evict2;

CREATE ICEBERG TABLE fc_evict3 (id bigint);
INSERT INTO fc_evict3 VALUES (1);
SELECT COUNT(*) FROM fc_evict3;

CREATE ICEBERG TABLE fc_evict4 (id bigint);
INSERT INTO fc_evict4 VALUES (1);
SELECT COUNT(*) FROM fc_evict4;

CREATE ICEBERG TABLE fc_evict5 (id bigint);
INSERT INTO fc_evict5 VALUES (1);
SELECT COUNT(*) FROM fc_evict5;

CREATE ICEBERG TABLE fc_evict6 (id bigint);
INSERT INTO fc_evict6 VALUES (1);
SELECT COUNT(*) FROM fc_evict6;

CREATE ICEBERG TABLE fc_evict7 (id bigint);
INSERT INTO fc_evict7 VALUES (1);
SELECT COUNT(*) FROM fc_evict7;

CREATE ICEBERG TABLE fc_evict8 (id bigint);
INSERT INTO fc_evict8 VALUES (1);
SELECT COUNT(*) FROM fc_evict8;

CREATE ICEBERG TABLE fc_evict9 (id bigint);
INSERT INTO fc_evict9 VALUES (1);
SELECT COUNT(*) FROM fc_evict9;

CREATE ICEBERG TABLE fc_evict10 (id bigint);
INSERT INTO fc_evict10 VALUES (1);
SELECT COUNT(*) FROM fc_evict10;

CREATE ICEBERG TABLE fc_evict11 (id bigint);
INSERT INTO fc_evict11 VALUES (1);
SELECT COUNT(*) FROM fc_evict11;

CREATE ICEBERG TABLE fc_evict12 (id bigint);
INSERT INTO fc_evict12 VALUES (1);
SELECT COUNT(*) FROM fc_evict12;

-- Go back to first table (should have been evicted from cache)
SELECT COUNT(*) FROM fc_cache1;

-- ============================================================
-- Test 4: Cache disabled (no caching)
-- ============================================================
SET datalake.enable_iceberg_fragment_cache = off;

SELECT COUNT(*) FROM fc_cache1;
SELECT COUNT(*) FROM fc_cache1;

-- Re-enable for next tests
SET datalake.enable_iceberg_fragment_cache = on;

-- ============================================================
-- Test 5: Cache with DELETE (cache should be invalidated)
-- ============================================================
DELETE FROM fc_cache1 WHERE id > 150;
SELECT COUNT(*) FROM fc_cache1;

-- ============================================================
-- Cleanup
-- ============================================================
DROP TABLE fc_cache1;
DROP TABLE fc_evict1;
DROP TABLE fc_evict2;
DROP TABLE fc_evict3;
DROP TABLE fc_evict4;
DROP TABLE fc_evict5;
DROP TABLE fc_evict6;
DROP TABLE fc_evict7;
DROP TABLE fc_evict8;
DROP TABLE fc_evict9;
DROP TABLE fc_evict10;
DROP TABLE fc_evict11;
DROP TABLE fc_evict12;

RESET datalake.enable_iceberg_fragment_cache;

DROP VOLUME fc_volume;
DROP USER MAPPING FOR current_user SERVER fc_volume_server;
DROP SERVER fc_volume_server;
DROP CATALOG fc_catalog;
DROP USER MAPPING FOR current_user SERVER fc_catalog_server;
DROP SERVER fc_catalog_server;
