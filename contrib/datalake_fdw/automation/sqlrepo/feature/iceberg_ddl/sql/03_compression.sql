-- 03_compression.sql
-- Test native Iceberg AM parquet write compression (per-table options
-- `compression` and `compression_level`).
--
-- Coverage:
--   * default codec is zstd (writes are compressed unless opted out)
--   * every supported parquet codec round-trips (write + read back)
--   * compression_level is accepted for codecs that support it (zstd/gzip)
--   * invalid codec / level-on-unsupported-codec / out-of-range level are
--     rejected with an ERROR at write time (not silently downgraded)

\i ../../../lib/sql/common_setup.sql

SELECT test_log('Feature Test: Iceberg Write Compression');

-- ============================================================
-- Setup: Catalog and volume
-- ============================================================
CREATE SERVER co_catalog_server FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER co_catalog_server;
CREATE FOREIGN CATALOG co_catalog SERVER co_catalog_server;
SET iceberg_default_catalog = 'co_catalog';

CREATE SERVER co_volume_server FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (type 's3', endpoint 'http://minio:9000', region 'us-east-1',
         bucket_name 'warehouse', path_style_access 'true');
CREATE USER MAPPING FOR current_user SERVER co_volume_server
OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME co_volume SERVER co_volume_server OPTIONS(base_path '/co_volume/');
SET iceberg_default_volume = 'co_volume';

-- ============================================================
-- Test 1: default compression (zstd) when no option is given
-- ============================================================
SELECT test_log('Test 1: default compression (zstd)');

CREATE ICEBERG TABLE co_default (id bigint, val text);
INSERT INTO co_default SELECT g, 'row_' || g FROM generate_series(1, 50) g;
SELECT count(*) AS n, min(id) AS lo, max(id) AS hi FROM co_default;
SELECT * FROM co_default ORDER BY id LIMIT 3;
DROP TABLE co_default;

-- ============================================================
-- Test 2: each supported parquet codec round-trips
-- ============================================================
SELECT test_log('Test 2: explicit codecs round-trip');

CREATE ICEBERG TABLE co_snappy (id bigint, val text) OPTIONS (compression 'snappy');
INSERT INTO co_snappy SELECT g, 'snappy_' || g FROM generate_series(1, 20) g;
SELECT count(*) AS snappy_n FROM co_snappy;
SELECT * FROM co_snappy ORDER BY id LIMIT 2;
DROP TABLE co_snappy;

CREATE ICEBERG TABLE co_gzip (id bigint, val text) OPTIONS (compression 'gzip');
INSERT INTO co_gzip SELECT g, 'gzip_' || g FROM generate_series(1, 20) g;
SELECT count(*) AS gzip_n FROM co_gzip;
DROP TABLE co_gzip;

CREATE ICEBERG TABLE co_lz4 (id bigint, val text) OPTIONS (compression 'lz4');
INSERT INTO co_lz4 SELECT g, 'lz4_' || g FROM generate_series(1, 20) g;
SELECT count(*) AS lz4_n FROM co_lz4;
DROP TABLE co_lz4;

CREATE ICEBERG TABLE co_none (id bigint, val text) OPTIONS (compression 'uncompress');
INSERT INTO co_none SELECT g, 'none_' || g FROM generate_series(1, 20) g;
SELECT count(*) AS none_n FROM co_none;
DROP TABLE co_none;

-- ============================================================
-- Test 3: compression_level for codecs that support one
-- ============================================================
SELECT test_log('Test 3: compression_level (zstd / gzip)');

CREATE ICEBERG TABLE co_zstd_lvl (id bigint, val text)
OPTIONS (compression 'zstd', compression_level '12');
INSERT INTO co_zstd_lvl SELECT g, 'zstd12_' || g FROM generate_series(1, 20) g;
SELECT count(*) AS zstd_lvl_n FROM co_zstd_lvl;
DROP TABLE co_zstd_lvl;

CREATE ICEBERG TABLE co_gzip_lvl (id bigint, val text)
OPTIONS (compression 'gzip', compression_level '6');
INSERT INTO co_gzip_lvl SELECT g, 'gzip6_' || g FROM generate_series(1, 20) g;
SELECT count(*) AS gzip_lvl_n FROM co_gzip_lvl;
DROP TABLE co_gzip_lvl;

-- ============================================================
-- Test 4: invalid codec is rejected at write time
-- ============================================================
SELECT test_log('Test 4: invalid codec rejected');

CREATE ICEBERG TABLE co_bad_codec (id bigint, val text)
OPTIONS (compression 'bogus');
DO $$
BEGIN
    INSERT INTO co_bad_codec VALUES (1, 'x');
    RAISE NOTICE 'UNEXPECTED: insert succeeded';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error: %', split_part(SQLERRM, E'\n', 1);
END $$;
DROP TABLE co_bad_codec;

-- ============================================================
-- Test 5: compression_level on a codec without levels is rejected
-- ============================================================
SELECT test_log('Test 5: level on snappy rejected');

CREATE ICEBERG TABLE co_snappy_lvl (id bigint, val text)
OPTIONS (compression 'snappy', compression_level '5');
DO $$
BEGIN
    INSERT INTO co_snappy_lvl VALUES (1, 'x');
    RAISE NOTICE 'UNEXPECTED: insert succeeded';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error: %', split_part(SQLERRM, E'\n', 1);
END $$;
DROP TABLE co_snappy_lvl;

-- ============================================================
-- Test 6: out-of-range compression_level is rejected
-- ============================================================
SELECT test_log('Test 6: out-of-range zstd level rejected');

CREATE ICEBERG TABLE co_zstd_bad (id bigint, val text)
OPTIONS (compression 'zstd', compression_level '99');
DO $$
BEGIN
    INSERT INTO co_zstd_bad VALUES (1, 'x');
    RAISE NOTICE 'UNEXPECTED: insert succeeded';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error: %', split_part(SQLERRM, E'\n', 1);
END $$;
DROP TABLE co_zstd_bad;

-- ============================================================
-- Cleanup
-- ============================================================
DROP VOLUME co_volume;
DROP USER MAPPING FOR current_user SERVER co_volume_server;
DROP SERVER co_volume_server;
DROP CATALOG co_catalog;
DROP USER MAPPING FOR current_user SERVER co_catalog_server;
DROP SERVER co_catalog_server;
