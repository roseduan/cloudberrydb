-- Iceberg Volume FDW Operations Test
-- Purpose: Exercise iceberg_toolkit_volume_fdw.c scan/write paths
-- Target: iceberg_toolkit_volume_fdw.c (+238), iceberg_volume_fdw.c (+66),
--         iceberg_volume_option.c, provider paths

CREATE EXTENSION IF NOT EXISTS datalake_fdw;

CREATE SERVER vo_catalog_server FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER vo_catalog_server;
CREATE FOREIGN CATALOG vo_catalog SERVER vo_catalog_server;
SET iceberg_default_catalog = 'vo_catalog';

CREATE SERVER vo_volume_server FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (type 's3', endpoint 'http://minio:9000', region 'us-east-1',
         bucket_name 'warehouse', path_style_access 'true');
CREATE USER MAPPING FOR current_user SERVER vo_volume_server
OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME vo_volume SERVER vo_volume_server OPTIONS(base_path '/vo_volume/');
SET iceberg_default_volume = 'vo_volume';

-- ============================================================
-- Test 1: Basic scan path with various data sizes
-- ============================================================
CREATE ICEBERG TABLE vo_scan (id bigint, data text);

-- Multiple small inserts to create multiple files (fragments)
INSERT INTO vo_scan SELECT i, 'row_' || i FROM generate_series(1, 100) i;
INSERT INTO vo_scan SELECT i, 'row_' || i FROM generate_series(101, 200) i;
INSERT INTO vo_scan SELECT i, 'row_' || i FROM generate_series(201, 300) i;

-- Multi-fragment scan
SELECT COUNT(*) FROM vo_scan;

-- Scan with WHERE pushdown
SELECT COUNT(*) FROM vo_scan WHERE id > 200;
SELECT COUNT(*) FROM vo_scan WHERE id BETWEEN 50 AND 150;

-- Column projection (only id)
SELECT id FROM vo_scan WHERE id <= 5 ORDER BY id;

-- Aggregate scan
SELECT MIN(id), MAX(id), COUNT(*) FROM vo_scan;

DROP TABLE vo_scan;

-- ============================================================
-- Test 2: INSERT batch sizes (APPEND operation path)
-- ============================================================
CREATE ICEBERG TABLE vo_append (id bigint, val decimal(10,2), name text);

-- Single row insert
INSERT INTO vo_append VALUES (1, 10.50, 'single');

-- Multi-row insert
INSERT INTO vo_append VALUES (2, 20.50, 'multi_a'), (3, 30.50, 'multi_b'), (4, 40.50, 'multi_c');

-- Bulk insert from generate_series
INSERT INTO vo_append SELECT i, (i * 1.5)::decimal(10,2), 'bulk_' || i FROM generate_series(5, 50) i;

SELECT COUNT(*) FROM vo_append;
SELECT * FROM vo_append WHERE id <= 4 ORDER BY id;

DROP TABLE vo_append;

-- ============================================================
-- Test 3: UPDATE operation path
-- ============================================================
CREATE ICEBERG TABLE vo_update (id bigint, status text, counter int);

INSERT INTO vo_update VALUES (1, 'active', 100), (2, 'active', 200),
    (3, 'inactive', 300), (4, 'active', 400), (5, 'inactive', 500);

-- Single row update
UPDATE vo_update SET status = 'updated', counter = counter + 1 WHERE id = 1;

-- Multi-row update
UPDATE vo_update SET status = 'batch_updated' WHERE status = 'active';

-- Update with expression
UPDATE vo_update SET counter = counter * 2 WHERE id <= 3;

SELECT * FROM vo_update ORDER BY id;
SELECT COUNT(*) FROM vo_update WHERE status = 'batch_updated';

DROP TABLE vo_update;

-- ============================================================
-- Test 4: INSERT/UPDATE/DELETE/SELECT interleaving
-- ============================================================
CREATE ICEBERG TABLE vo_interleave (id bigint, val text);

INSERT INTO vo_interleave SELECT i, 'v' || i FROM generate_series(1, 50) i;
SELECT COUNT(*) FROM vo_interleave;

UPDATE vo_interleave SET val = 'updated_' || val WHERE id % 10 = 0;
SELECT COUNT(*) FROM vo_interleave WHERE val LIKE 'updated_%';

DELETE FROM vo_interleave WHERE id > 40;
SELECT COUNT(*) FROM vo_interleave;

INSERT INTO vo_interleave SELECT i + 100, 'new_' || i FROM generate_series(1, 10) i;
SELECT COUNT(*) FROM vo_interleave;

-- Final verification
SELECT * FROM vo_interleave WHERE id <= 5 ORDER BY id;
SELECT * FROM vo_interleave WHERE id > 100 ORDER BY id;

DROP TABLE vo_interleave;

-- ============================================================
-- Test 5: Wide table with many columns (exercises column mapping)
-- ============================================================
CREATE ICEBERG TABLE vo_wide (
    c1 bigint, c2 text, c3 int, c4 boolean, c5 decimal(10,2),
    c6 date, c7 timestamp, c8 real, c9 double precision, c10 smallint,
    c11 text, c12 bigint, c13 int, c14 boolean, c15 decimal(15,4)
);

INSERT INTO vo_wide VALUES (
    1, 'text1', 100, true, 10.50,
    '2024-01-01', '2024-01-01 12:00:00', 1.5, 2.5, 10,
    'text11', 1000, 200, false, 1234.5678
);
INSERT INTO vo_wide VALUES (
    2, 'text2', 200, false, 20.50,
    '2024-06-15', '2024-06-15 18:30:00', 3.5, 4.5, 20,
    'text22', 2000, 300, true, 9876.5432
);

SELECT COUNT(*) FROM vo_wide;
SELECT c1, c2, c5, c6 FROM vo_wide ORDER BY c1;
SELECT c10, c11, c12, c15 FROM vo_wide ORDER BY c1;

DROP TABLE vo_wide;

-- ============================================================
-- Cleanup
-- ============================================================
DROP VOLUME vo_volume;
DROP USER MAPPING FOR current_user SERVER vo_volume_server;
DROP SERVER vo_volume_server;
DROP CATALOG vo_catalog;
DROP USER MAPPING FOR current_user SERVER vo_catalog_server;
DROP SERVER vo_catalog_server;
