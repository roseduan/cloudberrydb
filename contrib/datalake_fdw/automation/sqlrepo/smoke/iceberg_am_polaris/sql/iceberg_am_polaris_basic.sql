-- Polaris-catalog basic functional smoke for Iceberg AM
--
-- Verifies CREATE ICEBERG TABLE / INSERT / SELECT / UPDATE / DELETE work
-- end-to-end against a Polaris REST catalog + S3 (MinIO) volume.
--
-- The Polaris-managed table goes through a different code path than the
-- builtin catalog: the C-side tracker treats is_internal=true tables on a
-- non-builtin catalog as "external for commit purposes" and routes through
-- the dedicated commitAppend / commitUpdate agent endpoints so Polaris's
-- current-metadata pointer actually advances. Regression coverage for the
-- ebdd74af903 fix.

\i ../../../lib/sql/common_setup.sql

-- ===== Polaris REST catalog server + S3 volume =====
-- Volume server must exist before FOREIGN CATALOG creation: the catalog FDW
-- derives Polaris's storageConfigInfo from the bound volume, and Polaris
-- rejects catalog creation with "storageConfig cannot be null or empty" when
-- it's missing.
DROP SERVER IF EXISTS polaris_smoke_vol_srv CASCADE;
CREATE SERVER polaris_smoke_vol_srv FOREIGN DATA WRAPPER iceberg_volume_fdw
    OPTIONS (
        type 's3',
        endpoint 'http://minio:9000',
        region 'us-east-1',
        bucket_name 'warehouse',
        path_style_access 'true'
    );
CREATE USER MAPPING FOR current_user SERVER polaris_smoke_vol_srv
    OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME polaris_smoke_vol SERVER polaris_smoke_vol_srv
    OPTIONS (base_path '/polaris_smoke/', allow_writes 'true');
SET iceberg_default_volume = 'polaris_smoke_vol';

DROP SERVER IF EXISTS polaris_smoke_cat_srv CASCADE;
CREATE SERVER polaris_smoke_cat_srv FOREIGN DATA WRAPPER iceberg_catalog_fdw
    OPTIONS (type 'polaris', url 'http://polaris:8181/api/catalog');
CREATE USER MAPPING FOR current_user SERVER polaris_smoke_cat_srv
    OPTIONS (client_id 'root', client_secret 's3cr3t', scope 'PRINCIPAL_ROLE:ALL');
CREATE FOREIGN CATALOG polaris_smoke_cat SERVER polaris_smoke_cat_srv
    OPTIONS (catalog_name 'polaris_default_catalog', default_namespace 'public');
SET iceberg_default_catalog = 'polaris_smoke_cat';

-- ===== Schema + initial INSERT =====
DROP TABLE IF EXISTS polaris_smoke;
CREATE ICEBERG TABLE polaris_smoke (id INT, name VARCHAR(50), val DECIMAL(10,2));

\echo === T1 INSERT seed (5 rows) ===
INSERT INTO polaris_smoke
    VALUES (1, 'alpha',   1.50),
           (2, 'beta',    2.75),
           (3, 'gamma',   3.14),
           (4, 'delta',   4.20),
           (5, 'epsilon', 5.00);
SELECT count(*) AS after_seed FROM polaris_smoke;

-- ===== T2 single-row UPDATE =====
\echo === T2 single-row UPDATE id=2 ===
UPDATE polaris_smoke SET val = 99.99 WHERE id = 2;
SELECT id, name, val FROM polaris_smoke WHERE id = 2;

-- ===== T3 single-row DELETE =====
\echo === T3 single-row DELETE id=4 ===
DELETE FROM polaris_smoke WHERE id = 4;
SELECT count(*) AS after_delete FROM polaris_smoke;

-- ===== T4 incremental INSERT =====
\echo === T4 incremental INSERT (2 rows) ===
INSERT INTO polaris_smoke VALUES (6, 'zeta', 6.66), (7, 'eta', 7.77);
SELECT count(*) AS after_second_insert FROM polaris_smoke;

-- ===== T5 multi-row UPDATE =====
\echo === T5 multi-row UPDATE id<=3 (3 rows) ===
UPDATE polaris_smoke SET name = name || '_v2' WHERE id <= 3;

-- ===== Final state should be exactly 6 rows in this layout =====
\echo === final ordered state ===
SELECT id, name, val FROM polaris_smoke ORDER BY id;

\echo === final aggregate ===
SELECT count(*) AS final_rows,
       count(*) FILTER (WHERE name LIKE '%\_v2' ESCAPE '\') AS rows_with_v2_suffix,
       sum(val) AS sum_val
FROM polaris_smoke;

-- ===== Cleanup =====
DROP TABLE polaris_smoke;
