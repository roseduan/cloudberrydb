-- Conf-file mode smoke: server_name section merged under SQL OPTIONS.
--
-- Exercises the per-key merge semantics on the s3 volume path (iceberg
-- guide 4.3 "配置文件模式"):
--   1. conf fallback   -- connection + credentials come entirely from the
--                         s3.conf section, the DDL carries only
--                         type/server_name/bucket_name;
--   2. SQL wins        -- correct USER MAPPING credentials override broken
--                         credentials in the conf section (full data path);
--   3. negative control-- the same broken conf section without SQL
--                         credentials fails, proving the conf values are
--                         what case 2 overrode;
--   4. legacy keys     -- a section still written in the old fs.s3a.*
--                         vocabulary is rejected with a migration hint.
--
-- run.sh deploys the merge_* sections into the QD data directory s3.conf
-- (the datalake_agent working directory) before psql runs this file.
--
-- The conf-fallback cases stop at CREATE/DROP: those run on the QD + agent
-- metadata plane, which is where conf reading is wired today. The QE data
-- plane still requires inline connection options (case 2 provides them),
-- so INSERT/SELECT only appear there.

\i ../../../lib/sql/common_setup.sql

CREATE SERVER cm_catalog_server FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER cm_catalog_server;
CREATE FOREIGN CATALOG cm_catalog SERVER cm_catalog_server;
SET iceberg_default_catalog = 'cm_catalog';

-- ============================================================
-- Case 1: conf fallback -- DDL has no endpoint and no credentials
-- ============================================================
\echo === case 1: conf-only connection (endpoint + credentials from s3.conf) ===
CREATE SERVER cm_conf_vol_srv FOREIGN DATA WRAPPER iceberg_volume_fdw
    OPTIONS (type 's3', server_name 'merge_minio', bucket_name 'warehouse');
CREATE USER MAPPING FOR current_user SERVER cm_conf_vol_srv;
CREATE FOREIGN VOLUME cm_conf_vol SERVER cm_conf_vol_srv
    OPTIONS (base_path '/iceberg_conf_merge/', allow_writes 'true');
SET iceberg_default_volume = 'cm_conf_vol';

-- CREATE/DROP only: even an empty-table SELECT initializes the QE gopher
-- connection, which still requires inline options (conf wiring pending).
CREATE ICEBERG TABLE cm_conf_only (id INT, name VARCHAR(20));
DROP TABLE cm_conf_only;

-- ============================================================
-- Case 2: SQL OPTIONS win -- USER MAPPING credentials override the
-- broken secret in the merge_badcreds section
-- ============================================================
\echo === case 2: SQL credentials override broken conf credentials ===
CREATE SERVER cm_sqlwins_vol_srv FOREIGN DATA WRAPPER iceberg_volume_fdw
    OPTIONS (
        type 's3',
        server_name 'merge_badcreds',
        endpoint 'http://minio:9000',
        region 'us-east-1',
        bucket_name 'warehouse',
        path_style_access 'true'
    );
CREATE USER MAPPING FOR current_user SERVER cm_sqlwins_vol_srv
    OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME cm_sqlwins_vol SERVER cm_sqlwins_vol_srv
    OPTIONS (base_path '/iceberg_conf_merge/', allow_writes 'true');
SET iceberg_default_volume = 'cm_sqlwins_vol';

CREATE ICEBERG TABLE cm_sql_wins (id INT, name VARCHAR(20));
INSERT INTO cm_sql_wins VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma');
SELECT count(*) AS after_insert FROM cm_sql_wins;
SELECT id, name FROM cm_sql_wins ORDER BY id;
DROP TABLE cm_sql_wins;

-- ============================================================
-- Case 3: negative control -- the same broken section without SQL
-- credentials must fail, proving case 2 really overrode conf values
-- ============================================================
\echo === case 3: broken conf credentials fail without SQL override ===
CREATE SERVER cm_badcreds_vol_srv FOREIGN DATA WRAPPER iceberg_volume_fdw
    OPTIONS (type 's3', server_name 'merge_badcreds', bucket_name 'warehouse');
CREATE USER MAPPING FOR current_user SERVER cm_badcreds_vol_srv;
CREATE FOREIGN VOLUME cm_badcreds_vol SERVER cm_badcreds_vol_srv
    OPTIONS (base_path '/iceberg_conf_merge/', allow_writes 'true');
SET iceberg_default_volume = 'cm_badcreds_vol';

DO $$
BEGIN
    EXECUTE 'CREATE ICEBERG TABLE cm_bad_creds (id INT)';
    RAISE WARNING 'UNEXPECTED: create succeeded with broken conf credentials';
    EXECUTE 'DROP TABLE cm_bad_creds';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'create failed as expected with broken conf credentials';
END $$;

-- ============================================================
-- Case 4: legacy fs.s3a.* keys in the section are a hard error
-- ============================================================
\echo === case 4: legacy keys in s3.conf are rejected ===
CREATE SERVER cm_legacy_vol_srv FOREIGN DATA WRAPPER iceberg_volume_fdw
    OPTIONS (type 's3', server_name 'merge_legacy', bucket_name 'warehouse');
CREATE USER MAPPING FOR current_user SERVER cm_legacy_vol_srv;
CREATE FOREIGN VOLUME cm_legacy_vol SERVER cm_legacy_vol_srv
    OPTIONS (base_path '/iceberg_conf_merge/', allow_writes 'true');
SET iceberg_default_volume = 'cm_legacy_vol';

DO $$
DECLARE
    msg text;
BEGIN
    EXECUTE 'CREATE ICEBERG TABLE cm_legacy (id INT)';
    RAISE WARNING 'UNEXPECTED: create succeeded with legacy conf keys';
    EXECUTE 'DROP TABLE cm_legacy';
EXCEPTION WHEN OTHERS THEN
    msg := SQLERRM;
    IF position('legacy key' in msg) > 0 THEN
        RAISE NOTICE 'legacy keys rejected with migration hint as expected';
    ELSE
        RAISE WARNING 'UNEXPECTED error without legacy-key hint: %', msg;
    END IF;
END $$;

-- ============================================================
-- Cleanup
-- ============================================================
RESET iceberg_default_volume;
RESET iceberg_default_catalog;
DROP SERVER cm_legacy_vol_srv CASCADE;
DROP SERVER cm_badcreds_vol_srv CASCADE;
DROP SERVER cm_sqlwins_vol_srv CASCADE;
DROP SERVER cm_conf_vol_srv CASCADE;
DROP SERVER cm_catalog_server CASCADE;
