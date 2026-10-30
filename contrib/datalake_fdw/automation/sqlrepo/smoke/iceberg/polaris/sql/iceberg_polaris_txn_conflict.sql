-- Transactions against a table whose schema another engine moves, and
-- concurrent writers (issue #461, design points 8, 10 and 13).
--
-- There is no Spark here.  The other engine moves the schema the way any
-- engine can: it drops the table from Polaris (its REST API, called through
-- COPY FROM PROGRAM) and creates it again with other columns -- here by
-- mounting a definition with only `id`, which creates a table that does not
-- exist yet.  The writer whose transaction holds the old schema is a second
-- connection (dblink), so that its COMMIT's error can be caught.
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
CREATE EXTENSION IF NOT EXISTS dblink;
RESET client_min_messages;

CREATE SERVER cfx_vol_srv
FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (
    type 's3',
    endpoint 'http://minio:9000',
    region 'us-east-1',
    bucket_name 'warehouse',
    path_style_access 'true'
);
CREATE USER MAPPING FOR current_user
SERVER cfx_vol_srv
OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME cfx_vol SERVER cfx_vol_srv OPTIONS (base_path '/');

CREATE SERVER cfx_cat_srv
FOREIGN DATA WRAPPER iceberg_catalog_fdw
OPTIONS (type 'polaris', url 'http://polaris:8181/api/catalog');
CREATE USER MAPPING FOR current_user
SERVER cfx_cat_srv
OPTIONS (client_id 'root', client_secret 's3cr3t', scope 'PRINCIPAL_ROLE:ALL');
CREATE FOREIGN CATALOG cfx_cat
SERVER cfx_cat_srv
OPTIONS (catalog_name 'polaris_default_catalog');
SET iceberg_default_volume = 'cfx_vol';
SET iceberg_default_catalog = 'cfx_cat';

-- The agent's error carries a Java stack trace; keep its message only.
CREATE FUNCTION cfx_try(stmt text) RETURNS text LANGUAGE plpgsql AS $$
BEGIN
    EXECUTE stmt;
    RETURN 'ok';
EXCEPTION WHEN OTHERS THEN
    RETURN 'ERROR: ' || coalesce(substring(SQLERRM FROM 'IllegalArgumentException: ([^\n]*)'),
                                 split_part(SQLERRM, E'\n', 1));
END;
$$;

-- The same for a statement run on the dblink connection: dblink reports the
-- remote error's message as the local error's detail.
CREATE FUNCTION cfx_remote(conn text, stmt text) RETURNS text LANGUAGE plpgsql AS $$
DECLARE
    msg text;
    detail text;
BEGIN
    PERFORM dblink_exec(conn, stmt);
    RETURN 'ok';
EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS msg = MESSAGE_TEXT, detail = PG_EXCEPTION_DETAIL;
    RETURN 'ERROR: ' || coalesce(substring(msg || E'\n' || coalesce(detail, '') FROM 'IllegalArgumentException: ([^\n]*)'),
                                 nullif(split_part(coalesce(detail, ''), E'\n', 1), ''),
                                 split_part(msg, E'\n', 1));
END;
$$;

-- Drop a table from Polaris directly, as another engine would; returns the
-- HTTP status (204).
CREATE TEMP TABLE cfx_out (line text) DISTRIBUTED RANDOMLY;
CREATE FUNCTION cfx_polaris_drop(tbl text) RETURNS text LANGUAGE plpgsql AS $$
DECLARE
    cmd text;
BEGIN
    cmd := 'T=$(curl -s -X POST http://polaris:8181/api/catalog/v1/oauth/tokens'
        || ' -d grant_type=client_credentials -d client_id=root -d client_secret=s3cr3t'
        || ' -d scope=PRINCIPAL_ROLE:ALL'
        || ' | sed -E ''s/.*"access_token":"([^"]*)".*/\1/'');'
        || ' curl -s -o /dev/null -w ''%{http_code}'' -X DELETE -H "Authorization: Bearer $T"'
        || ' http://polaris:8181/api/catalog/v1/polaris_default_catalog/namespaces/public/tables/'
        || tbl;
    TRUNCATE cfx_out;
    EXECUTE format('COPY cfx_out FROM PROGRAM %L', cmd);
    RETURN (SELECT line FROM cfx_out);
END;
$$;

SELECT format('dbname=%s port=%s', current_database(), current_setting('port')) AS other
\gset
SELECT dblink_connect('w', :'other');

-- ============================================================
-- A. A transaction writes against the old schema; the schema moves before
--    its COMMIT, which is refused (point 13) and leaves nothing behind.
-- ============================================================
CREATE ICEBERG TABLE cfx_a (id int, v text) OPTIONS (metadata_cache '0');
INSERT INTO cfx_a VALUES (1, 'a');

SELECT cfx_remote('w', 'BEGIN');
SELECT cfx_remote('w', 'INSERT INTO cfx_a VALUES (2, ''b'')');

-- Another engine drops the table and creates it again with only `id`.
SELECT cfx_polaris_drop('cfx_a') AS dropped;
CREATE ICEBERG TABLE cfx_a2 (id int) OPTIONS (namespace 'public', table 'cfx_a');

SELECT cfx_remote('w', 'COMMIT');
SELECT count(*) AS rows_committed FROM cfx_a2;

-- ============================================================
-- B. Once the schema has moved, the old definition can neither write
--    (point 10) nor read (point 8).
-- ============================================================
SELECT cfx_remote('w', 'INSERT INTO cfx_a VALUES (3, ''c'')');
-- The read check needs a snapshot to read: the other engine writes one.
INSERT INTO cfx_a2 VALUES (3);
SELECT cfx_try('SELECT count(*) FROM cfx_a');
SELECT count(*) AS rows_committed FROM cfx_a2;

-- ============================================================
-- C. Two transactions of this cluster writing the same table are serialized
--    by its table lock: the second waits for the first instead of committing
--    a conflicting snapshot, and no update is lost.
-- ============================================================
CREATE ICEBERG TABLE cfx_c (id int, v text) OPTIONS (metadata_cache '0');
INSERT INTO cfx_c VALUES (1, 'a'), (2, 'b');

SELECT cfx_remote('w', 'BEGIN');
SELECT cfx_remote('w', 'UPDATE cfx_c SET v = ''first'' WHERE id = 1');
SET lock_timeout = '2s';
SELECT cfx_try('UPDATE cfx_c SET v = ''second'' WHERE id = 1');
RESET lock_timeout;
SELECT cfx_remote('w', 'COMMIT');
SELECT id, v FROM cfx_c ORDER BY id;

-- ============================================================
-- Cleanup
-- ============================================================
SELECT dblink_disconnect('w');
DROP TABLE cfx_c;
DROP TABLE cfx_a2;
DROP TABLE cfx_a;
DROP FUNCTION cfx_polaris_drop(text);
DROP FUNCTION cfx_remote(text, text);
DROP FUNCTION cfx_try(text);
RESET iceberg_default_catalog;
RESET iceberg_default_volume;
DROP CATALOG cfx_cat;
DROP VOLUME cfx_vol;
DROP USER MAPPING FOR current_user SERVER cfx_cat_srv;
DROP SERVER cfx_cat_srv;
DROP USER MAPPING FOR current_user SERVER cfx_vol_srv;
DROP SERVER cfx_vol_srv;
