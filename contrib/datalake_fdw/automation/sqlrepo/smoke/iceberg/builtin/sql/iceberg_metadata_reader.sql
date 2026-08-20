-- Kernel-side metadata.json reader (#382/#935).
-- pg_catalog.pg_iceberg_load_metadata_json() fetches the real document through dlagent using
-- the builtin volume's own credentials, so the REST catalog gateway needs no object storage
-- credentials of its own.
--
-- Lives in this suite, not contrib/datalake_fdw/{sql,expected}: CI does not run the top-level
-- REGRESS list, and its iceberg entries are deliberately infra-free (iceberg_am_misuse.sql
-- points at example.invalid:9000 because it only exercises rejection paths). This test needs a
-- live MinIO and a live dlagent, which is what this suite has.

CREATE EXTENSION IF NOT EXISTS datalake_fdw;

CREATE SERVER mdr_catalog_srv FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER mdr_catalog_srv;
CREATE FOREIGN CATALOG mdr_catalog SERVER mdr_catalog_srv;
SET iceberg_default_catalog = 'mdr_catalog';

CREATE SERVER mdr_volume_srv FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (type 's3', endpoint 'http://minio:9000', region 'us-east-1',
         bucket_name 'warehouse', path_style_access 'true');
CREATE USER MAPPING FOR current_user SERVER mdr_volume_srv
OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME mdr_volume SERVER mdr_volume_srv OPTIONS (base_path '/mdr_volume/');
SET iceberg_default_volume = 'mdr_volume';

CREATE SCHEMA mdr;
CREATE ICEBERG TABLE mdr.t (id int, v text);
INSERT INTO mdr.t VALUES (1, 'a'), (2, 'b');

-- The document must come back non-empty, be valid JSON, and its location must match what the
-- catalog recorded (pg_ext_aux.pg_iceberg_metadata) -- not merely look like a metadata path.
-- That is the property that matters: the location cannot be forged or substituted, only ever
-- the one the builtin catalog registered for this relid. Asserting on parsed fields rather than
-- raw text keeps the expected file stable across metadata filename churn.
SELECT (metadata_json IS NOT NULL) AS got_doc,
       (metadata_json::jsonb ? 'format-version') AS has_format_version,
       (metadata_json::jsonb ? 'table-uuid') AS has_table_uuid,
       (metadata_location = (SELECT metadata_location FROM pg_ext_aux.pg_iceberg_metadata
                             WHERE relid = 'mdr.t'::regclass)) AS loc_matches_catalog
FROM pg_catalog.pg_iceberg_load_metadata_json('mdr.t'::regclass);

-- Inside a transaction that has uncommitted DML, the reader must still return a usable document.
-- It resolves the pointer through pg_iceberg_tracker_get_scan_metadata_location(), the same
-- accessor the scan path uses, so it sees the rebased metadata rather than a stale catalog row.
-- Whether the location actually changes depends on the tracker's rebase-skip optimisation, so
-- this asserts the invariant that does hold: a valid, non-empty document, consistent with what
-- SELECT sees in the same transaction.
BEGIN;
INSERT INTO mdr.t VALUES (3, 'c');
SELECT (metadata_json::jsonb ? 'format-version') AS has_format_version,
       (metadata_location <> '') AS has_location
FROM pg_catalog.pg_iceberg_load_metadata_json('mdr.t'::regclass);
SELECT count(*) AS rows_visible_in_txn FROM mdr.t;
COMMIT;

-- A relation that is not an iceberg table at all. Deliberately a different message from the
-- external-catalog rejection in pg_iceberg_load_metadata_json(): "not an iceberg table" and
-- "iceberg, but on an external catalog" are different situations for the caller, and only the
-- second one is worth retrying against that catalog's own endpoint. The external-catalog branch
-- needs a live Hive/Polaris catalog and so is covered from the ../hive and ../polaris suites.
CREATE TABLE mdr.heap (id int) DISTRIBUTED BY (id);
SELECT * FROM pg_catalog.pg_iceberg_load_metadata_json('mdr.heap'::regclass);

DROP SCHEMA mdr CASCADE;
DROP VOLUME mdr_volume;
DROP USER MAPPING FOR current_user SERVER mdr_volume_srv;
DROP SERVER mdr_volume_srv;
DROP CATALOG mdr_catalog;
DROP USER MAPPING FOR current_user SERVER mdr_catalog_srv;
DROP SERVER mdr_catalog_srv;
