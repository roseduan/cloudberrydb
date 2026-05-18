-- Preinstall the 'iceberg' table access method with a stable OID.
--
-- The OID is hardcoded in src/include/utils/rel.h (ICEBERG_AM_OID 8320) and
-- used by the RelationIsIceberg() macro on hot paths. Installing the AM here
-- via initdb's cdb_init.d hook guarantees the OID is exactly 8320 on every
-- node, the same mechanism PAX uses for PAX_AM_OID 7047.
--
-- Inserting directly into pg_proc / pg_am (instead of going through
-- CREATE ACCESS METHOD in the datalake_fdw extension SQL) avoids the
-- "allocate a random OID, then UPDATE pg_am.oid to 8320" rewrite trick
-- that left orphan rows in pg_depend (see issue #320).

-- Handler function: pg_iceberg_tableam_handler(internal) -> table_am_handler
-- Columns follow Cloudberry's 32-column pg_proc layout (see pg_proc.h).
INSERT INTO pg_proc VALUES(8321,'pg_iceberg_tableam_handler',11,10,13,1,0,0,0,'f','f','f','t','f','v','u',1,0,269,'2281',null,null,null,null,null,'pg_iceberg_tableam_handler','$libdir/datalake_fdw',null,null,null,'n','a');

-- Access method: iceberg, TYPE TABLE (t), handler at OID 8321.
INSERT INTO pg_am VALUES(8320,'iceberg',8321,'t');

COMMENT ON FUNCTION pg_iceberg_tableam_handler(internal) IS 'iceberg table access method handler for datalake_fdw';
