-- Which foreign server a gphdfs:// external table binds to (issue #409)
--
-- CREATE EXTERNAL TABLE has no SERVER clause, so grammar_convert.c infers the
-- server from the address in gphdfs.conf.  It used to match on the value of the
-- "host" / "hdfs_namenodes" option alone; since 2.7 iceberg_volume_fdw servers
-- carry hdfs_namenodes too, so the table could bind to a volume server and
-- SIGSEGV the coordinator on the first SELECT.
--
-- gphdfs.conf [paa_cluster] resolves to namenode "hadoop", so every server here
-- advertises that address on purpose.
--
-- Server creation order matters: the volume server is created LAST in Test 1
-- because the resolver keeps the last match in catalog order.  Against the
-- pre-fix code that ordering makes the volume server win, so this test fails
-- (crash, or a wrong srvname) without the wrapper filter.
--
-- This lives in its own suite so that the only servers describing namenode
-- "hadoop" are the ones created here; smoke/hdfs creates one too, which would
-- make every case below ambiguous.
--
-- Known limit: Test 3 pins both the order of the server list in the WARNING and
-- which server was chosen, and the resolver reaches them through an unordered
-- catalog heap scan.  Within one run the servers are inserted sequentially into
-- a fresh catalog, so creation order is scan order; after a catalog rewrite the
-- expected output may need regenerating.  Test 4 is the order-independent
-- negative control for the wrong-wrapper binding.

\i ../../../lib/sql/common_setup.sql

DROP EXTERNAL TABLE IF EXISTS bind_hdfs_t;
DROP EXTERNAL TABLE IF EXISTS bind_alias_t;
DROP EXTERNAL TABLE IF EXISTS bind_ambig_t;
DROP FOREIGN TABLE IF EXISTS bind_direct_t;
DROP SERVER IF EXISTS bind_vol_srv CASCADE;
DROP SERVER IF EXISTS bind_hdfs_srv CASCADE;
DROP SERVER IF EXISTS bind_hdfs_srv2 CASCADE;
DROP SERVER IF EXISTS bind_alias_srv CASCADE;
DROP SERVER IF EXISTS bind_vol_alias_srv CASCADE;
DROP FOREIGN DATA WRAPPER IF EXISTS bind_alias_fdw CASCADE;
DROP FOREIGN DATA WRAPPER IF EXISTS bind_vol_alias_fdw CASCADE;

-- Reports the wrapper a table bound to, by handler name rather than wrapper
-- name, so the alias case in Test 2 is readable.
CREATE OR REPLACE VIEW bind_binding AS
SELECT c.relname, s.srvname, p.proname AS handler
FROM pg_class c
JOIN pg_foreign_table ft ON ft.ftrelid = c.oid
JOIN pg_foreign_server s ON s.oid = ft.ftserver
JOIN pg_foreign_data_wrapper w ON w.oid = s.srvfdw
JOIN pg_proc p ON p.oid = w.fdwhandler;

-- ============================================================
-- Test 1: a volume server on the same namenode must not win
-- ============================================================
CREATE SERVER bind_hdfs_srv FOREIGN DATA WRAPPER datalake_fdw
    OPTIONS (protocol 'hdfs', hdfs_namenodes 'hadoop', hdfs_port '8020',
             hdfs_auth_method 'simple', hadoop_rpc_protection 'authentication');
CREATE USER MAPPING FOR CURRENT_USER SERVER bind_hdfs_srv OPTIONS (user 'gpadmin');

-- created last on purpose; see header
CREATE SERVER bind_vol_srv FOREIGN DATA WRAPPER iceberg_volume_fdw
    OPTIONS (type 'hdfs', hdfs_namenodes 'hadoop', hdfs_port '8020');
CREATE USER MAPPING FOR CURRENT_USER SERVER bind_vol_srv OPTIONS (username 'gpadmin');

CREATE READABLE EXTERNAL TABLE bind_hdfs_t (id int, col text, col2 decimal(10,2))
LOCATION('gphdfs://test/basic/text/simple hdfs_cluster_name=paa_cluster')
FORMAT 'text';

SELECT srvname, handler FROM bind_binding WHERE relname = 'bind_hdfs_t';
SELECT count(*) AS rows_read FROM bind_hdfs_t;
DROP EXTERNAL TABLE bind_hdfs_t;

-- ============================================================
-- Test 2: a wrapper under a different name but the same handler still works.
--         The datalake_fdw wrapper object is created per installation, not by
--         the extension, so its name is not part of the contract.
-- ============================================================
DROP SERVER bind_hdfs_srv CASCADE;

CREATE FOREIGN DATA WRAPPER bind_alias_fdw
    HANDLER datalake_fdw_handler VALIDATOR datalake_fdw_validator
    OPTIONS (mpp_execute 'all segments');
CREATE SERVER bind_alias_srv FOREIGN DATA WRAPPER bind_alias_fdw
    OPTIONS (protocol 'hdfs', hdfs_namenodes 'hadoop', hdfs_port '8020',
             hdfs_auth_method 'simple', hadoop_rpc_protection 'authentication');
CREATE USER MAPPING FOR CURRENT_USER SERVER bind_alias_srv OPTIONS (user 'gpadmin');

CREATE READABLE EXTERNAL TABLE bind_alias_t (id int, col text, col2 decimal(10,2))
LOCATION('gphdfs://test/basic/text/simple hdfs_cluster_name=paa_cluster')
FORMAT 'text';

SELECT srvname, handler FROM bind_binding WHERE relname = 'bind_alias_t';
SELECT count(*) AS rows_read FROM bind_alias_t;
DROP EXTERNAL TABLE bind_alias_t;

-- ============================================================
-- Test 3: two servers of our own wrapper on one address -> WARNING, and the
--         last match in catalog order is still the one chosen
-- ============================================================
CREATE SERVER bind_hdfs_srv2 FOREIGN DATA WRAPPER bind_alias_fdw
    OPTIONS (protocol 'hdfs', hdfs_namenodes 'hadoop', hdfs_port '8020',
             hdfs_auth_method 'simple', hadoop_rpc_protection 'authentication');
CREATE USER MAPPING FOR CURRENT_USER SERVER bind_hdfs_srv2 OPTIONS (user 'gpadmin');

CREATE READABLE EXTERNAL TABLE bind_ambig_t (id int, col text, col2 decimal(10,2))
LOCATION('gphdfs://test/basic/text/simple hdfs_cluster_name=paa_cluster')
FORMAT 'text';

SELECT srvname FROM bind_binding WHERE relname = 'bind_ambig_t';
DROP EXTERNAL TABLE bind_ambig_t;
DROP SERVER bind_hdfs_srv2 CASCADE;
DROP SERVER bind_alias_srv CASCADE;
DROP FOREIGN DATA WRAPPER bind_alias_fdw CASCADE;

-- ============================================================
-- Test 4: only a volume server describes the address -> fail at CREATE, and say
--         why, instead of failing at the first SELECT
-- ============================================================
CREATE READABLE EXTERNAL TABLE bind_hdfs_t (id int, col text, col2 decimal(10,2))
LOCATION('gphdfs://test/basic/text/simple hdfs_cluster_name=paa_cluster')
FORMAT 'text';

-- ============================================================
-- Test 5: a foreign table pointed straight at a volume server.  This does not
--         go through the resolver at all -- iceberg_volume_fdw is a registered
--         wrapper with an accept-all validator -- so it needs its own guard on
--         both the read and the write callback.
-- ============================================================
CREATE FOREIGN TABLE bind_direct_t (a int) SERVER bind_vol_srv;
SELECT count(*) FROM bind_direct_t;

-- INSERT on the wrapper as the extension ships it is rejected earlier, by the
-- mpp_execute check in datalakeGetOptions(), so it never reaches the write
-- callback.  Repeat on a wrapper that does set mpp_execute so the write guard
-- itself is exercised; nothing stops an installation from creating the volume
-- wrapper that way.
INSERT INTO bind_direct_t VALUES (1);
DROP FOREIGN TABLE bind_direct_t;

CREATE FOREIGN DATA WRAPPER bind_vol_alias_fdw
    HANDLER iceberg_volume_fdw_handler VALIDATOR iceberg_volume_fdw_validator
    OPTIONS (mpp_execute 'all segments');
CREATE SERVER bind_vol_alias_srv FOREIGN DATA WRAPPER bind_vol_alias_fdw
    OPTIONS (type 'hdfs', hdfs_namenodes 'hadoop', hdfs_port '8020');
CREATE USER MAPPING FOR CURRENT_USER SERVER bind_vol_alias_srv OPTIONS (username 'gpadmin');

CREATE FOREIGN TABLE bind_direct_t (a int) SERVER bind_vol_alias_srv;
SELECT count(*) FROM bind_direct_t;
INSERT INTO bind_direct_t VALUES (1);
DROP FOREIGN TABLE bind_direct_t;
DROP SERVER bind_vol_alias_srv CASCADE;
DROP FOREIGN DATA WRAPPER bind_vol_alias_fdw CASCADE;

-- ============================================================
-- Cleanup
-- ============================================================
DROP VIEW bind_binding;
DROP SERVER bind_vol_srv CASCADE;
