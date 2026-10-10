-- Metadata cache and refresh on a hadoop catalog over an HDFS volume.
--
-- The polaris suite covers the cache against a REST catalog, where "which
-- metadata.json is the head?" is answered by a service.  A hadoop catalog
-- answers it from the filesystem instead -- the metadata files are named
-- v1.metadata.json, v2.metadata.json, ... and the current version comes from a
-- fixed path that is rewritten on every commit.  That is the one shape where
-- gopher's file cache could in principle hand back a stale answer, since it
-- caches content by path and every other Iceberg file is write-once.  The last
-- case here is what pins that down.
--
-- Note the table carries OPTIONS (namespace, table).  Without them
-- is_internal would be true -- the rule is "did you name an existing external
-- table", not "which catalog type" (pg_iceberg_catalog_helper.c) -- and an
-- internal table is not a cache candidate at all.

-- Not common_setup.sql: this file runs third in the suite, and that script's
-- CREATE EXTENSION IF NOT EXISTS emits an "already exists, skipping" NOTICE
-- when it is not the first file, which made the case pass alone and fail as
-- part of the suite.  Same shape the acid case uses.
SET client_min_messages = WARNING;
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
RESET client_min_messages;

-- Keep the autovacuum-driven refresher from racing the explicit calls: every
-- external table with a TTL is a candidate from birth, so a worker firing
-- mid-file would move a stamp an assertion below is watching.  Restored at the
-- start of cleanup.
ALTER SYSTEM SET datalake_fdw.metadata_refresh_enabled = off;
SELECT pg_reload_conf();

DROP SERVER IF EXISTS hd_mc_cat_srv CASCADE;
CREATE SERVER hd_mc_cat_srv FOREIGN DATA WRAPPER iceberg_catalog_fdw
    OPTIONS (type 'hadoop');
CREATE USER MAPPING FOR current_user SERVER hd_mc_cat_srv;
CREATE FOREIGN CATALOG hd_mc_cat SERVER hd_mc_cat_srv
    OPTIONS (warehouse_location_prefix 'hdfs://hadoop:8020/iceberg_hadoop_hdfs_metadata_cache/');
SET iceberg_default_catalog = 'hd_mc_cat';

DROP SERVER IF EXISTS hd_mc_vol_srv CASCADE;
CREATE SERVER hd_mc_vol_srv FOREIGN DATA WRAPPER iceberg_volume_fdw
    OPTIONS (type 'hdfs', endpoint 'hdfs://hadoop:8020');
CREATE USER MAPPING FOR current_user SERVER hd_mc_vol_srv
    OPTIONS (username 'gpadmin');
CREATE FOREIGN VOLUME hd_mc_vol SERVER hd_mc_vol_srv
    OPTIONS (base_path '/iceberg_hadoop_hdfs_metadata_cache/', allow_writes 'true');
SET iceberg_default_volume = 'hd_mc_vol';

CREATE ICEBERG TABLE hd_mc_t (id int, v text)
    OPTIONS (namespace 'public', table 'hd_mc_t', metadata_cache '5');

-- Re-attaches to whatever a previous run left in HDFS; clear the rows, keep
-- the table (see the Makefile header on why it must never be dropped).  The DO
-- wrapper hides the row count, which differs between the first run and later
-- ones.
DO $$ BEGIN DELETE FROM hd_mc_t; END $$;

\echo === T1 external, and a read stamps nothing ===
SELECT is_internal AS internal,
       metadata_location ~ 'v[0-9]+\.metadata\.json$' AS names_a_version_file
FROM pg_ext_aux.pg_iceberg_metadata WHERE relid = 'hd_mc_t'::regclass;

INSERT INTO hd_mc_t VALUES (1, 'a'), (2, 'b');
SELECT count(*) AS rows_seen FROM hd_mc_t;

SELECT metadata_cached_at IS NULL AS stamp_still_null_after_read
FROM pg_ext_aux.pg_iceberg_metadata WHERE relid = 'hd_mc_t'::regclass;

\echo === T2 the sweep is what stamps it ===
SELECT iceberg_refresh_metadata_cache() >= 1 AS swept_at_least_one;

SELECT metadata_cached_at IS NOT NULL AS stamp_seeded_by_sweep
FROM pg_ext_aux.pg_iceberg_metadata WHERE relid = 'hd_mc_t'::regclass;

\echo === T3 inside the TTL the sweep leaves it alone ===
SELECT metadata_cached_at AS after_sweep
FROM pg_ext_aux.pg_iceberg_metadata WHERE relid = 'hd_mc_t'::regclass
\gset

SELECT iceberg_refresh_metadata_cache() >= 0 AS ran;

SELECT metadata_cached_at = :after_sweep AS stamp_unchanged_within_ttl
FROM pg_ext_aux.pg_iceberg_metadata WHERE relid = 'hd_mc_t'::regclass;

\echo === T4 past the TTL the sweep re-resolves ===
SET allow_system_table_mods = on;
UPDATE pg_ext_aux.pg_iceberg_metadata
   SET metadata_cached_at = metadata_cached_at - (6 * 60 * 1000)
 WHERE relid = 'hd_mc_t'::regclass;
RESET allow_system_table_mods;

SELECT metadata_cached_at AS aged
FROM pg_ext_aux.pg_iceberg_metadata WHERE relid = 'hd_mc_t'::regclass
\gset

SELECT iceberg_refresh_metadata_cache() >= 1 AS refreshed_after_ttl;

SELECT metadata_cached_at > :aged AS stamp_advanced
FROM pg_ext_aux.pg_iceberg_metadata WHERE relid = 'hd_mc_t'::regclass;

\echo === T5 a drifted mirror is recovered, version-file naming and all ===
-- Advance the table one version, then roll the mirror back to the version
-- before it.  The refresh has to resolve the current version from the
-- filesystem -- past gopher's file cache, which was warmed by the write that
-- just happened -- and land on the newer file.
INSERT INTO hd_mc_t VALUES (3, 'c');

SELECT previous_metadata_location IS NOT NULL AS have_a_previous_version
FROM pg_ext_aux.pg_iceberg_metadata WHERE relid = 'hd_mc_t'::regclass;

SET allow_system_table_mods = on;
UPDATE pg_ext_aux.pg_iceberg_metadata
   SET metadata_location = previous_metadata_location
 WHERE relid = 'hd_mc_t'::regclass;
RESET allow_system_table_mods;

SELECT metadata_location AS drifted
FROM pg_ext_aux.pg_iceberg_metadata WHERE relid = 'hd_mc_t'::regclass
\gset

-- The NOTICE names both locations, which vary per run.
SET client_min_messages = warning;
SELECT refreshed,
       previous_head = :'drifted' AS reported_the_drifted_head,
       current_head <> :'drifted' AS head_moved_on
FROM iceberg_refresh_metadata('hd_mc_t');
RESET client_min_messages;

SELECT count(*) AS rows_after_recovery FROM hd_mc_t;

\echo === T6 already on the head is a no-op ===
SELECT refreshed AS refreshed_again,
       previous_head = current_head AS head_already_current
FROM iceberg_refresh_metadata('hd_mc_t');

-- ============================================================
-- Cleanup
-- ============================================================
-- Before anything else: this GUC is cluster-wide and outlives the session.
ALTER SYSTEM RESET datalake_fdw.metadata_refresh_enabled;
SELECT pg_reload_conf();

-- hd_mc_t is deliberately NOT dropped, for the reason in the Makefile header:
-- dropping purges the metadata tree, so the next run would write
-- v1.metadata.json again at a path gophermeta already has a length registered
-- for, and gopher reports that as a FATAL that takes the agent JVM with it.
DO $$ BEGIN DELETE FROM hd_mc_t; END $$;
