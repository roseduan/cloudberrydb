-- 03_rowgroup_pushdown.sql
-- Regression test for columnar row-group (Parquet) / stripe (ORC) min/max
-- predicate pushdown (issue #297).
--
-- Strategy: write 3 files with DISJOINT per-column ranges so that most
-- predicates prune whole files' row groups/stripes.  For every predicate we
-- compare the result with pushdown OFF vs ON; they MUST be identical -- this
-- catches any zone-map bug that wrongly drops a matching block.  ok=t for all
-- rows is the pass condition.

\i ../../../lib/sql/common_setup.sql

SELECT test_log('Feature Test: row-group / stripe min/max pushdown');

-- self-checking helper: run `SELECT count(*)/sum(id) FROM tbl WHERE wc` with
-- pushdown off then on; report both signatures and whether they match.
CREATE OR REPLACE FUNCTION pushdown_check(tbl text, wc text)
RETURNS TABLE(pred text, sig_off text, sig_on text, ok boolean) AS $$
DECLARE
    q text := format('SELECT count(*)::text || ''/'' || coalesce(sum(id),0)::text FROM %s WHERE %s', tbl, wc);
BEGIN
    PERFORM set_config('gp_external_enable_filter_pushdown', 'off', false);
    EXECUTE q INTO sig_off;
    PERFORM set_config('gp_external_enable_filter_pushdown', 'on', false);
    EXECUTE q INTO sig_on;
    pred := wc;
    ok := (sig_off = sig_on);
    RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

DROP SERVER IF EXISTS rgp_server CASCADE;
CREATE SERVER rgp_server FOREIGN DATA WRAPPER datalake_fdw
OPTIONS (host 'minio:9000', protocol 's3', isvirtual 'false', ishttps 'false');
CREATE USER MAPPING FOR gpadmin SERVER rgp_server
OPTIONS (user 'gpadmin', accesskey 'admin', secretkey 'admin12345');

-- ============================================================
-- Parquet: 3 files with disjoint ranges (id ~0 / ~1000 / ~2000)
-- ============================================================
CREATE FOREIGN TABLE rgp_pw (
    id int, f8 double precision, nm numeric(10,2), t text, c char(8),
    flag boolean, dt date, ts timestamp, n2 int
) SERVER rgp_server
OPTIONS (filePath '/warehouse/fdw-test/feature/pushdown/rgp_parquet/', format 'parquet');

INSERT INTO rgp_pw SELECT g, g::float8, g::numeric(10,2),
       'aaa'||lpad(g::text,4,'0'), ('AAAA'||lpad(g::text,4,'0'))::char(8),
       false, DATE '2020-01-01' + g, TIMESTAMP '2020-01-01' + (g||' min')::interval, NULL
  FROM generate_series(1,50) g;
INSERT INTO rgp_pw SELECT 1000+g, (1000+g)::float8, (1000+g)::numeric(10,2),
       'mmm'||lpad(g::text,4,'0'), ('MMMM'||lpad(g::text,4,'0'))::char(8),
       true, DATE '2021-06-01' + g, TIMESTAMP '2021-06-01' + (g||' min')::interval, 1000+g
  FROM generate_series(1,50) g;
INSERT INTO rgp_pw SELECT 2000+g, (2000+g)::float8, (2000+g)::numeric(10,2),
       'zzz'||lpad(g::text,4,'0'), ('ZZZZ'||lpad(g::text,4,'0'))::char(8),
       true, DATE '2022-12-01' + g, TIMESTAMP '2022-12-01' + (g||' min')::interval, 2000+g
  FROM generate_series(1,50) g;
DROP FOREIGN TABLE rgp_pw;

CREATE FOREIGN TABLE rgp_p (
    id int, f8 double precision, nm numeric(10,2), t text, c char(8),
    flag boolean, dt date, ts timestamp, n2 int
) SERVER rgp_server
OPTIONS (filePath '/warehouse/fdw-test/feature/pushdown/rgp_parquet/', format 'parquet');

-- ============================================================
-- ORC: same schema and data
-- ============================================================
CREATE FOREIGN TABLE rgp_ow (
    id int, f8 double precision, nm numeric(10,2), t text, c char(8),
    flag boolean, dt date, ts timestamp, n2 int
) SERVER rgp_server
OPTIONS (filePath '/warehouse/fdw-test/feature/pushdown/rgp_orc/', format 'orc');

INSERT INTO rgp_ow SELECT g, g::float8, g::numeric(10,2),
       'aaa'||lpad(g::text,4,'0'), ('AAAA'||lpad(g::text,4,'0'))::char(8),
       false, DATE '2020-01-01' + g, TIMESTAMP '2020-01-01' + (g||' min')::interval, NULL
  FROM generate_series(1,50) g;
INSERT INTO rgp_ow SELECT 1000+g, (1000+g)::float8, (1000+g)::numeric(10,2),
       'mmm'||lpad(g::text,4,'0'), ('MMMM'||lpad(g::text,4,'0'))::char(8),
       true, DATE '2021-06-01' + g, TIMESTAMP '2021-06-01' + (g||' min')::interval, 1000+g
  FROM generate_series(1,50) g;
INSERT INTO rgp_ow SELECT 2000+g, (2000+g)::float8, (2000+g)::numeric(10,2),
       'zzz'||lpad(g::text,4,'0'), ('ZZZZ'||lpad(g::text,4,'0'))::char(8),
       true, DATE '2022-12-01' + g, TIMESTAMP '2022-12-01' + (g||' min')::interval, 2000+g
  FROM generate_series(1,50) g;
DROP FOREIGN TABLE rgp_ow;

CREATE FOREIGN TABLE rgp_o (
    id int, f8 double precision, nm numeric(10,2), t text, c char(8),
    flag boolean, dt date, ts timestamp, n2 int
) SERVER rgp_server
OPTIONS (filePath '/warehouse/fdw-test/feature/pushdown/rgp_orc/', format 'orc');

-- ============================================================
-- Correctness: pushdown ON must equal pushdown OFF for every predicate,
-- on both formats.  ok must be t everywhere.
-- ============================================================
SELECT test_log('Parquet row-group pushdown on==off');
SELECT ok, pred, sig_on FROM pushdown_check('rgp_p', 'id = 1025') ORDER BY pred;
SELECT ok, pred, sig_on FROM pushdown_check('rgp_p', 'id >= 2001') ORDER BY pred;
SELECT ok, pred, sig_on FROM pushdown_check('rgp_p', 'id < 51') ORDER BY pred;
SELECT ok, pred, sig_on FROM pushdown_check('rgp_p', 'id IN (1025, 99999)') ORDER BY pred;
SELECT ok, pred, sig_on FROM pushdown_check('rgp_p', 'f8 >= 2000') ORDER BY pred;
SELECT ok, pred, sig_on FROM pushdown_check('rgp_p', 'nm BETWEEN 1000 AND 1050') ORDER BY pred;
SELECT ok, pred, sig_on FROM pushdown_check('rgp_p', 'f8 IN (2025, -1)') ORDER BY pred;
SELECT ok, pred, sig_on FROM pushdown_check('rgp_p', 'nm IN (1025.00)') ORDER BY pred;
SELECT ok, pred, sig_on FROM pushdown_check('rgp_p', 't = ''zzz0005''') ORDER BY pred;
SELECT ok, pred, sig_on FROM pushdown_check('rgp_p', 't IN (''aaa0005'',''aaa0006'')') ORDER BY pred;
SELECT ok, pred, sig_on FROM pushdown_check('rgp_p', 'c = ''MMMM0003''') ORDER BY pred;
SELECT ok, pred, sig_on FROM pushdown_check('rgp_p', 'c IN (''ZZZZ0003'')') ORDER BY pred;
SELECT ok, pred, sig_on FROM pushdown_check('rgp_p', 'dt >= DATE ''2022-01-01''') ORDER BY pred;
SELECT ok, pred, sig_on FROM pushdown_check('rgp_p', 'dt BETWEEN DATE ''2021-01-01'' AND DATE ''2021-12-31''') ORDER BY pred;
SELECT ok, pred, sig_on FROM pushdown_check('rgp_p', 'ts >= TIMESTAMP ''2022-01-01''') ORDER BY pred;
SELECT ok, pred, sig_on FROM pushdown_check('rgp_p', 'flag') ORDER BY pred;
SELECT ok, pred, sig_on FROM pushdown_check('rgp_p', 'NOT flag') ORDER BY pred;
SELECT ok, pred, sig_on FROM pushdown_check('rgp_p', 'n2 IS NULL') ORDER BY pred;
SELECT ok, pred, sig_on FROM pushdown_check('rgp_p', 'n2 IS NOT NULL') ORDER BY pred;
SELECT ok, pred, sig_on FROM pushdown_check('rgp_p', 'f8 >= 1000 AND f8 < 1051') ORDER BY pred;
SELECT ok, pred, sig_on FROM pushdown_check('rgp_p', 'f8 < 51 OR f8 >= 2000') ORDER BY pred;

SELECT test_log('ORC stripe pushdown on==off');
SELECT ok, pred, sig_on FROM pushdown_check('rgp_o', 'id = 1025') ORDER BY pred;
SELECT ok, pred, sig_on FROM pushdown_check('rgp_o', 'id >= 2001') ORDER BY pred;
SELECT ok, pred, sig_on FROM pushdown_check('rgp_o', 'id < 51') ORDER BY pred;
SELECT ok, pred, sig_on FROM pushdown_check('rgp_o', 'id IN (1025, 99999)') ORDER BY pred;
SELECT ok, pred, sig_on FROM pushdown_check('rgp_o', 'f8 >= 2000') ORDER BY pred;
SELECT ok, pred, sig_on FROM pushdown_check('rgp_o', 'nm BETWEEN 1000 AND 1050') ORDER BY pred;
SELECT ok, pred, sig_on FROM pushdown_check('rgp_o', 'f8 IN (2025, -1)') ORDER BY pred;
SELECT ok, pred, sig_on FROM pushdown_check('rgp_o', 'nm IN (1025.00)') ORDER BY pred;
SELECT ok, pred, sig_on FROM pushdown_check('rgp_o', 't = ''zzz0005''') ORDER BY pred;
SELECT ok, pred, sig_on FROM pushdown_check('rgp_o', 't IN (''aaa0005'',''aaa0006'')') ORDER BY pred;
SELECT ok, pred, sig_on FROM pushdown_check('rgp_o', 'c = ''MMMM0003''') ORDER BY pred;
SELECT ok, pred, sig_on FROM pushdown_check('rgp_o', 'c IN (''ZZZZ0003'')') ORDER BY pred;
SELECT ok, pred, sig_on FROM pushdown_check('rgp_o', 'dt >= DATE ''2022-01-01''') ORDER BY pred;
SELECT ok, pred, sig_on FROM pushdown_check('rgp_o', 'dt BETWEEN DATE ''2021-01-01'' AND DATE ''2021-12-31''') ORDER BY pred;
SELECT ok, pred, sig_on FROM pushdown_check('rgp_o', 'flag') ORDER BY pred;
SELECT ok, pred, sig_on FROM pushdown_check('rgp_o', 'NOT flag') ORDER BY pred;
SELECT ok, pred, sig_on FROM pushdown_check('rgp_o', 'n2 IS NULL') ORDER BY pred;
SELECT ok, pred, sig_on FROM pushdown_check('rgp_o', 'n2 IS NOT NULL') ORDER BY pred;
SELECT ok, pred, sig_on FROM pushdown_check('rgp_o', 'f8 >= 1000 AND f8 < 1051') ORDER BY pred;
SELECT ok, pred, sig_on FROM pushdown_check('rgp_o', 'f8 < 51 OR f8 >= 2000') ORDER BY pred;

-- A few materialized result checks (pushdown on) to lock the actual rows.
SET gp_external_enable_filter_pushdown = on;
SELECT test_log('Materialized result checks (pushdown on)');
SELECT id, f8, nm, t, c, flag, dt FROM rgp_p WHERE id IN (1025, 2050) ORDER BY id;
SELECT id, f8, nm, t, c, flag, dt FROM rgp_o WHERE id IN (1025, 2050) ORDER BY id;
SELECT count(*) AS p_total FROM rgp_p;
SELECT count(*) AS o_total FROM rgp_o;

-- cleanup
RESET gp_external_enable_filter_pushdown;
DROP FOREIGN TABLE rgp_p;
DROP FOREIGN TABLE rgp_o;
DROP FUNCTION pushdown_check(text, text);
DROP SERVER rgp_server CASCADE;
