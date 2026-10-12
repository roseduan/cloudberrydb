-- Manifest statistics built from what the segments sent (issue #442).
--
-- The agent no longer reopens every committed file to read its footer; it
-- builds each manifest entry from statistics the segment collected while
-- writing.  Nothing about that is visible in a query plan, so this test does
-- not look for it directly.  It looks for what would break if the statistics
-- were wrong.
--
-- A bound that is too tight makes a scan skip a file that holds matching
-- rows, so every range predicate below is written to straddle file
-- boundaries: if any file's bounds excluded rows it really contains, the
-- counts come out short.  A field id attached to the wrong column has the
-- same effect one column over, which is what the DROP COLUMN case is for.
-- And a position-delete file whose file_path bounds are wrong is applied to
-- the wrong data files, so the delete and update cases check row counts
-- rather than just that the statement succeeded.
--
-- Expected output was produced by a verified run: every count in it is the
-- arithmetic answer for the data the test writes, checked by hand rather than
-- accepted from the run -- so a change that breaks a bound fails here rather
-- than quietly recording new numbers.

CREATE EXTENSION IF NOT EXISTS datalake_fdw;

-- catalog
CREATE SERVER ws_catalog_server
FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER ws_catalog_server;
CREATE FOREIGN CATALOG ws_catalog SERVER ws_catalog_server;
set iceberg_default_catalog='ws_catalog';

-- volume
CREATE SERVER ws_volume_server
FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (
    type 's3',
    endpoint 'http://minio:9000',
    region 'us-east-1',
    bucket_name 'warehouse',
    path_style_access 'true'
);
CREATE USER MAPPING FOR current_user
SERVER ws_volume_server
OPTIONS (
    access_key_id 'admin',
    secret_access_key 'admin12345');

CREATE FOREIGN VOLUME ws_volume SERVER ws_volume_server
OPTIONS(base_path '/ws_volume/');
set iceberg_default_volume='ws_volume';

-- Every type the writer emits, so each one's bound encoding is exercised.
CREATE ICEBERG TABLE ws_types (
    id          bigint,
    small       smallint,
    flag        boolean,
    ratio       real,
    amount      double precision,
    price       numeric(12,2),
    when_day    date,
    when_ts     timestamp,
    when_tstz   timestamptz,
    label       text,
    note        varchar(64),
    payload     bytea,
    grp         text
) PARTITION BY (grp);

-- 8 partitions x 6 segments is enough files that a wrong bound shows up,
-- while staying quick enough for a smoke run.
INSERT INTO ws_types
SELECT i,
       (i % 300)::smallint,
       (i % 2 = 0),
       (i % 97)::real / 7.0,
       (i % 89)::double precision / 3.0,
       ((i % 10000)::numeric / 100.0),
       DATE '2024-01-01' + (i % 365),
       TIMESTAMP '2024-01-01 00:00:00' + ((i % 365) || ' days')::interval,
       TIMESTAMPTZ '2024-01-01 00:00:00+00' + ((i % 365) || ' days')::interval,
       'label-' || lpad((i % 5000)::text, 6, '0'),
       CASE WHEN i % 11 = 0 THEN NULL ELSE 'note-' || (i % 700) END,
       decode(lpad(to_hex(i % 65536), 4, '0'), 'hex'),
       'g' || (i % 8)
FROM generate_series(1, 20000) i;

SELECT count(*) AS total FROM ws_types;

-- Range predicates that straddle file boundaries.  Each of these is answered
-- from the manifest bounds first and the data second; a bound that excluded
-- real rows would show up here as a short count.
SELECT count(*) AS mid_ids FROM ws_types WHERE id BETWEEN 7001 AND 13000;
SELECT count(*) AS small_range FROM ws_types WHERE small BETWEEN 100 AND 200;
SELECT count(*) AS price_range FROM ws_types WHERE price > 50.00 AND price < 60.00;
SELECT count(*) AS day_range FROM ws_types
    WHERE when_day BETWEEN DATE '2024-03-01' AND DATE '2024-04-30';
SELECT count(*) AS ts_range FROM ws_types
    WHERE when_ts >= TIMESTAMP '2024-06-01 00:00:00'
      AND when_ts <  TIMESTAMP '2024-07-01 00:00:00';
SELECT count(*) AS tstz_range FROM ws_types
    WHERE when_tstz >= TIMESTAMPTZ '2024-06-01 00:00:00+00'
      AND when_tstz <  TIMESTAMPTZ '2024-07-01 00:00:00+00';
SELECT count(*) AS label_range FROM ws_types
    WHERE label >= 'label-001000' AND label < 'label-002000';
SELECT count(*) AS ratio_range FROM ws_types WHERE ratio > 5.0 AND ratio < 9.0;
SELECT count(*) AS amount_range FROM ws_types WHERE amount > 10.0 AND amount < 20.0;
SELECT count(*) AS flagged FROM ws_types WHERE flag;
SELECT count(*) AS notes_null FROM ws_types WHERE note IS NULL;

-- Boundary values: the exact minimum and maximum of the table.  An
-- off-by-one in a bound is invisible in the middle of a range and obvious
-- at its edges.
SELECT count(*) AS at_min FROM ws_types WHERE id = 1;
SELECT count(*) AS at_max FROM ws_types WHERE id = 20000;
SELECT min(id) AS min_id, max(id) AS max_id FROM ws_types;
SELECT min(label) AS min_label, max(label) AS max_label FROM ws_types;
SELECT min(price) AS min_price, max(price) AS max_price FROM ws_types;

-- Position deletes.  Their file_path bounds decide which data files a delete
-- file applies to, so a wrong bound deletes from the wrong file or from none.
DELETE FROM ws_types WHERE id % 97 = 0;
SELECT count(*) AS after_delete FROM ws_types;
SELECT count(*) AS deleted_gone FROM ws_types WHERE id % 97 = 0;

UPDATE ws_types SET price = price + 1000.00 WHERE id % 89 = 0;
SELECT count(*) AS updated FROM ws_types WHERE price >= 1000.00;
SELECT count(*) AS still_total FROM ws_types;

-- The same range predicates again, now that the table carries delete files:
-- a delete file misapplied through a bad bound changes answers here.
SELECT count(*) AS mid_ids_after FROM ws_types WHERE id BETWEEN 7001 AND 13000;
SELECT count(*) AS label_range_after FROM ws_types
    WHERE label >= 'label-001000' AND label < 'label-002000';

-- Dropping a column shifts nothing in the parquet files, which keep the
-- field ids they were written with.  Statistics that were numbered by column
-- position instead would land on the neighbouring column from here on.
ALTER TABLE ws_types DROP COLUMN note;

INSERT INTO ws_types
SELECT i, (i % 300)::smallint, (i % 2 = 0),
       (i % 97)::real / 7.0, (i % 89)::double precision / 3.0,
       ((i % 10000)::numeric / 100.0),
       DATE '2024-01-01' + (i % 365),
       TIMESTAMP '2024-01-01 00:00:00' + ((i % 365) || ' days')::interval,
       TIMESTAMPTZ '2024-01-01 00:00:00+00' + ((i % 365) || ' days')::interval,
       'label-' || lpad((i % 5000)::text, 6, '0'),
       decode(lpad(to_hex(i % 65536), 4, '0'), 'hex'),
       'g' || (i % 8)
FROM generate_series(30001, 34000) i;

SELECT count(*) AS after_drop_insert FROM ws_types;
SELECT count(*) AS new_rows FROM ws_types WHERE id >= 30001;
SELECT count(*) AS new_label_range FROM ws_types
    WHERE id >= 30001 AND label >= 'label-001000' AND label < 'label-002000';
SELECT min(id) AS min_id_after, max(id) AS max_id_after FROM ws_types;

-- An empty string is a legitimate lower bound; a column of them has no
-- usable upper bound and must simply carry none rather than a wrong one.
CREATE ICEBERG TABLE ws_empty (id int, s text);
INSERT INTO ws_empty SELECT i, '' FROM generate_series(1, 100) i;
SELECT count(*) AS empties FROM ws_empty WHERE s = '';
SELECT count(*) AS empties_ge FROM ws_empty WHERE s >= '';

-- A value well past the collector's per-bound ceiling: the column carries no
-- bound at all, which costs pruning and must not cost rows.
CREATE ICEBERG TABLE ws_long (id int, s text);
INSERT INTO ws_long SELECT i, repeat('y', 500) || i FROM generate_series(1, 200) i;
SELECT count(*) AS longs FROM ws_long;
SELECT count(*) AS long_match FROM ws_long WHERE s = repeat('y', 500) || '42';
SELECT count(*) AS long_range FROM ws_long WHERE s > repeat('y', 500);

DROP TABLE ws_long;
DROP TABLE ws_empty;
DROP TABLE ws_types;
