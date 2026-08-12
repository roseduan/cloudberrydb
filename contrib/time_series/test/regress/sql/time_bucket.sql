-- Regression tests for time_series extension: time_bucket
-- Adapted from TimescaleDB test/sql/timestamp.sql.in (Apache 2.0)

CREATE EXTENSION time_series;

-- Fix session timezone so expected output is stable across environments.
SET timezone = 'PST8PDT';

-- 1.1 Date bucketing: non-integer-day intervals should error
\set ON_ERROR_STOP 0
SELECT time_series.time_bucket('1 hour', DATE '2012-01-01');
SELECT time_series.time_bucket('25 hour', DATE '2012-01-01');
\set ON_ERROR_STOP 1

-- 1.2 Basic timestamp bucketing (1 day)
SELECT time_series.time_bucket(INTERVAL '1 day', TIMESTAMP '2011-01-02 01:01:01');

-- 1.3 Timestamp bucketing (2 day interval)
SELECT time, time_series.time_bucket(INTERVAL '2 day', time)
FROM unnest(ARRAY[
    TIMESTAMP '2011-01-01 01:01:01',
    TIMESTAMP '2011-01-02 01:01:01',
    TIMESTAMP '2011-01-03 01:01:01',
    TIMESTAMP '2011-01-04 01:01:01'
    ]) AS time;

-- 1.4 Multiple interval sizes
SELECT int_def, time_series.time_bucket(int_def, TIMESTAMP '2011-01-02 01:01:01.111')
FROM unnest(ARRAY[
    INTERVAL '1 millisecond',
    INTERVAL '1 second',
    INTERVAL '1 minute',
    INTERVAL '1 hour',
    INTERVAL '1 day',
    INTERVAL '2 millisecond',
    INTERVAL '2 second',
    INTERVAL '2 minute',
    INTERVAL '2 hour',
    INTERVAL '2 day'
    ]) AS int_def;

-- 1.5 Mixed month+day intervals should error
\set ON_ERROR_STOP 0
SELECT time_series.time_bucket(INTERVAL '1 year 1d', TIMESTAMP '2011-01-02 01:01:01.111');
SELECT time_series.time_bucket(INTERVAL '1 month 1 minute', TIMESTAMP '2011-01-02 01:01:01.111');
\set ON_ERROR_STOP 1

-- 1.6 5-minute bucketing (epoch alignment)
SELECT time, time_series.time_bucket(INTERVAL '5 minute', time)
FROM unnest(ARRAY[
    TIMESTAMP '1970-01-01 00:59:59.999999',
    TIMESTAMP '1970-01-01 01:01:00',
    TIMESTAMP '1970-01-01 01:04:59.999999',
    TIMESTAMP '1970-01-01 01:05:00'
    ]) AS time;

SELECT time, time_series.time_bucket(INTERVAL '5 minute', time)
FROM unnest(ARRAY[
    TIMESTAMP '2011-01-02 01:04:59.999999',
    TIMESTAMP '2011-01-02 01:05:00',
    TIMESTAMP '2011-01-02 01:09:59.999999',
    TIMESTAMP '2011-01-02 01:10:00'
    ]) AS time;

-- 1.7 Positive offset
SELECT time, time_series.time_bucket(INTERVAL '5 minute', time, INTERVAL '2 minutes')
FROM unnest(ARRAY[
    TIMESTAMP '2011-01-02 01:01:59.999999',
    TIMESTAMP '2011-01-02 01:02:00',
    TIMESTAMP '2011-01-02 01:06:59.999999',
    TIMESTAMP '2011-01-02 01:07:00'
    ]) AS time;

-- 1.8 Negative offset
SELECT time, time_series.time_bucket(INTERVAL '5 minute', time, - INTERVAL '2 minutes')
FROM unnest(ARRAY[
    TIMESTAMP '2011-01-02 01:02:59.999999',
    TIMESTAMP '2011-01-02 01:03:00',
    TIMESTAMP '2011-01-02 01:07:59.999999',
    TIMESTAMP '2011-01-02 01:08:00'
    ]) AS time;

-- 1.9 Infinity handling — timestamp
SELECT time, time_series.time_bucket(INTERVAL '1 week', time, INTERVAL '1 day')
FROM unnest(ARRAY[
    timestamp '-Infinity',
    timestamp 'Infinity'
    ]) AS time;

-- 1.10 Infinity handling — timestamptz
SELECT time, time_series.time_bucket(INTERVAL '1 week', time, INTERVAL '1 day')
FROM unnest(ARRAY[
    timestamp with time zone '-Infinity',
    timestamp with time zone 'Infinity'
    ]) AS time;

-- 1.11 Infinity handling — date
SELECT date, time_series.time_bucket(INTERVAL '1 week', date, INTERVAL '1 day')
FROM unnest(ARRAY[
    date '-Infinity',
    date 'Infinity'
    ]) AS date;

-- 1.12 Timestamptz with UTC
SET timezone TO 'UTC';
SELECT time, time_series.time_bucket(INTERVAL '1 hour', time)
FROM unnest(ARRAY[
    TIMESTAMP WITH TIME ZONE '2011-01-02 01:01:01',
    TIMESTAMP WITH TIME ZONE '2011-01-02 01:01:01+01',
    TIMESTAMP WITH TIME ZONE '2011-01-02 01:01:01+02'
    ]) AS time;

SELECT time, time_series.time_bucket(INTERVAL '1 day', time)
FROM unnest(ARRAY[
    TIMESTAMP WITH TIME ZONE '2011-01-02 01:01:01',
    TIMESTAMP WITH TIME ZONE '2011-01-02 01:01:01+01',
    TIMESTAMP WITH TIME ZONE '2011-01-02 01:01:01+02'
    ]) AS time;

-- 1.13 Timestamptz with America/New_York
SET timezone TO 'America/New_York';
SELECT time, time_series.time_bucket(INTERVAL '1 hour', time)
FROM unnest(ARRAY[
    TIMESTAMP WITH TIME ZONE '2011-01-02 01:01:01',
    TIMESTAMP WITH TIME ZONE '2011-01-02 01:01:01+01',
    TIMESTAMP WITH TIME ZONE '2011-01-02 01:01:01+02'
    ]) AS time;

-- 1.14 Timestamptz day bucketing (UTC-aligned, not local)
SELECT time, time_series.time_bucket(INTERVAL '1 day', time)
FROM unnest(ARRAY[
    TIMESTAMP WITH TIME ZONE '2011-01-02 01:01:01',
    TIMESTAMP WITH TIME ZONE '2011-01-03 01:01:01+01',
    TIMESTAMP WITH TIME ZONE '2011-01-04 01:01:01+02'
    ]) AS time;

-- 1.15 DST boundary handling
SELECT time, time_series.time_bucket(INTERVAL '1 hour', time)
FROM unnest(ARRAY[
    TIMESTAMP WITH TIME ZONE '2017-11-05 12:05:00+07',
    TIMESTAMP WITH TIME ZONE '2017-11-05 13:05:00+07'
    ]) AS time;

SELECT time, time_series.time_bucket(INTERVAL '2 hour', time)
FROM unnest(ARRAY[
    TIMESTAMP WITH TIME ZONE '2017-11-05 10:05:00+07',
    TIMESTAMP WITH TIME ZONE '2017-11-05 12:05:00+07',
    TIMESTAMP WITH TIME ZONE '2017-11-05 13:05:00+07',
    TIMESTAMP WITH TIME ZONE '2017-11-05 15:05:00+07'
    ]) AS time;

RESET timezone;

-- 1.16 Smallint bucketing
SELECT time,
    time_series.time_bucket(10::smallint, time) AS tb
FROM unnest(ARRAY[
     '-11', '-10', '-9', '-1', '0', '1', '99', '100', '109', '110'
    ]::smallint[]) AS time;

-- 1.17 Smallint with positive offset
SELECT time,
    time_series.time_bucket(10::smallint, time, 2::smallint) AS tb
FROM unnest(ARRAY[
      '-9', '-8', '-7', '1', '2', '3', '101', '102', '111', '112'
    ]::smallint[]) AS time;

-- 1.18 Smallint with negative offset
SELECT time,
    time_series.time_bucket(10::smallint, time, -2::smallint) AS tb
FROM unnest(ARRAY[
    '-13', '-12', '-11', '-3', '-2', '-1', '97', '98', '107', '108'
    ]::smallint[]) AS time;

-- 1.19 Smallint overflow errors
\set ON_ERROR_STOP 0
SELECT time_series.time_bucket(10::smallint, '-32768'::smallint);
SELECT time_series.time_bucket(10::smallint, '-32761'::smallint);
SELECT time_series.time_bucket(10::smallint, '-32768'::smallint, 1000::smallint);
SELECT time_series.time_bucket(10::smallint, '-32768'::smallint, '32767'::smallint);
SELECT time_series.time_bucket(10::smallint, '32767'::smallint, '-32768'::smallint);
\set ON_ERROR_STOP 1

SELECT time, time_series.time_bucket(10::smallint, time)
FROM unnest(ARRAY['-32760', '-32759', '32767']::smallint[]) AS time;

-- 1.20 Int overflow errors
\set ON_ERROR_STOP 0
SELECT time_series.time_bucket(10::int, '-2147483648'::int);
SELECT time_series.time_bucket(10::int, '-2147483641'::int);
SELECT time_series.time_bucket(1000::int, '-2147483000'::int, 1::int);
SELECT time_series.time_bucket(1000::int, '-2147483648'::int, '2147483647'::int);
SELECT time_series.time_bucket(1000::int, '2147483647'::int, '-2147483648'::int);
\set ON_ERROR_STOP 1

SELECT time, time_series.time_bucket(10::int, time)
FROM unnest(ARRAY['-2147483640', '-2147483639', '2147483647']::int[]) AS time;

-- 1.21 Bigint overflow errors
\set ON_ERROR_STOP 0
SELECT time_series.time_bucket(10::bigint, '-9223372036854775808'::bigint);
SELECT time_series.time_bucket(10::bigint, '-9223372036854775801'::bigint);
SELECT time_series.time_bucket(1000::bigint, '-9223372036854775000'::bigint, 1::bigint);
SELECT time_series.time_bucket(1000::bigint, '-9223372036854775808'::bigint, '9223372036854775807'::bigint);
SELECT time_series.time_bucket(1000::bigint, '9223372036854775807'::bigint, '-9223372036854775808'::bigint);
\set ON_ERROR_STOP 1

SELECT time, time_series.time_bucket(10::bigint, time)
FROM unnest(ARRAY['-9223372036854775800', '-9223372036854775799', '9223372036854775807']::bigint[]) AS time;

-- 1.22 Date bucketing (1 day, 4 day)
SELECT time, time_series.time_bucket(INTERVAL '1 day', time::date)
FROM unnest(ARRAY[
    date '2017-11-05',
    date '2017-11-06'
    ]) AS time;

SELECT time, time_series.time_bucket(INTERVAL '4 day', time::date)
FROM unnest(ARRAY[
    date '2017-11-04',
    date '2017-11-05',
    date '2017-11-08',
    date '2017-11-09'
    ]) AS time;

-- 1.23 Date bucketing with offset
SELECT time, time_series.time_bucket(INTERVAL '4 day', time::date, INTERVAL '2 day')
FROM unnest(ARRAY[
    date '2017-11-06',
    date '2017-11-07',
    date '2017-11-10',
    date '2017-11-11'
    ]) AS time;

-- 1.24 Week bucketing (date)
SELECT time, time_series.time_bucket(INTERVAL '1 week', time::date)
FROM unnest(ARRAY[
    date '2018-09-16',
    date '2018-09-17',
    date '2018-09-23',
    date '2018-09-24'
    ]) AS time;

-- 1.25 Week bucketing (timestamp)
SELECT time, time_series.time_bucket(INTERVAL '1 week', time)
FROM unnest(ARRAY[
    timestamp without time zone '2018-09-16',
    timestamp without time zone '2018-09-17',
    timestamp without time zone '2018-09-23',
    timestamp without time zone '2018-09-24'
    ]) AS time;

-- 1.26 Week bucketing (timestamptz)
SELECT time, time_series.time_bucket(INTERVAL '1 week', time)
FROM unnest(ARRAY[
    timestamp with time zone '2018-09-16',
    timestamp with time zone '2018-09-17',
    timestamp with time zone '2018-09-23',
    timestamp with time zone '2018-09-24'
    ]) AS time;

-- 1.27 Week bucketing with infinity
SELECT time, time_series.time_bucket(INTERVAL '1 week', time)
FROM unnest(ARRAY[
    timestamp with time zone '-Infinity',
    timestamp with time zone 'Infinity'
    ]) AS time;

SELECT time, time_series.time_bucket(INTERVAL '1 week', time)
FROM unnest(ARRAY[
    timestamp without time zone '-Infinity',
    timestamp without time zone 'Infinity'
    ]) AS time;

-- 1.28 Week alignment with date_trunc (far dates)
SELECT time, time_series.time_bucket(INTERVAL '1 week', time),
       date_trunc('week', time) = time_series.time_bucket(INTERVAL '1 week', time)
FROM unnest(ARRAY[
    timestamp without time zone '4714-11-24 01:01:01.0 BC',
    timestamp without time zone '294276-12-31 23:59:59.9999'
    ]) AS time;

-- 1.29 Weeks align 1000 years later
SELECT time, time_series.time_bucket(INTERVAL '1 week', time),
       date_trunc('week', time) = time_series.time_bucket(INTERVAL '1 week', time)
FROM unnest(ARRAY[
    timestamp without time zone '3018-09-14',
    timestamp without time zone '3018-09-20',
    timestamp without time zone '3018-09-21',
    timestamp without time zone '3018-09-22'
    ]) AS time;

-- 1.30 Origin parameter (timestamptz)
\x
SELECT time, time_series.time_bucket(INTERVAL '1 week', time) no_epoch,
             time_series.time_bucket(INTERVAL '1 week', time) = time_series.time_bucket(INTERVAL '1 week', time, timestamptz '2000-01-03 00:00:00+0') always_true,
             time_series.time_bucket(INTERVAL '1 week', time, timestamptz '2000-01-01 00:00:00+0') pg_epoch,
             time_series.time_bucket(INTERVAL '1 week', time, timestamptz 'epoch') unix_epoch
FROM unnest(ARRAY[
    timestamp with time zone '2000-01-01 00:00:00+0' - interval '1 second',
    timestamp with time zone '2000-01-01 00:00:00+0',
    timestamp with time zone '2000-01-03 00:00:00+0' - interval '1 second',
    timestamp with time zone '2000-01-03 00:00:00+0',
    timestamp with time zone '2000-01-01',
    timestamp with time zone '2000-01-02',
    timestamp with time zone '2000-01-03'
    ]) AS time;

-- 1.31 Origin parameter (timestamp)
SELECT time, time_series.time_bucket(INTERVAL '1 week', time) no_epoch,
             time_series.time_bucket(INTERVAL '1 week', time) = time_series.time_bucket(INTERVAL '1 week', time, timestamp '2000-01-03 00:00:00') always_true,
             time_series.time_bucket(INTERVAL '1 week', time, timestamp '2000-01-01 00:00:00') pg_epoch,
             time_series.time_bucket(INTERVAL '1 week', time, timestamp 'epoch') unix_epoch
FROM unnest(ARRAY[
    timestamp without time zone '2000-01-01 00:00:00' - interval '1 second',
    timestamp without time zone '2000-01-01 00:00:00',
    timestamp without time zone '2000-01-03 00:00:00' - interval '1 second',
    timestamp without time zone '2000-01-03 00:00:00',
    timestamp without time zone '2000-01-01',
    timestamp without time zone '2000-01-02',
    timestamp without time zone '2000-01-03'
    ]) AS time;

-- 1.32 Origin parameter (date)
SELECT time, time_series.time_bucket(INTERVAL '1 week', time) no_epoch,
             time_series.time_bucket(INTERVAL '1 week', time) = time_series.time_bucket(INTERVAL '1 week', time, date '2000-01-03') always_true,
             time_series.time_bucket(INTERVAL '1 week', time, date '2000-01-01') pg_epoch,
             time_series.time_bucket(INTERVAL '1 week', time, (timestamp 'epoch')::date) unix_epoch
FROM unnest(ARRAY[
    date '1999-12-31',
    date '2000-01-01',
    date '2000-01-02',
    date '2000-01-03'
    ]) AS time;
\x

-- 1.33 Origin overflow errors
\set ON_ERROR_STOP 0
SELECT time, time_series.time_bucket(INTERVAL '100000 day', time, timestamp without time zone '4710-11-24 01:01:01.0 BC')
FROM unnest(ARRAY[
    timestamp without time zone '294270-12-31 23:59:59.9999'
    ]) AS time;
SELECT time, time_series.time_bucket(INTERVAL '100000 day', time, timestamp with time zone '4710-11-25 01:01:01.0 BC')
FROM unnest(ARRAY[
    timestamp with time zone '294270-12-30 23:59:59.9999'
    ]) AS time;
SELECT time, time_series.time_bucket(INTERVAL '10000000 day', time, timestamp without time zone '294270-12-31 23:59:59.9999')
FROM unnest(ARRAY[
    timestamp without time zone '4710-11-24 01:01:01.0 BC'
    ]) AS time;
SELECT time, time_series.time_bucket(INTERVAL '10000000 day', time, timestamp with time zone '294270-12-31 23:59:59.9999')
FROM unnest(ARRAY[
    timestamp with time zone '4710-11-24 01:01:01.0 BC'
    ]) AS time;
\set ON_ERROR_STOP 1

-- 1.34 Month/year bucketing
SET datestyle TO ISO;

SELECT
  time::date,
  time_series.time_bucket('1 month', time::date) AS "1m",
  time_series.time_bucket('2 month', time::date) AS "2m",
  time_series.time_bucket('3 month', time::date) AS "3m",
  time_series.time_bucket('1 month', time::date, '2000-02-01'::date) AS "1m origin",
  time_series.time_bucket('2 month', time::date, '2000-02-01'::date) AS "2m origin",
  time_series.time_bucket('3 month', time::date, '2000-02-01'::date) AS "3m origin"
FROM generate_series('1990-01-03'::date, '1990-06-03'::date, '1month'::interval) time;

SELECT
  time,
  time_series.time_bucket('1 month', time) AS "1m",
  time_series.time_bucket('2 month', time) AS "2m",
  time_series.time_bucket('3 month', time) AS "3m",
  time_series.time_bucket('1 month', time, '2000-02-01'::timestamp) AS "1m origin",
  time_series.time_bucket('2 month', time, '2000-02-01'::timestamp) AS "2m origin",
  time_series.time_bucket('3 month', time, '2000-02-01'::timestamp) AS "3m origin"
FROM generate_series('1990-01-03'::timestamp, '1990-06-03'::timestamp, '1month'::interval) time;

SELECT
  time,
  time_series.time_bucket('1 month', time) AS "1m",
  time_series.time_bucket('2 month', time) AS "2m",
  time_series.time_bucket('3 month', time) AS "3m",
  time_series.time_bucket('1 month', time, '2000-02-01'::timestamptz) AS "1m origin",
  time_series.time_bucket('2 month', time, '2000-02-01'::timestamptz) AS "2m origin",
  time_series.time_bucket('3 month', time, '2000-02-01'::timestamptz) AS "3m origin"
FROM generate_series('1990-01-03'::timestamptz, '1990-06-03'::timestamptz, '1month'::interval) time;

RESET datestyle;

-- ============================================================
-- Section 2: NULL parameter handling
-- ============================================================

-- All STRICT overloads return NULL for any NULL input

-- 2a. NULL interval (timestamp variants)
SELECT time_series.time_bucket(NULL::interval, TIMESTAMP '2024-01-01');
SELECT time_series.time_bucket(NULL::interval, TIMESTAMPTZ '2024-01-01');
SELECT time_series.time_bucket(NULL::interval, DATE '2024-01-01');

-- 2b. NULL timestamp
SELECT time_series.time_bucket(INTERVAL '1 hour', NULL::timestamp);
SELECT time_series.time_bucket(INTERVAL '1 hour', NULL::timestamptz);
SELECT time_series.time_bucket(INTERVAL '1 day', NULL::date);

-- 2c. NULL integer types
SELECT time_series.time_bucket(NULL::int, 42);
SELECT time_series.time_bucket(10, NULL::int);
SELECT time_series.time_bucket(NULL::bigint, 42::bigint);
SELECT time_series.time_bucket(10::bigint, NULL::bigint);
SELECT time_series.time_bucket(NULL::smallint, 42::smallint);
SELECT time_series.time_bucket(10::smallint, NULL::smallint);

-- 2d. NULL origin (STRICT variants return NULL)
SELECT time_series.time_bucket(INTERVAL '1 hour', TIMESTAMP '2024-01-01', NULL::timestamp);
SELECT time_series.time_bucket(INTERVAL '1 hour', TIMESTAMPTZ '2024-01-01', NULL::timestamptz);
SELECT time_series.time_bucket(INTERVAL '1 day', DATE '2024-01-01', NULL::date);

-- 2e. NULL offset (STRICT variants return NULL)
SELECT time_series.time_bucket(INTERVAL '1 hour', TIMESTAMP '2024-01-01', NULL::interval);
SELECT time_series.time_bucket(INTERVAL '1 hour', TIMESTAMPTZ '2024-01-01', NULL::interval);

-- 2f. NULL integer offset
SELECT time_series.time_bucket(10, 42, NULL::int);
SELECT time_series.time_bucket(10::bigint, 42::bigint, NULL::bigint);
SELECT time_series.time_bucket(10::smallint, 42::smallint, NULL::smallint);

-- ============================================================
-- Section 3: Timezone bucket with month intervals
-- ============================================================

SET datestyle TO ISO;

-- Timezone-aware bucketing with 1 month interval
SELECT
    time,
    time_series.time_bucket('1 month'::interval, time, 'America/New_York') AS bucket
FROM generate_series(
    '2024-01-15'::timestamptz,
    '2024-06-15'::timestamptz,
    '1 month'::interval
) time;

-- Timezone-aware bucketing with 3 month (quarterly) interval
SELECT
    time,
    time_series.time_bucket('3 month'::interval, time, 'UTC') AS bucket
FROM generate_series(
    '2024-01-15'::timestamptz,
    '2024-12-15'::timestamptz,
    '2 month'::interval
) time;

RESET datestyle;

-- ============================================================
-- Section 4: Zero and negative period errors
-- ============================================================

\set ON_ERROR_STOP 0

-- Zero interval for timestamp
SELECT time_series.time_bucket(INTERVAL '0', TIMESTAMP '2024-01-01');
-- Zero interval for timestamptz
SELECT time_series.time_bucket(INTERVAL '0', TIMESTAMPTZ '2024-01-01');
-- Zero interval for date
SELECT time_series.time_bucket(INTERVAL '0 day', DATE '2024-01-01');

-- Negative interval for timestamp
SELECT time_series.time_bucket(INTERVAL '-1 hour', TIMESTAMP '2024-01-01');
-- Negative interval for timestamptz
SELECT time_series.time_bucket(INTERVAL '-1 hour', TIMESTAMPTZ '2024-01-01');
-- Negative interval for date
SELECT time_series.time_bucket(INTERVAL '-1 day', DATE '2024-01-01');

-- Zero integer period
SELECT time_series.time_bucket(0, 42);
SELECT time_series.time_bucket(0::bigint, 42::bigint);
SELECT time_series.time_bucket(0::smallint, 42::smallint);

-- Negative integer period
SELECT time_series.time_bucket(-10, 42);
SELECT time_series.time_bucket(-10::bigint, 42::bigint);
SELECT time_series.time_bucket(-10::smallint, 42::smallint);

\set ON_ERROR_STOP 1

-- ============================================================
-- Section 5: Sub-second bucketing
-- ============================================================

-- 500ms bucketing
SELECT time, time_series.time_bucket(INTERVAL '500 milliseconds', time)
FROM unnest(ARRAY[
    TIMESTAMP '2024-01-01 00:00:00.000',
    TIMESTAMP '2024-01-01 00:00:00.499',
    TIMESTAMP '2024-01-01 00:00:00.500',
    TIMESTAMP '2024-01-01 00:00:00.999',
    TIMESTAMP '2024-01-01 00:00:01.000',
    TIMESTAMP '2024-01-01 00:00:01.250'
    ]) AS time;

-- 100ms bucketing
SELECT time, time_series.time_bucket(INTERVAL '100 milliseconds', time)
FROM unnest(ARRAY[
    TIMESTAMP '2024-01-01 00:00:00.000',
    TIMESTAMP '2024-01-01 00:00:00.099',
    TIMESTAMP '2024-01-01 00:00:00.100',
    TIMESTAMP '2024-01-01 00:00:00.199',
    TIMESTAMP '2024-01-01 00:00:00.999'
    ]) AS time;

-- 1ms bucketing
SELECT time, time_series.time_bucket(INTERVAL '1 millisecond', time)
FROM unnest(ARRAY[
    TIMESTAMP '2024-01-01 00:00:00.000',
    TIMESTAMP '2024-01-01 00:00:00.0001',
    TIMESTAMP '2024-01-01 00:00:00.001',
    TIMESTAMP '2024-01-01 00:00:00.0019'
    ]) AS time;

-- Sub-second with timestamptz
SELECT time, time_series.time_bucket(INTERVAL '500 milliseconds', time)
FROM unnest(ARRAY[
    TIMESTAMPTZ '2024-01-01 00:00:00.000+00',
    TIMESTAMPTZ '2024-01-01 00:00:00.499+00',
    TIMESTAMPTZ '2024-01-01 00:00:00.500+00',
    TIMESTAMPTZ '2024-01-01 00:00:00.999+00'
    ]) AS time;

-- ============================================================
-- Section 6: date_trunc alignment verification
-- ============================================================

-- time_bucket('1 day') should align with date_trunc('day')
SELECT time,
    time_series.time_bucket(INTERVAL '1 day', time) AS tb,
    date_trunc('day', time) AS dt,
    time_series.time_bucket(INTERVAL '1 day', time) = date_trunc('day', time) AS aligned
FROM unnest(ARRAY[
    TIMESTAMP '2024-01-15 12:30:00',
    TIMESTAMP '2024-06-15 00:00:00',
    TIMESTAMP '2024-12-31 23:59:59'
    ]) AS time;

-- time_bucket('1 hour') should align with date_trunc('hour')
SELECT time,
    time_series.time_bucket(INTERVAL '1 hour', time) AS tb,
    date_trunc('hour', time) AS dt,
    time_series.time_bucket(INTERVAL '1 hour', time) = date_trunc('hour', time) AS aligned
FROM unnest(ARRAY[
    TIMESTAMP '2024-01-15 12:30:00',
    TIMESTAMP '2024-01-15 12:00:00',
    TIMESTAMP '2024-01-15 12:59:59.999'
    ]) AS time;

-- time_bucket('1 minute') should align with date_trunc('minute')
SELECT time,
    time_series.time_bucket(INTERVAL '1 minute', time) AS tb,
    date_trunc('minute', time) AS dt,
    time_series.time_bucket(INTERVAL '1 minute', time) = date_trunc('minute', time) AS aligned
FROM unnest(ARRAY[
    TIMESTAMP '2024-01-15 12:30:45',
    TIMESTAMP '2024-01-15 12:30:00',
    TIMESTAMP '2024-01-15 12:30:59.999'
    ]) AS time;

-- ============================================================
-- Section 7: Non-month interval error for date bucketing
-- ============================================================

\set ON_ERROR_STOP 0
-- Sub-day intervals should error for date bucketing
SELECT time_series.time_bucket(INTERVAL '1 hour', DATE '2024-01-01');
SELECT time_series.time_bucket(INTERVAL '25 hours', DATE '2024-01-01');
-- Non-multiple-of-day interval
SELECT time_series.time_bucket(INTERVAL '1 day 1 hour', DATE '2024-01-01');
\set ON_ERROR_STOP 1

-- ============================================================
-- Section 8: Timestamptz vs timestamp casting behavior
-- ============================================================

SET timezone TO 'America/New_York';

-- timestamptz bucketing respects UTC epoch, not local time
SELECT
    time,
    time_series.time_bucket(INTERVAL '1 day', time) AS bucket_tz,
    time_series.time_bucket(INTERVAL '1 day', time::timestamp) AS bucket_local
FROM unnest(ARRAY[
    TIMESTAMPTZ '2024-01-15 02:00:00+00',
    TIMESTAMPTZ '2024-06-15 02:00:00+00'
    ]) AS time;

RESET timezone;

-- ============================================================
-- Section 9: DST boundary with local timezone casting
-- ============================================================

SET timezone TO 'America/New_York';

-- Spring forward: 2024-03-10 02:00 AM doesn't exist in America/New_York
SELECT time, time_series.time_bucket(INTERVAL '1 hour', time)
FROM unnest(ARRAY[
    TIMESTAMPTZ '2024-03-10 06:00:00+00',  -- 01:00 EST
    TIMESTAMPTZ '2024-03-10 07:00:00+00',  -- 03:00 EDT (skipped 02:00)
    TIMESTAMPTZ '2024-03-10 08:00:00+00'   -- 04:00 EDT
    ]) AS time;

-- Fall back: 2024-11-03 01:00 AM exists twice in America/New_York
SELECT time, time_series.time_bucket(INTERVAL '1 hour', time)
FROM unnest(ARRAY[
    TIMESTAMPTZ '2024-11-03 04:00:00+00',  -- 00:00 EDT
    TIMESTAMPTZ '2024-11-03 05:00:00+00',  -- 01:00 EDT
    TIMESTAMPTZ '2024-11-03 06:00:00+00',  -- 01:00 EST (same local time)
    TIMESTAMPTZ '2024-11-03 07:00:00+00'   -- 02:00 EST
    ]) AS time;

-- Day-level bucketing across DST boundary
SELECT time, time_series.time_bucket(INTERVAL '1 day', time)
FROM unnest(ARRAY[
    TIMESTAMPTZ '2024-03-09 12:00:00-05',
    TIMESTAMPTZ '2024-03-10 12:00:00-04',
    TIMESTAMPTZ '2024-03-11 12:00:00-04'
    ]) AS time;

RESET timezone;

-- ============================================================
-- Section 10: Month interval cannot mix with day/time components
-- ============================================================

\set ON_ERROR_STOP 0
-- These should error: month intervals with day or time components
SELECT time_series.time_bucket(INTERVAL '1 month 1 day', TIMESTAMP '2024-01-01');
SELECT time_series.time_bucket(INTERVAL '1 year 1 hour', TIMESTAMP '2024-01-01');
SELECT time_series.time_bucket(INTERVAL '1 month 1 second', TIMESTAMPTZ '2024-01-01');
\set ON_ERROR_STOP 1

-- ============================================================
-- Section 11: Rounding version (offset to simulate rounding)
-- (from TimescaleDB timestamp-15.sql line 254-262)
-- ============================================================

-- Using negative half-bucket offset + adding half back simulates rounding
SELECT time, time_series.time_bucket(INTERVAL '5 minute', time, - INTERVAL '2.5 minutes') + INTERVAL '2 minutes 30 seconds'
FROM unnest(ARRAY[
    TIMESTAMP '2011-01-02 01:05:01',
    TIMESTAMP '2011-01-02 01:07:29',
    TIMESTAMP '2011-01-02 01:02:30',
    TIMESTAMP '2011-01-02 01:07:30',
    TIMESTAMP '2011-01-02 01:02:29'
    ]) AS time;

-- ============================================================
-- Section 12: Force local alignment with timestamp cast
-- (from TimescaleDB timestamp-15.sql line 298-336)
-- ============================================================

SET timezone TO 'America/New_York';

-- Can force local bucketing with simple cast (timestamptz → timestamp strips timezone)
SELECT time,
    time_series.time_bucket(INTERVAL '1 day', time::timestamp) AS local_bucket,
    date_trunc('day', time) AS date_trunc_local
FROM unnest(ARRAY[
    TIMESTAMP WITH TIME ZONE '2011-01-02 01:01:01',
    TIMESTAMP WITH TIME ZONE '2011-01-03 01:01:01+01',
    TIMESTAMP WITH TIME ZONE '2011-01-04 01:01:01+02'
    ]) AS time;

-- Can also use interval offset to correct for timezone (-19h for EST = UTC-5)
SELECT time,
    time_series.time_bucket(INTERVAL '1 day', time, -INTERVAL '19 hours') AS offset_bucket,
    date_trunc('day', time) AS date_trunc_local
FROM unnest(ARRAY[
    TIMESTAMP WITH TIME ZONE '2011-01-02 01:01:01',
    TIMESTAMP WITH TIME ZONE '2011-01-03 01:01:01+01',
    TIMESTAMP WITH TIME ZONE '2011-01-04 01:01:01+02'
    ]) AS time;

-- Local alignment preserved when bucketing by local time across DST boundary
SELECT time, time_series.time_bucket(INTERVAL '2 hour', time::timestamp)
FROM unnest(ARRAY[
    TIMESTAMP WITH TIME ZONE '2017-11-05 10:05:00+07',
    TIMESTAMP WITH TIME ZONE '2017-11-05 12:05:00+07',
    TIMESTAMP WITH TIME ZONE '2017-11-05 13:05:00+07',
    TIMESTAMP WITH TIME ZONE '2017-11-05 15:05:00+07'
    ]) AS time;

RESET timezone;

-- ============================================================
-- Section 13: Week alignment with local cast (timestamptz)
-- (from TimescaleDB timestamp-15.sql line 512-519)
-- ============================================================

SET timezone TO 'America/New_York';

-- Weeks align for timestamptz if cast to local time, but not if done at UTC
SELECT time,
    date_trunc('week', time) = time_series.time_bucket(INTERVAL '1 week', time) AS "utc_aligned",
    date_trunc('week', time) = time_series.time_bucket(INTERVAL '1 week', time::timestamp) AS "local_aligned"
FROM unnest(ARRAY[
    timestamp with time zone '3018-09-14',
    timestamp with time zone '3018-09-20',
    timestamp with time zone '3018-09-21',
    timestamp with time zone '3018-09-22'
    ]) AS time;

RESET timezone;
SET timezone = 'PST8PDT';

-- ============================================================
-- Section 14: BC dates as origin (working, not error)
-- (from TimescaleDB timestamp-15.sql line 583-599)
-- ============================================================

-- Really old origin works if date around that time
SELECT time, time_series.time_bucket(INTERVAL '1 week', time, timestamp without time zone '4710-11-24 01:01:01.0 BC')
FROM unnest(ARRAY[
    timestamp without time zone '4710-11-24 01:01:01.0 BC',
    timestamp without time zone '4710-11-25 01:01:01.0 BC',
    timestamp without time zone '2001-01-01',
    timestamp without time zone '3001-01-01'
    ]) AS time;

-- Really new origin
SELECT time, time_series.time_bucket(INTERVAL '1 week', time, timestamp without time zone '294270-12-30 23:59:59.9999')
FROM unnest(ARRAY[
    timestamp without time zone '294270-12-29 23:59:59.9999',
    timestamp without time zone '294270-12-30 23:59:59.9999',
    timestamp without time zone '294270-12-31 23:59:59.9999',
    timestamp without time zone '2001-01-01',
    timestamp without time zone '3001-01-01'
    ]) AS time;

-- ============================================================
-- Section 15: Timezone NULL args
-- (from TimescaleDB timestamp-15.sql line 663-672)
-- ============================================================

SELECT time_series.time_bucket(NULL::interval, now(), 'Europe/Berlin');
SELECT time_series.time_bucket('1day', NULL::timestamptz, 'Europe/Berlin');
SELECT time_series.time_bucket('1day', now(), NULL::text);

-- ============================================================
-- Section 16: Multi-timezone parallel bucketing
-- (from TimescaleDB timestamp-15.sql line 674-695)
-- ============================================================

SET datestyle TO ISO;

SELECT
  time_series.time_bucket('1day', ts) AS "UTC",
  time_series.time_bucket('1day', ts, 'Europe/Berlin') AS "Berlin",
  time_series.time_bucket('1day', ts, 'Europe/London') AS "London",
  time_series.time_bucket('1day', ts, 'America/New_York') AS "New_York",
  time_series.time_bucket('1day', ts, 'PST') AS "PST",
  time_series.time_bucket('1day', ts, current_setting('timezone')) AS "current"
FROM generate_series('1999-12-31 17:00'::timestamptz,'2000-01-02 3:00'::timestamptz, '1hour'::interval) ts;

SELECT
  time_series.time_bucket('1month', ts) AS "UTC",
  time_series.time_bucket('1month', ts, 'Europe/Berlin') AS "Berlin",
  time_series.time_bucket('1month', ts, 'America/New_York') AS "New_York",
  time_series.time_bucket('1month', ts, current_setting('timezone')) AS "current",
  time_series.time_bucket('2month', ts, current_setting('timezone')) AS "2m"
FROM generate_series('1999-12-01'::timestamptz,'2000-09-01'::timestamptz, '9 day'::interval) ts;

RESET datestyle;

-- ============================================================
-- Section 17: Origin with custom values (extended from Section 1.30)
-- (from TimescaleDB timestamp-15.sql line 521-580)
-- ============================================================

\x
-- timestamptz origin with custom origins
SELECT time, time_series.time_bucket(INTERVAL '1 week', time) no_epoch,
             time_series.time_bucket(INTERVAL '1 week', time) = time_series.time_bucket(INTERVAL '1 week', time, timestamptz '2000-01-03 00:00:00+0') always_true,
             time_series.time_bucket(INTERVAL '1 week', time, timestamptz '2000-01-01 00:00:00+0') pg_epoch,
             time_series.time_bucket(INTERVAL '1 week', time, timestamptz 'epoch') unix_epoch
FROM unnest(ARRAY[
    timestamp with time zone '3018-09-12',
    timestamp with time zone '3018-09-13',
    timestamp with time zone '3018-09-14',
    timestamp with time zone '3018-09-15'
    ]) AS time;

-- timestamp origin with custom origins
SELECT time, time_series.time_bucket(INTERVAL '1 week', time) no_epoch,
             time_series.time_bucket(INTERVAL '1 week', time) = time_series.time_bucket(INTERVAL '1 week', time, timestamp '2000-01-03 00:00:00') always_true,
             time_series.time_bucket(INTERVAL '1 week', time, timestamp '2000-01-01 00:00:00+0') pg_epoch,
             time_series.time_bucket(INTERVAL '1 week', time, timestamp 'epoch') unix_epoch
FROM unnest(ARRAY[
    timestamp without time zone '3018-09-12',
    timestamp without time zone '3018-09-13',
    timestamp without time zone '3018-09-14',
    timestamp without time zone '3018-09-15'
    ]) AS time;

-- date origin with custom origins
SELECT time, time_series.time_bucket(INTERVAL '1 week', time) no_epoch,
             time_series.time_bucket(INTERVAL '1 week', time) = time_series.time_bucket(INTERVAL '1 week', time, date '2000-01-03') always_true,
             time_series.time_bucket(INTERVAL '1 week', time, date '2000-01-01') pg_epoch,
             time_series.time_bucket(INTERVAL '1 week', time, (timestamp 'epoch')::date) unix_epoch
FROM unnest(ARRAY[
    date '3018-09-12',
    date '3018-09-13',
    date '3018-09-14',
    date '3018-09-15'
    ]) AS time;
\x

-- ============================================================
-- Section 18: Origin alignment example
-- (from TimescaleDB timestamp-15.sql line 244-251)
-- ============================================================

-- Example to align with an origin using offset arithmetic
SELECT time, time_series.time_bucket(INTERVAL '5 minute', time - (TIMESTAMP '2011-01-02 00:02:00' - TIMESTAMP 'epoch')) + (TIMESTAMP '2011-01-02 00:02:00' - TIMESTAMP 'epoch')
FROM unnest(ARRAY[
    TIMESTAMP '2011-01-02 01:01:59.999999',
    TIMESTAMP '2011-01-02 01:02:00',
    TIMESTAMP '2011-01-02 01:06:59.999999',
    TIMESTAMP '2011-01-02 01:07:00'
    ]) AS time;

-- ============================================================
-- Section 19: Int/Bigint bucketing (combined test)
-- (from TimescaleDB timestamp-15.sql line 339-354)
-- ============================================================

-- All three integer types in one test
SELECT time,
    time_series.time_bucket(10::smallint, time) AS time_bucket_smallint,
    time_series.time_bucket(10::int, time::int) AS time_bucket_int,
    time_series.time_bucket(10::bigint, time::bigint) AS time_bucket_bigint
FROM unnest(ARRAY[
     '-11', '-10', '-9', '-1', '0', '1', '99', '100', '109', '110'
    ]::smallint[]) AS time;

SELECT time,
    time_series.time_bucket(10::smallint, time, 2::smallint) AS time_bucket_smallint,
    time_series.time_bucket(10::int, time::int, 2::int) AS time_bucket_int,
    time_series.time_bucket(10::bigint, time::bigint, 2::bigint) AS time_bucket_bigint
FROM unnest(ARRAY[
      '-9', '-8', '-7', '1', '2', '3', '101', '102', '111', '112'
    ]::smallint[]) AS time;

SELECT time,
    time_series.time_bucket(10::smallint, time, -2::smallint) AS time_bucket_smallint,
    time_series.time_bucket(10::int, time::int, -2::int) AS time_bucket_int,
    time_series.time_bucket(10::bigint, time::bigint, -2::bigint) AS time_bucket_bigint
FROM unnest(ARRAY[
    '-13', '-12', '-11', '-3', '-2', '-1', '97', '98', '107', '108'
    ]::smallint[]) AS time;

DROP EXTENSION time_series CASCADE;
