-- Regression coverage for vec Motion hash on TIMESTAMP / TIMESTAMPTZ / TIME.
--
-- Without motion_rewrite_numeric_type_schema() relabelling these arrow
-- types to int64, Gandiva fails projector init with:
--   "Function int32 cohash32(timestamp[us]) not supported yet".
SET vector.enable_vectorization = ON;

DROP TABLE IF EXISTS vec_motion_time_redist;
CREATE TABLE vec_motion_time_redist (
    id  int,
    ts  timestamp,
    tsz timestamptz,
    tm  time
) WITH (appendonly=true, orientation=column) DISTRIBUTED BY (id);

INSERT INTO vec_motion_time_redist VALUES
    (1, '2020-01-01 00:00:00', '2020-01-01 00:00:00+00', '00:00:00'),
    (2, '2020-01-01 00:01:00', '2020-01-01 00:01:00+00', '00:01:00'),
    (3, '2020-01-01 00:02:00', '2020-01-01 00:02:00+00', '00:02:00');

-- TIMESTAMP: plan should show Vec Redistribute Motion with Hash Key: ts.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(*) AS n FROM (SELECT ts  FROM vec_motion_time_redist GROUP BY ts)  s;
SELECT count(*) AS n FROM (SELECT ts  FROM vec_motion_time_redist GROUP BY ts)  s;

-- TIMESTAMPTZ: GPORCA currently falls back to row engine for this plan
-- shape, so the Motion is a plain Redistribute Motion.  We keep the query
-- for forward coverage if the engine ever vectorises this path.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(*) AS n FROM (SELECT tsz FROM vec_motion_time_redist GROUP BY tsz) s;
SELECT count(*) AS n FROM (SELECT tsz FROM vec_motion_time_redist GROUP BY tsz) s;

-- TIME: plan should show Vec Redistribute Motion with Hash Key: tm.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(*) AS n FROM (SELECT tm  FROM vec_motion_time_redist GROUP BY tm)  s;
SELECT count(*) AS n FROM (SELECT tm  FROM vec_motion_time_redist GROUP BY tm)  s;

DROP TABLE vec_motion_time_redist;
