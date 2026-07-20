-- backpressure.sql
--
-- Lightweight regression coverage for vector backpressure routing.
-- These queries intentionally force tiny Arrow batches so that an incorrect
-- plain-sink backpressure configuration would block quickly instead of only
-- showing up under large data volumes.

CREATE SCHEMA backpressure_test;
SET search_path = backpressure_test;

SET optimizer = off;
SET vector.enable_vectorization = on;
SET vector.max_batch_size = 1;
SET statement_timeout = '10s';

CREATE TABLE bp_rows (
    id  int,
    grp int,
    pad text
) USING pax DISTRIBUTED BY (id);

INSERT INTO bp_rows
SELECT i, (i - 1) % 3, repeat('x', 128)
FROM generate_series(1, 12) AS i;

ANALYZE bp_rows;

-- Batch path must not install sink backpressure, or this multi-batch query
-- can deadlock before ExecuteVecPlan() starts consuming.  Use the smallest
-- legal memory budget so an incorrectly-armed SinkNode would trip
-- immediately rather than only under large data volumes.
SET vector.enable_vec_pipeline = off;
SET vector.backpressure_memory_mb = 1;

SELECT id
FROM bp_rows
WHERE id <= 3
ORDER BY id;

-- The default budget (256MB) is generous relative to this table. It should
-- remain a no-op even when the pipeline GUC is enabled.
SET vector.enable_vec_pipeline = on;
SET vector.backpressure_memory_mb = 256;

SELECT id
FROM bp_rows
WHERE id <= 3
ORDER BY id;

-- Blocking operators (Sort/Agg) work as synchronization points in
-- push_pipeline: they accumulate input, then output through SinkNode.
SET vector.backpressure_memory_mb = 1;

SELECT id
FROM bp_rows
ORDER BY id
LIMIT 3;

SELECT grp, count(*)
FROM bp_rows
GROUP BY grp
ORDER BY grp
LIMIT 3;

RESET statement_timeout;
RESET vector.max_batch_size;
RESET vector.backpressure_memory_mb;
RESET vector.enable_vec_pipeline;
RESET vector.enable_vectorization;
RESET optimizer;

DROP SCHEMA backpressure_test CASCADE;
