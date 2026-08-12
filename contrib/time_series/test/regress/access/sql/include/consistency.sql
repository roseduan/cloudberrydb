-- contrib/time_series/test/regress/access/sql/include/consistency.sql
--
-- Catalog ↔ catalog consistency invariant checker.
--
-- Defines test.ts_check_consistency(rel regclass) which returns 'OK' when
-- the time_series catalogs are mutually consistent for the given table,
-- or a comma-separated list of detected problems otherwise.
--
-- Usage (include after setup.sql):
--   \ir include/setup.sql
--   \ir include/consistency.sql
--   ...test body...
--   SELECT test.ts_check_consistency('mytab'::regclass);   -- expect: OK
--
-- Invariants checked (status codes from ts_compress.h):
--   (a) status = COMPRESSED (1)  → ts_compressed_chunk has a matching row
--   (b) status = PARTIAL    (2)  → ts_compressed_chunk has a matching row
--   (c) ts_compressed_chunk row  → ts_chunk has a matching row
--   (d) status = ACTIVE     (0)  → ts_compressed_chunk has NO matching row
--
-- Each row of ts_chunk / ts_compressed_chunk is per-segment, so we project
-- to DISTINCT chunk_number to make the check segment-count independent.

CREATE OR REPLACE FUNCTION test.ts_check_consistency(rel regclass)
RETURNS text
LANGUAGE SQL
STABLE AS $$
WITH
chunk_status AS (
    SELECT DISTINCT chunk_number, status
    FROM time_series.ts_chunk
    WHERE table_oid = rel
),
compressed_present AS (
    SELECT DISTINCT chunk_number
    FROM time_series.ts_compressed_chunk
    WHERE table_oid = rel
),
problems AS (
    -- (a) COMPRESSED requires compressed_chunk row
    SELECT format('COMPRESSED_without_meta:%s', chunk_number) AS msg
    FROM chunk_status
    WHERE status = 1
      AND chunk_number NOT IN (SELECT chunk_number FROM compressed_present)

    UNION ALL
    -- (b) PARTIAL also requires compressed_chunk row
    SELECT format('PARTIAL_without_meta:%s', chunk_number)
    FROM chunk_status
    WHERE status = 2
      AND chunk_number NOT IN (SELECT chunk_number FROM compressed_present)

    UNION ALL
    -- (c) compressed_chunk row must have a backing ts_chunk row
    SELECT format('orphan_compressed_meta:%s', chunk_number)
    FROM compressed_present
    WHERE chunk_number NOT IN (SELECT chunk_number FROM chunk_status)

    UNION ALL
    -- (d) ACTIVE must NOT have a compressed_chunk row
    SELECT format('ACTIVE_with_compressed_meta:%s', chunk_number)
    FROM chunk_status
    WHERE status = 0
      AND chunk_number IN (SELECT chunk_number FROM compressed_present)
)
SELECT COALESCE(string_agg(msg, ', ' ORDER BY msg), 'OK')
FROM problems;
$$;

COMMENT ON FUNCTION test.ts_check_consistency(regclass) IS
'Verifies time_series catalog consistency for the given table; returns OK or a list of problems.';
