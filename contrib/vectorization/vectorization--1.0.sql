-- dummy types use shell_in/shell_out that will error
-- if typein/typeout is really called

-- Function: clearfile(boolean)
-- This function is named 'clearfile' which takes a boolean parameter.

-- The function is defined to return a boolean value.
-- The implementation of this function is linked to an external C library.
-- The library location is specified as '$libdir/vectorization' and the function name within that library is 'clearfile'.
-- The language used for this function is C.

-- Regarding the behavior based on the input boolean value:
-- If the input is false, it means non - mandatory check for cleaning all vectorization files.
-- In this case, if a file is in use (occupied), it will not be processed. Only the files that are not in use will be cleared.
-- If the input is true, it indicates a mandatory clean - up of all temporary files related to vectorization.

CREATE TYPE pg_ext_aux.stddev;

CREATE FUNCTION pg_ext_aux.stddev_type_in(cstring)
 RETURNS pg_ext_aux.stddev
 AS '$libdir/vectorization', 'vector_stddev_in'
 LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION pg_ext_aux.stddev_type_out(pg_ext_aux.stddev)
 RETURNS cstring
 AS '$libdir/vectorization', 'vector_stddev_out'
 LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION clearfile(boolean)
  RETURNS boolean
AS '$libdir/vectorization', 'clearfile'
LANGUAGE C STRICT VOLATILE;

CREATE TYPE pg_ext_aux.stddev(
 input = pg_ext_aux.stddev_type_in,
 output = pg_ext_aux.stddev_type_out,
 internallength = 8);

--
-- Dummy type used to convert arrow types
--

CREATE TYPE pg_ext_aux.arrow_avg_int_bytea AS (sum int, count int);


-- Memory arrow held for the last vectorized query in this backend.
--   segindex            which segment answered (-1 is the coordinator)
--   query_peak_bytes    high-water mark of the query-scoped pool
--   live_bytes          what is still held now
--   arrow_total_peak    arrow's process-wide high-water mark, never reset
--
-- Per segment:  SELECT * FROM gp_dist_random('vector_memory_stats');
CREATE FUNCTION vector_memory_stats(
    OUT segindex int4,
    OUT query_peak_bytes int8,
    OUT live_bytes int8,
    OUT arrow_total_peak int8)
RETURNS record
AS '$libdir/vectorization', 'vector_memory_stats'
LANGUAGE C VOLATILE;

-- gp_dist_random() takes a relation, so the per-segment reading goes through
-- a view over the function:
--
--   SELECT segindex, query_peak_bytes
--     FROM gp_dist_random('vector_memory_stats_v')
--    ORDER BY query_peak_bytes DESC;
--
-- Read it from the session that ran the query.  The figures live in the
-- backends that executed it, and a new session gets new ones.
CREATE VIEW vector_memory_stats_v AS SELECT * FROM vector_memory_stats();
