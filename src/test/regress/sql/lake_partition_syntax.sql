--
-- Iceberg lake-table PARTITION BY grammar (syntax-only).
--
-- A lake table can only be created with the datalake_fdw extension (plus a
-- foreign catalog and volume), so the functional behaviour of PARTITION BY
-- (partition layout, NULL partitions, pruning, duplicate/unknown column and
-- HUDI rejection) is covered by the datalake_fdw regression suite
-- (contrib/datalake_fdw .../iceberg_ddl/04_partition_by).
--
-- This test only locks the gram.y OptIcebergPartitionBy production so an
-- accidental grammar change is caught in the core suite: the well-formed
-- PARTITION BY (...) clause must PARSE (it then stops at the
-- datalake_fdw-not-installed check, which proves it was not a syntax error),
-- and the malformed forms must be rejected at parse time.

-- Well-formed: parses, then fails at extension resolution (not a syntax error).
-- Single identity column.
CREATE ICEBERG TABLE lake_pt_syntax1 (a int, b text) PARTITION BY (a);
-- Multiple identity columns.
CREATE ICEBERG TABLE lake_pt_syntax2 (a int, b text) PARTITION BY (a, b);
-- PARTITION BY is grammatically accepted on HUDI as well (it is rejected later
-- in laketablecmds, which likewise needs the extension).
CREATE HUDI TABLE lake_pt_syntax3 (a int) PARTITION BY (a);

-- Malformed: the clause shape is fixed as PARTITION BY ( columnList ).
CREATE ICEBERG TABLE lake_pt_bad1 (a int) PARTITION BY ();
CREATE ICEBERG TABLE lake_pt_bad2 (a int) PARTITION BY a;
CREATE ICEBERG TABLE lake_pt_bad3 (a int) PARTITION (a);
