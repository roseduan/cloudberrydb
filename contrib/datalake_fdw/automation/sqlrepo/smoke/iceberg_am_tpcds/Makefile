# TPC-DS basic functional smoke for Iceberg AM (CREATE ICEBERG TABLE)
#
# Verifies the builtin-catalog + S3-volume path against the 24-table TPC-DS
# schema: schema DDL -> tiny synthetic load -> 34 functional checks covering
# star joins, GROUP BY / HAVING, window functions (rank/row_number/lag/sum),
# GROUPING SETS, DISTINCT ON, FILTER clause, CTEs (Sequence rewrite path),
# correlated subqueries, NOT EXISTS / NOT IN, UNION ALL / INTERSECT / EXCEPT,
# LATERAL join, LEFT/RIGHT/FULL OUTER JOIN, INSERT VALUES / INSERT SELECT,
# TRUNCATE, ALTER TABLE ADD/DROP COLUMN (schema evolution), date + string
# functions, NULL semantics, self-join, VACUUM compaction, and EXPLAIN.
#
# Requires the singlecluster stack (lakehouse container) running with MinIO
# at minio:9000 (admin/password) and bucket "warehouse".

PGXS := $(shell pg_config --pgxs)
include $(PGXS)

REGRESS = iceberg_am_tpcds_basic

installcheck:
	$(pg_regress_installcheck) $(REGRESS)

clean:
	rm -rf results/ regression.diffs regression.out tmp_check/ log/
