# TPC-H basic functional smoke for Iceberg AM (CREATE ICEBERG TABLE)
#
# Verifies the builtin-catalog + S3-volume path end-to-end:
#   schema + catalog/volume DDL -> 8 TPC-H tables -> small synthetic load ->
#   COUNT/JOIN/aggregate/UPDATE/DELETE sanity checks.
#
# Requires the singlecluster stack (lakehouse container) running with MinIO
# at minio:9000 (admin/password) and bucket "warehouse".

PGXS := $(shell pg_config --pgxs)
include $(PGXS)

REGRESS = iceberg_am_tpch_basic

installcheck:
	$(pg_regress_installcheck) $(REGRESS)

clean:
	rm -rf results/ regression.diffs regression.out tmp_check/ log/
