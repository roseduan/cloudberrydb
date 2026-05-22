# S3-catalog functional smoke for Iceberg AM.
#
# Verifies the path-based S3 catalog (iceberg HadoopCatalog under the hood)
# end-to-end: CREATE/INSERT/SELECT/UPDATE/DELETE/multi-row UPDATE with the
# warehouse layout `s3a://bucket/<warehouse_location_prefix>/<ns>/<table>/`.
#
# Requires the singlecluster lakehouse stack (MinIO at minio:9000,
# admin/password, bucket "warehouse").

PGXS := $(shell pg_config --pgxs)
include $(PGXS)

REGRESS = iceberg_am_s3_basic iceberg_am_s3_acid_double_update

installcheck:
	$(pg_regress_installcheck) $(REGRESS)

clean:
	rm -rf results/ regression.diffs regression.out tmp_check/ log/
