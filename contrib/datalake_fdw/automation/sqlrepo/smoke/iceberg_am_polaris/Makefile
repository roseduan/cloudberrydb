# Polaris-catalog functional smoke for Iceberg AM (CREATE ICEBERG TABLE)
#
# Verifies the non-builtin (Polaris REST) catalog + S3 volume path end-to-end:
#   server/catalog/volume DDL -> CREATE ICEBERG TABLE -> INSERT -> SELECT
#   roundtrip -> UPDATE -> DELETE -> multi-row UPDATE -> drop.
#
# Requires the singlecluster stack (lakehouse + singlecluster-polaris-1
# containers) running, with:
#   - Polaris reachable at polaris:8181/api/catalog (creds root/s3cr3t)
#   - MinIO at minio:9000 (admin/password), bucket "warehouse"

PGXS := $(shell pg_config --pgxs)
include $(PGXS)

REGRESS = iceberg_am_polaris_basic

installcheck:
	$(pg_regress_installcheck) $(REGRESS)

clean:
	rm -rf results/ regression.diffs regression.out tmp_check/ log/
