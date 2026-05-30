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

REGRESS = iceberg_am_polaris_basic iceberg_am_polaris_acid_double_update

POLARIS_CLI = ../../../docker/singlecluster/scripts/polaris_cli.sh
POLARIS_ENV = API_HOST=http://polaris:8181 \
              S3_ENDPOINT=http://minio:9000 \
              S3_SECRET_KEY=admin12345

MC_ALIAS_URL = http://minio:9000
MC_ALIAS_KEY = admin
MC_ALIAS_SECRET = admin12345

installcheck:
	@echo "=== Cleaning Polaris catalog + minio data ==="
	@$(POLARIS_ENV) $(POLARIS_CLI) delete-catalog-force || \
		echo "WARNING: Polaris catalog cleanup failed (non-fatal)"
	@$(POLARIS_ENV) $(POLARIS_CLI) create-namespace || \
		echo "WARNING: Polaris namespace creation failed (non-fatal)"
	@mc alias set local $(MC_ALIAS_URL) $(MC_ALIAS_KEY) $(MC_ALIAS_SECRET) 2>/dev/null || true
	@mc rm --recursive --force local/warehouse/public/ 2>/dev/null || true
	$(pg_regress_installcheck) $(REGRESS)

clean:
	rm -rf results/ regression.diffs regression.out tmp_check/ log/
