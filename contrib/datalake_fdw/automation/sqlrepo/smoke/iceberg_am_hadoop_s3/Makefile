# Hadoop-catalog over S3 functional smoke for Iceberg AM.
#
# Same iceberg HadoopCatalog implementation as type='s3' but reached via
# type='hadoop'. Verifies the new agent branch in processServerResource
# routes hadoop catalog requests through processS3OrHadoopServerResource.
#
# Requires the singlecluster lakehouse stack (MinIO at minio:9000).

PGXS := $(shell pg_config --pgxs)
include $(PGXS)

REGRESS = iceberg_am_hadoop_s3_basic iceberg_am_hadoop_s3_acid_double_update

MC_ALIAS_URL = http://minio:9000
MC_ALIAS_KEY = admin
MC_ALIAS_SECRET = admin12345

installcheck:
	@echo "=== Cleaning old iceberg_am_hadoop_s3 minio data ==="
	@mc alias set local $(MC_ALIAS_URL) $(MC_ALIAS_KEY) $(MC_ALIAS_SECRET) 2>/dev/null || true
	@mc rm --recursive --force local/warehouse/iceberg_hadoop_s3_smoke/ 2>/dev/null || true
	@mc rm --recursive --force local/warehouse/iceberg_hadoop_s3_acid_double_update/ 2>/dev/null || true
	@echo "=== Running pg_regress tests ==="
	$(pg_regress_installcheck) $(REGRESS)

clean:
	rm -rf results/ regression.diffs regression.out tmp_check/ log/
