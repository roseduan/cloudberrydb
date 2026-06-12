# Conf-file merge smoke for the iceberg volume path.
#
# Verifies the server_name conf-file mode after the key-vocabulary
# unification: section keys equal the SQL OPTION names, SQL OPTIONS override
# the section per key, and legacy fs.s3a.* spellings are hard errors.
#
# Requires the singlecluster lakehouse stack (MinIO at minio:9000,
# admin/admin12345, bucket "warehouse") and the merge_* sections deployed
# into the QD data directory's s3.conf -- use run.sh, which handles the
# deployment and cleanup.

PGXS := $(shell pg_config --pgxs)
include $(PGXS)

REGRESS = iceberg_conf_merge

MC_ALIAS_URL = http://minio:9000
MC_ALIAS_KEY = admin
MC_ALIAS_SECRET = admin12345

installcheck:
	@echo "=== Cleaning old iceberg_conf_merge minio data ==="
	@mc alias set local $(MC_ALIAS_URL) $(MC_ALIAS_KEY) $(MC_ALIAS_SECRET) 2>/dev/null || true
	@mc rm --recursive --force local/warehouse/iceberg_conf_merge/ 2>/dev/null || true
	@echo "=== Running pg_regress tests ==="
	$(pg_regress_installcheck) $(REGRESS)

clean:
	rm -rf results/ regression.diffs regression.out tmp_check/ log/
