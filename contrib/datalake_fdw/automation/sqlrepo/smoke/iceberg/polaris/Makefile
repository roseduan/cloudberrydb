# Iceberg polaris smoke test Makefile

PGXS := $(shell pg_config --pgxs)
include $(PGXS)

REGRESS = iceberg_polaris iceberg_polaris_types iceberg_polaris_statistics iceberg_polaris_deferred_commit \
          iceberg_polaris_options iceberg_polaris_toolkit

POLARIS_CLI = ../../../../docker/singlecluster/scripts/polaris_cli.sh
# Multi-container topology (vs singlecluster all-in-one): override the
# defaults in polaris_cli.sh to point at the actual container hostnames /
# credentials in use here.
POLARIS_ENV = API_HOST=http://polaris:8181 \
              S3_ENDPOINT=http://minio:9000 \
              S3_SECRET_KEY=admin12345

MC_ALIAS_URL = http://minio:9000
MC_ALIAS_KEY = admin
MC_ALIAS_SECRET = admin12345

installcheck:
	@echo "Cleaning Polaris catalog + minio data before test..."
	@rm -f regression.diffs
	@$(POLARIS_ENV) $(POLARIS_CLI) delete-catalog-force || \
		echo "WARNING: Polaris catalog cleanup failed (non-fatal)"
	@$(POLARIS_ENV) $(POLARIS_CLI) create-namespace || \
		echo "WARNING: Polaris namespace creation failed (non-fatal)"
	@mc alias set local $(MC_ALIAS_URL) $(MC_ALIAS_KEY) $(MC_ALIAS_SECRET) 2>/dev/null || true
	@mc rm --recursive --force local/warehouse/public/ 2>/dev/null || true
	$(pg_regress_installcheck) $(REGRESS)

clean:
	rm -rf results/ regression.diffs regression.out tmp_check/ log/
