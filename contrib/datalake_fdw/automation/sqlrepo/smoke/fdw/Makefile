# FDW (Foreign Table) smoke test Makefile
# Tests the datalake_fdw FDW path (CREATE FOREIGN TABLE), NOT iceberg AM path.
# Self-contained: writes data to S3/MinIO then reads back. Only needs MinIO.
#
# Each test runs in its own pg_regress invocation with S3 cleanup between
# them to prevent data accumulation across tests.

PGXS := $(shell pg_config --pgxs)
include $(PGXS)

MC_ALIAS_URL = http://minio:9000
MC_ALIAS_KEY = admin
MC_ALIAS_SECRET = admin12345

TESTS = fdw_parquet fdw_orc fdw_avro fdw_text_csv fdw_compression fdw_filters fdw_types fdw_multiformat \
        fdw_orc_types fdw_avro_types fdw_filter_pushdown fdw_coverage_boost fdw_vectorized_orc fdw_error_read

PGPORT ?= 7000
export PGPORT

mc_setup = @mc alias set local $(MC_ALIAS_URL) $(MC_ALIAS_KEY) $(MC_ALIAS_SECRET) 2>/dev/null || true

mc_clean = mc alias set local $(MC_ALIAS_URL) $(MC_ALIAS_KEY) $(MC_ALIAS_SECRET) 2>/dev/null || true && \
	mc rm --recursive --force local/warehouse/fdw-test/ 2>/dev/null || true

installcheck:
	@echo "=== Running FDW smoke tests ==="
	@PASS=0; FAIL=0; \
	for test in $(TESTS); do \
		$(mc_clean); \
		echo "--- $$test ---"; \
		if $(pg_regress_installcheck) $$test; then \
			PASS=$$((PASS + 1)); \
		else \
			FAIL=$$((FAIL + 1)); \
			echo "FAILED: $$test"; \
		fi; \
	done; \
	echo ""; \
	echo "====================="; \
	echo "$$PASS of $$(( PASS + FAIL )) tests passed."; \
	echo "====================="; \
	test $$FAIL -eq 0

clean:
	rm -rf results/ regression.diffs regression.out tmp_check/ log/
