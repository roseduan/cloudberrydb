# S3 smoke test Makefile

PGXS := $(shell pg_config --pgxs)
include $(PGXS)

AUTOMATION_DIR = $(shell cd ../../.. && pwd)
S3_PREPARE_SQL = $(AUTOMATION_DIR)/prepare/prepare_s3/s3_smoke.sql
S3_VERIFY_SQL = $(AUTOMATION_DIR)/prepare/prepare_s3/s3_verify_cb_write.sql

# Hive/Hadoop environment for beeline.  The automation framework downloads
# a beeline wrapper into tools/bin as part of `make install-tools`; prefer
# that so the smoke suite does not depend on a system-wide Hive install
# under /workspace/share.
BEELINE ?= $(AUTOMATION_DIR)/tools/bin/beeline
BEELINE_URL = jdbc:hive2://hiveserver2:10000/default

# MinIO endpoint (path-style, non-TLS)
MC_ALIAS_URL = http://minio:9000
MC_ALIAS_KEY = admin
MC_ALIAS_SECRET = admin12345

REGRESS = s3_smoke s3_write_smoke
PGPORT ?= 7000
export PGPORT

installcheck:
	@echo "=== Configuring mc alias for MinIO ==="
	@mc alias set local $(MC_ALIAS_URL) $(MC_ALIAS_KEY) $(MC_ALIAS_SECRET) 2>/dev/null || true
	@echo "=== Cleaning old S3 test data ==="
	@mc rm --recursive --force local/warehouse/s3-smoke-hive-parquet/ 2>/dev/null || true
	@mc rm --recursive --force local/warehouse/s3-smoke-hive-avro/ 2>/dev/null || true
	@mc rm --recursive --force local/warehouse/s3-smoke-hive-orc/ 2>/dev/null || true
	@mc rm --recursive --force local/warehouse/s3-smoke-hive-text/ 2>/dev/null || true
	@mc rm --recursive --force local/warehouse/s3-smoke-hive-csv/ 2>/dev/null || true
	@mc rm --recursive --force local/warehouse/smoke-write-test/ 2>/dev/null || true
	@echo "=== Preparing S3 test data via Hive (Hive writes -> CB reads) ==="
	@if [ -f "$(S3_PREPARE_SQL)" ]; then \
		$(BEELINE) -u "$(BEELINE_URL)" -n hive -f "$(S3_PREPARE_SQL)" 2>&1 || \
		{ echo "ERROR: Failed to prepare S3 test data"; exit 1; }; \
	else \
		echo "ERROR: S3 prepare SQL not found at $(S3_PREPARE_SQL)"; exit 1; \
	fi
	@echo "=== Running pg_regress tests ==="
	$(pg_regress_installcheck) $(REGRESS)
	@echo "=== Verifying Hive can read CB-written data (CB writes -> Hive reads) ==="
	@if [ -f "$(S3_VERIFY_SQL)" ]; then \
		$(BEELINE) -u "$(BEELINE_URL)" -n hive -f "$(S3_VERIFY_SQL)" 2>&1 || \
		{ echo "ERROR: CB->Hive verification failed"; exit 1; }; \
	else \
		echo "ERROR: Verify SQL not found at $(S3_VERIFY_SQL)"; exit 1; \
	fi
	@echo "=== All S3 smoke tests passed ==="

clean:
	rm -rf results/ regression.diffs regression.out tmp_check/ log/
