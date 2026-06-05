# S3 smoke test Makefile

PGXS := $(shell pg_config --pgxs)
include $(PGXS)

AUTOMATION_DIR = $(shell cd ../../.. && pwd)
S3_PREPARE_SQL = $(AUTOMATION_DIR)/prepare/prepare_s3/s3_smoke.sql
S3_VERIFY_SQL = $(AUTOMATION_DIR)/prepare/prepare_s3/s3_verify_cb_write.sql

# Hive beeline.  Default: shell out into the hiveserver2 container's own
# beeline (matches what services/datalake-test-env.yml ships and what the
# .gitlab-ci.yml prep step uses).  This avoids depending on a local
# hadoop / hive install (the tools/bin/beeline wrapper requires HADOOP_HOME
# which neither the dev container nor the CI runner has set up).  Override
# with `make BEELINE=...` if you really need a different path.
#
# NOTE: the SQL is fed on stdin (`beeline ... < file.sql`), not via
# `-f file.sql`.  With a containerized beeline, `-f` resolves the path
# *inside* the hiveserver2 container where the host repo isn't mounted;
# stdin is opened by the host shell, so the file is read host-side and
# piped through `docker exec -i`.
BEELINE ?= docker exec -i hiveserver2 /opt/hive/bin/beeline
BEELINE_URL = jdbc:hive2://hiveserver2:10000/default

# MinIO endpoint (path-style, non-TLS)
MC_ALIAS_URL = http://minio:9000
MC_ALIAS_KEY = admin
MC_ALIAS_SECRET = admin12345

REGRESS = s3_smoke s3_write_smoke
PGPORT ?= 7000
export PGPORT

# SMOKE_S3_SKIP_PREP=1 skips the beeline prep/verify steps.  In CI the smoke
# harness runs inside the devel test container, where the docker CLI has no
# reachable daemon, so the default `docker exec -i hiveserver2 ...` BEELINE
# dies instantly.  The .gitlab-ci.yml job script already runs the exact same
# prep (docker cp + beeline -f) on the runner before launching the harness,
# so the in-Makefile prep is redundant there; the CB->Hive verify step is
# skipped as well (no working beeline inside the container).
SMOKE_S3_SKIP_PREP ?=

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
	@if [ -n "$(SMOKE_S3_SKIP_PREP)" ]; then \
		echo "=== Skipping S3 Hive prep (SMOKE_S3_SKIP_PREP set; prepared by CI job script) ==="; \
	elif [ -f "$(S3_PREPARE_SQL)" ]; then \
		echo "=== Preparing S3 test data via Hive (Hive writes -> CB reads) ==="; \
		$(BEELINE) -u "$(BEELINE_URL)" -n hive < "$(S3_PREPARE_SQL)" 2>&1 || \
		{ echo "ERROR: Failed to prepare S3 test data"; exit 1; }; \
	else \
		echo "ERROR: S3 prepare SQL not found at $(S3_PREPARE_SQL)"; exit 1; \
	fi
	@echo "=== Running pg_regress tests ==="
	$(pg_regress_installcheck) $(REGRESS)
	@if [ -n "$(SMOKE_S3_SKIP_PREP)" ]; then \
		echo "=== Skipping CB->Hive verify (SMOKE_S3_SKIP_PREP set; no beeline in this container) ==="; \
	elif [ -f "$(S3_VERIFY_SQL)" ]; then \
		echo "=== Verifying Hive can read CB-written data (CB writes -> Hive reads) ==="; \
		$(BEELINE) -u "$(BEELINE_URL)" -n hive < "$(S3_VERIFY_SQL)" 2>&1 || \
		{ echo "ERROR: CB->Hive verification failed"; exit 1; }; \
	else \
		echo "ERROR: Verify SQL not found at $(S3_VERIFY_SQL)"; exit 1; \
	fi
	@echo "=== All S3 smoke tests passed ==="

clean:
	rm -rf results/ regression.diffs regression.out tmp_check/ log/
