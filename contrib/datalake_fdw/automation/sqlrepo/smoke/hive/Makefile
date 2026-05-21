# Hive smoke test Makefile

# Force bash + pipefail so spark-sql failures in the prep steps below
# (which pipe into `tail`) are surfaced instead of swallowed.  Without
# this the recipe shell is /bin/sh which lacks pipefail; tail always
# succeeds, the `||` fallback never fires, and pg_regress runs against
# an empty Hive metastore — failures reappear later as "relation does
# not exist" inside individual cases, much harder to diagnose.
SHELL := /bin/bash
.SHELLFLAGS := -ec -o pipefail

PGXS := $(shell pg_config --pgxs)
include $(PGXS)

# hive_text_compressed and hive_edge_coverage are temporarily disabled:
# both require `test_text_snappy` which is built by `hive_smoke.sql` via an
# `INSERT ... SET SnappyCodec` that needs Hive-on-MR + YARN, but the
# lakehouse-allinone image doesn't run a ResourceManager/NodeManager.
# Re-enable once the image ships YARN (or the prep is rewritten to use spark-sql).
REGRESS = hive_smoke hive_dlproxy_coverage hive_iceberg_fdw hive_iceberg_fdw_write

AUTOMATION_DIR = $(shell cd ../../.. && pwd)
HIVE_PREPARE_SQL          = $(AUTOMATION_DIR)/prepare/prepare_hive/hive_smoke.sql
HIVE_ICEBERG_PREPARE_SQL  = $(AUTOMATION_DIR)/prepare/prepare_hive/hive_iceberg_fdw.sql

# Spark + Iceberg-on-Hive catalog config for the iceberg_fdw_test fixture.
SPARK_SQL ?= spark-sql
SPARK_CONF = \
	--master local[*] \
	--conf spark.sql.legacy.createHiveTableByDefault=true \
	--conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
	--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
	--conf spark.hadoop.fs.s3a.access.key=admin \
	--conf spark.hadoop.fs.s3a.secret.key=admin12345 \
	--conf spark.hadoop.fs.s3a.path.style.access=true \
	--conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
	--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
	--conf spark.sql.catalog.ic=org.apache.iceberg.spark.SparkCatalog \
	--conf spark.sql.catalog.ic.type=hive \
	--conf spark.sql.catalog.ic.uri=thrift://hive-metastore:9083

installcheck:
	@echo "Loading Hive test data via spark-sql (avoids hive INSERT MR/YARN requirement)..."
	@if [ -f "$(HIVE_PREPARE_SQL)" ] && command -v $(SPARK_SQL) >/dev/null 2>&1; then \
		$(SPARK_SQL) $(SPARK_CONF) -f "$(HIVE_PREPARE_SQL)" 2>&1 | tail -20 || \
		echo "WARNING: Failed to load Hive test data"; \
	else \
		echo "WARNING: skipping Hive prep (spark-sql or SQL file missing)"; \
	fi
	@echo "Loading Iceberg-on-Hive fixture (default.iceberg_fdw_test)..."
	@if [ -f "$(HIVE_ICEBERG_PREPARE_SQL)" ] && command -v $(SPARK_SQL) >/dev/null 2>&1; then \
		$(SPARK_SQL) $(SPARK_CONF) -f "$(HIVE_ICEBERG_PREPARE_SQL)" 2>&1 | tail -10 || \
		echo "WARNING: Failed to load Iceberg-on-Hive fixture"; \
	else \
		echo "WARNING: skipping Iceberg-on-Hive prep (spark-sql or SQL file missing)"; \
	fi
	$(pg_regress_installcheck) $(REGRESS)

clean:
	rm -rf results/ regression.diffs regression.out tmp_check/ log/
