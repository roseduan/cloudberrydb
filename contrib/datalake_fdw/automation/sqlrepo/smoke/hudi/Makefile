# Hudi smoke test Makefile

PGXS := $(shell pg_config --pgxs)
include $(PGXS)

AUTOMATION_DIR = $(shell cd ../../.. && pwd)
HUDI_PREPARE_SMOKE = $(AUTOMATION_DIR)/prepare/prepare_hudi/hudi_smoke.sql
HUDI_PREPARE_COV = $(AUTOMATION_DIR)/prepare/prepare_hudi/hudi_coverage.sql

# Spark configuration
SPARK_SQL ?= spark-sql

# S3A credentials (for Spark to access MinIO)
S3A_ENDPOINT ?= http://minio:9000
S3A_ACCESS_KEY ?= admin
S3A_SECRET_KEY ?= admin12345

# Service endpoints (matching test_config.env)
HDFS_NAMENODE ?= hadoop
HDFS_PORT ?= 8020
HIVE_METASTORE_HOST ?= hive-metastore
HIVE_METASTORE_PORT ?= 9083

# Spark conf: local mode + Hudi 0.13 extensions + S3A + KryoSerializer
SPARK_CONF = \
	--master local[*] \
	--conf spark.hadoop.fs.s3a.endpoint=$(S3A_ENDPOINT) \
	--conf spark.hadoop.fs.s3a.access.key=$(S3A_ACCESS_KEY) \
	--conf spark.hadoop.fs.s3a.secret.key=$(S3A_SECRET_KEY) \
	--conf spark.hadoop.fs.s3a.path.style.access=true \
	--conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
	--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
	--conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
	--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
	--conf spark.hadoop.dfs.client.block.write.replace-datanode-on-failure.policy=NEVER \
	--conf spark.hadoop.dfs.client.block.write.replace-datanode-on-failure.enable=false \
	--conf spark.hadoop.dfs.replication=1

REGRESS = hudi_smoke hudi_coverage hudi_btree_merger
PGPORT ?= 7000
export PGPORT

# Deploy gphdfs.conf and gphive.conf to all primary data directories (master + segments).
# Generates config inline to avoid template path/escaping issues.
ensure-conf:
	@echo "=== Deploying gphdfs.conf and gphive.conf ==="
	@DATADIRS=$$(psql -d postgres -t -A -c "SELECT datadir FROM gp_segment_configuration WHERE role='p'"); \
	if [ -z "$$DATADIRS" ]; then \
		echo "ERROR: Could not query data directories from gp_segment_configuration"; exit 1; \
	fi; \
	for dir in $$DATADIRS; do \
		echo "  Deploying to $$dir"; \
		printf 'paa_cluster:\n    hdfs_namenodes: $(HDFS_NAMENODE)\n    hdfs_port: $(HDFS_PORT)\n    hdfs_auth_method: simple\n    hadoop_rpc_protection: authentication\n\nhive_cluster:\n    hdfs_namenodes: $(HDFS_NAMENODE)\n    hdfs_port: $(HDFS_PORT)\n    hdfs_auth_method: simple\n    hadoop_rpc_protection: authentication\n' > "$$dir/gphdfs.conf"; \
		printf 'hive_cluster:\n    url: thrift://$(HIVE_METASTORE_HOST):$(HIVE_METASTORE_PORT)\n    auth_method: simple\n' > "$$dir/gphive.conf"; \
	done

installcheck: ensure-conf
	@echo "=== Preparing Hudi test data via Spark SQL ==="
	@if [ -f "$(HUDI_PREPARE_SMOKE)" ]; then \
		$(SPARK_SQL) $(SPARK_CONF) -f "$(HUDI_PREPARE_SMOKE)" 2>&1 || \
		{ echo "ERROR: Failed to prepare Hudi smoke data"; exit 1; }; \
	else \
		echo "ERROR: Hudi prepare SQL not found at $(HUDI_PREPARE_SMOKE)"; exit 1; \
	fi
	@if [ -f "$(HUDI_PREPARE_COV)" ]; then \
		echo "=== Preparing Hudi coverage test data ==="; \
		$(SPARK_SQL) $(SPARK_CONF) -f "$(HUDI_PREPARE_COV)" 2>&1 || \
		{ echo "ERROR: Failed to prepare Hudi coverage data"; exit 1; }; \
	else \
		echo "WARNING: Hudi coverage prepare SQL not found, skipping"; \
	fi
	@echo "=== Running pg_regress tests ==="
	$(pg_regress_installcheck) $(REGRESS)
	@echo "=== All Hudi smoke tests passed ==="

clean:
	rm -rf results/ regression.diffs regression.out tmp_check/ log/
