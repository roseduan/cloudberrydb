# Compression formats smoke test Makefile

PGXS := $(shell pg_config --pgxs)
include $(PGXS)

# MinIO endpoint (path-style, non-TLS)
MC_ALIAS_URL = http://minio:9000
MC_ALIAS_KEY = admin
MC_ALIAS_SECRET = admin12345

# HDFS
HDFS_CMD = hdfs dfs

REGRESS = compression_smoke
PGPORT ?= 7000
export PGPORT

installcheck:
	@echo "=== Cleaning old HDFS compression test data ==="
	@$(HDFS_CMD) -rm -r -f /test/compression/parquet_snappy 2>/dev/null || true
	@$(HDFS_CMD) -rm -r -f /test/compression/parquet_gzip 2>/dev/null || true
	@$(HDFS_CMD) -rm -r -f /test/compression/parquet_zstd 2>/dev/null || true
	@$(HDFS_CMD) -rm -r -f /test/compression/parquet_lz4 2>/dev/null || true
	@$(HDFS_CMD) -rm -r -f /test/compression/text_gzip 2>/dev/null || true
	@$(HDFS_CMD) -rm -r -f /test/compression/text_zip 2>/dev/null || true
	@$(HDFS_CMD) -rm -r -f /test/compression/avro_snappy 2>/dev/null || true
	@echo "=== Cleaning old S3 compression test data ==="
	@mc alias set local $(MC_ALIAS_URL) $(MC_ALIAS_KEY) $(MC_ALIAS_SECRET) 2>/dev/null || true
	@mc rm --recursive --force local/warehouse/smoke-compression/ 2>/dev/null || true
	@echo "=== Running pg_regress tests ==="
	$(pg_regress_installcheck) $(REGRESS)

clean:
	rm -rf results/ regression.diffs regression.out tmp_check/ log/
