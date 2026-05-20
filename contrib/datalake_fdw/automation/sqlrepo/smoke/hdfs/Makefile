# HDFS smoke test Makefile

PGXS := $(shell pg_config --pgxs)
include $(PGXS)

AUTOMATION_DIR = $(shell cd ../../.. && pwd)
HDFS_PREPARE_SQL = $(AUTOMATION_DIR)/prepare/prepare_hdfs/hdfs_smoke.sql

# Ensure hdfs CLI uses our core-site.xml (hadoop:8020) instead of system config
export HADOOP_CONF_DIR = $(AUTOMATION_DIR)/tools/conf/hadoop

REGRESS = 01_connection 02_text_basic 03_parquet_basic 04_orc_basic \
          05_partition_basic 07_avro_basic 08_datatypes 09_edge_cases 06_cleanup

# HDFS test data must be pre-loaded before running tests.
# From the HOST, run:
#   docker exec lakehouse hdfs dfs -rm -r -f /test/basic
#   docker cp <prepare_sql> lakehouse:/tmp/hdfs_smoke.sql
#   docker exec lakehouse spark-sql --master local[2] -f /tmp/hdfs_smoke.sql

HDFS_CMD            = hdfs dfs
HDFS_PREPARE_SQL    = $(AUTOMATION_DIR)/prepare/prepare_hdfs/hdfs_smoke.sql

installcheck:
	@echo "=== Reset HDFS /test/basic (drops both stale write-data and preload) ==="
	@$(HDFS_CMD) -rm -r -f -skipTrash /test/basic 2>/dev/null || true
	@echo "=== Re-prepare HDFS test data via beeline ==="
	@if [ -f "$(HDFS_PREPARE_SQL)" ]; then \
		beeline -u "jdbc:hive2://hiveserver2:10000/default" -n hive --silent=true \
			-f "$(HDFS_PREPARE_SQL)" 2>&1 | grep -vE '^(SLF4J|WARNING:|INFO|Connecting|Closing)' | tail -10; \
	else \
		echo "WARNING: HDFS prepare SQL not found at $(HDFS_PREPARE_SQL)"; \
	fi
	$(pg_regress_installcheck) $(REGRESS)

generate-expected:
	@if [ -d results ]; then \
		mkdir -p expected; \
		cp results/*.out expected/; \
		echo "Copied results to expected/"; \
	else \
		echo "No results/ directory. Run 'make installcheck' first."; \
	fi

clean:
	rm -rf results/ regression.diffs regression.out tmp_check/ log/
