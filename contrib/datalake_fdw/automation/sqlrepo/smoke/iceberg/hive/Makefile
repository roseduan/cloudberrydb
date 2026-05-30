# Iceberg hive catalog smoke test Makefile

PGXS := $(shell pg_config --pgxs)
include $(PGXS)

REGRESS = iceberg_hive iceberg_hive_deferred_commit iceberg_hive_options

# Hive Metastore requires the "public" database to exist (PG schema maps to Hive DB).
HIVE_BEELINE = beeline -u "jdbc:hive2://hiveserver2:10000" --silent=true
MC_ALIAS_URL = http://minio:9000
MC_ALIAS_KEY = admin
MC_ALIAS_SECRET = admin12345

prepare:
	@echo "=== Cleaning old iceberg-hive minio data ==="
	@mc alias set local $(MC_ALIAS_URL) $(MC_ALIAS_KEY) $(MC_ALIAS_SECRET) 2>/dev/null || true
	@mc rm --recursive --force local/warehouse/hive_volume/ 2>/dev/null || true
	@mc rm --recursive --force local/warehouse/hiveopt_volume/ 2>/dev/null || true
	@mc rm --recursive --force local/warehouse/hive_test/ 2>/dev/null || true
	@echo "=== Dropping + recreating Hive 'public' database (clean slate) ==="
	@$(HIVE_BEELINE) -e "DROP DATABASE IF EXISTS public CASCADE;" 2>/dev/null || \
		echo "WARNING: Could not drop Hive 'public' database (beeline unavailable)"
	@$(HIVE_BEELINE) -e "CREATE DATABASE IF NOT EXISTS public;" 2>/dev/null || \
		echo "WARNING: Could not create Hive 'public' database (beeline unavailable)"

installcheck: prepare
	$(pg_regress_installcheck) $(REGRESS)

clean:
	rm -rf results/ regression.diffs regression.out tmp_check/ log/
