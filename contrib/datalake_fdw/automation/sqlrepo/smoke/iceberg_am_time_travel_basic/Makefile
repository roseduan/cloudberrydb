# Iceberg AM time-travel basic regression (database-only, runs in CI).
#
# Covers the iceberg_snapshot_scan() behaviors that need only the database +
# MinIO (HEAD read, ORCA-native plan, missing-snapshot rejection, SELECT ACL).
# The schema-evolution / field-id cases that require an externally evolved
# fixture live in the sibling iceberg_am_time_travel test (Spark-driven).
#
# Requires the singlecluster lakehouse stack (MinIO bucket "warehouse").

PGXS := $(shell pg_config --pgxs)
include $(PGXS)

REGRESS = iceberg_am_time_travel_basic

installcheck:
	$(pg_regress_installcheck) $(REGRESS)

clean:
	rm -rf results/ regression.diffs regression.out tmp_check/ log/
