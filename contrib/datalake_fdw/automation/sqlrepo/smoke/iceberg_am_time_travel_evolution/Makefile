# Iceberg AM time-travel schema-evolution regression (database-only, runs in CI).
#
# Builds a rename+drop+add evolution history with builtin ALTER TABLE (#401)
# and asserts exact values when time-traveling to every historical snapshot,
# including the field-id-hole case a positional column match reads corrupt.
#
# Requires the singlecluster lakehouse stack (MinIO bucket "warehouse").

PGXS := $(shell pg_config --pgxs)
include $(PGXS)

REGRESS = iceberg_am_time_travel_evolution

installcheck:
	$(pg_regress_installcheck) $(REGRESS)

clean:
	rm -rf results/ regression.diffs regression.out tmp_check/ log/
