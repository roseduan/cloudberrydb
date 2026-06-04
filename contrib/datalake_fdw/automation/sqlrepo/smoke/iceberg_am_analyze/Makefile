# Iceberg AM ANALYZE sampling regression (issue #352).
#
# Verifies that ANALYZE on a native Iceberg table samples rows on the QEs and
# populates pg_statistic (per-column NDV), instead of the old no-op that left
# ORCA without column selectivity.  Covers single-table, multi-table, the
# datalake.enable_iceberg_analyze_sampling switch, and that the internal
# transport GUC is hidden.
#
# Requires the singlecluster lakehouse stack (MinIO + bucket "warehouse") and
# a pre-clean of the warehouse/iceberg_am_analyze/ prefix; see run.sh.

PGXS := $(shell pg_config --pgxs)
include $(PGXS)

REGRESS = iceberg_am_analyze

installcheck:
	$(pg_regress_installcheck) $(REGRESS)

clean:
	rm -rf results/ regression.diffs regression.out tmp_check/ log/
