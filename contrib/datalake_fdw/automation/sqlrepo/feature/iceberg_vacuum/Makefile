# Iceberg Vacuum Feature Tests Makefile

PGXS := $(shell pg_config --pgxs)
include $(PGXS)

REGRESS = 01_vacuum_basic 02_vacuum_advanced 03_vacuum_partitioned

installcheck:
	$(pg_regress_installcheck) $(REGRESS)

clean:
	rm -rf results/ regression.diffs regression.out tmp_check/ log/
