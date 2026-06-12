# Iceberg AM timestamptz interop (issue #366).
#
# CREATE ICEBERG TABLE must declare PG timestamptz columns as Iceberg
# "timestamptz" (isAdjustedToUTC) so external engines (Spark/Trino/Flink)
# interpret the stored UTC microseconds as instants.  See run.sh for the
# metadata.json assertion and the optional Spark cross-check.

PGXS := $(shell pg_config --pgxs)
include $(PGXS)

REGRESS = iceberg_am_timestamptz

installcheck:
	$(pg_regress_installcheck) $(REGRESS)

clean:
	rm -rf results/ regression.diffs regression.out tmp_check/ log/
