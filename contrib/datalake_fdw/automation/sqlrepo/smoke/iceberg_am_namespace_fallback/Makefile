# Iceberg namespace resolution on a non-builtin catalog (issue #411).
#
# Covers the tier-1 / tier-2 / tier-3 matrix on an external (hadoop) catalog.
# The tier-3 row -- OPTIONS (table ...) with neither an OPTIONS namespace nor
# a catalog default_namespace -- is the one that used to fail at COMMIT.
# See run.sh for the object-storage assertion on where each table landed.

PGXS := $(shell pg_config --pgxs)
include $(PGXS)

REGRESS = iceberg_am_namespace_fallback

installcheck:
	$(pg_regress_installcheck) $(REGRESS)

clean:
	rm -rf results/ regression.diffs regression.out tmp_check/ log/
