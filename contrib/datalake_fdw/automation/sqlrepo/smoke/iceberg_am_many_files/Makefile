# Iceberg AM many-files / many-delete-files regression (issue #362).
#
# The fragment list ships in the dispatched plan as the agent's deduplicated
# JSON wire format instead of an expanded FileScanTask node tree, keeping the
# serialized plan and the QEs' MessageContext O(data files + delete files)
# rather than O(data files x delete files).  This test exercises the scan and
# modify carrier paths on a table with many small data files and accumulated
# position-delete files.
#
# Requires the singlecluster lakehouse stack (MinIO + bucket "warehouse") and
# a pre-clean of the warehouse/iceberg_am_many_files/ prefix; see run.sh.

PGXS := $(shell pg_config --pgxs)
include $(PGXS)

REGRESS = iceberg_am_many_files

installcheck:
	$(pg_regress_installcheck) $(REGRESS)

clean:
	rm -rf results/ regression.diffs regression.out tmp_check/ log/
