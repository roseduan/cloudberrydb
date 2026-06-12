# Iceberg FDW foreign table over a Polaris catalog (issue #844).
#
# Verifies the dlproxy getFragments request delivers the gopher connection
# config to the agent (config JSON body on both the cached and non-cached
# fragment branches, gopher.worker_path emission, the agent's POST
# /dlproxy/read mapping, and the legacy-over-baseline merge order), so the
# agent can build its GopherFileIO instead of failing with "Gopher Worker
# path is required".
#
# Requires the lakehouse stack (MinIO + bucket "warehouse") and a Polaris
# server with catalog polaris_default_catalog; see run.sh for the pre-clean
# of both the MinIO prefix and the Polaris-side table entry.

PGXS := $(shell pg_config --pgxs)
include $(PGXS)

REGRESS = iceberg_fdw_polaris

installcheck:
	$(pg_regress_installcheck) $(REGRESS)

clean:
	rm -rf results/ regression.diffs regression.out tmp_check/ log/
