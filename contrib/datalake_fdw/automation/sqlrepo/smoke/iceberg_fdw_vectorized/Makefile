# FDW-iceberg vectorized read (FDW-iceberg vectorization).
#
# Reads an append-only iceberg AM table (parquet data files) back through a
# datalake_fdw foreign table with format 'iceberg' and vectorization on, using
# vector.force_vectorization so the scan ERRORs if it cannot be vectorized.
#
# Requires the lakehouse stack (MinIO + bucket "warehouse") and a Polaris
# server with catalog polaris_default_catalog; see run.sh for the pre-clean of
# both the MinIO prefix and the Polaris-side table entry.

PGXS := $(shell pg_config --pgxs)
include $(PGXS)

REGRESS = iceberg_fdw_vectorized

installcheck:
	$(pg_regress_installcheck) $(REGRESS)

clean:
	rm -rf results/ regression.diffs regression.out tmp_check/ log/
