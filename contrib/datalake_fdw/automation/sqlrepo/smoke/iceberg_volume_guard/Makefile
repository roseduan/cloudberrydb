# Volume address guard smoke test (issue #410, step 1)
#
# Kept out of smoke/iceberg_conf_merge because that suite currently fails on an
# unrelated segment crash (issue #418), so it cannot serve as a green home for
# these cases.  The s3.conf staging below mirrors that suite's.

PGXS := $(shell pg_config --pgxs)
include $(PGXS)

REGRESS = iceberg_volume_guard

installcheck:
	@QD_DATADIR="$$(psql -X -t -A -d postgres -c 'SHOW data_directory' 2>/dev/null | tr -d '[:space:]')"; \
	if [ -z "$$QD_DATADIR" ]; then \
		echo "ERROR: could not resolve QD data_directory; is the cluster up?" >&2; \
		exit 1; \
	fi; \
	echo "=== Deploying guard_* sections into $$QD_DATADIR/s3.conf ==="; \
	if [ -f "$$QD_DATADIR/s3.conf" ] && [ ! -f "$$QD_DATADIR/s3.conf.volguard.bak" ]; then \
		cp "$$QD_DATADIR/s3.conf" "$$QD_DATADIR/s3.conf.volguard.bak"; \
	fi; \
	printf '%s\n' \
		'guard_minio:' \
		'    type: s3' \
		'    endpoint: http://minio:9000' \
		'    region: us-east-1' \
		'    path_style_access: true' \
		'    access_key_id: admin' \
		'    secret_access_key: admin12345' \
		> "$$QD_DATADIR/s3.conf"; \
	echo "=== Running pg_regress tests ==="; \
	status=0; \
	$(pg_regress_installcheck) $(REGRESS) || status=$$?; \
	if [ -f "$$QD_DATADIR/s3.conf.volguard.bak" ]; then \
		mv "$$QD_DATADIR/s3.conf.volguard.bak" "$$QD_DATADIR/s3.conf"; \
	else \
		rm -f "$$QD_DATADIR/s3.conf"; \
	fi; \
	exit $$status

generate-expected:
	@if [ -d results ]; then mkdir -p expected; cp results/*.out expected/; \
		echo "Copied results to expected/"; else echo "Run installcheck first."; fi
