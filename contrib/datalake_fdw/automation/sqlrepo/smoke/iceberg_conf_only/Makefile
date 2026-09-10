# A volume that names only a conf section (issue #410).
#
# The volume carries type / server_name / bucket_name and nothing else: the
# endpoint, region and credentials live in the s3.conf section it names.  The
# agent merges that section on the metadata plane and returns the result, and
# both planes fill their gaps from it, so a table on such a volume is readable
# and writable.  The test asserts INSERT, UPDATE and DELETE separately rather
# than assuming one implies the others: UPDATE and DELETE additionally carry
# the fragment list, which travels by a different route than the volume
# configuration does.
#
# The HDFS counterpart lives in smoke/iceberg_conf_only_hdfs.

PGXS := $(shell pg_config --pgxs)
include $(PGXS)

REGRESS = iceberg_conf_only

MC_ALIAS_URL = http://minio:9000
MC_ALIAS_KEY = admin
MC_ALIAS_SECRET = admin12345

POLARIS_CLI = ../../../docker/singlecluster/scripts/polaris_cli.sh
POLARIS_ENV = API_HOST=http://polaris:8181 \
              S3_ENDPOINT=http://minio:9000 \
              S3_SECRET_KEY=admin12345

installcheck:
	@echo "=== Cleaning old iceberg_conf_only minio data ==="
	@mc alias set local $(MC_ALIAS_URL) $(MC_ALIAS_KEY) $(MC_ALIAS_SECRET) 2>/dev/null || true
	@mc rm --recursive --force local/warehouse/iceberg_conf_only/ 2>/dev/null || true
	@$(POLARIS_ENV) $(POLARIS_CLI) create-namespace 2>/dev/null || \
		echo "WARNING: Polaris namespace creation failed (non-fatal)"
	@QD_DATADIR="$$(psql -X -t -A -d postgres -c 'SHOW data_directory' 2>/dev/null | tr -d '[:space:]')"; \
	if [ -z "$$QD_DATADIR" ]; then \
		echo "ERROR: could not resolve QD data_directory; is the cluster up?" >&2; \
		exit 1; \
	fi; \
	echo "=== Deploying confonly_* sections into $$QD_DATADIR/s3.conf ==="; \
	if [ -f "$$QD_DATADIR/s3.conf" ] && [ ! -f "$$QD_DATADIR/s3.conf.confonly.bak" ]; then \
		cp "$$QD_DATADIR/s3.conf" "$$QD_DATADIR/s3.conf.confonly.bak"; \
	fi; \
	printf '%s\n' \
		'confonly_minio:' \
		'    type: s3' \
		'    endpoint: http://minio:9000' \
		'    region: us-east-1' \
		'    path_style_access: true' \
		'    access_key_id: admin' \
		'    secret_access_key: admin12345' \
		> "$$QD_DATADIR/s3.conf"; \
	if ! grep -q '^confonly_minio:' "$$QD_DATADIR/s3.conf"; then \
		echo "ERROR: the confonly_minio section did not reach $$QD_DATADIR/s3.conf" >&2; \
		echo "       the test would fail as though the volume's section were" >&2; \
		echo "       unreadable, which points at the product, not the staging" >&2; \
		if [ -f "$$QD_DATADIR/s3.conf.confonly.bak" ]; then \
			mv "$$QD_DATADIR/s3.conf.confonly.bak" "$$QD_DATADIR/s3.conf"; \
		fi; \
		exit 1; \
	fi; \
	echo "=== Running pg_regress tests ==="; \
	status=0; \
	$(pg_regress_installcheck) $(REGRESS) || status=$$?; \
	if [ -f "$$QD_DATADIR/s3.conf.confonly.bak" ]; then \
		mv "$$QD_DATADIR/s3.conf.confonly.bak" "$$QD_DATADIR/s3.conf"; \
	else \
		rm -f "$$QD_DATADIR/s3.conf"; \
	fi; \
	exit $$status

generate-expected:
	@if [ -d results ]; then mkdir -p expected; cp results/*.out expected/; \
		echo "Copied results to expected/"; else echo "Run installcheck first."; fi

clean:
	rm -rf results/ regression.diffs regression.out tmp_check/ log/
