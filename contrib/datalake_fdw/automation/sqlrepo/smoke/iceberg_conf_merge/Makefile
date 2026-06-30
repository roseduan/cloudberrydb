# Conf-file merge smoke for the iceberg volume path.
#
# Verifies the server_name conf-file mode after the key-vocabulary
# unification: section keys equal the SQL OPTION names, SQL OPTIONS override
# the section per key, and legacy fs.s3a.* spellings are hard errors.
#
# Requires the singlecluster lakehouse stack (MinIO at minio:9000,
# admin/admin12345, bucket "warehouse").
#
# The datalake_agent reads its server sections from ./s3.conf relative to its
# working directory, which is the QD (coordinator) data directory.  run.sh
# stages that file when driving the test by hand; under pg_regress (the CI
# path) installcheck stages the merge_* sections here itself, backing up any
# existing s3.conf and restoring it afterwards, pass or fail.  The data
# directory is resolved live from the running coordinator so this works
# regardless of the deploy layout.

PGXS := $(shell pg_config --pgxs)
include $(PGXS)

REGRESS = iceberg_conf_merge

MC_ALIAS_URL = http://minio:9000
MC_ALIAS_KEY = admin
MC_ALIAS_SECRET = admin12345

installcheck:
	@echo "=== Cleaning old iceberg_conf_merge minio data ==="
	@mc alias set local $(MC_ALIAS_URL) $(MC_ALIAS_KEY) $(MC_ALIAS_SECRET) 2>/dev/null || true
	@mc rm --recursive --force local/warehouse/iceberg_conf_merge/ 2>/dev/null || true
	@QD_DATADIR="$$(psql -X -t -A -c 'SHOW data_directory' 2>/dev/null | tr -d '[:space:]')"; \
	if [ -z "$$QD_DATADIR" ]; then \
		echo "ERROR: could not resolve QD data_directory; is the cluster up?" >&2; \
		exit 1; \
	fi; \
	echo "=== Deploying merge_* sections into $$QD_DATADIR/s3.conf ==="; \
	if [ -f "$$QD_DATADIR/s3.conf" ] && [ ! -f "$$QD_DATADIR/s3.conf.confmerge.bak" ]; then \
		cp "$$QD_DATADIR/s3.conf" "$$QD_DATADIR/s3.conf.confmerge.bak"; \
	fi; \
	printf '%s\n' \
		'merge_minio:' \
		'    type: s3' \
		'    endpoint: http://minio:9000' \
		'    region: us-east-1' \
		'    path_style_access: true' \
		'    access_key_id: admin' \
		'    secret_access_key: admin12345' \
		'' \
		'merge_badcreds:' \
		'    type: s3' \
		'    endpoint: http://minio:9000' \
		'    region: us-east-1' \
		'    path_style_access: true' \
		'    access_key_id: admin' \
		'    secret_access_key: wrong-secret' \
		'' \
		'merge_legacy:' \
		'    fs.s3a.endpoint: http://minio:9000' \
		'    fs.s3a.access.key: admin' \
		'    fs.s3a.secret.key: admin12345' \
		'    fs.s3a.path.style.access: true' \
		> "$$QD_DATADIR/s3.conf"; \
	echo "=== Running pg_regress tests ==="; \
	status=0; \
	$(pg_regress_installcheck) $(REGRESS) || status=$$?; \
	if [ -f "$$QD_DATADIR/s3.conf.confmerge.bak" ]; then \
		mv "$$QD_DATADIR/s3.conf.confmerge.bak" "$$QD_DATADIR/s3.conf"; \
	else \
		rm -f "$$QD_DATADIR/s3.conf"; \
	fi; \
	exit $$status

clean:
	rm -rf results/ regression.diffs regression.out tmp_check/ log/
