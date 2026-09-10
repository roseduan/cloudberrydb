# A catalog that names only a gphive.conf section (issue #410).
#
# gphive.conf does not ship in the data directories, so unlike the HDFS suite
# this one stages its own -- backing up whatever is there first, because
# smoke/hudi writes its own gphive.conf and the two must not clobber each
# other.  Only the coordinator's copy matters: the agent reads it, and the
# segments never speak to a metastore.
#
# Note for anyone copying smoke/hudi's section: it writes both `url` and
# `uris`, and `uris` is a rejected legacy key on this path -- the agent fails
# with "legacy key(s) no longer supported: \"uris\" (use \"url\")".
#
# The bucket prefix is not pre-cleaned and the table is not dropped: the test
# clears its own rows instead, so it passes on consecutive runs without needing
# mc, which is not on PATH in every test environment.

PGXS := $(shell pg_config --pgxs)
include $(PGXS)

REGRESS = iceberg_conf_only_hive

HIVE_METASTORE_URI ?= thrift://hive-metastore:9083

installcheck:
	@QD_DATADIR="$$(psql -X -t -A -d postgres -c 'SHOW data_directory' 2>/dev/null | tr -d '[:space:]')"; \
	if [ -z "$$QD_DATADIR" ]; then \
		echo "ERROR: could not resolve QD data_directory; is the cluster up?" >&2; \
		exit 1; \
	fi; \
	echo "=== Deploying cohive_* sections into $$QD_DATADIR/gphive.conf ==="; \
	if [ -f "$$QD_DATADIR/gphive.conf.confonlyhive.bak" ]; then \
		mv "$$QD_DATADIR/gphive.conf.confonlyhive.bak" "$$QD_DATADIR/gphive.conf"; \
	fi; \
	if [ -f "$$QD_DATADIR/gphive.conf" ]; then \
		cp "$$QD_DATADIR/gphive.conf" "$$QD_DATADIR/gphive.conf.confonlyhive.bak"; \
	fi; \
	printf '%s\n' \
		'cohive_meta:' \
		'    url: $(HIVE_METASTORE_URI)' \
		'    auth_method: simple' \
		'' \
		'cohive_empty:' \
		> "$$QD_DATADIR/gphive.conf"; \
	if ! grep -q '^cohive_meta:' "$$QD_DATADIR/gphive.conf"; then \
		echo "ERROR: the cohive_meta section did not reach $$QD_DATADIR/gphive.conf" >&2; \
		echo "       the test would fail with \"not found in gphive.conf\", which" >&2; \
		echo "       reads like a product bug rather than a staging failure" >&2; \
		if [ -f "$$QD_DATADIR/gphive.conf.confonlyhive.bak" ]; then \
			mv "$$QD_DATADIR/gphive.conf.confonlyhive.bak" "$$QD_DATADIR/gphive.conf"; \
		fi; \
		exit 1; \
	fi; \
	status=0; \
	$(pg_regress_installcheck) $(REGRESS) || status=$$?; \
	if [ -f "$$QD_DATADIR/gphive.conf.confonlyhive.bak" ]; then \
		mv "$$QD_DATADIR/gphive.conf.confonlyhive.bak" "$$QD_DATADIR/gphive.conf"; \
	else \
		rm -f "$$QD_DATADIR/gphive.conf"; \
	fi; \
	exit $$status

generate-expected:
	@if [ -d results ]; then mkdir -p expected; cp results/*.out expected/; \
		echo "Copied results to expected/"; else echo "Run installcheck first."; fi

clean:
	rm -rf results/ regression.diffs regression.out tmp_check/ log/
