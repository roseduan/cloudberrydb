# An HDFS volume that names only a conf section (issue #410).
#
# The sections this suite reads are appended to the coordinator's gphdfs.conf
# and removed again afterwards, so the rest of the file -- which other suites
# read -- is left exactly as it was.
#
# It used to read paa_cluster instead of staging its own, and that failed in CI
# while passing locally.  paa_cluster is not one file: locally it comes from
# automation/config/gphdfs.conf.template, in CI from smoke/hudi's ensure-conf,
# and hudi's version also writes hdfs_namenode_host / hdfs_namenode_port, which
# SiteConfigLoader.rejectLegacyKeys refuses outright.  So the section a suite
# gets depends on which other suite last wrote the file.  Owning the section
# removes that coupling.
#
# The HDFS warehouse is deliberately NOT pre-cleaned, and the test leaves its
# table in place.  gophermeta registers a file's length and Iceberg's hadoop
# catalog names metadata files vN.metadata.json, so wiping the warehouse while
# the cache survives makes the next write collide with a stale registration --
# which gopher reports as a FATAL that takes the agent JVM with it.  Keeping
# the warehouse monotonic avoids the collision; the test clears its rows in SQL.

PGXS := $(shell pg_config --pgxs)
include $(PGXS)

REGRESS = iceberg_conf_only_hdfs

HDFS_NAMENODE ?= hadoop
HDFS_PORT ?= 8020

installcheck:
	@QD_DATADIR="$$(psql -X -t -A -d postgres -c 'SHOW data_directory' 2>/dev/null | tr -d '[:space:]')"; \
	if [ -z "$$QD_DATADIR" ]; then \
		echo "ERROR: could not resolve QD data_directory; is the cluster up?" >&2; \
		exit 1; \
	fi; \
	if [ ! -f "$$QD_DATADIR/gphdfs.conf" ]; then \
		echo "ERROR: $$QD_DATADIR/gphdfs.conf is missing; the agent needs it" >&2; \
		exit 1; \
	fi; \
	echo "=== Appending co_hdfs_* sections to $$QD_DATADIR/gphdfs.conf ==="; \
	if [ -f "$$QD_DATADIR/gphdfs.conf.confonlyhdfs.bak" ]; then \
		mv "$$QD_DATADIR/gphdfs.conf.confonlyhdfs.bak" "$$QD_DATADIR/gphdfs.conf"; \
	fi; \
	cp "$$QD_DATADIR/gphdfs.conf" "$$QD_DATADIR/gphdfs.conf.confonlyhdfs.bak"; \
	printf '%s\n' \
		'' \
		'co_hdfs_cluster:' \
		'    hdfs_namenodes: $(HDFS_NAMENODE)' \
		'    hdfs_port: $(HDFS_PORT)' \
		'    hdfs_auth_method: simple' \
		'    hadoop_rpc_protection: authentication' \
		'' \
		'co_hdfs_ha_cluster:' \
		'    hdfs_namenodes: co_hdfs_ns' \
		'    hdfs_auth_method: simple' \
		'    is_ha_supported: true' \
		'    dfs_nameservices: co_hdfs_ns' \
		'    dfs_ha_namenodes: nn1' \
		'    dfs_namenode_rpc_address: $(HDFS_NAMENODE):$(HDFS_PORT)' \
		'    dfs_client_failover_proxy_provider: org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider' \
		'' \
		'co_hdfs_empty:' \
		>> "$$QD_DATADIR/gphdfs.conf"; \
	if ! grep -q '^co_hdfs_ha_cluster:' "$$QD_DATADIR/gphdfs.conf"; then \
		echo "ERROR: the co_hdfs_* sections did not reach $$QD_DATADIR/gphdfs.conf" >&2; \
		mv "$$QD_DATADIR/gphdfs.conf.confonlyhdfs.bak" "$$QD_DATADIR/gphdfs.conf"; \
		exit 1; \
	fi; \
	status=0; \
	$(pg_regress_installcheck) $(REGRESS) || status=$$?; \
	mv "$$QD_DATADIR/gphdfs.conf.confonlyhdfs.bak" "$$QD_DATADIR/gphdfs.conf"; \
	exit $$status

generate-expected:
	@if [ -d results ]; then mkdir -p expected; cp results/*.out expected/; \
		echo "Copied results to expected/"; else echo "Run installcheck first."; fi

clean:
	rm -rf results/ regression.diffs regression.out tmp_check/ log/
