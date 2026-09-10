# Foreign-server binding smoke test (issue #409)
#
# Kept out of smoke/hdfs on purpose: that suite's 01_connection creates the
# datalake_fdw wrapper and an hdfs_server on the same namenode, so sharing a
# database would both perturb its expected output and make every case here
# ambiguous.

PGXS := $(shell pg_config --pgxs)
include $(PGXS)

AUTOMATION_DIR = $(shell cd ../../.. && pwd)

# Reuse the HDFS smoke fixture: the binding cases read it to prove the table is
# actually usable through the server it bound to, not merely created.
export HADOOP_CONF_DIR = $(AUTOMATION_DIR)/tools/conf/hadoop
HDFS_CMD           = hdfs dfs
HDFS_PREPARE_SQL   = $(AUTOMATION_DIR)/prepare/prepare_hdfs/hdfs_smoke.sql

REGRESS = fdw_binding

# The fixture is confirmed by re-testing the directory, not by testing
# beeline's exit status: the status of a pipeline is tail's, so a beeline
# failure would be swallowed and pg_regress would run against a fixture that
# is not there, reporting a preparation failure as a test failure.  Re-testing
# also covers a beeline that exits 0 with its SQL having failed.
#
# The probe against / comes first because $(HDFS_CMD) is not always there to
# probe with.  CI's runner has no hdfs CLI -- it prepares this fixture with
# `docker exec hiveserver2 beeline -f hdfs_smoke.sql` before the smoke run --
# and the suite is gated on the namenode's port answering, not on the CLI
# existing.  Without this arm the absent CLI reads as an absent fixture and
# the suite aborts wherever it would otherwise have passed.
installcheck:
	@echo "=== Ensure HDFS text fixture exists ==="
	@if ! $(HDFS_CMD) -test -d / 2>/dev/null; then \
		echo "  no usable hdfs CLI here; leaving the fixture to the test"; \
	elif $(HDFS_CMD) -test -d /test/basic/text/simple 2>/dev/null; then \
		echo "  /test/basic/text/simple already present, reusing"; \
	else \
		echo "  /test/basic/text/simple missing, preparing via beeline"; \
		beeline -u "jdbc:hive2://hiveserver2:10000/default" -n hive --silent=true \
			-f "$(HDFS_PREPARE_SQL)" 2>&1 | grep -vE '^(SLF4J|WARNING:|INFO|Connecting|Closing)' | tail -5; \
		if ! $(HDFS_CMD) -test -d /test/basic/text/simple 2>/dev/null; then \
			echo "ERROR: beeline left /test/basic/text/simple absent; aborting" >&2; \
			exit 1; \
		fi; \
	fi
	$(pg_regress_installcheck) $(REGRESS)

generate-expected:
	@if [ -d results ]; then mkdir -p expected; cp results/*.out expected/; \
		echo "Copied results to expected/"; else echo "Run installcheck first."; fi
