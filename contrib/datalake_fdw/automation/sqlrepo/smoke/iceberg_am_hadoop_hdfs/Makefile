# Hadoop-catalog over HDFS volume functional smoke for Iceberg AM.
#
# Verifies type='hadoop' catalog with type='hdfs' volume against the
# lakehouse HDFS namenode at hdfs://hadoop:8020.  Exercises the
# new processVolumeHdfsResource agent helper plus the C-side HDFS
# namenode parsing in iceberg_volume_fdw.parseVolumeOption.
#
# Requires:
#   - HDFS namenode at hadoop:8020 (no Kerberos, simple auth)
#   - /iceberg_hadoop_hdfs writable by gpadmin
#
# The warehouse paths are deliberately NOT pre-cleaned, and the tests leave
# their tables in place.  gophermeta registers a file's length and Iceberg's
# hadoop catalog names metadata files vN.metadata.json, so wiping the warehouse
# while that cache survives makes the next write collide with a stale
# registration -- which gopher reports as a FATAL that takes the agent JVM down
# with it, turning a 15-second suite into an eight-minute cascade of curl 7.
# Keeping the warehouse monotonic avoids the collision; the tests clear their
# rows in SQL.

PGXS := $(shell pg_config --pgxs)
include $(PGXS)

REGRESS = iceberg_am_hadoop_hdfs_basic iceberg_am_hadoop_hdfs_acid_double_update \
          iceberg_am_hadoop_hdfs_metadata_cache

installcheck:
	$(pg_regress_installcheck) $(REGRESS)

generate-expected:
	@if [ -d results ]; then mkdir -p expected; cp results/*.out expected/; \
		echo "Copied results to expected/"; else echo "Run installcheck first."; fi

clean:
	rm -rf results/ regression.diffs regression.out tmp_check/ log/
