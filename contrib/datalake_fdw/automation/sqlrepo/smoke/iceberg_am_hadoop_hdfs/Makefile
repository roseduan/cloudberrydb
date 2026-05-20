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

PGXS := $(shell pg_config --pgxs)
include $(PGXS)

REGRESS = iceberg_am_hadoop_hdfs_basic

installcheck:
	$(pg_regress_installcheck) $(REGRESS)

clean:
	rm -rf results/ regression.diffs regression.out tmp_check/ log/
