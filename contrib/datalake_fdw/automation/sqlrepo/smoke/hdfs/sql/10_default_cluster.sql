-- Test 10: Default HDFS cluster
-- Purpose: a gphdfs:// external table that omits hdfs_cluster_name resolves
-- the cluster from the top-level "default" key in gphdfs.conf.

-- Test 1: omit hdfs_cluster_name -> use gphdfs.conf "default" (paa_cluster)
DROP EXTERNAL TABLE IF EXISTS test_default_read;
CREATE READABLE EXTERNAL TABLE test_default_read(id int, col text, col2 decimal(10,2))
LOCATION('gphdfs://test/basic/text/simple')
FORMAT 'text';

SELECT * FROM test_default_read ORDER BY id LIMIT 5;
SELECT COUNT(*) FROM test_default_read;

DROP EXTERNAL TABLE test_default_read;

-- Test 2: an explicitly named but unknown cluster must still fail to resolve a
-- server; the default is only used when no cluster name is given.
CREATE READABLE EXTERNAL TABLE test_unknown_cluster(id int, col text, col2 decimal(10,2))
LOCATION('gphdfs://test/basic/text/simple hdfs_cluster_name=no_such_cluster')
FORMAT 'text';
