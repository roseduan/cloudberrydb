# Hadoop-catalog over S3 functional smoke for Iceberg AM.
#
# Same iceberg HadoopCatalog implementation as type='s3' but reached via
# type='hadoop'. Verifies the new agent branch in processServerResource
# routes hadoop catalog requests through processS3OrHadoopServerResource.
#
# Requires the singlecluster lakehouse stack (MinIO at minio:9000).

PGXS := $(shell pg_config --pgxs)
include $(PGXS)

REGRESS = iceberg_am_hadoop_s3_basic iceberg_am_hadoop_s3_acid_double_update

installcheck:
	$(pg_regress_installcheck) $(REGRESS)

clean:
	rm -rf results/ regression.diffs regression.out tmp_check/ log/
