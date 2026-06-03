# FDW Write Feature Tests Makefile

PGXS := $(shell pg_config --pgxs)
include $(PGXS)

REGRESS = 01_format_writes 02_write_edge_cases

S3_PATHS = /warehouse/fdw-test/feature/write/ /warehouse/fdw-test/feature/write_edge/

installcheck: clean-s3
	$(pg_regress_installcheck) $(REGRESS)

clean-s3:
	@for p in $(S3_PATHS); do \
		MC_HOST_minio=http://$${MINIO_ACCESS_KEY:-admin}:$${MINIO_SECRET_KEY:-admin12345}@$${MINIO_HOST:-minio}:$${MINIO_PORT:-9000} \
		mc rm --recursive --force minio$$p 2>/dev/null; \
	done; true

clean:
	rm -rf results/ regression.diffs regression.out tmp_check/ log/
