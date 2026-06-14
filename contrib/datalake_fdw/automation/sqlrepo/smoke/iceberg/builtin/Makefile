# Iceberg builtin smoke test Makefile

PGXS := $(shell pg_config --pgxs)
include $(PGXS)

REGRESS = iceberg_buildin iceberg_buildin_statistics iceberg_buildin_vacuum iceberg_buildin_deferred_commit \
          iceberg_buildin_distribution iceberg_buildin_meta_safety \
          iceberg_catalog_options iceberg_volume_options iceberg_ddl_types \
          iceberg_dml_edge_cases iceberg_update_subquery iceberg_acid_double_update \
          iceberg_metadata_cache iceberg_namespace \
          iceberg_deferred_commit_extended iceberg_volume_hdfs iceberg_vacuum_extended \
          iceberg_analyze iceberg_delete_filters iceberg_toolkit_ops \
          iceberg_write_types iceberg_delete_coverage iceberg_volume_ops \
          iceberg_catalog_ops iceberg_toolkit_errors iceberg_fragment_cache \
          iceberg_coverage_boost iceberg_deep_coverage iceberg_internal_funcs \
          iceberg_alter_unsupported \
          iceberg_drop_schema_cleanup

installcheck:
	$(pg_regress_installcheck) $(REGRESS)

clean:
	rm -rf results/ regression.diffs regression.out tmp_check/ log/
