# Iceberg AM time-travel field-id regression.
#
# Unlike the pg_regress-based smoke tests, this one needs an external Spark
# step to build a multi-schema Iceberg fixture (CBDB blocks in-place ALTER)
# and captures Spark-assigned snapshot ids at runtime, so it is driven by
# run.sh rather than pg_regress.  See run.sh for the required containers and
# overridable variables (DB / CONTAINER / SPARK_CONTAINER / LAKEHOUSE).

installcheck:
	./run.sh

clean:
	rm -rf results/*.out regression.diffs regression.out tmp_check/ log/
