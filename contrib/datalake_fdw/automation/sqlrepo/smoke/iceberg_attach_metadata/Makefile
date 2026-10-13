# iceberg_toolkit.attach_metadata() (issue #457): re-point a fresh builtin
# iceberg table at an existing metadata.json, as gprestore does.

PGXS := $(shell pg_config --pgxs)
include $(PGXS)

REGRESS = iceberg_attach_metadata

installcheck:
	$(pg_regress_installcheck) $(REGRESS)

generate-expected:
	@if [ -d results ]; then mkdir -p expected; cp results/*.out expected/; \
		echo "Copied results to expected/"; else echo "Run installcheck first."; fi

clean:
	rm -rf results/ regression.diffs regression.out tmp_check/ log/
