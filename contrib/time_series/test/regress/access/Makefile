#
# Makefile for time_series core access regression tests
#
# Covers hypertable / tableam / compression / chunk_append behaviour.
# CAGG / BGW tests live one level up under test/regress/.
#

subdir = contrib/time_series/test/regress/access
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

PG_REGRESS = $(top_builddir)/src/test/regress/pg_regress

REGRESS = ts_fork_storage \
          ts_batch_insert ts_compress ts_compress_types ts_compress_edge \
          ts_compress_ddl ts_compress_constraints ts_compress_policy \
          ts_big_table ts_tableam \
          ts_chunk_catalog ts_memory ts_reclaim_then_insert \
          ts_many_chunks ts_sparse_chunks \
          ts_drop_truncate_files \
          ts_drop_pax_files \
          ts_pax_mirror_repl \
          chunk_append

# --ignore-plans: skip EXPLAIN-output diffs.  time_series intentionally
# leaves plan presentation in flux (chunk-status counts removed from
# planning, future ORCA support, etc.); regression should assert row
# results, not plan formatting.
installcheck:
	$(PG_REGRESS) \
		--inputdir=./ \
		--bindir='$(bindir)' \
		--dbname=contrib_regression \
		--init-file=../../init_file \
		--ignore-plans \
		$(REGRESS)

clean:
	rm -rf results regression.diffs regression.out
