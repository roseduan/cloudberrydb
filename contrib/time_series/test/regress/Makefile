#
# Makefile for time_series regression tests
#

subdir = test/regress
top_builddir = ../../../../
include $(top_builddir)/src/Makefile.global

PG_REGRESS = $(top_builddir)/src/test/regress/pg_regress

REGRESS = cagg_bgw_policy_scheduler_run bgw_scheduler \
          cagg_bgw_policy_e2e cagg_bgw_policy_multidb_e2e \
          bgw_launcher bgw_mock_time \
          cagg_bgw_policy_permissions \
          cagg_bgw_policy_api cagg_bgw_policy_run bgw_job \
          time_bucket gapfill gapfill_mpp \
          cagg_create cagg_invalidation cagg_refresh \
          cagg_union_view cagg_permissions cagg_smoke \
          cagg_fault_tolerance cagg_planning \
          bgw_teardown

installcheck:
	$(PG_REGRESS) \
		--inputdir=./ \
		--bindir='$(bindir)' \
		--dbname=contrib_regression \
		--init-file=../init_file \
		$(REGRESS)

clean:
	rm -rf results regression.diffs regression.out
